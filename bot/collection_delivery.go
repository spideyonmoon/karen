package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"

	"golang.org/x/sync/errgroup"
)

type collectionDeliveryKey struct{}

type collectionDeliveryPlan struct {
	info       *collectionMeta
	tracks     []collectionDeliveryTrack
	forceAAC   bool
	prepared   map[string]*preparedCollectionTrack
	stageChunk func(context.Context, []string, int, string) error
}

type collectionDeliveryTrack struct {
	trackID string
	dumpID  int64
	msgID   int
}

type preparedCollectionTrack struct {
	path    string
	meta    AudioMeta
	dumpID  int64
	msgID   int
}

// deliverOrderedCollection never advances past a failed batch until each item
// in that batch has been retried in place. Keep batches within one dump and in
// increasing message-ID order, so Telegram cannot reorder an old cached message
// around a newly uploaded one. Missing uploads are handled at their own position.
func deliverOrderedCollection(ctx context.Context, tracks []collectionDeliveryTrack,
	batch func(int64, []int) error, fallback func(int) bool) (int, error) {
	delivered := 0
	for i := 0; i < len(tracks); {
		if err := ctx.Err(); err != nil {
			return delivered, err
		}
		if tracks[i].dumpID == 0 || tracks[i].msgID == 0 {
			if fallback(i) {
				delivered++
			}
			i++
			continue
		}
		end := i + 1
		for end < len(tracks) && end-i < 90 &&
			tracks[end].dumpID == tracks[i].dumpID && tracks[end].msgID > tracks[end-1].msgID {
			end++
		}
		ids := make([]int, end-i)
		for j := i; j < end; j++ {
			ids[j-i] = tracks[j].msgID
		}
		if err := batch(tracks[i].dumpID, ids); err == nil {
			delivered += len(ids)
		} else {
			for j := i; j < end; j++ {
				if err := ctx.Err(); err != nil {
					return delivered, err
				}
				if fallback(j) {
					delivered++
				}
			}
		}
		i = end
	}
	return delivered, ctx.Err()
}

// deliverCatalogCollection runs only after the download phase. Prepare every
// fresh track in the dump before copying anything to the user's chat, then merge
// those message references with cached ones using the original collection list.
func (b *TelegramBot) deliverCatalogCollection(chatID int64, replyToID int, format string, status *DownloadStatus,
	ctx context.Context, plan *collectionDeliveryPlan, paths []string) {
	if ctx.Err() != nil {
		status.UpdateSync("Cancelled", 0, 0)
		return
	}
	_ = b.prepareCollectionPaths(ctx, plan, paths, format, status)
	prepared := plan.prepared
	if ctx.Err() != nil {
		status.UpdateSync("Cancelled", 0, 0)
		return
	}

	refs := append([]collectionDeliveryTrack(nil), plan.tracks...)
	for i, track := range refs {
		if track.msgID == 0 {
			if ready := prepared[track.trackID]; ready != nil {
				refs[i].dumpID, refs[i].msgID = ready.dumpID, ready.msgID
			}
		}
	}
	b.sendCollectionCover(chatID, plan.info, format, len(refs), replyToID)
	seenArtwork := make(map[string]bool)
	for _, path := range paths {
		if ctx.Err() != nil {
			status.UpdateSync("Cancelled", 0, 0)
			return
		}
		if isAnimatedArtwork(path) && !seenArtwork[path] {
			seenArtwork[path] = true
			caption := "Animated Artwork"
			if strings.Contains(filepath.Base(path), "tall") {
				caption = "Animated Artwork (Tall)"
			}
			_ = b.sendVideoWithReply(chatID, path, caption, replyToID)
		}
	}
	status.Update("Delivering", 0, 0)
	delivered, err := deliverOrderedCollection(ctx, refs, func(dumpID int64, ids []int) error {
		return b.pool.DeliverManyFromDump(ctx, dumpID, ids, chatID, replyToID)
	}, func(i int) bool {
		track := refs[i]
		if track.dumpID != 0 && track.msgID != 0 {
			if err := b.pool.DeliverFromDump(ctx, track.dumpID, track.msgID, chatID, replyToID); err == nil {
				return true
			}
		}
		ready := prepared[track.trackID]
		if ready != nil {
			if _, err := os.Stat(ready.path); err != nil {
				ripStateFrom(ctx).forgetDone(ready.meta.AlbumID)
				ready = nil // A staged chunk may already have reclaimed this file.
			}
		}
		// A deleted cache message is recovered in place, before later tracks go out.
		if ready == nil && track.msgID != 0 && ctx.Err() == nil {
			status.Update(fmt.Sprintf("Recovering track %d/%d", i+1, len(refs)), 0, 0)
			ripCtx := ctx
			if plan.info.isPlaylist {
				ripCtx = context.WithValue(ctx, playlistCollectionKey{}, plan.info)
			}
			if err := ripSong(track.trackID, b.appleToken, Config.Storefront, plan.forceAAC, ripCtx); err != nil {
				recordDownloadFailure(ctx, "Track %s: %v", track.trackID, err)
			}
			for _, path := range ripStateFrom(ctx).snapshotPaths() {
				if meta, ok := getDownloadedMeta(ctx, path); ok && meta.TrackID == track.trackID {
					ready = &preparedCollectionTrack{path: path, meta: meta}
					prepared[track.trackID] = ready
				}
			}
			if ready != nil && ctx.Err() == nil {
				b.prepareCollectionTrack(ctx, ready, format, status)
				if ready.msgID != 0 {
					if err := b.pool.DeliverFromDump(ctx, ready.dumpID, ready.msgID, chatID, replyToID); err == nil {
						return true
					}
				}
			}
		}
		if ready != nil && b.deliverSingleTrackFallback(chatID, ready.path, replyToID, format, ready.meta, true, status, ctx) {
			return true
		}
		recordDownloadFailure(ctx, "Track %s could not be delivered", track.trackID)
		return false
	})
	if err != nil {
		status.UpdateSync(friendlyTaskError("Delivery interrupted", err), 0, 0)
		return
	}
	if delivered == len(refs) {
		status.UpdateSync(fmt.Sprintf("✅ Delivered %d tracks.", delivered), 0, 0)
		status.Stop()
		return
	}
	status.UpdateSync(fmt.Sprintf("Delivered %d/%d tracks. %s", delivered, len(refs), downloadFailureSummary(ctx)), 0, 0)
	_ = b.sendMessageWithReply(chatID, fmt.Sprintf("⚠️ Delivered %d/%d tracks. %s", delivered, len(refs), downloadFailureSummary(ctx)), nil, replyToID)
}

// The disk-pressure checkpoint stages files in the dump without delivering them.
// A failed stage keeps its source files, using RipState's existing flush rules.
func (b *TelegramBot) prepareCollectionPaths(ctx context.Context, plan *collectionDeliveryPlan, paths []string, format string, status *DownloadStatus) error {
	var pending []*preparedCollectionTrack
	var unsupported bool
	seen := make(map[string]bool)
	for _, path := range paths {
		if meta, ok := getDownloadedMeta(ctx, path); ok && meta.TrackID != "" {
			if seen[meta.TrackID] {
				continue
			}
			seen[meta.TrackID] = true
			if ready := plan.prepared[meta.TrackID]; ready != nil && ready.msgID != 0 {
				continue
			}
			if _, exists := plan.prepared[meta.TrackID]; !exists {
				ready := &preparedCollectionTrack{path: path, meta: meta}
				plan.prepared[meta.TrackID] = ready
				pending = append(pending, ready)
			} else {
				pending = append(pending, plan.prepared[meta.TrackID])
			}
		} else {
			unsupported = true
		}
	}

	var group errgroup.Group
	if n := b.pool.Size(); n > 0 {
		group.SetLimit(n)
	}
	var done atomic.Int64
	for _, ready := range pending {
		group.Go(func() error {
			if ctx.Err() != nil {
				return nil
			}
			b.prepareCollectionTrack(ctx, ready, format, status)
			status.Update(fmt.Sprintf("Uploading %d/%d", done.Add(1), len(pending)), 0, 0)
			return nil
		})
	}
	_ = group.Wait()
	if err := ctx.Err(); err != nil {
		return err
	}
	for _, ready := range pending {
		if ready.msgID == 0 {
			return fmt.Errorf("track %s could not be staged", ready.meta.TrackID)
		}
	}
	if unsupported {
		return fmt.Errorf("chunk contains supplementary files; retaining sources")
	}
	return nil
}

func (b *TelegramBot) prepareCollectionTrack(ctx context.Context, ready *preparedCollectionTrack, format string, status *DownloadStatus) {
	m := buildTrackMeta(ready.path, format, ready.meta, true)
	dumpID, msgID, err := b.pool.uploadToDump(ctx, ready.path, m, status)
	if err != nil {
		fmt.Printf("collection dump upload failed for %s: %v\n", ready.path, err)
		return
	}
	ready.dumpID, ready.msgID = dumpID, msgID
	if b.catalog != nil {
		if err := b.catalog.IndexInline(ctx, dumpID, msgID, m); err != nil {
			fmt.Printf("catalog IndexInline (msg=%d): %v\n", msgID, err)
		}
	}
}
