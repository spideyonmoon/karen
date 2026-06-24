package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"

	"main/catalog"

	"golang.org/x/sync/errgroup"
)

// Pool is the Phase 1 multi-account upload engine (docs/CATALOG_ARCHITECTURE.md
// §4.5, PHASE1_MULTI_ACCOUNT_UPLOAD.md). It owns a set of helper bot clients that
// upload files in parallel to a shared dump channel, plus a reference to the main
// bot client which performs the DropAuthor copy that delivers a stored file to a
// user. Telegram throttles per account, so spreading an album's tracks across N
// helpers divides both the wall-time and the FLOOD_WAIT pressure.
//
// Layout note (deviation from the doc's "bot/dump package"): the whole bot is a
// single `package main`, and the Pool reuses unexported internals of mtproto.go
// (the tuned uploader, FloodWait middleware, reconnect supervisor, peer cache) as
// well as DownloadStatus. A separate package would create an import cycle, so the
// Pool lives in package main. The frozen §4.5 method set (UploadToDump,
// DeliverFromDump) is honored exactly so Phase 2 can call it post-merge.
type Pool struct {
	main       *MTProtoClient // the account the user talks to; performs delivery copies
	dumpChatID int64          // Bot API -100… id of the dump channel
	helpers    []*helperSlot
}

// helperSlot pairs a helper client with its in-flight load counter. load is the
// number of uploads currently assigned to the helper (incremented on Acquire,
// decremented on Release) and drives least-loaded scheduling.
type helperSlot struct {
	client *MTProtoClient
	load   atomic.Int64
}

// NewUploadPool boots the helper clients in parallel and returns a ready Pool.
// main is the already-running main bot MTProto client (it also resolves the dump
// peer for delivery). Returns an error if any helper fails first auth — the caller
// treats that as "pool unavailable" and falls back to the single-client path.
func NewUploadPool(ctx context.Context, main *MTProtoClient, apiID int, apiHash string, helperTokens []string, dumpChatID int64) (*Pool, error) {
	if main == nil {
		return nil, fmt.Errorf("upload pool requires a main MTProto client")
	}
	if dumpChatID == 0 {
		return nil, fmt.Errorf("upload pool requires a dump channel id")
	}
	if len(helperTokens) == 0 {
		return nil, fmt.Errorf("upload pool requires at least one helper token")
	}

	p := &Pool{main: main, dumpChatID: dumpChatID}
	p.helpers = make([]*helperSlot, len(helperTokens))

	// Helpers maintain their own long-lived connection contexts (set up in their
	// constructor), so boot uses a plain errgroup just to parallelize first-auth.
	var g errgroup.Group
	for i, tok := range helperTokens {
		g.Go(func() error {
			label := fmt.Sprintf("helper-%d", i+1)
			c, err := NewHelperMTProtoClient(apiID, apiHash, tok, label)
			if err != nil {
				return fmt.Errorf("%s: %w", label, err)
			}
			p.helpers[i] = &helperSlot{client: c}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		p.Close() // tear down any helpers that did come up
		return nil, err
	}
	fmt.Printf("Upload pool ready: %d helper(s) → dump %d\n", len(p.helpers), dumpChatID)
	return p, nil
}

// Ready reports whether the pool can serve work: the main client is up and at
// least one helper is connected. Callers gate on this and fall back to the
// single-client upload path when false.
func (p *Pool) Ready() bool {
	if p == nil || p.main == nil || len(p.helpers) == 0 {
		return false
	}
	if !p.main.IsReady() {
		return false
	}
	for _, h := range p.helpers {
		if h != nil && h.client.IsReady() {
			return true
		}
	}
	return false
}

// Size returns the number of helper clients in the pool.
func (p *Pool) Size() int {
	if p == nil {
		return 0
	}
	return len(p.helpers)
}

// Acquire returns the index and client of the least-loaded READY helper and
// increments its load. The caller MUST call Release(idx) when done (defer it). If
// no helper is currently ready it still returns the least-loaded one so the upload
// can wait out a reconnect rather than fail outright.
func (p *Pool) Acquire() (int, *MTProtoClient) {
	best := -1
	var min int64
	for i, h := range p.helpers {
		if !h.client.IsReady() {
			continue
		}
		v := h.load.Load()
		if best == -1 || v < min {
			best, min = i, v
		}
	}
	if best == -1 {
		best, min = 0, p.helpers[0].load.Load()
		for i := 1; i < len(p.helpers); i++ {
			if v := p.helpers[i].load.Load(); v < min {
				best, min = i, v
			}
		}
	}
	p.helpers[best].load.Add(1)
	return best, p.helpers[best].client
}

// Release returns a helper's slot to the scheduler.
func (p *Pool) Release(idx int) { p.helpers[idx].load.Add(-1) }

// UploadToDump uploads one file to the dump via the least-loaded helper, writes the
// canonical caption (FormatCaption(m)), and returns the durable handle
// (dumpID, msgID). It is the frozen §4.5 entry point Phase 2's MISS path calls.
// The caller MUST have populated m.Kind, m.LyricsSync, m.CoverEmbedded and
// m.Variant (=VariantKey(m)) from the effective rip prefs before calling.
func (p *Pool) UploadToDump(ctx context.Context, path string, m catalog.TrackMeta) (dumpID int64, msgID int, err error) {
	return p.uploadToDump(ctx, path, m, nil)
}

// uploadToDump is UploadToDump with an optional progress board for the Phase 1
// delivery wiring (the frozen seam carries no status).
func (p *Pool) uploadToDump(ctx context.Context, path string, m catalog.TrackMeta, status *DownloadStatus) (int64, int, error) {
	if p == nil || len(p.helpers) == 0 {
		return 0, 0, fmt.Errorf("upload pool not available")
	}
	idx, client := p.Acquire()
	defer p.Release(idx)

	caption := catalog.FormatCaption(m)
	var (
		mid int
		err error
	)
	switch m.Kind {
	case "", catalog.KindTrack:
		// Derive a Telegram thumbnail from the album cover if present (the audio
		// file usually already embeds cover art; this is a best-effort nicety).
		thumb := ""
		if cover := findCoverFile(filepath.Dir(path)); cover != "" {
			if tp, e := makeTelegramThumb(cover); e == nil {
				thumb = tp
				defer os.Remove(tp)
			}
		}
		mid, err = client.UploadAudioToDump(ctx, p.dumpChatID, path, m.Title, m.Artist, m.DurationSec, caption, thumb, status)
	default:
		// album_zip / cover / lrc / anything else → plain document.
		mid, err = client.UploadDocumentToDump(ctx, p.dumpChatID, path, filepath.Base(path), caption, status)
	}
	if err != nil {
		return 0, 0, err
	}
	return p.dumpChatID, mid, nil
}

// DeliverFromDump copies an already-stored dump message to a recipient with no
// "forwarded from" header (DropAuthor). This is what a catalog HIT calls; the
// Phase 1 MISS path calls it right after UploadToDump. Delivery always runs on the
// main bot (the account the user talks to).
func (p *Pool) DeliverFromDump(ctx context.Context, dumpID int64, msgID int, recipientID int64, replyToID int) error {
	if p == nil || p.main == nil {
		return fmt.Errorf("upload pool not available")
	}
	return p.main.DeliverFromDump(ctx, dumpID, msgID, recipientID, replyToID)
}

// DeliverManyFromDump copies several same-dump messages to a recipient in one
// forward RPC (DropAuthor), in msgIDs order. Used by the MISS path to deliver a
// whole album with one send instead of one per track.
func (p *Pool) DeliverManyFromDump(ctx context.Context, dumpID int64, msgIDs []int, recipientID int64, replyToID int) error {
	if p == nil || p.main == nil {
		return fmt.Errorf("upload pool not available")
	}
	return p.main.DeliverManyFromDump(ctx, dumpID, msgIDs, recipientID, replyToID)
}

// DumpID returns the configured dump channel id (Bot API -100… form).
func (p *Pool) DumpID() int64 {
	if p == nil {
		return 0
	}
	return p.dumpChatID
}

// ResolveDumpAccessHash returns the dump channel's access hash via the main
// client. Phase 2 calls this once at startup to register the dumps row
// (catalog.UpsertDump) BEFORE any inline index, satisfying the tracks→dumps FK.
func (p *Pool) ResolveDumpAccessHash(ctx context.Context) (int64, error) {
	if p == nil || p.main == nil {
		return 0, fmt.Errorf("upload pool not available")
	}
	return p.main.ChannelAccessHash(ctx, p.dumpChatID)
}

// Close tears down the helper clients (and their supervisors). The main client is
// owned and closed elsewhere.
func (p *Pool) Close() {
	if p == nil {
		return
	}
	for _, h := range p.helpers {
		if h != nil && h.client != nil {
			h.client.Close()
		}
	}
}
