package catalog

import "context"

// CrawledMessage is one message read from a dump, normalized for indexing. Only
// messages whose caption carries our #karenidx tag are indexed in Phase 2;
// everything else (text, service, foreign media) is carried through so the crawl
// can advance past it but is skipped by ParseCaption.
type CrawledMessage struct {
	ID           int
	Caption      string // message text/caption
	FileUniqueID string // stable per-file id (the document id, as text); "" if no doc
	FileName     string // from DocumentAttributeFilename; "" if unknown
	SizeBytes    int64  // document size; 0 if no doc
}

// MessageFetcher reads a dump's history. Keeping it an interface keeps this
// package Telegram-free and unit-testable; the gotd implementation lives in
// package main (dump_index.go). Its RPCs inherit the client-level
// FloodWaitMiddleware, so the crawl is floodwait-aware for free.
type MessageFetcher interface {
	// FetchOlder returns up to limit messages with id < offsetID (or the newest
	// messages when offsetID == 0), in DESCENDING id order — exactly the native
	// messages.GetHistory shape. An empty slice means there are no older messages.
	FetchOlder(ctx context.Context, offsetID, limit int) ([]CrawledMessage, error)
}

const crawlPageSize = 100

// IndexDump crawls a dump newest→oldest, indexing every message above the stored
// checkpoint (its #karenidx caption parsed into a pointer row), and stops as soon
// as it reaches a message id <= the checkpoint. The checkpoint is then advanced to
// the newest id seen, so the next run only walks the messages added since
// (incremental + resumable across runs). Within a single interrupted run the
// checkpoint is not advanced, so a re-run re-walks from newest — upserts are
// idempotent on (dump_id, message_id), so that re-work is harmless.
//
// Phase 2 scope: only our own #karenidx captions are indexed; foreign-dump tag
// extraction (download-to-read-tags) is Phase 3. progress (optional) is called
// after each page with the running indexed count and the newest id seen.
func (c *Catalog) IndexDump(ctx context.Context, f MessageFetcher, dumpID int64, progress func(indexed, newestID int)) (int, error) {
	checkpoint, err := c.dumpCheckpoint(ctx, dumpID)
	if err != nil {
		return 0, err
	}

	indexed := 0
	newest := checkpoint
	offsetID := 0 // 0 = start from the latest message

	for {
		if ctx.Err() != nil {
			return indexed, ctx.Err()
		}
		msgs, err := f.FetchOlder(ctx, offsetID, crawlPageSize)
		if err != nil {
			return indexed, err
		}
		if len(msgs) == 0 {
			break // reached the start of the channel
		}

		reachedCheckpoint := false
		for _, msg := range msgs { // descending
			offsetID = msg.ID // next page continues older than this
			if msg.ID <= checkpoint {
				reachedCheckpoint = true
				break
			}
			if meta, ok := ParseCaption(msg.Caption); ok {
				if meta.SizeBytes == 0 {
					meta.SizeBytes = msg.SizeBytes
				}
				if err := c.UpsertTrack(ctx, dumpID, msg.ID, meta, msg.FileUniqueID, msg.Caption); err != nil {
					return indexed, err
				}
				indexed++
			}
			if msg.ID > newest {
				newest = msg.ID
			}
		}

		if progress != nil {
			progress(indexed, newest)
		}
		if reachedCheckpoint {
			break
		}
	}

	if newest > checkpoint {
		if err := c.setDumpCheckpoint(ctx, dumpID, newest); err != nil {
			return indexed, err
		}
	}
	return indexed, nil
}
