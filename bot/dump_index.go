package main

import (
	"context"
	"fmt"
	"strconv"

	"main/catalog"

	"github.com/gotd/td/tg"
)

// mtprotoFetcher adapts MTProtoClient to catalog.MessageFetcher so the catalog's
// gotd-free crawler (catalog/indexer.go) can read a dump's history. It resolves
// the peer once at construction and pages messages.GetHistory natively
// (newest-first). FloodWait retry is already installed as a client middleware in
// mtproto.go, so these RPCs are floodwait-aware without extra handling.
type mtprotoFetcher struct {
	m    *MTProtoClient
	peer tg.InputPeerClass
}

// newMTProtoFetcher resolves the dump channel and returns a fetcher plus the
// channel's access_hash (for the dumps table). dumpID is a Bot API channel id
// (-100…). Returns an error if the client isn't ready or the channel can't be
// resolved (e.g. the bot isn't an admin/member).
func newMTProtoFetcher(ctx context.Context, m *MTProtoClient, dumpID int64) (*mtprotoFetcher, int64, error) {
	if m == nil {
		return nil, 0, fmt.Errorf("MTProto client not configured")
	}
	peer, err := m.resolveInputPeer(ctx, dumpID)
	if err != nil {
		return nil, 0, fmt.Errorf("resolve dump %d: %w", dumpID, err)
	}
	var accessHash int64
	if ch, ok := peer.(*tg.InputPeerChannel); ok {
		accessHash = ch.AccessHash
	}
	return &mtprotoFetcher{m: m, peer: peer}, accessHash, nil
}

// FetchOlder returns up to limit messages with id < offsetID (or the newest when
// offsetID == 0), descending — the native messages.GetHistory shape. All message
// kinds are returned (so the crawler can advance past text/service messages); only
// those whose caption carries #karenidx are indexed by IndexDump.
func (f *mtprotoFetcher) FetchOlder(ctx context.Context, offsetID, limit int) ([]catalog.CrawledMessage, error) {
	api, _, ready := f.m.snapshot()
	if !ready || api == nil {
		return nil, fmt.Errorf("MTProto client not ready")
	}
	res, err := api.MessagesGetHistory(ctx, &tg.MessagesGetHistoryRequest{
		Peer:     f.peer,
		OffsetID: offsetID,
		Limit:    limit,
	})
	if err != nil {
		return nil, fmt.Errorf("GetHistory(offset=%d): %w", offsetID, err)
	}

	raw, ok := historyMessages(res)
	if !ok {
		return nil, nil
	}
	out := make([]catalog.CrawledMessage, 0, len(raw))
	for _, mc := range raw {
		cm := catalog.CrawledMessage{ID: mc.GetID()}
		if msg, ok := mc.(*tg.Message); ok {
			cm.Caption = msg.Message
			if doc := documentOf(msg); doc != nil {
				cm.FileUniqueID = strconv.FormatInt(doc.ID, 10)
				cm.SizeBytes = doc.Size
				cm.FileName = filenameOf(doc)
			}
		}
		out = append(out, cm)
	}
	return out, nil
}

// historyMessages extracts the message slice from a messages.GetHistory response.
// ok is false for MessagesMessagesNotModified (no messages to read).
func historyMessages(res tg.MessagesMessagesClass) ([]tg.MessageClass, bool) {
	switch v := res.(type) {
	case *tg.MessagesMessages:
		return v.Messages, true
	case *tg.MessagesMessagesSlice:
		return v.Messages, true
	case *tg.MessagesChannelMessages:
		return v.Messages, true
	default:
		return nil, false
	}
}

// documentOf returns the document attached to a message, or nil if the message
// carries no document media (text, photo, service, etc.).
func documentOf(msg *tg.Message) *tg.Document {
	media, ok := msg.GetMedia()
	if !ok {
		return nil
	}
	mediaDoc, ok := media.(*tg.MessageMediaDocument)
	if !ok {
		return nil
	}
	doc, ok := mediaDoc.Document.(*tg.Document)
	if !ok {
		return nil
	}
	return doc
}

// filenameOf returns the document's filename attribute, or "" if absent.
func filenameOf(doc *tg.Document) string {
	for _, attr := range doc.Attributes {
		if fn, ok := attr.(*tg.DocumentAttributeFilename); ok {
			return fn.FileName
		}
	}
	return ""
}
