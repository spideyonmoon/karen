package main

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/uploader"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// mtprotoLogger builds the gotd internal logger. gotd logs transport- and
// engine-level events (reconnects, read/write errors, the reason an engine is
// torn down) through this zap logger; without it those reasons are invisible and
// we only see the wrapped "engine forcibly closed: context canceled" at the call
// site. We log at Info so we capture connection lifecycle and teardown causes
// without the per-message Debug firehose. Output goes to stderr, interleaving
// with the bot's existing fmt logs in `docker compose logs`.
func mtprotoLogger() *zap.Logger {
	cfg := zap.NewProductionEncoderConfig()
	cfg.TimeKey = "ts"
	cfg.EncodeTime = zapcore.ISO8601TimeEncoder
	core := zapcore.NewCore(
		zapcore.NewConsoleEncoder(cfg),
		zapcore.Lock(os.Stderr),
		zapcore.InfoLevel,
	)
	return zap.New(core).Named("gotd")
}

func cryptoRandID() int64 {
	var b [8]byte
	_, _ = cryptorand.Read(b[:])
	return int64(binary.LittleEndian.Uint64(b[:]))
}

// MTProtoClient wraps the gotd/td client for direct Telegram uploads.
//
// A single supervisor goroutine keeps the connection alive: when client.Run
// returns (a terminal disconnect — gotd handles transient blips internally), it
// rebuilds a fresh client and re-runs with capped exponential backoff. The
// connection-dependent fields (client/api/runCtx/ready) are rebuilt each cycle
// and guarded by stateMu so readers never touch a torn-down client.
type MTProtoClient struct {
	// Immutable after construction.
	apiID       int
	apiHash     string
	botToken    string
	sessionPath string
	storage     *FileSessionStorage
	logger      *zap.Logger
	parentCtx   context.Context
	cancel      context.CancelFunc

	// Guarded by stateMu, rebuilt each connection cycle.
	stateMu sync.RWMutex
	client  *telegram.Client
	api     *tg.Client
	runCtx  context.Context
	ready   bool

	peerMu sync.Mutex
	peers  map[int64]tg.InputPeerClass

	// uploadGate serializes all uploads to a single in-flight transfer. Telegram
	// uploads share one DC connection, and concurrent multi-part uploads from two
	// tasks starve each other and trip FloodWait; one TG upload at a time is the
	// safe ceiling. It is a size-1 channel rather than a sync.Mutex so a queued
	// upload still honors ctx cancellation (/stop, shutdown) while it waits.
	uploadGate chan struct{}
}

// acquireUploadGate blocks until the single upload slot is free or ctx is done.
// Returns a release func (nil if ctx fired first, in which case the returned error
// is non-nil and the caller must not upload).
func (m *MTProtoClient) acquireUploadGate(ctx context.Context) (func(), error) {
	select {
	case m.uploadGate <- struct{}{}:
		return func() { <-m.uploadGate }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// snapshot returns the current api client, its per-cycle context, and readiness
// under a read lock so callers operate on a consistent view even while the
// supervisor is rebuilding the client mid-reconnect.
func (m *MTProtoClient) snapshot() (*tg.Client, context.Context, bool) {
	m.stateMu.RLock()
	defer m.stateMu.RUnlock()
	return m.api, m.runCtx, m.ready
}

// uploadMaxAttempts bounds how many times a non-resumable upload (single audio/doc/
// video) is retried across reconnects before the caller falls back to Gofile — each
// retry restarts from byte zero, so it stays small. groupUploadMaxAttempts is larger
// because the group upload resumes (see UploadAndSendAudioGroup): a retry re-sends only
// the items that haven't registered yet, so the budget counts tolerated reconnects, not
// full re-uploads. uploadReadyWait bounds how long each attempt waits for the supervisor
// to bring the connection back.
const (
	uploadMaxAttempts      = 3
	groupUploadMaxAttempts = 10
	uploadReadyWait        = 45 * time.Second
)

// uploadPartSize is Telegram's maximum upload part size (512 KB) — cannot go higher.
// uploadThreads is how many parts gotd keeps in flight concurrently over the single
// DC connection. On a high-bandwidth, high-latency path (e.g. NZ→Telegram DC5, ~176ms
// RTT) the throughput ceiling is roughly (uploadThreads × uploadPartSize) ÷ RTT, so the
// thread count is the dominant speed lever:
//   threads=8  → 4 MB in flight → ~22 MB/s ceiling
//   threads=16 → 8 MB in flight → ~45 MB/s ceiling
//
// History: 16 originally starved gotd's keepalive ping write — at 8 MB in flight the
// DC5 TCP send buffer (then the 4 MB kernel default) had no room for the ping → "pong
// missed" i/o timeout → engine teardown → upload drop/resume sawtooth (see AGENTS.md).
// The fix was capping at 8. That blocker is now removed: docker-compose.yml sets
// net.ipv4.tcp_wmem max to 16 MB (verified live in the container), so the in-flight
// window can grow while leaving room for the keepalive ping.
//
// At 16 the sawtooth was cured (clean log over a full 168 MB upload). Trialing 20 (10 MB
// in flight = 62% of the 16 MB buffer) RE-INTRODUCED the teardown: dc_id=5 "engine
// forcibly closed: context canceled" mid-upload (e.g. part 197), the same drop/resume
// sawtooth. So 20 over-fills the send buffer and re-starves the keepalive write even at
// 16 MB — the safe ceiling on this single connection is 16. Throughput at 16 stays
// moderate (the displayed ~6-7 MB/s is itself suspect — known progress-board display
// bug), but it is STABLE. gotd serializes RPC writes over the one connection, so the
// single connection is protocol-saturated; only a multi-connection path (Pyrofork
// sidecar or a Go client pool) lifts it further. DO NOT raise above 16 without a buffer
// increase. Watch the zap log for "pong missed" / "engine forcibly closed".
const (
	uploadPartSize = 512 * 1024
	uploadThreads  = 16
)

// awaitReady blocks until the supervisor has a live, authenticated client again and
// returns the fresh api. It returns (nil, false) if the caller's ctx ends, the client
// is shutting down, or the wait times out — letting an upload ride out a reconnect
// window instead of bailing straight to Gofile.
func (m *MTProtoClient) awaitReady(ctx context.Context, timeout time.Duration) (*tg.Client, bool) {
	if api, _, ready := m.snapshot(); ready && api != nil {
		return api, true
	}
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil, false
		case <-m.parentCtx.Done():
			return nil, false
		case <-ticker.C:
			if api, _, ready := m.snapshot(); ready && api != nil {
				return api, true
			}
			if time.Now().After(deadline) {
				return nil, false
			}
		}
	}
}

// isTransientConnErr reports whether err is a gotd connection/engine teardown caused
// by a reconnect (gotd's "engine was closed" / "engine forcibly closed" / "connection
// dead"), rather than a permanent failure. The forcibly-closed variant unwraps to
// context.Canceled, so callers MUST first confirm their own ctx is still alive — a real
// /stop unwraps to the same error and must not be retried.
func isTransientConnErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "engine was closed") ||
		strings.Contains(msg, "engine forcibly closed") ||
		strings.Contains(msg, "connection dead")
}

// withUploadRetry runs fn against a live client, retrying transient connection
// teardowns after waiting for the supervisor to reconnect. It aborts immediately if
// the caller's ctx is done (a real /stop or shutdown), so cancellation still
// propagates. After exhausting attempts it returns the last error so the caller can
// fall back to Gofile. fn must be safe to re-run: it should not post a Telegram message
// until its uploads succeed (all callers register media first and send once at the end).
func (m *MTProtoClient) withUploadRetry(ctx context.Context, op string, fn func(api *tg.Client) error) error {
	return m.withUploadRetryN(ctx, op, uploadMaxAttempts, fn)
}

// withUploadRetryN is withUploadRetry with an explicit attempt budget. Resumable uploads
// (the audio group) pass a larger budget because each retry only re-sends what hasn't
// already succeeded, so a higher cap costs reconnect-waits rather than re-uploaded bytes.
func (m *MTProtoClient) withUploadRetryN(ctx context.Context, op string, maxAttempts int, fn func(api *tg.Client) error) error {
	// Serialize to one in-flight upload across all tasks. Held across the retry
	// loop so a reconnect-retry of the same upload doesn't yield the slot to a
	// different task mid-transfer.
	release, err := m.acquireUploadGate(ctx)
	if err != nil {
		return err
	}
	defer release()

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		api, ok := m.awaitReady(ctx, uploadReadyWait)
		if !ok {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("MTProto client not ready")
		}
		err := fn(api)
		if err == nil {
			return nil
		}
		// A real caller cancellation is terminal — never retry past /stop or shutdown.
		if ctx.Err() != nil {
			return err
		}
		if !isTransientConnErr(err) {
			return err
		}
		lastErr = err
		fmt.Printf("MTProto %s hit transient connection error (attempt %d/%d): %v; waiting for reconnect...\n", op, attempt, maxAttempts, err)
	}
	return lastErr
}

// FloodWaitMiddleware automatically catches FLOOD_WAIT errors, sleeps for the required duration, and retries the RPC call.
type FloodWaitMiddleware struct{}

// maxFloodWaitTotal caps the cumulative time a single RPC will spend sleeping on
// FLOOD_WAIT before giving up. Without it, one large FLOOD_WAIT (Telegram can hand
// out hours) would block the single download worker indefinitely. Beyond the cap
// the error propagates so the caller falls back to Gofile.
const maxFloodWaitTotal = 5 * time.Minute

func (f FloodWaitMiddleware) Handle(next tg.Invoker) telegram.InvokeFunc {
	return func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
		var slept time.Duration
		for {
			before := time.Now()
			err := next.Invoke(ctx, input, output)
			if err == nil {
				return nil
			}
			// tgerr.FloodWait sleeps internally (respecting ctx) and reports whether
			// the error was a FLOOD_WAIT. We approximate the slept duration from the
			// time spent in this Invoke call and stop once it exceeds the cap.
			if waited, _ := tgerr.FloodWait(ctx, err); waited {
				slept += time.Since(before)
				if slept >= maxFloodWaitTotal {
					fmt.Printf("FLOOD_WAIT exceeded cap (%s slept); aborting RPC so caller can fall back.\n", slept.Truncate(time.Second))
					return err
				}
				fmt.Println("FLOOD_WAIT encountered in MTProto client. Automatically slept and retrying...")
				continue
			}
			return err
		}
	}
}

// UploadProgress reports upload progress to a DownloadStatus message.
type UploadProgress struct {
	status *DownloadStatus
	phase  string
}

// Chunk is called by the gotd uploader after each uploaded chunk.
func (p *UploadProgress) Chunk(ctx context.Context, state uploader.ProgressState) error {
	if p.status != nil {
		p.status.Update(p.phase, state.Uploaded, state.Total)
	}
	return nil
}

// NewMTProtoClient creates and authenticates a new MTProto client using the bot token.
// It blocks until the first authentication completes (or initially fails), then keeps
// the connection alive via a supervisor goroutine that reconnects on disconnect.
func NewMTProtoClient(apiID int, apiHash string, botToken string, sessionDir string) (*MTProtoClient, error) {
	if apiID == 0 || apiHash == "" {
		return nil, fmt.Errorf("telegram-api-id and telegram-api-hash must be set for MTProto uploads")
	}

	parentCtx, cancel := context.WithCancel(context.Background())

	sessionPath := filepath.Join(sessionDir, "mtproto-session.json")

	m := &MTProtoClient{
		apiID:       apiID,
		apiHash:     apiHash,
		botToken:    botToken,
		sessionPath: sessionPath,
		storage:     &FileSessionStorage{Path: sessionPath},
		logger:      mtprotoLogger(),
		parentCtx:   parentCtx,
		cancel:      cancel,
		peers:       make(map[int64]tg.InputPeerClass),
		uploadGate:  make(chan struct{}, 1),
	}

	// firstResult delivers the outcome of the FIRST connection cycle only, so
	// NewMTProtoClient keeps its original contract: block until first auth.
	firstResult := make(chan error, 1)
	go m.supervise(firstResult)

	if err := <-firstResult; err != nil {
		cancel()
		return nil, err
	}
	return m, nil
}

// supervise keeps the MTProto connection alive across disconnects. Each cycle builds
// a fresh telegram.Client (gotd does not allow re-running client.Run on the same
// instance) reusing the same on-disk session, so re-auth is cheap. The first cycle's
// outcome is reported on firstResult; later cycles reconnect with capped backoff.
func (m *MTProtoClient) supervise(firstResult chan error) {
	const baseBackoff = 1 * time.Second
	const maxBackoff = 60 * time.Second
	backoff := baseBackoff
	first := true

	for {
		if m.parentCtx.Err() != nil {
			return
		}

		// Fresh client + api each cycle — Run cannot be called twice on one client.
		// Logger is attached so gotd reports WHY a cycle ends (read timeout,
		// transport reset, server-side close), turning the opaque "engine forcibly
		// closed" at the upload call site into an actionable root cause.
		client := telegram.NewClient(m.apiID, m.apiHash, telegram.Options{
			SessionStorage: m.storage,
			Logger:         m.logger,
			Middlewares: []telegram.Middleware{
				FloodWaitMiddleware{},
			},
		})

		runErr := client.Run(m.parentCtx, func(runCtx context.Context) error {
			if _, err := client.Auth().Bot(runCtx, m.botToken); err != nil {
				return fmt.Errorf("MTProto bot auth failed: %w", err)
			}

			m.stateMu.Lock()
			m.client = client
			m.api = tg.NewClient(client)
			m.runCtx = runCtx
			m.ready = true
			m.stateMu.Unlock()

			fmt.Println("MTProto client authenticated successfully (2GB upload limit)")
			if first {
				first = false
				firstResult <- nil
			}
			backoff = baseBackoff // healthy cycle — reset backoff

			// Block until disconnect or deliberate Close().
			<-runCtx.Done()
			return runCtx.Err()
		})

		// Cycle ended — mark not ready and drop the torn-down api/ctx.
		m.stateMu.Lock()
		m.ready = false
		m.api = nil
		m.runCtx = nil
		m.stateMu.Unlock()

		if m.parentCtx.Err() != nil {
			return // deliberate Close() — do not reconnect
		}

		// First cycle failed before auth: report and stop (preserves startup contract).
		if first {
			first = false
			firstResult <- runErr
			return
		}

		fmt.Printf("MTProto disconnected (%v); reconnecting in %s\n", runErr, backoff)
		select {
		case <-time.After(backoff):
		case <-m.parentCtx.Done():
			return
		}
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

// Close shuts down the MTProto client and stops the supervisor.
func (m *MTProtoClient) Close() {
	if m.cancel != nil {
		m.cancel()
	}
}

// IsReady returns true if the client is authenticated and ready. During a reconnect
// window it returns false, so uploads fall back to Gofile until the connection recovers.
func (m *MTProtoClient) IsReady() bool {
	m.stateMu.RLock()
	defer m.stateMu.RUnlock()
	return m.ready
}

// resolveInputPeer converts a Bot API chat ID to an MTProto InputPeerClass.
// For channels/supergroups, it fetches the access hash via ChannelsGetChannels.
// Results are cached for reuse.
func (m *MTProtoClient) resolveInputPeer(ctx context.Context, chatID int64) (tg.InputPeerClass, error) {
	m.peerMu.Lock()
	if peer, ok := m.peers[chatID]; ok {
		m.peerMu.Unlock()
		return peer, nil
	}
	m.peerMu.Unlock()

	var peer tg.InputPeerClass

	if chatID < 0 {
		// Negative IDs are groups/channels
		// Bot API format for supergroups/channels: -100XXXXXXXXXX
		if chatID < -1000000000000 {
			channelID := -chatID - 1000000000000

			api, _, ready := m.snapshot()
			if !ready || api == nil {
				return nil, fmt.Errorf("MTProto client not ready")
			}
			// Fetch access hash from Telegram
			res, err := api.ChannelsGetChannels(ctx, []tg.InputChannelClass{
				&tg.InputChannel{ChannelID: channelID, AccessHash: 0},
			})
			if err != nil {
				return nil, fmt.Errorf("failed to resolve channel %d: %w", channelID, err)
			}

			chats := res.GetChats()
			for _, chat := range chats {
				if ch, ok := chat.(*tg.Channel); ok && ch.ID == channelID {
					peer = &tg.InputPeerChannel{
						ChannelID:  channelID,
						AccessHash: ch.AccessHash,
					}
					m.peerMu.Lock()
					m.peers[chatID] = peer
					m.peerMu.Unlock()
					return peer, nil
				}
			}
			return nil, fmt.Errorf("channel %d not found in API response", channelID)
		}
		// Regular group
		peer = &tg.InputPeerChat{
			ChatID: -chatID,
		}
	} else {
		// Positive IDs are users
		peer = &tg.InputPeerUser{
			UserID: chatID,
		}
	}

	m.peerMu.Lock()
	m.peers[chatID] = peer
	m.peerMu.Unlock()
	return peer, nil
}

// MTProtoAudioResult holds the result of an audio upload for caching.
type MTProtoAudioResult struct {
	FileID   string
	Duration int
}

// UploadAndSendAudio uploads an audio file with metadata and thumbnail via MTProto.
// It rides out reconnects: a connection teardown mid-upload is retried on a fresh
// client before the caller falls back to Gofile.
func (m *MTProtoClient) UploadAndSendAudio(
	chatID int64,
	filePath string,
	title string,
	performer string,
	durationSecs int,
	caption string,
	thumbPath string,
	replyToID int,
	status *DownloadStatus,
	ctx context.Context,
) error {
	return m.withUploadRetry(ctx, "audio upload", func(api *tg.Client) error {
		return m.uploadAndSendAudioOnce(api, chatID, filePath, title, performer, durationSecs, caption, thumbPath, replyToID, status, ctx)
	})
}

// uploadAndSendAudioOnce performs a single audio upload+send attempt against api.
func (m *MTProtoClient) uploadAndSendAudioOnce(
	api *tg.Client,
	chatID int64,
	filePath string,
	title string,
	performer string,
	durationSecs int,
	caption string,
	thumbPath string,
	replyToID int,
	status *DownloadStatus,
	ctx context.Context,
) error {
	u := uploader.NewUploader(api).WithPartSize(uploadPartSize).WithThreads(uploadThreads)
	if status != nil {
		u = u.WithProgress(&UploadProgress{status: status, phase: "Uploading"})
	}

	// Upload audio file
	if status != nil {
		status.Update("Uploading", 0, 0)
	}
	audioFile, err := u.FromPath(ctx, filePath)
	if err != nil {
		return fmt.Errorf("failed to upload audio via MTProto: %w", err)
	}

	// Upload thumbnail if available
	var thumb tg.InputFileClass
	if thumbPath != "" {
		thumbUploader := uploader.NewUploader(api).WithPartSize(uploadPartSize)
		thumbFile, err := thumbUploader.FromPath(ctx, thumbPath)
		if err != nil {
			fmt.Printf("Warning: failed to upload thumbnail: %v\n", err)
		} else {
			thumb = thumbFile
		}
	}

	// Build attributes
	attrs := []tg.DocumentAttributeClass{
		&tg.DocumentAttributeAudio{
			Title:     title,
			Performer: performer,
			Duration:  durationSecs,
		},
		&tg.DocumentAttributeFilename{
			FileName: filepath.Base(filePath),
		},
	}

	// Determine MIME type
	mimeType := mimeForAudioExt(filepath.Ext(filePath))

	// Build the media — set Thumb before SetFlags so the flag bit is included
	media := &tg.InputMediaUploadedDocument{
		File:       audioFile,
		MimeType:   mimeType,
		Attributes: attrs,
	}
	if thumb != nil {
		media.Thumb = thumb
		media.Flags.Set(2) // bit 2 = thumb flag in InputMediaUploadedDocument
	}

	// Resolve peer
	peer, err := m.resolveInputPeer(ctx, chatID)
	if err != nil {
		return fmt.Errorf("failed to resolve peer for chat %d: %w", chatID, err)
	}

	// Build request
	req := &tg.MessagesSendMediaRequest{
		Peer:     peer,
		Media:    media,
		Message:  caption,
		RandomID: cryptoRandID(),
	}
	if replyToID > 0 {
		req.ReplyTo = &tg.InputReplyToMessage{
			ReplyToMsgID: replyToID,
		}
		req.SetFlags()
	}

	// Send with FLOOD_WAIT retry
	_, err = api.MessagesSendMedia(ctx, req)
	if waited, _ := tgerr.FloodWait(ctx, err); waited {
		fmt.Println("FLOOD_WAIT for audio, retrying...")
		req.RandomID = cryptoRandID()
		_, err = api.MessagesSendMedia(ctx, req)
	}
	if err != nil {
		return fmt.Errorf("failed to send audio via MTProto: %w", err)
	}

	return nil
}

// AudioGroupItem holds the information for a single audio file in a media group.
type AudioGroupItem struct {
	FilePath     string
	Title        string
	Performer    string
	DurationSecs int
	Caption      string
	ThumbPath    string
}

// UploadAndSendAudioGroup uploads and sends a group of audio files as a media group/album via MTProto.
// A connection teardown mid-upload is retried on a fresh client before the caller falls back. Retries
// resume from the first unregistered item and re-send with the same per-item RandomIDs, so neither the
// upload nor the final send is repeated wholesale or double-posted.
func (m *MTProtoClient) UploadAndSendAudioGroup(
	chatID int64,
	items []AudioGroupItem,
	replyToID int,
	status *DownloadStatus,
	ctx context.Context,
) error {
	// registered[i] holds item i's Telegram document reference once it has been uploaded
	// and registered via MessagesUploadMedia. It persists across retry attempts so a
	// reconnect mid-group resumes from the first unregistered item instead of re-uploading
	// (and re-flooding) the whole album — re-uploading everything on every reconnect is
	// what turned a transient FLOOD_WAIT into a full Gofile cascade. Document refs are
	// account-level and survive reconnects, and the cached InputSingleMedia keeps its
	// RandomID, so a re-sent MessagesSendMultiMedia is idempotent (Telegram dedups).
	registered := make([]*tg.InputSingleMedia, len(items))
	return m.withUploadRetryN(ctx, "audio group upload", groupUploadMaxAttempts, func(api *tg.Client) error {
		return m.uploadAndSendAudioGroupOnce(api, chatID, items, registered, replyToID, status, ctx)
	})
}

// uploadAndSendAudioGroupOnce performs a single media-group upload+send attempt against api.
func (m *MTProtoClient) uploadAndSendAudioGroupOnce(
	api *tg.Client,
	chatID int64,
	items []AudioGroupItem,
	registered []*tg.InputSingleMedia,
	replyToID int,
	status *DownloadStatus,
	ctx context.Context,
) error {
	// Resolve peer
	peer, err := m.resolveInputPeer(ctx, chatID)
	if err != nil {
		return fmt.Errorf("failed to resolve peer for chat %d: %w", chatID, err)
	}

	u := uploader.NewUploader(api).WithPartSize(uploadPartSize).WithThreads(uploadThreads)

	for i, item := range items {
		if ctx != nil && ctx.Err() != nil {
			return ctx.Err()
		}

		// Resume: skip items already uploaded+registered on an earlier attempt so a
		// reconnect doesn't re-upload (and re-flood) what already succeeded.
		if registered[i] != nil {
			continue
		}

		if status != nil {
			status.Update(fmt.Sprintf("Uploading %d/%d", i+1, len(items)), 0, 0)
		}

		// Upload audio file
		audioFile, err := u.WithProgress(&UploadProgress{status: status, phase: fmt.Sprintf("Uploading %d/%d", i+1, len(items))}).FromPath(ctx, item.FilePath)
		if err != nil {
			return fmt.Errorf("failed to upload audio %d (%s) via MTProto: %w", i+1, filepath.Base(item.FilePath), err)
		}

		// Upload thumbnail if available
		var thumb tg.InputFileClass
		if item.ThumbPath != "" {
			thumbUploader := uploader.NewUploader(api).WithPartSize(uploadPartSize)
			thumbFile, err := thumbUploader.FromPath(ctx, item.ThumbPath)
			if err != nil {
				fmt.Printf("Warning: failed to upload thumbnail: %v\n", err)
			} else {
				thumb = thumbFile
			}
		}

		// Build attributes
		attrs := []tg.DocumentAttributeClass{
			&tg.DocumentAttributeAudio{
				Title:     item.Title,
				Performer: item.Performer,
				Duration:  item.DurationSecs,
			},
			&tg.DocumentAttributeFilename{
				FileName: filepath.Base(item.FilePath),
			},
		}

		// Determine MIME type
		mimeType := mimeForAudioExt(filepath.Ext(item.FilePath))

		// Build the media
		media := &tg.InputMediaUploadedDocument{
			File:       audioFile,
			MimeType:   mimeType,
			Attributes: attrs,
		}
		if thumb != nil {
			media.Thumb = thumb
			media.Flags.Set(2) // bit 2 = thumb flag
		}

		// Register the media with Telegram using MessagesUploadMedia to get a persistent document reference.
		// Using raw InputMediaUploadedDocument inside MessagesSendMultiMedia is not supported by Telegram.
		uploadedMedia, err := api.MessagesUploadMedia(ctx, &tg.MessagesUploadMediaRequest{
			Peer:  peer,
			Media: media,
		})
		if err != nil {
			return fmt.Errorf("failed to register audio %d (%s) via MessagesUploadMedia: %w", i+1, filepath.Base(item.FilePath), err)
		}

		var inputMedia tg.InputMediaClass
		if mmDoc, ok := uploadedMedia.(*tg.MessageMediaDocument); ok {
			if doc, ok := mmDoc.Document.(*tg.Document); ok {
				inputMedia = &tg.InputMediaDocument{
					ID: doc.AsInput(),
				}
			}
		}

		if inputMedia == nil {
			return fmt.Errorf("failed to extract document for item %d (%s) from UploadMedia response", i+1, filepath.Base(item.FilePath))
		}

		registered[i] = &tg.InputSingleMedia{
			Media:    inputMedia,
			RandomID: cryptoRandID(),
			Message:  item.Caption,
		}
	}

	// Assemble the album in original order from the per-item cache, now fully populated
	// (across however many attempts it took to register every item).
	multiMedia := make([]tg.InputSingleMedia, len(items))
	for i := range registered {
		if registered[i] == nil {
			return fmt.Errorf("internal: group item %d not registered before send", i+1)
		}
		multiMedia[i] = *registered[i]
	}

	// Build request
	req := &tg.MessagesSendMultiMediaRequest{
		Peer:       peer,
		MultiMedia: multiMedia,
	}
	if replyToID > 0 {
		req.ReplyTo = &tg.InputReplyToMessage{
			ReplyToMsgID: replyToID,
		}
		req.SetFlags()
	}

	// Send request (retries on FLOOD_WAIT are handled by middleware)
	_, err = api.MessagesSendMultiMedia(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to send multi-media group via MTProto: %w", err)
	}

	return nil
}

// UploadAndSendDocument uploads a file as a document (e.g., ZIP) via MTProto.
// A connection teardown mid-upload is retried on a fresh client before the caller falls back.
func (m *MTProtoClient) UploadAndSendDocument(
	chatID int64,
	filePath string,
	displayName string,
	caption string,
	replyToID int,
	status *DownloadStatus,
	ctx context.Context,
) error {
	return m.withUploadRetry(ctx, "document upload", func(api *tg.Client) error {
		return m.uploadAndSendDocumentOnce(api, chatID, filePath, displayName, caption, replyToID, status, ctx)
	})
}

// uploadAndSendDocumentOnce performs a single document upload+send attempt against api.
func (m *MTProtoClient) uploadAndSendDocumentOnce(
	api *tg.Client,
	chatID int64,
	filePath string,
	displayName string,
	caption string,
	replyToID int,
	status *DownloadStatus,
	ctx context.Context,
) error {
	u := uploader.NewUploader(api).WithPartSize(uploadPartSize).WithThreads(uploadThreads)
	if status != nil {
		u = u.WithProgress(&UploadProgress{status: status, phase: "Uploading"})
	}

	if status != nil {
		status.Update("Uploading", 0, 0)
	}
	docFile, err := u.FromPath(ctx, filePath)
	if err != nil {
		return fmt.Errorf("failed to upload document via MTProto: %w", err)
	}

	media := &tg.InputMediaUploadedDocument{
		File:     docFile,
		MimeType: mimeForDocExt(filepath.Ext(filePath)),
		Attributes: []tg.DocumentAttributeClass{
			&tg.DocumentAttributeFilename{
				FileName: displayName,
			},
		},
	}

	peer, err := m.resolveInputPeer(ctx, chatID)
	if err != nil {
		return fmt.Errorf("failed to resolve peer for chat %d: %w", chatID, err)
	}

	req := &tg.MessagesSendMediaRequest{
		Peer:     peer,
		Media:    media,
		Message:  caption,
		RandomID: cryptoRandID(),
	}
	if replyToID > 0 {
		req.ReplyTo = &tg.InputReplyToMessage{
			ReplyToMsgID: replyToID,
		}
		req.SetFlags()
	}

	_, err = api.MessagesSendMedia(ctx, req)
	if waited, _ := tgerr.FloodWait(ctx, err); waited {
		fmt.Println("FLOOD_WAIT for document, retrying...")
		req.RandomID = cryptoRandID()
		_, err = api.MessagesSendMedia(ctx, req)
	}
	if err != nil {
		return fmt.Errorf("failed to send document via MTProto: %w", err)
	}

	return nil
}

// UploadAndSendVideo uploads a video file as a streamable Telegram video (inline player +
// thumbnail) via MTProto. A connection teardown mid-upload is retried on a fresh client
// before the caller falls back to a document/Gofile.
func (m *MTProtoClient) UploadAndSendVideo(
	chatID int64,
	filePath string,
	caption string,
	durationSecs int,
	width int,
	height int,
	thumbPath string,
	replyToID int,
	status *DownloadStatus,
	ctx context.Context,
) error {
	return m.withUploadRetry(ctx, "video upload", func(api *tg.Client) error {
		return m.uploadAndSendVideoOnce(api, chatID, filePath, caption, durationSecs, width, height, thumbPath, replyToID, status, ctx)
	})
}

// uploadAndSendVideoOnce performs a single video upload+send attempt against api.
func (m *MTProtoClient) uploadAndSendVideoOnce(
	api *tg.Client,
	chatID int64,
	filePath string,
	caption string,
	durationSecs int,
	width int,
	height int,
	thumbPath string,
	replyToID int,
	status *DownloadStatus,
	ctx context.Context,
) error {
	u := uploader.NewUploader(api).WithPartSize(uploadPartSize).WithThreads(uploadThreads)
	if status != nil {
		u = u.WithProgress(&UploadProgress{status: status, phase: "Uploading"})
	}

	if status != nil {
		status.Update("Uploading", 0, 0)
	}
	videoFile, err := u.FromPath(ctx, filePath)
	if err != nil {
		return fmt.Errorf("failed to upload video via MTProto: %w", err)
	}

	// Upload thumbnail if available
	var thumb tg.InputFileClass
	if thumbPath != "" {
		thumbUploader := uploader.NewUploader(api).WithPartSize(uploadPartSize)
		thumbFile, terr := thumbUploader.FromPath(ctx, thumbPath)
		if terr != nil {
			fmt.Printf("Warning: failed to upload video thumbnail: %v\n", terr)
		} else {
			thumb = thumbFile
		}
	}

	attrs := []tg.DocumentAttributeClass{
		&tg.DocumentAttributeVideo{
			SupportsStreaming: true,
			Duration:          float64(durationSecs),
			W:                 width,
			H:                 height,
		},
		&tg.DocumentAttributeFilename{
			FileName: filepath.Base(filePath),
		},
	}

	media := &tg.InputMediaUploadedDocument{
		File:       videoFile,
		MimeType:   "video/mp4",
		Attributes: attrs,
	}
	if thumb != nil {
		media.Thumb = thumb
		media.Flags.Set(2) // bit 2 = thumb flag in InputMediaUploadedDocument
	}

	peer, err := m.resolveInputPeer(ctx, chatID)
	if err != nil {
		return fmt.Errorf("failed to resolve peer for chat %d: %w", chatID, err)
	}

	req := &tg.MessagesSendMediaRequest{
		Peer:     peer,
		Media:    media,
		Message:  caption,
		RandomID: cryptoRandID(),
	}
	if replyToID > 0 {
		req.ReplyTo = &tg.InputReplyToMessage{
			ReplyToMsgID: replyToID,
		}
		req.SetFlags()
	}

	_, err = api.MessagesSendMedia(ctx, req)
	if waited, _ := tgerr.FloodWait(ctx, err); waited {
		fmt.Println("FLOOD_WAIT for video, retrying...")
		req.RandomID = cryptoRandID()
		_, err = api.MessagesSendMedia(ctx, req)
	}
	if err != nil {
		return fmt.Errorf("failed to send video via MTProto: %w", err)
	}

	return nil
}

// mimeForAudioExt returns the MIME type for common audio file extensions.
func mimeForAudioExt(ext string) string {
	switch strings.ToLower(ext) {
	case ".m4a", ".mp4":
		return "audio/mp4"
	case ".flac":
		return "audio/flac"
	case ".mp3":
		return "audio/mpeg"
	case ".opus":
		return "audio/ogg"
	case ".wav":
		return "audio/wav"
	default:
		return "audio/mp4"
	}
}

// mimeForDocExt returns the MIME type for document file extensions.
func mimeForDocExt(ext string) string {
	switch strings.ToLower(ext) {
	case ".zip":
		return "application/zip"
	default:
		return "application/octet-stream"
	}
}

// FileSessionStorage implements telegram.SessionStorage using a JSON file.
type FileSessionStorage struct {
	Path string
	mu   sync.Mutex
	data []byte
}

// LoadSession reads the session from disk.
func (s *FileSessionStorage) LoadSession(_ context.Context) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.data != nil {
		return s.data, nil
	}
	data, err := os.ReadFile(s.Path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	s.data = data
	return data, nil
}

// StoreSession writes the session to disk.
func (s *FileSessionStorage) StoreSession(_ context.Context, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data = data
	dir := filepath.Dir(s.Path)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return err
		}
	}
	return os.WriteFile(s.Path, data, 0o600)
}