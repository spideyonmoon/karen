package main

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/uploader"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
)

func cryptoRandID() int64 {
	var b [8]byte
	_, _ = cryptorand.Read(b[:])
	return int64(binary.LittleEndian.Uint64(b[:]))
}

// MTProtoClient wraps the gotd/td client for direct Telegram uploads.
type MTProtoClient struct {
	client  *telegram.Client
	api     *tg.Client
	ctx     context.Context
	cancel  context.CancelFunc
	ready   chan struct{}
	running bool
	runMu   sync.RWMutex

	peerMu sync.Mutex
	peers  map[int64]tg.InputPeerClass
}

// FloodWaitMiddleware automatically catches FLOOD_WAIT errors, sleeps for the required duration, and retries the RPC call.
type FloodWaitMiddleware struct{}

func (f FloodWaitMiddleware) Handle(next tg.Invoker) telegram.InvokeFunc {
	return func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
		for {
			err := next.Invoke(ctx, input, output)
			if err == nil {
				return nil
			}
			if waited, _ := tgerr.FloodWait(ctx, err); waited {
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
// It blocks until authentication is complete.
func NewMTProtoClient(apiID int, apiHash string, botToken string, sessionDir string) (*MTProtoClient, error) {
	if apiID == 0 || apiHash == "" {
		return nil, fmt.Errorf("telegram-api-id and telegram-api-hash must be set for MTProto uploads")
	}

	ctx, cancel := context.WithCancel(context.Background())

	sessionPath := filepath.Join(sessionDir, "mtproto-session.json")

	client := telegram.NewClient(apiID, apiHash, telegram.Options{
		SessionStorage: &FileSessionStorage{Path: sessionPath},
		Middlewares: []telegram.Middleware{
			FloodWaitMiddleware{},
		},
	})

	m := &MTProtoClient{
		client:  client,
		ctx:     ctx,
		cancel:  cancel,
		ready:   make(chan struct{}),
		peers:   make(map[int64]tg.InputPeerClass),
		running: true,
	}

	errCh := make(chan error, 1)

	go func() {
		err := client.Run(ctx, func(ctx context.Context) error {
			// Authenticate as bot
			if _, err := client.Auth().Bot(ctx, botToken); err != nil {
				return fmt.Errorf("MTProto bot auth failed: %w", err)
			}
			m.api = tg.NewClient(client)
			close(m.ready)
			fmt.Println("MTProto client authenticated successfully (2GB upload limit)")

			// Block until context is cancelled (keeps the client alive)
			<-ctx.Done()
			return ctx.Err()
		})

		m.runMu.Lock()
		m.running = false
		m.runMu.Unlock()

		if err != nil && ctx.Err() == nil {
			errCh <- err
		}
	}()

	// Wait for auth to complete or fail
	select {
	case <-m.ready:
		return m, nil
	case err := <-errCh:
		cancel()
		return nil, err
	}
}

// Close shuts down the MTProto client.
func (m *MTProtoClient) Close() {
	if m.cancel != nil {
		m.cancel()
	}
}

// IsReady returns true if the client is authenticated and ready.
func (m *MTProtoClient) IsReady() bool {
	m.runMu.RLock()
	defer m.runMu.RUnlock()
	if !m.running {
		return false
	}
	select {
	case <-m.ready:
		return true
	default:
		return false
	}
}

// resolveInputPeer converts a Bot API chat ID to an MTProto InputPeerClass.
// For channels/supergroups, it fetches the access hash via ChannelsGetChannels.
// Results are cached for reuse.
func (m *MTProtoClient) resolveInputPeer(chatID int64) (tg.InputPeerClass, error) {
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

			// Fetch access hash from Telegram
			res, err := m.api.ChannelsGetChannels(m.ctx, []tg.InputChannelClass{
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
	if !m.IsReady() {
		return fmt.Errorf("MTProto client not ready")
	}

	u := uploader.NewUploader(m.api).WithPartSize(512 * 1024)
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
		thumbUploader := uploader.NewUploader(m.api).WithPartSize(512 * 1024)
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
	peer, err := m.resolveInputPeer(chatID)
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
	_, err = m.api.MessagesSendMedia(ctx, req)
	if waited, _ := tgerr.FloodWait(ctx, err); waited {
		fmt.Println("FLOOD_WAIT for audio, retrying...")
		req.RandomID = cryptoRandID()
		_, err = m.api.MessagesSendMedia(ctx, req)
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
func (m *MTProtoClient) UploadAndSendAudioGroup(
	chatID int64,
	items []AudioGroupItem,
	replyToID int,
	status *DownloadStatus,
	ctx context.Context,
) error {
	if !m.IsReady() {
		return fmt.Errorf("MTProto client not ready")
	}

	u := uploader.NewUploader(m.api).WithPartSize(512 * 1024)

	var multiMedia []tg.InputSingleMedia

	for i, item := range items {
		if ctx != nil && ctx.Err() != nil {
			return ctx.Err()
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
			thumbUploader := uploader.NewUploader(m.api).WithPartSize(512 * 1024)
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
		uploadedMedia, err := m.api.MessagesUploadMedia(ctx, &tg.MessagesUploadMediaRequest{
			Peer:  &tg.InputPeerEmpty{},
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

		multiMedia = append(multiMedia, tg.InputSingleMedia{
			Media:    inputMedia,
			RandomID: cryptoRandID(),
			Message:  item.Caption,
		})
	}

	// Resolve peer
	peer, err := m.resolveInputPeer(chatID)
	if err != nil {
		return fmt.Errorf("failed to resolve peer for chat %d: %w", chatID, err)
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
	_, err = m.api.MessagesSendMultiMedia(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to send multi-media group via MTProto: %w", err)
	}

	return nil
}

// UploadAndSendDocument uploads a file as a document (e.g., ZIP) via MTProto.
func (m *MTProtoClient) UploadAndSendDocument(
	chatID int64,
	filePath string,
	displayName string,
	caption string,
	replyToID int,
	status *DownloadStatus,
	ctx context.Context,
) error {
	if !m.IsReady() {
		return fmt.Errorf("MTProto client not ready")
	}

	u := uploader.NewUploader(m.api).WithPartSize(512 * 1024)
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

	peer, err := m.resolveInputPeer(chatID)
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

	_, err = m.api.MessagesSendMedia(ctx, req)
	if waited, _ := tgerr.FloodWait(ctx, err); waited {
		fmt.Println("FLOOD_WAIT for document, retrying...")
		req.RandomID = cryptoRandID()
		_, err = m.api.MessagesSendMedia(ctx, req)
	}
	if err != nil {
		return fmt.Errorf("failed to send document via MTProto: %w", err)
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