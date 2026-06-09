package main

import (
	"archive/zip"
	"bytes"
	"context"
	cryptorand "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	apputils "main/utils"
	"main/utils/ampapi"
	"main/utils/structs"
)

const (
	defaultSearchLimit           = 8
	defaultQueueSize             = 20
	pendingTTL                   = 10 * time.Minute
	defaultTelegramFormat        = "alac"
	defaultTelegramDownloadMaxGB = 3
	defaultTelegramTimeoutSecs   = 3600
)

const (
	telegramFormatAlac   = "alac"
	telegramFormatFlac   = "flac"
	transferModeOneByOne = "one"  // deprecated alias
	transferModeZip      = "zip"  // deprecated alias

	transferModeTelegramIndividual = "tg_individual"
	transferModeTelegramZip        = "tg_zip"
	transferModeGofileZip          = "gofile_zip"
	transferModeMv                 = "mv"        // music video → native Telegram video
	transferModeMvGofile           = "mv_gofile" // music video → Gofile direct (no zip)
	transferModeArt                = "art"       // artwork (cover + motion) → Telegram photo/video
	transferModeCancel             = "cancel"
)

var telegramBotTokenPattern = regexp.MustCompile(`bot[0-9]+:[A-Za-z0-9_-]+`)

type TelegramBot struct {
	token        string
	apiBase      string
	appleToken   string
	client       *http.Client
	allowedChats map[int64]bool
	cacheChatID  int64
	searchLimit  int
	maxFileBytes int64
	mtproto      *MTProtoClient

	formatMu    sync.Mutex
	chatFormats map[int64]string

	pendingMu sync.Mutex
	pending   map[int64]*PendingSelection

	transferMu       sync.Mutex
	pendingTransfers map[int64]*PendingTransfer

	queueMu       sync.Mutex
	downloadQueue chan *downloadRequest
	queuedReqs    []*downloadRequest // display-only mirror of downloadQueue for status board (guarded by queueMu)
	inProgress    bool
	userTaskCount map[int64]int
	activeReq     *downloadRequest
	activeStatus  *DownloadStatus // live status board for the running task; nil when idle
	// idleStatus* track the single board shown by /status when nothing is running,
	// so each /status replaces the previous one instead of stacking messages.
	idleStatusMsgID  int
	idleStatusChatID int64

	cacheMu   sync.Mutex
	cacheFile string
	cache     map[string]CachedAudio
	docCache  map[string]CachedDocument
}

type PendingSelection struct {
	Kind             string
	Query            string
	Title            string
	Offset           int
	HasNext          bool
	Items            []apputils.SearchResultItem
	CreatedAt        time.Time
	ReplyToMessageID int
	ResultsMessageID int
}

type PendingTransfer struct {
	AlbumID          string
	SongID           string
	PlaylistID       string
	StationID        string
	MvID             string
	MvStorefront     string
	Single           bool
	ForceAAC         bool
	ForceAtmos       bool
	ForceFlac        bool
	ReplyToMessageID int
	MessageID        int
	CreatedAt        time.Time
}

type downloadRequest struct {
	taskID          string
	chatID          int64
	userID          int64
	username        string
	replyToID       int
	single          bool
	format          string
	transferMode    string
	albumID         string
	forceAtmos      bool
	fn              func(ctx context.Context) error
	after           func()
	ctx             context.Context
	cancel          context.CancelFunc
	statusMessageID int
	startedAt       time.Time
}

func generateTaskID() string {
	b := make([]byte, 4)
	cryptorand.Read(b)
	return hex.EncodeToString(b)
}

type Update struct {
	UpdateID           int                 `json:"update_id"`
	Message            *Message            `json:"message,omitempty"`
	CallbackQuery      *CallbackQuery      `json:"callback_query,omitempty"`
	InlineQuery        *InlineQuery        `json:"inline_query,omitempty"`
	ChosenInlineResult *ChosenInlineResult `json:"chosen_inline_result,omitempty"`
}

type Message struct {
	MessageID int    `json:"message_id"`
	From      *User  `json:"from,omitempty"`
	Chat      Chat   `json:"chat"`
	Text      string `json:"text,omitempty"`
}

type CallbackQuery struct {
	ID              string   `json:"id"`
	From            *User    `json:"from,omitempty"`
	Message         *Message `json:"message,omitempty"`
	InlineMessageID string   `json:"inline_message_id,omitempty"`
	Data            string   `json:"data,omitempty"`
}

type InlineQuery struct {
	ID    string `json:"id"`
	From  *User  `json:"from,omitempty"`
	Query string `json:"query"`
}

type ChosenInlineResult struct {
	ResultID        string `json:"result_id"`
	From            *User  `json:"from,omitempty"`
	Query           string `json:"query,omitempty"`
	InlineMessageID string `json:"inline_message_id,omitempty"`
}

type User struct {
	ID       int64  `json:"id"`
	Username string `json:"username,omitempty"`
}

type Chat struct {
	ID   int64  `json:"id"`
	Type string `json:"type"`
}

type InlineKeyboardMarkup struct {
	InlineKeyboard [][]InlineKeyboardButton `json:"inline_keyboard"`
}

type InlineKeyboardButton struct {
	Text                         string  `json:"text"`
	CallbackData                 string  `json:"callback_data,omitempty"`
	SwitchInlineQuery            *string `json:"switch_inline_query,omitempty"`
	SwitchInlineQueryCurrentChat *string `json:"switch_inline_query_current_chat,omitempty"`
	Url                          string  `json:"url,omitempty"`
}

type ReplyKeyboardMarkup struct {
	Keyboard        [][]KeyboardButton `json:"keyboard"`
	ResizeKeyboard  bool               `json:"resize_keyboard,omitempty"`
	OneTimeKeyboard bool               `json:"one_time_keyboard,omitempty"`
}

type ReplyKeyboardRemove struct {
	RemoveKeyboard bool `json:"remove_keyboard"`
}

type KeyboardButton struct {
	Text string `json:"text"`
}

type getUpdatesResponse struct {
	OK          bool     `json:"ok"`
	Result      []Update `json:"result"`
	Description string   `json:"description,omitempty"`
}

type apiResponse struct {
	OK          bool   `json:"ok"`
	Description string `json:"description,omitempty"`
}

type sendMessageResponse struct {
	OK          bool    `json:"ok"`
	Result      Message `json:"result"`
	Description string  `json:"description,omitempty"`
}

type sendAudioResponse struct {
	OK          bool         `json:"ok"`
	Result      AudioMessage `json:"result"`
	Description string       `json:"description,omitempty"`
}

type sendDocumentResponse struct {
	OK          bool            `json:"ok"`
	Result      DocumentMessage `json:"result"`
	Description string          `json:"description,omitempty"`
}

type AudioMessage struct {
	MessageID int   `json:"message_id"`
	Audio     Audio `json:"audio"`
}

type DocumentMessage struct {
	MessageID int      `json:"message_id"`
	Document  Document `json:"document"`
}

type Audio struct {
	FileID       string `json:"file_id"`
	FileUniqueID string `json:"file_unique_id,omitempty"`
	FileSize     int64  `json:"file_size,omitempty"`
}

type Document struct {
	FileID       string `json:"file_id"`
	FileUniqueID string `json:"file_unique_id,omitempty"`
	FileSize     int64  `json:"file_size,omitempty"`
	FileName     string `json:"file_name,omitempty"`
}

type CachedAudio struct {
	FileID         string    `json:"file_id"`
	FileSize       int64     `json:"file_size"`
	Compressed     bool      `json:"compressed"`
	Format         string    `json:"format,omitempty"`
	SizeBytes      int64     `json:"size_bytes,omitempty"`
	BitrateKbps    float64   `json:"bitrate_kbps,omitempty"`
	DurationMillis int64     `json:"duration_millis,omitempty"`
	Title          string    `json:"title,omitempty"`
	Performer      string    `json:"performer,omitempty"`
	UpdatedAt      time.Time `json:"updated_at,omitempty"`
}

type CachedDocument struct {
	FileID    string    `json:"file_id"`
	FileSize  int64     `json:"file_size,omitempty"`
	UpdatedAt time.Time `json:"updated_at,omitempty"`
}

type telegramCacheFile struct {
	Version   int                       `json:"version"`
	Items     map[string]CachedAudio    `json:"items"`
	Documents map[string]CachedDocument `json:"documents,omitempty"`
}

type InlineQueryResultCachedAudio struct {
	Type        string `json:"type"`
	ID          string `json:"id"`
	AudioFileID string `json:"audio_file_id"`
	Caption     string `json:"caption,omitempty"`
}

type InlineQueryResultArticle struct {
	Type                string                `json:"type"`
	ID                  string                `json:"id"`
	Title               string                `json:"title"`
	Description         string                `json:"description,omitempty"`
	ThumbnailURL        string                `json:"thumbnail_url,omitempty"`
	ReplyMarkup         *InlineKeyboardMarkup `json:"reply_markup,omitempty"`
	InputMessageContent InputMessageContent   `json:"input_message_content"`
}

type InputMessageContent struct {
	MessageText string `json:"message_text"`
}

type InputMediaAudio struct {
	Type      string `json:"type"`
	Media     string `json:"media"`
	Caption   string `json:"caption,omitempty"`
	Title     string `json:"title,omitempty"`
	Performer string `json:"performer,omitempty"`
}

func runTelegramBot(appleToken string) {
	botToken := strings.TrimSpace(Config.TelegramBotToken)
	if botToken == "" {
		botToken = strings.TrimSpace(os.Getenv("TELEGRAM_BOT_TOKEN"))
	}
	if botToken == "" {
		fmt.Println("telegram-bot-token is not set. Add it to config.yaml or TELEGRAM_BOT_TOKEN.")
		return
	}
	if Config.TelegramDownloadFolder != "" {
		Config.AlacSaveFolder = Config.TelegramDownloadFolder
	}

	// Initialize MTProto client for direct Telegram uploads (>50MB, up to 2GB)
	var mtprotoClient *MTProtoClient
	if Config.TelegramApiID != 0 && Config.TelegramApiHash != "" {
		var err error
		mtprotoClient, err = NewMTProtoClient(Config.TelegramApiID, Config.TelegramApiHash, botToken, ".")
		if err != nil {
			fmt.Printf("Warning: MTProto init failed: %v\nFalling back to Gofile-only mode.\n", err)
		}
	} else {
		fmt.Println("MTProto not configured — set telegram-api-id and telegram-api-hash for direct Telegram uploads (up to 2GB).")
	}

	bot := newTelegramBot(botToken, appleToken)
	bot.mtproto = mtprotoClient
	fmt.Println("Telegram bot started. Waiting for updates...")
	bot.loop()
}

func normalizeTelegramAPIBase(raw string) string {
	base := strings.TrimSpace(raw)
	if base == "" {
		return "https://api.telegram.org"
	}
	return strings.TrimRight(base, "/")
}

func telegramDownloadMaxBytes() int64 {
	gb := Config.TelegramDownloadMaxGB
	if gb <= 0 {
		gb = defaultTelegramDownloadMaxGB
	}
	return int64(gb) * 1024 * 1024 * 1024
}

func telegramRequestTimeout() time.Duration {
	seconds := Config.TelegramRequestTimeoutSeconds
	if seconds <= 0 {
		seconds = defaultTelegramTimeoutSecs
	}
	return time.Duration(seconds) * time.Second
}

func (b *TelegramBot) sanitizeTelegramError(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s", b.sanitizeTelegramText(err.Error()))
}

func (b *TelegramBot) sanitizeTelegramText(text string) string {
	text = telegramBotTokenPattern.ReplaceAllString(text, "bot<redacted>")
	if b != nil && b.token != "" {
		text = strings.ReplaceAll(text, b.token, "<redacted>")
	}
	return text
}

func newTelegramBot(token, appleToken string) *TelegramBot {
	allowed := make(map[int64]bool)
	for _, id := range Config.TelegramAllowedChatIDs {
		allowed[id] = true
	}
	searchLimit := Config.TelegramSearchLimit
	if searchLimit <= 0 {
		searchLimit = defaultSearchLimit
	}
	maxFileBytes := int64(Config.TelegramMaxFileMB) * 1024 * 1024
	if maxFileBytes <= 0 {
		maxFileBytes = 50 * 1024 * 1024
	}
	cacheFile := strings.TrimSpace(Config.TelegramCacheFile)
	if cacheFile == "" {
		cacheFile = "telegram-cache.json"
	}
	queueSize := defaultQueueSize
	if queueSize <= 0 {
		queueSize = 1
	}
	apiBase := normalizeTelegramAPIBase(Config.TelegramAPIURL)
	bot := &TelegramBot{
		token:            token,
		appleToken:       appleToken,
		apiBase:          apiBase,
		client:           &http.Client{Timeout: time.Duration(Config.TelegramRequestTimeoutSeconds) * time.Second},
		allowedChats:     allowed,
		cacheChatID:      Config.TelegramCacheChatID,
		searchLimit:      searchLimit,
		maxFileBytes:     maxFileBytes,
		chatFormats:      make(map[int64]string),
		pending:          make(map[int64]*PendingSelection),
		pendingTransfers: make(map[int64]*PendingTransfer),
		downloadQueue:    make(chan *downloadRequest, defaultQueueSize),
		userTaskCount:    make(map[int64]int),
		cacheFile:        Config.TelegramCacheFile,
		cache:            make(map[string]CachedAudio),
		docCache:         make(map[string]CachedDocument),
	}
	bot.loadCache()
	bot.startDownloadWorker()
	return bot
}

func (b *TelegramBot) loop() {
	offset := 0
	for {
		updates, err := b.getUpdates(offset)
		if err != nil {
			fmt.Println("getUpdates error:", err)
			time.Sleep(2 * time.Second)
			continue
		}
		for _, upd := range updates {
			if upd.UpdateID >= offset {
				offset = upd.UpdateID + 1
			}
			if upd.Message != nil {
				b.handleMessage(upd.Message)
			} else if upd.CallbackQuery != nil {
				b.handleCallback(upd.CallbackQuery)
			} else if upd.InlineQuery != nil {
				b.handleInlineQuery(upd.InlineQuery)
			} else if upd.ChosenInlineResult != nil {
				b.handleChosenInlineResult(upd.ChosenInlineResult)
			}
		}
	}
}

func (b *TelegramBot) startDownloadWorker() {
	go func() {
		for req := range b.downloadQueue {
			b.queueMu.Lock()
			b.inProgress = true
			b.activeReq = req
			req.startedAt = time.Now()
			b.removeQueuedReqLocked(req.taskID)
			b.queueMu.Unlock()

			b.runDownload(req)
			if req.after != nil {
				req.after()
			}

			b.queueMu.Lock()
			b.inProgress = false
			b.activeReq = nil
			if req.userID != 0 {
				b.userTaskCount[req.userID]--
			}
			b.queueMu.Unlock()
		}
	}()
}

func normalizeTelegramFormat(format string) string {
	switch strings.ToLower(strings.TrimSpace(format)) {
	case telegramFormatAlac:
		return telegramFormatAlac
	case telegramFormatFlac:
		return telegramFormatFlac
	default:
		return ""
	}
}

func (b *TelegramBot) getChatFormat(chatID int64) string {
	b.formatMu.Lock()
	defer b.formatMu.Unlock()
	if b.chatFormats == nil {
		b.chatFormats = make(map[int64]string)
	}
	if format, ok := b.chatFormats[chatID]; ok {
		if normalized := normalizeTelegramFormat(format); normalized != "" {
			return normalized
		}
	}
	return defaultTelegramFormat
}

func (b *TelegramBot) setChatFormat(chatID int64, format string) string {
	normalized := normalizeTelegramFormat(format)
	if normalized == "" {
		return ""
	}
	b.formatMu.Lock()
	defer b.formatMu.Unlock()
	if b.chatFormats == nil {
		b.chatFormats = make(map[int64]string)
	}
	b.chatFormats[chatID] = normalized
	return normalized
}

func (b *TelegramBot) cacheKey(trackID, format string, compressed bool) string {
	normalized := normalizeTelegramFormat(format)
	if normalized == "" {
		normalized = telegramFormatFlac
	}
	return fmt.Sprintf("%s|%s|%t", trackID, normalized, compressed)
}

func (b *TelegramBot) albumZipCacheKey(albumID, format string) string {
	normalized := normalizeTelegramFormat(format)
	if normalized == "" {
		normalized = defaultTelegramFormat
	}
	return fmt.Sprintf("album:%s|%s|zip", albumID, normalized)
}

func (b *TelegramBot) loadCache() {
	b.cacheMu.Lock()
	defer b.cacheMu.Unlock()
	b.cache = make(map[string]CachedAudio)
	b.docCache = make(map[string]CachedDocument)
	if b.cacheFile == "" {
		return
	}
	data, err := os.ReadFile(b.cacheFile)
	if err != nil {
		return
	}
	var payload telegramCacheFile
	if err := json.Unmarshal(data, &payload); err != nil {
		return
	}
	if payload.Documents != nil {
		b.docCache = payload.Documents
	}
	if payload.Items == nil {
		if payload.Version > 0 && payload.Version < 3 {
			b.saveCacheLocked()
		}
		return
	}
	if payload.Version < 2 {
		migrated := make(map[string]CachedAudio)
		for key, entry := range payload.Items {
			parts := strings.Split(key, "|")
			if len(parts) == 2 {
				trackID := parts[0]
				compressed, err := strconv.ParseBool(parts[1])
				if err != nil {
					continue
				}
				entry.Compressed = compressed
				if entry.Format == "" {
					entry.Format = telegramFormatFlac
				}
				migrated[b.cacheKey(trackID, entry.Format, entry.Compressed)] = entry
				continue
			}
			if len(parts) >= 3 {
				trackID := parts[0]
				format := normalizeTelegramFormat(parts[1])
				compressed, err := strconv.ParseBool(parts[2])
				if err != nil {
					continue
				}
				if format == "" {
					format = telegramFormatFlac
				}
				entry.Compressed = compressed
				if entry.Format == "" {
					entry.Format = format
				}
				migrated[b.cacheKey(trackID, format, entry.Compressed)] = entry
			}
		}
		b.cache = migrated
		b.saveCacheLocked()
		return
	}
	b.cache = payload.Items
	for key, entry := range b.cache {
		if entry.Format == "" {
			parts := strings.Split(key, "|")
			if len(parts) >= 2 {
				entry.Format = normalizeTelegramFormat(parts[1])
			}
			if entry.Format == "" {
				entry.Format = telegramFormatFlac
			}
			b.cache[key] = entry
		}
	}
	if payload.Version < 3 {
		b.saveCacheLocked()
	}
}

func (b *TelegramBot) saveCacheLocked() {
	if b.cacheFile == "" {
		return
	}
	dir := filepath.Dir(b.cacheFile)
	if dir != "." && dir != "" {
		_ = os.MkdirAll(dir, 0755)
	}
	payload := telegramCacheFile{
		Version:   3,
		Items:     b.cache,
		Documents: b.docCache,
	}
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return
	}
	tmp := b.cacheFile + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return
	}
	_ = os.Rename(tmp, b.cacheFile)
}

func (b *TelegramBot) fetchTrackMeta(trackID string) (AudioMeta, error) {
	if trackID == "" {
		return AudioMeta{}, fmt.Errorf("empty track id")
	}
	resp, err := ampapi.GetSongResp(Config.Storefront, trackID, b.searchLanguage(), b.appleToken)
	if err != nil || resp == nil || len(resp.Data) == 0 {
		if err != nil {
			return AudioMeta{}, err
		}
		return AudioMeta{}, fmt.Errorf("empty song response")
	}
	data := resp.Data[0]
	return AudioMeta{
		TrackID:        trackID,
		Title:          strings.TrimSpace(data.Attributes.Name),
		Performer:      strings.TrimSpace(data.Attributes.ArtistName),
		DurationMillis: int64(data.Attributes.DurationInMillis),
	}, nil
}

func (b *TelegramBot) enrichCachedAudio(trackID string, entry CachedAudio) CachedAudio {
	updated := false
	sizeBytes := entry.SizeBytes
	if sizeBytes <= 0 {
		sizeBytes = entry.FileSize
		if sizeBytes > 0 {
			entry.SizeBytes = sizeBytes
			updated = true
		}
	}
	if trackID != "" && (entry.DurationMillis <= 0 || entry.Title == "" || entry.Performer == "") {
		if meta, err := b.fetchTrackMeta(trackID); err == nil {
			if entry.DurationMillis <= 0 && meta.DurationMillis > 0 {
				entry.DurationMillis = meta.DurationMillis
				updated = true
			}
			if entry.Title == "" && meta.Title != "" {
				entry.Title = meta.Title
				updated = true
			}
			if entry.Performer == "" && meta.Performer != "" {
				entry.Performer = meta.Performer
				updated = true
			}
		}
	}
	if entry.BitrateKbps <= 0 && sizeBytes > 0 && entry.DurationMillis > 0 {
		entry.BitrateKbps = calcBitrateKbps(sizeBytes, entry.DurationMillis)
		updated = true
	}
	if updated && trackID != "" {
		b.storeCachedAudio(trackID, entry)
	}
	return entry
}

func (b *TelegramBot) storeCachedAudio(trackID string, entry CachedAudio) {
	if trackID == "" || entry.FileID == "" {
		return
	}
	b.cacheMu.Lock()
	defer b.cacheMu.Unlock()
	if b.cache == nil {
		b.cache = make(map[string]CachedAudio)
	}
	entry.Format = normalizeTelegramFormat(entry.Format)
	if entry.Format == "" {
		entry.Format = telegramFormatFlac
	}
	entry.UpdatedAt = time.Now()
	b.cache[b.cacheKey(trackID, entry.Format, entry.Compressed)] = entry
	b.saveCacheLocked()
}

func (b *TelegramBot) deleteCachedAudio(trackID, format string, compressed bool) {
	if trackID == "" {
		return
	}
	b.cacheMu.Lock()
	defer b.cacheMu.Unlock()
	if b.cache == nil {
		return
	}
	delete(b.cache, b.cacheKey(trackID, format, compressed))
	b.saveCacheLocked()
}

func (b *TelegramBot) storeCachedDocument(key string, entry CachedDocument) {
	if key == "" || entry.FileID == "" {
		return
	}
	b.cacheMu.Lock()
	defer b.cacheMu.Unlock()
	if b.docCache == nil {
		b.docCache = make(map[string]CachedDocument)
	}
	entry.UpdatedAt = time.Now()
	b.docCache[key] = entry
	b.saveCacheLocked()
}

func (b *TelegramBot) getCachedDocument(key string) (CachedDocument, bool) {
	if key == "" {
		return CachedDocument{}, false
	}
	b.cacheMu.Lock()
	defer b.cacheMu.Unlock()
	if b.docCache == nil {
		return CachedDocument{}, false
	}
	entry, ok := b.docCache[key]
	return entry, ok
}

func (b *TelegramBot) deleteCachedDocument(key string) {
	if key == "" {
		return
	}
	b.cacheMu.Lock()
	defer b.cacheMu.Unlock()
	if b.docCache == nil {
		return
	}
	delete(b.docCache, key)
	b.saveCacheLocked()
}

func (b *TelegramBot) getCachedAudio(trackID string, maxBytes int64, format string) (CachedAudio, bool) {
	if trackID == "" {
		return CachedAudio{}, false
	}
	b.cacheMu.Lock()
	defer b.cacheMu.Unlock()
	if b.cache == nil {
		return CachedAudio{}, false
	}
	var candidates []CachedAudio
	normalized := normalizeTelegramFormat(format)
	if normalized != "" {
		if entry, ok := b.cache[b.cacheKey(trackID, normalized, false)]; ok {
			if entry.Format == "" {
				entry.Format = normalized
			}
			candidates = append(candidates, entry)
		}
		if entry, ok := b.cache[b.cacheKey(trackID, normalized, true)]; ok {
			if entry.Format == "" {
				entry.Format = normalized
			}
			candidates = append(candidates, entry)
		}
	} else {
		prefix := trackID + "|"
		for key, entry := range b.cache {
			if !strings.HasPrefix(key, prefix) {
				continue
			}
			if entry.Format == "" {
				parts := strings.Split(key, "|")
				if len(parts) >= 3 {
					entry.Format = normalizeTelegramFormat(parts[1])
				}
				if entry.Format == "" {
					entry.Format = telegramFormatFlac
				}
			}
			candidates = append(candidates, entry)
		}
	}
	var best *CachedAudio
	for _, entry := range candidates {
		entrySize := entry.SizeBytes
		if entrySize <= 0 {
			entrySize = entry.FileSize
		}
		if maxBytes > 0 && entrySize > 0 && entrySize > maxBytes {
			continue
		}
		if best == nil {
			copyEntry := entry
			best = &copyEntry
			continue
		}
		if best.Compressed && !entry.Compressed {
			copyEntry := entry
			best = &copyEntry
			continue
		}
		bestSize := best.SizeBytes
		if bestSize <= 0 {
			bestSize = best.FileSize
		}
		if best.Compressed == entry.Compressed && entrySize > bestSize {
			copyEntry := entry
			best = &copyEntry
		}
	}
	if best == nil {
		return CachedAudio{}, false
	}
	return *best, true
}

func (b *TelegramBot) handleMessage(msg *Message) {
	if msg.Text == "" {
		return
	}
	if msg.Chat.Type == "private" {
		_ = b.sendMessage(msg.Chat.ID, "This bot only operates in specific groups.", nil)
		return
	}
	if !b.isAllowedChat(msg.Chat.ID) {
		return // Silently ignore non-allowed groups to avoid spamming
	}
	text := strings.TrimSpace(msg.Text)
	if cmd, args, ok := parseCommand(text); ok {
		userID := int64(0)
		if msg.From != nil {
			userID = msg.From.ID
		}
		b.handleCommand(msg.Chat.ID, userID, cmd, args, msg.MessageID)
		return
	}
}

func (b *TelegramBot) handleCallback(cb *CallbackQuery) {
	if cb == nil || cb.Message == nil {
		return
	}
	if !b.isAllowedChat(cb.Message.Chat.ID) {
		return
	}
	data := strings.TrimSpace(cb.Data)
	if strings.HasPrefix(data, "sel:") {
		numStr := strings.TrimPrefix(data, "sel:")
		if n, err := strconv.Atoi(numStr); err == nil {
			b.handleSelection(cb.Message.Chat.ID, cb.Message.MessageID, n)
		}
	} else if strings.HasPrefix(data, "setting:") {
		format := strings.TrimPrefix(data, "setting:")
		if normalized := b.setChatFormat(cb.Message.Chat.ID, format); normalized != "" {
			text := fmt.Sprintf("Download format set to %s.", strings.ToUpper(normalized))
			_ = b.editMessageText(cb.Message.Chat.ID, cb.Message.MessageID, text, buildSettingsKeyboard(normalized))
		}
	} else if strings.HasPrefix(data, "album_transfer:") {
		mode := strings.TrimPrefix(data, "album_transfer:")
		username := ""
		if cb.From != nil {
			username = cb.From.Username
		}
		b.handleTransferMode(cb.Message.Chat.ID, cb.Message.MessageID, mode, username, cb.From.ID)
	} else if strings.HasPrefix(data, "transfer:") {
		mode := strings.TrimPrefix(data, "transfer:")
		username := ""
		if cb.From != nil {
			username = cb.From.Username
		}
		b.handleTransferMode(cb.Message.Chat.ID, cb.Message.MessageID, mode, username, cb.From.ID)
	} else if strings.HasPrefix(data, "page:") {
		deltaStr := strings.TrimPrefix(data, "page:")
		if delta, err := strconv.Atoi(deltaStr); err == nil {
			b.handlePage(cb.Message.Chat.ID, cb.Message.MessageID, delta)
		}
	}
	_ = b.answerCallbackQuery(cb.ID)
}

func (b *TelegramBot) handleInlineQuery(q *InlineQuery) {
	if q == nil || q.ID == "" {
		return
	}
	query := strings.TrimSpace(q.Query)
	if query == "" {
		_ = b.answerInlineQuery(q.ID, []any{}, true)
		return
	}
	b.answerInlineSearch(q.ID, "song", normalizeInlineSongSearchTerm(query))
}

func (b *TelegramBot) answerInlineSearch(inlineQueryID string, kind string, term string) {
	items, _, err := b.fetchSearchPage(kind, term, 0)
	if err != nil || len(items) == 0 {
		_ = b.answerInlineQuery(inlineQueryID, []any{}, true)
		return
	}
	results := make([]any, 0, len(items))
	for i, item := range items {
		if kind == "song" && item.ID == "" {
			continue
		}
		if kind == "song" {
			if cached, ok := b.inlineCachedAudioResult(item, i); ok {
				results = append(results, cached)
				continue
			}
		}
		messageText := inlineSearchMessageText(kind, item)
		if messageText == "" {
			continue
		}
		results = append(results, InlineQueryResultArticle{
			Type:         "article",
			ID:           inlineSearchResultID(kind, item.ID, i),
			Title:        inlineSearchTitle(item),
			Description:  item.Detail,
			ThumbnailURL: apputils.SearchArtworkURL(item.ArtworkURL, 160),
			ReplyMarkup:  inlineSearchReplyMarkup(item),
			InputMessageContent: InputMessageContent{
				MessageText: inlinePendingMessageText(kind, item, messageText),
			},
		})
	}
	_ = b.answerInlineQuery(inlineQueryID, results, true)
}

func (b *TelegramBot) inlineCachedAudioResult(item apputils.SearchResultItem, index int) (InlineQueryResultCachedAudio, bool) {
	entry, ok := b.getCachedAudio(item.ID, b.maxFileBytes, "")
	if !ok {
		return InlineQueryResultCachedAudio{}, false
	}
	entry = b.enrichCachedAudio(item.ID, entry)
	format := normalizeTelegramFormat(entry.Format)
	if format == "" {
		format = telegramFormatAlac
	}
	resultID := fmt.Sprintf("cached:%s:%d", item.ID, index)
	return InlineQueryResultCachedAudio{
		Type:        "audio",
		ID:          resultID,
		AudioFileID: entry.FileID,
		Caption:     formatTelegramCaption(entry.SizeBytes, entry.BitrateKbps, format),
	}, true
}

func (b *TelegramBot) handleChosenInlineResult(result *ChosenInlineResult) {
	if result == nil || result.From == nil {
		return
	}
	songID := songIDFromInlineResultID(result.ResultID)
	if songID == "" {
		return
	}
	chatID := result.From.ID
	if !b.isAllowedChat(chatID) {
		return
	}
	b.queueInlineSongDownload(chatID, songID, result.InlineMessageID)
}

func (b *TelegramBot) handleCommand(chatID int64, userID int64, cmd string, args []string, replyToID int) {
	if strings.HasPrefix(cmd, "stop_") {
		taskID := strings.TrimPrefix(cmd, "stop_")
		b.cancelTask(chatID, taskID, replyToID)
		return
	}

	switch cmd {
	case "start", "help":
		_ = b.sendMessage(chatID, "Send /dl <apple-music-link> to download a song or album.\nSend /status to view the download queue.", nil)
	case "status", "queue":
		b.queueMu.Lock()
		active := b.activeStatus
		inProgress := b.inProgress
		b.queueMu.Unlock()
		switch {
		case inProgress && active != nil && active.chatID == chatID:
			// Single board: drop the old message and resurface it at the bottom.
			active.Relocate(replyToID)
		case inProgress && active != nil:
			// Active task lives in another chat — give this chat a plain snapshot
			// without disturbing the live board.
			_ = b.sendMessageWithReply(chatID, active.RenderSnapshot(), nil, replyToID)
		default:
			// Idle: keep exactly one board, replacing any previous /status message.
			b.replaceIdleStatusBoard(chatID, replyToID, "📊 Karen Status Board\n\nNo active tasks."+b.queueBoardSuffix())
		}
	case "dl":
		b.queueMu.Lock()
		count := b.userTaskCount[userID]
		b.queueMu.Unlock()
		if count >= 3 {
			_ = b.sendMessageWithReply(chatID, "You have reached the maximum number of pending tasks (3). Please wait for them to finish.", nil, replyToID)
			return
		}
		if len(args) == 0 {
			_ = b.sendMessageWithReply(chatID, "Usage: /dl <apple-music-link> [-aac|-atmos] [-flac] [-art]", nil, replyToID)
			return
		}

		forceAAC := false
		forceAtmos := false
		forceFlac := false
		forceArt := false
		var link string
		for _, arg := range args {
			switch arg {
			case "-aac", "aac":
				forceAAC = true
			case "-atmos", "atmos":
				forceAtmos = true
			case "-flac", "flac":
				forceFlac = true
			case "-art", "art":
				forceArt = true
			default:
				link = arg
			}
		}

		// -art short-circuits everything else: grab only the cover + motion artwork
		// for an album/playlist/station, ignoring any codec flags (there's no audio).
		if forceArt {
			b.queueDownloadArtwork(chatID, link, replyToID, userID)
			return
		}

		// Music video URLs are distinct from song/album/playlist; check first.
		mvStorefront, mvID := checkUrlMv(link)
		if mvID != "" {
			b.queueDownloadMvWithReply(chatID, mvStorefront, mvID, replyToID, userID)
			return
		}

		_, songID := checkUrlSong(link)
		if songID != "" {
			b.queueDownloadSongWithReply(chatID, songID, replyToID, forceAAC, forceAtmos, forceFlac)
			return
		}

		_, albumID := checkUrl(link)
		if albumID != "" {
			b.queueDownloadAlbumWithReply(chatID, albumID, replyToID, forceAAC, forceAtmos, forceFlac)
			return
		}

		_, playlistID := checkUrlPlaylist(link)
		if playlistID != "" {
			b.queueDownloadPlaylistWithReply(chatID, playlistID, replyToID, forceAAC, forceAtmos, forceFlac)
			return
		}

		_, stationID := checkUrlStation(link)
		if stationID != "" {
			b.queueDownloadStationWithReply(chatID, stationID, replyToID, forceAAC, forceAtmos, forceFlac)
			return
		}

		_, artistID := checkUrlArtist(link)
		if artistID != "" {
			_ = b.sendMessageWithReply(chatID, "Downloading artist discographies is not allowed.", nil, replyToID)
			return
		}
		
		_ = b.sendMessageWithReply(chatID, "Invalid Apple Music link.", nil, replyToID)
	default:
		// Silently ignore unknown commands
	}
}

func (b *TelegramBot) handleSearch(chatID int64, kind string, query string, replyToID int) {
	query = strings.TrimSpace(query)
	if query == "" {
		_ = b.sendMessageWithReply(chatID, "Please provide a search query.", nil, replyToID)
		return
	}
	kind = strings.ToLower(kind)
	if kind != "song" && kind != "album" && kind != "artist" {
		_ = b.sendMessageWithReply(chatID, "Search type must be song, album, or artist.", nil, replyToID)
		return
	}
	offset := 0
	items, hasNext, err := b.fetchSearchPage(kind, query, offset)
	if err != nil {
		_ = b.sendMessageWithReply(chatID, fmt.Sprintf("Search failed: %v", err), nil, replyToID)
		return
	}
	if len(items) == 0 {
		_ = b.sendMessageWithReply(chatID, "No results found.", nil, replyToID)
		return
	}
	message := apputils.FormatSearchResults(kind, query, items)
	messageID, err := b.sendMessageWithReplyReturn(chatID, message, buildInlineKeyboard(len(items), offset > 0, hasNext), replyToID)
	if err != nil {
		return
	}
	b.setPending(chatID, kind, query, offset, items, hasNext, replyToID, messageID, "")
}

func (b *TelegramBot) searchLanguage() string {
	lang := strings.TrimSpace(Config.TelegramSearchLanguage)
	if lang == "" {
		lang = strings.TrimSpace(Config.Language)
	}
	return lang
}

func (b *TelegramBot) fetchSearchPage(kind string, query string, offset int) ([]apputils.SearchResultItem, bool, error) {
	apiType := kind + "s"
	resp, err := ampapi.Search(Config.Storefront, query, apiType, b.searchLanguage(), b.appleToken, b.searchLimit, offset)
	if err != nil {
		return nil, false, err
	}
	items, hasNext := apputils.BuildSearchItems(kind, resp)
	return items, hasNext, nil
}

func (b *TelegramBot) handleSelection(chatID int64, messageID int, choice int) {
	pending, ok := b.getPending(chatID)
	if !ok {
		_ = b.sendMessage(chatID, "No active selection. Start with /search_song or /search_album.", nil)
		return
	}
	if pending.ResultsMessageID != 0 && messageID != pending.ResultsMessageID {
		return
	}
	replyToID := pending.ReplyToMessageID
	if time.Since(pending.CreatedAt) > pendingTTL {
		b.clearPending(chatID)
		_ = b.sendMessageWithReply(chatID, "Selection expired. Please search again.", nil, replyToID)
		return
	}
	if choice < 1 || choice > len(pending.Items) {
		_ = b.sendMessageWithReply(chatID, "Selection out of range.", nil, replyToID)
		return
	}
	selected := pending.Items[choice-1]
	switch pending.Kind {
	case "song":
		setSearchMeta(selected.ID, selected.Name, selected.Artist)
		b.queueDownloadSongWithReply(chatID, selected.ID, replyToID, false, false, false)
	case "album", "artist_album":
		b.queueDownloadAlbumWithReply(chatID, selected.ID, replyToID, false, false, false)
	case "artist":
		b.showArtistAlbums(chatID, selected.ID, selected.Name, replyToID)
	default:
		b.clearPending(chatID)
	}
}

func (b *TelegramBot) showArtistAlbums(chatID int64, artistID string, artistName string, replyToID int) {
	artistName = strings.TrimSpace(artistName)
	if artistName == "" {
		artistName = artistID
	}
	albums, hasNext, err := apputils.FetchArtistAlbums(Config.Storefront, artistID, b.appleToken, b.searchLimit, 0, b.searchLanguage())
	if err != nil {
		_ = b.sendMessageWithReply(chatID, fmt.Sprintf("Failed to load artist albums: %v", err), nil, replyToID)
		return
	}
	if len(albums) == 0 {
		_ = b.sendMessageWithReply(chatID, "No albums found for this artist.", nil, replyToID)
		return
	}
	message := apputils.FormatArtistAlbums(artistName, albums)
	messageID, err := b.sendMessageWithReplyReturn(chatID, message, buildInlineKeyboard(len(albums), false, hasNext), replyToID)
	if err != nil {
		return
	}
	b.setPending(chatID, "artist_album", artistID, 0, albums, hasNext, replyToID, messageID, artistName)
}

func (b *TelegramBot) handleTransferMode(chatID int64, messageID int, mode string, username string, userID int64) {
	pending, ok := b.getPendingTransfer(chatID)
	if !ok {
		return
	}
	if pending.MessageID != 0 && messageID != pending.MessageID {
		return
	}
	if time.Since(pending.CreatedAt) > pendingTTL {
		b.clearPendingTransfer(chatID)
		_ = b.editMessageText(chatID, messageID, "Selection expired. Please request again.", nil)
		return
	}
	replyToID := pending.ReplyToMessageID
	b.clearPendingTransfer(chatID)

	switch mode {
	case transferModeCancel:
		_ = b.editMessageText(chatID, messageID, "Download cancelled.", nil)
		return
	case transferModeOneByOne:
		mode = transferModeTelegramIndividual
	case transferModeZip:
		mode = transferModeGofileZip
	}

	b.queueMu.Lock()
	inProgress := b.inProgress
	queueLen := len(b.downloadQueue)
	b.queueMu.Unlock()

	var statusText string
	if inProgress || queueLen > 0 {
		statusText = fmt.Sprintf("Queued. Position: %d", queueLen+1)
	} else {
		statusText = "Starting download..."
	}
	_ = b.editMessageText(chatID, messageID, statusText, nil)

	if pending.MvID != "" {
		mvMode := transferModeMv
		if mode == transferModeGofileZip {
			mvMode = transferModeMvGofile
		}
		b.enqueueMvDownload(chatID, userID, pending.MvStorefront, pending.MvID, replyToID, messageID, mvMode)
		return
	}

	if pending.Single && pending.SongID != "" {
		b.enqueueDownload(chatID, userID, username, replyToID, messageID, true, b.resolveFormat(chatID, pending.ForceFlac), mode, "", func(ctx context.Context) error {
			return ripSong(pending.SongID, b.appleToken, Config.Storefront, Config.MediaUserToken, pending.ForceAAC, ctx)
		})
	} else if pending.PlaylistID != "" {
		b.enqueuePlaylistDownload(chatID, pending.PlaylistID, replyToID, messageID, mode, pending.ForceAAC, pending.ForceAtmos, pending.ForceFlac, userID, username)
	} else if pending.StationID != "" {
		b.enqueueStationDownload(chatID, pending.StationID, replyToID, messageID, mode, pending.ForceAAC, pending.ForceAtmos, pending.ForceFlac, userID, username)
	} else if pending.AlbumID != "" {
		format := b.resolveFormat(chatID, pending.ForceFlac)
		if mode == transferModeGofileZip {
			if b.trySendCachedAlbumZip(chatID, pending.AlbumID, replyToID, format) {
				return
			}
		}
		b.enqueueAlbumDownload(chatID, pending.AlbumID, replyToID, messageID, mode, pending.ForceAAC, pending.ForceAtmos, pending.ForceFlac, userID, username)
	}
}

func (b *TelegramBot) handlePage(chatID int64, messageID int, delta int) {
	pending, ok := b.getPending(chatID)
	if !ok {
		return
	}
	if pending.ResultsMessageID != messageID {
		return
	}
	if pending.Query == "" {
		return
	}
	newOffset := pending.Offset + delta*b.searchLimit
	if newOffset < 0 {
		return
	}
	var (
		items   []apputils.SearchResultItem
		hasNext bool
		err     error
		message string
	)
	switch pending.Kind {
	case "song", "album", "artist":
		items, hasNext, err = b.fetchSearchPage(pending.Kind, pending.Query, newOffset)
		if err != nil {
			_ = b.editMessageText(chatID, messageID, fmt.Sprintf("Search failed: %v", err), nil)
			return
		}
		if len(items) == 0 {
			return
		}
		message = apputils.FormatSearchResults(pending.Kind, pending.Query, items)
	case "artist_album":
		items, hasNext, err = apputils.FetchArtistAlbums(Config.Storefront, pending.Query, b.appleToken, b.searchLimit, newOffset, b.searchLanguage())
		if err != nil {
			_ = b.editMessageText(chatID, messageID, fmt.Sprintf("Failed to load artist albums: %v", err), nil)
			return
		}
		if len(items) == 0 {
			return
		}
		message = apputils.FormatArtistAlbums(pending.Title, items)
	default:
		return
	}
	_ = b.editMessageText(chatID, messageID, message, buildInlineKeyboard(len(items), newOffset > 0, hasNext))
	b.setPending(chatID, pending.Kind, pending.Query, newOffset, items, hasNext, pending.ReplyToMessageID, messageID, pending.Title)
}

// resolveFormat returns the delivery format for a download: a one-off -flac flag
// overrides the persistent per-chat setting without mutating it.
func (b *TelegramBot) resolveFormat(chatID int64, forceFlac bool) string {
	if forceFlac {
		return telegramFormatFlac
	}
	return b.getChatFormat(chatID)
}

func (b *TelegramBot) queueDownloadSong(chatID int64, songID string) {
	b.queueDownloadSongWithReply(chatID, songID, 0, false, false, false)
}

func (b *TelegramBot) queueDownloadSongWithReply(chatID int64, songID string, replyToID int, forceAAC bool, forceAtmos bool, forceFlac bool) {
	if songID == "" {
		_ = b.sendMessage(chatID, "Song ID is empty.", nil)
		return
	}
	format := b.resolveFormat(chatID, forceFlac)
	if b.trySendCachedTrack(chatID, replyToID, songID, format) {
		return
	}
	b.promptTransferMode(chatID, "", songID, "", "", replyToID, true, forceAAC, forceAtmos, forceFlac)
}

func (b *TelegramBot) queueInlineSongDownload(chatID int64, songID string, inlineMessageID string) {
	if songID == "" {
		_ = b.sendMessage(chatID, "Song ID is empty.", nil)
		return
	}
	format := b.getChatFormat(chatID)
	if inlineMessageID != "" && b.tryEditInlineCachedTrack(inlineMessageID, songID, format) {
		return
	}
	uploadChatID := chatID
	if inlineMessageID != "" {
		if b.cacheChatID == 0 {
			_ = b.editInlineMessageText(inlineMessageID, "Preparing audio failed: telegram-cache-chat-id is not set.")
			return
		}
		uploadChatID = b.cacheChatID
	}
	after := func() {
		if inlineMessageID == "" {
			return
		}
		if b.tryEditInlineCachedTrack(inlineMessageID, songID, format) {
			return
		}
		_ = b.editInlineMessageText(inlineMessageID, "Download failed. Please check bot logs or cache chat permissions.")
	}
	ok := b.enqueueDownloadWithAfter(uploadChatID, 0, "", 0, 0, true, format, transferModeOneByOne, "", func(ctx context.Context) error {
		return ripSong(songID, b.appleToken, Config.Storefront, Config.MediaUserToken, false, ctx)
	}, after)
	if !ok && inlineMessageID != "" {
		_ = b.editInlineMessageText(inlineMessageID, "Download queue is full. Please try again later.")
	}
}

func (b *TelegramBot) queueDownloadAlbum(chatID int64, albumID string) {
	b.queueDownloadAlbumWithReply(chatID, albumID, 0, false, false, false)
}

func (b *TelegramBot) queueDownloadAlbumWithReply(chatID int64, albumID string, replyToID int, forceAAC bool, forceAtmos bool, forceFlac bool) {
	if albumID == "" {
		_ = b.sendMessage(chatID, "Album ID is empty.", nil)
		return
	}
	b.promptTransferMode(chatID, albumID, "", "", "", replyToID, false, forceAAC, forceAtmos, forceFlac)
}

func (b *TelegramBot) queueDownloadPlaylistWithReply(chatID int64, playlistID string, replyToID int, forceAAC bool, forceAtmos bool, forceFlac bool) {
	if playlistID == "" {
		_ = b.sendMessage(chatID, "Playlist ID is empty.", nil)
		return
	}
	b.promptTransferMode(chatID, "", "", playlistID, "", replyToID, false, forceAAC, forceAtmos, forceFlac)
}

func (b *TelegramBot) queueDownloadStationWithReply(chatID int64, stationID string, replyToID int, forceAAC bool, forceAtmos bool, forceFlac bool) {
	if stationID == "" {
		_ = b.sendMessage(chatID, "Station ID is empty.", nil)
		return
	}
	b.promptTransferMode(chatID, "", "", "", stationID, replyToID, false, forceAAC, forceAtmos, forceFlac)
}

// queueDownloadMvWithReply validates the preconditions for a music-video rip
// (media-user-token + mp4decrypt) and then prompts for the delivery target. The
// userID param is unused here — the chosen mode arrives via the inline button
// callback, which carries its own user — but is kept for call-site symmetry.
func (b *TelegramBot) queueDownloadMvWithReply(chatID int64, storefront string, mvID string, replyToID int, userID int64) {
	if mvID == "" {
		_ = b.sendMessage(chatID, "Music video ID is empty.", nil)
		return
	}
	if len(Config.MediaUserToken) <= 50 {
		_ = b.sendMessageWithReply(chatID, "Music videos require a valid media-user-token in config.yaml.", nil, replyToID)
		return
	}
	if _, err := exec.LookPath("mp4decrypt"); err != nil {
		_ = b.sendMessageWithReply(chatID, "Music video download is unavailable: mp4decrypt is not installed.", nil, replyToID)
		return
	}
	if storefront == "" {
		storefront = Config.Storefront
	}
	// Like every other /dl command, ask where to deliver. A single video only has two
	// sensible targets (native Telegram video vs Gofile), so the prompt omits ZIP.
	b.promptMvTransferMode(chatID, storefront, mvID, replyToID)
}

// promptMvTransferMode shows the music-video delivery keyboard and stores the pending
// MV selection; handleTransferMode picks it up on the button press.
func (b *TelegramBot) promptMvTransferMode(chatID int64, storefront string, mvID string, replyToID int) {
	messageID, err := b.sendMessageWithReplyReturn(chatID, "Choose transfer method:", buildMvTransferKeyboard(), replyToID)
	if err != nil {
		return
	}
	b.transferMu.Lock()
	b.pendingTransfers[chatID] = &PendingTransfer{
		MvID:             mvID,
		MvStorefront:     storefront,
		Single:           true,
		ReplyToMessageID: replyToID,
		MessageID:        messageID,
		CreatedAt:        time.Now(),
	}
	b.transferMu.Unlock()
}

// enqueueMvDownload queues a music-video rip with the chosen delivery mode
// (transferModeMv for native video, transferModeMvGofile for a direct Gofile link).
func (b *TelegramBot) enqueueMvDownload(chatID int64, userID int64, storefront string, mvID string, replyToID int, statusMessageID int, transferMode string) {
	if storefront == "" {
		storefront = Config.Storefront
	}
	saveDir := Config.AlacSaveFolder
	if saveDir == "" {
		saveDir = "."
	}
	b.enqueueDownload(chatID, userID, "", replyToID, statusMessageID, true, "", transferMode, "", func(ctx context.Context) error {
		return mvDownloader(ctx, mvID, saveDir, b.appleToken, storefront, Config.MediaUserToken, nil)
	})
}

func (b *TelegramBot) promptTransferMode(chatID int64, albumID string, songID string, playlistID string, stationID string, replyToID int, single bool, forceAAC bool, forceAtmos bool, forceFlac bool) {
	mtprotoReady := b.mtproto != nil && b.mtproto.IsReady()
	messageID, err := b.sendMessageWithReplyReturn(chatID, "Choose transfer method:", buildTransferKeyboard(mtprotoReady), replyToID)
	if err != nil {
		return
	}
	b.transferMu.Lock()
	b.pendingTransfers[chatID] = &PendingTransfer{
		AlbumID:          albumID,
		SongID:           songID,
		PlaylistID:       playlistID,
		StationID:        stationID,
		Single:           single,
		ForceAAC:         forceAAC,
		ForceAtmos:       forceAtmos,
		ForceFlac:        forceFlac,
		ReplyToMessageID: replyToID,
		MessageID:        messageID,
		CreatedAt:        time.Now(),
	}
	b.transferMu.Unlock()
}

func (b *TelegramBot) enqueueAlbumDownload(chatID int64, albumID string, replyToID int, statusMessageID int, transferMode string, forceAAC bool, forceAtmos bool, forceFlac bool, userID int64, username string) {
	if albumID == "" {
		_ = b.sendMessage(chatID, "Album ID is empty.", nil)
		return
	}
	format := b.resolveFormat(chatID, forceFlac)
	b.enqueueDownload(chatID, userID, username, replyToID, statusMessageID, false, format, transferMode, albumID, func(ctx context.Context) error {
		if forceAtmos {
			dl_atmos = true
		}
		return ripAlbum(albumID, b.appleToken, Config.Storefront, Config.MediaUserToken, "", forceAAC, ctx)
	})
}

func (b *TelegramBot) enqueuePlaylistDownload(chatID int64, playlistID string, replyToID int, statusMessageID int, transferMode string, forceAAC bool, forceAtmos bool, forceFlac bool, userID int64, username string) {
	if playlistID == "" {
		_ = b.sendMessage(chatID, "Playlist ID is empty.", nil)
		return
	}
	format := b.resolveFormat(chatID, forceFlac)
	b.enqueueDownload(chatID, userID, username, replyToID, statusMessageID, false, format, transferMode, playlistID, func(ctx context.Context) error {
		if forceAtmos {
			dl_atmos = true
		}
		return ripPlaylist(playlistID, b.appleToken, Config.Storefront, Config.MediaUserToken, forceAAC, ctx)
	})
}

// enqueueStationDownload queues an Apple Music radio/station rip. ripStation reads
// the codec from the dl_atmos/dl_aac globals (it takes no forceAAC param), so we set
// them inside the closure — runDownload resets both before fn runs.
func (b *TelegramBot) enqueueStationDownload(chatID int64, stationID string, replyToID int, statusMessageID int, transferMode string, forceAAC bool, forceAtmos bool, forceFlac bool, userID int64, username string) {
	if stationID == "" {
		_ = b.sendMessage(chatID, "Station ID is empty.", nil)
		return
	}
	format := b.resolveFormat(chatID, forceFlac)
	b.enqueueDownload(chatID, userID, username, replyToID, statusMessageID, false, format, transferMode, "", func(ctx context.Context) error {
		if forceAtmos {
			dl_atmos = true
		}
		if forceAAC {
			dl_aac = true
		}
		return ripStation(stationID, b.appleToken, Config.Storefront, Config.MediaUserToken, ctx)
	})
}

// queueDownloadArtwork enqueues an artwork-only job (cover + motion artwork, no
// tracks) for an album, playlist, or station link. There is no codec or delivery
// prompt — the files are small and go straight to Telegram as a photo/video.
func (b *TelegramBot) queueDownloadArtwork(chatID int64, link string, replyToID int, userID int64) {
	_, albumID := checkUrl(link)
	_, playlistID := checkUrlPlaylist(link)
	_, stationID := checkUrlStation(link)
	if albumID == "" && playlistID == "" && stationID == "" {
		_ = b.sendMessageWithReply(chatID, "Artwork extraction supports album, playlist, and station links.", nil, replyToID)
		return
	}
	b.enqueueDownload(chatID, userID, "", replyToID, 0, true, "", transferModeArt, "", func(ctx context.Context) error {
		return ripArtwork(link, b.appleToken, Config.Storefront, Config.MediaUserToken, ctx)
	})
}

func (b *TelegramBot) enqueueDownload(chatID int64, userID int64, username string, replyToID int, statusMessageID int, single bool, format string, transferMode string, albumID string, fn func(ctx context.Context) error) {
	_ = b.enqueueDownloadWithAfter(chatID, userID, username, replyToID, statusMessageID, single, format, transferMode, albumID, fn, nil)
}

func (b *TelegramBot) enqueueDownloadWithAfter(chatID int64, userID int64, username string, replyToID int, statusMessageID int, single bool, format string, transferMode string, albumID string, fn func(ctx context.Context) error, after func()) bool {
	// Accept all valid transfer modes
	taskID := generateTaskID()
	ctx, cancelFn := context.WithCancel(context.Background())
	req := &downloadRequest{
		taskID:          taskID,
		chatID:          chatID,
		userID:          userID,
		username:        username,
		replyToID:       replyToID,
		single:          single,
		format:          format,
		transferMode:    transferMode,
		albumID:         albumID,
		fn:              fn,
		after:           after,
		ctx:             ctx,
		cancel:          cancelFn,
		statusMessageID: statusMessageID,
	}
	b.queueMu.Lock()
	inProgress := b.inProgress
	queueLen := len(b.downloadQueue)
	queueCap := cap(b.downloadQueue)
	position := queueLen + 1
	if inProgress {
		position++
	}
	if queueLen >= queueCap {
		b.queueMu.Unlock()
		_ = b.sendMessageWithReply(chatID, "Download queue is full. Please try again later.", nil, replyToID)
		return false
	}
	// Non-blocking send + mirror append happen atomically under queueMu so the
	// display queue can never drift from the channel.
	select {
	case b.downloadQueue <- req:
		b.queuedReqs = append(b.queuedReqs, req)
	default:
		b.queueMu.Unlock()
		_ = b.sendMessageWithReply(chatID, "Download queue is full. Please try again later.", nil, replyToID)
		return false
	}
	if userID != 0 {
		b.userTaskCount[userID]++
	}
	active := b.activeStatus
	b.queueMu.Unlock()

	// A new task was added. Instead of a separate "Queued" message, refresh the
	// single live board (its queue section now lists this task) and resurface it.
	// If nothing is running, the worker will create the board momentarily.
	if active != nil && active.chatID == chatID {
		active.Relocate(replyToID)
	} else if inProgress || queueLen > 0 {
		_ = b.sendMessageWithReply(chatID, fmt.Sprintf("Queued (ID: %s). Position: %d\nStop: /stop_%s", taskID, position, taskID), nil, replyToID)
	}
	return true
}

func (b *TelegramBot) trySendCachedTrack(chatID int64, replyToID int, trackID string, format string) bool {
	entry, ok := b.getCachedAudio(trackID, b.maxFileBytes, format)
	if !ok {
		return false
	}
	if err := b.sendAudioByFileID(chatID, entry, replyToID, trackID); err != nil {
		b.deleteCachedAudio(trackID, entry.Format, entry.Compressed)
		return false
	}
	return true
}

func (b *TelegramBot) tryEditInlineCachedTrack(inlineMessageID string, trackID string, format string) bool {
	if inlineMessageID == "" {
		return false
	}
	entry, ok := b.getCachedAudio(trackID, b.maxFileBytes, format)
	if !ok {
		return false
	}
	if err := b.editInlineMessageAudio(inlineMessageID, entry, trackID); err != nil {
		fmt.Println("edit inline audio failed:", err)
		return false
	}
	return true
}

func (b *TelegramBot) trySendCachedAlbumZip(chatID int64, albumID string, replyToID int, format string) bool {
	if albumID == "" {
		return false
	}
	key := b.albumZipCacheKey(albumID, format)
	entry, ok := b.getCachedDocument(key)
	if !ok {
		return false
	}
	if err := b.sendDocumentByFileID(chatID, entry, replyToID); err != nil {
		b.deleteCachedDocument(key)
		return false
	}
	return true
}

func (b *TelegramBot) runDownload(req *downloadRequest) {
	chatID := req.chatID
	fn := req.fn
	single := req.single
	replyToID := req.replyToID
	format := req.format
	transferMode := req.transferMode
	albumID := req.albumID

	if req.ctx != nil && req.ctx.Err() != nil {
		return
	}

	lastDownloadedPaths = nil
	downloadedMetaMu.Lock()
	downloadedMeta = make(map[string]AudioMeta)
	downloadedMetaMu.Unlock()
	resetDownloadFailures()
	counter = structs.Counter{}
	okDict = make(map[string][]int)

	dl_atmos = false
	dl_aac = false
	dl_select = false
	if single {
		dl_song = true
	} else {
		dl_song = false
	}

	format = normalizeTelegramFormat(format)
	if format == "" {
		format = defaultTelegramFormat
	}
	defer b.cleanupDownloadsIfNeeded()
	Config.ConvertAfterDownload = format == telegramFormatFlac
	if format == telegramFormatFlac {
		Config.ConvertFormat = telegramFormatFlac
		Config.ConvertKeepOriginal = false
		Config.ConvertSkipLossyToLossless = false
		if _, err := exec.LookPath(Config.FFmpegPath); err != nil {
			_ = b.sendMessageWithReply(chatID, fmt.Sprintf("ffmpeg not found at '%s'.", Config.FFmpegPath), nil, replyToID)
			dl_song = false
			return
		}
	} else {
		Config.ConvertFormat = ""
	}

	status, err := newDownloadStatus(b, chatID, replyToID, req.statusMessageID, req)
	if err != nil {
		_ = b.sendMessageWithReply(chatID, fmt.Sprintf("Failed to create status message: %v", err), nil, replyToID)
		dl_song = false
		return
	}
	defer status.Stop()

	// Expose the live status board so /status can render the same rich view, and
	// retire any idle board left by a previous /status so only one board remains.
	b.queueMu.Lock()
	b.activeStatus = status
	idleID := b.idleStatusMsgID
	idleChat := b.idleStatusChatID
	b.idleStatusMsgID = 0
	b.queueMu.Unlock()
	if idleID != 0 {
		_ = b.deleteMessage(idleChat, idleID)
	}
	defer func() {
		b.queueMu.Lock()
		b.activeStatus = nil
		b.queueMu.Unlock()
	}()

	progress := func(phase string, done, total int64) {
		status.Update(phase, done, total)
	}
	activeProgress = progress
	defer func() { activeProgress = nil }()

	status.Update("Downloading", 0, 0)
	err = fn(req.ctx)
	if err != nil {
		status.UpdateSync(fmt.Sprintf("Failed: %v", err), 0, 0)
		dl_song = false
		return
	}
	dl_song = false

	activeProgress = nil

	paths := append([]string{}, lastDownloadedPaths...)
	if len(paths) == 0 {
		if summary := downloadFailureSummary(); summary != "" {
			status.UpdateSync("No files were downloaded: "+summary, 0, 0)
			return
		}
		if counter.Error > 0 || counter.Unavailable > 0 {
			status.UpdateSync(fmt.Sprintf("No files were downloaded. Errors: %d, unavailable: %d.", counter.Error, counter.Unavailable), 0, 0)
			return
		}
		status.UpdateSync("No files were downloaded.", 0, 0)
		return
	}

	switch transferMode {
	case transferModeMv:
		b.deliverMusicVideo(chatID, paths[0], replyToID, status, req.ctx)
	case transferModeMvGofile:
		b.deliverMvGofile(chatID, paths[0], replyToID, status, req.ctx)
	case transferModeArt:
		b.deliverArtwork(chatID, paths, replyToID, status, req.ctx)
	case transferModeTelegramIndividual:
		b.deliverTelegramIndividual(chatID, paths, replyToID, format, status, req.ctx)
	case transferModeTelegramZip:
		b.deliverTelegramZip(chatID, paths, replyToID, albumID, format, status, req.ctx)
	case transferModeGofileZip:
		b.deliverGofileZip(chatID, paths, replyToID, single, status, req.ctx)
	default:
		if transferMode == transferModeZip {
			b.deliverGofileZip(chatID, paths, replyToID, single, status, req.ctx)
		} else {
			b.deliverTelegramIndividual(chatID, paths, replyToID, format, status, req.ctx)
		}
	}
}

// deliverTelegramIndividual uploads tracks as audio messages in groups of up to 10 via MTProto with cover art.
func (b *TelegramBot) deliverTelegramIndividual(chatID int64, paths []string, replyToID int, format string, status *DownloadStatus, ctx context.Context) {
	if ctx != nil && ctx.Err() != nil {
		status.UpdateSync("Cancelled", 0, 0)
		return
	}
	if b.mtproto == nil || !b.mtproto.IsReady() {
		// Fallback: try Bot API sendAudioFile for small files, or Gofile for large
		b.deliverTelegramIndividualFallback(chatID, paths, replyToID, format, status, ctx)
		return
	}

	sentAny := false
	// Send cover art as standalone photo with album info
	if len(paths) > 0 {
		if coverPath := findCoverFile(filepath.Dir(paths[0])); coverPath != "" {
			coverCaption := buildCoverCaption(paths)
			_ = b.sendPhotoWithReply(chatID, coverPath, coverCaption, replyToID)
		}
	}

	// Chunk paths into groups of up to 10
	const maxGroupSize = 10
	for i := 0; i < len(paths); i += maxGroupSize {
		if ctx != nil && ctx.Err() != nil {
			status.UpdateSync("Cancelled", 0, 0)
			return
		}
		end := i + maxGroupSize
		if end > len(paths) {
			end = len(paths)
		}
		chunk := paths[i:end]

		// Prepare AudioGroupItem slice
		var groupItems []AudioGroupItem
		for _, path := range chunk {
			meta, hasMeta := getDownloadedMeta(path)
			title := filepath.Base(path)
			performer := ""
			durationSecs := 0
			if hasMeta {
				title = meta.Title
				performer = meta.Performer
				if meta.DurationMillis > 0 {
					durationSecs = int(meta.DurationMillis / 1000)
				}
			}

			// Prepare thumbnail
			thumbPath := ""
			coverPath := findCoverFile(filepath.Dir(path))
			if coverPath != "" {
				if tp, err := makeTelegramThumb(coverPath); err == nil {
					thumbPath = tp
				}
			}

			info, err := os.Stat(path)
			var sizeBytes int64
			if err == nil {
				sizeBytes = info.Size()
			}
			bitrateKbps := calcBitrateKbps(sizeBytes, meta.DurationMillis)
			caption := formatTelegramCaption(sizeBytes, bitrateKbps, format)

			groupItems = append(groupItems, AudioGroupItem{
				FilePath:     path,
				Title:        title,
				Performer:    performer,
				DurationSecs: durationSecs,
				Caption:      caption,
				ThumbPath:    thumbPath,
			})
		}

		// Send as group
		err := b.mtproto.UploadAndSendAudioGroup(chatID, groupItems, replyToID, status, ctx)

		if err != nil {
			if ctx != nil && ctx.Err() != nil {
				// Clean up thumbnails
				for _, item := range groupItems {
					if item.ThumbPath != "" {
						_ = os.Remove(item.ThumbPath)
					}
				}
				status.UpdateSync("Cancelled", 0, 0)
				return
			}
			fmt.Printf("MTProto group upload failed (chatID=%d): %v. Falling back to individual upload...\n", chatID, err)
			status.Update(fmt.Sprintf("⚠️ Group upload failed: %v. Sending individually via MTProto...", err), 0, 0)

			// Fallback: send each file in the group individually via MTProto first
			for _, item := range groupItems {
				if ctx != nil && ctx.Err() != nil {
					// Clean up thumbnails
					for _, it := range groupItems {
						if it.ThumbPath != "" {
							_ = os.Remove(it.ThumbPath)
						}
					}
					status.UpdateSync("Cancelled", 0, 0)
					return
				}

				errIndiv := b.mtproto.UploadAndSendAudio(
					chatID,
					item.FilePath,
					item.Title,
					item.Performer,
					item.DurationSecs,
					item.Caption,
					item.ThumbPath,
					replyToID,
					status,
					ctx,
				)
				if errIndiv != nil {
					fmt.Printf("MTProto individual upload failed for %s: %v. Falling back to Gofile...\n", filepath.Base(item.FilePath), errIndiv)
					status.Update(fmt.Sprintf("Upload failed for %s: %v. Uploading to Gofile...", item.Title, errIndiv), 0, 0)
					
					downloadLink, gofileErr := apputils.UploadToGofile(ctx, item.FilePath, Config.GofileToken)
					if gofileErr == nil {
						msg := fmt.Sprintf("File: %s\nDownload Link: %s", filepath.Base(item.FilePath), downloadLink)
						_ = b.sendMessageWithReply(chatID, msg, nil, replyToID)
						sentAny = true
					}
				} else {
					sentAny = true
				}
			}
		} else {
			sentAny = true
		}

		// Clean up thumbnails
		for _, item := range groupItems {
			if item.ThumbPath != "" {
				_ = os.Remove(item.ThumbPath)
			}
		}
	}

	if sentAny {
		status.Stop()
		_ = b.deleteMessage(chatID, status.MessageID())
	}
}

// deliverTelegramIndividualFallback sends tracks via Bot API (limited to maxFileBytes) or Gofile.
func (b *TelegramBot) deliverTelegramIndividualFallback(chatID int64, paths []string, replyToID int, format string, status *DownloadStatus, ctx context.Context) {
	sentAny := false
	var lastErr error
	// Send cover art as standalone photo with album info
	if len(paths) > 0 {
		if coverPath := findCoverFile(filepath.Dir(paths[0])); coverPath != "" {
			coverCaption := buildCoverCaption(paths)
			_ = b.sendPhotoWithReply(chatID, coverPath, coverCaption, replyToID)
		}
	}
	for _, path := range paths {
		if ctx != nil && ctx.Err() != nil {
			status.UpdateSync("Cancelled", 0, 0)
			return
		}
		info, err := os.Stat(path)
		if err != nil {
			lastErr = err
			continue
		}
		if info.Size() <= b.maxFileBytes {
			// Small enough for Bot API
			err = b.sendAudioFile(chatID, path, 0, status, format)
			if err == nil {
				sentAny = true
				continue
			}
			fmt.Printf("Bot API audio upload failed, falling back to Gofile: %v\n", err)
		}

		if ctx != nil && ctx.Err() != nil {
			status.UpdateSync("Cancelled", 0, 0)
			return
		}
		// Too large or Bot API failed — use Gofile
		status.UpdateSync("Uploading to Gofile...", 0, 0)
		downloadLink, err := apputils.UploadToGofile(ctx, path, Config.GofileToken)
		if err != nil {
			lastErr = err
			status.UpdateSync(fmt.Sprintf("Gofile upload failed: %v", err), 0, 0)
			continue
		}
		msg := fmt.Sprintf("File: %s\nDownload Link: %s", filepath.Base(path), downloadLink)
		_ = b.sendMessage(chatID, msg, nil)
		sentAny = true
	}
	if sentAny {
		status.Stop()
		_ = b.deleteMessage(chatID, status.MessageID())
		return
	}
	b.reportDeliveryFailure(chatID, replyToID, status, lastErr)
}

// deliverTelegramZip creates a ZIP and uploads it as a document via MTProto.
func (b *TelegramBot) deliverTelegramZip(chatID int64, paths []string, replyToID int, albumID string, format string, status *DownloadStatus, ctx context.Context) {
	if ctx != nil && ctx.Err() != nil {
		if status != nil {
			status.UpdateSync("Cancelled", 0, 0)
		}
		return
	}
	if status != nil {
		status.Update("Zipping", 0, 0)
	}
	zipPath, displayName, err := createZipFromPaths(paths)
	if err != nil {
		status.UpdateSync(fmt.Sprintf("Failed to create ZIP: %v", err), 0, 0)
		return
	}
	defer os.Remove(zipPath)

	// Check ZIP size — Telegram limit is 2GB, use 1.90GB safety margin
	const maxTelegramZipBytes = int64(1900 * 1024 * 1024) // ~1.90 GB
	if info, err := os.Stat(zipPath); err == nil && info.Size() > maxTelegramZipBytes {
		sizeMB := float64(info.Size()) / (1024 * 1024)
		fmt.Printf("ZIP too large for Telegram (%.0f MB), redirecting to Gofile\n", sizeMB)
		status.UpdateSync(fmt.Sprintf("ZIP is %.0f MB (>1.90 GB) — uploading to Gofile instead.", sizeMB), 0, 0)
		b.deliverGofileZipFromPath(chatID, zipPath, displayName, replyToID, status, ctx)
		return
	}

	if ctx != nil && ctx.Err() != nil {
		status.UpdateSync("Cancelled", 0, 0)
		return
	}

	if b.mtproto != nil && b.mtproto.IsReady() {
		err = b.mtproto.UploadAndSendDocument(chatID, zipPath, displayName, "", replyToID, status, ctx)
		if err != nil {
			if ctx != nil && ctx.Err() != nil {
				status.UpdateSync("Cancelled", 0, 0)
				return
			}
			fmt.Printf("MTProto ZIP upload failed, falling back to Gofile: %v\n", err)
			// Fallback to Gofile
			b.deliverGofileZipFromPath(chatID, zipPath, displayName, replyToID, status, ctx)
		} else {
			status.Stop()
			_ = b.deleteMessage(chatID, status.MessageID())
		}
	} else {
		// No MTProto — fallback to Gofile
		b.deliverGofileZipFromPath(chatID, zipPath, displayName, replyToID, status, ctx)
	}
}

// deliverGofileZip creates a ZIP and uploads it to Gofile (original behavior).
func (b *TelegramBot) deliverGofileZip(chatID int64, paths []string, replyToID int, single bool, status *DownloadStatus, ctx context.Context) {
	if ctx != nil && ctx.Err() != nil {
		if status != nil {
			status.UpdateSync("Cancelled", 0, 0)
		}
		return
	}
	if single {
		// Single song: upload each file to Gofile
		sentAny := false
		var lastErr error
		// Send cover art as standalone photo with album info
		if len(paths) > 0 {
			if coverPath := findCoverFile(filepath.Dir(paths[0])); coverPath != "" {
				coverCaption := buildCoverCaption(paths)
				_ = b.sendPhotoWithReply(chatID, coverPath, coverCaption, replyToID)
			}
		}
		for _, path := range paths {
			if ctx != nil && ctx.Err() != nil {
				status.UpdateSync("Cancelled", 0, 0)
				return
			}
			status.UpdateSync("Uploading to Gofile...", 0, 0)
			downloadLink, err := apputils.UploadToGofile(ctx, path, Config.GofileToken)
			if err != nil {
				lastErr = err
				status.UpdateSync(fmt.Sprintf("Gofile upload failed: %v", err), 0, 0)
				continue
			}
			msg := fmt.Sprintf("File: %s\nDownload Link: %s", filepath.Base(path), downloadLink)
			_ = b.sendMessageWithReply(chatID, msg, nil, replyToID)
			sentAny = true
		}
		if sentAny {
			status.Stop()
			_ = b.deleteMessage(chatID, status.MessageID())
			return
		}
		b.reportDeliveryFailure(chatID, replyToID, status, lastErr)
		return
	}

	if status != nil {
		status.Update("Zipping", 0, 0)
	}
	zipPath, displayName, err := createZipFromPaths(paths)
	if err != nil {
		status.UpdateSync(fmt.Sprintf("Failed to create ZIP: %v", err), 0, 0)
		return
	}
	defer os.Remove(zipPath)

	b.deliverGofileZipFromPath(chatID, zipPath, displayName, replyToID, status, ctx)
}

// deliverGofileZipFromPath uploads a pre-created ZIP file to Gofile and sends the link.
func (b *TelegramBot) deliverGofileZipFromPath(chatID int64, zipPath string, displayName string, replyToID int, status *DownloadStatus, ctx context.Context) {
	if ctx != nil && ctx.Err() != nil {
		if status != nil {
			status.UpdateSync("Cancelled", 0, 0)
		}
		return
	}
	status.UpdateSync("Uploading to Gofile...", 0, 0)
	downloadLink, err := apputils.UploadToGofile(ctx, zipPath, Config.GofileToken)
	if err != nil {
		b.reportDeliveryFailure(chatID, replyToID, status, err)
		return
	}

	msg := fmt.Sprintf("File: %s\nDownload Link: %s", displayName, downloadLink)
	_ = b.sendMessageWithReply(chatID, msg, nil, replyToID)

	status.Stop()
	_ = b.deleteMessage(chatID, status.MessageID())
}

// reportDeliveryFailure gives the user a clear terminal message when nothing could be
// delivered, and freezes the status board on a definitive "failed" state (rather than
// leaving a stale "Uploading..." that never resolves). If the failure was a context
// cancellation, it reports that instead of a generic error.
func (b *TelegramBot) reportDeliveryFailure(chatID int64, replyToID int, status *DownloadStatus, lastErr error) {
	if lastErr != nil && errors.Is(lastErr, context.Canceled) {
		if status != nil {
			status.UpdateSync("Cancelled", 0, 0)
		}
		return
	}
	detail := "delivery failed"
	if lastErr != nil {
		detail = lastErr.Error()
	}
	if status != nil {
		status.UpdateSync(fmt.Sprintf("Failed: %s", detail), 0, 0)
		status.Stop()
	}
	_ = b.sendMessageWithReply(chatID, fmt.Sprintf("❌ Upload failed — no files could be delivered.\n%s", detail), nil, replyToID)
}

// deliverMusicVideo sends a downloaded music video as a native Telegram video (inline
// player + thumbnail), falling back to a document and then Gofile. The status board is
// always resolved to a terminal state.
func (b *TelegramBot) deliverMusicVideo(chatID int64, path string, replyToID int, status *DownloadStatus, ctx context.Context) {
	if ctx != nil && ctx.Err() != nil {
		status.UpdateSync("Cancelled", 0, 0)
		return
	}

	var sizeBytes int64
	if info, err := os.Stat(path); err == nil {
		sizeBytes = info.Size()
	}

	// Caption from metadata when available.
	caption := filepath.Base(path)
	durationSecs := 0
	if meta, ok := getDownloadedMeta(path); ok {
		if meta.Title != "" {
			caption = meta.Title
			if meta.Performer != "" {
				caption = fmt.Sprintf("%s — %s", meta.Performer, meta.Title)
			}
		}
		if meta.DurationMillis > 0 {
			durationSecs = int(meta.DurationMillis / 1000)
		}
	}

	// Best-effort probe + thumbnail; failures are non-fatal (Telegram tolerates zeros).
	width, height, probedDur := probeVideo(ctx, path)
	if probedDur > 0 {
		durationSecs = probedDur
	}
	thumbPath := makeVideoThumb(ctx, path)
	if thumbPath != "" {
		defer os.Remove(thumbPath)
	}

	// MTProto upload ceiling (~2GB). Above it, or when MTProto is down, go straight to Gofile.
	const mtprotoMaxBytes = 2 * 1000 * 1000 * 1024
	if b.mtproto != nil && b.mtproto.IsReady() && (sizeBytes == 0 || sizeBytes <= mtprotoMaxBytes) {
		// Try native video first.
		err := b.mtproto.UploadAndSendVideo(chatID, path, caption, durationSecs, width, height, thumbPath, replyToID, status, ctx)
		if err == nil {
			status.Stop()
			_ = b.deleteMessage(chatID, status.messageID)
			return
		}
		if ctx != nil && ctx.Err() != nil {
			status.UpdateSync("Cancelled", 0, 0)
			return
		}
		fmt.Printf("MV video upload failed: %v. Trying document...\n", err)
		status.Update(fmt.Sprintf("⚠️ Video send failed: %v. Sending as document...", err), 0, 0)

		// Fall back to sending the same file as a document.
		errDoc := b.mtproto.UploadAndSendDocument(chatID, path, filepath.Base(path), caption, replyToID, status, ctx)
		if errDoc == nil {
			status.Stop()
			_ = b.deleteMessage(chatID, status.messageID)
			return
		}
		if ctx != nil && ctx.Err() != nil {
			status.UpdateSync("Cancelled", 0, 0)
			return
		}
		fmt.Printf("MV document upload failed: %v. Falling back to Gofile...\n", errDoc)
	}

	// Gofile fallback.
	if ctx != nil && ctx.Err() != nil {
		status.UpdateSync("Cancelled", 0, 0)
		return
	}
	status.UpdateSync("Uploading to Gofile...", 0, 0)
	link, err := apputils.UploadToGofile(ctx, path, Config.GofileToken)
	if err != nil {
		b.reportDeliveryFailure(chatID, replyToID, status, err)
		return
	}
	msg := fmt.Sprintf("File: %s\nDownload Link: %s", filepath.Base(path), link)
	_ = b.sendMessageWithReply(chatID, msg, nil, replyToID)
	status.Stop()
	_ = b.deleteMessage(chatID, status.messageID)
}

// deliverMvGofile uploads a single music video straight to Gofile — no Telegram attempt,
// no zip — used when the user explicitly picks Gofile at the prompt. The status board is
// always resolved to a terminal state.
func (b *TelegramBot) deliverMvGofile(chatID int64, path string, replyToID int, status *DownloadStatus, ctx context.Context) {
	if ctx != nil && ctx.Err() != nil {
		status.UpdateSync("Cancelled", 0, 0)
		return
	}
	status.UpdateSync("Uploading to Gofile...", 0, 0)
	link, err := apputils.UploadToGofile(ctx, path, Config.GofileToken)
	if err != nil {
		b.reportDeliveryFailure(chatID, replyToID, status, err)
		return
	}
	msg := fmt.Sprintf("File: %s\nDownload Link: %s", filepath.Base(path), link)
	_ = b.sendMessageWithReply(chatID, msg, nil, replyToID)
	status.Stop()
	_ = b.deleteMessage(chatID, status.messageID)
}

// deliverArtwork sends extracted artwork to Telegram: image files as photos and the
// motion artwork (.mp4) as a video. These are small, so the Bot API is sufficient —
// no MTProto or Gofile. The status board is always resolved to a terminal state.
func (b *TelegramBot) deliverArtwork(chatID int64, paths []string, replyToID int, status *DownloadStatus, ctx context.Context) {
	if ctx != nil && ctx.Err() != nil {
		status.UpdateSync("Cancelled", 0, 0)
		return
	}
	status.UpdateSync("Uploading artwork...", 0, 0)

	sentAny := false
	var lastErr error
	for _, p := range paths {
		if ctx != nil && ctx.Err() != nil {
			status.UpdateSync("Cancelled", 0, 0)
			return
		}
		caption := strings.TrimSuffix(filepath.Base(p), filepath.Ext(p))
		var err error
		switch strings.ToLower(filepath.Ext(p)) {
		case ".mp4", ".mov", ".m4v":
			err = b.sendVideoWithReply(chatID, p, caption, replyToID)
		default:
			err = b.sendPhotoWithReply(chatID, p, caption, replyToID)
		}
		// sendPhoto caps at 10MB and sendVideo at 50MB; a full-res 5000x5000 cover
		// can exceed the photo limit. Fall back to a plain document (≤50MB) so the
		// file is still delivered, just without the inline preview.
		if err != nil {
			fmt.Printf("artwork inline send failed for %s: %v — trying document...\n", filepath.Base(p), err)
			if derr := b.sendDocumentFile(chatID, p, filepath.Base(p), replyToID, nil, ""); derr != nil {
				lastErr = derr
				fmt.Printf("artwork document send failed for %s: %v\n", filepath.Base(p), derr)
			} else {
				sentAny = true
			}
		} else {
			sentAny = true
		}
	}

	if !sentAny {
		b.reportDeliveryFailure(chatID, replyToID, status, lastErr)
		return
	}
	status.Stop()
	_ = b.deleteMessage(chatID, status.messageID)
}

// probeVideo returns the video's width, height, and duration (seconds) via ffprobe.
// Any failure yields zeros — Telegram accepts a video document without these hints.
func probeVideo(ctx context.Context, path string) (width int, height int, durationSecs int) {
	out, err := exec.CommandContext(ctx, "ffprobe", "-v", "error",
		"-select_streams", "v:0",
		"-show_entries", "stream=width,height:format=duration",
		"-of", "default=noprint_wrappers=1",
		path).Output()
	if err != nil {
		return 0, 0, 0
	}
	for _, line := range strings.Split(string(out), "\n") {
		line = strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(line, "width="):
			width, _ = strconv.Atoi(strings.TrimPrefix(line, "width="))
		case strings.HasPrefix(line, "height="):
			height, _ = strconv.Atoi(strings.TrimPrefix(line, "height="))
		case strings.HasPrefix(line, "duration="):
			if f, perr := strconv.ParseFloat(strings.TrimPrefix(line, "duration="), 64); perr == nil {
				durationSecs = int(f)
			}
		}
	}
	return width, height, durationSecs
}

// makeVideoThumb extracts a JPEG thumbnail (~320px wide) from the video for Telegram's
// inline preview. Returns "" if extraction fails; the caller treats a thumb as optional.
func makeVideoThumb(ctx context.Context, path string) string {
	thumb := path + ".thumb.jpg"
	cmd := exec.CommandContext(ctx, Config.FFmpegPath, "-y", "-ss", "1", "-i", path,
		"-frames:v", "1", "-vf", "scale=320:-2", thumb)
	if err := cmd.Run(); err != nil {
		return ""
	}
	if info, err := os.Stat(thumb); err != nil || info.Size() == 0 {
		_ = os.Remove(thumb)
		return ""
	}
	return thumb
}

type downloadFileEntry struct {
	path    string
	size    int64
	modTime time.Time
}

func (b *TelegramBot) cleanupDownloadsIfNeeded() {
	root := strings.TrimSpace(Config.AlacSaveFolder)
	if root == "" {
		return
	}
	cleanRoot := filepath.Clean(root)
	if cleanRoot == "." || cleanRoot == string(filepath.Separator) {
		fmt.Printf("Skip cleanup for unsafe download folder: %s\n", root)
		return
	}
	info, err := os.Stat(cleanRoot)
	if err != nil {
		if os.IsNotExist(err) {
			return
		}
		fmt.Printf("Download folder check failed: %v\n", err)
		return
	}
	if !info.IsDir() {
		return
	}
	totalSize, files, err := scanDownloadFolder(cleanRoot, Config.TelegramCacheFile)
	if err != nil {
		fmt.Printf("Download folder scan failed: %v\n", err)
		return
	}
	maxBytes := telegramDownloadMaxBytes()
	if totalSize <= maxBytes {
		return
	}
	sort.Slice(files, func(i, j int) bool {
		return files[i].modTime.Before(files[j].modTime)
	})
	for _, entry := range files {
		if totalSize <= maxBytes {
			break
		}
		if err := os.Remove(entry.path); err != nil {
			continue
		}
		totalSize -= entry.size
	}
}

func scanDownloadFolder(root string, cacheFile string) (int64, []downloadFileEntry, error) {
	var totalSize int64
	entries := []downloadFileEntry{}
	cachePath := ""
	if cacheFile != "" {
		cachePath = filepath.Clean(cacheFile)
	}
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		if cachePath != "" && filepath.Clean(path) == cachePath {
			return nil
		}
		size := info.Size()
		totalSize += size
		entries = append(entries, downloadFileEntry{
			path:    path,
			size:    size,
			modTime: info.ModTime(),
		})
		return nil
	})
	if err != nil {
		return totalSize, entries, err
	}
	return totalSize, entries, nil
}

func createZipFromPaths(paths []string) (string, string, error) {
	if len(paths) == 0 {
		return "", "", fmt.Errorf("no files to zip")
	}
	displayName := zipDisplayName(paths)
	tmp, err := os.CreateTemp("", "amdl-*.zip")
	if err != nil {
		return "", "", err
	}
	tmpPath := tmp.Name()
	zipWriter := zip.NewWriter(tmp)
	fail := func(err error) (string, string, error) {
		_ = zipWriter.Close()
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		return "", "", err
	}
	rootDir := commonZipRoot(paths)
	added := 0
	for _, path := range paths {
		info, err := os.Stat(path)
		if err != nil {
			return fail(err)
		}
		if info.IsDir() {
			continue
		}
		header, err := zip.FileInfoHeader(info)
		if err != nil {
			return fail(err)
		}
		relName := filepath.Base(path)
		if rootDir != "" {
			if rel, err := filepath.Rel(rootDir, path); err == nil && rel != "." && !strings.HasPrefix(rel, "..") {
				relName = rel
			}
		}
		header.Name = filepath.ToSlash(relName)
		header.Method = zip.Deflate
		writer, err := zipWriter.CreateHeader(header)
		if err != nil {
			return fail(err)
		}
		file, err := os.Open(path)
		if err != nil {
			return fail(err)
		}
		_, err = io.Copy(writer, file)
		file.Close()
		if err != nil {
			return fail(err)
		}
		added++
	}
	if err := zipWriter.Close(); err != nil {
		return fail(err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return "", "", err
	}
	if added == 0 {
		_ = os.Remove(tmpPath)
		return "", "", fmt.Errorf("no files to zip")
	}
	return tmpPath, displayName, nil
}

func zipDisplayName(paths []string) string {
	root := commonZipRoot(paths)
	if root == "" {
		return "album.zip"
	}
	base := filepath.Base(root)
	if base == "" || base == "." || base == string(filepath.Separator) {
		return "album.zip"
	}
	return base + ".zip"
}

func commonZipRoot(paths []string) string {
	if len(paths) == 0 {
		return ""
	}
	root := filepath.Dir(paths[0])
	for _, path := range paths[1:] {
		dir := filepath.Dir(path)
		for !isParentDir(root, dir) {
			parent := filepath.Dir(root)
			if parent == root {
				return root
			}
			root = parent
		}
	}
	return root
}

func isParentDir(parent, child string) bool {
	if parent == "" || child == "" {
		return false
	}
	rel, err := filepath.Rel(parent, child)
	if err != nil {
		return false
	}
	if rel == "." {
		return true
	}
	return !strings.HasPrefix(rel, "..")
}

func (b *TelegramBot) sendAudioFile(chatID int64, filePath string, replyToID int, status *DownloadStatus, format string) error {
	format = normalizeTelegramFormat(format)
	if format == "" {
		format = defaultTelegramFormat
	}
	ext := strings.ToLower(filepath.Ext(filePath))
	switch format {
	case telegramFormatFlac:
		if ext != ".flac" {
			return fmt.Errorf("output is not FLAC: %s", filepath.Base(filePath))
		}
	case telegramFormatAlac:
		if ext != ".m4a" && ext != ".mp4" {
			return fmt.Errorf("output is not ALAC: %s", filepath.Base(filePath))
		}
	}
	sendPath := filePath
	displayName := filepath.Base(filePath)
	thumbPath := ""
	compressed := false
	meta, hasMeta := getDownloadedMeta(filePath)
	cleanup := func() {
		if thumbPath != "" {
			_ = os.Remove(thumbPath)
		}
	}
	defer cleanup()

	info, err := os.Stat(sendPath)
	if err != nil {
		return err
	}
	if info.Size() > b.maxFileBytes {
		if format != telegramFormatFlac {
			return fmt.Errorf("ALAC file exceeds Telegram limit (%dMB). Use /settings flac or raise telegram-max-file-mb.", b.maxFileBytes/1024/1024)
		}
		if status != nil {
			status.Update("Compressing", 0, 0)
		}
		compressedPath, err := b.compressFlacToSize(sendPath, b.maxFileBytes)
		if err != nil {
			return err
		}
		sendPath = compressedPath
		compressed = true
		cleanup = func() {
			_ = os.Remove(compressedPath)
		}
		info, err = os.Stat(sendPath)
		if err != nil {
			return err
		}
		if info.Size() > b.maxFileBytes {
			return fmt.Errorf("compressed file still too large: %s", filepath.Base(sendPath))
		}
	}
	file, err := os.Open(sendPath)
	if err != nil {
		return err
	}
	defer file.Close()

	sizeBytes := info.Size()
	durationMillis := int64(0)
	if hasMeta {
		durationMillis = meta.DurationMillis
	}
	bitrateKbps := calcBitrateKbps(sizeBytes, durationMillis)
	if bitrateKbps <= 0 {
		if seconds, err := getAudioDurationSeconds(sendPath); err == nil && seconds > 0 {
			durationMillis = int64(seconds * 1000.0)
			bitrateKbps = calcBitrateKbps(sizeBytes, durationMillis)
		}
	}
	caption := formatTelegramCaption(sizeBytes, bitrateKbps, format)
	if status != nil {
		status.Update("Uploading", 0, 0)
	}
	coverPath := findCoverFile(filepath.Dir(filePath))
	if coverPath != "" {
		if path, err := makeTelegramThumb(coverPath); err != nil { fmt.Printf("makeTelegramThumb failed: %v\n", err) } else {
			thumbPath = path
		}
	}

	pr, pw := io.Pipe()
	writer := multipart.NewWriter(pw)
	contentType := writer.FormDataContentType()
	writeErrCh := make(chan error, 1)

	req, err := http.NewRequest("POST", b.apiURL("sendAudio"), pr)
	if err != nil {
		_ = pw.CloseWithError(err)
		return b.sanitizeTelegramError(err)
	}
	req.Header.Set("Content-Type", contentType)
	go func() {
		err := func() error {
			if err := writer.WriteField("chat_id", strconv.FormatInt(chatID, 10)); err != nil {
				return err
			}
			if replyToID > 0 {
				if err := writer.WriteField("reply_to_message_id", strconv.Itoa(replyToID)); err != nil {
					return err
				}
			}
			if caption != "" {
				if err := writer.WriteField("caption", caption); err != nil {
					return err
				}
			}
			if hasMeta {
				if meta.Title != "" {
					if err := writer.WriteField("title", meta.Title); err != nil {
						return err
					}
				}
				if meta.Performer != "" {
					if err := writer.WriteField("performer", meta.Performer); err != nil {
						return err
					}
				}
			}
			part, err := writer.CreateFormFile("audio", displayName)
			if err != nil {
				return err
			}
			if _, err := io.Copy(part, file); err != nil {
				return err
			}
			if thumbPath != "" {
				thumbFile, err := os.Open(thumbPath)
				if err == nil {
					defer thumbFile.Close()
					thumbPart, err := writer.CreateFormFile("thumbnail", filepath.Base(thumbPath))
					if err == nil {
						if _, err := io.Copy(thumbPart, thumbFile); err != nil {
							return err
						}
					}
				}
			}
			return writer.Close()
		}()
		if err != nil {
			_ = pw.CloseWithError(err)
		} else {
			_ = pw.Close()
		}
		writeErrCh <- err
	}()
	resp, err := b.client.Do(req)
	if err != nil {
		_ = pw.CloseWithError(err)
		writeErr := <-writeErrCh
		if writeErr != nil {
			return writeErr
		}
		return b.sanitizeTelegramError(err)
	}
	defer resp.Body.Close()
	writeErr := <-writeErrCh
	if writeErr != nil {
		return writeErr
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("telegram sendAudio failed: %s", resp.Status)
	}
	apiResp := sendAudioResponse{}
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		return err
	}
	if !apiResp.OK {
		return fmt.Errorf("telegram sendAudio error: %s", b.sanitizeTelegramText(apiResp.Description))
	}
	if hasMeta && meta.TrackID != "" && apiResp.Result.Audio.FileID != "" {
		b.storeCachedAudio(meta.TrackID, CachedAudio{
			FileID:         apiResp.Result.Audio.FileID,
			FileSize:       apiResp.Result.Audio.FileSize,
			Compressed:     compressed,
			Format:         format,
			SizeBytes:      sizeBytes,
			BitrateKbps:    bitrateKbps,
			DurationMillis: durationMillis,
			Title:          meta.Title,
			Performer:      meta.Performer,
		})
	}
	return nil
}

func (b *TelegramBot) sendDocumentFile(chatID int64, filePath string, displayName string, replyToID int, status *DownloadStatus, cacheKey string) error {
	if displayName == "" {
		displayName = filepath.Base(filePath)
	}
	info, err := os.Stat(filePath)
	if err != nil {
		return err
	}
	if info.Size() > b.maxFileBytes {
		return fmt.Errorf("ZIP exceeds Telegram limit (%dMB)", b.maxFileBytes/1024/1024)
	}
	if status != nil {
		status.Update("Uploading ZIP", 0, 0)
	}

	pr, pw := io.Pipe()
	writer := multipart.NewWriter(pw)
	contentType := writer.FormDataContentType()
	writeErrCh := make(chan error, 1)

	req, err := http.NewRequest("POST", b.apiURL("sendDocument"), pr)
	if err != nil {
		_ = pw.CloseWithError(err)
		return b.sanitizeTelegramError(err)
	}
	req.Header.Set("Content-Type", contentType)
	go func() {
		err := func() error {
			if err := writer.WriteField("chat_id", strconv.FormatInt(chatID, 10)); err != nil {
				return err
			}
			if replyToID > 0 {
				if err := writer.WriteField("reply_to_message_id", strconv.Itoa(replyToID)); err != nil {
					return err
				}
			}
			part, err := writer.CreateFormFile("document", displayName)
			if err != nil {
				return err
			}
			file, err := os.Open(filePath)
			if err != nil {
				return err
			}
			defer file.Close()
			if _, err := io.Copy(part, file); err != nil {
				return err
			}
			return writer.Close()
		}()
		if err != nil {
			_ = pw.CloseWithError(err)
		} else {
			_ = pw.Close()
		}
		writeErrCh <- err
	}()
	resp, err := b.client.Do(req)
	if err != nil {
		_ = pw.CloseWithError(err)
		writeErr := <-writeErrCh
		if writeErr != nil {
			return writeErr
		}
		return b.sanitizeTelegramError(err)
	}
	defer resp.Body.Close()
	writeErr := <-writeErrCh
	if writeErr != nil {
		return writeErr
	}
	if resp.StatusCode != http.StatusOK {
		responseBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("telegram sendDocument failed: %s", b.sanitizeTelegramText(strings.TrimSpace(string(responseBody))))
	}
	apiResp := sendDocumentResponse{}
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		return err
	}
	if !apiResp.OK {
		return fmt.Errorf("telegram sendDocument error: %s", b.sanitizeTelegramText(apiResp.Description))
	}
	if cacheKey != "" && apiResp.Result.Document.FileID != "" {
		b.storeCachedDocument(cacheKey, CachedDocument{
			FileID:   apiResp.Result.Document.FileID,
			FileSize: apiResp.Result.Document.FileSize,
		})
	}
	return nil
}

func (b *TelegramBot) sendDocumentByFileID(chatID int64, entry CachedDocument, replyToID int) error {
	if entry.FileID == "" {
		return fmt.Errorf("document file_id is empty")
	}
	payload := map[string]any{
		"chat_id":  chatID,
		"document": entry.FileID,
	}
	if replyToID > 0 {
		payload["reply_to_message_id"] = replyToID
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequest("POST", b.apiURL("sendDocument"), bytes.NewReader(body))
	if err != nil {
		return b.sanitizeTelegramError(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := b.client.Do(req)
	if err != nil {
		return b.sanitizeTelegramError(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		responseBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("telegram sendDocument failed: %s", b.sanitizeTelegramText(strings.TrimSpace(string(responseBody))))
	}
	apiResp := apiResponse{}
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		return err
	}
	if !apiResp.OK {
		return fmt.Errorf("telegram sendDocument error: %s", b.sanitizeTelegramText(apiResp.Description))
	}
	return nil
}

// queueBoardSuffix returns the queued-tasks listing appended to a status board, or
// "" when the queue is empty (so a clean single download shows no queue section).
// It reads the display-only queuedReqs mirror so it never touches the live channel
// (no draining/blocking) even when called every few seconds from the update loop.
func (b *TelegramBot) queueBoardSuffix() string {
	b.queueMu.Lock()
	reqs := make([]*downloadRequest, len(b.queuedReqs))
	copy(reqs, b.queuedReqs)
	b.queueMu.Unlock()
	if len(reqs) == 0 {
		return ""
	}
	out := fmt.Sprintf("\n\n📋 Queue: %d task(s)\n", len(reqs))
	for i, r := range reqs {
		user := r.username
		if user == "" {
			user = fmt.Sprintf("ID:%d", r.userID)
		}
		out += fmt.Sprintf("%d. %s (by @%s) — /stop_%s\n", i+1, r.taskID, user, r.taskID)
	}
	return out
}

// removeQueuedReqLocked drops a task from the display-only queue mirror.
// The caller must hold queueMu.
func (b *TelegramBot) removeQueuedReqLocked(taskID string) {
	for i, r := range b.queuedReqs {
		if r.taskID == taskID {
			b.queuedReqs = append(b.queuedReqs[:i], b.queuedReqs[i+1:]...)
			return
		}
	}
}

// replaceIdleStatusBoard shows a single idle/queue board, deleting any previous one
// so /status never stacks duplicate messages when no download is running.
func (b *TelegramBot) replaceIdleStatusBoard(chatID int64, replyToID int, text string) {
	b.queueMu.Lock()
	oldID := b.idleStatusMsgID
	oldChat := b.idleStatusChatID
	b.queueMu.Unlock()
	if oldID != 0 {
		_ = b.deleteMessage(oldChat, oldID)
	}
	newID, err := b.sendMessageWithReplyReturn(chatID, text, nil, replyToID)
	if err != nil {
		return
	}
	b.queueMu.Lock()
	b.idleStatusMsgID = newID
	b.idleStatusChatID = chatID
	b.queueMu.Unlock()
}

// progressSample is one (time, bytesDone) point in the rolling speed window.
type progressSample struct {
	at   time.Time
	done int64
}

// speedWindow is how far back the live speed/ETA readout looks. Computing speed
// over a trailing window (rather than cumulative bytes ÷ phase-elapsed) gives a
// responsive "current rate" that reacts within a few seconds and reads on the
// same basis for a 7 MB audio file and a 300 MB video. Samples are recorded
// in-memory on every progress callback; this does NOT change how often the board
// is edited (still throttled to ≥3s in flush), so it adds zero FloodWait risk.
const speedWindow = 6 * time.Second

type DownloadStatus struct {
	bot            *TelegramBot
	chatID         int64
	messageID      int
	taskID         string
	mode           string
	startedAt      time.Time
	phaseStartedAt time.Time
	lastPhase      string
	lastPercent    int
	lastText       string
	lastUpdate     time.Time
	mu             sync.Mutex
	latestPhase    string
	latestDone     int64
	latestTotal    int64
	dirty          bool
	updateCh       chan struct{}
	stopCh         chan struct{}
	stopOnce       sync.Once
	userID         int64
	username       string
	speedSamples   []progressSample
}

func newDownloadStatus(bot *TelegramBot, chatID int64, replyToID int, existingMsgID int, req *downloadRequest) (*DownloadStatus, error) {
	status := &DownloadStatus{
		bot:            bot,
		chatID:         chatID,
		messageID:      existingMsgID,
		taskID:         req.taskID,
		mode:           req.transferMode,
		startedAt:      req.startedAt,
		phaseStartedAt: req.startedAt,
		userID:         req.userID,
		username:       req.username,
		updateCh:       make(chan struct{}, 1),
		stopCh:         make(chan struct{}),
	}
	if existingMsgID <= 0 {
		msgID, err := bot.sendMessageWithReplyReturn(chatID, "Starting download...", nil, replyToID)
		if err != nil {
			return nil, err
		}
		status.messageID = msgID
	}
	go status.loop()
	return status, nil
}

func (s *DownloadStatus) Stop() {
	if s == nil || s.bot == nil {
		return
	}
	s.stopOnce.Do(func() {
		close(s.stopCh)
	})
}

func (s *DownloadStatus) Update(phase string, done, total int64) {
	if s == nil || s.bot == nil {
		return
	}
	s.mu.Lock()
	s.setLatestLocked(phase, done, total)
	s.mu.Unlock()
	select {
	case s.updateCh <- struct{}{}:
	default:
	}
}

func (s *DownloadStatus) UpdateSync(phase string, done, total int64) {
	if s == nil || s.bot == nil {
		return
	}
	s.mu.Lock()
	s.setLatestLocked(phase, done, total)
	s.mu.Unlock()
	s.flush(true)
}

// MessageID returns the board's current Telegram message ID under lock, so callers
// (delivery cleanup, etc.) stay correct even if Relocate moved the board concurrently.
func (s *DownloadStatus) MessageID() int {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.messageID
}

// RenderSnapshot returns the current status board text using the same renderer
// as the live message, so /status shows the identical rich view (progress bar,
// speed, ETA, mode) plus the queue section when the queue is non-empty.
func (s *DownloadStatus) RenderSnapshot() string {
	if s == nil {
		return ""
	}
	s.mu.Lock()
	phase := s.latestPhase
	done := s.latestDone
	total := s.latestTotal
	s.mu.Unlock()
	if strings.TrimSpace(phase) == "" {
		phase = "Working"
	}
	percent := -1
	if total > 0 {
		percent = int(float64(done) / float64(total) * 100)
		if percent < 0 {
			percent = 0
		}
		if percent > 100 {
			percent = 100
		}
	}
	return s.formatProgressText(phase, done, total, percent) + s.bot.queueBoardSuffix()
}

// Relocate deletes the current board message and re-sends it at the bottom of the
// chat, keeping a single up-to-date board (no stale duplicates) when /status is run
// or a new task is enqueued. The live update loop continues on the new message.
func (s *DownloadStatus) Relocate(replyToID int) {
	if s == nil || s.bot == nil {
		return
	}
	text := s.RenderSnapshot()
	newID, err := s.bot.sendMessageWithReplyReturn(s.chatID, text, nil, replyToID)
	if err != nil {
		return
	}
	s.mu.Lock()
	oldID := s.messageID
	s.messageID = newID
	s.lastText = text
	s.lastUpdate = time.Now()
	s.mu.Unlock()
	if oldID != 0 && oldID != newID {
		_ = s.bot.deleteMessage(s.chatID, oldID)
	}
}

func (s *DownloadStatus) setLatestLocked(phase string, done, total int64) {
	normalizedPhase := strings.TrimSpace(phase)
	if normalizedPhase == "" {
		normalizedPhase = "Working"
	}
	now := time.Now()
	phaseChanged := normalizedPhase != s.latestPhase
	if phaseChanged {
		s.phaseStartedAt = now
	} else if s.phaseStartedAt.IsZero() {
		s.phaseStartedAt = now
	}
	// Maintain a short rolling window of (time, bytes) samples for a live speed
	// readout. Reset it whenever the phase changes or the byte counter rewinds
	// (the per-file audio group reports each file starting from 0), so we never
	// compute a delta across a discontinuity.
	if phaseChanged || done < s.latestDone {
		s.speedSamples = s.speedSamples[:0]
	}
	s.speedSamples = append(s.speedSamples, progressSample{at: now, done: done})
	s.speedSamples = pruneSpeedSamples(s.speedSamples, now)

	s.latestPhase = normalizedPhase
	s.latestDone = done
	s.latestTotal = total
	s.dirty = true
}

// pruneSpeedSamples drops samples older than speedWindow. The caller always appends
// a now-stamped sample before pruning, so the result is never empty.
func pruneSpeedSamples(samples []progressSample, now time.Time) []progressSample {
	cutoff := now.Add(-speedWindow)
	n := 0
	for _, smp := range samples {
		if !smp.at.Before(cutoff) {
			samples[n] = smp
			n++
		}
	}
	return samples[:n]
}

// rollingSpeedBytesPerSec returns the upload/processing rate over the trailing
// speedWindow, or 0 when there isn't enough spread to measure. It takes its own
// lock and must be called WITHOUT s.mu held (flush/RenderSnapshot release it first).
func (s *DownloadStatus) rollingSpeedBytesPerSec() float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.speedSamples) < 2 {
		return 0
	}
	first := s.speedSamples[0]
	last := s.speedSamples[len(s.speedSamples)-1]
	dt := last.at.Sub(first.at).Seconds()
	db := last.done - first.done
	if dt <= 0 || db <= 0 {
		return 0
	}
	return float64(db) / dt
}

func (s *DownloadStatus) loop() {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-s.updateCh:
			s.flush(false)
		case <-ticker.C:
			s.flush(false)
		case <-s.stopCh:
			return
		}
	}
}

func (s *DownloadStatus) flush(force bool) {
	if s == nil || s.bot == nil {
		return
	}
	s.mu.Lock()
	if !s.dirty && !force {
		s.mu.Unlock()
		return
	}
	phase := s.latestPhase
	done := s.latestDone
	total := s.latestTotal
	s.dirty = false
	lastText := s.lastText
	lastUpdate := s.lastUpdate
	msgID := s.messageID
	s.mu.Unlock()

	percent := -1
	if total > 0 {
		percent = int(float64(done) / float64(total) * 100)
		if percent < 0 {
			percent = 0
		}
		if percent > 100 {
			percent = 100
		}
	}

	text := s.formatProgressText(phase, done, total, percent) + s.bot.queueBoardSuffix()
	now := time.Now()
	if !force {
		if text == lastText {
			return
		}
		if now.Sub(lastUpdate) < 3*time.Second {
			s.mu.Lock()
			s.dirty = true
			s.mu.Unlock()
			return
		}
	}

	if err := s.bot.editMessageText(s.chatID, msgID, text, nil); err != nil {
		s.mu.Lock()
		s.dirty = true
		s.mu.Unlock()
		return
	}
	s.mu.Lock()
	s.lastPhase = phase
	s.lastPercent = percent
	s.lastText = text
	s.lastUpdate = now
	s.mu.Unlock()
}

func formatUploadMode(mode string) string {
	switch mode {
	case transferModeTelegramIndividual:
		return "Telegram (individual)"
	case transferModeTelegramZip:
		return "Telegram (ZIP)"
	case transferModeGofileZip:
		return "Gofile"
	case transferModeMv:
		return "Music video (Telegram)"
	case transferModeMvGofile:
		return "Music video (Gofile)"
	case transferModeArt:
		return "Artwork"
	default:
		return mode
	}
}

func formatDuration(d time.Duration) string {
	d = d.Truncate(time.Second)
	h := d / time.Hour
	d -= h * time.Hour
	m := d / time.Minute
	d -= m * time.Minute
	s := d / time.Second

	if h > 0 {
		return fmt.Sprintf("%dh %dm %ds", h, m, s)
	}
	if m > 0 {
		return fmt.Sprintf("%dm %ds", m, s)
	}
	return fmt.Sprintf("%ds", s)
}

func (s *DownloadStatus) formatProgressText(phase string, done, total int64, percent int) string {
	userIdent := ""
	if s.username != "" {
		userIdent = fmt.Sprintf(" (by @%s)", s.username)
	} else if s.userID != 0 {
		userIdent = fmt.Sprintf(" (by ID:%d)", s.userID)
	}

	header := fmt.Sprintf("📥 Task ID: %s%s\n╭ Status: %s", s.taskID, userIdent, phase)
	modeName := formatUploadMode(s.mode)

	// Total elapsed time since the task started
	totalElapsedStr := "-"
	if !s.startedAt.IsZero() {
		totalElapsedStr = formatDuration(time.Since(s.startedAt))
	}

	// Live rate over the trailing speedWindow — a current-speed readout that reacts
	// within a few seconds, rather than a cumulative average smeared over the phase.
	speedStr := "-"
	etaStr := "-"
	if speedBytesPerSec := s.rollingSpeedBytesPerSec(); speedBytesPerSec > 0 {
		speedStr = fmt.Sprintf("%s/s", formatBytes(int64(speedBytesPerSec)))
		if total > done {
			etaSecs := float64(total-done) / speedBytesPerSec
			etaDuration := time.Duration(etaSecs) * time.Second
			etaStr = formatDuration(etaDuration)
		}
	}

	if total > 0 {
		if percent < 0 {
			percent = 0
		}
		if percent > 100 {
			percent = 100
		}
		barLength := 15
		filledCount := (percent * barLength) / 100
		var bar string
		for i := 0; i < barLength; i++ {
			if i < filledCount {
				bar += "█"
			} else if i == filledCount {
				bar += "▒"
			} else {
				bar += "░"
			}
		}

		// Progress bar wrapped in monospace format
		barFormatted := fmt.Sprintf("`[%s]`", bar)

		return fmt.Sprintf("%s\n├ %s %d%%\n├ Progress: %s / %s\n├ Speed: %s\n├ ETA (Elapsed): %s (%s)\n├ Mode: %s\n╰ Stop: /stop_%s",
			header, barFormatted, percent, formatBytes(done), formatBytes(total), speedStr, etaStr, totalElapsedStr, modeName, s.taskID)
	}

	if done > 0 {
		return fmt.Sprintf("%s\n├ Processed: %s\n├ Speed: %s\n├ Elapsed: %s\n├ Mode: %s\n╰ Stop: /stop_%s",
			header, formatBytes(done), speedStr, totalElapsedStr, modeName, s.taskID)
	}

	return fmt.Sprintf("%s\n├ Elapsed: %s\n├ Mode: %s\n╰ Stop: /stop_%s",
		header, totalElapsedStr, modeName, s.taskID)
}

func formatBytes(value int64) string {
	if value < 1024 {
		return fmt.Sprintf("%dB", value)
	}
	units := []string{"B", "KB", "MB", "GB", "TB"}
	size := float64(value)
	unitIndex := 0
	for size >= 1024 && unitIndex < len(units)-1 {
		size /= 1024
		unitIndex++
	}
	precision := 1
	if unitIndex >= 2 {
		precision = 2
	}
	return fmt.Sprintf("%.*f%s", precision, size, units[unitIndex])
}

func calcBitrateKbps(sizeBytes int64, durationMillis int64) float64 {
	if sizeBytes <= 0 || durationMillis <= 0 {
		return 0
	}
	seconds := float64(durationMillis) / 1000.0
	if seconds <= 0 {
		return 0
	}
	return (float64(sizeBytes) * 8.0) / (seconds * 1000.0)
}

func formatTelegramCaption(sizeBytes int64, bitrateKbps float64, format string) string {
	return ""
}

func findCoverFile(dir string) string {
	candidates := []string{
		"cover.jpg",
		"cover.png",
		"folder.jpg",
		"folder.png",
	}
	for _, name := range candidates {
		path := filepath.Join(dir, name)
		if info, err := os.Stat(path); err == nil && !info.IsDir() {
			return path
		}
	}
	return ""
}

func makeTelegramThumb(coverPath string) (string, error) {
	tmp, err := os.CreateTemp("", "amdl-thumb-*.jpg")
	if err != nil {
		return "", err
	}
	tmpPath := tmp.Name()
	if err := tmp.Close(); err != nil {
		return "", err
	}
	args := []string{
		"-y", "-i", coverPath,
		"-vf", "scale=320:320:force_original_aspect_ratio=decrease",
		"-frames:v", "1",
		"-q:v", "5",
		tmpPath,
	}
	cmd := exec.Command(Config.FFmpegPath, args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		_ = os.Remove(tmpPath)
		return "", fmt.Errorf("ffmpeg thumb failed: %v: %s", err, strings.TrimSpace(string(output)))
	}
	if info, err := os.Stat(tmpPath); err == nil && info.Size() > 200*1024 {
		_ = os.Remove(tmpPath)
		return "", fmt.Errorf("thumb too large")
	}
	return tmpPath, nil
}

func (b *TelegramBot) compressFlacToSize(srcPath string, maxBytes int64) (string, error) {
	outPath, err := makeTempFlacPath()
	if err != nil {
		return "", err
	}
	coverPath := findCoverFile(filepath.Dir(srcPath))
	if err := runFlacCompress(srcPath, outPath, 0, 0, false, coverPath); err != nil {
		_ = os.Remove(outPath)
		return "", err
	}
	info, err := os.Stat(outPath)
	if err != nil {
		_ = os.Remove(outPath)
		return "", err
	}
	if info.Size() <= maxBytes {
		return outPath, nil
	}

	duration, err := getAudioDurationSeconds(srcPath)
	if err != nil {
		_ = os.Remove(outPath)
		return "", err
	}
	if duration <= 0 {
		_ = os.Remove(outPath)
		return "", fmt.Errorf("invalid duration for %s", filepath.Base(srcPath))
	}

	targetBitsPerSec := (float64(maxBytes) * 8.0 / duration) * 0.95
	sampleRate, channels := chooseResamplePlan(targetBitsPerSec)
	if err := runFlacCompress(srcPath, outPath, sampleRate, channels, true, coverPath); err != nil {
		_ = os.Remove(outPath)
		return "", err
	}

	info, err = os.Stat(outPath)
	if err != nil {
		_ = os.Remove(outPath)
		return "", err
	}
	if info.Size() > maxBytes {
		return "", fmt.Errorf("cannot compress below %dMB", maxBytes/1024/1024)
	}
	return outPath, nil
}

func runFlacCompress(srcPath, outPath string, sampleRate int, channels int, force16 bool, coverPath string) error {
	args := []string{"-y", "-i", srcPath}
	if coverPath != "" {
		args = append(args, "-i", coverPath)
		args = append(args,
			"-map", "0:a",
			"-map", "1:v",
			"-c:v", "mjpeg",
			"-disposition:v", "attached_pic",
		)
	} else {
		args = append(args, "-map", "0:a", "-map", "0:v?")
	}
	args = append(args,
		"-c:a", "flac",
		"-compression_level", "12",
	)
	if force16 {
		args = append(args, "-sample_fmt", "s16")
	}
	if sampleRate > 0 {
		args = append(args, "-ar", strconv.Itoa(sampleRate))
	}
	if channels > 0 {
		args = append(args, "-ac", strconv.Itoa(channels))
	}
	args = append(args, outPath)
	cmd := exec.Command(Config.FFmpegPath, args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("ffmpeg compress failed: %v: %s", err, strings.TrimSpace(string(output)))
	}
	return nil
}

func chooseResamplePlan(targetBitsPerSec float64) (int, int) {
	channels := 2
	targetRate := targetBitsPerSec / float64(16*channels)
	if targetRate < 12000 {
		channels = 1
		targetRate = targetBitsPerSec / float64(16*channels)
	}
	return pickSampleRate(targetRate), channels
}

func pickSampleRate(target float64) int {
	rates := []int{48000, 44100, 32000, 24000, 22050, 16000, 12000, 11025, 8000}
	for _, rate := range rates {
		if float64(rate) <= target {
			return rate
		}
	}
	return rates[len(rates)-1]
}

func makeTempFlacPath() (string, error) {
	tmp, err := os.CreateTemp("", "amdl-*.flac")
	if err != nil {
		return "", err
	}
	path := tmp.Name()
	if err := tmp.Close(); err != nil {
		return "", err
	}
	return path, nil
}

func getAudioDurationSeconds(path string) (float64, error) {
	if _, err := exec.LookPath("ffprobe"); err == nil {
		cmd := exec.Command("ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", path)
		out, err := cmd.Output()
		if err == nil {
			value := strings.TrimSpace(string(out))
			if value != "" {
				if secs, err := strconv.ParseFloat(value, 64); err == nil && secs > 0 {
					return secs, nil
				}
			}
		}
	}

	cmd := exec.Command(Config.FFmpegPath, "-i", path)
	out, _ := cmd.CombinedOutput()
	re := regexp.MustCompile(`Duration:\s+(\d+):(\d+):(\d+(?:\.\d+)?)`)
	match := re.FindStringSubmatch(string(out))
	if len(match) != 4 {
		return 0, fmt.Errorf("failed to read duration from ffmpeg output")
	}
	hours, _ := strconv.ParseFloat(match[1], 64)
	minutes, _ := strconv.ParseFloat(match[2], 64)
	seconds, _ := strconv.ParseFloat(match[3], 64)
	return hours*3600 + minutes*60 + seconds, nil
}

// sendPhotoWithReply sends a photo file with an optional caption, replying to a specific message.
func (b *TelegramBot) sendPhotoWithReply(chatID int64, photoPath string, caption string, replyToID int) error {
	file, err := os.Open(photoPath)
	if err != nil {
		return err
	}
	defer file.Close()

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	_ = writer.WriteField("chat_id", strconv.FormatInt(chatID, 10))
	if replyToID > 0 {
		_ = writer.WriteField("reply_to_message_id", strconv.Itoa(replyToID))
	}
	if caption != "" {
		_ = writer.WriteField("caption", caption)
	}
	part, err := writer.CreateFormFile("photo", filepath.Base(photoPath))
	if err != nil {
		return err
	}
	if _, err := io.Copy(part, file); err != nil {
		return err
	}
	_ = writer.Close()

	req, err := http.NewRequest("POST", b.apiURL("sendPhoto"), body)
	if err != nil {
		return b.sanitizeTelegramError(err)
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	resp, err := b.client.Do(req)
	if err != nil {
		return b.sanitizeTelegramError(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("telegram sendPhoto failed: %s", resp.Status)
	}
	return nil
}

// sendVideoWithReply uploads a small video file via the Bot API (used for motion
// artwork). Streams inline in Telegram's player; subject to the Bot API's 50MB cap.
func (b *TelegramBot) sendVideoWithReply(chatID int64, videoPath string, caption string, replyToID int) error {
	file, err := os.Open(videoPath)
	if err != nil {
		return err
	}
	defer file.Close()

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	_ = writer.WriteField("chat_id", strconv.FormatInt(chatID, 10))
	if replyToID > 0 {
		_ = writer.WriteField("reply_to_message_id", strconv.Itoa(replyToID))
	}
	if caption != "" {
		_ = writer.WriteField("caption", caption)
	}
	_ = writer.WriteField("supports_streaming", "true")
	part, err := writer.CreateFormFile("video", filepath.Base(videoPath))
	if err != nil {
		return err
	}
	if _, err := io.Copy(part, file); err != nil {
		return err
	}
	_ = writer.Close()

	req, err := http.NewRequest("POST", b.apiURL("sendVideo"), body)
	if err != nil {
		return b.sanitizeTelegramError(err)
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	resp, err := b.client.Do(req)
	if err != nil {
		return b.sanitizeTelegramError(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("telegram sendVideo failed: %s", resp.Status)
	}
	return nil
}

func (b *TelegramBot) sendMessage(chatID int64, text string, markup any) error {
	return b.sendMessageWithReply(chatID, text, markup, 0)
}

func (b *TelegramBot) sendMessageWithReply(chatID int64, text string, markup any, replyToID int) error {
	_, err := b.sendMessageWithReplyReturn(chatID, text, markup, replyToID)
	return err
}

func (b *TelegramBot) sendMessageWithReplyReturn(chatID int64, text string, markup any, replyToID int) (int, error) {
	payload := map[string]any{
		"chat_id": chatID,
		"text":    text,
	}
	if markup != nil {
		payload["reply_markup"] = markup
	}
	if replyToID > 0 {
		payload["reply_to_message_id"] = replyToID
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return 0, err
	}
	req, err := http.NewRequest("POST", b.apiURL("sendMessage"), bytes.NewReader(body))
	if err != nil {
		return 0, b.sanitizeTelegramError(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := b.client.Do(req)
	if err != nil {
		return 0, b.sanitizeTelegramError(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("telegram sendMessage failed: %s", resp.Status)
	}
	apiResp := sendMessageResponse{}
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		return 0, err
	}
	if !apiResp.OK {
		return 0, fmt.Errorf("telegram sendMessage error: %s", b.sanitizeTelegramText(apiResp.Description))
	}
	return apiResp.Result.MessageID, nil
}

func (b *TelegramBot) sendAudioByFileID(chatID int64, entry CachedAudio, replyToID int, trackID string) error {
	entry = b.enrichCachedAudio(trackID, entry)
	sizeBytes := entry.SizeBytes
	if sizeBytes <= 0 {
		sizeBytes = entry.FileSize
	}
	bitrateKbps := entry.BitrateKbps
	format := normalizeTelegramFormat(entry.Format)
	if format == "" {
		format = telegramFormatFlac
	}
	caption := formatTelegramCaption(sizeBytes, bitrateKbps, format)
	payload := map[string]any{
		"chat_id": chatID,
		"audio":   entry.FileID,
		"caption": caption,
	}
	if entry.Title != "" {
		payload["title"] = entry.Title
	}
	if entry.Performer != "" {
		payload["performer"] = entry.Performer
	}
	if replyToID > 0 {
		payload["reply_to_message_id"] = replyToID
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequest("POST", b.apiURL("sendAudio"), bytes.NewReader(body))
	if err != nil {
		return b.sanitizeTelegramError(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := b.client.Do(req)
	if err != nil {
		return b.sanitizeTelegramError(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		responseBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("telegram sendAudio failed: %s", b.sanitizeTelegramText(strings.TrimSpace(string(responseBody))))
	}
	apiResp := sendAudioResponse{}
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		return err
	}
	if !apiResp.OK {
		return fmt.Errorf("telegram sendAudio error: %s", b.sanitizeTelegramText(apiResp.Description))
	}
	return nil
}

func (b *TelegramBot) editInlineMessageAudio(inlineMessageID string, entry CachedAudio, trackID string) error {
	if inlineMessageID == "" {
		return nil
	}
	if entry.FileID == "" {
		return fmt.Errorf("cached audio file_id is empty")
	}
	entry = b.enrichCachedAudio(trackID, entry)
	sizeBytes := entry.SizeBytes
	if sizeBytes <= 0 {
		sizeBytes = entry.FileSize
	}
	format := normalizeTelegramFormat(entry.Format)
	if format == "" {
		format = telegramFormatFlac
	}
	media := InputMediaAudio{
		Type:    "audio",
		Media:   entry.FileID,
		Caption: formatTelegramCaption(sizeBytes, entry.BitrateKbps, format),
	}
	if entry.Title != "" {
		media.Title = entry.Title
	}
	if entry.Performer != "" {
		media.Performer = entry.Performer
	}
	payload := map[string]any{
		"inline_message_id": inlineMessageID,
		"media":             media,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequest("POST", b.apiURL("editMessageMedia"), bytes.NewReader(body))
	if err != nil {
		return b.sanitizeTelegramError(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := b.client.Do(req)
	if err != nil {
		return b.sanitizeTelegramError(err)
	}
	defer resp.Body.Close()
	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		apiResp := apiResponse{}
		if err := json.Unmarshal(responseBody, &apiResp); err == nil && apiResp.Description != "" {
			if strings.Contains(apiResp.Description, "message is not modified") {
				return nil
			}
			return fmt.Errorf("telegram editMessageMedia error: %s", b.sanitizeTelegramText(apiResp.Description))
		}
		return fmt.Errorf("telegram editMessageMedia failed: %s", b.sanitizeTelegramText(strings.TrimSpace(string(responseBody))))
	}
	apiResp := apiResponse{}
	if err := json.Unmarshal(responseBody, &apiResp); err != nil {
		return err
	}
	if !apiResp.OK {
		if strings.Contains(apiResp.Description, "message is not modified") {
			return nil
		}
		return fmt.Errorf("telegram editMessageMedia error: %s", b.sanitizeTelegramText(apiResp.Description))
	}
	return nil
}

func (b *TelegramBot) editInlineMessageText(inlineMessageID string, text string) error {
	if inlineMessageID == "" {
		return nil
	}
	payload := map[string]any{
		"inline_message_id": inlineMessageID,
		"text":              text,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequest("POST", b.apiURL("editMessageText"), bytes.NewReader(body))
	if err != nil {
		return b.sanitizeTelegramError(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := b.client.Do(req)
	if err != nil {
		return b.sanitizeTelegramError(err)
	}
	defer resp.Body.Close()
	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		apiResp := apiResponse{}
		if err := json.Unmarshal(responseBody, &apiResp); err == nil && apiResp.Description != "" {
			if strings.Contains(apiResp.Description, "message is not modified") {
				return nil
			}
			return fmt.Errorf("telegram editMessageText error: %s", b.sanitizeTelegramText(apiResp.Description))
		}
		return fmt.Errorf("telegram editMessageText failed: %s", b.sanitizeTelegramText(strings.TrimSpace(string(responseBody))))
	}
	apiResp := apiResponse{}
	if err := json.Unmarshal(responseBody, &apiResp); err != nil {
		return err
	}
	if !apiResp.OK {
		if strings.Contains(apiResp.Description, "message is not modified") {
			return nil
		}
		return fmt.Errorf("telegram editMessageText error: %s", b.sanitizeTelegramText(apiResp.Description))
	}
	return nil
}

func (b *TelegramBot) answerCallbackQuery(callbackID string) error {
	if callbackID == "" {
		return nil
	}
	payload := map[string]any{
		"callback_query_id": callbackID,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequest("POST", b.apiURL("answerCallbackQuery"), bytes.NewReader(body))
	if err != nil {
		return b.sanitizeTelegramError(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := b.client.Do(req)
	if err != nil {
		return b.sanitizeTelegramError(err)
	}
	defer resp.Body.Close()
	return nil
}

func (b *TelegramBot) answerInlineQuery(inlineQueryID string, results any, personal bool) error {
	if inlineQueryID == "" {
		return nil
	}
	payload := map[string]any{
		"inline_query_id": inlineQueryID,
		"results":         results,
		"is_personal":     personal,
		"cache_time":      0,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequest("POST", b.apiURL("answerInlineQuery"), bytes.NewReader(body))
	if err != nil {
		return b.sanitizeTelegramError(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := b.client.Do(req)
	if err != nil {
		return b.sanitizeTelegramError(err)
	}
	defer resp.Body.Close()
	return nil
}

func (b *TelegramBot) editMessageText(chatID int64, messageID int, text string, markup any) error {
	if messageID == 0 {
		return nil
	}
	payload := map[string]any{
		"chat_id":    chatID,
		"message_id": messageID,
		"text":       text,
	}
	if markup != nil {
		payload["reply_markup"] = markup
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequest("POST", b.apiURL("editMessageText"), bytes.NewReader(body))
	if err != nil {
		return b.sanitizeTelegramError(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := b.client.Do(req)
	if err != nil {
		return b.sanitizeTelegramError(err)
	}
	defer resp.Body.Close()
	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		apiResp := apiResponse{}
		if err := json.Unmarshal(responseBody, &apiResp); err == nil {
			if strings.Contains(apiResp.Description, "message is not modified") {
				return nil
			}
			if apiResp.Description != "" {
				return fmt.Errorf("telegram editMessageText error: %s", b.sanitizeTelegramText(apiResp.Description))
			}
		}
		return fmt.Errorf("telegram editMessageText failed: %s", b.sanitizeTelegramText(strings.TrimSpace(string(responseBody))))
	}
	apiResp := apiResponse{}
	if err := json.Unmarshal(responseBody, &apiResp); err != nil {
		return err
	}
	if !apiResp.OK {
		if strings.Contains(apiResp.Description, "message is not modified") {
			return nil
		}
		return fmt.Errorf("telegram editMessageText error: %s", b.sanitizeTelegramText(apiResp.Description))
	}
	return nil
}

func (b *TelegramBot) deleteMessage(chatID int64, messageID int) error {
	if messageID == 0 {
		return nil
	}
	payload := map[string]any{
		"chat_id":    chatID,
		"message_id": messageID,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequest("POST", b.apiURL("deleteMessage"), bytes.NewReader(body))
	if err != nil {
		return b.sanitizeTelegramError(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := b.client.Do(req)
	if err != nil {
		return b.sanitizeTelegramError(err)
	}
	defer resp.Body.Close()
	return nil
}

func (b *TelegramBot) getUpdates(offset int) ([]Update, error) {
	req, err := http.NewRequest("GET", b.apiURL("getUpdates"), nil)
	if err != nil {
		return nil, b.sanitizeTelegramError(err)
	}
	query := req.URL.Query()
	query.Set("timeout", "30")
	query.Set("allowed_updates", `["message","callback_query","inline_query","chosen_inline_result"]`)
	if offset > 0 {
		query.Set("offset", strconv.Itoa(offset))
	}
	req.URL.RawQuery = query.Encode()
	resp, err := b.client.Do(req)
	if err != nil {
		return nil, b.sanitizeTelegramError(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("getUpdates failed: %s", resp.Status)
	}
	var data getUpdatesResponse
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return nil, err
	}
	if !data.OK {
		return nil, fmt.Errorf("getUpdates error: %s", b.sanitizeTelegramText(data.Description))
	}
	return data.Result, nil
}

func (b *TelegramBot) apiURL(method string) string {
	return fmt.Sprintf("%s/bot%s/%s", b.apiBase, b.token, method)
}

func (b *TelegramBot) isAllowedChat(chatID int64) bool {
	if len(b.allowedChats) == 0 {
		return false
	}
	return b.allowedChats[chatID]
}

func (b *TelegramBot) setPending(chatID int64, kind string, query string, offset int, items []apputils.SearchResultItem, hasNext bool, replyToID int, resultsMessageID int, title string) {
	b.pendingMu.Lock()
	defer b.pendingMu.Unlock()
	b.pending[chatID] = &PendingSelection{
		Kind:             kind,
		Query:            query,
		Title:            title,
		Offset:           offset,
		HasNext:          hasNext,
		Items:            items,
		CreatedAt:        time.Now(),
		ReplyToMessageID: replyToID,
		ResultsMessageID: resultsMessageID,
	}
}

func (b *TelegramBot) getPending(chatID int64) (*PendingSelection, bool) {
	b.pendingMu.Lock()
	defer b.pendingMu.Unlock()
	pending, ok := b.pending[chatID]
	return pending, ok
}

func (b *TelegramBot) clearPending(chatID int64) {
	b.pendingMu.Lock()
	defer b.pendingMu.Unlock()
	delete(b.pending, chatID)
}

func (b *TelegramBot) setPendingTransfer(chatID int64, albumID string, replyToID int, messageID int) {
	b.transferMu.Lock()
	defer b.transferMu.Unlock()
	b.pendingTransfers[chatID] = &PendingTransfer{
		AlbumID:          albumID,
		ReplyToMessageID: replyToID,
		MessageID:        messageID,
		CreatedAt:        time.Now(),
	}
}

func (b *TelegramBot) getPendingTransfer(chatID int64) (*PendingTransfer, bool) {
	b.transferMu.Lock()
	defer b.transferMu.Unlock()
	pending, ok := b.pendingTransfers[chatID]
	return pending, ok
}

func (b *TelegramBot) clearPendingTransfer(chatID int64) {
	b.transferMu.Lock()
	defer b.transferMu.Unlock()
	delete(b.pendingTransfers, chatID)
}

func parseCommand(text string) (string, []string, bool) {
	if !strings.HasPrefix(text, "/") {
		return "", nil, false
	}
	parts := strings.Fields(text)
	if len(parts) == 0 {
		return "", nil, false
	}
	cmd := strings.TrimPrefix(parts[0], "/")
	if idx := strings.Index(cmd, "@"); idx != -1 {
		cmd = cmd[:idx]
	}
	return strings.ToLower(cmd), parts[1:], true
}

func normalizeInlineSongSearchTerm(query string) string {
	fields := strings.Fields(strings.TrimSpace(query))
	if len(fields) < 2 {
		return strings.TrimSpace(query)
	}
	cmd := strings.TrimPrefix(strings.ToLower(fields[0]), "/")
	switch cmd {
	case "search_song", "serach_song":
		return strings.Join(fields[1:], " ")
	default:
		return strings.TrimSpace(query)
	}
}

func inlineSearchResultID(kind string, itemID string, index int) string {
	return fmt.Sprintf("%s:%s:%d", kind, itemID, index)
}

func songIDFromInlineResultID(resultID string) string {
	parts := strings.Split(resultID, ":")
	if len(parts) < 2 || parts[0] != "song" {
		return ""
	}
	return strings.TrimSpace(parts[1])
}

func inlineSearchTitle(item apputils.SearchResultItem) string {
	title := strings.TrimSpace(item.Name)
	switch strings.ToLower(item.ContentRating) {
	case "explicit":
		title = "[E] " + title
	case "clean":
		title = "[C] " + title
	}
	return title
}

func inlineSearchMessageText(kind string, item apputils.SearchResultItem) string {
	switch kind {
	case "song":
		title := inlineSearchTitle(item)
		if title == "" {
			return strings.TrimSpace(item.Detail)
		}
		if item.Detail != "" {
			return title + "\n" + item.Detail
		}
		return title
	case "album":
		if item.ID == "" {
			return ""
		}
		return "/albumid " + item.ID
	case "artist":
		if item.ID == "" {
			return ""
		}
		text := "/artistid " + item.ID
		if item.Name != "" {
			text += " " + item.Name
		}
		return text
	default:
		return ""
	}
}

func inlinePendingMessageText(kind string, item apputils.SearchResultItem, fallback string) string {
	if kind != "song" {
		return fallback
	}
	text := strings.TrimSpace(fallback)
	if text == "" {
		text = strings.TrimSpace(item.Name)
	}
	if text == "" {
		return "Preparing audio..."
	}
	return text + "\nPreparing audio..."
}

func inlineSearchReplyMarkup(item apputils.SearchResultItem) *InlineKeyboardMarkup {
	url := strings.TrimSpace(item.URL)
	if url == "" {
		return nil
	}
	return &InlineKeyboardMarkup{
		InlineKeyboard: [][]InlineKeyboardButton{
			{
				{
					Text: "Apple Music",
					Url:  url,
				},
			},
		},
	}
}

func buildInlineKeyboard(count int, hasPrev bool, hasNext bool) InlineKeyboardMarkup {
	rowSize := 4
	rows := [][]InlineKeyboardButton{}
	row := []InlineKeyboardButton{}
	for i := 1; i <= count; i++ {
		row = append(row, InlineKeyboardButton{
			Text:         strconv.Itoa(i),
			CallbackData: fmt.Sprintf("sel:%d", i),
		})
		if len(row) == rowSize {
			rows = append(rows, row)
			row = []InlineKeyboardButton{}
		}
	}
	if len(row) > 0 {
		rows = append(rows, row)
	}
	navRow := []InlineKeyboardButton{}
	if hasPrev {
		navRow = append(navRow, InlineKeyboardButton{
			Text:         "Prev",
			CallbackData: "page:-1",
		})
	}
	if hasNext {
		navRow = append(navRow, InlineKeyboardButton{
			Text:         "Next",
			CallbackData: "page:1",
		})
	}
	if len(navRow) > 0 {
		rows = append(rows, navRow)
	}
	return InlineKeyboardMarkup{
		InlineKeyboard: rows,
	}
}


// buildCoverCaption creates a caption for the album cover photo using track metadata.
func buildCoverCaption(paths []string) string {
	if len(paths) == 0 {
		return ""
	}
	artist := ""
	albumName := ""
	releaseDate := ""
	contentRating := ""
	quality := ""
	codec := ""

	downloadedMetaMu.Lock()
	for _, p := range paths {
		if meta, ok := downloadedMeta[p]; ok {
			if artist == "" && meta.Performer != "" {
				artist = meta.Performer
			}
			if albumName == "" && meta.AlbumName != "" {
				albumName = meta.AlbumName
			}
			if releaseDate == "" && meta.ReleaseDate != "" {
				releaseDate = meta.ReleaseDate
			}
			if contentRating == "" && meta.ContentRating != "" {
				contentRating = meta.ContentRating
			}
			if quality == "" && meta.Quality != "" {
				quality = meta.Quality
			}
			if codec == "" && meta.Codec != "" {
				codec = meta.Codec
			}
		}
	}
	downloadedMetaMu.Unlock()

	if albumName == "" {
		albumName = filepath.Base(filepath.Dir(paths[0]))
		if albumName == "." || albumName == "" {
			albumName = "Unknown"
		}
	}

	qualityDisplay := formatQualityDisplay(quality, codec)

	explicit := "False"
	if contentRating == "explicit" {
		explicit = "True"
	}

	return fmt.Sprintf(
		"Artist : %s\nAlbum : %s\nRelease Date : %s\nTotal Tracks : %d\nQuality : %s\nExplicit : %s",
		artist,
		albumName,
		releaseDate,
		len(paths),
		qualityDisplay,
		explicit,
	)
}

func formatQualityDisplay(quality string, codec string) string {
	if quality == "" {
		if codec != "" {
			return codec
		}
		return "Unknown"
	}
	// ALAC quality format: "24B-48.0kHz" → "24Bit - 48kHz"
	re := regexp.MustCompile(`^(\d+)B-(\d+(?:\.\d+)?)kHz$`)
	if m := re.FindStringSubmatch(quality); len(m) == 3 {
		sampleRate := m[2]
		if strings.HasSuffix(sampleRate, ".0") {
			sampleRate = strings.TrimSuffix(sampleRate, ".0")
		}
		return fmt.Sprintf("%sBit - %skHz", m[1], sampleRate)
	}
	return quality
}

func buildTransferKeyboard(mtprotoReady bool) InlineKeyboardMarkup {
	var row1 []InlineKeyboardButton
	row1 = append(row1, InlineKeyboardButton{Text: "🎵 Telegram", CallbackData: "transfer:tg_individual"})
	if mtprotoReady {
		row1 = append(row1, InlineKeyboardButton{Text: "📦 Telegram (ZIP)", CallbackData: "transfer:tg_zip"})
	}

	return InlineKeyboardMarkup{
		InlineKeyboard: [][]InlineKeyboardButton{
			row1,
			{
				{Text: "📁 Gofile (ZIP)", CallbackData: "transfer:gofile_zip"},
			},
			{
				{Text: "❌ Cancel", CallbackData: "transfer:cancel"},
			},
		},
	}
}

// buildMvTransferKeyboard offers the two delivery targets that make sense for a single
// music video: a native Telegram video (with document/Gofile fallback) or a direct Gofile
// link. ZIP is omitted — there's nothing to bundle. Reuses the standard transfer callbacks
// so the existing dispatcher routes them to handleTransferMode.
func buildMvTransferKeyboard() InlineKeyboardMarkup {
	return InlineKeyboardMarkup{
		InlineKeyboard: [][]InlineKeyboardButton{
			{
				{Text: "🎬 Telegram", CallbackData: "transfer:tg_individual"},
				{Text: "📁 Gofile", CallbackData: "transfer:gofile_zip"},
			},
			{
				{Text: "❌ Cancel", CallbackData: "transfer:cancel"},
			},
		},
	}
}

func buildSettingsKeyboard(current string) InlineKeyboardMarkup {
	current = normalizeTelegramFormat(current)
	if current == "" {
		current = defaultTelegramFormat
	}
	alacText := "ALAC"
	flacText := "FLAC"
	if current == telegramFormatAlac {
		alacText = "ALAC (current)"
	} else if current == telegramFormatFlac {
		flacText = "FLAC (current)"
	}
	return InlineKeyboardMarkup{
		InlineKeyboard: [][]InlineKeyboardButton{
			{
				{Text: alacText, CallbackData: "setting:alac"},
				{Text: flacText, CallbackData: "setting:flac"},
			},
		},
	}
}

// cancelTask cancels a task by its ID.
func (b *TelegramBot) cancelTask(chatID int64, taskID string, replyToID int) {
	if taskID == "" {
		_ = b.sendMessageWithReply(chatID, "Usage: /stop_<task_id>", nil, replyToID)
		return
	}
	b.queueMu.Lock()
	// Check active task
	if b.activeReq != nil && b.activeReq.taskID == taskID {
		if b.activeReq.cancel != nil {
			b.activeReq.cancel()
		}
		b.queueMu.Unlock()
		_ = b.sendMessageWithReply(chatID, fmt.Sprintf("⛔ Task %s stopped.", taskID), nil, replyToID)
		return
	}
	// Check queued tasks — drain and re-enqueue without the target
	found := false
	queueLen := len(b.downloadQueue)
	for i := 0; i < queueLen; i++ {
		select {
		case req := <-b.downloadQueue:
			if req.taskID == taskID {
				if req.cancel != nil {
					req.cancel()
				}
				b.removeQueuedReqLocked(req.taskID)
				found = true
			} else {
				b.downloadQueue <- req
			}
		default:
			break
		}
	}
	b.queueMu.Unlock()
	if found {
		_ = b.sendMessageWithReply(chatID, fmt.Sprintf("⛔ Queued task %s stopped.", taskID), nil, replyToID)
	} else {
		_ = b.sendMessageWithReply(chatID, fmt.Sprintf("Task %s not found.", taskID), nil, replyToID)
	}
}
func botHelpText() string {
	return strings.TrimSpace(`
Commands:
/dl <apple-music-link> [-aac|-atmos]  download a song, album, or playlist
/status or /queue                     show active task and queue count
/help                                 show this message

Flags:
  -aac       download in AAC-LC format
  -atmos     download in Dolby Atmos format

Inline:
@bot <keywords>            search songs
`)
}
