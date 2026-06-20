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
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	apputils "main/utils"
	"main/utils/ampapi"
	"main/utils/structs"
	"main/utils/task"
)

const (
	defaultSearchLimit           = 8
	defaultQueueSize             = 20
	pendingTTL                   = 10 * time.Minute
	defaultTelegramFormat        = "alac"
	defaultTelegramDownloadMaxGB = 3
	defaultTelegramTimeoutSecs   = 3600
	// downloadPurgeInterval is how often the background routine wipes the local
	// download cache folders. This is a hard time-based purge (separate from the
	// size-threshold cleanupDownloadsIfNeeded) so the disk never creeps over time.
	downloadPurgeInterval = 12 * time.Hour
)

const (
	telegramFormatAlac   = "alac"
	telegramFormatFlac   = "flac"
	transferModeOneByOne = "one" // deprecated alias
	transferModeZip      = "zip" // deprecated alias
	// transferModeCancel is a sentinel set when the user hits the inline "Cancel"
	// button on the delivery-mode picker. The transfer-mode switch in
	// runTransfer then short-circuits to a "Download cancelled." edit and returns
	// without enqueueing anything.
	transferModeCancel = "cancel"

	transferModeTelegramIndividual = "tg_individual"
	transferModeTelegramZip        = "tg_zip"
	transferModeGofileZip          = "gofile_zip"
	transferModeMv                 = "mv"        // music video → native Telegram video
	transferModeMvGofile           = "mv_gofile" // music video → Gofile direct (no zip
	transferModeArt                = "art"       // artwork (cover + motion) → Telegram photo/video
)

// Outline-style status board symbols. Variation selector U+FE0E forces text
// (outline) presentation on emoji that have it; the rest are inherently
// non-colored Unicode symbols. Renders identically across iOS/Android/Desktop
// Telegram clients.
const (
	symDownload = "\u25B8"          // ▸ right-pointing small triangle
	symQueue    = "\u2261"          // ≡ identical-to (three lines)
	symSpeed    = "\u26A1\uFE0E"    // ⚡︎ outline lightning
	symElapsed  = "\u231B\uFE0E"    // ⌛︎ outline hourglass
	symActive   = "\u25B8"          // ▸ active track
	symDone     = "\u2713"          // ✓ done
	symQueued   = "\u2026"          // … queued
	symFailed   = "\u2717"          // ✗ failed
	symCancel   = "\u2715"          // ✕ cancel
	symBarFull  = "\u25B0"          // ▰ filled hex (or █)
	symBarEmpty = "\u25B1"          // ▱ empty hex
)

// Per-board limits so the rendered text never blows Telegram's 4096-char limit.
const (
	statusTrackListCap = 30 // top-N active tracks shown; rest collapse into "and N more"
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

	// username is the bot's own @username (lowercased, no @), fetched via getMe at
	// startup. Used to reject commands explicitly addressed to a different bot
	// (e.g. "/help@otherbot"). Empty if getMe failed → mention check is skipped.
	username string

	// richUnavailable latches true the first time the Bot API rejects a Rich
	// Message (Bot API 10.1+) — e.g. when pointed at a self-hosted API server
	// that predates 10.1. Once latched, all rich helpers transparently fall back
	// to plain text so we never spam the API with calls it can't serve.
	richUnavailable atomic.Bool

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

	// Task-concurrency scheduler state (guarded by queueMu; only used when
	// Config.TaskConcurrency is on). schedHeadRip/schedHeadMode describe the head
	// that is currently in its download phase, which the scheduler reads to decide
	// whether to lend. schedBorrowReq holds the single sticky borrower (nil when
	// the borrow slot is free); the slot is non-preemptive — once filled it stays
	// filled until that borrower finishes.
	schedHeadRip   *RipState
	schedHeadMode  string
	schedBorrowReq *downloadRequest
	// uploadingReqs tracks head tasks that have finished downloading and are now
	// delivering/uploading (guarded by queueMu). Under task concurrency the scheduler
	// promotes the next head into activeReq at download-done, so a head still uploading
	// is no longer activeReq, not the borrower, and gone from the queue — this map lets
	// /stop still find and cancel it. Keyed by taskID; entries are removed when the task
	// fully finishes.
	uploadingReqs map[string]*downloadRequest
	// activeBoards is the per-task render/data source for every running task, keyed
	// by taskID (guarded by queueMu). Under task concurrency the head and the sticky
	// borrower each register their own entry, so /status can resurface all of them;
	// on the serial path it only ever holds one entry. Empty when idle. Each entry
	// is a member of exactly one chatBoard (the message owner for its chat).
	activeBoards map[string]*DownloadStatus
	// chatBoards owns the single live Telegram message per chat (guarded by queueMu).
	// All of a chat's active tasks render into that one message via a single edit
	// loop, so two concurrent tasks (head + borrower) never produce two messages or
	// two edit streams. Created on the first task in a chat, removed when the last
	// one retires.
	chatBoards map[int64]*chatBoard
	// idleStatus* track the single board shown by /status when nothing is running,
	// so each /status replaces the previous one instead of stacking messages.
	idleStatusMsgID  int
	idleStatusChatID int64

	cacheMu   sync.Mutex
	cacheFile string
	cache     map[string]CachedAudio
	docCache  map[string]CachedDocument

	// admins is built once from Config.TelegramAdminIDs and is immutable after
	// construction (no lock needed to read).
	admins map[int64]bool

	// stateMu guards adminLock + scheduledJobs and the telegram-state.json file.
	// It is a LEAF lock: never acquire queueMu while holding it, and never hold
	// it across an enqueue*/cancel/purge call (those take queueMu).
	stateMu       sync.Mutex
	stateFile     string               // admin lock + user profiles (DM-backed-up daily)
	scheduleFile  string               // pending sleeptime rips (persisted, NOT backed up)
	adminLock     bool                 // true => only admins may use the bot (persisted)
	scheduledJobs []*scheduledJob      // pending sleeptime rips (persisted)
	userPrefs     map[int64]*UserPrefs // saved per-user rip profiles (persisted, keyed by user ID)

	// profileOwners tracks who opened each live /profile panel ("chatID:messageID"
	// → userID) so only that user may operate its buttons. Guarded by profileMu.
	profileMu     sync.Mutex
	profileOwners map[string]int64
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
	// UserID is the Telegram user who issued the search; only they may click the
	// selection/paging buttons. Zero means "unknown owner" (legacy path) → unguarded.
	UserID int64
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
	NoCache          bool
	ReplyToMessageID int
	MessageID        int
	CreatedAt        time.Time
	// UserID is the Telegram user who initiated the download; only they may pick
	// the transfer mode. Zero means "unknown owner" (legacy path) → unguarded.
	UserID int64
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
	noCache         bool
	fn              func(ctx context.Context) error
	after           func()
	ctx             context.Context
	cancel          context.CancelFunc
	statusMessageID int
	startedAt       time.Time

	// Task-concurrency scheduling (only set when Config.TaskConcurrency is on).
	// rip is the per-rip state the scheduler builds up front so it can read this
	// task's live remaining-track count and hand a borrower its wrapper budget;
	// runDownload uses it instead of building its own. onDownloadComplete fires
	// once, the moment the download phase ends (before delivery), so the scheduler
	// can free the head slot and promote the next task while this one uploads.
	rip                *RipState
	onDownloadComplete func()
	// peekedTracks caches this request's track count from the scheduler's lend
	// check so a large, ineligible front isn't re-fetched on every tick. 0 means
	// "not yet peeked"; -1 means "peek failed / not a track album".
	peekedTracks int
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
	Style                        string  `json:"style,omitempty"`
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
	bot.fetchUsername()
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
	admins := make(map[int64]bool)
	for _, id := range Config.TelegramAdminIDs {
		admins[id] = true
	}
	searchLimit := Config.TelegramSearchLimit
	if searchLimit <= 0 {
		searchLimit = defaultSearchLimit
	}
	maxFileBytes := int64(Config.TelegramMaxFileMB) * 1024 * 1024
	if maxFileBytes <= 0 {
		maxFileBytes = 50 * 1024 * 1024
	}
	// All mutable JSON state lives together in the bind-mounted state/ directory
	// (see docker-compose.yml) so atomic tmp+rename saves actually persist. The
	// cache file defaults into that dir; the state + schedule files are derived
	// from its directory so an explicit telegram-cache-file override keeps them
	// colocated.
	cacheFile := strings.TrimSpace(Config.TelegramCacheFile)
	if cacheFile == "" {
		cacheFile = "state/telegram-cache.json"
	}
	stateDir := filepath.Dir(cacheFile)
	stateFile := "telegram-state.json"
	scheduleFile := "telegram-schedule.json"
	if stateDir != "." && stateDir != "" {
		stateFile = filepath.Join(stateDir, "telegram-state.json")
		scheduleFile = filepath.Join(stateDir, "telegram-schedule.json")
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
		activeBoards:     make(map[string]*DownloadStatus),
		chatBoards:       make(map[int64]*chatBoard),
		uploadingReqs:    make(map[string]*downloadRequest),
		cacheFile:        cacheFile,
		cache:            make(map[string]CachedAudio),
		docCache:         make(map[string]CachedDocument),
		admins:           admins,
		stateFile:        stateFile,
		scheduleFile:     scheduleFile,
	}
	bot.loadCache()
	bot.loadState()
	bot.loadSchedule()
	bot.startDownloadWorker()
	bot.startPurgeRoutine()
	bot.startScheduler()
	bot.startBackupRoutine()
	return bot
}

// startPurgeRoutine launches a background ticker that wipes the local download
// cache folders every downloadPurgeInterval. It's a safety net so finished
// downloads never accumulate on disk between the size-based cleanups.
func (b *TelegramBot) startPurgeRoutine() {
	go func() {
		ticker := time.NewTicker(downloadPurgeInterval)
		defer ticker.Stop()
		for range ticker.C {
			b.purgeDownloadCaches()
		}
	}()
}

// purgeDownloadCaches empties every configured save folder (ALAC / Atmos / AAC).
// It skips the cycle entirely while a download is in progress so it can never
// yank files out from under an active transfer, and it removes only the folders'
// contents — not the folders themselves — so the volume mounts stay valid.
func (b *TelegramBot) purgeDownloadCaches() {
	b.queueMu.Lock()
	busy := b.inProgress
	b.queueMu.Unlock()
	if busy {
		fmt.Println("Scheduled download purge skipped: a transfer is in progress.")
		return
	}

	cacheFile := ""
	if cf := strings.TrimSpace(Config.TelegramCacheFile); cf != "" {
		cacheFile = filepath.Clean(cf)
	}

	seen := make(map[string]bool)
	for _, root := range []string{Config.AlacSaveFolder, Config.AtmosSaveFolder, Config.AacSaveFolder} {
		root = strings.TrimSpace(root)
		if root == "" {
			continue
		}
		cleanRoot := filepath.Clean(root)
		// Guard against wiping the working dir or filesystem root if a folder is
		// misconfigured (mirrors cleanupDownloadsIfNeeded's safety check).
		if cleanRoot == "." || cleanRoot == string(filepath.Separator) {
			fmt.Printf("Skip purge for unsafe download folder: %s\n", root)
			continue
		}
		if seen[cleanRoot] {
			continue
		}
		seen[cleanRoot] = true
		b.purgeFolderContents(cleanRoot, cacheFile)
	}
}

// purgeFolderContents deletes every entry inside dir (keeping dir itself),
// skipping the Telegram cache file in case it lives under a download root.
func (b *TelegramBot) purgeFolderContents(dir, cacheFile string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if !os.IsNotExist(err) {
			fmt.Printf("Purge scan failed for %s: %v\n", dir, err)
		}
		return
	}
	removed := 0
	for _, entry := range entries {
		p := filepath.Join(dir, entry.Name())
		if cacheFile != "" && filepath.Clean(p) == cacheFile {
			continue
		}
		if err := os.RemoveAll(p); err != nil {
			fmt.Printf("Purge failed to remove %s: %v\n", p, err)
			continue
		}
		removed++
	}
	if removed > 0 {
		fmt.Printf("Purged %d item(s) from %s\n", removed, dir)
	}
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
	if Config.TaskConcurrency {
		go b.scheduleDownloads()
		return
	}
	// Strictly serial worker — one rip at a time, download then upload inline.
	// This is the historical behavior, kept byte-identical for the flag-off path.
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

// scheduleDownloads is the task-concurrency worker (Config.TaskConcurrency on).
// A single scheduler goroutine promotes one head at a time from the queue and,
// while that head is downloading, may lend part of the wrapper pool to one
// sticky borrower pulled from the front of the queue. The head runs with the
// full pool; its delivery (upload) runs detached so the next head is promoted
// the moment the head's *download* finishes. Telegram uploads are serialized by
// the upload gate (Phase 1); Gofile uploads stay concurrent.
func (b *TelegramBot) scheduleDownloads() {
	for req := range b.downloadQueue {
		headDone := b.startTask(req, false, 0)

		// While the head downloads, periodically try to launch one borrower.
		ticker := time.NewTicker(2 * time.Second)
	wait:
		for {
			select {
			case <-headDone:
				break wait
			case <-ticker.C:
				b.tryLendToBorrower()
			}
		}
		ticker.Stop()
		// Head download finished. Loop to promote the next head; the head's
		// delivery and any borrower keep running in their own goroutines.
	}
}

// startTask builds the per-rip state, registers the task as head or borrower,
// and launches its rip goroutine. It returns a channel closed once the task's
// download phase completes (used by the scheduler to gate head promotion). For a
// borrower, budget is the granted wrapper count k; for a head, budget 0 means
// the full pool.
func (b *TelegramBot) startTask(req *downloadRequest, borrower bool, budget int) <-chan struct{} {
	rip := newRipState()
	rip.NoCache = req.noCache
	rip.Song = req.single
	rip.WrapperBudget = budget
	if req.userID != 0 && b.hasPrefs(req.userID) {
		p := b.getPrefs(req.userID)
		rip.Prefs = &p
	}
	req.rip = rip

	done := make(chan struct{})
	var once sync.Once
	req.onDownloadComplete = func() {
		once.Do(func() {
			// The download phase is done; the task now moves to delivery/upload and is
			// no longer activeReq. Register it so /stop can still cancel it mid-upload.
			b.queueMu.Lock()
			b.uploadingReqs[req.taskID] = req
			b.queueMu.Unlock()
			close(done)
		})
	}

	b.queueMu.Lock()
	req.startedAt = time.Now()
	b.removeQueuedReqLocked(req.taskID)
	if borrower {
		b.schedBorrowReq = req
	} else {
		b.inProgress = true
		b.activeReq = req
		b.schedHeadRip = rip
		b.schedHeadMode = req.transferMode
	}
	b.queueMu.Unlock()

	go func() {
		b.runDownload(req)
		if req.after != nil {
			req.after()
		}
		// Guarantee the head channel is closed even if the rip returned before the
		// download phase reported completion (e.g. an early error path).
		req.onDownloadComplete()

		b.queueMu.Lock()
		delete(b.uploadingReqs, req.taskID)
		if borrower {
			if b.schedBorrowReq == req {
				b.schedBorrowReq = nil
			}
		} else {
			// Only clear head state if a newer head hasn't already taken over (the
			// next head is promoted at download-done, while this one still uploads).
			if b.activeReq == req {
				b.activeReq = nil
				b.inProgress = false
			}
			if b.schedHeadRip == rip {
				b.schedHeadRip = nil
				b.schedHeadMode = ""
			}
		}
		if req.userID != 0 && b.userTaskCount[req.userID] > 0 {
			b.userTaskCount[req.userID]--
		}
		b.queueMu.Unlock()
	}()

	return done
}

// tryLendToBorrower checks the lend rules and, if met, promotes the front queued
// task to the single sticky borrower slot. Rules (all required):
//   - the borrow slot is free (non-preemptive: a held slot is never reassigned),
//   - the head is a Telegram delivery whose remaining tracks exceed the threshold,
//   - the front queued task is a Gofile rip whose total tracks are under the max.
//
// The lend amount k scales with the borrower's size (small → 1, larger → 2).
func (b *TelegramBot) tryLendToBorrower() {
	b.queueMu.Lock()
	if b.schedBorrowReq != nil { // slot occupied — non-preemptive
		b.queueMu.Unlock()
		return
	}
	headRip := b.schedHeadRip
	headMode := b.schedHeadMode
	// Peek the front of the queue via the display mirror, which tracks the channel
	// order in lockstep. We only consume it from the channel once it qualifies.
	var cand *downloadRequest
	if len(b.queuedReqs) > 0 {
		cand = b.queuedReqs[0]
	}
	b.queueMu.Unlock()

	if headRip == nil || cand == nil {
		return
	}
	if !isTelegramDeliveryMode(headMode) || headRip.remainingTracks() <= Config.LendHeadRemainingThreshold {
		return
	}
	if cand.transferMode != transferModeGofileZip {
		return // borrower must be a Gofile rip; don't skip ahead of it for head order
	}
	// Track count, cached on the request so an ineligible front isn't re-fetched
	// every tick.
	count := cand.peekedTracks
	if count == 0 {
		if c, ok := b.peekTrackCount(cand); ok {
			count = c
		} else {
			count = -1
		}
		cand.peekedTracks = count
	}
	if count <= 0 || count >= Config.BorrowerMaxTracks {
		return
	}
	k := 1
	if count > Config.BorrowerMaxTracks/2 {
		k = 2
	}

	// Qualified — consume the candidate from the channel under the lock (so we
	// don't race /stop's queue drain), re-validating that the slot is still free
	// and cand is still the front, then launch it as the borrower.
	b.queueMu.Lock()
	if b.schedBorrowReq != nil || len(b.queuedReqs) == 0 || b.queuedReqs[0].taskID != cand.taskID {
		b.queueMu.Unlock()
		return
	}
	var got *downloadRequest
	select {
	case got = <-b.downloadQueue:
	default:
		b.queueMu.Unlock()
		return
	}
	if got.taskID != cand.taskID {
		// Ordering drifted (should not happen with a single consumer); put it back
		// and bail rather than borrow the wrong task.
		b.downloadQueue <- got
		b.queueMu.Unlock()
		return
	}
	b.queueMu.Unlock()
	fmt.Printf("task-concurrency: lending %d wrapper(s) to borrower %s (%d tracks); head %s has %d tracks remaining\n",
		k, got.taskID, count, headMode, headRip.remainingTracks())
	b.startTask(got, true, k)
}

// isTelegramDeliveryMode reports whether mode delivers tracks as Telegram audio
// (individual or zip) — the only head modes eligible to lend pool to a borrower.
func isTelegramDeliveryMode(mode string) bool {
	return mode == transferModeTelegramIndividual || mode == transferModeTelegramZip
}

// peekTrackCount returns the number of tracks a queued album/playlist rip will
// download, fetching its metadata. The album/playlist responses follow Apple's
// pagination, so the returned slice holds every track. Returns ok=false for
// station/MV/artwork requests (not track-album borrowers) or on fetch failure.
func (b *TelegramBot) peekTrackCount(req *downloadRequest) (int, bool) {
	id := req.albumID
	lang := b.searchLanguage()
	switch {
	case strings.HasPrefix(id, "pl."):
		resp, err := ampapi.GetPlaylistResp(Config.Storefront, id, lang, b.appleToken)
		if err != nil || resp == nil || len(resp.Data) == 0 {
			return 0, false
		}
		return len(resp.Data[0].Relationships.Tracks.Data), true
	case id != "":
		resp, err := ampapi.GetAlbumResp(Config.Storefront, id, lang, b.appleToken)
		if err != nil || resp == nil || len(resp.Data) == 0 {
			return 0, false
		}
		return len(resp.Data[0].Relationships.Tracks.Data), true
	default:
		return 0, false
	}
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
	if cmd, mention, args, ok := parseCommand(text); ok {
		// In a group, "/help@OtherBot" is addressed to a different bot — ignore it.
		// We only act on a bare command or one mentioning us. If getMe never resolved
		// our username (b.username == ""), skip the check and stay responsive.
		if mention != "" && b.username != "" && mention != b.username {
			return
		}
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
	clickerID := int64(0)
	username := ""
	if cb.From != nil {
		clickerID = cb.From.ID
		username = cb.From.Username
	}
	// Lockdown also blocks non-admins from completing in-flight button flows
	// (delivery-mode picker, paging, selection).
	if b.isLocked() && !b.isAdmin(clickerID) {
		_ = b.answerCallbackAlert(cb.ID, "The bot is currently restricted to admins.")
		return
	}
	data := strings.TrimSpace(cb.Data)
	// alert is a non-empty toast when a guarded handler rejects the click (e.g. a
	// non-owner tapping someone else's buttons); otherwise we just ack the callback.
	alert := ""
	if strings.HasPrefix(data, "sel:") {
		numStr := strings.TrimPrefix(data, "sel:")
		if n, err := strconv.Atoi(numStr); err == nil {
			alert = b.handleSelection(cb.Message.Chat.ID, cb.Message.MessageID, n, clickerID)
		}
	} else if strings.HasPrefix(data, "setting:") {
		format := strings.TrimPrefix(data, "setting:")
		if normalized := b.setChatFormat(cb.Message.Chat.ID, format); normalized != "" {
			text := fmt.Sprintf("Download format set to %s.", strings.ToUpper(normalized))
			_ = b.editMessageText(cb.Message.Chat.ID, cb.Message.MessageID, text, buildSettingsKeyboard(normalized))
		}
	} else if strings.HasPrefix(data, "album_transfer:") {
		mode := strings.TrimPrefix(data, "album_transfer:")
		alert = b.handleTransferMode(cb.Message.Chat.ID, cb.Message.MessageID, mode, username, clickerID)
	} else if strings.HasPrefix(data, "transfer:") {
		mode := strings.TrimPrefix(data, "transfer:")
		alert = b.handleTransferMode(cb.Message.Chat.ID, cb.Message.MessageID, mode, username, clickerID)
	} else if strings.HasPrefix(data, "page:") {
		deltaStr := strings.TrimPrefix(data, "page:")
		if delta, err := strconv.Atoi(deltaStr); err == nil {
			alert = b.handlePage(cb.Message.Chat.ID, cb.Message.MessageID, delta, clickerID)
		}
	} else if strings.HasPrefix(data, "pf:") {
		alert = b.handleProfileCallback(cb, data, clickerID)
	}
	if alert != "" {
		_ = b.answerCallbackAlert(cb.ID, alert)
	} else {
		_ = b.answerCallbackQuery(cb.ID)
	}
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
	// Admin lockdown: when /auth is active, non-admins can't run ANY command
	// (including /stop and /status). Admins are unaffected.
	if b.isLocked() && !b.isAdmin(userID) {
		_ = b.sendMessageWithReply(chatID, "The bot is currently restricted to admins.", nil, replyToID)
		return
	}
	if strings.HasPrefix(cmd, "stop_") {
		taskID := strings.TrimPrefix(cmd, "stop_")
		b.cancelTask(chatID, userID, taskID, replyToID)
		return
	}

	switch cmd {
	case "start", "help":
		_, _ = b.sendRichMessage(chatID, botHelpRich(), botHelpText(), nil, replyToID)
	case "profile":
		b.handleProfileCommand(chatID, userID, replyToID)
	case "status", "queue":
		// Split active boards into this chat's and others'. Boards in this chat are
		// resurfaced (each dropped + re-sent at the bottom); active tasks living in
		// other chats get a plain snapshot here without disturbing their live board.
		var mine, others []*DownloadStatus
		for _, s := range b.activeBoardsSnapshot() {
			if s.chatID == chatID {
				mine = append(mine, s)
			} else {
				others = append(others, s)
			}
		}
		switch {
		case len(mine) > 0:
			// All of this chat's tasks share one combined board; resurface it once.
			b.queueMu.Lock()
			grp := b.chatBoards[chatID]
			b.queueMu.Unlock()
			if grp != nil {
				grp.relocate(replyToID)
			} else {
				// The chat's last task retired in the window between the snapshot and
				// this lookup, so there's no live board to resurface — fall back to the
				// idle/queue board instead of producing no /status output at all.
				b.replaceIdleStatusBoard(chatID, replyToID, "📊 Karen Status Board\n\nNo active tasks."+b.queueBoardSuffix())
			}
		case len(others) > 0:
			var sb strings.Builder
			for i, s := range others {
				if i > 0 {
					sb.WriteString("\n\n")
				}
				sb.WriteString(s.RenderSnapshotBare())
			}
			sb.WriteString(b.queueBoardSuffix())
			_ = b.sendMessageWithReply(chatID, sb.String(), nil, replyToID)
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
			_ = b.sendMessageWithReply(chatID, "Usage: /dl <apple-music-link> [-aac|-atmos] [-flac] [-art] [-nc] [-tgu|-tgz|-go]", nil, replyToID)
			return
		}

		forceAAC := false
		forceAtmos := false
		forceFlac := false
		forceArt := false
		forceNoCache := false
		headlessMode := "" // -tgu / -tgz / -go: skip delivery keyboard
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
			case "-nc", "nc":
				forceNoCache = true
			case "-tgu", "tgu":
				headlessMode = transferModeTelegramIndividual
			case "-tgz", "tgz":
				headlessMode = transferModeTelegramZip
			case "-go", "go":
				headlessMode = transferModeGofileZip
			default:
				link = arg
			}
		}

		// Seed unset choices from the user's saved profile so /dl runs zero-prompt.
		// Explicit command flags ALWAYS win — this only fills what the user left
		// blank, and never mutates the stored profile (one-off override). Codec maps
		// to the force-flags; a concrete delivery target fills headlessMode, which
		// already bypasses the transfer-mode prompt for every link type below.
		if userID != 0 && b.hasPrefs(userID) {
			prefs := b.getPrefs(userID)
			if !forceAAC && !forceAtmos && !forceFlac {
				switch prefs.Codec {
				case "aac":
					forceAAC = true
				case "atmos":
					forceAtmos = true
				case "flac":
					forceFlac = true
				// "alac" / "" → default codec, no force flag needed.
				}
			}
			if headlessMode == "" {
				switch prefs.DeliveryTarget {
				case "telegram":
					headlessMode = transferModeTelegramIndividual
				case "telegram_zip":
					headlessMode = transferModeTelegramZip
				case "gofile":
					headlessMode = transferModeGofileZip
				// "ask" / "" → keep the interactive transfer prompt.
				}
			}
		}

		// -art short-circuits everything else: grab only the cover + motion artwork
		// for an album/playlist/station, ignoring any codec flags (there's no audio).
		link = resolveAppleMusicURL(link)
		if forceArt {
			b.queueDownloadArtwork(chatID, link, replyToID, userID)
			return
		}

		// Music video URLs are distinct from song/album/playlist; check first.
		mvStorefront, mvID := checkUrlMv(link)
		if mvID != "" {
			if headlessMode != "" {
				b.queueDownloadMvHeadless(chatID, userID, mvStorefront, mvID, replyToID, headlessMode, forceNoCache)
			} else {
				b.queueDownloadMvWithReply(chatID, mvStorefront, mvID, replyToID, userID, forceNoCache)
			}
			return
		}

		_, songID := checkUrlSong(link)
		if songID == "" {
			// Apple Music's "Share Song" copies an album URL with the song id in
			// the ?i= query param (…/album/name/123?i=456). Treat that as a single
			// song so we don't rip the whole album.
			if _, albumID := checkUrl(link); albumID != "" {
				songID = songIDFromURLParam(link)
			}
		}
		if songID != "" {
			if headlessMode != "" {
				format := b.resolveFormat(chatID, forceFlac)
				if forceNoCache || !b.trySendCachedTrack(chatID, replyToID, songID, format) {
					b.enqueueDownload(chatID, userID, "", replyToID, 0, true, format, headlessMode, "", forceNoCache, func(ctx context.Context) error {
						// Honor -atmos for headless single-song rips (see handleTransferMode).
						if forceAtmos {
							if rs := ripStateFrom(ctx); rs != nil {
								rs.Atmos = true
							} else {
								dl_atmos = true
							}
						}
						return ripSong(songID, b.appleToken, Config.Storefront, forceAAC, ctx)
					})
				}
			} else {
				b.queueDownloadSongWithReply(chatID, userID, songID, replyToID, forceAAC, forceAtmos, forceFlac, forceNoCache)
			}
			return
		}

		_, albumID := checkUrl(link)
		if albumID != "" {
			if headlessMode != "" {
				b.enqueueAlbumDownload(chatID, albumID, replyToID, 0, headlessMode, forceAAC, forceAtmos, forceFlac, forceNoCache, userID, "")
			} else {
				b.queueDownloadAlbumWithReply(chatID, userID, albumID, replyToID, forceAAC, forceAtmos, forceFlac, forceNoCache)
			}
			return
		}

		playlistStorefront, playlistID := checkUrlPlaylist(link)
		if playlistID != "" {
			if b.isAdmin(userID) {
				// Admins bypass the >100-track sleeptime gate entirely.
				b.dispatchPlaylistNormal(chatID, userID, playlistID, replyToID, headlessMode, forceAAC, forceAtmos, forceFlac, forceNoCache)
			} else {
				// Non-admins: count first (blocking → goroutine); >100 tracks defers
				// to the sleeptime window forced to Gofile ZIP.
				go b.routePlaylistNonAdmin(chatID, userID, playlistStorefront, playlistID, link, replyToID, headlessMode, forceAAC, forceAtmos, forceFlac, forceNoCache)
			}
			return
		}

		_, stationID := checkUrlStation(link)
		if stationID != "" {
			if headlessMode != "" {
				b.enqueueStationDownload(chatID, stationID, replyToID, 0, headlessMode, forceAAC, forceAtmos, forceFlac, forceNoCache, userID, "")
			} else {
				b.queueDownloadStationWithReply(chatID, userID, stationID, replyToID, forceAAC, forceAtmos, forceFlac, forceNoCache)
			}
			return
		}

		artistStorefront, artistID := checkUrlArtist(link)
		if artistID != "" {
			if b.isAdmin(userID) {
				// Admins bypass sleeptime gating — rip the whole discography now,
				// forced to Gofile ZIP. Blocking enumeration → goroutine.
				go b.runArtistRip(chatID, userID, "", artistStorefront, artistID, replyToID, forceAAC, forceAtmos, forceFlac)
			} else {
				go b.scheduleOrRun(&scheduledJob{
					Kind:       "artist",
					ChatID:     chatID,
					UserID:     userID,
					ReplyToID:  replyToID,
					Link:       link,
					Storefront: artistStorefront,
					ResourceID: artistID,
					ForceAAC:   forceAAC,
					ForceAtmos: forceAtmos,
					ForceFlac:  forceFlac,
				})
			}
			return
		}

		switch {
		case strings.TrimSpace(link) == "":
			_ = b.sendMessageWithReply(chatID, "No Apple Music link found. Paste a music.apple.com URL after /dl (flags like -aac/-atmos/-flac can go before or after it).", nil, replyToID)
		case !strings.Contains(link, "music.apple.com"):
			_ = b.sendMessageWithReply(chatID, "That doesn't look like an Apple Music link. Copy the share URL from the Apple Music app — it should start with music.apple.com.", nil, replyToID)
		default:
			_ = b.sendMessageWithReply(chatID, "Couldn't recognize that Apple Music link. Supported: songs, albums, playlists, stations, artists, and music videos.", nil, replyToID)
		}
	case "count":
		if len(args) == 0 {
			_ = b.sendMessageWithReply(chatID, "Usage: /count <apple-music-link>", nil, replyToID)
			return
		}
		// Blocking metadata fetches — run off the update loop.
		go b.handleCount(chatID, args[0], replyToID)
	case "auth":
		if !b.isAdmin(userID) {
			return // don't reveal the command to non-admins
		}
		b.setAdminLock(true)
		_ = b.sendMessageWithReply(chatID, "🔒 Bot restricted to admins. Use /unauth to reopen.", nil, replyToID)
	case "unauth":
		if !b.isAdmin(userID) {
			return
		}
		b.setAdminLock(false)
		_ = b.sendMessageWithReply(chatID, "🔓 Bot reopened to all allowed users.", nil, replyToID)
	case "purge":
		if !b.isAdmin(userID) {
			return
		}
		b.adminPurge(chatID, replyToID)
	default:
		// Silently ignore unknown commands
	}
}

func (b *TelegramBot) handleSearch(chatID int64, userID int64, kind string, query string, replyToID int) {
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
	b.setPending(chatID, userID, kind, query, offset, items, hasNext, replyToID, messageID, "")
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

// handleSelection processes a numbered selection button. clickerID is the user who
// tapped it; it must match the selection's owner. Returns a non-empty alert string
// when the click is rejected (shown to the clicker as a callback toast), else "".
func (b *TelegramBot) handleSelection(chatID int64, messageID int, choice int, clickerID int64) string {
	pending, ok := b.getPending(chatID)
	if !ok {
		_ = b.sendMessage(chatID, "No active selection. Start with /search_song or /search_album.", nil)
		return ""
	}
	if pending.ResultsMessageID != 0 && messageID != pending.ResultsMessageID {
		return ""
	}
	if pending.UserID != 0 && clickerID != pending.UserID {
		return "This isn't your selection."
	}
	replyToID := pending.ReplyToMessageID
	if time.Since(pending.CreatedAt) > pendingTTL {
		b.clearPending(chatID)
		_ = b.sendMessageWithReply(chatID, "Selection expired. Please search again.", nil, replyToID)
		return ""
	}
	if choice < 1 || choice > len(pending.Items) {
		_ = b.sendMessageWithReply(chatID, "Selection out of range.", nil, replyToID)
		return ""
	}
	owner := pending.UserID
	selected := pending.Items[choice-1]
	switch pending.Kind {
	case "song":
		setSearchMeta(selected.ID, selected.Name, selected.Artist)
		b.queueDownloadSongWithReply(chatID, owner, selected.ID, replyToID, false, false, false, false)
	case "album", "artist_album":
		b.queueDownloadAlbumWithReply(chatID, owner, selected.ID, replyToID, false, false, false, false)
	case "artist":
		b.showArtistAlbums(chatID, owner, selected.ID, selected.Name, replyToID)
	default:
		b.clearPending(chatID)
	}
	return ""
}

func (b *TelegramBot) showArtistAlbums(chatID int64, userID int64, artistID string, artistName string, replyToID int) {
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
	b.setPending(chatID, userID, "artist_album", artistID, 0, albums, hasNext, replyToID, messageID, artistName)
}

// handleTransferMode processes a transfer-mode button. userID is the clicker; it must
// match the download's owner. Returns a non-empty alert string when rejected (shown to
// the clicker as a callback toast), else "".
func (b *TelegramBot) handleTransferMode(chatID int64, messageID int, mode string, username string, userID int64) string {
	pending, ok := b.getPendingTransfer(chatID)
	if !ok {
		return ""
	}
	if pending.MessageID != 0 && messageID != pending.MessageID {
		return ""
	}
	// Owner check before we mutate (clear) the pending transfer, so a stranger's click
	// can't cancel the owner's prompt.
	if pending.UserID != 0 && userID != pending.UserID {
		return "This isn't your download."
	}
	if time.Since(pending.CreatedAt) > pendingTTL {
		b.clearPendingTransfer(chatID)
		_ = b.editMessageText(chatID, messageID, "Selection expired. Please request again.", nil)
		return ""
	}
	replyToID := pending.ReplyToMessageID
	b.clearPendingTransfer(chatID)

	switch mode {
	case transferModeCancel:
		_ = b.editMessageText(chatID, messageID, "Download cancelled.", nil)
		return ""
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
		b.enqueueMvDownload(chatID, userID, pending.MvStorefront, pending.MvID, replyToID, messageID, mvMode, pending.NoCache)
		return ""
	}

	if pending.Single && pending.SongID != "" {
		b.enqueueDownload(chatID, userID, username, replyToID, messageID, true, b.resolveFormat(chatID, pending.ForceFlac), mode, "", pending.NoCache, func(ctx context.Context) error {
			// Honor -atmos for single songs too. ripSong → ripAlbum reads the codec
			// from the rip state, so without this the flag was silently dropped and
			// the user got ALAC instead of the Atmos they asked for.
			if pending.ForceAtmos {
				if rs := ripStateFrom(ctx); rs != nil {
					rs.Atmos = true
				} else {
					dl_atmos = true
				}
			}
			return ripSong(pending.SongID, b.appleToken, Config.Storefront, pending.ForceAAC, ctx)
		})
	} else if pending.PlaylistID != "" {
		b.enqueuePlaylistDownload(chatID, pending.PlaylistID, replyToID, messageID, mode, pending.ForceAAC, pending.ForceAtmos, pending.ForceFlac, pending.NoCache, userID, username)
	} else if pending.StationID != "" {
		b.enqueueStationDownload(chatID, pending.StationID, replyToID, messageID, mode, pending.ForceAAC, pending.ForceAtmos, pending.ForceFlac, pending.NoCache, userID, username)
	} else if pending.AlbumID != "" {
		format := b.resolveFormat(chatID, pending.ForceFlac)
		if mode == transferModeGofileZip && !pending.NoCache {
			if b.trySendCachedAlbumZip(chatID, pending.AlbumID, replyToID, format) {
				return ""
			}
		}
		b.enqueueAlbumDownload(chatID, pending.AlbumID, replyToID, messageID, mode, pending.ForceAAC, pending.ForceAtmos, pending.ForceFlac, pending.NoCache, userID, username)
	}
	return ""
}

func (b *TelegramBot) handlePage(chatID int64, messageID int, delta int, clickerID int64) string {
	pending, ok := b.getPending(chatID)
	if !ok {
		return ""
	}
	if pending.ResultsMessageID != messageID {
		return ""
	}
	if pending.UserID != 0 && clickerID != pending.UserID {
		return "This isn't your selection."
	}
	if pending.Query == "" {
		return ""
	}
	newOffset := pending.Offset + delta*b.searchLimit
	if newOffset < 0 {
		return ""
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
			return ""
		}
		if len(items) == 0 {
			return ""
		}
		message = apputils.FormatSearchResults(pending.Kind, pending.Query, items)
	case "artist_album":
		items, hasNext, err = apputils.FetchArtistAlbums(Config.Storefront, pending.Query, b.appleToken, b.searchLimit, newOffset, b.searchLanguage())
		if err != nil {
			_ = b.editMessageText(chatID, messageID, fmt.Sprintf("Failed to load artist albums: %v", err), nil)
			return ""
		}
		if len(items) == 0 {
			return ""
		}
		message = apputils.FormatArtistAlbums(pending.Title, items)
	default:
		return ""
	}
	_ = b.editMessageText(chatID, messageID, message, buildInlineKeyboard(len(items), newOffset > 0, hasNext))
	b.setPending(chatID, pending.UserID, pending.Kind, pending.Query, newOffset, items, hasNext, pending.ReplyToMessageID, messageID, pending.Title)
	return ""
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
	b.queueDownloadSongWithReply(chatID, 0, songID, 0, false, false, false, false)
}

func (b *TelegramBot) queueDownloadSongWithReply(chatID int64, userID int64, songID string, replyToID int, forceAAC bool, forceAtmos bool, forceFlac bool, noCache bool) {
	if songID == "" {
		_ = b.sendMessage(chatID, "Song ID is empty.", nil)
		return
	}
	format := b.resolveFormat(chatID, forceFlac)
	// --no-cache bypasses the file_id cache so the track is genuinely re-ripped.
	if !noCache && b.trySendCachedTrack(chatID, replyToID, songID, format) {
		return
	}
	b.promptTransferMode(chatID, userID, "", songID, "", "", replyToID, true, forceAAC, forceAtmos, forceFlac, noCache)
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
	ok := b.enqueueDownloadWithAfter(uploadChatID, 0, "", 0, 0, true, format, transferModeOneByOne, "", false, func(ctx context.Context) error {
		return ripSong(songID, b.appleToken, Config.Storefront, false, ctx)
	}, after)
	if !ok && inlineMessageID != "" {
		_ = b.editInlineMessageText(inlineMessageID, "Download queue is full. Please try again later.")
	}
}

func (b *TelegramBot) queueDownloadAlbum(chatID int64, albumID string) {
	b.queueDownloadAlbumWithReply(chatID, 0, albumID, 0, false, false, false, false)
}

func (b *TelegramBot) queueDownloadAlbumWithReply(chatID int64, userID int64, albumID string, replyToID int, forceAAC bool, forceAtmos bool, forceFlac bool, noCache bool) {
	if albumID == "" {
		_ = b.sendMessage(chatID, "Album ID is empty.", nil)
		return
	}
	b.promptTransferMode(chatID, userID, albumID, "", "", "", replyToID, false, forceAAC, forceAtmos, forceFlac, noCache)
}

func (b *TelegramBot) queueDownloadPlaylistWithReply(chatID int64, userID int64, playlistID string, replyToID int, forceAAC bool, forceAtmos bool, forceFlac bool, noCache bool) {
	if playlistID == "" {
		_ = b.sendMessage(chatID, "Playlist ID is empty.", nil)
		return
	}
	b.promptTransferMode(chatID, userID, "", "", playlistID, "", replyToID, false, forceAAC, forceAtmos, forceFlac, noCache)
}

func (b *TelegramBot) queueDownloadStationWithReply(chatID int64, userID int64, stationID string, replyToID int, forceAAC bool, forceAtmos bool, forceFlac bool, noCache bool) {
	if stationID == "" {
		_ = b.sendMessage(chatID, "Station ID is empty.", nil)
		return
	}
	b.promptTransferMode(chatID, userID, "", "", "", stationID, replyToID, false, forceAAC, forceAtmos, forceFlac, noCache)
}

// queueDownloadMvWithReply validates the preconditions for a music-video rip and then
// prompts for the delivery target. userID is the initiator; it's stored on the pending
// transfer so only they can pick the mode via the inline button callback.
func (b *TelegramBot) queueDownloadMvWithReply(chatID int64, storefront string, mvID string, replyToID int, userID int64, noCache bool) {
	if mvID == "" {
		_ = b.sendMessage(chatID, "Music video ID is empty.", nil)
		return
	}
	if storefront == "" {
		storefront = Config.Storefront
	}
	b.promptMvTransferMode(chatID, userID, storefront, mvID, replyToID, noCache)
}

// queueDownloadMvHeadless runs the same validation as queueDownloadMvWithReply but
// skips the delivery keyboard and enqueues immediately with the supplied mode.
func (b *TelegramBot) queueDownloadMvHeadless(chatID int64, userID int64, storefront string, mvID string, replyToID int, headlessMode string, noCache bool) {
	if mvID == "" {
		_ = b.sendMessage(chatID, "Music video ID is empty.", nil)
		return
	}
	if storefront == "" {
		storefront = Config.Storefront
	}
	mvMode := transferModeMv
	if headlessMode == transferModeGofileZip {
		mvMode = transferModeMvGofile
	}
	b.enqueueMvDownload(chatID, userID, storefront, mvID, replyToID, 0, mvMode, noCache)
}

// promptMvTransferMode shows the music-video delivery keyboard and stores the pending
// MV selection; handleTransferMode picks it up on the button press.
func (b *TelegramBot) promptMvTransferMode(chatID int64, userID int64, storefront string, mvID string, replyToID int, noCache bool) {
	messageID, err := b.sendMessageWithReplyReturn(chatID, "Choose transfer method:", buildMvTransferKeyboard(), replyToID)
	if err != nil {
		return
	}
	b.transferMu.Lock()
	b.pendingTransfers[chatID] = &PendingTransfer{
		MvID:             mvID,
		MvStorefront:     storefront,
		Single:           true,
		NoCache:          noCache,
		ReplyToMessageID: replyToID,
		MessageID:        messageID,
		CreatedAt:        time.Now(),
		UserID:           userID,
	}
	b.transferMu.Unlock()
}

// enqueueMvDownload queues a music-video rip with the chosen delivery mode
// (transferModeMv for native video, transferModeMvGofile for a direct Gofile link).
func (b *TelegramBot) enqueueMvDownload(chatID int64, userID int64, storefront string, mvID string, replyToID int, statusMessageID int, transferMode string, noCache bool) {
	if storefront == "" {
		storefront = Config.Storefront
	}
	saveDir := Config.AlacSaveFolder
	if saveDir == "" {
		saveDir = "."
	}
	b.enqueueDownload(chatID, userID, "", replyToID, statusMessageID, true, "", transferMode, "", noCache, func(ctx context.Context) error {
		return mvDownloader(ctx, mvID, saveDir, b.appleToken, storefront, nil, nil)
	})
}

func (b *TelegramBot) promptTransferMode(chatID int64, userID int64, albumID string, songID string, playlistID string, stationID string, replyToID int, single bool, forceAAC bool, forceAtmos bool, forceFlac bool, noCache bool) {
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
		NoCache:          noCache,
		ReplyToMessageID: replyToID,
		MessageID:        messageID,
		CreatedAt:        time.Now(),
		UserID:           userID,
	}
	b.transferMu.Unlock()
}

func (b *TelegramBot) enqueueAlbumDownload(chatID int64, albumID string, replyToID int, statusMessageID int, transferMode string, forceAAC bool, forceAtmos bool, forceFlac bool, noCache bool, userID int64, username string) {
	if albumID == "" {
		_ = b.sendMessage(chatID, "Album ID is empty.", nil)
		return
	}
	format := b.resolveFormat(chatID, forceFlac)
	b.enqueueDownload(chatID, userID, username, replyToID, statusMessageID, false, format, transferMode, albumID, noCache, func(ctx context.Context) error {
		if forceAtmos {
			if rs := ripStateFrom(ctx); rs != nil {
				rs.Atmos = true
			} else {
				dl_atmos = true
			}
		}
		return ripAlbum(albumID, b.appleToken, Config.Storefront, "", forceAAC, ctx)
	})
}

func (b *TelegramBot) enqueuePlaylistDownload(chatID int64, playlistID string, replyToID int, statusMessageID int, transferMode string, forceAAC bool, forceAtmos bool, forceFlac bool, noCache bool, userID int64, username string) {
	if playlistID == "" {
		_ = b.sendMessage(chatID, "Playlist ID is empty.", nil)
		return
	}
	format := b.resolveFormat(chatID, forceFlac)
	b.enqueueDownload(chatID, userID, username, replyToID, statusMessageID, false, format, transferMode, playlistID, noCache, func(ctx context.Context) error {
		if forceAtmos {
			if rs := ripStateFrom(ctx); rs != nil {
				rs.Atmos = true
			} else {
				dl_atmos = true
			}
		}
		return ripPlaylist(playlistID, b.appleToken, Config.Storefront, forceAAC, ctx)
	})
}

// enqueueStationDownload queues an Apple Music radio/station rip. ripStation reads
// the codec from the rip's atmos/aac flags (it takes no forceAAC param), so we set
// them inside the closure on the per-rip state carried by ctx (falling back to the
// globals in CLI mode where ctx carries no RipState).
func (b *TelegramBot) enqueueStationDownload(chatID int64, stationID string, replyToID int, statusMessageID int, transferMode string, forceAAC bool, forceAtmos bool, forceFlac bool, noCache bool, userID int64, username string) {
	if stationID == "" {
		_ = b.sendMessage(chatID, "Station ID is empty.", nil)
		return
	}
	format := b.resolveFormat(chatID, forceFlac)
	b.enqueueDownload(chatID, userID, username, replyToID, statusMessageID, false, format, transferMode, "", noCache, func(ctx context.Context) error {
		if rs := ripStateFrom(ctx); rs != nil {
			if forceAtmos {
				rs.Atmos = true
			}
			if forceAAC {
				rs.AAC = true
			}
		} else {
			if forceAtmos {
				dl_atmos = true
			}
			if forceAAC {
				dl_aac = true
			}
		}
		return ripStation(stationID, b.appleToken, Config.Storefront, ctx)
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
	b.enqueueDownload(chatID, userID, "", replyToID, 0, true, "", transferModeArt, "", false, func(ctx context.Context) error {
		return ripArtwork(link, b.appleToken, Config.Storefront, ctx)
	})
}

func (b *TelegramBot) enqueueDownload(chatID int64, userID int64, username string, replyToID int, statusMessageID int, single bool, format string, transferMode string, albumID string, noCache bool, fn func(ctx context.Context) error) {
	_ = b.enqueueDownloadWithAfter(chatID, userID, username, replyToID, statusMessageID, single, format, transferMode, albumID, noCache, fn, nil)
}

func (b *TelegramBot) enqueueDownloadWithAfter(chatID int64, userID int64, username string, replyToID int, statusMessageID int, single bool, format string, transferMode string, albumID string, noCache bool, fn func(ctx context.Context) error, after func()) bool {
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
		noCache:         noCache,
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
	// Grab this chat's combined board (if any) under the lock.
	grp := b.chatBoards[chatID]
	b.queueMu.Unlock()

	// A new task was added. Instead of a separate "Queued" message, refresh this
	// chat's combined board — its queue section now lists this task — and resurface
	// it. If nothing is running here, the worker will create the board momentarily.
	if grp != nil {
		grp.relocate(replyToID)
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

// friendlyTaskError turns an internal error into a concise, user-facing one-liner
// for the status board. Cancellation reads as "Cancelled"; everything else is
// trimmed to a single capped line so raw ffmpeg/mp4decrypt stderr and segment URLs
// don't spill into the chat. The full error is still logged for debugging.
func friendlyTaskError(prefix string, err error) string {
	if err == nil {
		return prefix
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return "⛔ Cancelled."
	}
	msg := strings.TrimSpace(err.Error())
	if i := strings.IndexAny(msg, "\n\r"); i >= 0 {
		msg = strings.TrimSpace(msg[:i]) // first line only — drop multi-line stderr dumps
	}
	const maxLen = 180
	if len(msg) > maxLen {
		msg = msg[:maxLen] + "…"
	}
	if msg == "" {
		return "❌ " + prefix + "."
	}
	return "❌ " + prefix + ": " + msg
}

func (b *TelegramBot) runDownload(req *downloadRequest) {
	// Last-resort safety net: a panic on the single-track/song/MV path (which does
	// not go through the per-track recover in main.go) must not take the whole bot
	// down with it. The per-rip cleanup defers below still run during the unwind.
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("runDownload panic recovered (chat %d): %v\n", req.chatID, r)
		}
	}()
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

	format = normalizeTelegramFormat(format)
	if format == "" {
		format = defaultTelegramFormat
	}
	defer b.cleanupDownloadsIfNeeded()

	// Per-rip state. When task-concurrency is enabled each rip carries its own
	// RipState through ctx so concurrent rips never share the download-mode flags,
	// counter, path list, meta map, or conversion overrides. When disabled we fall
	// back to the historical package globals and the strictly serial worker, so the
	// behavior is byte-identical to before.
	var rs *RipState
	rctx := req.ctx
	if Config.TaskConcurrency {
		// The scheduler builds the rip state up front (so it can read live counts
		// and set a borrower's budget) and attaches it to the request. Fall back to
		// a fresh one for any concurrent path that didn't go through the scheduler.
		rs = req.rip
		if rs == nil {
			rs = newRipState()
			rs.NoCache = req.noCache
			rs.Song = single
			if req.userID != 0 && b.hasPrefs(req.userID) {
				p := b.getPrefs(req.userID)
				rs.Prefs = &p
			}
		}
		rctx = withRipState(req.ctx, rs)
	} else {
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
		dl_noCache = req.noCache
		dl_song = single
	}

	// Conversion overrides: written to the per-rip state when concurrent, else to the
	// global Config as before.
	convertAfter := format == telegramFormatFlac
	convertFormat := ""
	if format == telegramFormatFlac {
		convertFormat = telegramFormatFlac
		if _, err := exec.LookPath(Config.FFmpegPath); err != nil {
			_ = b.sendMessageWithReply(chatID, fmt.Sprintf("ffmpeg not found at '%s'.", Config.FFmpegPath), nil, replyToID)
			return
		}
	}
	if rs != nil {
		rs.ConvertAfterDownload = convertAfter
		rs.ConvertFormat = convertFormat
		rs.ConvertKeepOriginal = false
		rs.ConvertSkipLossyToLossless = false
	} else {
		Config.ConvertAfterDownload = convertAfter
		Config.ConvertFormat = convertFormat
		if convertAfter {
			Config.ConvertKeepOriginal = false
			Config.ConvertSkipLossyToLossless = false
		}
	}

	// Register this task into its chat's combined status board (head and sticky
	// borrower stack into one message keyed by chat), retire any idle board left by
	// a previous /status, and ensure the section is removed when this task finishes.
	status, err := b.attachBoard(chatID, replyToID, req)
	if err != nil {
		_ = b.sendMessageWithReply(chatID, fmt.Sprintf("Failed to create status message: %v", err), nil, replyToID)
		dl_song = false
		return
	}
	defer b.retireBoard(status)

	progressFactory := func(track *task.Track) apputils.ProgressFunc {
		totalTracks := track.TaskTotal
		if single {
			totalTracks = 1
		}
		// releaseTitle: for a playlist use the playlist's own name (otherwise the
		// board would show the first track's album); for albums prefer the album
		// name; fall back to the track's own name for a single-track download.
		releaseTitle := strings.TrimSpace(track.Resp.Attributes.AlbumName)
		if track.PreType == "playlists" && strings.TrimSpace(track.PlaylistData.Attributes.Name) != "" {
			releaseTitle = strings.TrimSpace(track.PlaylistData.Attributes.Name)
		}
		if releaseTitle == "" {
			releaseTitle = track.Resp.Attributes.Name
		}
		return func(phase string, done, total int64) {
			status.UpdateTrack(track.ID, track.Resp.Attributes.Name, releaseTitle, track.WorkerID, track.TaskNum, totalTracks, phase, done, total)
		}
	}
	rs.setProgressFactory(progressFactory)
	defer rs.setProgressFactory(nil)

	status.Update("Downloading", 0, 0)
	err = fn(rctx)
	// Download phase done (success or not): free the head slot so the scheduler can
	// promote the next head while this rip proceeds to deliver/upload. No-op on the
	// serial path, where onDownloadComplete is never set.
	if req.onDownloadComplete != nil {
		req.onDownloadComplete()
	}
	if err != nil {
		fmt.Printf("rip failed (chat %d, task %s): %v\n", chatID, req.taskID, err)
		status.UpdateSync(friendlyTaskError("Download failed", err), 0, 0)
		return
	}

	rs.setProgressFactory(nil)

	paths := rs.snapshotPaths()
	ctr := rs.ctr()
	if len(paths) == 0 {
		if summary := rs.failureSummary(); summary != "" {
			status.UpdateSync("No files were downloaded: "+summary, 0, 0)
			return
		}
		if ctr.Error > 0 || ctr.Unavailable > 0 {
			status.UpdateSync(fmt.Sprintf("No files were downloaded. Errors: %d, unavailable: %d.", ctr.Error, ctr.Unavailable), 0, 0)
			return
		}
		status.UpdateSync("No files were downloaded.", 0, 0)
		return
	}

	// Playlists with >40 tracks are automatically routed to Gofile regardless of the
	// chosen delivery mode — sending 40+ individual uploads triggers FloodWait hard.
	const largePlaylistThreshold = 40
	if strings.HasPrefix(req.albumID, "pl.") && len(paths) > largePlaylistThreshold &&
		transferMode != transferModeGofileZip && transferMode != transferModeMv && transferMode != transferModeMvGofile && transferMode != transferModeArt {
		status.UpdateSync(fmt.Sprintf("Large playlist (%d tracks) — routing to Gofile.", len(paths)), 0, 0)
		b.deliverGofileZip(chatID, paths, replyToID, single, status, rctx)
		return
	}

	switch transferMode {
	case transferModeMv:
		b.deliverMusicVideo(chatID, paths[0], replyToID, status, rctx)
	case transferModeMvGofile:
		b.deliverMvGofile(chatID, paths[0], replyToID, status, rctx)
	case transferModeArt:
		b.deliverArtwork(chatID, paths, replyToID, status, rctx)
	case transferModeTelegramIndividual:
		b.deliverTelegramIndividual(chatID, paths, replyToID, format, status, rctx)
	case transferModeTelegramZip:
		b.deliverTelegramZip(chatID, paths, replyToID, albumID, format, status, rctx)
	case transferModeGofileZip:
		b.deliverGofileZip(chatID, paths, replyToID, single, status, rctx)
	default:
		if transferMode == transferModeZip {
			b.deliverGofileZip(chatID, paths, replyToID, single, status, rctx)
		} else {
			b.deliverTelegramIndividual(chatID, paths, replyToID, format, status, rctx)
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
			coverCaption := buildCoverCaption(ctx, paths)
			_ = b.sendPhotoWithReply(chatID, coverPath, coverCaption, replyToID)
		}
	}

	// Music videos that land inside an album/playlist rip (e.g. a playlist that mixes
	// songs and MVs) must go out as native Telegram video, not as an audio message —
	// otherwise Telegram tags the .mp4 with an audio attribute and plays it as a song.
	// Partition them out and send the videos first; the rest go through the audio group.
	var audioPaths []string
	for _, p := range paths {
		if !isVideoFile(p) {
			audioPaths = append(audioPaths, p)
			continue
		}
		if ctx != nil && ctx.Err() != nil {
			status.UpdateSync("Cancelled", 0, 0)
			return
		}
		if err := b.sendVideoFileMTProto(chatID, p, replyToID, status, ctx); err != nil {
			if errors.Is(err, context.Canceled) {
				status.UpdateSync("Cancelled", 0, 0)
				return
			}
			// Last resort for this one file: Gofile.
			if link, gerr := apputils.UploadToGofile(ctx, p, Config.GofileToken); gerr == nil {
				_ = b.sendMessageWithReply(chatID, fmt.Sprintf("File: %s\nDownload Link: %s", filepath.Base(p), link), nil, replyToID)
				sentAny = true
			} else {
				fmt.Printf("MV delivery failed for %s: %v (gofile: %v)\n", filepath.Base(p), err, gerr)
			}
		} else {
			sentAny = true
		}
	}

	// Chunk the audio tracks into groups of up to 10
	const maxGroupSize = 10
	for i := 0; i < len(audioPaths); i += maxGroupSize {
		if ctx != nil && ctx.Err() != nil {
			status.UpdateSync("Cancelled", 0, 0)
			return
		}
		end := i + maxGroupSize
		if end > len(audioPaths) {
			end = len(audioPaths)
		}
		chunk := audioPaths[i:end]

		// Prepare AudioGroupItem slice
		var groupItems []AudioGroupItem
		for _, path := range chunk {
			meta, hasMeta := getDownloadedMeta(ctx, path)
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
		b.sendDeliverySummary(ctx, chatID, paths, format, replyToID)
	}
}

// deliverTelegramIndividualFallback sends tracks via Bot API (limited to maxFileBytes) or Gofile.
func (b *TelegramBot) deliverTelegramIndividualFallback(chatID int64, paths []string, replyToID int, format string, status *DownloadStatus, ctx context.Context) {
	sentAny := false
	var lastErr error
	// Send cover art as standalone photo with album info
	if len(paths) > 0 {
		if coverPath := findCoverFile(filepath.Dir(paths[0])); coverPath != "" {
			coverCaption := buildCoverCaption(ctx, paths)
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
			err = b.sendAudioFile(ctx, chatID, path, 0, status, format)
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
		b.sendDeliverySummary(ctx, chatID, paths, format, replyToID)
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
			b.sendDeliverySummary(ctx, chatID, paths, format, replyToID)
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
				coverCaption := buildCoverCaption(ctx, paths)
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

// isVideoFile reports whether a downloaded path is a music video (or other video)
// rather than an audio track, by extension. Used to route MVs that land inside an
// album/playlist rip to the native-video sender instead of the audio-group path.
func isVideoFile(path string) bool {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".mp4", ".m4v", ".mov":
		return true
	}
	return false
}

// sendVideoFileMTProto uploads a single video as a native Telegram video (inline player +
// thumbnail) via MTProto, falling back to a document. It does NOT resolve the status board
// or attempt Gofile — callers decide terminal handling and any further fallback. Returns nil
// once the file is delivered (as video or document), context.Canceled if cancelled mid-send,
// or the last error otherwise (including when MTProto is unavailable or the file is too large).
func (b *TelegramBot) sendVideoFileMTProto(chatID int64, path string, replyToID int, status *DownloadStatus, ctx context.Context) error {
	if ctx != nil && ctx.Err() != nil {
		return context.Canceled
	}
	if b.mtproto == nil || !b.mtproto.IsReady() {
		return fmt.Errorf("mtproto not ready")
	}

	var sizeBytes int64
	if info, err := os.Stat(path); err == nil {
		sizeBytes = info.Size()
	}
	// MTProto upload ceiling (~2GB).
	const mtprotoMaxBytes = 2 * 1000 * 1000 * 1024
	if sizeBytes > mtprotoMaxBytes {
		return fmt.Errorf("file exceeds MTProto size ceiling (%d bytes)", sizeBytes)
	}

	// Caption from metadata when available.
	caption := filepath.Base(path)
	durationSecs := 0
	if meta, ok := getDownloadedMeta(ctx, path); ok {
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

	// Append a video-quality + file-size line to the caption (best-effort; only
	// the parts we actually have are shown).
	var details []string
	if quality := videoQualityLabel(height); quality != "" {
		if width > 0 && height > 0 {
			details = append(details, fmt.Sprintf("%s (%d×%d)", quality, width, height))
		} else {
			details = append(details, quality)
		}
	}
	if sizeBytes > 0 {
		details = append(details, formatBytes(sizeBytes))
	}
	if len(details) > 0 {
		caption = fmt.Sprintf("%s\n%s", caption, strings.Join(details, " · "))
	}

	// Try native video first.
	err := b.mtproto.UploadAndSendVideo(chatID, path, caption, durationSecs, width, height, thumbPath, replyToID, status, ctx)
	if err == nil {
		return nil
	}
	if ctx != nil && ctx.Err() != nil {
		return context.Canceled
	}
	fmt.Printf("MV video upload failed: %v. Trying document...\n", err)
	status.Update(fmt.Sprintf("⚠️ Video send failed: %v. Sending as document...", err), 0, 0)

	// Fall back to sending the same file as a document.
	errDoc := b.mtproto.UploadAndSendDocument(chatID, path, filepath.Base(path), caption, replyToID, status, ctx)
	if errDoc == nil {
		return nil
	}
	if ctx != nil && ctx.Err() != nil {
		return context.Canceled
	}
	fmt.Printf("MV document upload failed: %v.\n", errDoc)
	return errDoc
}

// mvBoardTitle derives a human heading for the status board from the MV's recorded
// metadata ("Performer — Title"), falling back to the file's base name (sans
// extension) when no metadata is available. Mirrors the caption logic in
// sendVideoFileMTProto.
func mvBoardTitle(ctx context.Context, path string) string {
	if meta, ok := getDownloadedMeta(ctx, path); ok && meta.Title != "" {
		if meta.Performer != "" {
			return fmt.Sprintf("%s — %s", meta.Performer, meta.Title)
		}
		return meta.Title
	}
	return strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
}

// deliverMusicVideo sends a downloaded music video as a native Telegram video (inline
// player + thumbnail), falling back to a document and then Gofile. The status board is
// always resolved to a terminal state.
func (b *TelegramBot) deliverMusicVideo(chatID int64, path string, replyToID int, status *DownloadStatus, ctx context.Context) {
	if ctx != nil && ctx.Err() != nil {
		status.UpdateSync("Cancelled", 0, 0)
		return
	}

	// Give the board a real heading; MV progress flows through status.Update, which
	// never sets releaseTitle, so without this the board reads "Untitled".
	status.SetReleaseTitle(mvBoardTitle(ctx, path))

	var sizeBytes int64
	if info, err := os.Stat(path); err == nil {
		sizeBytes = info.Size()
	}

	// MTProto upload ceiling (~2GB). Above it, or when MTProto is down, go straight to Gofile.
	const mtprotoMaxBytes = 2 * 1000 * 1000 * 1024
	if b.mtproto != nil && b.mtproto.IsReady() && (sizeBytes == 0 || sizeBytes <= mtprotoMaxBytes) {
		err := b.sendVideoFileMTProto(chatID, path, replyToID, status, ctx)
		if err == nil {
			status.Stop()
			return
		}
		if errors.Is(err, context.Canceled) || (ctx != nil && ctx.Err() != nil) {
			status.UpdateSync("Cancelled", 0, 0)
			return
		}
		fmt.Printf("MV upload failed: %v. Falling back to Gofile...\n", err)
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
}

// deliverMvGofile uploads a single music video straight to Gofile — no Telegram attempt,
// no zip — used when the user explicitly picks Gofile at the prompt. The status board is
// always resolved to a terminal state.
func (b *TelegramBot) deliverMvGofile(chatID int64, path string, replyToID int, status *DownloadStatus, ctx context.Context) {
	if ctx != nil && ctx.Err() != nil {
		status.UpdateSync("Cancelled", 0, 0)
		return
	}
	status.SetReleaseTitle(mvBoardTitle(ctx, path))
	status.UpdateSync("Uploading to Gofile...", 0, 0)
	link, err := apputils.UploadToGofile(ctx, path, Config.GofileToken)
	if err != nil {
		b.reportDeliveryFailure(chatID, replyToID, status, err)
		return
	}
	msg := fmt.Sprintf("File: %s\nDownload Link: %s", filepath.Base(path), link)
	_ = b.sendMessageWithReply(chatID, msg, nil, replyToID)
	status.Stop()
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

	// Cover-delivery profile pref: "document" sends still images as a plain file
	// (full quality, no Telegram re-compression) instead of an inline photo.
	preferDoc := false
	if rs := ripStateFrom(ctx); rs != nil && rs.Prefs != nil && rs.Prefs.CoverDelivery == "document" {
		preferDoc = true
	}

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
		case ".jpg", ".jpeg", ".png", ".webp":
			if preferDoc {
				err = b.sendDocumentFile(chatID, p, filepath.Base(p), replyToID, nil, "")
			} else {
				err = b.sendPhotoWithReply(chatID, p, caption, replyToID)
			}
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

func (b *TelegramBot) sendAudioFile(ctx context.Context, chatID int64, filePath string, replyToID int, status *DownloadStatus, format string) error {
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
	meta, hasMeta := getDownloadedMeta(ctx, filePath)
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
			return fmt.Errorf("ALAC file exceeds the %dMB Telegram limit. Re-run /dl with -flac to compress it under the limit, or raise telegram-max-file-mb.", b.maxFileBytes/1024/1024)
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
		if path, err := makeTelegramThumb(coverPath); err != nil {
			fmt.Printf("makeTelegramThumb failed: %v\n", err)
		} else {
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
// Renders with outline-style symbols to match the active-task board.
// activeBoardsSnapshot returns the live status boards for all running tasks. The
// returned slice is a copy, so callers can iterate without holding queueMu.
func (b *TelegramBot) activeBoardsSnapshot() []*DownloadStatus {
	b.queueMu.Lock()
	defer b.queueMu.Unlock()
	if len(b.activeBoards) == 0 {
		return nil
	}
	boards := make([]*DownloadStatus, 0, len(b.activeBoards))
	for _, s := range b.activeBoards {
		boards = append(boards, s)
	}
	return boards
}

func (b *TelegramBot) queueBoardSuffix() string {
	b.queueMu.Lock()
	reqs := make([]*downloadRequest, len(b.queuedReqs))
	copy(reqs, b.queuedReqs)
	b.queueMu.Unlock()
	if len(reqs) == 0 {
		return ""
	}
	var sb strings.Builder
	fmt.Fprintf(&sb, "\n\n%s Queue · %d waiting", symQueue, len(reqs))
	for i, r := range reqs {
		user := r.username
		if user == "" {
			user = fmt.Sprintf("ID:%d", r.userID)
		}
		if user != "" && user[0] != '@' {
			user = "@" + user
		}
		fmt.Fprintf(&sb, "\n#%d %s · %s · /stop_%s",
			i+1, user, shortMode(r.transferMode), r.taskID)
	}
	return sb.String()
}

// queueBoardSuffixRich is the Rich-Markdown counterpart of queueBoardSuffix. It
// renders the waiting queue as a small heading + ordered list so the leading
// "#N" can't be mistaken for an H1 heading and pipes/underscores in usernames
// can't smuggle in formatting.
func (b *TelegramBot) queueBoardSuffixRich() string {
	b.queueMu.Lock()
	reqs := make([]*downloadRequest, len(b.queuedReqs))
	copy(reqs, b.queuedReqs)
	b.queueMu.Unlock()
	if len(reqs) == 0 {
		return ""
	}
	var sb strings.Builder
	fmt.Fprintf(&sb, "\n\n### %s Queue · %d waiting\n", symQueue, len(reqs))
	for i, r := range reqs {
		user := r.username
		if user == "" {
			user = fmt.Sprintf("ID:%d", r.userID)
		}
		if user != "" && user[0] != '@' {
			user = "@" + user
		}
		// /stop_<id> stays bare (no code span) so Telegram auto-links it as a
		// tappable command; task IDs are hex, safe from GFM emphasis.
		fmt.Fprintf(&sb, "%d. %s · %s · /stop_%s\n",
			i+1, escapeRichMD(user), escapeRichMD(shortMode(r.transferMode)), r.taskID)
	}
	return sb.String()
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

type trackProgressState struct {
	id           string
	title        string
	number       int
	total        int
	phase        string
	done         int64
	size         int64
	// maxBytes is the high-water mark of every byte count ever seen for this track
	// (max of `done` and `total` across all phases). `size` mirrors only the reported
	// `total`, which stays 0 for the HLS-segment download path and gets clobbered to
	// 0/1 by the post-download Decrypting/Converting sentinels — so it can't be trusted
	// as the final size. maxBytes survives those, giving finishedSizes a real number
	// (and keeping the upload bar's aggregate total honest) while `size` is left alone
	// so the live per-track % stays indeterminate for unknown-total downloads.
	maxBytes     int64
	startedAt    time.Time
	updatedAt    time.Time
	workerID     string
	speedSamples []progressSample
}

// speedWindow is how far back the live speed/ETA readout looks. Computing speed
// over a trailing window (rather than cumulative bytes ÷ phase-elapsed) gives a
// responsive "current rate" that reacts within a few seconds and reads on the
// same basis for a 7 MB audio file and a 300 MB video. Samples are recorded
// in-memory on every progress callback; this does NOT change how often the board
// is edited (still throttled to ≥3s in flush), so it adds zero FloodWait risk.
const speedWindow = 6 * time.Second

type DownloadStatus struct {
	bot   *TelegramBot
	group *chatBoard // chat's combined-message owner (message + edit loop)
	chatID         int64
	taskID         string
	mode           string
	startedAt      time.Time
	phaseStartedAt time.Time
	mu             sync.Mutex
	latestPhase    string
	latestDone     int64
	latestTotal    int64
	dirty          bool
	retired        bool // set once retired (idempotent); guarded by queueMu
	userID         int64
	username       string
	speedSamples   []progressSample
	tracks         map[string]trackProgressState
	finishedTracks int
	// finishedSizes retains the byte size of every track that has completed, so
	// the aggregated progress bar keeps a stable total as active tracks come and
	// go. Without this, the denominator shrinks with each finishing track and
	// done can exceed total (the "30MB / 18MB" overflow bug).
	finishedSizes []int64
	trackTotal    int
	workerLimit   int
	// releaseTitle is captured from the first track's album name (or the track
	// name itself for a single-track download). It's the user-facing heading on
	// the status board, set lazily so the task runner doesn't have to plumb it.
	releaseTitle string
	// single is true for a one-shot track download (--song / direct URL). The
	// renderer uses it to skip the per-track list and show a single-track view.
	single bool

	// Upload-phase accounting. During the "Uploading" phase the per-file byte
	// counter (latestDone/latestTotal) rewinds to 0 for each file, so it can't drive
	// a stable board on its own. uploadDoneBytes is a MONOTONIC running total of
	// bytes pushed across all files; uploadPrevDone is the previous file's counter
	// used to fold per-file deltas into it. Both reset when the upload phase begins.
	// The denominator is the aggregate track size (sum of finishedSizes), since the
	// files being uploaded are exactly the tracks that finished downloading.
	uploadDoneBytes int64
	uploadPrevDone  int64
	uploading       bool
}

// isUploadPhase reports whether a phase string denotes the Telegram upload step. The
// group path tags it "Uploading i/N"; the single/document/video paths use "Uploading".
func isUploadPhase(phase string) bool {
	return strings.HasPrefix(strings.TrimSpace(phase), "Uploading")
}

// newDownloadStatus builds the per-task render/data source. It performs no I/O —
// the owning chatBoard sends and edits the actual Telegram message. attachBoard
// wires the returned status into its chat's group.
func newDownloadStatus(bot *TelegramBot, chatID int64, req *downloadRequest) *DownloadStatus {
	status := &DownloadStatus{
		bot:            bot,
		chatID:         chatID,
		taskID:         req.taskID,
		mode:           req.transferMode,
		startedAt:      req.startedAt,
		phaseStartedAt: req.startedAt,
		userID:         req.userID,
		username:       req.username,
		tracks:         make(map[string]trackProgressState),
		finishedSizes:  nil,
		workerLimit:    len(Config.WrapperManagerAddrs),
		single:         req.single,
	}
	if status.workerLimit < 1 {
		status.workerLimit = 1
	}
	return status
}

// chatBoard owns the single live status message for one chat. Every active task in
// the chat is a member; the board renders them stacked (head first, then borrower)
// into one message and edits it from a single 3s loop, so N concurrent tasks never
// produce N messages or N edit streams (the source of the double-edit floodwait risk
// once task-concurrency could run two rips at once). Guarded by mu; the bot's
// chatBoards map is guarded by queueMu.
type chatBoard struct {
	bot        *TelegramBot
	chatID     int64
	mu         sync.Mutex
	messageID  int
	members    []*DownloadStatus
	lastText   string
	lastUpdate time.Time
	updateCh   chan struct{}
	stopCh     chan struct{}
	stopOnce   sync.Once
}

// signal nudges the loop to re-render soon (non-blocking; the loop coalesces).
func (g *chatBoard) signal() {
	if g == nil {
		return
	}
	select {
	case g.updateCh <- struct{}{}:
	default:
	}
}

func (g *chatBoard) stop() {
	if g == nil {
		return
	}
	g.stopOnce.Do(func() { close(g.stopCh) })
}

func (g *chatBoard) loop() {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-g.updateCh:
			g.flush(false)
		case <-ticker.C:
			g.flush(false)
		case <-g.stopCh:
			return
		}
	}
}

// membersSnapshot copies the live members slice under lock.
func (g *chatBoard) membersSnapshot() []*DownloadStatus {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]*DownloadStatus, len(g.members))
	copy(out, g.members)
	return out
}

// renderCombined builds the plain + rich text for the whole chat: each live member's
// section stacked in registration order, joined by a divider, with the queue listing
// appended exactly once.
func (g *chatBoard) renderCombined() (plain, rich string) {
	members := g.membersSnapshot()
	var pb, rb strings.Builder
	for i, m := range members {
		if i > 0 {
			pb.WriteString("\n\n──────────\n\n")
			rb.WriteString("\n\n---\n\n")
		}
		pb.WriteString(m.RenderSnapshotBare())
		rb.WriteString(m.renderSnapshotBareRich())
	}
	pb.WriteString(g.bot.queueBoardSuffix())
	rb.WriteString(g.bot.queueBoardSuffixRich())
	return pb.String(), rb.String()
}

// flush re-renders the combined board and edits the chat's single message, with the
// same 3s dedup/throttle the old per-board loop used. On throttle it simply returns;
// the 3s ticker guarantees a follow-up edit, so there's no busy-loop.
func (g *chatBoard) flush(force bool) {
	if g == nil || g.bot == nil {
		return
	}
	g.mu.Lock()
	msgID := g.messageID
	lastText := g.lastText
	lastUpdate := g.lastUpdate
	g.mu.Unlock()
	if msgID == 0 {
		return
	}
	plain, rich := g.renderCombined()
	now := time.Now()
	if !force {
		if plain == lastText {
			return
		}
		if now.Sub(lastUpdate) < 3*time.Second {
			return
		}
	}
	if _, err := g.bot.editMessageRich(g.chatID, msgID, rich, plain, nil); err != nil {
		return
	}
	g.mu.Lock()
	g.lastText = plain
	g.lastUpdate = now
	g.mu.Unlock()
}

// relocate deletes the current message and re-sends the combined board at the bottom
// of the chat, so /status and new enqueues resurface a single up-to-date board (no
// stale duplicates). The loop continues on the new message; the next flush upgrades
// the plain re-send to the rich render.
func (g *chatBoard) relocate(replyToID int) {
	if g == nil || g.bot == nil {
		return
	}
	plain, _ := g.renderCombined()
	newID, err := g.bot.sendMessageWithReplyReturn(g.chatID, plain, nil, replyToID)
	if err != nil {
		return
	}
	g.mu.Lock()
	oldID := g.messageID
	g.messageID = newID
	g.lastText = plain
	g.lastUpdate = time.Now()
	g.mu.Unlock()
	if oldID != 0 && oldID != newID {
		_ = g.bot.deleteMessage(g.chatID, oldID)
	}
}

// attachBoard registers a task's render source into its chat's combined board,
// creating the board (message + loop) on the first task in the chat and joining the
// existing one otherwise. Returns the per-task DownloadStatus the runner updates.
func (b *TelegramBot) attachBoard(chatID int64, replyToID int, req *downloadRequest) (*DownloadStatus, error) {
	status := newDownloadStatus(b, chatID, req)

	b.queueMu.Lock()
	grp := b.chatBoards[chatID]
	newGroup := grp == nil
	if newGroup {
		grp = &chatBoard{
			bot:      b,
			chatID:   chatID,
			updateCh: make(chan struct{}, 1),
			stopCh:   make(chan struct{}),
		}
		b.chatBoards[chatID] = grp
	}
	status.group = grp
	grp.mu.Lock()
	grp.members = append(grp.members, status)
	grp.mu.Unlock()
	b.activeBoards[req.taskID] = status
	idleID := b.idleStatusMsgID
	idleChat := b.idleStatusChatID
	b.idleStatusMsgID = 0
	b.queueMu.Unlock()

	if idleID != 0 {
		_ = b.deleteMessage(idleChat, idleID)
	}

	if newGroup {
		// First task in this chat: adopt the request's pre-sent placeholder if it has
		// one, else send the initial message. Set the message ID before starting the
		// loop so flush never runs against a zero ID.
		msgID := req.statusMessageID
		if msgID <= 0 {
			id, err := b.sendMessageWithReplyReturn(chatID, "Starting download...", nil, replyToID)
			if err != nil {
				// Roll back this registration. Only drop the group if no other task
				// raced in and joined it meanwhile, so we never orphan a sibling.
				b.queueMu.Lock()
				delete(b.activeBoards, req.taskID)
				grp.mu.Lock()
				for i, m := range grp.members {
					if m == status {
						grp.members = append(grp.members[:i], grp.members[i+1:]...)
						break
					}
				}
				empty := len(grp.members) == 0
				grp.mu.Unlock()
				if empty {
					delete(b.chatBoards, chatID)
				}
				b.queueMu.Unlock()
				return nil, err
			}
			msgID = id
		}
		grp.mu.Lock()
		grp.messageID = msgID
		grp.mu.Unlock()
		go grp.loop()
	} else {
		// Joining an existing board: drop any placeholder this request pre-sent so it
		// doesn't dangle, then refresh the combined message to include the new task.
		if req.statusMessageID > 0 {
			_ = b.deleteMessage(chatID, req.statusMessageID)
		}
		grp.signal()
	}
	return status, nil
}

// retireBoard removes a finished task's section from its chat's combined board. It is
// idempotent. When other tasks remain in the chat, the section simply disappears
// (success or failure alike). When this was the last task, the message is deleted on
// success, or kept showing the final state on a terminal failure/cancel — so a lone
// error stays visible, matching the pre-combined single-board behavior.
func (b *TelegramBot) retireBoard(s *DownloadStatus) {
	if s == nil {
		return
	}
	b.queueMu.Lock()
	if s.retired {
		b.queueMu.Unlock()
		return
	}
	s.retired = true
	delete(b.activeBoards, s.taskID)
	grp := s.group
	empty := false
	if grp != nil {
		grp.mu.Lock()
		for i, m := range grp.members {
			if m == s {
				grp.members = append(grp.members[:i], grp.members[i+1:]...)
				break
			}
		}
		empty = len(grp.members) == 0
		grp.mu.Unlock()
		if empty {
			delete(b.chatBoards, s.chatID)
		}
	}
	b.queueMu.Unlock()

	if grp == nil {
		return
	}
	if !empty {
		grp.signal() // re-render without this section
		return
	}
	// Last task in this chat: stop the loop, then finalize the single message.
	grp.stop()
	grp.mu.Lock()
	msgID := grp.messageID
	grp.mu.Unlock()
	if msgID == 0 {
		return
	}
	if s.finishedAsFailure() {
		plain := s.RenderSnapshotBare() + b.queueBoardSuffix()
		rich := s.renderSnapshotBareRich() + b.queueBoardSuffixRich()
		_, _ = b.editMessageRich(s.chatID, msgID, rich, plain, nil)
		return
	}
	_ = b.deleteMessage(s.chatID, msgID)
}

// finishedAsFailure reports whether the task's final phase denotes a terminal failure
// or cancellation (vs a clean finish), so retireBoard can keep a lone error visible
// instead of deleting the message.
func (s *DownloadStatus) finishedAsFailure() bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	p := strings.ToLower(strings.TrimSpace(s.latestPhase))
	s.mu.Unlock()
	return strings.Contains(p, "fail") || strings.Contains(p, "cancel") || strings.HasPrefix(p, "no files")
}

// Stop retires this task's board (idempotent). It's a thin alias to retireBoard so
// the delivery call sites can mark completion without reaching into chatBoard.
func (s *DownloadStatus) Stop() {
	if s == nil || s.bot == nil {
		return
	}
	s.bot.retireBoard(s)
}

func (s *DownloadStatus) Update(phase string, done, total int64) {
	if s == nil || s.bot == nil {
		return
	}
	s.mu.Lock()
	s.setLatestLocked(phase, done, total)
	s.mu.Unlock()
	s.group.signal()
}

func (s *DownloadStatus) UpdateSync(phase string, done, total int64) {
	if s == nil || s.bot == nil {
		return
	}
	s.mu.Lock()
	s.setLatestLocked(phase, done, total)
	s.mu.Unlock()
	s.group.flush(true)
}

// SetReleaseTitle sets the board's heading when no per-track UpdateTrack call will
// supply one — e.g. music-video / artwork deliveries, whose progress flows through
// status.Update (not UpdateTrack), leaving releaseTitle empty and the board showing
// "Untitled". First non-empty value wins, mirroring UpdateTrack.
func (s *DownloadStatus) SetReleaseTitle(title string) {
	if s == nil {
		return
	}
	title = strings.TrimSpace(title)
	if title == "" {
		return
	}
	s.mu.Lock()
	if s.releaseTitle == "" {
		s.releaseTitle = title
	}
	s.mu.Unlock()
}

func (s *DownloadStatus) UpdateTrack(id, title, releaseTitle, workerID string, number, trackTotal int, phase string, done, total int64) {
	if s == nil {
		return
	}
	now := time.Now()
	s.mu.Lock()
	if trackTotal > s.trackTotal {
		s.trackTotal = trackTotal
	}
	if releaseTitle != "" && s.releaseTitle == "" {
		s.releaseTitle = releaseTitle
	}
	if phase == "Finished" {
		if prev, ok := s.tracks[id]; ok {
			// Retain the size so the aggregate progress bar keeps a stable total.
			// maxBytes is the trustworthy high-water size (see field doc); fall back to
			// the reported total, then the last seen done, for older/edge cases.
			size := prev.maxBytes
			if size <= 0 {
				size = prev.size
			}
			if size <= 0 {
				size = prev.done
			}
			if size > 0 {
				s.finishedSizes = append(s.finishedSizes, size)
			}
			delete(s.tracks, id)
			s.finishedTracks++
		}
	} else {
		state, ok := s.tracks[id]
		if !ok {
			state = trackProgressState{
				id:        id,
				title:     title,
				number:    number,
				total:     trackTotal,
				startedAt: now,
			}
		}
		// Per-track speed: reset on phase change or byte rewind, then append sample.
		// Compare against OLD values before overwriting.
		oldPhase := state.phase
		oldDone := state.done
		state.phase = phase
		state.done = done
		state.size = total
		// Maintain maxBytes as the true-size high-water mark (see field doc). The
		// direct-stream path reports total=resp.ContentLength; the HLS-segment path
		// reports total=0 and exposes size only via accumulated `done`. Post-download
		// phases report 0/1 sentinels and Decrypting rewinds `done` to 0 — none of
		// which can lower maxBytes, so finishedSizes captures the real track size and
		// the upload bar no longer collapses to "7.00MB / 7.00MB".
		if total > state.maxBytes {
			state.maxBytes = total
		}
		if done > state.maxBytes {
			state.maxBytes = done
		}
		state.updatedAt = now
		if workerID != "" {
			state.workerID = workerID
		}
		if phase != oldPhase || done < oldDone {
			state.speedSamples = state.speedSamples[:0]
		}
		state.speedSamples = append(state.speedSamples, progressSample{at: now, done: done})
		state.speedSamples = pruneSpeedSamples(state.speedSamples, now)
		s.tracks[id] = state
	}
	s.dirty = true
	s.mu.Unlock()
	s.group.signal()
}

// RenderSnapshot returns the current status board text using the same renderer
// as the live message, so /status shows the identical rich view (progress bar,
// speed, ETA, mode) plus the queue section when the queue is non-empty.
func (s *DownloadStatus) RenderSnapshot() string {
	if s == nil {
		return ""
	}
	return s.RenderSnapshotBare() + s.bot.queueBoardSuffix()
}

// RenderSnapshotBare is RenderSnapshot without the trailing queue section, so a
// caller rendering several boards at once can append the queue list just once.
func (s *DownloadStatus) RenderSnapshotBare() string {
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
	return s.formatProgressText(phase, done, total, percent)
}

// renderSnapshotBareRich is the Rich-Markdown counterpart of RenderSnapshotBare: the
// task's section without the queue suffix, so chatBoard.renderCombined can stack
// several sections and append the queue listing exactly once.
func (s *DownloadStatus) renderSnapshotBareRich() string {
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
	return s.formatProgressRich(phase, done, total, percent)
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
	uploading := isUploadPhase(normalizedPhase)
	if uploading {
		// Upload phase: the per-file counter (done) rewinds to 0 for each of the N
		// files and the phase text changes ("Uploading 1/N" → "2/N"), so sampling
		// `done` directly would reset the speed window every file (the old jitter
		// bug). Fold each file's progress into a monotonic running total and sample
		// THAT — giving a steady cross-file upload speed and a bar that climbs across
		// the whole batch instead of sticking at the finished-download 100%.
		if !s.uploading {
			s.uploadDoneBytes = 0
			s.uploadPrevDone = 0
			s.speedSamples = s.speedSamples[:0]
		}
		delta := done - s.uploadPrevDone
		if delta < 0 {
			// Counter rewound → a new file started; its `done` is the fresh contribution.
			delta = done
		}
		if delta > 0 {
			s.uploadDoneBytes += delta
		}
		s.uploadPrevDone = done
		s.speedSamples = append(s.speedSamples, progressSample{at: now, done: s.uploadDoneBytes})
		s.speedSamples = pruneSpeedSamples(s.speedSamples, now)
	} else {
		// Maintain a short rolling window of (time, bytes) samples for a live speed
		// readout. Reset it whenever the phase changes or the byte counter rewinds
		// (the per-file audio group reports each file starting from 0), so we never
		// compute a delta across a discontinuity.
		if phaseChanged || done < s.latestDone {
			s.speedSamples = s.speedSamples[:0]
		}
		s.speedSamples = append(s.speedSamples, progressSample{at: now, done: done})
		s.speedSamples = pruneSpeedSamples(s.speedSamples, now)
	}
	s.uploading = uploading

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

func truncateStatusTitle(value string, maxRunes int) string {
	value = strings.TrimSpace(value)
	runes := []rune(value)
	if len(runes) <= maxRunes {
		return value
	}
	return string(runes[:maxRunes-1]) + "…"
}

// shortMode returns a compact, terminal-aesthetic label for a transfer mode.
// Used in the status board header; full human labels still live in formatUploadMode.
func shortMode(mode string) string {
	switch mode {
	case transferModeTelegramIndividual:
		return "TG_Individual"
	case transferModeTelegramZip:
		return "TG_Zip"
	case transferModeGofileZip:
		return "Gofile"
	case transferModeMv:
		return "MV_TG"
	case transferModeMvGofile:
		return "MV_Gofile"
	case transferModeArt:
		return "Cover"
	default:
		return mode
	}
}

// renderBar returns a fixed-width progress bar using outline hex cells.
// Uses ▰ (filled) and ▱ (empty) — no half-cell, so the percentage label
// (printed next to it) carries the granular readout.
func renderBar(pct, width int) string {
	if pct < 0 {
		pct = 0
	}
	if pct > 100 {
		pct = 100
	}
	if width < 1 {
		width = 1
	}
	filled := (pct * width) / 100
	b := make([]byte, 0, width*3)
	for i := 0; i < width; i++ {
		if i < filled {
			b = append(b, symBarFull...)
		} else {
			b = append(b, symBarEmpty...)
		}
	}
	return string(b)
}

// medianSpeed returns the median bytes/sec across adjacent speed samples in the
// rolling window. Median (not mean) kills the "412MB/s" outlier that happens
// when one sample in the window catches a track transitioning through a phase
// boundary or finishing. Returns 0 when there's not enough data to estimate.
func medianSpeed(samples []progressSample) float64 {
	if len(samples) < 2 {
		return 0
	}
	deltas := make([]float64, 0, len(samples)-1)
	for i := 1; i < len(samples); i++ {
		dt := samples[i].at.Sub(samples[i-1].at).Seconds()
		db := float64(samples[i].done - samples[i-1].done)
		if dt > 0 && db > 0 {
			deltas = append(deltas, db/dt)
		}
	}
	if len(deltas) == 0 {
		return 0
	}
	sort.Float64s(deltas)
	return deltas[len(deltas)/2]
}

// aggregatedProgress snapshots everything the renderer needs in a single locked
// block, so formatProgressText can be called without juggling mutexes.
//   - active:     currently downloading tracks, sorted by track number
//   - finished:   count of completed tracks
//   - total:      planned track count
//   - aggDone,
//     aggTotal:   bytes — sum of finished track sizes plus live track progress.
//                 Stable denominator: finished tracks are retained, not forgotten.
type progressSnapshot struct {
	active        []trackProgressState
	finished      int
	total         int
	aggDone       int64
	aggTotal      int64
	hasActiveData bool
	releaseTitle  string
	workerLimit   int
	single        bool
	// uploading is set while the Telegram upload step runs; uploadDone is the
	// monotonic running total of bytes uploaded across all files so far (the bar
	// numerator during upload; aggTotal is its denominator).
	uploading  bool
	uploadDone int64
}

func (s *DownloadStatus) snapshot() progressSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	snap := progressSnapshot{
		finished:     s.finishedTracks,
		total:        s.trackTotal,
		releaseTitle: s.releaseTitle,
		workerLimit:  s.workerLimit,
		single:       s.single,
		uploading:    s.uploading,
		uploadDone:   s.uploadDoneBytes,
	}
	snap.active = make([]trackProgressState, 0, len(s.tracks))
	for _, st := range s.tracks {
		snap.active = append(snap.active, st)
		// Clamp done to size so an over-shoot at the phase boundary can't
		// push aggDone past aggTotal (the symptom of the original bug).
		d := st.done
		if st.size > 0 && d > st.size {
			d = st.size
		}
		if d < 0 {
			d = 0
		}
		snap.aggDone += d
		snap.aggTotal += st.size
		snap.hasActiveData = true
	}
	for _, sz := range s.finishedSizes {
		snap.aggDone += sz
		snap.aggTotal += sz
	}
	if snap.total < snap.finished+len(snap.active) {
		snap.total = snap.finished + len(snap.active)
	}
	sort.Slice(snap.active, func(i, j int) bool {
		if snap.active[i].number == snap.active[j].number {
			return snap.active[i].id < snap.active[j].id
		}
		return snap.active[i].number < snap.active[j].number
	})
	return snap
}

// uploadAwareBar picks the progress-bar numerator/denominator for the current phase.
// During the upload phase the per-track download aggregate is frozen at 100% (every
// track already finished downloading), so the bar would stick there; instead we drive
// it from the monotonic uploaded-bytes total over the same aggregate size. Outside the
// upload phase it keeps the original behaviour: per-track aggregate when available,
// else the raw (done, total) for phases like Zipping that don't stream track progress.
func uploadAwareBar(snap progressSnapshot, done, total int64) (barDone, barTotal int64, hasBar bool) {
	if snap.uploading && snap.aggTotal > 0 {
		d := snap.uploadDone
		if d > snap.aggTotal {
			d = snap.aggTotal
		}
		if d < 0 {
			d = 0
		}
		return d, snap.aggTotal, true
	}
	if snap.hasActiveData || snap.finished > 0 {
		return snap.aggDone, snap.aggTotal, true
	}
	return done, total, total > 0
}

// formatProgressText is the single renderer for the active-task status board.
// The output uses outline-style symbols (see sym* constants) and a single
// monospace code block for the bar+stats+per-track list so columns align
// across Telegram's font variations.
func (s *DownloadStatus) formatProgressText(phase string, done, total int64, percent int) string {
	snap := s.snapshot()

	barDone, barTotal, hasBar := uploadAwareBar(snap, done, total)
	barPct := -1
	if hasBar && barTotal > 0 {
		barPct = int(barDone * 100 / barTotal)
		if barPct < 0 {
			barPct = 0
		}
		if barPct > 100 {
			barPct = 100
		}
	}

	// Speed: aggregate median across active tracks, or fall back to the global
	// rolling sample when we're in a post-download phase.
	var totalSpeed float64
	for _, st := range snap.active {
		if v := medianSpeed(st.speedSamples); v > 0 {
			totalSpeed += v
		}
	}
	if totalSpeed == 0 {
		totalSpeed = s.rollingSpeedBytesPerSec()
	}

	// Elapsed only. ETA was removed: on small screens it never had room and was
	// frequently unknown (stuck at "-"), so it added noise without value.
	elapsedStr := "-"
	if !s.startedAt.IsZero() {
		elapsedStr = formatDuration(time.Since(s.startedAt))
	}

	// Header (plain proportional-font text — the board uses no parse_mode, so we
	// must not wrap the body in ``` fences; Telegram would render them literally).
	header := s.formatHeader(phase, snap)
	bar := renderBar(barPct, 10)

	var b strings.Builder
	b.WriteString(header)
	b.WriteString("\n\n") // blank line between the header and the progress block

	// Progress block, each element on its own line so it reads cleanly on narrow
	// screens: bar + percent, then the byte counter, then speed · elapsed.
	// (ETA was removed earlier — usually unknown and space-hungry.)
	if hasBar {
		if barPct >= 0 {
			fmt.Fprintf(&b, "%s  %d%%\n", bar, barPct)
		} else {
			fmt.Fprintf(&b, "%s\n", bar)
		}
		fmt.Fprintf(&b, "%s / %s\n", formatBytes(barDone), formatBytes(barTotal))
	}
	fmt.Fprintf(&b, "%s %s/s  ·  %s %s\n",
		symSpeed, formatBytes(int64(totalSpeed)),
		symElapsed, elapsedStr,
	)

	// Per-track list (only when we have track data and not a single-track download).
	// Suppressed during upload: active is empty there, so the block would show only
	// the DOWNLOAD tally ("10 done"), contradicting the "Uploading · i/N" header.
	if !snap.single && !snap.uploading && (len(snap.active) > 0 || snap.finished > 0 || snap.total > 0) {
		b.WriteString("\n")
		shown := 0
		for _, st := range snap.active {
			if shown >= statusTrackListCap {
				break
			}
			fmt.Fprintf(&b, "%s%s %02d %s   %s/%s · %s/s\n",
				workerPrefix(st.workerID),
				symActive, st.number, truncateStatusTitle(st.title, 28),
				formatBytes(st.done), formatBytes(st.size),
				formatBytes(int64(medianSpeed(st.speedSamples))),
			)
			shown++
		}
		if snap.finished > 0 {
			fmt.Fprintf(&b, "     %s %d done\n", symDone, snap.finished)
		}
		remaining := snap.total - snap.finished - len(snap.active)
		if remaining > 0 {
			if shown >= statusTrackListCap {
				fmt.Fprintf(&b, "     %s %d more queued\n", symQueued, remaining)
			} else {
				fmt.Fprintf(&b, "     %s %d queued\n", symQueued, remaining)
			}
		}
	}
	return b.String()
}

// richMDEscaper backslash-escapes the GitHub-Flavored-Markdown metacharacters
// that Rich Messages (Bot API 10.1) interpret, so user/track text can't smuggle
// in stray formatting. The pipe is the critical one inside table cells.
var richMDEscaper = strings.NewReplacer(
	`\`, `\\`,
	"`", "\\`",
	"*", `\*`,
	"_", `\_`,
	"~", `\~`,
	"|", `\|`,
	"[", `\[`,
	"]", `\]`,
	"#", `\#`,
	// '<' opens an HTML tag in the Bot API 10.1 Rich renderer (which interprets the
	// <details>/<summary> tags we emit), so a track/album/user name containing '<'
	// could break rendering or smuggle in markup. Map it to the HTML entity, which
	// the renderer decodes back to a literal '<' and never treats as a tag.
	"<", "&lt;",
	">", `\>`,
	"=", `\=`,
)

// escapeRichMD escapes inline text and collapses newlines (which would break a
// table row or a single-line heading) into spaces.
func escapeRichMD(s string) string {
	return richMDEscaper.Replace(strings.ReplaceAll(s, "\n", " "))
}

// formatProgressRich renders the live status board as Bot API 10.1 Rich Markdown:
// an H1 release heading, a status line, an aggregate progress bar, a per-track
// table (active tracks with live percent + speed), a finished/queued task-list
// summary, and a blockquote footer with the aggregate speed and elapsed time. It
// reads from the same snapshot() the plain renderer uses, so the two stay in
// lockstep; flush() falls back to formatProgressText if the rich edit is
// rejected.
func (s *DownloadStatus) formatProgressRich(phase string, done, total int64, percent int) string {
	snap := s.snapshot()

	// Bar source — identical logic to formatProgressText (upload-aware).
	barDone, barTotal, hasBar := uploadAwareBar(snap, done, total)
	barPct := -1
	if hasBar && barTotal > 0 {
		barPct = int(barDone * 100 / barTotal)
		if barPct < 0 {
			barPct = 0
		}
		if barPct > 100 {
			barPct = 100
		}
	}

	var totalSpeed float64
	for _, st := range snap.active {
		if v := medianSpeed(st.speedSamples); v > 0 {
			totalSpeed += v
		}
	}
	if totalSpeed == 0 {
		totalSpeed = s.rollingSpeedBytesPerSec()
	}
	elapsedStr := "-"
	if !s.startedAt.IsZero() {
		elapsedStr = formatDuration(time.Since(s.startedAt))
	}

	title := strings.TrimSpace(snap.releaseTitle)
	if title == "" {
		title = "Untitled"
	}
	user := "@" + s.username
	if s.username == "" {
		user = fmt.Sprintf("ID:%d", s.userID)
	}

	// Status line: phase + finished/total counter, mirroring formatHeader's
	// double-counter guard for upload phases that bake in their own "i/N".
	statusLine := strings.TrimSpace(phase)
	if m := phaseTrailingCounter.FindStringSubmatch(statusLine); m != nil {
		statusLine = m[1] + " · " + m[2]
	} else if snap.total > 0 {
		counter := fmt.Sprintf("%d/%d", snap.finished, snap.total)
		if statusLine == "" {
			statusLine = counter
		} else {
			statusLine += " · " + counter
		}
	}

	var b strings.Builder
	fmt.Fprintf(&b, "# %s %s\n", symDownload, escapeRichMD(truncateStatusTitle(title, 80)))
	if statusLine != "" {
		fmt.Fprintf(&b, "%s\n", escapeRichMD(statusLine))
	}

	if hasBar {
		bar := renderBar(barPct, 12)
		if barPct >= 0 {
			fmt.Fprintf(&b, "\n`%s`  **%d%%**  ·  %s / %s\n", bar, barPct, formatBytes(barDone), formatBytes(barTotal))
		} else {
			fmt.Fprintf(&b, "\n`%s`\n", bar)
		}
	}

	// Per-track table — only for multi-track releases with live data. The track
	// number folds into the Track cell ("01 · Title") so there's no wide right-
	// aligned "#" column shoving the rest of the table to the right. The table is
	// wrapped in a collapsed <details> block (Bot API 10.1 Rich Markdown renders
	// supported HTML directly) so the board stays compact — important now that two
	// tasks can share one combined message; the user taps "Tracks · N active" to
	// expand. Blank lines around the table keep GFM parsing it as a table inside the
	// details block.
	if !snap.single && len(snap.active) > 0 {
		fmt.Fprintf(&b, "\n<details>\n<summary>Tracks · %d active</summary>\n\n", len(snap.active))
		b.WriteString("| Track | Progress | Speed |\n|:------|:--------:|------:|\n")
		shown := 0
		for _, st := range snap.active {
			if shown >= statusTrackListCap {
				break
			}
			prog := "…"
			if st.size > 0 {
				p := int(st.done * 100 / st.size)
				if p > 100 {
					p = 100
				}
				prog = fmt.Sprintf("%d%%", p)
			}
			spd := "—"
			if v := medianSpeed(st.speedSamples); v > 0 {
				spd = formatBytes(int64(v)) + "/s"
			}
			// Tag the track with the wrapper-manager instance handling it (e.g.
			// "[wm-1]") so a slow/stuck worker is easy to spot at a glance. Omitted
			// while a track is still between workers (Preparing phase, no worker yet).
			title := escapeRichMD(truncateStatusTitle(st.title, 32))
			if st.workerID != "" {
				title += " [" + escapeRichMD(st.workerID) + "]"
			}
			fmt.Fprintf(&b, "| %02d · %s | %s | %s |\n",
				st.number, title, prog, spd)
			shown++
		}
		b.WriteString("\n</details>\n")
	}

	// Finished / queued task-list summary (checkboxes read naturally in rich).
	// Hidden during the upload phase: there it's the DOWNLOAD tally ("10 done"),
	// which reads as a contradiction next to the "Uploading · i/N" file counter on
	// the status line. The bar + status line carry upload progress on their own.
	if !snap.single && !snap.uploading && (snap.finished > 0 || snap.total > 0) {
		b.WriteString("\n")
		if snap.finished > 0 {
			fmt.Fprintf(&b, "- [x] %d done\n", snap.finished)
		}
		remaining := snap.total - snap.finished - len(snap.active)
		if remaining > 0 {
			fmt.Fprintf(&b, "- [ ] %d queued\n", remaining)
		}
	}

	// Footer blockquote: three lines — speed+elapsed, who·mode, then the cancel
	// command. Each non-final line ends in TWO spaces (a GFM hard break): without it
	// Telegram's rich blockquote soft-wraps the lines into one run-on (the old
	// four-line footer rendered as a single line). The stray "✕" is gone. /stop_<id>
	// stays bare so Telegram auto-links it as a tappable command; task IDs are hex,
	// safe from GFM emphasis.
	fmt.Fprintf(&b, "\n> %s %s/s  %s %s  \n> by %s · %s  \n> cancel /stop_%s\n",
		symSpeed, formatBytes(int64(totalSpeed)),
		symElapsed, elapsedStr,
		escapeRichMD(user), escapeRichMD(shortMode(s.mode)),
		s.taskID,
	)
	return b.String()
}

// phaseTrailingCounter matches a phase that ends in its own "i/N" progress
// counter (e.g. "Uploading 7/7"), so we can detect it and normalize the spacing
// to "Uploading · 7/7" rather than double up with the track counter.
var phaseTrailingCounter = regexp.MustCompile(`^(.*\S)\s+(\d+/\d+)$`)

// formatHeader produces the four-line task heading for the status board:
//
//	▸ <title>
//	<phase> · <counter>
//	by <user> · <mode>
//	✕ cancel  /stop_<id>
//
// It's plain proportional-font text (the board uses no parse_mode). Each piece
// sits on its own line so the header doesn't read as one dense run-on.
func (s *DownloadStatus) formatHeader(phase string, snap progressSnapshot) string {
	title := strings.TrimSpace(snap.releaseTitle)
	if title == "" {
		title = "Untitled"
	}
	title = truncateStatusTitle(title, 56)

	user := "@" + s.username
	if s.username == "" {
		user = fmt.Sprintf("ID:%d", s.userID)
	}

	// Status line = phase plus the finished/total track counter. The upload phase
	// bakes its own "i/N" file counter into the phase string, so only append the
	// track counter when the phase carries none — otherwise we'd render the count
	// twice (e.g. "Uploading 7/7 · 7/7").
	statusLine := strings.TrimSpace(phase)
	if m := phaseTrailingCounter.FindStringSubmatch(statusLine); m != nil {
		statusLine = m[1] + " · " + m[2]
	} else if snap.total > 0 {
		counter := fmt.Sprintf("%d/%d", snap.finished, snap.total)
		if statusLine == "" {
			statusLine = counter
		} else {
			statusLine += " · " + counter
		}
	}

	lines := []string{fmt.Sprintf("%s %s", symDownload, title)}
	if statusLine != "" {
		lines = append(lines, statusLine)
	}
	lines = append(lines, fmt.Sprintf("by %s · %s", user, shortMode(s.mode)))
	lines = append(lines, fmt.Sprintf("%s cancel  /stop_%s", symCancel, s.taskID))
	return strings.Join(lines, "\n")
}

// workerPrefix returns the "wm-1 " (with trailing space) prefix for a per-track
// line, or 5 spaces of padding when the track has no worker attached yet (e.g.
// during the "Preparing" phase before a wrapper-manager is acquired). The fixed
// width keeps the track title column aligned across rows.
func workerPrefix(id string) string {
	if id == "" {
		return "     "
	}
	if len(id) >= 5 {
		return id + " "
	}
	return id + strings.Repeat(" ", 5-len(id))
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

// videoQualityLabel maps a video's pixel height to a familiar quality tag
// (e.g. 1080 -> "1080p", 2160 -> "4K"). Returns "" when the height is unknown.
func videoQualityLabel(height int) string {
	switch {
	case height <= 0:
		return ""
	case height >= 2160:
		return "4K"
	case height >= 1440:
		return "1440p"
	case height >= 1080:
		return "1080p"
	case height >= 720:
		return "720p"
	case height >= 480:
		return "480p"
	case height >= 360:
		return "360p"
	default:
		return fmt.Sprintf("%dp", height)
	}
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

// InputRichMessage mirrors the Bot API 10.1 type. Exactly one of Markdown or
// HTML must be set; Telegram parses it into a structured block tree server-side.
type InputRichMessage struct {
	Markdown            string `json:"markdown,omitempty"`
	HTML                string `json:"html,omitempty"`
	IsRTL               bool   `json:"is_rtl,omitempty"`
	SkipEntityDetection bool   `json:"skip_entity_detection,omitempty"`
}

// richSendResult is the value returned by the rich send/edit helpers so callers
// can learn the message ID and whether the rich path was actually taken (vs. a
// plain-text fallback) without threading two return values everywhere.
type richSendResult struct {
	messageID int
	rich      bool // true if sent as a Rich Message; false if it fell back to plain
}

// isNotModified reports whether a Telegram error description is the benign
// "message is not modified" — expected when a live-edited board lands on an
// identical render between ticks.
func isNotModified(desc string) bool {
	return strings.Contains(desc, "message is not modified")
}

// richSupported reports whether we should still attempt Rich Messages. Once the
// API has rejected one with an "unsupported"/"unknown method/field" style error
// we latch off and stop trying for the lifetime of the process.
func (b *TelegramBot) richSupported() bool {
	return !b.richUnavailable.Load()
}

// markRichUnsupported latches the rich path off after a capability-style
// rejection, logging once so the operator knows why boards went plain.
func (b *TelegramBot) markRichUnsupported(method, desc string) {
	if b.richUnavailable.CompareAndSwap(false, true) {
		fmt.Printf("[rich] %s rejected (%s); falling back to plain text for the rest of this run\n", method, b.sanitizeTelegramText(desc))
	}
}

// looksLikeCapabilityError distinguishes "this server/method doesn't know about
// Rich Messages" (latch off, fall back) from ordinary content errors like a
// malformed table (don't latch — that would be a bug we want surfaced).
func looksLikeCapabilityError(desc string) bool {
	d := strings.ToLower(desc)
	switch {
	case strings.Contains(d, "method not found"),
		strings.Contains(d, "not found: rich"),
		strings.Contains(d, "unknown field"),
		strings.Contains(d, "unsupported"),
		strings.Contains(d, "rich_message"),
		strings.Contains(d, "rich message"):
		return true
	}
	return false
}

// sendRichMessage posts a Rich Message (Bot API 10.1). On any capability error
// it latches the rich path off and falls back to plainFallback via the normal
// sendMessage path, so callers always get a usable message. markup may be nil.
func (b *TelegramBot) sendRichMessage(chatID int64, markdown, plainFallback string, markup any, replyToID int) (richSendResult, error) {
	if !b.richSupported() {
		id, err := b.sendMessageWithReplyReturn(chatID, plainFallback, markup, replyToID)
		return richSendResult{messageID: id, rich: false}, err
	}
	payload := map[string]any{
		"chat_id":      chatID,
		"rich_message": InputRichMessage{Markdown: markdown},
	}
	if markup != nil {
		payload["reply_markup"] = markup
	}
	if replyToID > 0 {
		payload["reply_parameters"] = map[string]any{"message_id": replyToID}
	}
	id, desc, ok, err := b.doRichRequest("sendRichMessage", payload)
	if err == nil && ok {
		return richSendResult{messageID: id, rich: true}, nil
	}
	if desc != "" && looksLikeCapabilityError(desc) {
		b.markRichUnsupported("sendRichMessage", desc)
		id, ferr := b.sendMessageWithReplyReturn(chatID, plainFallback, markup, replyToID)
		return richSendResult{messageID: id, rich: false}, ferr
	}
	if err != nil {
		return richSendResult{}, err
	}
	return richSendResult{}, fmt.Errorf("telegram sendRichMessage error: %s", b.sanitizeTelegramText(desc))
}

// editMessageRich edits an existing message into rich content (Bot API 10.1),
// preserving its inline keyboard. Returns rich=false (and edits as plain text)
// when the rich path is unavailable, so the live board degrades gracefully. A
// benign "message is not modified" is treated as success.
func (b *TelegramBot) editMessageRich(chatID int64, messageID int, markdown, plainFallback string, markup any) (bool, error) {
	if messageID == 0 {
		return false, nil
	}
	if !b.richSupported() {
		return false, b.editMessageText(chatID, messageID, plainFallback, markup)
	}
	payload := map[string]any{
		"chat_id":      chatID,
		"message_id":   messageID,
		"rich_message": InputRichMessage{Markdown: markdown},
	}
	if markup != nil {
		payload["reply_markup"] = markup
	}
	_, desc, ok, err := b.doRichRequest("editMessageText", payload)
	if err == nil && ok {
		return true, nil
	}
	if desc != "" && isNotModified(desc) {
		return true, nil
	}
	if desc != "" && looksLikeCapabilityError(desc) {
		b.markRichUnsupported("editMessageText(rich)", desc)
		return false, b.editMessageText(chatID, messageID, plainFallback, markup)
	}
	if err != nil {
		return false, err
	}
	return false, fmt.Errorf("telegram editMessageText(rich) error: %s", b.sanitizeTelegramText(desc))
}

// doRichRequest performs a POST for a rich method and decodes the standard
// envelope. It returns (messageID, description, ok, transportErr). messageID is
// 0 when the result isn't a Message (e.g. editing an inline message yields
// True). A non-nil transportErr is a network/HTTP failure; a false ok with a
// description is an API-level rejection the caller decides how to handle.
func (b *TelegramBot) doRichRequest(method string, payload map[string]any) (int, string, bool, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return 0, "", false, err
	}
	req, err := http.NewRequest("POST", b.apiURL(method), bytes.NewReader(body))
	if err != nil {
		return 0, "", false, b.sanitizeTelegramError(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := b.client.Do(req)
	if err != nil {
		return 0, "", false, b.sanitizeTelegramError(err)
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, "", false, err
	}
	envelope := sendMessageResponse{}
	if jerr := json.Unmarshal(respBody, &envelope); jerr != nil {
		// Result may be a bare `true` (inline edits) which doesn't fit Message;
		// retry with a description-only envelope before giving up.
		alt := apiResponse{}
		if aerr := json.Unmarshal(respBody, &alt); aerr == nil {
			return 0, alt.Description, alt.OK, nil
		}
		return 0, "", false, jerr
	}
	return envelope.Result.MessageID, envelope.Description, envelope.OK, nil
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

// answerCallbackAlert acks a callback query with a visible alert/toast — used to tell a
// non-owner that the buttons they tapped aren't theirs.
func (b *TelegramBot) answerCallbackAlert(callbackID string, text string) error {
	if callbackID == "" {
		return nil
	}
	payload := map[string]any{
		"callback_query_id": callbackID,
		"text":              text,
		"show_alert":        true,
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

// fetchUsername populates b.username from getMe so command @-mention filtering can tell
// "/help@ThisBot" from "/help@OtherBot". Best-effort: on failure b.username stays empty
// and the mention check is skipped (fail-open, so a transient error can't deafen the bot).
func (b *TelegramBot) fetchUsername() {
	req, err := http.NewRequest("GET", b.apiURL("getMe"), nil)
	if err != nil {
		fmt.Printf("getMe request build failed: %v\n", b.sanitizeTelegramError(err))
		return
	}
	resp, err := b.client.Do(req)
	if err != nil {
		fmt.Printf("getMe failed: %v\n", b.sanitizeTelegramError(err))
		return
	}
	defer resp.Body.Close()
	var data struct {
		OK     bool `json:"ok"`
		Result struct {
			Username string `json:"username"`
		} `json:"result"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		fmt.Printf("getMe decode failed: %v\n", err)
		return
	}
	if !data.OK || data.Result.Username == "" {
		fmt.Println("getMe returned no username; @-mention filtering disabled")
		return
	}
	b.username = strings.ToLower(data.Result.Username)
	fmt.Printf("Bot username: @%s\n", data.Result.Username)
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

func (b *TelegramBot) setPending(chatID int64, userID int64, kind string, query string, offset int, items []apputils.SearchResultItem, hasNext bool, replyToID int, resultsMessageID int, title string) {
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
		UserID:           userID,
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

// parseCommand splits a "/cmd@bot args..." message. It returns the lowercased command,
// the lowercased @-mention target ("" if none), the remaining args, and whether the text
// was a command at all. The caller decides whether a non-empty mention belongs to us.
func parseCommand(text string) (cmd string, mention string, args []string, ok bool) {
	if !strings.HasPrefix(text, "/") {
		return "", "", nil, false
	}
	parts := strings.Fields(text)
	if len(parts) == 0 {
		return "", "", nil, false
	}
	cmd = strings.TrimPrefix(parts[0], "/")
	if idx := strings.Index(cmd, "@"); idx != -1 {
		mention = strings.ToLower(cmd[idx+1:])
		cmd = cmd[:idx]
	}
	return strings.ToLower(cmd), mention, parts[1:], true
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

// songIDFromURLParam returns the song id carried in an Apple Music album link's
// ?i= query param, or "" if absent/unparseable.
func songIDFromURLParam(link string) string {
	parsed, err := url.Parse(link)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(parsed.Query().Get("i"))
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
// sendDeliverySummary posts a Bot API 10.1 Rich Message summarizing a completed
// multi-track delivery: an H2 release heading and a table of (#, track, size,
// format) plus a footer with the total. Single-track and music-video/artwork
// deliveries skip it (the file itself is the summary). It is best-effort — a
// failure is swallowed so it never blocks the delivery flow — and degrades to a
// plain-text list when the API can't serve rich content.
func (b *TelegramBot) sendDeliverySummary(ctx context.Context, chatID int64, paths []string, format string, replyToID int) {
	if len(paths) < 2 {
		return
	}

	type row struct {
		num    int
		title  string
		artist string
		size   int64
	}
	rows := make([]row, 0, len(paths))
	var totalBytes int64
	albumName, albumArtist, quality, codec, playlistName, playlistArtist := "", "", "", "", "", ""

	for i, p := range paths {
		r := row{num: i + 1, title: filepath.Base(p)}
		if meta, ok := getDownloadedMeta(ctx, p); ok {
			if meta.Title != "" {
				r.title = meta.Title
			}
			r.artist = meta.Performer
			if albumName == "" {
				albumName = meta.AlbumName
				albumArtist = meta.Performer
			}
			if quality == "" {
				quality = meta.Quality
				codec = meta.Codec
			}
			if playlistName == "" && meta.PlaylistName != "" {
				playlistName = meta.PlaylistName
				playlistArtist = meta.PlaylistArtist
			}
		}
		if info, err := os.Stat(p); err == nil {
			r.size = info.Size()
			totalBytes += r.size
		}
		rows = append(rows, r)
	}

	if playlistName != "" {
		albumName = playlistName
		albumArtist = playlistArtist
	}
	if albumName == "" {
		albumName = filepath.Base(filepath.Dir(paths[0]))
		if albumName == "." || albumName == "" {
			albumName = "Unknown"
		}
	}
	fmtLabel := strings.ToUpper(normalizeTelegramFormat(format))
	if q := formatQualityDisplay(quality, codec); q != "" {
		fmtLabel = q
	}

	heading := albumName
	if albumArtist != "" {
		heading = albumArtist + " — " + albumName
	}

	// Rich Markdown. The track list lives in a collapsible <details> block (Bot API
	// 10.1 renders supported HTML directly) — collapsed by default since this is a
	// historical summary the user can expand on demand. The track number folds into
	// the Track cell ("1 · Artist — Title") so there's no wide right-aligned "#"
	// column. Blank lines around the table keep GFM parsing it inside the details.
	var rb strings.Builder
	fmt.Fprintf(&rb, "## %s %s\n", symDone, escapeRichMD(truncateStatusTitle(heading, 80)))
	fmt.Fprintf(&rb, "<details>\n<summary>%d tracks · %s · %s</summary>\n\n", len(rows), formatBytes(totalBytes), escapeRichMD(fmtLabel))
	rb.WriteString("| Track | Size |\n|:------|-----:|\n")
	for _, r := range rows {
		t := r.title
		if r.artist != "" {
			t = r.artist + " — " + t
		}
		fmt.Fprintf(&rb, "| %d · %s | %s |\n", r.num, escapeRichMD(truncateStatusTitle(t, 48)), formatBytes(r.size))
	}
	rb.WriteString("\n</details>\n")
	fmt.Fprintf(&rb, "\n> Delivered %d files · %s total\n", len(rows), formatBytes(totalBytes))

	// Plain fallback.
	var pb strings.Builder
	fmt.Fprintf(&pb, "%s %s\n%d tracks · %s · %s\n", symDone, heading, len(rows), formatBytes(totalBytes), fmtLabel)
	for _, r := range rows {
		t := r.title
		if r.artist != "" {
			t = r.artist + " — " + t
		}
		fmt.Fprintf(&pb, "%2d. %s  (%s)\n", r.num, t, formatBytes(r.size))
	}

	_, _ = b.sendRichMessage(chatID, rb.String(), strings.TrimRight(pb.String(), "\n"), nil, replyToID)
}

func buildCoverCaption(ctx context.Context, paths []string) string {
	if len(paths) == 0 {
		return ""
	}
	artist := ""
	albumName := ""
	releaseDate := ""
	contentRating := ""
	quality := ""
	codec := ""
	playlistName := ""
	playlistArtist := ""

	for _, p := range paths {
		if meta, ok := getDownloadedMeta(ctx, p); ok {
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
			if playlistName == "" && meta.PlaylistName != "" {
				playlistName = meta.PlaylistName
				playlistArtist = meta.PlaylistArtist
			}
		}
	}

	// For a playlist, the per-track album/artist describe individual songs, so the
	// aggregate caption would otherwise masquerade as the first track's album.
	// Present the playlist's own identity instead (track count already correct).
	if playlistName != "" {
		albumName = playlistName
		artist = playlistArtist
		releaseDate = ""
	}

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

	lines := []string{}
	// Skip the Artist line entirely when unknown rather than printing a blank field.
	if artist != "" {
		lines = append(lines, fmt.Sprintf("Artist : %s", artist))
	}
	lines = append(lines, fmt.Sprintf("Album : %s", albumName))
	// Release Date is omitted for playlists (it has no single meaningful value)
	// and whenever it's otherwise unknown, rather than printing a blank field.
	if releaseDate != "" {
		lines = append(lines, fmt.Sprintf("Release Date : %s", releaseDate))
	}
	lines = append(lines,
		fmt.Sprintf("Total Tracks : %d", len(paths)),
		fmt.Sprintf("Quality : %s", qualityDisplay),
		fmt.Sprintf("Explicit : %s", explicit),
	)
	return strings.Join(lines, "\n")
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

// cancelTask cancels a task by its ID. issuerID is the user running /stop; the
// task's owner may cancel it, and admins may cancel anyone's task.
func (b *TelegramBot) cancelTask(chatID int64, issuerID int64, taskID string, replyToID int) {
	if taskID == "" {
		_ = b.sendMessageWithReply(chatID, "Usage: /stop_<task_id>", nil, replyToID)
		return
	}
	const notYours = "You can only stop your own tasks."
	b.queueMu.Lock()
	// Check active task
	if b.activeReq != nil && b.activeReq.taskID == taskID {
		if b.activeReq.userID != 0 && issuerID != b.activeReq.userID && !b.isAdmin(issuerID) {
			b.queueMu.Unlock()
			_ = b.sendMessageWithReply(chatID, notYours, nil, replyToID)
			return
		}
		if b.activeReq.cancel != nil {
			b.activeReq.cancel()
		}
		b.queueMu.Unlock()
		_ = b.sendMessageWithReply(chatID, fmt.Sprintf("⛔ Task %s stopped.", taskID), nil, replyToID)
		return
	}
	// Check the sticky borrower (task-concurrency mode): it runs concurrently with
	// the head and is neither activeReq nor still in the queue.
	if b.schedBorrowReq != nil && b.schedBorrowReq.taskID == taskID {
		if b.schedBorrowReq.userID != 0 && issuerID != b.schedBorrowReq.userID && !b.isAdmin(issuerID) {
			b.queueMu.Unlock()
			_ = b.sendMessageWithReply(chatID, notYours, nil, replyToID)
			return
		}
		if b.schedBorrowReq.cancel != nil {
			b.schedBorrowReq.cancel()
		}
		b.queueMu.Unlock()
		_ = b.sendMessageWithReply(chatID, fmt.Sprintf("⛔ Task %s stopped.", taskID), nil, replyToID)
		return
	}
	// Check in-flight uploads (task-concurrency head): once a head finishes its
	// download phase the scheduler promotes the next head into activeReq, so a head
	// still uploading is neither activeReq nor the borrower nor in the queue. Without
	// this it would falsely report "not found" and the upload couldn't be stopped.
	if req, ok := b.uploadingReqs[taskID]; ok {
		if req.userID != 0 && issuerID != req.userID && !b.isAdmin(issuerID) {
			b.queueMu.Unlock()
			_ = b.sendMessageWithReply(chatID, notYours, nil, replyToID)
			return
		}
		if req.cancel != nil {
			req.cancel()
		}
		b.queueMu.Unlock()
		_ = b.sendMessageWithReply(chatID, fmt.Sprintf("⛔ Task %s stopped.", taskID), nil, replyToID)
		return
	}
	// Check queued tasks — drain and re-enqueue without the target
	found := false
	denied := false
	queueLen := len(b.downloadQueue)
drain:
	for i := 0; i < queueLen; i++ {
		select {
		case req := <-b.downloadQueue:
			switch {
			case req.taskID != taskID:
				b.downloadQueue <- req
			case req.userID != 0 && issuerID != req.userID && !b.isAdmin(issuerID):
				// Not the owner (and not an admin) — put it back untouched.
				b.downloadQueue <- req
				denied = true
			default:
				if req.cancel != nil {
					req.cancel()
				}
				b.removeQueuedReqLocked(req.taskID)
				// A queued task never reaches the worker's completion decrement
				// (telegram_bot.go ~603), so release its slot here or the user stays
				// pinned at the pending cap.
				if req.userID != 0 && b.userTaskCount[req.userID] > 0 {
					b.userTaskCount[req.userID]--
				}
				found = true
			}
		default:
			// A bare `break` here would only break the select, leaving the for loop
			// spinning the remaining count. Break the loop once the queue is drained.
			break drain
		}
	}
	b.queueMu.Unlock()
	switch {
	case found:
		_ = b.sendMessageWithReply(chatID, fmt.Sprintf("⛔ Queued task %s stopped.", taskID), nil, replyToID)
	case denied:
		_ = b.sendMessageWithReply(chatID, notYours, nil, replyToID)
	default:
		_ = b.sendMessageWithReply(chatID, fmt.Sprintf("Task %s not found.", taskID), nil, replyToID)
	}
}
func botHelpText() string {
	return strings.TrimSpace(`
Commands:
/dl <apple-music-link> [flags]   download a song, album, artist, or playlist
/count <apple-music-link>        count the streamable tracks behind a link
/status or /queue                show active task and queue count
/stop_<task_id>                  cancel a running or queued download
/profile                         set your saved rip preferences (buttons)
/help                            show this message

Delivery: playlists over 40 tracks are always delivered as a Gofile ZIP
(sending dozens of individual files trips Telegram's rate limits). Very heavy
rips (full artist discographies, playlists over 100 tracks) also wait for the
2:30–6:00 AM Dhaka window.

Default quality is ALAC. -aac and -atmos pick a different source codec; -flac
re-encodes the downloaded audio to FLAC. If you combine codec flags, one wins.

Flags:
  -aac     download in AAC-LC format
  -atmos   download in Dolby Atmos format
  -flac    convert the downloaded audio to FLAC
  -art     grab cover art + motion artwork (no audio)
  -nc      no-cache: delete any cached copy and re-rip fresh
  -tgu     send as individual Telegram tracks (skip keyboard)
  -tgz     send as Telegram ZIP (skip keyboard)
  -go      send as Gofile ZIP (skip keyboard)
`)
}

// botHelpRich is the Bot API 10.1 Rich Markdown rendering of the help text:
// real headings, a flags table, and code-formatted command examples. Falls back
// to botHelpText() when the API can't serve rich content.
func botHelpRich() string {
	return strings.TrimSpace(`
# Karen — Apple Music downloader

Rips lossless ALAC, Dolby Atmos, and AAC from Apple Music straight to your chat.

## Commands

| Command | What it does |
|:--------|:-------------|
| ` + "`/dl <link> [flags]`" + ` | Download a song, album, artist, or playlist |
| ` + "`/count <link>`" + ` | Count the streamable tracks behind a link |
| ` + "`/status`" + ` or ` + "`/queue`" + ` | Show the active task and queue |
| ` + "`/stop_<task_id>`" + ` | Cancel a running or queued download |
| ` + "`/profile`" + ` | Set your saved rip preferences (all buttons) |
| ` + "`/help`" + ` | Show this message |

> Playlists over **40 tracks** are always delivered as a Gofile ZIP (dozens of individual uploads trip Telegram's rate limits). Very heavy rips (full artist discographies, playlists over 100 tracks) also wait for the 2:30–6:00 AM Dhaka window.
>
> Default quality is ALAC. ` + "`-aac`/`-atmos`" + ` pick a different source codec; ` + "`-flac`" + ` re-encodes after download. If you combine codec flags, one wins.

## Flags

| Flag | Effect |
|:-----|:-------|
| ` + "`-aac`" + ` | Download in AAC-LC format |
| ` + "`-atmos`" + ` | Download in Dolby Atmos format |
| ` + "`-flac`" + ` | Convert the downloaded audio to FLAC |
| ` + "`-art`" + ` | Grab cover art + motion artwork (no audio) |
| ` + "`-nc`" + ` | No-cache: delete any cached copy and re-rip fresh |
| ` + "`-tgu`" + ` | Send as individual Telegram tracks |
| ` + "`-tgz`" + ` | Send as a Telegram ZIP |
| ` + "`-go`" + ` | Send as a Gofile ZIP |

## Examples

` + "```" + `
/dl https://music.apple.com/album/123456
/dl https://music.apple.com/album/123456 -aac
/dl https://music.apple.com/song/789012 -atmos -tgz
` + "```" + `
`)
}
