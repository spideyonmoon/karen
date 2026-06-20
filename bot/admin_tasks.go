package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	apputils "main/utils"
	"main/utils/ampapi"
)

// =============================================================================
// Persistent bot state (admin lock + scheduled sleeptime jobs)
//
// Lives in telegram-state.json next to the media cache, but kept separate so a
// cache migration/corruption can't take down the admin lock and vice versa. The
// file MUST be bind-mounted into the container (docker-compose.yml) or it is
// wiped on every --build deploy, exactly like the mtproto-session.json gap.
// =============================================================================

// scheduledJob is a heavy rip (artist discography or >100-track playlist)
// submitted by a non-admin outside the Dhaka sleeptime window. It is persisted
// and re-armed on restart. Times are stored as unix seconds to dodge any
// timezone-in-JSON ambiguity.
type scheduledJob struct {
	ID            string `json:"id"`
	Kind          string `json:"kind"` // "artist" | "playlist"
	ChatID        int64  `json:"chat_id"`
	UserID        int64  `json:"user_id"`
	Username      string `json:"username"`
	ReplyToID     int    `json:"reply_to_id"`
	Link          string `json:"link"`
	Storefront    string `json:"storefront"`
	ResourceID    string `json:"resource_id"`
	ForceAAC      bool   `json:"force_aac"`
	ForceAtmos    bool   `json:"force_atmos"`
	ForceFlac     bool   `json:"force_flac"`
	ReleaseAtUnix int64  `json:"release_at_unix"`
	CreatedAtUnix int64  `json:"created_at_unix"`
}

func (j *scheduledJob) releaseTime() time.Time { return time.Unix(j.ReleaseAtUnix, 0) }

// UserPrefs is a single user's saved rip profile. Every field is "unset" at its
// zero value (empty string, nil pointer, or 0), and an unset field means "inherit
// the global config default" — so a user who has never touched /profile sees the
// exact same behavior as before this feature existed. Pointers are used for the
// tri-state booleans (nil = inherit, &true / &false = explicit) so a saved "off"
// is distinguishable from "never set". Keyed by Telegram user ID so a profile
// follows the person across chats.
type UserPrefs struct {
	// Core
	Codec          string `json:"codec"`           // "" | alac | flac | aac | atmos
	Quality        string `json:"quality"`         // "" | redbook | hires  (AlacMax/AtmosMax)
	LyricMode      string `json:"lyric_mode"`      // "" | off | static | synced | word
	EmbedLrc       *bool  `json:"embed_lrc"`       // nil = inherit config default
	CoverDelivery  string `json:"cover_delivery"`  // "" | photo | document
	AnimatedArt    *bool  `json:"animated_art"`    // include motion artwork
	EmbedCover     *bool  `json:"embed_cover"`     // embed cover in the audio file
	DeliveryTarget string `json:"delivery_target"` // "" | ask | telegram | telegram_zip | gofile
	// Selected extras
	AacType        string `json:"aac_type"`        // "" | aac-lc | aac | he-aac | binaural | downmix
	ExplicitChoice string `json:"explicit_choice"` // "" | explicit | clean
	AppleMaster    *bool  `json:"apple_master"`    // prefer Apple Digital Master
	CoverFormat    string `json:"cover_format"`    // "" | jpg | png | original
	CoverSize      string `json:"cover_size"`      // "" | e.g. 1000x1000 | 3000x3000
	Language       string `json:"language"`        // "" | metadata/lyric locale (e.g. ja, en)
	MVMax          int    `json:"mv_max"`          // 0=inherit | 360|480|720|1080|2160 (max cap)
	ArtistZip      string `json:"artist_zip"`      // "" | per_release | combined
	SilentDelivery *bool  `json:"silent_delivery"` // disable_notification on sends
	UpdatedAtUnix  int64  `json:"updated_at_unix"`
}

// telegramStateFile is the IMPORTANT, DM-backed-up state: the admin lock and
// every user's saved /profile. ScheduledJobs is retained only for one-time
// backward-compat reading of pre-split telegram-state.json files (it is no
// longer written here — see scheduleStateFile).
type telegramStateFile struct {
	Version       int                  `json:"version"`
	AdminLock     bool                 `json:"admin_lock"`
	ScheduledJobs []*scheduledJob      `json:"scheduled_jobs,omitempty"`
	UserPrefs     map[int64]*UserPrefs `json:"user_prefs"`
}

// scheduleStateFile is the ephemeral sleeptime-rip queue, kept in its own file
// (telegram-schedule.json) so it is NOT swept into the daily admin backup — it
// is best-effort working state, not user data worth preserving across a VPS loss.
type scheduleStateFile struct {
	Version       int             `json:"version"`
	ScheduledJobs []*scheduledJob `json:"scheduled_jobs"`
}

// loadState reads telegram-state.json into adminLock + scheduledJobs. A missing
// file means defaults (lock open, no jobs) — the zero values already give that.
func (b *TelegramBot) loadState() {
	b.stateMu.Lock()
	defer b.stateMu.Unlock()
	b.adminLock = false
	b.scheduledJobs = nil
	b.userPrefs = make(map[int64]*UserPrefs)
	if b.stateFile == "" {
		return
	}
	data, err := os.ReadFile(b.stateFile)
	if err != nil || len(data) == 0 {
		// Missing or freshly-touched empty file → defaults (lock open, no jobs).
		return
	}
	var payload telegramStateFile
	if err := json.Unmarshal(data, &payload); err != nil {
		fmt.Println("state load: corrupt telegram-state.json, starting fresh:", err)
		return
	}
	b.adminLock = payload.AdminLock
	b.scheduledJobs = payload.ScheduledJobs
	if payload.UserPrefs != nil {
		b.userPrefs = payload.UserPrefs
	}
}

// saveStateLocked persists current state. Caller must hold stateMu. Mirrors
// saveCacheLocked: atomic tmp + rename so a crash mid-write can't truncate it.
func (b *TelegramBot) saveStateLocked() {
	if b.stateFile == "" {
		return
	}
	dir := filepath.Dir(b.stateFile)
	if dir != "." && dir != "" {
		_ = os.MkdirAll(dir, 0755)
	}
	payload := telegramStateFile{
		Version:   1,
		AdminLock: b.adminLock,
		UserPrefs: b.userPrefs,
		// ScheduledJobs intentionally omitted — it now lives in telegram-schedule.json.
	}
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return
	}
	tmp := b.stateFile + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return
	}
	_ = os.Rename(tmp, b.stateFile)
}

// loadSchedule reads telegram-schedule.json into scheduledJobs. It runs AFTER
// loadState, so if the schedule file is absent but loadState parsed jobs out of a
// pre-split telegram-state.json, those legacy jobs are migrated into the new file
// here. A missing/empty file with no legacy jobs just means "no pending rips".
func (b *TelegramBot) loadSchedule() {
	b.stateMu.Lock()
	defer b.stateMu.Unlock()
	if b.scheduleFile == "" {
		return
	}
	data, err := os.ReadFile(b.scheduleFile)
	if err != nil || len(data) == 0 {
		if len(b.scheduledJobs) > 0 {
			// Legacy jobs carried over by loadState → persist into the new file.
			b.saveScheduleLocked()
		}
		return
	}
	var payload scheduleStateFile
	if err := json.Unmarshal(data, &payload); err != nil {
		fmt.Println("schedule load: corrupt telegram-schedule.json, starting fresh:", err)
		return
	}
	b.scheduledJobs = payload.ScheduledJobs
}

// saveScheduleLocked persists the pending sleeptime-rip queue. Caller must hold
// stateMu. Atomic tmp+rename like saveStateLocked.
func (b *TelegramBot) saveScheduleLocked() {
	if b.scheduleFile == "" {
		return
	}
	dir := filepath.Dir(b.scheduleFile)
	if dir != "." && dir != "" {
		_ = os.MkdirAll(dir, 0755)
	}
	payload := scheduleStateFile{
		Version:       1,
		ScheduledJobs: b.scheduledJobs,
	}
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return
	}
	tmp := b.scheduleFile + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return
	}
	_ = os.Rename(tmp, b.scheduleFile)
}

// getPrefs returns a copy of userID's saved profile, or a zero-value UserPrefs
// (every field "unset" → inherit global config) when the user has none. A copy is
// returned so callers can read it without holding stateMu; the tri-state *bool
// pointers are shared, but callers only ever read through them.
func (b *TelegramBot) getPrefs(userID int64) UserPrefs {
	b.stateMu.Lock()
	defer b.stateMu.Unlock()
	if p, ok := b.userPrefs[userID]; ok && p != nil {
		return *p
	}
	return UserPrefs{}
}

// hasPrefs reports whether userID has any saved profile row at all (used to skip
// work for the common no-profile case).
func (b *TelegramBot) hasPrefs(userID int64) bool {
	b.stateMu.Lock()
	defer b.stateMu.Unlock()
	_, ok := b.userPrefs[userID]
	return ok
}

// setPrefs mutates userID's profile under stateMu via the supplied callback,
// stamps UpdatedAtUnix, and persists the whole state file. The row is created on
// first use. A nil mutate is a no-op.
func (b *TelegramBot) setPrefs(userID int64, mutate func(*UserPrefs)) {
	if mutate == nil {
		return
	}
	b.stateMu.Lock()
	defer b.stateMu.Unlock()
	if b.userPrefs == nil {
		b.userPrefs = make(map[int64]*UserPrefs)
	}
	p := b.userPrefs[userID]
	if p == nil {
		p = &UserPrefs{}
		b.userPrefs[userID] = p
	}
	mutate(p)
	p.UpdatedAtUnix = time.Now().Unix()
	b.saveStateLocked()
}

// resetPrefs drops userID's entire profile (every field back to inherit-global)
// and persists. A no-op if the user had none.
func (b *TelegramBot) resetPrefs(userID int64) {
	b.stateMu.Lock()
	defer b.stateMu.Unlock()
	if _, ok := b.userPrefs[userID]; !ok {
		return
	}
	delete(b.userPrefs, userID)
	b.saveStateLocked()
}

// =============================================================================
// Admin helpers
// =============================================================================

// isAdmin reports whether userID is in the configured admin list. An empty admin
// list means nobody is an admin: /auth /unauth /purge become inert and the /stop
// override never applies — the safe default.
func (b *TelegramBot) isAdmin(userID int64) bool {
	return userID != 0 && b.admins[userID]
}

// isLocked reports whether the bot is in admins-only mode.
func (b *TelegramBot) isLocked() bool {
	b.stateMu.Lock()
	defer b.stateMu.Unlock()
	return b.adminLock
}

func (b *TelegramBot) setAdminLock(on bool) {
	b.stateMu.Lock()
	b.adminLock = on
	b.saveStateLocked()
	b.stateMu.Unlock()
}

// adminPurge wipes the local download caches on demand. It refuses while a
// transfer is in progress (purgeDownloadCaches also self-guards) so it can never
// yank files out from under an active upload.
func (b *TelegramBot) adminPurge(chatID int64, replyToID int) {
	b.queueMu.Lock()
	busy := b.inProgress
	b.queueMu.Unlock()
	if busy {
		_ = b.sendMessageWithReply(chatID, "A transfer is in progress — purge skipped. Try again when idle.", nil, replyToID)
		return
	}
	b.purgeDownloadCaches()
	_ = b.sendMessageWithReply(chatID, "🧹 Cached downloads purged.", nil, replyToID)
}

// =============================================================================
// Dhaka sleeptime window + scheduler
// =============================================================================

// dhakaZone is UTC+6. Bangladesh observes no DST, so a fixed zone avoids any
// tzdata dependency in the container.
var dhakaZone = time.FixedZone("Asia/Dhaka", 6*3600)

const (
	sleepWindowStartMin = 2*60 + 30 // 02:30
	sleepWindowEndMin   = 6 * 60    // 06:00
)

// inSleepWindow reports whether t (converted to Dhaka local) is within
// [02:30, 06:00).
func inSleepWindow(t time.Time) bool {
	lt := t.In(dhakaZone)
	mins := lt.Hour()*60 + lt.Minute()
	return mins >= sleepWindowStartMin && mins < sleepWindowEndMin
}

// nextWindowStart returns the next 02:30 Dhaka strictly after t.
func nextWindowStart(t time.Time) time.Time {
	lt := t.In(dhakaZone)
	start := time.Date(lt.Year(), lt.Month(), lt.Day(), 2, 30, 0, 0, dhakaZone)
	if !start.After(lt) {
		start = start.AddDate(0, 0, 1)
	}
	return start
}

// startScheduler launches the single-ticker scheduler. It catches up immediately
// on boot (releasing any job whose window passed while the bot was down — e.g.
// across a deploy) then polls once a minute. A coarse tick is fine for a 3.5h
// window and, unlike per-job timers, reconstructs purely from persisted state.
func (b *TelegramBot) startScheduler() {
	go func() {
		b.releaseDueJobs()
		t := time.NewTicker(60 * time.Second)
		defer t.Stop()
		for range t.C {
			b.releaseDueJobs()
		}
	}()
}

// releaseDueJobs removes every job whose release time has passed (under stateMu,
// before releasing) and dispatches each onto the normal download queue. stateMu
// is a leaf lock: it is dropped before releaseJob, which takes queueMu via the
// enqueue* funcs — never the two locks nested.
func (b *TelegramBot) releaseDueJobs() {
	now := time.Now()
	b.stateMu.Lock()
	if len(b.scheduledJobs) == 0 {
		b.stateMu.Unlock()
		return
	}
	var due, remaining []*scheduledJob
	for _, j := range b.scheduledJobs {
		if j.releaseTime().After(now) {
			remaining = append(remaining, j)
		} else {
			due = append(due, j)
		}
	}
	if len(due) > 0 {
		b.scheduledJobs = remaining
		b.saveScheduleLocked()
	}
	b.stateMu.Unlock()
	for _, j := range due {
		b.releaseJob(j)
	}
}

// releaseJob dispatches a due job onto the download queue, always forced to
// Gofile ZIP delivery. Must be called WITHOUT stateMu held.
func (b *TelegramBot) releaseJob(j *scheduledJob) {
	switch j.Kind {
	case "playlist":
		_ = b.sendMessageWithReply(j.ChatID, "🌙 Sleeptime reached — starting your scheduled playlist rip (Gofile ZIP).", nil, j.ReplyToID)
		b.enqueuePlaylistDownload(j.ChatID, j.ResourceID, j.ReplyToID, 0, transferModeGofileZip, j.ForceAAC, j.ForceAtmos, j.ForceFlac, false, j.UserID, j.Username)
	case "artist":
		_ = b.sendMessageWithReply(j.ChatID, "🌙 Sleeptime reached — starting your scheduled artist rip (Gofile ZIP).", nil, j.ReplyToID)
		b.runArtistRip(j.ChatID, j.UserID, j.Username, j.Storefront, j.ResourceID, j.ReplyToID, j.ForceAAC, j.ForceAtmos, j.ForceFlac)
	}
}

// scheduleJob persists a future job.
func (b *TelegramBot) scheduleJob(j *scheduledJob) {
	b.stateMu.Lock()
	b.scheduledJobs = append(b.scheduledJobs, j)
	b.saveScheduleLocked()
	b.stateMu.Unlock()
}

// scheduleOrRun runs a heavy job immediately if we're inside the sleeptime
// window, otherwise persists it for the next window and tells the user when.
// May block on network (artist enumeration) when running immediately, so callers
// on the update loop should invoke it in a goroutine.
func (b *TelegramBot) scheduleOrRun(j *scheduledJob) {
	now := time.Now()
	if inSleepWindow(now) {
		b.releaseJob(j)
		return
	}
	j.ID = generateTaskID()
	j.ReleaseAtUnix = nextWindowStart(now).Unix()
	j.CreatedAtUnix = now.Unix()
	b.scheduleJob(j)

	label := "rip"
	switch j.Kind {
	case "artist":
		label = "artist rip"
	case "playlist":
		label = "playlist rip"
	}
	when := nextWindowStart(now).In(dhakaZone).Format("Mon Jan 2, 3:04 PM")
	_ = b.sendMessageWithReply(j.ChatID, fmt.Sprintf(
		"🌙 This %s is heavy, so it's scheduled for the sleeptime window. It'll start around %s (Dhaka time) and deliver as a Gofile ZIP.",
		label, when), nil, j.ReplyToID)
}

// =============================================================================
// Daily state backup to admin DM
// =============================================================================

// backupHourDhaka is the daily wall-clock hour (Dhaka) at which the bot DMs each
// admin a copy of telegram-state.json. 04:00 sits inside the quiet sleeptime
// window, after the 02:30 scheduled rips have kicked off.
const backupHourDhaka = 4

// nextDailyAt returns the next hour:min Dhaka strictly after t.
func nextDailyAt(t time.Time, hour, min int) time.Time {
	lt := t.In(dhakaZone)
	next := time.Date(lt.Year(), lt.Month(), lt.Day(), hour, min, 0, 0, dhakaZone)
	if !next.After(lt) {
		next = next.AddDate(0, 0, 1)
	}
	return next
}

// startBackupRoutine DMs every admin a copy of the important state (admin lock +
// user profiles) once a day at backupHourDhaka, so a VPS loss costs at most one
// day of profile changes. The fire time is computed from the clock rather than
// "every 24h from boot", so frequent restarts/deploys never spam backups. The
// scheduled-rip queue is deliberately excluded — it is best-effort working state,
// not user data worth preserving.
func (b *TelegramBot) startBackupRoutine() {
	if len(b.admins) == 0 {
		return // nobody to back up to
	}
	go func() {
		for {
			time.Sleep(time.Until(nextDailyAt(time.Now(), backupHourDhaka, 0)))
			b.performBackup()
		}
	}()
}

// performBackup marshals a snapshot of admin lock + user profiles and sends it as
// a JSON document to every configured admin's DM. Errors are logged, never fatal.
func (b *TelegramBot) performBackup() {
	b.stateMu.Lock()
	payload := telegramStateFile{
		Version:   1,
		AdminLock: b.adminLock,
		UserPrefs: b.userPrefs,
	}
	profileCount := len(b.userPrefs)
	locked := b.adminLock
	data, err := json.MarshalIndent(payload, "", "  ")
	b.stateMu.Unlock()
	if err != nil {
		fmt.Println("backup: marshal failed:", err)
		return
	}

	day := time.Now().In(dhakaZone).Format("2006-01-02")
	name := fmt.Sprintf("karen-state-%s.json", day)

	// sendDocumentFile streams from a path, so spill the snapshot to a temp file
	// in the (bind-mounted) state dir and clean it up afterward.
	tmp := filepath.Join(filepath.Dir(b.stateFile), ".backup-"+name)
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		fmt.Println("backup: write temp failed:", err)
		return
	}
	defer os.Remove(tmp)

	lockState := "off"
	if locked {
		lockState = "on"
	}
	header := fmt.Sprintf("🗄️ Daily backup — %d profile(s), admin-lock %s.", profileCount, lockState)

	for adminID := range b.admins {
		_ = b.sendMessageWithReply(adminID, header, nil, 0)
		if err := b.sendDocumentFile(adminID, tmp, name, 0, nil, ""); err != nil {
			fmt.Printf("backup: send to %d failed: %v\n", adminID, err)
		}
	}
}

// =============================================================================
// Artist rips (re-enabled): fan an artist out into Gofile-ZIP job(s)
// =============================================================================

// runArtistRip enumerates every album for an artist and enqueues them as
// Gofile-ZIP downloads. The user's ArtistZip profile pref selects between:
//   - "per_release" / "" (default): one queue slot per album, each zipped and
//     uploaded separately (N Gofile links).
//   - "combined": one queue slot for the whole discography, all albums
//     downloaded sequentially into a single RipState, then one combined ZIP
//     uploaded to Gofile (1 Gofile link).
//
// Blocks on network (album enumeration) — call in a goroutine from the update
// loop.
func (b *TelegramBot) runArtistRip(chatID, userID int64, username, storefront, artistID string, replyToID int, forceAAC, forceAtmos, forceFlac bool) {
	if storefront == "" {
		storefront = Config.Storefront
	}
	albums, _, err := apputils.FetchArtistAlbums(storefront, artistID, b.appleToken, 0, 0, b.searchLanguage())
	if err != nil {
		_ = b.sendMessageWithReply(chatID, fmt.Sprintf("Failed to load artist albums: %v", err), nil, replyToID)
		return
	}
	if len(albums) == 0 {
		_ = b.sendMessageWithReply(chatID, "No albums found for this artist.", nil, replyToID)
		return
	}

	combined := userID != 0 && b.getPrefs(userID).ArtistZip == "combined"

	if combined {
		b.runArtistRipCombined(chatID, userID, username, storefront, artistID, albums, replyToID, forceAAC, forceAtmos, forceFlac)
	} else {
		b.runArtistRipPerRelease(chatID, userID, username, albums, replyToID, forceAAC, forceAtmos, forceFlac)
	}
}

// runArtistRipPerRelease is the default: one queue slot per album.
func (b *TelegramBot) runArtistRipPerRelease(chatID, userID int64, username string, albums []apputils.SearchResultItem, replyToID int, forceAAC, forceAtmos, forceFlac bool) {
	queued := 0
	for _, alb := range albums {
		if alb.ID == "" {
			continue
		}
		if !b.enqueueAlbumDownloadChecked(chatID, alb.ID, replyToID, transferModeGofileZip, forceAAC, forceAtmos, forceFlac, userID, username) {
			_ = b.sendMessageWithReply(chatID, fmt.Sprintf(
				"⚠️ Download queue is full — queued %d of %d album(s). Re-send the artist link later for the rest.",
				queued, len(albums)), nil, replyToID)
			return
		}
		queued++
	}
	_ = b.sendMessageWithReply(chatID, fmt.Sprintf("📀 Queued %d album(s) for the artist rip (Gofile ZIP).", queued), nil, replyToID)
}

// runArtistRipCombined enqueues one download request that iterates every album
// sequentially. All tracks accumulate in the same RipState, so delivery creates
// a single combined ZIP uploaded to Gofile.
func (b *TelegramBot) runArtistRipCombined(chatID, userID int64, username, storefront, artistID string, albums []apputils.SearchResultItem, replyToID int, forceAAC, forceAtmos, forceFlac bool) {
	format := b.resolveFormat(chatID, forceFlac)
	ok := b.enqueueDownloadWithAfter(chatID, userID, username, replyToID, 0, false, format, transferModeGofileZip, "artist:"+artistID, false, func(ctx context.Context) error {
		if forceAtmos {
			if rs := ripStateFrom(ctx); rs != nil {
				rs.Atmos = true
			} else {
				dl_atmos = true
			}
		}
		for i, alb := range albums {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if alb.ID == "" {
				continue
			}
			if err := ripAlbum(alb.ID, b.appleToken, storefront, "", forceAAC, ctx); err != nil {
				fmt.Printf("Artist combined rip: album %d/%d (%s) failed: %v\n", i+1, len(albums), alb.ID, err)
			}
		}
		return nil
	}, nil)
	if ok {
		_ = b.sendMessageWithReply(chatID, fmt.Sprintf("📀 Queued artist rip (%d albums → combined ZIP).", len(albums)), nil, replyToID)
	}
}

// enqueueAlbumDownloadChecked mirrors enqueueAlbumDownload but returns whether
// the album was accepted onto the queue (false == queue full), so the artist
// fan-out can stop cleanly instead of silently dropping albums.
func (b *TelegramBot) enqueueAlbumDownloadChecked(chatID int64, albumID string, replyToID int, transferMode string, forceAAC, forceAtmos, forceFlac bool, userID int64, username string) bool {
	if albumID == "" {
		return true
	}
	format := b.resolveFormat(chatID, forceFlac)
	return b.enqueueDownloadWithAfter(chatID, userID, username, replyToID, 0, false, format, transferMode, albumID, false, func(ctx context.Context) error {
		if forceAtmos {
			if rs := ripStateFrom(ctx); rs != nil {
				rs.Atmos = true
			} else {
				dl_atmos = true
			}
		}
		return ripAlbum(albumID, b.appleToken, Config.Storefront, "", forceAAC, ctx)
	}, nil)
}

// playlistSleeptimeThreshold is the track count above which a non-admin's
// playlist rip is deferred to the sleeptime window (forced Gofile ZIP).
const playlistSleeptimeThreshold = 100

// routePlaylistNonAdmin fetches a playlist's track count and, if it exceeds the
// threshold, schedules it for the sleeptime window; otherwise it runs the normal
// delivery path. A count failure falls through to the normal path so a transient
// API error never blocks the user. Blocking HTTP — call in a goroutine.
func (b *TelegramBot) routePlaylistNonAdmin(chatID, userID int64, storefront, playlistID, link string, replyToID int, headlessMode string, forceAAC, forceAtmos, forceFlac, noCache bool) {
	resp, err := ampapi.GetPlaylistResp(orStorefront(storefront), playlistID, b.searchLanguage(), b.appleToken)
	if err != nil || resp == nil || len(resp.Data) == 0 {
		b.dispatchPlaylistNormal(chatID, userID, playlistID, replyToID, headlessMode, forceAAC, forceAtmos, forceFlac, noCache)
		return
	}
	if len(resp.Data[0].Relationships.Tracks.Data) > playlistSleeptimeThreshold {
		b.scheduleOrRun(&scheduledJob{
			Kind:       "playlist",
			ChatID:     chatID,
			UserID:     userID,
			ReplyToID:  replyToID,
			Link:       link,
			Storefront: storefront,
			ResourceID: playlistID,
			ForceAAC:   forceAAC,
			ForceAtmos: forceAtmos,
			ForceFlac:  forceFlac,
		})
		return
	}
	b.dispatchPlaylistNormal(chatID, userID, playlistID, replyToID, headlessMode, forceAAC, forceAtmos, forceFlac, noCache)
}

// dispatchPlaylistNormal is the pre-existing playlist behavior: headless enqueue
// when a delivery flag was given, otherwise the delivery-mode keyboard prompt.
func (b *TelegramBot) dispatchPlaylistNormal(chatID, userID int64, playlistID string, replyToID int, headlessMode string, forceAAC, forceAtmos, forceFlac, noCache bool) {
	if headlessMode != "" {
		b.enqueuePlaylistDownload(chatID, playlistID, replyToID, 0, headlessMode, forceAAC, forceAtmos, forceFlac, noCache, userID, "")
	} else {
		b.queueDownloadPlaylistWithReply(chatID, userID, playlistID, replyToID, forceAAC, forceAtmos, forceFlac, noCache)
	}
}

// =============================================================================
// /count — streamable (rip-able) track count for any Apple Music link
// =============================================================================

const artistCountAlbumCap = 50 // above this, /count skips the per-album scan

// countStreamable counts tracks that are actually playable/rip-able: a track
// with an empty PlayParams.ID is unavailable (region-locked, pulled, etc.).
func countStreamable(tracks []ampapi.TrackRespData) int {
	n := 0
	for _, t := range tracks {
		if t.Attributes.PlayParams.ID != "" {
			n++
		}
	}
	return n
}

func orStorefront(sf string) string {
	if sf == "" {
		return Config.Storefront
	}
	return sf
}

// handleCount replies with the number of streamable tracks behind a link. Runs
// on its own goroutine (album/artist fetches are blocking HTTP) so it never
// stalls the update loop.
func (b *TelegramBot) handleCount(chatID int64, link string, replyToID int) {
	link = strings.TrimSpace(link)

	if _, mvID := checkUrlMv(link); mvID != "" {
		_ = b.sendMessageWithReply(chatID, "🎬 That's a music video — 1 rip-able item.", nil, replyToID)
		return
	}
	if _, songID := checkUrlSong(link); songID != "" {
		_ = b.sendMessageWithReply(chatID, "🎵 1 streamable track.", nil, replyToID)
		return
	}
	if sf, albumID := checkUrl(link); albumID != "" {
		// A shared-song link is an album URL with ?i=<songID>; count it as one track.
		if songIDFromURLParam(link) != "" {
			_ = b.sendMessageWithReply(chatID, "🎵 1 streamable track.", nil, replyToID)
			return
		}
		resp, err := ampapi.GetAlbumResp(orStorefront(sf), albumID, b.searchLanguage(), b.appleToken)
		if err != nil || resp == nil || len(resp.Data) == 0 {
			_ = b.sendMessageWithReply(chatID, "Couldn't load that album.", nil, replyToID)
			return
		}
		tracks := resp.Data[0].Relationships.Tracks.Data
		n := countStreamable(tracks)
		_ = b.sendMessageWithReply(chatID, fmt.Sprintf("💿 %q — %d streamable track(s)%s.",
			resp.Data[0].Attributes.Name, n, unavailableNote(len(tracks)-n)), nil, replyToID)
		return
	}
	if sf, playlistID := checkUrlPlaylist(link); playlistID != "" {
		resp, err := ampapi.GetPlaylistResp(orStorefront(sf), playlistID, b.searchLanguage(), b.appleToken)
		if err != nil || resp == nil || len(resp.Data) == 0 {
			_ = b.sendMessageWithReply(chatID, "Couldn't load that playlist.", nil, replyToID)
			return
		}
		tracks := resp.Data[0].Relationships.Tracks.Data
		n := countStreamable(tracks)
		_ = b.sendMessageWithReply(chatID, fmt.Sprintf("🎶 %q — %d streamable track(s)%s.",
			resp.Data[0].Attributes.Name, n, unavailableNote(len(tracks)-n)), nil, replyToID)
		return
	}
	if _, stationID := checkUrlStation(link); stationID != "" {
		_ = b.sendMessageWithReply(chatID, "📻 Stations stream continuously and have no fixed track count.", nil, replyToID)
		return
	}
	if sf, artistID := checkUrlArtist(link); artistID != "" {
		b.countArtist(chatID, orStorefront(sf), artistID, replyToID)
		return
	}
	_ = b.sendMessageWithReply(chatID, "Invalid Apple Music link.", nil, replyToID)
}

// countArtist sums streamable tracks across an artist's albums. For artists with
// more albums than artistCountAlbumCap it bails with the album count rather than
// firing dozens of sequential album fetches.
func (b *TelegramBot) countArtist(chatID int64, storefront, artistID string, replyToID int) {
	albums, _, err := apputils.FetchArtistAlbums(storefront, artistID, b.appleToken, 0, 0, b.searchLanguage())
	if err != nil {
		_ = b.sendMessageWithReply(chatID, fmt.Sprintf("Failed to load artist albums: %v", err), nil, replyToID)
		return
	}
	if len(albums) == 0 {
		_ = b.sendMessageWithReply(chatID, "No albums found for this artist.", nil, replyToID)
		return
	}
	if len(albums) > artistCountAlbumCap {
		_ = b.sendMessageWithReply(chatID, fmt.Sprintf(
			"👤 This artist has %d albums — too many to tally track-by-track. Send an album or playlist link for an exact count.",
			len(albums)), nil, replyToID)
		return
	}
	_ = b.sendMessageWithReply(chatID, fmt.Sprintf("🔎 Counting streamable tracks across %d album(s)…", len(albums)), nil, replyToID)
	total := 0
	for _, alb := range albums {
		if alb.ID == "" {
			continue
		}
		resp, err := ampapi.GetAlbumResp(storefront, alb.ID, b.searchLanguage(), b.appleToken)
		if err != nil || resp == nil || len(resp.Data) == 0 {
			continue
		}
		total += countStreamable(resp.Data[0].Relationships.Tracks.Data)
	}
	_ = b.sendMessageWithReply(chatID, fmt.Sprintf("👤 %d streamable track(s) across %d album(s).", total, len(albums)), nil, replyToID)
}

func unavailableNote(unavailable int) string {
	if unavailable <= 0 {
		return ""
	}
	return fmt.Sprintf(" (%d unavailable)", unavailable)
}
