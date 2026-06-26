package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

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
	Kind          string `json:"kind"`              // "artist" | "playlist"
	Section       string `json:"section,omitempty"` // artist scope: "" (all) | full-albums | singles | music-videos
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

// UserStats is a single user's lifetime usage tally, kept alongside (but separate
// from) their UserPrefs so that tapping "Reset" in /profile wipes preferences
// without erasing the usage history. Every field is a monotonic counter that only
// ever grows; zero values mean "never did that yet". Keyed by Telegram user ID,
// persisted in telegram-state.json and swept into the daily admin backup.
type UserStats struct {
	TotalCommands int64 `json:"total_commands"` // every recognized command issued
	AlbumRips     int64 `json:"album_rips"`     // /dl album links
	ArtistRips    int64 `json:"artist_rips"`    // /dl artist links (any scope)
	PlaylistRips  int64 `json:"playlist_rips"`  // /dl playlist links
	SongRips      int64 `json:"song_rips"`      // /dl single-song links
	Cancels       int64 `json:"cancels"`        // /stop_* + /cancel_* they issued
	FirstSeenUnix int64 `json:"first_seen_unix"`
	LastSeenUnix  int64 `json:"last_seen_unix"`
}

// telegramStateFile is the IMPORTANT, DM-backed-up state: the admin lock and
// every user's saved /profile. ScheduledJobs is retained only for one-time
// backward-compat reading of pre-split telegram-state.json files (it is no
// longer written here — see scheduleStateFile).
type telegramStateFile struct {
	Version          int                  `json:"version"`
	AdminLock        bool                 `json:"admin_lock"`
	ScheduledJobs    []*scheduledJob      `json:"scheduled_jobs,omitempty"`
	UserPrefs        map[int64]*UserPrefs `json:"user_prefs"`
	UserStats        map[int64]*UserStats `json:"user_stats,omitempty"`
	BlockedUserIDs   []int64              `json:"blocked_user_ids,omitempty"`
	BlockedUsernames []string             `json:"blocked_usernames,omitempty"`
	DonorUserIDs     []int64              `json:"donor_user_ids,omitempty"`
	DonorUsernames   []string             `json:"donor_usernames,omitempty"`
}

// stateFilePayloadLocked snapshots the persistable state (admin lock, profiles,
// per-user bans) into a serializable struct. Caller must hold stateMu. Shared by
// saveStateLocked and the daily backup so all three stay in sync. The block sets
// are stored as sorted-free slices; key order is irrelevant on reload.
func (b *TelegramBot) stateFilePayloadLocked() telegramStateFile {
	payload := telegramStateFile{
		Version:   1,
		AdminLock: b.adminLock,
		UserPrefs: b.userPrefs,
		UserStats: b.userStats,
	}
	for id := range b.blockedUserIDs {
		payload.BlockedUserIDs = append(payload.BlockedUserIDs, id)
	}
	for name := range b.blockedUsernames {
		payload.BlockedUsernames = append(payload.BlockedUsernames, name)
	}
	for id := range b.donorUserIDs {
		payload.DonorUserIDs = append(payload.DonorUserIDs, id)
	}
	for name := range b.donorUsernames {
		payload.DonorUsernames = append(payload.DonorUsernames, name)
	}
	return payload
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
	b.userStats = make(map[int64]*UserStats)
	b.blockedUserIDs = make(map[int64]bool)
	b.blockedUsernames = make(map[string]bool)
	b.donorUserIDs = make(map[int64]bool)
	b.donorUsernames = make(map[string]bool)
	if b.stateFile == "" {
		return
	}
	data, err := os.ReadFile(b.stateFile)
	if err != nil || len(data) == 0 {
		// No live state file yet. On a fresh box (a VPS migration), auto-restore from
		// the newest daily backup if one was dropped in the state dir — the backups
		// are named karen-state-YYYY-MM-DD.json and DM'd to admins. This removes the
		// easy-to-miss manual rename to telegram-state.json. If none is found, fall
		// back to defaults (lock open, no jobs).
		data = b.restoreFromBackupLocked()
		if len(data) == 0 {
			return
		}
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
	if payload.UserStats != nil {
		b.userStats = payload.UserStats
	}
	for _, id := range payload.BlockedUserIDs {
		if id != 0 {
			b.blockedUserIDs[id] = true
		}
	}
	for _, name := range payload.BlockedUsernames {
		if name = strings.ToLower(strings.TrimSpace(name)); name != "" {
			b.blockedUsernames[name] = true
		}
	}
	for _, id := range payload.DonorUserIDs {
		if id != 0 {
			b.donorUserIDs[id] = true
		}
	}
	for _, name := range payload.DonorUsernames {
		if name = strings.ToLower(strings.TrimSpace(name)); name != "" {
			b.donorUsernames[name] = true
		}
	}
}

// restoreFromBackupLocked looks in the state directory for daily-backup files
// (karen-state-YYYY-MM-DD.json) and, if any exist, returns the bytes of the
// newest one AND promotes it to the live state file so subsequent saves/backups
// are normal. Returns nil when none are found. Caller must hold stateMu.
//
// The date suffix sorts lexicographically the same as chronologically, so the
// last name after a plain sort is the most recent. The transient ".backup-…"
// temp file the backup routine spills is excluded by the glob (it starts with a
// dot, not "karen-state-").
func (b *TelegramBot) restoreFromBackupLocked() []byte {
	dir := filepath.Dir(b.stateFile)
	matches, _ := filepath.Glob(filepath.Join(dir, "karen-state-*.json"))
	if len(matches) == 0 {
		return nil
	}
	sort.Strings(matches)
	newest := matches[len(matches)-1]
	data, err := os.ReadFile(newest)
	if err != nil || len(data) == 0 {
		return nil
	}
	if err := os.WriteFile(b.stateFile, data, 0644); err != nil {
		fmt.Printf("state restore: read %s but could not promote it to %s: %v\n", filepath.Base(newest), filepath.Base(b.stateFile), err)
	} else {
		fmt.Printf("state restore: no live state — loaded newest backup %s\n", filepath.Base(newest))
	}
	return data
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
	// ScheduledJobs intentionally omitted — it now lives in telegram-schedule.json.
	payload := b.stateFilePayloadLocked()
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

// getStats returns a copy of userID's lifetime usage tally, or a zero-value
// UserStats (all counters 0) when the user has never been seen. A copy is returned
// so callers can read it without holding stateMu.
func (b *TelegramBot) getStats(userID int64) UserStats {
	b.stateMu.Lock()
	defer b.stateMu.Unlock()
	if s, ok := b.userStats[userID]; ok && s != nil {
		return *s
	}
	return UserStats{}
}

// bumpStats mutates userID's usage tally under stateMu via the supplied callback,
// stamps LastSeenUnix (and FirstSeenUnix on the row's first creation), and persists
// the state file. The row is created on first use. A zero userID or nil mutate is a
// no-op. CALLER MUST NOT hold stateMu or queueMu (this takes stateMu, the leaf lock).
func (b *TelegramBot) bumpStats(userID int64, mutate func(*UserStats)) {
	if userID == 0 || mutate == nil {
		return
	}
	now := time.Now().Unix()
	b.stateMu.Lock()
	defer b.stateMu.Unlock()
	if b.userStats == nil {
		b.userStats = make(map[int64]*UserStats)
	}
	s := b.userStats[userID]
	if s == nil {
		s = &UserStats{FirstSeenUnix: now}
		b.userStats[userID] = s
	}
	mutate(s)
	s.LastSeenUnix = now
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

// isBlockedUser reports whether a user has been banned via /unauth <id>. A user
// matches by numeric ID OR by (case-insensitive) @username, so an admin can ban
// someone they only know by handle. Admins are never blocked — this is the safety
// net that stops an admin locking themselves out by username.
func (b *TelegramBot) isBlockedUser(userID int64, username string) bool {
	if b.isAdmin(userID) {
		return false
	}
	b.stateMu.Lock()
	defer b.stateMu.Unlock()
	if userID != 0 && b.blockedUserIDs[userID] {
		return true
	}
	if username != "" && b.blockedUsernames[strings.ToLower(username)] {
		return true
	}
	return false
}

// setUserBlocked bans (blocked=true) or unbans a user by ID and/or username, then
// persists. Unbanning also clears the in-session "already notified" flag so a
// re-banned user gets a fresh notice. Either id or username may be zero/empty.
func (b *TelegramBot) setUserBlocked(id int64, username string, blocked bool) {
	username = strings.ToLower(strings.TrimSpace(username))
	b.stateMu.Lock()
	if blocked {
		if id != 0 {
			b.blockedUserIDs[id] = true
		}
		if username != "" {
			b.blockedUsernames[username] = true
		}
	} else {
		if id != 0 {
			delete(b.blockedUserIDs, id)
			delete(b.blockNotified, id)
		}
		if username != "" {
			delete(b.blockedUsernames, username)
		}
	}
	b.saveStateLocked()
	b.stateMu.Unlock()
}

// markBlockNotified records that a banned user has been told they lost access and
// reports whether this was the first time. Used to send the notice once, then stay
// silent. Keyed by user ID; username-only bans get notified once their ID is seen.
func (b *TelegramBot) markBlockNotified(userID int64) bool {
	if userID == 0 {
		return false
	}
	b.stateMu.Lock()
	defer b.stateMu.Unlock()
	if b.blockNotified[userID] {
		return false
	}
	b.blockNotified[userID] = true
	return true
}

// isUserDonor reports whether a user has been granted donor perks via /p <id>. A
// user matches by numeric ID OR by (case-insensitive) @username, mirroring the ban
// system so an admin can promote someone they only know by handle. Admins are not
// implicitly donors — they already bypass the limits donors are exempted from, and
// the ⭐ badge is reserved for actual supporters.
func (b *TelegramBot) isUserDonor(userID int64, username string) bool {
	b.stateMu.Lock()
	defer b.stateMu.Unlock()
	if userID != 0 && b.donorUserIDs[userID] {
		return true
	}
	if username != "" && b.donorUsernames[strings.ToLower(username)] {
		return true
	}
	return false
}

// setUserDonor grants (donor=true) or revokes donor perks by ID and/or username,
// then persists. Either id or username may be zero/empty.
func (b *TelegramBot) setUserDonor(id int64, username string, donor bool) {
	username = strings.ToLower(strings.TrimSpace(username))
	b.stateMu.Lock()
	if donor {
		if id != 0 {
			b.donorUserIDs[id] = true
		}
		if username != "" {
			b.donorUsernames[username] = true
		}
	} else {
		if id != 0 {
			delete(b.donorUserIDs, id)
		}
		if username != "" {
			delete(b.donorUsernames, username)
		}
	}
	b.saveStateLocked()
	b.stateMu.Unlock()
}

// donorStar returns a "⭐ " prefix for a donor (matched by ID or username), or ""
// otherwise — used to badge a donor wherever their ID/handle is shown.
func (b *TelegramBot) donorStar(userID int64, username string) string {
	if b.isUserDonor(userID, username) {
		return "⭐ "
	}
	return ""
}

// countPendingScheduled returns how many of a user's scheduled (not-yet-released)
// jobs match the given kind ("artist" | "playlist"). Used to enforce the per-user
// heavy-job caps. Takes stateMu — a leaf lock, never call while holding queueMu.
func (b *TelegramBot) countPendingScheduled(userID int64, kind string) int {
	b.stateMu.Lock()
	defer b.stateMu.Unlock()
	n := 0
	for _, j := range b.scheduledJobs {
		if j.UserID == userID && j.Kind == kind {
			n++
		}
	}
	return n
}

// parseUserRef interprets an /auth|/unauth argument as either a numeric Telegram
// user ID or an @username. A leading "@" is optional; usernames are lowercased for
// case-insensitive matching. Returns (0, "") for empty/garbage input.
func parseUserRef(s string) (id int64, username string) {
	s = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(s), "@"))
	if s == "" {
		return 0, ""
	}
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return n, ""
	}
	return 0, strings.ToLower(s)
}

// formatUserRef renders a user reference for confirmation messages.
func formatUserRef(id int64, username string) string {
	if username != "" {
		return "@" + username
	}
	return "user " + strconv.FormatInt(id, 10)
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

// scheduledReleaseStagger spaces out the dispatch of jobs that all come due in the
// same tick (e.g. everything queued for the 02:30 window). Without it, N jobs fire
// their "🌙 Sleeptime reached" message + enqueue in the same instant — a guaranteed
// FloodWait once N grows. The rips themselves still run via the normal serialized
// queue; this only trickles out the releases.
const scheduledReleaseStagger = 5 * time.Second

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
	for i, j := range due {
		if i > 0 {
			time.Sleep(scheduledReleaseStagger) // trickle releases to dodge FloodWait
		}
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
		b.runArtistRipScoped(j.ChatID, j.UserID, j.Username, j.Storefront, j.ResourceID, j.Section, j.ReplyToID, j.ForceAAC, j.ForceAtmos, j.ForceFlac)
	}
}

// scheduleJob persists a future job. Donor jobs are bumped one slot up the queue
// (inserted just before the current tail) instead of appended dead-last as plain
// FIFO would — a modest priority perk over regular users' scheduled rips. Both the
// /scheduled board and the window-release order follow slice order, so the bump
// applies to display and dispatch alike.
func (b *TelegramBot) scheduleJob(j *scheduledJob, donor bool) {
	b.stateMu.Lock()
	if donor && len(b.scheduledJobs) > 0 {
		i := len(b.scheduledJobs) - 1
		b.scheduledJobs = append(b.scheduledJobs[:i], append([]*scheduledJob{j}, b.scheduledJobs[i:]...)...)
	} else {
		b.scheduledJobs = append(b.scheduledJobs, j)
	}
	b.saveScheduleLocked()
	b.stateMu.Unlock()
}

// scheduleOrRun runs a heavy job immediately if we're inside the sleeptime
// window, otherwise persists it for the next window and tells the user when.
// May block on network (artist enumeration) when running immediately, so callers
// on the update loop should invoke it in a goroutine.
func (b *TelegramBot) scheduleOrRun(j *scheduledJob) {
	now := time.Now()

	// Per-user heavy-job cap: how many rips of this kind a user may have waiting in
	// the scheduled queue at once. Donors get a higher ceiling. Discography (artist)
	// and huge playlists are tracked separately. Admins never reach here (they
	// bypass scheduling entirely), so this only ever gates non-admins.
	donor := b.isUserDonor(j.UserID, j.Username)
	limit, capLabel := 1, "discography"
	if j.Kind == "playlist" {
		limit, capLabel = 2, "huge playlist"
		if donor {
			limit = 3
		}
	} else if donor { // "artist" discography
		limit = 2
	}
	if j.UserID != 0 && b.countPendingScheduled(j.UserID, j.Kind) >= limit {
		_ = b.sendMessageWithReply(j.ChatID, fmt.Sprintf(
			"You already have %d %s rip(s) scheduled — the max for your tier. Cancel one with /scheduled or wait for it to run.",
			limit, capLabel), nil, j.ReplyToID)
		return
	}

	if inSleepWindow(now) {
		b.releaseJob(j)
		return
	}
	j.ID = generateTaskID()
	j.ReleaseAtUnix = nextWindowStart(now).Unix()
	j.CreatedAtUnix = now.Unix()
	b.scheduleJob(j, donor)

	label := "rip"
	switch j.Kind {
	case "artist":
		label = "artist rip"
	case "playlist":
		label = "playlist rip"
	}
	when := nextWindowStart(now).In(dhakaZone).Format("Mon Jan 2, 3:04 PM")
	_ = b.sendMessageWithReply(j.ChatID, fmt.Sprintf(
		"🌙 This %s is heavy, so it's scheduled for the sleeptime window. It'll start around %s (Dhaka time) and deliver as a Gofile ZIP. (Track or cancel it any time with /scheduled.)",
		label, when), nil, j.ReplyToID)
}

// autoDelete removes a message after the given delay (best-effort). Used for
// short-lived live boards like /scheduled.
func (b *TelegramBot) autoDelete(chatID int64, messageID int, after time.Duration) {
	if messageID == 0 {
		return
	}
	time.AfterFunc(after, func() { _ = b.deleteMessage(chatID, messageID) })
}

// handleScheduledBoard posts the list of deferred (sleeptime) rips — each as the
// raw stored link (no re-fetch), its requester, and a /cancel_<id> command — and
// auto-deletes the message after 60s since it's a transient live board.
func (b *TelegramBot) handleScheduledBoard(chatID int64, replyToID int) {
	b.stateMu.Lock()
	jobs := make([]*scheduledJob, len(b.scheduledJobs))
	copy(jobs, b.scheduledJobs)
	b.stateMu.Unlock()

	if len(jobs) == 0 {
		res, _ := b.sendRichMessage(chatID, "## 🌙 Scheduled tasks\n\nNothing scheduled.", "🌙 Scheduled tasks\nNothing scheduled.", nil, replyToID)
		b.autoDelete(chatID, res.messageID, 60*time.Second)
		return
	}

	var rb, pb strings.Builder
	rb.WriteString("## 🌙 Scheduled tasks\n\n")
	pb.WriteString("🌙 Scheduled tasks — start ~2:30 AM Dhaka\n")
	for i, j := range jobs {
		star := b.donorStar(j.UserID, j.Username)
		who := star + "@" + j.Username
		if j.Username == "" {
			who = star + fmt.Sprintf("user %d", j.UserID)
		}
		// Link sits in a code span (literal, not expanded); the command escapes its
		// underscore so the Rich renderer keeps it as a tappable /cancel_<id>.
		fmt.Fprintf(&rb, "%d. `%s` by %s — /cancel\\_%s  \n", i+1, j.Link, escapeRichMD(who), j.ID)
		fmt.Fprintf(&pb, "%d. %s by %s — /cancel_%s\n", i+1, j.Link, who, j.ID)
	}
	rb.WriteString("\n> Auto-clears in 60s.")
	res, _ := b.sendRichMessage(chatID, rb.String(), pb.String(), nil, replyToID)
	b.autoDelete(chatID, res.messageID, 60*time.Second)
}

// cancelScheduledJob removes a deferred job by ID. Only its requester or an admin
// may cancel it; the change is persisted immediately.
func (b *TelegramBot) cancelScheduledJob(chatID, userID int64, jobID string, replyToID int) {
	b.stateMu.Lock()
	idx := -1
	var job *scheduledJob
	for i, j := range b.scheduledJobs {
		if j.ID == jobID {
			idx, job = i, j
			break
		}
	}
	if job == nil {
		b.stateMu.Unlock()
		_ = b.sendMessageWithReply(chatID, fmt.Sprintf("No scheduled task %s found (it may have already started).", jobID), nil, replyToID)
		return
	}
	if job.UserID != userID && !b.isAdmin(userID) {
		b.stateMu.Unlock()
		_ = b.sendMessageWithReply(chatID, "That scheduled task isn't yours to cancel.", nil, replyToID)
		return
	}
	b.scheduledJobs = append(b.scheduledJobs[:idx], b.scheduledJobs[idx+1:]...)
	b.saveScheduleLocked()
	b.stateMu.Unlock()
	b.bumpStats(userID, func(s *UserStats) { s.Cancels++ })
	_ = b.sendMessageWithReply(chatID, fmt.Sprintf("❌ Cancelled scheduled task %s.", jobID), nil, replyToID)
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
	payload := b.stateFilePayloadLocked()
	profileCount := len(b.userPrefs)
	blockedCount := len(b.blockedUserIDs) + len(b.blockedUsernames)
	donorCount := len(b.donorUserIDs) + len(b.donorUsernames)
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
	header := fmt.Sprintf("🗄️ Daily backup — %d profile(s), %d banned, %d donor(s), admin-lock %s.", profileCount, blockedCount, donorCount, lockState)

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

// artistScope describes a rippable slice of an artist's catalog. apiPath is
// appended to /artists/{id}/ (a relationship like "albums"/"music-videos" or a
// "view/<name>" path). The same keys serve both URL section suffixes and the
// selector-button callback data.
type artistScope struct {
	key     string
	apiPath string
	label   string
	isMV    bool
}

// artistScopeFor maps a section key (URL suffix or button choice) to its Apple
// endpoint. Unknown keys return ok=false so the caller falls back to the
// selector prompt rather than guessing.
func artistScopeFor(key string) (artistScope, bool) {
	switch key {
	case "", "all", "albums":
		return artistScope{key: "all", apiPath: "albums", label: "entire discography"}, true
	case "full-albums":
		return artistScope{key: "full-albums", apiPath: "view/full-albums", label: "full albums"}, true
	case "singles":
		return artistScope{key: "singles", apiPath: "view/singles", label: "singles & EPs"}, true
	case "music-videos":
		return artistScope{key: "music-videos", apiPath: "music-videos", label: "music videos", isMV: true}, true
	}
	return artistScope{}, false
}

// runArtistRipScoped enumerates one section of an artist's catalog (the whole
// discography, full albums, singles/EPs, or music videos) and enqueues it as
// Gofile delivery. Album sections always go out as ONE job (a single queue slot);
// the user's ArtistZip pref picks per-release ZIPs (default) vs one combined ZIP.
// Music videos are queued per item.
//
// Blocks on network (section enumeration) — call in a goroutine from the update
// loop.
func (b *TelegramBot) runArtistRipScoped(chatID, userID int64, username, storefront, artistID, sectionKey string, replyToID int, forceAAC, forceAtmos, forceFlac bool) {
	if storefront == "" {
		storefront = Config.Storefront
	}
	scope, ok := artistScopeFor(sectionKey)
	if !ok {
		scope, _ = artistScopeFor("all")
	}

	items, err := ampapi.ListArtistSection(storefront, artistID, scope.apiPath, b.searchLanguage(), b.appleToken)
	if err != nil {
		_ = b.sendMessageWithReply(chatID, fmt.Sprintf("Failed to load this artist's %s: %v", scope.label, err), nil, replyToID)
		return
	}
	if len(items) == 0 {
		_ = b.sendMessageWithReply(chatID, fmt.Sprintf("No %s found for this artist.", scope.label), nil, replyToID)
		return
	}

	if scope.isMV {
		b.runArtistMusicVideos(chatID, userID, storefront, items, replyToID)
		return
	}

	// Always ONE job (one queue slot / board row / /stop) — never a per-release queue
	// fan-out, which was the source of the 02:30 flood + silent drops. The user's
	// ArtistZip pref now only picks the delivery shape: "combined" = one ZIP for the
	// whole discography; anything else (the default) = one ZIP per release.
	perRelease := !(userID != 0 && b.getPrefs(userID).ArtistZip == "combined")
	b.runArtistRip(chatID, userID, username, storefront, artistID, items, replyToID, forceAAC, forceAtmos, forceFlac, scope.label, perRelease)
}

// runArtistMusicVideos queues each of an artist's music videos as a direct
// Gofile delivery (per item — MVs don't combine into one ZIP).
func (b *TelegramBot) runArtistMusicVideos(chatID, userID int64, storefront string, items []ampapi.ArtistSectionItem, replyToID int) {
	queued := 0
	for _, mv := range items {
		if mv.ID == "" {
			continue
		}
		b.enqueueMvDownload(chatID, userID, storefront, mv.ID, replyToID, 0, transferModeMvGofile, false)
		queued++
	}
	_ = b.sendMessageWithReply(chatID, fmt.Sprintf("📺 Queued %d music video(s) (Gofile).", queued), nil, replyToID)
}

// runArtistRip enqueues ONE download job that rips an artist's releases sequentially.
// It is always a single queue slot / board row / /stop — never a per-release fan-out.
// perRelease picks the delivery shape:
//   - true  : ship each release as its own Gofile ZIP (one link per album) the moment
//             its rip finishes, reclaiming that release's disk before the next.
//   - false : accumulate every release and deliver one combined Gofile ZIP at the end.
//
// The 20 GB byte-threshold valve stays active underneath either way (runDownload
// enables it for every multi-track rip), composing through the shared flush cursor:
// whatever unit would exceed the threshold (a single release when per-release, or the
// whole discography when combined) is flushed in "Part N" chunks mid-rip so disk never
// spikes. If task-concurrency is off (rs == nil) flushing is unavailable, so
// per-release degrades to one final combined ZIP.
func (b *TelegramBot) runArtistRip(chatID, userID int64, username, storefront, artistID string, albums []ampapi.ArtistSectionItem, replyToID int, forceAAC, forceAtmos, forceFlac bool, label string, perRelease bool) {
	format := b.resolveFormat(chatID, forceFlac)
	ok := b.enqueueDownloadWithAfter(chatID, userID, username, replyToID, 0, false, format, transferModeGofileZip, "artist:"+artistID, false, func(ctx context.Context) error {
		rs := ripStateFrom(ctx)
		if forceAtmos {
			if rs != nil {
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
				fmt.Printf("Artist rip: release %d/%d (%s) failed: %v\n", i+1, len(albums), alb.ID, err)
			}
			if perRelease {
				// Ship this release as its own ZIP and reclaim its disk before the next.
				relName := alb.Name
				if relName == "" {
					relName = fmt.Sprintf("Release %d", i+1)
				}
				if err := rs.flushReleaseBoundary(ctx, relName); err != nil {
					fmt.Printf("Artist rip: delivering release %q failed, folding into final delivery: %v\n", relName, err)
				}
			}
		}
		return nil
	}, nil)
	if !ok {
		return
	}
	if perRelease {
		_ = b.sendMessageWithReply(chatID, fmt.Sprintf("📀 Queued %s (%d releases → one Gofile ZIP per album).", label, len(albums)), nil, replyToID)
	} else {
		_ = b.sendMessageWithReply(chatID, fmt.Sprintf("📀 Queued %s (%d releases → one combined Gofile ZIP).", label, len(albums)), nil, replyToID)
	}
}

// playlistSleeptimeThreshold is the track count above which a non-admin's
// playlist rip is deferred to the sleeptime window (forced Gofile ZIP).
const playlistSleeptimeThreshold = 100

// routePlaylistNonAdmin fetches a playlist's track count and, if it exceeds the
// threshold, schedules it for the sleeptime window; otherwise it runs the normal
// delivery path. A count failure falls through to the normal path so a transient
// API error never blocks the user. Blocking HTTP — call in a goroutine.
func (b *TelegramBot) routePlaylistNonAdmin(chatID, userID int64, username, storefront, playlistID, link string, replyToID int, headlessMode string, forceAAC, forceAtmos, forceFlac, noCache bool) {
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
			Username:   username,
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
// /check — metadata + track count for any Apple Music link (album, playlist,
// song, music video), or a full categorized breakdown for an artist
// =============================================================================

// inlineTracklistMax is the cutoff for inlining the tracklist in the Telegram
// message. At or below it the full list renders as a table in the card; above
// it the list is published to a Telegraph page and the card just links it.
const inlineTracklistMax = 40

// countTracklistCap bounds inline rows in the fallback case (Telegraph publish
// failed for a >inlineTracklistMax list) so the message can't blow past
// Telegram's size limit. Extra tracks fold into an "…and N more" line.
const countTracklistCap = 60

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

// regionBadge turns a 2-letter storefront code ("us", "jp") into a flag + code
// ("🇺🇸 US"), composing the flag from Unicode regional-indicator symbols. Odd or
// empty codes degrade to the upper-cased code (or "").
func regionBadge(sf string) string {
	sf = strings.ToLower(strings.TrimSpace(sf))
	if len(sf) != 2 || sf[0] < 'a' || sf[0] > 'z' || sf[1] < 'a' || sf[1] > 'z' {
		return strings.ToUpper(sf)
	}
	const base = 0x1F1E6 // 🇦
	flag := string(rune(base+int(sf[0]-'a'))) + string(rune(base+int(sf[1]-'a')))
	return flag + " " + strings.ToUpper(sf)
}

// qualityBadge maps Apple's album/song audioTraits to a friendly, ordered label
// such as "Hi-Res Lossless · Dolby Atmos". Returns "" when no known trait is
// present (so the caller can omit the line entirely).
func qualityBadge(traits []string) string {
	has := func(t string) bool {
		for _, x := range traits {
			if x == t {
				return true
			}
		}
		return false
	}
	var parts []string
	switch {
	case has("hi-res-lossless"):
		parts = append(parts, "Hi-Res Lossless")
	case has("lossless"):
		parts = append(parts, "Lossless")
	}
	if has("atmos") || has("spatial") {
		parts = append(parts, "Dolby Atmos")
	}
	if len(parts) == 0 && has("lossy-stereo") {
		parts = append(parts, "AAC")
	}
	return strings.Join(parts, " · ")
}

// primaryGenre returns the most specific genre, skipping the catch-all "Music".
func primaryGenre(genres []string) string {
	for _, g := range genres {
		if g != "" && g != "Music" {
			return g
		}
	}
	if len(genres) > 0 {
		return genres[0]
	}
	return ""
}

// sumTrackDuration totals the runtime across a track list.
func sumTrackDuration(tracks []ampapi.TrackRespData) time.Duration {
	ms := 0
	for _, t := range tracks {
		ms += t.Attributes.DurationInMillis
	}
	return time.Duration(ms) * time.Millisecond
}

// shortDuration renders a millisecond duration as "m:ss" (or "h:mm:ss"), the
// compact form used inside tracklist rows.
func shortDuration(ms int) string {
	s := ms / 1000
	if h := s / 3600; h > 0 {
		return fmt.Sprintf("%d:%02d:%02d", h, (s%3600)/60, s%60)
	}
	return fmt.Sprintf("%d:%02d", s/60, s%60)
}

// countCard is the assembled metadata for an album- or playlist-style /check
// reply. Empty fields are dropped from the render, so the same struct serves a
// bare playlist (just name + tracks) and a fully-tagged album.
type countCard struct {
	emoji       string // 💿 album, 🎶 playlist
	title       string
	subtitle    string // artist or curator (may be "")
	label       string
	release     string
	genre       string
	quality     string
	region      string
	upc         string
	copyright   string
	streamable  int
	totalTracks int
	duration    time.Duration
	tracks      []ampapi.TrackRespData
	// quality holds the coarse audioTraits badge ("Hi-Res Lossless · Dolby Atmos").
	// preciseQuality, when set, is the exact probed spec ("ALAC 24-bit/48 kHz · …")
	// and supersedes it on the headline. qualityReports is aligned to the rendered
	// (capped) tracklist, one entry per shown track ("" / nil = lossy/unavailable).
	preciseQuality string
	qualityReports []*trackQualityReport
	// seqNumbers numbers the tracklist by position (1…N) rather than by each
	// track's source-album TrackNumber — correct for playlists, where album
	// numbers repeat. tracklistURL, when set, is a Telegraph page holding the
	// full list; the card links it instead of inlining the table.
	seqNumbers   bool
	tracklistURL string
}

// richLines joins body lines with GFM hard breaks (two trailing spaces + a
// newline) so Telegram's Rich renderer keeps them on separate lines instead of
// soft-wrapping the paragraph into one run-on — the same trick the status
// board's footer blockquote uses.
func richLines(lines []string) string {
	return strings.Join(lines, "  \n")
}

// render builds the Bot API 10.1 Rich-Markdown body and a plain-text fallback.
// The tracklist lives in a collapsed <details> block (same construct the live
// board and completion summary use) so even a 60-row table stays out of the way
// until the user expands it.
func (c countCard) render() (rich, plain string) {
	var rb, pb strings.Builder

	fmt.Fprintf(&rb, "## %s %s\n\n", c.emoji, escapeRichMD(c.title))
	fmt.Fprintf(&pb, "%s %s\n", c.emoji, c.title)

	var meta []string
	if c.label != "" {
		meta = append(meta, "🏷️ "+c.label)
	}
	if c.release != "" {
		meta = append(meta, "📅 "+c.release)
	}
	if c.genre != "" {
		meta = append(meta, "🎸 "+c.genre)
	}

	count := fmt.Sprintf("%d streamable track(s)", c.streamable)
	if c.totalTracks > c.streamable {
		count = fmt.Sprintf("%d of %d tracks streamable", c.streamable, c.totalTracks)
	}
	headline := "🔢 " + count
	if c.duration > 0 {
		headline += " · ⏱️ " + formatDuration(c.duration)
	}

	// Body lines, each its own visual line via hard breaks (rich) / newlines (plain).
	var richBody, plainBody []string
	if c.subtitle != "" {
		richBody = append(richBody, "**"+escapeRichMD(c.subtitle)+"**")
		plainBody = append(plainBody, c.subtitle)
	}
	if len(meta) > 0 {
		richBody = append(richBody, escapeRichMD(strings.Join(meta, " · ")))
		plainBody = append(plainBody, strings.Join(meta, " · "))
	}
	// Prefer the exact probed spec over the coarse audioTraits badge when available.
	qualityLine := c.quality
	if c.preciseQuality != "" {
		qualityLine = c.preciseQuality
	}
	if qualityLine != "" {
		richBody = append(richBody, "🎧 "+escapeRichMD(qualityLine))
		plainBody = append(plainBody, "🎧 "+qualityLine)
	}
	richBody = append(richBody, escapeRichMD(headline))
	plainBody = append(plainBody, headline)
	if c.region != "" {
		richBody = append(richBody, "🌍 "+escapeRichMD(c.region))
		plainBody = append(plainBody, "🌍 "+c.region)
	}
	// Full tracklist hosted on Telegraph (large lists). Link is intentionally NOT
	// escaped so the Markdown link renders (and triggers Instant View).
	if c.tracklistURL != "" {
		richBody = append(richBody, fmt.Sprintf("📄 [Full tracklist · %d track(s)](%s)", len(c.tracks), c.tracklistURL))
		plainBody = append(plainBody, fmt.Sprintf("📄 Full tracklist (%d tracks): %s", len(c.tracks), c.tracklistURL))
	}
	rb.WriteString(richLines(richBody))
	rb.WriteString("\n")
	pb.WriteString(strings.Join(plainBody, "\n"))
	pb.WriteString("\n")

	// Inline tracklist (only when not hosted on Telegraph). The track number is
	// folded into the Track cell rather than a dedicated "#" column, which the
	// Rich renderer pads wide for little benefit.
	if c.tracklistURL == "" && len(c.tracks) > 0 {
		shown := c.tracks
		extra := 0
		if len(shown) > countTracklistCap {
			extra = len(shown) - countTracklistCap
			shown = shown[:countTracklistCap]
		}
		withQuality := len(c.qualityReports) > 0
		fmt.Fprintf(&rb, "\n<details>\n<summary>Tracklist · %d track(s)</summary>\n\n", len(c.tracks))
		if withQuality {
			rb.WriteString("| Track | Quality | ⏱ |\n|:------|:------|--:|\n")
		} else {
			rb.WriteString("| Track | ⏱ |\n|:------|--:|\n")
		}
		pb.WriteString("\nTracklist:\n")
		for i, t := range shown {
			num := t.Attributes.TrackNumber
			if c.seqNumbers || num == 0 {
				num = i + 1
			}
			mark := ""
			if t.Attributes.PlayParams.ID == "" {
				mark = " ⚠️" // unavailable / not rip-able
			}
			dur := shortDuration(t.Attributes.DurationInMillis)
			label := fmt.Sprintf("%d. %s", num, truncateStatusTitle(t.Attributes.Name, 48))
			name := escapeRichMD(label)
			if withQuality {
				q := "—"
				if i < len(c.qualityReports) {
					q = qualityCell(c.qualityReports[i])
				}
				fmt.Fprintf(&rb, "| %s%s | %s | %s |\n", name, mark, escapeRichMD(q), dur)
				fmt.Fprintf(&pb, "%s%s [%s] — %s\n", label, mark, q, dur)
			} else {
				fmt.Fprintf(&rb, "| %s%s | %s |\n", name, mark, dur)
				fmt.Fprintf(&pb, "%s%s — %s\n", label, mark, dur)
			}
		}
		if extra > 0 {
			fmt.Fprintf(&rb, "\n…and %d more\n", extra)
			fmt.Fprintf(&pb, "…and %d more\n", extra)
		}
		rb.WriteString("\n</details>\n")
	}

	var foot []string
	if c.upc != "" {
		foot = append(foot, "🆔 "+c.upc)
	}
	if c.copyright != "" {
		foot = append(foot, "©️ "+c.copyright)
	}
	if len(foot) > 0 {
		fmt.Fprintf(&rb, "\n> %s\n", escapeRichMD(strings.Join(foot, " · ")))
		fmt.Fprintf(&pb, "%s\n", strings.Join(foot, " · "))
	}

	return rb.String(), strings.TrimRight(pb.String(), "\n")
}

// probeCardQuality reads exact per-track audio quality from EVERY track's HLS
// manifest (no cap — Go handles the fan-out) and fills the card's preciseQuality +
// qualityReports. Bounded-concurrency network work; for larger sets it posts a
// brief progress note so the chat isn't silent during the fetches.
func (b *TelegramBot) probeCardQuality(chatID int64, card *countCard, replyToID int) {
	shown := card.tracks
	urls := make([]string, len(shown))
	withURL := 0
	for i, t := range shown {
		urls[i] = t.Attributes.ExtendedAssetUrls.EnhancedHls
		if urls[i] != "" {
			withURL++
		}
	}
	if withURL == 0 {
		// Every track came back without an enhancedHls manifest URL, so there's
		// nothing to probe — the card shows the coarse audioTraits badge only. Log it
		// so a card full of "—" is explained rather than mysterious.
		fmt.Printf("/check quality: 0/%d tracks expose an enhancedHls URL — skipping per-track probe\n", len(shown))
		return
	}
	if len(shown) >= 10 {
		_ = b.sendMessageWithReply(chatID, fmt.Sprintf("🔎 Reading per-track quality from %d track(s)…", len(shown)), nil, replyToID)
	}
	// Per-track probes are network-bound (one CDN manifest fetch each), so scale the
	// timeout generously and fan out wide.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	reports := probeTracksQuality(ctx, urls, 24)
	card.qualityReports = reports
	card.preciseQuality = bestQualitySummary(reports)
	// Diagnostic: how many probes actually yielded a quality. A card full of "—"
	// despite manifest URLs being present points the finger at the probe/manifest,
	// not at the cap — this line distinguishes the two.
	got := 0
	for _, r := range reports {
		if r != nil {
			got++
		}
	}
	fmt.Printf("/check quality: %d/%d tracks had a manifest URL, %d returned quality\n", withURL, len(shown), got)
}

// publishTracklist renders the card's full tracklist to a Telegraph page (a
// numbered list — Telegraph has no tables) and returns its URL. Per-track quality
// is included for every track whose manifest probe succeeded; the rest show name +
// duration only.
func (b *TelegramBot) publishTracklist(c countCard) (string, error) {
	lis := make([]interface{}, 0, len(c.tracks))
	for i, t := range c.tracks {
		parts := []string{t.Attributes.Name}
		// On playlists the per-track artist varies, so include it; on a
		// single-artist album it would just repeat the subtitle.
		if c.seqNumbers && t.Attributes.ArtistName != "" && t.Attributes.ArtistName != c.subtitle {
			parts = append(parts, t.Attributes.ArtistName)
		}
		line := strings.Join(parts, " — ")

		var tail []string
		if i < len(c.qualityReports) && c.qualityReports[i] != nil {
			if q := qualityCell(c.qualityReports[i]); q != "" && q != "—" {
				tail = append(tail, q)
			}
		}
		if t.Attributes.PlayParams.ID == "" {
			tail = append(tail, "unavailable")
		}
		tail = append(tail, shortDuration(t.Attributes.DurationInMillis))
		line += " · " + strings.Join(tail, " · ")
		lis = append(lis, tgEl("li", tgText(line)))
	}

	var content []interface{}
	var intro []string
	if c.subtitle != "" {
		intro = append(intro, c.subtitle)
	}
	summary := fmt.Sprintf("%d track(s)", c.streamable)
	if c.totalTracks > c.streamable {
		summary = fmt.Sprintf("%d of %d track(s) streamable", c.streamable, c.totalTracks)
	}
	if c.duration > 0 {
		summary += " · " + formatDuration(c.duration)
	}
	intro = append(intro, summary)
	content = append(content, tgEl("p", tgText(strings.Join(intro, " · "))))
	content = append(content, tgEl("ol", lis...))
	if len(c.qualityReports) > 0 && len(c.qualityReports) < len(c.tracks) {
		content = append(content, tgEl("p", tgText(fmt.Sprintf("Per-track quality probed for the first %d tracks.", len(c.qualityReports)))))
	}

	return telegraphCreatePage(c.title, c.subtitle, content)
}

// maybePublishTracklist sends the card's tracklist to Telegraph when it's larger
// than the inline cap, setting tracklistURL on success. On failure it leaves the
// URL empty so render falls back to a capped inline table.
func (b *TelegramBot) maybePublishTracklist(card *countCard) {
	if len(card.tracks) <= inlineTracklistMax {
		return
	}
	if url, err := b.publishTracklist(*card); err == nil {
		card.tracklistURL = url
	}
}

// handleCount replies with the number of streamable tracks behind a link. Runs
// on its own goroutine (album/artist fetches are blocking HTTP) so it never
// stalls the update loop.
func (b *TelegramBot) handleCount(chatID int64, link string, replyToID int) {
	link = strings.TrimSpace(link)

	if sf, mvID := checkUrlMv(link); mvID != "" {
		b.countMusicVideo(chatID, orStorefront(sf), mvID, sf, replyToID)
		return
	}
	if sf, songID := checkUrlSong(link); songID != "" {
		b.countSong(chatID, orStorefront(sf), songID, sf, replyToID)
		return
	}
	if sf, albumID := checkUrl(link); albumID != "" {
		// A shared-song link is an album URL with ?i=<songID>; treat it as one song.
		if songID := songIDFromURLParam(link); songID != "" {
			b.countSong(chatID, orStorefront(sf), songID, sf, replyToID)
			return
		}
		resp, err := ampapi.GetAlbumResp(orStorefront(sf), albumID, b.searchLanguage(), b.appleToken)
		if err != nil || resp == nil || len(resp.Data) == 0 {
			_ = b.sendMessageWithReply(chatID, "Couldn't load that album.", nil, replyToID)
			return
		}
		a := resp.Data[0].Attributes
		tracks := resp.Data[0].Relationships.Tracks.Data
		card := countCard{
			emoji:       "💿",
			title:       a.Name,
			subtitle:    a.ArtistName,
			label:       a.RecordLabel,
			release:     a.ReleaseDate,
			genre:       primaryGenre(a.GenreNames),
			quality:     qualityBadge(a.AudioTraits),
			region:      regionBadge(orStorefront(sf)),
			upc:         a.Upc,
			copyright:   a.Copyright,
			streamable:  countStreamable(tracks),
			totalTracks: len(tracks),
			duration:    sumTrackDuration(tracks),
			tracks:      tracks,
		}
		b.probeCardQuality(chatID, &card, replyToID)
		b.maybePublishTracklist(&card)
		rich, plain := card.render()
		_, _ = b.sendRichMessage(chatID, rich, plain, nil, replyToID)
		b.sendRegionAvailability(chatID, albumID, replyToID)
		return
	}
	if sf, playlistID := checkUrlPlaylist(link); playlistID != "" {
		resp, err := ampapi.GetPlaylistResp(orStorefront(sf), playlistID, b.searchLanguage(), b.appleToken)
		if err != nil || resp == nil || len(resp.Data) == 0 {
			_ = b.sendMessageWithReply(chatID, "Couldn't load that playlist.", nil, replyToID)
			return
		}
		a := resp.Data[0].Attributes
		tracks := resp.Data[0].Relationships.Tracks.Data
		card := countCard{
			emoji:       "🎶",
			title:       a.Name,
			subtitle:    a.ArtistName,
			genre:       primaryGenre(a.GenreNames),
			quality:     qualityBadge(a.AudioTraits),
			region:      regionBadge(orStorefront(sf)),
			streamable:  countStreamable(tracks),
			totalTracks: len(tracks),
			duration:    sumTrackDuration(tracks),
			tracks:      tracks,
			seqNumbers:  true, // playlist: number by position, not source-album track #
		}
		b.probeCardQuality(chatID, &card, replyToID)
		b.maybePublishTracklist(&card)
		rich, plain := card.render()
		_, _ = b.sendRichMessage(chatID, rich, plain, nil, replyToID)
		return
	}
	if _, stationID := checkUrlStation(link); stationID != "" {
		_ = b.sendMessageWithReply(chatID, "📻 Stations stream continuously and have no fixed track count.", nil, replyToID)
		return
	}
	if sf, artistID := checkUrlArtist(link); artistID != "" {
		b.countArtist(chatID, orStorefront(sf), artistID, sf, replyToID)
		return
	}
	switch {
	case strings.TrimSpace(link) == "":
		_ = b.sendMessageWithReply(chatID, "Usage: /check <apple-music-link> (album, playlist, song, music video, or artist).", nil, replyToID)
	case !strings.Contains(link, "music.apple.com"):
		_ = b.sendMessageWithReply(chatID, "That doesn't look like an Apple Music link. Copy the share URL from the Apple Music app — it should start with music.apple.com.", nil, replyToID)
	default:
		_ = b.sendMessageWithReply(chatID, "Couldn't recognize that Apple Music link. /check supports songs, albums, playlists, stations, music videos, and artists.", nil, replyToID)
	}
}

// countSong renders a one-track card for a song link (a /song URL or an album
// link carrying an ?i=<songID> param). Falls back to a one-line reply if the
// song can't be loaded. sfCode is the link's storefront code for the region
// badge ("" → configured default).
func (b *TelegramBot) countSong(chatID int64, storefront, songID, sfCode string, replyToID int) {
	resp, err := ampapi.GetSongResp(storefront, songID, b.searchLanguage(), b.appleToken)
	if err != nil || resp == nil || len(resp.Data) == 0 {
		_ = b.sendMessageWithReply(chatID, "🎵 1 streamable track.", nil, replyToID)
		return
	}
	a := resp.Data[0].Attributes

	var meta []string
	if a.AlbumName != "" {
		meta = append(meta, "💿 "+a.AlbumName)
	}
	if g := primaryGenre(a.GenreNames); g != "" {
		meta = append(meta, "🎸 "+g)
	}
	if a.ReleaseDate != "" {
		meta = append(meta, "📅 "+a.ReleaseDate)
	}

	headline := "🔢 1 streamable track"
	if a.PlayParams.ID == "" {
		headline = "🔢 1 track (unavailable)"
	}
	if a.DurationInMillis > 0 {
		headline += " · ⏱️ " + shortDuration(a.DurationInMillis)
	}

	var richBody, plainBody []string
	if a.ArtistName != "" {
		richBody = append(richBody, "**"+escapeRichMD(a.ArtistName)+"**")
		plainBody = append(plainBody, a.ArtistName)
	}
	if len(meta) > 0 {
		richBody = append(richBody, escapeRichMD(strings.Join(meta, " · ")))
		plainBody = append(plainBody, strings.Join(meta, " · "))
	}
	// Exact probed spec when the manifest is readable; else the coarse badge.
	quality := qualityBadge(a.AudioTraits)
	if a.ExtendedAssetUrls.EnhancedHls != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		if s := bestQualitySummary([]*trackQualityReport{probeTrackQuality(ctx, a.ExtendedAssetUrls.EnhancedHls)}); s != "" {
			quality = s
		}
		cancel()
	}
	if quality != "" {
		richBody = append(richBody, "🎧 "+escapeRichMD(quality))
		plainBody = append(plainBody, "🎧 "+quality)
	}
	richBody = append(richBody, escapeRichMD(headline))
	plainBody = append(plainBody, headline)
	if r := regionBadge(orStorefront(sfCode)); r != "" {
		richBody = append(richBody, "🌍 "+escapeRichMD(r))
		plainBody = append(plainBody, "🌍 "+r)
	}

	rich := fmt.Sprintf("## 🎵 %s\n\n%s\n", escapeRichMD(a.Name), richLines(richBody))
	plain := "🎵 " + a.Name + "\n" + strings.Join(plainBody, "\n")
	_, _ = b.sendRichMessage(chatID, rich, plain, nil, replyToID)
	b.sendRegionAvailability(chatID, songID, replyToID)
}

// countMusicVideo renders a card for a music-video link: title, artist, album,
// genre, duration, region, and a video-quality line (4K / HDR) read from the
// MV's catalog attributes. Falls back to a one-liner if it can't be loaded.
func (b *TelegramBot) countMusicVideo(chatID int64, storefront, mvID, sfCode string, replyToID int) {
	resp, err := ampapi.GetMusicVideoResp(storefront, mvID, b.searchLanguage(), b.appleToken)
	if err != nil || resp == nil || len(resp.Data) == 0 {
		_ = b.sendMessageWithReply(chatID, "🎬 That's a music video — 1 rip-able item.", nil, replyToID)
		return
	}
	a := resp.Data[0].Attributes

	var meta []string
	if a.AlbumName != "" {
		meta = append(meta, "💿 "+a.AlbumName)
	}
	if g := primaryGenre(a.GenreNames); g != "" {
		meta = append(meta, "🎸 "+g)
	}
	if a.ReleaseDate != "" {
		meta = append(meta, "📅 "+a.ReleaseDate)
	}

	// Video-quality line from catalog flags (resolution class + dynamic range).
	video := "HD 1080p"
	if a.Has4K {
		video = "4K (2160p)"
	}
	if a.HasHDR {
		video += " · HDR"
	}

	headline := "🔢 1 music video"
	if a.PlayParams.ID == "" {
		headline = "🔢 1 music video (unavailable)"
	}
	if a.DurationInMillis > 0 {
		headline += " · ⏱️ " + shortDuration(a.DurationInMillis)
	}

	var richBody, plainBody []string
	if a.ArtistName != "" {
		richBody = append(richBody, "**"+escapeRichMD(a.ArtistName)+"**")
		plainBody = append(plainBody, a.ArtistName)
	}
	if len(meta) > 0 {
		richBody = append(richBody, escapeRichMD(strings.Join(meta, " · ")))
		plainBody = append(plainBody, strings.Join(meta, " · "))
	}
	richBody = append(richBody, "📺 "+escapeRichMD(video))
	plainBody = append(plainBody, "📺 "+video)
	richBody = append(richBody, escapeRichMD(headline))
	plainBody = append(plainBody, headline)
	if r := regionBadge(orStorefront(sfCode)); r != "" {
		richBody = append(richBody, "🌍 "+escapeRichMD(r))
		plainBody = append(plainBody, "🌍 "+r)
	}

	rich := fmt.Sprintf("## 🎬 %s\n\n%s\n", escapeRichMD(a.Name), richLines(richBody))
	plain := "🎬 " + a.Name + "\n" + strings.Join(plainBody, "\n")
	_, _ = b.sendRichMessage(chatID, rich, plain, nil, replyToID)
}

// groupThousands renders an int with comma thousands separators (1842 → "1,842").
func groupThousands(n int) string {
	neg := ""
	if n < 0 {
		neg = "-"
		n = -n
	}
	s := strconv.Itoa(n)
	var out []byte
	for i := 0; i < len(s); i++ {
		if i > 0 && (len(s)-i)%3 == 0 {
			out = append(out, ',')
		}
		out = append(out, s[i])
	}
	return neg + string(out)
}

// countLabel formats a count with its noun, pluralized regularly and grouped
// with thousands separators: countLabel(1, "album") → "1 album",
// countLabel(120, "album") → "120 albums".
func countLabel(n int, noun string) string {
	if n == 1 {
		return "1 " + noun
	}
	return groupThousands(n) + " " + noun + "s"
}

// isLiveAlbum is a conservative live-release heuristic: it only fires on an
// explicit parenthesized/suffixed "live" marker so studio titles like
// "Live and Let Die" aren't miscategorized.
func isLiveAlbum(name string) bool {
	l := strings.ToLower(name)
	return strings.Contains(l, "(live") || strings.HasSuffix(l, " - live") || strings.HasSuffix(l, " [live]")
}

// artistBreakdown is the tallied, categorized view of an artist's catalog used
// by the /check artist card. Track totals are summed from each release's
// catalog trackCount (no per-album tracklist fetch), so it scales to any
// discography size.
type artistBreakdown struct {
	name        string
	region      string
	genre       string
	releases    int // total album-type releases (albums + EPs + singles + compilations + live)
	tracks      int // summed catalog trackCount across all releases
	fullAlbums  int
	eps         int
	singles     int
	comps       int
	live        int
	musicVideos int
	playlists   int
	appearsOn   int
	// Max-quality breakdown, tallied from each release's album-level audioTraits
	// (no per-release probing — Apple only exposes the tier, not exact kHz, at album
	// level). Each release counts once toward its top lossless tier (hi-res >
	// lossless > lossy); Atmos is orthogonal and counted separately.
	hiResReleases, hiResTracks       int
	losslessReleases, losslessTracks int
	lossyReleases, lossyTracks       int
	atmosReleases, atmosTracks       int
}

// hasTrait reports whether an album's audioTraits slice contains t.
func hasTrait(traits []string, t string) bool {
	for _, x := range traits {
		if x == t {
			return true
		}
	}
	return false
}

// countArtist builds a categorized breakdown of an artist's entire catalog:
// albums / EPs / singles / compilations / live releases (split from the single
// paginated `albums` relationship via Apple's isSingle/isCompilation flags +
// name heuristics), plus music-video / playlist / appears-on counts from their
// own relationships. Track totals come from each release's catalog trackCount,
// so there are no per-album fetches and no discography-size cap. sfCode is the
// link's storefront code for the region badge.
func (b *TelegramBot) countArtist(chatID int64, storefront, artistID, sfCode string, replyToID int) {
	albums, err := ampapi.GetArtistAlbums(storefront, artistID, b.searchLanguage(), b.appleToken)
	if err != nil {
		_ = b.sendMessageWithReply(chatID, fmt.Sprintf("Failed to load this artist: %v", err), nil, replyToID)
		return
	}

	bd := artistBreakdown{
		name:     "This artist",
		region:   regionBadge(orStorefront(sfCode)),
		releases: len(albums),
	}

	var genres []string
	for _, a := range albums {
		if bd.name == "This artist" && a.ArtistName != "" {
			bd.name = a.ArtistName
		}
		if len(genres) == 0 {
			genres = a.GenreNames
		}
		bd.tracks += a.TrackCount
		switch {
		case a.IsCompilation:
			bd.comps++
		case isLiveAlbum(a.Name):
			bd.live++
		case a.IsSingle && strings.HasSuffix(strings.ToLower(a.Name), " - ep"):
			bd.eps++
		case a.IsSingle:
			bd.singles++
		default:
			bd.fullAlbums++
		}

		// Max-quality tier from the album's audioTraits (instant, no probing).
		switch {
		case hasTrait(a.AudioTraits, "hi-res-lossless"):
			bd.hiResReleases++
			bd.hiResTracks += a.TrackCount
		case hasTrait(a.AudioTraits, "lossless"):
			bd.losslessReleases++
			bd.losslessTracks += a.TrackCount
		default:
			bd.lossyReleases++
			bd.lossyTracks += a.TrackCount
		}
		if hasTrait(a.AudioTraits, "atmos") || hasTrait(a.AudioTraits, "spatial") {
			bd.atmosReleases++
			bd.atmosTracks += a.TrackCount
		}
	}
	bd.genre = primaryGenre(genres)

	// Count-only relationships. A missing bucket (some artists/storefronts lack
	// playlists or appears-on) just contributes 0; the error is ignored on
	// purpose so one absent relationship doesn't sink the whole card.
	bd.musicVideos, _ = ampapi.CountArtistRelationship(storefront, artistID, "music-videos", b.searchLanguage(), b.appleToken)
	bd.playlists, _ = ampapi.CountArtistRelationship(storefront, artistID, "playlists", b.searchLanguage(), b.appleToken)
	bd.appearsOn, _ = ampapi.CountArtistRelationship(storefront, artistID, "view/appears-on-albums", b.searchLanguage(), b.appleToken)

	if bd.releases == 0 && bd.musicVideos == 0 && bd.playlists == 0 && bd.appearsOn == 0 {
		_ = b.sendMessageWithReply(chatID, "Nothing found for this artist.", nil, replyToID)
		return
	}

	rich, plain := bd.render()
	_, _ = b.sendRichMessage(chatID, rich, plain, nil, replyToID)
}

// render builds the artist /check card: a name heading, a region/genre line, a
// track + release total, a release-type breakdown, and a media line. Any line
// (or any bucket within a line) whose count is zero is dropped, so a singles-only
// artist and a 177-release composer both read cleanly.
func (bd artistBreakdown) render() (rich, plain string) {
	var body []string // each entry becomes its own visual line

	// Region · genre.
	var head []string
	if bd.region != "" {
		head = append(head, "🌍 "+bd.region)
	}
	if bd.genre != "" {
		head = append(head, "🎸 "+bd.genre)
	}
	if len(head) > 0 {
		body = append(body, strings.Join(head, " · "))
	}

	// Track + release totals.
	if bd.releases > 0 {
		body = append(body, fmt.Sprintf("🔢 %s across %s", countLabel(bd.tracks, "track"), countLabel(bd.releases, "release")))
	}

	// Release-type breakdown — only non-zero buckets.
	var rel []string
	if bd.fullAlbums > 0 {
		rel = append(rel, "💿 "+countLabel(bd.fullAlbums, "album"))
	}
	if bd.eps > 0 {
		rel = append(rel, "💽 "+countLabel(bd.eps, "EP"))
	}
	if bd.singles > 0 {
		rel = append(rel, "🎵 "+countLabel(bd.singles, "single"))
	}
	if bd.comps > 0 {
		rel = append(rel, "🗂 "+countLabel(bd.comps, "compilation"))
	}
	if bd.live > 0 {
		rel = append(rel, "🎤 "+countLabel(bd.live, "live release"))
	}
	if len(rel) > 0 {
		body = append(body, strings.Join(rel, " · "))
	}

	// Max-quality breakdown — releases + tracks per top lossless tier, plus an Atmos
	// row. Header + one line per non-zero tier; dropped entirely for an artist whose
	// releases expose no audioTraits at all.
	if bd.hiResReleases+bd.losslessReleases+bd.lossyReleases > 0 {
		body = append(body, "🎚 Max quality")
		if bd.hiResReleases > 0 {
			body = append(body, fmt.Sprintf("💎 Hi-Res Lossless · %s · %s", countLabel(bd.hiResReleases, "release"), countLabel(bd.hiResTracks, "track")))
		}
		if bd.losslessReleases > 0 {
			body = append(body, fmt.Sprintf("🔷 Lossless · %s · %s", countLabel(bd.losslessReleases, "release"), countLabel(bd.losslessTracks, "track")))
		}
		if bd.lossyReleases > 0 {
			body = append(body, fmt.Sprintf("🔸 AAC / Lossy · %s · %s", countLabel(bd.lossyReleases, "release"), countLabel(bd.lossyTracks, "track")))
		}
		if bd.atmosReleases > 0 {
			body = append(body, fmt.Sprintf("🎧 Dolby Atmos · %s · %s", countLabel(bd.atmosReleases, "release"), countLabel(bd.atmosTracks, "track")))
		}
	}

	// Media + related — only non-zero buckets.
	var media []string
	if bd.musicVideos > 0 {
		media = append(media, "📺 "+countLabel(bd.musicVideos, "music video"))
	}
	if bd.playlists > 0 {
		media = append(media, "🎶 "+countLabel(bd.playlists, "playlist"))
	}
	if bd.appearsOn > 0 {
		media = append(media, "🤝 "+groupThousands(bd.appearsOn)+" appears-on")
	}
	if len(media) > 0 {
		body = append(body, strings.Join(media, " · "))
	}

	var richBody []string
	for _, line := range body {
		richBody = append(richBody, escapeRichMD(line))
	}
	rich = fmt.Sprintf("## 👤 %s\n\n%s\n", escapeRichMD(bd.name), richLines(richBody))
	plain = "👤 " + bd.name + "\n" + strings.Join(body, "\n")
	return rich, plain
}
