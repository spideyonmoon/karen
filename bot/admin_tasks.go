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

type telegramStateFile struct {
	Version       int             `json:"version"`
	AdminLock     bool            `json:"admin_lock"`
	ScheduledJobs []*scheduledJob `json:"scheduled_jobs"`
}

// loadState reads telegram-state.json into adminLock + scheduledJobs. A missing
// file means defaults (lock open, no jobs) — the zero values already give that.
func (b *TelegramBot) loadState() {
	b.stateMu.Lock()
	defer b.stateMu.Unlock()
	b.adminLock = false
	b.scheduledJobs = nil
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
		Version:       1,
		AdminLock:     b.adminLock,
		ScheduledJobs: b.scheduledJobs,
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
		b.saveStateLocked()
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
		b.enqueuePlaylistDownload(j.ChatID, j.ResourceID, j.ReplyToID, 0, transferModeGofileZip, j.ForceAAC, j.ForceAtmos, j.ForceFlac, j.UserID, j.Username)
	case "artist":
		_ = b.sendMessageWithReply(j.ChatID, "🌙 Sleeptime reached — starting your scheduled artist rip (Gofile ZIP).", nil, j.ReplyToID)
		b.runArtistRip(j.ChatID, j.UserID, j.Username, j.Storefront, j.ResourceID, j.ReplyToID, j.ForceAAC, j.ForceAtmos, j.ForceFlac)
	}
}

// scheduleJob persists a future job.
func (b *TelegramBot) scheduleJob(j *scheduledJob) {
	b.stateMu.Lock()
	b.scheduledJobs = append(b.scheduledJobs, j)
	b.saveStateLocked()
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
// Artist rips (re-enabled): fan an artist out into one Gofile-ZIP album job each
// =============================================================================

// runArtistRip enumerates every album for an artist and enqueues each as a
// forced-Gofile-ZIP download. It stops and reports truncation if the download
// queue fills. Blocks on network (album enumeration) — call in a goroutine from
// the update loop.
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

// enqueueAlbumDownloadChecked mirrors enqueueAlbumDownload but returns whether
// the album was accepted onto the queue (false == queue full), so the artist
// fan-out can stop cleanly instead of silently dropping albums.
func (b *TelegramBot) enqueueAlbumDownloadChecked(chatID int64, albumID string, replyToID int, transferMode string, forceAAC, forceAtmos, forceFlac bool, userID int64, username string) bool {
	if albumID == "" {
		return true
	}
	format := b.resolveFormat(chatID, forceFlac)
	return b.enqueueDownloadWithAfter(chatID, userID, username, replyToID, 0, false, format, transferMode, albumID, func(ctx context.Context) error {
		if forceAtmos {
			dl_atmos = true
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
func (b *TelegramBot) routePlaylistNonAdmin(chatID, userID int64, storefront, playlistID, link string, replyToID int, headlessMode string, forceAAC, forceAtmos, forceFlac bool) {
	resp, err := ampapi.GetPlaylistResp(orStorefront(storefront), playlistID, b.searchLanguage(), b.appleToken)
	if err != nil || resp == nil || len(resp.Data) == 0 {
		b.dispatchPlaylistNormal(chatID, userID, playlistID, replyToID, headlessMode, forceAAC, forceAtmos, forceFlac)
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
	b.dispatchPlaylistNormal(chatID, userID, playlistID, replyToID, headlessMode, forceAAC, forceAtmos, forceFlac)
}

// dispatchPlaylistNormal is the pre-existing playlist behavior: headless enqueue
// when a delivery flag was given, otherwise the delivery-mode keyboard prompt.
func (b *TelegramBot) dispatchPlaylistNormal(chatID, userID int64, playlistID string, replyToID int, headlessMode string, forceAAC, forceAtmos, forceFlac bool) {
	if headlessMode != "" {
		b.enqueuePlaylistDownload(chatID, playlistID, replyToID, 0, headlessMode, forceAAC, forceAtmos, forceFlac, userID, "")
	} else {
		b.queueDownloadPlaylistWithReply(chatID, userID, playlistID, replyToID, forceAAC, forceAtmos, forceFlac)
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
