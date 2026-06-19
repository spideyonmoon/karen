package main

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"

	apputils "main/utils"
	"main/utils/structs"
	"main/utils/task"
)

// RipState holds every piece of mutable state that is scoped to a single rip
// (one queued download request). Historically these lived as package globals and
// were reset at the top of runDownload before each serial rip. With task-level
// concurrency two rips can be in flight at once (a head downloading while a
// borrower downloads, or a finished rip uploading while the next downloads), so
// the state must be per-rip instead of global.
//
// State is carried through the existing ctx that already threads into the rip
// functions. Code obtains its RipState via ripStateFrom(ctx); when ctx carries
// none (CLI single-shot mode, or task-concurrency disabled) the accessors fall
// back to the package globals so behavior is byte-identical to before.
type RipState struct {
	// Download-mode flags, formerly the dl_* package globals.
	Atmos   bool
	AAC     bool
	Select  bool
	Song    bool
	NoCache bool

	// Per-request conversion overrides, formerly mutated on the global Config.
	ConvertAfterDownload       bool
	ConvertFormat              string
	ConvertKeepOriginal        bool
	ConvertSkipLossyToLossless bool

	// Prefs is the issuing user's saved rip profile, or nil for the CLI / no-profile
	// path. ripConfig() overlays its set fields onto the per-rip Config copy so a
	// user's saved lyrics/cover/quality/etc. choices take effect without mutating the
	// global Config or disturbing concurrent rips by other users. nil → no overlay,
	// behavior is byte-identical to before.
	Prefs *UserPrefs

	Counter structs.Counter

	okDictMu sync.Mutex
	okDict   map[string][]int

	pathsMu sync.Mutex
	paths   []string

	metaMu sync.Mutex
	meta   map[string]AudioMeta

	failMu   sync.Mutex
	failures []string

	// WrapperBudget caps how many wrapper-manager clients this rip may hold
	// concurrently — its slice of the shared wmgrpc.Pool. The head runs with the
	// full pool; a borrower is granted a small k. A value <= 0 (or above the pool
	// size) means "no per-rip cap" → use the full pool. Set by the scheduler.
	WrapperBudget int

	// Live track accounting the scheduler reads to make its lend decision (head
	// remaining-tracks threshold). planTracks sets totalTracks to the rip's full
	// planned track count ONCE, up front; trackDone increments doneTracks as each
	// finishes. remainingTracks() = total - done = album tracks not yet finished.
	// (totalTracks must be the whole plan, not the in-flight count — the launch
	// loop is gated by the wrapper semaphore, so incrementing per-launch would cap
	// the total at ~pool size and the head's remaining would never exceed it.)
	// Read from the scheduler goroutine while the rip writes, so both are atomic.
	totalTracks atomic.Int64
	doneTracks  atomic.Int64

	// progressFactory builds the per-track progress callback for the status board.
	progressFactory func(track *task.Track) apputils.ProgressFunc
}

func newRipState() *RipState {
	return &RipState{
		okDict: make(map[string][]int),
		meta:   make(map[string]AudioMeta),
	}
}

type ripStateKey struct{}

// withRipState returns a child ctx carrying rs. A nil rs returns ctx unchanged.
func withRipState(ctx context.Context, rs *RipState) context.Context {
	if rs == nil {
		return ctx
	}
	return context.WithValue(ctx, ripStateKey{}, rs)
}

// ripStateFrom returns the RipState carried by ctx, or nil if none. A nil ctx is
// tolerated. Callers use the *RipState helper methods, which themselves fall back
// to package globals on a nil receiver, so call sites need not branch.
func ripStateFrom(ctx context.Context) *RipState {
	if ctx == nil {
		return nil
	}
	rs, _ := ctx.Value(ripStateKey{}).(*RipState)
	return rs
}

// --- mode flags -----------------------------------------------------------
// On a nil receiver these read the package globals so CLI / flag-off behavior
// is unchanged.

func (rs *RipState) atmos() bool {
	if rs == nil {
		return dl_atmos
	}
	return rs.Atmos
}

func (rs *RipState) aac() bool {
	if rs == nil {
		return dl_aac
	}
	return rs.AAC
}

func (rs *RipState) selectMode() bool {
	if rs == nil {
		return dl_select
	}
	return rs.Select
}

func (rs *RipState) song() bool {
	if rs == nil {
		return dl_song
	}
	return rs.Song
}

func (rs *RipState) noCache() bool {
	if rs == nil {
		return dl_noCache
	}
	return rs.NoCache
}

// --- counter --------------------------------------------------------------
// ctr returns the counter to mutate: the per-rip one, or the package global on a
// nil receiver. The returned pointer is stable for the rip's lifetime.

func (rs *RipState) ctr() *structs.Counter {
	if rs == nil {
		return &counter
	}
	return &rs.Counter
}

// --- per-rip config copy ---------------------------------------------------
// ripConfig returns a per-rip copy of the global Config with this rip's
// conversion overrides AND the issuing user's saved profile overlaid, so
// concurrent rips with different output formats / profiles never clobber a
// shared Config. On a nil receiver it returns the global Config unchanged (the
// CLI/flag-off path already mutates the global in place), so behavior there is
// byte-identical to before.
func (rs *RipState) ripConfig() structs.ConfigSet {
	if rs == nil {
		return Config
	}
	cfg := Config
	cfg.ConvertAfterDownload = rs.ConvertAfterDownload
	cfg.ConvertFormat = rs.ConvertFormat
	cfg.ConvertKeepOriginal = rs.ConvertKeepOriginal
	cfg.ConvertSkipLossyToLossless = rs.ConvertSkipLossyToLossless
	applyPrefsToConfig(&cfg, rs.Prefs)
	return cfg
}

// lyricStripTimestamps reports whether the saved profile asks for a "static"
// (plain, un-timed) .lrc — synced lyrics are fetched then timestamps are stripped
// on write. Used by ripTrack; false on a nil receiver / no profile.
func (rs *RipState) lyricStripTimestamps() bool {
	return rs != nil && rs.Prefs != nil && rs.Prefs.LyricMode == "static"
}

// applyPrefsToConfig overlays a user's saved profile onto a per-rip Config copy.
// Every field is "unset" at its zero value and an unset field is left at the
// global default, so a user with no (or a partial) profile sees unchanged
// behavior. p may be nil. Codec/delivery are NOT applied here — codec flows
// through rs.Atmos/rs.AAC + the delivery format seeded at command entry, and the
// delivery target is honored by promptTransferMode.
func applyPrefsToConfig(cfg *structs.ConfigSet, p *UserPrefs) {
	if p == nil {
		return
	}

	// Lyrics sidecar mode → SaveLrcFile + LrcType. "static" shares the timed
	// "lyrics" fetch with "synced"; ripTrack strips its timestamps on write.
	switch p.LyricMode {
	case "off":
		cfg.SaveLrcFile = false
	case "synced", "static":
		cfg.SaveLrcFile = true
		cfg.LrcType = "lyrics"
	case "word":
		cfg.SaveLrcFile = true
		cfg.LrcType = "syllable-lyrics"
	}
	if p.EmbedLrc != nil {
		cfg.EmbedLrc = *p.EmbedLrc
	}
	if p.EmbedCover != nil {
		cfg.EmbedCover = *p.EmbedCover
	}
	if p.AnimatedArt != nil {
		cfg.SaveAnimatedArtwork = *p.AnimatedArt
	}

	// Quality tier caps the lossless stream: Red Book = 16-bit/44.1kHz, Hi-Res =
	// 24-bit up to 192kHz. AlacMax is a sample-rate cap (Hz); AtmosMax is a bitrate.
	switch p.Quality {
	case "redbook":
		cfg.AlacMax = 44100
		cfg.AtmosMax = 2448
	case "hires":
		cfg.AlacMax = 192000
		cfg.AtmosMax = 2768
	}

	if p.AacType != "" {
		cfg.AacType = p.AacType
	}
	if p.CoverFormat != "" {
		cfg.CoverFormat = p.CoverFormat
	}
	if p.CoverSize != "" {
		cfg.CoverSize = p.CoverSize
	}
	if p.Language != "" {
		cfg.Language = p.Language
	}
	if p.MVMax != 0 {
		cfg.MVMax = p.MVMax
	}
	// Apple Digital Master tag: a saved "off" clears the filename tag; "on" keeps
	// whatever the global default tag is (or a sensible default if unset).
	if p.AppleMaster != nil {
		if *p.AppleMaster {
			if cfg.AppleMasterChoice == "" {
				cfg.AppleMasterChoice = "[M]"
			}
		} else {
			cfg.AppleMasterChoice = ""
		}
	}
}

// --- wrapper budget + track accounting -------------------------------------

// poolSize returns the configured wrapper-manager pool size (>= 1).
func poolSize() int {
	n := len(Config.WrapperManagerAddrs)
	if n < 1 {
		return 1
	}
	return n
}

// wrapperBudget returns the max wrapper clients this rip may hold concurrently.
// A nil receiver, an unset budget, or one exceeding the pool size all mean "full
// pool" — so the CLI / flag-off path and the head both get every client, exactly
// as before. Only a borrower carries a smaller positive budget.
func (rs *RipState) wrapperBudget() int {
	n := poolSize()
	if rs == nil || rs.WrapperBudget <= 0 || rs.WrapperBudget > n {
		return n
	}
	return rs.WrapperBudget
}

// planTracks records the rip's full planned track count (call once, before the
// download loop). trackDone records one track finishing (success or failure).
// Both are no-ops on a nil receiver (CLI / flag-off), where the scheduler never
// reads the counts.
func (rs *RipState) planTracks(n int) {
	if rs == nil || n <= 0 {
		return
	}
	rs.totalTracks.Add(int64(n))
}

func (rs *RipState) trackDone() {
	if rs == nil {
		return
	}
	rs.doneTracks.Add(1)
}

// remainingTracks reports planned-but-not-yet-finished tracks (album tracks left
// to download). The scheduler uses it for the head's lend-eligibility threshold.
// Zero on a nil receiver.
func (rs *RipState) remainingTracks() int {
	if rs == nil {
		return 0
	}
	r := rs.totalTracks.Load() - rs.doneTracks.Load()
	if r < 0 {
		return 0
	}
	return int(r)
}

// --- progress factory ------------------------------------------------------

func (rs *RipState) setProgressFactory(f func(track *task.Track) apputils.ProgressFunc) {
	if rs == nil {
		activeProgressFactory = f
		return
	}
	rs.progressFactory = f
}

func (rs *RipState) progress(track *task.Track) apputils.ProgressFunc {
	if rs == nil {
		if activeProgressFactory != nil {
			return activeProgressFactory(track)
		}
		return nil
	}
	if rs.progressFactory != nil {
		return rs.progressFactory(track)
	}
	return nil
}

// --- okDict ---------------------------------------------------------------
// A nil receiver routes to the package-global okDict (CLI / flag-off path).

func (rs *RipState) markDone(preID string, taskNum int) {
	if rs == nil {
		okDictMu.Lock()
		okDict[preID] = append(okDict[preID], taskNum)
		okDictMu.Unlock()
		return
	}
	rs.okDictMu.Lock()
	rs.okDict[preID] = append(rs.okDict[preID], taskNum)
	rs.okDictMu.Unlock()
}

func (rs *RipState) isDone(preID string, taskNum int) bool {
	if rs == nil {
		okDictMu.Lock()
		defer okDictMu.Unlock()
		return isInArray(okDict[preID], taskNum)
	}
	rs.okDictMu.Lock()
	defer rs.okDictMu.Unlock()
	return isInArray(rs.okDict[preID], taskNum)
}

// --- downloaded paths -----------------------------------------------------

func (rs *RipState) addPath(p string) {
	if rs == nil {
		lastPathsMu.Lock()
		lastDownloadedPaths = append(lastDownloadedPaths, p)
		lastPathsMu.Unlock()
		return
	}
	rs.pathsMu.Lock()
	rs.paths = append(rs.paths, p)
	rs.pathsMu.Unlock()
}

func (rs *RipState) snapshotPaths() []string {
	if rs == nil {
		lastPathsMu.Lock()
		defer lastPathsMu.Unlock()
		return append([]string{}, lastDownloadedPaths...)
	}
	rs.pathsMu.Lock()
	defer rs.pathsMu.Unlock()
	return append([]string{}, rs.paths...)
}

// --- downloaded meta ------------------------------------------------------

func (rs *RipState) putMeta(path string, m AudioMeta) {
	if rs == nil {
		downloadedMetaMu.Lock()
		downloadedMeta[path] = m
		downloadedMetaMu.Unlock()
		return
	}
	rs.metaMu.Lock()
	rs.meta[path] = m
	rs.metaMu.Unlock()
}

func (rs *RipState) getMeta(path string) (AudioMeta, bool) {
	if rs == nil {
		downloadedMetaMu.Lock()
		defer downloadedMetaMu.Unlock()
		m, ok := downloadedMeta[path]
		return m, ok
	}
	rs.metaMu.Lock()
	defer rs.metaMu.Unlock()
	m, ok := rs.meta[path]
	return m, ok
}

// --- failure log ----------------------------------------------------------

func (rs *RipState) recordFailure(msg string) {
	msg = strings.TrimSpace(msg)
	if msg == "" {
		return
	}
	if rs == nil {
		downloadFailureMu.Lock()
		defer downloadFailureMu.Unlock()
		for _, e := range lastDownloadFailures {
			if e == msg {
				return
			}
		}
		lastDownloadFailures = append(lastDownloadFailures, msg)
		return
	}
	rs.failMu.Lock()
	defer rs.failMu.Unlock()
	for _, e := range rs.failures {
		if e == msg {
			return
		}
	}
	rs.failures = append(rs.failures, msg)
}

func (rs *RipState) failureSummary() string {
	if rs == nil {
		downloadFailureMu.Lock()
		defer downloadFailureMu.Unlock()
		return summarizeFailures(lastDownloadFailures)
	}
	rs.failMu.Lock()
	defer rs.failMu.Unlock()
	return summarizeFailures(rs.failures)
}
