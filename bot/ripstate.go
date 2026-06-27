package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
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

	// held tracks the files this rip has registered in the global in-use registry
	// (inUseFiles) and not yet released, so releaseAllHeld can drop exactly this
	// rip's outstanding refs — including for failed/cancelled rips — without
	// double-decrementing paths a mid-rip flush already released. Guarded by heldMu.
	heldMu sync.Mutex
	held   map[string]int

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

	// --- mid-rip Gofile flush ("checkpoint") -------------------------------
	// When a rip's not-yet-delivered files on disk exceed flushThreshold bytes,
	// the launch loop drains in-flight tracks and calls flush() to zip+upload that
	// chunk to Gofile, then deletes the source files to reclaim disk before
	// continuing. This bounds peak disk use for huge rips (full discographies,
	// hundred-track playlists) that would otherwise have to land entirely on disk
	// before the single final ZIP. flush is nil and flushThreshold <= 0 on the
	// CLI / single-track / disabled paths, where checkpointFlush is a no-op.
	flushMu        sync.Mutex
	flushThreshold int64
	flush          func(ctx context.Context, paths []string, part int, label string) error
	flushSeq       int        // chunks flushed so far (→ "Part N")
	flushStart     int        // index into paths of the first not-yet-flushed file
	measuredIdx    int        // paths[:measuredIdx] already summed into pendingBytes
	pendingBytes   int64      // on-disk size of paths[flushStart:measuredIdx]
	flushedAny     atomic.Bool
	// flushName is a human label for this rip's Gofile ZIP parts (e.g. the artist
	// name for a discography), set once at rip start. When set, flush/delivery names
	// the zip after it instead of the collapsed download-folder basename ("ALAC").
	// Guarded by flushMu.
	flushName string

	// cacheDelivered counts tracks already delivered straight from the dump by the
	// D9 read-through (catalogServeCollection) before/instead of ripping. When a
	// fully-cached collection rips nothing, runDownload uses this to report success
	// instead of "No files were downloaded".
	cacheDelivered atomic.Int64

	// quotaOwnerCancel records that THIS rip was cancelled by its own requester
	// (not an admin or a /restart). The per-day quota refund logic reads it to apply
	// the user-only exemption: a user who bails after >50% of releases (or after a
	// ~20 GB "Part" already went out) keeps the charge. Set by cancelTask before it
	// cancels the context; read in the after() finalize closure. Default false →
	// admin/restart/failure always refunds.
	quotaOwnerCancel atomic.Bool
}

func newRipState() *RipState {
	return &RipState{
		okDict: make(map[string][]int),
		meta:   make(map[string]AudioMeta),
		held:   make(map[string]int),
	}
}

// inUseFiles is a process-global reference count of source files that an active
// rip has downloaded and still needs on disk for delivery. It exists because the
// download folder is content-keyed (…/<artist>/<album>), so concurrent rips of the
// same album — and a rip's own trailing cleanup firing while a *different* rip is
// still uploading — would otherwise delete files out from under an in-flight
// upload (observed as "open …: no such file or directory" mid-delivery). Every
// deleter (cleanupDownloadsIfNeeded, the mid-rip flush, the periodic purge, the
// --no-cache re-rip) consults this before unlinking, and skips anything still held.
var inUseFiles = struct {
	mu  sync.Mutex
	ref map[string]int
}{ref: make(map[string]int)}

// inUseKey normalizes a path so the same file registered via differently-formed
// paths (relative vs absolute) maps to one ref entry.
func inUseKey(p string) string {
	if abs, err := filepath.Abs(p); err == nil {
		return filepath.Clean(abs)
	}
	return filepath.Clean(p)
}

func retainInUse(p string) {
	if p == "" {
		return
	}
	k := inUseKey(p)
	inUseFiles.mu.Lock()
	inUseFiles.ref[k]++
	inUseFiles.mu.Unlock()
}

// releaseInUse drops one global ref on p and returns the remaining count.
func releaseInUse(p string) int {
	if p == "" {
		return 0
	}
	k := inUseKey(p)
	inUseFiles.mu.Lock()
	defer inUseFiles.mu.Unlock()
	if inUseFiles.ref[k] > 0 {
		inUseFiles.ref[k]--
		if inUseFiles.ref[k] == 0 {
			delete(inUseFiles.ref, k)
			return 0
		}
		return inUseFiles.ref[k]
	}
	return 0
}

// isInUse reports whether any active rip still needs p on disk.
func isInUse(p string) bool {
	if p == "" {
		return false
	}
	k := inUseKey(p)
	inUseFiles.mu.Lock()
	defer inUseFiles.mu.Unlock()
	return inUseFiles.ref[k] > 0
}

// anyInUse reports whether any file at all is currently held by an active rip.
func anyInUse() bool {
	inUseFiles.mu.Lock()
	defer inUseFiles.mu.Unlock()
	return len(inUseFiles.ref) > 0
}

// retain registers p as in-use by this rip (one global ref + one per-rip held ref).
// No-op on a nil receiver (CLI / serial path, where there is no concurrent deleter).
func (rs *RipState) retain(p string) {
	if rs == nil || p == "" {
		return
	}
	rs.heldMu.Lock()
	if rs.held == nil {
		rs.held = make(map[string]int)
	}
	rs.held[p]++
	rs.heldMu.Unlock()
	retainInUse(p)
}

// releaseHeld drops one ref this rip holds on p (if it holds any) and reports the
// global remaining count plus whether this rip actually held it. Used by the
// mid-rip flush to decide if a delivered file can be unlinked yet.
func (rs *RipState) releaseHeld(p string) (remaining int, held bool) {
	if rs == nil || p == "" {
		return 0, false
	}
	rs.heldMu.Lock()
	if rs.held[p] > 0 {
		rs.held[p]--
		if rs.held[p] == 0 {
			delete(rs.held, p)
		}
		held = true
	}
	rs.heldMu.Unlock()
	if held {
		remaining = releaseInUse(p)
	}
	return
}

// releaseAllHeld drops every ref this rip still holds. Deferred by runDownload so a
// rip's files become eligible for cleanup the moment its delivery finishes — and
// not a moment sooner. Idempotent.
func (rs *RipState) releaseAllHeld() {
	if rs == nil {
		return
	}
	rs.heldMu.Lock()
	paths := make([]string, 0, len(rs.held))
	for p, n := range rs.held {
		for i := 0; i < n; i++ {
			paths = append(paths, p)
		}
	}
	rs.held = make(map[string]int)
	rs.heldMu.Unlock()
	for _, p := range paths {
		releaseInUse(p)
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
	// Protect this file from every deleter until this rip finishes delivering it.
	rs.retain(p)
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

// --- mid-rip Gofile flush --------------------------------------------------

// setFlush enables mid-rip flushing: once accumulated on-disk bytes cross
// threshold, the launch loops call checkpointFlush, which drains in-flight tracks
// and invokes fn to deliver that chunk. A threshold <= 0 or nil fn leaves flushing
// disabled. Set once by runDownload before the download begins; never called on a
// nil receiver (CLI / single-track paths simply never enable it).
func (rs *RipState) setFlush(threshold int64, fn func(ctx context.Context, paths []string, part int, label string) error) {
	if rs == nil {
		return
	}
	rs.flushMu.Lock()
	rs.flushThreshold = threshold
	rs.flush = fn
	rs.flushMu.Unlock()
}

// setFlushName records the human label used to name this rip's Gofile ZIP parts
// (e.g. the artist name). No-op on a nil receiver or empty name.
func (rs *RipState) setFlushName(name string) {
	if rs == nil || name == "" {
		return
	}
	rs.flushMu.Lock()
	rs.flushName = name
	rs.flushMu.Unlock()
}

// flushDisplayName returns the label set by setFlushName, or "" if none. Callers use
// it to name flush/delivery zips after the content instead of the folder basename.
func (rs *RipState) flushDisplayName() string {
	if rs == nil {
		return ""
	}
	rs.flushMu.Lock()
	defer rs.flushMu.Unlock()
	return rs.flushName
}

// flushedSomething reports whether at least one chunk was flushed mid-rip. When
// true, runDownload forces the leftover remainder to Gofile too (the whole huge
// rip arrives as a series of Gofile links). False on a nil receiver.
func (rs *RipState) flushedSomething() bool {
	if rs == nil {
		return false
	}
	return rs.flushedAny.Load()
}

// deliveredReleases returns how many chunks this rip has flushed+delivered so far
// (flushSeq). In per-release artist mode each released album increments it, so it
// doubles as the delivered-release count for the quota >50% exemption; for other
// rips it is the number of ~20 GB "Part N" valve flushes. 0 on a nil receiver.
func (rs *RipState) deliveredReleases() int {
	if rs == nil {
		return 0
	}
	rs.flushMu.Lock()
	defer rs.flushMu.Unlock()
	return rs.flushSeq
}

// markQuotaOwnerCancel flags that this rip was cancelled by its own requester (see
// quotaOwnerCancel). No-op on a nil receiver.
func (rs *RipState) markQuotaOwnerCancel() {
	if rs != nil {
		rs.quotaOwnerCancel.Store(true)
	}
}

// quotaCancelledByOwner reports whether the requester (not an admin / restart)
// cancelled this rip. False on a nil receiver.
func (rs *RipState) quotaCancelledByOwner() bool {
	return rs != nil && rs.quotaOwnerCancel.Load()
}

// markCacheDelivered records that n tracks were delivered from the dump by the D9
// read-through (no-op on the nil/CLI path).
func (rs *RipState) markCacheDelivered(n int) {
	if rs == nil || n <= 0 {
		return
	}
	rs.cacheDelivered.Add(int64(n))
}

// cacheDeliveredCount returns how many tracks were delivered from the dump.
func (rs *RipState) cacheDeliveredCount() int {
	if rs == nil {
		return 0
	}
	return int(rs.cacheDelivered.Load())
}

// remainderPaths returns the files not yet delivered by a mid-rip flush — i.e. the
// tail accumulated since the last checkpoint. With no flushing it is every path, so
// behavior is identical to snapshotPaths. A nil receiver falls back to the package
// globals (CLI / flag-off), where flushing never runs.
func (rs *RipState) remainderPaths() []string {
	if rs == nil {
		lastPathsMu.Lock()
		defer lastPathsMu.Unlock()
		return append([]string{}, lastDownloadedPaths...)
	}
	// Lock order is always flushMu → pathsMu; never hold both here.
	rs.flushMu.Lock()
	start := rs.flushStart
	rs.flushMu.Unlock()
	rs.pathsMu.Lock()
	defer rs.pathsMu.Unlock()
	if start < 0 || start > len(rs.paths) {
		start = 0
	}
	return append([]string{}, rs.paths[start:]...)
}

// checkpointFlush is called at the top of each track-launch loop iteration. It is
// cheap on the common path: it sums only the files added since the last call into a
// running total and returns once that total is below the threshold. When the
// threshold is crossed it calls drain (the loop's wg.Wait) so every in-flight track
// finishes — guaranteeing no half-written file is zipped — then zips+uploads the
// chunk via the injected flush callback, deletes those source files to reclaim
// disk, and advances the cursor so the files are neither re-delivered nor
// re-counted. No-op when flushing is disabled, on a nil receiver, or after ctx is
// cancelled. On flush failure the files are kept (they roll into the final
// delivery) and pendingBytes is reset so the next threshold's worth retries.
func (rs *RipState) checkpointFlush(ctx context.Context, drain func()) {
	if rs == nil {
		return
	}
	rs.flushMu.Lock()
	if rs.flush == nil || rs.flushThreshold <= 0 {
		rs.flushMu.Unlock()
		return
	}
	if ctx != nil && ctx.Err() != nil {
		rs.flushMu.Unlock()
		return
	}

	rs.measurePendingLocked()
	if rs.pendingBytes < rs.flushThreshold {
		rs.flushMu.Unlock()
		return
	}

	// Over threshold: let in-flight downloads finish, then re-measure the files
	// that completed during the drain so the chunk captures them too.
	if drain != nil {
		rs.flushMu.Unlock()
		drain()
		rs.flushMu.Lock()
		// flush may have been cleared concurrently (defensive).
		if rs.flush == nil {
			rs.flushMu.Unlock()
			return
		}
		rs.measurePendingLocked()
	}

	rs.pathsMu.Lock()
	chunk := append([]string{}, rs.paths[rs.flushStart:]...)
	chunkEnd := len(rs.paths)
	rs.pathsMu.Unlock()

	if len(chunk) == 0 {
		rs.pendingBytes = 0
		rs.flushMu.Unlock()
		return
	}

	part := rs.flushSeq + 1
	fn := rs.flush
	rs.flushMu.Unlock()

	// Empty label → flushChunkToGofile uses its "Part N" naming (the byte-threshold
	// valve). Release-boundary flushes pass an album name instead.
	err := fn(ctx, chunk, part, "")

	rs.flushMu.Lock()
	if err != nil {
		fmt.Printf("mid-rip flush (part %d) failed, keeping files for final delivery: %v\n", part, err)
		rs.pendingBytes = 0
		rs.flushMu.Unlock()
		return
	}
	// Delivered: drop the source files to reclaim disk, then advance cursors.
	rs.removeFlushedFiles(chunk)
	rs.flushSeq = part
	rs.flushStart = chunkEnd
	rs.measuredIdx = chunkEnd
	rs.pendingBytes = 0
	rs.flushedAny.Store(true)
	rs.flushMu.Unlock()
}

// flushReleaseBoundary delivers everything accumulated since the last checkpoint as
// one labeled chunk, regardless of the byte threshold — used to ship each artist
// release as its own ZIP the moment its rip finishes. The caller must ensure the
// release's tracks are already on disk (ripAlbum returns synchronously), so unlike
// checkpointFlush there is no in-flight drain. It shares flushStart with the 20 GB
// valve, so the two never double-deliver a file; if the valve already flushed part
// of this release mid-rip, only the tail since then is shipped here. No-op when
// flushing was never enabled, the receiver is nil, ctx is cancelled, or nothing new
// accumulated. On flush failure the files are kept (they roll into the final
// remainder delivery) and the error is returned for logging.
func (rs *RipState) flushReleaseBoundary(ctx context.Context, label string) error {
	if rs == nil {
		return nil
	}
	rs.flushMu.Lock()
	if rs.flush == nil {
		rs.flushMu.Unlock()
		return nil
	}
	if ctx != nil && ctx.Err() != nil {
		rs.flushMu.Unlock()
		return ctx.Err()
	}
	rs.pathsMu.Lock()
	start := rs.flushStart
	if start < 0 || start > len(rs.paths) {
		start = 0
	}
	chunk := append([]string{}, rs.paths[start:]...)
	chunkEnd := len(rs.paths)
	rs.pathsMu.Unlock()
	if len(chunk) == 0 {
		rs.flushMu.Unlock()
		return nil
	}
	part := rs.flushSeq + 1
	fn := rs.flush
	rs.flushMu.Unlock()

	if err := fn(ctx, chunk, part, label); err != nil {
		return err
	}

	rs.flushMu.Lock()
	rs.removeFlushedFiles(chunk)
	rs.flushSeq = part
	rs.flushStart = chunkEnd
	rs.measuredIdx = chunkEnd
	rs.pendingBytes = 0
	rs.flushedAny.Store(true)
	rs.flushMu.Unlock()
	return nil
}

// measurePendingLocked sums the on-disk size of paths added since the last
// measurement into pendingBytes, advancing measuredIdx. Caller holds flushMu.
func (rs *RipState) measurePendingLocked() {
	rs.pathsMu.Lock()
	defer rs.pathsMu.Unlock()
	if rs.measuredIdx < rs.flushStart {
		rs.measuredIdx = rs.flushStart
	}
	for ; rs.measuredIdx < len(rs.paths); rs.measuredIdx++ {
		if info, err := os.Stat(rs.paths[rs.measuredIdx]); err == nil && !info.IsDir() {
			rs.pendingBytes += info.Size()
		}
	}
}

// removeFlushedFiles deletes the source files of a chunk this rip just delivered
// and prunes any album directories left empty, reclaiming the disk the whole
// feature exists for. It releases this rip's in-use ref on each file first and
// only unlinks once no *other* concurrent rip still needs the (shared, content-
// keyed) file — otherwise a rip flushing its part would delete a file another rip
// of the same album is still uploading. Best-effort: failures are ignored (the
// periodic cache cleanup is the backstop).
func (rs *RipState) removeFlushedFiles(paths []string) {
	dirs := make(map[string]struct{})
	for _, p := range paths {
		if p == "" {
			continue
		}
		remaining, held := rs.releaseHeld(p)
		if held && remaining > 0 {
			// Another active rip still needs this shared file; leave it for them.
			continue
		}
		if isInUse(p) {
			// Defensive: a path not tracked in this rip's held set but still held
			// elsewhere (e.g. nil-receiver paths) must not be deleted.
			continue
		}
		if err := os.Remove(p); err == nil {
			dirs[filepath.Dir(p)] = struct{}{}
		}
	}
	for d := range dirs {
		// os.Remove only succeeds on an already-empty dir, so this never deletes a
		// folder that still holds undelivered files.
		_ = os.Remove(d)
	}
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
