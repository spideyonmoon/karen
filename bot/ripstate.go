package main

import (
	"context"
	"strings"
	"sync"

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

	Counter structs.Counter

	okDictMu sync.Mutex
	okDict   map[string][]int

	pathsMu sync.Mutex
	paths   []string

	metaMu sync.Mutex
	meta   map[string]AudioMeta

	failMu   sync.Mutex
	failures []string

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

// --- conversion overrides --------------------------------------------------
// applyConvertConfig writes the rip's conversion overrides onto a copy of the
// global Config and returns it, so concurrent rips with different output formats
// don't clobber a shared Config. On a nil receiver it returns the global Config
// unchanged (the CLI/flag-off path already mutates the global in place).

func (rs *RipState) convertConfig() structs.ConfigSet {
	if rs == nil {
		return Config
	}
	cfg := Config
	cfg.ConvertAfterDownload = rs.ConvertAfterDownload
	cfg.ConvertFormat = rs.ConvertFormat
	cfg.ConvertKeepOriginal = rs.ConvertKeepOriginal
	cfg.ConvertSkipLossyToLossless = rs.ConvertSkipLossyToLossless
	return cfg
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
