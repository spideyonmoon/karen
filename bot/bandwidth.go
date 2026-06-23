package main

import (
	"bufio"
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// =============================================================================
// Bandwidth accounting — cumulative UL/DL bytes for the VPS quota line in /sys.
//
// The kernel exposes per-interface byte counters in /proc/net/dev, but those are
// network-namespaced and reset to zero whenever the interface comes up — i.e.
// every time the bot container is recreated (each --build deploy). A raw read
// would therefore only ever show traffic since the last deploy, which is useless
// for tracking a finite monthly VPS allowance.
//
// So we accumulate. A background ticker samples the counters every minute, adds
// the delta since the previous sample to a running total, and persists it to
// bandwidth.json in the state dir (the same dir-mounted, atomic-save location as
// the rest of the bot state). When a sample comes back LOWER than the last one
// the interface counter was reset (container restart), so we add the new value
// outright instead of a negative delta. Worst case we lose up to one tick (~60s)
// of traffic across a restart — acceptable for a best-effort budget gauge.
//
// "RX" (received by the VPS) is download; "TX" (transmitted) is upload.
// =============================================================================

const bandwidthSampleInterval = time.Minute

// bandwidthState is the persisted accumulator. Totals are cumulative bytes since
// tracking began; the Last* fields are the previous raw /proc/net/dev readings,
// used to compute the next delta and to detect counter resets.
type bandwidthState struct {
	TotalRX uint64 `json:"total_rx"` // cumulative bytes downloaded (received)
	TotalTX uint64 `json:"total_tx"` // cumulative bytes uploaded (transmitted)
	LastRX  uint64 `json:"last_rx"`  // previous raw RX sample
	LastTX  uint64 `json:"last_tx"`  // previous raw TX sample
	Since   int64  `json:"since"`    // unix seconds when tracking started
}

// bandwidthTracker owns the accumulator and its persistence. It is independent
// of the DM-backed-up telegram-state.json: a bandwidth count is throwaway working
// data, not user data worth restoring after a VPS loss, so it lives in its own
// file and is never swept into the daily backup.
type bandwidthTracker struct {
	mu    sync.Mutex
	file  string
	state bandwidthState
}

// newBandwidthTracker loads any existing counts from file (missing/corrupt is
// fine — we just start fresh) and records the tracking-start time on first use.
func newBandwidthTracker(file string) *bandwidthTracker {
	bt := &bandwidthTracker{file: file}
	if data, err := os.ReadFile(file); err == nil {
		_ = json.Unmarshal(data, &bt.state)
	}
	if bt.state.Since == 0 {
		bt.state.Since = time.Now().Unix()
	}
	return bt
}

// start launches the periodic sampler. It takes one sample immediately so /sys is
// meaningful even right after boot, then ticks every interval.
func (bt *bandwidthTracker) start() {
	bt.sample()
	go func() {
		ticker := time.NewTicker(bandwidthSampleInterval)
		defer ticker.Stop()
		for range ticker.C {
			bt.sample()
		}
	}()
}

// sample reads the live interface counters, folds the delta into the running
// totals (handling resets), and persists. Safe to call concurrently with reads.
func (bt *bandwidthTracker) sample() {
	rx, tx, ok := readNetDev()
	if !ok {
		return
	}
	bt.mu.Lock()
	defer bt.mu.Unlock()

	// On a counter reset (container restart) the live value is below the last
	// sample; treat the whole live value as the delta. Otherwise add the diff.
	if rx >= bt.state.LastRX {
		bt.state.TotalRX += rx - bt.state.LastRX
	} else {
		bt.state.TotalRX += rx
	}
	if tx >= bt.state.LastTX {
		bt.state.TotalTX += tx - bt.state.LastTX
	} else {
		bt.state.TotalTX += tx
	}
	bt.state.LastRX = rx
	bt.state.LastTX = tx
	bt.saveLocked()
}

// totals returns a current snapshot: bytes downloaded, bytes uploaded, and the
// time tracking began. It samples first so the figure is up to the minute.
func (bt *bandwidthTracker) totals() (downBytes, upBytes uint64, since time.Time) {
	bt.sample()
	bt.mu.Lock()
	defer bt.mu.Unlock()
	return bt.state.TotalRX, bt.state.TotalTX, time.Unix(bt.state.Since, 0)
}

// saveLocked atomically writes the accumulator. Caller must hold bt.mu. tmp and
// target share the state dir (a directory bind mount), so the rename is atomic —
// see docker-compose.yml on why a single-file mount would break this.
func (bt *bandwidthTracker) saveLocked() {
	if bt.file == "" {
		return
	}
	data, err := json.Marshal(bt.state)
	if err != nil {
		return
	}
	tmp := bt.file + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return
	}
	_ = os.Rename(tmp, bt.file)
}

// readNetDev sums RX and TX bytes across all real interfaces in /proc/net/dev,
// skipping loopback. Inside the bot container this is effectively eth0, i.e. all
// traffic in and out of the VPS for this service. Returns ok=false if unreadable.
func readNetDev() (rx, tx uint64, ok bool) {
	f, err := os.Open("/proc/net/dev")
	if err != nil {
		return 0, 0, false
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		// Lines look like: "  eth0: 12345 67 0 0 0 0 0 0 8910 11 ..." with the
		// interface name and its stats split by a colon.
		idx := strings.IndexByte(line, ':')
		if idx < 0 {
			continue // header rows have no colon
		}
		name := strings.TrimSpace(line[:idx])
		if name == "" || name == "lo" {
			continue
		}
		fields := strings.Fields(line[idx+1:])
		// Columns: 0=rx_bytes 1=rx_packets ... 8=tx_bytes 9=tx_packets ...
		if len(fields) < 9 {
			continue
		}
		if v, err := strconv.ParseUint(fields[0], 10, 64); err == nil {
			rx += v
		}
		if v, err := strconv.ParseUint(fields[8], 10, 64); err == nil {
			tx += v
		}
		ok = true
	}
	return rx, tx, ok
}
