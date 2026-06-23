package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// =============================================================================
// /sys — admin-only system status (CPU, RAM, disk, uptime, speed test)
//
// All metrics are read from the Linux /proc filesystem and syscall.Statfs, so
// there are no extra dependencies. The speed test hits Cloudflare's public
// endpoint (no API key, no install) and only runs when an admin issues /sys.
// =============================================================================

const (
	// Speed-test transfer sizes — small enough to finish in a few seconds on a
	// VPS link, large enough to give a representative number.
	speedTestDownBytes = 25 * 1024 * 1024 // 25 MiB download sample
	speedTestUpBytes   = 10 * 1024 * 1024 // 10 MiB upload sample
	speedTestTimeout   = 40 * time.Second
)

// handleSysStatus gathers system metrics and posts them as a single card. It runs
// off the update loop (the caller spawns a goroutine) because the speed test and
// CPU sample both block for a moment. A placeholder is sent first, then edited
// with the finished card so the admin gets immediate feedback.
func (b *TelegramBot) handleSysStatus(chatID int64, replyToID int) {
	msgID, err := b.sendMessageWithReplyReturn(chatID, "🖥 Gathering system status… (running speed test, ~10s)", nil, replyToID)
	if err != nil {
		// Couldn't even post the placeholder — nothing more we can do.
		return
	}

	var sb strings.Builder
	sb.WriteString("🖥 System Status\n\n")

	// Uptime: host (from /proc/uptime) and this bot process.
	hostUp := readHostUptime()
	botUp := time.Since(b.bootTime)
	sb.WriteString("⏱ Uptime: host ")
	if hostUp > 0 {
		sb.WriteString(formatUptime(hostUp))
	} else {
		sb.WriteString("n/a")
	}
	sb.WriteString(" • bot " + formatUptime(botUp) + "\n")

	// CPU: usage % over a short sample, core count, and load average.
	cpuPct := sampleCPUUsage(400 * time.Millisecond)
	cpuLine := fmt.Sprintf("🧮 CPU: %d cores", runtime.NumCPU())
	if cpuPct >= 0 {
		cpuLine = fmt.Sprintf("🧮 CPU: %.0f%% of %d cores", cpuPct, runtime.NumCPU())
	}
	if load := readLoadAvg(); load != "" {
		cpuLine += " • load " + load
	}
	sb.WriteString(cpuLine + "\n")

	// RAM.
	if total, avail, ok := readMemInfo(); ok && total > 0 {
		used := total - avail
		sb.WriteString(fmt.Sprintf("🧠 RAM: %s / %s (%.0f%%)\n",
			formatBytes(int64(used)), formatBytes(int64(total)), 100*float64(used)/float64(total)))
	} else {
		sb.WriteString("🧠 RAM: n/a\n")
	}

	// Disk: the downloads mount (where rips land) and the root fs.
	downloadsDir := "/downloads"
	if Config.AlacSaveFolder != "" {
		downloadsDir = filepath.Dir(Config.AlacSaveFolder)
	}
	if total, free, ok := diskUsage(downloadsDir); ok && total > 0 {
		used := total - free
		sb.WriteString(fmt.Sprintf("💾 Disk (%s): %s / %s used (%.0f%%)\n",
			downloadsDir, formatBytes(int64(used)), formatBytes(int64(total)), 100*float64(used)/float64(total)))
	}
	if total, free, ok := diskUsage("/"); ok && total > 0 {
		used := total - free
		sb.WriteString(fmt.Sprintf("   Root (/): %s / %s used (%.0f%%)\n",
			formatBytes(int64(used)), formatBytes(int64(total)), 100*float64(used)/float64(total)))
	}

	// Bandwidth: cumulative UL/DL since tracking began, for the VPS quota. These
	// are accumulated across deploys (see bandwidth.go), unlike the raw counters.
	// Scope is the bot container's own interface: media download + Telegram upload,
	// i.e. nearly all real traffic. The wrappers' FairPlay license/auth handshakes
	// with Apple (kilobytes per track) go out their own netns and aren't counted,
	// so this is a close floor on the billed total, not the exact host figure.
	if b.bandwidth != nil {
		down, up, since := b.bandwidth.totals()
		sb.WriteString(fmt.Sprintf("📊 Bandwidth (bot): ↓ %s • ↑ %s (Σ %s)\n",
			formatBytes(int64(down)), formatBytes(int64(up)), formatBytes(int64(down+up))))
		sb.WriteString("   since " + since.In(dhakaZone).Format("2006-01-02") + " • excl. wrapper license traffic\n")
	}

	// Speed test (download + upload). Slowest part — done last.
	down, up := runSpeedTest()
	switch {
	case down > 0 && up > 0:
		sb.WriteString(fmt.Sprintf("🌐 Speed: ↓ %.1f Mbps • ↑ %.1f Mbps\n", down, up))
	case down > 0:
		sb.WriteString(fmt.Sprintf("🌐 Speed: ↓ %.1f Mbps • ↑ n/a\n", down))
	default:
		sb.WriteString("🌐 Speed: n/a\n")
	}

	sb.WriteString("\n🕒 " + time.Now().In(dhakaZone).Format("2006-01-02 15:04") + " Dhaka")

	// Edit the placeholder into the final card; fall back to a plain send if the
	// edit fails (e.g. the message was deleted).
	if err := b.editMessageText(chatID, msgID, sb.String(), nil); err != nil {
		_ = b.sendMessageWithReply(chatID, sb.String(), nil, replyToID)
	}
}

// readHostUptime returns the host uptime from /proc/uptime (first field, seconds).
// Inside Docker this reflects the host, which is what "VPS uptime" means here.
func readHostUptime() time.Duration {
	data, err := os.ReadFile("/proc/uptime")
	if err != nil {
		return 0
	}
	fields := strings.Fields(string(data))
	if len(fields) == 0 {
		return 0
	}
	secs, err := strconv.ParseFloat(fields[0], 64)
	if err != nil {
		return 0
	}
	return time.Duration(secs * float64(time.Second))
}

// readLoadAvg returns the 1/5/15-minute load averages from /proc/loadavg as a
// space-separated string, or "" if unreadable.
func readLoadAvg() string {
	data, err := os.ReadFile("/proc/loadavg")
	if err != nil {
		return ""
	}
	fields := strings.Fields(string(data))
	if len(fields) < 3 {
		return ""
	}
	return strings.Join(fields[:3], " ")
}

// readMemInfo returns total and available memory in bytes from /proc/meminfo.
func readMemInfo() (total, available uint64, ok bool) {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0, 0, false
	}
	defer f.Close()
	var haveTotal, haveAvail bool
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case strings.HasPrefix(line, "MemTotal:"):
			total = parseMeminfoKB(line)
			haveTotal = true
		case strings.HasPrefix(line, "MemAvailable:"):
			available = parseMeminfoKB(line)
			haveAvail = true
		}
		if haveTotal && haveAvail {
			break
		}
	}
	return total, available, haveTotal && haveAvail
}

// parseMeminfoKB pulls the kB value out of a /proc/meminfo line and returns bytes.
func parseMeminfoKB(line string) uint64 {
	fields := strings.Fields(line)
	if len(fields) < 2 {
		return 0
	}
	kb, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return 0
	}
	return kb * 1024
}

// sampleCPUUsage returns aggregate CPU busy percentage measured across two reads
// of /proc/stat spaced by d. Returns -1 if unreadable.
func sampleCPUUsage(d time.Duration) float64 {
	idle1, total1, ok1 := readCPUTimes()
	if !ok1 {
		return -1
	}
	time.Sleep(d)
	idle2, total2, ok2 := readCPUTimes()
	if !ok2 {
		return -1
	}
	dTotal := float64(total2 - total1)
	dIdle := float64(idle2 - idle1)
	if dTotal <= 0 {
		return -1
	}
	usage := 100 * (1 - dIdle/dTotal)
	if usage < 0 {
		usage = 0
	}
	if usage > 100 {
		usage = 100
	}
	return usage
}

// readCPUTimes parses the aggregate "cpu" line of /proc/stat, returning idle
// (idle+iowait) and total jiffies.
func readCPUTimes() (idle, total uint64, ok bool) {
	data, err := os.ReadFile("/proc/stat")
	if err != nil {
		return 0, 0, false
	}
	for _, line := range strings.Split(string(data), "\n") {
		if !strings.HasPrefix(line, "cpu ") {
			continue
		}
		fields := strings.Fields(line)[1:] // user nice system idle iowait irq softirq steal ...
		for i, f := range fields {
			v, err := strconv.ParseUint(f, 10, 64)
			if err != nil {
				continue
			}
			total += v
			if i == 3 || i == 4 { // idle, iowait
				idle += v
			}
		}
		return idle, total, true
	}
	return 0, 0, false
}

// diskUsage returns total and free bytes for the filesystem holding path.
func diskUsage(path string) (total, free uint64, ok bool) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0, 0, false
	}
	bsize := uint64(st.Bsize)
	return st.Blocks * bsize, st.Bavail * bsize, true
}

// runSpeedTest measures download and upload throughput (Mbps) against
// Cloudflare's public speed-test endpoint. Either value is 0 on failure.
func runSpeedTest() (downMbps, upMbps float64) {
	ctx, cancel := context.WithTimeout(context.Background(), speedTestTimeout)
	defer cancel()
	client := &http.Client{}

	// Download.
	downURL := fmt.Sprintf("https://speed.cloudflare.com/__down?bytes=%d", speedTestDownBytes)
	if req, err := http.NewRequestWithContext(ctx, http.MethodGet, downURL, nil); err == nil {
		start := time.Now()
		if resp, err := client.Do(req); err == nil {
			n, _ := io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if elapsed := time.Since(start).Seconds(); elapsed > 0 && n > 0 {
				downMbps = float64(n) * 8 / elapsed / 1e6
			}
		}
	}

	// Upload — Cloudflare's __up sink discards the body.
	if req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://speed.cloudflare.com/__up", &zeroReader{remaining: speedTestUpBytes}); err == nil {
		req.ContentLength = speedTestUpBytes
		req.Header.Set("Content-Type", "application/octet-stream")
		start := time.Now()
		if resp, err := client.Do(req); err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if elapsed := time.Since(start).Seconds(); elapsed > 0 {
				upMbps = float64(speedTestUpBytes) * 8 / elapsed / 1e6
			}
		}
	}
	return downMbps, upMbps
}

// zeroReader streams `remaining` zero bytes, used as a no-allocation upload body.
type zeroReader struct{ remaining int }

func (z *zeroReader) Read(p []byte) (int, error) {
	if z.remaining <= 0 {
		return 0, io.EOF
	}
	n := len(p)
	if n > z.remaining {
		n = z.remaining
	}
	for i := 0; i < n; i++ {
		p[i] = 0
	}
	z.remaining -= n
	return n, nil
}

// formatUptime renders a duration compactly, e.g. "5d 3h", "2h 14m", "47m".
func formatUptime(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	days := int(d.Hours()) / 24
	hours := int(d.Hours()) % 24
	mins := int(d.Minutes()) % 60
	switch {
	case days > 0:
		return fmt.Sprintf("%dd %dh", days, hours)
	case hours > 0:
		return fmt.Sprintf("%dh %dm", hours, mins)
	default:
		return fmt.Sprintf("%dm", mins)
	}
}
