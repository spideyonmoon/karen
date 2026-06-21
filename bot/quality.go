package main

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// =============================================================================
// Per-track audio quality probe (used by /check)
//
// Apple advertises a track's available variants in its HLS manifest
// (extendedAssetUrls.enhancedHls) via an EXT-X-SESSION-DATA blob — format,
// channels, bit-rate, bit-depth, sample-rate, atmos flag. The advertised ALAC
// bit-depth/sample-rate is frequently INFLATED (Apple lists 24/192 when the
// real encode is 24/48), so for ALAC we also read the first audio segment's MP4
// `alac` atom for the true values. Technique ported from SuperSaltyGamer/ame.
//
// This reads manifests/segments only — no decryption, no full download.
// =============================================================================

// trackQuality is one decoded audio variant.
type trackQuality struct {
	Format     string // "alac", "ec+3", "aac ", "aach", "ac-3", …
	Channels   int
	BitRate    int // bits/sec, 0 if absent
	BitDepth   int
	SampleRate int // Hz
	IsAtmos    bool
	firstSeg   string // FIRST-SEGMENT-URI, for the REAL ALAC segment fetch
}

// trackQualityReport is the best stereo + best atmos variant for one track, plus
// the REAL (segment-derived) ALAC quality when it could be read.
type trackQualityReport struct {
	stereo     *trackQuality // best variant with ≤2 channels
	atmos      *trackQuality // best variant with >2 channels (Dolby Atmos)
	stereoReal *trackQuality // true ALAC quality for the stereo variant (nil if N/A)
}

// formatOrder ranks codecs best-first (atmos, lossless, then lossy).
var formatOrder = []string{"ec+3", "alac", "aac ", "aach"}

func formatRank(f string) int {
	for i, x := range formatOrder {
		if x == f {
			return i
		}
	}
	return len(formatOrder)
}

// rawQuality mirrors one entry of the com.apple.hls.audioAssetMetadata map.
type rawQuality struct {
	FirstSeg   string `json:"FIRST-SEGMENT-URI"`
	Format     string `json:"AUDIO-FORMAT-ID"`
	ChannelUse string `json:"CHANNEL-USAGE"`
	Channels   string `json:"CHANNEL-COUNT"`
	BitRate    int    `json:"BIT-RATE"`
	SampleRate int    `json:"SAMPLE-RATE"`
	BitDepth   int    `json:"BIT-DEPTH"`
	IsAtmos    bool   `json:"IS-ATMOS"`
}

var qualityHTTP = &http.Client{Timeout: 25 * time.Second}

func qualityGet(ctx context.Context, rawURL string, rangeHdr string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", rawURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
	if rangeHdr != "" {
		req.Header.Set("Range", rangeHdr)
	}
	resp, err := qualityHTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		return nil, fmt.Errorf("quality fetch %s: %s", rawURL, resp.Status)
	}
	return io.ReadAll(resp.Body)
}

// probeTrackQuality fetches one track's enhancedHls manifest and returns its
// best stereo + atmos variants, refining the ALAC stereo variant with the REAL
// segment-derived quality when possible. Returns nil for a lossy/unavailable
// track (no usable session-data).
func probeTrackQuality(ctx context.Context, manifestURL string) *trackQualityReport {
	if manifestURL == "" {
		return nil
	}
	body, err := qualityGet(ctx, manifestURL, "")
	if err != nil {
		return nil
	}

	var encoded string
	for _, line := range strings.Split(string(body), "\n") {
		if !strings.HasPrefix(line, `#EXT-X-SESSION-DATA:DATA-ID="com.apple.hls.audioAssetMetadata"`) {
			continue
		}
		i := strings.Index(line, "VALUE=")
		if i < 0 {
			break
		}
		v := strings.TrimSpace(line[i+len("VALUE="):])
		encoded = strings.Trim(v, `"`)
		break
	}
	if encoded == "" {
		return nil
	}
	jsonBytes, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil
	}
	var data map[string]rawQuality
	if err := json.Unmarshal(jsonBytes, &data); err != nil {
		return nil
	}

	var qualities []trackQuality
	for _, r := range data {
		ch, _ := strconv.Atoi(r.Channels)
		qualities = append(qualities, trackQuality{
			Format:     r.Format,
			Channels:   ch,
			BitRate:    r.BitRate,
			BitDepth:   r.BitDepth,
			SampleRate: r.SampleRate,
			IsAtmos:    r.IsAtmos,
			firstSeg:   r.FirstSeg,
		})
	}
	if len(qualities) == 0 {
		return nil
	}

	sort.Slice(qualities, func(i, j int) bool {
		a, b := qualities[i], qualities[j]
		if ra, rb := formatRank(a.Format), formatRank(b.Format); ra != rb {
			return ra < rb
		}
		if a.BitDepth != b.BitDepth {
			return a.BitDepth > b.BitDepth
		}
		if a.SampleRate != b.SampleRate {
			return a.SampleRate > b.SampleRate
		}
		return a.BitRate > b.BitRate
	})

	rep := &trackQualityReport{}
	for i := range qualities {
		q := &qualities[i]
		if q.Channels > 2 {
			if rep.atmos == nil {
				rep.atmos = q
			}
		} else {
			if rep.stereo == nil {
				rep.stereo = q
			}
		}
	}

	// REAL check for the chosen ALAC stereo variant.
	if rep.stereo != nil && rep.stereo.Format == "alac" && rep.stereo.firstSeg != "" {
		base := manifestURL
		if k := strings.LastIndex(base, "/"); k >= 0 {
			base = base[:k]
		}
		if real := probeRealAlac(ctx, base+"/"+rep.stereo.firstSeg); real != nil {
			rep.stereoReal = real
		}
	}
	return rep
}

// probeRealAlac fetches the first ~16 KB of an ALAC segment and walks its MP4
// atoms to read the true channel-count / bit-depth / sample-rate from the `alac`
// sample entry. Port of ame's fetchRealAlacQuality. Returns nil on any mismatch
// or parse failure (caller falls back to advertised values).
func probeRealAlac(ctx context.Context, segmentURL string) *trackQuality {
	b, err := qualityGet(ctx, segmentURL, "bytes=0-16384")
	if err != nil || len(b) < 16 {
		return nil
	}
	be := binary.BigEndian
	atom := func(p int) uint32 {
		if p+4 > len(b) {
			return 0
		}
		return be.Uint32(b[p:])
	}
	// ftyp / iso5 sanity check.
	if atom(4) != 0x66747970 || atom(8) != 0x69736F35 {
		return nil
	}

	pos := 0
	for loops := 0; pos+8 <= len(b) && loops < 100; loops++ {
		size := int(atom(pos))
		typ := atom(pos + 4)
		switch typ {
		case 0x6D6F6F76, 0x7472616B, 0x6D646961, 0x6D696E66, 0x7374626C: // moov/trak/mdia/minf/stbl
			pos += 8
		case 0x73747364: // stsd
			pos += 16
		case 0x656E6361: // enca (encrypted audio) — descend, then read alac fields
			pos += 36
			fallthrough
		case 0x616C6163: // alac
			if pos+8+24+4 > len(b) {
				return nil
			}
			return &trackQuality{
				Format:     "alac",
				Channels:   int(b[pos+8+13]),
				BitDepth:   int(b[pos+8+9]),
				SampleRate: int(be.Uint32(b[pos+8+24:])),
			}
		default:
			if size <= 0 {
				return nil
			}
			pos += size
		}
	}
	return nil
}

// probeTracksQuality probes many tracks concurrently (bounded), preserving input
// order in the result slice. A nil entry means lossy/unavailable/failed.
func probeTracksQuality(ctx context.Context, manifestURLs []string, concurrency int) []*trackQualityReport {
	out := make([]*trackQualityReport, len(manifestURLs))
	if concurrency < 1 {
		concurrency = 1
	}
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	for i, u := range manifestURLs {
		if u == "" {
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(i int, u string) {
			defer wg.Done()
			defer func() { <-sem }()
			out[i] = probeTrackQuality(ctx, u)
		}(i, u)
	}
	wg.Wait()
	return out
}

// =============================================================================
// Display helpers
// =============================================================================

// formatKHz renders a sample rate in Hz as kHz: 48000→"48", 44100→"44.1".
func formatKHz(hz int) string {
	if hz <= 0 {
		return ""
	}
	if hz%1000 == 0 {
		return strconv.Itoa(hz / 1000)
	}
	return strconv.FormatFloat(float64(hz)/1000, 'f', 1, 64)
}

// qualityLabel renders one variant as a friendly label, e.g. "ALAC 24-bit/48 kHz",
// "Atmos 768 kbps", "AAC 256 kbps".
func qualityLabel(q *trackQuality) string {
	if q == nil {
		return ""
	}
	switch {
	case q.Format == "alac":
		if q.BitDepth > 0 && q.SampleRate > 0 {
			return fmt.Sprintf("ALAC %d-bit/%s kHz", q.BitDepth, formatKHz(q.SampleRate))
		}
		return "ALAC Lossless"
	case q.IsAtmos || q.Format == "ec+3":
		if q.BitRate > 0 {
			return fmt.Sprintf("Atmos %d kbps", q.BitRate/1000)
		}
		return "Dolby Atmos"
	case strings.HasPrefix(q.Format, "aac"):
		if q.BitRate > 0 {
			return fmt.Sprintf("AAC %d kbps", q.BitRate/1000)
		}
		return "AAC"
	default:
		if q.BitRate > 0 {
			return fmt.Sprintf("%s %d kbps", strings.ToUpper(strings.TrimSpace(q.Format)), q.BitRate/1000)
		}
		return strings.ToUpper(strings.TrimSpace(q.Format))
	}
}

// inflated reports whether the advertised ALAC spec exceeds the real one (Apple
// listed a higher bit-depth/sample-rate than the file actually contains).
func inflated(adv, real *trackQuality) bool {
	if adv == nil || real == nil {
		return false
	}
	return real.BitDepth < adv.BitDepth || real.SampleRate < adv.SampleRate
}

// qualityCell renders a compact per-track tracklist cell, preferring the REAL
// ALAC spec and flagging inflated advertised hi-res with ⚠️. Includes an Atmos
// tag when an atmos variant exists.
func qualityCell(rep *trackQualityReport) string {
	if rep == nil {
		return "—"
	}
	var parts []string
	if rep.stereo != nil {
		q := rep.stereo
		mark := ""
		if rep.stereoReal != nil {
			if inflated(rep.stereo, rep.stereoReal) {
				mark = " ⚠️"
			}
			q = rep.stereoReal
			q.Format = "alac"
		}
		parts = append(parts, qualityLabel(q)+mark)
	}
	if rep.atmos != nil {
		parts = append(parts, "Atmos")
	}
	if len(parts) == 0 {
		return "—"
	}
	return strings.Join(parts, " · ")
}

// bestQualitySummary folds per-track reports into a single album/playlist-level
// quality line (the highest stereo spec seen, plus whether Atmos is present and
// whether any advertised hi-res was inflated). Returns "" when nothing probed.
func bestQualitySummary(reports []*trackQualityReport) string {
	var best *trackQuality
	hasAtmos := false
	anyInflated := false
	for _, rep := range reports {
		if rep == nil {
			continue
		}
		if rep.atmos != nil {
			hasAtmos = true
		}
		q := rep.stereo
		if rep.stereoReal != nil {
			if inflated(rep.stereo, rep.stereoReal) {
				anyInflated = true
			}
			q = rep.stereoReal
			q.Format = "alac"
		}
		if q == nil {
			continue
		}
		if best == nil || formatRank(q.Format) < formatRank(best.Format) ||
			q.BitDepth > best.BitDepth || (q.BitDepth == best.BitDepth && q.SampleRate > best.SampleRate) {
			best = q
		}
	}
	if best == nil && !hasAtmos {
		return ""
	}
	var parts []string
	if best != nil {
		parts = append(parts, qualityLabel(best))
	}
	if hasAtmos {
		parts = append(parts, "Dolby Atmos")
	}
	label := strings.Join(parts, " · ")
	if anyInflated {
		label += " ⚠️ advertised hi-res inflated on some tracks"
	}
	return label
}
