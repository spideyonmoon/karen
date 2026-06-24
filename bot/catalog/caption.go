package catalog

import (
	"fmt"
	"strconv"
	"strings"
)

// Caption grammar (master §4.2). The caption is a human-readable block plus ONE
// machine-parseable tag line. The tag line is the contract and is versioned via
// v=. Telegram's caption limit is 1024 chars; the tag line carries only
// IDs/enums/numbers (Title/Artist/Album live in the human block), so it stays
// well under that.
//
//	🎵 {Title} — {Artist}
//	💿 {Album} · {FORMAT} {quality-human}
//
//	#karenidx v=1 kind={Kind} adam={AppleTrackID} album={AppleAlbumID} isrc={ISRC} fmt={Format} br={Bitrate} sr={SampleRate} bd={BitDepth} sf={Storefront} dur={DurationSec} sz={SizeBytes} lrc={LyricsSync} cov={0|1} var={Variant}
//
// Rules: single-line tag line, space-separated key=value; a key is omitted
// entirely when its value is empty/zero (so we never emit adam=0); v= lets the
// parser evolve. AppleURL is intentionally NOT carried in the caption (it is
// derivable from adamID and stored directly at IndexInline) — it does not
// round-trip through the caption.

const captionTag = "#karenidx"

// captionVersion is the tag-line schema version emitted by FormatCaption. The
// parser switches on the v= value it reads, so old captions keep parsing if the
// grammar ever evolves.
const captionVersion = 1

// FormatCaption renders the canonical dump caption for m. Phase 1 calls this on
// every dump upload; the crawler round-trips it via ParseCaption.
func FormatCaption(m TrackMeta) string {
	var b strings.Builder

	// --- human block ---
	if m.Title != "" || m.Artist != "" {
		b.WriteString("🎵 ")
		b.WriteString(m.Title)
		if m.Artist != "" {
			b.WriteString(" — ")
			b.WriteString(m.Artist)
		}
		b.WriteString("\n")
	}
	if m.Album != "" {
		b.WriteString("💿 ")
		b.WriteString(m.Album)
		if up := strings.ToUpper(m.Format); up != "" {
			b.WriteString(" · ")
			b.WriteString(up)
			if q := humanQuality(m); q != "" {
				b.WriteString(" ")
				b.WriteString(q)
			}
		}
		b.WriteString("\n")
	}
	b.WriteString("\n")

	// --- machine tag line ---
	b.WriteString(captionTag)
	b.WriteString(" v=")
	b.WriteString(strconv.Itoa(captionVersion))
	writeTok(&b, "kind", canonicalKind(m.Kind), KindTrack) // omit the default
	writeI64(&b, "adam", m.AppleTrackID)
	writeI64(&b, "album", m.AppleAlbumID)
	writeStr(&b, "isrc", m.ISRC)
	writeStr(&b, "fmt", m.Format)
	writeInt(&b, "br", m.Bitrate)
	writeInt(&b, "sr", m.SampleRate)
	writeInt(&b, "bd", m.BitDepth)
	writeStr(&b, "sf", m.Storefront)
	writeInt(&b, "dur", m.DurationSec)
	writeI64(&b, "sz", m.SizeBytes)
	writeTok(&b, "lrc", canonicalLyrics(m.LyricsSync), "none") // omit the default
	if m.CoverEmbedded {
		b.WriteString(" cov=1")
	}
	writeStr(&b, "var", m.Variant)

	return b.String()
}

// ParseCaption extracts a TrackMeta from a caption. ok is false when the caption
// has no #karenidx tag line (→ caller falls back to filename/tag heuristics for
// foreign dumps in Phase 3). Title/Artist/Album are recovered best-effort from the
// human block; all identity/quality fields come from the tag line.
func ParseCaption(s string) (TrackMeta, bool) {
	tagLine := findTagLine(s)
	if tagLine == "" {
		return TrackMeta{}, false
	}
	kv := parseTagLine(tagLine)

	// Switch on version so the grammar can evolve without breaking old captions.
	switch kv["v"] {
	case "1", "": // treat a missing v as v1 for forward-tolerance
	default:
		// Unknown future version: still return what the shared keys give us.
	}

	m := TrackMeta{
		Kind:          valueOr(kv, "kind", KindTrack),
		AppleTrackID:  atoi64(kv["adam"]),
		AppleAlbumID:  atoi64(kv["album"]),
		ISRC:          kv["isrc"],
		Format:        kv["fmt"],
		Bitrate:       atoi(kv["br"]),
		SampleRate:    atoi(kv["sr"]),
		BitDepth:      atoi(kv["bd"]),
		Storefront:    kv["sf"],
		DurationSec:   atoi(kv["dur"]),
		SizeBytes:     atoi64(kv["sz"]),
		LyricsSync:    valueOr(kv, "lrc", "none"),
		CoverEmbedded: kv["cov"] == "1",
		Variant:       kv["var"],
	}

	// Reconstruct the variant from fmt when var= is absent. Phase 1 omits var=
	// under the omit-empty rule whenever the tier is non-cacheable, and may omit it
	// for cacheable tiers too; either way the tier is derivable from fmt, so the
	// parsed meta stays self-consistent (a present var= always wins). VariantKey
	// returns "" for a non-cacheable fmt, which UpsertTrack treats as "do not store."
	if m.Variant == "" {
		m.Variant = VariantKey(m)
	}

	// Recover Title/Artist/Album from the human block (display + fuzzy match).
	m.Title, m.Artist, m.Album = parseHumanBlock(s)
	return m, true
}

// --- human-block helpers ---

// humanQuality renders a short human quality string from the numeric fields. It is
// display-only and is NOT parsed back (quality comes from br/sr/bd in the tag line).
func humanQuality(m TrackMeta) string {
	switch m.Format {
	case "aac":
		if m.Bitrate > 0 {
			return strconv.Itoa(m.Bitrate) + "kbps"
		}
	default: // alac / flac / atmos (lossless-ish)
		if m.BitDepth > 0 && m.SampleRate > 0 {
			return fmt.Sprintf("%d-bit/%.3gkHz", m.BitDepth, float64(m.SampleRate)/1000)
		}
	}
	return ""
}

// parseHumanBlock recovers Title/Artist/Album from the 🎵/💿 lines. It is
// best-effort: titles containing the " — " or " · " separators are split on the
// first/last occurrence respectively, which is correct for the common case.
func parseHumanBlock(s string) (title, artist, album string) {
	for _, line := range strings.Split(s, "\n") {
		line = strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(line, "🎵"):
			rest := strings.TrimSpace(strings.TrimPrefix(line, "🎵"))
			if i := strings.Index(rest, " — "); i >= 0 {
				title = strings.TrimSpace(rest[:i])
				artist = strings.TrimSpace(rest[i+len(" — "):])
			} else {
				title = rest
			}
		case strings.HasPrefix(line, "💿"):
			rest := strings.TrimSpace(strings.TrimPrefix(line, "💿"))
			// Strip the trailing " · {FORMAT} {quality}" suffix if present.
			if i := strings.LastIndex(rest, " · "); i >= 0 {
				album = strings.TrimSpace(rest[:i])
			} else {
				album = rest
			}
		}
	}
	return title, artist, album
}

// --- tag-line helpers ---

func findTagLine(s string) string {
	for _, line := range strings.Split(s, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, captionTag) {
			return line
		}
	}
	return ""
}

func parseTagLine(line string) map[string]string {
	kv := make(map[string]string)
	for _, tok := range strings.Fields(line) {
		if tok == captionTag {
			continue
		}
		k, v, ok := strings.Cut(tok, "=")
		if !ok {
			continue
		}
		kv[k] = v
	}
	return kv
}

func canonicalKind(k string) string {
	if k == "" {
		return KindTrack
	}
	return k
}

func canonicalLyrics(l string) string {
	if l == "" {
		return "none"
	}
	return l
}

// writeTok writes " key=val" unless val equals the omitted default.
func writeTok(b *strings.Builder, key, val, omitDefault string) {
	if val == "" || val == omitDefault {
		return
	}
	b.WriteString(" ")
	b.WriteString(key)
	b.WriteString("=")
	b.WriteString(val)
}

func writeStr(b *strings.Builder, key, val string) {
	if val == "" {
		return
	}
	b.WriteString(" ")
	b.WriteString(key)
	b.WriteString("=")
	b.WriteString(val)
}

func writeInt(b *strings.Builder, key string, val int) {
	if val == 0 {
		return
	}
	b.WriteString(" ")
	b.WriteString(key)
	b.WriteString("=")
	b.WriteString(strconv.Itoa(val))
}

func writeI64(b *strings.Builder, key string, val int64) {
	if val == 0 {
		return
	}
	b.WriteString(" ")
	b.WriteString(key)
	b.WriteString("=")
	b.WriteString(strconv.FormatInt(val, 10))
}

func valueOr(kv map[string]string, key, def string) string {
	if v, ok := kv[key]; ok && v != "" {
		return v
	}
	return def
}

func atoi(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}

func atoi64(s string) int64 {
	n, _ := strconv.ParseInt(s, 10, 64)
	return n
}
