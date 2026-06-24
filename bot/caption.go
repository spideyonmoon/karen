package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// TrackMeta is the identity+quality record passed between the upload engine
// (Phase 1) and the catalog (Phase 2). It is defined here as a PRE-MERGE MIRROR
// of the canonical struct that Phase 2 owns in package bot/catalog (see
// docs/CATALOG_ARCHITECTURE.md §4.1). Field names and encoding are kept
// byte-for-byte identical so the merge is mechanical: at merge time this struct,
// VariantKey, and FormatCaption move to bot/catalog and package main imports them.
//
// PHASE-MERGE: replace this mirror with an import of bot/catalog.TrackMeta.
type TrackMeta struct {
	AppleTrackID int64  // adamID — PRIMARY identity. 0 if unknown (foreign dump).
	AppleAlbumID int64  // 0 if unknown
	AppleURL     string // canonical music.apple.com link, if known
	ISRC         string // uppercase, 12 chars, "" if unknown
	Title        string
	Artist       string
	Album        string
	Format       string // one of: "alac" | "aac" | "atmos" | "flac"
	Bitrate      int    // kbps; 0 if lossless/unknown
	SampleRate   int    // Hz; 0 if unknown
	BitDepth     int    // bits; 0 if unknown
	Storefront   string // e.g. "us"; "" if unknown
	DurationSec  int    // 0 if unknown
	SizeBytes    int64  // 0 if unknown

	// --- artifact kind + byte-variation (see §4.6) ---
	Kind          string // "track" (default) | "album_zip" | "cover" | "animated" | "lrc"
	LyricsSync    string // DESCRIPTIVE only (not in Variant): "none" | "line" | "word"
	CoverEmbedded bool   // DESCRIPTIVE only (not in Variant): embedded cover present
	Variant       string // cache-identity QUALITY tier (§4.6); "" = non-cacheable
}

// QualityTier collapses a ripped format into the cache-identity quality tier.
// This SUPERSEDES the earlier format-tier (coordinated with Phase 2,
// feat/catalog-db): the audio container is not a cache dimension, so ALAC and
// FLAC both collapse to "lossless". Anything not recognized as a cacheable tier
// returns "" — meaning "not cacheable": such a rip is uploaded and delivered but
// MUST NOT be indexed.
//
//	alac, flac → lossless    aac → aac    atmos → atmos    (other) → ""
//
// PHASE-MERGE: catalog.QualityTier is the source of truth; delete this mirror.
func QualityTier(format string) string {
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "alac", "flac":
		return "lossless"
	case "aac":
		return "aac"
	case "atmos":
		return "atmos"
	default:
		return ""
	}
}

// VariantKey is the cache-identity key: v1|q=<tier> (§4.6). It returns "" when the
// format is non-cacheable (QualityTier == ""), which callers treat as "upload &
// deliver, but do not index". Cosmetic options (cover, lyrics sync, embed flags)
// are deliberately NOT in the key — they are embedded maximally into every file.
//
// Phase 2 MUST produce this string identically; catalog.VariantKey is canonical.
func VariantKey(m TrackMeta) string {
	tier := QualityTier(m.Format)
	if tier == "" {
		return ""
	}
	return "v1|q=" + tier
}

// kindOrDefault returns m.Kind, defaulting to "track" when unset.
func kindOrDefault(m TrackMeta) string {
	if m.Kind == "" {
		return "track"
	}
	return m.Kind
}

// lyricsSyncOrDefault returns m.LyricsSync, defaulting to "none" when unset.
func lyricsSyncOrDefault(m TrackMeta) string {
	if m.LyricsSync == "" {
		return "none"
	}
	return m.LyricsSync
}

// FormatCaption builds the dump-channel caption for an upload (§4.2). It is a
// human-readable block plus ONE machine-parseable, versioned tag line that Phase
// 2's ParseCaption round-trips. Rules:
//   - the "#karenidx v=1 …" line is single-line, space-separated key=value;
//   - a key is omitted entirely when its value is empty/zero (the parser treats
//     a missing key as zero/empty) — in particular we never emit adam=0;
//   - the tag line carries only IDs/enums/numbers (token-safe, no spaces);
//     Title/Artist/Album live ONLY in the human block.
//
// The result stays well within Telegram's 1024-char caption limit: the tag line
// is short and the only long fields (Title/Artist/Album) are realistically small.
func FormatCaption(m TrackMeta) string {
	var human strings.Builder
	human.WriteString("🎵 " + m.Title)
	if m.Artist != "" {
		human.WriteString(" — " + m.Artist)
	}
	human.WriteString("\n💿 ")
	if m.Album != "" {
		human.WriteString(m.Album + " · ")
	}
	human.WriteString(strings.ToUpper(m.Format))
	if q := humanQuality(m); q != "" {
		human.WriteString(" " + q)
	}

	// Tag line — emit keys in the §4.2 documented order, omitting zero/empty.
	tag := []string{"#karenidx", "v=1", "kind=" + kindOrDefault(m)}
	if m.AppleTrackID != 0 {
		tag = append(tag, fmt.Sprintf("adam=%d", m.AppleTrackID))
	}
	if m.AppleAlbumID != 0 {
		tag = append(tag, fmt.Sprintf("album=%d", m.AppleAlbumID))
	}
	if m.ISRC != "" {
		tag = append(tag, "isrc="+strings.ToUpper(m.ISRC))
	}
	if m.Format != "" {
		tag = append(tag, "fmt="+m.Format)
	}
	if m.Bitrate != 0 {
		tag = append(tag, fmt.Sprintf("br=%d", m.Bitrate))
	}
	if m.SampleRate != 0 {
		tag = append(tag, fmt.Sprintf("sr=%d", m.SampleRate))
	}
	if m.BitDepth != 0 {
		tag = append(tag, fmt.Sprintf("bd=%d", m.BitDepth))
	}
	if m.Storefront != "" {
		tag = append(tag, "sf="+m.Storefront)
	}
	if m.DurationSec != 0 {
		tag = append(tag, fmt.Sprintf("dur=%d", m.DurationSec))
	}
	if m.SizeBytes != 0 {
		tag = append(tag, fmt.Sprintf("sz=%d", m.SizeBytes))
	}
	if ls := lyricsSyncOrDefault(m); ls != "none" {
		tag = append(tag, "lrc="+ls)
	}
	if m.CoverEmbedded {
		tag = append(tag, "cov=1")
	}
	// var is omitted when the tier is non-cacheable ("") — per the omit-empty rule,
	// the crawler reconstructs the tier from fmt via QualityTier when var is absent.
	variant := m.Variant
	if variant == "" {
		variant = VariantKey(m)
	}
	if variant != "" {
		tag = append(tag, "var="+variant)
	}

	return human.String() + "\n\n" + strings.Join(tag, " ")
}

// buildTrackMeta assembles the TrackMeta for a downloaded audio file from its
// recorded metadata, the effective format tier, and the global rip config. It is
// the bridge between Karen's existing rip state and the catalog identity record:
// the caller hands it to the Pool, which both writes the dump caption and (post
// Phase-2 merge) the inline catalog row. Cosmetic fields (LyricsSync,
// CoverEmbedded) are descriptive only and never enter the variant key (D10).
func buildTrackMeta(path, format string, meta AudioMeta, hasMeta bool) TrackMeta {
	title := filepath.Base(path)
	performer, album, isrc := "", "", ""
	durationSec := 0
	var adam, albumID int64
	if hasMeta {
		if meta.Title != "" {
			title = meta.Title
		}
		performer = meta.Performer
		album = meta.AlbumName
		isrc = meta.ISRC
		if meta.DurationMillis > 0 {
			durationSec = int(meta.DurationMillis / 1000)
		}
		if id, err := strconv.ParseInt(strings.TrimSpace(meta.TrackID), 10, 64); err == nil {
			adam = id
		}
		if id, err := strconv.ParseInt(strings.TrimSpace(meta.AlbumID), 10, 64); err == nil {
			albumID = id
		}
	}

	var sizeBytes int64
	if info, err := os.Stat(path); err == nil {
		sizeBytes = info.Size()
	}

	lrc := "none"
	if Config.EmbedLrc {
		// We embed the richest available (time-synced) lyrics, which degrade to
		// line-by-line in players that ignore word timing (§4.6). Descriptive only.
		lrc = "word"
	}

	m := TrackMeta{
		AppleTrackID:  adam,
		AppleAlbumID:  albumID,
		ISRC:          isrc,
		Title:         title,
		Artist:        performer,
		Album:         album,
		Format:        normalizeFormatTier(format, meta),
		Storefront:    Config.Storefront,
		DurationSec:   durationSec,
		SizeBytes:     sizeBytes,
		Kind:          "track",
		LyricsSync:    lrc,
		CoverEmbedded: Config.EmbedCover,
	}
	m.Variant = VariantKey(m)
	return m
}

// albumZipMeta builds the TrackMeta for an album-as-zip artifact (kind=album_zip,
// §4.6/D9). displayName becomes the dump caption's title; the format tier still
// applies to the whole zip.
func albumZipMeta(albumID, format, displayName string) TrackMeta {
	m := TrackMeta{
		Kind:   "album_zip",
		Title:  displayName,
		Album:  displayName,
		Format: normalizeFormatTier(format, AudioMeta{}),
	}
	if id, err := strconv.ParseInt(strings.TrimSpace(albumID), 10, 64); err == nil {
		m.AppleAlbumID = id
	}
	m.Variant = VariantKey(m)
	return m
}

// normalizeFormatTier maps Karen's effective format string (and, as a fallback,
// the track's recorded codec) to the canonical variant tier {alac|aac|atmos|flac}.
// Defaults to "aac" when nothing else matches (AAC is Karen's default tier).
func normalizeFormatTier(format string, meta AudioMeta) string {
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "alac":
		return "alac"
	case "flac":
		return "flac"
	case "aac":
		return "aac"
	case "atmos":
		return "atmos"
	}
	c := strings.ToLower(meta.Codec)
	switch {
	case strings.Contains(c, "alac"):
		return "alac"
	case strings.Contains(c, "flac"):
		return "flac"
	case strings.Contains(c, "atmos"), strings.Contains(c, "ec-3"), strings.Contains(c, "ec3"), strings.Contains(c, "eac3"):
		return "atmos"
	case strings.Contains(c, "aac"):
		return "aac"
	}
	return "aac"
}

// humanQuality renders a short human-readable quality string for the caption's
// display block (NOT the tag line). Best-effort; absence is fine.
func humanQuality(m TrackMeta) string {
	switch m.Format {
	case "atmos":
		return "Dolby Atmos"
	case "aac":
		if m.Bitrate > 0 {
			return fmt.Sprintf("%dkbps", m.Bitrate)
		}
		return ""
	default: // alac / flac / unknown lossless
		switch {
		case m.BitDepth > 0 && m.SampleRate > 0:
			return fmt.Sprintf("%d-bit/%.3gkHz", m.BitDepth, float64(m.SampleRate)/1000)
		case m.SampleRate > 0:
			return fmt.Sprintf("%.3gkHz", float64(m.SampleRate)/1000)
		default:
			return ""
		}
	}
}
