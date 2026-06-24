// Package catalog is the Phase 2 read-through catalog: a Postgres DB of pointer
// rows over Telegram dump channels. It owns the canonical TrackMeta identity
// record (§4.1), the caption grammar (§4.2), the schema/migration (§4.3), the
// read-through Lookup (§4.4), inline indexing, and the own-dump crawler.
//
// This package is the canonical home of TrackMeta and the caption helpers; the
// Phase 1 (multi-account upload) branch imports them at merge. It is deliberately
// free of any gotd/Telegram dependency so it stays unit-testable — the crawler
// takes an injected MessageFetcher (indexer.go) and the gotd glue lives in package
// main.
package catalog

import "strings"

// TrackMeta is the identity+quality record both phases pass around (master §4.1).
// Phase 1 writes it into the dump caption and hands it to IndexInline for our own
// uploads; Phase 2 parses it back out of captions when crawling. Field names are
// frozen by the shared contract — do not rename without operator sign-off.
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
	Variant       string // cache-identity tier; v1 = format only (§4.6, D10)
}

// Artifact kinds (D12). "track" is the default for an embedded-cover/lyrics audio
// file; standalone deliverables get their own kind.
const (
	KindTrack    = "track"
	KindAlbumZip = "album_zip"
	KindCover    = "cover"
	KindAnimated = "animated"
	KindLRC      = "lrc"
)

// Quality tiers — the ONLY cache dimensions (operator decision 2026-06-25).
// ALAC and FLAC are one "lossless" tier: lossless audio is bit-identical and the
// container is not worth a duplicate cached file (a FLAC-preferring user is served
// the cached lossless file with a note to re-run -nc for a fresh FLAC rip).
const (
	TierLossless = "lossless"
	TierAAC      = "aac"
	TierAtmos    = "atmos"
)

// QualityTier collapses an Apple/ripper format string into a cache tier. Anything
// outside the three tiers (e.g. binaural) returns "" = NOT cacheable: the caller
// rips fresh and does not store a row.
func QualityTier(format string) string {
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "alac", "flac", "lossless":
		return TierLossless
	case "aac":
		return TierAAC
	case "atmos":
		return TierAtmos
	default:
		return ""
	}
}

// VariantKey is the cache-identity tier (§4.6, D10). v1 = QUALITY TIER only:
//
//	v1|q=lossless   v1|q=aac   v1|q=atmos
//
// It returns "" when the format isn't a cached tier, which both phases MUST treat
// as "do not look up, do not store — always rip." Cosmetics (cover, lyrics, embed
// flags) and the ALAC-vs-FLAC container are deliberately NOT cache dimensions, so
// the cache never forks beyond these three tiers. Both phases MUST produce this key
// identically (Phase 1's pre-merge mirror included).
func VariantKey(m TrackMeta) string {
	tier := QualityTier(m.Format)
	if tier == "" {
		return ""
	}
	return "v1|q=" + tier
}
