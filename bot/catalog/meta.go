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

// VariantKey is the cache-identity tier (§4.6, D10). v1 = format only. Cosmetic
// options (cover, lyrics sync, embed flags) are deliberately NOT in the key: every
// cached file is tagged maximally (highest-res cover + word-synced lyrics, a
// superset of line-synced), so one file serves cover/word/line users alike and the
// cache never forks on cosmetics. The format/column is kept extensible ONLY for a
// future genuinely non-derivable master dimension; do NOT add cosmetic options here.
// Both phases MUST produce this key identically.
func VariantKey(m TrackMeta) string {
	return "v1|fmt=" + m.Format
}
