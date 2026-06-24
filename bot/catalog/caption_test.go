package catalog

import (
	"reflect"
	"strings"
	"testing"
)

// canonical returns m with Variant filled from VariantKey, matching how Phase 1
// stamps it before FormatCaption. The round-trip is defined over canonical metas
// (AppleURL is intentionally not carried in the caption — see caption.go).
func canonical(m TrackMeta) TrackMeta {
	if m.Kind == "" {
		m.Kind = KindTrack
	}
	if m.LyricsSync == "" {
		m.LyricsSync = "none"
	}
	m.Variant = VariantKey(m)
	return m
}

func TestCaptionRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		in   TrackMeta
	}{
		{"full alac", TrackMeta{
			AppleTrackID: 1440913903, AppleAlbumID: 1440913166,
			ISRC: "USUM71807062", Title: "Title One", Artist: "Some Artist",
			Album: "The Album", Format: "alac", SampleRate: 192000, BitDepth: 24,
			Storefront: "us", DurationSec: 213, SizeBytes: 41234567,
			Kind: KindTrack, LyricsSync: "word", CoverEmbedded: true,
		}},
		{"aac with bitrate", TrackMeta{
			AppleTrackID: 100, Format: "aac", Bitrate: 256, SampleRate: 44100,
			Title: "Song", Artist: "Band", Album: "Rec", Storefront: "gb",
			DurationSec: 180, SizeBytes: 6000000, Kind: KindTrack,
			LyricsSync: "line", CoverEmbedded: true,
		}},
		{"atmos", TrackMeta{
			AppleTrackID: 200, Format: "atmos", Title: "A", Artist: "B",
			Album: "C", Kind: KindTrack, LyricsSync: "none", CoverEmbedded: false,
		}},
		{"flac no cover no lyrics", TrackMeta{
			AppleTrackID: 300, Format: "flac", SampleRate: 96000, BitDepth: 24,
			Title: "T", Artist: "Ar", Album: "Al", Kind: KindTrack,
		}},
		{"album zip", TrackMeta{
			AppleAlbumID: 999, Format: "alac", Album: "Whole Album",
			Kind: KindAlbumZip, SizeBytes: 500000000,
		}},
		{"minimal foreign-ish (isrc only)", TrackMeta{
			ISRC: "GBAYE0601498", Format: "flac", Kind: KindTrack,
		}},
		{"empty title block", TrackMeta{
			AppleTrackID: 42, Format: "aac", Bitrate: 64, Kind: KindTrack,
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := canonical(tc.in)
			caption := FormatCaption(in)

			// The tag line must be a single line.
			tag := findTagLine(caption)
			if tag == "" {
				t.Fatalf("no #karenidx tag line in caption:\n%s", caption)
			}
			if strings.Contains(tag, "\n") {
				t.Fatalf("tag line is not single-line: %q", tag)
			}
			// Never emit adam=0.
			if strings.Contains(tag, "adam=0 ") || strings.HasSuffix(tag, "adam=0") {
				t.Fatalf("emitted adam=0: %q", tag)
			}

			got, ok := ParseCaption(caption)
			if !ok {
				t.Fatalf("ParseCaption returned ok=false for:\n%s", caption)
			}
			if !reflect.DeepEqual(got, in) {
				t.Fatalf("round-trip mismatch\n in:  %+v\n got: %+v\n caption:\n%s", in, got, caption)
			}
		})
	}
}

func TestParseCaptionNoTag(t *testing.T) {
	for _, s := range []string{"", "just a plain caption", "🎵 Song — Artist\n💿 Album"} {
		if _, ok := ParseCaption(s); ok {
			t.Fatalf("expected ok=false for non-karenidx caption %q", s)
		}
	}
}

func TestVariantKey(t *testing.T) {
	cases := map[string]string{
		"alac":     "v1|q=lossless", // ALAC + FLAC collapse to one lossless tier
		"flac":     "v1|q=lossless",
		"aac":      "v1|q=aac",
		"atmos":    "v1|q=atmos",
		"binaural": "", // exotic → not cacheable
		"":         "",
	}
	for format, want := range cases {
		if got := VariantKey(TrackMeta{Format: format}); got != want {
			t.Fatalf("VariantKey(%q) = %q, want %q", format, got, want)
		}
	}
}
