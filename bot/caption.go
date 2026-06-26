package main

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"main/catalog"
)

// This file is the package-main glue that builds a catalog.TrackMeta from Karen's
// rip state (AudioMeta + Config). The identity record, the #karenidx caption
// grammar, the quality-tier VariantKey, and ParseCaption are all owned by package
// catalog (the canonical home); these builders just translate local rip data into
// that shared shape before handing it to the upload Pool, which writes the caption
// (catalog.FormatCaption) and the inline catalog row.

// buildTrackMeta assembles the catalog.TrackMeta for a downloaded audio file from
// its recorded metadata, the effective format, and the global rip config. Cosmetic
// fields (LyricsSync, CoverEmbedded) are descriptive only and never enter the
// variant key (D10).
func buildTrackMeta(path, format string, meta AudioMeta, hasMeta bool) catalog.TrackMeta {
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

	m := catalog.TrackMeta{
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
		Kind:          catalog.KindTrack,
		LyricsSync:    lrc,
		CoverEmbedded: Config.EmbedCover,
	}
	m.Variant = catalog.VariantKey(m)
	return m
}

// albumZipMeta builds the catalog.TrackMeta for an album-as-zip artifact
// (kind=album_zip, §4.6/D9). displayName becomes the dump caption's title; the
// quality tier still applies to the whole zip.
func albumZipMeta(albumID, format, displayName string) catalog.TrackMeta {
	m := catalog.TrackMeta{
		Kind:   catalog.KindAlbumZip,
		Title:  displayName,
		Album:  displayName,
		Format: normalizeFormatTier(format, AudioMeta{}),
	}
	if id, err := strconv.ParseInt(strings.TrimSpace(albumID), 10, 64); err == nil {
		m.AppleAlbumID = id
	}
	m.Variant = catalog.VariantKey(m)
	return m
}

// normalizeFormatTier maps Karen's effective format string (and, as a fallback, the
// track's recorded codec) to a canonical format {alac|aac|atmos|flac}; catalog's
// VariantKey then collapses alac/flac → the single "lossless" tier. Defaults to
// "aac" when nothing else matches (AAC is Karen's default).
func normalizeFormatTier(format string, meta AudioMeta) string {
	// Prefer the REAL codec probed from the delivered file (recordDownloadedTrack
	// via ffprobe) over the requested format. They diverge on fallbacks (lossless→
	// AAC, Widevine AAC) and -atmos degradation, and only the real format may set
	// the catalog tier — otherwise an AAC file gets indexed as q=lossless and a
	// future lossless request is served that AAC file.
	switch strings.ToLower(strings.TrimSpace(meta.ActualFormat)) {
	case "alac":
		return "alac"
	case "flac":
		return "flac"
	case "aac":
		return "aac"
	case "atmos":
		return "atmos"
	}
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
