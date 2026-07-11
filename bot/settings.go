package main

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"main/catalog"
)

// =============================================================================
// Live-settings overlay (the /configure Class-A store).
//
// A thin, mutable layer that sits IN FRONT of the boot config (Config) and the
// hardcoded defaults. It holds ONLY the keys an admin explicitly changed from
// inside Telegram — nothing else. Every effective getter resolves as:
//
//     overlay[key]  ??  Config.<field>  ??  code default
//
// so when the overlay is empty (fresh deploy, DB unset, or DB unreachable) the bot
// behaves EXACTLY as it did on its boot config. This is the hard guarantee that
// /configure never clobbers the operator's VPS config and "picks up where they left
// off": untouched settings pass straight through; only deltas are stored.
//
// Storage mirrors the quota template (catalog/settings.go): in-memory map is
// authoritative for reads; writes are write-through to the bot_settings table;
// boot-restore reloads the map once at startup. Best-effort DB — a blip only means a
// changed value might not survive a restart, never that a value is silently reset.
//
// Values are stored as strings; typing lives here. Keys are the seKey* constants.
// =============================================================================

// Setting keys. Stable identifiers — persisted in bot_settings and referenced by the
// /configure UI. Do not rename without a migration.
const (
	seArtistRipsOff       = "artist_rips_off"        // bool: whole-discography artist rips disabled bot-wide
	seArtistDelivery      = "artist_delivery"        // enum: "" (no override) | "gofile" | "tg_individual"
	seTelegramMaxGB       = "telegram_download_max_gb"
	seRipFlushGB          = "rip_flush_threshold_gb"
	seHeavyLimitArtist    = "heavy_limit_artist"     // int: regular-user per-day artist cap
	seHeavyLimitArtistDon = "heavy_limit_artist_donor"
	seHeavyLimitPlaylist  = "heavy_limit_playlist"
	seHeavyLimitPlDonor   = "heavy_limit_playlist_donor"
)

// artistDelivery* are the accepted values for the artist-delivery override.
const (
	artistDeliveryDefault  = ""              // no override: use the built-in Gofile-ZIP behavior
	artistDeliveryGofile   = "gofile"        // force combined/per-release Gofile ZIP (the current hardcode)
	artistDeliveryTelegram = "tg_individual" // deliver each release's tracks to Telegram individually
)

// settingsOverlay is the process-wide overlay. It's a package global (like Config)
// so the package-level effective getters — heavyRipLimit, telegramDownloadMaxBytes,
// ripFlushThresholdBytes — can consult it without threading a receiver through every
// call site. Set once at boot by initSettings; nil-safe (a nil overlay reads as
// "no overrides", i.e. pure config passthrough).
type settingsOverlay struct {
	mu      sync.RWMutex
	values  map[string]string
	catalog *catalog.Catalog // durable mirror; may be disabled (writes no-op)
}

// settingsStore is the live overlay. nil until initSettings runs; every getter
// tolerates nil so ordering during boot can't panic.
var settingsStore *settingsOverlay

// initSettings builds the overlay and boot-restores it from the catalog. Called once
// at startup right after the catalog is wired (telegram_bot.go). A disabled/empty
// catalog yields an empty overlay = config passthrough.
func initSettings(cat *catalog.Catalog) {
	s := &settingsOverlay{values: make(map[string]string), catalog: cat}
	if cat != nil && cat.Enabled() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		loaded, err := cat.SettingsLoadAll(ctx)
		if err != nil {
			fmt.Printf("settings boot-load: %v\n", err)
		} else if len(loaded) > 0 {
			s.values = loaded
			fmt.Printf("settings: restored %d override(s)\n", len(loaded))
		}
	}
	settingsStore = s
}

// getRaw returns the overridden string value and whether an override exists.
func (s *settingsOverlay) getRaw(key string) (string, bool) {
	if s == nil {
		return "", false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.values[key]
	return v, ok
}

// set writes an override (in-memory authoritative + write-through to the DB).
func (s *settingsOverlay) set(key, value string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.values[key] = value
	s.mu.Unlock()
	if s.catalog != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := s.catalog.SettingsUpsert(ctx, key, value); err != nil {
			fmt.Printf("settings upsert %s: %v\n", key, err)
		}
	}
}

// clear removes an override so the key falls back to config/default (reset-to-default).
func (s *settingsOverlay) clear(key string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	delete(s.values, key)
	s.mu.Unlock()
	if s.catalog != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := s.catalog.SettingsDelete(ctx, key); err != nil {
			fmt.Printf("settings delete %s: %v\n", key, err)
		}
	}
}

// -----------------------------------------------------------------------------
// Typed accessors (package-level helpers). Each: overlay ?? fallback.
// -----------------------------------------------------------------------------

// settingBool reads a bool override, falling back to def when unset/unparseable.
func settingBool(key string, def bool) bool {
	if raw, ok := settingsStore.getRaw(key); ok {
		if v, err := strconv.ParseBool(raw); err == nil {
			return v
		}
	}
	return def
}

// settingInt reads an int override, falling back to def when unset/unparseable.
func settingInt(key string, def int) int {
	if raw, ok := settingsStore.getRaw(key); ok {
		if v, err := strconv.Atoi(raw); err == nil {
			return v
		}
	}
	return def
}

// settingString reads a string override, falling back to def when unset.
func settingString(key, def string) string {
	if raw, ok := settingsStore.getRaw(key); ok {
		return raw
	}
	return def
}

// setSettingBool / setSettingInt / clearSetting are the mutation entry points used by
// /configure and the /artistrips command.
func setSettingBool(key string, v bool) { settingsStore.set(key, strconv.FormatBool(v)) }
func setSettingInt(key string, v int)   { settingsStore.set(key, strconv.Itoa(v)) }
func setSettingString(key, v string)    { settingsStore.set(key, v) }
func clearSetting(key string)           { settingsStore.clear(key) }

// -----------------------------------------------------------------------------
// Effective getters for the specific settings (single source of truth per value).
// -----------------------------------------------------------------------------

// artistRipsDisabledEff reports whether artist rips are gated off. Default false
// (enabled) = current behavior.
func artistRipsDisabledEff() bool { return settingBool(seArtistRipsOff, false) }

// artistDeliveryOverride returns the configured artist-delivery override, or
// artistDeliveryDefault ("" = no override, use the built-in Gofile behavior).
func artistDeliveryOverride() string {
	v := settingString(seArtistDelivery, artistDeliveryDefault)
	switch v {
	case artistDeliveryGofile, artistDeliveryTelegram:
		return v
	default:
		return artistDeliveryDefault
	}
}
