package catalog

import (
	"context"
	"fmt"
)

// =============================================================================
// bot_settings — the /configure live-settings overlay (write-through + boot-restore).
//
// A tiny key/value table holding ONLY the settings an admin explicitly changed from
// inside Telegram. Like quota.go these methods are best-effort: every one is a no-op
// when the catalog is disabled (no DATABASE_URL / DB unreachable). That is the whole
// safety story for the overlay — with the table empty or unreachable, the bot reads
// its boot config exactly as before, so shipping this feature never clobbers the
// operator's current VPS config. Only keys with a row here override anything.
//
// Values are stored as text; the main package owns typing (int / bool / enum) via its
// effective-getter layer. Keeping the column untyped means new settings need no
// migration — just a new key.
// =============================================================================

// SettingsLoadAll returns every override row as a key→value map. Called once at boot
// to seed the in-memory overlay. Returns nil (no error) when disabled, so the caller
// starts from an empty overlay = pure config passthrough.
func (c *Catalog) SettingsLoadAll(ctx context.Context) (map[string]string, error) {
	if !c.Enabled() {
		return nil, nil
	}
	rows, err := c.pool.Query(ctx, `select key, value from bot_settings`)
	if err != nil {
		return nil, fmt.Errorf("settings load: %w", err)
	}
	defer rows.Close()
	out := make(map[string]string)
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return nil, fmt.Errorf("settings load scan: %w", err)
		}
		out[k] = v
	}
	return out, rows.Err()
}

// SettingsUpsert writes (or overwrites) one override. The in-memory overlay is the
// authoritative copy the bot reads; this row is the durable mirror so the change
// survives a restart. No-op when disabled.
func (c *Catalog) SettingsUpsert(ctx context.Context, key, value string) error {
	if !c.Enabled() {
		return nil
	}
	_, err := c.pool.Exec(ctx, `
		insert into bot_settings (key, value, updated_at)
		values ($1, $2, now())
		on conflict (key) do update set value = excluded.value, updated_at = now()`,
		key, value)
	if err != nil {
		return fmt.Errorf("settings upsert %s: %w", key, err)
	}
	return nil
}

// SettingsDelete removes an override, so the key falls back to the boot config /
// default again (reset-to-default). Idempotent; no-op when disabled.
func (c *Catalog) SettingsDelete(ctx context.Context, key string) error {
	if !c.Enabled() {
		return nil
	}
	_, err := c.pool.Exec(ctx, `delete from bot_settings where key = $1`, key)
	if err != nil {
		return fmt.Errorf("settings delete %s: %w", key, err)
	}
	return nil
}
