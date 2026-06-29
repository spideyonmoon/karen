package catalog

import (
	"context"
	"fmt"
	"time"
)

// =============================================================================
// Gofile re-rip dedup persistence (best-effort, read-through + write-through).
//
// Mirrors the catalog's design philosophy (see db.go / quota.go): every method is
// a safe no-op / empty result when the catalog is disabled, so a missing
// DATABASE_URL or a DB blip simply FAILS OPEN — the bot rips fresh, exactly as it
// did before this feature existed. Dedup must never hard-block a legitimate
// request, so unlike the quota there is deliberately NO in-memory authoritative
// copy: the DB is the only source of truth, and "DB unreachable" maps to "no hit".
//
// One row is written per Gofile link delivered for a collection rip. A multi-part
// heavy rip (discography / huge playlist flushed as several "Part N" ZIPs) writes
// several rows that share one content_key; Lookup returns all of them so a
// re-request relists every part. expires_at (created_at + the bot's 7-day TTL)
// gates lookups; PruneExpired drops the dead rows at the daily reset.
// =============================================================================

// GofileDelivery is one delivered Gofile link. ContentKey is the bot's dedup key
// ("<kind>|<id>|<variant>"); the broken-out kind/content_id/variant columns are
// stored for future querying/debugging and are not required by Lookup.
type GofileDelivery struct {
	ContentKey string
	Kind       string
	ContentID  string
	Variant    string
	Label      string // human label (release/artist title, optionally "(Part N)")
	Link       string
	UserID     int64
	Username   string
	CreatedAt  time.Time
}

// GofileDeliveryLookup returns every non-expired link recorded for contentKey,
// oldest first (so a multi-part rip relists in delivery order). A disabled catalog,
// an empty key, or no rows all return (nil, nil) — i.e. a miss that lets the caller
// rip fresh.
func (c *Catalog) GofileDeliveryLookup(ctx context.Context, contentKey string) ([]GofileDelivery, error) {
	if !c.Enabled() || contentKey == "" {
		return nil, nil
	}
	rows, err := c.pool.Query(ctx, `
		select content_key, kind, content_id, variant, label, gofile_link, user_id, coalesce(username,''), created_at
		from gofile_deliveries
		where content_key = $1 and expires_at > now()
		order by created_at`, contentKey)
	if err != nil {
		return nil, fmt.Errorf("gofile delivery lookup %s: %w", contentKey, err)
	}
	defer rows.Close()
	var out []GofileDelivery
	for rows.Next() {
		var d GofileDelivery
		if err := rows.Scan(&d.ContentKey, &d.Kind, &d.ContentID, &d.Variant, &d.Label, &d.Link, &d.UserID, &d.Username, &d.CreatedAt); err != nil {
			return nil, fmt.Errorf("gofile delivery scan: %w", err)
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// GofileDeliveryRecord inserts one delivered link, expiring ttl from now. No-op when
// disabled or when the key/link is empty (nothing useful to record).
func (c *Catalog) GofileDeliveryRecord(ctx context.Context, d GofileDelivery, ttl time.Duration) error {
	if !c.Enabled() || d.ContentKey == "" || d.Link == "" {
		return nil
	}
	_, err := c.pool.Exec(ctx, `
		insert into gofile_deliveries
			(content_key, kind, content_id, variant, label, gofile_link, user_id, username, expires_at)
		values ($1, $2, $3, $4, $5, $6, $7, $8, now() + $9::interval)`,
		d.ContentKey, d.Kind, d.ContentID, d.Variant, d.Label, d.Link, d.UserID, nullStr(d.Username),
		fmt.Sprintf("%d seconds", int64(ttl.Seconds())))
	if err != nil {
		return fmt.Errorf("gofile delivery record %s: %w", d.ContentKey, err)
	}
	return nil
}

// GofileDeliveryPruneExpired deletes rows whose link has expired. Called at the
// daily reset to keep the table bounded; lookups already filter on expires_at, so
// this is pure housekeeping. No-op when disabled.
func (c *Catalog) GofileDeliveryPruneExpired(ctx context.Context) error {
	if !c.Enabled() {
		return nil
	}
	_, err := c.pool.Exec(ctx, `delete from gofile_deliveries where expires_at < now()`)
	if err != nil {
		return fmt.Errorf("gofile delivery prune: %w", err)
	}
	return nil
}
