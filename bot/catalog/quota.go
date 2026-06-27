package catalog

import (
	"context"
	"fmt"
)

// =============================================================================
// Heavy-rip per-day quota persistence (write-through + boot-restore ONLY).
//
// These methods are the DURABLE mirror of the bot's in-memory quota. They are
// deliberately best-effort: every one is a no-op when the catalog is disabled
// (no DATABASE_URL / DB unreachable at boot), exactly like the rest of the
// catalog. That is SAFE here because the in-memory quota in the main package —
// not these rows — is what gates admission. A DB blip therefore never lifts the
// quota into "unlimited"; at worst a restart forgets some of today's usage,
// which fails toward the limit, never past it.
// =============================================================================

// QuotaCharge is one persisted charge row, used to restore in-memory state at boot.
type QuotaCharge struct {
	ChargeID      string
	UserID        int64
	Kind          string
	State         string // open | committed | refunded
	ReleasesTotal int
	Delivered     int
}

// QuotaInsertCharge records a freshly-admitted charge in the open state. Keyed by
// charge_id; a replay (same id) is ignored so the write is idempotent. day is the
// bot's 1PM-Dhaka quota-day key as "YYYY-MM-DD". No-op when disabled.
func (c *Catalog) QuotaInsertCharge(ctx context.Context, chargeID string, userID int64, kind, day string, releasesTotal int) error {
	if !c.Enabled() {
		return nil
	}
	_, err := c.pool.Exec(ctx, `
		insert into quota_charges (charge_id, user_id, kind, quota_day, state, releases_total)
		values ($1, $2, $3, $4, 'open', $5)
		on conflict (charge_id) do nothing`,
		chargeID, userID, kind, day, releasesTotal)
	if err != nil {
		return fmt.Errorf("quota insert charge %s: %w", chargeID, err)
	}
	return nil
}

// QuotaMarkCommitted finalizes a charge as consumed. Only an open row transitions
// (a backstop alongside the in-memory CAS), so a late duplicate is a no-op. No-op
// when disabled.
func (c *Catalog) QuotaMarkCommitted(ctx context.Context, chargeID string, delivered int) error {
	return c.quotaFinalize(ctx, chargeID, "committed", delivered)
}

// QuotaMarkRefunded finalizes a charge as refunded (the slot is returned). Only an
// open row transitions. No-op when disabled.
func (c *Catalog) QuotaMarkRefunded(ctx context.Context, chargeID string, delivered int) error {
	return c.quotaFinalize(ctx, chargeID, "refunded", delivered)
}

func (c *Catalog) quotaFinalize(ctx context.Context, chargeID, state string, delivered int) error {
	if !c.Enabled() {
		return nil
	}
	_, err := c.pool.Exec(ctx, `
		update quota_charges
		set state = $2, delivered = $3, finalized_at = now()
		where charge_id = $1 and state = 'open'`,
		chargeID, state, delivered)
	if err != nil {
		return fmt.Errorf("quota finalize %s→%s: %w", chargeID, state, err)
	}
	return nil
}

// QuotaLoadDay returns every charge row for the given quota-day, used at boot to
// rebuild the in-memory usage tally and open-charge records. Returns nil (no error)
// when disabled, so the caller starts from an empty — but still enforcing — memory.
func (c *Catalog) QuotaLoadDay(ctx context.Context, day string) ([]QuotaCharge, error) {
	if !c.Enabled() {
		return nil, nil
	}
	rows, err := c.pool.Query(ctx, `
		select charge_id, user_id, kind, state, releases_total, delivered
		from quota_charges where quota_day = $1`, day)
	if err != nil {
		return nil, fmt.Errorf("quota load day %s: %w", day, err)
	}
	defer rows.Close()
	var out []QuotaCharge
	for rows.Next() {
		var q QuotaCharge
		if err := rows.Scan(&q.ChargeID, &q.UserID, &q.Kind, &q.State, &q.ReleasesTotal, &q.Delivered); err != nil {
			return nil, fmt.Errorf("quota load day scan: %w", err)
		}
		out = append(out, q)
	}
	return out, rows.Err()
}

// QuotaPruneBefore deletes charge rows for quota-days strictly before day. Called
// from the daily reset to keep the table bounded. No-op when disabled.
func (c *Catalog) QuotaPruneBefore(ctx context.Context, day string) error {
	if !c.Enabled() {
		return nil
	}
	_, err := c.pool.Exec(ctx, `delete from quota_charges where quota_day < $1`, day)
	if err != nil {
		return fmt.Errorf("quota prune before %s: %w", day, err)
	}
	return nil
}
