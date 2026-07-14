package catalog

import (
	"context"
	"fmt"
)

// =============================================================================
// accounts — Apple-Music credentials, the source of truth for /configure account
// management (Phase 3) and the deploy renderer (Phase 4).
//
// Today accounts live in hand-edited .env (APPLE_ID_1..N, gapless), read by
// generate.sh at deploy time. Moving them here lets the bot add/remove/disable
// accounts from inside Telegram; a later `[deploy]` renders generate.sh's inputs
// from these rows and recreates the wrapper containers.
//
// Unlike the settings overlay these methods are NOT no-op-when-disabled in a way the
// caller can ignore: account management is meaningless without the DB, so the UI
// gates on catalog.Enabled() and never opens the panel when disabled. The methods
// still guard defensively (empty/no-op) so a disabled catalog can't panic.
//
// Credentials are stored in plaintext — same trust level as the .env they replace
// (private DB, admin-only). No encryption at rest by decision (§7.3).
// =============================================================================

// AccountStatus values.
const (
	AccountActive   = "active"
	AccountDisabled = "disabled" // "shut-down": kept, but skipped when rendering containers
)

// Account is one Apple-Music credential row. ID is a stable surrogate key (NOT the
// wrapper slot number — that's derived at render time from active-account order).
type Account struct {
	ID        int64
	AppleID   string
	ApplePass string
	Status    string
}

// AccountsLoadAll returns every account in insertion order (stable slot ordering for
// the deploy renderer). Empty slice when disabled.
func (c *Catalog) AccountsLoadAll(ctx context.Context) ([]Account, error) {
	if !c.Enabled() {
		return nil, nil
	}
	rows, err := c.pool.Query(ctx, `
		select id, apple_id, apple_pass, status
		from accounts order by id`)
	if err != nil {
		return nil, fmt.Errorf("accounts load: %w", err)
	}
	defer rows.Close()
	var out []Account
	for rows.Next() {
		var a Account
		if err := rows.Scan(&a.ID, &a.AppleID, &a.ApplePass, &a.Status); err != nil {
			return nil, fmt.Errorf("accounts load scan: %w", err)
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// AccountInsert adds a new account (active) and returns its assigned id. Errors when
// disabled — the caller must have checked Enabled() first.
func (c *Catalog) AccountInsert(ctx context.Context, appleID, applePass string) (int64, error) {
	if !c.Enabled() {
		return 0, fmt.Errorf("accounts: catalog disabled")
	}
	var id int64
	err := c.pool.QueryRow(ctx, `
		insert into accounts (apple_id, apple_pass, status)
		values ($1, $2, 'active')
		returning id`,
		appleID, applePass).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("account insert: %w", err)
	}
	return id, nil
}

// AccountUpdateCreds changes an account's Apple ID and/or password. No-op when disabled.
func (c *Catalog) AccountUpdateCreds(ctx context.Context, id int64, appleID, applePass string) error {
	if !c.Enabled() {
		return fmt.Errorf("accounts: catalog disabled")
	}
	_, err := c.pool.Exec(ctx, `
		update accounts set apple_id = $2, apple_pass = $3 where id = $1`,
		id, appleID, applePass)
	if err != nil {
		return fmt.Errorf("account update creds %d: %w", id, err)
	}
	return nil
}

// AccountSetStatus flips an account between active and disabled (shut-down). No-op
// when disabled.
func (c *Catalog) AccountSetStatus(ctx context.Context, id int64, status string) error {
	if !c.Enabled() {
		return fmt.Errorf("accounts: catalog disabled")
	}
	_, err := c.pool.Exec(ctx, `update accounts set status = $2 where id = $1`, id, status)
	if err != nil {
		return fmt.Errorf("account set status %d=%s: %w", id, status, err)
	}
	return nil
}

// AccountDelete removes an account permanently. No-op when disabled.
func (c *Catalog) AccountDelete(ctx context.Context, id int64) error {
	if !c.Enabled() {
		return fmt.Errorf("accounts: catalog disabled")
	}
	_, err := c.pool.Exec(ctx, `delete from accounts where id = $1`, id)
	if err != nil {
		return fmt.Errorf("account delete %d: %w", id, err)
	}
	return nil
}
