package main

import (
	"context"
	"fmt"
	"time"
)

// =============================================================================
// Per-day heavy-rip quota — in-memory AUTHORITATIVE state.
//
// Replaces the old in-flight cap (countUserHeavyRips), which had a loophole:
// inside the 02:30–06:00 sleeptime window heavy rips run immediately, so once one
// finished the in-flight count dropped to 0 and a user could chain rips all night.
//
// Model: a slot is CHARGED at submission (scheduleOrRun) and REFUNDED if the rip
// doesn't successfully finish — so failures/cancels don't burn the day, but a
// completed (or substantially-delivered) rip consumes it. Limits come from
// heavyRipLimit(kind, donor); the tally is per 1PM-Dhaka calendar day.
//
// quotaUsage is the gate the admission path reads; quotaCharges makes refunds
// idempotent via a state CAS (open → committed|refunded). Both are guarded by
// quotaMu, a strict LEAF lock (declared in TelegramBot): never take queueMu/stateMu
// while holding it, and never hold it across DB I/O or a send. The catalog rows
// (catalog/quota.go) are a best-effort mirror only — the quota stays enforced from
// these maps even when the DB is down, so a blip can never lift it into "unlimited".
// =============================================================================

const (
	quotaOpen      = "open"
	quotaCommitted = "committed"
	quotaRefunded  = "refunded"

	// quotaResetHourDhaka is the wall-clock hour (Dhaka) the daily tally resets and
	// the group is notified. 13:00 sits safely AFTER the 02:30–06:00 run window, so a
	// night's rips never straddle two quota-days.
	quotaResetHourDhaka = 13

	quotaDBTimeout = 5 * time.Second
)

// quotaKey identifies one user's tally for a kind on a given quota-day.
type quotaKey struct {
	userID int64
	day    string // 1PM-Dhaka day key, "YYYY-MM-DD"
	kind   string // "artist" | "playlist"
}

// quotaCharge is a single live charge. state walks open → committed|refunded; once
// it leaves open, refund/commit are no-ops (idempotency). Kept until pruned at the
// day rollover.
type quotaCharge struct {
	userID int64
	kind   string
	day    string
	state  string
}

// quotaDay returns the quota-day key for t: a quota-day runs from 1PM Dhaka to the
// next 1PM Dhaka, keyed by the date it began. Shifting back 13h puts the 1PM
// boundary at local midnight, so the date of the shifted time is the key. The
// admission gate always calls this with time.Now(), so the tally rolls over on its
// own even if the reset routine is late.
func quotaDay(t time.Time) string {
	return t.In(dhakaZone).Add(-quotaResetHourDhaka * time.Hour).Format("2006-01-02")
}

// quotaCheckAndCharge admits a heavy rip of kind for userID and charges a slot,
// returning the new chargeID. ok=false means the user is at their per-day limit for
// this kind (caller rejects). A zero userID (headless) is admitted without a charge
// (chargeID ""), matching the old cap which never gated userID==0; an empty chargeID
// is a no-op everywhere downstream.
func (b *TelegramBot) quotaCheckAndCharge(userID int64, username, kind string) (string, bool) {
	if userID == 0 {
		return "", true
	}
	// isUserDonor takes stateMu — resolve it BEFORE taking quotaMu (leaf order: the
	// two are never nested).
	donor := b.isUserDonor(userID, username)
	limit := heavyRipLimit(kind, donor)
	day := quotaDay(time.Now())
	key := quotaKey{userID: userID, day: day, kind: kind}

	b.quotaMu.Lock()
	if b.quotaUsage[key] >= limit {
		b.quotaMu.Unlock()
		return "", false
	}
	chargeID := generateTaskID()
	b.quotaUsage[key]++
	b.quotaCharges[chargeID] = &quotaCharge{userID: userID, kind: kind, day: day, state: quotaOpen}
	b.quotaMu.Unlock()

	// Write-through (best-effort, no lock held). scheduleOrRun runs off the update
	// loop, so a brief DB round-trip here is fine.
	if b.catalog != nil {
		ctx, cancel := context.WithTimeout(context.Background(), quotaDBTimeout)
		defer cancel()
		if err := b.catalog.QuotaInsertCharge(ctx, chargeID, userID, kind, day, 0); err != nil {
			fmt.Printf("quota insert %s: %v\n", chargeID, err)
		}
	}
	return chargeID, true
}

// quotaCommit consumes a charge (it stays counted for the day). Idempotent.
func (b *TelegramBot) quotaCommit(chargeID string, delivered int) {
	b.quotaSetState(chargeID, quotaCommitted, delivered, false)
}

// quotaRefund returns a charge's slot to the user. Idempotent. DB write is async so
// it never blocks the caller's locks.
func (b *TelegramBot) quotaRefund(chargeID string, delivered int) {
	b.quotaSetState(chargeID, quotaRefunded, delivered, false)
}

// quotaRefundSync is quotaRefund with a SYNCHRONOUS DB write, for the /restart path
// (cancelAllTasksLocked → os.Exit): the refund must hit Postgres before the process
// exits, or boot-load would restore the charge as committed and the user would never
// get it back. The in-memory CAS still makes a racing after() refund a no-op.
func (b *TelegramBot) quotaRefundSync(chargeID string, delivered int) {
	b.quotaSetState(chargeID, quotaRefunded, delivered, true)
}

// quotaSetState performs the open→newState CAS in memory (decrementing usage on a
// refund), then mirrors it to the catalog. syncDB picks a blocking write (restart)
// vs a fire-and-forget goroutine (everything else, so caller locks are never held
// across I/O).
func (b *TelegramBot) quotaSetState(chargeID, newState string, delivered int, syncDB bool) {
	if chargeID == "" {
		return
	}
	b.quotaMu.Lock()
	c := b.quotaCharges[chargeID]
	if c == nil || c.state != quotaOpen {
		b.quotaMu.Unlock()
		return // unknown or already finalized — idempotent no-op
	}
	c.state = newState
	if newState == quotaRefunded {
		key := quotaKey{userID: c.userID, day: c.day, kind: c.kind}
		if b.quotaUsage[key] > 0 {
			b.quotaUsage[key]--
		}
	}
	b.quotaMu.Unlock()

	persist := func() {
		if b.catalog == nil {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), quotaDBTimeout)
		defer cancel()
		var err error
		if newState == quotaRefunded {
			err = b.catalog.QuotaMarkRefunded(ctx, chargeID, delivered)
		} else {
			err = b.catalog.QuotaMarkCommitted(ctx, chargeID, delivered)
		}
		if err != nil {
			fmt.Printf("quota %s %s: %v\n", newState, chargeID, err)
		}
	}
	if syncDB {
		persist()
	} else {
		go persist()
	}
}

// finalizeHeavyQuota commits or refunds a heavy rip's charge exactly once, from the
// after() closure that runs when the rip's worker goroutine finishes (success,
// failure, or cancellation). Rules:
//   - ran to completion and delivered something      → commit (slot consumed)
//   - the USER cancelled past the exemption threshold → commit (don't refund real work)
//   - anything else (failure, admin/restart cancel,
//     early user cancel, delivered nothing)          → refund (slot returned)
//
// The exemption threshold is >50% of releases delivered for a per-release artist rip,
// or ≥1 delivered ~20 GB "Part" otherwise. The RipState accessors are nil-safe, the
// quota* calls are idempotent, and a "" chargeID is a no-op, so this is safe to call
// unconditionally from any rip's after().
func (b *TelegramBot) finalizeHeavyQuota(chargeID string, rs *RipState, ctx context.Context, deliveredSomething bool, totalReleases int, perRelease bool) {
	if chargeID == "" {
		return
	}
	delivered := rs.deliveredReleases()
	cancelled := ctx != nil && ctx.Err() != nil
	byOwner := rs.quotaCancelledByOwner()
	var pastThreshold bool
	if perRelease {
		pastThreshold = totalReleases > 0 && delivered*2 > totalReleases
	} else {
		pastThreshold = rs.flushedSomething()
	}
	switch {
	case !cancelled && deliveredSomething:
		b.quotaCommit(chargeID, delivered)
	case cancelled && byOwner && pastThreshold:
		b.quotaCommit(chargeID, delivered)
	default:
		b.quotaRefund(chargeID, delivered)
	}
}

// quotaUsageFor returns userID's current per-day count for kind (for /profile).
func (b *TelegramBot) quotaUsageFor(userID int64, kind string) int {
	day := quotaDay(time.Now())
	b.quotaMu.Lock()
	defer b.quotaMu.Unlock()
	return b.quotaUsage[quotaKey{userID: userID, day: day, kind: kind}]
}

// quotaLoadBoot restores today's tally from the catalog so a restart doesn't wipe
// usage. Runs once at startup AFTER the catalog is wired and loadSchedule has
// populated scheduledJobs. Refunded rows are ignored; open/committed rows count.
// An OPEN row that belongs to a live deferred job is kept open (so its later
// success/cancel still finalizes correctly); an open row with NO live job is the
// remnant of an in-flight rip lost to the restart/crash (those aren't persisted) —
// treat it as committed (loophole-safe: it counts and can't be double-refunded).
func (b *TelegramBot) quotaLoadBoot() {
	if b.catalog == nil || !b.catalog.Enabled() {
		return
	}
	day := quotaDay(time.Now())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	charges, err := b.catalog.QuotaLoadDay(ctx, day)
	if err != nil {
		fmt.Printf("quota boot-load: %v\n", err)
		return
	}

	live := make(map[string]bool)
	b.stateMu.Lock()
	for _, j := range b.scheduledJobs {
		if j.QuotaChargeID != "" {
			live[j.QuotaChargeID] = true
		}
	}
	b.stateMu.Unlock()

	restored := 0
	b.quotaMu.Lock()
	for _, q := range charges {
		if q.State == quotaRefunded {
			continue
		}
		state := q.State
		if state == quotaOpen && !live[q.ChargeID] {
			state = quotaCommitted
		}
		b.quotaCharges[q.ChargeID] = &quotaCharge{userID: q.UserID, kind: q.Kind, day: day, state: state}
		b.quotaUsage[quotaKey{userID: q.UserID, day: day, kind: q.Kind}]++
		restored++
	}
	b.quotaMu.Unlock()
	if restored > 0 {
		fmt.Printf("quota: restored %d charge(s) for %s\n", restored, day)
	}
}

// quotaPrune drops in-memory entries for past quota-days and deletes their rows.
// Called at the daily reset; cheap and keeps both memory and the table bounded.
func (b *TelegramBot) quotaPrune() {
	day := quotaDay(time.Now())
	b.quotaMu.Lock()
	for k := range b.quotaUsage {
		if k.day != day {
			delete(b.quotaUsage, k)
		}
	}
	for id, c := range b.quotaCharges {
		if c.day != day {
			delete(b.quotaCharges, id)
		}
	}
	b.quotaMu.Unlock()
	if b.catalog != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := b.catalog.QuotaPruneBefore(ctx, day); err != nil {
			fmt.Printf("quota prune: %v\n", err)
		}
	}
}

// startQuotaResetRoutine fires once a day at 1 PM Dhaka: prune the rolled-over day
// and announce the reset to every allowed group chat. Mirrors startBackupRoutine —
// the fire time is computed from the clock, so restarts/deploys never double-fire.
// The tally itself rolls over automatically via quotaDay(now), so a late tick only
// delays the announcement, never the actual reset.
func (b *TelegramBot) startQuotaResetRoutine() {
	go func() {
		for {
			time.Sleep(time.Until(nextDailyAt(time.Now(), quotaResetHourDhaka, 0)))
			b.quotaPrune()
			b.pruneGofileDeliveries()
			b.announceQuotaReset()
		}
	}()
}

// announceQuotaReset posts the daily reset notice to every allowed chat. Skipped
// when no chats are configured (the bot allows all chats → no known target).
func (b *TelegramBot) announceQuotaReset() {
	if len(b.allowedChats) == 0 {
		return
	}
	const msg = "🔄 Heavy-rip quota reset — your discography & huge-playlist limits are fresh for today. (Resets daily at 1 PM Dhaka.)"
	for chatID := range b.allowedChats {
		_ = b.sendMessageWithReply(chatID, msg, nil, 0)
	}
}
