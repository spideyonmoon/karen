package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"main/catalog"
)

// =============================================================================
// Gofile re-rip dedup (7-day) — see catalog/gofiledelivery.go for the storage and
// docs/CONFIGURE_FEATURE.md / the plan for the rationale.
//
// Telegram delivery already dedups via cached file_ids (trySendCachedTrack /
// trySendCachedAlbumZip). Gofile — the most-used mode — had no equivalent, so
// re-ripping the same collection at the same quality tier burned a full rip plus
// the VPS's capped upload bandwidth every time. This tracks each delivered Gofile
// link in the catalog DB and, on a re-request within the link's ~7-day life,
// returns the existing link(s) instead of ripping again.
//
// The check FAILS OPEN: a disabled catalog, an un-cacheable tier, an empty key, or
// any DB error all mean "no hit → rip normally". Recording is fire-and-forget. So
// the worst a DB problem can do is let an extra rip through — it can never block a
// legitimate request.
// =============================================================================

const (
	// gofileDedupTTL is how long a recorded Gofile link is treated as live. Gofile
	// guest links last about a week; 7 days matches that.
	gofileDedupTTL = 7 * 24 * time.Hour

	gofileDedupDBTimeout = 5 * time.Second
)

// gofileDedupInfo is the identity a rip carries so its Gofile deliveries can be
// recorded under the same key the admission check used. Stored on RipState; nil
// when the rip isn't a dedup-tracked Gofile collection (single song, Telegram
// delivery, artwork, etc.).
type gofileDedupInfo struct {
	key     string
	kind    string // "album" | "playlist" | "artist"
	id      string // apple album/playlist id, or artist id
	variant string // catalog quality tier: "v1|q=lossless" | "v1|q=aac" | "v1|q=atmos"
	label   string // human display name for the report/admin message
	userID  int64  // the original ripper (recorded for admin/report context)
}

// gofileDedupVariant maps a request's format + force flags to the catalog quality
// tier ("" when the tier isn't cacheable, e.g. binaural — dedup then skips). Reuses
// the exact same effectiveCatalogFormat → VariantKey path the read-through catalog
// uses, so a dedup key and a catalog variant never disagree.
func gofileDedupVariant(format string, forceAAC, forceAtmos bool) string {
	return catalog.VariantKey(catalog.TrackMeta{Format: effectiveCatalogFormat(format, forceAAC, forceAtmos)})
}

// gofileDedupKey builds the global content key. Empty id or variant → "" (dedup
// disabled for this request).
func gofileDedupKey(kind, id, variant string) string {
	if id == "" || variant == "" {
		return ""
	}
	return kind + "|" + id + "|" + variant
}

// artistDedupID folds an artist's scope into its content id so different sections
// of the same artist ("full-albums" vs "singles" vs the whole discography) are
// distinct dedup targets. An empty section is the entire-discography scope.
func artistDedupID(artistID, section string) string {
	if section == "" {
		section = "all"
	}
	return artistID + "/" + section
}

// newGofileDedupInfo assembles the per-rip identity, or nil when the request isn't
// dedup-eligible (un-cacheable tier / missing id).
func newGofileDedupInfo(kind, id, format, label string, forceAAC, forceAtmos bool, userID int64) *gofileDedupInfo {
	variant := gofileDedupVariant(format, forceAAC, forceAtmos)
	key := gofileDedupKey(kind, id, variant)
	if key == "" {
		return nil
	}
	return &gofileDedupInfo{key: key, kind: kind, id: id, variant: variant, label: label, userID: userID}
}

// --- compact report callback encoding ---------------------------------------
// Telegram caps callback_data at 64 BYTES, and a full content key
// ("playlist|pl.<35 chars>|v1|q=lossless") can blow past that. The Report button
// therefore carries a compact, self-describing token — kind (1 char) + tier (1
// char) + "|" + id — which handleReportButton decodes back into the content key
// (so it survives a restart, unlike an in-memory token map would).

var kindToCode = map[string]string{"album": "a", "playlist": "p", "artist": "r"}
var codeToKind = map[string]string{"a": "album", "p": "playlist", "r": "artist"}
var variantToCode = map[string]string{"v1|q=lossless": "l", "v1|q=aac": "c", "v1|q=atmos": "x"}
var codeToVariant = map[string]string{"l": "v1|q=lossless", "c": "v1|q=aac", "x": "v1|q=atmos"}

// dedupReportData builds the callback payload (the part after the "report:" prefix)
// for a given identity, or "" if the kind/tier can't be encoded.
func dedupReportData(kind, id, variant string) string {
	kc, ok1 := kindToCode[kind]
	vc, ok2 := variantToCode[variant]
	if !ok1 || !ok2 || id == "" {
		return ""
	}
	return kc + vc + "|" + id
}

// parseDedupReportData reverses dedupReportData. ok=false on a malformed token.
func parseDedupReportData(data string) (kind, id, variant string, ok bool) {
	if len(data) < 4 || data[2] != '|' {
		return "", "", "", false
	}
	kind, ok1 := codeToKind[data[0:1]]
	variant, ok2 := codeToVariant[data[1:2]]
	id = data[3:]
	if !ok1 || !ok2 || id == "" {
		return "", "", "", false
	}
	return kind, id, variant, true
}

// tierLabel renders a quality tier for the user-facing message.
func tierLabel(variant string) string {
	switch variant {
	case "v1|q=lossless":
		return "Lossless"
	case "v1|q=aac":
		return "AAC"
	case "v1|q=atmos":
		return "Dolby Atmos"
	default:
		return "this format"
	}
}

// checkGofileDedup is the admission gate for a Gofile collection rip. It returns
// true when an existing, non-expired delivery covers this exact (kind, id, tier):
// it sends the user the existing link(s) + a bandwidth warning + a Report button,
// and the caller MUST skip the rip. It returns false (rip normally) on a disabled
// catalog, an un-cacheable tier, a lookup miss, or ANY error — dedup fails open.
//
// Callers gate on a Gofile transfer mode before calling this; Telegram modes have
// their own file_id cache and are not deduped here.
func (b *TelegramBot) checkGofileDedup(chatID int64, replyToID int, kind, id, variant string) bool {
	key := gofileDedupKey(kind, id, variant)
	if key == "" || b.catalog == nil || !b.catalog.Enabled() {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), gofileDedupDBTimeout)
	defer cancel()
	rows, err := b.catalog.GofileDeliveryLookup(ctx, key)
	if err != nil {
		fmt.Printf("gofile dedup lookup %s: %v\n", key, err)
		return false
	}
	if len(rows) == 0 {
		return false
	}

	label := rows[0].Label
	if label == "" {
		label = "this " + kind
	}
	markdown, plain := buildDedupMessage(kind, label, variant, rows)
	var markup any
	if data := dedupReportData(kind, id, variant); data != "" {
		markup = InlineKeyboardMarkup{InlineKeyboard: [][]InlineKeyboardButton{
			{{Text: "🚩 Report a problem", CallbackData: "report:" + data, Style: "danger"}},
		}}
	}
	_, _ = b.sendRichMessage(chatID, markdown, plain, markup, replyToID)
	return true
}

// buildDedupMessage renders the "already ripped recently" notice (rich markdown +
// plain fallback) listing every live link for the content.
func buildDedupMessage(kind, label, variant string, rows []catalog.GofileDelivery) (markdown, plain string) {
	var md, pl strings.Builder
	header := fmt.Sprintf("This %s (%s) was already ripped to Gofile in the last 7 days", kind, tierLabel(variant))

	md.WriteString("## ♻️ Already ripped recently\n\n")
	fmt.Fprintf(&md, "**%s** — %s. Here %s the existing link%s:\n\n",
		escapeRichMD(label), header, plural(len(rows), "is", "are"), plural(len(rows), "", "s"))
	pl.WriteString("♻️ Already ripped recently\n\n")
	fmt.Fprintf(&pl, "%s — %s. Existing link%s:\n\n", label, header, plural(len(rows), "", "s"))

	for _, r := range rows {
		part := strings.TrimSpace(r.Label)
		if part != "" && part != label {
			fmt.Fprintf(&md, "- `%s`\n  🔗 %s\n", escapeRichMD(part), r.Link)
			fmt.Fprintf(&pl, "• %s\n  %s\n", part, r.Link)
		} else {
			fmt.Fprintf(&md, "- 🔗 %s\n", r.Link)
			fmt.Fprintf(&pl, "• %s\n", r.Link)
		}
	}

	const warn = "Our VPS has a capped bandwidth limit, so re-ripping the same thing wastes it and may lead to warnings. Please use the link(s) above."
	const reportNote = "If a link or its files have a problem, tap 🚩 Report below and tell us what's wrong — an admin will review it."
	fmt.Fprintf(&md, "\n> ⚠️ %s\n\n%s", warn, reportNote)
	fmt.Fprintf(&pl, "\n⚠️ %s\n\n%s", warn, reportNote)
	return md.String(), pl.String()
}

// plural is a tiny helper for one-vs-many wording.
func plural(n int, one, many string) string {
	if n == 1 {
		return one
	}
	return many
}

// recordGofileDelivery records one delivered Gofile link under the rip's dedup
// identity (set on its RipState at rip start). No-op when the rip isn't dedup-
// tracked (nil identity), the catalog is disabled, or the link is empty. The write
// is fire-and-forget so it never blocks delivery; a failure just means the next
// identical request rips again (fail-open).
func (b *TelegramBot) recordGofileDelivery(ctx context.Context, link, partLabel string) {
	info := ripStateFrom(ctx).dedupInfo()
	if info == nil || link == "" || b.catalog == nil || !b.catalog.Enabled() {
		return
	}
	label := info.label
	if partLabel != "" {
		label = partLabel
	}
	d := catalog.GofileDelivery{
		ContentKey: info.key,
		Kind:       info.kind,
		ContentID:  info.id,
		Variant:    info.variant,
		Label:      label,
		Link:       link,
		UserID:     info.userID,
	}
	go func() {
		c, cancel := context.WithTimeout(context.Background(), gofileDedupDBTimeout)
		defer cancel()
		if err := b.catalog.GofileDeliveryRecord(c, d, gofileDedupTTL); err != nil {
			fmt.Printf("gofile dedup record %s: %v\n", d.ContentKey, err)
		}
	}()
}

// pruneGofileDeliveries drops expired dedup rows. Called at the daily reset
// alongside quotaPrune; expiry already gates lookups, so this is just housekeeping.
// Best-effort: a disabled catalog or an error is logged and ignored.
func (b *TelegramBot) pruneGofileDeliveries() {
	if b.catalog == nil || !b.catalog.Enabled() {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := b.catalog.GofileDeliveryPruneExpired(ctx); err != nil {
		fmt.Printf("gofile dedup prune: %v\n", err)
	}
}

// =============================================================================
// "Report a problem" flow — a user who tapped 🚩 Report on a dedup notice (because
// a link is dead or the files are wrong) is asked for a reason, which is DM'd to
// every admin for review. Kept here next to the dedup it backs.
// =============================================================================

// reportTTL bounds how long an awaited report reason stays pending after the button
// tap; a reason sent later is treated as a normal message.
const reportTTL = 10 * time.Minute

// reportKey scopes a pending report to one user in one chat (group chats have many
// users, so chatID alone isn't enough).
type reportKey struct {
	chatID int64
	userID int64
}

// pendingReport is the context captured when the Report button is tapped, awaiting
// the user's free-text reason.
type pendingReport struct {
	contentKey string
	label      string
	links      []string
	createdAt  time.Time
}

// handleReportButton acks a 🚩 Report tap: it snapshots the reported delivery's
// links/label, records a pending report for this (chat,user), and prompts for a
// reason. Returns a toast string for the callback ack.
func (b *TelegramBot) handleReportButton(cb *CallbackQuery, data string, userID int64, username string) string {
	if cb == nil || cb.Message == nil {
		return ""
	}
	kind, id, variant, ok := parseDedupReportData(data)
	if !ok {
		return ""
	}
	contentKey := gofileDedupKey(kind, id, variant)
	chatID := cb.Message.Chat.ID

	var label string
	var links []string
	if b.catalog != nil && b.catalog.Enabled() {
		ctx, cancel := context.WithTimeout(context.Background(), gofileDedupDBTimeout)
		defer cancel()
		if rows, err := b.catalog.GofileDeliveryLookup(ctx, contentKey); err == nil {
			for _, r := range rows {
				links = append(links, r.Link)
				if label == "" {
					label = r.Label
				}
			}
		}
	}

	b.pendingReportsMu.Lock()
	b.pendingReports[reportKey{chatID: chatID, userID: userID}] = &pendingReport{
		contentKey: contentKey,
		label:      label,
		links:      links,
		createdAt:  time.Now(),
	}
	b.pendingReportsMu.Unlock()

	_ = b.sendMessageWithReply(chatID,
		"🚩 Reply with a short reason for the report (e.g. \"link is dead\", \"wrong tracks\", \"corrupt zip\"). An admin will review it.",
		nil, cb.Message.MessageID)
	return "Tell us what's wrong"
}

// consumeReportReason checks whether text is the awaited reason for a pending report
// from (chatID, userID). If so it DMs every admin the report and acks the user,
// returning true (the caller must stop processing the message). A stale pending
// report (older than reportTTL) is dropped and treated as "no pending report".
func (b *TelegramBot) consumeReportReason(chatID, userID int64, username, text string, replyToID int) bool {
	if userID == 0 || text == "" {
		return false
	}
	k := reportKey{chatID: chatID, userID: userID}
	b.pendingReportsMu.Lock()
	pr := b.pendingReports[k]
	if pr != nil && time.Since(pr.createdAt) > reportTTL {
		delete(b.pendingReports, k)
		pr = nil
	}
	if pr != nil {
		delete(b.pendingReports, k)
	}
	b.pendingReportsMu.Unlock()
	if pr == nil {
		return false
	}

	who := "@" + username
	if username == "" {
		who = fmt.Sprintf("user %d", userID)
	}
	content := pr.label
	if content == "" {
		content = pr.contentKey
	}

	var sb strings.Builder
	sb.WriteString("🚩 Re-rip report\n\n")
	fmt.Fprintf(&sb, "From: %s (id %d)\n", who, userID)
	fmt.Fprintf(&sb, "Content: %s\n", content)
	fmt.Fprintf(&sb, "Key: %s\n", pr.contentKey)
	if len(pr.links) > 0 {
		sb.WriteString("Links:\n")
		for _, l := range pr.links {
			fmt.Fprintf(&sb, "  %s\n", l)
		}
	}
	fmt.Fprintf(&sb, "\nReason: %s", text)

	notified := 0
	for adminID := range b.admins {
		if err := b.sendMessageWithReply(adminID, sb.String(), nil, 0); err == nil {
			notified++
		}
	}
	if notified == 0 {
		_ = b.sendMessageWithReply(chatID, "⚠️ Couldn't reach an admin right now — please try again later.", nil, replyToID)
		return true
	}
	_ = b.sendMessageWithReply(chatID, "✅ Thanks — your report was sent to the admins for review.", nil, replyToID)
	return true
}
