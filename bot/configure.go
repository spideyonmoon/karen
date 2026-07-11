package main

import (
	"fmt"
	"strings"
	"time"
)

// =============================================================================
// /configure — sudo-only live bot configuration (Bot API 10.1 Rich UI)
//
// A button-driven admin panel modelled on /profile (profile.go): one Rich Message
// edited in place, a menu router (root card → section sub-panels), callbacks
// namespaced under "cfg:" and guarded to the admin who opened the panel.
//
// This file holds:
//   - the per-chat configSession (ownership + TTL + text-input step state),
//   - the reusable "wait for the next text message" primitive (Phase 1), and
//   - the Phase 0 scaffolding: /configure command, root menu, section stubs.
//
// Class A (hot-apply) settings and Class B (accounts / deploy) flows land in later
// phases; the sub-panels here are deliberate stubs so the shell can ship and be
// exercised first. See docs/CONFIGURE_FEATURE.md for the full plan.
// =============================================================================

const (
	// configSessionTTL bounds an idle panel; taps past it are rejected and the
	// session is swept. Mirrors profileOwners' implicit lifetime but explicit.
	configSessionTTL = 15 * time.Minute
	// configInputTTL is how long a text-input step blocks for the next message
	// before it gives up and reverts the panel to view mode (NEO uses 60s).
	configInputTTL = 60 * time.Second
)

// configSession is the live state behind one /configure panel, keyed
// "chatID:messageID" in b.configSessions. Only Owner may tap its buttons or
// satisfy its input steps.
type configSession struct {
	Owner     int64
	ChatID    int64
	MessageID int
	Panel     string // which sub-panel is currently shown ("root" | section key)
	CreatedAt time.Time
}

// inputWaiter is one outstanding "next text message" capture (Phase 1). The
// handler that armed it blocks on Ch; deliverPendingInput feeds it or it times
// out. Keyed by chatID in b.pendingInputs; UserID pins it to the owner so a
// waiter can't swallow another user's message in a group.
type inputWaiter struct {
	Ch        chan string
	UserID    int64
	ExpiresAt time.Time
}

func configKey(chatID int64, messageID int) string {
	return fmt.Sprintf("%d:%d", chatID, messageID)
}

// =============================================================================
// Phase 1 — text-input-wait primitive
// =============================================================================

// awaitInput arms a one-shot waiter for the next text message from userID in
// chatID and blocks (up to configInputTTL) for it. Returns the text and true on
// receipt, or "" and false on timeout / superseded waiter. Callers run this off
// the update loop (in a goroutine) so the bot keeps serving other chats.
//
// Only one waiter per chat exists at a time: arming a new one replaces (and
// abandons) any previous waiter for that chat.
func (b *TelegramBot) awaitInput(chatID, userID int64) (string, bool) {
	w := &inputWaiter{
		Ch:        make(chan string, 1),
		UserID:    userID,
		ExpiresAt: time.Now().Add(configInputTTL),
	}
	b.pendingInputs.Store(chatID, w)

	select {
	case text := <-w.Ch:
		return text, true
	case <-time.After(configInputTTL):
		// Only clear if we're still the current waiter — a newer arm may have
		// replaced us, and we must not delete its entry.
		b.pendingInputs.CompareAndDelete(chatID, w)
		return "", false
	}
}

// deliverPendingInput is called from handleMessage BEFORE command routing. If a
// waiter is armed for this chat and the sender owns it, it hands the text to the
// waiter, deletes the user's message (keeping the menu clean), and returns true
// to stop normal command handling. Returns false when there's nothing to capture.
func (b *TelegramBot) deliverPendingInput(chatID, userID int64, text string, messageID int) bool {
	v, ok := b.pendingInputs.Load(chatID)
	if !ok {
		return false
	}
	w, _ := v.(*inputWaiter)
	if w == nil {
		b.pendingInputs.Delete(chatID)
		return false
	}
	// Expired but not yet swept: drop it and let this message route normally.
	if time.Now().After(w.ExpiresAt) {
		b.pendingInputs.CompareAndDelete(chatID, w)
		return false
	}
	// A pending waiter must not swallow another user's message in a group.
	if w.UserID != 0 && userID != w.UserID {
		return false
	}
	// Consume the waiter and deliver. Only proceed if we're still the current
	// waiter — a newer arm may have replaced w, in which case this message is
	// meant for the new waiter (or is a normal command) and must route on.
	if !b.pendingInputs.CompareAndDelete(chatID, w) {
		return false
	}
	// Ch is buffered(1) so this never blocks even if the arming goroutine has
	// already timed out.
	w.Ch <- text
	// Keep the panel clean: remove the raw value the user typed. Best-effort —
	// in a group the bot may lack delete rights; ignore the error.
	_ = b.deleteMessage(chatID, messageID)
	return true
}

// =============================================================================
// Phase 0 — command entry, session lifecycle, callback router
// =============================================================================

// handleArtistRipsCommand is the quick command-line alternative to the Settings
// toggle: `/artistrips` shows state, `/artistrips on|off` sets it. Admin-gated by
// the caller.
func (b *TelegramBot) handleArtistRipsCommand(chatID int64, args []string, replyToID int) {
	if len(args) == 0 {
		state := "✅ enabled"
		if b.artistRipsDisabled() {
			state = "🚫 disabled"
		}
		_ = b.sendMessageWithReply(chatID, "Artist (full-discography) rips are "+state+".\nUse /artistrips on or /artistrips off to change.", nil, replyToID)
		return
	}
	switch strings.ToLower(strings.TrimSpace(args[0])) {
	case "on", "enable", "enabled":
		b.setArtistRipsDisabled(false)
		_ = b.sendMessageWithReply(chatID, "✅ Artist rips enabled.", nil, replyToID)
	case "off", "disable", "disabled":
		b.setArtistRipsDisabled(true)
		_ = b.sendMessageWithReply(chatID, "🚫 Artist rips disabled. Albums, songs and playlists still work.", nil, replyToID)
	default:
		_ = b.sendMessageWithReply(chatID, "Usage: /artistrips on|off", nil, replyToID)
	}
}

// admin so only they can operate its buttons. Admin gate is enforced by the
// caller (handleCommand). Panels are DM-first but work in any allowed chat.
func (b *TelegramBot) handleConfigureCommand(chatID, userID int64, replyToID int) {
	rich, plain := b.renderConfig("root")
	markup := b.configMarkup("root")
	res, err := b.sendRichMessage(chatID, rich, plain, markup, replyToID)
	if err != nil || res.messageID == 0 {
		return
	}
	b.configMu.Lock()
	if b.configSessions == nil {
		b.configSessions = make(map[string]*configSession)
	}
	b.configSessions[configKey(chatID, res.messageID)] = &configSession{
		Owner:     userID,
		ChatID:    chatID,
		MessageID: res.messageID,
		Panel:     "root",
		CreatedAt: time.Now(),
	}
	b.configMu.Unlock()
}

// getConfigSession returns the live session for a panel and whether it's valid
// (known and not expired). Expired sessions are swept here.
func (b *TelegramBot) getConfigSession(chatID int64, messageID int) (*configSession, bool) {
	key := configKey(chatID, messageID)
	b.configMu.Lock()
	defer b.configMu.Unlock()
	s, ok := b.configSessions[key]
	if !ok {
		return nil, false
	}
	if time.Since(s.CreatedAt) > configSessionTTL {
		delete(b.configSessions, key)
		return nil, false
	}
	return s, true
}

func (b *TelegramBot) endConfigSession(chatID int64, messageID int) {
	b.configMu.Lock()
	delete(b.configSessions, configKey(chatID, messageID))
	b.configMu.Unlock()
}

// handleConfigCallback routes a "cfg:*" callback: enforces admin + ownership,
// navigates panels, and re-renders the same message in place. Returns a non-empty
// alert string when the tap is rejected (shown as a toast).
func (b *TelegramBot) handleConfigCallback(cb *CallbackQuery, data string, clickerID int64) string {
	chatID := cb.Message.Chat.ID
	messageID := cb.Message.MessageID

	if !b.isAdmin(clickerID) {
		return "Admins only."
	}
	s, ok := b.getConfigSession(chatID, messageID)
	if !ok {
		return "This panel has expired. Send /configure again."
	}
	if s.Owner != 0 && clickerID != s.Owner {
		return "This isn't your configure panel."
	}

	// data is "cfg:<action>[:<rest>]".
	rest := strings.TrimPrefix(data, "cfg:")
	parts := strings.SplitN(rest, ":", 3)
	action := parts[0]

	panel := s.Panel
	switch action {
	case "nop":
		return ""
	case "nav":
		if len(parts) >= 2 {
			panel = parts[1]
		}
	case "toggle":
		if len(parts) >= 2 {
			b.applyConfigToggle(parts[1])
		}
		panel = "settings"
	case "done":
		b.endConfigSession(chatID, messageID)
		rich, plain := b.renderConfig("done")
		_, _ = b.editMessageRich(chatID, messageID, rich, plain, nil)
		return ""
	default:
		// Unknown action within an active session — re-render current panel.
	}

	s.Panel = panel
	rich, plain := b.renderConfig(panel)
	markup := b.configMarkup(panel)
	_, _ = b.editMessageRich(chatID, messageID, rich, plain, markup)
	return ""
}

// applyConfigToggle flips a boolean Class-A setting by key. This is the hot-apply
// side-effect table (NEO §5) — new toggles register here. All are applied live in
// the running process and persisted; none need a restart.
func (b *TelegramBot) applyConfigToggle(field string) {
	switch field {
	case "artist_rips":
		b.setArtistRipsDisabled(!b.artistRipsDisabled())
	}
}

// =============================================================================
// Rendering
// =============================================================================

// renderConfig builds the rich Markdown and plain-text fallback for a panel.
// Sub-panels are Phase-0 stubs: they name what's coming so the shell is
// navigable before the real controls land.
func (b *TelegramBot) renderConfig(panel string) (rich, plain string) {
	var rb, pb strings.Builder
	switch panel {
	case "done":
		rb.WriteString("# " + symDone + " Configure closed\n")
		pb.WriteString("Configure closed.\n")
		return rb.String(), pb.String()
	case "settings":
		artistState := "✅ Enabled"
		if b.artistRipsDisabled() {
			artistState = "🚫 Disabled"
		}
		rb.WriteString("# 🎚 Settings\n")
		rb.WriteString("Live-tune the hot-applyable limits (no restart).\n\n")
		rb.WriteString("**Artist rips**: " + artistState + "\n")
		rb.WriteString("_Disabling blocks whole-discography artist rips bot-wide; albums, songs and playlists are unaffected. Admins can still run artist rips to test._\n\n")
		rb.WriteString("_More coming soon: delivery override · quota limits · size thresholds._\n")
		pb.WriteString("🎚 Settings\n")
		pb.WriteString("Artist rips: " + artistState + "\n")
		pb.WriteString("Disabling blocks whole-discography artist rips bot-wide; albums, songs and playlists are unaffected.\n")
		pb.WriteString("\nMore coming soon: delivery override · quota limits · size thresholds.\n")
	case "accounts":
		writeConfigStub(&rb, &pb, "🍏 Accounts",
			"Add, change, disable or remove Apple-Music accounts.",
			[]string{
				"Each account = one wrapper container (needs a deploy to apply)",
				"[new] asks for ID then password",
				"[change] · [delete] · [shut-down] per account",
			})
	case "deploy":
		writeConfigStub(&rb, &pb, "🚀 Deploy",
			"Apply structural changes (accounts, DB) by recreating containers.",
			[]string{
				"⚠️ Recreating containers kills in-flight rips",
				"Refuses while the bot is busy — confirm to force",
			})
	default: // root
		rb.WriteString("# ⚙︎ Configure\n")
		rb.WriteString("Live bot configuration. Tap a section.\n")
		rb.WriteString("\n")
		rb.WriteString("🎚 **Settings** — hot-apply limits & delivery\n")
		rb.WriteString("🍏 **Accounts** — Apple-Music credentials\n")
		rb.WriteString("🚀 **Deploy** — apply structural changes\n")
		pb.WriteString("⚙︎ Configure\n")
		pb.WriteString("Live bot configuration. Tap a section.\n")
		return rb.String(), pb.String()
	}
	return rb.String(), pb.String()
}

// writeConfigStub renders a placeholder sub-panel: a title, one lead line, and a
// bullet list of what the finished panel will do.
func writeConfigStub(rb, pb *strings.Builder, title, lead string, bullets []string) {
	rb.WriteString("# " + escapeRichMD(title) + "\n")
	rb.WriteString(escapeRichMD(lead) + "\n\n")
	pb.WriteString(title + "\n")
	pb.WriteString(lead + "\n\n")
	for _, line := range bullets {
		rb.WriteString("• " + escapeRichMD(line) + "\n")
		pb.WriteString("• " + line + "\n")
	}
	rb.WriteString("\n_Coming soon._\n")
	pb.WriteString("\nComing soon.\n")
}

// =============================================================================
// Keyboards
// =============================================================================

func (b *TelegramBot) configMarkup(panel string) InlineKeyboardMarkup {
	switch panel {
	case "settings":
		toggleText := "🚫 Disable artist rips"
		toggleStyle := "danger"
		if b.artistRipsDisabled() {
			toggleText = "✅ Enable artist rips"
			toggleStyle = "success"
		}
		return InlineKeyboardMarkup{InlineKeyboard: [][]InlineKeyboardButton{
			{{Text: toggleText, CallbackData: "cfg:toggle:artist_rips", Style: toggleStyle}},
			{{Text: "‹ Back", CallbackData: "cfg:nav:root"}},
		}}
	case "accounts", "deploy":
		return InlineKeyboardMarkup{InlineKeyboard: [][]InlineKeyboardButton{
			{{Text: "‹ Back", CallbackData: "cfg:nav:root"}},
		}}
	default: // root
		return InlineKeyboardMarkup{InlineKeyboard: [][]InlineKeyboardButton{
			{{Text: "🎚 Settings", CallbackData: "cfg:nav:settings", Style: "primary"}},
			{{Text: "🍏 Accounts", CallbackData: "cfg:nav:accounts", Style: "primary"}},
			{{Text: "🚀 Deploy", CallbackData: "cfg:nav:deploy", Style: "danger"}},
			{{Text: "✓ Done", CallbackData: "cfg:done", Style: "success"}},
		}}
	}
}
