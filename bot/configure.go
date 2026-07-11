package main

import (
	"fmt"
	"strconv"
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
	case "set":
		// cfg:set:<field>:<value> — immediate choice set (e.g. artist delivery).
		if len(parts) >= 3 {
			b.applyConfigSet(parts[1], parts[2])
		}
		panel = "settings"
	case "edit":
		// cfg:edit:<field> — start a text-input-wait to enter a numeric value. The
		// panel stays; a prompt is sent and the value applied on reply (Phase 1).
		if len(parts) >= 2 {
			go b.beginValueEdit(chatID, clickerID, messageID, s.Panel, parts[1])
		}
		return "" // don't re-render yet; the edit goroutine re-renders on completion
	case "reset":
		// cfg:reset:<field> — clear an override so it falls back to config/default.
		if len(parts) >= 2 {
			b.applyConfigReset(parts[1])
		}
		panel = s.Panel
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

// applyConfigSet handles a choice-value set (cfg:set:<field>:<value>). Selecting the
// built-in default clears the override so the key falls back to config/default.
func (b *TelegramBot) applyConfigSet(field, value string) {
	switch field {
	case "artist_delivery":
		if value == artistDeliveryTelegram {
			setSettingString(seArtistDelivery, artistDeliveryTelegram)
		} else {
			// "gofile" is the built-in behavior — clearing the override falls back to it.
			clearSetting(seArtistDelivery)
		}
	}
}

// applyConfigReset clears a numeric override so it returns to config/default.
func (b *TelegramBot) applyConfigReset(token string) {
	if f, ok := cfgNumFieldByToken(token); ok {
		clearSetting(f.key)
	}
}

// cfgNumField describes one editable integer setting in the Limits sub-panel. baseline
// is the effective value when NOT overridden — the current boot config or hardcoded
// default — so an unedited field displays exactly what the bot uses today.
type cfgNumField struct {
	token    string // cfg:edit:<token> / cfg:reset:<token>
	key      string // overlay key
	label    string
	unit     string
	min, max int
	baseline func() int
}

func cfgNumFields() []cfgNumField {
	return []cfgNumField{
		{"tg_max_gb", seTelegramMaxGB, "Telegram max size", "GB", 1, 4000, func() int {
			if Config.TelegramDownloadMaxGB > 0 {
				return Config.TelegramDownloadMaxGB
			}
			return defaultTelegramDownloadMaxGB
		}},
		{"flush_gb", seRipFlushGB, "Mid-rip flush", "GB", -1, 4000, func() int {
			if Config.RipFlushThresholdGB != 0 {
				return Config.RipFlushThresholdGB
			}
			return defaultRipFlushThresholdGB
		}},
		{"hl_artist", seHeavyLimitArtist, "Artist / day (regular)", "", 0, 100, func() int { return heavyRipLimitDefault("artist", false) }},
		{"hl_artist_d", seHeavyLimitArtistDon, "Artist / day (donor)", "", 0, 100, func() int { return heavyRipLimitDefault("artist", true) }},
		{"hl_playlist", seHeavyLimitPlaylist, "Huge-playlist / day (regular)", "", 0, 100, func() int { return heavyRipLimitDefault("playlist", false) }},
		{"hl_playlist_d", seHeavyLimitPlDonor, "Huge-playlist / day (donor)", "", 0, 100, func() int { return heavyRipLimitDefault("playlist", true) }},
	}
}

func cfgNumFieldByToken(token string) (cfgNumField, bool) {
	for _, f := range cfgNumFields() {
		if f.token == token {
			return f, true
		}
	}
	return cfgNumField{}, false
}

// effective returns the field's current value and whether it's an explicit override.
func (f cfgNumField) effective() (val int, overridden bool) {
	if raw, ok := settingsStore.getRaw(f.key); ok {
		if v, err := strconv.Atoi(raw); err == nil {
			return v, true
		}
	}
	return f.baseline(), false
}

// display formats the value for the panel ("off"/"disabled" for the -1 flush sentinel).
func (f cfgNumField) display(val int) string {
	if f.token == "flush_gb" && val < 0 {
		return "off (no mid-rip flush)"
	}
	if f.unit != "" {
		return strconv.Itoa(val) + " " + f.unit
	}
	return strconv.Itoa(val)
}

// beginValueEdit runs a text-input-wait (Phase 1) to set a numeric field, then
// re-renders the panel in place. Runs in its own goroutine off the update loop.
func (b *TelegramBot) beginValueEdit(chatID, userID int64, messageID int, panel, token string) {
	f, ok := cfgNumFieldByToken(token)
	if !ok {
		return
	}
	cur, _ := f.effective()
	prompt := fmt.Sprintf("Send a new value for %q (%d–%d%s).\nCurrent: %s. Send /cancel to keep it.",
		f.label, f.min, f.max, unitSuffix(f.unit), f.display(cur))
	promptID, _ := b.sendMessageWithReplyReturn(chatID, prompt, nil, 0)

	text, got := b.awaitInput(chatID, userID)
	if promptID != 0 {
		_ = b.deleteMessage(chatID, promptID)
	}
	if !got {
		b.rerenderConfig(chatID, messageID, panel)
		return
	}
	text = strings.TrimSpace(text)
	if text == "" || strings.EqualFold(text, "/cancel") {
		b.rerenderConfig(chatID, messageID, panel)
		return
	}
	n, err := strconv.Atoi(text)
	if err != nil || n < f.min || n > f.max {
		_ = b.sendMessage(chatID, fmt.Sprintf("Ignored — expected a whole number %d–%d. %s left unchanged.", f.min, f.max, f.label), nil)
		b.rerenderConfig(chatID, messageID, panel)
		return
	}
	setSettingInt(f.key, n)
	b.rerenderConfig(chatID, messageID, panel)
}

func unitSuffix(unit string) string {
	if unit == "" {
		return ""
	}
	return " " + unit
}

// rerenderConfig repaints a panel in place if its session is still live.
func (b *TelegramBot) rerenderConfig(chatID int64, messageID int, panel string) {
	if _, ok := b.getConfigSession(chatID, messageID); !ok {
		return
	}
	rich, plain := b.renderConfig(panel)
	markup := b.configMarkup(panel)
	_, _ = b.editMessageRich(chatID, messageID, rich, plain, markup)
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
		delivery := "Default (Gofile ZIP)"
		if artistDeliveryOverride() == artistDeliveryTelegram {
			delivery = "Telegram (individual)"
		}
		rb.WriteString("# 🎚 Settings\n")
		rb.WriteString("Live-tune the hot-applyable settings. Changes apply immediately, persist across restarts, and only ever override the values you touch.\n\n")
		rb.WriteString("**Artist rips**: " + artistState + "\n")
		rb.WriteString("**Artist delivery**: " + escapeRichMD(delivery) + "\n")
		rb.WriteString("**Limits & sizes**: tap below to view/edit\n")
		pb.WriteString("🎚 Settings\n")
		pb.WriteString("Artist rips: " + artistState + "\n")
		pb.WriteString("Artist delivery: " + delivery + "\n")
	case "limits":
		rb.WriteString("# 📏 Limits & sizes\n")
		rb.WriteString("Tap a value to change it; ↺ resets it to the current config default. An unedited value shows exactly what the bot uses now.\n\n")
		pb.WriteString("📏 Limits & sizes\n\n")
		rb.WriteString("| Setting | Value | Source |\n|:--------|:------|:------|\n")
		for _, f := range cfgNumFields() {
			val, over := f.effective()
			src := "default"
			if over {
				src = "overridden"
			}
			fmt.Fprintf(&rb, "| %s | %s | %s |\n", escapeRichMD(f.label), escapeRichMD(f.display(val)), src)
			marker := ""
			if over {
				marker = " *"
			}
			fmt.Fprintf(&pb, "%s: %s%s\n", f.label, f.display(val), marker)
		}
		rb.WriteString("\n_* = overridden. Others follow your .env / built-in defaults._\n")
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
		// Artist-delivery choice: Gofile (default/cleared) vs Telegram individual.
		tg := artistDeliveryOverride() == artistDeliveryTelegram
		gofileBtn := InlineKeyboardButton{Text: "Gofile ZIP", CallbackData: "cfg:set:artist_delivery:" + artistDeliveryGofile}
		tgBtn := InlineKeyboardButton{Text: "TG individual", CallbackData: "cfg:set:artist_delivery:" + artistDeliveryTelegram}
		if tg {
			tgBtn.Text = symDone + " " + tgBtn.Text
			tgBtn.Style = "success"
		} else {
			gofileBtn.Text = symDone + " " + gofileBtn.Text
			gofileBtn.Style = "success"
		}
		return InlineKeyboardMarkup{InlineKeyboard: [][]InlineKeyboardButton{
			{{Text: toggleText, CallbackData: "cfg:toggle:artist_rips", Style: toggleStyle}},
			{{Text: "── Artist delivery ──", CallbackData: "cfg:nop", Style: "primary"}},
			{gofileBtn, tgBtn},
			{{Text: "📏 Limits & sizes", CallbackData: "cfg:nav:limits", Style: "primary"}},
			{{Text: "‹ Back", CallbackData: "cfg:nav:root"}},
		}}
	case "limits":
		var rows [][]InlineKeyboardButton
		for _, f := range cfgNumFields() {
			_, over := f.effective()
			editBtn := InlineKeyboardButton{Text: "✎ " + f.label, CallbackData: "cfg:edit:" + f.token}
			row := []InlineKeyboardButton{editBtn}
			if over {
				row = append(row, InlineKeyboardButton{Text: "↺", CallbackData: "cfg:reset:" + f.token, Style: "danger"})
			}
			rows = append(rows, row)
		}
		rows = append(rows, []InlineKeyboardButton{{Text: "‹ Back", CallbackData: "cfg:nav:settings"}})
		return InlineKeyboardMarkup{InlineKeyboard: rows}
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
