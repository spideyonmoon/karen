package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// =============================================================================
// /profile — fully button-driven saved rip preferences (Bot API 10.1 Rich UI)
//
// Everything is buttons: no typed input anywhere. The panel is one Rich Message
// edited in place. A menu router (root card → Audio / Lyrics / Artwork / Delivery
// sub-panels) keeps each keyboard within Telegram's row/width caps. Callbacks are
// namespaced under "pf:" and guarded to the user who opened the panel.
//
// Built entirely on the existing rich helpers (sendRichMessage / editMessageRich /
// escapeRichMD) so it degrades to plain text + identical keyboards on a pre-10.1
// API server.
// =============================================================================

// pfChoice is one selectable value in a choice row; an empty value means
// "Default" (inherit the global config).
type pfChoice struct {
	value string
	label string
}

var (
	pfCodecChoices = []pfChoice{
		{"", "Default"}, {"alac", "ALAC"}, {"flac", "FLAC"}, {"aac", "AAC"}, {"atmos", "Atmos"},
	}
	pfQualityChoices = []pfChoice{
		{"", "Default"}, {"redbook", "Red Book 16/44.1"}, {"hires", "Hi-Res 24/192"},
	}
	pfAacChoices = []pfChoice{
		{"", "Default"}, {"aac-lc", "AAC-LC 256"}, {"aac", "AAC"}, {"aac-binaural", "Binaural"}, {"aac-downmix", "Downmix"},
	}
	pfLyricChoices = []pfChoice{
		{"", "Default"}, {"off", "Off"}, {"static", "Static"}, {"synced", "Line-synced"}, {"word", "Word-by-word"},
	}
	pfCoverDelivChoices = []pfChoice{
		{"", "Default"}, {"photo", "Photo"}, {"document", "Document"},
	}
	pfTargetChoices = []pfChoice{
		{"", "Default"}, {"ask", "Ask"}, {"telegram", "Telegram"}, {"telegram_zip", "Telegram ZIP"}, {"gofile", "Gofile"},
	}
	pfLanguageChoices = []pfChoice{
		{"", "Default"}, {"en", "English"}, {"ja", "日本語"}, {"ko", "한국어"}, {"zh-Hans", "简体"}, {"zh-Hant", "繁體"},
	}
	pfMVChoices = []pfChoice{
		{"0", "Default"}, {"360", "360p"}, {"480", "480p"}, {"720", "720p"}, {"1080", "1080p"}, {"2160", "2160p"},
	}
	pfArtistZipChoices = []pfChoice{
		{"", "Default"}, {"per_release", "Per release"}, {"combined", "Combined"},
	}
)

// profileKey is the ownership map key: a Rich Message panel is owned by the user
// who opened it, identified by chat+message so two users in one group can each
// hold their own panel.
func profileKey(chatID int64, messageID int) string {
	return fmt.Sprintf("%d:%d", chatID, messageID)
}

// handleProfileCommand renders the root profile card and records its owner so only
// they can operate its buttons.
func (b *TelegramBot) handleProfileCommand(chatID int64, userID int64, replyToID int) {
	prefs := b.getPrefs(userID)
	rich, plain := b.renderProfile("root", prefs, b.getStats(userID), b.isUserDonor(userID, ""))
	markup := b.profileMarkup("root", prefs)
	res, err := b.sendRichMessage(chatID, rich, plain, markup, replyToID)
	if err != nil || res.messageID == 0 {
		return
	}
	b.profileMu.Lock()
	if b.profileOwners == nil {
		b.profileOwners = make(map[string]int64)
	}
	b.profileOwners[profileKey(chatID, res.messageID)] = userID
	b.profileMu.Unlock()
}

// handleProfileCallback routes a "pf:*" callback: enforces ownership, mutates the
// saved profile, and re-renders the same message in place. Returns a non-empty
// alert string when the tap is rejected (shown as a toast).
func (b *TelegramBot) handleProfileCallback(cb *CallbackQuery, data string, clickerID int64) string {
	chatID := cb.Message.Chat.ID
	messageID := cb.Message.MessageID

	b.profileMu.Lock()
	owner, known := b.profileOwners[profileKey(chatID, messageID)]
	b.profileMu.Unlock()
	if known && owner != 0 && clickerID != owner {
		return "This isn't your profile panel."
	}

	// data is "pf:<action>[:<rest>]".
	rest := strings.TrimPrefix(data, "pf:")
	parts := strings.SplitN(rest, ":", 3)
	action := parts[0]

	panel := "root"
	switch action {
	case "nop":
		return ""
	case "nav":
		if len(parts) >= 2 {
			panel = parts[1]
		}
	case "set":
		if len(parts) >= 3 {
			field, value := parts[1], parts[2]
			b.setPrefs(clickerID, func(p *UserPrefs) { applyProfileSet(p, field, value) })
		}
		panel = panelForField(parts)
	case "toggle":
		if len(parts) >= 2 {
			field := parts[1]
			b.setPrefs(clickerID, func(p *UserPrefs) { applyProfileToggle(p, field) })
			panel = panelForToggle(field)
		}
	case "reset":
		b.resetPrefs(clickerID)
		panel = "root"
	case "done":
		b.profileMu.Lock()
		delete(b.profileOwners, profileKey(chatID, messageID))
		b.profileMu.Unlock()
		prefs := b.getPrefs(clickerID)
		rich, plain := b.renderProfile("done", prefs, b.getStats(clickerID), b.isUserDonor(clickerID, ""))
		_, _ = b.editMessageRich(chatID, messageID, rich, plain, nil)
		return ""
	}

	prefs := b.getPrefs(clickerID)
	rich, plain := b.renderProfile(panel, prefs, b.getStats(clickerID), b.isUserDonor(clickerID, ""))
	markup := b.profileMarkup(panel, prefs)
	_, _ = b.editMessageRich(chatID, messageID, rich, plain, markup)
	return ""
}

// panelForField / panelForToggle map a just-edited field back to the sub-panel to
// re-render so the user stays where they were.
func panelForField(parts []string) string {
	if len(parts) < 2 {
		return "root"
	}
	switch parts[1] {
	case "codec", "quality", "aac_type":
		return "audio"
	case "lyric_mode":
		return "lyrics"
	case "cover_delivery":
		return "artwork"
	case "delivery_target", "language", "mv_max", "artist_zip":
		return "delivery"
	}
	return "root"
}

func panelForToggle(field string) string {
	switch field {
	case "embed_lrc":
		return "lyrics"
	case "embed_cover", "animated_art":
		return "artwork"
	case "apple_master":
		return "audio"
	}
	return "root"
}

// applyProfileSet writes a choice value onto the profile. Tapping the already-active
// value clears it back to "Default" (inherit) — the tap-to-unset affordance.
func applyProfileSet(p *UserPrefs, field, value string) {
	switch field {
	case "codec":
		p.Codec = toggleSame(p.Codec, value)
	case "quality":
		p.Quality = toggleSame(p.Quality, value)
	case "aac_type":
		p.AacType = toggleSame(p.AacType, value)
	case "lyric_mode":
		p.LyricMode = toggleSame(p.LyricMode, value)
	case "cover_delivery":
		p.CoverDelivery = toggleSame(p.CoverDelivery, value)
	case "delivery_target":
		p.DeliveryTarget = toggleSame(p.DeliveryTarget, value)
	case "language":
		p.Language = toggleSame(p.Language, value)
	case "mv_max":
		n, _ := strconv.Atoi(value)
		if p.MVMax == n {
			p.MVMax = 0
		} else {
			p.MVMax = n
		}
	case "artist_zip":
		p.ArtistZip = toggleSame(p.ArtistZip, value)
	}
}

// applyProfileToggle flips a tri-state boolean: nil (inherit) → true → false → true.
func applyProfileToggle(p *UserPrefs, field string) {
	switch field {
	case "embed_lrc":
		p.EmbedLrc = togglePtr(p.EmbedLrc)
	case "embed_cover":
		p.EmbedCover = togglePtr(p.EmbedCover)
	case "animated_art":
		p.AnimatedArt = togglePtr(p.AnimatedArt)
	case "apple_master":
		p.AppleMaster = togglePtr(p.AppleMaster)
	}
}

// toggleSame returns "" when value already equals current (tap-to-unset), else value.
func toggleSame(current, value string) string {
	if current == value {
		return ""
	}
	return value
}

func togglePtr(b *bool) *bool {
	v := true
	if b != nil {
		v = !*b
	}
	return &v
}

// =============================================================================
// Rendering
// =============================================================================

// renderProfile builds the rich Markdown and plain-text fallback for a panel. The
// root card also carries the user's lifetime usage box (stats); sub-panels ignore it.
func (b *TelegramBot) renderProfile(panel string, p UserPrefs, stats UserStats, donor bool) (rich, plain string) {
	var rb, pb strings.Builder
	switch panel {
	case "done":
		rb.WriteString("# " + symDone + " Profile saved\n")
		rb.WriteString(profileCompactRich(p))
		pb.WriteString("Profile saved.\n")
		pb.WriteString(profileCompactPlain(p))
		return rb.String(), pb.String()
	case "audio":
		writeProfilePanelHeader(&rb, &pb, "🎵 Audio", audioSummaryRows(p))
		return rb.String(), pb.String()
	case "lyrics":
		writeProfilePanelHeader(&rb, &pb, "🎤 Lyrics", lyricsSummaryRows(p))
		return rb.String(), pb.String()
	case "artwork":
		writeProfilePanelHeader(&rb, &pb, "🖼 Artwork", artworkSummaryRows(p))
		return rb.String(), pb.String()
	case "delivery":
		writeProfilePanelHeader(&rb, &pb, "📤 Delivery", deliverySummaryRows(p))
		return rb.String(), pb.String()
	default: // root
		rb.WriteString("# ⚙︎ Your rip profile\n")
		rb.WriteString("Tap a category to change it. Unset values follow the bot defaults.\n")
		if donor {
			rb.WriteString("\n⭐ **Donor** — thanks for supporting!\n")
		}
		rb.WriteString(profileCompactRich(p))
		rb.WriteString(usageBoxRich(stats))
		pb.WriteString("⚙︎ Your rip profile\n")
		pb.WriteString("Tap a category to change it. Unset values follow the bot defaults.\n")
		if donor {
			pb.WriteString("\n⭐ Donor — thanks for supporting!\n")
		}
		pb.WriteString(profileCompactPlain(p))
		pb.WriteString(usageBoxPlain(stats))
		return rb.String(), pb.String()
	}
}

func writeProfilePanelHeader(rb, pb *strings.Builder, title string, rows [][2]string) {
	rb.WriteString("# " + escapeRichMD(title) + "\n")
	rb.WriteString("Active choice is marked " + symDone + ". Tap it again to clear.\n")
	rb.WriteString(panelTableRich(rows))
	pb.WriteString(title + "\n")
	pb.WriteString("Active choice is marked. Tap it again to clear.\n")
	pb.WriteString(panelTablePlain(rows))
}

// panelTableRich renders a small two-column table for a sub-panel.
func panelTableRich(rows [][2]string) string {
	var b strings.Builder
	b.WriteString("\n| Setting | Value |\n|:--------|:------|\n")
	for _, row := range rows {
		fmt.Fprintf(&b, "| %s | %s |\n", escapeRichMD(row[0]), escapeRichMD(row[1]))
	}
	return b.String()
}

func panelTablePlain(rows [][2]string) string {
	var b strings.Builder
	b.WriteString("\n")
	for _, row := range rows {
		fmt.Fprintf(&b, "%s: %s\n", row[0], row[1])
	}
	return b.String()
}

// profileCompactRich renders all settings as one condensed line per category
// (much smaller than the old 12-row table).
func profileCompactRich(p UserPrefs) string {
	var b strings.Builder
	b.WriteString("\n")
	for _, g := range profileGroups(p) {
		fmt.Fprintf(&b, "%s  %s\n", g.icon, escapeRichMD(g.line))
	}
	return b.String()
}

func profileCompactPlain(p UserPrefs) string {
	var b strings.Builder
	b.WriteString("\n")
	for _, g := range profileGroups(p) {
		fmt.Fprintf(&b, "%s  %s\n", g.icon, g.line)
	}
	return b.String()
}

// usageBoxRich renders the user's lifetime usage tally as a compact table appended
// under the root profile card. A brand-new user (all zeroes) still gets the box so
// the layout is stable; FirstSeenUnix is 0 until their first command lands.
func usageBoxRich(s UserStats) string {
	var b strings.Builder
	b.WriteString("\n## 📊 Your usage\n")
	b.WriteString("| Activity | Count |\n|:---------|------:|\n")
	fmt.Fprintf(&b, "| Commands | %d |\n", s.TotalCommands)
	fmt.Fprintf(&b, "| Album rips | %d |\n", s.AlbumRips)
	fmt.Fprintf(&b, "| Artist rips | %d |\n", s.ArtistRips)
	fmt.Fprintf(&b, "| Playlist rips | %d |\n", s.PlaylistRips)
	fmt.Fprintf(&b, "| Song rips | %d |\n", s.SongRips)
	fmt.Fprintf(&b, "| Cancels | %d |\n", s.Cancels)
	if s.FirstSeenUnix > 0 {
		fmt.Fprintf(&b, "\n_First seen %s_\n", escapeRichMD(usageDate(s.FirstSeenUnix)))
	}
	return b.String()
}

func usageBoxPlain(s UserStats) string {
	var b strings.Builder
	b.WriteString("\n📊 Your usage\n")
	fmt.Fprintf(&b, "Commands: %d\n", s.TotalCommands)
	fmt.Fprintf(&b, "Album rips: %d\n", s.AlbumRips)
	fmt.Fprintf(&b, "Artist rips: %d\n", s.ArtistRips)
	fmt.Fprintf(&b, "Playlist rips: %d\n", s.PlaylistRips)
	fmt.Fprintf(&b, "Song rips: %d\n", s.SongRips)
	fmt.Fprintf(&b, "Cancels: %d\n", s.Cancels)
	if s.FirstSeenUnix > 0 {
		fmt.Fprintf(&b, "First seen %s\n", usageDate(s.FirstSeenUnix))
	}
	return b.String()
}

// usageDate formats a unix timestamp as a short Dhaka-local date for the usage box.
func usageDate(unix int64) string {
	return time.Unix(unix, 0).In(dhakaZone).Format("Jan 2, 2006")
}

type profileGroup struct {
	icon string
	line string
}

// profileGroups builds the compact one-liner-per-category view.
func profileGroups(p UserPrefs) []profileGroup {
	join := func(rows [][2]string) string {
		parts := make([]string, len(rows))
		for i, r := range rows {
			parts[i] = r[0] + ": " + r[1]
		}
		return strings.Join(parts, " · ")
	}
	return []profileGroup{
		{"🎵", join(audioSummaryRows(p))},
		{"🎤", join(lyricsSummaryRows(p))},
		{"🖼", join(artworkSummaryRows(p))},
		{"📤", join(deliverySummaryRows(p))},
	}
}

// Per-category summary row functions — the single source of truth for which
// settings belong to each panel.

func audioSummaryRows(p UserPrefs) [][2]string {
	return [][2]string{
		{"Codec", choiceLabel(pfCodecChoices, p.Codec)},
		{"Quality", choiceLabel(pfQualityChoices, p.Quality)},
		{"AAC variant", choiceLabel(pfAacChoices, p.AacType)},
		{"Apple Master", boolLabel(p.AppleMaster)},
	}
}

func lyricsSummaryRows(p UserPrefs) [][2]string {
	return [][2]string{
		{"Lyrics", choiceLabel(pfLyricChoices, p.LyricMode)},
		{"Embed lyrics", boolLabel(p.EmbedLrc)},
	}
}

func artworkSummaryRows(p UserPrefs) [][2]string {
	return [][2]string{
		{"Cover delivery", choiceLabel(pfCoverDelivChoices, p.CoverDelivery)},
		{"Animated artwork", boolLabel(p.AnimatedArt)},
		{"Embed cover", boolLabel(p.EmbedCover)},
	}
}

func deliverySummaryRows(p UserPrefs) [][2]string {
	return [][2]string{
		{"Delivery target", choiceLabel(pfTargetChoices, p.DeliveryTarget)},
		{"Lyric language", choiceLabel(pfLanguageChoices, p.Language)},
		{"MV resolution", mvLabel(p.MVMax)},
		{"Artist zip", choiceLabel(pfArtistZipChoices, p.ArtistZip)},
	}
}

// profileSummaryRows returns all rows (used by callers that need the full list).
func profileSummaryRows(p UserPrefs) [][2]string {
	var all [][2]string
	all = append(all, audioSummaryRows(p)...)
	all = append(all, lyricsSummaryRows(p)...)
	all = append(all, artworkSummaryRows(p)...)
	all = append(all, deliverySummaryRows(p)...)
	return all
}

func choiceLabel(choices []pfChoice, value string) string {
	for _, c := range choices {
		if c.value == value {
			if c.value == "" {
				return "Default"
			}
			return c.label
		}
	}
	if value == "" {
		return "Default"
	}
	return value
}

func boolLabel(b *bool) string {
	if b == nil {
		return "Default"
	}
	if *b {
		return "On"
	}
	return "Off"
}

func mvLabel(n int) string {
	if n == 0 {
		return "Default"
	}
	return strconv.Itoa(n) + "p"
}

// =============================================================================
// Keyboards
// =============================================================================

func (b *TelegramBot) profileMarkup(panel string, p UserPrefs) InlineKeyboardMarkup {
	switch panel {
	case "audio":
		return InlineKeyboardMarkup{InlineKeyboard: concatRows(
			labelRow("Codec"),
			choiceRows("codec", pfCodecChoices, p.Codec),
			labelRow("Quality"),
			choiceRows("quality", pfQualityChoices, p.Quality),
			labelRow("AAC variant"),
			choiceRows("aac_type", pfAacChoices, p.AacType),
			toggleRow("apple_master", "Apple Master", p.AppleMaster),
			backRow(),
		)}
	case "lyrics":
		return InlineKeyboardMarkup{InlineKeyboard: concatRows(
			labelRow("Lyrics mode"),
			choiceRows("lyric_mode", pfLyricChoices, p.LyricMode),
			toggleRow("embed_lrc", "Embed lyrics", p.EmbedLrc),
			backRow(),
		)}
	case "artwork":
		return InlineKeyboardMarkup{InlineKeyboard: concatRows(
			labelRow("Cover delivery"),
			choiceRows("cover_delivery", pfCoverDelivChoices, p.CoverDelivery),
			toggleRow("animated_art", "Animated artwork", p.AnimatedArt),
			toggleRow("embed_cover", "Embed cover", p.EmbedCover),
			backRow(),
		)}
	case "delivery":
		return InlineKeyboardMarkup{InlineKeyboard: concatRows(
			labelRow("Delivery target"),
			choiceRows("delivery_target", pfTargetChoices, p.DeliveryTarget),
			labelRow("Language"),
			choiceRows("language", pfLanguageChoices, p.Language),
			labelRow("MV resolution"),
			choiceRows("mv_max", pfMVChoices, strconv.Itoa(p.MVMax)),
			labelRow("Artist zip"),
			choiceRows("artist_zip", pfArtistZipChoices, p.ArtistZip),
			backRow(),
		)}
	default: // root
		return InlineKeyboardMarkup{InlineKeyboard: [][]InlineKeyboardButton{
			{
				{Text: "🎵 Audio", CallbackData: "pf:nav:audio", Style: "primary"},
				{Text: "🎤 Lyrics", CallbackData: "pf:nav:lyrics", Style: "primary"},
			},
			{
				{Text: "🖼 Artwork", CallbackData: "pf:nav:artwork", Style: "primary"},
				{Text: "📤 Delivery", CallbackData: "pf:nav:delivery", Style: "primary"},
			},
			{
				{Text: "↺ Reset", CallbackData: "pf:reset", Style: "danger"},
				{Text: "✓ Done", CallbackData: "pf:done", Style: "success"},
			},
		}}
	}
}

// choiceRows lays out a field's choices as tappable buttons (max 3 per row), the
// active value marked with a leading symDone.
func choiceRows(field string, choices []pfChoice, current string) [][]InlineKeyboardButton {
	var rows [][]InlineKeyboardButton
	var row []InlineKeyboardButton
	for _, c := range choices {
		label := c.label
		style := ""
		if c.value == current {
			label = symDone + " " + label
			style = "success"
		}
		row = append(row, InlineKeyboardButton{
			Text:         label,
			CallbackData: "pf:set:" + field + ":" + c.value,
			Style:        style,
		})
		if len(row) == 3 {
			rows = append(rows, row)
			row = nil
		}
	}
	if len(row) > 0 {
		rows = append(rows, row)
	}
	return rows
}

// toggleRow renders a tri-state boolean as a single button showing its state.
func toggleRow(field, label string, v *bool) [][]InlineKeyboardButton {
	state := "Default"
	style := ""
	if v != nil {
		if *v {
			state = "On"
			style = "success"
		} else {
			state = "Off"
			style = "danger"
		}
	}
	return [][]InlineKeyboardButton{{
		{Text: fmt.Sprintf("%s: %s", label, state), CallbackData: "pf:toggle:" + field, Style: style},
	}}
}

func labelRow(text string) [][]InlineKeyboardButton {
	return [][]InlineKeyboardButton{{
		{Text: "── " + text + " ──", CallbackData: "pf:nop", Style: "primary"},
	}}
}

func backRow() [][]InlineKeyboardButton {
	return [][]InlineKeyboardButton{{
		{Text: "‹ Back", CallbackData: "pf:nav:root"},
	}}
}

func concatRows(groups ...[][]InlineKeyboardButton) [][]InlineKeyboardButton {
	var out [][]InlineKeyboardButton
	for _, g := range groups {
		out = append(out, g...)
	}
	return out
}
