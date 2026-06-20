# Combined per-chat status board + footer/collapse polish

## Context

With task-concurrency live, a chat can have two active boards at once (head + sticky
borrower). Today each `DownloadStatus` owns its **own** Telegram message and its **own**
3s edit loop, so two tasks = two messages, two independent edit streams to the same chat
(~0.67 edits/s combined) — visually redundant and a real `editMessageText` floodwait risk.

Three changes, all in `bot/telegram_bot.go`:

1. **One message per chat.** Stitch all of a chat's live tasks into a single message,
   ordered task 1 → task 2 → queue, edited by one loop at a bounded cadence (N tasks still
   = 1 edit stream). The 32768-char rich limit makes this trivially safe (~1.5–2k chars/board).
2. **Collapse the per-track table** for every task (start collapsed; tap to expand).
3. **Reformat the footer** into three real lines and drop the stray `✕`.

## Design

### A. Per-chat aggregator (`chatBoard`)

Move message ownership + the edit loop off `DownloadStatus` onto a new per-chat type.
`DownloadStatus` stays the per-task **data + renderer** (keeps `snapshot`, `UpdateTrack`,
`Update`, `setLatestLocked`, speed sampling, `formatProgressText`, `formatProgressRich`);
it loses `messageID`, `loop()`, `flush()`, `Relocate()` and its own `updateCh`/`stopCh`.

```go
type chatBoard struct {
    bot        *TelegramBot
    chatID     int64
    mu         sync.Mutex
    messageID  int
    members    []*DownloadStatus // registration order: head, then borrower
    lastText   string
    lastUpdate time.Time
    dirty      bool
    updateCh   chan struct{}
    stopCh     chan struct{}
    stopOnce   sync.Once
}
```

Bot fields (in the struct near line 137):
- keep `activeBoards map[string]*DownloadStatus` (taskID → status) for per-task lookups.
- add `chatBoards map[int64]*chatBoard` (chatID → group), init in the constructor (~535),
  both guarded by the existing `queueMu`.

**Lock discipline:** `queueMu` guards `activeBoards`/`chatBoards`; `chatBoard.mu` guards
members/messageID/lastText; `DownloadStatus.mu` guards its own data. Ordering when nested:
`group.mu → member.mu` only (flush). Never hold `queueMu` during a network send.

### B. Lifecycle

- **Attach** (`runDownload`, replaces `newDownloadStatus` at ~2328 + the registration block
  ~2341): build the data-only `DownloadStatus`; under `queueMu` get-or-create the chat's
  `chatBoard`, append the member, set `activeBoards[taskID]`, clear the idle board. Outside
  the lock: if the group is new, send the initial message (or adopt `req.statusMessageID`
  if >0) and `go grp.loop()`; if joining an existing group, `grp.signal()` to re-render and
  delete the now-orphaned `req.statusMessageID` placeholder if one was pre-sent.
- **Retire** `b.retireBoard(s *DownloadStatus, ok bool)`:
  - idempotent (guard on an `s.retired` flag).
  - remove `s` from `activeBoards` and from the group's `members`.
  - **members remain** → `grp.signal()` (section disappears immediately; success *or*
    failure — honors "remove immediately").
  - **group now empty** → stop the loop, remove from `chatBoards`, then:
    - `ok == true` (success) → delete the message (matches today).
    - `ok == false` (failure/cancel) → final edit to `s`'s standalone snapshot and **keep**
      the message, so a lone failure stays visible (preserves today's single-task behavior;
      no regression where the only error message vanishes).
- **Call-site migration:**
  - The `defer status.Stop()` + `defer delete(activeBoards…)` pair (~2334/2350) → single
    `defer b.retireBoard(status, false)` (catch-all = treat as non-success unless already
    retired by an explicit success call).
  - The ~6 success sites that do `status.Stop(); _ = b.deleteMessage(chatID, status.MessageID())`
    (lines ~2616, 2670, 2722, 2768, 2807, 2958/2978 area) → `b.retireBoard(status, true)`.
  - `status.UpdateSync("Failed…/Cancelled/No files…")` calls stay; they set the final text
    that `retireBoard(…, false)` shows if it was the last member. `UpdateSync` now triggers
    a **synchronous group flush** instead of `s.flush(true)`.
  - `mtproto.go` only calls `status.Update(...)` — unchanged.

### C. Group render + loop

- `chatBoard.loop()`: same 3s ticker + `updateCh` as today's `DownloadStatus.loop()`.
- `chatBoard.flush(force)`: under `grp.mu`, skip if `messageID == 0`; for each live member
  render its **section** (no queue suffix); join with a divider (`\n\n---\n\n` rich / a
  `─` rule plain); append `queueBoardSuffix()` / `queueBoardSuffixRich()` **once**; dedup on
  plain text + 3s throttle (lift the existing logic from `DownloadStatus.flush`, lines
  4088–4148); edit via `editMessageRich` with plain fallback.
- Add member section helpers reusing existing renderers: `renderSectionRich()` (reads latest
  under lock → `formatProgressRich`) and reuse `RenderSnapshotBare()` for plain. The current
  `RenderSnapshot`/`RenderSnapshotBare` (3928–3960) stay for `/status`'s "other chats" path.
- `chatBoard.relocate(replyToID)`: delete current message, re-send combined text at bottom,
  keep the loop (replaces `DownloadStatus.Relocate`).

### D. `/status` and enqueue refresh

- `/status` (~1435): group `activeBoardsSnapshot()` by chat. For **this** chat, call the
  chat's `chatBoard.relocate()` **once** (not per-board). "Other chats" path unchanged
  (per-board `RenderSnapshotBare()` + one queue suffix). Idle path unchanged.
- Enqueue refresh (~2185): relocate this chat's single `chatBoard` once instead of looping
  `s.Relocate` per board.

### E. Collapsible track table (one-line change)

`formatProgressRich` line 4558: `<details open>` → `<details>` (covers both tasks since both
use this renderer). Live per-track rows are then behind a tap; the summary line still shows
`Tracks · N active`.

### F. Footer reformat (rich renderer, lines 4610–4615)

The four `> …` lines currently soft-wrap into one run-on line in Telegram's rich blockquote.
Rebuild as **three** lines with GFM hard breaks (two trailing spaces before `\n`), drop
`symCancel`, and put speed+elapsed together:

```
> ⚡︎ <speed>/s  ⌛︎ <elapsed>␠␠
> by <user> · <mode>␠␠
> cancel /stop_<id>
```

(`␠␠` = two trailing spaces = GFM hard break.) If two-space breaks don't hold on deploy,
fall back to `<br>`. The plain renderer (`formatHeader`/`formatProgressText`) already emits
real newlines and is only the rich-unsupported fallback — leave its layout as-is.

## Critical files

- `bot/telegram_bot.go` — all changes: new `chatBoard` type + methods, bot fields/constructor,
  `runDownload` attach/retire, the ~6 success call sites, `/status`, enqueue refresh,
  `formatProgressRich` (collapse + footer). `DownloadStatus` loses message/loop ownership.
- No changes to `bot/mtproto.go`, `bot/main.go`, `bot/ripstate.go`, or config.

## Verification

No local Go build (never build locally). Push branch → GitHub **Build Check** runs
`go build ./...`; `gh run watch <id> --exit-status`. Then merge → **Deploy Karen Bot** to the
VPS and exercise on the live bot:

1. `/dl <playlist >50 tracks> -tgu` then `/dl <album <30 tracks> -go` → expect **one** message
   with both boards stitched (task 1 then task 2) + queue at the bottom; only one edit stream.
2. Both track tables start **collapsed**; tapping expands.
3. Footer renders as **three** lines, no `✕`, `/stop_…` still tappable.
4. `/stop` one task → its section disappears immediately; the other stays live in the same
   message.
5. Let a single task finish → message is removed; let a single task fail → message persists
   showing the failure.
6. Two tasks in **different** chats → still two separate messages (per-chat scope), each fine.
