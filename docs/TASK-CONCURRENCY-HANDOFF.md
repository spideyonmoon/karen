# Task-concurrency + gofile-lending — handoff

Branch: `feat/task-concurrency-gofile-lending` (pushed, Build Check green).

## Done (committed)
- **Phase 1** — TG uploads serialized via a size-1 `uploadGate` in `bot/mtproto.go`
  (`withUploadRetryN`, ctx-cancellable, held across retries). Gofile stays concurrent.
- **Phase 0** — per-rip state in `bot/ripstate.go` (`RipState`, carried via ctx with
  `withRipState`/`ripStateFrom`). All accessors fall back to package globals on a nil
  receiver → flag OFF and CLI are byte-identical to before. `runDownload`
  (telegram_bot.go) builds a RipState only when `Config.TaskConcurrency` is on and
  threads `rctx` into `fn` + delivery. Enqueue closures (telegram_bot.go ~1808-1865,
  admin_tasks.go ~313, ripSong main.go ~2634) set flags on ctx state w/ global fallback.
- Config keys added to structs.go: `TaskConcurrency` (default false),
  `LendHeadRemainingThreshold` (50), `BorrowerMaxTracks` (30).

## Locked design rules (from user)
- Max 2 active downloaders: head (full pool) + ONE sticky borrower.
- Lend eligibility (BOTH): head is a TG mode AND head remaining tracks > 50;
  borrower is gofile AND borrower total tracks < 30.
- Lend amount k: borrower ≤15 tracks → 1; 16–29 → 2. (≥10 instances = future formula.)
- Borrow slot is single-occupancy + NON-preemptive: once a borrower holds it, later
  arrivals WAIT even if eligible (fairness to the front). No drain/preempt.
- Head finishes downloading → its upload runs detached (TG gate serializes); next queued
  task becomes head with full pool; existing borrower keeps its wrappers until done.
- Uploads: TG one-at-a-time (done, Phase 1); gofile concurrent.

## TODO
- **Phase 2** — DONE (committed, Build Check green). `startDownloadWorker` now branches
  on `Config.TaskConcurrency`: flag off keeps the verbatim serial loop; flag on runs
  `scheduleDownloads` (telegram_bot.go). Mechanism:
  - Per-rip wrapper budget = the `sem` size in the three fan-out loops (main.go album /
    playlist / station), sourced from `rs.wrapperBudget()`. Head budget 0 → full pool;
    borrower → k. Borrower self-limits to k concurrent track goroutines (≤ k Acquire).
  - `RipState` gained `WrapperBudget` + atomic `totalTracks`/`doneTracks`
    (`planTrack`/`trackDone`/`remainingTracks`) so the scheduler reads the head's live
    remaining count.
  - Head download done → `req.onDownloadComplete()` (fired in runDownload right after
    `fn`) closes the head's done-chan → scheduler promotes next head; finished head's
    delivery runs on in its own goroutine (TG gate serializes uploads).
  - Borrow slot = `b.schedBorrowReq` (single, non-preemptive). `tryLendToBorrower`
    (2s tick) peeks the front via the `queuedReqs` mirror, fetches its track count
    (`peekTrackCount`, cached on `req.peekedTracks`), and consumes from the channel under
    `queueMu` only once eligible. `/stop` cancels the borrower too.
  - Head/borrower lifecycle clears `activeReq`/`schedHeadRip`/`schedBorrowReq` only if not
    already superseded (next head is promoted while the old one still uploads).
- **Phase 3** — DONE (committed). Per-task status boards: `activeStatus` (single
  `*DownloadStatus`) replaced by `activeBoards map[string]*DownloadStatus` keyed by
  taskID (guarded by queueMu, init in the constructor). `runDownload` now registers
  EVERY task's board (head + sticky borrower) under its taskID and `defer`s the
  delete — the old `!req.isBorrower` guard and the "only clear if still ours"
  supersede dance are gone (distinct keys = no contention; the dead `isBorrower`
  field was removed). `/status` (telegram_bot.go ~1429) splits boards into this
  chat's (Relocate each) vs other chats' (one combined snapshot via new
  `RenderSnapshotBare()` + a single `queueBoardSuffix()`); idle → idle board.
  Enqueue refresh (~2183) relocates all of this chat's boards. Helpers added:
  `activeBoardsSnapshot()` and `DownloadStatus.RenderSnapshotBare()` (RenderSnapshot
  = bare + queue suffix). Flag-off serial path is unchanged behavior (one entry).
- **Phase 4** — add the 3 config keys to `bot/config.yaml.example` + update docs.

## Verify
No local Go toolchain (never build locally). Push branch → `.github/workflows/build-check.yml`
runs `go build ./...`. `gh run watch <id> --exit-status`.
