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
- **Phase 2** — replace the serial worker in `startDownloadWorker` (telegram_bot.go ~627)
  with a flag-gated scheduler implementing the rules above. Per-task wrapper budget
  layered on existing `wmgrpc.Pool` (borrower self-limits to k concurrent Acquire; head
  takes rest). Flag off = keep current `for req := range downloadQueue` serial loop.
- **Phase 3** — per-task status boards: `activeStatus` (single) → registry keyed by
  taskID; `/status` lists active + queued.
- **Phase 4** — add the 3 config keys to `bot/config.yaml.example` + update docs.

## Verify
No local Go toolchain (never build locally). Push branch → `.github/workflows/build-check.yml`
runs `go build ./...`. `gh run watch <id> --exit-status`.
