# Karen — Apple Music Telegram Bot

## Project structure
- `bot/` — Go binary (module `main`, Go 1.25.5), entrypoint `main.go`
- `bot/utils/wmgrpc/` — gRPC client for wrapper-manager (decryption backend)
- `wrapper-manager/` — Dockerfile to build [WorldObservationLog/wrapper-manager](https://github.com/WorldObservationLog/wrapper-manager)

## Quick start
```bash
cp .env.example .env
# edit .env (secrets + APPLE_ID_N/APPLE_PASS_N pairs), then one-time bootstrap:
./setup.sh
```

## Config — .env is the single source of truth
- Edit `.env` only. `./generate.sh` reads it and writes `bot/config.yaml` + `docker-compose.override.yml`; both are GENERATED — never hand-edit.
- The count of `APPLE_ID_N`/`APPLE_PASS_N` pairs in `.env` drives instance count: wrapper services, volumes, ports (`8080+N`), and the `wrapper-manager-addrs` list all follow from it.
- `storefront` is fixed to `us` in the generator (not in `.env`). Authoritative non-secret config values live in `generate.sh`'s heredoc, NOT `bot/config.yaml.example` (kept only as human reference and may drift).
- `setup.sh` = full bootstrap (generate + clone AMD login client + build + per-account login + start). Re-run it only when the account list changes.
- Gitignored: `.env`, `docker-compose.override.yml`, `bot/config.yaml`, `bot/state/`, `bot/downloads/`, `.logins/wm-*.toml`.

## Day-2 operations (steady state)
| Goal | Action |
| --- | --- |
| Ship a code/script change | `git push origin main` — deploy SSHes in, `git reset --hard origin/main`, regenerates config, rebuilds bot |
| Add / remove Apple accounts | edit `.env` on the VPS, then `./setup.sh` (logs in only NEW accounts) |
| Force fresh login of all accounts | `RELOGIN=1 ./setup.sh` |

- **No manual `git pull` on the VPS, ever.** The deploy's `git reset --hard origin/main` syncs all tracked files (incl. the `*.sh` scripts) on every push to `main`. So when you next run `./setup.sh` to add accounts, it's already the latest version. `.env` / `docker-compose.override.yml` / `.logins/wm-*.toml` are gitignored, so the reset never touches them.
- **Login is idempotent.** `setup.sh` skips any wrapper whose volume already has `/root/data/instances.json` (a token). Re-authing an already-logged-in account FAILS — that's what made wm-1 fail on a second run. Only ever log in new/failed accounts.
- **`.env` is parsed literally, never `source`d.** A password with `$`/backtick/`\` would be shell-expanded and break under `set -u`. `generate.sh`/`setup.sh` have a `load_env()` that strips only outer quotes and takes the value verbatim. Proven on a `$`-containing password (wm-2).
- **Compose runs with `--env-file /dev/null`** (in `setup.sh` and `deploy.yml`) so it doesn't parse the secrets `.env` for `${VAR}` interpolation (a `$` in a password warned "variable is not set"). Our compose files use literal values only.
- **OPEN THREAD (unverified):** the idempotency skip assumes the token path is `/root/data/instances.json`. Never confirmed on the box. If wrong, the skip silently no-ops and `setup.sh` re-logs ALL accounts (no worse than before, but risks the re-auth failure). Confirm with `docker compose --env-file /dev/null exec -T wrapper-manager-2 sh -c 'ls -la /root/data'`; fix the one path in `setup.sh` step 4 if the filename differs.

## CLI flags (binary has two modes)
- `--bot` — Telegram bot mode (production)
- `--search [album|song|artist] <query>` — interactive search
- `--atmos` / `--aac` — quality override
- `--select` — interactive track selection
- `--song` — single song from album URL
- `--all-album` — download all artist albums
- `--debug` — print quality info without downloading
- Default: pass Apple Music URLs directly as args for CLI download

## Architecture
- **Single sequential download worker** — `startDownloadWorker()` reads from a buffered chan (cap 20). One download at a time due to package-level globals.
- **Decryption via wrapper-manager gRPC** — `bot/utils/wmgrpc/client.go` dials wrapper-manager instances. `DownloadAndDecrypt()` in `decrypt.go` downloads fMP4 segments via HLS, decrypts via a persistent bidirectional `Decrypt` gRPC stream, and remuxes with `ffmpeg -c copy`.
- **Multi-account pool** — `bot/utils/wmgrpc/pool.go` implements a channel-based FIFO client pool. `wmPool.Acquire()` / `wmPool.Release()` distributes load across wrapper-manager instances. Each instance runs its own Android emulator.
- **Three delivery modes** (user picks via inline keyboard):
  1. Telegram Bot API — single tracks <50MB
  2. MTProto (`gotd/td`) — ZIP up to 2GB; session in `mtproto-session.json`
  3. Gofile — fallback for oversized packages
- Bot API (long-polling `getUpdates`) for receiving messages; MTProto only for file uploads

## State & persistence (`bot/state/`)
- **All JSON state lives in one directory mount: `./bot/state:/app/state`** (`docker-compose.yml`). It holds three files:
  - `telegram-cache.json` — Telegram `file_id` cache (re-send without re-upload).
  - `telegram-state.json` — admin lock + per-user `/profile` rip prefs (`UserPrefs map[int64]*UserPrefs`). This is the **important** data — the only thing the daily backup ships.
  - `telegram-schedule.json` — queued sleeptime-window jobs. Persisted (so the scheduler survives restarts) but **not** backed up; gitignore + ephemeral by design.
- **Why a directory mount, not three single-file mounts:** saves are atomic (write `<file>.tmp` → `os.Rename` over target). A single-file bind mount puts `.tmp` on the container overlay fs while the target is a separate device, so `os.Rename` returns `EXDEV` and the host file silently never updates. A dir mount keeps tmp+target on the same fs. This was a real bug: it's why the scheduler "never delivered" (jobs only lived in memory, wiped on every deploy) and why the cache never persisted. Mount the *directory* of any file you save with tmp+rename — never the file.
- `generate.sh` does `mkdir -p bot/state` (not `touch` a file) so Docker mounts a directory.
- Paths derive from `cacheFile` (default `state/telegram-cache.json`) in `newTelegramBot`; `stateFile`/`scheduleFile` are siblings in the same dir.
- **Daily backup:** `startBackupRoutine`/`performBackup` (`bot/admin_tasks.go`) DM every `ADMIN_IDS` a copy of the admin-lock + profiles (a `karen-state-YYYY-MM-DD.json` document) once a day at **04:00 Dhaka**. The next fire time is computed from the wall clock, so frequent restarts don't spam backups. Worst-case data loss on a VPS wipe is one day. Schedule jobs are deliberately excluded from the backup.
- Legacy orphans: old `bot/telegram-cache.json` / `bot/telegram-state.json` host files (pre-dir-mount) are unused; harmless, can be `rm`'d on the VPS.

## Docker services
- `docker-compose.yml` (tracked) — base, defines only the `bot` service. No `depends_on` (wrappers aren't in this file; bot reaches them via gRPC at runtime).
- `docker-compose.override.yml` (generated, gitignored) — `wrapper-manager-N` services + `wm-data-N` volumes, one per account. Compose auto-merges both files.
- Instance `N`: container `karen-wm-N`, port `8080+N` bound to `127.0.0.1`, volume `wm-data-N`.

## Wrapper-manager setup (handled by setup.sh)
- `./setup.sh` builds the shared `karen-wrapper-manager:local` image, boots the wrappers, and logs in each account via the gRPC `Login` RPC (through `do-login.sh`, which reads `AMD=/tmp/AppleMusicDecrypt`).
- There is NO login CLI flag — the upstream `-L "id:pass"` shortcut was removed and now errors `flag provided but not defined: -L`. gRPC only.
- 2FA is not handled (accounts in use have it disabled); `do-login.sh` feeds creds on a non-interactive stdin pipe.
- Wrapper-manager needs `--privileged` (Frida hooks inside Android emulator).

## Required external binaries (bundled in Docker)
- `ffmpeg` — fMP4 to flat MP4 remuxing, format conversion
- GPAC base image provides additional tooling

## Gotchas
- `bot/utils/wmgrpc/` — gRPC stubs imported from `github.com/WorldObservationLog/wrapper-manager/proto` (no local proto generation)
- AAC-LC uses `WebPlayback` RPC (patched `wrapper-manager/webplay.go` with nil-safe checks — upstream crashes on some responses)
- ALAC/Atmos uses `M3U8` RPC + bidirectional `Decrypt` streaming RPC (one stream per track)
- MTProto peer cache is in-memory only; lost on restart
- `bot/utils/gofile.go` hardcodes `upload-ap-sgp.gofile.io`
- Fork of [moeleak/apple-music-downloader-bot](https://github.com/moeleak/apple-music-downloader-bot); upstream commits may be relevant for porting

## Known issues
- Progress bar is cosmetic (estimate-based, not accurate)
- No retry logic on failed segment downloads
- `wrapper-manager/webplay.go` `GetLicense()` has unsafe type assertion (line 106) that can panic
- **MTProto upload sawtooth on DC5** — large uploads repeatedly drop with `engine forcibly closed: context canceled` at the call site. Root cause (confirmed via gotd's zap logger): `pingLoop: disconnect (pong missed): write tcp …91.108.56.112:443: i/o timeout`. `uploadThreads = 16` (`mtproto.go`, 8 MB in flight) saturates the single-connection TCP send buffer to DC5 (Singapore, ~176 ms RTT, near its bandwidth-delay product), so gotd's keepalive ping write times out → engine torn down → in-flight upload parts canceled. The resumable uploader + `withUploadRetryN` rides it out (each retry resumes at a higher part number), so files **do** deliver, just with a drop/resume sawtooth — degraded, not broken. Ruled out: not FloodWait (no 429/FLOOD_WAIT), not bandwidth (39↑/72↓ MB/s measured), not MTU/routing, not resources, not duplicate sessions, not the bot token. gotd v0.144.0 does NOT expose ping/pong timeout in `telegram.Options` (hardcoded in `mtproto/ping.go`), so the keepalive can't be relaxed without forking. **Currently trialing `uploadThreads = 8`** (4 MB in flight) as the first code-side mitigation: halving in-flight data leaves the TCP send buffer headroom for the keepalive ping, at some cost to peak upload speed. Watch the zap log for `pong missed` recurrence and compare throughput; revert to `16` (or make this a per-DC config field) if speed regresses without curing the sawtooth. Note `4` is too low (kills the upload-speed surge on a fat pipe) — `8` is the deliberate middle ground. Untried alternative if `8` doesn't hold: raise OS socket send buffer (`net.ipv4.tcp_wmem` sysctl on the VPS) so data + ping coexist — no code change, instantly reversible, ~60-70% odds. Other levers: `ReconnectionBackoff` (shrinks dead time, exposed in Options), or make `uploadThreads` a config field (default 16) to tune per-VPS/per-DC. The gotd zap logger that surfaced this is wired in `mtproto.go` (`mtprotoLogger()`, Info level) — verbose but invaluable; keep or gate behind a debug flag.
- **`mtproto-session.json` is NOT in a compose volume** — every `--build` deploy wipes it (it lives in the container layer at `/app/`, only `config.yaml`/`downloads`/`telegram-cache.json` are mounted), forcing a cold MTProto re-auth (`Generating new auth key`) on each deploy. Latent bug, not today's root cause. Fix: add a volume mount for it in `docker-compose.yml`.

## Build & deploy (read before touching a branch)
- **Never build locally.** The only Go module is `bot/go.mod` (module `main`, Go 1.25.5); `wrapper-manager/` is NOT a Go module. Don't run `go build`/`go vet`/`gofmt` on the dev box — push and let GitHub compile.
- **CI compile gate:** `.github/workflows/build-check.yml` runs `go build ./...` (working-dir `bot/`, Go version read from `bot/go.mod`) on **feature-branch pushes and all PRs** (pushes to `main` are excluded — Deploy's own `--build` is the gate there). Green = it compiles. This is the ONLY automated compile check — there is no test suite.
- **Deploy:** `.github/workflows/deploy.yml` triggers **only on push to `main`**. It SSHes to the VPS and runs `git fetch origin main` → `git reset --hard origin/main` → `docker compose up -d --build bot`. It never references any other branch, so feature-branch pushes are deploy-safe.
- **Branch workflow:** do work on a feature branch (e.g. `UX`) → push → confirm "Build Check" is green on the GitHub Actions page → merge to `main` (PR or fast-forward) → the `main` push deploys. A feature branch can never reach prod until you deliberately merge it.
- **Gotchas when committing here:** the shell mangles pasted multi-line `-m "$(cat <<EOF …)"` commit messages (indentation breaks the heredoc; wrapped lines make `git` see `-m` with no value). Use a single-line `-m`, or several `-m` flags each on ONE physical line.
- **`gh` CLI is installed** (v2.94+) — use `gh run list`/`gh run watch` to check CI, `gh pr create`/`gh pr merge` to ship. Note `gh pr view --json` has no `merged` field; use `state,mergedAt,mergeCommit`.

## Branches
- `main` — current development (post-wrapper-manager overhaul, "v2")
- `v1-stable` — frozen snapshot of the pre-overhaul code (single-account wrapper, MP4Box remux, serial downloads). The legendary v1. Preserved for reference; do not target new work here.
- `v1.0.0` — git tag pointing at the same commit as `v1-stable`, visible on the GitHub Releases page.
- See `SESSION_HISTORY.md` for the full migration log.
