# Karen — Apple Music Telegram Bot

## Project structure
- `bot/` — Go binary (module `main`, Go 1.25.5), entrypoint `main.go`
- `bot/utils/wmgrpc/` — gRPC client for wrapper-manager (decryption backend)
- `wrapper-manager/` — Dockerfile to build [WorldObservationLog/wrapper-manager](https://github.com/WorldObservationLog/wrapper-manager)

## Quick start
```bash
cp bot/config.yaml.example bot/config.yaml
# edit config, then:
docker compose up -d --build
```

## Config
- Copy `bot/config.yaml.example` -> `bot/config.yaml`, fill in all fields
- Gitignored: `bot/config.yaml`, `bot/telegram-cache.json`, `bot/downloads/`

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

## Docker services (docker-compose.yml)
- `wrapper-manager-1` — port 8081, volume `wm-data-1`
- `wrapper-manager-2` — port 8082, volume `wm-data-2`
- `bot` — depends on both wrapper-managers

## Wrapper-manager setup (one-time per account)
```bash
docker compose build wrapper-manager
# For each account, run interactively:
docker compose run --rm wrapper-manager-1 -L "APPLE_ID:PASSWORD"
```
Wrapper-manager needs `--privileged` (Frida hooks inside Android emulator).

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
- **MTProto upload sawtooth on DC5** — large uploads repeatedly drop with `engine forcibly closed: context canceled` at the call site. Root cause (confirmed via gotd's zap logger): `pingLoop: disconnect (pong missed): write tcp …91.108.56.112:443: i/o timeout`. `uploadThreads = 16` (`mtproto.go`, 8 MB in flight) saturates the single-connection TCP send buffer to DC5 (Singapore, ~176 ms RTT, near its bandwidth-delay product), so gotd's keepalive ping write times out → engine torn down → in-flight upload parts canceled. The resumable uploader + `withUploadRetryN` rides it out (each retry resumes at a higher part number), so files **do** deliver, just with a drop/resume sawtooth — degraded, not broken. Ruled out: not FloodWait (no 429/FLOOD_WAIT), not bandwidth (39↑/72↓ MB/s measured), not MTU/routing, not resources, not duplicate sessions, not the bot token. `16` is intentional (gives a real upload-speed surge; do NOT just drop to 4). gotd v0.144.0 does NOT expose ping/pong timeout in `telegram.Options` (hardcoded in `mtproto/ping.go`), so the keepalive can't be relaxed without forking. Untried fix to test first: raise OS socket send buffer (`net.ipv4.tcp_wmem` sysctl on the VPS) so data + ping coexist — no code change, instantly reversible, ~60-70% odds. Other levers: `ReconnectionBackoff` (shrinks dead time, exposed in Options), or make `uploadThreads` a config field (default 16) to tune per-VPS/per-DC. The gotd zap logger that surfaced this is wired in `mtproto.go` (`mtprotoLogger()`, Info level) — verbose but invaluable; keep or gate behind a debug flag.
- **`mtproto-session.json` is NOT in a compose volume** — every `--build` deploy wipes it (it lives in the container layer at `/app/`, only `config.yaml`/`downloads`/`telegram-cache.json` are mounted), forcing a cold MTProto re-auth (`Generating new auth key`) on each deploy. Latent bug, not today's root cause. Fix: add a volume mount for it in `docker-compose.yml`.

## Branches
- `main` — current development (post-wrapper-manager overhaul, "v2")
- `v1-stable` — frozen snapshot of the pre-overhaul code (single-account wrapper, MP4Box remux, serial downloads). The legendary v1. Preserved for reference; do not target new work here.
- `v1.0.0` — git tag pointing at the same commit as `v1-stable`, visible on the GitHub Releases page.
- See `SESSION_HISTORY.md` for the full migration log.
