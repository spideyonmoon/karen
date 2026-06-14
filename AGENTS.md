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

## Branch: feat/wrapper-manager
- 28 commits on top of `main`. See `SESSION_HISTORY.md` for details.
- Full-track ALAC delivery verified end-to-end.
