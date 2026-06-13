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
- Copy `bot/config.yaml.example` → `bot/config.yaml`, fill in all fields
- Gitignored: `bot/config.yaml`, `bot/telegram-cache.json`, `bot/downloads/`

## CLI flags (binary has two modes)
- `--bot` — Telegram bot mode (production mode)
- `--search [album|song|artist] <query>` — interactive search
- `--atmos` / `--aac` — quality override
- `--select` — interactive track selection
- `--song` — single song from album URL
- `--all-album` — download all artist albums
- `--debug` — print quality info without downloading
- Default: pass Apple Music URLs directly as args for CLI download

## Architecture
- **Single sequential download worker** — `startDownloadWorker()` reads from a buffered chan (cap 20). Only one download at a time due to package-level globals (`dl_atmos`, `dl_aac`, `dl_song`, `lastDownloadedPaths`, `downloadedMeta`)
- **Decryption via wrapper-manager gRPC** — `bot/utils/wmgrpc/client.go` dials wrapper-manager at `wrapper-manager-addr` (default `wrapper-manager:8080`). `DownloadAndDecrypt()` in `decrypt.go` downloads fMP4 segments, decrypts via a persistent bidirectional `Decrypt` stream, and reassembles with MP4Box.
- **Three delivery modes** (user picks via inline keyboard):
  1. Telegram Bot API — single tracks <50MB
  2. MTProto (`gotd/td`) — ZIP up to 2GB; persists session in `mtproto-session.json`
  3. Gofile — fallback for oversized packages
- Bot API (long-polling `getUpdates`) for receiving messages; MTProto only for file uploads
- **Required external binaries** (bundled in Docker): `ffmpeg`, `MP4Box` (GPAC)

## Wrapper-manager setup (one-time)
```bash
docker compose build wrapper-manager
docker compose run --rm wrapper-manager -L "APPLE_ID:PASSWORD"
```
Wrapper-manager needs `--privileged` (Frida hooks inside Android emulator). Use tools like `login.py` from [AppleMusicDecrypt](https://github.com/WorldObservationLog/AppleMusicDecrypt) to add accounts.

## Gotchas
- `bot/utils/wmgrpc/` — gRPC stubs are imported directly from `github.com/WorldObservationLog/wrapper-manager/proto` (no local proto generation needed)
- AAC-LC uses `WebPlayback` RPC (patched `webplay.go` with nil-safe checks — upstream crashes on some responses)
- Non-legacy (ALAC/Atmos) uses `M3U8` RPC + `Decrypt` streaming RPC
- The `Decrypt` RPC is bidirectional streaming — one stream per track, samples sent/received sequentially (not per-sample RPC)
- MTProto peer cache is in-memory only; lost on restart
- `bot/utils/gofile.go` hardcodes `upload-ap-sgp.gofile.io` as the upload server
- The bot is a fork of [moeleak/apple-music-downloader-bot](https://github.com/moeleak/apple-music-downloader-bot); upstream commits (e.g. SQLite cache, timeout fixes) may be relevant for porting

## Branch: feat/wrapper-manager
See `docs/WRAPPER-MANAGER-MIGRATION.md` for detailed migration notes.
- VPS is currently on `main` (old Frida wrapper) — needs re-login before switching to `feat/wrapper-manager`
- Wrapper-manager accounts are ephemeral — lost on container rebuild
- Progress bar still cosmetic (minor)
