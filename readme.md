# Karen

Karen pulls lossless audio off Apple Music and delivers it to your Telegram chat — ALAC, Dolby Atmos, or AAC, with cover art and metadata baked in. Under the hood she's a **read-through catalog**: anything ripped once is stored in a private Telegram channel and copied back instantly the next time anyone asks. Popular music gets ripped exactly once and delivered forever.

A docs site is on the way. Until then this readme is the short version — enough to understand it and get it running.

## What she does

- **Lossless and then some** — ALAC up to 192 kHz, Dolby Atmos, AAC-LC, optional FLAC conversion. Music videos too.
- **Ripped once, served forever** — a catalog backed by a private Telegram dump channel. The first request for a track rips it; every request after that copies the stored file straight back, no re-ripping. A half-cached album only fetches the tracks it's missing.
- **Parallel everything** — multiple Apple Music accounts rip concurrently across emulator backends, and a pool of helper bot accounts uploads in parallel. Telegram throttles per account, so spreading an album's tracks across several bots cuts upload time roughly proportionally and spreads out the rate limits.
- **Delivery that fits** — files arrive as clean copies (no "forwarded from" header). Tracks, a zip, or a Gofile link depending on size; huge discographies are flushed to Gofile in numbered parts mid-rip, so a rip never has to fit on disk all at once.
- **Per-user profiles** — save your codec, quality, and delivery preferences once and `/dl` runs with zero flags and zero prompts.
- **Made to run unattended** — concurrent download scheduler, live per-task status boards, a queue, bulk `/dl`, inline search, and a full admin/sudo toolkit (bans, usage stats, restart, system status).

## How it works

Karen treats Telegram itself as the storage layer. Every file she's produced lives in a private dump channel; a small Postgres catalog holds *pointers* to those messages — never the bytes themselves. So a request is, first and foremost, a lookup:

```
request → resolve to track IDs → catalog lookup (per track)
  ├─ HIT   → copy the stored file straight to you  (no rip, no re-upload — instant)
  └─ MISS  → rip it → upload to the dump → index it → copy it to you
```

The pieces behind that flow:

```
   ┌──────────┐   /dl url     ┌────────────────────┐
   │ Telegram │ ────────────▶ │      Bot (Go)      │ ──lookup──▶ Postgres
   │   user   │ ◀──────────── │  orchestrator +    │ ◀──pointers─ (catalog)
   └──────────┘  clean copy   │  delivery          │
                              └──┬──────────────┬──┘
                       rip + decrypt        upload in parallel
                                │              │
                     ┌──────────▼─────┐  ┌─────▼──────────────┐
                     │ wrapper-mgr ×N │  │  helper-bot pool   │
                     │ gRPC, 1 Apple  │  │  → dump channel    │
                     │ account each   │  │  (stores the bytes)│
                     └────────────────┘  └────────────────────┘
```

- **wrapper-manager ×N** — Android emulators running a Frida-hooked Apple Music app, one account each, exposing gRPC for playlists, WebPlayback, and live DRM decryption. The rip engine for cache misses.
- **helper-bot pool** — extra bot accounts that upload to the dump channel in parallel, dividing both wall-time and FLOOD_WAIT pressure across accounts.
- **catalog** — Postgres (managed on Supabase) holding pointer rows keyed by Apple track ID + format tier. This is the lookup that turns repeat requests into instant copies.
- **bot** (Go) — fetches the playlist, downloads HLS segments in parallel, decrypts over a gRPC stream, remuxes with ffmpeg, and copies the result to you with no trace of the dump.

The catalog and helper pool are optional. With neither configured, Karen falls back to the original behavior: rip on demand and upload directly.

## Quick start

You'll need a Linux host with Docker, a Telegram bot token, and at least one Apple Music account (2FA off — the login path doesn't prompt).

```bash
git clone https://github.com/spideyonmoon/karen.git ~/karen
cd ~/karen
cp .env.example .env   # fill in tokens + one APPLE_ID_N / APPLE_PASS_N per account
./setup.sh
```

`.env` is the single source of truth: the number of account pairs in it decides how many backend instances get built. `setup.sh` does the rest — generates the config and compose file, builds the images, logs each account in once (the session persists in a volume, so you never log in again), and starts everything. After that, pushing to `main` redeploys via GitHub Actions; `.env` stays untouched on the host.

The catalog and parallel-upload pool are opt-in: add `HELPER_BOT_TOKENS`, `DUMP_CHANNEL_ID`, and `DATABASE_URL` to `.env` to turn them on (channel setup details will live in the docs site).

## Usage

```
/dl <url> [flags]     rip a song, album, playlist, or artist
/profile              set saved prefs so /dl needs no flags
/status               active task + queue
/stop_<id>            cancel a task
@bot <keywords>       inline song search
```

Common flags: `-aac`, `-atmos`, `-flac`, `-art`, plus delivery overrides `-tgu` / `-tgz` / `-go`. Admins also get bans, usage stats, system status, and restart controls.

```
/dl https://music.apple.com/album/123456
/dl https://music.apple.com/album/123456 -atmos -tgz
```

## Layout

```
bot/               Go service — orchestration, delivery, profiles, admin
  catalog/         Postgres read-through catalog (pointer rows) + indexer
  pool.go          helper-bot upload pool → dump channel
  utils/wmgrpc/    gRPC client + parallel download/decrypt across rip backends
wrapper-manager/   upstream emulator backend + patches
setup.sh           one-shot bootstrap: generate → build → login → start
generate.sh        renders config.yaml + docker-compose from .env
```

## Built on the shoulders of

Karen wouldn't exist without the work of others. Standing on, forked from, and inspired by:

- [WorldObservationLog/wrapper-manager](https://github.com/WorldObservationLog/wrapper-manager) and [/wrapper](https://github.com/WorldObservationLog/wrapper) — the hooked Apple Music backend
- [WorldObservationLog/AppleMusicDecrypt](https://github.com/WorldObservationLog/AppleMusicDecrypt) — login + decrypt tooling
- [zhaarey/apple-music-downloader](https://github.com/zhaarey/apple-music-downloader) — the original downloader
- [moeleak/apple-music-downloader-bot](https://github.com/moeleak/apple-music-downloader-bot) — Telegram bot groundwork
- [irisXDR/NEO-WZML](https://github.com/irisXDR/NEO-WZML) — bot UX inspiration

Special thanks to **Akash Mitra**.

## License

GPL-3.0 — see [LICENSE](LICENSE). This is a personal and educational project; respect Apple Music's terms and the rights of artists, and own how you use it.
