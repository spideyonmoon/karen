# Karen

Karen pulls lossless audio off Apple Music and delivers it to your Telegram chat — ALAC, Dolby Atmos, or AAC, with cover art and metadata baked in. Under the hood she's a **read-through catalog**: anything ripped once is stored in a private Telegram channel and copied back instantly the next time anyone asks. Popular music gets ripped exactly once and delivered forever.

A docs site is on the way. Until then this readme is the short version — enough to understand it and get it running.

## What she does

- **Lossless and then some** — ALAC up to 192 kHz, Dolby Atmos, AAC-LC, optional FLAC conversion. Music videos too.
- **Ripped once, served forever** — a catalog backed by a private Telegram dump channel. The first request for a track rips it; every request after that copies the stored file straight back, no re-ripping. A half-cached album only fetches the tracks it's missing.
- **Parallel everything** — wrapper-manager routes work across multiple Apple Music accounts while Temari decrypts locally across CPU cores, and a pool of helper bot accounts uploads in parallel.
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
                     │ wrapper-mgr v2 │  │  helper-bot pool   │
                     │ HTTP gateway   │  │  → dump channel    │
                     │ many accounts  │  │  (stores the bytes)│
                     └────────────────┘  └────────────────────┘
```

- **wrapper-manager v2** — one HTTP gateway supervising lightweight `wrapper-lite` processes, one per Apple account. It supplies playlists, licenses, lyrics, and FairPlay key context and routes around unavailable regions/accounts.
- **helper-bot pool** — extra bot accounts that upload to the dump channel in parallel, dividing both wall-time and FLOOD_WAIT pressure across accounts.
- **catalog** — Postgres (managed on Supabase) holding pointer rows keyed by Apple track ID + format tier. This is the lookup that turns repeat requests into instant copies.
- **bot** (Go) — fetches playlists over HTTP, downloads HLS segments in parallel, decrypts them locally through Temari's Go binding, remuxes with ffmpeg, and copies the result to you with no trace of the dump.

The catalog and helper pool are optional. With neither configured, Karen falls back to the original behavior: rip on demand and upload directly.

## Quick start

You'll need a Linux host with Docker, a Telegram bot token, and at least one Apple Music account. Two-factor login is supported interactively during setup.

```bash
git clone https://github.com/spideyonmoon/karen.git ~/karen
cd ~/karen
cp .env.example .env   # fill in tokens + one APPLE_ID_N / APPLE_PASS_N per account
./setup.sh
```

`.env` is the single source of truth. `setup.sh` generates config, builds one multi-account manager and the Temari-enabled bot, reconciles the account list, logs in new accounts, and starts everything. Sessions persist in the `wm-data-v3` volume. Removed accounts are logged out on the next setup run; `RELOGIN=1 ./setup.sh` refreshes every current account.

The v3 migration intentionally uses a new volume. Old `wm-data-N` volumes are not deleted, so the previous backend remains recoverable until you deliberately remove those volumes.

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
  utils/wmclient/  wrapper-manager HTTP client + local Temari decryption
wrapper-manager/   pinned upstream v2 HTTP gateway image
setup.sh           one-shot bootstrap: generate → build → login → start
generate.sh        renders config.yaml + docker-compose from .env
```

## Built on the shoulders of

Karen wouldn't exist without the work of others. Standing on, forked from, and inspired by:

- [WorldObservationLog/wrapper-manager](https://github.com/WorldObservationLog/wrapper-manager) and [/wrapper](https://github.com/WorldObservationLog/wrapper) — the hooked Apple Music backend
- [WorldObservationLog/Temari](https://github.com/WorldObservationLog/Temari) — local FairPlay decryption library and Go binding
- [WorldObservationLog/AppleMusicDecrypt](https://github.com/WorldObservationLog/AppleMusicDecrypt) — reference implementation for the modern HTTP/Temari architecture
- [zhaarey/apple-music-downloader](https://github.com/zhaarey/apple-music-downloader) — the original downloader
- [moeleak/apple-music-downloader-bot](https://github.com/moeleak/apple-music-downloader-bot) — Telegram bot groundwork
- [irisXDR/NEO-WZML](https://github.com/irisXDR/NEO-WZML) — bot UX inspiration

Special thanks to **Akash Mitra**.

## License

GPL-3.0 — see [LICENSE](LICENSE). This is a personal and educational project; respect Apple Music's terms and the rights of artists, and own how you use it.
