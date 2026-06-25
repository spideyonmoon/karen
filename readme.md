# Karen

Karen pulls lossless audio off Apple Music and drops it straight into your Telegram chat — ALAC, Dolby Atmos, or AAC, with cover art and metadata already baked in. She drives a hooked Apple Music app to deal with the DRM, rips the HLS stream in parallel, and works out the smartest way to hand you the result: individual tracks under 50 MB over the Bot API, a zip up to 2 GB over MTProto, or a Gofile link when it's bigger than that.

A docs site is on the way. Until then this readme is the short version — enough to understand it and get it running.

## What she does

- **Lossless and then some** — ALAC up to 192 kHz, Dolby Atmos, AAC-LC, optional FLAC conversion. Music videos too.
- **Many accounts, in parallel** — one Apple Music account per backend instance, with a pool spreading rips and uploads across all of them. Adding an account adds throughput.
- **A catalog that remembers** — anything ripped once is cached in a private Telegram dump and served back instantly next time instead of being re-ripped. Only the missing tracks get fetched.
- **Delivery that fits** — tracks, zip, or Gofile, chosen around Telegram's size limits. Huge discographies are flushed to Gofile in numbered parts mid-rip, so a rip never has to fit on disk all at once.
- **Per-user profiles** — save your codec, quality, and delivery preferences once and `/dl` runs with zero flags and zero prompts.
- **Made to run unattended** — concurrent download scheduler, live per-task status boards, a queue, bulk `/dl`, inline search, and a full admin/sudo toolkit (bans, usage stats, restart, system status).

## How it works

```
┌─────────────┐     ┌──────────────┐     ┌──────────────┐
│  Telegram   │────▶│  Bot (Go)    │────▶│  Wrapper-Mgr │  ← Android emulator
│  User       │◀────│  docker      │◀────│  gRPC :8081  │     + Frida hooks
└─────────────┘     │              │     └──────────────┘
                    │   pool       │     ┌──────────────┐
                    │  spreads     │────▶│  Wrapper-Mgr │  ← second account
                    │  across N    │◀────│  gRPC :8082  │
                    └──────────────┘     └──────────────┘   ...
```

- **wrapper-manager** runs an Android emulator with a Frida-hooked Apple Music app and exposes gRPC for playlists, WebPlayback, and live DRM decryption. One account per instance.
- **bot** (Go) takes your command, fetches the playlist, downloads HLS segments in parallel, decrypts them over a gRPC stream, remuxes with ffmpeg, and delivers.
- **pool** distributes work across every wrapper-manager instance and rotates accounts.
- **catalog** (optional, Postgres) tracks what's already been uploaded so repeat requests are read-through, not re-ripped.

## Quick start

You'll need a Linux host with Docker, a Telegram bot token, and at least one Apple Music account (2FA off — the login path doesn't prompt).

```bash
git clone https://github.com/spideyonmoon/karen.git ~/karen
cd ~/karen
cp .env.example .env   # fill in tokens + one APPLE_ID_N / APPLE_PASS_N per account
./setup.sh
```

`.env` is the single source of truth: the number of account pairs in it decides how many backend instances get built. `setup.sh` handles the rest — it generates the config and compose file, builds the images, logs each account in once (the session persists in a volume, so you never log in again), and starts everything. After that, pushing to `main` redeploys via GitHub Actions; `.env` stays untouched on the host.

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
bot/               Go service — ripping, delivery, catalog, profiles, admin
  utils/wmgrpc/    gRPC client + parallel download/decrypt + account pool
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
