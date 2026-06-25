# Karen — Apple Music Telegram Bot

Karen rips lossless ALAC, Atmos, and AAC from Apple Music and delivers files directly to your Telegram chat. She handles DRM decryption, metadata embedding, cover art, and intelligent delivery — sending individual tracks under 50MB via Bot API, zipping and uploading up to 2GB via MTProto, or falling back to Gofile for oversized packages.

---

## Architecture

```
┌─────────────┐     ┌──────────────┐     ┌──────────────┐
│  Telegram    │────▶│  Bot (Go)    │────▶│  Wrapper-Mgr │  ← runs Android emulator
│  User        │◀────│  docker      │◀────│  gRPC :8081  │     with Frida hooks
└─────────────┘     │              │     └──────────────┘
                    │  pool.go     │     ┌──────────────┐
                    │  distributes │────▶│  Wrapper-Mgr │  ← second account
                    │  across N    │◀────│  gRPC :8082  │
                    │  instances   │     └──────────────┘
                    └──────────────┘          ...
```

- **Wrapper-manager** ([upstream](https://github.com/WorldObservationLog/wrapper-manager)) — runs an Android emulator with a hooked Apple Music app. Exposes gRPC endpoints for M3U8 playlists, WebPlayback (AAC-LC), and real-time DRM decryption. Each instance holds one Apple Music account.
- **Bot** — Go binary that receives Telegram commands, fetches playlists via gRPC, downloads HLS segments in parallel, decrypts them through a bidirectional gRPC stream, remuxes with ffmpeg, and delivers files.
- **Pool** — channel-based FIFO pool that distributes load across all wrapper-manager instances. Acquire a client, use it, release it. Automatically rotates accounts.

---

## Prerequisites

1. **Linux VPS** (Ubuntu/Debian recommended) with Docker and Docker Compose
2. **Telegram Bot Token** from [@BotFather](https://t.me/BotFather)
3. **Apple Music accounts** — one account per wrapper-manager instance
4. **(Optional)** Telegram API credentials from [my.telegram.org](https://my.telegram.org) for MTProto uploads >50MB
5. **(Optional)** Gofile API token for fallback delivery

---

## Setup

### 1. Clone and configure `.env`

Everything is driven by a single `.env` file. The number of Apple accounts you put in it decides how many wrapper-manager instances are created — no manual `docker-compose.yml` or `config.yaml` editing.

```bash
git clone -b feat/wrapper-manager https://github.com/spideyonmoon/karen.git ~/karen
cd ~/karen
cp .env.example .env
nano .env
```

Fill in your secrets and one `APPLE_ID_N` / `APPLE_PASS_N` pair per account:

```bash
TELEGRAM_BOT_TOKEN="YOUR_BOT_TOKEN"
TELEGRAM_API_ID=12345
TELEGRAM_API_HASH="YOUR_API_HASH"
TELEGRAM_ALLOWED_CHAT_IDS="-100123456789"   # comma-separated
ADMIN_IDS="111111111"                        # comma-separated, sudo users
MEDIA_USER_TOKEN="YOUR_MEDIA_USER_TOKEN"     # from browser devtools on music.apple.com
GOFILE_TOKEN=""

APPLE_ID_1="apple-id-1@example.com"
APPLE_PASS_1="password1"
APPLE_ID_2="apple-id-2@example.com"
APPLE_PASS_2="password2"
# add APPLE_ID_3 / APPLE_PASS_3 ... to scale up
```

`storefront` is fixed to `us` in the generated config and is intentionally not in `.env`.

### 2. Run the one-time setup

```bash
./setup.sh
```

`setup.sh` is the whole bootstrap. It:

1. Runs `./generate.sh`, which reads `.env` and writes `bot/config.yaml` and `docker-compose.override.yml` (the N wrapper-manager services + volumes). Both are gitignored.
2. Clones the AppleMusicDecrypt login client to `/tmp/AppleMusicDecrypt` and installs its Python deps (first run only).
3. Builds the shared `karen-wrapper-manager:local` image (clones upstream wrapper-manager, patches the WebPlayback nil-safety bug, bumps the emulator timeout 5s → 25s) and boots every wrapper-manager.
4. Logs in each Apple account once via the gRPC `Login` RPC (see below). The token persists in that instance's `wm-data-N` volume and is reused on every subsequent start, so you never log in again unless the volume is lost.
5. Builds and starts the bot.

> [!NOTE]
> The accounts used here are assumed to have 2FA disabled. The login path does not handle an interactive 2FA prompt.

<details>
<summary>How login actually works (for reference)</summary>

Each wrapper-manager instance holds exactly one account. The wrapper-manager exposes a bidirectional gRPC `Login` RPC on its configured port. The client sends `{username, password}`; the wrapper-manager launches the Apple Music app in its Android emulator, types the credentials, taps Sign In, and replies with the result. There is no CLI flag for it — login is exclusively via gRPC, driven from the host by AppleMusicDecrypt's `tools/login.py` (wrapped by `do-login.sh`). `setup.sh` calls `do-login.sh` for each account automatically; you normally never run it by hand.</details>

Monitor startup:

```bash
docker compose logs -f bot
```

You should see each wrapper-manager report `Wrapper ready` and the bot print `Telegram bot started. Waiting for updates...`.

> [!NOTE]
> The upstream `wrapper-manager` binary has no login CLI flag (the old `-L "id:pass"` shortcut was removed and now errors with `flag provided but not defined: -L`). Login is exclusively via the gRPC `Login` RPC, which `setup.sh` drives for you through `do-login.sh`.

---

## Deploys and scaling

**Deploys are automatic.** Once `.env` exists on the VPS and the GitHub Actions SSH secrets are set, every push to `main` redeploys: the workflow pulls, re-runs `./generate.sh` (so config/template changes land), and rebuilds the bot. `.env` and the generated `docker-compose.override.yml` are gitignored, so they persist untouched across deploys. You never edit files on the VPS for a normal code change.

**To add or remove accounts**, edit the `APPLE_ID_N` / `APPLE_PASS_N` pairs in `.env` and re-run:

```bash
./setup.sh
```

The account count drives everything — `generate.sh` rewrites `config.yaml` (the `wrapper-manager-addrs` list) and `docker-compose.override.yml` (the wrapper services + volumes), and `setup.sh` logs in any new accounts. Already-logged-in accounts keep their token in their `wm-data-N` volume and are not touched.

**Key invariants** (handled automatically, listed for reference):

- Instance `N` uses port `8080+N`, container `karen-wm-N`, volume `wm-data-N`, address `karen-wm-N:808N`.
- All instances share the `karen-wrapper-manager:local` image.
- gRPC ports bind to `127.0.0.1` only — never expose them publicly.

---

## Data & backups

All persistent state lives in `bot/state/` (a directory bind mount, gitignored): the Telegram `file_id` cache, per-user `/profile` preferences, and the sleeptime job schedule. Saves are atomic.

Because a VPS can vanish, the bot DMs every admin (`ADMIN_IDS`) a copy of the profile data (`karen-state-YYYY-MM-DD.json`) once a day at **04:00 Dhaka time**. Worst-case loss on a total VPS wipe is one day of profile changes — restore by dropping the file back into `bot/state/`. The job schedule is intentionally not backed up.

---

## Usage

### Commands

| Command | Description |
|---|---|
| `/dl <url> [flags]` | Download a song, album, or playlist |
| `/profile` | Set saved rip preferences (codec, quality, lyrics, cover, delivery target…) so `/dl` runs with zero flags and zero prompts. Fully button-driven UI; prefs persist per user. |
| `/status` or `/queue` | Show active task and queue count |
| `/stop_<task_id>` | Cancel a running or queued download |
| `/help` | Show help message |

### Flags (appended to `/dl`)

| Flag | Description |
|---|---|
| `-aac` | Download in AAC-LC format |
| `-atmos` | Download in Dolby Atmos format |
| `-flac` | Convert to FLAC after download (requires ffmpeg) |
| `-art` | Save artist cover art |
| `-tgu` | Skip delivery keyboard — send as individual Telegram tracks |
| `-tgz` | Skip delivery keyboard — send as Telegram ZIP |
| `-go` | Skip delivery keyboard — send as Gofile ZIP |

**Examples:**
```
/dl https://music.apple.com/album/123456
/dl https://music.apple.com/album/123456 -aac
/dl https://music.apple.com/song/789012 -atmos -tgz
```

### Delivery modes

After `/dl`, the bot presents an inline keyboard (unless a headless flag is used):

- **Individual Tracks** — sends each file separately via Bot API (<50MB each)
- **Telegram ZIP** — MTProto upload up to 2GB
- **Gofile ZIP** — fallback for oversized packages

Very large rips (full discographies, hundred-track playlists) are flushed in parts:
once the files on disk pass `rip-flush-threshold-gb` (default 20GB) mid-rip, that
chunk is zipped, uploaded to Gofile as a numbered `… (Part N).zip` link, and deleted
to free disk before the rip continues — so the bot never has to fit the whole rip on
the VPS at once.

### Inline search

```
@bot <keywords> — search songs directly in any chat
```

---

## Config reference

| Field                       | Description                                                  |
| --------------------------- | ------------------------------------------------------------ |
| `telegram-bot-token`        | Bot token from @BotFather                                    |
| `telegram-allowed-chat-ids` | Chat ID allowlist (e.g. `[-100123456789]`)                   |
| `storefront`                | Account region code (`us`, `jp`, `gb`, etc.)                 |
| `media-user-token`          | Apple Music API token from browser devtools                  |
| `wrapper-manager-addrs`     | List of `host:port` for each wrapper-manager instance        |
| `alac-max`                  | Max ALAC sample rate (`192000`, `96000`, `48000`, `44100`)   |
| `atmos-max`                 | Max Atmos bitrate (`2768`, `2448`)                           |
| `aac-type`                  | AAC variant (`aac-lc`, `aac`, `aac-binaural`, `aac-downmix`) |
| `convert-after-download`    | Post-download format conversion (requires ffmpeg)            |
| `convert-format`            | Target format (`flac`, `mp3`, `opus`, `wav`, `copy`)         |
| `telegram-api-id`           | MTProto API ID from my.telegram.org                          |
| `telegram-api-hash`         | MTProto API hash from my.telegram.org                        |
| `gofile-token`              | Gofile API token for fallback delivery                       |
| `rip-flush-threshold-gb`    | Mid-rip disk cap (GB): once exceeded, the chunk is zipped to Gofile and deleted; continues in parts. `0`=default 20, negative=disable |
| `song-file-format`          | Output filename template (e.g. `"{SongNumer}. {SongName}"`)  |
| `album-folder-format`       | Album folder name template                                   |

---

## Troubleshooting

**Wrapper-manager fails to start:**

- Ensure `--privileged` is set (required for Frida hooks in the Android emulator)
- Check `docker compose logs wrapper-manager-1` for emulator errors
- If the emulator is stuck, restart the container: `docker compose restart wrapper-manager-1`

**"The item you tried to download is no longer available":**

- The wrapper-manager's emulator may be in a stale state. Restart the affected instance:
  
  ```bash
  docker compose restart wrapper-manager-1
  ```
- Verify the account has an active Apple Music subscription
- Some tracks may be region-restricted — check your account's storefront

**Tracks are preview clips (15-30 seconds):**

- The bot may be falling back to the public catalog API instead of the authenticated M3U8 endpoint
- Ensure `wrapper-manager-addrs` is configured and instances are running
- Check that `needDlAacLc` is `false` in logs (should use authenticated M3U8 for ALAC)

**Bot can't connect to wrapper-manager:**

- Verify containers are on the same Docker network
- Use Docker service names in config (e.g. `karen-wm-1:8081`), not `localhost`
- Check `docker compose ps` to confirm all services are running

**MTProto FLOOD_WAIT errors:**

- Normal behavior — the bot automatically sleeps and retries
- For persistent issues, check your API credentials at my.telegram.org

---

## Project structure

```
karen/
├── bot/                        # Go binary
│   ├── main.go                 # Core: ripTrack, extractMedia, album/playlist flows
│   ├── telegram_bot.go         # Telegram bot handler, download queue, delivery
│   ├── profile.go              # /profile saved-prefs rich UI + pf:* callbacks
│   ├── admin_tasks.go          # State persistence, sleeptime scheduler, daily backup
│   ├── ripstate.go             # Per-rip Config overlay (ripConfig) from user prefs
│   ├── mtproto.go              # MTProto client for large uploads
│   ├── config.yaml.example     # Config template
│   ├── utils/
│   │   ├── wmgrpc/
│   │   │   ├── client.go       # gRPC client: M3U8, WebPlayback, Decrypt
│   │   │   ├── decrypt.go      # HLS download + decrypt pipeline
│   │   │   └── pool.go         # Channel-based FIFO client pool
│   │   ├── structs/            # Shared data structures
│   │   ├── task/               # Album/playlist/track task models
│   │   ├── ampapi/             # Apple Music catalog API
│   │   └── gofile.go           # Gofile upload client
│   └── Dockerfile              # Multi-stage: Go build + GPAC + ffmpeg
├── wrapper-manager/
│   ├── Dockerfile              # Builds upstream wrapper-manager + patches
│   └── webplay.go              # Nil-safety patch for WebPlayback handler
├── bot/state/                  # Persisted JSON: cache, user profiles, schedule (dir mount, gitignored)
├── .env.example                # Single source of truth — copy to .env, fill in
├── setup.sh                    # One-time bootstrap: generate + login + start
├── generate.sh                 # Writes config.yaml + docker-compose.override.yml from .env
├── do-login.sh                 # Wrapper around AppleMusicDecrypt/tools/login.py
├── .logins/                    # Per-instance AppleMusicDecrypt configs (host-only)
├── docker-compose.yml          # Base: bot service only
├── docker-compose.override.yml # Generated: N wrapper-managers + volumes (gitignored)
└── SESSION_HISTORY.md          # Development log
```

## License

Licensed under the GNU General Public License v3.0 — see [LICENSE](LICENSE).
