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

### 1. Clone and configure

```bash
git clone -b feat/wrapper-manager https://github.com/spideyonmoon/karen.git ~/karen
cd ~/karen
cp bot/config.yaml.example bot/config.yaml
nano bot/config.yaml
```

Fill in the required fields:

```yaml
telegram-bot-token: "YOUR_BOT_TOKEN"
telegram-allowed-chat-ids: [-100123456789]   # your chat/group IDs
storefront: "us"                              # account region (us, jp, etc.)
media-user-token: "YOUR_MEDIA_USER_TOKEN"     # from browser devtools on music.apple.com

# MTProto (optional, for >50MB uploads)
telegram-api-id: 0
telegram-api-hash: ""

gofile-token: ""

# Wrapper-manager addresses — one per instance
wrapper-manager-addrs:
  - "karen-wm-1:8081"
  - "karen-wm-2:8082"
```

### 2. Build wrapper-manager image

```bash
docker compose build wrapper-manager
```

This builds a single shared image (`karen-wrapper-manager:local`) used by all instances. It clones the upstream wrapper-manager, patches the WebPlayback nil-safety bug, and increases the emulator timeout from 5s to 25s.

### 3. Login Apple Music accounts

Each wrapper-manager instance holds exactly one account. Login is a one-time interactive step per account.

**For the first instance:**

```bash
docker compose run --rm wrapper-manager-1 -L "APPLE_ID:PASSWORD"
```

**For the second instance:**

```bash
docker compose run --rm wrapper-manager-2 -L "APPLE_ID:PASSWORD"
```

Wait for `Login successful` or the wrapper ready message, then `Ctrl+C`. The account data persists in the Docker volume (`wm-data-X`).

> [!IMPORTANT]
> Login must go through the wrapper-manager's gRPC interface (the `-L` flag), not the raw wrapper binary. The manager registers the instance in `instances.json` and manages the emulator lifecycle. Raw `wrapper -L` login does NOT persist under the correct volume path.

### 4. Start the stack

```bash
docker compose up -d --build
```

Monitor startup:

```bash
docker compose logs -f bot
```

You should see both wrapper-managers report `Wrapper ready` and the bot print `Telegram bot started. Waiting for updates...`.

---

## Scaling to N accounts

To add more wrapper-manager instances (e.g., 6-8 accounts):

### 1. Add services to `docker-compose.yml`

Each instance needs its own service definition and named volume:

```yaml
services:
  wrapper-manager-1:
    build: ./wrapper-manager
    image: karen-wrapper-manager:local
    privileged: true
    container_name: karen-wm-1
    volumes:
      - wm-data-1:/root/data
    command: ["--host", "0.0.0.0", "--port", "8081", "-debug"]
    restart: unless-stopped

  wrapper-manager-2:
    build: ./wrapper-manager
    image: karen-wrapper-manager:local
    privileged: true
    container_name: karen-wm-2
    volumes:
      - wm-data-2:/root/data
    command: ["--host", "0.0.0.0", "--port", "8082", "-debug"]
    restart: unless-stopped

  wrapper-manager-3:
    build: ./wrapper-manager
    image: karen-wrapper-manager:local
    privileged: true
    container_name: karen-wm-3
    volumes:
      - wm-data-3:/root/data
    command: ["--host", "0.0.0.0", "--port", "8083", "-debug"]
    restart: unless-stopped

  # ... repeat for wrapper-manager-4, 5, 6, etc.

  bot:
    build: ./bot
    container_name: karen-bot
    depends_on:
      - wrapper-manager-1
      - wrapper-manager-2
      - wrapper-manager-3
      # ... list all instances here
    volumes:
      - ./bot/config.yaml:/app/config.yaml
      - ./bot/downloads:/downloads
      - ./bot/telegram-cache.json:/app/telegram-cache.json
    command: ["--bot"]
    restart: unless-stopped

volumes:
  wm-data-1:
  wm-data-2:
  wm-data-3:
  # ... one volume per instance
```

**Key rules:**

- Each instance gets a unique **port** (8081, 8082, 8083, ...)
- Each instance gets a unique **volume** (wm-data-1, wm-data-2, wm-data-3, ...)
- Each instance gets a unique **container name** (karen-wm-1, karen-wm-2, ...)
- All instances share the same `karen-wrapper-manager:local` image
- The bot's `depends_on` must list all instances

### 2. Update `bot/config.yaml`

Add all instance addresses:

```yaml
wrapper-manager-addrs:
  - "karen-wm-1:8081"
  - "karen-wm-2:8082"
  - "karen-wm-3:8083"
  - "karen-wm-4:8084"
  - "karen-wm-5:8085"
  - "karen-wm-6:8086"
  - "karen-wm-7:8087"
  - "karen-wm-8:8088"
```

### 3. Login each account

```bash
for i in 1 2 3 4 5 6 7 8; do
  echo "=== Login wrapper-manager-$i ==="
  docker compose run --rm wrapper-manager-$i -L "APPLE_ID_$i:PASSWORD_$i"
done
```

### 4. Start everything

```bash
docker compose up -d --build
```

The bot's pool automatically discovers and distributes across all configured instances. No code changes needed.

---

## Usage

### Commands

| Command | Description |
|---|---|
| `/dl <url> [flags]` | Download a song, album, or playlist |
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
│   ├── mtproto.go              # MTProto client for large uploads
│   ├── config.yaml.example     # Config template
│   ├── utils/
│   │   ├── wmgrpc/
│   │   │   ├── client.go       # gRPC client: M3U8, WebPlayback, Decrypt, Login
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
├── docker-compose.yml          # Stack: N wrapper-managers + bot
└── SESSION_HISTORY.md          # Development log
```
