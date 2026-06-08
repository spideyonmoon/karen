# 🎵 Karen - Apple Music Telegram Bot

Karen is a high-performance, robust Telegram bot built in Go. It acts as a bridge to directly rip music from Apple Music in lossless ALAC/FLAC or AAC quality, embed all metadata and cover arts, and deliver the files directly to your Telegram chat. 

When Telegram's native file-size limits hit, Karen flexes her muscles: she leverages MTProto to upload files up to 2GB or seamlessly falls back to Gofile for delivery.

---

## ✨ Features

- **Pristine Quality:** Rips lossless ALAC, FLAC, and AAC directly from Apple Music.
- **Music Videos:** Full support for downloading and muxing Music Videos.
- **Smart Delivery:** 
  - **Telegram Bot API:** Fast delivery for individual tracks under 50MB.
  - **MTProto Integration:** Bypasses Telegram limits to deliver entire ZIPs (up to 2GB) directly in chat.
  - **Gofile Integration:** Automatically uploads and provides high-speed download links for oversized packages or when MTProto isn't ready.
- **Dynamic Status Board:** Beautiful, live-updating ASCII status board showing download progress, transfer modes, and queue status.
- **Instant Cancellation:** Powerful `/cancel_<id>` command instantly severs HTTP connections and kills background ffmpeg processes mid-download.
- **Fully Dockerized:** Easy deployment with Docker Compose, running seamlessly alongside the decryption wrapper.

---

## 🛠 Prerequisites

Before starting, ensure you have:
1. A Linux server/VPS (Ubuntu/Debian recommended).
2. [Docker](https://docs.docker.com/get-docker/) & [Docker Compose](https://docs.docker.com/compose/install/) installed.
3. A **Telegram Bot Token** (from [@BotFather](https://t.me/BotFather)).
4. An **Apple ID** with an active Apple Music subscription.
5. (Optional) A **Telegram MTProto Session** if you want to deliver files >50MB natively in Telegram.
6. (Optional) A **Gofile API Token** to avoid guest upload limits.

---

## 🚀 Installation & Deployment

Karen is deployed in two parts: the **Decryption Wrapper** (which handles Apple Music API auth and DRM) and the **Bot Core**.

> [!WARNING]
> The wrapper is an upstream dependency and is **not maintained in this repository**. If a new Apple Music update rolls out or the wrapper breaks, you should pull the latest version directly from the [WorldObservationLog/wrapper](https://github.com/WorldObservationLog/wrapper/) repository.

### 1. Clone the Repository
```bash
git clone https://github.com/spideyonmoon/karen.git ~/karen
cd ~/karen
```

### 2. Configure the Bot
Configure your bot's settings by editing the config file:
```bash
mkdir -p bot
nano bot/config.yaml
```

Fill in the critical details:
```yaml
telegram-bot-token: "YOUR_BOT_TOKEN_HERE"
telegram-allowed-chat-ids: [-100123456789] # Your private chat/group IDs

# Optional: Gofile Token for oversized files
gofile-token: "YOUR_GOFILE_TOKEN_HERE"

# Keep these network settings so the bot finds the wrapper via Docker
decrypt-m3u8-port: "wrapper:10020"
get-m3u8-port: "wrapper:20020"
```

> [!NOTE]
> To configure MTProto, you will also need to provide your `telegram-api-id` and `telegram-api-hash` (which can be obtained from [my.telegram.org](https://my.telegram.org)). Run the bot locally once to generate a session string, which is then placed in your config. No OTP or extra verification is needed for this workflow!

### 3. Initialize the Apple Music Wrapper
Before launching the background services, the wrapper must be authenticated with your Apple ID. This is a one-time interactive step.

First, build the wrapper image:
```bash
sudo docker compose build wrapper
```

Next, run the interactive login:
> [!IMPORTANT]  
> Replace `YOUR_APPLE_ID:YOUR_PASSWORD` with your actual Apple credentials.
```bash
sudo docker run --privileged --rm -it \
  -v ./wrapper/wrapper-data:/app/rootfs/data \
  --entrypoint /app/wrapper \
  karen-wrapper:local -L "YOUR_APPLE_ID:YOUR_PASSWORD" -H 0.0.0.0
```
*Wait for the "Login successful" message, then press `Ctrl+C` to exit. Your session is now saved in the `wrapper/wrapper-data` volume.*

### 4. Fire It Up!
With the configuration saved and the wrapper authenticated, launch the full stack in the background:

```bash
sudo docker compose up -d --build
```

You can monitor the bot's logs to ensure everything started smoothly:
```bash
sudo docker compose logs -f bot
```

---

## 🎮 Usage

Simply send an Apple Music link (album, playlist, or individual track) to the bot in Telegram. 

- `/dl <apple_music_url>` - Initiates the download. The bot will present an inline keyboard to choose your preferred delivery method (Individual Tracks, Telegram ZIP, or Gofile ZIP).
- `/status` or `/queue` - Displays the live ASCII status board showing the active task, elapsed time, and any pending items in the queue.
- `/cancel_<task_id>` - Instantly aborts a running or queued download.

---
