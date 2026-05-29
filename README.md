# Karen Apple Music Bot

This is a high-performance Apple Music Telegram bot built in Go. It rips music directly from Apple Music in lossless ALAC quality and uploads it directly to Gofile to bypass Telegram's file size limits.

## Prerequisites
- Docker and Docker Compose
- A Telegram Bot Token
- An Apple ID with an active Apple Music subscription

## 1. Setting up the Wrapper
First, we need to set up the wrapper which handles the Apple Music API communication and decryption.

```bash
# Create directory and navigate into it
mkdir -p ~/karen/wrapper && cd ~/karen/wrapper

# Download the latest wrapper release
wget https://github.com/WorldObservationLog/wrapper/releases/download/wrapper.x86_64.latest/Wrapper.x86_64.latest.zip

# Unzip and clean up
unzip Wrapper.x86_64.latest.zip
rm Wrapper.x86_64.latest.zip

# Make binaries executable
chmod +x wrapper entrypoint.sh

# Go back to the main directory
cd ..

# Build the wrapper Docker image
sudo docker build -t karen-wrapper:local -f wrapper/Dockerfile ./wrapper
```

### Initial Login
Run the wrapper once interactively to log in to your Apple account. Replace `your_apple_id:your_password` with your actual credentials.

```bash
sudo docker run --privileged --rm -it \
 -v "$PWD/wrapper/wrapper-data:/app/rootfs/data" \
 --entrypoint ./wrapper \
 karen-wrapper:local -L "your_apple_id:your_password" -H 0.0.0.0
```
*Press `Ctrl+C` to exit this session once the login is successful.*

## 2. Configuring the Bot
Before launching, you need to configure the bot's settings. Open `bot/config.yaml` and fill in the following critical details:

1. **Telegram Token & Permissions**:
   ```yaml
   telegram-bot-token: "YOUR_BOT_TOKEN_HERE"
   telegram-allowed-chat-ids: [-100123456789] # Your private group ID
   ```

2. **Gofile Token** (Optional but recommended to avoid guest limits):
   ```yaml
   gofile-token: "YOUR_GOFILE_TOKEN_HERE"
   ```

3. **Wrapper Network Settings**:
   Ensure the bot is pointing to the internal wrapper hostname for decryption:
   ```yaml
   decrypt-m3u8-port: "wrapper:10020"
   get-m3u8-port: "wrapper:20020"
   ```

## 3. Launching
Once the wrapper is set up and the bot is configured, launch everything in the background using Docker Compose:

```bash
sudo docker-compose up -d --build
```
