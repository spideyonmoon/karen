#!/bin/bash
# One-time bootstrap for a fresh VPS (and whenever you add/remove accounts).
# Reads .env, generates config + compose override, builds images, logs in every
# Apple account, and starts the bot. After this, GitHub Actions handles deploys;
# re-run this only when the account list in .env changes (new logins needed).
set -euo pipefail

KAREN="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$KAREN"
AMD=/tmp/AppleMusicDecrypt

if [[ ! -f .env ]]; then
  echo "ERROR: .env not found. Copy .env.example to .env and fill it in." >&2
  exit 1
fi

# 1. Generate bot/config.yaml + docker-compose.override.yml from .env
./generate.sh

# Parse .env literally — do NOT `source` it (see note in generate.sh): a password
# containing $, backticks or \ would be expanded/mangled and break under `set -u`.
load_env() {
  local line key val
  while IFS= read -r line || [[ -n "$line" ]]; do
    line="${line%$'\r'}"
    line="${line#"${line%%[![:space:]]*}"}"
    [[ -z "$line" || "$line" == \#* || "$line" != *=* ]] && continue
    key="${line%%=*}"
    val="${line#*=}"
    key="${key%"${key##*[![:space:]]}"}"
    case "$val" in
      \"*\") val="${val#\"}"; val="${val%\"}" ;;
      \'*\') val="${val#\'}"; val="${val%\'}" ;;
    esac
    printf -v "$key" '%s' "$val"
    export "${key?}"
  done < .env
}
load_env

# Recount accounts (same logic as generate.sh) to drive the login loop.
N=0
while :; do
  next=$((N + 1)); id_var="APPLE_ID_${next}"
  [[ -n "${!id_var:-}" ]] && N=$next || break
done

# 2. Ensure the AppleMusicDecrypt login client + Python deps are present
if [[ ! -d "$AMD" ]]; then
  echo "Cloning AppleMusicDecrypt login client to $AMD ..."
  git clone --depth 1 https://github.com/WorldObservationLog/AppleMusicDecrypt.git "$AMD"
  pip3 install --break-system-packages creart grpcio 'protobuf>=6' pydantic \
    prompt-toolkit m3u8 regex beautifulsoup4 lxml tenacity async-lru loguru six
fi

# 3. Build the shared wrapper image and boot all wrapper-managers
docker compose build wrapper-manager-1
docker compose up -d $(for ((i=1;i<=N;i++)); do echo "wrapper-manager-$i"; done)

echo "Waiting 20s for wrapper-manager gRPC servers to come up ..."
sleep 20

# 4. Log in each account (one-time; token persists in wm-data-N volume)
for ((i = 1; i <= N; i++)); do
  port=$((8080 + i))
  id_var="APPLE_ID_${i}"; pass_var="APPLE_PASS_${i}"
  echo "=== Login wrapper-manager-$i (port $port) ==="
  cp .logins/wm-1.toml.example ".logins/wm-${i}.toml"
  sed -i "s|127.0.0.1:8081|127.0.0.1:${port}|" ".logins/wm-${i}.toml"
  ./do-login.sh "wm-${i}" "${!id_var}" "${!pass_var}"
done

# 5. Start the bot
docker compose up -d --build bot
echo "Setup complete. Tail logs with: docker compose logs -f bot"
