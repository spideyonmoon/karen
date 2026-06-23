#!/bin/bash
# One-time bootstrap for a fresh VPS (and whenever you add/remove accounts).
# Reads .env, generates config + compose override, builds images, logs in every
# Apple account, and starts the bot. After this, GitHub Actions handles deploys;
# re-run this only when the account list in .env changes (new logins needed).
set -euo pipefail

KAREN="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$KAREN"

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

# 2. Build the containerized login client. This replaces the old host-side
#    AppleMusicDecrypt clone + `pip install` — the login client now runs in a
#    pinned python:3.11 image (see login/), so the HOST needs only Docker. No
#    host Python, no pip flags, no Python-version assumptions.
echo "Building login client image (karen-login:local) ..."
docker build -t karen-login:local ./login

# Compose must NOT auto-read our secrets .env for ${VAR} interpolation — a
# password containing $ triggers "variable is not set" warnings and needless
# parsing of secrets. Our compose files use literal values only.
DC=(docker compose --env-file /dev/null)

# 3. Build the shared wrapper image and boot all wrapper-managers
"${DC[@]}" build wrapper-manager-1
"${DC[@]}" up -d $(for ((i=1;i<=N;i++)); do echo "wrapper-manager-$i"; done)

echo "Waiting 20s for wrapper-manager gRPC servers to come up ..."
sleep 20

# 4. Log in each account. Idempotent: an account that already has a token
#    (instances.json in its volume) is skipped, so re-running setup.sh after
#    adding accounts only logs in the NEW ones — re-authing a live account
#    fails. Force a fresh login for all with: RELOGIN=1 ./setup.sh
for ((i = 1; i <= N; i++)); do
  port=$((8080 + i))
  id_var="APPLE_ID_${i}"; pass_var="APPLE_PASS_${i}"
  if [[ "${RELOGIN:-0}" != "1" ]] && \
     "${DC[@]}" exec -T "wrapper-manager-$i" sh -c 'test -s /root/data/instances.json' 2>/dev/null; then
    echo "=== wrapper-manager-$i already logged in — skipping (RELOGIN=1 to force) ==="
    continue
  fi
  echo "=== Login wrapper-manager-$i (port $port) ==="
  cp .logins/wm-1.toml.example ".logins/wm-${i}.toml"
  # Point the login client at the wrapper's container DNS name on the compose
  # network (the login container can't reach the host's 127.0.0.1 ports).
  sed -i "s|127.0.0.1:8081|karen-wm-${i}:${port}|" ".logins/wm-${i}.toml"
  ./do-login.sh "wm-${i}" "${!id_var}" "${!pass_var}"
done

# 5. Start the bot
"${DC[@]}" up -d --build bot
echo "Setup complete. Tail logs with: docker compose logs -f bot"
