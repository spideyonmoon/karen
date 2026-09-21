#!/bin/bash
# Bootstrap for wrapper-manager v2. One HTTP gateway owns all Apple accounts;
# Temari decrypts locally inside the Go bot. Re-run after changing the account
# list. Login is idempotent and supports an interactive 2FA prompt when needed.
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

# 2. Build the tiny stdlib-only HTTP login helper. The host needs no Python.
echo "Building login client image (karen-login:local) ..."
docker build -t karen-login:local ./login

# Compose must NOT auto-read our secrets .env for ${VAR} interpolation — a
# password containing $ triggers "variable is not set" warnings and needless
# parsing of secrets. Our compose files use literal values only.
DC=(docker compose --env-file /dev/null)

# 3. Build and boot the one multi-account manager. --remove-orphans removes the
# old karen-wm-N containers during the v3 migration but deliberately preserves
# their named volumes for rollback.
"${DC[@]}" build wrapper-manager
"${DC[@]}" up -d --remove-orphans wrapper-manager
./do-login.sh wait

# 4. Reconcile accounts removed from .env, then idempotently log in every current
# account. The local registry contains usernames only; credentials remain solely
# in .env. RELOGIN=1 logs current accounts out before signing them in again.
mkdir -p .logins
ACCOUNT_REGISTRY=".logins/v3-accounts.txt"
if [[ -f "$ACCOUNT_REGISTRY" ]]; then
  while IFS= read -r old_id || [[ -n "$old_id" ]]; do
    [[ -z "$old_id" ]] && continue
    still_present=0
    for ((i = 1; i <= N; i++)); do
      id_var="APPLE_ID_${i}"
      [[ "${!id_var}" == "$old_id" ]] && still_present=1
    done
    if [[ "$still_present" -eq 0 ]]; then
      echo "=== Logging out removed account: $old_id ==="
      ./do-login.sh logout "$old_id"
    fi
  done < "$ACCOUNT_REGISTRY"
fi

REGISTRY_TMP="${ACCOUNT_REGISTRY}.tmp"
: > "$REGISTRY_TMP"
for ((i = 1; i <= N; i++)); do
  id_var="APPLE_ID_${i}"
  echo "=== Login Apple account $i/$N ==="
  ./do-login.sh login "$i"
  printf '%s\n' "${!id_var}" >> "$REGISTRY_TMP"
done
mv "$REGISTRY_TMP" "$ACCOUNT_REGISTRY"

# 5. Start the bot
"${DC[@]}" up -d --build bot
echo "Setup complete. Tail logs with: docker compose logs -f bot"
