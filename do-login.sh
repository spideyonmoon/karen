#!/bin/bash
# Usage: do-login.sh wait | login <account-index> | logout <username>
# The helper is containerized; the host still needs only Docker. Login reads
# credentials directly from the read-only .env mount so passwords never appear
# in argv or generated Compose files. It prompts on stdin only when Apple asks
# for a 2FA code.
set -euo pipefail
ACTION=${1:-}
TARGET=${2:-}
KAREN="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MANAGER_URL="http://karen-wrapper-manager:8080"

# Discover the actual Docker network the wrapper container sits on, rather than
# guessing "<project>_default" (compose derives it from the dir name). Fall back
# to karen_default if inspection fails.
NET="$(docker inspect "karen-wrapper-manager" --format '{{range $k,$_ := .NetworkSettings.Networks}}{{$k}}{{end}}' 2>/dev/null)"
[ -z "$NET" ] && NET="karen_default"

case "$ACTION" in
  wait)
    docker run --rm --network "$NET" karen-login:local wait "$MANAGER_URL"
    ;;
  login)
    [[ "$TARGET" =~ ^[0-9]+$ ]] || { echo "account index required" >&2; exit 2; }
    docker run --rm -i --network "$NET" \
      -e RELOGIN="${RELOGIN:-0}" \
      -v "$KAREN/.env:/run/secrets/karen.env:ro" \
      karen-login:local login "$MANAGER_URL" "$TARGET"
    ;;
  logout)
    [[ -n "$TARGET" ]] || { echo "username required" >&2; exit 2; }
    docker run --rm --network "$NET" karen-login:local logout "$MANAGER_URL" "$TARGET"
    ;;
  *)
    echo "Usage: $0 wait | login <account-index> | logout <username>" >&2
    exit 2
    ;;
esac
