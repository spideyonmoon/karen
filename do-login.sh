#!/bin/bash
# Usage: do-login.sh <wm-n> <email> <password>
# Logs to <karen>/.logins/login-<wm-n>.log
#
# Runs the containerized AppleMusicDecrypt login client (image karen-login:local,
# built by setup.sh from ./login) against wrapper-manager <wm-n>'s gRPC endpoint.
# The host needs only Docker — no Python. The wrapper address is read from the
# mounted .logins/<wm-n>.toml (setup.sh points it at the container DNS name
# karen-<wm-n>:<port> over the compose network); Apple creds are fed on stdin.
set -u
WM=$1
EMAIL=$2
PASS=$3
KAREN="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG="$KAREN/.logins/login-$WM.log"
TOML="$KAREN/.logins/$WM.toml"

# Discover the actual Docker network the wrapper container sits on, rather than
# guessing "<project>_default" (compose derives it from the dir name). Fall back
# to karen_default if inspection fails.
NET="$(docker inspect "karen-$WM" --format '{{range $k,$_ := .NetworkSettings.Networks}}{{$k}}{{end}}' 2>/dev/null)"
[ -z "$NET" ] && NET="karen_default"

# Feed creds on stdin, then watch the live log for the success/failure marker
# instead of blocking on a fixed sleep. Hard cap ~120s as a fallback; on timeout
# stdin closes and login.py exits on EOF.
: > "$LOG"
{
  echo "$EMAIL"
  echo "$PASS"
  for _ in $(seq 1 120); do
    grep -qE 'Login Success!|Login Failed|Login failed' "$LOG" 2>/dev/null && break
    sleep 1
  done
} | docker run --rm -i \
      --network "$NET" \
      -v "$TOML:/app/config.toml:ro" \
      karen-login:local 2>&1 | tee "$LOG"
