#!/bin/bash
# Usage: do-login.sh <wm-n> <email> <password>
# Logs to <karen>/.logins/login-wm-n.log

set -u
WM=$1
EMAIL=$2
PASS=$3
KAREN=~/karen
AMD=/tmp/AppleMusicDecrypt
WORK=$(mktemp -d)
LOG="$KAREN/.logins/login-$WM.log"

cp "$KAREN/.logins/$WM.toml" "$WORK/config.toml"
ln -s "$AMD/src" "$WORK/src"
ln -s "$AMD/tools" "$WORK/tools"
ln -s "$AMD/assets" "$WORK/assets" 2>/dev/null || true

# If AMD has pyproject.toml-derived things, also link
ln -s "$AMD/main.py" "$WORK/main.py" 2>/dev/null || true

cd "$WORK"

# Feed creds on stdin; if 2FA comes, the script will block on `input("2FA code: ")`
# We don't know 2FA upfront, so just feed creds and let it block if needed.
{
  echo "$EMAIL"
  echo "$PASS"
  # No 2FA pre-fed. If prompt comes, log will show it and we can relay code later.
  sleep 600
} | python3 tools/login.py 2>&1 | tee "$LOG"

rm -rf "$WORK"
