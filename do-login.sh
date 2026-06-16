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

# Feed creds on stdin, then stop as soon as the login resolves. We watch the log
# (which `tee` writes live) for the success/failure marker instead of blocking on
# a fixed sleep — the old `sleep 600` held the pipe open for 10 min AFTER
# `Login Success!`, stalling the whole setup. Hard cap ~120s as a fallback; on
# timeout stdin closes and login.py exits on EOF.
: > "$LOG"
{
  echo "$EMAIL"
  echo "$PASS"
  for _ in $(seq 1 120); do
    grep -qE 'Login Success!|Login Failed|Login failed' "$LOG" 2>/dev/null && break
    sleep 1
  done
} | python3 tools/login.py 2>&1 | tee "$LOG"

rm -rf "$WORK"
