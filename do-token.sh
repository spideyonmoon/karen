#!/bin/bash
# Usage: do-token.sh
# Mints an Apple Music media-user-token from the Apple IDs in .env and writes it to
# bot/state/media-user-token, which the bot re-reads on its own (no restart needed).
# Containerized like the login helper: the host needs only Docker, and credentials
# come from the read-only .env mount, never argv. Safe to re-run any time; an
# existing token is only replaced when a new one is obtained.
set -euo pipefail
KAREN="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
mkdir -p "$KAREN/bot/state"
docker build -q -t karen-token:local "$KAREN/token" >/dev/null
docker run --rm \
  -v "$KAREN/.env:/run/secrets/karen.env:ro" \
  -v "$KAREN/bot/state:/out" \
  karen-token:local
