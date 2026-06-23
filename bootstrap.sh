#!/bin/bash
# One-time HOST provisioning for a fresh VPS. Idempotent — safe to re-run.
#
# Installs the only two things the host needs: Docker (engine + compose plugin)
# and git. Everything else — the bot, the wrapper-managers, AND the Apple-account
# login client — runs inside containers, so the host needs NO Python, no pip, no
# language runtimes. That's deliberate: it's what makes a migration to any fresh
# box painless regardless of its distro/Python version.
#
# After this:  cp .env.example .env && nano .env   →   ./setup.sh
set -euo pipefail

# Use sudo only when not already root.
SUDO=""
if [[ "$(id -u)" -ne 0 ]]; then
  SUDO="sudo"
fi

# --- Docker (engine + compose plugin) ---
if command -v docker >/dev/null 2>&1; then
  echo "Docker already installed: $(docker --version)"
else
  echo "Installing Docker via get.docker.com ..."
  curl -fsSL https://get.docker.com | $SUDO sh
fi

# Make sure the daemon is up and starts on boot (no-op if already enabled).
$SUDO systemctl enable --now docker 2>/dev/null || true

# --- git (usually preinstalled) ---
if ! command -v git >/dev/null 2>&1; then
  echo "Installing git ..."
  $SUDO apt-get update && $SUDO apt-get install -y git
fi

echo
echo "Host ready. Versions:"
echo "  $(docker --version)"
echo "  $(docker compose version 2>/dev/null | head -1)"
echo "  $(git --version)"
echo
echo "Next steps:"
echo "  1. cp .env.example .env && nano .env      # fill in secrets + Apple accounts"
echo "  2. (optional) drop your latest karen-state-*.json into bot/state/ to restore profiles/donors/bans"
echo "  3. ./setup.sh                              # builds images, logs in accounts, starts the bot"
