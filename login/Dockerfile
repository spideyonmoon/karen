# Apple Music account login client, containerized so the HOST needs no Python.
#
# Before: setup.sh cloned AppleMusicDecrypt to /tmp and ran tools/login.py with
# the host's python3 — which forced host assumptions that broke on a fresh VPS:
# Python >=3.11 (for the stdlib `tomllib`), a pip new enough for
# `--break-system-packages`, and ~13 pip packages installed on the host. Baking
# it into a pinned python:3.11 image removes ALL of that — the only host
# requirement becomes Docker.
#
# The container is ephemeral (run with --rm by do-login.sh): it connects to a
# wrapper-manager's gRPC endpoint (address read from a mounted /app/config.toml),
# is fed Apple credentials on stdin, and the resulting token is stored by the
# wrapper-manager in ITS OWN volume over gRPC — nothing needs to persist here.
FROM python:3.11-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends git ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# AMD root becomes the working dir, exactly mirroring the old host layout where
# do-login.sh ran `python3 tools/login.py` from a dir containing src/ tools/
# assets/ main.py. tools/login.py reads ./config.toml from this working dir.
WORKDIR /app
RUN git clone --depth 1 https://github.com/WorldObservationLog/AppleMusicDecrypt.git .

# Pinned deps (see requirements.txt) instead of the old unpinned host install.
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# config.toml (the per-instance wm-N.toml) is bind-mounted at runtime to
# /app/config.toml; Apple credentials arrive on stdin.
ENTRYPOINT ["python", "tools/login.py"]
