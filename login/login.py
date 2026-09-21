#!/usr/bin/env python3
import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request


def load_env(path):
    values = {}
    with open(path, "r", encoding="utf-8") as handle:
        for raw in handle:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            value = value.strip()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
                value = value[1:-1]
            values[key.strip()] = value
    return values


def post(base_url, path, payload):
    body = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        base_url.rstrip("/") + path,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=180) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code}: {detail}") from exc


def status(base_url):
    request = urllib.request.Request(base_url.rstrip("/") + "/status")
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.loads(response.read().decode("utf-8"))


def logout(base_url, username, quiet=False):
    result = post(base_url, "/logout", {"username": username})
    if result.get("code") == 0 or "no such account" in result.get("msg", "").lower():
        if not quiet:
            print(f"Logged out {username}")
        return
    raise RuntimeError(result.get("msg") or "logout failed")


def login(base_url, account_index):
    values = load_env("/run/secrets/karen.env")
    username = values.get(f"APPLE_ID_{account_index}", "")
    password = values.get(f"APPLE_PASS_{account_index}", "")
    if not username or not password:
        raise RuntimeError(f"missing APPLE_ID_{account_index}/APPLE_PASS_{account_index}")

    if os.environ.get("RELOGIN", "0") == "1":
        logout(base_url, username, quiet=True)

    result = post(base_url, "/login", {"username": username, "password": password})
    message = result.get("msg", "")
    if result.get("code") == 0 or "already login" in message.lower():
        print(f"Account ready: {username}")
        return
    if result.get("code") != 2:
        raise RuntimeError(message or "login failed")

    code = input(f"Two-factor code for {username}: ").strip()
    if not code:
        raise RuntimeError("2FA code is required")
    result = post(base_url, "/login", {
        "username": username,
        "password": password,
        "code": code,
    })
    if result.get("code") != 0:
        raise RuntimeError(result.get("msg") or "2FA login failed")
    print(f"Account ready: {username}")


def wait_ready(base_url):
    deadline = time.monotonic() + 300
    last_error = None
    while time.monotonic() < deadline:
        try:
            result = status(base_url)
            if result.get("code") == 0:
                print("wrapper-manager HTTP endpoint is ready")
                return
        except Exception as exc:
            last_error = exc
        time.sleep(2)
    raise RuntimeError(f"wrapper-manager did not start within 5 minutes: {last_error}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("wait", "login", "logout"))
    parser.add_argument("base_url")
    parser.add_argument("target", nargs="?")
    args = parser.parse_args()
    if args.action == "wait":
        wait_ready(args.base_url)
    elif args.action == "login":
        if args.target is None:
            raise RuntimeError("account index is required")
        login(args.base_url, int(args.target))
    else:
        if not args.target:
            raise RuntimeError("username is required")
        logout(args.base_url, args.target)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
