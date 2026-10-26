#!/usr/bin/env python3
"""Mint an Apple Music media-user-token from the Apple IDs already in .env.

Signs in through Apple's web login (SRP) for APPLE_ID_n / APPLE_PASS_n, tries the
accounts in order, and writes the first token that works to <out>/media-user-token
(0600, tmp+rename). Secrets never reach argv or stdout: only "account n: ok
(token length L)" or a failure reason is printed.

One attempt per account, no retries, so a wrong password can't turn into a lockout.
An account that asks for a 2FA code is skipped (this helper is non-interactive).
Exit status is 0 when a token was written, 1 otherwise; an existing token file is
never touched on failure.
"""
import argparse
import base64
import hashlib
import os
import re
import sys
from uuid import uuid4

import pbkdf2
import requests
from Cryptodome.Hash import SHA256
import srp._pysrp as srp

# Apple's s2k flavour of SRP: RFC 5054 padding, and the username is left out of x.
srp.rfc5054_enable()
srp.no_username_in_x()

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
AUTH_URL = "https://idmsa.apple.com/appleauth/"
CLIENT_ID = "06f8d74b71c73757a2f82158d5e948ae7bae11ec45fda9a58690f55e35945c51"
TIMEOUT = 30


class TwoFactorRequired(Exception):
    pass


def b64enc(data):
    return base64.b64encode(data).decode()


def b64dec(data):
    return base64.b64decode(data)


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


def service_error(response):
    try:
        errors = response.json().get("serviceErrors") or [{}]
        return errors[0].get("message") or response.text[:200]
    except ValueError:
        return response.text[:200]


class AppleMusicAuth:
    def __init__(self):
        self.s = requests.Session()
        self.user_agent = UA
        self.language = "en-US"
        self.dev_token = None

    def headers(self):
        return {
            "User-Agent": UA,
            "Origin": "https://music.apple.com",
            "Referer": "https://music.apple.com/",
            "Accept": "application/json",
        }

    def music_headers(self):
        headers = self.headers()
        headers["Authorization"] = "Bearer " + self.fetch_dev_token()
        headers["Content-Type"] = "application/json"
        return headers

    def fetch_dev_token(self):
        """Apple Music's public web developer token, scraped like ampapi.GetToken."""
        if self.dev_token:
            return self.dev_token
        page = self.s.get("https://music.apple.com", headers={"User-Agent": UA}, timeout=TIMEOUT)
        match = re.search(r"/assets/index[-~.][^\"' <>]+\.js", page.text)
        if not match:
            raise Exception("could not find the music.apple.com JS bundle")
        bundle = self.s.get("https://music.apple.com" + match.group(0),
                            headers={"User-Agent": UA}, timeout=TIMEOUT)
        token = re.search(r"eyJ[^\"'`]+", bundle.text)
        if not token:
            raise Exception("could not extract the developer token")
        self.dev_token = token.group(0)
        return self.dev_token

    def login(self, email, password):
        frame_id = "auth-" + str(uuid4()).lower()

        # 1. Initialise sign-in and collect the base cookies.
        r = self.s.get(AUTH_URL + "auth/authorize/signin", headers=self.headers(), timeout=TIMEOUT, params={
            "frame_id": frame_id, "language": "en_us", "skVersion": "7", "iframeId": frame_id,
            "client_id": CLIENT_ID, "redirect_uri": "https://music.apple.com", "response_type": "code",
            "response_mode": "web_message", "account_ind": "1", "state": frame_id, "authVersion": "latest",
        })
        if r.status_code != 200:
            raise Exception("auth init failed: HTTP %d" % r.status_code)
        auth_attributes = r.headers.get("X-Apple-Auth-Attributes", "")

        # 2. Mimic the page's performance log; Apple sets the 'aa' cookie on it.
        self.s.post(AUTH_URL + "jslog", headers=self.headers(), timeout=TIMEOUT, json={
            "type": "INFO", "title": "AppleAuthPerf-s-y",
            "message": 'APPLE ID : TTI {"data":{"initApp":{"startTime":1154.2},"loadAuthComponent":{"startTime":1500.7},"startAppToTTI":{"duration":346.7}},"order":["initApp","loadAuthComponent","startAppToTTI"]}',
            "iframeId": frame_id, "details": '{"pageVisibilityState":"visible"}',
        })

        # 3. Federation.
        fingerprint = uuid4().hex
        auth_headers = {
            "Accept": "application/json", "Referer": "https://idmsa.apple.com/",
            "Content-Type": "application/json", "X-Apple-Widget-Key": CLIENT_ID,
            "X-Apple-Frame-Id": frame_id, "X-Apple-Domain-Id": "3", "X-Apple-Locale": "en_us",
            "X-Requested-With": "XMLHttpRequest", "Origin": "https://idmsa.apple.com",
            "X-Apple-I-Require-UE": "true",
            "X-Apple-I-FD-Client-Info": '{"U":"%s","L":"%s","Z":"GMT-8:00","V":"1.1","F":"%s"}' % (
                self.user_agent, self.language, fingerprint),
            "X-Apple-Auth-Attributes": auth_attributes, "User-Agent": self.user_agent,
            "X-Apple-Mandate-Security-Upgrade": "0",
        }
        params = {"isRememberMeEnabled": "false"}
        r = self.s.post(AUTH_URL + "auth/federate", headers=auth_headers, params=params, timeout=TIMEOUT,
                        json={"accountName": email, "rememberMe": "false"})
        if "federated" not in r.json():
            raise Exception("federation failed: " + service_error(r))

        # 4. SRP handshake.
        user = srp.User(email, bytes(), hash_alg=srp.SHA256, ng_type=srp.NG_2048)
        _, A = user.start_authentication()
        r = self.s.post(AUTH_URL + "auth/signin/init", headers=auth_headers, timeout=TIMEOUT,
                        json={"a": b64enc(A), "accountName": email, "protocols": ["s2k", "s2k_fo"]})
        out = r.json()
        if r.status_code != 200:
            raise Exception("sign-in init error: " + service_error(r))
        if "b" not in out:
            raise Exception("sign-in init returned no 'b'")
        if out.get("protocol") != "s2k":
            raise Exception("unsupported protocol %r" % out.get("protocol"))

        # 5. Derive the password key from Apple's salt and iteration count.
        salt, iterations, B, c = b64dec(out["salt"]), out["iteration"], b64dec(out["b"]), out["c"]
        pass_hash = hashlib.sha256(password.encode("utf-8")).digest()
        user.p = pbkdf2.PBKDF2(pass_hash, salt, iterations, SHA256).read(32)
        M1 = user.process_challenge(salt, B)
        if M1 is None:
            raise Exception("SRP challenge failed")
        M2 = user.K

        # 6. Complete the sign-in.
        r = self.s.post(AUTH_URL + "auth/signin/complete", headers=auth_headers, params=params, timeout=TIMEOUT,
                        json={"accountName": email, "c": c, "m1": b64enc(M1), "m2": b64enc(M2),
                              "rememberMe": "false"})
        if r.status_code == 409:
            raise TwoFactorRequired()
        if r.status_code != 200:
            raise Exception("authentication failed: " + service_error(r))

        # 7. Trade the session for the Apple Music media-user-token cookie.
        r = self.s.post("https://buy.music.apple.com/account/web/auth", headers=self.music_headers(),
                        timeout=TIMEOUT, json={"webAuthorizationFlowContext": "music"})
        if r.status_code != 200:
            raise Exception("token exchange failed: HTTP %d" % r.status_code)
        token = None
        for cookie in self.s.cookies:
            if cookie.name == "media-user-token":
                token = cookie.value
        if not token:
            raise Exception("signed in but no media-user-token cookie came back")
        return token


def write_token(out_dir, token):
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, "media-user-token")
    tmp = path + ".tmp"
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as handle:
        handle.write(token)
    os.replace(tmp, path)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default="/run/secrets/karen.env")
    parser.add_argument("--out", default="/out")
    args = parser.parse_args()

    env = load_env(args.env)
    index, tried = 1, 0
    while env.get("APPLE_ID_%d" % index):
        email, password = env["APPLE_ID_%d" % index], env.get("APPLE_PASS_%d" % index, "")
        index += 1
        tried += 1
        label = "account %d" % (index - 1)
        try:
            token = AppleMusicAuth().login(email, password)
        except TwoFactorRequired:
            print("%s: asks for a 2FA code, skipped" % label)
            continue
        except Exception as exc:  # one account failing must not stop the next
            print("%s: failed: %s" % (label, exc))
            continue
        write_token(args.out, token)
        print("%s: ok (token length %d)" % (label, len(token)))
        return 0
    if tried == 0:
        print("no APPLE_ID_n entries found in the env file")
    return 1


if __name__ == "__main__":
    sys.exit(main())
