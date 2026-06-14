# Session History: `feat/wrapper-manager` branch

## Session 1: June 13, 2026 (initial migration)

### Goal
Replace legacy Frida TCP wrapper (runv2/runv3) with wrapper-manager gRPC backend.

### What was done
1. Created `bot/utils/wmgrpc/client.go` — gRPC client (Status, M3U8, WebPlayback, License, Lyrics, Decrypt)
2. Created `bot/utils/wmgrpc/decrypt.go` — HLS download + decrypt pipeline
3. Modified `bot/main.go` — ripTrack/mvDownloader/checkM3u8 all use wmgrpc gRPC calls
4. Modified `bot/telegram_bot.go` — removed MediaUserToken params from rip calls
5. Modified `bot/utils/structs/structs.go` — removed old wrapper fields, added `WrapperManagerAddrs`
6. Modified `bot/Dockerfile` — removed mp4decrypt/Bento4, added go mod tidy
7. Modified `docker-compose.yml` — replaced wrapper service with wrapper-manager
8. Created `wrapper-manager/Dockerfile` — builds wrapper-manager from GitHub
9. Deleted legacy files: `bot/utils/runv2/`, `bot/utils/runv3/`, `bot/agent.js`, `bot/agent-arm64.js`, `wrapper/`

### Bugs found and fixed during this session
- **Volume mount path wrong** (`wrapper-manager-data:/app/rootfs/data` -> `wrapper-manager-data:/root/rootfs/data`)
- **wrapper-manager listens on localhost** — fixed by adding `--host 0.0.0.0` to command
- **Slice bounds panic** — out-of-order parallel segment download crashed. Fixed with map-based collector
- **fMP4 tagging corruption** — `go-mp4tag` corrupts fragmented MP4. Fixed by remuxing to flat MP4 before tagging (first MP4Box, later replaced with ffmpeg)
- **WebPlayback crash** — nil type assertion in upstream webplay.go. Fixed with patched `wrapper-manager/webplay.go`
- **Per-sample gRPC streams** — 3000 TCP handshakes per song. Fixed with persistent `DecryptionStream` type (one bidirectional stream per track)
- **Wrong progress totalBytes** — used duration as byte count. Fixed with estimation

### VPS test results
- Deployed and tested on VPS (`~/karen`)
- 8/8 tracks ripped "successfully" for Pink Floyd 8-Tracks album
- **BUT: tracks were 15-30 second preview clips, not full songs.** This was not detected at the time.

---

## Session 2: June 14, 2026 (multi-account + authenticated M3U8)

### What was done
1. **Multi-account pool** (`bot/utils/wmgrpc/pool.go`) — channel-based FIFO client pool
2. **Parallel track downloads** across pool clients
3. **Nil-safe context** — added `context.Background()` fallback for nil contexts
4. **Cold-start timeout fix** — patched wrapper-manager to increase emulator timeout from 5s to 25s
5. **0-byte file validation** — `fileExists()` now returns false for 0-byte files
6. **Pipeline download+decrypt** — segments sent to decrypt stream as they arrive, not after full download
7. **Replaced MP4Box with ffmpeg** for fMP4 remuxing (faster)
8. **Authenticated M3U8** — switched from `track.M3u8` (public catalog, 15s preview) to `wm.M3U8` gRPC endpoint (authenticated, full track)
9. **HLS byte-range downloads** — `downloadBytesRange()` for `#EXT-X-BYTERANGE` segments
10. **Relative URL resolution** — `url.URL.ResolveReference()` to preserve auth query params on segment URLs

### Root cause of 15s preview bug
- Bot was using `track.M3u8` from the public iTunes catalog API, which returns a playlist for the 15-second unauthenticated AAC preview
- Fix: use `wm.M3U8` gRPC endpoint which hits the Widevine/Android-backed authenticated playlist

### Status
- Full-track delivery (fix 8-10) **NOT YET VERIFIED** with duration/size checks
- Branch has 25 commits, ready for testing

---

## Session 3: June 14, 2026 (verification + volume fix + account setup)

### Goals
1. Fix ephemeral accounts — accounts wiped on container rebuild
2. Deploy and test on VPS
3. Verify full-length ALAC tracks

### Account persistence fix
**Root cause:** The docker-compose volume mount `wm-data-1:/root/rootfs/data` points to the wrong path. The wrapper-manager stores emulator data at `/root/data/wrapper/rootfs/data/data/com.apple.android.music/`.

**Fix:** Changed volume mount to `wm-data-1:/root/data` (`2b6055f`).

### Account setup on VPS
- Location: `~/test/karen`
- Both wrapper-managers recreated with correct mount
- Accounts logged in via gRPC `Login` endpoint (not raw wrapper binary):
  - wm-1: `orh87@c35.net` (instance `1d681dd2`, US region)
  - wm-2: `mlws@c35.net` (instance `23b6aaaa`, US region)
- `instances.json` created, wrapper processes running
- Status reports `clientCount: 1, ready: true` on both

### Key learning
- Raw `wrapper -L` login creates data at default path, NOT under per-instance directories
- Only gRPC `Login` properly registers instances in `instances.json` and manages emulator lifecycle
- `GetToken()` fetches `https://music.apple.com`, finds JS asset, extracts JWT bearer token

### Status
- Accounts persisted across container restarts
- Ready for ALAC verification

---

## Session 4: June 14, 2026 (ALAC decryption fix + variant selection)

### Goals
1. Fix encrypted audio output (mean_volume: -91 dB)
2. Fix bot sending AAC instead of ALAC
3. Achieve end-to-end verified ALAC delivery

### Bug 1: Encrypted audio pass-through

**Symptom:** Downloaded files had `mean_volume: -91.0 dB`, hundreds of AAC decode errors ("Prediction is not allowed in AAC-LC"). The mdat box contained encrypted data that was never actually decrypted.

**Root cause:** `frag.Encode()` in `decrypt.go` re-encrypted the original `mdat` bytes. The code decrypted samples into `seg.decryptedSamples` but then called `Encode()` which wrote back the original `seg.frag.Mdat.Data` (still encrypted).

**Fix** (`09ec1a6`, `20ec982`): After decryption, replace `seg.frag.Mdat.Data` with decrypted sample bytes before encoding:
```go
var decryptedMdat []byte
for _, sample := range seg.decryptedSamples {
    decryptedMdat = append(decryptedMdat, sample.Data...)
}
seg.frag.Mdat.Data = decryptedMdat
```

### Bug 2: Wrong variant selection (AAC instead of ALAC)

**Symptom:** Bot sent 6-7MB files that were AAC-LC 256kbps containers, not ALAC.

**Root cause:** Two issues in `ripTrack()`:

1. `extractMedia()` correctly found the ALAC variant URL but discarded it: `_, Quality, err = extractMedia(downloadM3u8, true)`
2. `downloadM3u8` stayed as the master playlist URL, so `DownloadAndDecrypt()` received the master and blindly picked `Variants[0]` = AAC-LC 256kbps
3. Additionally, `extractMedia` was only called if `Config.SongFileFormat` contained "Quality"

**Fix** (`85c3845`): Capture the variant URL and always run variant selection for ALAC:
```go
var variantURL string
variantURL, Quality, err = extractMedia(downloadM3u8, true)
if variantURL != "" {
    downloadM3u8 = variantURL  // pass ALAC media playlist to DownloadAndDecrypt
}
```

### Bug 3: Stale wrapper-manager emulator state

**Symptom:** 5 out of 7 tracks failed with "The item you tried to download is no longer available" from the Android emulator, while the other wrapper-manager succeeded on all its tracks.

**Root cause:** The wm-1 emulator got into a stuck state after repeated concurrent M3U8 requests. The dialog handler dismissed the error but the emulator couldn't recover.

**Fix:** `docker compose restart wrapper-manager-1` — fresh emulator state resolved the issue.

### Final result
- 7/7 tracks downloaded as proper ALAC files
- End-to-end verified: ALAC variant selection → authenticated M3U8 → HLS segment download → gRPC decryption → mdat replacement → fMP4 remux → metadata tagging

### Commits in this session
- `2b6055f` — fix: correct wrapper-manager volume mount to /root/data
- `09ec1a6` — fix: replace mdat with decrypted sample data
- `20ec982` — fix: remove unexported lazyDataSize reference
- `85c3845` — fix: use extracted ALAC variant URL instead of master playlist

---

## Scaling notes (for 6-8 accounts)

Each wrapper-manager instance requires:
1. A service block in `docker-compose.yml` with unique port and volume
2. An entry in `bot/config.yaml` under `wrapper-manager-addrs`
3. An interactive login via `docker compose run --rm wrapper-manager-N -L "APPLE_ID:PASSWORD"`

The bot's pool automatically distributes across all configured instances. No code changes needed for scaling.

### Resource considerations
- Each wrapper-manager runs an Android emulator (Frida hooks, ~200-400MB RAM)
- 8 instances ≈ 2-3GB RAM for wrapper-managers alone
- The bot itself uses minimal resources
- Parallel downloads limited to `len(Config.WrapperManagerAddrs)` concurrent tracks
