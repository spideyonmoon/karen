# Karen — wrapper-manager migration: Hall of Fame & Shame

## Date: 2026-06-13 (12:10 AM – 12:39 AM)

## Goal
Replace old Frida TCP wrapper (runv2/runv3) with wrapper-manager gRPC backend. Enable concurrent multi-account ripping.

---

## Step-by-step log

### Step 1: Initial state assessment
- Read all source files: `bot/main.go`, `bot/telegram_bot.go`, `bot/utils/structs/structs.go`, `bot/utils/runv2/`, `bot/utils/runv3/`
- Studied wrapper-manager gRPC API from GitHub: `proto/manager.proto`, `main.go`, `handler.go`, `decrypt_instance.go`, `webplay.go`
- Found the gRPC service has: `Status`, `Login` (bidi stream), `Logout`, `Decrypt` (bidi stream), `M3U8`, `Lyrics`, `License`, `WebPlayback`
- Key finding: **No `DecryptSample` unary RPC exists** — only the bidirectional streaming `Decrypt` RPC

### Step 2: Created `bot/utils/wmgrpc/client.go`
- Imported proto stubs directly from `github.com/WorldObservationLog/wrapper-manager/proto` (no local proto generation)
- Implemented: `NewClient(addr)`, `Close()`, `Status()`, `M3U8()`, `WebPlayback()`, `License()`, `Lyrics()`
- Implemented `DecryptSample()` — **WRONG APPROACH**: opened a new gRPC stream per sample, sent one request, received one response, closed stream. This was the root cause of the performance disaster.
- Later replaced with `DecryptionStream` type: one persistent bidirectional stream per track

### Step 3: Created `bot/utils/wmgrpc/decrypt.go`
- Implemented `DownloadAndDecrypt()`:
  1. Download master playlist → parse → get media playlist URL
  2. Download media playlist → parse segments
  3. Extract key URIs from EXT-X-KEY tags
  4. Download init segment → parse with `mp4ff` → `mp4.DecryptInit()` to get track info
  5. Filter `sbgp`/`sgpd` boxes (encryption metadata) from init segment
  6. Write init segment to output file
  7. Download all segments in parallel goroutines
  8. Collect segments in order
  9. Decrypt samples via gRPC
  10. Encode and write segments

### Step 4: Modified `bot/main.go` — ripTrack, mvDownloader, checkM3u8
- `ripTrack()`: replaced `runv2.Run()` / `runv3.Run()` with `wmgrpc.M3U8()` + `wmgrpc.DownloadAndDecrypt()`
- AAC-LC path: `wmClient.WebPlayback(ctx, track.ID)` → `wmgrpc.DownloadAndDecrypt()`
- ALAC/Atmos path: `wmClient.M3U8(ctx, track.ID)` → `wmgrpc.DownloadAndDecrypt()`
- Kept all post-processing: MP4Box tags, writeMP4Tags, convertIfNeeded, recordDownloadedTrack
- Removed `MediaUserToken` parameter from all `ripXxx()` function calls
- Removed `mp4decrypt` pre-flight checks from MV download

### Step 5: Modified `bot/telegram_bot.go`
- Removed `MediaUserToken` from all rip function calls (it's no longer needed since wmgrpc handles auth internally)

### Step 6: Modified `bot/utils/structs/structs.go`
- Removed old wrapper fields: `DecryptM3u8Port`, `GetM3u8Port`, `GetM3u8FromDevice`, `GetM3u8Mode`
- Added `WrapperManagerAddr string`
- Re-added `MediaUserToken` (needed for Apple Music API calls: lyrics, stations)

### Step 7: Modified `bot/Dockerfile`
- Removed Bento4/mp4decrypt build stage (wrapper-manager handles decryption)
- Added `go mod tidy` before `go build` (to resolve new gRPC dependencies)

### Step 8: Modified `docker-compose.yml`
- Replaced `wrapper` service with `wrapper-manager` service
- Fixed volume mount path: `wrapper-manager-data:/app/rootfs/data` → `wrapper-manager-data:/root/rootfs/data`
- Added `command: ["--host", "0.0.0.0"]` for wrapper-manager (listens on all interfaces)

### Step 9: Created `wrapper-manager/Dockerfile`
- Clones `github.com/WorldObservationLog/wrapper-manager` at build time
- Builds Go binary in golang:1.23, runs in ubuntu:latest

### Step 10: Deleted old files
- `bot/utils/runv2/` — old Frida TCP wrapper v2
- `bot/utils/runv3/` — old Frida TCP wrapper v3 (AAC-LC)
- `bot/agent.js` — Node.js agent for wrapper communication
- `bot/agent-arm64.js` — ARM64 variant
- `wrapper/` — old wrapper Dockerfile and entrypoint

### Step 11: Deployed to VPS
- `git push origin feat/wrapper-manager`
- `ssh vps: cd ~/karen && git pull && docker compose build && docker compose up -d`
- Bot compiled successfully, connected to wrapper-manager

### Step 12: First test — Nevermind album
- **Panic**: `slice bounds out of range [18:1]` at `decrypt.go:311`
- Root cause: segments arrive out of order, `copy(parsed[res.index+1:], parsed[res.index:])` panicked when `res.index > len(parsed)`
- **Fix**: replaced ordered slice insertion with `map[int]parsedSeg` collection, then convert to ordered slice after all segments received

### Step 13: Remux fix for MTProto FILE_PARTS_INVALID
- Root cause: `go-mp4tag`'s `writeMP4Tags()` corrupts fragmented MP4 (adjusts `stco` offsets but not `trun.data_offset` in moof boxes)
- **Fix**: added `MP4Box -add trackPath -new remuxPath` step after `DownloadAndDecrypt` to convert fMP4 → standard MP4 before tagging

### Step 14: WebPlayback crash
- Wrapper-manager panicked: `interface conversion: interface {} is nil, not []interface {}` at `webplay.go:40`
- Apple's API returns `songList`/`assets` as nil for some tracks
- **Fix**: created `wrapper-manager/webplay.go` with safe nil checks at every type assertion, copied over git-cloned version in Dockerfile

### Step 15: Performance bottleneck — per-sample gRPC streams
- Each `DecryptSample()` call opened a new gRPC stream, sent one sample, received one result, closed stream
- ~3000 samples per song = 3000 TCP connections + gRPC handshakes
- **Fix**: created `DecryptionStream` type with one persistent bidirectional stream per track
- Removed worker pool code (unnecessary — wrapper-manager's decrypt instance processes sequentially due to `connMu.Lock()`)

### Step 16: Progress bar fix
- `totalBytes` was computed using `seg.Limit` (duration in seconds) instead of byte count
- Download showed 100% immediately because `dlProgress` (bytes) > `totalBytes` (seconds)
- **Fix**: initial estimate `len(initData) * (1 + totalSegments)`, corrected to actual sizes after download completes

### Step 17: VPS revert
- Switched VPS back to `main` branch
- Rebuilt old bot (with mp4decrypt, old wrapper)
- Removed orphaned wrapper-manager container
- Bot running but wrapper needs re-login ("SSL token expired")

---

## Files changed (feat/wrapper-manager branch)

### New files
- `bot/utils/wmgrpc/client.go` — gRPC client: M3U8, WebPlayback, License, Lyrics, Status, DecryptionStream
- `bot/utils/wmgrpc/decrypt.go` — DownloadAndDecrypt: parallel segment download, persistent decrypt stream, MP4 reassembly
- `wrapper-manager/Dockerfile` — builds wrapper-manager from GitHub
- `wrapper-manager/webplay.go` — patched GetWebPlayback with nil-safe checks
- `docs/WRAPPER-MANAGER-MIGRATION.md` — this file

### Modified files
- `bot/main.go` — ripTrack/mvDownloader/checkM3u8 all use wmgrpc gRPC calls
- `bot/telegram_bot.go` — removed MediaUserToken params from rip calls, removed mp4decrypt checks
- `bot/utils/structs/structs.go` — removed old wrapper fields, added WrapperManagerAddr
- `bot/Dockerfile` — removed mp4decrypt/Bento4 build stage, added go mod tidy
- `docker-compose.yml` — replaced wrapper service with wrapper-manager, fixed volume mount

### Deleted files
- `bot/utils/runv2/`, `bot/utils/runv3/`, `bot/agent.js`, `bot/agent-arm64.js`, `wrapper/`

---

## Bugs fixed

1. **Slice bounds panic** (`decrypt.go:311`) — out-of-order segment insertion crashed. Fixed with map-based collector.

2. **fMP4 corruption** — `go-mp4tag` corrupts fragmented MP4 files. Fixed by remuxing via `MP4Box -add` before tagging.

3. **WebPlayback crash** — wrapper-manager panicked on nil type assertion. Fixed by patching webplay.go with nil-safe checks.

4. **Wrong progress totalBytes** — used `seg.Limit` (duration) as byte count. Fixed with proper estimation.

5. **Performance bottleneck** — per-sample gRPC streams (3000 dials/song). Fixed with persistent `DecryptionStream`.

---

## Architecture (current on feat/wrapper-manager)

```
Bot → gRPC (plaintext, wrapper-manager:8080)
  → M3U8 RPC → get ALAC/Atmos HLS playlist URL
  → WebPlayback RPC → get AAC-LC HLS playlist URL (patched webplay.go)
  → Decrypt stream (bidirectional) → samples sent sequentially, decrypted in-place
    → wrapper-manager → TCP → Android emulator (Frida hooks)
  → License RPC → Widevine license
  → Lyrics RPC → track lyrics
```

## Known issues for next session
1. Re-login accounts on old wrapper (main branch) — wrapper shows "SSL token expired"
2. When switching to feat/wrapper-manager, re-login via gRPC `Login` RPC
3. Wrapper-manager accounts are ephemeral — lost on container rebuild
4. Progress bar still cosmetic (not critical)
5. MTProto `FILE_PARTS_INVALID` may still occur for files >50MB (test needed once instances are back)
