# Karen — wrapper-manager migration: Hall of Fame & Shame

## Date: 2026-06-13

## Goal
Replace old Frida TCP wrapper (runv2/runv3) with wrapper-manager gRPC backend. Enable concurrent multi-account ripping.

## What was built

### New files
- `bot/utils/wmgrpc/client.go` — gRPC client: M3U8, WebPlayback, License, Lyrics, Status, DecryptionStream
- `bot/utils/wmgrpc/decrypt.go` — DownloadAndDecrypt: parallel segment download, persistent decrypt stream, MP4 reassembly
- `wrapper-manager/Dockerfile` — builds wrapper-manager from GitHub
- `wrapper-manager/webplay.go` — patched GetWebPlayback with nil-safe checks

### Modified files
- `bot/main.go` — ripTrack/mvDownloader/checkM3u8 all use wmgrpc gRPC calls
- `bot/telegram_bot.go` — removed MediaUserToken params from rip calls, removed mp4decrypt checks
- `bot/utils/structs/structs.go` — removed old wrapper fields, added WrapperManagerAddr
- `bot/Dockerfile` — removed mp4decrypt/Bento4 build stage, added go mod tidy
- `docker-compose.yml` — replaced wrapper service with wrapper-manager, fixed volume mount

### Deleted files
- `bot/utils/runv2/`, `bot/utils/runv3/`, `bot/agent.js`, `bot/agent-arm64.js`, `wrapper/`

## Bugs fixed along the way

1. **Slice bounds panic** (`decrypt.go:311`) — out-of-order segment insertion crashed with `slice bounds out of range [18:1]`. Fixed by using `map[int]parsedSeg` for collection instead of ordered slice with insert.

2. **fMP4 corruption** — `go-mp4tag`'s `writeMP4Tags` corrupts fragmented MP4 files (adjusts `stco` offsets but not `trun.data_offset` in moof boxes). Fixed by remuxing fMP4 → standard MP4 via `MP4Box -add` before tagging.

3. **WebPlayback crash** — wrapper-manager's `webplay.go:40` panicked: `interface conversion: interface {} is nil, not []interface{}`. Apple's API returns `songList`/`assets` as nil in some cases. Fixed by patching wrapper-manager's webplay.go with nil-safe type assertions.

4. **Wrong totalBytes** — progress used `seg.Limit` (duration in seconds) as byte count. Fixed by using `len(initData) * (1 + totalSegments)` initially, then correcting to actual sizes after download.

## Performance bottleneck found & fixed

**Before:** `DecryptSample` (client.go) opened a **new gRPC stream per sample**. A typical song has ~3000 samples = 3000 TCP connections + gRPC handshakes.

**After:** `DecryptionStream` opens ONE persistent bidirectional stream per track. All samples sent/received on the same connection. 1 dial instead of 3000.

### Remaining issue
- "no available instance" — wrapper-manager instances crash on WebPlayback panic and don't recover. Need re-login via gRPC `Login` RPC after container rebuild.
- The volume `wrapper-manager-data` is ephemeral (lost on `docker compose build wrapper-manager`). Accounts need re-login after rebuild.

## Architecture summary (current)

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
1. Re-login accounts after container rebuild (data volume was lost)
2. Progress bar still cosmetic (not critical)
3. MTProto `FILE_PARTS_INVALID` may still occur for files >50MB (test needed once instances are back)
