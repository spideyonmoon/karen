package main

import (
	"os"
	"strings"
	"sync"
	"time"
)

// =============================================================================
// Apple Music media-user-token resolution.
//
// ./do-token.sh (also run by setup.sh when the file is missing, or with RETOKEN=1)
// mints a token from the Apple IDs in .env and writes it to state/media-user-token.
// mediaUserToken() prefers that file and falls back to MEDIA_USER_TOKEN from
// config.yaml, so a hand-pasted token still works and an empty .env value is fine.
// The file is re-read whenever its mtime changes, so a refresh needs no restart.
// =============================================================================

// mediaTokenFile sits in the bind-mounted state dir, like state/mtproto-session.json.
const mediaTokenFile = "state/media-user-token"

// mediaTokenMinLen mirrors the lyrics client's sanity check: anything shorter is not
// a real token (an empty or truncated file).
const mediaTokenMinLen = 50

var mediaTok struct {
	sync.Mutex
	mtime time.Time
	val   string
}

// mediaUserToken returns the token to send as Media-User-Token: the auto-minted one
// when present and plausible, else the configured one.
func mediaUserToken() string {
	if v := autoMediaToken(); v != "" {
		return v
	}
	return Config.MediaUserToken
}

func autoMediaToken() string {
	info, err := os.Stat(mediaTokenFile)
	if err != nil {
		return ""
	}
	mediaTok.Lock()
	defer mediaTok.Unlock()
	if info.ModTime().Equal(mediaTok.mtime) {
		return mediaTok.val
	}
	data, err := os.ReadFile(mediaTokenFile)
	if err != nil {
		return ""
	}
	val := strings.TrimSpace(string(data))
	if len(val) < mediaTokenMinLen {
		val = ""
	}
	mediaTok.mtime, mediaTok.val = info.ModTime(), val
	return val
}
