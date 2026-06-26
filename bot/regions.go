package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"
)

// =============================================================================
// Region-availability sweep (used by /check)
//
// For an album or song, we ask the public iTunes lookup endpoint
// (itunes.apple.com/lookup?id=…&country=…) once per storefront and collect the
// countries where the ID exists (resultCount>0). It is unauthenticated and
// lightweight, so the whole ~175-storefront fan-out finishes in a couple of
// seconds with bounded concurrency.
// =============================================================================

// appleStorefronts is the set of iTunes storefront country codes the sweep probes.
// Over-inclusion is harmless — a code that isn't a real storefront simply returns
// resultCount 0 and never shows up as "available".
var appleStorefronts = []string{
	"ae", "ag", "ai", "al", "am", "ao", "ar", "at", "au", "az",
	"ba", "bb", "be", "bf", "bg", "bh", "bj", "bm", "bn", "bo",
	"br", "bs", "bt", "bw", "by", "bz", "ca", "cd", "cg", "ch",
	"ci", "cl", "cm", "cn", "co", "cr", "cv", "cy", "cz", "de",
	"dk", "dm", "do", "dz", "ec", "ee", "eg", "es", "fi", "fj",
	"fm", "fr", "ga", "gb", "gd", "ge", "gh", "gm", "gr", "gt",
	"gw", "gy", "hk", "hn", "hr", "hu", "id", "ie", "il", "in",
	"iq", "is", "it", "jm", "jo", "jp", "ke", "kg", "kh", "kn",
	"kr", "kw", "ky", "kz", "la", "lb", "lc", "lk", "lr", "lt",
	"lu", "lv", "ly", "ma", "md", "me", "mg", "mk", "ml", "mm",
	"mn", "mo", "mr", "ms", "mt", "mu", "mv", "mw", "mx", "my",
	"mz", "na", "ne", "ng", "ni", "nl", "no", "np", "nz", "om",
	"pa", "pe", "pg", "ph", "pk", "pl", "pt", "pw", "py", "qa",
	"ro", "rs", "ru", "rw", "sa", "sb", "sc", "se", "sg", "si",
	"sk", "sl", "sn", "sr", "sv", "sz", "tc", "td", "th", "tj",
	"tm", "tn", "to", "tr", "tt", "tw", "tz", "ua", "ug", "us",
	"uy", "uz", "vc", "ve", "vg", "vn", "vu", "xk", "ye", "za",
	"zm", "zw",
}

var regionHTTP = &http.Client{Timeout: 15 * time.Second}

// itunesHasID reports whether `id` exists in the given storefront, via the public
// lookup endpoint. Any error (network, non-200, decode) is treated as "not found"
// so a single flaky storefront never fails the whole sweep.
func itunesHasID(ctx context.Context, id, country string) bool {
	q := url.Values{}
	q.Set("id", id)
	q.Set("country", country)
	req, err := http.NewRequestWithContext(ctx, "GET", "https://itunes.apple.com/lookup?"+q.Encode(), nil)
	if err != nil {
		return false
	}
	resp, err := regionHTTP.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return false
	}
	var r struct {
		ResultCount int `json:"resultCount"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return false
	}
	return r.ResultCount > 0
}

// regionAvailability fans the lookup out across every storefront (bounded
// concurrency) and returns the available codes, sorted.
func regionAvailability(ctx context.Context, id string) []string {
	var mu sync.Mutex
	available := make([]string, 0, len(appleStorefronts))

	sem := make(chan struct{}, 24)
	var wg sync.WaitGroup
	for _, sf := range appleStorefronts {
		wg.Add(1)
		sem <- struct{}{}
		go func(sf string) {
			defer wg.Done()
			defer func() { <-sem }()
			if itunesHasID(ctx, id, sf) {
				mu.Lock()
				available = append(available, sf)
				mu.Unlock()
			}
		}(sf)
	}
	wg.Wait()
	sort.Strings(available)
	return available
}

// formatRegionAvailability renders the sweep as flag badges: a count headline, the
// available regions, and — when the unavailable set is short — exactly which
// storefronts are missing (the common, useful case of "everywhere but RU").
func formatRegionAvailability(available []string) string {
	total := len(appleStorefronts)
	if len(available) == 0 {
		return "🌍 Availability: not found in any of the checked regions."
	}

	avail := map[string]bool{}
	for _, sf := range available {
		avail[sf] = true
	}
	var missing []string
	for _, sf := range appleStorefronts {
		if !avail[sf] {
			missing = append(missing, sf)
		}
	}

	badge := func(codes []string) string {
		parts := make([]string, len(codes))
		for i, sf := range codes {
			parts[i] = regionBadge(sf)
		}
		return strings.Join(parts, ", ")
	}

	var sb strings.Builder
	fmt.Fprintf(&sb, "🌍 Available in %d of %d regions\n%s", len(available), total, badge(available))
	// When only a handful are missing, listing them is far more useful than the long
	// available block; when many are missing, the available list above already says it.
	if len(missing) > 0 && len(missing) <= 15 {
		fmt.Fprintf(&sb, "\n\n❌ Not available in %d: %s", len(missing), badge(missing))
	}
	return sb.String()
}

// sendRegionAvailability runs the storefront sweep for an album/song id and posts
// the result as its own message after the /check card. Runs inline on the caller's
// goroutine (handleCount already runs off the update loop), so the card shows first.
func (b *TelegramBot) sendRegionAvailability(chatID int64, id string, replyToID int) {
	if id == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	available := regionAvailability(ctx, id)
	_ = b.sendMessageWithReply(chatID, formatRegionAvailability(available), nil, replyToID)
}
