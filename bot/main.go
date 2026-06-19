package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	apputils "main/utils"
	"main/utils/ampapi"
	"main/utils/lyrics"
	"main/utils/runv3"
	"main/utils/structs"
	"main/utils/task"
	"main/utils/wmgrpc"

	"github.com/fatih/color"
	"github.com/grafov/m3u8"
	"github.com/olekukonko/tablewriter"
	"github.com/spf13/pflag"
	"github.com/zhaarey/go-mp4tag"
	"gopkg.in/yaml.v2"
)

var (
	forbiddenNames        = regexp.MustCompile(`[/\\<>:"|?*]`)
	dl_atmos              bool
	dl_aac                bool
	dl_select             bool
	dl_song               bool
	// dl_noCache forces a fresh re-rip even when the file already exists on disk:
	// the skip checks delete the stale original/converted file and download anew.
	// Set per-request from downloadRequest.noCache in runDownload (serial worker),
	// reset there before each rip — mirrors how dl_atmos/dl_aac are managed.
	dl_noCache            bool
	wmPool                *wmgrpc.Pool
	artist_select         bool
	debug_mode            bool
	alac_max              *int
	atmos_max             *int
	mv_max                *int
	mv_audio_type         *string
	aac_type              *string
	Config                structs.ConfigSet
	counter               structs.Counter
	okDictMu              sync.Mutex
	okDict                = make(map[string][]int)
	lastPathsMu           sync.Mutex
	lastDownloadedPaths   []string
	activeProgressFactory func(track *task.Track) apputils.ProgressFunc
	downloadedMetaMu      sync.Mutex
	downloadedMeta        = make(map[string]AudioMeta)
	searchMetaMu          sync.Mutex
	searchMetaByID        = make(map[string]AudioMeta)
	downloadFailureMu     sync.Mutex
	lastDownloadFailures  []string
)

type AudioMeta struct {
	TrackID        string
	Title          string
	Performer      string
	DurationMillis int64
	AlbumName      string
	ReleaseDate    string
	ContentRating  string
	Quality        string
	Codec          string
	// PlaylistName / PlaylistArtist are set only when the track was downloaded as
	// part of a playlist, so the Telegram caption can show the playlist's identity
	// instead of masquerading as the first track's album. Empty for albums/songs.
	PlaylistName   string
	PlaylistArtist string
}

func loadConfig() error {
	data, err := os.ReadFile("config.yaml")
	if err != nil {
		return err
	}
	err = yaml.Unmarshal(data, &Config)
	if err != nil {
		return err
	}
	if len(Config.Storefront) != 2 {
		Config.Storefront = "us"
	}
	// MV defaults tuned for Telegram inline playback when unset in config.
	if Config.MVMax <= 0 {
		Config.MVMax = 1080
	}
	if Config.MVAudioType == "" {
		Config.MVAudioType = "aac"
	}
	// Task-concurrency lend thresholds. These are read directly (no fallback) by
	// the scheduler, so default them here: an unset BorrowerMaxTracks (0) would make
	// `count >= max` always true and silently disable all borrowing. Inert when
	// task-concurrency is off.
	if Config.LendHeadRemainingThreshold <= 0 {
		Config.LendHeadRemainingThreshold = 50
	}
	if Config.BorrowerMaxTracks <= 0 {
		Config.BorrowerMaxTracks = 30
	}
	return nil
}

func recordDownloadedTrack(ctx context.Context, track *task.Track) {
	if track == nil || track.SavePath == "" {
		return
	}
	rs := ripStateFrom(ctx)
	rs.addPath(track.SavePath)
	meta := AudioMeta{
		TrackID:        strings.TrimSpace(track.ID),
		Title:          strings.TrimSpace(track.Resp.Attributes.Name),
		Performer:      strings.TrimSpace(track.Resp.Attributes.ArtistName),
		DurationMillis: int64(track.Resp.Attributes.DurationInMillis),
		AlbumName:      strings.TrimSpace(track.Resp.Attributes.AlbumName),
		ReleaseDate:    strings.TrimSpace(track.Resp.Attributes.ReleaseDate),
		ContentRating:  strings.TrimSpace(track.Resp.Attributes.ContentRating),
		Quality:        strings.TrimSpace(track.Quality),
		Codec:          strings.TrimSpace(track.Codec),
	}
	if track.PreType == "playlists" {
		meta.PlaylistName = strings.TrimSpace(track.PlaylistData.Attributes.Name)
		meta.PlaylistArtist = strings.TrimSpace(track.PlaylistData.Attributes.ArtistName)
	}
	if meta.TrackID != "" {
		if override, ok := popSearchMeta(meta.TrackID); ok {
			if override.Title != "" {
				meta.Title = override.Title
			}
			if override.Performer != "" {
				meta.Performer = override.Performer
			}
		}
	}
	if meta.Title != "" || meta.Performer != "" {
		rs.putMeta(track.SavePath, meta)
	}
}

func getDownloadedMeta(ctx context.Context, path string) (AudioMeta, bool) {
	return ripStateFrom(ctx).getMeta(path)
}

// clearDownloadState clears the package-global download state. It is only used by
// the CLI single-shot path and the flag-off serial worker; per-rip state lives on
// the RipState and is garbage-collected with the request, so nothing to clear there.
func clearDownloadState() {
	lastPathsMu.Lock()
	lastDownloadedPaths = nil
	lastPathsMu.Unlock()
	downloadedMetaMu.Lock()
	downloadedMeta = make(map[string]AudioMeta)
	downloadedMetaMu.Unlock()
	debug.FreeOSMemory()
}

func resetDownloadFailures() {
	downloadFailureMu.Lock()
	lastDownloadFailures = nil
	downloadFailureMu.Unlock()
}

func recordDownloadFailure(ctx context.Context, format string, args ...any) {
	ripStateFrom(ctx).recordFailure(fmt.Sprintf(format, args...))
}

func downloadFailureSummary(ctx context.Context) string {
	return ripStateFrom(ctx).failureSummary()
}

// summarizeFailures renders up to the first 3 failure messages plus an overflow
// count. Shared by the global and per-rip failure logs.
func summarizeFailures(failures []string) string {
	if len(failures) == 0 {
		return ""
	}
	limit := len(failures)
	if limit > 3 {
		limit = 3
	}
	summary := strings.Join(failures[:limit], "; ")
	if len(failures) > limit {
		summary += fmt.Sprintf("; and %d more", len(failures)-limit)
	}
	return summary
}

func setSearchMeta(trackID string, title string, performer string) {
	trackID = strings.TrimSpace(trackID)
	if trackID == "" {
		return
	}
	meta := AudioMeta{
		TrackID:   trackID,
		Title:     strings.TrimSpace(title),
		Performer: strings.TrimSpace(performer),
	}
	if meta.Title == "" && meta.Performer == "" {
		return
	}
	searchMetaMu.Lock()
	searchMetaByID[trackID] = meta
	searchMetaMu.Unlock()
}

func popSearchMeta(trackID string) (AudioMeta, bool) {
	searchMetaMu.Lock()
	defer searchMetaMu.Unlock()
	meta, ok := searchMetaByID[trackID]
	if ok {
		delete(searchMetaByID, trackID)
	}
	return meta, ok
}

func LimitString(s string) string {
	if len([]rune(s)) > Config.LimitMax {
		return string([]rune(s)[:Config.LimitMax])
	}
	return s
}

func isInArray(arr []int, target int) bool {
	for _, num := range arr {
		if num == target {
			return true
		}
	}
	return false
}

func fileExists(path string) (bool, error) {
	f, err := os.Stat(path)
	if err == nil {
		return !f.IsDir() && f.Size() > 0, nil
	} else if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func checkUrl(url string) (string, string) {
	pat := regexp.MustCompile(`^(?:https:\/\/(?:beta\.music|music|classical\.music)\.apple\.com\/(\w{2})(?:\/album|\/album\/.+))\/(?:id)?(\d[^\D]+)(?:$|\?)`)
	matches := pat.FindAllStringSubmatch(url, -1)

	if matches == nil {
		return "", ""
	} else {
		return matches[0][1], matches[0][2]
	}
}
func checkUrlMv(url string) (string, string) {
	pat := regexp.MustCompile(`^(?:https:\/\/(?:beta\.music|music)\.apple\.com\/(\w{2})(?:\/music-video|\/music-video\/.+))\/(?:id)?(\d[^\D]+)(?:$|\?)`)
	matches := pat.FindAllStringSubmatch(url, -1)

	if matches == nil {
		return "", ""
	} else {
		return matches[0][1], matches[0][2]
	}
}
func checkUrlSong(url string) (string, string) {
	pat := regexp.MustCompile(`^(?:https:\/\/(?:beta\.music|music|classical\.music)\.apple\.com\/(\w{2})(?:\/song|\/song\/.+))\/(?:id)?(\d[^\D]+)(?:$|\?)`)
	matches := pat.FindAllStringSubmatch(url, -1)

	if matches == nil {
		return "", ""
	} else {
		return matches[0][1], matches[0][2]
	}
}
func checkUrlPlaylist(url string) (string, string) {
	pat := regexp.MustCompile(`^(?:https:\/\/(?:beta\.music|music|classical\.music)\.apple\.com\/(\w{2})(?:\/playlist|\/playlist\/.+))\/(?:id)?(pl\.[\w-]+)(?:$|\?)`)
	matches := pat.FindAllStringSubmatch(url, -1)

	if matches == nil {
		return "", ""
	} else {
		return matches[0][1], matches[0][2]
	}
}

func checkUrlStation(url string) (string, string) {
	pat := regexp.MustCompile(`^(?:https:\/\/(?:beta\.music|music)\.apple\.com\/(\w{2})(?:\/station|\/station\/.+))\/(?:id)?(ra\.[\w-]+)(?:$|\?)`)
	matches := pat.FindAllStringSubmatch(url, -1)

	if matches == nil {
		return "", ""
	} else {
		return matches[0][1], matches[0][2]
	}
}

func checkUrlArtist(url string) (string, string) {
	pat := regexp.MustCompile(`^(?:https:\/\/(?:beta\.music|music|classical\.music)\.apple\.com\/(\w{2})(?:\/artist|\/artist\/.+))\/(?:id)?(\d[^\D]+)(?:$|\?)`)
	matches := pat.FindAllStringSubmatch(url, -1)

	if matches == nil {
		return "", ""
	} else {
		return matches[0][1], matches[0][2]
	}
}
func resolveAppleMusicURL(rawURL string) string {
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" {
		return ""
	}
	if strings.Contains(rawURL, "/album/") || strings.Contains(rawURL, "/playlist/") || strings.Contains(rawURL, "/station/") || strings.Contains(rawURL, "/song/") || strings.Contains(rawURL, "/music-video/") || strings.Contains(rawURL, "/artist/") {
		return rawURL
	}
	if !strings.Contains(rawURL, "music.apple.com") {
		return rawURL
	}
	req, err := http.NewRequest("GET", rawURL, nil)
	if err != nil {
		return rawURL
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36")
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return rawURL
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return rawURL
	}
	pat := regexp.MustCompile(`https://(?:beta\.)?music\.apple\.com/[a-z]{2}/(?:album|song|playlist|station|music-video|artist)/[^"'<>\s?]+(?:\?[^"'<>\s]*)?`)
	if match := pat.Find(body); match != nil {
		return string(match)
	}
	return rawURL
}
func getUrlSong(songUrl string, token string) (string, error) {
	storefront, songId := checkUrlSong(songUrl)
	manifest, err := ampapi.GetSongResp(storefront, songId, Config.Language, token)
	if err != nil {
		fmt.Println("\u26A0 Failed to get manifest:", err)
		counter.NotSong++
		return "", err
	}
	albumId := manifest.Data[0].Relationships.Albums.Data[0].ID
	songAlbumUrl := fmt.Sprintf("https://music.apple.com/%s/album/1/%s?i=%s", storefront, albumId, songId)
	return songAlbumUrl, nil
}
func getUrlArtistName(artistUrl string, token string) (string, string, error) {
	storefront, artistId := checkUrlArtist(artistUrl)
	req, err := http.NewRequest("GET", fmt.Sprintf("https://amp-api.music.apple.com/v1/catalog/%s/artists/%s", storefront, artistId), nil)
	if err != nil {
		return "", "", err
	}
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
	req.Header.Set("Origin", "https://music.apple.com")
	query := url.Values{}
	query.Set("l", Config.Language)
	req.URL.RawQuery = query.Encode()
	do, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", "", err
	}
	defer do.Body.Close()
	if do.StatusCode != http.StatusOK {
		return "", "", errors.New(do.Status)
	}
	obj := new(structs.AutoGeneratedArtist)
	err = json.NewDecoder(do.Body).Decode(&obj)
	if err != nil {
		return "", "", err
	}
	return obj.Data[0].Attributes.Name, obj.Data[0].ID, nil
}

func checkArtist(artistUrl string, token string, relationship string) ([]string, error) {
	storefront, artistId := checkUrlArtist(artistUrl)
	Num := 0
	//id := 1
	var args []string
	var urls []string
	var options [][]string
	for {
		req, err := http.NewRequest("GET", fmt.Sprintf("https://amp-api.music.apple.com/v1/catalog/%s/artists/%s/%s?limit=100&offset=%d&l=%s", storefront, artistId, relationship, Num, Config.Language), nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
		req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
		req.Header.Set("Origin", "https://music.apple.com")
		do, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		defer do.Body.Close()
		if do.StatusCode != http.StatusOK {
			return nil, errors.New(do.Status)
		}
		obj := new(structs.AutoGeneratedArtist)
		err = json.NewDecoder(do.Body).Decode(&obj)
		if err != nil {
			return nil, err
		}
		for _, album := range obj.Data {
			options = append(options, []string{album.Attributes.Name, album.Attributes.ReleaseDate, album.ID, album.Attributes.URL})
		}
		Num = Num + 100
		if len(obj.Next) == 0 {
			break
		}
	}
	sort.Slice(options, func(i, j int) bool {
		// 将日期字符串解析为 time.Time 类型进行比较
		dateI, _ := time.Parse("2006-01-02", options[i][1])
		dateJ, _ := time.Parse("2006-01-02", options[j][1])
		return dateI.Before(dateJ) // 返回 true 表示 i 在 j 前面
	})

	table := tablewriter.NewWriter(os.Stdout)
	if relationship == "albums" {
		table.SetHeader([]string{"", "Album Name", "Date", "Album ID"})
	} else if relationship == "music-videos" {
		table.SetHeader([]string{"", "MV Name", "Date", "MV ID"})
	}
	table.SetRowLine(false)
	table.SetHeaderColor(tablewriter.Colors{},
		tablewriter.Colors{tablewriter.FgRedColor, tablewriter.Bold},
		tablewriter.Colors{tablewriter.Bold, tablewriter.FgBlackColor},
		tablewriter.Colors{tablewriter.Bold, tablewriter.FgBlackColor})

	table.SetColumnColor(tablewriter.Colors{tablewriter.FgCyanColor},
		tablewriter.Colors{tablewriter.Bold, tablewriter.FgRedColor},
		tablewriter.Colors{tablewriter.Bold, tablewriter.FgBlackColor},
		tablewriter.Colors{tablewriter.Bold, tablewriter.FgBlackColor})
	for i, v := range options {
		urls = append(urls, v[3])
		options[i] = append([]string{fmt.Sprint(i + 1)}, v[:3]...)
		table.Append(options[i])
	}
	table.Render()
	if artist_select {
		fmt.Println("You have selected all options:")
		return urls, nil
	}
	reader := bufio.NewReader(os.Stdin)
	fmt.Println("Please select from the " + relationship + " options above (multiple options separated by commas, ranges supported, or type 'all' to select all)")
	cyanColor := color.New(color.FgCyan)
	cyanColor.Print("Enter your choice: ")
	input, _ := reader.ReadString('\n')

	input = strings.TrimSpace(input)
	if input == "all" {
		fmt.Println("You have selected all options:")
		return urls, nil
	}

	selectedOptions := [][]string{}
	parts := strings.Split(input, ",")
	for _, part := range parts {
		if strings.Contains(part, "-") {
			rangeParts := strings.Split(part, "-")
			selectedOptions = append(selectedOptions, rangeParts)
		} else {
			selectedOptions = append(selectedOptions, []string{part})
		}
	}

	fmt.Println("You have selected the following options:")
	for _, opt := range selectedOptions {
		if len(opt) == 1 {
			num, err := strconv.Atoi(opt[0])
			if err != nil {
				fmt.Println("Invalid option:", opt[0])
				continue
			}
			if num > 0 && num <= len(options) {
				fmt.Println(options[num-1])
				args = append(args, urls[num-1])
			} else {
				fmt.Println("Option out of range:", opt[0])
			}
		} else if len(opt) == 2 {
			start, err1 := strconv.Atoi(opt[0])
			end, err2 := strconv.Atoi(opt[1])
			if err1 != nil || err2 != nil {
				fmt.Println("Invalid range:", opt)
				continue
			}
			if start < 1 || end > len(options) || start > end {
				fmt.Println("Range out of range:", opt)
				continue
			}
			for i := start; i <= end; i++ {
				fmt.Println(options[i-1])
				args = append(args, urls[i-1])
			}
		} else {
			fmt.Println("Invalid option:", opt)
		}
	}
	return args, nil
}

func writeCover(sanAlbumFolder, name string, url string) (string, error) {
	originalUrl := url
	var ext string
	var covPath string
	if Config.CoverFormat == "original" {
		ext = strings.Split(url, "/")[len(strings.Split(url, "/"))-2]
		ext = ext[strings.LastIndex(ext, ".")+1:]
		covPath = filepath.Join(sanAlbumFolder, name+"."+ext)
	} else {
		covPath = filepath.Join(sanAlbumFolder, name+"."+Config.CoverFormat)
	}
	exists, err := fileExists(covPath)
	if err != nil {
		fmt.Println("Failed to check if cover exists.")
		return "", err
	}
	if exists {
		_ = os.Remove(covPath)
	}
	if Config.CoverFormat == "png" {
		re := regexp.MustCompile(`\{w\}x\{h\}`)
		parts := re.Split(url, 2)
		url = parts[0] + "{w}x{h}" + strings.Replace(parts[1], ".jpg", ".png", 1)
	}
	url = strings.Replace(url, "{w}x{h}", Config.CoverSize, 1)
	if Config.CoverFormat == "original" {
		url = strings.Replace(url, "is1-ssl.mzstatic.com/image/thumb", "a5.mzstatic.com/us/r1000/0", 1)
		url = url[:strings.LastIndex(url, "/")]
	}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
	do, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer do.Body.Close()
	if do.StatusCode != http.StatusOK {
		if Config.CoverFormat == "original" {
			fmt.Println("Failed to get cover, falling back to " + ext + " url.")
			splitByDot := strings.Split(originalUrl, ".")
			last := splitByDot[len(splitByDot)-1]
			fallback := originalUrl[:len(originalUrl)-len(last)] + ext
			fallback = strings.Replace(fallback, "{w}x{h}", Config.CoverSize, 1)
			fmt.Println("Fallback URL:", fallback)
			req, err = http.NewRequest("GET", fallback, nil)
			if err != nil {
				fmt.Println("Failed to create request for fallback url.")
				return "", err
			}
			req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
			do, err = http.DefaultClient.Do(req)
			if err != nil {
				fmt.Println("Failed to get cover from fallback url.")
				return "", err
			}
			defer do.Body.Close()
			if do.StatusCode != http.StatusOK {
				fmt.Println(fallback)
				return "", errors.New(do.Status)
			}
		} else {
			return "", errors.New(do.Status)
		}
	}
	f, err := os.Create(covPath)
	if err != nil {
		return "", err
	}
	defer f.Close()
	_, err = io.Copy(f, do.Body)
	if err != nil {
		return "", err
	}
	return covPath, nil
}

func writeLyrics(sanAlbumFolder, filename string, lrc string) error {
	lyricspath := filepath.Join(sanAlbumFolder, filename)
	f, err := os.Create(lyricspath)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.WriteString(lrc)
	if err != nil {
		return err
	}
	return nil
}

func contains(slice []string, item string) bool {
	for _, v := range slice {
		if v == item {
			return true
		}
	}
	return false
}

func setDlFlags(quality string) {
	dl_atmos = false
	dl_aac = false

	switch quality {
	case "atmos":
		dl_atmos = true
		fmt.Println("Quality set to: Dolby Atmos")
	case "aac":
		dl_aac = true
		*aac_type = "aac"
		fmt.Println("Quality set to: High-Quality (AAC)")
	case "alac":
		fmt.Println("Quality set to: Lossless (ALAC)")
	}
}

func handleSearch(searchType string, queryParts []string, token string) (string, error) {
	selection, err := apputils.HandleSearch(searchType, queryParts, token, Config.Storefront, Config.Language)
	if err != nil {
		return "", err
	}
	if selection == nil || selection.URL == "" {
		return "", nil
	}
	if selection.IsSong {
		dl_song = true
	}
	if selection.Quality != "" && selection.Quality != "default" {
		setDlFlags(selection.Quality)
	}
	return selection.URL, nil
}

func convertIfNeeded(cfg *structs.ConfigSet, track *task.Track, lrc string, progress apputils.ProgressFunc) {
	coverPath := ""
	if strings.EqualFold(cfg.ConvertFormat, "flac") && track.SaveDir != "" {
		coverPath = findCoverFile(track.SaveDir)
	}
	apputils.ConvertIfNeeded(track, lrc, cfg, coverPath, progress)
}

// lrcLineTimestampRe matches a line-level LRC timestamp tag like "[00:12.34]" or
// "[01:02:123]"; lrcWordTimestampRe matches the word/syllable variant "<00:12.34>".
// Metadata tags ("[ti:...]", "[ar:...]") are left intact — only digit-shaped time
// tags are removed.
var lrcLineTimestampRe = regexp.MustCompile(`\[\d{1,2}:\d{2}(?:[.:]\d{1,3})?\]`)
var lrcWordTimestampRe = regexp.MustCompile(`<\d{1,2}:\d{2}(?:[.:]\d{1,3})?>`)

// stripLrcTimestamps turns timed lyrics into plain text by removing the per-line
// and per-word timestamp tags, then trimming the whitespace they leave behind.
// Used for the profile's "static" lyric mode (best-effort).
func stripLrcTimestamps(s string) string {
	s = lrcLineTimestampRe.ReplaceAllString(s, "")
	s = lrcWordTimestampRe.ReplaceAllString(s, "")
	lines := strings.Split(s, "\n")
	for i, ln := range lines {
		lines[i] = strings.TrimLeft(ln, " \t")
	}
	return strings.Join(lines, "\n")
}

func ripTrack(track *task.Track, token string, ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	if ctx.Err() != nil {
		return
	}
	var err error
	rs := ripStateFrom(ctx)
	ctr := rs.ctr()
	cfg := rs.ripConfig()
	ctr.Inc(&ctr.Total)
	var trackProgress apputils.ProgressFunc
	if tp := rs.progress(track); tp != nil {
		trackProgress = tp
		trackProgress("Preparing", 0, 0)
		defer trackProgress("Finished", 0, 0)
	}
	fmt.Printf("Track %d of %d: %s\n", track.TaskNum, track.TaskTotal, track.Type)

	if track.PreType == "playlists" && Config.UseSongInfoForPlaylist {
		track.GetAlbumData(token)
	}

	if track.Type == "music-videos" {
		err := mvDownloader(ctx, track.ID, track.SaveDir, token, track.Storefront, track, trackProgress)
		if err != nil {
			fmt.Println("\u26A0 Failed to dl MV:", err)
			ctr.Inc(&ctr.Error)
			return
		}
		ctr.Inc(&ctr.Success)
		return
	}

	needDlAacLc := false
	if (rs.aac() || track.Codec == "AAC") && (cfg.AacType == "aac-lc" || track.Codec == "AAC") {
		needDlAacLc = true
	}
	if track.WebM3u8 == "" && !needDlAacLc {
		if rs.atmos() {
			fmt.Println("Unavailable")
			recordDownloadFailure(ctx, "%s: Dolby Atmos is unavailable", track.Name)
			ctr.Inc(&ctr.Unavailable)
			return
		}
		fmt.Println("Unavailable, trying to dl aac-lc")
		needDlAacLc = true
	}

	// If AAC-LC, get M3U8 via WebPlayback; otherwise use track.M3u8
	var downloadM3u8 string
	if needDlAacLc {
		for attempt := 0; attempt < 2; attempt++ {
			wm := wmPool.Acquire()
			downloadM3u8, err = wm.WebPlayback(ctx, track.ID)
			wmPool.Release(wm)
			if err == nil {
				break
			}
			if attempt == 0 {
				fmt.Printf("WebPlayback attempt 1 failed, retrying with different instance: %v\n", err)
				time.Sleep(2 * time.Second)
			}
		}
		if err != nil {
			fmt.Println("Failed to get AAC-LC playback URL:", err)
			recordDownloadFailure(ctx, "%s: AAC-LC WebPlayback failed: %v", track.Name, err)
			ctr.Inc(&ctr.Unavailable)
			return
		}
	} else {
		for attempt := 0; attempt < 2; attempt++ {
			wm := wmPool.Acquire()
			downloadM3u8, err = wm.M3U8(ctx, track.ID)
			wmPool.Release(wm)
			if err == nil {
				break
			}
			if attempt == 0 {
				fmt.Printf("M3U8 attempt 1 failed, retrying with different instance: %v\n", err)
				time.Sleep(2 * time.Second)
			}
		}
		if err != nil {
			fmt.Println("Failed to get ALAC/Atmos playback URL:", err)
			recordDownloadFailure(ctx, "%s: ALAC/Atmos M3U8 failed: %v", track.Name, err)
			ctr.Inc(&ctr.Unavailable)
			return
		}
	}

	var Quality string
	if rs.atmos() {
		Quality = fmt.Sprintf("%dKbps", cfg.AtmosMax-2000)
	} else if needDlAacLc {
		Quality = "256Kbps"
	} else {
		var variantURL string
		variantURL, Quality, err = extractMedia(ctx, downloadM3u8, true)
		if err != nil {
			fmt.Println("Failed to extract quality from manifest.\n", err)
			recordDownloadFailure(ctx, "%s: failed to read quality from manifest: %v", track.Name, err)
			ctr.Inc(&ctr.Error)
			return
		}
		if variantURL != "" {
			downloadM3u8 = variantURL
		}
	}
	track.Quality = Quality

	stringsToJoin := []string{}
	if track.Resp.Attributes.IsAppleDigitalMaster {
		if cfg.AppleMasterChoice != "" {
			stringsToJoin = append(stringsToJoin, cfg.AppleMasterChoice)
		}
	}
	if track.Resp.Attributes.ContentRating == "explicit" {
		if cfg.ExplicitChoice != "" {
			stringsToJoin = append(stringsToJoin, cfg.ExplicitChoice)
		}
	}
	if track.Resp.Attributes.ContentRating == "clean" {
		if cfg.CleanChoice != "" {
			stringsToJoin = append(stringsToJoin, cfg.CleanChoice)
		}
	}
	Tag_string := strings.Join(stringsToJoin, " ")

	songName := strings.NewReplacer(
		"{SongId}", track.ID,
		"{SongNumer}", fmt.Sprintf("%02d", track.TaskNum),
		"{SongName}", LimitString(track.Resp.Attributes.Name),
		"{ArtistName}", LimitString(track.Resp.Attributes.ArtistName),
		"{DiscNumber}", fmt.Sprintf("%0d", track.Resp.Attributes.DiscNumber),
		"{TrackNumber}", fmt.Sprintf("%0d", track.Resp.Attributes.TrackNumber),
		"{Quality}", Quality,
		"{Tag}", Tag_string,
		"{Codec}", track.Codec,
	).Replace(Config.SongFileFormat)
	fmt.Println(songName)
	filename := fmt.Sprintf("%s.m4a", forbiddenNames.ReplaceAllString(songName, "_"))
	track.SaveName = filename
	trackPath := filepath.Join(track.SaveDir, track.SaveName)
	lrcFilename := fmt.Sprintf("%s.%s", forbiddenNames.ReplaceAllString(songName, "_"), cfg.LrcFormat)

	var convertedPath string
	conversionEnabled := cfg.ConvertAfterDownload &&
		cfg.ConvertFormat != "" &&
		strings.ToLower(cfg.ConvertFormat) != "copy"
	considerConverted := false
	if conversionEnabled {
		convertedPath = strings.TrimSuffix(trackPath, filepath.Ext(trackPath)) + "." + strings.ToLower(cfg.ConvertFormat)
		if !cfg.ConvertKeepOriginal {
			considerConverted = true
		}
	}
	var lrc string = ""
	if cfg.EmbedLrc || cfg.SaveLrcFile {
		lrcStr, err := lyrics.Get(track.Storefront, track.ID, cfg.LrcType, cfg.Language, cfg.LrcFormat, token, cfg.MediaUserToken)
		if err != nil {
			fmt.Println(err)
		} else {
			// "static" profile mode: fetch the timed lyrics, then strip the
			// per-line "[mm:ss.xx]" timestamps so the saved .lrc is plain text.
			if rs.lyricStripTimestamps() {
				lrcStr = stripLrcTimestamps(lrcStr)
			}
			if cfg.SaveLrcFile {
				err := writeLyrics(track.SaveDir, lrcFilename, lrcStr)
				if err != nil {
					fmt.Printf("Failed to write lyrics")
				}
			}
			if cfg.EmbedLrc {
				lrc = lrcStr
			}
		}
	}

	existsOriginal, err := fileExists(trackPath)
	if err != nil {
		fmt.Println("Failed to check if track exists.")
	}
	// --no-cache: drop any cached copy so the checks below fall through to a fresh rip.
	if rs.noCache() {
		if existsOriginal {
			fmt.Println("--no-cache: removing existing track to re-rip fresh.")
		}
		_ = os.Remove(trackPath)
		if considerConverted {
			_ = os.Remove(convertedPath)
		}
		existsOriginal = false
	}
	if existsOriginal {
		fmt.Println("Track already exists locally.")
		track.SavePath = trackPath
		track.SaveName = filepath.Base(trackPath)
		if conversionEnabled {
			if considerConverted {
				existsConverted, err2 := fileExists(convertedPath)
				if err2 == nil && existsConverted {
					track.SavePath = convertedPath
					track.SaveName = filepath.Base(convertedPath)
				} else {
					convertIfNeeded(&cfg, track, lrc, trackProgress)
				}
			} else {
				convertIfNeeded(&cfg, track, lrc, trackProgress)
			}
		}
		recordDownloadedTrack(ctx, track)
		ctr.Inc(&ctr.Success)
		rs.markDone(track.PreID, track.TaskNum)
		return
	}
	if considerConverted {
		existsConverted, err2 := fileExists(convertedPath)
		if err2 == nil && existsConverted {
			fmt.Println("Converted track already exists locally.")
			track.SavePath = convertedPath
			track.SaveName = filepath.Base(convertedPath)
			recordDownloadedTrack(ctx, track)
			ctr.Inc(&ctr.Success)
			rs.markDone(track.PreID, track.TaskNum)
			return
		}
	}

	{
		// Retry on a different pool client if the wrapper crashes mid-decrypt
		// (Invalid CKC from the Android CDM, connection reset, etc.). Mirrors
		// the M3U8/WebPlayback retry pattern added in b7d1ea9 — without this,
		// a single bad wrapper takes the whole track down.
		for attempt := 0; attempt < 2; attempt++ {
			wm := wmPool.Acquire()
			track.WorkerID = wm.ID()
			err = wmgrpc.DownloadAndDecrypt(ctx, wm, track.ID, downloadM3u8, trackPath, wmgrpc.ProgressFunc(trackProgress))
			wmPool.Release(wm)
			if err == nil {
				break
			}
			if attempt == 0 {
				fmt.Printf("DownloadAndDecrypt attempt 1 failed, retrying with different instance: %v\n", err)
				time.Sleep(2 * time.Second)
			}
		}
	}
	if err != nil {
		fmt.Println("Failed to download/decrypt:", err)
		recordDownloadFailure(ctx, "%s: download/decrypt failed: %v", track.Name, err)
		if strings.Contains(err.Error(), "Unavailable") {
			ctr.Inc(&ctr.Unavailable)
			return
		}
		ctr.Inc(&ctr.Error)
		return
	}

	{
		// Remux fMP4 → flat MP4 so go-mp4tag can tag it safely.
		// ffmpeg stream-copy is significantly faster than MP4Box -add.
		remuxPath := trackPath + ".remux.m4a"
		remuxCmd := exec.CommandContext(ctx, "ffmpeg",
			"-y",
			"-i", trackPath,
			"-c", "copy",
			remuxPath,
		)
		if out, err := remuxCmd.CombinedOutput(); err != nil {
			fmt.Printf("Failed to remux fMP4: %v. Output: %s\n", err, string(out))
			recordDownloadFailure(ctx, "%s: ffmpeg remux failed: %v, output: %s", track.Name, err, string(out))
			ctr.Inc(&ctr.Error)
			return
		}
		if err := os.Rename(remuxPath, trackPath); err != nil {
			fmt.Println("Failed to replace with remuxed file:", err)
			recordDownloadFailure(ctx, "%s: rename failed: %v", track.Name, err)
			ctr.Inc(&ctr.Error)
			return
		}
	}

	// Download per-track cover for playlist/station entries if needed.
	if cfg.EmbedCover {
		if (strings.Contains(track.PreID, "pl.") || strings.Contains(track.PreID, "ra.")) && Config.DlAlbumcoverForPlaylist {
			track.CoverPath, err = writeCover(track.SaveDir, track.ID, track.Resp.Attributes.Artwork.URL)
			if err != nil {
				fmt.Println("Failed to write cover.")
			}
		}
	}

	track.SavePath = trackPath
	err = writeMP4Tags(track, lrc)
	if err != nil {
		fmt.Println("\u26A0 Failed to write tags in media:", err)
		recordDownloadFailure(ctx, "%s: failed to write MP4 tags: %v", track.Name, err)
		ctr.Inc(&ctr.Unavailable)
		return
	}

	// Clean up per-track cover written above (not the shared album cover).
	if cfg.EmbedCover && (strings.Contains(track.PreID, "pl.") || strings.Contains(track.PreID, "ra.")) && Config.DlAlbumcoverForPlaylist {
		_ = os.Remove(track.CoverPath)
	}

	convertIfNeeded(&cfg, track, lrc, trackProgress)

	recordDownloadedTrack(ctx, track)
	ctr.Inc(&ctr.Success)
	rs.markDone(track.PreID, track.TaskNum)
}

func ripStation(albumId string, token string, storefront string, ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	rs := ripStateFrom(ctx)
	ctr := rs.ctr()
	station := task.NewStation(storefront, albumId)
	err := station.GetResp(Config.MediaUserToken, token, Config.Language)
	if err != nil {
		return err
	}
	fmt.Println(" -", station.Type)
	meta := station.Resp

	// Live radio (e.g. Apple Music 1) is a continuous broadcast with no finite asset —
	// the stream path would chase a VOD index that doesn't exist and die with a cryptic
	// subprocess error. Bail out early with a message the user can actually act on.
	if len(meta.Data) > 0 && meta.Data[0].Attributes.IsLive {
		return errors.New("this is a live radio station (continuous broadcast) — it can't be downloaded as a file")
	}

	var Codec string
	if rs.atmos() {
		Codec = "ATMOS"
	} else if rs.aac() {
		Codec = "AAC"
	} else {
		Codec = "ALAC"
	}
	station.Codec = Codec
	var singerFoldername string
	if Config.ArtistFolderFormat != "" {
		singerFoldername = strings.NewReplacer(
			"{ArtistName}", "Apple Music Station",
			"{ArtistId}", "",
			"{UrlArtistName}", "Apple Music Station",
		).Replace(Config.ArtistFolderFormat)
		if strings.HasSuffix(singerFoldername, ".") {
			singerFoldername = strings.ReplaceAll(singerFoldername, ".", "")
		}
		singerFoldername = strings.TrimSpace(singerFoldername)
		fmt.Println(singerFoldername)
	}
	singerFolder := filepath.Join(Config.AlacSaveFolder, forbiddenNames.ReplaceAllString(singerFoldername, "_"))
	if rs.atmos() {
		singerFolder = filepath.Join(Config.AtmosSaveFolder, forbiddenNames.ReplaceAllString(singerFoldername, "_"))
	}
	if rs.aac() {
		singerFolder = filepath.Join(Config.AacSaveFolder, forbiddenNames.ReplaceAllString(singerFoldername, "_"))
	}
	os.MkdirAll(singerFolder, os.ModePerm)
	station.SaveDir = singerFolder

	playlistFolder := strings.NewReplacer(
		"{ArtistName}", "Apple Music Station",
		"{PlaylistName}", LimitString(station.Name),
		"{PlaylistId}", station.ID,
		"{Quality}", "",
		"{Codec}", Codec,
		"{Tag}", "",
	).Replace(Config.PlaylistFolderFormat)
	if strings.HasSuffix(playlistFolder, ".") {
		playlistFolder = strings.ReplaceAll(playlistFolder, ".", "")
	}
	playlistFolder = strings.TrimSpace(playlistFolder)
	playlistFolderPath := filepath.Join(singerFolder, forbiddenNames.ReplaceAllString(playlistFolder, "_"))
	os.MkdirAll(playlistFolderPath, os.ModePerm)
	station.SaveName = playlistFolder
	fmt.Println(playlistFolder)

	covPath, err := writeCover(playlistFolderPath, "cover", meta.Data[0].Attributes.Artwork.URL)
	if err != nil {
		fmt.Println("Failed to write cover.")
	}
	station.CoverPath = covPath

	if ripStateFrom(ctx).ripConfig().SaveAnimatedArtwork && meta.Data[0].Attributes.EditorialVideo.MotionSquare.Video != "" {
		fmt.Println("Found Animation Artwork.")

		motionvideoUrlSquare, err := extractVideo(ctx, meta.Data[0].Attributes.EditorialVideo.MotionSquare.Video)
		if err != nil {
			fmt.Println("no motion video square.\n", err)
		} else {
			exists, err := fileExists(filepath.Join(playlistFolderPath, "square_animated_artwork.mp4"))
			if err != nil {
				fmt.Println("Failed to check if animated artwork square exists.")
			}
			if exists {
				fmt.Println("Animated artwork square already exists locally.")
			} else {
				fmt.Println("Animation Artwork Square Downloading...")
				cmd := exec.CommandContext(ctx, "ffmpeg", "-loglevel", "quiet", "-y", "-i", motionvideoUrlSquare, "-c", "copy", filepath.Join(playlistFolderPath, "square_animated_artwork.mp4"))
				if err := cmd.Run(); err != nil {
					fmt.Printf("animated artwork square dl err: %v\n", err)
				} else {
					fmt.Println("Animation Artwork Square Downloaded")
				}
			}
		}

		if Config.EmbyAnimatedArtwork {
			cmd3 := exec.CommandContext(ctx, "ffmpeg", "-i", filepath.Join(playlistFolderPath, "square_animated_artwork.mp4"), "-vf", "scale=440:-1", "-r", "24", "-f", "gif", filepath.Join(playlistFolderPath, "folder.jpg"))
			if err := cmd3.Run(); err != nil {
				fmt.Printf("animated artwork square to gif err: %v\n", err)
			}
		}
	}
	if station.Type == "stream" {
		ctr.Total++
		if rs.isDone(station.ID, 1) {
			ctr.Success++
			return nil
		}
		songName := strings.NewReplacer(
			"{SongId}", station.ID,
			"{SongNumer}", "01",
			"{SongName}", LimitString(station.Name),
			"{ArtistName}", "Apple Music Station",
			"{DiscNumber}", "1",
			"{TrackNumber}", "1",
			"{Quality}", "256Kbps",
			"{Tag}", "",
			"{Codec}", "AAC",
		).Replace(Config.SongFileFormat)
		fmt.Println(songName)
		trackPath := filepath.Join(playlistFolderPath, fmt.Sprintf("%s.m4a", forbiddenNames.ReplaceAllString(songName, "_")))
		exists, _ := fileExists(trackPath)
		if exists {
			ctr.Success++
			rs.markDone(station.ID, 1)

			fmt.Println("Radio already exists locally.")
			return nil
		}
		assetsUrl, _, err := ampapi.GetStationAssetsUrlAndServerUrl(station.ID, Config.MediaUserToken, token)
		if err != nil {
			fmt.Println("Failed to get station assets url.", err)
			ctr.Error++
			return err
		}
		trackM3U8 := strings.ReplaceAll(assetsUrl, "index.m3u8", "256/prog_index.m3u8")
		wm := wmPool.Acquire()
		err = wmgrpc.DownloadAndDecrypt(ctx, wm, station.ID, trackM3U8, trackPath, nil)
		wmPool.Release(wm)
		if err != nil {
			fmt.Println("Failed to download station stream.", err)
			ctr.Error++
			return err
		}
		tags := []string{
			"tool=",
			"disk=1/1",
			"track=1",
			"tracknum=1/1",
			fmt.Sprintf("artist=%s", "Apple Music Station"),
			fmt.Sprintf("performer=%s", "Apple Music Station"),
			fmt.Sprintf("album_artist=%s", "Apple Music Station"),
			fmt.Sprintf("album=%s", station.Name),
			fmt.Sprintf("title=%s", station.Name),
		}
		if Config.EmbedCover {
			tags = append(tags, fmt.Sprintf("cover=%s", station.CoverPath))
		}
		tagsString := strings.Join(tags, ":")
		cmd := exec.CommandContext(ctx, "MP4Box", "-itags", tagsString, trackPath)
		if err := cmd.Run(); err != nil {
			fmt.Printf("Embed failed: %v\n", err)
		}
		ctr.Success++
		rs.markDone(station.ID, 1)
		return nil
	}

	for i := range station.Tracks {
		station.Tracks[i].CoverPath = covPath
		station.Tracks[i].SaveDir = playlistFolderPath
		station.Tracks[i].Codec = Codec
	}

	trackTotal := len(station.Tracks)
	arr := make([]int, trackTotal)
	for i := 0; i < trackTotal; i++ {
		arr[i] = i + 1
	}
	var selected []int

	if true {
		selected = arr
	}

	concurrency := rs.wrapperBudget()
	// Record the full planned track count up front so the scheduler's head
	// remaining-tracks gate sees album-remaining, not the sem-capped in-flight count.
	rs.planTracks(len(selected))
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	for i := range station.Tracks {
		i++
		if !isInArray(selected, i) {
			continue
		}
		trackIdx := i - 1
		wg.Add(1)
		sem <- struct{}{}
		go func(idx int) {
			defer wg.Done()
			defer rs.trackDone()
			defer func() { <-sem }()
			ripTrack(&station.Tracks[idx], token, ctx)
		}(trackIdx)
	}
	wg.Wait()
	return nil
}

// firstNonEmpty returns the first non-empty string from vals, or "".
func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

// ripArtwork downloads only the cover image and, when available, the animated
// (motion) artwork for an album, playlist, or station — no tracks. A song URL
// resolves to its parent album's artwork (checkUrl matches both). Output paths are
// appended to lastDownloadedPaths so the Telegram layer delivers the cover as a
// photo and the motion artwork as a video. Unlike the per-track rips, this ignores
// Config.SaveAnimatedArtwork — the user asked for artwork explicitly via -art.
func ripArtwork(link string, token string, storefront string, ctx context.Context) error {
	var artworkURL, motionURL, name, idForDir string

	if sf, id := checkUrlPlaylist(link); id != "" {
		if sf != "" {
			storefront = sf
		}
		pl := task.NewPlaylist(storefront, id)
		if err := pl.GetResp(token, Config.Language); err != nil {
			return err
		}
		if len(pl.Resp.Data) == 0 {
			return errors.New("playlist not found")
		}
		a := pl.Resp.Data[0].Attributes
		artworkURL = a.Artwork.URL
		motionURL = firstNonEmpty(a.EditorialVideo.MotionDetailSquare.Video, a.EditorialVideo.MotionSquare.Video, a.EditorialVideo.MotionDetailTall.Video, a.EditorialVideo.MotionTall.Video)
		name = a.Name
		idForDir = id
	} else if sf, id := checkUrlStation(link); id != "" {
		if sf != "" {
			storefront = sf
		}
		st := task.NewStation(storefront, id)
		if err := st.GetResp(Config.MediaUserToken, token, Config.Language); err != nil {
			return err
		}
		if len(st.Resp.Data) == 0 {
			return errors.New("station not found")
		}
		a := st.Resp.Data[0].Attributes
		artworkURL = a.Artwork.URL
		motionURL = firstNonEmpty(a.EditorialVideo.MotionSquare.Video, a.EditorialVideo.MotionDetailSquare.Video)
		name = st.Name
		idForDir = id
	} else if sf, id := checkUrl(link); id != "" {
		if sf != "" {
			storefront = sf
		}
		al := task.NewAlbum(storefront, id)
		if err := al.GetResp(token, Config.Language); err != nil {
			return err
		}
		if len(al.Resp.Data) == 0 {
			return errors.New("album not found")
		}
		a := al.Resp.Data[0].Attributes
		artworkURL = a.Artwork.URL
		motionURL = firstNonEmpty(a.EditorialVideo.MotionDetailSquare.Video, a.EditorialVideo.MotionSquare.Video, a.EditorialVideo.MotionDetailTall.Video, a.EditorialVideo.MotionTall.Video)
		name = a.Name
		idForDir = id
	} else {
		return errors.New("unsupported link for artwork (use an album, playlist, or station link)")
	}

	if artworkURL == "" {
		return errors.New("no artwork available for this item")
	}

	// Dedicated output dir keyed by ID so cover.jpg never collides with a real download.
	baseDir := Config.AlacSaveFolder
	if baseDir == "" {
		baseDir = "."
	}
	outDir := filepath.Join(baseDir, "artwork", forbiddenNames.ReplaceAllString(idForDir, "_"))
	if err := os.MkdirAll(outDir, os.ModePerm); err != nil {
		return err
	}

	coverName := "cover"
	if name != "" {
		coverName = forbiddenNames.ReplaceAllString(LimitString(name), "_")
	}
	coverPath, err := writeCover(outDir, coverName, artworkURL)
	if err != nil {
		return fmt.Errorf("failed to download cover: %w", err)
	}
	ripStateFrom(ctx).addPath(coverPath)

	// Motion artwork is best-effort; its absence (or a muxing failure) is not fatal.
	if motionURL != "" {
		if hlsURL, err := extractVideo(ctx, motionURL); err != nil {
			fmt.Println("animated artwork unavailable:", err)
		} else {
			motionPath := filepath.Join(outDir, coverName+"_animated.mp4")
			cmd := exec.CommandContext(ctx, "ffmpeg", "-loglevel", "quiet", "-y", "-i", hlsURL, "-c", "copy", motionPath)
			if err := cmd.Run(); err != nil {
				fmt.Printf("animated artwork download failed: %v\n", err)
			} else {
				ripStateFrom(ctx).addPath(motionPath)
			}
		}
	}
	return nil
}

func ripAlbum(albumId string, token string, storefront string, urlArg_i string, forceAAC bool, ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	rs := ripStateFrom(ctx)
	ctr := rs.ctr()
	album := task.NewAlbum(storefront, albumId)
	err := album.GetResp(token, Config.Language)
	if err != nil {
		fmt.Println("Failed to get album response.")
		return err
	}
	meta := album.Resp
	if debug_mode {
		fmt.Println(meta.Data[0].Attributes.ArtistName)
		fmt.Println(meta.Data[0].Attributes.Name)

		for trackNum, track := range meta.Data[0].Relationships.Tracks.Data {
			trackNum++
			fmt.Printf("\nTrack %d of %d:\n", trackNum, len(meta.Data[0].Relationships.Tracks.Data))
			fmt.Printf("%02d. %s\n", trackNum, track.Attributes.Name)

			manifest, err := ampapi.GetSongResp(storefront, track.ID, album.Language, token)
			if err != nil {
				fmt.Printf("Failed to get manifest for track %d: %v\n", trackNum, err)
				continue
			}

			var m3u8Url string
			if manifest.Data[0].Attributes.ExtendedAssetUrls.EnhancedHls != "" {
				m3u8Url = manifest.Data[0].Attributes.ExtendedAssetUrls.EnhancedHls
			}

			_, _, err = extractMedia(ctx, m3u8Url, true)
			if err != nil {
				fmt.Printf("Failed to extract quality info for track %d: %v\n", trackNum, err)
				continue
			}
		}
		return nil
	}
	var Codec string
	if rs.atmos() {
		Codec = "ATMOS"
	} else if rs.aac() || forceAAC {
		Codec = "AAC"
	} else {
		Codec = "ALAC"
	}
	album.Codec = Codec
	var singerFoldername string
	if Config.ArtistFolderFormat != "" {
		if len(meta.Data[0].Relationships.Artists.Data) > 0 {
			singerFoldername = strings.NewReplacer(
				"{UrlArtistName}", LimitString(meta.Data[0].Attributes.ArtistName),
				"{ArtistName}", LimitString(meta.Data[0].Attributes.ArtistName),
				"{ArtistId}", meta.Data[0].Relationships.Artists.Data[0].ID,
			).Replace(Config.ArtistFolderFormat)
		} else {
			singerFoldername = strings.NewReplacer(
				"{UrlArtistName}", LimitString(meta.Data[0].Attributes.ArtistName),
				"{ArtistName}", LimitString(meta.Data[0].Attributes.ArtistName),
				"{ArtistId}", "",
			).Replace(Config.ArtistFolderFormat)
		}
		if strings.HasSuffix(singerFoldername, ".") {
			singerFoldername = strings.ReplaceAll(singerFoldername, ".", "")
		}
		singerFoldername = strings.TrimSpace(singerFoldername)
		fmt.Println(singerFoldername)
	}
	singerFolder := filepath.Join(Config.AlacSaveFolder, forbiddenNames.ReplaceAllString(singerFoldername, "_"))
	if rs.atmos() {
		singerFolder = filepath.Join(Config.AtmosSaveFolder, forbiddenNames.ReplaceAllString(singerFoldername, "_"))
	} else if rs.aac() || forceAAC {
		singerFolder = filepath.Join(Config.AacSaveFolder, forbiddenNames.ReplaceAllString(singerFoldername, "_"))
	}
	os.MkdirAll(singerFolder, os.ModePerm)
	album.SaveDir = singerFolder
	var Quality string
	if strings.Contains(Config.AlbumFolderFormat, "Quality") {
		if rs.atmos() {
			Quality = fmt.Sprintf("%dKbps", Config.AtmosMax-2000)
		} else if (rs.aac() || forceAAC) && (Config.AacType == "aac-lc" || forceAAC) {
			Quality = "256Kbps"
		} else {
			manifest1, err := ampapi.GetSongResp(storefront, meta.Data[0].Relationships.Tracks.Data[0].ID, album.Language, token)
			if err != nil {
				fmt.Println("Failed to get manifest.\n", err)
			} else {
				if manifest1.Data[0].Attributes.ExtendedAssetUrls.EnhancedHls == "" {
					Codec = "AAC"
					Quality = "256Kbps"
				} else {
					_, Quality, err = extractMedia(ctx, manifest1.Data[0].Attributes.ExtendedAssetUrls.EnhancedHls, true)
					if err != nil {
						fmt.Println("Failed to extract quality from manifest.\n", err)
					}
				}
			}
		}
	}
	stringsToJoin := []string{}
	if meta.Data[0].Attributes.IsAppleDigitalMaster || meta.Data[0].Attributes.IsMasteredForItunes {
		if Config.AppleMasterChoice != "" {
			stringsToJoin = append(stringsToJoin, Config.AppleMasterChoice)
		}
	}
	if meta.Data[0].Attributes.ContentRating == "explicit" {
		if Config.ExplicitChoice != "" {
			stringsToJoin = append(stringsToJoin, Config.ExplicitChoice)
		}
	}
	if meta.Data[0].Attributes.ContentRating == "clean" {
		if Config.CleanChoice != "" {
			stringsToJoin = append(stringsToJoin, Config.CleanChoice)
		}
	}
	Tag_string := strings.Join(stringsToJoin, " ")
	var albumFolderName string
	albumFolderName = strings.NewReplacer(
		"{ReleaseDate}", meta.Data[0].Attributes.ReleaseDate,
		"{ReleaseYear}", meta.Data[0].Attributes.ReleaseDate[:4],
		"{ArtistName}", LimitString(meta.Data[0].Attributes.ArtistName),
		"{AlbumName}", LimitString(meta.Data[0].Attributes.Name),
		"{UPC}", meta.Data[0].Attributes.Upc,
		"{RecordLabel}", meta.Data[0].Attributes.RecordLabel,
		"{Copyright}", meta.Data[0].Attributes.Copyright,
		"{AlbumId}", albumId,
		"{Quality}", Quality,
		"{Codec}", Codec,
		"{Tag}", Tag_string,
	).Replace(Config.AlbumFolderFormat)

	if strings.HasSuffix(albumFolderName, ".") {
		albumFolderName = strings.ReplaceAll(albumFolderName, ".", "")
	}
	albumFolderName = strings.TrimSpace(albumFolderName)
	albumFolderPath := filepath.Join(singerFolder, forbiddenNames.ReplaceAllString(albumFolderName, "_"))
	os.MkdirAll(albumFolderPath, os.ModePerm)
	album.SaveName = albumFolderName
	fmt.Println(albumFolderName)
	if Config.SaveArtistCover && len(meta.Data[0].Relationships.Artists.Data) > 0 {
		if meta.Data[0].Relationships.Artists.Data[0].Attributes.Artwork.Url != "" {
			_, err = writeCover(singerFolder, "folder", meta.Data[0].Relationships.Artists.Data[0].Attributes.Artwork.Url)
			if err != nil {
				fmt.Println("Failed to write artist cover.")
			}
		}
	}
	covPath, err := writeCover(albumFolderPath, "cover", meta.Data[0].Attributes.Artwork.URL)
	if err != nil {
		fmt.Println("Failed to write cover.")
	}
	if ripStateFrom(ctx).ripConfig().SaveAnimatedArtwork && meta.Data[0].Attributes.EditorialVideo.MotionDetailSquare.Video != "" {
		fmt.Println("Found Animation Artwork.")

		motionvideoUrlSquare, err := extractVideo(ctx, meta.Data[0].Attributes.EditorialVideo.MotionDetailSquare.Video)
		if err != nil {
			fmt.Println("no motion video square.\n", err)
		} else {
			exists, err := fileExists(filepath.Join(albumFolderPath, "square_animated_artwork.mp4"))
			if err != nil {
				fmt.Println("Failed to check if animated artwork square exists.")
			}
			if exists {
				fmt.Println("Animated artwork square already exists locally.")
			} else {
				fmt.Println("Animation Artwork Square Downloading...")
				cmd := exec.CommandContext(ctx, "ffmpeg", "-loglevel", "quiet", "-y", "-i", motionvideoUrlSquare, "-c", "copy", filepath.Join(albumFolderPath, "square_animated_artwork.mp4"))
				if err := cmd.Run(); err != nil {
					fmt.Printf("animated artwork square dl err: %v\n", err)
				} else {
					fmt.Println("Animation Artwork Square Downloaded")
				}
			}
		}

		if Config.EmbyAnimatedArtwork {
			cmd3 := exec.CommandContext(ctx, "ffmpeg", "-i", filepath.Join(albumFolderPath, "square_animated_artwork.mp4"), "-vf", "scale=440:-1", "-r", "24", "-f", "gif", filepath.Join(albumFolderPath, "folder.jpg"))
			if err := cmd3.Run(); err != nil {
				fmt.Printf("animated artwork square to gif err: %v\n", err)
			}
		}

		motionvideoUrlTall, err := extractVideo(ctx, meta.Data[0].Attributes.EditorialVideo.MotionDetailTall.Video)
		if err != nil {
			fmt.Println("no motion video tall.\n", err)
		} else {
			exists, err := fileExists(filepath.Join(albumFolderPath, "tall_animated_artwork.mp4"))
			if err != nil {
				fmt.Println("Failed to check if animated artwork tall exists.")
			}
			if exists {
				fmt.Println("Animated artwork tall already exists locally.")
			} else {
				fmt.Println("Animation Artwork Tall Downloading...")
				cmd := exec.CommandContext(ctx, "ffmpeg", "-loglevel", "quiet", "-y", "-i", motionvideoUrlTall, "-c", "copy", filepath.Join(albumFolderPath, "tall_animated_artwork.mp4"))
				if err := cmd.Run(); err != nil {
					fmt.Printf("animated artwork tall dl err: %v\n", err)
				} else {
					fmt.Println("Animation Artwork Tall Downloaded")
				}
			}
		}
	}
	for i := range album.Tracks {
		album.Tracks[i].CoverPath = covPath
		album.Tracks[i].SaveDir = albumFolderPath
		album.Tracks[i].Codec = Codec
	}
	trackTotal := len(meta.Data[0].Relationships.Tracks.Data)
	arr := make([]int, trackTotal)
	for i := 0; i < trackTotal; i++ {
		arr[i] = i + 1
	}

	if rs.song() {
		if urlArg_i == "" {
		} else {
			for i := range album.Tracks {
				if urlArg_i == album.Tracks[i].ID {
					ripTrack(&album.Tracks[i], token, ctx)
					return nil
				}
			}
			return ripAlbumSongFallback(album, urlArg_i, token, storefront, albumFolderPath, covPath, Codec, forceAAC, ctx)
		}
		return nil
	}
	var selected []int
	if !rs.selectMode() {
		selected = arr
	} else {
		selected = album.ShowSelect()
	}

	// Run track downloads in parallel, one goroutine per pool slot. The semaphore
	// (sem) limits concurrency to this rip's wrapper budget — the full pool for the
	// head (and for CLI / flag-off), a smaller k for a borrower — so we never hold
	// more wrapper clients than granted.
	concurrency := rs.wrapperBudget()
	// Record the full planned track count up front so the scheduler's head
	// remaining-tracks gate sees album-remaining, not the sem-capped in-flight count.
	// (Already-downloaded tracks skipped on resume keep this slightly conservative,
	// which only makes the head marginally more willing to lend — harmless.)
	rs.planTracks(len(selected))
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	for i := range album.Tracks {
		i++
		if rs.isDone(albumId, i) {
			ctr.Inc(&ctr.Total)
			ctr.Inc(&ctr.Success)
			continue
		}
		if !isInArray(selected, i) {
			continue
		}
		trackIdx := i - 1
		wg.Add(1)
		sem <- struct{}{}
		go func(idx int) {
			defer wg.Done()
			defer rs.trackDone()
			defer func() { <-sem }()
			ripTrack(&album.Tracks[idx], token, ctx)
		}(trackIdx)
	}
	wg.Wait()
	return nil

}
func ripPlaylist(playlistId string, token string, storefront string, forceAAC bool, ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	rs := ripStateFrom(ctx)
	ctr := rs.ctr()
	playlist := task.NewPlaylist(storefront, playlistId)
	err := playlist.GetResp(token, Config.Language)
	if err != nil {
		fmt.Println("Failed to get playlist response.")
		return err
	}
	meta := playlist.Resp
	if debug_mode {
		fmt.Println(meta.Data[0].Attributes.ArtistName)
		fmt.Println(meta.Data[0].Attributes.Name)

		for trackNum, track := range meta.Data[0].Relationships.Tracks.Data {
			trackNum++
			fmt.Printf("\nTrack %d of %d:\n", trackNum, len(meta.Data[0].Relationships.Tracks.Data))
			fmt.Printf("%02d. %s\n", trackNum, track.Attributes.Name)

			manifest, err := ampapi.GetSongResp(storefront, track.ID, playlist.Language, token)
			if err != nil {
				fmt.Printf("Failed to get manifest for track %d: %v\n", trackNum, err)
				continue
			}

			var m3u8Url string
			if manifest.Data[0].Attributes.ExtendedAssetUrls.EnhancedHls != "" {
				m3u8Url = manifest.Data[0].Attributes.ExtendedAssetUrls.EnhancedHls
			}

			_, _, err = extractMedia(ctx, m3u8Url, true)
			if err != nil {
				fmt.Printf("Failed to extract quality info for track %d: %v\n", trackNum, err)
				continue
			}
		}
		return nil
	}
	var Codec string
	if rs.atmos() {
		Codec = "ATMOS"
	} else if rs.aac() || forceAAC {
		Codec = "AAC"
	} else {
		Codec = "ALAC"
	}
	playlist.Codec = Codec
	var singerFoldername string
	if Config.ArtistFolderFormat != "" {
		singerFoldername = strings.NewReplacer(
			"{ArtistName}", "Apple Music",
			"{ArtistId}", "",
			"{UrlArtistName}", "Apple Music",
		).Replace(Config.ArtistFolderFormat)
		if strings.HasSuffix(singerFoldername, ".") {
			singerFoldername = strings.ReplaceAll(singerFoldername, ".", "")
		}
		singerFoldername = strings.TrimSpace(singerFoldername)
		fmt.Println(singerFoldername)
	}
	singerFolder := filepath.Join(Config.AlacSaveFolder, forbiddenNames.ReplaceAllString(singerFoldername, "_"))
	if rs.atmos() {
		singerFolder = filepath.Join(Config.AtmosSaveFolder, forbiddenNames.ReplaceAllString(singerFoldername, "_"))
	} else if rs.aac() || forceAAC {
		singerFolder = filepath.Join(Config.AacSaveFolder, forbiddenNames.ReplaceAllString(singerFoldername, "_"))
	}
	os.MkdirAll(singerFolder, os.ModePerm)
	playlist.SaveDir = singerFolder

	var Quality string
	if strings.Contains(Config.AlbumFolderFormat, "Quality") {
		if rs.atmos() {
			Quality = fmt.Sprintf("%dKbps", Config.AtmosMax-2000)
		} else if (rs.aac() || forceAAC) && (Config.AacType == "aac-lc" || forceAAC) {
			Quality = "256Kbps"
		} else {
			manifest1, err := ampapi.GetSongResp(storefront, meta.Data[0].Relationships.Tracks.Data[0].ID, playlist.Language, token)
			if err != nil {
				fmt.Println("Failed to get manifest.\n", err)
			} else {
				if manifest1.Data[0].Attributes.ExtendedAssetUrls.EnhancedHls == "" {
					Codec = "AAC"
					Quality = "256Kbps"
				} else {
					_, Quality, err = extractMedia(ctx, manifest1.Data[0].Attributes.ExtendedAssetUrls.EnhancedHls, true)
					if err != nil {
						fmt.Println("Failed to extract quality from manifest.\n", err)
					}
				}
			}
		}
	}
	stringsToJoin := []string{}
	if meta.Data[0].Attributes.IsAppleDigitalMaster || meta.Data[0].Attributes.IsMasteredForItunes {
		if Config.AppleMasterChoice != "" {
			stringsToJoin = append(stringsToJoin, Config.AppleMasterChoice)
		}
	}
	if meta.Data[0].Attributes.ContentRating == "explicit" {
		if Config.ExplicitChoice != "" {
			stringsToJoin = append(stringsToJoin, Config.ExplicitChoice)
		}
	}
	if meta.Data[0].Attributes.ContentRating == "clean" {
		if Config.CleanChoice != "" {
			stringsToJoin = append(stringsToJoin, Config.CleanChoice)
		}
	}
	Tag_string := strings.Join(stringsToJoin, " ")
	playlistFolder := strings.NewReplacer(
		"{ArtistName}", "Apple Music",
		"{PlaylistName}", LimitString(meta.Data[0].Attributes.Name),
		"{PlaylistId}", playlistId,
		"{Quality}", Quality,
		"{Codec}", Codec,
		"{Tag}", Tag_string,
	).Replace(Config.PlaylistFolderFormat)
	if strings.HasSuffix(playlistFolder, ".") {
		playlistFolder = strings.ReplaceAll(playlistFolder, ".", "")
	}
	playlistFolder = strings.TrimSpace(playlistFolder)
	playlistFolderPath := filepath.Join(singerFolder, forbiddenNames.ReplaceAllString(playlistFolder, "_"))
	os.MkdirAll(playlistFolderPath, os.ModePerm)
	playlist.SaveName = playlistFolder
	fmt.Println(playlistFolder)
	covPath, err := writeCover(playlistFolderPath, "cover", meta.Data[0].Attributes.Artwork.URL)
	if err != nil {
		fmt.Println("Failed to write cover.")
	}

	for i := range playlist.Tracks {
		playlist.Tracks[i].CoverPath = covPath
		playlist.Tracks[i].SaveDir = playlistFolderPath
		playlist.Tracks[i].Codec = Codec
	}

	if ripStateFrom(ctx).ripConfig().SaveAnimatedArtwork && meta.Data[0].Attributes.EditorialVideo.MotionDetailSquare.Video != "" {
		fmt.Println("Found Animation Artwork.")

		motionvideoUrlSquare, err := extractVideo(ctx, meta.Data[0].Attributes.EditorialVideo.MotionDetailSquare.Video)
		if err != nil {
			fmt.Println("no motion video square.\n", err)
		} else {
			exists, err := fileExists(filepath.Join(playlistFolderPath, "square_animated_artwork.mp4"))
			if err != nil {
				fmt.Println("Failed to check if animated artwork square exists.")
			}
			if exists {
				fmt.Println("Animated artwork square already exists locally.")
			} else {
				fmt.Println("Animation Artwork Square Downloading...")
				cmd := exec.CommandContext(ctx, "ffmpeg", "-loglevel", "quiet", "-y", "-i", motionvideoUrlSquare, "-c", "copy", filepath.Join(playlistFolderPath, "square_animated_artwork.mp4"))
				if err := cmd.Run(); err != nil {
					fmt.Printf("animated artwork square dl err: %v\n", err)
				} else {
					fmt.Println("Animation Artwork Square Downloaded")
				}
			}
		}

		if Config.EmbyAnimatedArtwork {
			cmd3 := exec.CommandContext(ctx, "ffmpeg", "-i", filepath.Join(playlistFolderPath, "square_animated_artwork.mp4"), "-vf", "scale=440:-1", "-r", "24", "-f", "gif", filepath.Join(playlistFolderPath, "folder.jpg"))
			if err := cmd3.Run(); err != nil {
				fmt.Printf("animated artwork square to gif err: %v\n", err)
			}
		}

		motionvideoUrlTall, err := extractVideo(ctx, meta.Data[0].Attributes.EditorialVideo.MotionDetailTall.Video)
		if err != nil {
			fmt.Println("no motion video tall.\n", err)
		} else {
			exists, err := fileExists(filepath.Join(playlistFolderPath, "tall_animated_artwork.mp4"))
			if err != nil {
				fmt.Println("Failed to check if animated artwork tall exists.")
			}
			if exists {
				fmt.Println("Animated artwork tall already exists locally.")
			} else {
				fmt.Println("Animation Artwork Tall Downloading...")
				cmd := exec.CommandContext(ctx, "ffmpeg", "-loglevel", "quiet", "-y", "-i", motionvideoUrlTall, "-c", "copy", filepath.Join(playlistFolderPath, "tall_animated_artwork.mp4"))
				if err := cmd.Run(); err != nil {
					fmt.Printf("animated artwork tall dl err: %v\n", err)
				} else {
					fmt.Println("Animation Artwork Tall Downloaded")
				}
			}
		}
	}
	trackTotal := len(meta.Data[0].Relationships.Tracks.Data)
	arr := make([]int, trackTotal)
	for i := 0; i < trackTotal; i++ {
		arr[i] = i + 1
	}
	var selected []int

	if !rs.selectMode() {
		selected = arr
	} else {
		selected = playlist.ShowSelect()
	}

	concurrency := rs.wrapperBudget()
	// Record the full planned track count up front so the scheduler's head
	// remaining-tracks gate sees album-remaining, not the sem-capped in-flight count.
	// (Already-downloaded tracks skipped on resume keep this slightly conservative,
	// which only makes the head marginally more willing to lend — harmless.)
	rs.planTracks(len(selected))
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	for i := range playlist.Tracks {
		i++
		if rs.isDone(playlistId, i) {
			ctr.Inc(&ctr.Total)
			ctr.Inc(&ctr.Success)
			continue
		}
		if !isInArray(selected, i) {
			continue
		}
		trackIdx := i - 1
		wg.Add(1)
		sem <- struct{}{}
		go func(idx int) {
			defer wg.Done()
			defer rs.trackDone()
			defer func() { <-sem }()
			ripTrack(&playlist.Tracks[idx], token, ctx)
		}(trackIdx)
	}
	wg.Wait()
	return nil
}

func writeMP4Tags(track *task.Track, lrc string) error {
	t := &mp4tag.MP4Tags{
		Title:      track.Resp.Attributes.Name,
		TitleSort:  track.Resp.Attributes.Name,
		Artist:     track.Resp.Attributes.ArtistName,
		ArtistSort: track.Resp.Attributes.ArtistName,
		Custom: map[string]string{
			"PERFORMER":   track.Resp.Attributes.ArtistName,
			"RELEASETIME": track.Resp.Attributes.ReleaseDate,
			"ISRC":        track.Resp.Attributes.Isrc,
			"LABEL":       "",
			"UPC":         "",
		},
		Composer:     track.Resp.Attributes.ComposerName,
		ComposerSort: track.Resp.Attributes.ComposerName,
		CustomGenre:  track.Resp.Attributes.GenreNames[0],
		Lyrics:       lrc,
		TrackNumber:  int16(track.Resp.Attributes.TrackNumber),
		DiscNumber:   int16(track.Resp.Attributes.DiscNumber),
		Album:        track.Resp.Attributes.AlbumName,
		AlbumSort:    track.Resp.Attributes.AlbumName,
	}

	if track.PreType == "albums" {
		albumID, _ := strconv.ParseUint(track.PreID, 10, 64)
		t.ItunesAlbumID = int32(albumID)
	}

	if len(track.Resp.Relationships.Artists.Data) > 0 {
		artistID, _ := strconv.ParseUint(track.Resp.Relationships.Artists.Data[0].ID, 10, 64)
		t.ItunesArtistID = int32(artistID)
	}

	if (track.PreType == "playlists" || track.PreType == "stations") && !Config.UseSongInfoForPlaylist {
		t.DiscNumber = 1
		t.DiscTotal = 1
		t.TrackNumber = int16(track.TaskNum)
		t.TrackTotal = int16(track.TaskTotal)
		t.Album = track.PlaylistData.Attributes.Name
		t.AlbumSort = track.PlaylistData.Attributes.Name
		t.AlbumArtist = track.PlaylistData.Attributes.ArtistName
		t.AlbumArtistSort = track.PlaylistData.Attributes.ArtistName
	} else if (track.PreType == "playlists" || track.PreType == "stations") && Config.UseSongInfoForPlaylist {
		t.DiscTotal = int16(track.DiscTotal)
		t.TrackTotal = int16(track.AlbumData.Attributes.TrackCount)
		t.AlbumArtist = track.AlbumData.Attributes.ArtistName
		t.AlbumArtistSort = track.AlbumData.Attributes.ArtistName
		t.Custom["UPC"] = track.AlbumData.Attributes.Upc
		t.Custom["LABEL"] = track.AlbumData.Attributes.RecordLabel
		t.Date = track.AlbumData.Attributes.ReleaseDate
		t.Copyright = track.AlbumData.Attributes.Copyright
		t.Publisher = track.AlbumData.Attributes.RecordLabel
	} else {
		t.DiscTotal = int16(track.DiscTotal)
		t.TrackTotal = int16(track.AlbumData.Attributes.TrackCount)
		t.AlbumArtist = track.AlbumData.Attributes.ArtistName
		t.AlbumArtistSort = track.AlbumData.Attributes.ArtistName
		t.Custom["UPC"] = track.AlbumData.Attributes.Upc
		t.Date = track.AlbumData.Attributes.ReleaseDate
		t.Copyright = track.AlbumData.Attributes.Copyright
		t.Publisher = track.AlbumData.Attributes.RecordLabel
	}

	if track.Resp.Attributes.ContentRating == "explicit" {
		t.ItunesAdvisory = mp4tag.ItunesAdvisoryExplicit
	} else if track.Resp.Attributes.ContentRating == "clean" {
		t.ItunesAdvisory = mp4tag.ItunesAdvisoryClean
	} else {
		t.ItunesAdvisory = mp4tag.ItunesAdvisoryNone
	}

	if track.CoverPath != "" && Config.EmbedCover {
		if coverData, err := os.ReadFile(track.CoverPath); err == nil {
			t.Pictures = []*mp4tag.MP4Picture{
				{Data: coverData},
			}
		}
	}

	mp4, err := mp4tag.Open(track.SavePath)
	if err != nil {
		return err
	}
	defer mp4.Close()
	err = mp4.Write(t, []string{})
	if err != nil {
		return err
	}
	return nil
}

func main() {
	err := loadConfig()
	if err != nil {
		fmt.Printf("load Config failed: %v", err)
		return
	}
	token, err := ampapi.GetToken()
	if err != nil {
		if Config.AuthorizationToken != "" && Config.AuthorizationToken != "your-authorization-token" {
			token = strings.Replace(Config.AuthorizationToken, "Bearer ", "", -1)
		} else {
			fmt.Println("Failed to get token.")
			return
		}
	}
	var search_type string
	var bot_mode bool
	pflag.StringVar(&search_type, "search", "", "Search for 'album', 'song', or 'artist'. Provide query after flags.")
	pflag.BoolVar(&bot_mode, "bot", false, "Run Telegram bot mode")
	pflag.BoolVar(&dl_atmos, "atmos", false, "Enable atmos download mode")
	pflag.BoolVar(&dl_aac, "aac", false, "Enable adm-aac download mode")
	pflag.BoolVar(&dl_select, "select", false, "Enable selective download")
	pflag.BoolVar(&dl_song, "song", false, "Enable single song download mode")
	pflag.BoolVar(&dl_noCache, "no-cache", false, "Force re-rip: delete any existing file and download fresh")
	pflag.BoolVar(&artist_select, "all-album", false, "Download all artist albums")
	pflag.BoolVar(&debug_mode, "debug", false, "Enable debug mode to show audio quality information")
	alac_max = pflag.Int("alac-max", Config.AlacMax, "Specify the max quality for download alac")
	atmos_max = pflag.Int("atmos-max", Config.AtmosMax, "Specify the max quality for download atmos")
	aac_type = pflag.String("aac-type", Config.AacType, "Select AAC type, aac aac-binaural aac-downmix")
	mv_audio_type = pflag.String("mv-audio-type", Config.MVAudioType, "Select MV audio type, atmos ac3 aac")
	mv_max = pflag.Int("mv-max", Config.MVMax, "Specify the max quality for download MV")

	pflag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options] [url1 url2 ...]\n", "[main | main.exe | go run main.go]")
		fmt.Fprintf(os.Stderr, "Search Usage: %s --search [album|song|artist] [query]\n", "[main | main.exe | go run main.go]")
		fmt.Println("\nOptions:")
		pflag.PrintDefaults()
	}

	pflag.Parse()
	Config.AlacMax = *alac_max
	Config.AtmosMax = *atmos_max
	Config.AacType = *aac_type
	Config.MVAudioType = *mv_audio_type
	Config.MVMax = *mv_max

	var initErr error
	wmPool, initErr = wmgrpc.NewPool(Config.WrapperManagerAddrs)
	if initErr != nil {
		log.Fatalf("Failed to connect to wrapper-manager: %v", initErr)
	}
	defer wmPool.Close()

	if bot_mode {
		runTelegramBot(token)
		return
	}

	args := pflag.Args()

	if search_type != "" {
		if len(args) == 0 {
			fmt.Println("Error: --search flag requires a query.")
			pflag.Usage()
			return
		}
		selectedUrl, err := handleSearch(search_type, args, token)
		if err != nil {
			fmt.Printf("\nSearch process failed: %v\n", err)
			return
		}
		if selectedUrl == "" {
			fmt.Println("\nExiting.")
			return
		}
		os.Args = []string{selectedUrl}
	} else {
		if len(args) == 0 {
			fmt.Println("No URLs provided. Please provide at least one URL.")
			pflag.Usage()
			return
		}
		for i := range args {
			args[i] = resolveAppleMusicURL(args[i])
		}
		os.Args = args
	}

	os.Args[0] = resolveAppleMusicURL(os.Args[0])
	if strings.Contains(os.Args[0], "/artist/") {
		urlArtistName, urlArtistID, err := getUrlArtistName(os.Args[0], token)
		if err != nil {
			fmt.Println("Failed to get artistname.")
			return
		}
		Config.ArtistFolderFormat = strings.NewReplacer(
			"{UrlArtistName}", LimitString(urlArtistName),
			"{ArtistId}", urlArtistID,
		).Replace(Config.ArtistFolderFormat)
		albumArgs, err := checkArtist(os.Args[0], token, "albums")
		if err != nil {
			fmt.Println("Failed to get artist albums.")
			return
		}
		mvArgs, err := checkArtist(os.Args[0], token, "music-videos")
		if err != nil {
			fmt.Println("Failed to get artist music-videos.")
		}
		os.Args = append(albumArgs, mvArgs...)
	}
	albumTotal := len(os.Args)
	for {
		for albumNum, urlRaw := range os.Args {
			fmt.Printf("Queue %d of %d: ", albumNum+1, albumTotal)
			var storefront, albumId string
			urlRaw = resolveAppleMusicURL(urlRaw)

			if strings.Contains(urlRaw, "/music-video/") {
				fmt.Println("Music Video")
				if debug_mode {
					continue
				}
				counter.Total++
				mvSaveDir := strings.NewReplacer(
					"{ArtistName}", "",
					"{UrlArtistName}", "",
					"{ArtistId}", "",
				).Replace(Config.ArtistFolderFormat)
				if mvSaveDir != "" {
					mvSaveDir = filepath.Join(Config.AlacSaveFolder, forbiddenNames.ReplaceAllString(mvSaveDir, "_"))
				} else {
					mvSaveDir = Config.AlacSaveFolder
				}
				storefront, albumId = checkUrlMv(urlRaw)
				err := mvDownloader(context.Background(), albumId, mvSaveDir, token, storefront, nil, nil)
				if err != nil {
					fmt.Println("\u26A0 Failed to dl MV:", err)
					counter.Error++
					continue
				}
				counter.Success++
				continue
			}
			if strings.Contains(urlRaw, "/song/") {
				fmt.Printf("Song->")
				storefront, songId := checkUrlSong(urlRaw)
				if storefront == "" || songId == "" {
					fmt.Println("Invalid song URL format.")
					continue
				}
				err := ripSong(songId, token, storefront, false, nil)
				if err != nil {
					fmt.Println("Failed to rip song:", err)
				}
				continue
			}
			parse, err := url.Parse(urlRaw)
			if err != nil {
				log.Fatalf("Invalid URL: %v", err)
			}
			var urlArg_i = parse.Query().Get("i")

			if strings.Contains(urlRaw, "/album/") {
				fmt.Println("Album")
				storefront, albumId = checkUrl(urlRaw)
				err := ripAlbum(albumId, token, storefront, urlArg_i, false, nil)
				if err != nil {
					fmt.Println("Failed to rip album:", err)
				}
			} else if strings.Contains(urlRaw, "/playlist/") {
				fmt.Println("Playlist")
				storefront, albumId = checkUrlPlaylist(urlRaw)
				err := ripPlaylist(albumId, token, storefront, false, nil)
				if err != nil {
					fmt.Println("Failed to rip playlist:", err)
				}
			} else if strings.Contains(urlRaw, "/station/") {
				fmt.Printf("Station")
				storefront, albumId = checkUrlStation(urlRaw)
				err := ripStation(albumId, token, storefront, nil)
				if err != nil {
					fmt.Println("Failed to rip station:", err)
				}
			} else {
				fmt.Println("Invalid type")
			}
		}
		fmt.Printf("=======  [\u2714 ] Completed: %d/%d  |  [\u26A0 ] Warnings: %d  |  [\u2716 ] Errors: %d  =======\n", counter.Success, counter.Total, counter.Unavailable+counter.NotSong, counter.Error)
		if counter.Error == 0 {
			break
		}
		fmt.Println("Error detected, press Enter to try again...")
		fmt.Scanln()
		fmt.Println("Start trying again...")
		counter = structs.Counter{}
	}
}

func mvDownloader(ctx context.Context, adamID string, saveDir string, token string, storefront string, track *task.Track, progress apputils.ProgressFunc) error {
	MVInfo, err := ampapi.GetMusicVideoResp(storefront, adamID, Config.Language, token)
	if err != nil {
		fmt.Println("\u26A0 Failed to get MV manifest:", err)
		return nil
	}

	if strings.HasSuffix(saveDir, ".") {
		saveDir = strings.ReplaceAll(saveDir, ".", "")
	}
	saveDir = strings.TrimSpace(saveDir)

	vidPath := filepath.Join(saveDir, fmt.Sprintf("%s_vid.mp4", adamID))
	audPath := filepath.Join(saveDir, fmt.Sprintf("%s_aud.mp4", adamID))
	mvSaveName := fmt.Sprintf("%s (%s)", MVInfo.Data[0].Attributes.Name, adamID)
	if track != nil {
		mvSaveName = fmt.Sprintf("%02d. %s", track.TaskNum, MVInfo.Data[0].Attributes.Name)
	}

	mvOutPath := filepath.Join(saveDir, fmt.Sprintf("%s.mp4", forbiddenNames.ReplaceAllString(mvSaveName, "_")))

	fmt.Println(MVInfo.Data[0].Attributes.Name)

	// Register the muxed MV with the rip state so the Telegram delivery layer can find
	// it the same way it finds audio tracks. Used by both the cached and freshly-ripped
	// paths so a cached file is still delivered/uploaded. Harmless for the CLI path.
	registerMV := func() {
		rs := ripStateFrom(ctx)
		rs.addPath(mvOutPath)
		mvMeta := AudioMeta{
			TrackID:        strings.TrimSpace(adamID),
			Title:          strings.TrimSpace(MVInfo.Data[0].Attributes.Name),
			Performer:      strings.TrimSpace(MVInfo.Data[0].Attributes.ArtistName),
			DurationMillis: int64(MVInfo.Data[0].Attributes.DurationInMillis),
			AlbumName:      strings.TrimSpace(MVInfo.Data[0].Attributes.AlbumName),
			ReleaseDate:    strings.TrimSpace(MVInfo.Data[0].Attributes.ReleaseDate),
			ContentRating:  strings.TrimSpace(MVInfo.Data[0].Attributes.ContentRating),
			Codec:          "MV",
		}
		if mvMeta.Title != "" || mvMeta.Performer != "" {
			rs.putMeta(mvOutPath, mvMeta)
		}
	}

	exists, _ := fileExists(mvOutPath)
	if ripStateFrom(ctx).noCache() && exists {
		fmt.Println("--no-cache: removing existing MV to re-rip fresh.")
		_ = os.Remove(mvOutPath)
		exists = false
	}
	if exists {
		fmt.Println("MV already cached locally — skipping download, will upload.")
		if progress != nil {
			progress("Cached, preparing upload", 0, 0)
		}
		registerMV()
		return nil
	}

	// Music videos are Widevine-encrypted (PSSH keys), not FairPlay (skd://).
	// wrapper-manager only decrypts FairPlay, so MV uses the bundled Widevine CDM
	// (runv3) + mp4decrypt path instead of the wmgrpc decrypt stream.
	mvm3u8url, _, _, _ := runv3.GetWebplayback(adamID, token, Config.MediaUserToken, true)
	if mvm3u8url == "" {
		return errors.New("media-user-token may be wrong or expired")
	}

	os.MkdirAll(saveDir, os.ModePerm)
	videom3u8url, _ := extractVideo(ctx, mvm3u8url)
	videokeyAndUrls, err := runv3.Run(ctx, adamID, videom3u8url, token, Config.MediaUserToken, true, "", nil)
	if err != nil {
		return fmt.Errorf("video key retrieval failed: %w", err)
	}
	if err := runv3.ExtMvData(ctx, videokeyAndUrls, vidPath, runv3.ProgressFunc(progress)); err != nil {
		return fmt.Errorf("video track download failed: %w", err)
	}
	defer os.Remove(vidPath)
	audiom3u8url, _ := extractMvAudio(mvm3u8url)
	audiokeyAndUrls, err := runv3.Run(ctx, adamID, audiom3u8url, token, Config.MediaUserToken, true, "", nil)
	if err != nil {
		return fmt.Errorf("audio key retrieval failed: %w", err)
	}
	if err := runv3.ExtMvData(ctx, audiokeyAndUrls, audPath, runv3.ProgressFunc(progress)); err != nil {
		return fmt.Errorf("audio track download failed: %w", err)
	}
	defer os.Remove(audPath)

	tags := []string{
		"tool=",
		fmt.Sprintf("artist=%s", MVInfo.Data[0].Attributes.ArtistName),
		fmt.Sprintf("title=%s", MVInfo.Data[0].Attributes.Name),
		fmt.Sprintf("genre=%s", MVInfo.Data[0].Attributes.GenreNames[0]),
		fmt.Sprintf("created=%s", MVInfo.Data[0].Attributes.ReleaseDate),
		fmt.Sprintf("ISRC=%s", MVInfo.Data[0].Attributes.Isrc),
	}

	if MVInfo.Data[0].Attributes.ContentRating == "explicit" {
		tags = append(tags, "rating=1")
	} else if MVInfo.Data[0].Attributes.ContentRating == "clean" {
		tags = append(tags, "rating=2")
	} else {
		tags = append(tags, "rating=0")
	}

	if track != nil {
		if track.PreType == "playlists" && !Config.UseSongInfoForPlaylist {
			tags = append(tags, "disk=1/1")
			tags = append(tags, fmt.Sprintf("album=%s", track.PlaylistData.Attributes.Name))
			tags = append(tags, fmt.Sprintf("track=%d", track.TaskNum))
			tags = append(tags, fmt.Sprintf("tracknum=%d/%d", track.TaskNum, track.TaskTotal))
			tags = append(tags, fmt.Sprintf("album_artist=%s", track.PlaylistData.Attributes.ArtistName))
			tags = append(tags, fmt.Sprintf("performer=%s", track.Resp.Attributes.ArtistName))
		} else if track.PreType == "playlists" && Config.UseSongInfoForPlaylist {
			tags = append(tags, fmt.Sprintf("album=%s", track.AlbumData.Attributes.Name))
			tags = append(tags, fmt.Sprintf("disk=%d/%d", track.Resp.Attributes.DiscNumber, track.DiscTotal))
			tags = append(tags, fmt.Sprintf("track=%d", track.Resp.Attributes.TrackNumber))
			tags = append(tags, fmt.Sprintf("tracknum=%d/%d", track.Resp.Attributes.TrackNumber, track.AlbumData.Attributes.TrackCount))
			tags = append(tags, fmt.Sprintf("album_artist=%s", track.AlbumData.Attributes.ArtistName))
			tags = append(tags, fmt.Sprintf("performer=%s", track.Resp.Attributes.ArtistName))
			tags = append(tags, fmt.Sprintf("copyright=%s", track.AlbumData.Attributes.Copyright))
			tags = append(tags, fmt.Sprintf("UPC=%s", track.AlbumData.Attributes.Upc))
		} else {
			tags = append(tags, fmt.Sprintf("album=%s", track.AlbumData.Attributes.Name))
			tags = append(tags, fmt.Sprintf("disk=%d/%d", track.Resp.Attributes.DiscNumber, track.DiscTotal))
			tags = append(tags, fmt.Sprintf("track=%d", track.Resp.Attributes.TrackNumber))
			tags = append(tags, fmt.Sprintf("tracknum=%d/%d", track.Resp.Attributes.TrackNumber, track.AlbumData.Attributes.TrackCount))
			tags = append(tags, fmt.Sprintf("album_artist=%s", track.AlbumData.Attributes.ArtistName))
			tags = append(tags, fmt.Sprintf("performer=%s", track.Resp.Attributes.ArtistName))
			tags = append(tags, fmt.Sprintf("copyright=%s", track.AlbumData.Attributes.Copyright))
			tags = append(tags, fmt.Sprintf("UPC=%s", track.AlbumData.Attributes.Upc))
		}
	} else {
		tags = append(tags, fmt.Sprintf("album=%s", MVInfo.Data[0].Attributes.AlbumName))
		tags = append(tags, fmt.Sprintf("disk=%d", MVInfo.Data[0].Attributes.DiscNumber))
		tags = append(tags, fmt.Sprintf("track=%d", MVInfo.Data[0].Attributes.TrackNumber))
		tags = append(tags, fmt.Sprintf("tracknum=%d", MVInfo.Data[0].Attributes.TrackNumber))
		tags = append(tags, fmt.Sprintf("performer=%s", MVInfo.Data[0].Attributes.ArtistName))
	}

	var covPath string
	if true {
		thumbURL := MVInfo.Data[0].Attributes.Artwork.URL
		baseThumbName := forbiddenNames.ReplaceAllString(mvSaveName, "_") + "_thumbnail"
		covPath, err = writeCover(saveDir, baseThumbName, thumbURL)
		if err != nil {
			fmt.Println("Failed to save MV thumbnail:", err)
		} else {
			tags = append(tags, fmt.Sprintf("cover=%s", covPath))
		}
	}
	defer os.Remove(covPath)

	tagsString := strings.Join(tags, ":")
	if progress != nil {
		progress("Remuxing", 0, 0)
	}
	muxCmd := exec.CommandContext(ctx, "MP4Box", "-itags", tagsString, "-quiet", "-add", vidPath, "-add", audPath, "-keep-utc", "-new", mvOutPath)
	fmt.Printf("MV Remuxing...")
	if err := muxCmd.Run(); err != nil {
		fmt.Printf("MV mux failed: %v\n", err)
		return err
	}
	fmt.Printf("\rMV Remuxed.   \n")

	// Register the muxed MV so the Telegram delivery layer can find it (same as the
	// cached path above).
	registerMV()
	return nil
}

func extractMvAudio(c string) (string, error) {
	MediaUrl, err := url.Parse(c)
	if err != nil {
		return "", err
	}

	resp, err := http.Get(c)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", errors.New(resp.Status)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	audioString := string(body)
	from, listType, err := m3u8.DecodeFrom(strings.NewReader(audioString), true)
	if err != nil || listType != m3u8.MASTER {
		return "", errors.New("m3u8 not of media type")
	}

	audio := from.(*m3u8.MasterPlaylist)

	var audioPriority = []string{"audio-atmos", "audio-ac3", "audio-stereo-256"}
	if Config.MVAudioType == "ac3" {
		audioPriority = []string{"audio-ac3", "audio-stereo-256"}
	} else if Config.MVAudioType == "aac" {
		audioPriority = []string{"audio-stereo-256"}
	}

	re := regexp.MustCompile(`_gr(\d+)_`)

	type AudioStream struct {
		URL     string
		Rank    int
		GroupID string
	}
	var audioStreams []AudioStream

	for _, variant := range audio.Variants {
		for _, audiov := range variant.Alternatives {
			if audiov.URI != "" {
				for _, priority := range audioPriority {
					if audiov.GroupId == priority {
						matches := re.FindStringSubmatch(audiov.URI)
						if len(matches) == 2 {
							var rank int
							fmt.Sscanf(matches[1], "%d", &rank)
							streamUrl, _ := MediaUrl.Parse(audiov.URI)
							audioStreams = append(audioStreams, AudioStream{
								URL:     streamUrl.String(),
								Rank:    rank,
								GroupID: audiov.GroupId,
							})
						}
					}
				}
			}
		}
	}

	if len(audioStreams) == 0 {
		return "", errors.New("no suitable audio stream found")
	}

	sort.Slice(audioStreams, func(i, j int) bool {
		return audioStreams[i].Rank > audioStreams[j].Rank
	})
	fmt.Println("Audio: " + audioStreams[0].GroupID)
	return audioStreams[0].URL, nil
}

func checkM3u8(b string, f string) (string, error) {
	if wmPool == nil {
		return "", errors.New("wrapper-manager pool not initialized")
	}
	wm := wmPool.Acquire()
	m3u8URL, err := wm.M3U8(context.Background(), b)
	wmPool.Release(wm)
	if err != nil {
		return "none", fmt.Errorf("M3U8 RPC failed for %s: %w", b, err)
	}
	if f == "song" {
		fmt.Println("Received URL:", m3u8URL)
	}
	return m3u8URL, nil
}

func formatAvailability(available bool, quality string) string {
	if !available {
		return "Not Available"
	}
	return quality
}

func extractMedia(ctx context.Context, b string, more_mode bool) (string, string, error) {
	rs := ripStateFrom(ctx)
	cfg := rs.ripConfig()
	masterUrl, err := url.Parse(b)
	if err != nil {
		return "", "", err
	}
	resp, err := http.Get(b)
	if err != nil {
		return "", "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", "", errors.New(resp.Status)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", "", err
	}
	masterString := string(body)
	from, listType, err := m3u8.DecodeFrom(strings.NewReader(masterString), true)
	if err != nil || listType != m3u8.MASTER {
		return "", "", errors.New("m3u8 not of master type")
	}
	master := from.(*m3u8.MasterPlaylist)
	var streamUrl *url.URL
	sort.Slice(master.Variants, func(i, j int) bool {
		return master.Variants[i].AverageBandwidth > master.Variants[j].AverageBandwidth
	})
	if debug_mode && more_mode {
		fmt.Println("\nDebug: All Available Variants:")
		var data [][]string
		for _, variant := range master.Variants {
			data = append(data, []string{variant.Codecs, variant.Audio, fmt.Sprint(variant.Bandwidth)})
		}
		table := tablewriter.NewWriter(os.Stdout)
		table.SetHeader([]string{"Codec", "Audio", "Bandwidth"})
		table.SetAutoMergeCells(true)
		table.SetRowLine(true)
		table.AppendBulk(data)
		table.Render()

		var hasAAC, hasLossless, hasHiRes, hasAtmos, hasDolbyAudio bool
		var aacQuality, losslessQuality, hiResQuality, atmosQuality, dolbyAudioQuality string

		for _, variant := range master.Variants {
			if variant.Codecs == "mp4a.40.2" { // AAC
				hasAAC = true
				split := strings.Split(variant.Audio, "-")
				if len(split) >= 3 {
					bitrate, _ := strconv.Atoi(split[2])
					currentBitrate := 0
					if aacQuality != "" {
						current := strings.Split(aacQuality, " | ")[2]
						current = strings.Split(current, " ")[0]
						currentBitrate, _ = strconv.Atoi(current)
					}
					if bitrate > currentBitrate {
						aacQuality = fmt.Sprintf("AAC | 2 Channel | %d Kbps", bitrate)
					}
				}
			} else if variant.Codecs == "ec-3" && strings.Contains(variant.Audio, "atmos") { // Dolby Atmos
				hasAtmos = true
				split := strings.Split(variant.Audio, "-")
				if len(split) > 0 {
					bitrateStr := split[len(split)-1]
					if len(bitrateStr) == 4 && bitrateStr[0] == '2' {
						bitrateStr = bitrateStr[1:]
					}
					bitrate, _ := strconv.Atoi(bitrateStr)
					currentBitrate := 0
					if atmosQuality != "" {
						current := strings.Split(strings.Split(atmosQuality, " | ")[2], " ")[0]
						currentBitrate, _ = strconv.Atoi(current)
					}
					if bitrate > currentBitrate {
						atmosQuality = fmt.Sprintf("E-AC-3 | 16 Channel | %d Kbps", bitrate)
					}
				}
			} else if variant.Codecs == "alac" { // ALAC (Lossless or Hi-Res)
				split := strings.Split(variant.Audio, "-")
				if len(split) >= 3 {
					bitDepth := split[len(split)-1]
					sampleRate := split[len(split)-2]
					sampleRateInt, _ := strconv.Atoi(sampleRate)
					if sampleRateInt > 48000 { // Hi-Res
						hasHiRes = true
						hiResQuality = fmt.Sprintf("ALAC | 2 Channel | %s-bit/%d kHz", bitDepth, sampleRateInt/1000)
					} else { // Standard Lossless
						hasLossless = true
						losslessQuality = fmt.Sprintf("ALAC | 2 Channel | %s-bit/%d kHz", bitDepth, sampleRateInt/1000)
					}
				}
			} else if variant.Codecs == "ac-3" { // Dolby Audio
				hasDolbyAudio = true
				split := strings.Split(variant.Audio, "-")
				if len(split) > 0 {
					bitrate, _ := strconv.Atoi(split[len(split)-1])
					dolbyAudioQuality = fmt.Sprintf("AC-3 |  16 Channel | %d Kbps", bitrate)
				}
			}
		}

		fmt.Println("Available Audio Formats:")
		fmt.Println("------------------------")
		fmt.Printf("AAC             : %s\n", formatAvailability(hasAAC, aacQuality))
		fmt.Printf("Lossless        : %s\n", formatAvailability(hasLossless, losslessQuality))
		fmt.Printf("Hi-Res Lossless : %s\n", formatAvailability(hasHiRes, hiResQuality))
		fmt.Printf("Dolby Atmos     : %s\n", formatAvailability(hasAtmos, atmosQuality))
		fmt.Printf("Dolby Audio     : %s\n", formatAvailability(hasDolbyAudio, dolbyAudioQuality))
		fmt.Println("------------------------")

		return "", "", nil
	}
	var Quality string
	for _, variant := range master.Variants {
		if rs.atmos() {
			if variant.Codecs == "ec-3" && strings.Contains(variant.Audio, "atmos") {
				if debug_mode && !more_mode {
					fmt.Printf("Debug: Found Dolby Atmos variant - %s (Bitrate: %d Kbps)\n",
						variant.Audio, variant.Bandwidth/1000)
				}
				split := strings.Split(variant.Audio, "-")
				length := len(split)
				length_int, err := strconv.Atoi(split[length-1])
				if err != nil {
					return "", "", err
				}
				if length_int <= cfg.AtmosMax {
					if !debug_mode && !more_mode {
						fmt.Printf("%s\n", variant.Audio)
					}
					streamUrlTemp, err := masterUrl.Parse(variant.URI)
					if err != nil {
						return "", "", err
					}
					streamUrl = streamUrlTemp
					Quality = fmt.Sprintf("%s Kbps", split[len(split)-1])
					break
				}
			} else if variant.Codecs == "ac-3" { // Add Dolby Audio support
				if debug_mode && !more_mode {
					fmt.Printf("Debug: Found Dolby Audio variant - %s (Bitrate: %d Kbps)\n",
						variant.Audio, variant.Bandwidth/1000)
				}
				streamUrlTemp, err := masterUrl.Parse(variant.URI)
				if err != nil {
					return "", "", err
				}
				streamUrl = streamUrlTemp
				split := strings.Split(variant.Audio, "-")
				Quality = fmt.Sprintf("%s Kbps", split[len(split)-1])
				break
			}
		} else if rs.aac() {
			if variant.Codecs == "mp4a.40.2" {
				if debug_mode && !more_mode {
					fmt.Printf("Debug: Found AAC variant - %s (Bitrate: %d)\n", variant.Audio, variant.Bandwidth)
				}
				aacregex := regexp.MustCompile(`audio-stereo-\d+`)
				replaced := aacregex.ReplaceAllString(variant.Audio, "aac")
				if replaced == cfg.AacType {
					if !debug_mode && !more_mode {
						fmt.Printf("%s\n", variant.Audio)
					}
					streamUrlTemp, err := masterUrl.Parse(variant.URI)
					if err != nil {
						panic(err)
					}
					streamUrl = streamUrlTemp
					split := strings.Split(variant.Audio, "-")
					Quality = fmt.Sprintf("%s Kbps", split[2])
					break
				}
			}
		} else {
			if variant.Codecs == "alac" {
				split := strings.Split(variant.Audio, "-")
				length := len(split)
				length_int, err := strconv.Atoi(split[length-2])
				if err != nil {
					return "", "", err
				}
				if length_int <= cfg.AlacMax {
					if !debug_mode && !more_mode {
						fmt.Printf("%s-bit / %s Hz\n", split[length-1], split[length-2])
					}
					streamUrlTemp, err := masterUrl.Parse(variant.URI)
					if err != nil {
						panic(err)
					}
					streamUrl = streamUrlTemp
					KHZ := float64(length_int) / 1000.0
					Quality = fmt.Sprintf("%sB-%.1fkHz", split[length-1], KHZ)
					break
				}
			}
		}
	}
	if streamUrl == nil {
		return "", "", errors.New("no codec found")
	}
	return streamUrl.String(), Quality, nil
}
func extractVideo(ctx context.Context, c string) (string, error) {
	MediaUrl, err := url.Parse(c)
	if err != nil {
		return "", err
	}

	resp, err := http.Get(c)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", errors.New(resp.Status)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	videoString := string(body)

	from, listType, err := m3u8.DecodeFrom(strings.NewReader(videoString), true)
	if err != nil || listType != m3u8.MASTER {
		return "", errors.New("m3u8 not of media type")
	}

	video := from.(*m3u8.MasterPlaylist)

	re := regexp.MustCompile(`_(\d+)x(\d+)`)

	var streamUrl *url.URL
	sort.Slice(video.Variants, func(i, j int) bool {
		return video.Variants[i].AverageBandwidth > video.Variants[j].AverageBandwidth
	})

	maxHeight := ripStateFrom(ctx).ripConfig().MVMax

	for _, variant := range video.Variants {
		matches := re.FindStringSubmatch(variant.URI)
		if len(matches) == 3 {
			height := matches[2]
			var h int
			_, err := fmt.Sscanf(height, "%d", &h)
			if err != nil {
				continue
			}
			if h <= maxHeight {
				streamUrl, err = MediaUrl.Parse(variant.URI)
				if err != nil {
					return "", err
				}
				fmt.Println("Video: " + variant.Resolution + "-" + variant.VideoRange)
				break
			}
		}
	}

	if streamUrl == nil {
		return "", errors.New("no suitable video stream found")
	}

	return streamUrl.String(), nil
}

func ripSong(songId string, token string, storefront string, forceAAC bool, ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	// Get song info to find album ID
	manifest, err := ampapi.GetSongResp(storefront, songId, Config.Language, token)
	if err != nil {
		fmt.Println("Failed to get song response.")
		return err
	}
	if manifest == nil || len(manifest.Data) == 0 {
		return fmt.Errorf("empty song response for %s", songId)
	}

	songData := manifest.Data[0]
	if len(songData.Relationships.Albums.Data) == 0 {
		return fmt.Errorf("song %s has no album relationship", songId)
	}
	albumId := songData.Relationships.Albums.Data[0].ID

	// Use album approach but only download the specific song
	if rs := ripStateFrom(ctx); rs != nil {
		rs.Song = true
	} else {
		dl_song = true
	}
	err = ripAlbum(albumId, token, storefront, songId, forceAAC, ctx)
	if err != nil {
		fmt.Println("Failed to rip song:", err)
		return err
	}

	return nil
}

func songRespDataToTrackRespData(song ampapi.SongRespData) (ampapi.TrackRespData, error) {
	var track ampapi.TrackRespData
	data, err := json.Marshal(song)
	if err != nil {
		return track, err
	}
	if err := json.Unmarshal(data, &track); err != nil {
		return track, err
	}
	if track.ID == "" {
		track.ID = song.ID
	}
	if track.Type == "" {
		track.Type = "songs"
	}
	return track, nil
}

func songBelongsToAlbum(song ampapi.SongRespData, albumID string) bool {
	if albumID == "" || len(song.Relationships.Albums.Data) == 0 {
		return true
	}
	for _, album := range song.Relationships.Albums.Data {
		if album.ID == albumID {
			return true
		}
	}
	return false
}

func buildAlbumTrackFromSongData(song ampapi.SongRespData, album *task.Album, albumFolderPath string, coverPath string, codec string) (*task.Track, error) {
	if album == nil || len(album.Resp.Data) == 0 {
		return nil, errors.New("album metadata is empty")
	}
	if song.ID == "" {
		return nil, errors.New("song id is empty")
	}
	if !songBelongsToAlbum(song, album.ID) {
		return nil, fmt.Errorf("song %s does not belong to album %s", song.ID, album.ID)
	}
	trackResp, err := songRespDataToTrackRespData(song)
	if err != nil {
		return nil, err
	}
	discTotal := song.Attributes.DiscNumber
	for _, track := range album.Resp.Data[0].Relationships.Tracks.Data {
		if track.Attributes.DiscNumber > discTotal {
			discTotal = track.Attributes.DiscNumber
		}
	}
	if discTotal <= 0 {
		discTotal = 1
	}
	taskNum := song.Attributes.TrackNumber
	if taskNum <= 0 {
		taskNum = 1
	}
	taskTotal := album.Resp.Data[0].Attributes.TrackCount
	if taskTotal <= 0 {
		taskTotal = len(album.Resp.Data[0].Relationships.Tracks.Data)
	}
	if taskTotal <= 0 {
		taskTotal = 1
	}
	trackType := song.Type
	if trackType == "" {
		trackType = "songs"
	}
	return &task.Track{
		ID:         song.ID,
		Type:       trackType,
		Name:       song.Attributes.Name,
		Language:   album.Language,
		Storefront: album.Storefront,
		SaveDir:    albumFolderPath,
		Codec:      codec,
		TaskNum:    taskNum,
		TaskTotal:  taskTotal,
		M3u8:       song.Attributes.ExtendedAssetUrls.EnhancedHls,
		WebM3u8:    song.Attributes.ExtendedAssetUrls.EnhancedHls,
		CoverPath:  coverPath,
		Resp:       trackResp,
		PreType:    "albums",
		PreID:      album.ID,
		DiscTotal:  discTotal,
		AlbumData:  album.Resp.Data[0],
	}, nil
}

func ripAlbumSongFallback(album *task.Album, songID string, token string, storefront string, albumFolderPath string, coverPath string, codec string, forceAAC bool, ctx context.Context) error {
	manifest, err := ampapi.GetSongResp(storefront, songID, album.Language, token)
	if err != nil {
		recordDownloadFailure(ctx, "song %s: failed to fetch direct song metadata: %v", songID, err)
		return err
	}
	if manifest == nil || len(manifest.Data) == 0 {
		err := fmt.Errorf("empty song response for %s", songID)
		recordDownloadFailure(ctx, "song %s: %v", songID, err)
		return err
	}
	track, err := buildAlbumTrackFromSongData(manifest.Data[0], album, albumFolderPath, coverPath, codec)
	if err != nil {
		recordDownloadFailure(ctx, "song %s: failed to build direct download metadata: %v", songID, err)
		return err
	}
	fmt.Println("Song was not found in album track list, downloading by song metadata.")
	ripTrack(track, token, ctx)
	return nil
}
