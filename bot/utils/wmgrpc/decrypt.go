package wmgrpc

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/grafov/m3u8"
	"github.com/itouakirai/mp4ff/mp4"
)

type ProgressFunc func(phase string, done, total int64)

type progressWriter struct {
	cb    ProgressFunc
	phase string
	total int64
	done  int64
}

func (p *progressWriter) Write(b []byte) (int, error) {
	n := len(b)
	p.done += int64(n)
	if p.cb != nil {
		p.cb(p.phase, p.done, p.total)
	}
	return n, nil
}

const prefetchKey = "skd://itunes.apple.com/P000000000/s1/e1"

func downloadBytes(ctx context.Context, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, url)
	}
	return io.ReadAll(resp.Body)
}

func parseMediaPlaylist(r io.Reader) ([]*m3u8.MediaSegment, error) {
	playlist, listType, err := m3u8.DecodeFrom(r, true)
	if err != nil {
		return nil, err
	}
	if listType != m3u8.MEDIA {
		return nil, errors.New("not a media playlist")
	}
	return playlist.(*m3u8.MediaPlaylist).Segments, nil
}

func extractKeyURIs(segments []*m3u8.MediaSegment) []string {
	seen := make(map[string]bool)
	keyURIs := []string{prefetchKey}
	for _, seg := range segments {
		if seg != nil && seg.Key != nil && seg.Key.URI != "" {
			uri := seg.Key.URI
			if !seen[uri] {
				seen[uri] = true
				keyURIs = append(keyURIs, uri)
			}
		}
	}
	return keyURIs
}

type sampleJob struct {
	Index   int
	Data    []byte
	KeyIdx  int
}

type sampleResult struct {
	Index int
	Data  []byte
	Err   error
}

func DownloadAndDecrypt(ctx context.Context, wm *Client, adamID string, playlistURL string, outfile string, progress ProgressFunc) error {
	masterBytes, err := downloadBytes(ctx, playlistURL)
	if err != nil {
		return fmt.Errorf("download master playlist: %w", err)
	}

	masterPlaylist, listType, err := m3u8.DecodeFrom(bytes.NewReader(masterBytes), true)
	if err != nil {
		return fmt.Errorf("decode master playlist: %w", err)
	}

	var mediaURL string
	if listType == m3u8.MASTER {
		master := masterPlaylist.(*m3u8.MasterPlaylist)
		if len(master.Variants) == 0 {
			return errors.New("no variants in master playlist")
		}
		mediaURL = master.Variants[0].URI
		if !strings.HasPrefix(mediaURL, "http") {
			lastSlash := strings.LastIndex(playlistURL, "/")
			if lastSlash >= 0 {
				mediaURL = playlistURL[:lastSlash+1] + mediaURL
			}
		}
	} else if listType == m3u8.MEDIA {
		mediaURL = playlistURL
	} else {
		return fmt.Errorf("unexpected playlist type: %d", listType)
	}

	mediaBytes, err := downloadBytes(ctx, mediaURL)
	if err != nil {
		return fmt.Errorf("download media playlist: %w", err)
	}

	segments, err := parseMediaPlaylist(bytes.NewReader(mediaBytes))
	if err != nil {
		return fmt.Errorf("parse media playlist: %w", err)
	}

	keyURIs := extractKeyURIs(segments)

	keyToIndex := make(map[string]int)
	for i, k := range keyURIs {
		keyToIndex[k] = i
	}

	segment0 := segments[0]
	if segment0 == nil {
		return errors.New("no segments in playlist")
	}

	var baseURL string
	if strings.HasPrefix(segment0.URI, "http") {
		baseURL = segment0.URI[:strings.LastIndex(segment0.URI, "/")+1]
	} else {
		lastSlash := strings.LastIndex(mediaURL, "/")
		if lastSlash >= 0 {
			baseURL = mediaURL[:lastSlash+1]
		} else {
			baseURL = ""
		}
	}

	var initURL string
	if segment0.Map != nil && segment0.Map.URI != "" {
		if strings.HasPrefix(segment0.Map.URI, "http") {
			initURL = segment0.Map.URI
		} else {
			initURL = baseURL + segment0.Map.URI
		}
	}

	if initURL == "" {
		return errors.New("no init segment URL in playlist")
	}

	initData, err := downloadBytes(ctx, initURL)
	if err != nil {
		return fmt.Errorf("download init segment: %w", err)
	}

	initReader := bytes.NewReader(initData)
	initSeg := mp4.NewMP4Init()
	var offset uint64 = 0
	for i := 0; i < 2; i++ {
		box, err := mp4.DecodeBox(offset, initReader)
		if err != nil {
			return fmt.Errorf("decode init box %d: %w", i, err)
		}
		bt := box.Type()
		if bt != "ftyp" && bt != "moov" {
			return fmt.Errorf("unexpected init box type: %s", bt)
		}
		initSeg.AddChild(box)
		offset += box.Size()
	}

	tracks, err := mp4.DecryptInit(initSeg)
	if err != nil {
		return fmt.Errorf("decrypt init: %w", err)
	}
	trackMap := make(map[uint32]mp4.DecryptTrackInfo)
	for _, ti := range tracks.TrackInfos {
		trackMap[ti.TrackID] = ti
	}

	for _, trak := range initSeg.Moov.Traks {
		stbl := trak.Mdia.Minf.Stbl
		stbl.Children, _ = filterSbgpSgpd(stbl.Children)
	}

	ofh, err := os.Create(outfile)
	if err != nil {
		return fmt.Errorf("create output: %w", err)
	}
	defer ofh.Close()
	outBuf := bufio.NewWriter(ofh)

	err = initSeg.Encode(outBuf)
	if err != nil {
		return fmt.Errorf("write init: %w", err)
	}

	totalBytes := int64(len(initData))
	for _, seg := range segments {
		if seg != nil {
			totalBytes += seg.Limit
		}
	}

	if progress != nil {
		progress("Downloading", int64(len(initData)), totalBytes)
	}

	var totalDecrypted int64
	for i := 0; ; i++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if i >= len(segments) || segments[i] == nil {
			break
		}

		seg := segments[i]
		var segURL string
		if strings.HasPrefix(seg.URI, "http") {
			segURL = seg.URI
		} else {
			segURL = baseURL + seg.URI
		}

		segData, err := downloadBytes(ctx, segURL)
		if err != nil {
			return fmt.Errorf("download segment %d: %w", i, err)
		}

		segReader := bytes.NewReader(segData)
		frag := mp4.NewFragment()
		var soffset uint64 = 0
		for {
			box, err := mp4.DecodeBox(soffset, segReader)
			if err == io.EOF {
				break
			}
			if err != nil {
				return fmt.Errorf("decode segment %d box: %w", i, err)
			}
			bt := box.Type()
			soffset += box.Size()
			if bt == "moof" || bt == "emsg" || bt == "prft" {
				frag.AddChild(box)
				continue
			}
			if bt == "mdat" {
				frag.AddChild(box)
				break
			}
		}

		if frag.Moof == nil {
			return fmt.Errorf("segment %d: no moof", i)
		}

		var keyForSample string
		if seg.Key != nil && seg.Key.URI != "" {
			keyForSample = seg.Key.URI
		} else {
			keyForSample = keyURIs[0]
		}

		for _, traf := range frag.Moof.Trafs {
			ti, ok := trackMap[traf.Tfhd.TrackID]
			if !ok || ti.Sinf == nil {
				continue
			}

			samples, err := frag.GetFullSamples(ti.Trex)
			if err != nil {
				return fmt.Errorf("get samples segment %d: %w", i, err)
			}

			for j := range samples {
				if ctx.Err() != nil {
					return ctx.Err()
				}

				decrypted, err := wm.DecryptSample(ctx, adamID, keyForSample, samples[j].Data, int32(j))
				if err != nil {
					return fmt.Errorf("decrypt sample %d/%d: %w", i, j, err)
				}
				samples[j].Data = decrypted
				totalDecrypted += int64(len(decrypted))
				if progress != nil {
					progress("Decrypting", totalDecrypted, totalBytes)
				}
			}
		}

		err = frag.Encode(outBuf)
		if err != nil {
			return fmt.Errorf("write segment %d: %w", i, err)
		}

		if progress != nil {
			progress("Decrypting", int64(len(segData))+int64(len(initData)), totalBytes)
		}
	}

	err = outBuf.Flush()
	if err != nil {
		return fmt.Errorf("flush output: %w", err)
	}
	return nil
}

func filterSbgpSgpd(children []mp4.Box) ([]mp4.Box, uint64) {
	var removed uint64
	remaining := make([]mp4.Box, 0, len(children))
	for _, child := range children {
		switch box := child.(type) {
		case *mp4.SbgpBox:
			if box.GroupingType == "seam" || box.GroupingType == "seig" {
				removed += child.Size()
				continue
			}
		case *mp4.SgpdBox:
			if box.GroupingType == "seam" || box.GroupingType == "seig" {
				removed += child.Size()
				continue
			}
		}
		remaining = append(remaining, child)
	}
	return remaining, removed
}
