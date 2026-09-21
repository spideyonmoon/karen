package wmclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	temari "github.com/WorldObservationLog/Temari/bindings/go"
)

type StatusData struct {
	Status      bool     `json:"status"`
	Regions     []string `json:"regions"`
	ClientCount int      `json:"clientCount"`
	Ready       bool     `json:"ready"`
}

type envelope struct {
	Code int             `json:"code"`
	Msg  string          `json:"msg"`
	Data json.RawMessage `json:"data"`
}

type Client struct {
	baseURL string
	http    *http.Client
	lib     *temari.Library

	templatesMu sync.Mutex
	templates   map[string]*temari.Temari
}

func NewClient(addr, temariLibraryPath string) (*Client, error) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return nil, fmt.Errorf("wrapper-manager URL is empty")
	}
	if !strings.Contains(addr, "://") {
		addr = "http://" + addr
	}
	parsed, err := url.Parse(addr)
	if err != nil || parsed.Host == "" {
		return nil, fmt.Errorf("invalid wrapper-manager URL %q", addr)
	}

	var lib *temari.Library
	if temariLibraryPath != "" {
		lib, err = temari.Load(temariLibraryPath)
	} else {
		lib, err = temari.LoadDefault()
	}
	if err != nil {
		return nil, fmt.Errorf("load Temari: %w", err)
	}

	return &Client{
		baseURL:  strings.TrimRight(parsed.String(), "/"),
		http:     &http.Client{Timeout: 45 * time.Second},
		lib:      lib,
		templates: make(map[string]*temari.Temari),
	}, nil
}

func (c *Client) Close() error {
	c.templatesMu.Lock()
	defer c.templatesMu.Unlock()
	for key, template := range c.templates {
		template.Close()
		delete(c.templates, key)
	}
	c.http.CloseIdleConnections()
	return nil
}

func (c *Client) request(ctx context.Context, method, path string, query url.Values, body any) (json.RawMessage, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reader = bytes.NewReader(encoded)
	}
	endpoint := c.baseURL + path
	if len(query) != 0 {
		endpoint += "?" + query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, reader)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("wrapper HTTP %s: %w", path, err)
	}
	defer resp.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return nil, fmt.Errorf("wrapper HTTP %s: %w", path, err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("wrapper HTTP %s: status %d: %s", path, resp.StatusCode, strings.TrimSpace(string(payload)))
	}
	var reply envelope
	if err := json.Unmarshal(payload, &reply); err != nil {
		return nil, fmt.Errorf("wrapper HTTP %s: invalid JSON: %w", path, err)
	}
	if reply.Code != 0 {
		return nil, fmt.Errorf("wrapper HTTP %s: %s", path, reply.Msg)
	}
	return reply.Data, nil
}

func (c *Client) Status(ctx context.Context) (*StatusData, error) {
	data, err := c.request(ctx, http.MethodGet, "/status", nil, nil)
	if err != nil {
		return nil, err
	}
	var status StatusData
	if err := json.Unmarshal(data, &status); err != nil {
		return nil, fmt.Errorf("wrapper status: %w", err)
	}
	return &status, nil
}

func (c *Client) M3U8(ctx context.Context, adamID string) (string, error) {
	return c.getString(ctx, "/m3u8", url.Values{"adamId": {adamID}}, "m3u8")
}

func (c *Client) WebPlayback(ctx context.Context, adamID string) (string, error) {
	return c.getString(ctx, "/webplayback", url.Values{"adamId": {adamID}}, "m3u8")
}

func (c *Client) Lyrics(ctx context.Context, adamID, language, script string, syllable bool) (string, error) {
	query := url.Values{
		"adamId":  {adamID},
		"language": {language},
		"syllable": {map[bool]string{true: "1", false: "0"}[syllable]},
	}
	if script != "" {
		query.Set("script", script)
	}
	return c.getString(ctx, "/lyrics", query, "lyrics")
}

func (c *Client) License(ctx context.Context, adamID, challenge, uri, drmType string) (string, error) {
	body := map[string]string{"adamId": adamID, "challenge": challenge, "uri": uri}
	if drmType != "" {
		body["drm-type"] = drmType
	}
	data, err := c.request(ctx, http.MethodPost, "/license", nil, body)
	if err != nil {
		return "", err
	}
	var result struct {
		License string `json:"license"`
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return "", err
	}
	return result.License, nil
}

func (c *Client) getString(ctx context.Context, path string, query url.Values, field string) (string, error) {
	data, err := c.request(ctx, http.MethodGet, path, query, nil)
	if err != nil {
		return "", err
	}
	var result map[string]json.RawMessage
	if err := json.Unmarshal(data, &result); err != nil {
		return "", err
	}
	var value string
	if err := json.Unmarshal(result[field], &value); err != nil {
		return "", fmt.Errorf("wrapper HTTP %s: missing %s", path, field)
	}
	return value, nil
}

func (c *Client) template(ctx context.Context, adamID, keyURI string) (*temari.Temari, error) {
	requestID := adamID
	if keyURI == prefetchKey {
		requestID = "0"
	}
	cacheKey := requestID + "\x00" + keyURI
	c.templatesMu.Lock()
	defer c.templatesMu.Unlock()
	if template := c.templates[cacheKey]; template != nil {
		return template, nil
	}
	data, err := c.request(ctx, http.MethodGet, "/key", url.Values{
		"adamId": {requestID},
		"uri":     {keyURI},
	}, nil)
	if err != nil {
		return nil, err
	}
	template, err := c.lib.FromJSON(data)
	if err != nil {
		return nil, fmt.Errorf("create Temari template for %s: %w", adamID, err)
	}
	c.templates[cacheKey] = template
	return template, nil
}

func (c *Client) DecryptSamples(ctx context.Context, adamID, keyURI string, samples [][]byte) ([][]byte, error) {
	template, err := c.template(ctx, adamID, keyURI)
	if err != nil {
		return nil, err
	}
	plain, err := template.DecryptPar(samples)
	if err != nil {
		return nil, fmt.Errorf("Temari decrypt: %w", err)
	}
	return plain, nil
}
