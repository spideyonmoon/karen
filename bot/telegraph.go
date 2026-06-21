package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// =============================================================================
// Telegraph publishing (telegra.ph) — used by /check to host tracklists too
// large to inline in a Telegram message. telegra.ph is Telegram's own service:
// pages render as Instant View, and an anonymous account (one createAccount
// call, token cached in memory) is enough to publish.
//
// Note: Telegraph's content model has NO <table> — only lists, paragraphs, and
// a few inline tags — so tracklists are rendered as an ordered list.
// =============================================================================

var (
	tgTokenMu sync.Mutex
	tgToken   string // cached anonymous access token (process lifetime)
)

var telegraphHTTP = &http.Client{Timeout: 30 * time.Second}

// telegraphAccessToken returns a cached anonymous access token, creating one on
// first use.
func telegraphAccessToken() (string, error) {
	tgTokenMu.Lock()
	defer tgTokenMu.Unlock()
	if tgToken != "" {
		return tgToken, nil
	}
	form := url.Values{}
	form.Set("short_name", "Karen")
	form.Set("author_name", "Karen")
	resp, err := telegraphHTTP.PostForm("https://api.telegra.ph/createAccount", form)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var out struct {
		Ok     bool `json:"ok"`
		Result struct {
			AccessToken string `json:"access_token"`
		} `json:"result"`
		Error string `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", err
	}
	if !out.Ok || out.Result.AccessToken == "" {
		return "", errors.New("telegraph createAccount failed: " + out.Error)
	}
	tgToken = out.Result.AccessToken
	return tgToken, nil
}

// tgText is a Telegraph text node (a plain JSON string in the content array).
func tgText(s string) interface{} { return s }

// tgEl builds a Telegraph element node {tag, children}. Allowed tags include
// p, ol, ul, li, b, i, a, h3, h4, blockquote, br — but NOT table.
func tgEl(tag string, children ...interface{}) interface{} {
	m := map[string]interface{}{"tag": tag}
	if len(children) > 0 {
		m["children"] = children
	}
	return m
}

// telegraphTitle clamps a page title to Telegraph's 1–256 char window.
func telegraphTitle(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		s = "Tracklist"
	}
	if len(s) > 256 {
		s = s[:256]
	}
	return s
}

// telegraphCreatePage publishes a page and returns its URL. content is a slice
// of Telegraph nodes (strings and/or tgEl elements).
func telegraphCreatePage(title, authorName string, content []interface{}) (string, error) {
	token, err := telegraphAccessToken()
	if err != nil {
		return "", err
	}
	cj, err := json.Marshal(content)
	if err != nil {
		return "", err
	}
	form := url.Values{}
	form.Set("access_token", token)
	form.Set("title", telegraphTitle(title))
	if authorName != "" {
		form.Set("author_name", authorName)
	}
	form.Set("content", string(cj))
	form.Set("return_content", "false")
	resp, err := telegraphHTTP.PostForm("https://api.telegra.ph/createPage", form)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var out struct {
		Ok     bool `json:"ok"`
		Result struct {
			URL string `json:"url"`
		} `json:"result"`
		Error string `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", err
	}
	if !out.Ok || out.Result.URL == "" {
		return "", errors.New("telegraph createPage failed: " + out.Error)
	}
	return out.Result.URL, nil
}
