package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
)

func GetWebPlayback(adamId string, token string, musicToken string) (string, error) {
	reqBody, err := json.Marshal(map[string]string{"salableAdamId": adamId})
	if err != nil {
		return "", err
	}
	req, err := http.NewRequest("POST", "https://play.music.apple.com/WebObjects/MZPlay.woa/wa/webPlayback", bytes.NewBuffer(reqBody))
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
	req.Header.Set("X-Apple-Music-User-Token", musicToken)
	req.Header.Set("Content-Type", "application/json")
	resp, err := GetHttpClient().Do(req)
	if err != nil {
		return "", err
	}
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	var bodyJson map[string]any
	err = json.Unmarshal(respBody, &bodyJson)
	if err != nil {
		return "", err
	}
	if bodyJson["errors"] != nil {
		return "", errors.New("failed to get asset")
	}
	songList, ok := bodyJson["songList"].([]any)
	if !ok || len(songList) == 0 {
		return "", errors.New("empty songList in WebPlayback response")
	}
	firstSong, ok := songList[0].(map[string]interface{})
	if !ok {
		return "", errors.New("invalid song entry in WebPlayback response")
	}
	if playlist, ok := firstSong["hls-playlist-url"].(string); ok && playlist != "" {
		return playlist, nil
	}
	assetsRaw, ok := firstSong["assets"].([]any)
	if !ok {
		return "", errors.New("no assets in WebPlayback response")
	}
	for _, a := range assetsRaw {
		asset, ok := a.(map[string]interface{})
		if !ok {
			continue
		}
		flavor, ok := asset["flavor"].(string)
		if !ok {
			continue
		}
		if flavor == "28:ctrp256" {
			url, ok := asset["URL"].(string)
			if !ok {
				continue
			}
			return url, nil
		}
	}
	return "", errors.New("no available asset")
}

func GetLicense(adamId string, challenge string, uri string, token string, musicToken string) (string, int, error) {
	reqBody, err := json.Marshal(map[string]any{"challenge": challenge, "uri": uri, "key-system": "com.widevine.alpha", "adamId": adamId, "isLibrary": false, "user-initiated": true})
	if err != nil {
		return "", 0, err
	}
	req, err := http.NewRequest("POST", "https://play.itunes.apple.com/WebObjects/MZPlay.woa/wa/acquireWebPlaybackLicense", bytes.NewBuffer(reqBody))
	if err != nil {
		return "", 0, err
	}
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
	req.Header.Set("X-Apple-Music-User-Token", musicToken)
	req.Header.Set("Content-Type", "application/json")
	resp, err := GetHttpClient().Do(req)
	if err != nil {
		return "", 0, err
	}
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", 0, err
	}
	var respJson map[string]any
	err = json.Unmarshal(respBody, &respJson)
	if err != nil {
		return "", 0, err
	}
	if respJson["errors"] != nil {
		return "", 0, errors.New("failed to get license")
	}
	license, ok := respJson["license"].(string)
	if !ok || license == "" {
		return "", 0, errors.New("failed to get license")
	}
	// renew-after is optional and not always present; default to 0 when absent
	// or of an unexpected type rather than panicking on a bare type assertion.
	renew := 0
	if r, ok := respJson["renew-after"].(float64); ok {
		renew = int(r)
	}
	return license, renew, nil
}
