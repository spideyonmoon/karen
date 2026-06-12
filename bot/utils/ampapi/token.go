package ampapi

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
)

func GetToken() (string, error) {
	req, err := http.NewRequest("GET", "https://music.apple.com", nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	regex := regexp.MustCompile(`/assets/index[-~.][^"' <>]+\.js`)
	indexJsUri := regex.FindString(string(body))
	if indexJsUri == "" {
		return "", errors.New("GetToken: could not find index JS bundle URL in music.apple.com response")
	}
	fmt.Println("GetToken: found bundle", indexJsUri)

	req, err = http.NewRequest("GET", "https://music.apple.com"+indexJsUri, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")

	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err = io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	regex = regexp.MustCompile(`eyJ([^"'` + "`" + `]+)`)
	token := regex.FindString(string(body))
	if token == "" {
		return "", errors.New("GetToken: could not extract JWT from bundle " + indexJsUri)
	}

	return token, nil
}
