package utils

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
)

type gofileResponse struct {
	Status string `json:"status"`
	Data   struct {
		DownloadPage string `json:"downloadPage"`
	} `json:"data"`
}

func UploadToGofile(filePath, token string) (string, error) {
	serverURL := "https://upload-ap-sgp.gofile.io/uploadFile"
	if token != "" {
		serverURL = "https://upload-ap-sgp.gofile.io/contents/uploadfile"
	}

	file, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, err := writer.CreateFormFile("file", filepath.Base(filePath))
	if err != nil {
		return "", fmt.Errorf("failed to create form file: %w", err)
	}
	_, err = io.Copy(part, file)
	if err != nil {
		return "", fmt.Errorf("failed to copy file contents: %w", err)
	}

	err = writer.Close()
	if err != nil {
		return "", fmt.Errorf("failed to close multipart writer: %w", err)
	}

	req, err := http.NewRequest("POST", serverURL, body)
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", writer.FormDataContentType())
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("gofile upload request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("gofile upload returned non-200 status: %d, response: %s", resp.StatusCode, string(respBody))
	}

	var gofileResp gofileResponse
	if err := json.Unmarshal(respBody, &gofileResp); err != nil {
		return "", fmt.Errorf("failed to parse gofile response: %w, response: %s", err, string(respBody))
	}

	if gofileResp.Status != "ok" {
		return "", fmt.Errorf("gofile API returned error status: %s", gofileResp.Status)
	}

	return gofileResp.Data.DownloadPage, nil
}
