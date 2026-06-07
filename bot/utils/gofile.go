package utils

import (
	"bytes"
	"context"
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

func getGofileServer(ctx context.Context) (string, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", "https://api.gofile.io/servers", nil)
	if err != nil {
		return "", err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var result struct {
		Status string `json:"status"`
		Data   struct {
			Servers []struct {
				Name string `json:"name"`
			} `json:"servers"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", err
	}
	if result.Status != "ok" || len(result.Data.Servers) == 0 {
		return "", fmt.Errorf("failed to get gofile servers, status: %s", result.Status)
	}
	return result.Data.Servers[0].Name, nil
}

func UploadToGofile(ctx context.Context, filePath, token string) (string, error) {
	serverName, err := getGofileServer(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to fetch optimal gofile server: %w", err)
	}

	serverURL := fmt.Sprintf("https://%s.gofile.io/uploadFile", serverName)
	if token != "" {
		serverURL = fmt.Sprintf("https://%s.gofile.io/contents/uploadfile", serverName)
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

	req, err := http.NewRequestWithContext(ctx, "POST", serverURL, body)
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
