package utils

import (
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

func UploadToGofile(ctx context.Context, filePath, token string) (string, error) {
	return UploadToGofileAs(ctx, filePath, token, "")
}

// UploadToGofileAs uploads filePath to Gofile but presents it under uploadName,
// so the link shows a human-friendly filename instead of the on-disk name (e.g. a
// temp "amdl-1234567.zip"). An empty uploadName falls back to the file's basename.
func UploadToGofileAs(ctx context.Context, filePath, token, uploadName string) (string, error) {
	if uploadName == "" {
		uploadName = filepath.Base(filePath)
	}

	// Try guest upload first
	guestURL := "https://upload-ap-sgp.gofile.io/uploadFile"
	downloadPage, err := uploadToGofileEndpoint(ctx, filePath, guestURL, "", uploadName)
	if err == nil {
		return downloadPage, nil
	}

	// If guest upload fails and token is available, try authenticated upload
	if token != "" {
		authURL := "https://upload-ap-sgp.gofile.io/contents/uploadfile"
		downloadPage, errAuth := uploadToGofileEndpoint(ctx, filePath, authURL, token, uploadName)
		if errAuth == nil {
			return downloadPage, nil
		}
		return "", fmt.Errorf("both guest and authenticated gofile uploads failed. Guest error: %v, Auth error: %v", err, errAuth)
	}

	return "", fmt.Errorf("gofile guest upload failed and no token provided: %w", err)
}

func uploadToGofileEndpoint(ctx context.Context, filePath, serverURL, token, uploadName string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	pr, pw := io.Pipe()
	writer := multipart.NewWriter(pw)
	contentType := writer.FormDataContentType()
	writeErrCh := make(chan error, 1)

	go func() {
		defer pw.Close()
		err := func() error {
			part, err := writer.CreateFormFile("file", uploadName)
			if err != nil {
				return fmt.Errorf("failed to create form file: %w", err)
			}
			_, err = io.Copy(part, file)
			if err != nil {
				return fmt.Errorf("failed to copy file contents: %w", err)
			}
			return writer.Close()
		}()
		writeErrCh <- err
		if err != nil {
			_ = pw.CloseWithError(err)
		}
	}()

	req, err := http.NewRequestWithContext(ctx, "POST", serverURL, pr)
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", contentType)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("gofile upload request failed: %w", err)
	}
	defer resp.Body.Close()

	// Wait/check for writer error
	select {
	case writeErr := <-writeErrCh:
		if writeErr != nil {
			return "", fmt.Errorf("multipart write failed: %w", writeErr)
		}
	default:
	}

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
