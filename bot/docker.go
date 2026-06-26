package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"
)

// This file is the only place Karen talks to the Docker Engine. It exists solely
// for the admin /restart command, which cycles the wrapper-manager containers (in
// addition to the bot restarting itself via Docker's restart policy). It speaks the
// Engine API over the unix socket that docker-compose.yml bind-mounts into the bot
// container (/var/run/docker.sock). Everything here is best-effort and nil-safe: if
// the socket isn't mounted (or the bot lacks permission), calls fail and the caller
// degrades to "bot-only restart" rather than erroring out.

const dockerSocketPath = "/var/run/docker.sock"

// dockerHTTPClient returns an HTTP client whose transport dials the local Docker
// Engine over the unix socket instead of TCP. The host in request URLs is a dummy.
func dockerHTTPClient() *http.Client {
	return &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, "unix", dockerSocketPath)
			},
		},
	}
}

// restartContainer asks the Engine to restart one container by name
// (POST /containers/{name}/restart). t=10 grants a 10s graceful stop before SIGKILL.
// A 204 is success; 404 means no such container; anything else is an error.
func restartContainer(ctx context.Context, cli *http.Client, name string) error {
	url := fmt.Sprintf("http://docker/containers/%s/restart?t=10", name)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
	if err != nil {
		return err
	}
	resp, err := cli.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return nil
}

// wrapperContainerNames derives the wrapper-manager container names from the
// configured gRPC addresses (generate.sh emits "karen-wm-1:8081", …), de-duplicated
// and in config order.
func wrapperContainerNames() []string {
	var names []string
	seen := map[string]bool{}
	for _, addr := range Config.WrapperManagerAddrs {
		host := addr
		if h, _, err := net.SplitHostPort(addr); err == nil {
			host = h
		}
		if host != "" && !seen[host] {
			seen[host] = true
			names = append(names, host)
		}
	}
	return names
}

// restartAllWrappers restarts every configured wrapper-manager container via the
// Engine API. Best-effort: returns the names it could NOT restart (socket missing,
// container absent, engine error). An empty result means all succeeded — or none
// were configured.
func restartAllWrappers() []string {
	names := wrapperContainerNames()
	if len(names) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cli := dockerHTTPClient()
	var failed []string
	for _, name := range names {
		if err := restartContainer(ctx, cli, name); err != nil {
			fmt.Printf("Admin /restart: failed to restart %s: %v\n", name, err)
			failed = append(failed, name)
		} else {
			fmt.Printf("Admin /restart: restarted %s\n", name)
		}
	}
	return failed
}
