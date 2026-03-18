package cluster

import (
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"time"
)

func ParseServeConfig(command string, args []string, isInit bool) (Config, string, error) {
	fs := flag.NewFlagSet(command, flag.ContinueOnError)

	var cfg Config
	fs.IntVar(&cfg.NodeID, "node-id", 0, "numeric node identifier")
	fs.StringVar(&cfg.ListenAddr, "listen-addr", ":8080", "listen address for the HTTP server")
	fs.StringVar(&cfg.AdvertiseAddr, "advertise-addr", "", "public address other nodes should use, for example http://node1:8080")
	fs.StringVar(&cfg.DataDir, "data-dir", "", "directory used for persisted state")
	fs.StringVar(&cfg.ClusterID, "cluster-id", "", "cluster identifier")
	fs.StringVar(&cfg.JoinToken, "join-token", "", "shared join token")
	fs.StringVar(&cfg.ManagerAddr, "manager", "", "manager/leader address used for node join")

	var heartbeat string
	var electionTimeout string
	var joinRetry string
	fs.StringVar(&heartbeat, "heartbeat", "800ms", "heartbeat interval")
	fs.StringVar(&electionTimeout, "election-timeout", "2400ms", "leader election timeout")
	fs.StringVar(&joinRetry, "join-retry", "1s", "retry interval while waiting to join")

	if err := fs.Parse(args); err != nil {
		return Config{}, "", err
	}
	if cfg.NodeID <= 0 {
		return Config{}, "", fmt.Errorf("%s requires --node-id > 0", command)
	}

	var err error
	cfg.HeartbeatInterval, err = time.ParseDuration(heartbeat)
	if err != nil {
		return Config{}, "", fmt.Errorf("invalid --heartbeat: %w", err)
	}
	cfg.ElectionTimeout, err = time.ParseDuration(electionTimeout)
	if err != nil {
		return Config{}, "", fmt.Errorf("invalid --election-timeout: %w", err)
	}
	cfg.JoinRetryInterval, err = time.ParseDuration(joinRetry)
	if err != nil {
		return Config{}, "", fmt.Errorf("invalid --join-retry: %w", err)
	}

	if cfg.DataDir == "" {
		cfg.DataDir = filepath.Join("data", fmt.Sprintf("node-%d", cfg.NodeID))
	}
	cfg.AdvertiseAddr = normalizeHTTPAddr(cfg.AdvertiseAddr)
	cfg.ManagerAddr = normalizeHTTPAddr(cfg.ManagerAddr)

	if isInit {
		if cfg.ClusterID == "" {
			cfg.ClusterID, err = randomHex(8)
			if err != nil {
				return Config{}, "", err
			}
		}
		if cfg.JoinToken == "" {
			cfg.JoinToken, err = randomHex(16)
			if err != nil {
				return Config{}, "", err
			}
		}
	} else {
		if cfg.ClusterID == "" {
			return Config{}, "", fmt.Errorf("%s requires --cluster-id", command)
		}
		if cfg.JoinToken == "" {
			return Config{}, "", fmt.Errorf("%s requires --join-token", command)
		}
		if cfg.ManagerAddr == "" {
			return Config{}, "", fmt.Errorf("%s requires --manager", command)
		}
	}

	banner := fmt.Sprintf("%s with node-id=%d, listen-addr=%s, data-dir=%s", command, cfg.NodeID, cfg.ListenAddr, cfg.DataDir)
	return cfg, banner, nil
}

func normalizeHTTPAddr(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if strings.HasPrefix(raw, "http://") || strings.HasPrefix(raw, "https://") {
		return strings.TrimRight(raw, "/")
	}
	return "http://" + strings.TrimRight(raw, "/")
}

func joinURL(base, path string) string {
	base = normalizeHTTPAddr(base)
	return strings.TrimRight(base, "/") + path
}

func randomHex(byteLen int) (string, error) {
	buf := make([]byte, byteLen)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generate random token: %w", err)
	}
	return hex.EncodeToString(buf), nil
}

func hostPortURL(addr string) string {
	addr = normalizeHTTPAddr(addr)
	if addr == "" {
		return ""
	}
	u, err := url.Parse(addr)
	if err != nil {
		return addr
	}
	return u.Host
}
