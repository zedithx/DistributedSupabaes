package cluster

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

type ControlPlaneGateway struct {
	listenAddr string
	targets    []string
	client     *http.Client

	mu           sync.RWMutex
	activeTarget string
}

func RunControlPlane(args []string) error {
	fs := flag.NewFlagSet("control-plane serve", flag.ContinueOnError)
	listenAddr := fs.String("listen-addr", ":8090", "listen address for the immortal control-plane gateway")
	targetsRaw := fs.String("targets", "", "comma-separated node addresses, for example http://node1:8080,http://node2:8080")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*targetsRaw) == "" {
		return fmt.Errorf("control-plane serve requires --targets")
	}

	targets := make([]string, 0)
	for _, target := range strings.Split(*targetsRaw, ",") {
		target = normalizeHTTPAddr(strings.TrimSpace(target))
		if target != "" {
			targets = append(targets, target)
		}
	}
	if len(targets) == 0 {
		return fmt.Errorf("control-plane serve requires at least one valid target")
	}

	gateway := &ControlPlaneGateway{
		listenAddr: *listenAddr,
		targets:    targets,
		client: &http.Client{
			Timeout: 8 * time.Second,
		},
	}

	fmt.Printf("control-plane listening on %s\n", *listenAddr)
	fmt.Printf("seed-targets=%s\n", strings.Join(targets, ","))

	server := &http.Server{
		Addr:         *listenAddr,
		Handler:      gateway.routes(),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  30 * time.Second,
	}
	return server.ListenAndServe()
}

func (g *ControlPlaneGateway) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", g.handleHealth)
	mux.HandleFunc("/", g.handleProxy)
	return mux
}

func (g *ControlPlaneGateway) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	target, err := g.pickHealthyTarget(r.Context())
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"ok":           false,
			"gateway":      true,
			"activeTarget": g.currentTarget(),
			"targets":      g.targets,
			"error":        err.Error(),
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"ok":           true,
		"gateway":      true,
		"activeTarget": target,
		"targets":      g.targets,
		"mode":         "immortal control-plane gateway",
	})
}

func (g *ControlPlaneGateway) handleProxy(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/internal/cluster/") {
		writeError(w, http.StatusForbidden, "internal cluster endpoints are not exposed through the control-plane gateway")
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("read request body: %v", err))
		return
	}
	_ = r.Body.Close()

	target, err := g.pickHealthyTarget(r.Context())
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err.Error())
		return
	}

	resp, err := g.doProxyRequest(r.Context(), target, r, body)
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	defer resp.Body.Close()

	for key, values := range resp.Header {
		if strings.EqualFold(key, "Content-Length") {
			continue
		}
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	w.Header().Set("X-Supaswarm-Gateway-Target", target)
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func (g *ControlPlaneGateway) pickHealthyTarget(ctx context.Context) (string, error) {
	ordered := g.targetOrder()
	if len(ordered) == 0 {
		return "", fmt.Errorf("no control-plane targets configured")
	}

	for _, target := range ordered {
		if err := g.pingTarget(ctx, target); err == nil {
			g.setCurrentTarget(target)
			return target, nil
		}
	}
	return "", fmt.Errorf("no healthy cluster node is reachable through the configured control-plane targets")
}

func (g *ControlPlaneGateway) targetOrder() []string {
	g.mu.RLock()
	active := g.activeTarget
	g.mu.RUnlock()

	if active == "" {
		out := make([]string, len(g.targets))
		copy(out, g.targets)
		return out
	}

	out := make([]string, 0, len(g.targets))
	out = append(out, active)
	for _, target := range g.targets {
		if target != active {
			out = append(out, target)
		}
	}
	return out
}

func (g *ControlPlaneGateway) pingTarget(ctx context.Context, target string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, joinURL(target, "/health"), nil)
	if err != nil {
		return err
	}
	resp, err := g.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= http.StatusBadRequest {
		return fmt.Errorf("target %s returned %s", target, resp.Status)
	}
	return nil
}

func (g *ControlPlaneGateway) doProxyRequest(ctx context.Context, target string, original *http.Request, body []byte) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, original.Method, joinURL(target, original.URL.RequestURI()), bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create proxy request: %w", err)
	}
	req.Header = original.Header.Clone()

	resp, err := g.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("proxy request to %s failed: %w", target, err)
	}
	return resp, nil
}

func (g *ControlPlaneGateway) currentTarget() string {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.activeTarget
}

func (g *ControlPlaneGateway) setCurrentTarget(target string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.activeTarget = target
}
