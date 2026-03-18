package cluster

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestGatewayFallsBackToHealthyTarget(t *testing.T) {
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			http.Error(w, "down", http.StatusServiceUnavailable)
			return
		}
		http.Error(w, "bad target", http.StatusBadGateway)
	}))
	defer bad.Close()

	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/health":
			writeJSON(w, http.StatusOK, map[string]any{"ok": true})
		case "/api/v1/control-plane":
			writeJSON(w, http.StatusOK, map[string]any{"clusterId": "demo", "leaderId": 3})
		default:
			http.NotFound(w, r)
		}
	}))
	defer good.Close()

	gateway := &ControlPlaneGateway{
		targets: []string{bad.URL, good.URL},
		client:  &http.Client{Timeout: 2 * time.Second},
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/control-plane", nil)
	rec := httptest.NewRecorder()
	gateway.handleProxy(rec, req)

	res := rec.Result()
	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)

	if res.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from healthy fallback target, got %d: %s", res.StatusCode, string(body))
	}
	if got := res.Header.Get("X-Supaswarm-Gateway-Target"); got != good.URL {
		t.Fatalf("expected gateway to target %s, got %s", good.URL, got)
	}
}

func TestGatewayBlocksInternalEndpoints(t *testing.T) {
	gateway := &ControlPlaneGateway{
		targets: []string{"http://127.0.0.1:1234"},
		client:  &http.Client{Timeout: 2 * time.Second},
	}

	req := httptest.NewRequest(http.MethodPost, "/internal/cluster/join", nil)
	rec := httptest.NewRecorder()
	gateway.handleProxy(rec, req)

	if rec.Result().StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 for internal cluster endpoint, got %d", rec.Result().StatusCode)
	}
}
