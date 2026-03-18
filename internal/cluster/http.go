package cluster

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

func (n *Node) routes() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/", n.handleDashboard)
	mux.HandleFunc("/dashboard", n.handleDashboard)
	mux.HandleFunc("/health", n.handleHealth)

	mux.HandleFunc("/api/v1/status", n.handleStatus)
	mux.HandleFunc("/api/v1/kv/", n.handleKV)
	mux.HandleFunc("/api/v1/lock/acquire", n.handleLockAcquire)
	mux.HandleFunc("/api/v1/lock/release", n.handleLockRelease)

	mux.HandleFunc("/internal/cluster/join", n.handleJoin)
	mux.HandleFunc("/internal/cluster/heartbeat", n.handleHeartbeat)
	mux.HandleFunc("/internal/cluster/election", n.handleElection)
	mux.HandleFunc("/internal/cluster/coordinator", n.handleCoordinator)
	mux.HandleFunc("/internal/cluster/append", n.handleAppend)
	mux.HandleFunc("/internal/cluster/sync", n.handleSync)

	return mux
}

func (n *Node) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":            true,
		"nodeId":        n.cfg.NodeID,
		"clusterId":     n.StatusSnapshot().ClusterID,
		"advertiseAddr": n.AdvertiseAddr(),
	})
}

func (n *Node) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	writeJSON(w, http.StatusOK, n.StatusSnapshot())
}

func (n *Node) handleKV(w http.ResponseWriter, r *http.Request) {
	keyPart := strings.TrimPrefix(r.URL.Path, "/api/v1/kv/")
	key, err := url.PathUnescape(keyPart)
	if err != nil || strings.TrimSpace(key) == "" {
		writeError(w, http.StatusBadRequest, "invalid key")
		return
	}

	switch r.Method {
	case http.MethodPut:
		if !n.isLeader() {
			if handled, proxyErr := n.forwardToLeader(w, r); handled {
				if proxyErr != nil {
					writeError(w, http.StatusBadGateway, proxyErr.Error())
				}
				return
			}
			writeLeaderError(w, n)
			return
		}

		var req WriteRequest
		if err := decodeJSON(r, &req); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		resp, err := n.ApplyWrite(key, req.Value)
		if err != nil {
			writeError(w, http.StatusConflict, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, resp)
	case http.MethodGet:
		if !n.isLeader() {
			if handled, proxyErr := n.forwardToLeader(w, r); handled {
				if proxyErr != nil {
					writeError(w, http.StatusBadGateway, proxyErr.Error())
				}
				return
			}
			writeLeaderError(w, n)
			return
		}

		n.mu.RLock()
		value, ok := n.kv[key]
		leaderID := n.leaderID
		n.mu.RUnlock()

		writeJSON(w, http.StatusOK, ReadResponse{
			Key:      key,
			Value:    value,
			Exists:   ok,
			LeaderID: leaderID,
		})
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (n *Node) handleLockAcquire(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if !n.isLeader() {
		if handled, proxyErr := n.forwardToLeader(w, r); handled {
			if proxyErr != nil {
				writeError(w, http.StatusBadGateway, proxyErr.Error())
			}
			return
		}
		writeLeaderError(w, n)
		return
	}

	var req LeaseRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.Name == "" {
		req.Name = "maintenance"
	}
	if req.Owner == "" {
		req.Owner = "operator"
	}
	if req.TTLSeconds <= 0 {
		req.TTLSeconds = 30
	}

	resp, err := n.AcquireLease(req.Name, req.Owner, time.Duration(req.TTLSeconds)*time.Second)
	if err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	status := http.StatusOK
	if !resp.Granted {
		status = http.StatusConflict
	}
	writeJSON(w, status, resp)
}

func (n *Node) handleLockRelease(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if !n.isLeader() {
		if handled, proxyErr := n.forwardToLeader(w, r); handled {
			if proxyErr != nil {
				writeError(w, http.StatusBadGateway, proxyErr.Error())
			}
			return
		}
		writeLeaderError(w, n)
		return
	}

	var req LeaseRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.Name == "" {
		req.Name = "maintenance"
	}

	resp, err := n.ReleaseLease(req.Name, req.Owner)
	if err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	status := http.StatusOK
	if !resp.Granted {
		status = http.StatusConflict
	}
	writeJSON(w, status, resp)
}

func (n *Node) handleJoin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req JoinRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	n.mu.Lock()
	if n.leaderID != n.cfg.NodeID {
		leaderAddr := n.leaderAddressLocked()
		n.mu.Unlock()
		writeJSON(w, http.StatusConflict, map[string]any{
			"error":      "join must be sent to the current leader",
			"leaderId":   n.leaderID,
			"leaderAddr": leaderAddr,
		})
		return
	}
	if req.ClusterID != n.clusterID {
		n.mu.Unlock()
		writeError(w, http.StatusForbidden, "cluster id does not match")
		return
	}
	if req.Token != n.joinToken {
		n.mu.Unlock()
		writeError(w, http.StatusForbidden, "invalid join token")
		return
	}

	now := time.Now().UTC()
	n.members[req.NodeID] = Member{
		ID:            req.NodeID,
		Address:       normalizeHTTPAddr(req.Address),
		Status:        "follower",
		LastSeen:      now,
		LastHeartbeat: now,
		LastApplied:   req.LastLogIndex,
		LastCommitted: n.commitIndex,
	}
	resp := JoinResponse{
		ClusterID:   n.clusterID,
		LeaderID:    n.leaderID,
		CurrentTerm: n.currentTerm,
		Lamport:     n.nextLamportLocked(0),
		CommitIndex: n.commitIndex,
		Members:     n.memberSliceLocked(),
		Log:         cloneLog(n.log),
		KV:          cloneMap(n.kv),
		Lease:       cloneLease(n.lease),
	}
	err := n.persistLocked()
	n.mu.Unlock()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, resp)
	n.runAsync(n.broadcastHeartbeat)
}

func (n *Node) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req HeartbeatRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, n.HandleHeartbeat(req))
}

func (n *Node) handleElection(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req ElectionRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, n.HandleElection(req))
}

func (n *Node) handleCoordinator(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req CoordinatorAnnouncement
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	n.HandleCoordinator(req)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (n *Node) handleAppend(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req AppendEntriesRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, n.HandleAppend(req))
}

func (n *Node) handleSync(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	writeJSON(w, http.StatusOK, n.HandleSync())
}

func (n *Node) isLeader() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.leaderID == n.cfg.NodeID
}

func (n *Node) forwardToLeader(w http.ResponseWriter, r *http.Request) (bool, error) {
	n.mu.RLock()
	targetBase := n.leaderAddressLocked()
	selfAddr := n.cfg.AdvertiseAddr
	n.mu.RUnlock()

	if targetBase == "" || targetBase == selfAddr {
		return false, nil
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		return true, fmt.Errorf("read request body: %w", err)
	}
	targetURL := joinURL(targetBase, r.URL.RequestURI())
	req, err := http.NewRequestWithContext(r.Context(), r.Method, targetURL, strings.NewReader(string(body)))
	if err != nil {
		return true, fmt.Errorf("create proxy request: %w", err)
	}
	req.Header = r.Header.Clone()

	resp, err := n.client.Do(req)
	if err != nil {
		return true, fmt.Errorf("proxy to leader: %w", err)
	}
	defer resp.Body.Close()

	for key, values := range resp.Header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	w.WriteHeader(resp.StatusCode)
	_, copyErr := io.Copy(w, resp.Body)
	return true, copyErr
}

func writeLeaderError(w http.ResponseWriter, n *Node) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	writeJSON(w, http.StatusConflict, map[string]any{
		"error":      "request must be sent to the leader",
		"leaderId":   n.leaderID,
		"leaderAddr": n.leaderAddressLocked(),
	})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]any{"error": msg})
}

func decodeJSON(r *http.Request, out any) error {
	defer r.Body.Close()
	if err := json.NewDecoder(r.Body).Decode(out); err != nil {
		if errors.Is(err, io.EOF) {
			return fmt.Errorf("request body is required")
		}
		return fmt.Errorf("decode json: %w", err)
	}
	return nil
}
