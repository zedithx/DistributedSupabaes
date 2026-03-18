package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

type Node struct {
	cfg    Config
	client *http.Client

	mu                  sync.RWMutex
	clusterID           string
	joinToken           string
	currentTerm         int
	leaderID            int
	lamport             int64
	commitIndex         int
	members             map[int]Member
	log                 []LogEntry
	kv                  map[string]string
	lease               *LeaseState
	lastLeaderHeartbeat time.Time
	electionInProgress  bool

	server   *http.Server
	listener net.Listener
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

func NewNode(cfg Config) (*Node, error) {
	if cfg.ListenAddr == "" {
		cfg.ListenAddr = ":8080"
	}
	if cfg.DataDir == "" {
		cfg.DataDir = filepath.Join("data", fmt.Sprintf("node-%d", cfg.NodeID))
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = 800 * time.Millisecond
	}
	if cfg.ElectionTimeout <= 0 {
		cfg.ElectionTimeout = 2400 * time.Millisecond
	}
	if cfg.JoinRetryInterval <= 0 {
		cfg.JoinRetryInterval = time.Second
	}

	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	node := &Node{
		cfg:     cfg,
		client:  &http.Client{Timeout: 2 * cfg.ElectionTimeout},
		members: make(map[int]Member),
		log:     make([]LogEntry, 0),
		kv:      make(map[string]string),
	}

	if err := node.loadState(); err != nil {
		return nil, err
	}
	if node.clusterID == "" && cfg.ClusterID != "" {
		node.clusterID = cfg.ClusterID
	}
	if node.joinToken == "" && cfg.JoinToken != "" {
		node.joinToken = cfg.JoinToken
	}

	return node, nil
}

func (n *Node) Start() error {
	n.mu.Lock()
	if n.ctx != nil {
		n.mu.Unlock()
		return nil
	}
	n.ctx, n.cancel = context.WithCancel(context.Background())
	n.mu.Unlock()

	ln, err := net.Listen("tcp", n.cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", n.cfg.ListenAddr, err)
	}

	n.mu.Lock()
	n.listener = ln
	if n.cfg.AdvertiseAddr == "" {
		n.cfg.AdvertiseAddr = normalizeHTTPAddr("http://" + ln.Addr().String())
	}
	now := time.Now().UTC()
	self := n.members[n.cfg.NodeID]
	self.ID = n.cfg.NodeID
	self.Address = n.cfg.AdvertiseAddr
	self.Status = coalesceStatus(self.Status, "starting")
	self.LastSeen = now
	self.LastHeartbeat = now
	n.members[n.cfg.NodeID] = self
	if err := n.persistLocked(); err != nil {
		n.mu.Unlock()
		_ = ln.Close()
		return err
	}
	n.mu.Unlock()

	n.server = &http.Server{
		Handler:      n.routes(),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  30 * time.Second,
	}

	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		if err := n.server.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fmt.Fprintf(os.Stderr, "server error: %v\n", err)
		}
	}()

	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		n.heartbeatLoop()
	}()

	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		n.electionLoop()
	}()

	return nil
}

func (n *Node) Shutdown(ctx context.Context) error {
	n.mu.RLock()
	cancel := n.cancel
	n.mu.RUnlock()
	if cancel != nil {
		cancel()
	}

	var err error
	if n.server != nil {
		err = n.server.Shutdown(ctx)
	}

	done := make(chan struct{})
	go func() {
		n.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		if err == nil {
			err = ctx.Err()
		}
	}

	n.mu.Lock()
	persistErr := n.persistLocked()
	n.mu.Unlock()
	if err != nil {
		return err
	}
	return persistErr
}

func (n *Node) InitCluster() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.clusterID == "" {
		n.clusterID = n.cfg.ClusterID
	}
	if n.joinToken == "" {
		n.joinToken = n.cfg.JoinToken
	}
	now := time.Now().UTC()
	n.currentTerm = maxInt(n.currentTerm, 1)
	n.leaderID = n.cfg.NodeID
	n.lastLeaderHeartbeat = now
	n.nextLamportLocked(0)

	self := n.members[n.cfg.NodeID]
	self.ID = n.cfg.NodeID
	self.Address = n.cfg.AdvertiseAddr
	self.Status = "leader"
	self.LastSeen = now
	self.LastHeartbeat = now
	n.members[n.cfg.NodeID] = self

	return n.persistLocked()
}

func (n *Node) JoinCluster(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	req := JoinRequest{
		NodeID:       n.cfg.NodeID,
		Address:      n.AdvertiseAddr(),
		ClusterID:    n.cfg.ClusterID,
		Token:        n.cfg.JoinToken,
		LastLogIndex: n.LastLogIndex(),
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		var resp JoinResponse
		err := n.postJSON(ctx, joinURL(n.cfg.ManagerAddr, "/internal/cluster/join"), req, &resp)
		if err == nil {
			n.mu.Lock()
			n.clusterID = resp.ClusterID
			n.joinToken = n.cfg.JoinToken
			n.currentTerm = resp.CurrentTerm
			n.leaderID = resp.LeaderID
			n.lamport = maxInt64(n.lamport, resp.Lamport)
			n.commitIndex = resp.CommitIndex
			n.members = membersFromSlice(resp.Members)
			n.members[n.cfg.NodeID] = Member{
				ID:            n.cfg.NodeID,
				Address:       n.cfg.AdvertiseAddr,
				Status:        "follower",
				LastSeen:      time.Now().UTC(),
				LastHeartbeat: time.Now().UTC(),
			}
			n.log = cloneLog(resp.Log)
			n.kv = cloneMap(resp.KV)
			n.lease = cloneLease(resp.Lease)
			n.lastLeaderHeartbeat = time.Now().UTC()
			if err := n.persistLocked(); err != nil {
				n.mu.Unlock()
				return err
			}
			n.mu.Unlock()
			return nil
		}

		timer := time.NewTimer(n.cfg.JoinRetryInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (n *Node) AdvertiseAddr() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.cfg.AdvertiseAddr
}

func (n *Node) LastLogIndex() int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.lastLogIndexLocked()
}

func (n *Node) StatusSnapshot() StatusResponse {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.clearExpiredLeaseLocked(time.Now().UTC())

	return StatusResponse{
		NodeID:        n.cfg.NodeID,
		ClusterID:     n.clusterID,
		LeaderID:      n.leaderID,
		CurrentTerm:   n.currentTerm,
		Lamport:       n.lamport,
		IsLeader:      n.leaderID == n.cfg.NodeID,
		AdvertiseAddr: n.cfg.AdvertiseAddr,
		CommitIndex:   n.commitIndex,
		LastLogIndex:  n.lastLogIndexLocked(),
		LastHeartbeat: n.lastLeaderHeartbeat,
		Members:       n.memberSliceLocked(),
		Data:          cloneMap(n.kv),
		Lease:         cloneLease(n.lease),
		Log:           cloneLog(n.log),
	}
}

func (n *Node) ControlPlaneSnapshot(ctx context.Context) ControlPlaneResponse {
	local := n.StatusSnapshot()

	n.mu.RLock()
	leaderAddr := n.leaderAddressLocked()
	selfAddr := n.cfg.AdvertiseAddr
	n.mu.RUnlock()

	if leaderAddr == "" || local.IsLeader || leaderAddr == selfAddr {
		return ControlPlaneResponse{
			StatusResponse:      local,
			SourceNodeID:        local.NodeID,
			SourceAdvertiseAddr: local.AdvertiseAddr,
			ServedByLeader:      local.IsLeader || leaderAddr == "" || leaderAddr == selfAddr,
			Degraded:            false,
			DiscoveryMethod:     "cluster membership table maintained by heartbeats and join events",
		}
	}

	leaderStatus, err := n.fetchRemoteStatus(ctx, leaderAddr)
	if err != nil {
		return ControlPlaneResponse{
			StatusResponse:      local,
			SourceNodeID:        local.NodeID,
			SourceAdvertiseAddr: local.AdvertiseAddr,
			ServedByLeader:      false,
			Degraded:            true,
			DiscoveryMethod:     "cluster membership table maintained by heartbeats and join events",
		}
	}

	return ControlPlaneResponse{
		StatusResponse:      leaderStatus,
		SourceNodeID:        leaderStatus.NodeID,
		SourceAdvertiseAddr: leaderStatus.AdvertiseAddr,
		ServedByLeader:      true,
		Degraded:            false,
		DiscoveryMethod:     "cluster membership table maintained by heartbeats and join events",
	}
}

func (n *Node) heartbeatLoop() {
	ticker := time.NewTicker(n.cfg.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-n.ctx.Done():
			return
		case <-ticker.C:
			n.broadcastHeartbeat()
		}
	}
}

func (n *Node) electionLoop() {
	ticker := time.NewTicker(maxDuration(250*time.Millisecond, n.cfg.HeartbeatInterval/2))
	defer ticker.Stop()

	for {
		select {
		case <-n.ctx.Done():
			return
		case <-ticker.C:
			n.maybeStartElection()
		}
	}
}

func (n *Node) maybeStartElection() {
	n.mu.Lock()
	if n.clusterID == "" || n.leaderID == n.cfg.NodeID {
		n.mu.Unlock()
		return
	}
	if len(n.members) <= 1 {
		n.leaderID = n.cfg.NodeID
		n.currentTerm = maxInt(n.currentTerm, 1)
		n.electionInProgress = false
		n.mu.Unlock()
		return
	}
	if n.electionInProgress {
		n.mu.Unlock()
		return
	}
	if !n.lastLeaderHeartbeat.IsZero() && time.Since(n.lastLeaderHeartbeat) < n.cfg.ElectionTimeout {
		n.mu.Unlock()
		return
	}

	n.electionInProgress = true
	n.currentTerm++
	term := n.currentTerm
	lamport := n.nextLamportLocked(0)
	higher := make([]Member, 0)
	for _, member := range n.members {
		if member.ID > n.cfg.NodeID && member.Address != "" {
			higher = append(higher, member)
		}
	}
	n.mu.Unlock()

	higherAlive := false
	for _, member := range higher {
		var resp ElectionResponse
		err := n.postJSON(context.Background(), joinURL(member.Address, "/internal/cluster/election"), ElectionRequest{
			CandidateID: n.cfg.NodeID,
			Term:        term,
			Lamport:     lamport,
		}, &resp)
		if err == nil && resp.Alive {
			higherAlive = true
		}
	}

	if higherAlive {
		timer := time.NewTimer(n.cfg.ElectionTimeout / 2)
		defer timer.Stop()
		select {
		case <-n.ctx.Done():
		case <-timer.C:
		}

		n.mu.Lock()
		n.electionInProgress = false
		n.mu.Unlock()
		return
	}

	n.becomeLeader(term)
}

func (n *Node) becomeLeader(term int) {
	n.mu.Lock()
	if n.currentTerm < term {
		n.currentTerm = term
	}
	n.truncateToCommitLocked()
	n.rebuildStateMachineLocked()
	n.leaderID = n.cfg.NodeID
	n.lastLeaderHeartbeat = time.Now().UTC()
	n.electionInProgress = false
	n.nextLamportLocked(0)

	self := n.members[n.cfg.NodeID]
	self.ID = n.cfg.NodeID
	self.Address = n.cfg.AdvertiseAddr
	self.Status = "leader"
	self.LastSeen = time.Now().UTC()
	self.LastHeartbeat = time.Now().UTC()
	self.LastApplied = n.commitIndex
	self.LastCommitted = n.commitIndex
	n.members[n.cfg.NodeID] = self

	peers := n.membersWithoutSelfLocked()
	announcement := CoordinatorAnnouncement{
		LeaderID: n.cfg.NodeID,
		Term:     n.currentTerm,
		Lamport:  n.lamport,
	}
	_ = n.persistLocked()
	n.mu.Unlock()

	for _, peer := range peers {
		var resp map[string]any
		_ = n.postJSON(context.Background(), joinURL(peer.Address, "/internal/cluster/coordinator"), announcement, &resp)
	}
	n.broadcastHeartbeat()
}

func (n *Node) broadcastHeartbeat() {
	n.mu.Lock()
	if n.clusterID == "" || n.leaderID != n.cfg.NodeID {
		n.mu.Unlock()
		return
	}
	peers := n.membersWithoutSelfLocked()
	req := HeartbeatRequest{
		LeaderID:     n.cfg.NodeID,
		Term:         n.currentTerm,
		Lamport:      n.nextLamportLocked(0),
		CommitIndex:  n.commitIndex,
		LastLogIndex: n.lastLogIndexLocked(),
		Members:      n.memberSliceLocked(),
	}
	self := n.members[n.cfg.NodeID]
	self.Status = "leader"
	self.LastSeen = time.Now().UTC()
	self.LastHeartbeat = time.Now().UTC()
	self.LastApplied = n.commitIndex
	self.LastCommitted = n.commitIndex
	n.members[n.cfg.NodeID] = self
	_ = n.persistLocked()
	n.mu.Unlock()

	for _, peer := range peers {
		var resp HeartbeatResponse
		err := n.postJSON(context.Background(), joinURL(peer.Address, "/internal/cluster/heartbeat"), req, &resp)
		n.mu.Lock()
		member := n.members[peer.ID]
		member.ID = peer.ID
		member.Address = peer.Address
		member.LastSeen = time.Now().UTC()
		if err != nil {
			member.Status = "suspect"
		} else {
			member.Status = "follower"
			member.LastHeartbeat = time.Now().UTC()
			member.LastApplied = resp.LastLogIndex
			member.LastCommitted = minInt(req.CommitIndex, resp.LastLogIndex)
			member.LastLamportSeen = resp.Lamport
		}
		n.members[peer.ID] = member
		_ = n.persistLocked()
		n.mu.Unlock()
	}
}

func (n *Node) HandleHeartbeat(req HeartbeatRequest) HeartbeatResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.nextLamportLocked(req.Lamport)
	if req.Term < n.currentTerm {
		return HeartbeatResponse{OK: false, Lamport: n.lamport, LastLogIndex: n.lastLogIndexLocked()}
	}

	n.clusterID = coalesceString(n.clusterID, n.cfg.ClusterID)
	n.currentTerm = maxInt(n.currentTerm, req.Term)
	n.leaderID = req.LeaderID
	n.lastLeaderHeartbeat = time.Now().UTC()
	n.mergeMembersLocked(req.Members)

	if n.leaderID != n.cfg.NodeID {
		self := n.members[n.cfg.NodeID]
		self.ID = n.cfg.NodeID
		self.Address = n.cfg.AdvertiseAddr
		self.Status = "follower"
		self.LastSeen = time.Now().UTC()
		self.LastHeartbeat = time.Now().UTC()
		n.members[n.cfg.NodeID] = self
	}

	needSync := req.LastLogIndex > n.lastLogIndexLocked()
	if req.CommitIndex > n.commitIndex {
		n.commitIndex = minInt(req.CommitIndex, n.lastLogIndexLocked())
		n.rebuildStateMachineLocked()
	}
	_ = n.persistLocked()

	if needSync {
		n.runAsync(n.syncFromLeader)
	}

	return HeartbeatResponse{
		OK:           true,
		Lamport:      n.lamport,
		LastLogIndex: n.lastLogIndexLocked(),
	}
}

func (n *Node) HandleElection(req ElectionRequest) ElectionResponse {
	n.mu.Lock()
	n.nextLamportLocked(req.Lamport)
	n.currentTerm = maxInt(n.currentTerm, req.Term)
	alive := n.cfg.NodeID > req.CandidateID
	lamport := n.lamport
	nodeID := n.cfg.NodeID
	n.mu.Unlock()

	if alive {
		n.runAsync(n.maybeStartElection)
	}

	return ElectionResponse{
		Alive:   alive,
		NodeID:  nodeID,
		Term:    maxInt(req.Term, n.currentTerm),
		Lamport: lamport,
	}
}

func (n *Node) HandleCoordinator(req CoordinatorAnnouncement) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.nextLamportLocked(req.Lamport)
	n.currentTerm = maxInt(n.currentTerm, req.Term)
	n.leaderID = req.LeaderID
	n.lastLeaderHeartbeat = time.Now().UTC()
	n.electionInProgress = false

	if req.LeaderID != n.cfg.NodeID {
		self := n.members[n.cfg.NodeID]
		self.ID = n.cfg.NodeID
		self.Address = n.cfg.AdvertiseAddr
		self.Status = "follower"
		self.LastSeen = time.Now().UTC()
		n.members[n.cfg.NodeID] = self
	}

	_ = n.persistLocked()
}

func (n *Node) HandleAppend(req AppendEntriesRequest) AppendEntriesResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.nextLamportLocked(req.Lamport)
	if req.Term < n.currentTerm {
		return AppendEntriesResponse{OK: false, Lamport: n.lamport, LastLogIndex: n.lastLogIndexLocked()}
	}

	n.currentTerm = maxInt(n.currentTerm, req.Term)
	n.leaderID = req.LeaderID
	n.lastLeaderHeartbeat = time.Now().UTC()

	index := req.Entry.Index
	switch {
	case index == n.lastLogIndexLocked()+1:
		n.log = append(n.log, req.Entry)
	case index > 0 && index <= n.lastLogIndexLocked():
		n.log = append(cloneLog(n.log[:index-1]), req.Entry)
	default:
		n.runAsync(n.syncFromLeader)
		return AppendEntriesResponse{OK: false, Lamport: n.lamport, LastLogIndex: n.lastLogIndexLocked()}
	}

	if req.CommitIndex > n.commitIndex {
		n.commitIndex = minInt(req.CommitIndex, n.lastLogIndexLocked())
		n.rebuildStateMachineLocked()
	}
	self := n.members[n.cfg.NodeID]
	self.ID = n.cfg.NodeID
	self.Address = n.cfg.AdvertiseAddr
	self.Status = "follower"
	self.LastSeen = time.Now().UTC()
	self.LastApplied = n.lastLogIndexLocked()
	self.LastCommitted = n.commitIndex
	n.members[n.cfg.NodeID] = self
	_ = n.persistLocked()

	return AppendEntriesResponse{
		OK:           true,
		Lamport:      n.lamport,
		LastLogIndex: n.lastLogIndexLocked(),
	}
}

func (n *Node) HandleSync() SyncResponse {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.clearExpiredLeaseLocked(time.Now().UTC())

	return SyncResponse{
		ClusterID:   n.clusterID,
		LeaderID:    n.leaderID,
		CurrentTerm: n.currentTerm,
		Lamport:     n.lamport,
		CommitIndex: n.commitIndex,
		Members:     n.memberSliceLocked(),
		Log:         cloneLog(n.log),
		KV:          cloneMap(n.kv),
		Lease:       cloneLease(n.lease),
	}
}

func (n *Node) ApplyWrite(key, value string) (WriteResponse, error) {
	entry := LogEntry{
		Type:      "put",
		Key:       key,
		Value:     value,
		Timestamp: time.Now().UTC(),
	}
	committed, err := n.replicateEntry(entry)
	if err != nil {
		return WriteResponse{}, err
	}

	n.mu.RLock()
	defer n.mu.RUnlock()
	return WriteResponse{
		Key:       key,
		Value:     value,
		Committed: true,
		LeaderID:  n.leaderID,
		LogIndex:  committed.Index,
	}, nil
}

func (n *Node) AcquireLease(name, owner string, ttl time.Duration) (LeaseResponse, error) {
	n.mu.Lock()
	n.clearExpiredLeaseLocked(time.Now().UTC())
	if n.lease != nil && n.lease.Name == name {
		if n.lease.Owner == owner {
			lease := cloneLease(n.lease)
			n.mu.Unlock()
			return LeaseResponse{
				Granted:  true,
				LeaderID: n.leaderID,
				Lease:    lease,
				Message:  "lease already held by owner",
			}, nil
		}
		lease := cloneLease(n.lease)
		n.mu.Unlock()
		return LeaseResponse{
			Granted:  false,
			LeaderID: n.leaderID,
			Lease:    lease,
			Message:  "lease currently held by another owner",
		}, nil
	}
	n.mu.Unlock()

	token, err := randomHex(8)
	if err != nil {
		return LeaseResponse{}, err
	}

	entry := LogEntry{
		Type:       "lease_acquire",
		LeaseName:  name,
		LeaseOwner: owner,
		LeaseToken: token,
		LeaseTTL:   int64(ttl / time.Second),
		Timestamp:  time.Now().UTC(),
	}
	if _, err := n.replicateEntry(entry); err != nil {
		return LeaseResponse{}, err
	}

	n.mu.RLock()
	defer n.mu.RUnlock()
	return LeaseResponse{
		Granted:  true,
		LeaderID: n.leaderID,
		Lease:    cloneLease(n.lease),
		Message:  "lease granted",
	}, nil
}

func (n *Node) ReleaseLease(name, owner string) (LeaseResponse, error) {
	n.mu.Lock()
	n.clearExpiredLeaseLocked(time.Now().UTC())
	if n.lease == nil || n.lease.Name != name {
		n.mu.Unlock()
		return LeaseResponse{
			Granted:  false,
			LeaderID: n.leaderID,
			Message:  "no active lease for requested name",
		}, nil
	}
	if owner != "" && n.lease.Owner != owner {
		lease := cloneLease(n.lease)
		n.mu.Unlock()
		return LeaseResponse{
			Granted:  false,
			LeaderID: n.leaderID,
			Lease:    lease,
			Message:  "lease owned by different owner",
		}, nil
	}
	n.mu.Unlock()

	entry := LogEntry{
		Type:       "lease_release",
		LeaseName:  name,
		LeaseOwner: owner,
		Timestamp:  time.Now().UTC(),
	}
	if _, err := n.replicateEntry(entry); err != nil {
		return LeaseResponse{}, err
	}

	return LeaseResponse{
		Granted:  true,
		LeaderID: n.leaderID,
		Message:  "lease released",
	}, nil
}

func (n *Node) replicateEntry(entry LogEntry) (LogEntry, error) {
	n.mu.Lock()
	if n.leaderID != n.cfg.NodeID {
		leaderAddr := n.leaderAddressLocked()
		leaderID := n.leaderID
		n.mu.Unlock()
		return LogEntry{}, fmt.Errorf("node %d is not leader; current leader is %d at %s", n.cfg.NodeID, leaderID, leaderAddr)
	}

	entry.Index = n.lastLogIndexLocked() + 1
	entry.Term = n.currentTerm
	entry.Lamport = n.nextLamportLocked(0)
	n.log = append(n.log, entry)
	quorum := n.quorumLocked()
	peers := n.membersWithoutSelfLocked()
	commitIndex := n.commitIndex
	leaderID := n.cfg.NodeID
	term := n.currentTerm
	lamport := n.lamport
	if err := n.persistLocked(); err != nil {
		n.mu.Unlock()
		return LogEntry{}, err
	}
	n.mu.Unlock()

	successes := 1
	for _, peer := range peers {
		var resp AppendEntriesResponse
		err := n.postJSON(context.Background(), joinURL(peer.Address, "/internal/cluster/append"), AppendEntriesRequest{
			LeaderID:    leaderID,
			Term:        term,
			Lamport:     lamport,
			CommitIndex: commitIndex,
			Entry:       entry,
		}, &resp)
		if err == nil && resp.OK {
			successes++
		}
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if successes < quorum {
		n.truncateToCommitLocked()
		n.rebuildStateMachineLocked()
		_ = n.persistLocked()
		return LogEntry{}, fmt.Errorf("write could not reach quorum (%d/%d replicas)", successes, quorum)
	}

	n.commitIndex = maxInt(n.commitIndex, entry.Index)
	n.rebuildStateMachineLocked()
	if err := n.persistLocked(); err != nil {
		return LogEntry{}, err
	}

	n.runAsync(n.broadcastHeartbeat)

	return entry, nil
}

func (n *Node) syncFromLeader() {
	select {
	case <-n.ctx.Done():
		return
	default:
	}

	n.mu.RLock()
	leaderAddr := n.leaderAddressLocked()
	n.mu.RUnlock()
	if leaderAddr == "" || n.leaderID == n.cfg.NodeID {
		return
	}

	var resp SyncResponse
	if err := n.postJSON(context.Background(), joinURL(leaderAddr, "/internal/cluster/sync"), SyncRequest{RequesterID: n.cfg.NodeID}, &resp); err != nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	n.clusterID = resp.ClusterID
	n.currentTerm = resp.CurrentTerm
	n.leaderID = resp.LeaderID
	n.lamport = maxInt64(n.lamport, resp.Lamport)
	n.commitIndex = minInt(resp.CommitIndex, len(resp.Log))
	n.members = membersFromSlice(resp.Members)
	self := n.members[n.cfg.NodeID]
	self.ID = n.cfg.NodeID
	self.Address = n.cfg.AdvertiseAddr
	if n.leaderID == n.cfg.NodeID {
		self.Status = "leader"
	} else {
		self.Status = "follower"
	}
	self.LastSeen = time.Now().UTC()
	n.members[n.cfg.NodeID] = self
	n.log = cloneLog(resp.Log)
	n.kv = cloneMap(resp.KV)
	n.lease = cloneLease(resp.Lease)
	_ = n.persistLocked()
}

func (n *Node) postJSON(ctx context.Context, target string, reqBody any, out any) error {
	payload, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := n.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= http.StatusBadRequest {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 8*1024))
		return fmt.Errorf("request to %s failed with %s: %s", target, resp.Status, string(body))
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func (n *Node) fetchRemoteStatus(ctx context.Context, targetBase string) (StatusResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, joinURL(targetBase, "/api/v1/status"), nil)
	if err != nil {
		return StatusResponse{}, fmt.Errorf("new status request: %w", err)
	}

	resp, err := n.client.Do(req)
	if err != nil {
		return StatusResponse{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= http.StatusBadRequest {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 8*1024))
		return StatusResponse{}, fmt.Errorf("status request failed with %s: %s", resp.Status, string(body))
	}

	var status StatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return StatusResponse{}, fmt.Errorf("decode status response: %w", err)
	}
	return status, nil
}

func (n *Node) statePath() string {
	return filepath.Join(n.cfg.DataDir, "state.json")
}

func (n *Node) loadState() error {
	data, err := os.ReadFile(n.statePath())
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("read state: %w", err)
	}

	var ps persistentState
	if err := json.Unmarshal(data, &ps); err != nil {
		return fmt.Errorf("unmarshal state: %w", err)
	}

	n.clusterID = ps.ClusterID
	n.joinToken = ps.JoinToken
	n.currentTerm = ps.CurrentTerm
	n.leaderID = ps.LeaderID
	n.lamport = ps.Lamport
	n.commitIndex = ps.CommitIndex
	n.members = ps.Members
	if n.members == nil {
		n.members = make(map[int]Member)
	}
	n.log = ps.Log
	n.kv = ps.KV
	if n.kv == nil {
		n.kv = make(map[string]string)
	}
	n.lease = ps.Lease
	if len(n.log) > 0 {
		n.rebuildStateMachineLocked()
	}

	return nil
}

func (n *Node) persistLocked() error {
	ps := persistentState{
		ClusterID:   n.clusterID,
		JoinToken:   n.joinToken,
		CurrentTerm: n.currentTerm,
		LeaderID:    n.leaderID,
		Lamport:     n.lamport,
		CommitIndex: n.commitIndex,
		Members:     n.members,
		Log:         n.log,
		KV:          n.kv,
		Lease:       n.lease,
	}

	payload, err := json.MarshalIndent(ps, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal state: %w", err)
	}

	tmp := n.statePath() + ".tmp"
	if err := os.WriteFile(tmp, payload, 0o644); err != nil {
		return fmt.Errorf("write temp state: %w", err)
	}
	if err := os.Rename(tmp, n.statePath()); err != nil {
		return fmt.Errorf("rename state: %w", err)
	}
	return nil
}

func (n *Node) nextLamportLocked(observed int64) int64 {
	n.lamport = maxInt64(n.lamport, observed) + 1
	return n.lamport
}

func (n *Node) lastLogIndexLocked() int {
	return len(n.log)
}

func (n *Node) quorumLocked() int {
	size := len(n.members)
	if size == 0 {
		size = 1
	}
	return size/2 + 1
}

func (n *Node) leaderAddressLocked() string {
	member, ok := n.members[n.leaderID]
	if !ok {
		return ""
	}
	return member.Address
}

func (n *Node) membersWithoutSelfLocked() []Member {
	members := make([]Member, 0, len(n.members))
	for _, member := range n.members {
		if member.ID == n.cfg.NodeID {
			continue
		}
		members = append(members, member)
	}
	sort.Slice(members, func(i, j int) bool {
		return members[i].ID < members[j].ID
	})
	return members
}

func (n *Node) memberSliceLocked() []Member {
	members := make([]Member, 0, len(n.members))
	for _, member := range n.members {
		members = append(members, member)
	}
	sort.Slice(members, func(i, j int) bool {
		return members[i].ID < members[j].ID
	})
	return members
}

func (n *Node) mergeMembersLocked(incoming []Member) {
	for _, member := range incoming {
		existing := n.members[member.ID]
		if member.Address == "" {
			member.Address = existing.Address
		}
		if member.Status == "" {
			member.Status = coalesceStatus(existing.Status, "follower")
		}
		n.members[member.ID] = member
	}
	self := n.members[n.cfg.NodeID]
	self.ID = n.cfg.NodeID
	self.Address = n.cfg.AdvertiseAddr
	if n.leaderID == n.cfg.NodeID {
		self.Status = "leader"
	} else {
		self.Status = coalesceStatus(self.Status, "follower")
	}
	n.members[n.cfg.NodeID] = self
}

func (n *Node) truncateToCommitLocked() {
	if n.commitIndex < len(n.log) {
		n.log = cloneLog(n.log[:n.commitIndex])
	}
}

func (n *Node) rebuildStateMachineLocked() {
	n.kv = make(map[string]string)
	n.lease = nil
	limit := minInt(n.commitIndex, len(n.log))
	for i := 0; i < limit; i++ {
		entry := n.log[i]
		switch entry.Type {
		case "put":
			n.kv[entry.Key] = entry.Value
		case "lease_acquire":
			n.lease = &LeaseState{
				Name:         entry.LeaseName,
				Owner:        entry.LeaseOwner,
				HolderNodeID: n.leaderID,
				Token:        entry.LeaseToken,
				AcquiredAt:   entry.Timestamp,
				ExpiresAt:    entry.Timestamp.Add(time.Duration(entry.LeaseTTL) * time.Second),
			}
		case "lease_release":
			if n.lease != nil && n.lease.Name == entry.LeaseName {
				n.lease = nil
			}
		}
	}
	n.clearExpiredLeaseLocked(time.Now().UTC())
}

func (n *Node) clearExpiredLeaseLocked(now time.Time) {
	if n.lease != nil && !n.lease.ExpiresAt.IsZero() && now.After(n.lease.ExpiresAt) {
		n.lease = nil
	}
}

func membersFromSlice(in []Member) map[int]Member {
	out := make(map[int]Member, len(in))
	for _, member := range in {
		out[member.ID] = member
	}
	return out
}

func cloneLog(in []LogEntry) []LogEntry {
	out := make([]LogEntry, len(in))
	copy(out, in)
	return out
}

func cloneMap(in map[string]string) map[string]string {
	if in == nil {
		return map[string]string{}
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func cloneLease(in *LeaseState) *LeaseState {
	if in == nil {
		return nil
	}
	copyValue := *in
	return &copyValue
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}

func coalesceString(current, fallback string) string {
	if current != "" {
		return current
	}
	return fallback
}

func coalesceStatus(current, fallback string) string {
	if current != "" {
		return current
	}
	return fallback
}

func (n *Node) runAsync(fn func()) {
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		select {
		case <-n.ctx.Done():
			return
		default:
		}
		fn()
	}()
}
