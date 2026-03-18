package cluster

import (
	"context"
	"testing"
	"time"
)

func TestJoinRejectsInvalidToken(t *testing.T) {
	clusterID := "test-cluster"
	token := "secret-token"

	leader, shutdownLeader := startTestNode(t, Config{
		NodeID:            1,
		ListenAddr:        "127.0.0.1:0",
		DataDir:           t.TempDir(),
		ClusterID:         clusterID,
		JoinToken:         token,
		HeartbeatInterval: 100 * time.Millisecond,
		ElectionTimeout:   350 * time.Millisecond,
		JoinRetryInterval: 100 * time.Millisecond,
	}, true)
	defer shutdownLeader()

	var resp JoinResponse
	err := requestJSON("POST", joinURL(leader.AdvertiseAddr(), "/internal/cluster/join"), JoinRequest{
		NodeID:    2,
		Address:   "http://127.0.0.1:9999",
		ClusterID: clusterID,
		Token:     "wrong-token",
	}, &resp)
	if err == nil {
		t.Fatal("expected invalid join token to be rejected")
	}
}

func TestReplicationAndRejoinCatchup(t *testing.T) {
	clusterID := "test-cluster"
	token := "secret-token"

	leader, shutdownLeader := startTestNode(t, Config{
		NodeID:            1,
		ListenAddr:        "127.0.0.1:0",
		DataDir:           t.TempDir(),
		ClusterID:         clusterID,
		JoinToken:         token,
		HeartbeatInterval: 100 * time.Millisecond,
		ElectionTimeout:   500 * time.Millisecond,
		JoinRetryInterval: 100 * time.Millisecond,
	}, true)
	defer shutdownLeader()

	node2Dir := t.TempDir()
	node2, shutdownNode2 := startTestNode(t, Config{
		NodeID:            2,
		ListenAddr:        "127.0.0.1:0",
		DataDir:           node2Dir,
		ClusterID:         clusterID,
		JoinToken:         token,
		ManagerAddr:       leader.AdvertiseAddr(),
		HeartbeatInterval: 100 * time.Millisecond,
		ElectionTimeout:   500 * time.Millisecond,
		JoinRetryInterval: 100 * time.Millisecond,
	}, false)
	defer shutdownNode2()

	node3Dir := t.TempDir()
	node3, shutdownNode3 := startTestNode(t, Config{
		NodeID:            3,
		ListenAddr:        "127.0.0.1:0",
		DataDir:           node3Dir,
		ClusterID:         clusterID,
		JoinToken:         token,
		ManagerAddr:       leader.AdvertiseAddr(),
		HeartbeatInterval: 100 * time.Millisecond,
		ElectionTimeout:   500 * time.Millisecond,
		JoinRetryInterval: 100 * time.Millisecond,
	}, false)

	waitFor(t, 4*time.Second, func() bool {
		return len(leader.StatusSnapshot().Members) == 3 &&
			len(node2.StatusSnapshot().Members) == 3 &&
			len(node3.StatusSnapshot().Members) == 3
	}, "all nodes to observe full membership")

	if _, err := leader.ApplyWrite("alpha", "1"); err != nil {
		t.Fatalf("apply write alpha: %v", err)
	}

	waitFor(t, 4*time.Second, func() bool {
		return node2.StatusSnapshot().Data["alpha"] == "1" &&
			node3.StatusSnapshot().Data["alpha"] == "1"
	}, "followers to replicate alpha")

	shutdownNode3()

	if _, err := leader.ApplyWrite("beta", "2"); err != nil {
		t.Fatalf("apply write beta: %v", err)
	}

	waitFor(t, 4*time.Second, func() bool {
		return node2.StatusSnapshot().Data["beta"] == "2"
	}, "node2 to replicate beta")

	rejoinedNode3, shutdownRejoined := startTestNode(t, Config{
		NodeID:            3,
		ListenAddr:        "127.0.0.1:0",
		DataDir:           node3Dir,
		ClusterID:         clusterID,
		JoinToken:         token,
		ManagerAddr:       leader.AdvertiseAddr(),
		HeartbeatInterval: 100 * time.Millisecond,
		ElectionTimeout:   500 * time.Millisecond,
		JoinRetryInterval: 100 * time.Millisecond,
	}, false)
	defer shutdownRejoined()

	waitFor(t, 4*time.Second, func() bool {
		state := rejoinedNode3.StatusSnapshot().Data
		return state["alpha"] == "1" && state["beta"] == "2"
	}, "rejoined node3 to catch up from leader sync")
}

func TestLeaderFailoverAndLeaseExclusivity(t *testing.T) {
	clusterID := "test-cluster"
	token := "secret-token"

	node1, shutdownNode1 := startTestNode(t, Config{
		NodeID:            1,
		ListenAddr:        "127.0.0.1:0",
		DataDir:           t.TempDir(),
		ClusterID:         clusterID,
		JoinToken:         token,
		HeartbeatInterval: 100 * time.Millisecond,
		ElectionTimeout:   450 * time.Millisecond,
		JoinRetryInterval: 100 * time.Millisecond,
	}, true)

	node2, shutdownNode2 := startTestNode(t, Config{
		NodeID:            2,
		ListenAddr:        "127.0.0.1:0",
		DataDir:           t.TempDir(),
		ClusterID:         clusterID,
		JoinToken:         token,
		ManagerAddr:       node1.AdvertiseAddr(),
		HeartbeatInterval: 100 * time.Millisecond,
		ElectionTimeout:   450 * time.Millisecond,
		JoinRetryInterval: 100 * time.Millisecond,
	}, false)
	defer shutdownNode2()

	node3, shutdownNode3 := startTestNode(t, Config{
		NodeID:            3,
		ListenAddr:        "127.0.0.1:0",
		DataDir:           t.TempDir(),
		ClusterID:         clusterID,
		JoinToken:         token,
		ManagerAddr:       node1.AdvertiseAddr(),
		HeartbeatInterval: 100 * time.Millisecond,
		ElectionTimeout:   450 * time.Millisecond,
		JoinRetryInterval: 100 * time.Millisecond,
	}, false)
	defer shutdownNode3()

	waitFor(t, 4*time.Second, func() bool {
		return len(node1.StatusSnapshot().Members) == 3 &&
			len(node2.StatusSnapshot().Members) == 3 &&
			len(node3.StatusSnapshot().Members) == 3
	}, "all nodes to join before failover")

	shutdownNode1()

	waitFor(t, 5*time.Second, func() bool {
		return node3.StatusSnapshot().IsLeader
	}, "highest-ID node to become leader after node1 failure")

	firstLease, err := node3.AcquireLease("maintenance", "reporter", 10*time.Second)
	if err != nil {
		t.Fatalf("acquire lease: %v", err)
	}
	if !firstLease.Granted {
		t.Fatalf("expected first lease acquisition to succeed: %#v", firstLease)
	}

	secondLease, err := node3.AcquireLease("maintenance", "intruder", 10*time.Second)
	if err != nil {
		t.Fatalf("second lease attempt returned error: %v", err)
	}
	if secondLease.Granted {
		t.Fatalf("expected second lease acquisition to fail while lease is held: %#v", secondLease)
	}

	releaseResp, err := node3.ReleaseLease("maintenance", "reporter")
	if err != nil {
		t.Fatalf("release lease: %v", err)
	}
	if !releaseResp.Granted {
		t.Fatalf("expected release to succeed: %#v", releaseResp)
	}

	thirdLease, err := node3.AcquireLease("maintenance", "intruder", 10*time.Second)
	if err != nil {
		t.Fatalf("third lease attempt returned error: %v", err)
	}
	if !thirdLease.Granted {
		t.Fatalf("expected lease acquisition after release to succeed: %#v", thirdLease)
	}
}

func startTestNode(t *testing.T, cfg Config, init bool) (*Node, func()) {
	t.Helper()

	node, err := NewNode(cfg)
	if err != nil {
		t.Fatalf("new node: %v", err)
	}
	if err := node.Start(); err != nil {
		t.Fatalf("start node: %v", err)
	}
	if init {
		if err := node.InitCluster(); err != nil {
			t.Fatalf("init cluster: %v", err)
		}
	} else {
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		defer cancel()
		if err := node.JoinCluster(ctx); err != nil {
			t.Fatalf("join cluster: %v", err)
		}
	}

	stopped := false
	shutdown := func() {
		if stopped {
			return
		}
		stopped = true
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = node.Shutdown(ctx)
	}

	t.Cleanup(shutdown)
	return node, shutdown
}

func waitFor(t *testing.T, timeout time.Duration, condition func() bool, label string) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", label)
}
