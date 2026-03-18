package cluster

import "time"

type Config struct {
	NodeID            int
	ListenAddr        string
	AdvertiseAddr     string
	DataDir           string
	ClusterID         string
	JoinToken         string
	ManagerAddr       string
	HeartbeatInterval time.Duration
	ElectionTimeout   time.Duration
	JoinRetryInterval time.Duration
}

type Member struct {
	ID              int       `json:"id"`
	Address         string    `json:"address"`
	Status          string    `json:"status"`
	LastSeen        time.Time `json:"lastSeen"`
	LastHeartbeat   time.Time `json:"lastHeartbeat"`
	LastApplied     int       `json:"lastApplied"`
	LastCommitted   int       `json:"lastCommitted"`
	LastLamportSeen int64     `json:"lastLamportSeen"`
}

type LeaseState struct {
	Name         string    `json:"name"`
	Owner        string    `json:"owner"`
	HolderNodeID int       `json:"holderNodeId"`
	Token        string    `json:"token"`
	AcquiredAt   time.Time `json:"acquiredAt"`
	ExpiresAt    time.Time `json:"expiresAt"`
}

type LogEntry struct {
	Index      int       `json:"index"`
	Term       int       `json:"term"`
	Lamport    int64     `json:"lamport"`
	Type       string    `json:"type"`
	Key        string    `json:"key,omitempty"`
	Value      string    `json:"value,omitempty"`
	LeaseName  string    `json:"leaseName,omitempty"`
	LeaseOwner string    `json:"leaseOwner,omitempty"`
	LeaseToken string    `json:"leaseToken,omitempty"`
	LeaseTTL   int64     `json:"leaseTtl,omitempty"`
	Timestamp  time.Time `json:"timestamp"`
}

type persistentState struct {
	ClusterID   string            `json:"clusterId"`
	JoinToken   string            `json:"joinToken"`
	CurrentTerm int               `json:"currentTerm"`
	LeaderID    int               `json:"leaderId"`
	Lamport     int64             `json:"lamport"`
	CommitIndex int               `json:"commitIndex"`
	Members     map[int]Member    `json:"members"`
	Log         []LogEntry        `json:"log"`
	KV          map[string]string `json:"kv"`
	Lease       *LeaseState       `json:"lease"`
}

type JoinRequest struct {
	NodeID       int    `json:"nodeId"`
	Address      string `json:"address"`
	ClusterID    string `json:"clusterId"`
	Token        string `json:"token"`
	LastLogIndex int    `json:"lastLogIndex"`
}

type JoinResponse struct {
	ClusterID   string            `json:"clusterId"`
	LeaderID    int               `json:"leaderId"`
	CurrentTerm int               `json:"currentTerm"`
	Lamport     int64             `json:"lamport"`
	CommitIndex int               `json:"commitIndex"`
	Members     []Member          `json:"members"`
	Log         []LogEntry        `json:"log"`
	KV          map[string]string `json:"kv"`
	Lease       *LeaseState       `json:"lease"`
}

type HeartbeatRequest struct {
	LeaderID     int      `json:"leaderId"`
	Term         int      `json:"term"`
	Lamport      int64    `json:"lamport"`
	CommitIndex  int      `json:"commitIndex"`
	LastLogIndex int      `json:"lastLogIndex"`
	Members      []Member `json:"members"`
}

type HeartbeatResponse struct {
	OK           bool  `json:"ok"`
	Lamport      int64 `json:"lamport"`
	LastLogIndex int   `json:"lastLogIndex"`
}

type ElectionRequest struct {
	CandidateID int   `json:"candidateId"`
	Term        int   `json:"term"`
	Lamport     int64 `json:"lamport"`
}

type ElectionResponse struct {
	Alive   bool  `json:"alive"`
	NodeID  int   `json:"nodeId"`
	Term    int   `json:"term"`
	Lamport int64 `json:"lamport"`
}

type CoordinatorAnnouncement struct {
	LeaderID int   `json:"leaderId"`
	Term     int   `json:"term"`
	Lamport  int64 `json:"lamport"`
}

type AppendEntriesRequest struct {
	LeaderID    int      `json:"leaderId"`
	Term        int      `json:"term"`
	Lamport     int64    `json:"lamport"`
	CommitIndex int      `json:"commitIndex"`
	Entry       LogEntry `json:"entry"`
}

type AppendEntriesResponse struct {
	OK           bool  `json:"ok"`
	Lamport      int64 `json:"lamport"`
	LastLogIndex int   `json:"lastLogIndex"`
}

type SyncRequest struct {
	RequesterID int `json:"requesterId"`
}

type SyncResponse struct {
	ClusterID   string            `json:"clusterId"`
	LeaderID    int               `json:"leaderId"`
	CurrentTerm int               `json:"currentTerm"`
	Lamport     int64             `json:"lamport"`
	CommitIndex int               `json:"commitIndex"`
	Members     []Member          `json:"members"`
	Log         []LogEntry        `json:"log"`
	KV          map[string]string `json:"kv"`
	Lease       *LeaseState       `json:"lease"`
}

type WriteRequest struct {
	Value string `json:"value"`
}

type WriteResponse struct {
	Key        string `json:"key"`
	Value      string `json:"value"`
	Committed  bool   `json:"committed"`
	LeaderID   int    `json:"leaderId"`
	LeaderAddr string `json:"leaderAddr,omitempty"`
	LogIndex   int    `json:"logIndex"`
}

type ReadResponse struct {
	Key        string `json:"key"`
	Value      string `json:"value"`
	Exists     bool   `json:"exists"`
	LeaderID   int    `json:"leaderId"`
	LeaderAddr string `json:"leaderAddr,omitempty"`
}

type LeaseRequest struct {
	Name       string        `json:"name"`
	Owner      string        `json:"owner"`
	TTLSeconds int64         `json:"ttlSeconds"`
	Wait       time.Duration `json:"-"`
}

type LeaseResponse struct {
	Granted    bool        `json:"granted"`
	LeaderID   int         `json:"leaderId"`
	LeaderAddr string      `json:"leaderAddr,omitempty"`
	Lease      *LeaseState `json:"lease,omitempty"`
	Message    string      `json:"message,omitempty"`
}

type StatusResponse struct {
	NodeID        int               `json:"nodeId"`
	ClusterID     string            `json:"clusterId"`
	LeaderID      int               `json:"leaderId"`
	CurrentTerm   int               `json:"currentTerm"`
	Lamport       int64             `json:"lamport"`
	IsLeader      bool              `json:"isLeader"`
	AdvertiseAddr string            `json:"advertiseAddr"`
	CommitIndex   int               `json:"commitIndex"`
	LastLogIndex  int               `json:"lastLogIndex"`
	LastHeartbeat time.Time         `json:"lastHeartbeat"`
	Members       []Member          `json:"members"`
	Data          map[string]string `json:"data"`
	Lease         *LeaseState       `json:"lease"`
	Log           []LogEntry        `json:"log"`
}

type ControlPlaneResponse struct {
	StatusResponse
	SourceNodeID        int    `json:"sourceNodeId"`
	SourceAdvertiseAddr string `json:"sourceAdvertiseAddr"`
	ServedByLeader      bool   `json:"servedByLeader"`
	Degraded            bool   `json:"degraded"`
	DiscoveryMethod     string `json:"discoveryMethod"`
}
