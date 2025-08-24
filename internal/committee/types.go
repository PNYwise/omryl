package committee

// ClusterID merepresentasikan ID unik untuk sebuah cluster.
type ClusterID string

// ClusterInfo merepresentasikan informasi tentang sebuah cluster.
type ClusterInfo struct {
	ClusterID     ClusterID `json:"cluster_id"`
	LeaderRaft    string    `json:"leader_raft"`
	LeaderHTTP    string    `json:"leader_http"`
	CommitteeHTTP string    `json:"committee_http"`
	Term          uint64    `json:"term"`
	LastSeenUnix  int64     `json:"last_seen_unix"`
}

// Envelope merepresentasikan pesan yang dikirim antar cluster.
type Envelope struct {
	FromCluster ClusterID `json:"from_cluster"`
	ToCluster   ClusterID `json:"to_cluster"` // empty = broadcast
	Type        string    `json:"type"`
	Payload     []byte    `json:"payload"`
	Nonce       string    `json:"nonce"`
	TimestampMs int64     `json:"ts"`
}
