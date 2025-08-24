package committee

import (
	"net/http"
	"omryl/internal/auth"
	"omryl/internal/config"
	"sync"
	"time"
)

// Node merepresentasikan sebuah node dalam komite.
type Node struct {
	mu           sync.RWMutex
	selfID       string
	httpAddr     string
	peers        []config.CommitteePeer
	clusters     map[ClusterID]*ClusterInfo // central registry
	internalAuth auth.IInternalAuth
	httpClient   *http.Client

	LocalDeliver func(env Envelope) (int, []byte)
}

// NewNode membuat instance Node baru.
func NewNode(selfID, httpAddr string, peers []config.CommitteePeer, auth auth.IInternalAuth) *Node {
	return &Node{
		selfID:       selfID,
		httpAddr:     httpAddr,
		peers:        peers,
		clusters:     make(map[ClusterID]*ClusterInfo),
		internalAuth: auth,
		httpClient:   &http.Client{Timeout: 5 * time.Second},
		LocalDeliver: func(env Envelope) (int, []byte) {
			return http.StatusNotImplemented, []byte(`"no LocalDeliver configured"`)
		},
	}
}

// UpsertCluster menambahkan atau memperbarui informasi cluster dalam registry.
func (n *Node) UpsertCluster(ci ClusterInfo) {
	n.mu.Lock()
	defer n.mu.Unlock()
	prev, ok := n.clusters[ci.ClusterID]
	if !ok || ci.Term >= prev.Term {
		v := ci
		n.clusters[ci.ClusterID] = &v
	}
}

// ResolveLeaderHTTP mengembalikan alamat HTTP pemimpin dan komite untuk cluster tertentu.
func (n *Node) ResolveLeaderHTTP(id ClusterID) (leaderHTTP string, committeeHTTP string, ok bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	ci, ok := n.clusters[id]
	if !ok {
		return "", "", false
	}
	return ci.LeaderHTTP, ci.CommitteeHTTP, true
}

// AddPeer menambahkan peer baru ke daftar peers jika belum ada.
func (n *Node) AddPeer(p config.CommitteePeer) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, ex := range n.peers {
		if ex.HTTPAddr == p.HTTPAddr {
			return
		}
	}
	n.peers = append(n.peers, p)
}
