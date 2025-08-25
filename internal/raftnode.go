package internal

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"omryl/internal/auth"
	"omryl/internal/committee"
	"omryl/internal/config"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

// RaftNode membungkus instance Raft dan FSM.
type RaftNode struct {
	raft           *raft.Raft
	fsm            *SQLiteFSM
	dbDir          string
	raftAddr       string
	httpAddr       string
	httpAddrBook   map[string]string
	publicJoinAuth *auth.PublicJoinAuth // simple token for /join
	publicAuth     *auth.PublicAuth     // simple token for /purpose, /query
	internalAuth   *auth.InternalAuth

	// Global Committee
	clusterID         string
	committee         *committee.Node
	committeePeers    []config.CommitteePeer
	committeeAuth     *auth.InternalAuth
	committeeSelfHTTP string

	federatePurpose bool
	federateTargets []string
}

// Setters for auth (call from main after construction)

// SetPublicAPIToken mengatur token untuk endpoint publik seperti /purpose dan /query.
func (rn *RaftNode) SetPublicAPIToken(a *auth.PublicAuth) { rn.publicAuth = a }

// SetPublicJoinToken mengatur token untuk endpoint publik /join.
func (rn *RaftNode) SetPublicJoinToken(a *auth.PublicJoinAuth) { rn.publicJoinAuth = a }

// SetInternalAuth mengatur otentikasi internal antar node.
func (rn *RaftNode) SetInternalAuth(a *auth.InternalAuth) { rn.internalAuth = a }

// SetFederation mengatur tujuan federasi (dipanggil dari main setelah konstruksi)
func (rn *RaftNode) SetFederation(on bool, targets []string) {
	rn.federatePurpose = on
	rn.federateTargets = targets
}

// NewRaftNode menginisialisasi dan mengembalikan node Raft baru.
func NewRaftNode(
	nodeID, raftAddr, httpAddr, dbDir string,
	httpAddrBook map[string]string,
	isBootstrap bool,
) (*RaftNode, error) {

	if nodeID == "" {
		return nil, fmt.Errorf("nodeID kosong")
	}
	if raftAddr == "" || httpAddr == "" {
		return nil, fmt.Errorf("alamat raft/http kosong")
	}

	// Konfigurasi Raft
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(nodeID)
	config.SnapshotInterval = 30 * time.Second // default bisa terlalu jarang
	config.SnapshotThreshold = 1024            // apply 1k entry → snapshot
	config.Logger = hclog.New(&hclog.LoggerOptions{
		Name:   fmt.Sprintf("raft-%s", nodeID),
		Output: os.Stderr,
		Level:  hclog.Debug,
	})

	// Pastikan direktori data ada
	if err := os.MkdirAll(dbDir, 0755); err != nil {
		return nil, fmt.Errorf("gagal membuat direktori data: %w", err)
	}

	// FSM
	fsm := NewSQLiteFSM(filepath.Join(dbDir, "sqlite.db"))

	// Transport
	addr, err := net.ResolveTCPAddr("tcp", raftAddr)
	if err != nil {
		return nil, fmt.Errorf("gagal resolve TCP: %w", err)
	}
	transport, err := raft.NewTCPTransport(raftAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("gagal membuat transport TCP: %w", err)
	}

	// Stores
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(dbDir, "raft-log.db"))
	if err != nil {
		return nil, fmt.Errorf("gagal membuat bolt log store: %w", err)
	}
	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(dbDir, "raft-stable.db"))
	if err != nil {
		return nil, fmt.Errorf("gagal membuat bolt stable store: %w", err)
	}
	snapshotStore, err := raft.NewFileSnapshotStore(dbDir, 2, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("gagal membuat file snapshot store: %w", err)
	}

	// Raft instance
	r, err := raft.NewRaft(config, fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return nil, fmt.Errorf("gagal membuat Raft: %w", err)
	}

	// Bootstrap jika tidak ada state
	hasState, err := raft.HasExistingState(logStore, stableStore, snapshotStore)
	if err != nil {
		return nil, fmt.Errorf("gagal cek state raft: %w", err)
	}
	if isBootstrap && !hasState {
		log.Printf("Mem-bootstrap klaster Raft dengan node: %s di %s", nodeID, raftAddr)
		cfg := raft.Configuration{
			Servers: []raft.Server{
				{ID: config.LocalID, Address: transport.LocalAddr(), Suffrage: raft.Voter},
			},
		}
		if f := r.BootstrapCluster(cfg); f.Error() != nil {
			return nil, fmt.Errorf("kesalahan saat bootstrap: %w", f.Error())
		}
	} else if isBootstrap && hasState {
		log.Printf("Lewati bootstrap: state raft sudah ada untuk %s", nodeID)
	}

	// Seed address book
	if httpAddrBook == nil {
		httpAddrBook = make(map[string]string)
	}
	httpAddrBook[raftAddr] = httpAddr
	httpAddrBook[string(config.LocalID)] = httpAddr

	rn := &RaftNode{
		raft:         r,
		fsm:          fsm,
		dbDir:        dbDir,
		raftAddr:     raftAddr,
		httpAddr:     httpAddr,
		httpAddrBook: httpAddrBook,
	}

	return rn, nil
}

// ProposeCommand mengajukan perintah SQL untuk direplikasi oleh Raft (harus leader).
func (rn *RaftNode) ProposeCommand(sqlCmd string) error {
	if rn.raft.State() != raft.Leader {
		return fmt.Errorf("node bukan pemimpin, tidak dapat mengajukan perintah")
	}
	cmd := Command{SQL: sqlCmd}
	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("marshal perintah: %w", err)
	}
	applyFuture := rn.raft.Apply(cmdBytes, 10*time.Second)
	if err := applyFuture.Error(); err != nil {
		return fmt.Errorf("apply raft: %w", err)
	}
	if resp := applyFuture.Response(); resp != nil {
		if err, ok := resp.(error); ok {
			return err
		}
	}
	return nil
}

// AttachCommittee mengaitkan node komite global ke RaftNode.
func (rn *RaftNode) AttachCommittee(clusterID, selfCommitteeHTTP string, peers []config.CommitteePeer, auth *auth.InternalAuth) error {
	if clusterID == "" {
		return nil
	}
	if selfCommitteeHTTP == "" || strings.HasPrefix(selfCommitteeHTTP, "0.0.0.0:") {
		selfCommitteeHTTP = rn.httpAddr
	}
	rn.clusterID = clusterID
	rn.committeeAuth = auth
	rn.committeePeers = peers
	rn.committeeSelfHTTP = selfCommitteeHTTP // Convert peers to committee type

	cpeers := make([]config.CommitteePeer, 0, len(peers))
	for _, p := range peers {
		cpeers = append(cpeers, config.CommitteePeer{ID: p.ID, HTTPAddr: p.HTTPAddr})
	}
	// create node
	rn.committee = committee.NewNode("committee-"+clusterID, selfCommitteeHTTP, cpeers, auth)
	rn.committee.LocalDeliver = rn.handleGlobalEnvelope
	return nil
}

// WatchLeadership to Monitor leadership changes and announce
func (rn *RaftNode) WatchLeadership() {
	if rn.committee == nil {
		return
	}
	ch := rn.raft.LeaderCh()
	for isLeader := range ch {
		if isLeader {
			rn.announceToCommittee()
		}
	}
}

// listRegistryClusters fetches all known clusters from the committee registry
func (rn *RaftNode) listRegistryClusters() ([]committee.ClusterInfo, error) {
	if rn.committee == nil || len(rn.committeePeers) == 0 {
		return nil, fmt.Errorf("committee/peers not configured")
	}
	url := fmt.Sprintf("http://%s/gc/list", rn.committeePeers[0].HTTPAddr)
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var out []committee.ClusterInfo
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return out, nil
}

// SendSQLToCluster mengirim perintah SQL ke cluster lain via committee
func (rn *RaftNode) SendSQLToCluster(destCluster, sql string) error {
	if rn.committee == nil || rn.committeeAuth == nil {
		return fmt.Errorf("committee not configured")
	}
	if len(rn.committeePeers) == 0 {
		return fmt.Errorf("no committee peers configured")
	}
	env := committee.Envelope{
		FromCluster: committee.ClusterID(rn.clusterID),
		ToCluster:   committee.ClusterID(destCluster),
		Type:        "ROUTE_SQL_WRITE",
		Payload:     mustJSON(map[string]string{"sql": sql}),
		Nonce:       fmt.Sprintf("%d", time.Now().UnixNano()),
		TimestampMs: time.Now().UnixMilli(),
	}
	body := mustJSON(env)
	url := fmt.Sprintf("http://%s/gc/route", rn.committeePeers[0].HTTPAddr)
	req, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rn.committeeAuth.SignRequest(req, body)
	_, err := http.DefaultClient.Do(req)
	return err
}

// fanoutSQL mengirim perintah SQL ke semua cluster lain (atau target tertentu jika diset)
func (rn *RaftNode) fanoutSQL(sql string) {
	// best effort, jangan panik kalau ada error
	clusters, err := rn.listRegistryClusters()
	if err != nil {
		log.Printf("[federation] list clusters failed: %v", err)
		return
	}
	// jika targets diset, pakai itu; kalau kosong → semua selain diri sendiri
	allow := map[string]bool{}
	if len(rn.federateTargets) > 0 {
		for _, id := range rn.federateTargets {
			allow[id] = true
		}
	}

	for _, ci := range clusters {
		id := string(ci.ClusterID)
		if id == rn.clusterID {
			continue
		}
		if len(allow) > 0 && !allow[id] {
			continue
		}
		if err := rn.SendSQLToCluster(id, sql); err != nil {
			log.Printf("[federation] send to %s failed: %v", id, err)
		} else {
			log.Printf("[federation] sent to %s", id)
		}
	}
}

// SendSQLToCluster PUBLIC helper to send SQL across clusters via committee (used by /fed/send)
func (rn *RaftNode) sendSQLToCluster(destCluster, sql string) error {
	if rn.committee == nil || rn.committeeAuth == nil {
		return fmt.Errorf("committee not configured")
	}
	if len(rn.committeePeers) == 0 {
		return fmt.Errorf("no committee peers configured")
	}
	peer := rn.committeePeers[0].HTTPAddr
	// CHANGE: normalisasi kalau ada yang isi 0.0.0.0
	if host, _, err := net.SplitHostPort(peer); err == nil && host == "0.0.0.0" {
		peer = rn.httpAddr
	}

	env := committee.Envelope{
		FromCluster: committee.ClusterID(rn.clusterID),
		ToCluster:   committee.ClusterID(destCluster),
		Type:        "ROUTE_SQL_WRITE",
		Payload:     mustJSON(map[string]string{"sql": sql}),
		Nonce:       fmt.Sprintf("%d", time.Now().UnixNano()),
		TimestampMs: time.Now().UnixMilli(),
	}

	body := mustJSON(env)
	url := fmt.Sprintf("http://%s/gc/route", rn.committeePeers[0].HTTPAddr)

	log.Printf("[committee] routing via registry peer %s", url)

	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		log.Printf("sendSQLToCluster: buat request: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")
	rn.committeeAuth.SignRequest(req, body)
	_, err = http.DefaultClient.Do(req)
	return err
}

// lookupLeaderHTTP mencari alamat HTTP leader dari buku alamat,
// atau fallback derivasi port: raft 800x -> http 900x
func (rn *RaftNode) lookupLeaderHTTP() (string, error) {
	leaderRaft := string(rn.raft.Leader())
	if leaderRaft == "" || leaderRaft == "127.0.0.1:0" {
		return "", fmt.Errorf("leader belum diketahui. coba lagi")
	}
	if rn.httpAddrBook != nil {
		if h := rn.httpAddrBook[leaderRaft]; h != "" {
			return h, nil
		}
	}
	if _, leaderID := rn.raft.LeaderWithID(); leaderID != "" && rn.httpAddrBook != nil {
		if h := rn.httpAddrBook[string(leaderID)]; h != "" {
			return h, nil
		}
	}
	host, port, err := net.SplitHostPort(leaderRaft)
	if err == nil {
		if p, err := strconv.Atoi(port); err == nil {
			return net.JoinHostPort(host, strconv.Itoa(p+1000)), nil
		}
	}
	return "", fmt.Errorf("Tidak ada mapping HTTP untuk leader %q", leaderRaft)
}

func (rn *RaftNode) announceToCommittee() {
	if rn.committee == nil {
		return
	}
	leaderAddr, _ := rn.raft.LeaderWithID()
	ci := committee.ClusterInfo{
		ClusterID:     committee.ClusterID(rn.clusterID),
		LeaderRaft:    string(leaderAddr),
		LeaderHTTP:    rn.httpAddr,
		CommitteeHTTP: rn.committeeSelfHTTP,
		Term:          uint64(rn.raft.AppliedIndex()),
		LastSeenUnix:  time.Now().Unix(),
	}
	body, _ := json.Marshal(ci)
	for _, p := range rn.committeePeers {
		url := fmt.Sprintf("http://%s/gc/announce", p.HTTPAddr)

		log.Printf("[committee] announcing to %s", url)

		req, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		if rn.committeeAuth != nil {
			rn.committeeAuth.SignRequest(req, body)
		}
		_, _ = http.DefaultClient.Do(req)
	}
}

// Global envelope delivery to local cluster
func (rn *RaftNode) handleGlobalEnvelope(env committee.Envelope) (int, []byte) {
	if env.ToCluster != "" && string(env.ToCluster) != rn.clusterID {
		return http.StatusNotImplemented, nil
	}

	switch env.Type {
	case "ROUTE_SQL_WRITE":
		if rn.internalAuth == nil {
			return http.StatusInternalServerError, []byte(`"internal auth missing"`)
		}
		var msg struct {
			SQL string `json:"sql"`
		}
		if err := json.Unmarshal(env.Payload, &msg); err != nil {
			return http.StatusBadRequest, []byte(`"bad payload"`)
		}
		url := fmt.Sprintf("http://%s/propose", rn.httpAddr)
		body, _ := json.Marshal(Command{SQL: msg.SQL})
		req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			log.Printf("handleGlobalEnvelope: buat request: %v", err)

		}

		req.Header.Set("Content-Type", "application/json")
		rn.internalAuth.SignRequest(req, body)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return http.StatusBadGateway, []byte(`"local propose failed"`)
		}
		defer resp.Body.Close()
		out, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, out
	case "ROUTE_SQL_READ":
		url := fmt.Sprintf("http://%s/query", rn.httpAddr)
		req, _ := http.NewRequest(http.MethodGet, url, nil)
		if rn.publicAuth.PublicAPIToken != "" {
			req.Header.Set("X-Token", rn.publicAuth.PublicAPIToken)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return http.StatusBadGateway, []byte(`"local query failed"`)
		}
		defer resp.Body.Close()
		out, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, out
	}
	return http.StatusNotImplemented, []byte(`"unknown type"`)
}
