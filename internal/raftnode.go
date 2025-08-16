package internal

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

// RaftNode membungkus instance Raft dan FSM.
type RaftNode struct {
	raft            *raft.Raft
	fsm             *SQLiteFSM
	dbDir           string
	raftAddr        string
	httpAddr        string
	httpAddrBook    map[string]string
	publicAPIToken  string // simple token for /purpose, /query
	publicJoinToken string // simple token for /join
	internalAuth    *InternalAuth
}

// Setters for auth (call from main after construction)

// SetPublicAPIToken mengatur token untuk endpoint publik seperti /purpose dan /query.
func (rn *RaftNode) SetPublicAPIToken(tok string) { rn.publicAPIToken = tok }

// SetPublicJoinToken mengatur token untuk endpoint publik /join.
func (rn *RaftNode) SetPublicJoinToken(tok string) { rn.publicJoinToken = tok }

// SetInternalAuth mengatur otentikasi internal antar node.
func (rn *RaftNode) SetInternalAuth(a *InternalAuth) { rn.internalAuth = a }

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

// lookupLeaderHTTP mencari alamat HTTP leader dari buku alamat,
// atau fallback derivasi port: raft 800x -> http 900x
func (rn *RaftNode) lookupLeaderHTTP() (string, error) {
	leaderRaft := string(rn.raft.Leader())
	if leaderRaft == "" || leaderRaft == "0.0.0.0:0" {
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
