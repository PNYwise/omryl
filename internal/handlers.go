package internal

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/hashicorp/raft"
)

// HandlePurpose PUBLIC endpoint (simple token) → proxy to leader /propose (HMAC)
func (rn *RaftNode) HandlePurpose(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Metode tidak diizinkan", http.StatusMethodNotAllowed)
		return
	}
	if !rn.publicAuth.VerifyRequest(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Gagal membaca body: %v", err), http.StatusBadRequest)
		return
	}
	_ = r.Body.Close()

	var req Command
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, fmt.Sprintf("Gagal mendekode permintaan: %v", err), http.StatusBadRequest)
		return
	}

	if rn.raft.State() != raft.Leader {
		leaderHTTP, err := rn.lookupLeaderHTTP()
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		if rn.internalAuth == nil {
			http.Error(w, "internal auth tidak dikonfigurasi", http.StatusInternalServerError)
			return
		}
		url := fmt.Sprintf("http://%s/propose", leaderHTTP)
		fwd, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
		fwd.Header.Set("Content-Type", "application/json")
		rn.internalAuth.SignRequest(fwd, body)
		resp, err := http.DefaultClient.Do(fwd)
		if err != nil {
			http.Error(w, fmt.Sprintf("Gagal mem-forward ke leader: %v", err), http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
		return
	}

	// Leader
	if err := rn.ProposeCommand(req.SQL); err != nil {
		http.Error(w, fmt.Sprintf("Gagal mengajukan perintah: %v", err), http.StatusInternalServerError)
		return
	}

	if rn.federatePurpose && rn.HasCommittee() {
		go rn.fanoutSQL(req.SQL)
	}

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("Perintah berhasil diajukan dan akan direplikasi."))
}

// HandlePropose INTERNAL endpoint (strict HMAC). Followers proxy w/ HMAC.
func (rn *RaftNode) HandlePropose(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Metode tidak diizinkan", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Gagal membaca body: %v", err), http.StatusBadRequest)
		return
	}
	_ = r.Body.Close()

	if rn.internalAuth == nil || !rn.internalAuth.VerifyRequest(r, body) {
		http.Error(w, "unauthorized (internal)", http.StatusUnauthorized)
		return
	}

	var req Command
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, fmt.Sprintf("Gagal mendekode permintaan: %v", err), http.StatusBadRequest)
		return
	}

	if rn.raft.State() != raft.Leader {
		leaderHTTP, err := rn.lookupLeaderHTTP()
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		url := fmt.Sprintf("http://%s/propose", leaderHTTP)
		fwd, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
		fwd.Header.Set("Content-Type", "application/json")
		rn.internalAuth.SignRequest(fwd, body)
		resp, err := http.DefaultClient.Do(fwd)
		if err != nil {
			http.Error(w, fmt.Sprintf("Gagal mem-forward ke leader: %v", err), http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
		return
	}

	if err := rn.ProposeCommand(req.SQL); err != nil {
		http.Error(w, fmt.Sprintf("Gagal mengajukan perintah: %v", err), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("Perintah berhasil diajukan dan akan direplikasi."))
}

// HandleQuery PUBLIC (read-only) with simple token
func (rn *RaftNode) HandleQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Metode tidak diizinkan", http.StatusMethodNotAllowed)
		return
	}
	if !rn.publicAuth.VerifyRequest(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	rn.fsm.mu.RLock()
	defer rn.fsm.mu.RUnlock()

	rows, err := rn.fsm.db.Query("SELECT id, name, quantity FROM items ORDER BY id DESC LIMIT 10;")
	if err != nil {
		http.Error(w, fmt.Sprintf("Gagal mengkueri data: %v", err), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var results []map[string]any
	for rows.Next() {
		var id int
		var name string
		var quantity int
		if err := rows.Scan(&id, &name, &quantity); err != nil {
			log.Printf("Gagal memindai baris: %v", err)
			continue
		}
		results = append(results, map[string]any{
			"id":       id,
			"name":     name,
			"quantity": quantity,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(results); err != nil {
		http.Error(w, fmt.Sprintf("Gagal meng-encode respons JSON: %v", err), http.StatusInternalServerError)
	}
}

// HandleJoin PUBLIC admin with simple token; followers proxy to leader (token forwarded)
func (rn *RaftNode) HandleJoin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Metode tidak diizinkan", http.StatusMethodNotAllowed)
		return
	}
	if !rn.publicJoinAuth.VerifyRequest(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	// Proxy to leader if follower
	if rn.raft.State() != raft.Leader {
		leaderHTTP, err := rn.lookupLeaderHTTP()
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, fmt.Sprintf("Gagal membaca body: %v", err), http.StatusBadRequest)
			return
		}
		_ = r.Body.Close()

		url := fmt.Sprintf("http://%s/join", leaderHTTP)
		fwd, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			http.Error(w, fmt.Sprintf("Gagal membuat request ke leader: %v", err), http.StatusInternalServerError)
			return
		}
		if ct := r.Header.Get("Content-Type"); ct != "" {
			fwd.Header.Set("Content-Type", ct)
		} else {
			fwd.Header.Set("Content-Type", "application/json")
		}
		if tok := r.Header.Get("X-Token"); tok != "" {
			fwd.Header.Set("X-Token", tok)
		}

		resp, err := http.DefaultClient.Do(fwd)
		if err != nil {
			http.Error(w, fmt.Sprintf("Gagal menghubungi leader: %v", err), http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
		return
	}

	// Leader: decode and add voter
	var req joinReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Gagal mendekode permintaan gabung: %v", err), http.StatusBadRequest)
		return
	}
	if req.ID == "" || req.RaftAddr == "" || req.HTTPAddr == "" {
		http.Error(w, "Field id, address, http wajib diisi", http.StatusBadRequest)
		return
	}
	log.Printf("Menambahkan server baru: ID=%s, Raft=%s, HTTP=%s", req.ID, req.RaftAddr, req.HTTPAddr)

	// Idempotency: if already present, just succeed and update book
	cfg := rn.raft.GetConfiguration()
	if err := cfg.Error(); err == nil {
		for _, s := range cfg.Configuration().Servers {
			if s.ID == raft.ServerID(req.ID) || s.Address == raft.ServerAddress(req.RaftAddr) {
				if rn.httpAddrBook == nil {
					rn.httpAddrBook = make(map[string]string)
				}
				rn.httpAddrBook[req.RaftAddr] = req.HTTPAddr
				rn.httpAddrBook[req.ID] = req.HTTPAddr
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte("Node sudah menjadi bagian dari klaster."))
				return
			}
		}
	}

	addFuture := rn.raft.AddVoter(raft.ServerID(req.ID), raft.ServerAddress(req.RaftAddr), 0, 0)
	if err := addFuture.Error(); err != nil {
		http.Error(w, fmt.Sprintf("Gagal menambahkan pemilih: %v", err), http.StatusInternalServerError)
		return
	}

	if rn.httpAddrBook == nil {
		rn.httpAddrBook = make(map[string]string)
	}
	rn.httpAddrBook[req.RaftAddr] = req.HTTPAddr
	rn.httpAddrBook[req.ID] = req.HTTPAddr

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("Node berhasil bergabung ke klaster."))
}

// HandleLeader info pemimpin saat ini (biarkan publik atau wrap requirePublicAPI)
func (rn *RaftNode) HandleLeader(w http.ResponseWriter, r *http.Request) {
	leaderAddr, leaderID := rn.raft.LeaderWithID()
	resp := map[string]string{
		"leader_id":      string(leaderID),
		"leader_address": string(leaderAddr),
		"node_state":     rn.raft.State().String(),
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, fmt.Sprintf("Gagal meng-encode respons JSON: %v", err), http.StatusInternalServerError)
	}
}

// HandleFedSend PUBLIC with simple token
// Optional HTTP wrapper to trigger cross-cluster send via curl
// POST /fed/send {"to":"cluster-b","sql":"INSERT ..."}
func (rn *RaftNode) HandleFedSend(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Metode tidak diizinkan", http.StatusMethodNotAllowed)
		return
	}
	if !rn.publicAuth.VerifyRequest(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	var req struct {
		To  string `json:"to"`
		SQL string `json:"sql"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if req.To == "" || req.SQL == "" {
		http.Error(w, "field 'to' dan 'sql' wajib diisi", http.StatusBadRequest)
		return
	}
	if err := rn.sendSQLToCluster(req.To, req.SQL); err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write([]byte("routed"))
}

func mustJSON(v any) []byte { b, _ := json.Marshal(v); return b }

// Committee helpers

// HasCommittee menginisialisasi node komite jika belum ada.
func (rn *RaftNode) HasCommittee() bool { return rn.committee != nil }

// CommitteeHandleAnnounce meneruskan permintaan pengumuman ke node komite.
func (rn *RaftNode) CommitteeHandleAnnounce(w http.ResponseWriter, r *http.Request) {
	rn.committee.HandleAnnounce(w, r)
}

// CommitteeHandleRoute meneruskan permintaan ke node komite.
func (rn *RaftNode) CommitteeHandleRoute(w http.ResponseWriter, r *http.Request) {
	rn.committee.HandleRoute(w, r)
}

// CommitteeHandleList mengembalikan daftar cluster yang diketahui komite.
func (rn *RaftNode) CommitteeHandleList(w http.ResponseWriter, r *http.Request) {
	rn.committee.HandleList(w, r)
}
