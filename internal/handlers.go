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

// Command merepresentasikan perintah SQL yang akan direplikasi.
type Command struct {
	SQL string `json:"sql"`
}

type joinReq struct {
	ID       string `json:"id"`
	RaftAddr string `json:"address"` // raft addr
	HTTPAddr string `json:"http"`    // http addr
}

// Public/simple token gate
func (rn *RaftNode) requirePublicAPI(w http.ResponseWriter, r *http.Request) bool {
	if rn.publicAPIToken == "" {
		return true
	}
	if r.Header.Get("X-Token") == rn.publicAPIToken {
		return true
	}
	http.Error(w, "unauthorized", http.StatusUnauthorized)
	return false
}
func (rn *RaftNode) requirePublicJoin(w http.ResponseWriter, r *http.Request) bool {
	tok := r.Header.Get("X-Token")
	if rn.publicJoinToken != "" && tok == rn.publicJoinToken {
		return true
	}
	if rn.publicAPIToken != "" && tok == rn.publicAPIToken {
		return true
	}
	if rn.publicJoinToken == "" && rn.publicAPIToken == "" {
		return true
	}
	http.Error(w, "unauthorized", http.StatusUnauthorized)
	return false
}

// HandlePurpose PUBLIC endpoint (simple token) → proxy to leader /propose (HMAC)
func (rn *RaftNode) HandlePurpose(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Metode tidak diizinkan", http.StatusMethodNotAllowed)
		return
	}
	if !rn.requirePublicAPI(w, r) {
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
	if !rn.requirePublicAPI(w, r) {
		return
	}

	rn.fsm.mu.Lock()
	defer rn.fsm.mu.Unlock()

	rows, err := rn.fsm.db.Query("SELECT id, name, quantity FROM items;")
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
	if !rn.requirePublicJoin(w, r) {
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
