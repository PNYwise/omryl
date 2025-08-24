package committee

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// HandleAnnounce is the only required discovery verb.
func (n *Node) HandleAnnounce(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	_ = r.Body.Close()
	if n.internalAuth == nil || !n.internalAuth.VerifyRequest(r, body) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	var ci ClusterInfo
	if err := json.Unmarshal(body, &ci); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	ci.LastSeenUnix = time.Now().Unix()
	n.UpsertCluster(ci)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`"ok"`))
}

// HandleRoute consults the registry and forwards directly to the destination cluster's committee HTTP.
func (n *Node) HandleRoute(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	_ = r.Body.Close()
	if n.internalAuth == nil || !n.internalAuth.VerifyRequest(r, body) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	var env Envelope
	if err := json.Unmarshal(body, &env); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// If this node fronts a local cluster and the target is local/broadcast, try local first
	code, resp := n.LocalDeliver(env)
	if code != http.StatusNotImplemented {
		w.WriteHeader(code)
		_, _ = w.Write(resp)
		if env.ToCluster != "" {
			return
		}
	}

	// Centralized discovery: target a specific cluster, not broadcast
	if env.ToCluster != "" {
		_, dstCommitteeHTTP, ok := n.ResolveLeaderHTTP(env.ToCluster)
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`"unknown target cluster"`))
			return
		}
		url := fmt.Sprintf("http://%s/gc/route", dstCommitteeHTTP)
		req, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		n.internalAuth.SignRequest(req, body)
		resp2, err := n.httpClient.Do(req)
		if err != nil {
			http.Error(w, "forward failed", http.StatusBadGateway)
			return
		}
		defer resp2.Body.Close()
		w.WriteHeader(resp2.StatusCode)
		_, _ = io.Copy(w, resp2.Body)
		return
	}

	// Broadcast remains optional; send to all peers
	for _, p := range n.peers {
		url := fmt.Sprintf("http://%s/gc/route", p.HTTPAddr)
		req, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		n.internalAuth.SignRequest(req, body)
		_, _ = n.httpClient.Do(req)
	}
	if code == http.StatusNotImplemented {
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`"forwarded"`))
	}
}

// HandleList returns all known clusters in the registry.
func (n *Node) HandleList(w http.ResponseWriter, r *http.Request) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	out := make([]*ClusterInfo, 0, len(n.clusters))
	for _, v := range n.clusters {
		out = append(out, v)
	}
	_ = json.NewEncoder(w).Encode(out)
}
