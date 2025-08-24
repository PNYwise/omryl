package main

import (
	"flag"
	"log"
	"net/http"
	"omryl/internal"
	"omryl/internal/auth"
	"omryl/internal/config"
	"time"

	// New import for hclog

	_ "github.com/mattn/go-sqlite3" // Driver SQLite
)

func main() {
	// Contoh penggunaan:
	// Untuk menjalankan klaster 3 node:
	//
	// Terminal 1 (Bootstrap Leader):
	// go run main.go node1 127.0.0.1:8001 127.0.0.1:9001 true
	//
	// Terminal 2 (Follower):
	// go run main.go node2 127.0.0.1:8002 127.0.0.1:9002 false
	//
	// Terminal 3 (Follower):
	// go run main.go node3 127.0.0.1:8003 127.0.0.1:9003 false
	//
	// Setelah node2 dan node3 berjalan, Anda perlu "menggabungkannya" ke node1 (pemimpin):
	//
	// Untuk node2:
	// curl -X POST -H "Content-Type: application/json" -d '{"id":"node2","address":"127.0.0.1:8002","http":"127.0.0.1:9002"}' http://127.0.0.1:9001/join
	//
	// Untuk node3:
	// curl -X POST -H "Content-Type: application/json" -d '{"id":"node3","address":"127.0.0.1:8003","http":"127.0.0.1:9003"}' http://127.0.0.1:9001/join
	//
	// Setelah bergabung, Anda dapat mengirim perintah ke node mana pun (akan diteruskan ke pemimpin):
	// curl -X POST -H "Content-Type: application/json" -d '{"sql":"INSERT INTO items (name, quantity) VALUES (\'Laptop\', 10);"}' http://127.0.0.1:9001/propose
	// curl -X POST -H "Content-Type: application/json" -d '{"sql":"UPDATE items SET quantity = 15 WHERE name = \'Laptop\';"}' http://127.0.0.1:9002/propose
	// curl -X POST -H "Content-Type: application/json" -d '{"sql":"DELETE FROM items WHERE name = \'Laptop\';"}' http://127.0.0.1:9003/propose
	//
	// Untuk mengkueri data (baca-saja dari node lokal):
	// curl http://127.0.0.1:9001/query
	// curl http://127.0.0.1:9002/query
	// curl http://127.0.0.1:9003/query
	//
	// Untuk memeriksa pemimpin:
	// curl http://127.0.0.1:9001/leader

	configPath := flag.String("config", "config.yaml", "path ke file config YAML")
	flag.Parse()

	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Gagal load config: %v", err)
	}

	// Optional: prefill known peers for dev
	var book map[string]string = nil
	// book = map[string]string{
	// 	"127.0.0.1:8001": "127.0.0.1:9001",
	// 	"127.0.0.1:8002": "127.0.0.1:9002",
	// 	"127.0.0.1:8003": "127.0.0.1:9003",
	// }

	node, err := internal.NewRaftNode(
		cfg.NodeID,
		cfg.RaftAddr,
		cfg.HTTPAddr,
		cfg.DataDir,
		book,
		cfg.IsBootstrap,
	)
	if err != nil {
		log.Fatalf("Gagal membuat node Raft: %v", err)
	}

	// Wire auth options
	if cfg.PublicAPIToken != "" {
		node.SetPublicAPIToken(&auth.PublicAuth{
			PublicAPIToken: cfg.PublicAPIToken,
		})
	}
	if cfg.PublicJoinToken != "" {
		node.SetPublicJoinToken(&auth.PublicJoinAuth{
			PublicJoinAPIToken: cfg.PublicJoinToken,
		})
	}
	if cfg.InternalSecret != "" {
		node.SetInternalAuth(&auth.InternalAuth{
			NodeID: cfg.InternalID,
			Secret: cfg.InternalSecret,
			Skew:   time.Duration(cfg.InternalClockSkewSec) * time.Second,
			// Allowlist: map[string]bool{"node1": true, "node2": true, "node3": true},
		})
	}

	// Attach Global Committee if configured
	if cfg.CommitteeClusterID != "" {
		err := node.AttachCommittee(
			cfg.CommitteeClusterID,
			cfg.HTTPAddr,
			cfg.CommitteePeers,
			&auth.InternalAuth{
				NodeID: cfg.InternalID,
				Secret: cfg.InternalSecret,
				Skew:   time.Duration(cfg.InternalClockSkewSec) * time.Second,
			})
		if err != nil {
			log.Fatalf("Gagal attach committee: %v", err)
		}

		node.SetFederation(cfg.FederatePurpose, cfg.FederateTargets)

		go node.WatchLeadership()
	}

	log.Printf("Node %s dimulai. Alamat Raft: %s, Alamat HTTP: %s", cfg.NodeID, cfg.RaftAddr, cfg.HTTPAddr)

	// Routes
	http.HandleFunc("/purpose", node.HandlePurpose) // public/simple token
	http.HandleFunc("/query", node.HandleQuery)     // public/simple token
	http.HandleFunc("/join", node.HandleJoin)       // public/simple token (admin)
	http.HandleFunc("/leader", node.HandleLeader)   // public

	// Internal (strict HMAC)
	http.HandleFunc("/propose", node.HandlePropose) // internal (strict HMAC)

	// Optional: cross-cluster send via HTTP wrapper
	http.HandleFunc("/fed/send", node.HandleFedSend) // public token required; forwards via committee

	// Global Committee endpoints (if enabled)
	if node.HasCommittee() {
		http.HandleFunc("/gc/announce", node.CommitteeHandleAnnounce)
		http.HandleFunc("/gc/route", node.CommitteeHandleRoute)
		http.HandleFunc("/gc/list", node.CommitteeHandleList)
	}

	log.Printf("Server HTTP mendengarkan di: %s", cfg.HTTPAddr)
	if err := http.ListenAndServe(cfg.HTTPAddr, nil); err != nil {
		log.Fatalf("Gagal memulai server HTTP: %v", err)
	}
}
