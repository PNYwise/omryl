# Omryl – Raft + Global Committee


This repo runs a local Raft cluster and an optional Global Committee layer for multi-cluster comms. 

(yada yada yada)

> TODO
> - deskripsi singkat
> - cara kerja
> - tech behind
> - draw.io
> - rekomendasi arsitektur untuk menjalankan


## 1) Install deps
> TODO
> - production ready instalation
> - development mode instalation
 

## 2) Start a single cluster with 3 nodes
Open 3 terminals and create 3 config files by copying config.yaml and editing:

- node1.yaml (bootstrap leader)

```yaml
node_id: "node1"
raft_addr: "127.0.0.1:8001"
http_addr: "0.0.0.0:9001"
is_bootstrap: true

public_api_token: "public-api-token"
public_join_token: "public-join-token"

internal_id: "node1"
internal_secret: "change-me-very-secret"
internal_clock_skew_sec: 60

committee_cluster_id: "indonesian-cluster"
committee_peers:
  - id: central
    http_addr: 0.0.0.0:9001

federate_purpose: true
federateTargets:
  - us-cluster

```

- node2.yaml

```yaml
node_id: "node2"
raft_addr: "127.0.0.1:8002"
http_addr: "0.0.0.0:9002"
is_bootstrap: false

public_api_token: "public-api-token"
public_join_token: "public-join-token"

internal_id: "node2"
internal_secret: "change-me-very-secret"
internal_clock_skew_sec: 60

committee_cluster_id: "indonesian-cluster"
committee_peers:
  - id: central
    http_addr: 0.0.0.0:9001

federate_purpose: true
federate_targets:
  - us-cluster 
```
- node3.yaml

```yaml
node_id: "node3"
raft_addr: "127.0.0.1:8003"
http_addr: "0.0.0.0:9003"
is_bootstrap: false

public_api_token: "public-api-token"
public_join_token: "public-join-token"

internal_id: "node2"
internal_secret: "change-me-very-secret"
internal_clock_skew_sec: 60

committee_cluster_id: "indonesian-cluster"
committee_peers:
  - id: central
    http_addr: 0.0.0.0:9001

federate_purpose: true
federateTargets:
  - us-cluster
```

Start processes:
```bash
# Terminal 1
go run . -config node1.yaml

# Terminal 2
go run . -config node2.yaml

# Terminal 3
go run . -config node3.yaml
```


Join followers to the leader (use the public join token):
```bash
curl -X POST -H "Content-Type: application/json" -H "X-Token: public-join-token" \
  -d '{"id":"node2","address":"127.0.0.1:8002","http":"0.0.0.0:9002"}' \
  http://0.0.0.0:9001/join

curl -X POST -H "Content-Type: application/json" -H "X-Token: public-join-token" \
  -d '{"id":"node3","address":"127.0.0.1:8003","http":"0.0.0.0:9003"}' \
  http://0.0.0.0:9001/join
```

Write through any node (they forward to the leader). Use the public API token:

```bash
curl -X POST -H "Content-Type: application/json" -H "X-Token: public-api-token" \
  -d '{"sql":"INSERT INTO items (name, quantity) VALUES (\"Laptop\", 10);"}' \
  http://0.0.0.0:9002/purpose
```

Query any node (local read):

```bash
curl -H "X-Token: public-api-token" http://0.0.0.0:9003/query
```

List leader:

```bash
curl http://0.0.0.0:9001/leader
```

## 3) Start a second cluster (for federation demo)

Create node4.yaml (bootstrap of cluster-b):

```yaml
node_id: "node1-us-cluster"
raft_addr: "127.0.0.1:8004"
http_addr: "0.0.0.0:9004"
is_bootstrap: true

public_api_token: "public-api-token"
public_join_token: "public-join-token"

internal_id: "node1-us-cluster"
internal_secret: "change-me-very-secret"
internal_clock_skew_sec: 60

committee_cluster_id: "us-cluster"
committee_peers:
  - id: central
    http_addr: 0.0.0.0:9001 ## indoesian cluster node address

federate_purpose: true
federate_targets: ## just comment if you wanna distribute to all avalilable cluster
  - indonesian-cluster
```

Start it:
```bash
go run . -config node4.yaml
```
When node1 becomes leader in indonesian-cluster, it will ANNOUNCE to the committee. Same for node4 (leader of us-cluster).

List all clusters known to the committee (any peer):
```bash
# HMAC-signed by internal code; for quick check (no auth) you can just hit it if you trust local dev
curl http://0.0.0.0:9001/gc/list
```