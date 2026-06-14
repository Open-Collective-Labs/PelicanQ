#!/usr/bin/env bash
set -euo pipefail

# dev-cluster.sh — start a 3-node PelicanQ cluster locally for manual testing.
#
# Each node gets a distinct HTTP port, Raft RPC port, and data directory.
# Node 1 (lowest ID) bootstraps the cluster automatically.
#
# Usage:
#   ./scripts/dev-cluster.sh [--build]
#
# Options:
#   --build   Rebuild the binaries before starting.

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if [[ "${1:-}" == "--build" ]]; then
  echo "==> Building pelicanqd …"
  cargo build -p pelicanqd 2>&1
fi

BINARY="${ROOT}/target/debug/pelicanqd"

# ---- Node configuration ----
# Node 1
NODE1_DIR="/tmp/pelicanq-dev/1"
NODE1_HTTP="127.0.0.1:7070"
NODE1_RAFT="127.0.0.1:7071"

# Node 2
NODE2_DIR="/tmp/pelicanq-dev/2"
NODE2_HTTP="127.0.0.1:7080"
NODE2_RAFT="127.0.0.1:7081"

# Node 3
NODE3_DIR="/tmp/pelicanq-dev/3"
NODE3_HTTP="127.0.0.1:7090"
NODE3_RAFT="127.0.0.1:7091"

MEMBERS="1@${NODE1_RAFT}=${NODE1_HTTP},2@${NODE2_RAFT}=${NODE2_HTTP},3@${NODE3_RAFT}=${NODE3_HTTP}"

# ---- Clean up any leftover processes ----
cleanup() {
  echo ""
  echo "==> Shutting down cluster …"
  for pid in /tmp/pelicanq-dev/pid-*.pid; do
    [[ -f "$pid" ]] && kill "$(cat "$pid")" 2>/dev/null || true
    rm -f "$pid"
  done
  echo "    done."
}
trap cleanup EXIT

# Kill any existing processes on our ports.
for port in 7070 7071 7080 7081 7090 7091; do
  lsof -ti ":$port" 2>/dev/null | xargs kill -9 2>/dev/null || true
done

# Clean old data for a fresh cluster start.
rm -rf /tmp/pelicanq-dev
mkdir -p "$NODE1_DIR" "$NODE2_DIR" "$NODE3_DIR"

# ---- Start nodes ----
echo "==> Starting 3-node cluster …"
echo ""

start_node() {
  local id="$1"
  local http="$2"
  local raft="$3"
  local dir="$4"

  mkdir -p "$dir"
  local pid_file="/tmp/pelicanq-dev/pid-${id}.pid"
  local log_file="/tmp/pelicanq-dev/node-${id}.log"

  PELICANQ_NODE_ID="$id" \
    PELICANQ_CLUSTER_MEMBERS="$MEMBERS" \
    PELICANQ_DATA_DIR="$dir" \
    PELICANQ_LISTEN_ADDR="$http" \
    "$BINARY" \
    >"$log_file" 2>&1 &
  echo $! >"$pid_file"

  echo "  Node $id  HTTP=$http  Raft=$raft  PID=$!"
}

start_node 1 "$NODE1_HTTP" "$NODE1_RAFT" "$NODE1_DIR"
sleep 0.5
start_node 2 "$NODE2_HTTP" "$NODE2_RAFT" "$NODE2_DIR"
sleep 0.5
start_node 3 "$NODE3_HTTP" "$NODE3_RAFT" "$NODE3_DIR"

# ---- Wait for cluster to stabilize ----
echo ""
echo "==> Waiting for leader election …"
for i in $(seq 1 20); do
  leader=$(curl -s http://127.0.0.1:7070/cluster/status 2>/dev/null \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('self_id','?'))" 2>/dev/null || echo "?")
  if [[ "$leader" != "?" ]]; then
    echo "    Node 1 reports self_id=$leader"
    break
  fi
  sleep 1
done

echo ""
echo "==> Cluster is running."
echo ""
echo "  Node 1 (HTTP API):  http://127.0.0.1:7070"
echo "  Node 2 (HTTP API):  http://127.0.0.1:7080"
echo "  Node 3 (HTTP API):  http://127.0.0.1:7090"
echo ""
echo "  Members: $MEMBERS"
echo ""
echo "  Check cluster status:  curl http://127.0.0.1:7070/cluster/status"
echo "  Check health:          curl http://127.0.0.1:7070/health"
echo "  Declare a queue:       curl -X POST http://127.0.0.1:7070/queues/test"
echo "  Publish a message:     curl -X POST http://127.0.0.1:7070/queues/test/publish \\"
echo "                           -H 'content-type: application/json' \\"
echo "                           -d '{\"payload_base64\":\"aGVsbG8=\"}'"
echo "  Consume a message:     curl -X POST http://127.0.0.1:7070/queues/test/consume"
echo ""
echo "  Tailing logs:"
echo "    tail -f /tmp/pelicanq-dev/node-1.log"
echo "    tail -f /tmp/pelicanq-dev/node-2.log"
echo "    tail -f /tmp/pelicanq-dev/node-3.log"
echo ""
echo "  Press Ctrl-C to stop the cluster."
echo ""

# Tail all logs so the operator sees live output.
tail -f /tmp/pelicanq-dev/node-*.log
