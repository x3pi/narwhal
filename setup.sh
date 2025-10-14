#!/bin/bash

# ==============================================================================
# SETUP SCRIPT (Build + Generate Config - Keep Data)
# ==============================================================================

set -e

# --- Cấu hình Benchmark ---
NODES=10
WORKERS_PER_NODE=2 
HEADER_SIZE=1000
MAX_HEADER_DELAY=200
GC_DEPTH=50
SYNC_RETRY_DELAY=10000
SYNC_RETRY_NODES=3
BATCH_SIZE=500000
MAX_BATCH_DELAY=200

# --- Đường dẫn ---
BENCHMARK_DIR="benchmark"
NODE_BINARY="./target/release/node"
CLIENT_BINARY="./target/release/benchmark_client"
EXECUTOR_BINARY="./go/bin/exetps"
COMMITTEE_BASE_PORT=4000

LOG_DIR="$BENCHMARK_DIR/logs"
COMMITTEE_FILE="$BENCHMARK_DIR/.committee.json"
PARAMETERS_FILE="$BENCHMARK_DIR/.parameters.json"

# --- Stage 0: Build ---
echo "--- Stage 0: Build ---"
cargo build --release --features benchmark
echo "INFO: Building Go executor binary..."
(cd go/cmd/exetps && go build -o ../../bin/exetps)

# --- Stage 1: Cleanup ---
echo "--- Stage 1: Cleanup (Keeping Databases) ---"
pkill -f "$NODE_BINARY" || true
pkill -f "$CLIENT_BINARY" || true
pkill -f "$EXECUTOR_BINARY" || true
sleep 1
# SỬA ĐỔI: Xóa mọi thứ NGOẠI TRỪ các thư mục database (db_*)
rm -rf "$LOG_DIR" "$BENCHMARK_DIR"/.node* "$COMMITTEE_FILE" "$PARAMETERS_FILE"
mkdir -p "$LOG_DIR"

# --- Stage 2: Generate Config ---
echo "--- Stage 2: Generate Config ---"
key_files=()
for i in $(seq 0 $((NODES-1))); do
    key_file="$BENCHMARK_DIR/.node-$i.json"
    $NODE_BINARY generate_keys --filename "$key_file"
    key_files+=("$key_file")
done

jq -n \
  --argjson batch_size "$BATCH_SIZE" \
  --argjson gc_depth "$GC_DEPTH" \
  --argjson header_size "$HEADER_SIZE" \
  --argjson max_batch_delay "$MAX_BATCH_DELAY" \
  --argjson max_header_delay "$MAX_HEADER_DELAY" \
  --argjson sync_retry_delay "$SYNC_RETRY_DELAY" \
  --argjson sync_retry_nodes "$SYNC_RETRY_NODES" \
  '{
    "batch_size": $batch_size,
    "gc_depth": $gc_depth,
    "header_size": $header_size,
    "max_batch_delay": $max_batch_delay,
    "max_header_delay": $max_header_delay,
    "sync_retry_delay": $sync_retry_delay,
    "sync_retry_nodes": $sync_retry_nodes
  }' > "$PARAMETERS_FILE"

json_template='{ "authorities": {} }'
committee_json="$json_template"
for i in $(seq 0 $((NODES-1))); do
    key_file="${key_files[$i]}"
    name=$(jq -r '.name' "$key_file")
    consensus_key=$(jq -r '.consensus_key' "$key_file")

    p2p_addr="127.0.0.1:$((COMMITTEE_BASE_PORT + i*100))"
    w2p_addr="127.0.0.1:$((COMMITTEE_BASE_PORT + i*100 + 1))"

    # Tạo JSON cho các workers
    workers_json="{}"
    for j in $(seq 0 $((WORKERS_PER_NODE-1))); do
        p2w_addr="127.0.0.1:$((COMMITTEE_BASE_PORT + i*100 + 10 + j*10))"
        tx_addr="127.0.0.1:$((COMMITTEE_BASE_PORT + i*100 + 10 + j*10 + 1))"
        w2w_addr="127.0.0.1:$((COMMITTEE_BASE_PORT + i*100 + 10 + j*10 + 2))"
        worker_entry=$(jq -n \
            --arg p2w "$p2w_addr" \
            --arg tx "$tx_addr" \
            --arg w2w "$w2w_addr" \
            '{ "primary_to_worker": $p2w, "transactions": $tx, "worker_to_worker": $w2w }'
        )
        workers_json=$(echo "$workers_json" | jq --arg j_str "$j" --argjson entry "$worker_entry" '.[$j_str] = $entry')
    done

    committee_json=$(echo "$committee_json" | jq \
      --arg name "$name" \
      --arg consensus_key "$consensus_key" \
      --arg p2p "$p2p_addr" \
      --arg w2p "$w2p_addr" \
      --argjson workers "$workers_json" \
      '.authorities[$name] = {
        "stake": 1,
        "consensus_key": $consensus_key,
        "primary": { "primary_to_primary": $p2p, "worker_to_primary": $w2p },
        "workers": $workers
      }'
    )
done
echo "$committee_json" | jq . > "$COMMITTEE_FILE"

echo "✅ Setup done! Config generated in $BENCHMARK_DIR/"