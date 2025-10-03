#!/bin/bash

# ==============================================================================
# SCRIPT CONFIGURATION
# ==============================================================================
# Dừng script ngay lập tức nếu có bất kỳ lệnh nào thất bại
set -e

# Cấu hình Benchmark
NODES=5
RATE=60000
TX_SIZE=600
DURATION=30

# Cấu hình Node đầy đủ
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

# --- Đường dẫn file cấu hình ---
LOG_DIR="$BENCHMARK_DIR/logs"
COMMITTEE_FILE="$BENCHMARK_DIR/.committee.json"
PARAMETERS_FILE="$BENCHMARK_DIR/.parameters.json"

# ==============================================================================
# SCRIPT EXECUTION
# ==============================================================================
# --- Stage 0: Build ---
echo "--- Stage 0: Building Binaries ---"
cargo build --release --features benchmark
# Biên dịch binary executor Go
echo "INFO: Building Go executor binary..."
(cd go/cmd/exetps && go build -o ../../bin/exetps) || { echo "LỖI: Không thể biên dịch Go executor."; exit 1; }

# --- Giai đoạn 1: Dọn dẹp và Kiểm tra ---
echo ""
echo "--- Stage 1: Cleanup and Preparation ---"
echo "INFO: Forcefully killing any lingering processes..."
pkill -f "$NODE_BINARY" || true
pkill -f "$CLIENT_BINARY" || true
pkill -f "$EXECUTOR_BINARY" || true
sleep 1

echo "INFO: Cleaning up old files..."
rm -rf "$BENCHMARK_DIR"/db_* "$BENCHMARK_DIR"/.node* "$COMMITTEE_FILE" "$PARAMETERS_FILE"
# Giữ lại thư mục logs nhưng xóa nội dung cũ
rm -rf "$LOG_DIR"/*
mkdir -p "$LOG_DIR"

# Kiểm tra các công cụ cần thiết
if ! command -v jq &> /dev/null; then
    echo "LỖI: Lệnh 'jq' không tồn tại. Vui lòng cài đặt (ví dụ: sudo apt-get install jq)"
    exit 1
fi
if ! command -v tmux &> /dev/null; then
    echo "LỖI: Lệnh 'tmux' không tồn tại. Vui lòng cài đặt (ví dụ: sudo apt-get install tmux)"
    exit 1
fi
for bin in "$NODE_BINARY" "$CLIENT_BINARY" "$EXECUTOR_BINARY"; do
    if [ ! -f "$bin" ]; then
        echo "LỖI: Không tìm thấy file thực thi tại '$bin'. Bạn đã biên dịch code chưa?"
        exit 1
    fi
    chmod +x "$bin" # Đảm bảo file có quyền thực thi
done

# --- Giai đoạn 2: Tạo Cấu hình ---
echo ""
echo "--- Stage 2: Configuration File Generation ---"
echo "INFO: Generating key files..."
key_files=()
for i in $(seq 0 $((NODES-1))); do
    key_file="$BENCHMARK_DIR/.node-$i.json"
    $NODE_BINARY generate_keys --filename "$key_file"
    key_files+=("$key_file")
done

echo "INFO: Creating parameters file ($PARAMETERS_FILE)..."
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

echo "INFO: Creating committee file ($COMMITTEE_FILE)..."
committee_json='{ "authorities": {} }'
for i in $(seq 0 $((NODES-1))); do
    key_file="${key_files[$i]}";
    name=$(jq -r '.name' "$key_file");

    primary_to_primary_addr="127.0.0.1:$((COMMITTEE_BASE_PORT + i*10))"
    worker_to_primary_addr="127.0.0.1:$((COMMITTEE_BASE_PORT + i*10 + 1))"
    primary_to_worker_addr="127.0.0.1:$((COMMITTEE_BASE_PORT + i*10 + 2))"
    transactions_addr="127.0.0.1:$((COMMITTEE_BASE_PORT + i*10 + 3))"
    worker_to_worker_addr="127.0.0.1:$((COMMITTEE_BASE_PORT + i*10 + 4))"

    committee_json=$(echo "$committee_json" | jq \
      --arg name "$name" \
      --arg p2p_addr "$primary_to_primary_addr" \
      --arg w2p_addr "$worker_to_primary_addr" \
      --arg p2w_addr "$primary_to_worker_addr" \
      --arg tx_addr "$transactions_addr" \
      --arg w2w_addr "$worker_to_worker_addr" \
      '.authorities[$name] = {
        "primary": { "primary_to_primary": $p2p_addr, "worker_to_primary": $w2p_addr },
        "stake": 1,
        "workers": { "0": { "primary_to_worker": $p2w_addr, "transactions": $tx_addr, "worker_to_worker": $w2w_addr } }
      }'
    )
done
echo "$committee_json" | jq . > "$COMMITTEE_FILE"
echo "INFO: Configuration files generated successfully."

# --- Giai đoạn 3: Khởi chạy Nodes và Workers ---
echo ""
echo "--- Stage 3: Launching Nodes and Workers ---"
echo "INFO: Launching $NODES primaries, workers, and executors..."
for i in $(seq 0 $((NODES-1))); do
    key_file="${key_files[$i]}";

    # Khởi chạy Primary
    primary_db_path="$BENCHMARK_DIR/db_primary_$i"; primary_log_file="$LOG_DIR/primary-$i.log"
    primary_cmd="$NODE_BINARY run --keys $key_file --committee $COMMITTEE_FILE --store $primary_db_path --parameters $PARAMETERS_FILE primary"
    tmux new -d -s "primary-$i" "sh -c 'RUST_LOG=info $primary_cmd > \"$primary_log_file\" 2>&1 || echo \"[FATAL] Primary process exited.\" >> \"$primary_log_file\"'"

    # Khởi chạy Worker
    worker_id=0 # Worker ID được gán cứng theo cấu hình
    worker_db_path="$BENCHMARK_DIR/db_worker_${i}_${worker_id}"; worker_log_file="$LOG_DIR/worker-${i}-${worker_id}.log"
    # Đảm bảo --id=$worker_id luôn là một số nguyên
    worker_cmd="$NODE_BINARY run --keys $key_file --committee $COMMITTEE_FILE --store $worker_db_path --parameters $PARAMETERS_FILE worker --id $worker_id"
    tmux new -d -s "worker-${i}-${worker_id}" "sh -c 'RUST_LOG=info $worker_cmd > \"$worker_log_file\" 2>&1 || echo \"[FATAL] Worker process exited.\" >> \"$worker_log_file\"'"

    # Khởi chạy Executor
    executor_log_file="$LOG_DIR/executor-$i.log"
    executor_cmd="$EXECUTOR_BINARY --id $i"
    tmux new -d -s "executor-$i" "sh -c '$executor_cmd > \"$executor_log_file\" 2>&1 || echo \"[FATAL] Executor process exited.\" >> \"$executor_log_file\"'"
done

echo ""
echo "INFO: Waiting for all nodes to boot (5 seconds)..."
sleep 5
echo "✅ All processes are now running in tmux sessions."
echo "   - Use 'tmux ls' to view all sessions."
echo "   - Use 'tail -f $LOG_DIR/primary-0.log' to monitor a primary node."
echo "   - Use 'tmux kill-server' to stop all nodes."