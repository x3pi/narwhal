#!/bin/bash

# ==============================================================================
# SCRIPT CONFIGURATION
# ==============================================================================
# Dừng script ngay lập tức nếu có bất kỳ lệnh nào thất bại
set -e

# Cấu hình Benchmark (giống hệt trong fabfile.py)
NODES=3
RATE=60000
TX_SIZE=600
DURATION=30

# Cấu hình Node đầy đủ (tương thích với cấu trúc parameters.json mới)
HEADER_SIZE=1000
MAX_HEADER_DELAY=200
GC_DEPTH=50
SYNC_RETRY_DELAY=10000
SYNC_RETRY_NODES=3
BATCH_SIZE=500000
MAX_BATCH_DELAY=200


# --- Đường dẫn ---
BASE_PORT=6000
BENCHMARK_DIR="benchmark"
NODE_BINARY="./target/release/node"
CLIENT_BINARY="./target/release/benchmark_client"
EXECUTOR_BINARY="./go/bin/exetps" # Đường dẫn mới cho binary executor

# New base port for committee addresses (based on the desired committee.json structure)
COMMITTEE_BASE_PORT=4000

# --- Đường dẫn file cấu hình ---
LOG_DIR="$BENCHMARK_DIR/logs"
FABFILE_PATH="fabfile.py"
COMMITTEE_FILE="$BENCHMARK_DIR/.committee.json"
PARAMETERS_FILE="$BENCHMARK_DIR/.parameters.json"

# ==============================================================================
# SCRIPT EXECUTION
# ==============================================================================
# --- Stage 0: Build ---
cargo clean
cargo build --release --features benchmark
# Biên dịch binary executor Go
echo "INFO: Building Go executor binary..."
(cd go/cmd/exetps && go build -o ../../bin/exetps) || { echo "LỖI: Không thể biên dịch Go executor."; exit 1; }

# --- Giai đoạn 1: Dọn dẹp và Kiểm tra ---
echo "--- Stage 1: Cleanup and Preparation ---"
echo "INFO: Stopping tmux server..."
# tmux kill-server > /dev/null 2>&1 || true
echo "INFO: Forcefully killing any lingering node or client processes..."
pkill -f "$NODE_BINARY" || true
pkill -f "$CLIENT_BINARY" || true
pkill -f "$EXECUTOR_BINARY" || true # Thêm pkill cho executor
sleep 1


echo "INFO: Cleaning up old files..."
rm -rf aba_deadlocks.log
rm -rf "$LOG_DIR" "$BENCHMARK_DIR"/db_* "$BENCHMARK_DIR"/.node* "$COMMITTEE_FILE" "$PARAMETERS_FILE"
mkdir -p "$LOG_DIR"

if ! command -v jq &> /dev/null; then
    echo "LỖI: Lệnh 'jq' không tồn tại. Vui lòng cài đặt: sudo apt-get install jq"
    exit 1
fi
if ! command -v tmux &> /dev/null; then
    echo "LỖI: Lệnh 'tmux' không tồn tại. Vui lòng cài đặt: sudo apt-get install tmux"
    exit 1
fi
for bin in "$NODE_BINARY" "$CLIENT_BINARY" "$EXECUTOR_BINARY"; do # Kiểm tra cả executor binary
    if [ ! -f "$bin" ]; then
        echo "LỖI: Không tìm thấy file thực thi tại '$bin'. Bạn đã biên dịch code (cargo build --release và go build) chưa?"
        exit 1
    fi
    if [ ! -x "$bin" ]; then
        echo "LỖI: File '$bin' không có quyền thực thi. Hãy chạy: chmod +x $bin"
        exit 1
    fi
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
# Mẫu JSON khởi tạo cho cấu trúc committee mới
json_template='{ "authorities": {} }'
committee_json="$json_template"
for i in $(seq 0 $((NODES-1))); do
    key_file="${key_files[$i]}";
    name=$(jq -r '.name' "$key_file");

    # Tính toán các địa chỉ mạng dựa trên cấu trúc committee.json mới
    primary_to_primary_addr="127.0.0.1:$((COMMITTEE_BASE_PORT + i*5))"
    worker_to_primary_addr="127.0.0.1:$((COMMITTEE_BASE_PORT + i*5 + 1))"
    primary_to_worker_addr="127.0.0.1:$((COMMITTEE_BASE_PORT + i*5 + 2))"
    transactions_addr="127.0.0.1:$((COMMITTEE_BASE_PORT + i*5 + 3))"
    worker_to_worker_addr="127.0.0.1:$((COMMITTEE_BASE_PORT + i*5 + 4))"

    # Xây dựng cấu trúc JSON cho mỗi authority sử dụng jq
    committee_json=$(echo "$committee_json" | jq \
      --arg name "$name" \
      --arg p2p_addr "$primary_to_primary_addr" \
      --arg w2p_addr "$worker_to_primary_addr" \
      --arg p2w_addr "$primary_to_worker_addr" \
      --arg tx_addr "$transactions_addr" \
      --arg w2w_addr "$worker_to_worker_addr" \
      '.authorities[$name] = {
        "primary": {
          "primary_to_primary": $p2p_addr,
          "worker_to_primary": $w2p_addr
        },
        "stake": 1,
        "workers": {
          "0": {
            "primary_to_worker": $p2w_addr,
            "transactions": $tx_addr,
            "worker_to_worker": $w2w_addr
          }
        }
      }'
    )
done
echo "$committee_json" | jq . > "$COMMITTEE_FILE"
echo "INFO: Configuration files generated successfully."

# --- Giai đoạn 3: Khởi chạy Nodes và Workers ---
echo ""
echo "--- Stage 3: Launching Nodes and Workers ---"
echo "INFO: Launching $NODES primaries and their respective workers..."
for i in $(seq 0 $((NODES-1))); do
    key_file="${key_files[$i]}";

    # Khởi chạy Primary
    primary_db_path="$BENCHMARK_DIR/db_primary_$i"; primary_log_file="$LOG_DIR/primary-$i.log"
    primary_cmd="$NODE_BINARY run --keys $key_file --committee $COMMITTEE_FILE --store $primary_db_path --parameters $PARAMETERS_FILE primary"
    full_primary_cmd_with_log="RUST_LOG=info $primary_cmd"
    tmux new -d -s "primary-$i" "sh -c '$full_primary_cmd_with_log 2> $primary_log_file || echo \"[FATAL] Primary process exited.\" >> $primary_log_file'"

    # Khởi chạy Worker 0 (theo cấu hình committee.json)
    worker_id=0
    worker_db_path="$BENCHMARK_DIR/db_worker_${i}_${worker_id}"; worker_log_file="$LOG_DIR/worker-${i}-${worker_id}.log"
    worker_cmd="$NODE_BINARY run --keys $key_file --committee $COMMITTEE_FILE --store $worker_db_path --parameters $PARAMETERS_FILE worker --id $worker_id"
    full_worker_cmd_with_log="RUST_LOG=info $worker_cmd"
    tmux new -d -s "worker-${i}-${worker_id}" "sh -c '$full_worker_cmd_with_log 2> $worker_log_file || echo \"[FATAL] Worker process exited.\" >> $worker_log_file'"

    # Khởi chạy Executor
    if [ "$i" -ne 0 ]; then
      executor_log_file="$LOG_DIR/executor-$i.log"
      executor_cmd="$EXECUTOR_BINARY --id $i"
      full_executor_cmd_with_log="RUST_LOG=info $executor_cmd"
      tmux new -d -s "executor-$i" "sh -c '$full_executor_cmd_with_log 2> $executor_log_file || echo \"[FATAL] Executor process exited.\" >> $executor_log_file'"
    fi
done

echo ""
echo "✅ Primaries, Workers, and Executors are now running. Please use 'tmux ls' to view sessions and 'tmux attach -t primary-0', 'tmux attach -t worker-0-0', or 'tmux attach -t executor-0' to inspect their output. To stop all nodes, run 'tmux kill-server'."