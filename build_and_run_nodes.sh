#!/bin/bash

# ==============================================================================
# RUN SCRIPT (v7 - Final with Process Order Fix)
# ==============================================================================

set -e

# --- Cấu hình ---
NODES=$(jq '.authorities | length' < benchmark/.committee.json)

# --- Đường dẫn ---
BENCHMARK_DIR="benchmark"
NODE_BINARY="./target/release/node"
EXECUTOR_BINARY="./go/bin/exetps"

# Danh sách prefix phiên tmux cần dọn dẹp
TMUX_SESSION_PREFIXES=("primary-" "worker-" "executor-")

cleanup_tmux_sessions() {
    if ! command -v tmux &> /dev/null; then
        return
    fi

    local sessions
    if ! sessions=$(tmux list-sessions -F '#{session_name}' 2>/dev/null); then
        return
    fi

    while IFS= read -r session_name; do
        for prefix in "${TMUX_SESSION_PREFIXES[@]}"; do
            if [[ "$session_name" == "$prefix"* ]]; then
                tmux kill-session -t "$session_name" 2>/dev/null || true
                break
            fi
        done
    done <<< "$sessions"
}

LOG_DIR="$BENCHMARK_DIR/logs"
COMMITTEE_FILE="$BENCHMARK_DIR/committlee.json"
PARAMETERS_FILE="$BENCHMARK_DIR/.parameters.json"

echo "--- 🛠️ Stage 0: Build ---"
cargo build --release --features benchmark
echo "✅ Build done!"

echo "--- 🧹 Stage 1: Cleanup ---"
pkill -f "$NODE_BINARY" || true
pkill -f "$EXECUTOR_BINARY" || true
cleanup_tmux_sessions
sleep 1
# SỬA LỖI: Xóa cả các file socket cũ trong /tmp
rm -rf "$LOG_DIR" "$BENCHMARK_DIR"/db-* /tmp/executor*.sock
mkdir -p "$LOG_DIR"
echo "✅ Cleanup done!"


echo "🚀 Launching Nodes, Workers, and Executors in tmux..."

# --- Lấy tên của tất cả các authority ---
AUTHORITY_NAMES=($(jq -r '.authorities | keys[]' < "$COMMITTEE_FILE"))

# --- Khởi chạy các node trong các session tmux ---
for i in $(seq 0 $((NODES-1))); do
    key_file="$BENCHMARK_DIR/node-$i.json"
    AUTHORITY_NAME=${AUTHORITY_NAMES[$i]}
    # --- SỬA LỖI: Thêm một khoảng nghỉ ngắn để executor tạo socket ---
    sleep 0.2

    # --- Khởi chạy Primary ---
    primary_db_path="$BENCHMARK_DIR/db-primary-$i"
    primary_log_file="$LOG_DIR/primary-$i.log"
    primary_cmd="$NODE_BINARY run --keys '$key_file' --committee '$COMMITTEE_FILE' --parameters '$PARAMETERS_FILE' --store '$primary_db_path' primary"
    tmux new -d -s "primary-$i" "RUST_LOG=info $primary_cmd > '$primary_log_file' 2>&1"
    
    # --- Khởi chạy tất cả Workers cho node này ---
    WORKERS_PER_NODE=$(jq ".authorities.\"$AUTHORITY_NAME\".workers | length" < "$COMMITTEE_FILE")
    for j in $(seq 0 $((WORKERS_PER_NODE-1))); do
        worker_db_path="$BENCHMARK_DIR/db-worker-$i-$j"
        worker_log_file="$LOG_DIR/worker-$i-$j.log"
        worker_cmd="$NODE_BINARY run --keys '$key_file' --committee '$COMMITTEE_FILE' --parameters '$PARAMETERS_FILE' --store '$worker_db_path' worker --id $j"
        tmux new -d -s "worker-$i-$j" "RUST_LOG=info $worker_cmd > '$worker_log_file' 2>&1"
    done
done

echo ""
echo "⏳ Waiting 5 seconds for processes to boot..."
sleep 5

echo "--- 🔍 Checking Status ---"
tmux ls

echo ""
echo "✅ All processes (Primaries, Workers, Executors) are launched in tmux."
echo "   - To view sessions: tmux ls"
echo "   - To attach to primary-0 session: tmux a -t primary-0"
echo "   - To monitor primary-0 log: tail -f $LOG_DIR/primary-0.log"
echo "   - To monitor executor-0 log: tail -f $LOG_DIR/executor-0.log"
echo "   - Use 'tmux kill-session -t <session_name>' để dừng từng tiến trình mà không ảnh hưởng các phiên tmux khác."