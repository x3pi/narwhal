#!/bin/bash

# ==============================================================================
# RUN NODE SCRIPT (Chạy một node đầy đủ: Primary + Worker + Executor)
# ==============================================================================

set -e

# --- Nhận và kiểm tra tham số node id ---
if [ -z "$1" ]; then
    echo "❌ Lỗi: Vui lòng cung cấp một node ID."
    echo "   Ví dụ: ./run_node.sh 0"
    exit 1
fi

# Kiểm tra xem tham số có phải là số nguyên không
if ! [[ "$1" =~ ^[0-9]+$ ]]; then
    echo "❌ Lỗi: Node ID '$1' phải là một số nguyên không âm."
    exit 1
fi
NODE_ID=$1

# --- Đường dẫn ---
BENCHMARK_DIR="benchmark"
NODE_BINARY="./target/release/node"
EXECUTOR_BINARY="./go/bin/exetps"

LOG_DIR="$BENCHMARK_DIR/logs"
COMMITTEE_FILE="$BENCHMARK_DIR/.committee.json"
PARAMETERS_FILE="$BENCHMARK_DIR/.parameters.json"
KEY_FILE="$BENCHMARK_DIR/.node-$NODE_ID.json"

# --- Kiểm tra các file cần thiết ---
for f in "$NODE_BINARY" "$EXECUTOR_BINARY" "$KEY_FILE" "$COMMITTEE_FILE" "$PARAMETERS_FILE"; do
    if [ ! -f "$f" ]; then
        echo "❌ Lỗi: Không tìm thấy file cần thiết: $f"
        echo "   Hãy đảm bảo bạn đã chạy script setup và biên dịch code thành công."
        exit 1
    fi
done

mkdir -p "$LOG_DIR"

# --- Khởi chạy Primary ---
primary_db="$BENCHMARK_DIR/db_primary_$NODE_ID"
primary_log="$LOG_DIR/primary-$NODE_ID.log"
primary_cmd="$NODE_BINARY run --keys \"$KEY_FILE\" --committee \"$COMMITTEE_FILE\" --store \"$primary_db\" --parameters \"$PARAMETERS_FILE\" primary"

echo "🚀 Khởi động Primary-$NODE_ID..."
# SỬA LỖI: Chuyển hướng cả stdout và stderr vào file log
tmux new -d -s "primary-$NODE_ID" "sh -c 'RUST_LOG=info $primary_cmd > \"$primary_log\" 2>&1 || echo \"[FATAL] Primary exited\" >> \"$primary_log\"'"

# --- Khởi chạy Worker ---
worker_id=0
worker_db="$BENCHMARK_DIR/db_worker_${NODE_ID}_${worker_id}"
worker_log="$LOG_DIR/worker-${NODE_ID}-${worker_id}.log"
worker_cmd="$NODE_BINARY run --keys \"$KEY_FILE\" --committee \"$COMMITTEE_FILE\" --store \"$worker_db\" --parameters \"$PARAMETERS_FILE\" worker --id $worker_id"

echo "🚀 Khởi động Worker-${NODE_ID}-${worker_id}..."
# SỬA LỖI: Chuyển hướng cả stdout và stderr vào file log
tmux new -d -s "worker-${NODE_ID}-${worker_id}" "sh -c 'RUST_LOG=info $worker_cmd > \"$worker_log\" 2>&1 || echo \"[FATAL] Worker exited\" >> \"$worker_log\"'"

# --- Khởi chạy Executor ---
executor_log="$LOG_DIR/executor-$NODE_ID.log"
executor_cmd="$EXECUTOR_BINARY --id $NODE_ID"

echo "🚀 Khởi động Executor-$NODE_ID..."
# SỬA LỖI: Chuyển hướng cả stdout và stderr vào file log
tmux new -d -s "executor-$NODE_ID" "sh -c '$executor_cmd > \"$executor_log\" 2>&1 || echo \"[FATAL] Executor exited\" >> \"$executor_log\"'"

echo ""
echo "✅ Node $NODE_ID và các thành phần liên quan đã được khởi chạy."
echo "   - Xem các session đang chạy: tmux ls"
echo "   - Theo dõi log của primary:   tail -f $primary_log"
echo "   - Theo dõi log của worker:    tail -f $worker_log"
echo "   - Theo dõi log của executor:  tail -f $executor_log"
echo ""
echo "   - Để dừng node này, chạy lệnh:"
echo "     tmux kill-session -t primary-$NODE_ID && tmux kill-session -t worker-${NODE_ID}-${worker_id} && tmux kill-session -t executor-$NODE_ID"