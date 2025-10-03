#!/bin/bash

# ==============================================================================
# RUN SCRIPT (Launch Nodes + Workers + Executors)
# ==============================================================================

set -e

# --- Cấu hình ---
NODES=5

# --- Đường dẫn ---
BENCHMARK_DIR="benchmark"
NODE_BINARY="./target/release/node"
EXECUTOR_BINARY="./go/bin/exetps"

LOG_DIR="$BENCHMARK_DIR/logs"
COMMITTEE_FILE="$BENCHMARK_DIR/.committee.json"
PARAMETERS_FILE="$BENCHMARK_DIR/.parameters.json"

echo "--- Launching Nodes, Workers, and Executors ---"

# --- Kiểm tra các file cấu hình cần thiết ---
if [ ! -f "$COMMITTEE_FILE" ] || [ ! -f "$PARAMETERS_FILE" ]; then
    echo "LỖI: Thiếu $COMMITTEE_FILE hoặc $PARAMETERS_FILE. Hãy chạy script setup trước."
    exit 1
fi

key_files=()
for i in $(seq 0 $((NODES-1))); do
    key_file="$BENCHMARK_DIR/.node-$i.json"
    if [ ! -f "$key_file" ]; then
        echo "LỖI: Thiếu $key_file. Hãy chạy script setup trước."
        exit 1
    fi
    key_files+=("$key_file")
done

# --- Khởi chạy các node trong các session tmux ---
for i in $(seq 0 $((NODES-1))); do
    key_file="${key_files[$i]}"

    # Primary
    primary_db="$BENCHMARK_DIR/db_primary_$i"
    primary_log="$LOG_DIR/primary-$i.log"
    primary_cmd="$NODE_BINARY run --keys \"$key_file\" --committee \"$COMMITTEE_FILE\" --store \"$primary_db\" --parameters \"$PARAMETERS_FILE\" primary"
    # SỬA LỖI: Chuyển hướng cả stdout và stderr vào file log
    tmux new -d -s "primary-$i" "sh -c 'RUST_LOG=info $primary_cmd > \"$primary_log\" 2>&1 || echo \"[FATAL] Primary exited\" >> \"$primary_log\"'"

    # Worker
    worker_id=0
    worker_db="$BENCHMARK_DIR/db_worker_${i}_${worker_id}"
    worker_log="$LOG_DIR/worker-${i}-${worker_id}.log"
    worker_cmd="$NODE_BINARY run --keys \"$key_file\" --committee \"$COMMITTEE_FILE\" --store \"$worker_db\" --parameters \"$PARAMETERS_FILE\" worker --id $worker_id"
    # SỬA LỖI: Chuyển hướng cả stdout và stderr vào file log
    tmux new -d -s "worker-${i}-${worker_id}" "sh -c 'RUST_LOG=info $worker_cmd > \"$worker_log\" 2>&1 || echo \"[FATAL] Worker exited\" >> \"$worker_log\"'"

    # Executor
    executor_log="$LOG_DIR/executor-$i.log"
    executor_cmd="$EXECUTOR_BINARY --id $i"
    # SỬA LỖI: Chuyển hướng cả stdout và stderr vào file log
    tmux new -d -s "executor-$i" "sh -c '$executor_cmd > \"$executor_log\" 2>&1 || echo \"[FATAL] Executor exited\" >> \"$executor_log\"'"
done

echo ""
echo "INFO: Chờ 3 giây để các tiến trình khởi động..."
sleep 3
echo "✅ Tất cả các tiến trình (Primaries, Workers, Executors) đang chạy."
echo "   - Xem các session: tmux ls"
echo "   - Theo dõi log của primary 0: tail -f $LOG_DIR/primary-0.log"
echo "   - Dừng tất cả: tmux kill-server"