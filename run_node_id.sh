#!/bin/bash

# ==============================================================================
# RUN NODE SCRIPT (Primary + Worker + Executor nếu node_id > 0)
# ==============================================================================

set -e

# --- Nhận tham số node id ---
if [ -z "$1" ]; then
    echo "❌ Vui lòng truyền tham số node_id (ví dụ: ./run_node.sh 0)"
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

# --- Kiểm tra file cần thiết ---
for f in "$NODE_BINARY" "$KEY_FILE" "$COMMITTEE_FILE" "$PARAMETERS_FILE"; do
    if [ ! -f "$f" ]; then
        echo "❌ Thiếu file: $f"
        echo "Hãy chạy ./setup.sh trước khi chạy script này."
        exit 1
    fi
done

mkdir -p "$LOG_DIR"

# --- Primary ---
primary_db="$BENCHMARK_DIR/db_primary_$NODE_ID"
primary_log="$LOG_DIR/primary-$NODE_ID.log"
primary_cmd="$NODE_BINARY run --keys $KEY_FILE --committee $COMMITTEE_FILE --store $primary_db --parameters $PARAMETERS_FILE primary"

echo "🚀 Khởi động Primary-$NODE_ID..."
tmux new -d -s "primary-$NODE_ID" "sh -c 'RUST_LOG=info $primary_cmd 2> $primary_log || echo \"[FATAL] Primary exited\" >> $primary_log'"

# --- Worker-<node>-0 ---
worker_id=0
worker_db="$BENCHMARK_DIR/db_worker_${NODE_ID}_${worker_id}"
worker_log="$LOG_DIR/worker-${NODE_ID}-${worker_id}.log"
worker_cmd="$NODE_BINARY run --keys $KEY_FILE --committee $COMMITTEE_FILE --store $worker_db --parameters $PARAMETERS_FILE worker --id $worker_id"

echo "🚀 Khởi động Worker-${NODE_ID}-${worker_id}..."
tmux new -d -s "worker-${NODE_ID}-${worker_id}" "sh -c 'RUST_LOG=info $worker_cmd 2> $worker_log || echo \"[FATAL] Worker exited\" >> $worker_log'"

# --- Executor (chỉ chạy nếu node_id > 0) --- đã comment để node cũng chạy
# if [ "$NODE_ID" -ne 0 ]; then
executor_log="$LOG_DIR/executor-$NODE_ID.log"
executor_cmd="$EXECUTOR_BINARY --id $NODE_ID"

echo "🚀 Khởi động Executor-$NODE_ID..."
tmux new -d -s "executor-$NODE_ID" "sh -c 'RUST_LOG=info $executor_cmd 2> $executor_log || echo \"[FATAL] Executor exited\" >> $executor_log'"
# fi

echo ""
echo "✅ Node $NODE_ID đã chạy!"
echo "👉 Xem session: tmux ls"
echo "👉 Vào log: tmux attach -t primary-$NODE_ID, tmux attach -t worker-${NODE_ID}-0"
if [ "$NODE_ID" -ne 0 ]; then
    echo "👉 Executor log: tmux attach -t executor-$NODE_ID"
fi
echo "👉 Dừng node: tmux kill-session -t primary-$NODE_ID && tmux kill-session -t worker-${NODE_ID}-0"
if [ "$NODE_ID" -ne 0 ]; then
    echo "   và tmux kill-session -t executor-$NODE_ID"
fi
