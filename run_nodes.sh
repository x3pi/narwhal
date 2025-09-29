#!/bin/bash

# ==============================================================================
# RUN SCRIPT (Launch Nodes + Workers + Executors)
# ==============================================================================

set -e

# --- Cấu hình Benchmark ---
NODES=3

# --- Đường dẫn ---
BENCHMARK_DIR="benchmark"
NODE_BINARY="./target/release/node"
EXECUTOR_BINARY="./go/bin/exetps"

LOG_DIR="$BENCHMARK_DIR/logs"
COMMITTEE_FILE="$BENCHMARK_DIR/.committee.json"
PARAMETERS_FILE="$BENCHMARK_DIR/.parameters.json"

echo "--- Stage 3: Launching Nodes and Workers ---"

# Kiểm tra file config
if [ ! -f "$COMMITTEE_FILE" ] || [ ! -f "$PARAMETERS_FILE" ]; then
    echo "LỖI: Thiếu $COMMITTEE_FILE hoặc $PARAMETERS_FILE. Hãy chạy setup.sh trước."
    exit 1
fi

# Kiểm tra key files
key_files=()
for i in $(seq 0 $((NODES-1))); do
    key_file="$BENCHMARK_DIR/.node-$i.json"
    if [ ! -f "$key_file" ]; then
        echo "LỖI: Thiếu $key_file. Hãy chạy setup.sh trước."
        exit 1
    fi
    key_files+=("$key_file")
done

# Khởi chạy các node
for i in $(seq 0 $((NODES-1))); do
    key_file="${key_files[$i]}"

    # Primary
    primary_db="$BENCHMARK_DIR/db_primary_$i"
    primary_log="$LOG_DIR/primary-$i.log"
    primary_cmd="$NODE_BINARY run --keys $key_file --committee $COMMITTEE_FILE --store $primary_db --parameters $PARAMETERS_FILE primary"
    tmux new -d -s "primary-$i" "sh -c 'RUST_LOG=info $primary_cmd 2> $primary_log || echo \"[FATAL] Primary exited\" >> $primary_log'"

    # Worker
    worker_id=0
    worker_db="$BENCHMARK_DIR/db_worker_${i}_${worker_id}"
    worker_log="$LOG_DIR/worker-${i}-${worker_id}.log"
    worker_cmd="$NODE_BINARY run --keys $key_file --committee $COMMITTEE_FILE --store $worker_db --parameters $PARAMETERS_FILE worker --id $worker_id"
    tmux new -d -s "worker-${i}-${worker_id}" "sh -c 'RUST_LOG=info $worker_cmd 2> $worker_log || echo \"[FATAL] Worker exited\" >> $worker_log'"

    # Executor (i > 0)
    if [ "$i" -ne 0 ]; then
        executor_log="$LOG_DIR/executor-$i.log"
        executor_cmd="$EXECUTOR_BINARY --id $i"
        tmux new -d -s "executor-$i" "sh -c 'RUST_LOG=info $executor_cmd 2> $executor_log || echo \"[FATAL] Executor exited\" >> $executor_log'"
    fi
done

echo ""
echo "✅ Primaries, Workers, Executors are running!"
echo "👉 Xem session: tmux ls"
echo "👉 Vào log: tmux attach -t primary-0 (hoặc worker-0-0, executor-1...)"
echo "👉 Dừng tất cả: tmux kill-server"
