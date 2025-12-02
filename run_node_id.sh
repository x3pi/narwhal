#!/bin/bash

# ==============================================================================
# RUN NODE SCRIPT (Chạy một node đầy đủ: Primary + Worker(s) + Executor)
# Phiên bản đã được cập nhật để nhất quán với run_nodes.sh
# ==============================================================================

set -e

# --- Nhận và kiểm tra tham số node id ---
if [ -z "$1" ]; then
    echo "❌ Lỗi: Vui lòng cung cấp một node ID."
    echo "   Ví dụ: ./run_node_id.sh 0"
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
COMMITTEE_FILE="$BENCHMARK_DIR/committlee.json"
PARAMETERS_FILE="$BENCHMARK_DIR/.parameters.json"
KEY_FILE="$BENCHMARK_DIR/node-$NODE_ID.json"

# --- Kiểm tra các file cần thiết ---
for f in "$NODE_BINARY" "$EXECUTOR_BINARY" "$KEY_FILE" "$COMMITTEE_FILE" "$PARAMETERS_FILE"; do
    if [ ! -f "$f" ]; then
        echo "❌ Lỗi: Không tìm thấy file cần thiết: $f"
        echo "   Hãy đảm bảo bạn đã chạy script setup và biên dịch code thành công."
        exit 1
    fi
done

mkdir -p "$LOG_DIR"

# --- Lấy thông tin cấu hình từ committee file ---
# Lấy tên của authority dựa trên NODE_ID (thứ tự trong mảng keys)
AUTHORITY_NAME=$(jq -r ".authorities | keys[$NODE_ID]" < "$COMMITTEE_FILE")
if [ "$AUTHORITY_NAME" == "null" ]; then
    echo "❌ Lỗi: Không tìm thấy authority cho Node ID '$NODE_ID' trong file $COMMITTEE_FILE."
    exit 1
fi

# Đếm số lượng workers được cấu hình cho authority này
WORKERS_PER_NODE=$(jq ".authorities.\"$AUTHORITY_NAME\".workers | length" < "$COMMITTEE_FILE")
echo "ℹ️ Thông tin Node $NODE_ID: Tên Authority '$AUTHORITY_NAME', Số Workers: $WORKERS_PER_NODE"
echo ""


# --- SỬA LỖI: Khởi chạy Executor TRƯỚC ---
# executor_log="$LOG_DIR/executor-$NODE_ID.log"
# executor_cmd="$EXECUTOR_BINARY --id $NODE_ID"

# echo "🚀 Khởi động Executor-$NODE_ID..."
# tmux new -d -s "executor-$NODE_ID" "sh -c '$executor_cmd > \"$executor_log\" 2>&1 || echo \"[FATAL] Executor exited\" >> \"$executor_log\"'"

# # --- SỬA LỖI: Thêm một khoảng nghỉ ngắn để executor tạo socket ---
# sleep 0.2


# --- Khởi chạy Primary ---
# SỬA ĐỔI: Tên DB nhất quán với run_nodes.sh
primary_db="$BENCHMARK_DIR/db-primary-$NODE_ID"
primary_log="$LOG_DIR/primary-$NODE_ID.log"
primary_cmd="$NODE_BINARY run --keys \"$KEY_FILE\" --committee \"$COMMITTEE_FILE\" --store \"$primary_db\" --parameters \"$PARAMETERS_FILE\" primary"

echo "🚀 Khởi động Primary-$NODE_ID..."
tmux new -d -s "primary-$NODE_ID" "sh -c 'RUST_LOG=info $primary_cmd > \"$primary_log\" 2>&1 || echo \"[FATAL] Primary exited\" >> \"$primary_log\"'"


# --- Khởi chạy TẤT CẢ Workers cho node này ---
for j in $(seq 0 $((WORKERS_PER_NODE-1))); do
    # SỬA ĐỔI: Tên DB nhất quán với run_nodes.sh
    worker_db="$BENCHMARK_DIR/db-worker-${NODE_ID}-${j}"
    worker_log="$LOG_DIR/worker-${NODE_ID}-${j}.log"
    worker_cmd="$NODE_BINARY run --keys \"$KEY_FILE\" --committee \"$COMMITTEE_FILE\" --store \"$worker_db\" --parameters \"$PARAMETERS_FILE\" worker --id $j"

    echo "🚀 Khởi động Worker-${NODE_ID}-${j}..."
    tmux new -d -s "worker-${NODE_ID}-${j}" "sh -c 'RUST_LOG=info $worker_cmd > \"$worker_log\" 2>&1 || echo \"[FATAL] Worker exited\" >> \"$worker_log\"'"
done


# --- Xây dựng lệnh dừng node ---
kill_cmd="tmux kill-session -t executor-$NODE_ID && tmux kill-session -t primary-$NODE_ID"
log_files_info=""
for j in $(seq 0 $((WORKERS_PER_NODE-1))); do
    kill_cmd="$kill_cmd && tmux kill-session -t worker-${NODE_ID}-${j}"
    log_files_info+="   - Theo dõi log của worker-$j:  tail -f $LOG_DIR/worker-${NODE_ID}-${j}.log\n"
done


echo ""
echo "✅ Node $NODE_ID và các thành phần liên quan đã được khởi chạy."
echo "   - Xem các session đang chạy: tmux ls"
echo "   - Theo dõi log của primary:   tail -f $primary_log"
echo -e "$log_files_info" # Dùng -e để diễn giải ký tự xuống dòng \n
echo "   - Theo dõi log của executor:  tail -f $executor_log"
echo ""
echo "   - Để dừng node này, chạy lệnh:"
echo "     $kill_cmd"