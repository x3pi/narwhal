#!/bin/bash

# ==============================================================================
# SCRIPT CONFIGURATION
# ==============================================================================
# Dừng script nếu có bất kỳ lỗi nào xảy ra
set -e

# Cấu hình Benchmark (giống hệt trong run_benchmark.sh)
NODES=3
RATE=60000
TX_SIZE=600
DURATION=30

# --- Đường dẫn ---
BENCHMARK_DIR="benchmark"
CLIENT_BINARY="./target/release/benchmark_client"

# --- Đường dẫn file cấu hình ---
LOG_DIR="$BENCHMARK_DIR/logs"
COMMITTEE_FILE="$BENCHMARK_DIR/.committee.json"

# ==============================================================================
# SCRIPT EXECUTION
# ==============================================================================

# Kiểm tra sự tồn tại của các công cụ và file thực thi
if ! command -v jq &> /dev/null; then
    echo "LỖI: Lệnh 'jq' không tồn tại. Vui lòng cài đặt: sudo apt-get install jq"
    exit 1
fi
if ! command -v tmux &> /dev/null; then
    echo "LỖI: Lệnh 'tmux' không tồn tại. Vui lòng cài đặt: sudo apt-get install tmux"
    exit 1
fi
if [ ! -f "$CLIENT_BINARY" ]; then
    echo "LỖI: Không tìm thấy file thực thi client tại '$CLIENT_BINARY'."
    exit 1
fi
if [ ! -x "$CLIENT_BINARY" ]; then
    echo "LỖI: File '$CLIENT_BINARY' không có quyền thực thi. Hãy chạy: chmod +x $CLIENT_BINARY"
    exit 1
fi

# Khởi chạy các client
echo "INFO: Launching $NODES clients..."
rate_share=$((RATE / NODES))
for i in $(seq 0 $((NODES-1))); do
    # Lấy tên (public key) của node hiện tại
    key_file="$BENCHMARK_DIR/.node-$i.json"
    node_name=$(jq -r '.name' "$key_file")
    
    # Đọc địa chỉ 'transactions' từ committee.json cho worker 0 của node này
    addr=$(jq -r --arg name "$node_name" '.authorities[$name].workers."0".transactions' "$COMMITTEE_FILE")
    
    log_file="$LOG_DIR/client-$i.log"
    full_cmd="$CLIENT_BINARY $addr --size $TX_SIZE --rate $rate_share"
    
    # Cưỡng bức ghi log và thêm cơ chế chẩn đoán lỗi
    tmux new -d -s "client-$i" "sh -c '$full_cmd 2> $log_file || echo \"[FATAL] Client process exited.\" >> $log_file'"
done
echo "INFO: Clients launched successfully in tmux sessions."