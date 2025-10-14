#!/bin/bash

# ==============================================================================
# KILL NODE SCRIPT (Dừng Primary + Tất cả Workers + Executor cho một ID cụ thể)
# Phiên bản đã được cập nhật để nhất quán với run_node_id.sh
# ==============================================================================

set -e

# --- Nhận và kiểm tra tham số node id ---
if [ -z "$1" ]; then
    echo "❌ Lỗi: Vui lòng cung cấp một node ID."
    echo "   Ví dụ: ./kill_node_id.sh 0"
    exit 1
fi

if ! [[ "$1" =~ ^[0-9]+$ ]]; then
    echo "❌ Lỗi: Node ID '$1' phải là một số nguyên không âm."
    exit 1
fi
NODE_ID=$1

# --- Đường dẫn đến file cấu hình ---
COMMITTEE_FILE="benchmark/.committee.json"

if [ ! -f "$COMMITTEE_FILE" ]; then
    echo "❌ Lỗi: Không tìm thấy file cấu hình: $COMMITTEE_FILE"
    echo "   Hãy đảm bảo bạn đang chạy script từ thư mục gốc của dự án."
    exit 1
fi

# --- Đọc cấu hình để xác định số lượng workers ---
AUTHORITY_NAME=$(jq -r ".authorities | keys[$NODE_ID]" < "$COMMITTEE_FILE")
if [ "$AUTHORITY_NAME" == "null" ]; then
    echo "⚠️ Cảnh báo: Không tìm thấy authority cho Node ID '$NODE_ID'. Sẽ chỉ cố gắng dừng các session theo tên."
    WORKERS_PER_NODE=1 # Giả định có ít nhất 1 worker để thử dừng
else
    WORKERS_PER_NODE=$(jq ".authorities.\"$AUTHORITY_NAME\".workers | length" < "$COMMITTEE_FILE")
fi

echo "INFO: Đang gửi lệnh dừng đến các session của Node ID: $NODE_ID (dự kiến có $WORKERS_PER_NODE worker(s))..."

# --- Định nghĩa tên các session ---
PRIMARY_SESSION="primary-$NODE_ID"
EXECUTOR_SESSION="executor-$NODE_ID"

# --- Dừng các session tmux ---
# Sử dụng '2>/dev/null || true' để script không báo lỗi và thoát nếu session không tồn tại.

echo " > Dừng session Executor: $EXECUTOR_SESSION..."
tmux kill-session -t "$EXECUTOR_SESSION" 2>/dev/null || true

echo " > Dừng session Primary: $PRIMARY_SESSION..."
tmux kill-session -t "$PRIMARY_SESSION" 2>/dev/null || true

# Lặp qua và dừng tất cả các session worker
for j in $(seq 0 $((WORKERS_PER_NODE-1))); do
    WORKER_SESSION="worker-${NODE_ID}-${j}"
    echo " > Dừng session Worker: $WORKER_SESSION..."
    tmux kill-session -t "$WORKER_SESSION" 2>/dev/null || true
done

echo ""
echo "✅ Hoàn tất! Đã gửi lệnh dừng cho tất cả các session liên quan đến Node ID $NODE_ID."