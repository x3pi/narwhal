#!/bin/bash

# ==============================================================================
# KILL NODE SCRIPT (Dừng Primary + Worker + Executor cho một ID cụ thể)
# ==============================================================================

set -e

# --- Nhận và kiểm tra tham số node id ---
if [ -z "$1" ]; then
    echo "❌ Lỗi: Vui lòng cung cấp một node ID."
    echo "   Ví dụ: ./kill_node.sh 0"
    exit 1
fi

if ! [[ "$1" =~ ^[0-9]+$ ]]; then
    echo "❌ Lỗi: Node ID '$1' phải là một số nguyên không âm."
    exit 1
fi

NODE_ID=$1
# Worker instance ID được gán cứng là 0, theo các script run.sh
WORKER_INSTANCE_ID=0

# --- Định nghĩa tên các session ---
PRIMARY_SESSION="primary-$NODE_ID"
WORKER_SESSION="worker-${NODE_ID}-${WORKER_INSTANCE_ID}"
# EXECUTOR_SESSION="executor-$NODE_ID"

echo "INFO: Đang gửi lệnh dừng đến các session của Node ID: $NODE_ID..."

# --- Dừng các session tmux ---
# Sử dụng '|| true' để script không báo lỗi và thoát nếu session không tồn tại
# (ví dụ: nó đã bị dừng trước đó).

echo " > Dừng session $PRIMARY_SESSION..."
tmux kill-session -t "$PRIMARY_SESSION" 2>/dev/null || true

echo " > Dừng session $WORKER_SESSION..."
tmux kill-session -t "$WORKER_SESSION" 2>/dev/null || true

# echo " > Dừng session $EXECUTOR_SESSION..."
# tmux kill-session -t "$EXECUTOR_SESSION" 2>/dev/null || true

echo ""
echo "✅ Hoàn tất! Đã gửi lệnh dừng cho tất cả các session của Node ID $NODE_ID."