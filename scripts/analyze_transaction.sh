#!/bin/bash
# Script để phân tích transaction lifecycle
# Usage: ./analyze_transaction.sh <tx_hash>

if [ $# -lt 1 ]; then
    echo "Usage: $0 <tx_hash>"
    echo "Example: $0 fb92b2e528153d4c52ec5ab4990dc4c09a096f79761bae2ca1b15fb5f67a2bf5"
    exit 1
fi

TX_HASH="$1"
LOG_DIR="benchmark/logs"

echo "=========================================="
echo "Phân tích Transaction: $TX_HASH"
echo "=========================================="
echo ""

# 1. Tìm transaction được nhận
echo "1. [TX RECEIVED] - Transaction được nhận:"
grep -h "$TX_HASH" "$LOG_DIR"/*.log | grep "TX RECEIVED" | head -1
echo ""

# 2. Tìm transaction được thêm vào batch
echo "2. [TX TO BATCH] - Transaction được thêm vào batch:"
TX_TO_BATCH=$(grep -h "$TX_HASH" "$LOG_DIR"/*.log | grep "TX TO BATCH" | head -1)
echo "$TX_TO_BATCH"
BATCH_ID=$(echo "$TX_TO_BATCH" | grep -oE '"batch_id":"[^"]*"' | cut -d'"' -f4)
echo "   Batch ID: $BATCH_ID"
echo ""

if [ -z "$BATCH_ID" ]; then
    echo "❌ ERROR: Không tìm thấy batch ID. Transaction có thể chưa được thêm vào batch."
    exit 1
fi

# 3. Tìm batch được seal
echo "3. [BATCH SEAL] - Batch được seal:"
grep -h "$BATCH_ID" "$LOG_DIR"/*.log | grep -E "BATCH SEAL|BATCH.*CREATED" | head -5
echo ""

# 4. Tìm batch được broadcast
echo "4. [BATCH BROADCAST] - Batch được broadcast đến workers:"
grep -h "$BATCH_ID" "$LOG_DIR"/*.log | grep "BATCH BROADCAST" | head -5
echo ""

# 5. Tìm batch được gửi đến primary
echo "5. [BATCH SENT TO PRIMARY] - Batch được gửi đến primary:"
grep -h "$BATCH_ID" "$LOG_DIR"/*.log | grep "BATCH SENT TO PRIMARY" | head -5
echo ""

# 6. Tìm batch được thêm vào queue
echo "6. [BATCH ADDED] - Batch được thêm vào proposer queue:"
grep -h "$BATCH_ID" "$LOG_DIR"/primary-*.log | grep "BATCH.*ADDED" | head -5
echo ""

# 7. Tìm batch được collect cho header
echo "7. [BATCH COLLECTED] - Batch được collect cho header:"
grep -h "$BATCH_ID" "$LOG_DIR"/primary-*.log | grep "BATCH.*COLLECTED" | head -5
echo ""

# 8. Tìm batch được commit
echo "8. [BATCH COMMITTED] - Batch được commit:"
grep -h "$BATCH_ID" "$LOG_DIR"/primary-*.log | grep "BATCH.*COMMITTED" | head -5
echo ""

# 9. Tìm batch được gửi đến UDS
echo "9. [BATCH SENT TO UDS] - Batch được gửi đến UDS:"
grep -h "$BATCH_ID" "$LOG_DIR"/*.log | grep "BATCH.*SENT TO UDS" | head -5
echo ""

# 10. Tìm transaction được gửi đến UDS
echo "10. [TX SENT TO UDS] - Transaction được gửi đến UDS:"
grep -h "$TX_HASH" "$LOG_DIR"/*.log | grep "TX SENT TO UDS" | head -5
echo ""

# Tóm tắt
echo "=========================================="
echo "TÓM TẮT:"
echo "=========================================="

if echo "$TX_TO_BATCH" | grep -q "TX TO BATCH"; then
    echo "✅ Transaction được thêm vào batch: $BATCH_ID"
else
    echo "❌ Transaction KHÔNG được thêm vào batch"
fi

if grep -h "$BATCH_ID" "$LOG_DIR"/*.log | grep -q "BATCH.*CREATED"; then
    echo "✅ Batch được tạo"
else
    echo "❌ Batch KHÔNG được tạo (chưa seal)"
fi

if grep -h "$BATCH_ID" "$LOG_DIR"/*.log | grep -q "BATCH.*COMMITTED"; then
    echo "✅ Batch được commit"
else
    echo "❌ Batch KHÔNG được commit"
fi

if grep -h "$TX_HASH" "$LOG_DIR"/*.log | grep -q "TX SENT TO UDS"; then
    echo "✅ Transaction được gửi đến UDS"
else
    echo "❌ Transaction KHÔNG được gửi đến UDS"
fi

echo ""

