#!/bin/bash
# Script nâng cao để filter log và loại bỏ log của batch đã được gửi đến UDS
# Script này sẽ:
# 1. Tìm tất cả batch IDs đã được gửi đến UDS
# 2. Loại bỏ tất cả log liên quan đến batch đó TRƯỚC khi batch được gửi đến UDS
# 3. Giữ lại log [BATCH SENT TO UDS] để đánh dấu batch đã hoàn thành
# Usage: ./filter_batch_logs_advanced.sh <input_log_file> <output_log_file>

if [ $# -lt 2 ]; then
    echo "Usage: $0 <input_log_file> <output_log_file>"
    echo "Example: $0 benchmark/logs/worker-0-0.log benchmark/logs/worker-0-0-filtered.log"
    exit 1
fi

INPUT_FILE="$1"
OUTPUT_FILE="$2"

if [ ! -f "$INPUT_FILE" ]; then
    echo "Error: Input file $INPUT_FILE does not exist"
    exit 1
fi

# Tạo file tạm
TEMP_BATCHES=$(mktemp)
TEMP_OUTPUT=$(mktemp)
TEMP_LINES=$(mktemp)

# Extract tất cả batch IDs đã được gửi đến UDS từ log
# Tìm pattern: [BATCH SENT TO UDS] hoặc [BATCH TO UDS] với batch_id
echo "Extracting batch IDs sent to UDS..."

# Với JSON log format
if grep -q '"target":"narwhal_audit"' "$INPUT_FILE" 2>/dev/null; then
    # JSON format: tìm batch_id trong log [BATCH SENT TO UDS]
    grep -E '\[BATCH SENT TO UDS\]|\[BATCH TO UDS\]' "$INPUT_FILE" | \
        grep -oE '"batch_id":"[^"]*"' | \
        sed 's/"batch_id":"\([^"]*\)"/\1/' | \
        sort -u > "$TEMP_BATCHES"
    
    # Nếu không tìm thấy batch_id field, thử tìm trong message
    if [ ! -s "$TEMP_BATCHES" ]; then
        grep -E '\[BATCH SENT TO UDS\]|\[BATCH TO UDS\]' "$INPUT_FILE" | \
            grep -oE 'batch [^ ]+' | \
            sed 's/batch //' | \
            sort -u > "$TEMP_BATCHES"
    fi
else
    # Plain text format
    grep -E '\[BATCH SENT TO UDS\]|\[BATCH TO UDS\]' "$INPUT_FILE" | \
        grep -oE 'batch [^ ]+' | \
        sed 's/batch //' | \
        sort -u > "$TEMP_BATCHES"
fi

BATCH_COUNT=$(wc -l < "$TEMP_BATCHES")
echo "Found $BATCH_COUNT batches sent to UDS"

if [ "$BATCH_COUNT" -eq 0 ]; then
    echo "No batches sent to UDS found. Copying original file..."
    cp "$INPUT_FILE" "$OUTPUT_FILE"
    rm -f "$TEMP_BATCHES" "$TEMP_OUTPUT" "$TEMP_LINES"
    exit 0
fi

# Đọc từng batch ID và loại bỏ log của nó
BATCH_INDEX=0
while IFS= read -r batch_id; do
    if [ -n "$batch_id" ]; then
        BATCH_INDEX=$((BATCH_INDEX + 1))
        echo "Processing batch $BATCH_INDEX/$BATCH_COUNT: $batch_id"
        
        # Tìm dòng số của log [BATCH SENT TO UDS] cho batch này
        UDS_LINE=$(grep -n "$batch_id" "$INPUT_FILE" | grep -E '\[BATCH SENT TO UDS\]|\[BATCH TO UDS\]' | head -1 | cut -d: -f1)
        
        if [ -n "$UDS_LINE" ]; then
            # Loại bỏ tất cả log chứa batch_id TRƯỚC dòng UDS_LINE
            # Nhưng giữ lại log [BATCH SENT TO UDS]
            awk -v batch_id="$batch_id" -v uds_line="$UDS_LINE" '
                {
                    # Nếu dòng chứa batch_id và không phải là [BATCH SENT TO UDS] và trước UDS_LINE
                    if (NR < uds_line && $0 ~ batch_id && $0 !~ /\[BATCH SENT TO UDS\]|\[BATCH TO UDS\]/) {
                        # Bỏ qua dòng này
                        next
                    }
                    # Giữ lại tất cả các dòng khác
                    print
                }
            ' "$INPUT_FILE" > "$TEMP_OUTPUT"
            
            mv "$TEMP_OUTPUT" "$TEMP_LINES"
            cp "$TEMP_LINES" "$INPUT_FILE"
        fi
    fi
done < "$TEMP_BATCHES"

# Copy kết quả cuối cùng
if [ -f "$TEMP_LINES" ]; then
    cp "$TEMP_LINES" "$OUTPUT_FILE"
else
    cp "$INPUT_FILE" "$OUTPUT_FILE"
fi

# Cleanup
rm -f "$TEMP_BATCHES" "$TEMP_OUTPUT" "$TEMP_LINES"

echo ""
echo "Filtered log saved to: $OUTPUT_FILE"
echo "Original log file: $INPUT_FILE ($(wc -l < "$INPUT_FILE") lines)"
echo "Filtered log file: $OUTPUT_FILE ($(wc -l < "$OUTPUT_FILE") lines)"
echo ""
echo "Note: This script removes logs of batches that were sent to UDS."
echo "      Only [BATCH SENT TO UDS] logs are kept to mark completion."

