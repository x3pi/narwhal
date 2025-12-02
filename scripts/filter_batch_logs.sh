#!/bin/bash
# Script để filter log và loại bỏ log của batch đã được gửi đến UDS
# Usage: ./filter_batch_logs.sh <input_log_file> <output_log_file>

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

# Tạo file tạm để lưu batch IDs đã được gửi đến UDS
TEMP_BATCHES=$(mktemp)
TEMP_OUTPUT=$(mktemp)

# Extract tất cả batch IDs đã được gửi đến UDS từ log
# Tìm pattern: [BATCH SENT TO UDS] hoặc [BATCH TO UDS]
grep -E "\[BATCH SENT TO UDS\]|\[BATCH TO UDS\]" "$INPUT_FILE" | \
    grep -oE 'batch[^"]*"[^"]*"' | \
    sed 's/.*"\([^"]*\)".*/\1/' | \
    sort -u > "$TEMP_BATCHES"

echo "Found $(wc -l < "$TEMP_BATCHES") batches sent to UDS"

# Filter log: Loại bỏ các dòng chứa batch ID đã được gửi đến UDS
# Nhưng giữ lại log [BATCH SENT TO UDS] và [BATCH TO UDS] để đánh dấu
while IFS= read -r batch_id; do
    if [ -n "$batch_id" ]; then
        # Loại bỏ các log chứa batch_id này, nhưng giữ lại log [BATCH SENT TO UDS]
        # Sử dụng grep với -v để loại bỏ, nhưng giữ lại dòng [BATCH SENT TO UDS]
        if [ ! -s "$TEMP_OUTPUT" ]; then
            # Lần đầu tiên, copy toàn bộ file
            cp "$INPUT_FILE" "$TEMP_OUTPUT"
        fi
        
        # Loại bỏ các dòng chứa batch_id nhưng không phải là [BATCH SENT TO UDS]
        grep -vE "\[BATCH SENT TO UDS\]|\[BATCH TO UDS\]" "$TEMP_OUTPUT" | \
            grep -v "$batch_id" > "${TEMP_OUTPUT}.tmp"
        
        # Giữ lại các dòng [BATCH SENT TO UDS] và [BATCH TO UDS]
        grep -E "\[BATCH SENT TO UDS\]|\[BATCH TO UDS\]" "$TEMP_OUTPUT" >> "${TEMP_OUTPUT}.tmp"
        
        mv "${TEMP_OUTPUT}.tmp" "$TEMP_OUTPUT"
    fi
done < "$TEMP_BATCHES"

# Nếu không có batch nào được gửi đến UDS, copy file gốc
if [ ! -s "$TEMP_BATCHES" ]; then
    cp "$INPUT_FILE" "$TEMP_OUTPUT"
fi

# Sort lại theo timestamp nếu có
if grep -q "timestamp" "$TEMP_OUTPUT"; then
    # JSON log format
    sort -t'"' -k4 "$TEMP_OUTPUT" > "$OUTPUT_FILE"
else
    # Plain text log format
    sort "$TEMP_OUTPUT" > "$OUTPUT_FILE"
fi

# Cleanup
rm -f "$TEMP_BATCHES" "$TEMP_OUTPUT"

echo "Filtered log saved to: $OUTPUT_FILE"
echo "Original log file: $INPUT_FILE ($(wc -l < "$INPUT_FILE") lines)"
echo "Filtered log file: $OUTPUT_FILE ($(wc -l < "$OUTPUT_FILE") lines)"

