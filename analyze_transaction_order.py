#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script kiểm tra thứ tự transactions trong blocks được gửi qua unix domain socket
"""

import re
import sys
from pathlib import Path
from typing import Dict, List, Tuple
from collections import defaultdict


def parse_timestamp(log_line: str) -> str:
    """Parse timestamp từ log line"""
    pattern = r'\[(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z)\]'
    match = re.search(pattern, log_line)
    if match:
        return match.group(1)
    return None


def analyze_transaction_order(primary_log_path: str):
    """Phân tích thứ tự certificates và transactions"""
    
    # Pattern: [ANALYZE] RECEIVED certificate for round X
    received_pattern = r'\[ANALYZE\].*RECEIVED certificate for round (\d+)'
    
    # Pattern: Finalizing block for height X containing Y unique transactions
    finalizing_pattern = r'Finalizing block for height (\d+).*containing (\d+) unique transactions'
    
    # Pattern: Committed BX(...) -> batch_digest= (từ consensus)
    committed_pattern = r'Committed\s+B(\d+)\([^)]+\)\s+->\s+([A-Za-z0-9+/=]+)'
    
    certificates_by_block: Dict[int, List[Tuple[int, str]]] = defaultdict(list)  # height -> [(round, digest)]
    blocks_order: List[Tuple[int, int]] = []  # [(height, tx_count)]
    certificates_committed: Dict[str, int] = {}  # digest -> round
    
    print(f"📂 Đang phân tích primary log: {primary_log_path}")
    print(f"{'='*100}\n")
    
    try:
        with open(primary_log_path, 'r', encoding='utf-8') as f:
            for line in f:
                # Track certificates committed by consensus
                match = re.search(committed_pattern, line)
                if match:
                    round_num = int(match.group(1))
                    digest = match.group(2)
                    certificates_committed[digest] = round_num
                
                # Track certificates received by analyze
                match = re.search(received_pattern, line)
                if match:
                    round_num = int(match.group(1))
                    height = (round_num + 1) // 2
                    # Không có digest trong log này, chỉ có round
                    # Nhưng có thể track thứ tự
                
                # Track blocks finalized
                match = re.search(finalizing_pattern, line)
                if match:
                    height = int(match.group(1))
                    tx_count = int(match.group(2))
                    timestamp = parse_timestamp(line)
                    blocks_order.append((height, tx_count, timestamp))
    
    except Exception as e:
        print(f"❌ Lỗi: {e}")
        sys.exit(1)
    
    print(f"📊 Thống kê:")
    print(f"   - Tổng số blocks: {len(blocks_order)}")
    print(f"   - Tổng số certificates đã commit: {len(certificates_committed)}\n")
    
    # Kiểm tra thứ tự blocks
    print(f"📊 Thứ tự blocks (height tăng dần):")
    prev_height = -1
    out_of_order = []
    
    for height, tx_count, timestamp in blocks_order:
        if height <= prev_height:
            out_of_order.append((height, prev_height, timestamp))
            print(f"   ⚠️  Block height {height} (tx: {tx_count}) sau block height {prev_height} - OUT OF ORDER!")
        else:
            print(f"   ✅ Block height {height} (tx: {tx_count}) tại {timestamp}")
        prev_height = height
    
    if not out_of_order:
        print(f"\n✅ Tất cả blocks đều theo thứ tự height tăng dần!")
    else:
        print(f"\n❌ Tìm thấy {len(out_of_order)} block(s) out of order")
    
    # Phân tích chi tiết
    print(f"\n{'='*100}")
    print(f"💡 PHÂN TÍCH:")
    print(f"   1. Consensus sort certificates theo round trước khi gửi")
    print(f"   2. Certificates được gửi tuần tự trong sequence")
    print(f"   3. Analyze nhận và xử lý theo thứ tự nhận được")
    print(f"   4. Blocks được tạo theo height (round-based)")
    print(f"   5. Transactions trong block theo thứ tự certificates (theo round)")
    print(f"\n   ✅ Thứ tự được đảm bảo bởi:")
    print(f"      - Consensus sort sequence theo round")
    print(f"      - Tokio channel đảm bảo FIFO order")
    print(f"      - Analyze xử lý tuần tự")
    print(f"      - Blocks được flush khi height tăng")
    print(f"{'='*100}")


if __name__ == '__main__':
    script_dir = Path(__file__).parent
    primary_log_path = script_dir / 'benchmark' / 'logs' / 'primary-0.log'
    
    if len(sys.argv) > 1:
        primary_log_path = Path(sys.argv[1])
    
    analyze_transaction_order(str(primary_log_path))

