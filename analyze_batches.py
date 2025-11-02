#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Chương trình phân tích log để tìm các batch không được create hoặc commit
"""

import re
import sys
from pathlib import Path
from typing import Set, Dict


def extract_batch_ids_from_worker_log(worker_log_path: str) -> Set[str]:
    """
    Đọc file worker log và trích xuất tất cả batch IDs.
    Format: "Batch XPY008foiRtOKZZLQyM0VXiq4Hu5yineMDhZBKn0Tw4= contains ..."
    """
    batch_ids = set()
    pattern = r'Batch\s+([A-Za-z0-9+/=]+)\s+contains'
    
    try:
        with open(worker_log_path, 'r', encoding='utf-8') as f:
            for line in f:
                match = re.search(pattern, line)
                if match:
                    batch_id = match.group(1)
                    batch_ids.add(batch_id)
    except FileNotFoundError:
        print(f"❌ Không tìm thấy file: {worker_log_path}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Lỗi khi đọc file worker log: {e}")
        sys.exit(1)
    
    return batch_ids


def extract_batch_ids_from_primary_log(primary_log_path: str) -> Dict[str, Dict[str, bool]]:
    """
    Đọc file primary log và trích xuất các batch IDs đã được Created và Committed.
    Format Created: "Created B34(...) -> XPY008foiRtOKZZLQyM0VXiq4Hu5yineMDhZBKn0Tw4="
    Format Committed: "Committed B93(...) -> XPY008foiRtOKZZLQyM0VXiq4Hu5yineMDhZBKn0Tw4="
    
    Returns: Dict với key là batch_id, value là dict chứa 'created' và 'committed'
    """
    batch_status = {}
    created_pattern = r'Created\s+B\d+\([^)]+\)\s+->\s+([A-Za-z0-9+/=]+)'
    committed_pattern = r'Committed\s+B\d+\([^)]+\)\s+->\s+([A-Za-z0-9+/=]+)'
    
    try:
        with open(primary_log_path, 'r', encoding='utf-8') as f:
            for line in f:
                # Tìm Created
                match = re.search(created_pattern, line)
                if match:
                    batch_id = match.group(1)
                    if batch_id not in batch_status:
                        batch_status[batch_id] = {'created': False, 'committed': False}
                    batch_status[batch_id]['created'] = True
                
                # Tìm Committed
                match = re.search(committed_pattern, line)
                if match:
                    batch_id = match.group(1)
                    if batch_id not in batch_status:
                        batch_status[batch_id] = {'created': False, 'committed': False}
                    batch_status[batch_id]['committed'] = True
    except FileNotFoundError:
        print(f"❌ Không tìm thấy file: {primary_log_path}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Lỗi khi đọc file primary log: {e}")
        sys.exit(1)
    
    return batch_status


def main():
    """Hàm chính"""
    # Đường dẫn mặc định
    script_dir = Path(__file__).parent
    worker_log_path = script_dir / 'benchmark' / 'logs' / 'worker-0-0.log'
    primary_log_path = script_dir / 'benchmark' / 'logs' / 'primary-0.log'
    
    # Cho phép override bằng command line arguments
    # Lọc ra --all flag
    args = [arg for arg in sys.argv[1:] if arg != '--all']
    
    if len(args) >= 2:
        worker_log_path = args[0]
        primary_log_path = args[1]
    elif len(args) == 1:
        print("📝 Sử dụng: python analyze_batches.py [worker_log_path] [primary_log_path] [--all]")
        print(f"📝 Hoặc sử dụng đường dẫn mặc định:")
        print(f"   Worker log: {worker_log_path}")
        print(f"   Primary log: {primary_log_path}")
        print(f"\n   Thêm --all để xem danh sách tất cả các batch")
        sys.exit(1)
    
    print(f"📂 Đang đọc worker log: {worker_log_path}")
    all_batch_ids = extract_batch_ids_from_worker_log(str(worker_log_path))
    print(f"✅ Tìm thấy {len(all_batch_ids)} batch(es) trong worker log")
    
    print(f"\n📂 Đang đọc primary log: {primary_log_path}")
    batch_status = extract_batch_ids_from_primary_log(str(primary_log_path))
    print(f"✅ Tìm thấy {len(batch_status)} batch(es) trong primary log")
    
    # Phân tích các batch
    created_batches = {bid for bid, status in batch_status.items() if status['created']}
    committed_batches = {bid for bid, status in batch_status.items() if status['committed']}
    
    print(f"\n📊 Thống kê:")
    print(f"   - Tổng số batch trong worker log: {len(all_batch_ids)}")
    print(f"   - Số batch đã được Created: {len(created_batches)}")
    print(f"   - Số batch đã được Committed: {len(committed_batches)}")
    
    # Tìm các batch không được create
    not_created = all_batch_ids - created_batches
    
    # Tìm các batch không được commit
    not_committed = all_batch_ids - committed_batches
    
    # Tìm các batch không được create HOẶC commit
    not_created_or_committed = all_batch_ids - created_batches - committed_batches
    
    # In kết quả
    print(f"\n{'='*80}")
    print("🔍 KẾT QUẢ PHÂN TÍCH")
    print(f"{'='*80}")
    
    if not_created:
        print(f"\n❌ Các batch KHÔNG được Created ({len(not_created)} batch):")
        for i, batch_id in enumerate(sorted(not_created), 1):
            status = ""
            if batch_id in committed_batches:
                status = " [Đã Committed nhưng chưa Created - CÓ VẤN ĐỀ]"
            print(f"   {i}. {batch_id}{status}")
    else:
        print(f"\n✅ Tất cả {len(all_batch_ids)} batch đều đã được Created!")
    
    if not_committed:
        print(f"\n❌ Các batch KHÔNG được Committed ({len(not_committed)} batch):")
        for i, batch_id in enumerate(sorted(not_committed), 1):
            status = ""
            if batch_id in created_batches:
                status = " [Đã Created nhưng chưa Committed]"
            print(f"   {i}. {batch_id}{status}")
    else:
        print(f"\n✅ Tất cả {len(all_batch_ids)} batch đều đã được Committed!")
    
    if not_created_or_committed:
        print(f"\n⚠️  Các batch KHÔNG được Create VÀ Committed ({len(not_created_or_committed)} batch):")
        for i, batch_id in enumerate(sorted(not_created_or_committed), 1):
            print(f"   {i}. {batch_id}")
    
    # Hiển thị danh sách tất cả batch nếu có tham số --all
    show_all = '--all' in sys.argv
    if show_all:
        print(f"\n{'='*80}")
        print(f"📋 DANH SÁCH TẤT CẢ CÁC BATCH ({len(all_batch_ids)} batch):")
        print(f"{'='*80}")
        for i, batch_id in enumerate(sorted(all_batch_ids), 1):
            status_parts = []
            if batch_id in created_batches:
                status_parts.append("✓ Created")
            else:
                status_parts.append("✗ Not Created")
            if batch_id in committed_batches:
                status_parts.append("✓ Committed")
            else:
                status_parts.append("✗ Not Committed")
            status_str = " | ".join(status_parts)
            print(f"   {i:3d}. {batch_id:<50} [{status_str}]")
    
    # Tóm tắt
    print(f"\n{'='*80}")
    print("📋 TÓM TẮT:")
    print(f"   - Batch chưa Created: {len(not_created)}")
    print(f"   - Batch chưa Committed: {len(not_committed)}")
    print(f"   - Batch chưa Created và Committed: {len(not_created_or_committed)}")
    if not show_all:
        print(f"\n💡 Sử dụng --all để xem danh sách tất cả các batch")
    print(f"{'='*80}")


if __name__ == '__main__':
    main()

