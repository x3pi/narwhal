#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script phân tích để tìm các batch rất lâu mới được commit.
Tính thời gian từ khi batch được tạo (BATCH_CREATED) đến khi commit (BATCH_COMMITTED).
"""

import re
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional, List, Tuple
from collections import defaultdict


class BatchTimeline:
    """Theo dõi timeline của một batch"""
    
    def __init__(self, batch_digest: str):
        self.batch_digest = batch_digest
        self.created_at: Optional[datetime] = None
        self.created_round: Optional[int] = None
        self.received_by_primary_at: Optional[datetime] = None
        self.received_round: Optional[int] = None
        self.included_in_header_at: Optional[datetime] = None
        self.included_in_header_round: Optional[int] = None
        self.committed_at: Optional[datetime] = None
        self.committed_round: Optional[int] = None
        self.certificate_id: Optional[str] = None
    
    def get_total_commit_delay(self) -> Optional[float]:
        """Tính thời gian từ khi tạo đến khi commit (seconds)"""
        if self.created_at and self.committed_at:
            delta = (self.committed_at - self.created_at).total_seconds()
            return delta
        return None
    
    def get_round_delay(self) -> Optional[int]:
        """Tính số rounds từ khi batch được tạo đến khi commit"""
        # Ưu tiên: từ created_round đến committed_round
        if self.created_round and self.committed_round:
            return self.committed_round - self.created_round
        # Fallback: từ included_round đến committed_round
        if self.included_in_header_round and self.committed_round:
            return self.committed_round - self.included_in_header_round
        # Fallback: từ received_round đến committed_round
        if self.received_round and self.committed_round:
            return self.committed_round - self.received_round
        return None
    
    def get_primary_receive_delay(self) -> Optional[float]:
        """Tính thời gian từ khi tạo đến khi primary nhận (seconds)"""
        if self.created_at and self.received_by_primary_at:
            delta = (self.received_by_primary_at - self.created_at).total_seconds()
            return delta
        return None
    
    def get_header_include_delay(self) -> Optional[float]:
        """Tính thời gian từ khi primary nhận đến khi include vào header (seconds)"""
        if self.received_by_primary_at and self.included_in_header_at:
            delta = (self.included_in_header_at - self.received_by_primary_at).total_seconds()
            return delta
        return None
    
    def get_commit_after_header_delay(self) -> Optional[float]:
        """Tính thời gian từ khi include vào header đến khi commit (seconds)"""
        if self.included_in_header_at and self.committed_at:
            delta = (self.committed_at - self.included_in_header_at).total_seconds()
            return delta
        return None


def parse_timestamp(log_line: str) -> Optional[datetime]:
    """Parse timestamp từ log line"""
    # Format: [2025-11-02T10:58:25.289Z INFO  ...]
    pattern = r'\[(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z)'
    match = re.search(pattern, log_line)
    if match:
        try:
            # Parse ISO format with milliseconds
            ts_str = match.group(1)
            return datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
        except Exception:
            return None
    return None


def parse_worker_log(worker_log_path: str) -> Dict[str, BatchTimeline]:
    """Parse worker log để tìm khi batch được tạo"""
    batches: Dict[str, BatchTimeline] = {}
    
    # Pattern: [BATCH_CREATED] Batch digest= created with N transactions...
    pattern = r'\[BATCH_CREATED\]\s+Batch\s+([A-Za-z0-9+/=]+)\s+created'
    
    try:
        with open(worker_log_path, 'r', encoding='utf-8') as f:
            for line in f:
                match = re.search(pattern, line)
                if match:
                    batch_digest = match.group(1)
                    if batch_digest not in batches:
                        batches[batch_digest] = BatchTimeline(batch_digest)
                    
                    timestamp = parse_timestamp(line)
                    if timestamp:
                        batches[batch_digest].created_at = timestamp
                        # Note: Worker log không có round info, nhưng có thể estimate từ timestamp
    except FileNotFoundError:
        print(f"❌ Không tìm thấy file: {worker_log_path}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Lỗi khi đọc worker log: {e}")
        sys.exit(1)
    
    return batches


def parse_primary_log(primary_log_path: str, batches: Dict[str, BatchTimeline]) -> Dict[str, BatchTimeline]:
    """Parse primary log để tìm khi batch được nhận, include vào header, và commit"""
    
    # Lưu mapping timestamp -> round để estimate created_round
    header_timestamps: List[Tuple[datetime, int]] = []
    
    # Pattern 1: Received batch from worker (nếu có log này)
    received_pattern = r'Received batch\s+([A-Za-z0-9+/=]+)\s+from worker\s+(\d+)\s+.*?at round\s+(\d+)'
    
    # Pattern 2: Created header with batch
    # Format: Created B47(...) -> batch_digest=
    created_pattern = r'Created\s+B(\d+)\([^)]+\)\s+->\s+([A-Za-z0-9+/=]+)'
    
    # Pattern 3: Batch committed
    # Format: [BATCH_COMMITTED] Batch batch_digest= committed at round X in certificate Y
    committed_pattern = r'\[BATCH_COMMITTED\]\s+Batch\s+([A-Za-z0-9+/=]+)\s+committed\s+at\s+round\s+(\d+)\s+in\s+certificate\s+([A-Za-z0-9+/=]+)'
    
    try:
        with open(primary_log_path, 'r', encoding='utf-8') as f:
            for line in f:
                timestamp = parse_timestamp(line)
                if not timestamp:
                    continue
                
                # Track header creation times để estimate created_round
                match = re.search(created_pattern, line)
                if match:
                    round_num = int(match.group(1))
                    batch_digest = match.group(2)
                    header_timestamps.append((timestamp, round_num))
                    
                    if batch_digest in batches:
                        batches[batch_digest].included_in_header_at = timestamp
                        batches[batch_digest].included_in_header_round = round_num
                
                # Tìm batch received (nếu có log)
                match = re.search(received_pattern, line)
                if match:
                    batch_digest = match.group(1)
                    round_num = int(match.group(3))
                    if batch_digest in batches:
                        batches[batch_digest].received_by_primary_at = timestamp
                        batches[batch_digest].received_round = round_num
                    elif batch_digest not in batches:
                        # Batch từ worker khác (không phải worker-0-0)
                        batches[batch_digest] = BatchTimeline(batch_digest)
                        batches[batch_digest].received_by_primary_at = timestamp
                        batches[batch_digest].received_round = round_num
                
                # Tìm batch committed
                match = re.search(committed_pattern, line)
                if match:
                    batch_digest = match.group(1)
                    round_num = int(match.group(2))
                    cert_id = match.group(3)
                    if batch_digest in batches:
                        batches[batch_digest].committed_at = timestamp
                        batches[batch_digest].committed_round = round_num
                        batches[batch_digest].certificate_id = cert_id
                    elif batch_digest not in batches:
                        # Batch từ worker khác
                        batches[batch_digest] = BatchTimeline(batch_digest)
                        batches[batch_digest].committed_at = timestamp
                        batches[batch_digest].committed_round = round_num
                        batches[batch_digest].certificate_id = cert_id
        
        # Estimate created_round cho batches dựa trên timestamp
        # Sắp xếp header_timestamps theo thời gian
        header_timestamps.sort()
        
        for batch_digest, timeline in batches.items():
            if timeline.created_at and not timeline.created_round and header_timestamps:
                # Tìm header gần nhất sau khi batch được tạo
                for header_time, header_round in header_timestamps:
                    if header_time >= timeline.created_at:
                        # Estimate: batch được tạo ở round trước header này
                        timeline.created_round = max(1, header_round - 5)  # Conservative estimate
                        break
                # Nếu không tìm thấy, dùng header cuối cùng
                if not timeline.created_round and header_timestamps:
                    last_header_round = header_timestamps[-1][1]
                    timeline.created_round = max(1, last_header_round - 10)  # Very conservative
    except FileNotFoundError:
        print(f"❌ Không tìm thấy file: {primary_log_path}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Lỗi khi đọc primary log: {e}")
        sys.exit(1)
    
    return batches


def format_duration(seconds: float) -> str:
    """Format duration thành string dễ đọc"""
    if seconds < 1:
        return f"{seconds*1000:.1f}ms"
    elif seconds < 60:
        return f"{seconds:.2f}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        secs = seconds % 60
        return f"{minutes}m {secs:.1f}s"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = seconds % 60
        return f"{hours}h {minutes}m {secs:.1f}s"


def main():
    """Hàm chính"""
    # Đường dẫn mặc định
    script_dir = Path(__file__).parent
    worker_log_path = script_dir / 'benchmark' / 'logs' / 'worker-0-0.log'
    primary_log_path = script_dir / 'benchmark' / 'logs' / 'primary-0.log'
    
    # Parse command line arguments
    args = sys.argv[1:]
    
    # Parse --top argument trước
    top_n = 20
    if '--top' in args:
        try:
            idx = args.index('--top')
            if idx + 1 < len(args):
                top_n = int(args[idx + 1])
                # Remove --top và giá trị của nó khỏi args
                args = args[:idx] + args[idx+2:]
        except (ValueError, IndexError):
            pass
    
    # Parse file paths
    if len(args) >= 2:
        worker_log_path = Path(args[0])
        primary_log_path = Path(args[1])
    elif len(args) == 1:
        print("📝 Sử dụng: python analyze_batch_commit_delay.py [worker_log_path] [primary_log_path] [--top N]")
        print(f"📝 Hoặc sử dụng đường dẫn mặc định:")
        print(f"   Worker log: {worker_log_path}")
        print(f"   Primary log: {primary_log_path}")
        print(f"\n   Thêm --top N để chỉ hiển thị N batch có delay lâu nhất (mặc định: 20)")
        sys.exit(1)
    
    print(f"📂 Đang đọc worker log: {worker_log_path}")
    batches = parse_worker_log(str(worker_log_path))
    print(f"✅ Tìm thấy {len(batches)} batch(es) được tạo trong worker log")
    
    print(f"\n📂 Đang đọc primary log: {primary_log_path}")
    batches = parse_primary_log(str(primary_log_path), batches)
    
    # Bỏ qua các batch của 150 round đầu
    MIN_ROUND = 150
    filtered_batches = {}
    skipped_count = 0
    
    for batch_digest, timeline in batches.items():
        # Bỏ qua batch nếu committed_round hoặc included_in_header_round <= 150
        should_skip = False
        if timeline.committed_round is not None and timeline.committed_round <= MIN_ROUND:
            should_skip = True
        elif timeline.included_in_header_round is not None and timeline.included_in_header_round <= MIN_ROUND:
            should_skip = True
        # Nếu không có round info, giữ lại để kiểm tra
        if not should_skip:
            filtered_batches[batch_digest] = timeline
        else:
            skipped_count += 1
    
    if skipped_count > 0:
        print(f"⏭️  Đã bỏ qua {skipped_count} batch(es) từ {MIN_ROUND} round đầu")
    
    batches = filtered_batches
    
    # Tính toán delays
    batches_with_delays: List[Tuple[str, BatchTimeline, float]] = []
    batches_with_round_delays: List[Tuple[str, BatchTimeline, int]] = []
    
    for batch_digest, timeline in batches.items():
        delay = timeline.get_total_commit_delay()
        round_delay = timeline.get_round_delay()
        
        if delay is not None:
            batches_with_delays.append((batch_digest, timeline, delay))
        if round_delay is not None:
            batches_with_round_delays.append((batch_digest, timeline, round_delay))
    
    print(f"✅ Tìm thấy {len(batches_with_delays)} batch(es) đã được commit")
    
    # Sắp xếp theo delay giảm dần
    batches_with_delays.sort(key=lambda x: x[2], reverse=True)
    batches_with_round_delays.sort(key=lambda x: x[2], reverse=True)
    
    print(f"\n{'='*100}")
    print("🔍 CÁC BATCH CÓ THỜI GIAN COMMIT LÂU NHẤT")
    print(f"{'='*100}")
    print(f"\n📊 Top {min(top_n, len(batches_with_delays))} batch(es) có thời gian commit lâu nhất (theo thời gian):\n")
    
    for i, (batch_digest, timeline, delay) in enumerate(batches_with_delays[:top_n], 1):
        round_delay = timeline.get_round_delay()
        
        print(f"{i:3d}. Batch: {batch_digest[:50]}...")
        print(f"     ⏱️  Tổng thời gian: {format_duration(delay)}")
        if round_delay is not None:
            print(f"     🔢 Round delay: {round_delay} rounds")
        
        if timeline.created_at:
            print(f"     📅 Created: {timeline.created_at.strftime('%H:%M:%S.%f')[:-3]}")
        if timeline.received_by_primary_at:
            primary_delay = timeline.get_primary_receive_delay()
            print(f"     📥 Received by Primary: {timeline.received_by_primary_at.strftime('%H:%M:%S.%f')[:-3]} "
                  f"(+{format_duration(primary_delay) if primary_delay else 'N/A'})")
        if timeline.included_in_header_at:
            header_delay = timeline.get_header_include_delay()
            print(f"     📄 Included in Header (round {timeline.included_in_header_round}): "
                  f"{timeline.included_in_header_at.strftime('%H:%M:%S.%f')[:-3]} "
                  f"(+{format_duration(header_delay) if header_delay else 'N/A'})")
        if timeline.committed_at:
            commit_delay = timeline.get_commit_after_header_delay()
            print(f"     ✅ Committed (round {timeline.committed_round}): "
                  f"{timeline.committed_at.strftime('%H:%M:%S.%f')[:-3]} "
                  f"(+{format_duration(commit_delay) if commit_delay else 'N/A'})")
            if timeline.certificate_id:
                print(f"     📜 Certificate: {timeline.certificate_id}")
        
        print()
    
    # Hiển thị top batches theo round delay
    if batches_with_round_delays:
        print(f"\n{'='*100}")
        print("🔍 CÁC BATCH CÓ ROUND DELAY CAO NHẤT")
        print(f"{'='*100}")
        print(f"\n📊 Top {min(top_n, len(batches_with_round_delays))} batch(es) có round delay cao nhất:\n")
        
        for i, (batch_digest, timeline, round_delay) in enumerate(batches_with_round_delays[:top_n], 1):
            time_delay = timeline.get_total_commit_delay()
            
            print(f"{i:3d}. Batch: {batch_digest[:50]}...")
            if time_delay is not None:
                print(f"     ⏱️  Tổng thời gian: {format_duration(time_delay)}")
            print(f"     🔢 Round delay: {round_delay} rounds")
            
            if timeline.created_at:
                print(f"     📅 Created: {timeline.created_at.strftime('%H:%M:%S.%f')[:-3]}")
                if timeline.created_round:
                    print(f"        (ước tính round ~{timeline.created_round})")
            if timeline.included_in_header_at and timeline.included_in_header_round:
                print(f"     📄 Included in Header tại round {timeline.included_in_header_round}: "
                      f"{timeline.included_in_header_at.strftime('%H:%M:%S.%f')[:-3]}")
            if timeline.committed_at and timeline.committed_round:
                print(f"     ✅ Committed tại round {timeline.committed_round}: "
                      f"{timeline.committed_at.strftime('%H:%M:%S.%f')[:-3]}")
                if timeline.certificate_id:
                    print(f"     📜 Certificate: {timeline.certificate_id}")
            
            print()
    
    # Thống kê
    if batches_with_delays:
        delays = [delay for _, _, delay in batches_with_delays]
        avg_delay = sum(delays) / len(delays)
        max_delay = max(delays)
        min_delay = min(delays)
        
        print(f"{'='*100}")
        print("📊 THỐNG KÊ THỜI GIAN COMMIT:")
        print(f"   - Tổng số batch đã commit: {len(batches_with_delays)}")
        print(f"   - Thời gian trung bình: {format_duration(avg_delay)}")
        print(f"   - Thời gian tối đa: {format_duration(max_delay)}")
        print(f"   - Thời gian tối thiểu: {format_duration(min_delay)}")
        
        if batches_with_round_delays:
            round_delays = [rd for _, _, rd in batches_with_round_delays]
            avg_round_delay = sum(round_delays) / len(round_delays)
            max_round_delay = max(round_delays)
            min_round_delay = min(round_delays)
            
            print(f"\n📊 THỐNG KÊ ROUND DELAY:")
            print(f"   - Round delay trung bình: {avg_round_delay:.1f} rounds")
            print(f"   - Round delay tối đa: {max_round_delay} rounds")
            print(f"   - Round delay tối thiểu: {min_round_delay} rounds")
        
        # Phân bố thời gian
        print(f"\n📊 PHÂN BỐ THỜI GIAN COMMIT:")
        bins = [(0, 1), (1, 5), (5, 10), (10, 30), (30, 60), (60, float('inf'))]
        for start, end in bins:
            if end == float('inf'):
                count = sum(1 for d in delays if d >= start)
                label = f">={start}s"
            else:
                count = sum(1 for d in delays if start <= d < end)
                label = f"{start}-{end}s"
            percentage = (count / len(delays)) * 100
            print(f"   - {label:>10}: {count:>4} batch ({percentage:>5.1f}%)")
    
    print(f"{'='*100}")


if __name__ == '__main__':
    main()

