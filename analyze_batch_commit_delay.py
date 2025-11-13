#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script phân tích để tìm các batch rất lâu mới được commit.
Tính thời gian từ khi batch được tạo (BATCH_CREATED) đến khi commit (BATCH_COMMITTED).
"""

import re
import sys
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Optional, List, Tuple
from collections import defaultdict


def normalize_datetime_for_sort(dt: Optional[datetime]) -> datetime:
    """Đưa datetime về dạng naive (UTC) để so sánh/sắp xếp an toàn."""
    if dt is None:
        return datetime.min
    if dt.tzinfo is None:
        return dt
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


class BatchTimeline:
    """Theo dõi timeline của một batch"""
    
    def __init__(self, batch_digest: str, display_digest: Optional[str] = None):
        self.batch_digest = batch_digest
        self.display_digest = display_digest or batch_digest
        self.created_at: Optional[datetime] = None
        self.created_round: Optional[int] = None
        self.received_by_primary_at: Optional[datetime] = None
        self.received_round: Optional[int] = None
        self.included_in_header_at: Optional[datetime] = None
        self.included_in_header_round: Optional[int] = None
        self.committed_at: Optional[datetime] = None
        self.committed_round: Optional[int] = None
        self.certificate_id: Optional[str] = None
        self.last_event_at: Optional[datetime] = None
        self.last_header_include_at: Optional[datetime] = None
        self.last_header_include_round: Optional[int] = None
        # Track tất cả các lần include vào header (để phát hiện retry)
        self.all_header_includes: List[Tuple[datetime, int]] = []
    
    def update_display_digest(self, candidate: Optional[str]):
        """Cập nhật display digest (ưu tiên chuỗi dài hơn)."""
        if not candidate:
            return
        if not self.display_digest or len(candidate) > len(self.display_digest):
            self.display_digest = candidate
    
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


def normalize_digest(digest: str) -> str:
    """Normalize digest bằng cách bỏ dấu = ở cuối nếu có"""
    return digest.rstrip('=')


def resolve_digest_key(batches: Dict[str, BatchTimeline], normalized_digest: str) -> str:
    """
    Tìm key tương ứng trong batches để gom các log dùng digest rút gọn và digest đầy đủ.
    Ưu tiên:
      1. Khớp chính xác.
      2. Khớp duy nhất theo prefix (rút gọn so với đầy đủ).
    """
    if normalized_digest in batches:
        return normalized_digest
    
    candidates = [
        key for key in batches.keys()
        if key.startswith(normalized_digest) or normalized_digest.startswith(key)
    ]
    
    if len(candidates) == 1:
        return candidates[0]
    
    return normalized_digest


def parse_worker_log(worker_log_path: str) -> Dict[str, BatchTimeline]:
    """Parse worker log để tìm khi batch được tạo"""
    batches: Dict[str, BatchTimeline] = {}
    
    # Pattern thực tế: Batch {digest}= contains sample tx {id}
    # Format: [timestamp INFO worker::batch_maker] Batch {digest}= contains ...
    # Lưu ý: pattern lấy cả dấu = nếu có
    pattern = r'Batch\s+([A-Za-z0-9+/=]+)\s+contains'
    
    try:
        with open(worker_log_path, 'r', encoding='utf-8') as f:
            for line in f:
                match = re.search(pattern, line)
                if match:
                    # Lấy digest và normalize (bỏ dấu = ở cuối)
                    batch_digest_raw = match.group(1)
                    batch_digest = normalize_digest(batch_digest_raw)
                    
                    resolved_key = resolve_digest_key(batches, batch_digest)
                    if resolved_key not in batches:
                        batches[resolved_key] = BatchTimeline(resolved_key, batch_digest_raw)
                    timeline = batches[resolved_key]
                    timeline.update_display_digest(batch_digest_raw)

                    timestamp = parse_timestamp(line)
                    if timestamp and not timeline.created_at:
                        # Chỉ set created_at nếu chưa có (lấy timestamp đầu tiên)
                        timeline.created_at = timestamp
                        # Note: Worker log không có round info, nhưng có thể estimate từ timestamp
                    if timestamp:
                        timeline.last_event_at = timestamp
    except FileNotFoundError:
        print(f"❌ Không tìm thấy file: {worker_log_path}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Lỗi khi đọc worker log: {e}")
        sys.exit(1)
    
    return batches


def parse_primary_log(primary_log_path: str, batches: Dict[str, BatchTimeline]) -> Tuple[Dict[str, BatchTimeline], Optional[datetime]]:
    """Parse primary log để tìm khi batch được nhận, include vào header, và commit"""
    
    # Lưu mapping timestamp -> round để estimate created_round
    header_timestamps: List[Tuple[datetime, int]] = []
    last_log_timestamp: Optional[datetime] = None
    
    # Pattern 1: Received batch from worker (nếu có log này - có thể không có)
    received_pattern = r'Received batch\s+([A-Za-z0-9+/=]+)\s+from worker\s+(\d+)\s+.*?at round\s+(\d+)'
    
    # Pattern 2: Created header with batch
    # Format thực tế: Created B69(AqJy7eip40qqZk7F) -> T72wUl2KEYsiVAEjM+2+qDVwB1oe/WWfMMhQfxzsYtE=
    created_pattern = r'Created\s+B(\d+)\([^)]+\)\s+->\s+([A-Za-z0-9+/=]+)'
    
    # Pattern 3: Batch committed
    # Format thực tế: Committed B93(AqJy7eip40qqZk7F) -> T72wUl2KEYsiVAEjM+2+qDVwB1oe/WWfMMhQfxzsYtE=
    committed_pattern = r'Committed\s+B(\d+)\([^)]+\)\s+->\s+([A-Za-z0-9+/=]+)'
    
    try:
        with open(primary_log_path, 'r', encoding='utf-8') as f:
            for line in f:
                timestamp = parse_timestamp(line)
                if not timestamp:
                    continue
                last_log_timestamp = timestamp
                
                # Track header creation times để estimate created_round
                match = re.search(created_pattern, line)
                if match:
                    round_num = int(match.group(1))
                    batch_digest_raw = match.group(2)
                    batch_digest = normalize_digest(batch_digest_raw)
                    resolved_key = resolve_digest_key(batches, batch_digest)
                    header_timestamps.append((timestamp, round_num))
                    
                    if resolved_key not in batches:
                        batches[resolved_key] = BatchTimeline(resolved_key, batch_digest_raw)
                    timeline = batches[resolved_key]
                    timeline.update_display_digest(batch_digest_raw)
                    # Track tất cả các lần include (để phát hiện retry)
                    timeline.all_header_includes.append((timestamp, round_num))
                    # Chỉ update included_in_header_at nếu chưa có (lần đầu tiên)
                    if not timeline.included_in_header_at:
                        timeline.included_in_header_at = timestamp
                        timeline.included_in_header_round = round_num
                    timeline.last_header_include_at = timestamp
                    timeline.last_header_include_round = round_num
                    timeline.last_event_at = timestamp
                
                # Tìm batch received (nếu có log)
                match = re.search(received_pattern, line)
                if match:
                    batch_digest_raw = match.group(1)
                    batch_digest = normalize_digest(batch_digest_raw)
                    round_num = int(match.group(3))
                    resolved_key = resolve_digest_key(batches, batch_digest)
                    if resolved_key not in batches:
                        batches[resolved_key] = BatchTimeline(resolved_key, batch_digest_raw)
                    timeline = batches[resolved_key]
                    timeline.update_display_digest(batch_digest_raw)
                    timeline.received_by_primary_at = timestamp
                    timeline.received_round = round_num
                    timeline.last_event_at = timestamp
                
                # Tìm batch committed
                match = re.search(committed_pattern, line)
                if match:
                    round_num = int(match.group(1))
                    batch_digest_raw = match.group(2)
                    batch_digest = normalize_digest(batch_digest_raw)
                    resolved_key = resolve_digest_key(batches, batch_digest)
                    if resolved_key not in batches:
                        batches[resolved_key] = BatchTimeline(resolved_key, batch_digest_raw)
                    timeline = batches[resolved_key]
                    timeline.update_display_digest(batch_digest_raw)
                    timeline.committed_at = timestamp
                    timeline.committed_round = round_num
                    # Certificate ID không có trong format mới, để None
                    timeline.certificate_id = None
                    timeline.last_event_at = timestamp
        
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
    
    return batches, last_log_timestamp


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


def format_optional_duration(seconds: Optional[float]) -> str:
    """Format duration có thể None."""
    if seconds is None:
        return "N/A"
    return format_duration(seconds)


def seconds_between(later: Optional[datetime], earlier: Optional[datetime]) -> Optional[float]:
    """Trả về số giây giữa hai mốc thời gian (nếu có)."""
    if later is None or earlier is None:
        return None
    delta = (later - earlier).total_seconds()
    if delta < 0:
        return 0.0
    return delta


def main():
    """Hàm chính"""
    # Đường dẫn mặc định
    script_dir = Path(__file__).parent
    worker_log_path = script_dir / 'benchmark' / 'logs' / 'worker-0-0.log'
    primary_log_path = script_dir / 'benchmark' / 'logs' / 'primary-0.log'
    
    # Parse command line arguments
    args = sys.argv[1:]
    show_round_timestamps = '--show-round-timestamps' in args
    if show_round_timestamps:
        args = [arg for arg in args if arg != '--show-round-timestamps']
    
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
        print("   Thêm --show-round-timestamps để bật chi tiết timestamp theo round")
        sys.exit(1)
    
    print(f"📂 Đang đọc worker log: {worker_log_path}")
    batches = parse_worker_log(str(worker_log_path))
    print(f"✅ Tìm thấy {len(batches)} batch(es) được tạo trong worker log")
    
    print(f"\n📂 Đang đọc primary log: {primary_log_path}")
    batches, last_log_timestamp = parse_primary_log(str(primary_log_path), batches)

    if not last_log_timestamp:
        last_log_timestamp = max(
            (timeline.last_event_at for timeline in batches.values() if timeline.last_event_at),
            default=None,
        )
    
    # Bỏ qua các batch của 150 round đầu
    MIN_ROUND = 500
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
    print(f"📊 Tổng số batch đã được tạo hoặc include: {len(batches)}")
    
    # Phân tích các batch bị bỏ sót (không được commit)
    uncommitted_batches: List[Tuple[str, BatchTimeline]] = []
    uncommitted_in_header: List[Tuple[str, BatchTimeline]] = []
    uncommitted_created: List[Tuple[str, BatchTimeline]] = []
    
    for batch_digest, timeline in batches.items():
        if not timeline.committed_at:
            # Batch chưa được commit
            uncommitted_batches.append((batch_digest, timeline))
            
            # Phân loại theo trạng thái
            if timeline.included_in_header_at:
                # Đã được include vào header nhưng chưa commit
                uncommitted_in_header.append((batch_digest, timeline))
            elif timeline.created_at:
                # Đã được tạo nhưng chưa được include vào header
                uncommitted_created.append((batch_digest, timeline))
    
    # Sắp xếp theo delay giảm dần
    batches_with_delays.sort(key=lambda x: x[2], reverse=True)
    batches_with_round_delays.sort(key=lambda x: x[2], reverse=True)
    
    # Sắp xếp các batch chưa commit theo thời gian (mới nhất trước)
    uncommitted_batches.sort(key=lambda x: normalize_datetime_for_sort(x[1].last_event_at), reverse=True)
    uncommitted_in_header.sort(key=lambda x: normalize_datetime_for_sort(x[1].included_in_header_at), reverse=True)
    uncommitted_created.sort(key=lambda x: normalize_datetime_for_sort(x[1].created_at), reverse=True)
    
    print(f"\n{'='*100}")
    print("🔍 CÁC BATCH CÓ THỜI GIAN COMMIT LÂU NHẤT")
    print(f"{'='*100}")
    print(f"\n📊 Top {min(top_n, len(batches_with_delays))} batch(es) có thời gian commit lâu nhất (theo thời gian):\n")
    
    for i, (batch_digest, timeline, delay) in enumerate(batches_with_delays[:top_n], 1):
        round_delay = timeline.get_round_delay()
        
        print(f"{i:3d}. Batch: {timeline.display_digest}")
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
            if show_round_timestamps:
                print(f"     📄 Included in Header (round {timeline.included_in_header_round}): "
                      f"{timeline.included_in_header_at.strftime('%H:%M:%S.%f')[:-3]} "
                      f"(+{format_duration(header_delay) if header_delay else 'N/A'})")
            else:
                if timeline.included_in_header_round is not None:
                    print(f"     📄 Included in Header: round {timeline.included_in_header_round}")
                else:
                    print("     📄 Included in Header")
                header_delay_text = format_duration(header_delay) if header_delay else 'N/A'
                print(f"        (+{header_delay_text})")
        if timeline.committed_at:
            commit_delay = timeline.get_commit_after_header_delay()
            if show_round_timestamps:
                print(f"     ✅ Committed (round {timeline.committed_round}): "
                      f"{timeline.committed_at.strftime('%H:%M:%S.%f')[:-3]} "
                      f"(+{format_duration(commit_delay) if commit_delay else 'N/A'})")
            else:
                if timeline.committed_round is not None:
                    print(f"     ✅ Committed: round {timeline.committed_round}")
                else:
                    print("     ✅ Committed")
                commit_delay_text = format_duration(commit_delay) if commit_delay else 'N/A'
                print(f"        (+{commit_delay_text} sau khi include)")
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
            
            print(f"{i:3d}. Batch: {timeline.display_digest}")
            if time_delay is not None:
                print(f"     ⏱️  Tổng thời gian: {format_duration(time_delay)}")
            print(f"     🔢 Round delay: {round_delay} rounds")
            
            if timeline.created_at:
                print(f"     📅 Created: {timeline.created_at.strftime('%H:%M:%S.%f')[:-3]}")
                if timeline.created_round:
                    print(f"        (ước tính round ~{timeline.created_round})")
            if timeline.included_in_header_at and timeline.included_in_header_round:
                if show_round_timestamps:
                    print(f"     📄 Included in Header tại round {timeline.included_in_header_round}: "
                          f"{timeline.included_in_header_at.strftime('%H:%M:%S.%f')[:-3]}")
                else:
                    print(f"     📄 Included in Header: round {timeline.included_in_header_round}")
            if timeline.committed_at and timeline.committed_round:
                if show_round_timestamps:
                    print(f"     ✅ Committed tại round {timeline.committed_round}: "
                          f"{timeline.committed_at.strftime('%H:%M:%S.%f')[:-3]}")
                else:
                    print(f"     ✅ Committed: round {timeline.committed_round}")
                if timeline.certificate_id:
                    print(f"     📜 Certificate: {timeline.certificate_id}")
            
            print()
    
    # Phân tích các batch bị retry nhiều lần (include nhiều lần trước khi commit)
    batches_with_retries: List[Tuple[str, BatchTimeline, int]] = []
    for batch_digest, timeline in batches.items():
        if len(timeline.all_header_includes) > 1:
            # Batch được include nhiều lần (retry)
            batches_with_retries.append((batch_digest, timeline, len(timeline.all_header_includes)))
    
    # Sắp xếp theo số lần retry giảm dần
    batches_with_retries.sort(
        key=lambda x: (x[2], normalize_datetime_for_sort(x[1].created_at)),
        reverse=True,
    )
    
    if batches_with_retries:
        print(f"\n{'='*100}")
        print("🔄 CÁC BATCH BỊ RETRY NHIỀU LẦN TRƯỚC KHI COMMIT")
        print(f"{'='*100}")
        print(f"\n📊 Tổng số batch bị retry (include nhiều lần): {len(batches_with_retries)}")
        print(f"\n📊 Top {min(top_n, len(batches_with_retries))} batch(es) bị retry nhiều lần nhất:\n")
        
        for i, (batch_digest, timeline, retry_count) in enumerate(batches_with_retries[:top_n], 1):
            print(f"{i:3d}. Batch: {timeline.display_digest}")
            print(f"     🔄 Số lần include vào header: {retry_count}")
            
            if timeline.created_at:
                print(f"     📅 Created: {timeline.created_at.strftime('%H:%M:%S.%f')[:-3]}")
            
            if show_round_timestamps:
                print(f"     📄 Tất cả các lần include vào header:")
                for include_time, include_round in timeline.all_header_includes:
                    time_str = include_time.strftime('%H:%M:%S.%f')[:-3]
                    print(f"        - Round {include_round}: {time_str}")
            elif len(timeline.all_header_includes) > 1:
                print("     📄 Tất cả các lần include vào header: ẩn (thêm --show-round-timestamps để xem chi tiết)")
            
            if timeline.committed_at:
                commit_delay = timeline.get_total_commit_delay()
                if show_round_timestamps:
                    print(f"     ✅ Committed tại round {timeline.committed_round}: "
                          f"{timeline.committed_at.strftime('%H:%M:%S.%f')[:-3]}")
                else:
                    if timeline.committed_round is not None:
                        print(f"     ✅ Committed: round {timeline.committed_round}")
                    else:
                        print("     ✅ Committed")
                if commit_delay:
                    print(f"        (Tổng thời gian: {format_duration(commit_delay)})")
                
                if timeline.all_header_includes:
                    first_include_time, first_include_round = timeline.all_header_includes[0]
                    time_to_commit = (timeline.committed_at - first_include_time).total_seconds()
                    rounds_to_commit = timeline.committed_round - first_include_round
                    print(f"        (Từ lần include đầu tiên: {format_duration(time_to_commit)}, {rounds_to_commit} rounds)")
            else:
                print(f"     ⚠️  CHƯA ĐƯỢC COMMIT")
            
            print()
    
    # Hiển thị các batch bị bỏ sót (không được commit)
    if uncommitted_batches:
        print(f"\n{'='*100}")
        print("⚠️  PHÁT HIỆN CÁC BATCH BỊ BỎ SÓT (KHÔNG ĐƯỢC COMMIT)")
        print(f"{'='*100}")
        print(f"\n📊 Tổng số batch chưa được commit: {len(uncommitted_batches)}")
        print(f"   - Đã include vào header nhưng chưa commit: {len(uncommitted_in_header)}")
        print(f"   - Đã tạo nhưng chưa include vào header: {len(uncommitted_created)}")
        print(f"   - Batch khác: {len(uncommitted_batches) - len(uncommitted_in_header) - len(uncommitted_created)}")
        
        STALL_THRESHOLD_SECONDS = 5 * 60
        
        # Hiển thị các batch đã include vào header nhưng chưa commit (quan trọng nhất)
        if uncommitted_in_header:
            print(f"\n🔴 CÁC BATCH ĐÃ INCLUDE VÀO HEADER NHƯNG CHƯA ĐƯỢC COMMIT:")
            print(f"   (Top {min(top_n, len(uncommitted_in_header))} batch mới nhất)\n")
            
            for i, (batch_digest, timeline) in enumerate(uncommitted_in_header[:top_n], 1):
                print(f"{i:3d}. Batch: {timeline.display_digest}")
                first_include_time = timeline.included_in_header_at
                first_include_round = timeline.included_in_header_round
                last_include_time = timeline.last_header_include_at or first_include_time
                last_include_round = timeline.last_header_include_round or first_include_round
                include_count = len(timeline.all_header_includes)
                
                if first_include_time is not None and first_include_round is not None:
                    if show_round_timestamps:
                        print(f"     📄 Lần include đầu (round {first_include_round}): {first_include_time.strftime('%H:%M:%S.%f')[:-3]}")
                    else:
                        print(f"     📄 Lần include đầu: round {first_include_round}")
                if include_count > 0:
                    if (
                        include_count > 1
                        and last_include_time is not None
                        and last_include_round is not None
                        and show_round_timestamps
                    ):
                        print(
                            f"     🔄 Tổng số lần include: {include_count} (lần cuối round {last_include_round} @ {last_include_time.strftime('%H:%M:%S.%f')[:-3]})"
                        )
                    else:
                        print(f"     🔄 Tổng số lần include: {include_count}")
                if last_log_timestamp and last_include_time:
                    pending_since_last_include = seconds_between(last_log_timestamp, last_include_time)
                    print(f"     ⏳  Chờ từ lần include cuối: {format_optional_duration(pending_since_last_include)} (tính tới log mới nhất)")
                
                if timeline.created_at:
                    print(f"     📅 Created: {timeline.created_at.strftime('%H:%M:%S.%f')[:-3]}")
                    time_create_to_include = seconds_between(first_include_time, timeline.created_at)
                    print(f"     ⏱️  Tạo → include đầu tiên: {format_optional_duration(time_create_to_include)}")
                    if last_log_timestamp:
                        waiting_since_create = seconds_between(last_log_timestamp, timeline.created_at)
                        print(f"     ⏳  Đang chờ kể từ khi tạo: {format_optional_duration(waiting_since_create)}")
                if timeline.received_by_primary_at:
                    print(f"     📥 Received by Primary: {timeline.received_by_primary_at.strftime('%H:%M:%S.%f')[:-3]}")
                    header_delay = seconds_between(first_include_time, timeline.received_by_primary_at)
                    print(f"        (+{format_optional_duration(header_delay)} từ lúc primary nhận tới lần include đầu)")
                if include_count > 1 and show_round_timestamps:
                    print("     📄 Lịch sử include gần nhất:")
                    for include_time, include_round in timeline.all_header_includes[-min(3, include_count):]:
                        print(f"        - Round {include_round}: {include_time.strftime('%H:%M:%S.%f')[:-3]}")
                
                print()
        
        # Hiển thị các batch đã tạo nhưng chưa include vào header
        if uncommitted_created:
            print(f"\n🟡 CÁC BATCH ĐÃ TẠO NHƯNG CHƯA ĐƯỢC INCLUDE VÀO HEADER:")
            print(f"   (Top {min(top_n, len(uncommitted_created))} batch mới nhất)\n")
            
            for i, (batch_digest, timeline) in enumerate(uncommitted_created[:top_n], 1):
                print(f"{i:3d}. Batch: {timeline.display_digest}")
                if timeline.created_at:
                    print(f"     📅 Created: {timeline.created_at.strftime('%H:%M:%S.%f')[:-3]}")
                    if timeline.created_round:
                        print(f"        (ước tính round ~{timeline.created_round})")
                    if last_log_timestamp:
                        waiting_since_create = seconds_between(last_log_timestamp, timeline.created_at)
                        print(f"     ⏳  Đang chờ kể từ khi tạo: {format_optional_duration(waiting_since_create)}")
                    if timeline.received_by_primary_at:
                        print(f"     📥 Received by Primary: {timeline.received_by_primary_at.strftime('%H:%M:%S.%f')[:-3]}")
                        primary_delay = timeline.get_primary_receive_delay()
                        if primary_delay:
                            print(f"        (+{format_duration(primary_delay)})")
                else:
                    if timeline.received_by_primary_at:
                        print(f"     📥 Received by Primary: {timeline.received_by_primary_at.strftime('%H:%M:%S.%f')[:-3]}")
                print()
        
        # Phân tích theo round
        if uncommitted_in_header:
            rounds_uncommitted = {}
            for batch_digest, timeline in uncommitted_in_header:
                if timeline.included_in_header_round:
                    round_num = timeline.included_in_header_round
                    if round_num not in rounds_uncommitted:
                        rounds_uncommitted[round_num] = []
                    rounds_uncommitted[round_num].append((batch_digest, timeline))
            
            if rounds_uncommitted:
                print(f"\n📊 PHÂN BỐ CÁC BATCH CHƯA COMMIT THEO ROUND:")
                sorted_rounds = sorted(rounds_uncommitted.keys())
                for round_num in sorted_rounds[:20]:  # Hiển thị 20 round đầu
                    count = len(rounds_uncommitted[round_num])
                    print(f"   - Round {round_num}: {count} batch(es) chưa commit")
                if len(sorted_rounds) > 20:
                    print(f"   ... và {len(sorted_rounds) - 20} round khác")
        
        stalled_batches: List[Tuple[str, BatchTimeline, float]] = []
        reference_time = last_log_timestamp
        if not reference_time:
            reference_time = max(
                (timeline.last_event_at for _, timeline in uncommitted_batches if timeline.last_event_at),
                default=None,
            )
        if reference_time:
            for batch_digest, timeline in uncommitted_batches:
                if not timeline.last_event_at:
                    continue
                wait_time = seconds_between(reference_time, timeline.last_event_at)
                if wait_time and wait_time >= STALL_THRESHOLD_SECONDS:
                    stalled_batches.append((batch_digest, timeline, wait_time))
        
        if stalled_batches:
            stalled_batches.sort(key=lambda x: x[2], reverse=True)
            print(f"\n{'='*100}")
            print("⏰ CÁC BATCH CHỜ COMMIT > 5 PHÚT")
            print(f"{'='*100}")
            print(f"\n📊 Tổng số batch: {len(stalled_batches)} (ngưỡng: 5 phút)\n")
            for i, (batch_digest, timeline, wait_time) in enumerate(stalled_batches[:top_n], 1):
                print(f"{i:3d}. Batch: {timeline.display_digest}")
                print(f"     ⏳  Thời gian chờ kể từ sự kiện cuối: {format_duration(wait_time)}")
                if timeline.last_header_include_at:
                    include_round = timeline.last_header_include_round or timeline.included_in_header_round
                    label = f"include header (round {include_round})" if include_round is not None else "include header"
                    print(f"     🕒 Sự kiện cuối: {label} @ {timeline.last_header_include_at.strftime('%H:%M:%S.%f')[:-3]}")
                elif timeline.received_by_primary_at:
                    print(f"     🕒 Sự kiện cuối: primary nhận batch @ {timeline.received_by_primary_at.strftime('%H:%M:%S.%f')[:-3]}")
                elif timeline.created_at:
                    print(f"     🕒 Sự kiện cuối: worker tạo batch @ {timeline.created_at.strftime('%H:%M:%S.%f')[:-3]}")
                print()
        
        sample_size = min(5, len(uncommitted_batches))
        if sample_size:
            print(f"\n🔬 MẪU BATCH CHƯA COMMIT (Top {sample_size} theo sự kiện gần nhất):\n")
            for i, (batch_digest, timeline) in enumerate(uncommitted_batches[:sample_size], 1):
                print(f"{i:3d}. Batch: {timeline.display_digest}")
                last_event_time = timeline.last_event_at
                last_event_label = "không rõ"
                if timeline.last_header_include_at:
                    round_info = timeline.last_header_include_round or timeline.included_in_header_round
                    if round_info is not None:
                        last_event_label = f"include header (round {round_info})"
                    else:
                        last_event_label = "include header"
                    if timeline.last_header_include_at:
                        last_event_time = timeline.last_header_include_at
                elif timeline.received_by_primary_at:
                    last_event_label = "primary nhận batch"
                    last_event_time = timeline.received_by_primary_at
                elif timeline.created_at:
                    last_event_label = "worker tạo batch"
                    last_event_time = timeline.created_at
                if last_event_time:
                    print(f"     🕒 Sự kiện cuối: {last_event_label} @ {last_event_time.strftime('%H:%M:%S.%f')[:-3]}")
                else:
                    print(f"     🕒 Sự kiện cuối: {last_event_label}")
                if last_log_timestamp:
                    waiting_since_last = seconds_between(last_log_timestamp, last_event_time)
                    print(f"     ⏳  Đang chờ kể từ sự kiện cuối: {format_optional_duration(waiting_since_last)}")
                if len(timeline.all_header_includes) > 0:
                    print(f"     🔄 Số lần include: {len(timeline.all_header_includes)}")
                if timeline.received_by_primary_at and not timeline.included_in_header_at:
                    time_received_to_now = seconds_between(last_log_timestamp, timeline.received_by_primary_at)
                    print(f"     ⏱️  Primary nhận → nay: {format_optional_duration(time_received_to_now)}")
                if timeline.created_at and not timeline.received_by_primary_at:
                    time_created_to_now = seconds_between(last_log_timestamp, timeline.created_at)
                    print(f"     ⏱️  Tạo → nay: {format_optional_duration(time_created_to_now)}")
                print()

        print(f"\n{'='*100}")
    
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
    
    # Thống kê tổng quan về commit rate
    if len(batches) > 0:
        commit_rate = (len(batches_with_delays) / len(batches)) * 100
        print(f"\n{'='*100}")
        print("📊 TỔNG QUAN VỀ TỶ LỆ COMMIT:")
        print(f"   - Tổng số batch đã được tạo hoặc include: {len(batches)}")
        print(f"   - Số batch đã commit: {len(batches_with_delays)}")
        print(f"   - Số batch chưa commit: {len(uncommitted_batches)}")
        print(f"   - Tỷ lệ commit: {commit_rate:.2f}%")
        if len(uncommitted_batches) > 0:
            uncommit_rate = (len(uncommitted_batches) / len(batches)) * 100
            print(f"   - Tỷ lệ chưa commit: {uncommit_rate:.2f}%")
    
    print(f"{'='*100}")


if __name__ == '__main__':
    main()
