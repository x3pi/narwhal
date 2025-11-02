#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script phân tích nguyên nhân gốc rễ của batch delay - phân tích leader rounds
"""

import re
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Set, Tuple
from collections import defaultdict


def parse_timestamp(log_line: str) -> datetime:
    """Parse timestamp từ log line"""
    pattern = r'\[(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z)'
    match = re.search(pattern, log_line)
    if match:
        try:
            ts_str = match.group(1)
            return datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
        except Exception:
            pass
    return None


def is_leader_round(round_num: int) -> bool:
    """Trong Bullshark, leader rounds là các rounds chẵn"""
    return round_num % 2 == 0


def analyze_batch_delay(primary_log_path: str, worker_log_path: str = None):
    """Phân tích nguyên nhân delay của batches"""
    
    created_pattern = r'Created\s+B(\d+)\(([^)]+)\)\s+->\s+([A-Za-z0-9+/=]+)'
    committed_pattern = r'\[BATCH_COMMITTED\]\s+Batch\s+([A-Za-z0-9+/=]+)\s+committed\s+at\s+round\s+(\d+)'
    consensus_committed_pattern = r'Committed\s+B(\d+)\([^)]+\)\s+->\s+([A-Za-z0-9+/=]+)'
    
    # Track batch includes và commits
    batch_includes: Dict[str, List[Tuple[int, datetime, str]]] = defaultdict(list)  # digest -> [(round, time, author)]
    batch_commits: Dict[str, Tuple[int, datetime]] = {}  # digest -> (round, time)
    
    # Track từ worker log
    batch_created: Dict[str, datetime] = {}  # digest -> created_time
    
    # Parse worker log nếu có
    if worker_log_path:
        try:
            batch_created_pattern = r'\[BATCH_CREATED\]\s+Batch\s+([A-Za-z0-9+/=]+)\s+created'
            with open(worker_log_path, 'r', encoding='utf-8') as f:
                for line in f:
                    match = re.search(batch_created_pattern, line)
                    if match:
                        digest = match.group(1)
                        timestamp = parse_timestamp(line)
                        if timestamp:
                            batch_created[digest] = timestamp
        except Exception as e:
            print(f"⚠️  Không thể đọc worker log: {e}")
    
    # Parse primary log
    try:
        with open(primary_log_path, 'r', encoding='utf-8') as f:
            for line in f:
                timestamp = parse_timestamp(line)
                if not timestamp:
                    continue
                
                # Track includes
                match = re.search(created_pattern, line)
                if match:
                    round_num = int(match.group(1))
                    author = match.group(2)
                    digest = match.group(3)
                    batch_includes[digest].append((round_num, timestamp, author))
                
                # Track commits
                match = re.search(committed_pattern, line)
                if match:
                    digest = match.group(1)
                    round_num = int(match.group(2))
                    batch_commits[digest] = (round_num, timestamp)
                
                # Track consensus commits
                match = re.search(consensus_committed_pattern, line)
                if match:
                    round_num = int(match.group(1))
                    digest = match.group(2)
                    if digest not in batch_commits:
                        batch_commits[digest] = (round_num, timestamp)
    
    except Exception as e:
        print(f"❌ Lỗi khi đọc log: {e}")
        sys.exit(1)
    
    # Phân tích các batches có delay cao
    batch_analyses = []
    
    for digest, commits in batch_commits.items():
        commit_round, commit_time = commits
        includes = batch_includes.get(digest, [])
        created_time = batch_created.get(digest)
        
        if not includes:
            continue
        
        # Tìm lần include cuối cùng trước khi commit
        last_include = None
        first_include = includes[0]
        
        for include_round, include_time, include_author in includes:
            if include_round <= commit_round:
                if last_include is None or include_time > last_include[1]:
                    last_include = (include_round, include_time, include_author)
        
        if last_include:
            last_round, last_time, last_author = last_include
            delay = (commit_time - last_time).total_seconds()
            round_delay = commit_round - last_round
            
            # Tính delay từ khi tạo (nếu có)
            creation_delay = None
            if created_time:
                creation_delay = (commit_time - created_time).total_seconds()
            
            # Kiểm tra xem có phải leader round không
            is_last_include_leader = is_leader_round(last_round)
            is_commit_leader = is_leader_round(commit_round)
            
            batch_analyses.append({
                'digest': digest,
                'first_include_round': first_include[0],
                'first_include_time': first_include[1],
                'last_include_round': last_round,
                'last_include_time': last_time,
                'last_include_author': last_author,
                'is_last_include_leader': is_last_include_leader,
                'commit_round': commit_round,
                'commit_time': commit_time,
                'is_commit_leader': is_commit_leader,
                'delay_from_last_include': delay,
                'round_delay': round_delay,
                'creation_delay': creation_delay,
                'num_includes': len(includes)
            })
    
    # Sắp xếp theo delay
    batch_analyses.sort(key=lambda x: x['delay_from_last_include'], reverse=True)
    
    return batch_analyses


def main():
    """Hàm chính"""
    script_dir = Path(__file__).parent
    primary_log_path = script_dir / 'benchmark' / 'logs' / 'primary-0.log'
    worker_log_path = script_dir / 'benchmark' / 'logs' / 'worker-0-0.log'
    
    args = sys.argv[1:]
    if len(args) >= 1:
        primary_log_path = Path(args[0])
    if len(args) >= 2:
        worker_log_path = Path(args[1])
    
    print(f"📂 Đang phân tích logs:")
    print(f"   Primary: {primary_log_path}")
    print(f"   Worker: {worker_log_path}")
    print(f"{'='*100}\n")
    
    batch_analyses = analyze_batch_delay(str(primary_log_path), str(worker_log_path) if worker_log_path.exists() else None)
    
    # Lọc các batches có delay cao (> 1s)
    high_delay_batches = [b for b in batch_analyses if b['delay_from_last_include'] > 1.0]
    
    print(f"🔍 Tìm thấy {len(high_delay_batches)} batch(es) có delay > 1s:\n")
    
    # Phân loại nguyên nhân
    non_leader_round_cause = []
    other_causes = []
    
    for batch in high_delay_batches:
        if not batch['is_last_include_leader'] and batch['is_commit_leader']:
            non_leader_round_cause.append(batch)
        else:
            other_causes.append(batch)
    
    print(f"📊 Phân loại nguyên nhân:")
    print(f"   - Batch được include vào non-leader round: {len(non_leader_round_cause)}")
    print(f"   - Nguyên nhân khác: {len(other_causes)}\n")
    
    # Hiển thị top batches
    print(f"📊 Top 10 batches có delay cao nhất:\n")
    for i, batch in enumerate(high_delay_batches[:10], 1):
        digest = batch['digest']
        print(f"{i:2d}. Batch: {digest[:50]}...")
        print(f"     ⏱️  Delay từ lần include cuối đến commit: {batch['delay_from_last_include']:.2f}s")
        if batch['creation_delay']:
            print(f"     ⏱️  Delay từ khi tạo đến commit: {batch['creation_delay']:.2f}s")
        print(f"     🔢 Round delay: {batch['round_delay']} rounds")
        print(f"     📅 Lần include đầu: Round {batch['first_include_round']} ({'Leader' if is_leader_round(batch['first_include_round']) else 'Non-Leader'}) tại {batch['first_include_time'].strftime('%H:%M:%S.%f')[:-3]}")
        print(f"     📅 Lần include cuối: Round {batch['last_include_round']} ({'Leader' if batch['is_last_include_leader'] else 'Non-Leader'}) tại {batch['last_include_time'].strftime('%H:%M:%S.%f')[:-3]}")
        print(f"     ✅ Committed tại round {batch['commit_round']} ({'Leader' if batch['is_commit_leader'] else 'Non-Leader'}): {batch['commit_time'].strftime('%H:%M:%S.%f')[:-3]}")
        print(f"     🔢 Số lần include: {batch['num_includes']}")
        
        # Đánh giá nguyên nhân
        if not batch['is_last_include_leader'] and batch['is_commit_leader']:
            print(f"     ⚠️  NGUYÊN NHÂN: Batch được include vào non-leader round {batch['last_include_round']}, phải đợi đến leader round {batch['commit_round']} mới commit")
        elif batch['num_includes'] > 1:
            print(f"     ⚠️  NGUYÊN NHÂN: Batch được include nhiều lần ({batch['num_includes']} lần)")
        else:
            print(f"     ⚠️  NGUYÊN NHÂN: Chưa xác định rõ")
        print()
    
    # Thống kê
    if high_delay_batches:
        delays = [b['delay_from_last_include'] for b in high_delay_batches]
        round_delays = [b['round_delay'] for b in high_delay_batches]
        
        print(f"{'='*100}")
        print(f"📊 THỐNG KÊ:")
        print(f"   - Tổng số batches có delay > 1s: {len(high_delay_batches)}")
        print(f"   - Delay trung bình: {sum(delays)/len(delays):.2f}s")
        print(f"   - Delay tối đa: {max(delays):.2f}s")
        print(f"   - Round delay trung bình: {sum(round_delays)/len(round_delays):.1f} rounds")
        print(f"   - Round delay tối đa: {max(round_delays)} rounds")
        
        non_leader_count = sum(1 for b in high_delay_batches if not b['is_last_include_leader'] and b['is_commit_leader'])
        print(f"\n   - Batches bị delay do non-leader round: {non_leader_count} ({non_leader_count/len(high_delay_batches)*100:.1f}%)")
    
    print(f"\n{'='*100}")
    print(f"💡 GIẢI THÍCH:")
    print(f"   Trong Bullshark consensus:")
    print(f"   - Chỉ có LEADER ROUNDS (rounds chẵn: 0, 2, 4, 6, ...) mới được commit")
    print(f"   - NON-LEADER ROUNDS (rounds lẻ: 1, 3, 5, 7, ...) được tạo nhưng KHÔNG được commit")
    print(f"   - Nếu batch được include vào non-leader round, nó phải đợi đến leader round tiếp theo mới commit")
    print(f"   - Đây là nguyên nhân chính gây delay ở local (không có network delay)")
    print(f"{'='*100}")


if __name__ == '__main__':
    main()

