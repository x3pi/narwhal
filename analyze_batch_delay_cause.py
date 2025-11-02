#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script phân tích nguyên nhân delay của batch - tìm các batch được gửi nhiều lần trong header
"""

import re
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Set, Tuple
from collections import defaultdict


def parse_timestamp(log_line: str) -> Tuple[datetime, str]:
    """Parse timestamp từ log line"""
    pattern = r'\[(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z)\]\s+(\w+)'
    match = re.search(pattern, log_line)
    if match:
        try:
            ts_str = match.group(1)
            level = match.group(2)
            timestamp = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
            return timestamp, level
        except Exception:
            pass
    return None, None


def analyze_batch_timeline(primary_log_path: str, batch_digest: str):
    """Phân tích timeline chi tiết của một batch"""
    
    # Pattern: Created B47(...) -> batch_digest=
    created_pattern = r'Created\s+B(\d+)\([^)]+\)\s+->\s+([A-Za-z0-9+/=]+)'
    
    # Pattern: [BATCH_COMMITTED] Batch batch_digest= committed at round X
    committed_pattern = r'\[BATCH_COMMITTED\]\s+Batch\s+([A-Za-z0-9+/=]+)\s+committed\s+at\s+round\s+(\d+)'
    
    # Pattern: Committed B47(...) -> batch_digest=
    consensus_committed_pattern = r'Committed\s+B(\d+)\([^)]+\)\s+->\s+([A-Za-z0-9+/=]+)'
    
    batch_events = []
    
    try:
        with open(primary_log_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                timestamp, level = parse_timestamp(line)
                if not timestamp:
                    continue
                
                # Tìm batch được include vào header
                match = re.search(created_pattern, line)
                if match:
                    round_num = int(match.group(1))
                    digest = match.group(2)
                    if digest == batch_digest:
                        batch_events.append({
                            'type': 'included_in_header',
                            'round': round_num,
                            'timestamp': timestamp,
                            'line': line_num,
                            'log': line.strip()
                        })
                
                # Tìm batch được commit (từ garbage collector)
                match = re.search(committed_pattern, line)
                if match:
                    digest = match.group(1)
                    round_num = int(match.group(2))
                    if digest == batch_digest:
                        batch_events.append({
                            'type': 'committed',
                            'round': round_num,
                            'timestamp': timestamp,
                            'line': line_num,
                            'log': line.strip()
                        })
                
                # Tìm batch được commit (từ consensus)
                match = re.search(consensus_committed_pattern, line)
                if match:
                    round_num = int(match.group(1))
                    digest = match.group(2)
                    if digest == batch_digest:
                        batch_events.append({
                            'type': 'consensus_committed',
                            'round': round_num,
                            'timestamp': timestamp,
                            'line': line_num,
                            'log': line.strip()
                        })
    
    except FileNotFoundError:
        print(f"❌ Không tìm thấy file: {primary_log_path}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Lỗi khi đọc primary log: {e}")
        sys.exit(1)
    
    # Sắp xếp theo thời gian
    batch_events.sort(key=lambda x: x['timestamp'])
    
    return batch_events


def analyze_all_batches(primary_log_path: str):
    """Phân tích tất cả batches để tìm những batch được include nhiều lần"""
    
    created_pattern = r'Created\s+B(\d+)\([^)]+\)\s+->\s+([A-Za-z0-9+/=]+)'
    committed_pattern = r'\[BATCH_COMMITTED\]\s+Batch\s+([A-Za-z0-9+/=]+)\s+committed\s+at\s+round\s+(\d+)'
    
    batch_includes: Dict[str, List[Tuple[int, datetime]]] = defaultdict(list)
    batch_commits: Dict[str, Tuple[int, datetime]] = {}
    
    try:
        with open(primary_log_path, 'r', encoding='utf-8') as f:
            for line in f:
                timestamp, _ = parse_timestamp(line)
                if not timestamp:
                    continue
                
                # Track includes
                match = re.search(created_pattern, line)
                if match:
                    round_num = int(match.group(1))
                    digest = match.group(2)
                    batch_includes[digest].append((round_num, timestamp))
                    # Debug: log để kiểm tra
                    if 'r/xYK6NvHjGFXjORIXCel' in digest:
                        print(f"DEBUG: Found include at round {round_num}: {digest[:50]}")
                
                # Track commits
                match = re.search(committed_pattern, line)
                if match:
                    digest = match.group(1)
                    round_num = int(match.group(2))
                    batch_commits[digest] = (round_num, timestamp)
    
    except Exception as e:
        print(f"❌ Lỗi: {e}")
        sys.exit(1)
    
    # Tìm batches được include nhiều lần
    batches_with_multiple_includes = {
        digest: includes for digest, includes in batch_includes.items()
        if len(includes) > 1
    }
    
    # Tính delay cho mỗi batch
    batch_delays = []
    for digest, includes in batches_with_multiple_includes.items():
        commit_info = batch_commits.get(digest)
        if commit_info:
            commit_round, commit_time = commit_info
            # Tìm lần include cuối cùng trước khi commit
            last_include_before_commit = None
            for include_round, include_time in includes:
                if include_round <= commit_round:
                    if last_include_before_commit is None or include_time > last_include_before_commit[1]:
                        last_include_before_commit = (include_round, include_time)
            
            if last_include_before_commit:
                delay = (commit_time - last_include_before_commit[1]).total_seconds()
                batch_delays.append({
                    'digest': digest,
                    'includes': includes,
                    'commit_round': commit_round,
                    'commit_time': commit_time,
                    'last_include_round': last_include_before_commit[0],
                    'last_include_time': last_include_before_commit[1],
                    'delay': delay,
                    'num_includes': len(includes)
                })
    
    # Sắp xếp theo delay
    batch_delays.sort(key=lambda x: x['delay'], reverse=True)
    
    return batch_delays, batch_includes, batch_commits


def main():
    """Hàm chính"""
    script_dir = Path(__file__).parent
    primary_log_path = script_dir / 'benchmark' / 'logs' / 'primary-0.log'
    
    args = sys.argv[1:]
    if len(args) >= 1:
        primary_log_path = Path(args[0])
    
    print(f"📂 Đang phân tích primary log: {primary_log_path}")
    print(f"{'='*100}\n")
    
    # Phân tích tất cả batches
    batch_delays, batch_includes, batch_commits = analyze_all_batches(str(primary_log_path))
    
    print(f"🔍 Tìm thấy {len(batch_delays)} batch(es) được include nhiều lần trong header:\n")
    
    # Hiển thị top 10 batches có delay cao nhất
    print(f"📊 Top 10 batches có delay cao nhất do được include nhiều lần:\n")
    for i, batch_info in enumerate(batch_delays[:10], 1):
        digest = batch_info['digest']
        print(f"{i:2d}. Batch: {digest[:50]}...")
        print(f"     ⏱️  Delay từ lần include cuối đến commit: {batch_info['delay']:.2f}s")
        print(f"     🔢 Số lần include: {batch_info['num_includes']}")
        print(f"     📅 Lần include đầu: Round {batch_info['includes'][0][0]} tại {batch_info['includes'][0][1].strftime('%H:%M:%S.%f')[:-3]}")
        if len(batch_info['includes']) > 1:
            print(f"     📅 Lần include cuối: Round {batch_info['last_include_round']} tại {batch_info['last_include_time'].strftime('%H:%M:%S.%f')[:-3]}")
        print(f"     ✅ Committed tại round {batch_info['commit_round']}: {batch_info['commit_time'].strftime('%H:%M:%S.%f')[:-3]}")
        print()
    
    # Phân tích chi tiết batch có delay cao nhất
    if batch_delays:
        worst_batch = batch_delays[0]
        print(f"\n{'='*100}")
        print(f"🔍 PHÂN TÍCH CHI TIẾT BATCH CÓ DELAY CAO NHẤT:")
        print(f"{'='*100}\n")
        print(f"Batch: {worst_batch['digest']}\n")
        
        events = analyze_batch_timeline(str(primary_log_path), worst_batch['digest'])
        
        print(f"Timeline:")
        for event in events:
            event_type = event['type'].replace('_', ' ').title()
            print(f"  [{event['timestamp'].strftime('%H:%M:%S.%f')[:-3]}] Round {event['round']}: {event_type}")
        
        print(f"\n📊 Phân tích:")
        print(f"   - Batch được include {len(worst_batch['includes'])} lần trong header")
        print(f"   - Delay từ lần include cuối đến commit: {worst_batch['delay']:.2f}s")
        
        # Tính delay giữa các lần include
        includes = worst_batch['includes']
        if len(includes) > 1:
            print(f"\n   Delay giữa các lần include:")
            for i in range(len(includes) - 1):
                delay = (includes[i+1][1] - includes[i][1]).total_seconds()
                print(f"     Round {includes[i][0]} -> Round {includes[i+1][0]}: {delay:.2f}s")
        
        # Tìm rounds giữa lần include cuối và commit
        last_include_round = worst_batch['last_include_round']
        commit_round = worst_batch['commit_round']
        rounds_between = commit_round - last_include_round
        print(f"\n   - Rounds giữa lần include cuối (round {last_include_round}) và commit (round {commit_round}): {rounds_between} rounds")
        
        if rounds_between == 0:
            print(f"   ✅ Batch được commit trong cùng round với lần include cuối (đây là hành vi bình thường)")
        else:
            print(f"   ⚠️  Batch được commit {rounds_between} rounds SAU lần include cuối - có thể do header không được vote hoặc bị reject")
    
    # Thống kê
    if batch_delays:
        delays = [b['delay'] for b in batch_delays]
        num_includes_list = [b['num_includes'] for b in batch_delays]
        
        print(f"\n{'='*100}")
        print(f"📊 THỐNG KÊ:")
        print(f"   - Tổng số batches được include nhiều lần: {len(batch_delays)}")
        print(f"   - Delay trung bình: {sum(delays)/len(delays):.2f}s")
        print(f"   - Delay tối đa: {max(delays):.2f}s")
        print(f"   - Số lần include trung bình: {sum(num_includes_list)/len(num_includes_list):.1f}")
        print(f"   - Số lần include tối đa: {max(num_includes_list)}")
    
    print(f"\n{'='*100}")
    print(f"💡 GIẢI THÍCH:")
    print(f"   Batch bị include nhiều lần thường do:")
    print(f"   1. Header được tạo nhưng không được vote/quorum")
    print(f"   2. Header bị reject hoặc bị skip trong consensus")
    print(f"   3. Batch bị requeue sau khi timeout (InFlight -> Pending)")
    print(f"   4. Network delay hoặc node chậm trong quá trình vote")
    print(f"{'='*100}")


if __name__ == '__main__':
    main()

