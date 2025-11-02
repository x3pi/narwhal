#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Phân tích leader selection và tại sao batch không được commit ở leader round ngay sau đó
"""

import re
from pathlib import Path

def analyze_leader_selection(primary_log_path: str, batch_digest: str):
    """Phân tích tại sao batch không được commit ngay"""
    
    created_pattern = r'Created\s+B(\d+)\(([^)]+)\)\s+->\s+([A-Za-z0-9+/=]+)'
    committed_pattern = r'Committed\s+B(\d+)\(([^)]+)\)'
    batch_committed_pattern = r'\[BATCH_COMMITTED\]\s+Batch\s+([A-Za-z0-9+/=]+)\s+committed\s+at\s+round\s+(\d+)'
    
    # Track headers created
    headers_created = {}  # round -> (author, batch_digest)
    headers_committed = {}  # round -> author (leader)
    
    # Committee size
    committee_size = 5  # From config
    
    print(f"🔍 Phân tích batch: {batch_digest[:50]}...\n")
    
    try:
        with open(primary_log_path, 'r', encoding='utf-8') as f:
            for line in f:
                # Track headers created
                match = re.search(created_pattern, line)
                if match:
                    round_num = int(match.group(1))
                    author = match.group(2)
                    digest = match.group(3)
                    headers_created[round_num] = (author, digest)
                
                # Track headers committed (leader)
                match = re.search(committed_pattern, line)
                if match:
                    round_num = int(match.group(1))
                    author = match.group(2)
                    headers_committed[round_num] = author
    except Exception as e:
        print(f"❌ Lỗi: {e}")
        return
    
    # Tìm rounds liên quan đến batch
    batch_rounds = []
    for round_num, (author, digest) in headers_created.items():
        if digest == batch_digest:
            batch_rounds.append((round_num, author))
    
    batch_rounds.sort()
    
    print(f"📊 Rounds batch được include:")
    for round_num, author in batch_rounds:
        is_leader_round = round_num % 2 == 0
        leader_round = round_num if is_leader_round else round_num + 1
        expected_leader = f"Authority{leader_round % committee_size}"
        
        print(f"  Round {round_num} ({'Leader' if is_leader_round else 'Non-Leader'}):")
        print(f"    - Author: {author}")
        print(f"    - Expected leader for round {leader_round}: {expected_leader} (round % {committee_size} = {leader_round % committee_size})")
        
        # Check nếu round này được commit
        if leader_round in headers_committed:
            committed_leader = headers_committed[leader_round]
            print(f"    - Actual committed leader at round {leader_round}: {committed_leader}")
            if committed_leader != author:
                print(f"    ⚠️  AUTHOR MISMATCH! Batch author không phải là leader của round {leader_round}")
                print(f"       → Batch không được commit ở round {leader_round}")
            else:
                print(f"    ✅ Author match - nhưng batch vẫn không commit?")
        else:
            print(f"    - Round {leader_round} chưa được commit")
        print()
    
    # Phân tích leader selection
    print(f"\n{'='*80}")
    print(f"📊 PHÂN TÍCH LEADER SELECTION:\n")
    print(f"Trong Bullshark, leader được chọn theo: round % committee_size")
    print(f"Committee size: {committee_size}\n")
    
    # Tính toán leader cho các rounds quan trọng
    important_rounds = []
    for round_num, author in batch_rounds:
        leader_round = round_num if round_num % 2 == 0 else round_num + 1
        important_rounds.append(leader_round)
    
    print(f"Leader cho các rounds liên quan:")
    for leader_round in sorted(set(important_rounds)):
        expected_leader_idx = leader_round % committee_size
        actual_leader = headers_committed.get(leader_round, "N/A")
        print(f"  Round {leader_round} (leader round):")
        print(f"    - Expected leader index: {expected_leader_idx} (round % {committee_size})")
        print(f"    - Actual leader: {actual_leader}")
        print(f"    - Batch author in rounds: {[a for r, a in batch_rounds if (r if r%2==0 else r+1) == leader_round]}")
        print()
    
    print(f"\n{'='*80}")
    print(f"💡 GIẢI THÍCH:")
    print(f"   Trong Bullshark, chỉ HEADER CỦA LEADER được commit ở leader round.")
    print(f"   Nếu batch được include vào header của non-leader node ở round 443,")
    print(f"   và leader của round 444 KHÔNG phải là node đó, thì batch KHÔNG được commit.")
    print(f"   Batch phải đợi đến khi:")
    print(f"   1. Node tạo header là leader ở một leader round khác, HOẶC")
    print(f"   2. Batch được requeue và include vào header của leader round")
    print(f"{'='*80}")


if __name__ == '__main__':
    import sys
    
    script_dir = Path(__file__).parent
    primary_log_path = script_dir / 'benchmark' / 'logs' / 'primary-0.log'
    
    if len(sys.argv) > 1:
        primary_log_path = Path(sys.argv[1])
    
    batch_digest = "r/xYK6NvHjGFXjORIXCel/jvGp5oWc3UO7JuIo0gg5k="
    
    analyze_leader_selection(str(primary_log_path), batch_digest)

