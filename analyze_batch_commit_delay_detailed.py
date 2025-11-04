#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script phân tích chi tiết tại sao một batch cụ thể commit muộn.
Phân tích leader selection và consensus behavior.
"""

import re
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional, List, Tuple
from collections import defaultdict


def parse_timestamp(log_line: str) -> Optional[datetime]:
    """Parse timestamp từ log line"""
    pattern = r'\[(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z)'
    match = re.search(pattern, log_line)
    if match:
        try:
            ts_str = match.group(1)
            return datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
        except Exception:
            return None
    return None


def normalize_digest(digest: str) -> str:
    """Normalize digest bằng cách bỏ dấu = ở cuối nếu có"""
    return digest.rstrip('=')


def analyze_batch_commit_delay(batch_digest: str, primary_log_path: str, all_primary_logs: List[str] = None):
    """Phân tích chi tiết tại sao một batch commit muộn"""
    
    batch_digest_normalized = normalize_digest(batch_digest)
    
    # Pattern để tìm các sự kiện liên quan đến batch
    created_pattern = r'Created\s+B(\d+)\(([^)]+)\)\s+->\s+([A-Za-z0-9+/=]+)'
    committed_pattern = r'Committed\s+B(\d+)\(([^)]+)\)\s+->\s+([A-Za-z0-9+/=]+)'
    committing_leader_pattern = r'Committing leader at round (\d+) with stake (\d+)/(\d+)'
    
    # Lưu tất cả các sự kiện liên quan đến batch từ TẤT CẢ primaries
    batch_events = []
    leader_commits = []  # (round, timestamp, stake)
    
    # Phân tích từ primary log chính
    log_files = [primary_log_path]
    if all_primary_logs:
        log_files.extend(all_primary_logs)
    
    for log_file in log_files:
        try:
            with open(log_file, 'r', encoding='utf-8') as f:
                for line in f:
                    timestamp = parse_timestamp(line)
                    if not timestamp:
                        continue
                    
                    # Tìm khi batch được include vào header
                    match = re.search(created_pattern, line)
                    if match:
                        round_num = int(match.group(1))
                        primary_id = match.group(2)
                        batch_digest_raw = match.group(3)
                        if normalize_digest(batch_digest_raw) == batch_digest_normalized:
                            batch_events.append({
                                'type': 'created',
                                'round': round_num,
                                'primary': primary_id,
                                'timestamp': timestamp,
                                'log_file': log_file
                            })
                    
                    # Tìm khi batch được commit
                    match = re.search(committed_pattern, line)
                    if match:
                        round_num = int(match.group(1))
                        primary_id = match.group(2)
                        batch_digest_raw = match.group(3)
                        if normalize_digest(batch_digest_raw) == batch_digest_normalized:
                            batch_events.append({
                                'type': 'committed',
                                'round': round_num,
                                'primary': primary_id,
                                'timestamp': timestamp,
                                'log_file': log_file
                            })
                    
                    # Tìm khi leader được commit
                    match = re.search(committing_leader_pattern, line)
                    if match:
                        round_num = int(match.group(1))
                        stake = int(match.group(2))
                        total_stake = int(match.group(3))
                        leader_commits.append({
                            'round': round_num,
                            'timestamp': timestamp,
                            'stake': stake,
                            'total_stake': total_stake
                        })
        
        except FileNotFoundError:
            print(f"⚠️  Không tìm thấy file: {log_file}, bỏ qua...")
            continue
        except Exception as e:
            print(f"⚠️  Lỗi khi đọc log {log_file}: {e}, bỏ qua...")
            continue
    
    # Phân tích
    print(f"{'='*100}")
    print(f"🔍 PHÂN TÍCH CHI TIẾT BATCH: {batch_digest[:60]}...")
    print(f"{'='*100}\n")
    
    if not batch_events:
        print(f"❌ Không tìm thấy batch này trong log!")
        return
    
    # Tìm các lần include vào header
    includes = [e for e in batch_events if e['type'] == 'created']
    commits = [e for e in batch_events if e['type'] == 'committed']
    
    print(f"📊 Tổng số lần include vào header: {len(includes)}")
    print(f"📊 Số lần commit: {len(commits)}\n")
    
    # Phân tích xem có primary nào khác include batch này không
    primary_ids = set()
    for include_event in includes:
        primary_ids.add(include_event['primary'])
    
    if len(primary_ids) == 1:
        print(f"⚠️  QUAN TRỌNG: Chỉ có 1 primary ({(list(primary_ids)[0])[:20]}...) include batch này vào header!")
        print(f"   Các primary khác KHÔNG include batch này vào header của họ.\n")
    else:
        print(f"✅ Có {len(primary_ids)} primary khác nhau include batch này: {[p[:20] for p in primary_ids]}\n")
    
    # Phân tích từng lần include
    for i, include_event in enumerate(includes, 1):
        round_num = include_event['round']
        include_time = include_event['timestamp']
        primary_id = include_event['primary']
        
        print(f"{'='*100}")
        print(f"📄 Lần include thứ {i}: Round {round_num} (Primary: {primary_id[:20]}...)")
        print(f"   Timestamp: {include_time.strftime('%H:%M:%S.%f')[:-3]}")
        
        # Tìm leader commit gần nhất sau khi include
        committed_round = None
        commit_time = None
        for commit_event in commits:
            if commit_event['round'] == round_num:
                committed_round = commit_event['round']
                commit_time = commit_event['timestamp']
                break
        
        if committed_round:
            delay = (commit_time - include_time).total_seconds()
            print(f"   ✅ Đã commit tại round {committed_round}")
            print(f"   ⏱️  Delay: {delay:.2f}s")
        else:
            print(f"   ⚠️  CHƯA ĐƯỢC COMMIT ở round này")
            
            # Kiểm tra leader nào được commit ở round này
            leader_commit = None
            for lc in leader_commits:
                if lc['round'] == round_num:
                    leader_commit = lc
                    break
            
            if leader_commit:
                print(f"   📌 Leader được commit ở round {round_num}: {leader_commit['stake']}/{leader_commit['total_stake']} stake")
                print(f"   ⚠️  Primary {primary_id[:20]}... KHÔNG PHẢI là leader của round {round_num}")
            else:
                # Tìm leader commit gần nhất sau round này
                next_leader_commits = [lc for lc in leader_commits if lc['round'] > round_num]
                if next_leader_commits:
                    next_lc = next_leader_commits[0]
                    rounds_wait = next_lc['round'] - round_num
                    time_wait = (next_lc['timestamp'] - include_time).total_seconds()
                    print(f"   ⏱️  Phải đợi {rounds_wait} rounds ({time_wait:.2f}s) đến round {next_lc['round']} mới có leader commit")
                    
                    # Kiểm tra xem primary có là leader ở round commit không
                    final_commit = commits[0] if commits else None
                    if final_commit:
                        final_round = final_commit['round']
                        # Tìm leader commit cho round đó
                        for lc in leader_commits:
                            if lc['round'] == final_round or lc['round'] == final_round + 1:
                                print(f"   📌 Cuối cùng leader được commit ở round {lc['round']}: {lc['stake']}/{lc['total_stake']} stake")
                                if final_commit['primary'] == primary_id:
                                    print(f"   ✅ Primary {primary_id[:20]}... là leader của round {final_round}")
                                break
        
        print()
    
    # Tính toán leader selection theo round-robin
    if includes:
        print(f"{'='*100}")
        print("🔍 PHÂN TÍCH LEADER SELECTION")
        print(f"{'='*100}\n")
        
        # Theo Bullshark, leader được chọn bằng round % num_validators
        # Cần biết số lượng validators - từ log có thể estimate
        primary_ids = set()
        for event in batch_events:
            primary_ids.add(event['primary'])
        
        # Tìm các round của các includes
        include_rounds = [e['round'] for e in includes]
        final_commit_round = commits[0]['round'] if commits else None
        
        print(f"📊 Các round batch được include: {include_rounds}")
        if final_commit_round:
            print(f"📊 Round cuối cùng được commit: {final_commit_round}\n")
        
        # Tính leader cho mỗi round (giả sử có 5 validators dựa trên log)
        # Round % 5 sẽ cho biết leader index
        num_validators = 5  # Estimate từ committee size
        
        print(f"🔢 Giả sử có {num_validators} validators, leader được chọn bằng round % {num_validators}")
        print(f"📌 Primary ID của batch này: {includes[0]['primary'][:20]}...\n")
        
        for round_num in include_rounds:
            leader_index = round_num % num_validators
            print(f"   Round {round_num}: Leader index = {round_num} % {num_validators} = {leader_index}")
        
        if final_commit_round:
            leader_index = final_commit_round % num_validators
            print(f"   Round {final_commit_round} (commit): Leader index = {final_commit_round} % {num_validators} = {leader_index}")
        
        print(f"\n💡 Lý do commit muộn:")
        if len(primary_ids) == 1:
            print(f"   ⚠️  CHỈ CÓ 1 PRIMARY include batch này vào header:")
            print(f"      - Batch chỉ được gửi từ worker của primary này đến primary này")
            print(f"      - Các primary khác KHÔNG biết về batch này hoặc không include nó")
            print(f"      - Có thể do:")
            print(f"        * Batch chưa được sync từ worker của primary khác")
            print(f"        * Các primary khác có batch riêng và ưu tiên batch của họ")
            print(f"        * Proposer retry mechanism của primary này tự retry thay vì đợi primary khác")
        print(f"   - Batch được include vào header nhiều lần nhưng primary không phải là leader")
        print(f"   - Trong Bullshark, chỉ header của leader được commit")
        print(f"   - Batch phải đợi đến khi primary này là leader để được commit")
        print(f"   - Điều này phụ thuộc vào round-robin leader selection")
        
        # Phân tích các round sau khi batch được include lần đầu
        if includes:
            first_include_round = includes[0]['round']
            print(f"\n🔍 PHÂN TÍCH CÁC ROUND SAU KHI BATCH ĐƯỢC INCLUDE LẦN ĐẦU (Round {first_include_round}):")
            print(f"   Các round tiếp theo (4119-4125) có thể có leader khác include batch này:")
            
            # Tìm leader commits trong các round này
            next_rounds = list(range(first_include_round + 1, first_include_round + 8))
            for round_num in next_rounds:
                leader_commit = next((lc for lc in leader_commits if lc['round'] == round_num), None)
                if leader_commit:
                    leader_index = round_num % 5
                    print(f"   - Round {round_num}: Leader index = {leader_index} (đã commit)")
                else:
                    leader_index = round_num % 5
                    print(f"   - Round {round_num}: Leader index = {leader_index} (chưa thấy commit)")
            
            print(f"\n   💡 Vấn đề: Các primary khác (leader index 0, 2, 3, 4) KHÔNG include batch này vào header của họ")
            print(f"      Ngay cả khi họ là leader ở các round tiếp theo!")
    
    print(f"\n{'='*100}")


def main():
    """Hàm chính"""
    if len(sys.argv) < 3:
        print("📝 Sử dụng: python analyze_batch_commit_delay_detailed.py <batch_digest> <primary_log_path> [other_primary_logs...]")
        print("📝 Ví dụ: python analyze_batch_commit_delay_detailed.py VJbcTbj16SMseeQSlHlgCzeofRCsaVXfg4K9askgG/0= benchmark/logs/primary-0.log")
        print("📝 Hoặc với nhiều log: python analyze_batch_commit_delay_detailed.py <digest> primary-0.log primary-1.log primary-4.log")
        sys.exit(1)
    
    batch_digest = sys.argv[1]
    primary_log_path = sys.argv[2]
    other_logs = sys.argv[3:] if len(sys.argv) > 3 else None
    
    analyze_batch_commit_delay(batch_digest, primary_log_path, other_logs)


if __name__ == '__main__':
    main()

