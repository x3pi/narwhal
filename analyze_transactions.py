#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Chương trình phân tích log để đối chiếu các giao dịch được gửi tới và được đưa vào batch nào,
và batch đó có được commit không. Mục đích là để theo dõi từng giao dịch xem có giao dịch nào chưa được commit.
"""

import re
import sys
from pathlib import Path
from typing import Dict, Set, List, Optional
from collections import defaultdict


class TransactionTracker:
    """Theo dõi trạng thái của từng transaction"""
    
    def __init__(self, tx_id: int):
        self.tx_id = tx_id
        self.received = False
        self.added_to_batch = False
        self.batch_digest: Optional[str] = None
        self.batch_created = False
        self.batch_committed = False
        self.commit_round: Optional[int] = None
        self.certificate_id: Optional[str] = None
        
    def is_fully_committed(self) -> bool:
        """Kiểm tra xem transaction có được commit hoàn toàn không"""
        return (self.received and 
                self.added_to_batch and 
                self.batch_created and 
                self.batch_committed)
    
    def get_status_summary(self) -> str:
        """Lấy tóm tắt trạng thái"""
        parts = []
        if self.received:
            parts.append("✓ Received")
        else:
            parts.append("✗ Not Received")
            
        if self.added_to_batch:
            parts.append("✓ In Batch")
        else:
            parts.append("✗ Not In Batch")
            
        if self.batch_created:
            parts.append("✓ Batch Created")
        else:
            parts.append("✗ Batch Not Created")
            
        if self.batch_committed:
            parts.append(f"✓ Committed (round {self.commit_round})")
        else:
            parts.append("✗ Not Committed")
            
        return " | ".join(parts)


def parse_worker_log(worker_log_path: str) -> tuple[Dict[str, TransactionTracker], Dict[str, Set[str]]]:
    """
    Đọc worker log và trích xuất:
    1. Thông tin về từng transaction (sử dụng tx_hash làm key)
    2. Mapping từ batch digest đến danh sách transaction hashes
    """
    transactions: Dict[str, TransactionTracker] = {}
    batch_to_txs: Dict[str, Set[str]] = {}
    
    # Patterns - updated to support tx_hash (base64 encoded Digest, 32 bytes của SHA-512)
    # Pattern 1: Transaction with original ID and tx_hash: "Transaction 167946 (tx_hash: ABC...) received"
    tx_received_pattern1 = r'\[TX_RECEIVED\]\s+Transaction\s+(\d+)\s+\(tx_hash:\s+([A-Za-z0-9+/=]+)\)\s+received'
    # Pattern 2: Transaction with only tx_hash: "Transaction (tx_hash: ABC...) received"
    tx_received_pattern2 = r'\[TX_RECEIVED\]\s+Transaction\s+\(tx_hash:\s+([A-Za-z0-9+/=]+)\)\s+received'
    
    # Pattern for TX_ADDED_TO_BATCH - similar format
    tx_added_pattern1 = r'\[TX_ADDED_TO_BATCH\]\s+Transaction\s+(\d+)\s+\(tx_hash:\s+([A-Za-z0-9+/=]+)\)\s+added'
    tx_added_pattern2 = r'\[TX_ADDED_TO_BATCH\]\s+Transaction\s+\(tx_hash:\s+([A-Za-z0-9+/=]+)\)\s+added'
    
    # Pattern for BATCH_CREATED - now uses tx_info format like "167946[hash:ABC...]"
    # Sử dụng non-greedy .+? để match đến ], bỏ qua các ] bên trong quotes
    batch_created_pattern = r'\[BATCH_CREATED\]\s+Batch\s+([A-Za-z0-9+/=]+)\s+created\s+with\s+\d+\s+transactions\s+\(tx_info:\s+\[(.+?)\],'
    
    try:
        with open(worker_log_path, 'r', encoding='utf-8') as f:
            for line in f:
                # Parse TX_RECEIVED with original ID and tx_hash
                match = re.search(tx_received_pattern1, line)
                if match:
                    orig_tx_id = int(match.group(1))
                    tx_hash = match.group(2)  # Base64 encoded hash
                    # Use tx_hash as primary key (cùng nội dung = cùng hash)
                    if tx_hash not in transactions:
                        # Sử dụng hash của tx_hash làm ID số để tương thích với TransactionTracker
                        tx_id = abs(hash(tx_hash)) % (10**15)  # Giới hạn để tránh overflow
                        transactions[tx_hash] = TransactionTracker(tx_id)
                        transactions[tx_hash].batch_digest = tx_hash  # Lưu hash vào batch_digest
                    transactions[tx_hash].received = True
                else:
                    # Parse TX_RECEIVED with only tx_hash
                    match = re.search(tx_received_pattern2, line)
                    if match:
                        tx_hash = match.group(1)
                        if tx_hash not in transactions:
                            tx_id = abs(hash(tx_hash)) % (10**15)
                            transactions[tx_hash] = TransactionTracker(tx_id)
                            transactions[tx_hash].batch_digest = tx_hash
                        transactions[tx_hash].received = True
                
                # Parse TX_ADDED_TO_BATCH with original ID and tx_hash
                match = re.search(tx_added_pattern1, line)
                if match:
                    orig_tx_id = int(match.group(1))
                    tx_hash = match.group(2)
                    if tx_hash not in transactions:
                        tx_id = abs(hash(tx_hash)) % (10**15)
                        transactions[tx_hash] = TransactionTracker(tx_id)
                        transactions[tx_hash].batch_digest = tx_hash
                    transactions[tx_hash].added_to_batch = True
                else:
                    # Parse TX_ADDED_TO_BATCH with only tx_hash
                    match = re.search(tx_added_pattern2, line)
                    if match:
                        tx_hash = match.group(1)
                        if tx_hash not in transactions:
                            tx_id = abs(hash(tx_hash)) % (10**15)
                            transactions[tx_hash] = TransactionTracker(tx_id)
                            transactions[tx_hash].batch_digest = tx_hash
                        transactions[tx_hash].added_to_batch = True
                
                # Parse BATCH_CREATED - format: "tx_info: [167946[hash:ABC...], [hash:XYZ...]]"
                match = re.search(batch_created_pattern, line)
                if match:
                    batch_digest = match.group(1)
                    tx_info_str = match.group(2)
                    
                    # Parse transaction info - format: ["167946[hash:ABC...]", "[hash:XYZ...]"]
                    tx_hashes = []
                    if tx_info_str.strip():
                        # Remove outer brackets nếu có
                        tx_info_str = tx_info_str.strip('[]').strip()
                        # Split by comma, nhưng giữ nguyên quotes bên trong
                        # Tìm tất cả các hash trong format [hash:...]
                        hash_matches = re.findall(r'\[hash:([A-Za-z0-9+/=]+)\]', tx_info_str)
                        for tx_hash in hash_matches:
                            tx_hashes.append(tx_hash)
                    
                    batch_to_txs[batch_digest] = set(tx_hashes)
                    
                    # Update transaction trackers
                    for tx_hash in tx_hashes:
                        if tx_hash not in transactions:
                            tx_id = abs(hash(tx_hash)) % (10**15)
                            transactions[tx_hash] = TransactionTracker(tx_id)
                            transactions[tx_hash].batch_digest = tx_hash
                        transactions[tx_hash].batch_digest = batch_digest
                        transactions[tx_hash].batch_created = True
                        if not transactions[tx_hash].received:
                            transactions[tx_hash].received = True  # Assume received if in batch
                        if not transactions[tx_hash].added_to_batch:
                            transactions[tx_hash].added_to_batch = True  # Assume added if in batch
                            
    except FileNotFoundError:
        print(f"❌ Không tìm thấy file: {worker_log_path}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Lỗi khi đọc worker log: {e}")
        sys.exit(1)
    
    return transactions, batch_to_txs


def parse_primary_log(primary_log_path: str, transactions: Dict[str, TransactionTracker], batch_to_txs: Dict[str, Set[str]]):
    """
    Đọc primary log để cập nhật thông tin về batches đã commit
    """
    # Pattern: [BATCH_COMMITTED] Batch DIGEST committed at round X in certificate Y
    batch_committed_pattern = r'\[BATCH_COMMITTED\]\s+Batch\s+([A-Za-z0-9+/=]+)\s+committed\s+at\s+round\s+(\d+)\s+in\s+certificate\s+([A-Za-z0-9+/=]+)'
    
    try:
        with open(primary_log_path, 'r', encoding='utf-8') as f:
            for line in f:
                match = re.search(batch_committed_pattern, line)
                if match:
                    batch_digest = match.group(1)
                    commit_round = int(match.group(2))
                    certificate_id = match.group(3)
                    
                    # Cập nhật tất cả transactions trong batch này
                    if batch_digest in batch_to_txs:
                        for tx_hash in batch_to_txs[batch_digest]:
                            if tx_hash in transactions:
                                transactions[tx_hash].batch_committed = True
                                transactions[tx_hash].commit_round = commit_round
                                transactions[tx_hash].certificate_id = certificate_id
                                
    except FileNotFoundError:
        print(f"❌ Không tìm thấy file: {primary_log_path}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Lỗi khi đọc primary log: {e}")
        sys.exit(1)


def main():
    """Hàm chính"""
    # Đường dẫn mặc định
    script_dir = Path(__file__).parent
    worker_log_path = script_dir / 'benchmark' / 'logs' / 'worker-0-0.log'
    primary_log_path = script_dir / 'benchmark' / 'logs' / 'primary-0.log'
    
    # Cho phép override bằng command line arguments
    args = [arg for arg in sys.argv[1:] if arg != '--all' and arg != '--detail']
    
    if len(args) >= 2:
        worker_log_path = Path(args[0])
        primary_log_path = Path(args[1])
    elif len(args) == 1:
        print("📝 Sử dụng: python analyze_transactions.py [worker_log_path] [primary_log_path] [--all] [--detail]")
        print(f"📝 Hoặc sử dụng đường dẫn mặc định:")
        print(f"   Worker log: {worker_log_path}")
        print(f"   Primary log: {primary_log_path}")
        print(f"\n   Thêm --all để xem danh sách tất cả các transaction")
        print(f"   Thêm --detail để xem chi tiết từng transaction")
        sys.exit(1)
    
    show_all = '--all' in sys.argv
    show_detail = '--detail' in sys.argv
    
    print(f"📂 Đang đọc worker log: {worker_log_path}")
    transactions, batch_to_txs = parse_worker_log(str(worker_log_path))
    print(f"✅ Tìm thấy {len(transactions)} transaction(s) trong worker log")
    print(f"✅ Tìm thấy {len(batch_to_txs)} batch(es) được tạo")
    
    print(f"\n📂 Đang đọc primary log: {primary_log_path}")
    parse_primary_log(str(primary_log_path), transactions, batch_to_txs)
    
    # Phân tích
    fully_committed = [tx for tx in transactions.values() if tx.is_fully_committed()]
    not_received = [tx for tx in transactions.values() if not tx.received]
    not_in_batch = [tx for tx in transactions.values() if tx.received and not tx.added_to_batch]
    batch_not_created = [tx for tx in transactions.values() if tx.added_to_batch and not tx.batch_created]
    not_committed = [tx for tx in transactions.values() if tx.batch_created and not tx.batch_committed]
    
    print(f"\n{'='*80}")
    print("📊 THỐNG KÊ")
    print(f"{'='*80}")
    print(f"   - Tổng số transaction: {len(transactions)}")
    print(f"   - Đã nhận (received): {len([tx for tx in transactions.values() if tx.received])}")
    print(f"   - Đã đưa vào batch: {len([tx for tx in transactions.values() if tx.added_to_batch])}")
    print(f"   - Batch đã được tạo: {len([tx for tx in transactions.values() if tx.batch_created])}")
    print(f"   - Đã commit hoàn toàn: {len(fully_committed)}")
    print(f"   - Chưa commit: {len(not_committed)}")
    
    print(f"\n{'='*80}")
    print("🔍 KẾT QUẢ PHÂN TÍCH")
    print(f"{'='*80}")
    
    if not_received:
        print(f"\n⚠️  Transactions chưa được nhận ({len(not_received)} transaction):")
        for tx in sorted(not_received, key=lambda x: x.tx_id):
            print(f"   - Transaction {tx.tx_id}")
    else:
        print(f"\n✅ Tất cả transactions đã được nhận!")
    
    if not_in_batch:
        print(f"\n⚠️  Transactions đã nhận nhưng chưa đưa vào batch ({len(not_in_batch)} transaction):")
        for tx in sorted(not_in_batch, key=lambda x: x.tx_id):
            print(f"   - Transaction {tx.tx_id}")
    else:
        print(f"\n✅ Tất cả transactions đã được đưa vào batch!")
    
    if batch_not_created:
        print(f"\n⚠️  Transactions trong batch nhưng batch chưa được tạo ({len(batch_not_created)} transaction):")
        for tx in sorted(batch_not_created, key=lambda x: x.tx_id):
            print(f"   - Transaction {tx.tx_id} (batch: {tx.batch_digest})")
    else:
        print(f"\n✅ Tất cả batches đã được tạo!")
    
    if not_committed:
        print(f"\n❌ Transactions CHƯA ĐƯỢC COMMIT ({len(not_committed)} transaction):")
        for tx in sorted(not_committed, key=lambda x: x.tx_id):
            batch_info = f" (batch: {tx.batch_digest})" if tx.batch_digest else ""
            print(f"   - Transaction {tx.tx_id}{batch_info}")
        if show_detail:
            print(f"\n   Chi tiết các transaction chưa commit:")
            for tx in sorted(not_committed, key=lambda x: x.tx_id):
                print(f"      Transaction {tx.tx_id}:")
                print(f"         - Received: {tx.received}")
                print(f"         - Added to batch: {tx.added_to_batch}")
                print(f"         - Batch created: {tx.batch_created}")
                print(f"         - Batch digest: {tx.batch_digest}")
                print(f"         - Committed: {tx.batch_committed}")
    else:
        print(f"\n✅ Tất cả {len(fully_committed)} transactions đã được commit!")
    
    if show_all:
        print(f"\n{'='*80}")
        print(f"📋 DANH SÁCH TẤT CẢ CÁC TRANSACTIONS ({len(transactions)} transaction):")
        print(f"{'='*80}")
        for tx in sorted(transactions.values(), key=lambda x: x.tx_id):
            status = tx.get_status_summary()
            print(f"   TX {tx.tx_id:>8}: {status}")
            if show_detail and tx.batch_digest:
                print(f"             Batch: {tx.batch_digest}")
                if tx.certificate_id:
                    print(f"             Certificate: {tx.certificate_id}, Round: {tx.commit_round}")
    
    # Tóm tắt
    print(f"\n{'='*80}")
    print("📋 TÓM TẮT:")
    print(f"   - Transaction chưa nhận: {len(not_received)}")
    print(f"   - Transaction chưa đưa vào batch: {len(not_in_batch)}")
    print(f"   - Batch chưa được tạo: {len(batch_not_created)}")
    print(f"   - Transaction chưa commit: {len(not_committed)}")
    print(f"   - Transaction đã commit hoàn toàn: {len(fully_committed)}")
    if not show_all:
        print(f"\n💡 Sử dụng --all để xem danh sách tất cả các transaction")
        print(f"💡 Sử dụng --detail để xem chi tiết từng transaction")
    print(f"{'='*80}")


if __name__ == '__main__':
    main()

