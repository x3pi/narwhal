# NGUYÊN NHÂN GỐC RỄ: GIAO DỊCH KHÔNG ĐƯỢC THỰC THI

## THÔNG TIN GIAO DỊCH

- **Transaction Hash:** `9e47ede4d1755440eb1d9a233578cccc6765e822cfa1ed47c7581afa75318488`
- **Batch Digest:** `8wljKObw0rpvzuQhTa9XpBvA3gajEVJxAj3DNma3Eos=`
- **Primary:** AqJy7eip40qqZk7F (primary-0)
- **Worker:** 0

---

## PHÂN TÍCH CHI TIẾT

### Timeline

1. **10:34:14.012Z:** Giao dịch được nhận bởi worker-0
2. **10:34:14.088Z:** Batch được tạo và gửi đến primary-0
3. **10:34:14.100Z:** Primary-0 tạo header B41611 với batch `8wljKObw0rpvzuQhTa9XpBvA3gajEVJxAj3DNma3Eos=`
4. **10:34:14.449Z:** Round 41612 - Primary-0 là **LEADER**
5. **10:34:14.449Z:** Consensus commit 8 certificates, **NHƯNG KHÔNG commit certificate của round 41611**

### Vấn Đề Phát Hiện

#### 1. Certificate Round 41611 KHÔNG Được Commit

Từ logs:
```
[2025-11-23T10:34:14.449Z INFO  consensus] Bullshark: committed 8 certificates via leader round 41612 -> [
  "{round: 41610, origin: AqJy7eip40qqZk7FqFMJsfRtrZS16YviyXCQ4gpcVYWK, digest: 8o3UXoY5OjpjCZ4zftLrqZJ5a29RtLwS6L1fAv1nAZo=}",
  "{round: 41610, origin: AuQGL6Xnz4NACJKryGkYcVjHHAx60zpzXY8lcWb7uAr6, digest: 49seq7RhAqo3NanCtMOfnKDiUKCoaNYd/c7C/Zji9CI=}",
  "{round: 41610, origin: A1NJsf/JYzCzRtTm030C/OhMJ67pj2n5jfCpIYQIMyX1, digest: 3cjRx3T1tdWukWViywnU930krDlsq+ujAB6OWwpR5VE=}",
  "{round: 41611, origin: A1NJsf/JYzCzRtTm030C/OhMJ67pj2n5jfCpIYQIMyX1, digest: 0w0uUu3QIqrTBNx0f4jcNk+ibvWwm5bJkRHW8J84pLc=}",
  "{round: 41611, origin: AgF2i8f4TnfU3BjsLKvg3GkELTR3JCvZyQT/R51QAlPM, digest: pvG1JToQtO43ybRMtAzncdnhAj48bObIKIDG64rkH/A=}",
  "{round: 41611, origin: AuQGL6Xnz4NACJKryGkYcVjHHAx60zpzXY8lcWb7uAr6, digest: GS1uFZoR+fgEryMpR5deyigapGecQFF7A0eCybReoIw=}",
  "{round: 41611, origin: ApvX+ZCVrGWssP/vAcNbnOL25dkQ/ZURCiKZrBFJhFrQ, digest: F0UoMW98Lae/wbf0L2ZI7yrtbjOS8c4LAs1hGh9dloU=}",
  "{round: 41612, origin: AqJy7eip40qqZk7FqFMJsfRtrZS16YviyXCQ4gpcVYWK, digest: XIniwnkLYgKyY4kzWMBwUlOfYPDqwqpQdC0zQdFJ5WA=}"
]
```

**Phân tích:**
- ✅ Certificate của round 41611 từ **A1NJsf/JYzCzRtTm030C** được commit
- ✅ Certificate của round 41611 từ **AgF2i8f4TnfU3BjsLKvg3GkELTR3JCvZyQT/R51QAlPM** được commit
- ✅ Certificate của round 41611 từ **AuQGL6Xnz4NACJKryGkYcVjHHAx60zpzXY8lcWb7uAr6** được commit
- ✅ Certificate của round 41611 từ **ApvX+ZCVrGWssP/vAcNbnOL25dkQ/ZURCiKZrBFJhFrQ** được commit
- ❌ **Certificate của round 41611 từ AqJy7eip40qqZk7F (primary-0) KHÔNG được commit**

#### 2. Batch Extraction Không Hoạt Động

Từ code `primary/src/proposer.rs`:
```rust
async fn extract_batches_from_headers(&mut self, header: &Header) {
    // Skip our own headers (we already know about these batches)
    if header.author == self.name {
        continue; // ❌ SKIP OWN HEADERS
    }
    // ...
}
```

**Vấn đề:**
- Primary-0 **skip headers của chính nó** trong batch extraction
- Khi primary-0 là leader, nó không extract batch từ header của chính nó
- Batch `8wljKObw0rpvzuQhTa9XpBvA3gajEVJxAj3DNma3Eos=` nằm trong header B41611 của primary-0
- Nhưng primary-0 skip header này, nên batch không được extract

#### 3. Certificate Không Được Commit

**Nguyên nhân:**
- Certificate của round 41611 từ primary-0 không được commit
- Chỉ có certificate của round 41612 (leader round) được commit
- Batch nằm trong certificate của round 41611, nên không được commit

---

## NGUYÊN NHÂN GỐC RỄ

### Vấn Đề 1: Leader Không Commit Certificate Của Chính Nó Ở Round Trước

**Cơ chế Bullshark:**
- Leader ở round N commit certificates từ rounds N-2, N-1
- Nhưng **không nhất thiết commit certificate của chính nó** ở round N-1
- Leader có thể commit certificates từ các primary khác thay vì certificate của chính nó

**Trong trường hợp này:**
- Round 41612: Primary-0 là leader
- Leader commit certificates từ round 41611, nhưng **KHÔNG commit certificate của primary-0**
- Certificate của primary-0 (chứa batch `8wljKObw0rpvzuQhTa9XpBvA3gajEVJxAj3DNma3Eos=`) không được commit

### Vấn Đề 2: Batch Extraction Không Hoạt Động Cho Own Headers

**Code hiện tại:**
```rust
if header.author == self.name {
    continue; // Skip own headers
}
```

**Vấn đề:**
- Khi primary là leader, nó skip headers của chính nó
- Batch trong own headers không được extract
- Batch bị stuck trong queue

### Vấn Đề 3: Batch Retry Logic Không Giải Quyết Vấn Đề

**Retry logic:**
- Batch InFlight sẽ được retry sau một thời gian
- Nhưng nếu certificate không được commit, batch vẫn bị stuck

---

## KẾT LUẬN

### Nguyên Nhân Chính

1. **Certificate của primary-0 ở round 41611 không được commit** bởi leader (chính primary-0) ở round 41612
2. **Batch extraction skip own headers**, nên batch không được extract khi primary-0 là leader
3. **Batch retry logic không giải quyết vấn đề** vì certificate không được commit

### Tại Sao Giao Dịch Sau Đó Cũng Không Được Thực Thi

- Batch queue bị đầy với batches không được commit
- Primary-0 tiếp tục retry batches cũ
- Batches mới cũng bị stuck trong queue

---

## GIẢI PHÁP ĐỀ XUẤT

### 1. Sửa Batch Extraction Để Không Skip Own Headers Khi Là Leader

**Thay đổi:**
```rust
async fn extract_batches_from_headers(&mut self, header: &Header) {
    // Don't skip own headers if we're the leader
    // Leader should extract batches from own headers to ensure they're included
    if header.author == self.name && !self.is_leader {
        continue; // Only skip own headers if we're not the leader
    }
    // ...
}
```

**Vấn đề:** Cần biết primary có phải leader không, điều này phức tạp.

### 2. Đảm Bảo Leader Commit Certificate Của Chính Nó

**Thay đổi:** Sửa logic commit để leader ưu tiên commit certificate của chính nó nếu có.

**Vấn đề:** Có thể vi phạm tính công bằng của consensus.

### 3. Cải Thiện Batch Retry Logic

**Thay đổi:** Khi certificate không được commit sau một thời gian, chuyển batch sang Pending state để leader khác có thể extract.

**Vấn đề:** Cần xác định khi nào certificate không được commit.

---

## KHUYẾN NGHỊ

**Giải pháp tốt nhất:** Sửa batch extraction để **không skip own headers** khi batch đang ở InFlight state. Điều này cho phép leader extract batch từ own headers và include vào header mới.

