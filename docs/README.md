# DOCUMENTATION

This directory contains all documentation related to the Narwhal consensus system improvements.

## Structure

```
docs/
├── README.md                    # This file
├── solutions/                   # Implementation solutions and fixes
│   ├── FINAL_FORK_SAFETY_CHECK.md          # Final comprehensive fork safety check
│   ├── REDUCE_RETRY_SOLUTION.md            # Solution to reduce retry and improve system smoothness
│   ├── WHY_RETRY_ANALYSIS.md               # Analysis of why retry is needed
│   ├── LEADER_BATCH_EXTRACTION.md          # Leader batch extraction implementation
│   ├── DUPLICATE_BATCH_FIX.md              # Fix for duplicate batch execution
│   ├── IMPROVED_BATCH_TRACKING_LOGGING.md  # Improved logging for batch tracking
│   └── archive/                            # Archived solutions (superseded by newer versions)
│       ├── LATE_BATCH_HANDLING_IMPROVED.md
│       ├── LATE_BATCH_HANDLING_SOLUTION.md
│       ├── LATE_BATCH_SOLUTIONS.md
│       ├── solution_late_batch.md
│       ├── FINAL_SAFETY_CHECK.md
│       └── BATCH_PROCESSING_SAFETY_REPORT.md
├── analysis/                    # Analysis reports for specific issues
│   └── BATCH_COMMIT_ANALYSIS.md            # Analysis of batch commit issues
└── BLOCK_SENDING_LOGIC.md       # Block sending logic documentation (existing)
```

## Key Documents

### Current System Status

- **CURRENT_SYSTEM_STATUS.md**: **⭐ TÀI LIỆU MỚI NHẤT** - Tổng hợp tình trạng hệ thống hiện tại, các tính năng đã triển khai, và safety guarantees

### Solutions

- **FINAL_FORK_SAFETY_CHECK.md**: Comprehensive safety check ensuring no fork, no duplicates, no dropped batches
- **REDUCE_RETRY_SOLUTION.md**: Solution to reduce retry operations and make the system smoother
- **WHY_RETRY_ANALYSIS.md**: Analysis of why retry is needed and how to minimize it
- **LEADER_BATCH_EXTRACTION.md**: Implementation of leader batch extraction from parent certificates (includes InFlight → Pending conversion)
- **DUPLICATE_BATCH_FIX.md**: Fix for duplicate batch execution across different certificates
- **IMPROVED_BATCH_TRACKING_LOGGING.md**: Enhanced logging for better batch lifecycle tracking

### Analysis Reports

Analysis reports for batch commit issues encountered during development and testing.

## Safety Guarantees

The system ensures:
1. **No Fork**: Block building and consensus order are deterministic
2. **No Duplicates**: Comprehensive duplicate detection at both batch and transaction levels
3. **No Dropped Batches**: Retry logic and late batch handling ensure all batches are eventually processed

## Implementation Highlights

### Primary Improvements
- Extract batches from parent certificates to accelerate commitment
- Convert InFlight batches to Pending when extracting from parent certificates
- Retry logic with safe retry buffer and certificate commitment detection
- Comprehensive duplicate batch detection

### Node Improvements
- Late batch handling for certificates arriving after their height
- Duplicate batch detection across blocks using `processed_batches`
- Duplicate transaction detection within blocks using `transaction_hashes`
- Detailed batch tracking logging

## Related Code Files

- `primary/src/proposer.rs`: Proposer logic including batch extraction and retry
- `node/src/main.rs`: Node logic including block building and duplicate detection

