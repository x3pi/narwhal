# Sử dụng đường dẫn mặc định (benchmark/logs/worker-0-0.log và primary-0.log)
python3 analyze_batches.py

# Chỉ định đường dẫn tùy chỉnh
python3 analyze_batches.py /path/to/worker.log /path/to/primary.log

# Xem danh sách tất cả các batch với trạng thái
python3 analyze_batches.py --all

# Kết hợp cả hai
python3 analyze_batches.py /path/to/worker.log /path/to/primary.log --all