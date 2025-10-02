# Xem tất cả session đang chạy
tmux ls

# Vào session cụ thể để xem log realtime
tmux attach -t primary-0
tmux attach -t worker-0-0  
tmux attach -t executor-0

# Thoát session (Ctrl+B rồi D)
# Hoặc kill session
tmux kill-session -t primary-0