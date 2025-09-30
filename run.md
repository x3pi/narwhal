## Hướng dẫn chạy Narwhal Nodes, Client và xem Executor

Tài liệu này sẽ hướng dẫn bạn cách khởi chạy từng node riêng lẻ, chạy các client để gửi giao dịch và kiểm tra nhật ký của Executor.

### Chuẩn bị trước

Đảm bảo rằng bạn đã chạy script `setup.sh` để tạo các file cấu hình và khóa cần thiết:

```bash
./setup.sh
```

### 1. Chạy từng Node (Node 0 đến Node 2)

Bạn có thể chạy từng node riêng lẻ bằng cách sử dụng script `run_node_id.sh` và truyền `node_id` làm tham số. Mỗi node sẽ khởi động một `Primary`, một `Worker` và một `Executor` (Executor sẽ chỉ chạy cho `node_id > 0` theo cấu hình mặc định trong `run_node_id.sh`, nhưng trong `run_nodes.sh` nó được comment để node 0 cũng chạy).

**Cấu trúc lệnh:**

```bash
./run_node_id.sh <node_id>
```

**Ví dụ:**

*   **Chạy Node 0:**
    ```bash
    ./run_node_id.sh 0
    ```
    Lệnh này sẽ khởi động `Primary-0` và `Worker-0-0`.

*   **Chạy Node 1:**
    ```bash
    ./run_node_id.sh 1
    ```
    Lệnh này sẽ khởi động `Primary-1`, `Worker-1-0` và `Executor-1`.

*   **Chạy Node 2:**
    ```bash
    ./run_node_id.sh 2
    ```
    Lệnh này sẽ khởi động `Primary-2`, `Worker-2-0` và `Executor-2`.

**Kiểm tra trạng thái các node:**

Sau khi chạy các lệnh trên, bạn có thể kiểm tra các phiên tmux đang chạy bằng lệnh:

```bash
tmux ls
```

Bạn sẽ thấy các phiên tương tự như `primary-0`, `worker-0-0`, `primary-1`, `worker-1-0`, `executor-1`, v.v.

**Vào xem log của từng thành phần:**

*   **Primary:**
    ```bash
    tmux attach -t primary-<node_id>
    ```
    Ví dụ: `tmux attach -t primary-0`

*   **Worker:**
    ```bash
    tmux attach -t worker-<node_id>-0
    ```
    Ví dụ: `tmux attach -t worker-0-0`

*   **Executor:** (chỉ có cho node_id > 0 theo cấu hình ban đầu, nhưng có thể đã được thay đổi trong `run_node_id.sh` để chạy cho mọi node)
    ```bash
    tmux attach -t executor-<node_id>
    ```
    Ví dụ: `tmux attach -t executor-1`

**Dừng các node riêng lẻ:**

Để dừng một node cụ thể (Primary và Worker của nó):

```bash
tmux kill-session -t primary-<node_id> && tmux kill-session -t worker-<node_id>-0
```

Nếu Executor cũng đang chạy cho node đó, bạn cần dừng thêm:

```bash
tmux kill-session -t executor-<node_id>
```

**Dừng tất cả các node cùng lúc:**

Nếu bạn đã chạy tất cả các node bằng script `run_nodes.sh` hoặc muốn dừng tất cả các phiên tmux liên quan, bạn có thể sử dụng:

```bash
tmux kill-server
```

### 2. Hướng dẫn chạy Client

Script `run_clients.sh` sẽ khởi chạy một số lượng client được cấu hình sẵn để gửi các giao dịch đến các worker node.

**Chạy Client:**

Để bắt đầu gửi giao dịch, bạn chỉ cần chạy script `run_clients.sh`:

```bash
./run_clients.sh
```

Script này sẽ:
*   Đọc số lượng node (`NODES`), tỷ lệ giao dịch (`RATE`), kích thước giao dịch (`TX_SIZE`) và thời lượng (`DURATION`) từ cấu hình.
*   Với mỗi node, nó sẽ tìm địa chỉ `transactions` của worker 0 từ file `.committee.json`.
*   Khởi chạy một client trong một phiên tmux riêng biệt, gửi các giao dịch với tốc độ được chia đều cho mỗi client.

**Kiểm tra trạng thái Client:**

Bạn có thể kiểm tra các phiên client đang chạy bằng `tmux ls`. Bạn sẽ thấy các phiên như `client-0`, `client-1`, v.v.

**Xem log của Client:**

Để xem nhật ký của một client cụ thể:

```bash
tmux attach -t client-<client_id>
```
Ví dụ: `tmux attach -t client-0`

**Dừng Client:**

Để dừng tất cả các client, bạn có thể sử dụng lệnh `tmux kill-server` (nếu không có các phiên tmux nào khác bạn muốn giữ) hoặc dừng từng phiên client riêng lẻ.

### 3. Hướng dẫn xem Executor

Executor là một thành phần quan trọng xử lý các giao dịch đã được sắp xếp và đồng thuận. Theo các script `run_node_id.sh` và `run_nodes.sh`, Executor được khởi chạy trong một phiên tmux riêng biệt với tên `executor-<node_id>`.

**Xem log của Executor:**

Để xem nhật ký của một Executor cụ thể (ví dụ: Executor của Node 1):

```bash
tmux attach -t executor-1
```

Các file log của Executor được lưu trong thư mục `benchmark/logs/`. Ví dụ, log của Executor-1 sẽ nằm ở `benchmark/logs/executor-1.log`. Bạn có thể đọc trực tiếp file này:

```bash
cat benchmark/logs/executor-1.log
```

**Chi tiết về Executor:**

Executor trong dự án này (có vẻ như là `exetps` từ `go/cmd/exetps/main.go`) là một ứng dụng Go chịu trách nhiệm lắng nghe dữ liệu `CommittedEpochData` từ một `Listener` (được tạo bởi `executor.NewListener`). Khi nhận được dữ liệu epoch, nó sẽ xử lý các block và giao dịch bên trong, in ra thông tin chi tiết về các block và giao dịch đã nhận, đồng thời tính toán và hiển thị TPS (Transactions Per Second) trung bình.

**Các chức năng chính của Executor:**

*   **Lắng nghe CommittedEpochData:** Executor sử dụng một `Listener` để nhận các `CommittedEpochData`. Mỗi `CommittedEpochData` chứa một tập hợp các block đã được đồng thuận.
*   **Xử lý Block và Giao dịch:** Khi nhận được một `CommittedEpochData`, Executor sẽ duyệt qua từng block trong đó. Với mỗi block, nó sẽ in ra:
    *   Round/Epoch của block.
    *   Height của block.
    *   Số lượng giao dịch trong block.
    *   Thời gian nhận block.
    *   Một vài giao dịch đầu tiên trong block (để kiểm tra, chỉ hiển thị 8 byte đầu tiên của `Digest` và `WorkerId`).
*   **Tính toán và hiển thị TPS:** Executor duy trì một bộ đếm giao dịch (`TransactionCounter`) và định kỳ (mỗi 30 giây) tính toán và hiển thị TPS trung bình dựa trên số lượng giao dịch đã nhận trong khoảng thời gian đó.