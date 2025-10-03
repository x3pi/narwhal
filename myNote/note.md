Tuyệt vời! Đây là những khái niệm cốt lõi nhất của Narwhal. Hiểu được chúng là bạn đã hiểu được "linh hồn" của toàn bộ hệ thống.

Hãy cùng nhau giải mã chúng bằng cách sử dụng lại **ví dụ nhà bếp** mà chúng ta đã quen thuộc.

---

### 1. Digest (Mã định danh duy nhất - "Cái Barcode")

*   **Nó là gì?** `Digest` là một **dấu vân tay** kỹ thuật số, một chuỗi ký tự duy nhất có độ dài cố định (ví dụ 32 byte) đại diện cho một khối dữ liệu bất kỳ. Nó được tạo ra bằng cách đưa dữ liệu qua một hàm băm mật mã (như SHA-512).
*   **Tương tự trong nhà bếp:** Hãy tưởng tượng mỗi "khay hàng" (batch) hay mỗi "thực đơn đề xuất" (header) đều được dán một cái **barcode** độc nhất. Thay vì phải mang cả khay hàng nặng trịch đi khắp nơi để nói về nó, các đầu bếp chỉ cần đọc số barcode cho nhau là đủ.
*   **Tại sao lại dùng nó?**
    *   **Hiệu quả:** Gửi một cái barcode 32 byte nhanh hơn rất nhiều so với gửi cả một "khay hàng" 1MB.
    *   **Toàn vẹn dữ liệu:** Nếu ai đó lén thay đổi dù chỉ một cọng hành trong khay, cái barcode khi quét lại sẽ ra một số hoàn toàn khác. Điều này đảm bảo không ai có thể thay đổi dữ liệu mà không bị phát hiện.
*   **Trong code:** `Header` không chứa toàn bộ `batch`, mà chỉ chứa một danh sách các `Digest` của các `batch` đó.

---

### 2. Header (Khối đề xuất - "Bản nháp Thực đơn")

*   **Nó là gì?** `Header` là đơn vị xây dựng cơ bản của Narwhal. Nó là một **đề xuất** do một `Primary` (Bếp trưởng) tạo ra trong một vòng (round).
*   **Tương tự trong nhà bếp:** Bếp trưởng (`Primary`) nhìn vào các barcode của những "khay hàng" (`batch`) mới nhất do "Đầu bếp phụ" (`Worker`) chuẩn bị. Anh ta viết ra một **bản nháp thực đơn** cho lượt ra món tiếp theo. Bản nháp này bao gồm:
    1.  **Tên tôi là ai?** (`author`: Bếp trưởng A).
    2.  **Lượt ra món số mấy?** (`round`: Lượt số 10).
    3.  **Thực đơn này gồm những gì?** (`payload`: danh sách các **barcode** của khay phở #123, khay nem rán #456).
    4.  **Thực đơn này nối tiếp những thực đơn nào đã được duyệt trước đó?** (`parents`: danh sách các **barcode** của các "Thực đơn đã chốt đơn" ở lượt số 9).
*   **Tại sao lại dùng nó?** Đây là cách một node đóng góp các giao dịch của mình vào lịch sử chung của mạng lưới. Nó là một lời đề xuất: "Này mọi người, tôi đề nghị chúng ta xử lý các lô hàng này tiếp theo, dựa trên những gì chúng ta đã đồng ý ở vòng trước."

---

### 3. Cấu trúc DAG (Đồ thị Phi chu trình có hướng - "Tấm bảng Lịch sử Đề xuất")

*   **Nó là gì?** DAG không phải là một "vật thể" cụ thể, mà là **toàn bộ cấu trúc** được tạo ra khi bạn kết nối tất cả các `Header` lại với nhau. Thay vì một chuỗi thẳng như Blockchain, nó là một mạng lưới các khối đan xen.
*   **Tương tự trong nhà bếp:** Hãy tưởng tượng có một **tấm bảng trắng khổng lồ** trong bếp.
    *   Mỗi khi một Bếp trưởng (`Primary`) tạo ra một "bản nháp thực đơn" (`Header`), anh ta dán nó lên bảng ở cột tương ứng với lượt ra món của nó (cột round 10).
    *   Sau đó, anh ta **vẽ các mũi tên** từ bản nháp của mình chỉ ngược về các "thực đơn đã được chốt đơn" (`Certificate`) ở cột trước đó (cột round 9) mà anh ta đã tham chiếu.
    *   Vì có nhiều Bếp trưởng cùng làm việc, họ sẽ dán nhiều bản nháp lên cùng một cột. Kết quả là trên tấm bảng không phải một đường thẳng, mà là một **mạng lưới các mũi tên chằng chịt**, nhưng luôn đi từ cột mới về cột cũ.
*   **Tại sao lại dùng DAG (Đây là điểm đột phá của Narwhal)?**
    *   **Hiệu suất cực cao:** Thay vì cả nhà bếp phải dừng lại, cãi nhau để chốt được duy nhất MỘT món tiếp theo (giống như Blockchain truyền thống), Narwhal cho phép **tất cả các Bếp trưởng được đề xuất thực đơn của họ một cách song song**. Việc "chốt đơn" sẽ được tính sau. Điều này tách rời việc "đưa dữ liệu lên" và việc "sắp xếp thứ tự cuối cùng", giúp giải quyết tắc nghẽn cổ chai.

---

### 4. Certificate (Chứng chỉ - "Thực đơn đã được Chốt đơn")

*   **Nó là gì?** `Certificate` là một phiên bản **nâng cấp, đã được xác thực** của một `Header`. Nó là một `Header` đã nhận đủ phiếu bầu (`Vote`) từ các `Primary` khác.
*   **Tương tự trong nhà bếp:** "Bản nháp thực đơn" (`Header`) của Bếp trưởng A được các Bếp trưởng khác xem xét. Khi có đủ số lượng Bếp trưởng (ví dụ 2/3) giơ ngón tay cái (`Vote`), Bếp trưởng A sẽ thu thập tất cả các chữ ký "đồng ý" này, đính chúng vào bản nháp ban đầu của mình, và đóng một con dấu đỏ **"ĐÃ DUYỆT"**. Toàn bộ gói "bản nháp + các chữ ký" này chính là `Certificate`.
*   **Tại sao lại dùng nó?**
    *   **Tính bất biến:** Một khi đã trở thành `Certificate`, nó là một điểm mốc vững chắc trong lịch sử, không thể bị thay đổi.
    *   **Làm "cha mẹ" hợp lệ:** Các Bếp trưởng ở lượt tiếp theo (round 11) chỉ được phép vẽ mũi tên tham chiếu đến các "thực đơn đã được chốt đơn" (`Certificate`) ở lượt 10, chứ không được tham chiếu đến các "bản nháp" (`Header`) chưa được duyệt.
    *   **Đầu ra của Narwhal:** Luồng `Certificate` đã được chốt này chính là sản phẩm cuối cùng của Narwhal. Luồng này sau đó được đưa cho một module khác (như Tusk/Bullshark) để sắp xếp chúng thành một thứ tự duy nhất, tuyến tính.

### Tóm tắt trong một bảng:

| Thuật ngữ | Ví dụ Nhà bếp | Vai trò |
| :--- | :--- | :--- |
| **Digest** | Barcode duy nhất | Mã định danh hiệu quả và an toàn cho dữ liệu. |
| **Header** | Bản nháp thực đơn | Lời đề xuất của một node trong một round, chứa các giao dịch. |
| **DAG** | Tấm bảng trắng lớn với các thực đơn và mũi tên | Cấu trúc tổng thể, cho phép đề xuất song song, tăng hiệu suất. |
| **Certificate** | Thực đơn đã được chốt đơn (có đủ chữ ký) | Lời đề xuất đã được đa số xác nhận, trở thành một điểm mốc lịch sử. |