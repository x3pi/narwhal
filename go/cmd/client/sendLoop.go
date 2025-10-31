package main

import (
	"encoding/binary"
	"encoding/hex"
	"log"
	"time" // Thêm package 'time'

	"github.com/meta-node-blockchain/meta-node/pkg/txsender"
)

// createRandomTransaction tạo ra một payload giao dịch ngẫu nhiên.
func createRandomTransaction(randomValue uint64, size int) []byte {
	payload := make([]byte, size)
	payload[0] = 1 // Loại giao dịch ngẫu nhiên
	binary.BigEndian.PutUint64(payload[1:9], randomValue)
	// Điền phần còn lại của payload bằng một số dữ liệu, ví dụ: lặp lại giá trị
	for i := 9; i+8 <= size; i += 8 {
		binary.BigEndian.PutUint64(payload[i:i+8], randomValue)
	}
	return payload
}

func main() {
	nodeAddress := "127.0.0.1:4011"       // Địa chỉ worker node
	transactionSize := 128                // Kích thước giao dịch
	sendInterval := 10 * time.Millisecond // Thời gian chờ giữa mỗi lần gửi

	// 1. Khởi tạo client.
	client := txsender.NewClient(nodeAddress)
	defer client.Close()

	// 2. Kết nối ban đầu.
	log.Println("Đang kết nối đến node...")
	if err := client.Connect(); err != nil {
		log.Fatalf("Kết nối ban đầu thất bại: %v", err)
	}
	log.Println("Kết nối thành công! Bắt đầu gửi giao dịch...")

	var counter uint64 = 0

	// 3. Vòng lặp gửi giao dịch vô tận
	for {
		counter++
		// Tạo dữ liệu giao dịch mới mỗi lần lặp
		txData := createRandomTransaction(counter, transactionSize)

		log.Printf("Đang gửi giao dịch #%d (data hex: %s...)", counter, hex.EncodeToString(txData[:17])) // Log 1 phần data

		// Gửi giao dịch
		if err := client.SendTransaction(txData); err != nil {
			log.Printf("Lỗi khi gửi giao dịch #%d: %v", counter, err)

			// Cố gắng kết nối lại nếu gửi thất bại
			log.Println("Đang thử kết nối lại...")
			if reconnErr := client.Connect(); reconnErr != nil {
				log.Printf("Kết nối lại thất bại: %v. Thử lại sau 1 giây.", reconnErr)
				// Chờ 1 giây trước khi thử lại vòng lặp (và kết nối lại)
				time.Sleep(1 * time.Second)
				continue // Bỏ qua lần sleep 10ms và bắt đầu lại vòng lặp
			}
			log.Println("Kết nối lại thành công.")
		} else {
			// Log thành công (có thể bị quá nhiều log, cân nhắc tắt đi)
			// log.Printf("Giao dịch #%d đã gửi.", counter)
		}

		// Chờ 10ms trước khi gửi giao dịch tiếp theo
		time.Sleep(sendInterval)
	}
}
