package main

import (
	"encoding/binary"
	"log"

	"github.com/meta-node-blockchain/meta-node/pkg/txsender" // Sửa lại nếu cần
)

// createSampleTransaction tạo ra một payload giao dịch mẫu.
func createSampleTransaction(id uint64, size int) []byte {
	payload := make([]byte, size)
	payload[0] = 0 // Loại giao dịch mẫu (sample transaction)
	binary.BigEndian.PutUint64(payload[1:9], id)
	return payload
}

// createRandomTransaction tạo ra một payload giao dịch ngẫu nhiên.
func createRandomTransaction(randomValue uint64, size int) []byte {
	payload := make([]byte, size)
	payload[0] = 1 // Loại giao dịch ngẫu nhiên
	binary.BigEndian.PutUint64(payload[1:9], randomValue)
	return payload
}

func main() {
	nodeAddress := "127.0.0.1:4018" // Thay đổi cổng này tới worker bạn muốn
	transactionSize := 128

	// 1. Khởi tạo client.
	client := txsender.NewClient(nodeAddress)
	defer client.Close()

	// Connect() sẽ được gọi tự động bên trong SendTransaction,
	// nhưng gọi trước ở đây giúp bắt lỗi kết nối sớm hơn.
	log.Println("Đang kết nối đến node...")
	if err := client.Connect(); err != nil {
		log.Fatalf("Kết nối thất bại: %v", err)
	}
	log.Println("Kết nối thành công!")

	// --- Gửi giao dịch 1 (sample transaction) ---
	log.Println("Đang gửi giao dịch #1...")
	txData1 := createSampleTransaction(553, transactionSize)
	if err := client.SendTransaction(txData1); err != nil {
		log.Fatalf("Gửi giao dịch #1 thất bại: %v", err)
	}
	log.Println("Giao dịch #1 đã gửi thành công.")

	// --- Gửi giao dịch 2 (random transaction) ---
	log.Println("Đang gửi giao dịch #2...")
	txData2 := createRandomTransaction(662, transactionSize)
	if err := client.SendTransaction(txData2); err != nil {
		log.Fatalf("Gửi giao dịch #2 thất bại: %v", err)
	}
	log.Println("Giao dịch #2 đã gửi thành công.")

	log.Println("Hoàn thành gửi giao dịch.")
}
