// @title network/handler.go
// @markdown `network/handler.go`
package network

import (
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	p_common "github.com/meta-node-blockchain/meta-node/pkg/common"
	"github.com/meta-node-blockchain/meta-node/pkg/logger"
	"github.com/meta-node-blockchain/meta-node/types/network"
)

// handleServerBusy là hàm xử lý mặc định cho lệnh ServerBusy.
// Nó được gọi khi server quá tải và không thể xử lý thêm yêu cầu.
func handleServerBusy(r network.Request) error {
	logger.Error("--- NHẬN TÍN HIỆU SERVER BUSY TỪ CLIENT: %s ---", r.Connection().RemoteAddrSafe())
	return nil
}

// tpsTracker quản lý việc theo dõi và thống kê TPS cho các route.
// Đây là struct mới được thêm vào để phục vụ tính năng đo lường hiệu suất.
type tpsTracker struct {
	mu          sync.Mutex       // Mutex để đảm bảo an toàn khi truy cập 'counts' từ nhiều goroutine.
	counts      map[string]int64 // Map để lưu số lượng request cho mỗi lệnh (route) trong khoảng thời gian 1 giây.
	lastLogTime time.Time        // Thời điểm cuối cùng mà log TPS được ghi.
}

// Handler chịu trách nhiệm xử lý các request đến dựa trên các route đã đăng ký.
type Handler struct {
	routes map[string]func(network.Request) error // Map lưu trữ các "route", ánh xạ từ tên lệnh sang hàm xử lý tương ứng.

	// WaitGroup dùng để đảm bảo rằng tất cả các tác vụ chạy ngầm (nếu có) trong các handler hoàn thành trước khi shutdown.
	wg sync.WaitGroup

	// CircuitBreaker là một pattern giúp ngăn chặn các lỗi dây chuyền, sẽ được tích hợp sau.
	circuitBreaker *CircuitBreaker

	// Con trỏ tới struct tpsTracker để quản lý việc đếm TPS.
	tps *tpsTracker
}

// NewHandler tạo một instance mới của Handler.
func NewHandler(
	routes map[string]func(network.Request) error,
	limits map[string]int, // Tham số này hiện không dùng nhưng được giữ lại để tương thích.
) *Handler {
	// Nếu không có route nào được cung cấp, tạo một map rỗng để tránh lỗi.
	if routes == nil {
		routes = make(map[string]func(network.Request) error)
		logger.Warn("NewHandler: 'routes' được cung cấp là nil.")
	}
	// Tự động đăng ký một handler mặc định cho lệnh 'ServerBusy'.
	if _, exists := routes[p_common.ServerBusy]; !exists {
		routes[p_common.ServerBusy] = handleServerBusy
		logger.Info("Đã tự động đăng ký handler cho lệnh 'ServerBusy'.")
	}

	h := &Handler{
		routes: routes,
		// Khởi tạo tpsTracker.
		tps: &tpsTracker{
			counts:      make(map[string]int64),
			lastLogTime: time.Now(),
		},
		// Tạm thời tạo một circuit breaker giả để tránh lỗi.
		circuitBreaker: NewCircuitBreaker(DefaultCircuitBreakerConfig()),
	}

	// **QUAN TRỌNG**: Khởi chạy một goroutine riêng để thực hiện việc ghi log TPS định kỳ.
	go h.logTPS()

	return h
}

// logTPS là goroutine chạy nền, có nhiệm vụ ghi log TPS mỗi giây.
func (h *Handler) logTPS() {
	// THAY ĐỔI: Tạo một Ticker sẽ "bắn" tín hiệu sau mỗi 100ms.
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop() // Dọn dẹp Ticker khi hàm kết thúc.

	// Vòng lặp vô hạn chờ tín hiệu từ Ticker.
	for range ticker.C {
		h.tps.mu.Lock() // Khóa mutex để đọc và reset dữ liệu một cách an toàn.

		now := time.Now()
		// Tính khoảng thời gian chính xác từ lần log cuối cùng.
		duration := now.Sub(h.tps.lastLogTime).Seconds()
		if duration < 0.05 { // Tránh trường hợp chia cho 0 hoặc số quá nhỏ.
			h.tps.mu.Unlock()
			continue
		}

		// Sử dụng strings.Builder để xây dựng chuỗi log một cách hiệu quả.
		var logBuilder strings.Builder
		logBuilder.WriteString("--- Báo Cáo TPS (100ms) ---\n")

		// Lấy danh sách các route đã nhận được request để sắp xếp.
		// Việc sắp xếp giúp output log luôn nhất quán, dễ đọc.
		routes := make([]string, 0, len(h.tps.counts))
		for route := range h.tps.counts {
			routes = append(routes, route)
		}
		sort.Strings(routes) // Sắp xếp tên các route theo thứ tự alphabet.

		totalTPS := 0.0

		// Duyệt qua các route đã được sắp xếp.
		for _, route := range routes {
			count := h.tps.counts[route]
			if count > 0 {
				tps := float64(count) / duration
				totalTPS += tps
				// Thêm thông tin của từng route vào log.
				logBuilder.WriteString(fmt.Sprintf("  - Route: %-40s | TPS: %.2f (Requests: %d)\n", route, tps, count))
			}
		}

		// Chỉ in log nếu có hoạt động trong khoảng thời gian vừa qua.
		if totalTPS > 0 {
			logBuilder.WriteString(fmt.Sprintf("  - TỔNG CỘNG                              | TPS: %.2f\n", totalTPS))
			logBuilder.WriteString("-----------------------------")
			log.Printf("xxxxxx : %s ", logBuilder.String())
		}

		// **QUAN TRỌNG**: Reset lại bộ đếm để bắt đầu đếm cho 100ms tiếp theo.
		h.tps.counts = make(map[string]int64)
		h.tps.lastLogTime = now

		h.tps.mu.Unlock() // Mở khóa mutex.
	}
}

// HandleRequest xử lý một request đến. Hàm này được thực thi đồng bộ
// bởi một worker trong Worker Pool của SocketServer.
func (h *Handler) HandleRequest(r network.Request) (err error) {
	// Defer được dùng để đảm bảo một số hành động luôn được thực hiện khi hàm kết thúc
	// (dù có lỗi hay không), ví dụ như cập nhật trạng thái của Circuit Breaker.
	defer func() {
		if err != nil {
			// h.circuitBreaker.RecordFailure()
		} else {
			// h.circuitBreaker.RecordSuccess()
		}
	}()

	// Kiểm tra tính hợp lệ của request đầu vào.
	if r == nil || r.Message() == nil {
		err = errors.New("request hoặc message không hợp lệ")
		return err // Trả về lỗi ngay lập tức.
	}

	// Lấy tên lệnh từ message.
	cmd := r.Message().Command()

	// --- CẬP NHẬT BỘ ĐẾM TPS ---
	// Đây là dòng code cốt lõi của tính năng đo TPS.
	h.tps.mu.Lock()     // Khóa mutex trước khi thay đổi map.
	h.tps.counts[cmd]++ // Tăng bộ đếm cho lệnh tương ứng.
	h.tps.mu.Unlock()   // Mở khóa ngay sau khi xong.
	// ---------------------------

	// Tìm hàm xử lý (route) tương ứng với lệnh.
	route, routeExists := h.routes[cmd]
	if !routeExists {
		err = fmt.Errorf("không tìm thấy lệnh: %s", cmd)
		return err
	}

	// Thực thi route và gán lỗi (nếu có) vào biến err.
	err = route(r)

	// Trả về lỗi từ việc thực thi route.
	return err
}

// Shutdown chờ tất cả các goroutine (nếu có) do các route handler tạo ra hoàn thành.
func (h *Handler) Shutdown() {
	logger.Info("Handler đang shutdown...")
	// Nếu các hàm xử lý của bạn có tạo ra goroutine và dùng h.wg.Add(),
	// thì h.wg.Wait() sẽ chờ chúng hoàn thành.
	logger.Info("Chờ các tác vụ bất đồng bộ (nếu có) của handler hoàn thành...")
	h.wg.Wait()
	logger.Info("Handler đã shutdown thành công.")
}

// GetCircuitBreakerStats trả về thống kê của Circuit Breaker.
func (h *Handler) GetCircuitBreakerStats() map[string]interface{} {
	if h.circuitBreaker != nil {
		return h.circuitBreaker.GetStats()
	}
	return nil
}

// ResetCircuitBreaker đặt lại Circuit Breaker về trạng thái ban đầu.
func (h *Handler) ResetCircuitBreaker() {
	if h.circuitBreaker != nil {
		h.circuitBreaker.Reset()
	}
}
