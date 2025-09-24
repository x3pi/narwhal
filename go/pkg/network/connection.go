// network/connection.go

package network

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/valyala/bytebufferpool"
	"google.golang.org/protobuf/proto"

	"github.com/meta-node-blockchain/meta-node/pkg/logger"
	pb "github.com/meta-node-blockchain/meta-node/pkg/proto"
	"github.com/meta-node-blockchain/meta-node/types/network"
)

var (
	ErrDisconnected        = errors.New("error: connection is disconnected")
	ErrExceedMessageLength = errors.New("error: message exceeds allowed length limit")
	ErrRequestChanFull     = errors.New("error: request channel is full after timeout")
)

var requestPool = sync.Pool{
	New: func() interface{} {
		return &Request{}
	},
}

// Các struct để giao tiếp với goroutine quản lý qua channel
type (
	// Yêu cầu lấy thông tin
	getIsConnectRequest  struct{ resp chan bool }
	getAddressRequest    struct{ resp chan common.Address }
	getChannelsRequest   struct{ resp chan getChannelsResponse }
	getTypeRequest       struct{ resp chan string }
	getRemoteAddrRequest struct{ resp chan string }
	getTCPAddrRequest    struct {
		isLocal bool // true for LocalAddr, false for RemoteAddr
		resp    chan net.Addr
	}

	// Struct chứa kết quả trả về
	getChannelsResponse struct {
		reqChan chan network.Request
		errChan chan error
	}

	// Payload để khởi tạo
	initPayload struct {
		address      common.Address
		cType        string
		realConnAddr string
	}
)

// Connection - Quản lý trạng thái bằng một goroutine duy nhất, không dùng Mutex.
type Connection struct {
	config  *Config
	cmdChan chan interface{} // Kênh lệnh đa năng
	runOnce sync.Once
}

// Các loại lệnh được gửi qua cmdChan
type (
	cmdConnect struct {
		realConnAddr string
		resp         chan error
	}
	cmdAccept struct {
		tcpConn net.Conn
	}
	cmdDisconnect  struct{}
	cmdSendMessage struct {
		message network.Message
		resp    chan error
	}
	cmdInit struct {
		payload initPayload
	}
	cmdClone struct {
		resp chan network.Connection
	}
)

// constructor chung
func newConnectionBase(config *Config) *Connection {
	if config == nil {
		config = DefaultConfig()
	}
	return &Connection{
		config:  config,
		cmdChan: make(chan interface{}, 10),
	}
}

// NewConnection tạo kết nối mới phía client
func NewConnection(
	address common.Address,
	cType string,
	config *Config,
) network.Connection {
	c := newConnectionBase(config)
	c.runOnce.Do(func() { go c.run() })
	c.Init(address, cType)
	return c
}

// ConnectionFromTcpConnection tạo kết nối từ phía server
func ConnectionFromTcpConnection(tcpConn net.Conn, config *Config) (network.Connection, error) {
	if tcpConn == nil {
		return nil, errors.New("tcpConn không được là nil")
	}
	c := newConnectionBase(config)
	c.runOnce.Do(func() { go c.run() })
	c.cmdChan <- cmdAccept{tcpConn: tcpConn}
	return c, nil
}

// run là goroutine quản lý state duy nhất, tuần tự hóa mọi truy cập.
func (c *Connection) run() {
	logger.Info("Running connection with graceful shutdown logic...")

	var (
		address      common.Address
		cType        string
		tcpConn      net.Conn
		connect      bool
		realConnAddr string
		requestChan  chan network.Request
		errorChan    chan error
		sendChan     chan network.Message
		writeWg      sync.WaitGroup
		readWg       sync.WaitGroup
		quitChan     chan struct{}
	)

	cleanup := func() {
		if !connect {
			return
		}
		connect = false
		logger.Debug("Cleanup: Initiated for %s", realConnAddr)

		// BƯỚC 1: Gửi tín hiệu dừng cho các goroutine con.
		if quitChan != nil {
			logger.Debug("Cleanup: Closing quitChan...")
			close(quitChan)
		}

		// BƯỚC 2: Đóng kết nối TCP và sendChan.
		if tcpConn != nil {
			logger.Debug("Cleanup: Closing TCP connection...")
			_ = tcpConn.Close()
		}
		if sendChan != nil {
			logger.Debug("Cleanup: Closing sendChan...")
			close(sendChan)
		}

		// BƯỚC 3: Chờ cho các goroutine I/O kết thúc hoàn toàn.
		logger.Debug("Cleanup: Waiting for IO goroutines to finish...")
		writeWg.Wait()
		readWg.Wait()
		logger.Debug("Cleanup: IO goroutines finished.")

		// BƯỚC 4: Đóng các channel downstream (requestChan, errorChan).
		if requestChan != nil {
			logger.Debug("Cleanup: Closing requestChan...")
			close(requestChan)
		}
		if errorChan != nil {
			logger.Debug("Cleanup: Closing errorChan...")
			close(errorChan)
		}
		logger.Info("Connection manager: Cleanup complete for %s", realConnAddr)
	}

	startIO := func(conn net.Conn) {
		requestChan = make(chan network.Request, c.config.RequestChanSize)
		errorChan = make(chan error, c.config.ErrorChanSize)
		sendChan = make(chan network.Message, c.config.SendChanSize)
		quitChan = make(chan struct{}) // Kênh tín hiệu để dừng

		writeWg.Add(1)
		readWg.Add(1)

		go c.writeLoop(conn, sendChan, &writeWg)
		// Truyền quitChan và readWg vào readLoop
		go c.readLoop(conn, requestChan, errorChan, &readWg, quitChan)
	}

	for cmd := range c.cmdChan {
		switch v := cmd.(type) {
		case cmdInit:
			address = v.payload.address
			cType = v.payload.cType
			realConnAddr = v.payload.realConnAddr

		case cmdAccept:
			if connect {
				continue
			}
			tcpConn = v.tcpConn
			realConnAddr = tcpConn.RemoteAddr().String()
			connect = true
			startIO(tcpConn)
			logger.Info("Connection manager: Accepted connection from %s", realConnAddr)

		case cmdConnect:
			if connect {
				v.resp <- nil
				continue
			}
			conn, err := net.DialTimeout("tcp", v.realConnAddr, c.config.DialTimeout)
			if err != nil {
				v.resp <- err
				continue
			}
			tcpConn = conn
			realConnAddr = v.realConnAddr
			connect = true
			startIO(tcpConn)
			logger.Info("Connection manager: Connected to %s", realConnAddr)
			v.resp <- nil

		case cmdSendMessage:
			if !connect {
				v.resp <- ErrDisconnected
				continue
			}
			select {
			case sendChan <- v.message:
				v.resp <- nil
			case <-time.After(c.config.WriteTimeout):
				v.resp <- errors.New("timeout khi gửi vào sendChan nội bộ")
				go func() { c.cmdChan <- cmdDisconnect{} }()
			}

		case cmdDisconnect:
			cleanup()
			return

		case cmdClone:
			newConn := NewConnection(address, cType, c.config)
			newConn.SetRealConnAddr(realConnAddr)
			v.resp <- newConn

		case getIsConnectRequest:
			v.resp <- connect
		case getAddressRequest:
			v.resp <- address
		case getChannelsRequest:
			v.resp <- getChannelsResponse{reqChan: requestChan, errChan: errorChan}
		case getTypeRequest:
			v.resp <- cType
		case getRemoteAddrRequest:
			v.resp <- realConnAddr
		case getTCPAddrRequest:
			if tcpConn != nil {
				if v.isLocal {
					v.resp <- tcpConn.LocalAddr()
				} else {
					v.resp <- tcpConn.RemoteAddr()
				}
			} else {
				v.resp <- nil
			}
		}
	}
}

// Các hàm public giờ chỉ gửi lệnh vào cmdChan
func (c *Connection) Connect() error {
	addr := c.RemoteAddrSafe()
	if addr == "" {
		return errors.New("kết nối thất bại: realConnAddr chưa được thiết lập. Hãy gọi SetRealConnAddr trước")
	}

	req := cmdConnect{
		realConnAddr: addr,
		resp:         make(chan error, 1),
	}
	c.cmdChan <- req
	return <-req.resp
}

func (c *Connection) Disconnect() error {
	select {
	case c.cmdChan <- cmdDisconnect{}:
	default:
	}
	return nil
}

func (c *Connection) SendMessage(message network.Message) error {
	if !c.IsConnect() {
		return ErrDisconnected
	}
	req := cmdSendMessage{
		message: message,
		resp:    make(chan error, 1),
	}
	c.cmdChan <- req
	return <-req.resp
}

func (c *Connection) ReadRequest() {
	// Phương thức này được để trống một cách có chủ ý.
	// Logic đọc thực sự nằm trong goroutine `readLoop` private.
}

// Các hàm lấy thông tin (getter)
func (c *Connection) IsConnect() bool {
	req := getIsConnectRequest{resp: make(chan bool, 1)}
	c.cmdChan <- req
	return <-req.resp
}

func (c *Connection) Address() common.Address {
	req := getAddressRequest{resp: make(chan common.Address, 1)}
	c.cmdChan <- req
	return <-req.resp
}

func (c *Connection) RequestChan() (chan network.Request, chan error) {
	req := getChannelsRequest{resp: make(chan getChannelsResponse, 1)}
	c.cmdChan <- req
	resp := <-req.resp
	return resp.reqChan, resp.errChan
}

func (c *Connection) RemoteAddrSafe() string {
	req := getRemoteAddrRequest{resp: make(chan string, 1)}
	c.cmdChan <- req
	return <-req.resp
}

func (c *Connection) Type() string {
	req := getTypeRequest{resp: make(chan string, 1)}
	c.cmdChan <- req
	return <-req.resp
}

func (c *Connection) TcpLocalAddr() net.Addr {
	req := getTCPAddrRequest{isLocal: true, resp: make(chan net.Addr, 1)}
	c.cmdChan <- req
	return <-req.resp
}

func (c *Connection) TcpRemoteAddr() net.Addr {
	req := getTCPAddrRequest{isLocal: false, resp: make(chan net.Addr, 1)}
	c.cmdChan <- req
	return <-req.resp
}

// Các hàm thiết lập thông tin (setter)
func (c *Connection) Init(address common.Address, cType string) {
	c.cmdChan <- cmdInit{payload: initPayload{address: address, cType: cType}}
}

func (c *Connection) SetRealConnAddr(realConnAddr string) {
	address := c.Address()
	cType := c.Type()
	c.cmdChan <- cmdInit{payload: initPayload{address: address, cType: cType, realConnAddr: realConnAddr}}
}

// Các hàm khác
func (c *Connection) Clone() network.Connection {
	req := cmdClone{resp: make(chan network.Connection, 1)}
	c.cmdChan <- req
	return <-req.resp
}

func (c *Connection) RemoteAddr() string {
	return c.RemoteAddrSafe()
}

func (c *Connection) ConnectionAddress() (string, error) {
	addr := c.RemoteAddrSafe()
	if addr == "" {
		return "", errors.New("địa chỉ kết nối thực chưa được đặt")
	}
	return addr, nil
}

func (c *Connection) String() string {
	addr := c.Address().Hex()
	cType := c.Type()
	connAddr := c.RemoteAddrSafe()
	isConnect := c.IsConnect()
	return fmt.Sprintf(
		"Connection[NodeAddress: %v, Type: %v, TCPAddress: %v, Connected: %t]",
		addr, cType, connAddr, isConnect,
	)
}

// --- Vòng lặp đọc/ghi (private), được gọi bởi goroutine quản lý ---

func (c *Connection) writeLoop(tcpConn net.Conn, sendChan chan network.Message, wg *sync.WaitGroup) {
	defer wg.Done()
	writer := bufio.NewWriter(tcpConn)
	remoteAddr := tcpConn.RemoteAddr().String()

	for message := range sendChan {
		b, err := message.Marshal()
		if err != nil {
			logger.Error("writeLoop %s: marshal error: %v", remoteAddr, err)
			continue
		}
		_ = tcpConn.SetWriteDeadline(time.Now().Add(c.config.WriteTimeout))
		length := make([]byte, 8)
		binary.LittleEndian.PutUint64(length, uint64(len(b)))

		if _, err := writer.Write(length); err != nil {
			logger.Error("writeLoop %s: write length error: %v", remoteAddr, err)
			return
		}
		if _, err := writer.Write(b); err != nil {
			logger.Error("writeLoop %s: write data error: %v", remoteAddr, err)
			return
		}
		if err := writer.Flush(); err != nil {
			logger.Error("writeLoop %s: flush error: %v", remoteAddr, err)
			return
		}
		_ = tcpConn.SetWriteDeadline(time.Time{})
	}
}

func (c *Connection) readLoop(tcpConn net.Conn, requestChan chan<- network.Request, errorChan chan<- error, wg *sync.WaitGroup, quit <-chan struct{}) {
	defer wg.Done()

	reader := bufio.NewReader(tcpConn)
	remoteAddr := tcpConn.RemoteAddr().String()

	// Hàm này xử lý việc gửi các lỗi nghiêm trọng (khiến kết nối phải đóng)
	// một cách an toàn để không bị panic.
	handleTerminalError := func(err error, context string) {
		logger.Warn("readLoop %s: Terminal error during '%s': %v. Signaling for disconnect.", remoteAddr, context, err)
		select {
		case errorChan <- err:
			// Đã gửi lỗi thành công. HandleConnection sẽ xử lý việc dọn dẹp.
		case <-quit:
			// Quá trình dọn dẹp đã được bắt đầu từ nơi khác, không cần gửi lỗi nữa.
			logger.Info("readLoop %s: Bypassing error send, quit signal already received.", remoteAddr)
		default:
			// Trường hợp này hiếm gặp, có thể do errorChan đầy.
			// Dù sao kết nối cũng sẽ bị đóng.
			logger.Error("readLoop %s: errorChan is full or closed. Could not send terminal error.", remoteAddr)
		}
	}

	for {
		bLength := make([]byte, 8)
		_, err := io.ReadFull(reader, bLength)
		if err != nil {
			// Bất kỳ lỗi nào từ ReadFull (kể cả io.EOF khi client đóng kết nối)
			// đều là tín hiệu kết thúc. Phải thông báo cho HandleConnection để dọn dẹp.
			handleTerminalError(err, "reading message length")
			return // Thoát khỏi vòng lặp đọc.
		}

		messageLength := binary.LittleEndian.Uint64(bLength)

		if messageLength == 0 {
			continue
		}
		if messageLength > c.config.MaxMessageLength {
			errExceed := fmt.Errorf("%w: received %d, max %d", ErrExceedMessageLength, messageLength, c.config.MaxMessageLength)
			handleTerminalError(errExceed, "checking message length")
			return
		}

		buf := bytebufferpool.Get()
		_, err = io.CopyN(buf, reader, int64(messageLength))
		if err != nil {
			bytebufferpool.Put(buf)
			handleTerminalError(err, "reading message content")
			return
		}

		msgProto := &pb.Message{}
		err = proto.Unmarshal(buf.B, msgProto)
		bytebufferpool.Put(buf)
		if err != nil {
			handleTerminalError(fmt.Errorf("unmarshal error: %w", err), "unmarshaling")
			return
		}

		req := requestPool.Get().(network.Request)
		req.Reset(c, NewMessage(msgProto))

		select {
		case requestChan <- req:
			// Gửi thành công.
		case <-quit:
			// Tín hiệu dọn dẹp từ goroutine khác. Hủy request và thoát.
			requestPool.Put(req)
			logger.Warn("readLoop %s: quit signal received, discarding request and exiting.", remoteAddr)
			return
		case <-time.After(c.config.RequestChanWaitTimeout):
			requestPool.Put(req)
			logger.Error("readLoop %s: request channel full. Dropping request.", remoteAddr)
		}
	}
}
