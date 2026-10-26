package websocket

import (
	"context"
	"net/url"
	"sync"
	"time"

	"github.com/google/uuid"
	ws "github.com/gorilla/websocket"
)

var channelBufSize = 256

// SessionID 是 websocket 会话唯一标识。
type SessionID string

// RequestInfo 保存 websocket 握手请求中后续 message trace 需要复用的信息。
type RequestInfo struct {
	Path       string
	RemoteAddr string
	RequestID  string
}

// Session 表示一个客户端 websocket 连接会话。
type Session struct {
	id SessionID

	conn     *ws.Conn
	queries  url.Values
	metadata SessionMetadata
	baseCtx  context.Context
	request  RequestInfo

	send chan outboundMessage

	hooks SessionHooks

	// 最近一次读写消息的时间，便于后续扩展心跳或空闲检测。
	lastReadMessageTime  time.Time
	lastWriteMessageTime time.Time

	waitingHeartbeatAck bool
	lastHeartbeatPingAt time.Time
	heartbeatAckDueAt   time.Time
	lastHeartbeatAckAt  time.Time

	metadataMu sync.RWMutex
	connMu     sync.RWMutex
	stateMu    sync.RWMutex
	done       chan struct{}
	listenOnce sync.Once
	closeOnce  sync.Once
	wg         sync.WaitGroup
}

type outboundMessage struct {
	data []byte
	kind outboundMessageKind
}

type outboundMessageKind uint8

const (
	outboundMessageKindPayload outboundMessageKind = iota
	outboundMessageKindBinary
)

// SessionHooks 定义 Session 与上层 Server 交互时需要回调的行为。
type SessionHooks interface {
	removeSession(*Session)
	handleSocketRawData(context.Context, SessionID, int, []byte) error
	getPayloadType() PayloadType
	handleBusinessHeartbeatFrame(SessionID, int, []byte) bool
}

// NewSession 创建一个新的 websocket 会话。
func NewSession(hooks SessionHooks, conn *ws.Conn, queries url.Values, metadata SessionMetadata) *Session {
	return newSessionWithContext(hooks, conn, queries, metadata, context.Background(), RequestInfo{})
}

func newSessionWithContext(hooks SessionHooks, conn *ws.Conn, queries url.Values, metadata SessionMetadata, baseCtx context.Context, request RequestInfo) *Session {
	if conn == nil {
		panic("conn cannot be nil")
	}
	if baseCtx == nil {
		baseCtx = context.Background()
	}

	return &Session{
		id:       SessionID(uuid.NewString()),
		conn:     conn,
		queries:  cloneURLValues(queries),
		metadata: cloneSessionMetadata(metadata),
		baseCtx:  baseCtx,
		request:  cloneRequestInfo(request),
		send:     make(chan outboundMessage, channelBufSize),
		hooks:    hooks,
		done:     make(chan struct{}),
	}
}

// Conn 返回底层 websocket 连接。
func (s *Session) Conn() *ws.Conn {
	s.connMu.RLock()
	defer s.connMu.RUnlock()
	return s.conn
}

// Queries 返回握手阶段的原始 query 参数。
func (s *Session) Queries() url.Values {
	return cloneURLValues(s.queries)
}

// Metadata 返回当前会话保存的服务端元数据副本。
func (s *Session) Metadata() SessionMetadata {
	s.metadataMu.RLock()
	defer s.metadataMu.RUnlock()
	return cloneSessionMetadata(s.metadata)
}

// Context 返回当前会话保存的握手 trace parent 上下文。
func (s *Session) Context() context.Context {
	if s.baseCtx == nil {
		return context.Background()
	}
	return s.baseCtx
}

// RequestInfo 返回握手请求信息副本。
func (s *Session) RequestInfo() RequestInfo {
	return cloneRequestInfo(s.request)
}

// Path 返回握手请求路径。
func (s *Session) Path() string {
	return s.request.Path
}

// RemoteAddr 返回握手请求来源地址。
func (s *Session) RemoteAddr() string {
	return s.request.RemoteAddr
}

// RequestID 返回握手请求中的 request_id。
func (s *Session) RequestID() string {
	return s.request.RequestID
}

// GetMetadata 读取指定 key 的元数据。
func (s *Session) GetMetadata(key string) (any, bool) {
	s.metadataMu.RLock()
	defer s.metadataMu.RUnlock()
	if s.metadata == nil {
		return nil, false
	}
	value, ok := s.metadata[key]
	return value, ok
}

// SetMetadata 设置指定 key 的元数据。
func (s *Session) SetMetadata(key string, value any) {
	s.metadataMu.Lock()
	defer s.metadataMu.Unlock()
	if s.metadata == nil {
		s.metadata = make(SessionMetadata)
	}
	s.metadata[key] = value
}

// DeleteMetadata 删除指定 key 的元数据。
func (s *Session) DeleteMetadata(key string) {
	s.metadataMu.Lock()
	defer s.metadataMu.Unlock()
	if s.metadata == nil {
		return
	}
	delete(s.metadata, key)
}

// SessionID 返回当前会话 ID。
func (s *Session) SessionID() SessionID {
	return s.id
}

// SendMessage 将消息投递到当前会话的发送队列。
func (s *Session) SendMessage(message []byte) {
	s.enqueueMessage(outboundMessage{data: message, kind: outboundMessageKindPayload})
}

// sendBinaryFrame 用于发送“绕过业务封包”的原始 binary frame。
// 目前只给业务层 heartbeat 使用，避免被 WithPayloadType / 消息编解码逻辑影响。
func (s *Session) sendBinaryFrame(message []byte) {
	s.enqueueMessage(outboundMessage{data: message, kind: outboundMessageKindBinary})
}

func (s *Session) enqueueMessage(message outboundMessage) {
	select {
	case <-s.done:
		return
	case s.send <- message:
	}
}

// Close 关闭当前会话并通知上层移除 session。
func (s *Session) Close() {
	s.closeOnce.Do(func() {
		close(s.done)
		s.markHeartbeatClosed()
		s.closeConnect()
		if s.hooks != nil {
			s.hooks.removeSession(s)
		}
	})
}

// Listen 启动当前会话的读写协程。
func (s *Session) Listen() {
	s.listenOnce.Do(func() {
		s.wg.Add(2)
		go s.writePump()
		go s.readPump()
	})
}

// Wait 等待读写协程退出。
func (s *Session) Wait() {
	s.wg.Wait()
}

func (s *Session) closeConnect() {
	s.connMu.Lock()
	conn := s.conn
	s.conn = nil
	s.connMu.Unlock()

	if conn != nil {
		if err := conn.Close(); err != nil {
			LogErrorf("disconnect error: %s", err.Error())
		}
	}
}

func (s *Session) sendPingMessage(message string) error {
	if s.Conn() == nil {
		return nil
	}
	return s.Conn().WriteMessage(ws.PingMessage, []byte(message))
}

func (s *Session) sendPongMessage(message string) error {
	if s.Conn() == nil {
		return nil
	}
	return s.Conn().WriteMessage(ws.PongMessage, []byte(message))
}

func (s *Session) writeTextMessage(message string) error {
	if s.Conn() == nil {
		return nil
	}
	return s.Conn().WriteMessage(ws.TextMessage, []byte(message))
}

func (s *Session) writeBinaryMessage(message []byte) error {
	if s.Conn() == nil {
		return nil
	}
	return s.Conn().WriteMessage(ws.BinaryMessage, message)
}

func (s *Session) writePump() {
	defer s.wg.Done()
	defer s.Close()

	for {
		select {
		case <-s.done:
			return
		case msg := <-s.send:
			s.lastWriteMessageTime = time.Now()

			var err error
			switch msg.kind {
			case outboundMessageKindBinary:
				err = s.writeBinaryMessage(msg.data)
			default:
				switch s.hooks.getPayloadType() {
				case PayloadTypeBinary:
					err = s.writeBinaryMessage(msg.data)
				case PayloadTypeText:
					err = s.writeTextMessage(string(msg.data))
				}
			}
			if err != nil {
				LogError("write message error: ", err)
				return
			}
		}
	}
}

func (s *Session) readPump() {
	defer s.wg.Done()
	defer s.Close()

	for {
		select {
		case <-s.done:
			return
		default:
		}

		conn := s.Conn()
		if conn == nil {
			return
		}

		messageType, data, err := conn.ReadMessage()
		if err != nil {
			if ws.IsUnexpectedCloseError(err, ws.CloseNormalClosure, ws.CloseGoingAway, ws.CloseAbnormalClosure) {
				LogErrorf("read message error: %v", err)
			}
			return
		}

		s.lastReadMessageTime = time.Now()

		switch messageType {
		case ws.CloseMessage:
			return
		case ws.BinaryMessage, ws.TextMessage:
			// 业务层 heartbeat 先于业务 handler 消费：
			// - 服务端把 0B binary frame 当作 heartbeat ack
			// - 其它消息再下沉到业务消息处理逻辑
			if s.hooks != nil && s.hooks.handleBusinessHeartbeatFrame(s.SessionID(), messageType, data) {
				continue
			}
			if s.hooks != nil {
				_ = s.hooks.handleSocketRawData(s.Context(), s.SessionID(), messageType, data)
			}
		case ws.PingMessage:
			if err = s.sendPongMessage(""); err != nil {
				LogError("write pong message error: ", err)
				return
			}
		case ws.PongMessage:
		}
	}
}

func (s *Session) beginHeartbeatWait(now time.Time, ackTimeout time.Duration) bool {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	select {
	case <-s.done:
		return false
	default:
	}

	if s.waitingHeartbeatAck {
		// 上一次 ping 还没收到 ack，本轮不重复发新的 ping，等待 timeout sweep 统一处理。
		return false
	}

	// 发送 ping 前先把“等待 ack”状态与 deadline 记录下来；
	// timeout loop 会按这个 deadline 判断是否需要踢连接。
	s.waitingHeartbeatAck = true
	s.lastHeartbeatPingAt = now
	s.heartbeatAckDueAt = now.Add(ackTimeout)
	return true
}

// acknowledgeHeartbeat 在服务端收到客户端 0B binary ack 后清除等待态。
func (s *Session) acknowledgeHeartbeat(now time.Time) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	s.waitingHeartbeatAck = false
	s.heartbeatAckDueAt = time.Time{}
	s.lastHeartbeatAckAt = now
}

// isHeartbeatAckTimedOut 判断当前 session 是否超过 ack deadline 仍未回 pong。
func (s *Session) isHeartbeatAckTimedOut(now time.Time) bool {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()

	return s.waitingHeartbeatAck && !s.heartbeatAckDueAt.IsZero() && !s.heartbeatAckDueAt.After(now)
}

// markHeartbeatClosed 在连接关闭时清理 heartbeat 状态，避免残留 deadline。
func (s *Session) markHeartbeatClosed() {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	s.waitingHeartbeatAck = false
	s.heartbeatAckDueAt = time.Time{}
}

func cloneSessionMetadata(src SessionMetadata) SessionMetadata {
	if len(src) == 0 {
		return make(SessionMetadata)
	}

	dst := make(SessionMetadata, len(src))
	for key, value := range src {
		dst[key] = value
	}
	return dst
}

func cloneRequestInfo(src RequestInfo) RequestInfo {
	return src
}
