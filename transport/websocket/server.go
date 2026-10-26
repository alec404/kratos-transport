package websocket

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/go-kratos/kratos/v2/encoding"
	"github.com/go-kratos/kratos/v2/middleware"
	kratosTransport "github.com/go-kratos/kratos/v2/transport"
	ws "github.com/gorilla/websocket"

	"github.com/alec404/kratos-transport/broker"
	transportutil "github.com/alec404/kratos-transport/transport"
)

var (
	_ kratosTransport.Server     = (*Server)(nil)
	_ kratosTransport.Endpointer = (*Server)(nil)
)

// Server 是基于 websocket 的 kratos transport.Server 实现。
type Server struct {
	*http.Server

	lis      net.Listener
	tlsConf  *tls.Config
	upgrader *ws.Upgrader
	endpoint *url.URL

	network     string
	address     string
	path        string
	strictSlash bool

	err   error
	codec encoding.Codec

	sessionManager *SessionManager

	payloadType PayloadType

	messageHandlers NetMessageHandlerMap

	netPacketMarshaler   NetPacketMarshaler
	netPacketUnmarshaler NetPacketUnmarshaler

	handshakeHandler  HandshakeHandler
	connectHandler    ConnectHandler
	disconnectHandler DisconnectHandler
	rawFrameHandler   RawFrameHandler

	middlewares []middleware.Middleware

	businessHeartbeat businessHeartbeatConfig
	heartbeatDone     chan struct{}
	heartbeatWg       sync.WaitGroup

	running   bool
	stateMu   sync.RWMutex
	handlerMu sync.RWMutex
}

// NewServer 创建一个 websocket Server。
func NewServer(opts ...ServerOption) *Server {
	srv := &Server{
		network:     "tcp",
		address:     ":0",
		strictSlash: true,
		path:        "/",

		messageHandlers: make(NetMessageHandlerMap),
		sessionManager:  NewSessionManager(nil),
		upgrader: &ws.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			CheckOrigin:     func(r *http.Request) bool { return true },
		},
		payloadType: PayloadTypeBinary,
		businessHeartbeat: businessHeartbeatConfig{
			checkInterval: defaultBusinessHeartbeatCheckInterval,
		},
	}

	if err := srv.init(opts...); err != nil {
		LogError("websocket server init error:", err)
		return nil
	}
	return srv
}

func (s *Server) init(opts ...ServerOption) error {
	for _, o := range opts {
		o(s)
	}

	if s.netPacketMarshaler == nil {
		s.netPacketMarshaler = s.defaultMarshalNetPacket
	}
	if s.netPacketUnmarshaler == nil {
		s.netPacketUnmarshaler = s.defaultUnmarshalNetPacket
	}
	if s.codec == nil {
		s.codec = encoding.GetCodec("json")
		if s.codec == nil {
			s.codec = encoding.GetCodec("bytes")
		}
	}

	mux := http.NewServeMux()
	mux.HandleFunc(s.path, s.wsHandler)
	if s.strictSlash {
		altPath := alternatePath(s.path)
		if altPath != s.path {
			mux.HandleFunc(altPath, s.wsHandler)
		}
	}

	s.Server = &http.Server{
		Handler:   mux,
		TLSConfig: s.tlsConf,
	}
	return s.err
}

// Name 返回 transport 名称。
func (s *Server) Name() string {
	return KindWebsocket
}

// RegisterMessageHandler 注册一个服务端业务消息处理器。
func (s *Server) RegisterMessageHandler(messageType NetMessageType, handler NetMessageHandler, binder Creator) {
	s.handlerMu.Lock()
	defer s.handlerMu.Unlock()

	if _, ok := s.messageHandlers[messageType]; ok {
		return
	}

	s.messageHandlers[messageType] = &MessageHandlerData{
		MessageType: messageType,
		Handler:     handler,
		Creator:     binder,
	}
}

// RegisterServerMessageHandler 使用泛型注册 context-first 服务端消息处理器。
func RegisterServerMessageHandler[T any](srv *Server, messageType NetMessageType, handler func(context.Context, *Session, *T) error) {
	srv.RegisterMessageHandler(messageType,
		func(ctx context.Context, session *Session, payload MessagePayload) error {
			switch t := payload.(type) {
			case *T:
				return handler(ctx, session, t)
			default:
				LogError("invalid payload struct type:", t)
				return errors.New("invalid payload struct type")
			}
		},
		func() any {
			var t T
			return &t
		},
	)
}

// DeregisterMessageHandler 注销一个消息处理器。
func (s *Server) DeregisterMessageHandler(messageType NetMessageType) {
	s.handlerMu.Lock()
	defer s.handlerMu.Unlock()
	delete(s.messageHandlers, messageType)
}

// GetMessageHandler 获取指定类型的消息处理器。
func (s *Server) GetMessageHandler(messageType NetMessageType) *MessageHandlerData {
	s.handlerMu.RLock()
	defer s.handlerMu.RUnlock()
	return s.messageHandlers[messageType]
}

func (s *Server) marshalMessage(messageType NetMessageType, message MessagePayload) ([]byte, error) {
	if s.netPacketMarshaler != nil {
		return s.netPacketMarshaler(messageType, message)
	}
	return s.defaultMarshalNetPacket(messageType, message)
}

func (s *Server) defaultMarshalNetPacket(messageType NetMessageType, message MessagePayload) ([]byte, error) {
	var err error
	var buff []byte

	switch s.payloadType {
	case PayloadTypeBinary:
		var msg BinaryNetPacket
		msg.Type = messageType
		msg.Payload, err = broker.Marshal(s.codec, message)
		if err != nil {
			return nil, err
		}
		buff, err = msg.Marshal()
		if err != nil {
			return nil, err
		}
	case PayloadTypeText:
		var buf []byte
		var msg TextNetPacket
		msg.Type = messageType
		buf, err = broker.Marshal(s.codec, message)
		if err != nil {
			return nil, err
		}
		msg.Payload = string(buf)
		buff, err = json.Marshal(msg)
		if err != nil {
			return nil, err
		}
	}

	return buff, nil
}

// SendRawMessage 向指定会话发送原始消息。
func (s *Server) SendRawMessage(sessionID SessionID, message []byte) error {
	session := s.sessionManager.GetSession(sessionID)
	if session == nil {
		LogError("session not found:", sessionID)
		return errors.New("session not found")
	}

	session.SendMessage(message)
	return nil
}

// SendMessage 向指定会话发送业务消息。
func (s *Server) SendMessage(sessionID SessionID, messageType NetMessageType, message MessagePayload) error {
	buf, err := s.marshalMessage(messageType, message)
	if err != nil {
		LogError("marshal message error:", err)
		return err
	}
	return s.SendRawMessage(sessionID, buf)
}

// Broadcast 向所有在线会话广播业务消息。
func (s *Server) Broadcast(messageType NetMessageType, message MessagePayload) {
	buf, err := s.marshalMessage(messageType, message)
	if err != nil {
		LogError("marshal message error:", err)
		return
	}

	s.sessionManager.RangeSessions(func(_ SessionID, session *Session) bool {
		session.SendMessage(buf)
		return true
	})
}

func (s *Server) unmarshalNetPacket(buf []byte) (*MessageHandlerData, MessagePayload, error) {
	if s.netPacketUnmarshaler != nil {
		return s.netPacketUnmarshaler(buf)
	}
	return s.defaultUnmarshalNetPacket(buf)
}

func (s *Server) defaultUnmarshalNetPacket(buf []byte) (handler *MessageHandlerData, payload MessagePayload, err error) {
	var messageType NetMessageType
	var rawPayload []byte

	switch s.payloadType {
	case PayloadTypeBinary:
		var msg BinaryNetPacket
		if err = msg.Unmarshal(buf); err != nil {
			LogErrorf("decode message exception: %s", err)
			return nil, nil, err
		}
		messageType = msg.Type
		rawPayload = msg.Payload
	case PayloadTypeText:
		var msg TextNetPacket
		if err = msg.Unmarshal(buf); err != nil {
			LogErrorf("decode message exception: %s", err)
			return nil, nil, err
		}
		messageType = msg.Type
		rawPayload = []byte(msg.Payload)
	}

	if handler = s.GetMessageHandler(messageType); handler == nil {
		LogError("message handler not found:", messageType)
		return nil, nil, errors.New("message handler not found")
	}

	if payload = handler.Create(); payload == nil {
		payload = rawPayload
	} else {
		if err = broker.Unmarshal(s.codec, rawPayload, payload); err != nil {
			LogErrorf("unmarshal message exception: %s", err)
			return nil, nil, err
		}
	}

	return handler, payload, nil
}

// handleSocketRawData 处理 websocket 原始数据。
func (s *Server) handleSocketRawData(ctx context.Context, sessionID SessionID, wsMessageType int, buf []byte) error {
	session := s.GetSession(sessionID)
	if session == nil {
		return errors.New("session not found")
	}

	frame := Frame{Type: wsMessageType, Data: buf}
	if s.rawFrameHandler != nil {
		frameCtx := s.newFrameContext(session.Context(), session, frame)
		handler := s.chainMiddleware(func(ctx context.Context, req any) (any, error) {
			return nil, s.rawFrameHandler(ctx, session, frame)
		})
		_, err := handler(frameCtx, frame)
		return err
	}

	handler, payload, err := s.unmarshalNetPacket(buf)
	if err != nil {
		LogErrorf("unmarshal message failed: %s", err)
		return err
	}

	messageCtx := s.newMessageContext(ctx, session, handler.MessageType)
	messageHandler := s.chainMiddleware(func(ctx context.Context, req any) (any, error) {
		if err = handler.Handle(ctx, session, payload); err != nil {
			LogErrorf("message handler failed: %s", err)
			return nil, err
		}
		return nil, nil
	})
	_, err = messageHandler(messageCtx, payload)
	return err
}

func (s *Server) handleConnect(ctx context.Context, session *Session) error {
	if s.connectHandler == nil || session == nil {
		return nil
	}
	return s.connectHandler(ctx, session)
}

func (s *Server) handleDisconnect(session *Session) {
	if s.disconnectHandler == nil || session == nil {
		return
	}
	ctx := s.newDisconnectContext(session.Context(), session)
	handler := s.chainMiddleware(func(ctx context.Context, req any) (any, error) {
		s.disconnectHandler(ctx, session)
		return nil, nil
	})
	if _, err := handler(ctx, session); err != nil {
		LogErrorf("disconnect handler failed: %s", err)
	}
}

func (s *Server) handleBusinessHeartbeatFrame(sessionID SessionID, messageType int, data []byte) bool {
	// 服务端只把“0B binary frame”视为 heartbeat ack；
	// heartbeat ping 本身是服务端主动发送，不会在这里消费 [0xB0, 0x04]。
	if !s.businessHeartbeat.isEnabled() || messageType != ws.BinaryMessage || len(data) != 0 {
		return false
	}

	session := s.GetSession(sessionID)
	if session != nil {
		session.acknowledgeHeartbeat(time.Now())
	}
	return true
}

func (s *Server) wsHandler(res http.ResponseWriter, req *http.Request) {
	baseCtx := newTraceBaseContext(req.Context(), req.Header)
	ctx := s.newHandshakeContext(baseCtx, res, req)
	handler := s.chainMiddleware(func(ctx context.Context, reqAny any) (any, error) {
		return nil, s.handleWebSocketHandshake(ctx, baseCtx, res, req)
	})
	if _, err := handler(ctx, req); err != nil {
		LogError("websocket handler error:", err)
	}
}

func (s *Server) handleWebSocketHandshake(ctx, baseCtx context.Context, res http.ResponseWriter, req *http.Request) error {
	req = req.WithContext(ctx)
	queries := cloneURLValues(req.URL.Query())
	metadata := make(SessionMetadata)

	if s.handshakeHandler != nil {
		if err := s.handshakeHandler(ctx, req, metadata); err != nil {
			LogWarnf("websocket handshake rejected: %v", err)
			http.Error(res, err.Error(), http.StatusUnauthorized)
			return err
		}
	}

	upgrader := *s.upgrader

	conn, err := upgrader.Upgrade(res, req, nil)
	if err != nil {
		LogError("upgrade exception:", err)
		return err
	}

	requestInfo := RequestInfo{
		Path:       req.URL.Path,
		RemoteAddr: req.RemoteAddr,
		RequestID:  requestIDFromHeader(req.Header),
	}
	session := newSessionWithContext(s, conn, queries, metadata, baseCtx, requestInfo)
	setTransportSession(ctx, session)
	s.sessionManager.AddSession(session)
	if err = s.handleConnect(ctx, session); err != nil {
		session.Close()
		return err
	}
	session.Listen()
	return nil
}

func (s *Server) newHandshakeContext(parent context.Context, res http.ResponseWriter, req *http.Request) context.Context {
	if parent == nil {
		parent = context.Background()
	}
	pathTemplate := s.pathTemplate(req.URL.Path)
	requestID := requestIDFromHeader(req.Header)
	tr := &Transport{
		endpoint:     s.endpointString(),
		operation:    websocketHandshakeOperation(pathTemplate),
		reqHeader:    headerCarrier(req.Header),
		replyHeader:  headerCarrier(res.Header()),
		request:      req,
		pathTemplate: pathTemplate,
		path:         req.URL.Path,
		remoteAddr:   req.RemoteAddr,
		requestID:    requestID,
	}
	ctx := kratosTransport.NewServerContext(parent, tr)
	tr.request = req.WithContext(ctx)
	return ctx
}

func (s *Server) newMessageContext(parent context.Context, session *Session, messageType NetMessageType) context.Context {
	return s.newSessionEventContext(parent, session, websocketMessageOperation(s.pathTemplate(session.Path()), messageType), func(tr *Transport) {
		tr.messageType = messageType
		tr.hasMessageType = true
	})
}

func (s *Server) newFrameContext(parent context.Context, session *Session, frame Frame) context.Context {
	return s.newSessionEventContext(parent, session, websocketFrameOperation(s.pathTemplate(session.Path()), frame.Type), func(tr *Transport) {
		tr.frameType = frame.Type
		tr.hasFrameType = true
	})
}

func (s *Server) newDisconnectContext(parent context.Context, session *Session) context.Context {
	return s.newSessionEventContext(parent, session, websocketDisconnectOperation(s.pathTemplate(session.Path())), nil)
}

func (s *Server) newSessionEventContext(parent context.Context, session *Session, operation string, setup func(*Transport)) context.Context {
	if parent == nil {
		parent = context.Background()
	}
	var requestInfo RequestInfo
	var sessionID SessionID
	if session != nil {
		requestInfo = session.RequestInfo()
		sessionID = session.SessionID()
	}
	pathTemplate := s.pathTemplate(requestInfo.Path)
	req := &http.Request{
		Method:     http.MethodGet,
		URL:        &url.URL{Path: requestInfo.Path},
		Header:     make(http.Header),
		RemoteAddr: requestInfo.RemoteAddr,
	}
	tr := &Transport{
		endpoint:     s.endpointString(),
		operation:    operation,
		reqHeader:    headerCarrier(http.Header{}),
		replyHeader:  headerCarrier(http.Header{}),
		request:      req,
		pathTemplate: pathTemplate,
		path:         requestInfo.Path,
		remoteAddr:   requestInfo.RemoteAddr,
		requestID:    requestInfo.RequestID,
		sessionID:    sessionID,
	}
	if setup != nil {
		setup(tr)
	}
	ctx := kratosTransport.NewServerContext(parent, tr)
	tr.request = req.WithContext(ctx)
	return ctx
}

func (s *Server) chainMiddleware(handler middleware.Handler) middleware.Handler {
	middlewares := make([]middleware.Middleware, 0, len(s.middlewares)+1)
	middlewares = append(middlewares, s.middlewares...)
	middlewares = append(middlewares, websocketSpanAttributeMiddleware)
	return middleware.Chain(middlewares...)(handler)
}

func (s *Server) endpointString() string {
	if s.endpoint == nil {
		return ""
	}
	return s.endpoint.String()
}

func (s *Server) pathTemplate(path string) string {
	if s.path != "" {
		return s.path
	}
	return path
}

func requestIDFromHeader(header http.Header) string {
	for _, key := range []string{"X-Request-Id", "X-Request-ID", "Request-Id", "Request-ID", "request_id"} {
		if value := header.Get(key); value != "" {
			return value
		}
	}
	return ""
}

func (s *Server) listenAndEndpoint() error {
	if s.lis == nil {
		lis, err := net.Listen(s.network, s.address)
		if err != nil {
			return err
		}
		s.lis = lis
	}

	if s.endpoint == nil {
		addr := s.lis.Addr().String()
		host, _, splitErr := net.SplitHostPort(addr)
		if splitErr == nil && (host == "" || host == "0.0.0.0" || host == "::" || host == "[::]") {
			addr, splitErr = transportutil.AdjustAddress(s.address, s.lis)
		}
		if splitErr != nil {
			return splitErr
		}
		s.endpoint = transportutil.NewRegistryEndpoint(KindWebsocket, addr)
	}

	return nil
}

// Endpoint 返回用于服务发现注册的 endpoint。
func (s *Server) Endpoint() (*url.URL, error) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	if err := s.listenAndEndpoint(); err != nil {
		return nil, err
	}
	return s.endpoint, nil
}

// Start 启动 websocket 服务。
func (s *Server) Start(ctx context.Context) error {
	s.stateMu.Lock()
	if s.running {
		s.stateMu.Unlock()
		return nil
	}
	if s.err = s.listenAndEndpoint(); s.err != nil {
		s.stateMu.Unlock()
		return s.err
	}
	s.running = true
	s.stateMu.Unlock()

	s.BaseContext = func(net.Listener) context.Context {
		return ctx
	}
	s.startBusinessHeartbeatLoops()
	defer s.stopBusinessHeartbeatLoops()
	LogInfof("server listening on: %s", s.lis.Addr().String())

	var err error
	if s.tlsConf != nil {
		err = s.ServeTLS(s.lis, "", "")
	} else {
		err = s.Serve(s.lis)
	}
	if !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, net.ErrClosed) {
		return err
	}
	return nil
}

// Stop 停止 websocket 服务并关闭所有在线连接。
func (s *Server) Stop(ctx context.Context) error {
	s.stateMu.Lock()
	if !s.running {
		s.stateMu.Unlock()
		return nil
	}
	s.running = false
	s.stateMu.Unlock()

	LogInfo("server stopping...")
	s.stopBusinessHeartbeatLoops()
	if s.sessionManager != nil {
		s.sessionManager.Clean()
	}

	err := s.Shutdown(ctx)
	s.err = nil
	LogInfo("server stopped.")
	return err
}

// SessionCount 返回当前在线 session 数量。
func (s *Server) SessionCount() int {
	if s.sessionManager == nil {
		return 0
	}
	return s.sessionManager.Count()
}

// GetSession 获取指定 sessionID 对应的会话。
func (s *Server) GetSession(sessionID SessionID) *Session {
	if s.sessionManager == nil {
		return nil
	}
	return s.sessionManager.GetSession(sessionID)
}

// RangeSessions 遍历当前所有在线会话。
func (s *Server) RangeSessions(fn func(SessionID, *Session) bool) {
	if s.sessionManager == nil || fn == nil {
		return
	}
	s.sessionManager.RangeSessions(fn)
}

// removeSession 从会话管理器中移除一个 session。
func (s *Server) removeSession(session *Session) {
	if s.sessionManager == nil {
		return
	}
	s.sessionManager.RemoveSession(session)
	s.handleDisconnect(session)
}

func (s *Server) getPayloadType() PayloadType {
	return s.payloadType
}

func (s *Server) startBusinessHeartbeatLoops() {
	if !s.businessHeartbeat.isEnabled() {
		return
	}

	s.stateMu.Lock()
	if s.heartbeatDone != nil {
		s.stateMu.Unlock()
		return
	}

	done := make(chan struct{})
	s.heartbeatDone = done
	interval := s.businessHeartbeat.interval
	checkInterval := s.businessHeartbeat.normalizedCheckInterval()
	s.heartbeatWg.Add(2)
	s.stateMu.Unlock()

	go s.runBusinessHeartbeatPingLoop(done, interval)
	go s.runBusinessHeartbeatTimeoutLoop(done, checkInterval)
}

func (s *Server) stopBusinessHeartbeatLoops() {
	s.stateMu.Lock()
	done := s.heartbeatDone
	if done == nil {
		s.stateMu.Unlock()
		return
	}
	s.heartbeatDone = nil
	close(done)
	s.stateMu.Unlock()

	s.heartbeatWg.Wait()
}

func (s *Server) runBusinessHeartbeatPingLoop(done <-chan struct{}, interval time.Duration) {
	defer s.heartbeatWg.Done()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case now := <-ticker.C:
			// ping loop 只负责“定时发起新一轮 heartbeat”。
			// 是否超时由独立的 timeout loop 判断，二者解耦后才能支持 interval > ackTimeout。
			s.broadcastBusinessHeartbeatPing(now)
		}
	}
}

func (s *Server) runBusinessHeartbeatTimeoutLoop(done <-chan struct{}, interval time.Duration) {
	defer s.heartbeatWg.Done()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case now := <-ticker.C:
			// timeout loop 负责扫描所有 session：
			// 只要超过 ack deadline 还没回 0B binary pong，就直接关闭连接。
			s.disconnectTimedOutHeartbeatSessions(now)
		}
	}
}

func (s *Server) broadcastBusinessHeartbeatPing(now time.Time) {
	s.RangeSessions(func(_ SessionID, session *Session) bool {
		if session == nil {
			return true
		}
		if !session.beginHeartbeatWait(now, s.businessHeartbeat.ackTimeout) {
			return true
		}
		// heartbeat 始终发送原始 binary frame，不走业务消息封包。
		session.sendBinaryFrame(append([]byte(nil), businessHeartbeatPingPayload...))
		return true
	})
}

func (s *Server) disconnectTimedOutHeartbeatSessions(now time.Time) {
	s.RangeSessions(func(_ SessionID, session *Session) bool {
		if session == nil {
			return true
		}
		if session.isHeartbeatAckTimedOut(now) {
			// 直接复用现有 Close 清理链路：
			// 关闭底层连接 -> 移出 SessionManager -> 触发 disconnect 回调。
			session.Close()
		}
		return true
	})
}

// OnSessionAdded 在会话加入时触发连接回调。
func (s *Server) OnSessionAdded(session *Session) {
	if session == nil {
		return
	}
	_ = s.handleConnect(session.Context(), session)
}

// OnSessionRemoved 在会话移除时触发断开回调。
func (s *Server) OnSessionRemoved(session *Session) {
	s.handleDisconnect(session)
}

func alternatePath(path string) string {
	if path == "/" {
		return path
	}
	if strings.HasSuffix(path, "/") {
		return strings.TrimRight(path, "/")
	}
	return path + "/"
}

func cloneURLValues(src url.Values) url.Values {
	if len(src) == 0 {
		return make(url.Values)
	}

	dst := make(url.Values, len(src))
	for key, values := range src {
		dst[key] = append([]string(nil), values...)
	}
	return dst
}
