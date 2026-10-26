package websocket

import (
	"context"
	"net/http"

	kratosTransport "github.com/go-kratos/kratos/v2/transport"
)

const (
	// KindWebsocket 表示 websocket transport 类型。
	KindWebsocket = "websocket"
)

var _ Transporter = (*Transport)(nil)

// Transporter 扩展了 kratos 的 Transporter，补充了 request/path 信息。
type Transporter interface {
	kratosTransport.Transporter
	Request() *http.Request
	PathTemplate() string
}

// Transport 是 websocket 连接在 kratos 上下文中的 transport 描述。
type Transport struct {
	endpoint       string
	operation      string
	reqHeader      headerCarrier
	replyHeader    headerCarrier
	request        *http.Request
	pathTemplate   string
	path           string
	remoteAddr     string
	requestID      string
	sessionID      SessionID
	messageType    NetMessageType
	hasMessageType bool
	frameType      int
	hasFrameType   bool
}

// Kind 返回 transport 类型。
func (tr *Transport) Kind() kratosTransport.Kind {
	return KindWebsocket
}

// Endpoint 返回 transport endpoint。
func (tr *Transport) Endpoint() string {
	return tr.endpoint
}

// Operation 返回 transport operation。
func (tr *Transport) Operation() string {
	return tr.operation
}

// Request 返回原始 HTTP 请求。
func (tr *Transport) Request() *http.Request {
	return tr.request
}

// RequestHeader 返回请求头。
func (tr *Transport) RequestHeader() kratosTransport.Header {
	return tr.reqHeader
}

// ReplyHeader 返回响应头。
func (tr *Transport) ReplyHeader() kratosTransport.Header {
	return tr.replyHeader
}

// PathTemplate 返回请求路径模板。
func (tr *Transport) PathTemplate() string {
	return tr.pathTemplate
}

// Path 返回 websocket 握手路径。
func (tr *Transport) Path() string {
	return tr.path
}

// RemoteAddr 返回客户端地址。
func (tr *Transport) RemoteAddr() string {
	return tr.remoteAddr
}

// RequestID 返回握手请求中的 request_id。
func (tr *Transport) RequestID() string {
	return tr.requestID
}

// SessionID 返回当前 websocket 会话 ID。
func (tr *Transport) SessionID() SessionID {
	return tr.sessionID
}

// MessageType 返回当前业务消息类型。
func (tr *Transport) MessageType() (NetMessageType, bool) {
	return tr.messageType, tr.hasMessageType
}

// FrameType 返回当前原始 websocket 帧类型。
func (tr *Transport) FrameType() (int, bool) {
	return tr.frameType, tr.hasFrameType
}

// SetOperation 在服务端上下文中动态设置 operation。
func SetOperation(ctx context.Context, op string) {
	if tr, ok := kratosTransport.FromServerContext(ctx); ok {
		if tr, ok := tr.(*Transport); ok {
			tr.operation = op
		}
	}
}

func setTransportSession(ctx context.Context, session *Session) {
	if session == nil {
		return
	}
	if tr, ok := kratosTransport.FromServerContext(ctx); ok {
		if tr, ok := tr.(*Transport); ok {
			tr.sessionID = session.SessionID()
		}
	}
}

type headerCarrier http.Header

// Get 返回 key 对应的 header 值。
func (hc headerCarrier) Get(key string) string {
	return http.Header(hc).Get(key)
}

// Set 设置一个 header。
func (hc headerCarrier) Set(key, value string) {
	http.Header(hc).Set(key, value)
}

// Keys 返回当前 carrier 中所有 header key。
func (hc headerCarrier) Keys() []string {
	keys := make([]string, 0, len(hc))
	for k := range http.Header(hc) {
		keys = append(keys, k)
	}
	return keys
}

// Add 追加一个 header 值。
func (hc headerCarrier) Add(key string, value string) {
	http.Header(hc).Add(key, value)
}

// Values 返回 key 对应的所有值。
func (hc headerCarrier) Values(key string) []string {
	return http.Header(hc).Values(key)
}
