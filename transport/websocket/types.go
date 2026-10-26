package websocket

import (
	"context"
	"net/http"
)

// SessionMetadata 表示服务端为当前 websocket 会话维护的附加元数据。
// 典型用途包括：保存鉴权后的用户身份、角色、租户信息等可信上下文。
type SessionMetadata map[string]any

// Frame 表示服务端收到的一帧原始 websocket 数据。
// Type 对应 gorilla/websocket 的 TextMessage / BinaryMessage 等消息类型。
type Frame struct {
	Type int
	Data []byte
}

// HandshakeHandler 用于在 websocket 握手阶段做自定义校验。
// 典型用途包括：读取 Cookie / Header 做鉴权，并把用户信息写入 metadata。
// 返回非 nil error 时，服务端会拒绝本次握手请求。
type HandshakeHandler func(ctx context.Context, req *http.Request, metadata SessionMetadata) error

// ConnectHandler 用于监听连接建立事件。
type ConnectHandler func(ctx context.Context, session *Session) error

// DisconnectHandler 用于监听连接断开事件。
type DisconnectHandler func(ctx context.Context, session *Session)

// RawFrameHandler 用于接管原始 websocket 帧处理。
type RawFrameHandler func(ctx context.Context, session *Session, frame Frame) error

// NetPacketMarshaler 将业务消息编码为网络包。
type NetPacketMarshaler func(messageType NetMessageType, message MessagePayload) ([]byte, error)

// NetPacketUnmarshaler 将网络包解码为 handler 与业务消息。
type NetPacketUnmarshaler func(buf []byte) (*MessageHandlerData, MessagePayload, error)

// NetMessageHandler 是服务端收到业务消息后的 context-first 处理函数。
type NetMessageHandler func(context.Context, *Session, MessagePayload) error

// Creator 用于创建消息体实例，便于按具体类型解码。
type Creator func() any

// MessageHandlerData 描述一个消息类型对应的处理器与绑定器。
type MessageHandlerData struct {
	MessageType NetMessageType
	Handler     NetMessageHandler
	Creator     Creator
}

// Create 创建消息体实例。
func (h *MessageHandlerData) Create() any {
	if h != nil && h.Creator != nil {
		return h.Creator()
	}
	return nil
}

// Handle 调用消息处理器。
func (h *MessageHandlerData) Handle(ctx context.Context, session *Session, payload MessagePayload) error {
	if h != nil && h.Handler != nil {
		return h.Handler(ctx, session, payload)
	}
	return nil
}

// NetMessageHandlerMap 是服务端消息处理器注册表。
type NetMessageHandlerMap map[NetMessageType]*MessageHandlerData

// ClientMessageHandler 是客户端收到业务消息后的处理函数。
type ClientMessageHandler func(MessagePayload) error

// ClientHandlerData 描述客户端一个消息类型对应的处理器与绑定器。
type ClientHandlerData struct {
	Handler ClientMessageHandler
	Creator Creator
}

// Create 创建客户端消息体实例。
func (h *ClientHandlerData) Create() any {
	if h != nil && h.Creator != nil {
		return h.Creator()
	}
	return nil
}

// ClientMessageHandlerMap 是客户端消息处理器注册表。
type ClientMessageHandlerMap map[NetMessageType]*ClientHandlerData
