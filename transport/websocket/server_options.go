package websocket

import (
	"crypto/tls"
	"net"
	"net/http"
	"time"

	"github.com/go-kratos/kratos/v2/encoding"
	"github.com/go-kratos/kratos/v2/middleware"
)

// PayloadType 表示 websocket 传输负载的帧格式。
type PayloadType uint8

const (
	// PayloadTypeBinary 使用二进制帧传输消息。
	PayloadTypeBinary PayloadType = 0
	// PayloadTypeText 使用文本帧传输消息。
	PayloadTypeText PayloadType = 1
)

// ServerOption 表示 websocket 服务端配置项。
type ServerOption func(o *Server)

// WithNetwork 设置监听网络类型。
func WithNetwork(network string) ServerOption {
	return func(s *Server) {
		if network != "" {
			s.network = network
		}
	}
}

// WithAddress 设置监听地址。
func WithAddress(addr string) ServerOption {
	return func(s *Server) {
		if addr != "" {
			s.address = addr
		}
	}
}

// WithStrictSlash 设置是否容忍路径尾部斜杠差异。
func WithStrictSlash(strictSlash bool) ServerOption {
	return func(s *Server) {
		s.strictSlash = strictSlash
	}
}

// WithPath 设置 websocket 握手路径。
func WithPath(path string) ServerOption {
	return func(s *Server) {
		if path != "" {
			s.path = path
		}
	}
}

// WithTLSConfig 设置 TLS 配置。
func WithTLSConfig(c *tls.Config) ServerOption {
	return func(o *Server) {
		o.tlsConf = c
	}
}

// WithListener 注入外部 listener。
func WithListener(lis net.Listener) ServerOption {
	return func(s *Server) {
		s.lis = lis
	}
}

// WithCodec 设置消息编解码器。
func WithCodec(c string) ServerOption {
	return func(s *Server) {
		if c != "" {
			s.codec = encoding.GetCodec(c)
		}
	}
}

// WithReadBufferSize 设置 websocket 读缓冲区大小。
func WithReadBufferSize(size int) ServerOption {
	return func(s *Server) {
		s.upgrader.ReadBufferSize = size
	}
}

// WithWriteBufferSize 设置 websocket 写缓冲区大小。
func WithWriteBufferSize(size int) ServerOption {
	return func(s *Server) {
		s.upgrader.WriteBufferSize = size
	}
}

// WithCheckOrigin 设置 Origin 校验逻辑。
func WithCheckOrigin(domain string) ServerOption {
	return func(s *Server) {
		s.upgrader.CheckOrigin = func(r *http.Request) bool {
			origin := r.Header.Get("Origin")
			return origin == domain
		}
	}
}

// WithEnableCompression 设置是否启用压缩。
func WithEnableCompression(enable bool) ServerOption {
	return func(s *Server) {
		s.upgrader.EnableCompression = enable
	}
}

// WithHandshakeTimeout 设置握手超时时间。
func WithHandshakeTimeout(timeout time.Duration) ServerOption {
	return func(s *Server) {
		s.upgrader.HandshakeTimeout = timeout
	}
}

// WithChannelBufferSize 设置单个会话发送队列缓冲区大小。
func WithChannelBufferSize(size int) ServerOption {
	return func(_ *Server) {
		if size > 0 {
			channelBufSize = size
		}
	}
}

// WithPayloadType 设置消息帧类型。
func WithPayloadType(payloadType PayloadType) ServerOption {
	return func(s *Server) {
		s.payloadType = payloadType
	}
}

// WithMiddleware 设置 Kratos middleware 链。
// 握手阶段和每条客户端触发的 websocket 消息都会进入这条链路。
func WithMiddleware(m ...middleware.Middleware) ServerOption {
	return func(s *Server) {
		if len(m) > 0 {
			s.middlewares = append(s.middlewares, m...)
		}
	}
}

// WithBusinessHeartbeat 显式开启业务层二进制心跳。
// 服务端按 interval 发送固定 2 字节 binary ping，并要求客户端在 ackTimeout 内回复 0 字节 binary pong。
func WithBusinessHeartbeat(interval, ackTimeout time.Duration) ServerOption {
	return func(s *Server) {
		if interval <= 0 || ackTimeout <= 0 {
			return
		}
		s.businessHeartbeat.enabled = true
		s.businessHeartbeat.interval = interval
		s.businessHeartbeat.ackTimeout = ackTimeout
		if s.businessHeartbeat.checkInterval <= 0 {
			s.businessHeartbeat.checkInterval = defaultBusinessHeartbeatCheckInterval
		}
	}
}

// WithBusinessHeartbeatCheckInterval 设置业务层心跳的超时扫描周期。
func WithBusinessHeartbeatCheckInterval(interval time.Duration) ServerOption {
	return func(s *Server) {
		if interval > 0 {
			s.businessHeartbeat.checkInterval = interval
		}
	}
}

// WithMessageMarshaler 自定义消息封包逻辑。
func WithMessageMarshaler(m NetPacketMarshaler) ServerOption {
	return func(s *Server) {
		s.netPacketMarshaler = m
	}
}

// WithMessageUnmarshaler 自定义消息拆包逻辑。
func WithMessageUnmarshaler(m NetPacketUnmarshaler) ServerOption {
	return func(s *Server) {
		s.netPacketUnmarshaler = m
	}
}

// WithHandshakeHandler 设置 websocket 握手阶段回调。
// 可用于读取 Cookie / Header 做鉴权，并将附加信息写入 session 元数据。
func WithHandshakeHandler(h HandshakeHandler) ServerOption {
	return func(s *Server) {
		s.handshakeHandler = h
	}
}

// WithConnectHandler 设置连接建立回调。
func WithConnectHandler(h ConnectHandler) ServerOption {
	return func(s *Server) {
		s.connectHandler = h
	}
}

// WithDisconnectHandler 设置连接断开回调。
func WithDisconnectHandler(h DisconnectHandler) ServerOption {
	return func(s *Server) {
		s.disconnectHandler = h
	}
}

// WithRawFrameHandler 设置原始帧处理回调。
// 设置后，服务端不会再执行默认业务消息解包与类型 handler 分发。
func WithRawFrameHandler(h RawFrameHandler) ServerOption {
	return func(s *Server) {
		if h != nil {
			s.rawFrameHandler = h
		}
	}
}
