package websocket

import "github.com/go-kratos/kratos/v2/encoding"

// ClientOption 表示 websocket 客户端配置项。
type ClientOption func(o *Client)

// WithClientCodec 设置客户端编解码器。
func WithClientCodec(c string) ClientOption {
	return func(o *Client) {
		o.codec = encoding.GetCodec(c)
	}
}

// WithEndpoint 设置客户端连接地址，例如 ws://127.0.0.1:8000/ws。
func WithEndpoint(uri string) ClientOption {
	return func(o *Client) {
		o.url = uri
	}
}

// WithClientPayloadType 设置客户端消息帧类型。
func WithClientPayloadType(payloadType PayloadType) ClientOption {
	return func(c *Client) {
		c.payloadType = payloadType
	}
}
