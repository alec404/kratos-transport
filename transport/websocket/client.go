package websocket

import (
	"encoding/json"
	"errors"
	"net/url"
	"sync"
	"time"

	"github.com/go-kratos/kratos/v2/encoding"
	ws "github.com/gorilla/websocket"

	"github.com/alec404/kratos-transport/broker"
)

// Client 是 websocket 客户端封装。
type Client struct {
	conn *ws.Conn

	url      string
	endpoint *url.URL

	codec           encoding.Codec
	messageHandlers ClientMessageHandlerMap

	timeout time.Duration

	payloadType PayloadType

	mu      sync.RWMutex
	writeMu sync.Mutex
}

// NewClient 创建一个 websocket 客户端。
func NewClient(opts ...ClientOption) *Client {
	cli := &Client{
		url:             "",
		timeout:         1 * time.Second,
		codec:           encoding.GetCodec("json"),
		messageHandlers: make(ClientMessageHandlerMap),
		payloadType:     PayloadTypeBinary,
	}

	cli.init(opts...)
	return cli
}

func (c *Client) init(opts ...ClientOption) {
	for _, o := range opts {
		o(c)
	}
	c.endpoint, _ = url.Parse(c.url)
}

// Connect 建立 websocket 连接。
func (c *Client) Connect() error {
	if c.endpoint == nil {
		return errors.New("endpoint is nil")
	}

	LogInfof("connecting to %s", c.endpoint.String())
	conn, resp, err := ws.DefaultDialer.Dial(c.endpoint.String(), nil)
	if err != nil {
		LogErrorf("%s [%v]", err.Error(), resp)
		return err
	}

	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()

	go c.run()
	return nil
}

// Disconnect 断开 websocket 连接。
func (c *Client) Disconnect() {
	c.mu.Lock()
	conn := c.conn
	c.conn = nil
	c.mu.Unlock()

	if conn != nil {
		if err := conn.Close(); err != nil {
			LogErrorf("disconnect error: %s", err.Error())
		}
	}
}

// RegisterMessageHandler 注册一个客户端消息处理器。
func (c *Client) RegisterMessageHandler(messageType NetMessageType, handler ClientMessageHandler, binder Creator) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.messageHandlers[messageType]; ok {
		return
	}
	c.messageHandlers[messageType] = &ClientHandlerData{Handler: handler, Creator: binder}
}

// RegisterClientMessageHandler 使用泛型注册客户端消息处理器。
func RegisterClientMessageHandler[T any](cli *Client, messageType NetMessageType, handler func(*T) error) {
	cli.RegisterMessageHandler(messageType,
		func(payload MessagePayload) error {
			switch t := payload.(type) {
			case *T:
				return handler(t)
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

// DeregisterMessageHandler 注销一个客户端消息处理器。
func (c *Client) DeregisterMessageHandler(messageType NetMessageType) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.messageHandlers, messageType)
}

func (c *Client) getMessageHandler(messageType NetMessageType) *ClientHandlerData {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.messageHandlers[messageType]
}

func (c *Client) marshalMessage(messageType NetMessageType, message MessagePayload) ([]byte, error) {
	var err error
	var buff []byte

	switch c.payloadType {
	case PayloadTypeBinary:
		var msg BinaryNetPacket
		msg.Type = messageType
		msg.Payload, err = broker.Marshal(c.codec, message)
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
		buf, err = broker.Marshal(c.codec, message)
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

// SendMessage 发送一条业务消息。
func (c *Client) SendMessage(messageType NetMessageType, message any) error {
	buff, err := c.marshalMessage(messageType, message)
	if err != nil {
		LogError("marshal message exception:", err)
		return err
	}

	switch c.payloadType {
	case PayloadTypeBinary:
		return c.sendBinaryMessage(buff)
	case PayloadTypeText:
		return c.sendTextMessage(string(buff))
	default:
		return nil
	}
}

func (c *Client) sendPingMessage(message string) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	conn := c.getConn()
	if conn == nil {
		return nil
	}
	return conn.WriteMessage(ws.PingMessage, []byte(message))
}

func (c *Client) sendPongMessage(message string) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	conn := c.getConn()
	if conn == nil {
		return nil
	}
	return conn.WriteMessage(ws.PongMessage, []byte(message))
}

func (c *Client) sendTextMessage(message string) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	conn := c.getConn()
	if conn == nil {
		return errors.New("connection is nil")
	}
	return conn.WriteMessage(ws.TextMessage, []byte(message))
}

func (c *Client) sendBinaryMessage(message []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	conn := c.getConn()
	if conn == nil {
		return errors.New("connection is nil")
	}
	return conn.WriteMessage(ws.BinaryMessage, message)
}

func (c *Client) getConn() *ws.Conn {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.conn
}

func (c *Client) run() {
	defer c.Disconnect()

	for {
		conn := c.getConn()
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

		switch messageType {
		case ws.CloseMessage:
			return
		case ws.BinaryMessage:
			if c.handleBusinessHeartbeat(data) {
				continue
			}
			_ = c.messageHandler(data)
		case ws.TextMessage:
			_ = c.messageHandler(data)
		case ws.PingMessage:
			if err := c.sendPongMessage(""); err != nil {
				LogError("write pong message error: ", err)
				return
			}
		case ws.PongMessage:
		}
	}
}

func (c *Client) handleBusinessHeartbeat(data []byte) bool {
	if !isBusinessHeartbeatPingPayload(data) {
		return false
	}

	// 客户端当前通过“完整 binary frame == [0xB0, 0x04]”识别 heartbeat ping，
	// 命中后立即回一个 0B binary frame，并且不再交给业务 handler。
	if err := c.sendBinaryMessage(nil); err != nil {
		LogError("write business heartbeat pong error: ", err)
	}
	return true
}

func (c *Client) unmarshalMessage(buf []byte) (*ClientHandlerData, MessagePayload, error) {
	var handler *ClientHandlerData
	var payload MessagePayload

	switch c.payloadType {
	case PayloadTypeBinary:
		var msg BinaryNetPacket
		if err := msg.Unmarshal(buf); err != nil {
			LogErrorf("decode message exception: %s", err)
			return nil, nil, err
		}

		handler = c.getMessageHandler(msg.Type)
		if handler == nil {
			LogError("message handler not found:", msg.Type)
			return nil, nil, errors.New("message handler not found")
		}

		if handler.Creator != nil {
			payload = handler.Creator()
			if err := broker.Unmarshal(c.codec, msg.Payload, payload); err != nil {
				LogErrorf("unmarshal message exception: %s", err)
				return nil, nil, err
			}
		} else {
			payload = msg.Payload
		}
	case PayloadTypeText:
		var msg TextNetPacket
		if err := msg.Unmarshal(buf); err != nil {
			LogErrorf("decode message exception: %s", err)
			return nil, nil, err
		}

		handler = c.getMessageHandler(msg.Type)
		if handler == nil {
			LogError("message handler not found:", msg.Type)
			return nil, nil, errors.New("message handler not found")
		}

		if handler.Creator != nil {
			payload = handler.Creator()
			if err := broker.Unmarshal(c.codec, []byte(msg.Payload), payload); err != nil {
				LogErrorf("unmarshal message exception: %s", err)
				return nil, nil, err
			}
		} else {
			payload = msg.Payload
		}
	}

	return handler, payload, nil
}

func (c *Client) messageHandler(buf []byte) error {
	handler, payload, err := c.unmarshalMessage(buf)
	if err != nil {
		LogErrorf("unmarshal message failed: %s", err)
		return err
	}
	if err = handler.Handler(payload); err != nil {
		LogErrorf("message handler exception: %s", err)
		return err
	}
	return nil
}
