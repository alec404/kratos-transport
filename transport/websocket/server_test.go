package websocket

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	ws "github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testMessageTypeChat NetMessageType = iota + 1
	testMessageTypeReply
)

type ChatMessage struct {
	Sender  string `json:"sender"`
	Message string `json:"message"`
}

func requireListener(t *testing.T) net.Listener {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		if strings.Contains(err.Error(), "operation not permitted") {
			t.Skipf("sandbox does not permit listen: %v", err)
		}
		require.NoError(t, err)
	}
	return lis
}

func startTestServer(t *testing.T, srv *Server) {
	t.Helper()

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Start(context.Background())
	}()

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = srv.Stop(ctx)

		select {
		case err := <-errCh:
			assert.NoError(t, err)
		case <-time.After(2 * time.Second):
			t.Fatal("server did not stop in time")
		}
	})
}

func TestServerAndClientRoundTrip(t *testing.T) {
	lis := requireListener(t)

	srv := NewServer(
		WithListener(lis),
		WithPath("/ws"),
		WithCodec("json"),
		WithPayloadType(PayloadTypeBinary),
	)
	require.NotNil(t, srv)

	serverRecv := make(chan *ChatMessage, 1)
	RegisterServerMessageHandler(srv, testMessageTypeChat, func(ctx context.Context, session *Session, msg *ChatMessage) error {
		serverRecv <- msg
		return srv.SendMessage(session.SessionID(), testMessageTypeReply, &ChatMessage{Sender: "server", Message: "pong:" + msg.Message})
	})

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Start(context.Background())
	}()
	t.Cleanup(func() {
		select {
		case err := <-errCh:
			assert.NoError(t, err)
		case <-time.After(2 * time.Second):
			t.Fatal("server did not stop in time")
		}
	})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = srv.Stop(ctx)
	})

	clientRecv := make(chan *ChatMessage, 1)
	cli := NewClient(
		WithEndpoint("ws://"+lis.Addr().String()+"/ws"),
		WithClientCodec("json"),
		WithClientPayloadType(PayloadTypeBinary),
	)
	RegisterClientMessageHandler(cli, testMessageTypeReply, func(msg *ChatMessage) error {
		clientRecv <- msg
		return nil
	})
	defer cli.Disconnect()

	require.Eventually(t, func() bool { return cli.Connect() == nil }, time.Second, 50*time.Millisecond)
	require.Eventually(t, func() bool { return srv.SessionCount() == 1 }, time.Second, 50*time.Millisecond)

	require.NoError(t, cli.SendMessage(testMessageTypeChat, &ChatMessage{Sender: "client", Message: "ping"}))

	select {
	case msg := <-serverRecv:
		require.Equal(t, "client", msg.Sender)
		require.Equal(t, "ping", msg.Message)
	case <-time.After(2 * time.Second):
		t.Fatal("server did not receive message")
	}

	select {
	case msg := <-clientRecv:
		require.Equal(t, "server", msg.Sender)
		require.Equal(t, "pong:ping", msg.Message)
	case <-time.After(2 * time.Second):
		t.Fatal("client did not receive reply")
	}
}

func TestHandshakeHandler(t *testing.T) {
	lis := requireListener(t)

	events := make(chan struct {
		connected bool
		user      string
		vip       string
	}, 2)

	srv := NewServer(
		WithListener(lis),
		WithPath("/ws"),
		WithHandshakeHandler(func(ctx context.Context, req *http.Request, metadata SessionMetadata) error {
			cookie, err := req.Cookie("demo_token")
			if err != nil {
				return err
			}
			metadata["user_name"] = cookie.Value
			metadata["vip"] = true
			return nil
		}),
		WithConnectHandler(func(ctx context.Context, session *Session) error {
			events <- struct {
				connected bool
				user      string
				vip       string
			}{
				connected: true,
				user:      metadataString(session.Metadata(), "user_name"),
				vip:       metadataString(session.Metadata(), "vip"),
			}
			return nil
		}),
		WithDisconnectHandler(func(ctx context.Context, session *Session) {
			events <- struct {
				connected bool
				user      string
				vip       string
			}{
				connected: false,
				user:      metadataString(session.Metadata(), "user_name"),
				vip:       metadataString(session.Metadata(), "vip"),
			}
		}),
	)
	require.NotNil(t, srv)

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Start(context.Background())
	}()
	t.Cleanup(func() {
		select {
		case err := <-errCh:
			assert.NoError(t, err)
		case <-time.After(2 * time.Second):
			t.Fatal("server did not stop in time")
		}
	})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = srv.Stop(ctx)
	})

	header := http.Header{}
	header.Add("Cookie", (&http.Cookie{Name: "demo_token", Value: "alice"}).String())

	conn, resp, err := ws.DefaultDialer.Dial("ws://"+lis.Addr().String()+"/ws", header)
	require.NoError(t, err, "%v", resp)

	select {
	case event := <-events:
		require.True(t, event.connected)
		require.Equal(t, "alice", event.user)
		require.Equal(t, "true", event.vip)
	case <-time.After(2 * time.Second):
		t.Fatal("connect event not received")
	}

	session := srv.GetSession(srvSessionIDFromEvents(t, srv))
	require.NotNil(t, session)
	require.Empty(t, session.Queries().Get("user_name"))
	require.Equal(t, "alice", metadataString(session.Metadata(), "user_name"))
	require.Equal(t, "true", metadataString(session.Metadata(), "vip"))

	require.NoError(t, conn.Close())

	select {
	case event := <-events:
		require.False(t, event.connected)
		require.Equal(t, "alice", event.user)
		require.Equal(t, "true", event.vip)
	case <-time.After(2 * time.Second):
		t.Fatal("disconnect event not received")
	}
}

func TestHandshakeHandlerReject(t *testing.T) {
	lis := requireListener(t)

	srv := NewServer(
		WithListener(lis),
		WithPath("/ws"),
		WithHandshakeHandler(func(ctx context.Context, req *http.Request, metadata SessionMetadata) error {
			return errors.New("unauthorized")
		}),
	)
	require.NotNil(t, srv)

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Start(context.Background())
	}()
	t.Cleanup(func() {
		select {
		case err := <-errCh:
			assert.NoError(t, err)
		case <-time.After(2 * time.Second):
			t.Fatal("server did not stop in time")
		}
	})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = srv.Stop(ctx)
	})

	conn, resp, err := ws.DefaultDialer.Dial("ws://"+lis.Addr().String()+"/ws", nil)
	require.Error(t, err)
	require.Nil(t, conn)
	require.NotNil(t, resp)
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

func TestServerBusinessHeartbeatAckKeepsSessionAlive(t *testing.T) {
	lis := requireListener(t)

	srv := NewServer(
		WithListener(lis),
		WithPath("/ws"),
		WithBusinessHeartbeat(40*time.Millisecond, 20*time.Millisecond),
		WithBusinessHeartbeatCheckInterval(5*time.Millisecond),
	)
	require.NotNil(t, srv)
	startTestServer(t, srv)

	conn, resp, err := ws.DefaultDialer.Dial("ws://"+lis.Addr().String()+"/ws", nil)
	require.NoError(t, err, "%v", resp)
	defer conn.Close()

	require.Eventually(t, func() bool { return srv.SessionCount() == 1 }, time.Second, 5*time.Millisecond)

	messageType, data, err := conn.ReadMessage()
	require.NoError(t, err)
	require.Equal(t, ws.BinaryMessage, messageType)
	require.Equal(t, businessHeartbeatPingPayload, data)

	require.NoError(t, conn.WriteMessage(ws.BinaryMessage, nil))
	time.Sleep(25 * time.Millisecond)
	require.Equal(t, 1, srv.SessionCount())
}

func TestServerBusinessHeartbeatTimeoutClosesSession(t *testing.T) {
	lis := requireListener(t)

	srv := NewServer(
		WithListener(lis),
		WithPath("/ws"),
		WithBusinessHeartbeat(60*time.Millisecond, 20*time.Millisecond),
		WithBusinessHeartbeatCheckInterval(5*time.Millisecond),
	)
	require.NotNil(t, srv)
	startTestServer(t, srv)

	conn, resp, err := ws.DefaultDialer.Dial("ws://"+lis.Addr().String()+"/ws", nil)
	require.NoError(t, err, "%v", resp)
	defer conn.Close()

	conn.SetReadDeadline(time.Now().Add(time.Second))
	messageType, data, err := conn.ReadMessage()
	require.NoError(t, err)
	require.Equal(t, ws.BinaryMessage, messageType)
	require.Equal(t, businessHeartbeatPingPayload, data)

	_, _, err = conn.ReadMessage()
	require.Error(t, err)
	require.Eventually(t, func() bool { return srv.SessionCount() == 0 }, time.Second, 5*time.Millisecond)
}

func TestClientAutoBusinessHeartbeatAck(t *testing.T) {
	lis := requireListener(t)

	srv := NewServer(
		WithListener(lis),
		WithPath("/ws"),
		WithCodec("json"),
		WithPayloadType(PayloadTypeBinary),
		WithBusinessHeartbeat(40*time.Millisecond, 20*time.Millisecond),
		WithBusinessHeartbeatCheckInterval(5*time.Millisecond),
	)
	require.NotNil(t, srv)

	clientRecv := make(chan *ChatMessage, 1)
	RegisterServerMessageHandler(srv, testMessageTypeChat, func(ctx context.Context, session *Session, msg *ChatMessage) error {
		return srv.SendMessage(session.SessionID(), testMessageTypeReply, &ChatMessage{Sender: "server", Message: "pong:" + msg.Message})
	})
	startTestServer(t, srv)

	cli := NewClient(
		WithEndpoint("ws://"+lis.Addr().String()+"/ws"),
		WithClientCodec("json"),
		WithClientPayloadType(PayloadTypeBinary),
	)
	RegisterClientMessageHandler(cli, testMessageTypeReply, func(msg *ChatMessage) error {
		clientRecv <- msg
		return nil
	})
	defer cli.Disconnect()

	require.Eventually(t, func() bool { return cli.Connect() == nil }, time.Second, 50*time.Millisecond)
	require.Eventually(t, func() bool { return srv.SessionCount() == 1 }, time.Second, 5*time.Millisecond)

	time.Sleep(90 * time.Millisecond)
	require.Equal(t, 1, srv.SessionCount())

	require.NoError(t, cli.SendMessage(testMessageTypeChat, &ChatMessage{Sender: "client", Message: "ping"}))

	select {
	case msg := <-clientRecv:
		require.Equal(t, "server", msg.Sender)
		require.Equal(t, "pong:ping", msg.Message)
	case <-time.After(2 * time.Second):
		t.Fatal("client did not receive reply")
	}
}

func TestBusinessHeartbeatWithTextPayload(t *testing.T) {
	lis := requireListener(t)

	srv := NewServer(
		WithListener(lis),
		WithPath("/ws"),
		WithCodec("json"),
		WithPayloadType(PayloadTypeText),
		WithBusinessHeartbeat(80*time.Millisecond, 30*time.Millisecond),
		WithBusinessHeartbeatCheckInterval(5*time.Millisecond),
	)
	require.NotNil(t, srv)
	startTestServer(t, srv)

	conn, resp, err := ws.DefaultDialer.Dial("ws://"+lis.Addr().String()+"/ws", nil)
	require.NoError(t, err, "%v", resp)
	defer conn.Close()

	require.Eventually(t, func() bool { return srv.SessionCount() == 1 }, time.Second, 5*time.Millisecond)

	messageType, data, err := conn.ReadMessage()
	require.NoError(t, err)
	require.Equal(t, ws.BinaryMessage, messageType)
	require.Equal(t, businessHeartbeatPingPayload, data)
	require.NoError(t, conn.WriteMessage(ws.BinaryMessage, nil))

	sessionID := srvSessionIDFromEvents(t, srv)
	require.NoError(t, srv.SendMessage(sessionID, testMessageTypeReply, &ChatMessage{Sender: "server", Message: "text"}))

	messageType, data, err = conn.ReadMessage()
	require.NoError(t, err)
	require.Equal(t, ws.TextMessage, messageType)

	var packet TextNetPacket
	require.NoError(t, packet.Unmarshal(data))
	require.Equal(t, testMessageTypeReply, packet.Type)

	var message ChatMessage
	require.NoError(t, json.Unmarshal([]byte(packet.Payload), &message))
	require.Equal(t, "server", message.Sender)
	require.Equal(t, "text", message.Message)
}

func srvSessionIDFromEvents(t *testing.T, srv *Server) SessionID {
	t.Helper()

	var found SessionID
	srv.RangeSessions(func(sessionID SessionID, _ *Session) bool {
		found = sessionID
		return false
	})
	require.NotEmpty(t, found)
	return found
}

func metadataString(metadata SessionMetadata, key string) string {
	if metadata == nil {
		return ""
	}
	value, ok := metadata[key]
	if !ok || value == nil {
		return ""
	}
	switch t := value.(type) {
	case string:
		return t
	case bool:
		if t {
			return "true"
		}
		return "false"
	default:
		return ""
	}
}
