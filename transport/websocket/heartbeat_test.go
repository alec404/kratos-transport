package websocket

import (
	"testing"
	"time"

	ws "github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
)

func newHeartbeatTestSession(srv *Server, sessionID SessionID) *Session {
	return &Session{
		id:    sessionID,
		send:  make(chan outboundMessage, 1),
		hooks: srv,
		done:  make(chan struct{}),
	}
}

func TestBroadcastBusinessHeartbeatPingQueuesBinaryFrame(t *testing.T) {
	srv := NewServer(
		WithBusinessHeartbeat(time.Second, 500*time.Millisecond),
	)
	require.NotNil(t, srv)

	session := newHeartbeatTestSession(srv, "session-1")
	srv.sessionManager.AddSession(session)

	now := time.Now()
	srv.broadcastBusinessHeartbeatPing(now)

	select {
	case msg := <-session.send:
		require.Equal(t, outboundMessageKindBinary, msg.kind)
		require.Equal(t, businessHeartbeatPingPayload, msg.data)
	case <-time.After(time.Second):
		t.Fatal("expected heartbeat ping to be queued")
	}

	require.False(t, session.isHeartbeatAckTimedOut(now.Add(499*time.Millisecond)))
	require.True(t, session.isHeartbeatAckTimedOut(now.Add(500*time.Millisecond)))
}

func TestHandleBusinessHeartbeatFrameConsumesEmptyBinaryAck(t *testing.T) {
	srv := NewServer(
		WithBusinessHeartbeat(time.Second, time.Second),
	)
	require.NotNil(t, srv)

	session := newHeartbeatTestSession(srv, "session-2")
	srv.sessionManager.AddSession(session)
	require.True(t, session.beginHeartbeatWait(time.Now(), time.Second))

	consumed := srv.handleBusinessHeartbeatFrame(session.SessionID(), ws.BinaryMessage, nil)
	require.True(t, consumed)
	require.False(t, session.isHeartbeatAckTimedOut(time.Now().Add(2*time.Second)))
}

func TestDisconnectTimedOutHeartbeatSessionsClosesSession(t *testing.T) {
	srv := NewServer(
		WithBusinessHeartbeat(time.Second, time.Second),
	)
	require.NotNil(t, srv)

	session := newHeartbeatTestSession(srv, "session-3")
	srv.sessionManager.AddSession(session)
	require.True(t, session.beginHeartbeatWait(time.Now().Add(-2*time.Second), time.Second))

	srv.disconnectTimedOutHeartbeatSessions(time.Now())

	require.Equal(t, 0, srv.SessionCount())
	select {
	case <-session.done:
	default:
		t.Fatal("expected session to be closed")
	}
}
