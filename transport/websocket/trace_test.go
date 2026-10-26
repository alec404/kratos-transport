package websocket

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/go-kratos/kratos/v2/middleware/tracing"
	kratosTransport "github.com/go-kratos/kratos/v2/transport"
	ws "github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestServerTracingMiddlewareCarriesHandshakeTraceToMessages(t *testing.T) {
	exporter := setupTraceProvider(t)
	parentTraceID, err := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	require.NoError(t, err)
	parentSpanID, err := trace.SpanIDFromHex("00f067aa0ba902b7")
	require.NoError(t, err)
	parentSC := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    parentTraceID,
		SpanID:     parentSpanID,
		TraceFlags: trace.FlagsSampled,
		Remote:     true,
	})

	lis := requireListener(t)
	srv := NewServer(
		WithListener(lis),
		WithPath("/ws"),
		WithCodec("json"),
		WithPayloadType(PayloadTypeBinary),
		WithMiddleware(tracing.Server()),
	)
	require.NotNil(t, srv)

	messageCtx := make(chan context.Context, 1)
	RegisterServerMessageHandler(srv, testMessageTypeChat, func(ctx context.Context, session *Session, msg *ChatMessage) error {
		messageCtx <- ctx
		return nil
	})
	startTestServer(t, srv)

	header := http.Header{}
	propagation.TraceContext{}.Inject(trace.ContextWithRemoteSpanContext(context.Background(), parentSC), propagation.HeaderCarrier(header))
	header.Set("X-Request-Id", "req-ws-trace")

	conn, resp, err := ws.DefaultDialer.Dial("ws://"+lis.Addr().String()+"/ws", header)
	require.NoError(t, err, "%v", resp)
	defer conn.Close()
	require.Eventually(t, func() bool { return srv.SessionCount() == 1 }, time.Second, 5*time.Millisecond)

	buf, err := srv.marshalMessage(testMessageTypeChat, &ChatMessage{Sender: "client", Message: "trace"})
	require.NoError(t, err)
	require.NoError(t, conn.WriteMessage(ws.BinaryMessage, buf))

	var gotCtx context.Context
	select {
	case gotCtx = <-messageCtx:
	case <-time.After(2 * time.Second):
		t.Fatal("server did not receive context-aware message")
	}
	gotMessageSC := trace.SpanContextFromContext(gotCtx)
	require.True(t, gotMessageSC.IsValid())
	require.Equal(t, parentTraceID, gotMessageSC.TraceID())

	var spans tracetest.SpanStubs
	require.Eventually(t, func() bool {
		spans = exporter.GetSpans()
		return len(spans) >= 2
	}, time.Second, 5*time.Millisecond)

	handshakeSpan := requireSpanNamed(t, spans, "/websocket/ws/handshake")
	messageSpan := requireSpanNamed(t, spans, "/websocket/ws/message/1")

	require.Equal(t, parentTraceID, handshakeSpan.SpanContext.TraceID())
	require.Equal(t, parentSpanID, handshakeSpan.Parent.SpanID())
	require.Equal(t, parentTraceID, messageSpan.SpanContext.TraceID())
	require.Equal(t, parentSpanID, messageSpan.Parent.SpanID())
	require.Equal(t, gotMessageSC.SpanID(), messageSpan.SpanContext.SpanID())

	require.Equal(t, "req-ws-trace", spanStringAttr(t, messageSpan, "x-request-id"))
	require.Equal(t, "/ws", spanStringAttr(t, messageSpan, "websocket.path"))
	require.NotEmpty(t, spanStringAttr(t, messageSpan, "websocket.session_id"))
	require.NotEmpty(t, spanStringAttr(t, messageSpan, "websocket.remote_addr"))
	require.Equal(t, int64(testMessageTypeChat), spanInt64Attr(t, messageSpan, "websocket.message_type"))
}

func TestContextAwareRawAndConnectHandlersReceiveServerContext(t *testing.T) {
	lis := requireListener(t)

	connectCtx := make(chan context.Context, 1)
	rawCtx := make(chan context.Context, 1)
	srv := NewServer(
		WithListener(lis),
		WithPath("/ws"),
		WithConnectHandler(func(ctx context.Context, session *Session) error {
			connectCtx <- ctx
			return nil
		}),
		WithRawFrameHandler(func(ctx context.Context, session *Session, frame Frame) error {
			rawCtx <- ctx
			return nil
		}),
	)
	require.NotNil(t, srv)
	startTestServer(t, srv)

	conn, resp, err := ws.DefaultDialer.Dial("ws://"+lis.Addr().String()+"/ws", nil)
	require.NoError(t, err, "%v", resp)
	defer conn.Close()

	var gotConnectCtx context.Context
	select {
	case gotConnectCtx = <-connectCtx:
	case <-time.After(2 * time.Second):
		t.Fatal("connect context handler not called")
	}
	requireServerContext(t, gotConnectCtx)

	require.NoError(t, conn.WriteMessage(ws.TextMessage, []byte("raw")))
	var gotRawCtx context.Context
	select {
	case gotRawCtx = <-rawCtx:
	case <-time.After(2 * time.Second):
		t.Fatal("raw context handler not called")
	}
	requireServerContext(t, gotRawCtx)
}

func setupTraceProvider(t *testing.T) *tracetest.InMemoryExporter {
	t.Helper()

	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		_ = tp.Shutdown(context.Background())
		otel.SetTracerProvider(previous)
	})
	return exporter
}

func requireServerContext(t *testing.T, ctx context.Context) {
	t.Helper()

	tr, ok := kratosTransport.FromServerContext(ctx)
	require.True(t, ok)
	require.Equal(t, KindWebsocket, tr.Kind().String())
	require.Equal(t, "/ws", tr.(*Transport).PathTemplate())
}

func requireSpanNamed(t *testing.T, spans tracetest.SpanStubs, name string) tracetest.SpanStub {
	t.Helper()

	for _, span := range spans {
		if span.Name == name {
			return span
		}
	}
	t.Fatalf("span %q not found in %#v", name, spans)
	return tracetest.SpanStub{}
}

func spanStringAttr(t *testing.T, span tracetest.SpanStub, key string) string {
	t.Helper()

	for _, attr := range span.Attributes {
		if string(attr.Key) == key {
			return attr.Value.AsString()
		}
	}
	t.Fatalf("attribute %q not found in span %q", key, span.Name)
	return ""
}

func spanInt64Attr(t *testing.T, span tracetest.SpanStub, key string) int64 {
	t.Helper()

	for _, attr := range span.Attributes {
		if string(attr.Key) == key {
			require.Equal(t, attribute.INT64, attr.Value.Type())
			return attr.Value.AsInt64()
		}
	}
	t.Fatalf("attribute %q not found in span %q", key, span.Name)
	return 0
}
