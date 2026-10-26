package websocket

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/go-kratos/kratos/v2/middleware"
	kratosTransport "github.com/go-kratos/kratos/v2/transport"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

var websocketTracePropagator = propagation.NewCompositeTextMapPropagator(
	propagation.Baggage{},
	propagation.TraceContext{},
)

func newTraceBaseContext(parent context.Context, header http.Header) context.Context {
	if parent == nil {
		parent = context.Background()
	}
	return websocketTracePropagator.Extract(context.WithoutCancel(parent), propagation.HeaderCarrier(header))
}

func websocketSpanAttributeMiddleware(next middleware.Handler) middleware.Handler {
	return func(ctx context.Context, req any) (reply any, err error) {
		reply, err = next(ctx, req)
		setWebSocketSpanAttributes(ctx)
		return reply, err
	}
}

func setWebSocketSpanAttributes(ctx context.Context) {
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return
	}

	tr, ok := kratosTransport.FromServerContext(ctx)
	if !ok {
		return
	}
	wsTr, ok := tr.(*Transport)
	if !ok {
		return
	}

	attrs := make([]attribute.KeyValue, 0, 6)
	if wsTr.sessionID != "" {
		attrs = append(attrs, attribute.String("websocket.session_id", string(wsTr.sessionID)))
	}
	if wsTr.path != "" {
		attrs = append(attrs, attribute.String("websocket.path", wsTr.path))
	}
	if wsTr.remoteAddr != "" {
		attrs = append(attrs, attribute.String("websocket.remote_addr", wsTr.remoteAddr))
	}
	if wsTr.requestID != "" {
		attrs = append(attrs, attribute.String("x-request-id", wsTr.requestID))
	}
	if wsTr.hasMessageType {
		attrs = append(attrs, attribute.Int64("websocket.message_type", int64(wsTr.messageType)))
	}
	if wsTr.hasFrameType {
		attrs = append(attrs, attribute.Int("websocket.frame_type", wsTr.frameType))
	}
	if len(attrs) > 0 {
		span.SetAttributes(attrs...)
	}
}

func websocketHandshakeOperation(path string) string {
	return "/websocket" + websocketOperationPath(path) + "/handshake"
}

func websocketMessageOperation(path string, messageType NetMessageType) string {
	return fmt.Sprintf("/websocket%s/message/%d", websocketOperationPath(path), messageType)
}

func websocketFrameOperation(path string, frameType int) string {
	return fmt.Sprintf("/websocket%s/frame/%d", websocketOperationPath(path), frameType)
}

func websocketDisconnectOperation(path string) string {
	return "/websocket" + websocketOperationPath(path) + "/disconnect"
}

func websocketOperationPath(path string) string {
	path = strings.TrimSpace(path)
	if path == "" || path == "/" {
		return ""
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return strings.TrimRight(path, "/")
}
