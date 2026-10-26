# Websocket

## 说明

将 WebSocket 封装为可接入 Kratos 生命周期的 `transport.Server`。

当前实现支持：

- 基于 Kratos 生命周期启动/停止 WebSocket Server
- 会话管理（SessionManager）
- 原始握手 query 与服务端 metadata 分离存储
- 按消息类型注册服务端/客户端 Handler
- 握手与客户端消息可接入 Kratos middleware / tracing
- 文本帧、二进制帧两种封包模式
- 可显式开启业务层二进制心跳（2B ping / 0B pong）
- 广播、定向发送、原始消息处理

## 主要类型

- `Server`：WebSocket 服务端
- `Client`：WebSocket 客户端
- `Session`：单个连接会话
- `Transport`：注入 Kratos context 的 transport 信息

## 基本用法

```go
srv := websocket.NewServer(
    websocket.WithAddress(":9000"),
    websocket.WithPath("/ws"),
    websocket.WithCodec("json"),
)

websocket.RegisterServerMessageHandler(srv, 1, func(ctx context.Context, session *websocket.Session, req *HelloReq) error {
    return srv.SendMessage(session.SessionID(), 2, &HelloResp{Message: "pong"})
})
```

## Kratos middleware / tracing

服务端可通过 `WithMiddleware` 接入 Kratos middleware。握手请求会注入 Kratos server context，
并从 HTTP header 提取 `traceparent`；后续每条客户端触发的 WS 消息都会基于 Session 保存的
trace parent 创建独立 server span。

```go
srv := websocket.NewServer(
    websocket.WithPath("/ws"),
    websocket.WithMiddleware(
        recovery.Recovery(),
        tracing.Server(),
    ),
)

websocket.RegisterServerMessageHandler(srv, 1, func(ctx context.Context, session *websocket.Session, req *HelloReq) error {
    return repo.Save(ctx, req)
})
```

服务端 handler 均为 context-first：

- `WithHandshakeHandler(func(ctx, req, metadata) error)`
- `WithConnectHandler(func(ctx, session) error)`
- `WithDisconnectHandler(func(ctx, session))`
- `WithRawFrameHandler(func(ctx, session, frame) error)`
- `RegisterServerMessageHandler(func(ctx, session, payload) error)`

默认 span 属性包括 `websocket.session_id`、`websocket.message_type`、`websocket.path`、
`websocket.remote_addr` 与 `x-request-id`。

## 业务层二进制心跳

可通过以下配置显式开启业务层心跳：

```go
srv := websocket.NewServer(
    websocket.WithBusinessHeartbeat(30*time.Second, 15*time.Second),
    websocket.WithBusinessHeartbeatCheckInterval(5*time.Second),
)
```

- `server -> client`：固定发送 `[]byte{0xB0, 0x04}` 的 binary data frame
- `client -> server`：固定回复 `[]byte{}` 的 binary data frame
- 心跳不走 websocket control frame，也不经过业务消息封包/解包逻辑
- DevTools 中会看到类似 `2B` / `0B` 的收发记录

仓库内置的 Go `websocket.Client` 会自动回复这个 `0B` binary pong；浏览器或其它客户端需要自行按该协议回复。


## 运行示例

1. 进入 `_example/server/websocket（从仓库根目录进入）`
2. 执行 `go run .`
3. 浏览器访问 `http://127.0.0.1:8080`
4. 页面会通过 `ws://127.0.0.1:8800/ws` 与示例服务通信
