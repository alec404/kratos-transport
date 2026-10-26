# kratos-transport

将 Cron 与 WebSocket 接入 [Kratos](https://go-kratos.dev/docs/) 的
`transport.Server` 生命周期，通过 `kratos.Server(...)` 注册。

## 功能

- [Cron](transport/cron/README.md)：支持 5/6 位表达式、`@every` 等 descriptor、动态任务管理与优雅停止。
- [WebSocket](transport/websocket/README.md)：服务端/客户端、会话管理、连接回调、原始消息与类型消息处理，以及 middleware / tracing。

## 模块结构

Go 版本要求：1.25.0 或更新版本。

| 目录 | Module |
| --- | --- |
| 根目录 | `github.com/alec404/kratos-transport` |
| `transport/cron` | `github.com/alec404/kratos-transport/transport/cron` |
| `transport/websocket` | `github.com/alec404/kratos-transport/transport/websocket` |

根模块只保留 WebSocket 所需的 `broker` 编解码辅助与 `transport` 网络工具；
`broker` 不包含消息队列实现。两个示例各自是独立 Go module。

## 本地开发与示例

当前为首次公开发布前的本地审核版本。仓内依赖使用 `v0.0.0` 占位及相对路径
`replace`，完整检出仓库即可开发，无需获取尚未发布的仓内模块。

从仓库根目录运行：

```bash
GOWORK=off go test ./broker ./transport
(cd transport/cron && GOWORK=off go test .)
(cd transport/websocket && GOWORK=off go test .)
(cd _example/server/cron && GOWORK=off go test .)
(cd _example/server/websocket && GOWORK=off go test .)
```

运行示例：

```bash
cd _example/server/cron # 或 _example/server/websocket
GOWORK=off go run .
```

WebSocket 示例的鉴权逻辑及固定密钥仅用于本地演示，不可直接用于生产。
请求关联使用 `X-Request-Id`，对应 span 属性为 `x-request-id`。

## 发布前检查

- 确认代码公开授权和许可证；当前未附带许可证文件。
- 确定根模块及两个 transport 模块的公开版本，替换仓内 `v0.0.0` 占位依赖。
- 外部消费者不会继承本仓 `replace`；发布前应移除 transport 库的本地替换，
  并验证依赖可从公开地址获取。示例可继续使用相对路径替换。
- 嵌套模块使用独立的 `transport/cron/vX.Y.Z`、`transport/websocket/vX.Y.Z` tag；
  不沿用旧仓版本或批量打 tag。

手动编辑 `script/tag.sh` 或 `script/tag_transport.sh` 中的 `git tag` 命令，
填写版本号并取消所需命令的注释后，从仓库根目录执行对应脚本。
脚本不接受参数、不自动递增版本、不推送远端；初始命令均为注释，不会创建 tag。
