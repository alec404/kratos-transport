# Cron

## 说明

将 cron 定时任务封装为可接入 Kratos 生命周期的 `transport.Server`。

表达式和启动行为：

- 默认支持 **5 位标准表达式**、**6 位秒级表达式** 以及 `@every` / `@daily` 等 descriptor
- 可以在 `app.Run()` 前先注册任务，不需要额外再起 goroutine 等待服务启动

当前实现支持：

- 基于 Kratos 生命周期启动/停止 cron 调度器
- 动态注册、移除单个任务、移除全部任务
- 优雅停止：`Stop` 默认等待运行中的任务执行完成
- 任务 panic recover 日志保护
- 手动指定 endpoint（如需接入服务发现）

## 主要类型

- `Server`：Cron 服务端
- `Transport`：注入 Kratos context 的 transport 描述（预留扩展）

## 基本用法

```go
cronSrv := cron.NewServer()

_, err := cronSrv.StartTimerJob("@every 10s", func() {
    log.Info("sync task running")
})
if err != nil {
    panic(err)
}

app := kratos.New(
    kratos.Name("cron-example"),
    kratos.Server(cronSrv),
)
```

## 表达式示例

```text
*/10 * * * * *   -> 每 10 秒
*/5 * * * *      -> 每 5 分钟
@every 30s       -> 每 30 秒
@daily           -> 每天零点
```

## 关于 endpoint

cron 服务本身不监听网络端口，因此默认 `Endpoint()` 返回空 URL。

如果你的应用需要把 cron 服务实例注册到服务发现，可在创建时手动传入：

```go
endpoint, _ := url.Parse("cron://jobs/settlement")
cronSrv := cron.NewServer(cron.WithEndpoint(endpoint))
```

## 运行示例

1. 进入 `_example/server/cron（从仓库根目录进入）`
2. 执行 `go run .`
3. 观察控制台每 5 秒输出一次定时任务日志
