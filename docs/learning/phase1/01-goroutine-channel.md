# Phase 1-1: goroutine + channel

## Go 概念速览

goroutine 是 Go 的轻量级协程，channel 是 goroutine 间的通信管道。Go 哲学：**"不要通过共享内存来通信，而要通过通信来共享内存"**。

## 项目中的最佳范例：channel 解耦 RPC handler 和业务逻辑

### 模式：同步 RPC → channel → 异步处理

`gateway/rpc/service/service.go:23-33`

```go
// DelConn 是 gRPC handler——同步方法，state 等它返回
func (s *Service) DelConn(ctx context.Context, gr *GatewayRequest) (*GatewayResponse, error) {
    c := context.TODO()
    // 关键：不在这里做业务逻辑，直接把请求扔进 channel
    s.CmdChannel <- &CmdContext{
        Ctx:    &c,
        Cmd:    DelConnCmd,
        ConnID: gr.ConnID,
    }
    // 立即返回，不阻塞调用方
    return &GatewayResponse{Code: 0, Msg: "success"}, nil
}
```

消费端在 `gateway/server.go:72-84`：

```go
func cmdHandler() {
    for cmd := range cmdChannel {  // range channel：阻塞等待，channel 关闭时退出
        switch cmd.Cmd {
        case service.DelConnCmd:
            wPool.Submit(func() { closeConn(cmd) })
        case service.PushCmd:
            wPool.Submit(func() { sendMsgByCmd(cmd) })
        }
    }
}
```

**为什么这样设计？**
- gRPC server 会为每个请求分配 goroutine，如果 handler 里直接做 IO 操作（比如写客户端 TCP），会拖慢 RPC 响应
- channel 解耦后，handler 只做投递（纳秒级），RPC 快速返回，实际工作由后台 goroutine 消费
- channel 自带缓冲（`cmd_channel_num: 2048`），能吸收瞬时流量尖刺

### 同样模式在 state

`state/rpc/service/service.go:25-37` — 完全一样的设计。

**最佳实践**：这是 Go 中最常见的"生产者-消费者"模式：
- buffered channel = 有界队列，内存安全（满了会阻塞生产者，天然背压）
- 消费者通常只有一个 goroutine 或固定数量协程池

---

## channel 基础用法（对照项目代码）

### 1. 创建 channel

```go
// 无缓冲 channel——发送者必须等接收者就绪
ch := make(chan int)

// 有缓冲 channel——缓冲满之前不阻塞发送
ch := make(chan int, 100) // 项目中：cmdChannel = make(chan *CmdContext, config.GetCmdChannelNum())
```

### 2. 发送与接收

```go
ch <- 42         // 发送
v := <-ch        // 接收
v, ok := <-ch    // 接收 + 检查 channel 是否关闭
for v := range ch {} // 循环接收直到 channel 关闭
```

### 3. select 多路复用

`gateway/epoll.go:106-123`：

```go
for {
    select {
    case <-e.done:      // 退出信号
        return
    default:            // 没有信号时执行 default，非阻塞
        connections, err := ep.wait(200)
        // ...处理连接
    }
}
```

`select` 是 Go 中处理多个 channel 的唯一方式。`default` 分支让 select 变成非阻塞——如果没有 channel 就绪，立即执行 default。

**最佳实践**：
- 用 `case <-done` 作为退出信号，这是 Go 中 goroutine 优雅退出的标准模式
- `done` 通常是 `chan struct{}`（空结构体，不占内存）

### 4. 单向 channel（只读/只写）

`ipconf/source/event.go:9-11`：

```go
var eventChan chan *Event           // 双向
func EventChan() <-chan *Event {    // 对外只暴露只读 channel
    return eventChan
}
```

**为什么？** 防止调用方往 channel 里写数据，编译期保证安全。

---

## goroutine 基础

### 启动 goroutine

`gateway/epoll.go:51-73`（多 Accept 协程）：

```go
for i := 0; i < runtime.NumCPU(); i++ {
    go func() {  // 闭包捕获外层变量
        for {
            conn, e := e.ln.AcceptTCP()
            // ...
        }
    }()
}
```

**常见陷阱**：循环变量捕获

```go
// ❌ 错误——所有 goroutine 共享同一个 i 变量
for i := 0; i < 10; i++ {
    go func() {
        fmt.Println(i) // 打印的 i 未知，大概率全是 10
    }()
}

// ✅ 正确——传参复制
for i := 0; i < 10; i++ {
    go func(idx int) {
        fmt.Println(idx) // 每个 goroutine 有自己的 idx
    }(i)
}
```

### 协程池

`gateway/workpool.go:12-17`：

```go
func initWorkPoll() {
    var err error
    wPool, err = ants.NewPool(config.GetGatewayWorkerPoolNum())
}
```

**为什么用协程池？** 无限制 `go func()` 在突发流量下会导致 goroutine 数量爆炸、GC 压力增大。ants 协程池预创建固定数量 goroutine，任务提交到池中排队。

---

### sync.WaitGroup — 等待多个 goroutine 完成

Java 用 `CountDownLatch` 或 `ExecutorService.awaitTermination()`。Go 用 `sync.WaitGroup`：

`common/timingwheel/timingwheel.go:135-152`（简化）：

```go
var wg sync.WaitGroup

wg.Add(1)               // 计数器 +1
go func() {
    defer wg.Done()     // goroutine 退出时 -1
    tw.queue.Poll(...)
}()

wg.Add(1)
go func() {
    defer wg.Done()
    for elem := range tw.queue.C { ... }
}()

wg.Wait()               // 阻塞直到计数器归零
```

`timingwheel.go:134-153` 中用的是 `waitGroupWrapper`（对 `sync.WaitGroup` 的简单封装），本质一样。

**常见陷阱**：
```go
// ❌ Add 必须在 go 之前调用，不能放在 goroutine 内部
go func() {
    wg.Add(1)  // 可能 wg.Wait() 先执行，直接返回了
    defer wg.Done()
    doWork()
}()

// ✅ 正确
wg.Add(1)
go func() {
    defer wg.Done()
    doWork()
}()
wg.Wait()
```

---

## 项目中 goroutine 的使用场景汇总

| 场景 | 位置 | 为什么用 goroutine |
|------|------|-------------------|
| Accept 连接 | gateway/epoll.go:51 | 多核并行 Accept，提高建连吞吐 |
| epoll 轮询实例 | gateway/epoll.go:76 | 每个 epoll 实例独立 goroutine 事件循环 |
| 消费 cmdChannel | gateway/server.go:44, state/server.go:30 | 异步处理 RPC 下发的命令 |
| 时间轮驱动 | common/timingwheel/timingwheel.go:135,141 | 轮询 DelayQueue + 消费到期 bucket |
| 滑动窗口更新 | ipconf/domain/endport.go:25 | 每个 Endport 独立 goroutine 消费 statChan |
| 登陆槽恢复 | state/cache.go:45 | state 重启后并发恢复各 slot 的连接状态 |
| 客户端心跳 | common/sdk/api.go:165 | 定时发送心跳包 |

---

## 学习目标

学完后能：用 goroutine + channel 实现生产者-消费者模式，理解 buffered/unbuffered 区别和 select 多路复用，识别项目中所有 channel 解耦点。

---

## 试一下

1. 在 `gateway/server.go:44` 的 `go cmdHandler()` 前面加一行 `fmt.Println("goroutine started")`，重新编译运行，确认 goroutine 何时启动
2. **修改**：把 `cmdChannel` 的缓冲从 2048 改为 0，启动 gateway，用压测工具打 100 个连接看是否卡住（理解 buffered channel 的背压作用）
3. 在 `gateway/epoll.go:106-123` 的 select 中，把 `case <-e.done:` 删掉，然后给 `e.done` 发信号，观察 goroutine 能不能退出（理解 goroutine 泄漏）

1. **buffered channel 的大小设置**：项目中 `cmd_channel_num: 2048`，经验值是预期 QPS * 最大处理延迟。太大浪费内存，太小导致背压。

2. **goroutine 泄漏检测**：如果 goroutine 在等待一个永远不会写入的 channel，它就泄漏了。生产用 `runtime.NumGoroutine()` 监控，或用 `go.uber.org/goleak` 测试。

3. **channel 关闭规则**：永远由发送方关闭 channel。接收方关闭会 panic。`gateway/server.go` 中的 `cmdChannel` 就是由 `gateway.RunMain` 创建、`cmdHandler` 消费，各自职责清晰。

4. **nil channel 的妙用**：对 nil channel 读写会永久阻塞。可以在 select 中用 `case <-nilCh:` 来"禁用"某个分支。

