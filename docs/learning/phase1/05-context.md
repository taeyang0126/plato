# Phase 1-5: context.Context

## Go 概念

`context.Context` 是 Go 中传递**请求作用域**的标准方式：超时控制、取消传播、值传递。每个应该被取消的操作都接受 `ctx context.Context` 作为第一个参数。

## 为什么需要 context

Java 中取消一个操作往往靠 `Thread.interrupt()` 或自定义标志位。Go 中靠 context：

```go
// ❌ Go 中不能这样（没有 Thread.interrupt）
thread.interrupt()

// ✅ Go 中通过 context 传播取消
ctx, cancel := context.WithTimeout(parentCtx, 100*time.Millisecond)
defer cancel()
doSomething(ctx)  // 如果 100ms 内未完成，ctx.Done() 关闭，doSomething 应该停止
```

## 四种创建方式

```go
// 1. 根 context（永不取消，作为起点）
ctx := context.Background()
ctx := context.TODO()  // 语义：TODO，以后会替换

// 2. 带超时
ctx, cancel := context.WithTimeout(parent, 100*time.Millisecond)
defer cancel()  // ⚠️ 必须调 cancel() 释放资源

// 3. 带截止时间
ctx, cancel := context.WithDeadline(parent, time.Now().Add(5*time.Second))

// 4. 带值（少用，仅用于请求范围的元数据）
ctx := context.WithValue(parent, key, value)
```

## 项目中的使用

### 1. 超时控制 — gRPC 调用

`gateway/rpc/client/state.go:28-29`：

```go
func CancelConn(ctx *context.Context, endpoint string, connID uint64, Payload []byte) error {
    rpcCtx, _ := context.WithTimeout(*ctx, 100*time.Millisecond)
    stateClient.CancelConn(rpcCtx, &service.StateRequest{...})
    // 如果 100ms 内 state 没响应，gRPC 自动取消调用
}
```

**为什么 100ms？** 内部 RPC 延迟通常在 1ms 以内，100ms 给足余量。如果超时说明 state 出问题了，不应该让调用方（epoll 事件循环）被阻塞。

### 2. 取消传播 — 启动流程

`state/server.go:16-17`：

```go
func RunMain(path string) {
    ctx := context.TODO()  // TODO: 应该用 Background()
    // ... 整个启动流程传同一个 ctx
}
```

`context.TODO()` vs `context.Background()`：功能完全相同。`TODO()` 表示这里应该有更好的 context 来源，标记需要重构。

### 3. 信号处理 — 优雅退出

`common/prpc/server.go:156-170`：

```go
c := make(chan os.Signal, 1)
signal.Notify(c, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
sig := <-c  // 阻塞等待系统信号
switch sig {
case syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
    s.Stop()
    p.d.UnRegister(ctx, &service)  // 注销 etcd
    return
}
```

虽然没有直接用 `ctx.Done()`，但这本质上就是 context 的取消模式——等待一个取消信号然后优雅退出。

### 4. Redis 操作的 ctx 传递

```go
func GetBytes(ctx context.Context, key string) ([]byte, error) {
    cmd := rdb.Conn().Get(ctx, key)  // ctx 传递给 Redis 命令
    // 如果 ctx 已取消，Redis 命令不会执行
}
```

所有 Redis 操作都接受 ctx。如果 ctx 超时/取消，Redis 命令立即返回 error。

---

## context 使用规则

1. **ctx 永远是第一个参数**：`func Do(ctx context.Context, arg string) error`
2. **不要存 ctx 到 struct**：ctx 是请求级别的，不是对象级别的
3. **ctx 是值传递**：每个函数得到 ctx 的副本，调用 `WithTimeout` 返回新 ctx
4. **总是 defer cancel()**：`ctx, cancel := ...` 之后立即 `defer cancel()`
5. **不要传递 nil ctx**：不确定时用 `context.TODO()`

## 项目中违反规则的地方

`gateway/rpc/service/service.go:24`：

```go
func (s *Service) DelConn(ctx context.Context, gr *GatewayRequest) (*GatewayResponse, error) {
    c := context.TODO()  // ⚠️ 丢弃了调用方传入的 ctx！
    s.CmdChannel <- &CmdContext{Ctx: &c, ...}
    return ...
}
```

**问题**：gRPC 调用方可能设置了超时，但这里用 `TODO()` 替代，导致取消传播断裂。由于采用了 channel 解耦模式，原 ctx 的有效期太短（RPC handler 返回后 ctx 就取消了），这是有意为之但不够优雅。更好的做法是创建 `context.Background()` 的衍生 ctx。

---

## context 取消传播链路

```
客户端中断请求
  ↓
gRPC client ctx.Cancel()
  ↓
gRPC server handler ctx 被取消
  ↓
(但在 plato 中，handler 用 TODO() 替代，传播断裂)
  ↓
理想：Redis/DB 操作收到 ctx.Done()，提前退出
```

**理解这一点很重要**：channel 解耦（handler 只写入 channel）的代价就是丢失了原始的 ctx 取消信息。

---

## 🔑 Java 对比

| Java | Go |
|------|-----|
| `Future.cancel(true)` | `ctx, cancel := context.WithTimeout(parent, d)` + goroutine 检查 `ctx.Done()` |
| `@Timeout` 注解 | `context.WithTimeout` |
| `ThreadLocal` | `context.WithValue`（不鼓励） |
| `InterruptedException` | `ctx.Err() == context.Canceled` |
| try-with-resources | `defer cancel()` |

---

## 🔑 context 坑

1. **忘记 defer cancel() 导致 goroutine 泄漏**：
```go
// ❌ cancel 没调，内部 goroutine 永远不会退出
ctx, _ := context.WithTimeout(parent, time.Second)
doSomething(ctx)
```

2. **把 ctx 存到 struct**：ctx 的生命周期应该等于请求周期，存到 struct 会导致 ctx 过期后还被引用。

3. **WithValue 的 key 应该是不导出的类型**：防止不同包用相同的字符串 key 冲突。

---

## 学习目标

学完后能：正确创建带超时/取消的 context，解释项目中 context.TODO() 的使用场景和问题，理解 context 取消传播断裂的原因。

---

## 试一下

1. `grep -rn "context.TODO\(\)" --include="*.go" .` 列出所有 TODO()，逐一判断应该改成什么
2. **修改**：在 `gateway/rpc/client/state.go:28` 中，把 `100*time.Millisecond` 改为 `1*time.Millisecond`，压测 1000 条消息，观察是否有 RPC 超时报错
3. 在 `state/server.go:16` 的 `context.TODO()` 处，改为 `ctx, cancel := context.WithCancel(context.Background()); defer cancel()`，然后在 `s.Start(ctx)` 后加 `cancel()`，观察 gRPC server 是否立刻退出
