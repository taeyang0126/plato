# Phase 1-4: Go 惯用法

贯穿全项目的 Go 编码模式，每个都结合项目代码展示。

## 学习目标

学完后能：正确使用 defer/error/init/Option/接口写出符合 Go 社区惯例的代码，识别项目中违反惯用法的地方。

## 1. init() 函数——包级自动初始化

Go 的 `init()` 在包被导入时自动执行，且先于 `main()`。常用于注册、初始化全局变量。

`cmd/gateway.go:8-10`：

```go
func init() {
    rootCmd.AddCommand(gatewayCmd)  // 导入 cmd 包时自动注册子命令
}
```

**规则**：
- 一个包可以有多个 `init()`（按声明顺序执行）
- 不能手动调用 `init()`
- 避免在 `init()` 里做耗时操作或依赖外部服务——会拖慢启动

---

## 2. defer——延迟执行，常用于资源清理

`ipconf/api.go:20-22`：

```go
func GetIpInfoList(c context.Context, ctx *app.RequestContext) {
    defer func() {
        if err := recover(); err != nil {
            ctx.JSON(consts.StatusBadRequest, utils.H{"err": err})
        }
    }()
    // ...业务逻辑...
}
```

**defer 规则**：
1. **LIFO 顺序**：后 defer 的先执行（像栈）
2. **参数在 defer 语句时求值**：

```go
func demo() {
    i := 0
    defer fmt.Println(i)  // 此时 i=0，defer 立即求值参数，所以会打印 0
    i++
}
```

3. **recover 必须直接在 defer 函数内**：

```go
// ✅ 正确
defer func() {
    if err := recover(); err != nil {
        log.Println(err)
    }
}()

// ❌ 错误——recover 不在 defer 的直接影响范围内
defer recover()
```

---

## 3. error 处理——值传递，不是异常

Go 没有 try-catch，error 是普通值。这是 Go 新手最需要适应的模式。

### 基本模式

```go
// 项目中最常见的模式
dataBuf, err := tcp.ReadData(c.conn)
if err != nil {
    if errors.Is(err, io.EOF) {  // 用 errors.Is 判断具体错误类型
        ep.remove(c)
    }
    return  // 向上传播错误
}
// err == nil，继续使用 dataBuf
```

### error 包装与解包（Go 1.13+）

```go
// 底层函数：用 %w 包装底层错误，保留错误链
func ReadData(conn *net.TCPConn) ([]byte, error) {
    // ...
    if err != nil {
        return nil, fmt.Errorf("read headlen error: %w", err)  // %w = wrap
    }
}

// 上层函数：用 errors.Is 沿链查找
if errors.Is(err, io.EOF) { ... }   // 判断是否是（或包装了）io.EOF
if errors.As(err, &netErr) { ... }  // 提取链中特定类型的错误
```

**关键**：`%w` 保留错误链，`errors.Is/As` 沿链查找。不用 `==` 比较 error（会断开包装链）。

### 项目中的 fail-fast 哲学

plato 中大量使用 `panic()`，这不是坏习惯——这是故意的：

```go
// state/cache.go:19 — 启动时 Redis 连不上，直接 crash
if _, err := rdb.Ping(ctx).Result(); err != nil {
    panic(err)  // 没法降级，让进程重启
}

// gateway/epoll.go:195 — 调不了 ulimit，直接 crash
if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit); err != nil {
    panic(err)
}
```

**fail-fast 原则**：
- 启动时依赖不可用（Redis/etcd/配置文件）→ panic，由 systemd/k8s 重启
- 运行时不可恢复的错误（如 protobuf 解析失败）→ panic，由 recovery 拦截器兜底
- 可恢复的错误（如网络超时）→ 返回 error，调用方处理

**Java 对比**：
```java
// Java: checked exception + try-catch（编译期强制）
try { doSomething(); }
catch (IOException e) { /* 必须处理 */ }

// Go: 返回值检查（无编译期强制，靠约定和 linter）
data, err := doSomething()
if err != nil { return err }
```

### 项目中违反规则的地方

`common/sdk/net.go:41`：
```go
proto.Unmarshal(data, ackMsg)  // ← 忽略 error！如果解析失败 ackMsg 可能半初始化的
```
`common/sdk/api.go:56`：
```go
data, _ := json.Marshal(msg)  // ← 忽略 error
```

这些是 SDK 代码不够严谨的地方，生产级代码应该处理。

### 🔑 最佳实践

| 规则 | 反例 | 正例 |
|------|------|------|
| 不要忽略 error | `data, _ := fn()` | 至少 log 一下 |
| 用 `%w` 包装 | `fmt.Errorf("err: %v", err)` | `fmt.Errorf("err: %w", err)` |
| error 描述 what+why | `errors.New("failed")` | `fmt.Errorf("read TCP from %s: %w", addr, err)` |
| 调用方处理，底层返回 | 底层 log + return nil | 底层 return err, 上层 log |
| 启动错误 fail-fast | 启动失败默默降级 | `panic(err)` 让进程重启 |
| sentinel error 放包顶层 | 随函数定义 | `var ErrNotFound = errors.New("not found")` |

---

## 4. 类型断言——interface{} → 具体类型

`gateway/server.go:87`：

```go
if connPtr, ok := ep.tables.Load(cmd.ConnID); ok {
    conn, _ := connPtr.(*connection)  // 不安全——如果类型不对会 panic
    conn.Close()
}
```

**安全版本**：

```go
conn, ok := connPtr.(*connection)
if !ok {
    return // 类型不对，安全退出
}
```

**switch 类型断言**：

```go
switch v := x.(type) {
case int:
    fmt.Println("int:", v)
case string:
    fmt.Println("string:", v)
default:
    fmt.Println("unknown")
}
```

---

## 5. Option 模式——函数式选项配置

`common/prpc/server.go:38-66`：

```go
type ServerOption func(opts *serverOptions) // 选项函数类型

func WithServiceName(name string) ServerOption {
    return func(opts *serverOptions) {
        opts.serviceName = name
    }
}

func NewPServer(opts ...ServerOption) *PServer {
    opt := serverOptions{}
    for _, o := range opts {  // 逐个应用选项
        o(&opt)
    }
    // ...
}
```

调用侧 `gateway/server.go:33-37`：

```go
s := prpc.NewPServer(
    prpc.WithServiceName(config.GetGatewayServiceName()),
    prpc.WithIP(config.GetGatewayServiceAddr()),
    prpc.WithPort(config.GetGatewayRPCServerPort()),
    prpc.WithWeight(config.GetGatewayRPCWeight()),
)
```

**优点**：
- 可选参数不需要传零值或 nil
- 新增选项不破坏现有调用方
- 比 config struct 更灵活，比 builder 更简洁

**最佳实践**：Go 标准库 `net.Dialer`、gRPC `grpc.Dial` 都用此模式。

---

## 6. 空结构体 `struct{}` 作为信号

`gateway/epoll.go:25`：

```go
done chan struct{}  // 退出信号，struct{} 不占内存
```

发送信号：`e.done <- struct{}{}` 或 `close(e.done)`
接收信号：`<-e.done`

**为什么用 `struct{}` 而非 `bool`？** — 零内存占用，语义更清晰（"有信号"而非"什么值"）。

---

## 7. 接口——隐式实现

Go 的接口是隐式的：不需要写 `implements`，只要方法签名匹配就行。

`common/timingwheel/timingwheel.go:177-183`：

```go
type Scheduler interface {
    Next(time.Time) time.Time
}
```

任何有 `Next(time.Time) time.Time` 方法的类型都实现了 `Scheduler`，无需声明。

**最佳实践**：
- **接口要小**：1-3 个方法最常见。标准库 `io.Reader` 只有一个 `Read` 方法
- **在使用方定义接口**，而非实现方
- **接受接口，返回具体类型**

---

## 8. `_` 的使用场景

```go
import _ "package"      // 只执行包的 init()，不使用包内符号
_ = c.SetKeepAlive(true) // 显式忽略返回值
var _ SomeInterface = (*MyType)(nil) // 编译期验证 MyType 实现了 SomeInterface
```

---

## 试一下

1. 搜索项目中 `_ =` 和 `defer` 和 `init()` 的使用，每找到一处打勾，确认你理解它的目的
2. **修改**：找一个用 `errors.Is` 的地方，把底层 error 包装函数中的 `%w` 改为 `%v`，用 `errors.Is` 是否还能匹配到？理解 error 包装链的断裂
3. 阅读 `go doc errors.Is` 和 `go doc errors.As` 的官方文档，对比项目中所有 `err ==` 的用法，哪些该改为 `errors.Is`？

---

## 🔑 Go 不是 Java——概念对照

| Java | Go |
|------|-----|
| `Thread` | `goroutine`（更轻量，非 1:1 线程） |
| `BlockingQueue` | `channel` |
| `ConcurrentHashMap` | `sync.Map` 或 `map + sync.RWMutex` |
| `synchronized` / `ReentrantLock` | `sync.Mutex` / `sync.RWMutex` |
| `volatile` | `atomic.Load/Store` |
| `AtomicInteger` | `atomic.AddInt32` |
| `CountDownLatch` | `sync.WaitGroup` |
| `try-catch-finally` | `defer + recover` |
| `@Override` | 隐式接口，无需注解 |
| `Optional` | error 返回值 或 `v, ok := m[key]` |
| Builder 模式 | Option 模式（函数式选项） |
| `extends/implements` | struct 嵌入（组合）+ 隐式接口 |

## 🔑 Go Proverbs（社区金句）

| Proverb | 在这个项目中的体现 |
|---------|-------------------|
| "Don't communicate by sharing memory; share memory by communicating." | channel 解耦 RPC handler 和业务处理 |
| "The bigger the interface, the weaker the abstraction." | `Scheduler` 接口只有 `Next()` 一个方法 |
| "Accept interfaces, return structs." | `NewPServer` 返回 `*PServer` 具体类型 |
| "errors are values." | 每个函数返回 `(result, error)` |
| "Don't panic." | recovery 拦截器兜底 |
| "A little copying is better than a little dependency." | 自定义 TCP 协议而不是引入框架 |
| "Clear is better than clever." | 整个项目的代码命名直白 |

## 🔑 Table-Driven Tests（Go 标准测试模式）

当写测试时，Go 的惯用风格是用表驱动：

```go
func TestCalculateScore(t *testing.T) {
    tests := []struct {
        name     string
        input    *Stat
        expected float64
    }{
        {"zero", &Stat{MessageBytes: 0}, 0},
        {"oneGB", &Stat{MessageBytes: 1 << 30}, 1.0},
        {"edge", &Stat{MessageBytes: 1 << 29}, 0.5},
    }
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            got := tt.input.CalculateActiveSorce()
            if got != tt.expected {
                t.Errorf("got %v, want %v", got, tt.expected)
            }
        })
    }
}
```

**为什么用这个模式？** — 新增测试用例 = 新增一行 struct，而不是复制粘贴函数。Go 的 `t.Run` 支持按名称跑单个用例：`go test -run TestCalculateScore/edge`。
