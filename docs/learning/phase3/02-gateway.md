# Phase 3-2: TCP 接入层（gateway）

## 模块职责

gateway 是 plato 的性能核心。它直接使用 Linux epoll 管理 TCP 长连接，绕过了 Go 标准库的 netpoller，实现单机百万并发连接。

## 为什么绕过 Go 的 netpoller

Go 标准库的 `net.Conn` 底层用 epoll，但每个连接至少 1 个 goroutine（`Read` 阻塞），100 万连接 = 100 万 goroutine。栈内存开销（~2KB/goroutine）+ 调度开销 ≈ 2GB+ 内存。直接使用 epoll 所有连接共享固定数量的 goroutine。

## 启动流程

`gateway/server.go:22-47`：

```go
func RunMain(path string) {
    config.Init(path)
    // 1. 监听 TCP 端口
    ln, _ := net.ListenTCP("tcp", &net.TCPAddr{Port: config.GetGatewayTCPServerPort()})

    // 2. 初始化协程池
    initWorkPoll()

    // 3. 初始化 epoll
    initEpoll(ln, runProc)  // runProc 是每个可读事件的回调

    // 4. 创建命令 channel
    cmdChannel = make(chan *service.CmdContext, config.GetGatewayCmdChannelNum())

    // 5. 启动 gRPC server（接收 state 的 Push/DelConn）
    s := prpc.NewPServer(...)
    s.RegisterService(func(server *grpc.Server) {
        service.RegisterGatewayServer(server, &service.Service{CmdChannel: cmdChannel})
    })

    // 6. 启动 RPC client（连接 state）
    client.Init()

    // 7. 启动命令消费 goroutine
    go cmdHandler()

    // 8. 启动 gRPC server（阻塞）
    s.Start(context.TODO())
}
```

**启动顺序的依赖关系**：

```
config.Init → initWorkPoll → initEpoll → cmdChannel 创建
    → gRPC Server 注册（需要 cmdChannel）
    → RPC client 初始化
    → cmdHandler goroutine（需要 cmdChannel）
    → gRPC Server Start（阻塞）
```

---

## epoll 核心实现

### 全局结构

`gateway/epoll.go:23-29`：

```go
type ePool struct {
    eChan  chan *connection   // 新连接 channel（Accept 到 → epoll 实例）
    tables sync.Map           // connID → *connection（全局连接表）
    eSize  int                // epoll 实例数量（= epoll_num: 4）
    done   chan struct{}      // 退出信号
    ln     *net.TCPListener   // TCP 监听器
    f      func(c *connection, ep *epoller)  // 可读事件的回调（= runProc）
}

type epoller struct {
    fd            int        // epoll fd（epoll_create1 返回）
    fdToConnTable sync.Map   // fd → *connection（fd 快速查找）
}
```

### initEpoll

`gateway/epoll.go:31-36`：

```go
func initEpoll(ln *net.TCPListener, f func(c *connection, ep *epoller)) {
    setLimit()               // ① 调高文件描述符上限
    ep = newEPool(ln, f)     // ② 创建 ePool
    ep.createAcceptProcess() // ③ 启动 Accept 协程池
    ep.startEPool()          // ④ 启动 epoll 事件循环池
}
```

### ① setLimit — 文件描述符上限

`gateway/epoll.go:192-204`：

```go
func setLimit() {
    var rLimit syscall.Rlimit
    syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit)
    rLimit.Cur = rLimit.Max  // 设置到系统最大值
    syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit)
}
```

Linux 默认每个进程 1024 个 fd。百万连接需要调高。等价于 shell 中的 `ulimit -n`。

### ② newEPool

`gateway/epoll.go:38-47`：

```go
func newEPool(ln *net.TCPListener, cb func(c *connection, ep *epoller)) *ePool {
    return &ePool{
        eChan:  make(chan *connection, config.GetGatewayEpollerChanNum()),  // 缓冲 100
        done:   make(chan struct{}),
        eSize:  config.GetGatewayEpollerNum(),  // 4 个 epoll 实例
        tables: sync.Map{},
        ln:     ln,
        f:      cb,
    }
}
```

### ③ createAcceptProcess — 多协程 Accept

`gateway/epoll.go:50-73`：

```go
func (e *ePool) createAcceptProcess() {
    for i := 0; i < runtime.NumCPU(); i++ {
        go func() {
            for {
                conn, e := e.ln.AcceptTCP()
                // 限流：如果超过 tcp_max_num，关闭新连接
                if !checkTcp() {
                    conn.Close()
                    continue
                }
                setTcpConifg(conn)   // 设置 KeepAlive
                c := NewConnection(conn)  // 雪花算法生成 connID
                ep.addTask(c)         // → eChan
            }
        }()
    }
}
```

**为什么 NumCPU 个 Accept 协程？** Linux 内核在多个线程同时 Accept 同一 socket 时有惊群效应。Go 1.14+ 通过 runtime 改善了此问题。NumCPU 个协程基本够用。

### ④ startEPool — epoll 事件循环

`gateway/epoll.go:75-79,82-124`：

```go
func (e *ePool) startEPool() {
    for i := 0; i < e.eSize; i++ {
        go e.startEProc()  // 每个 epoll 实例一个 goroutine
    }
}

func (e *ePool) startEProc() {
    ep, _ := newEpoller()  // 创建 epoll fd（EpollCreate1）

    // 子 goroutine A：监听新连接
    go func() {
        for {
            select {
            case <-e.done:
                return
            case conn := <-e.eChan:  // 从 Accept goroutine 接收连接
                addTcpNum()
                ep.add(conn)          // 注册到 epoll
            }
        }
    }()

    // 主循环：epoll_wait 等待事件
    for {
        select {
        case <-e.done:
            return
        default:
            connections, _ := ep.wait(200)  // 200ms 超时
            for _, conn := range connections {
                e.f(conn, ep)  // 调用 runProc
            }
        }
    }
}
```

**层级关系**：

```
Acceptor goroutines (NumCPU 个)
  │ accept → conn → eChan
  ▼
每个 epoll 实例内部：
  ├── 子 goroutine A: eChan → ep.add(conn) [注册 fd 到 epoll]
  └── 主循环: ep.wait(200ms) → 可读事件 → runProc(conn, ep)
```

### epoll 系统调用封装

**创建** `gateway/epoll.go:136-143`：

```go
func newEpoller() (*epoller, error) {
    fd, err := unix.EpollCreate1(0)  // 等价于 C 的 epoll_create1(0)
    return &epoller{fd: fd}, nil
}
```

**注册** `gateway/epoll.go:147-158`：

```go
func (e *epoller) add(conn *connection) error {
    fd := conn.fd  // 通过反射拿到 TCPConn 的系统 fd
    // EPOLLIN: 可读 | EPOLLHUP: 连接挂起（对端关闭）
    err := unix.EpollCtl(e.fd, syscall.EPOLL_CTL_ADD, fd,
        &unix.EpollEvent{Events: unix.EPOLLIN | unix.EPOLLHUP, Fd: int32(fd)})
    // 维护两个映射表
    e.fdToConnTable.Store(conn.fd, conn)  // fd → connection（快速查找）
    ep.tables.Store(conn.id, conn)        // connID → connection（下行消息）
    conn.BindEpoller(e)
    return nil
}
```

**等待** `gateway/epoll.go:170-183`：

```go
func (e *epoller) wait(msec int) ([]*connection, error) {
    events := make([]unix.EpollEvent, config.GetGatewayEpollWaitQueueSize())  // 100
    n, err := unix.EpollWait(e.fd, events, msec)
    // events 是固定大小数组，epoll_wait 只返回就绪的 fd
    var connections []*connection
    for i := 0; i < n; i++ {
        if conn, ok := e.fdToConnTable.Load(int(events[i].Fd)); ok {
            connections = append(connections, conn.(*connection))
        }
    }
    return connections, nil
}
```

### 获取 socket fd（反射）

`gateway/epoll.go:184-189`：

```go
func socketFD(conn *net.TCPConn) int {
    tcpConn := reflect.Indirect(reflect.ValueOf(*conn)).FieldByName("conn")
    fdVal := tcpConn.FieldByName("fd")
    pfdVal := reflect.Indirect(fdVal).FieldByName("pfd")
    return int(pfdVal.FieldByName("Sysfd").Int())
}
```

**为什么用反射？** Go 标准库的 `net.TCPConn` 没有暴露 fd。需要通过反射访问内部结构体的 `Sysfd` 字段。这是高性能 Go 网络编程的常见技巧。注意：Go 版本升级可能改变内部结构体字段名。

---

## runProc — 可读事件处理

`gateway/server.go:49-70`：

```go
func runProc(c *connection, ep *epoller) {
    ctx := context.Background()
    // Step 1: 读一个完整 TCP 包
    dataBuf, err := tcp.ReadData(c.conn)
    if err != nil {
        if errors.Is(err, io.EOF) {
            ep.remove(c)            // 连接断开，清理 epoll 注册
            client.CancelConn(&ctx, getEndpoint(), c.id, nil)  // 通知 state
        }
        return
    }
    // Step 2: 提交到协程池处理
    err = wPool.Submit(func() {
        client.SendMsg(&ctx, getEndpoint(), c.id, dataBuf)  // → state RPC
    })
}
```

**为什么用协程池而非直接 go？**
- 每个消息都 `go func()` 会导致 goroutine 数量难以控制
- 协程池限制并发数，避免 goroutine 爆炸

---

## cmdHandler — 处理 state 下发的命令

`gateway/server.go:72-84`：

```go
func cmdHandler() {
    for cmd := range cmdChannel {
        switch cmd.Cmd {
        case service.DelConnCmd:
            wPool.Submit(func() { closeConn(cmd) })
        case service.PushCmd:
            wPool.Submit(func() { sendMsgByCmd(cmd) })
        }
    }
}

func closeConn(cmd *service.CmdContext) {
    if connPtr, ok := ep.tables.Load(cmd.ConnID); ok {
        conn, _ := connPtr.(*connection)
        conn.Close()  // 关闭 TCP 连接
    }
}

func sendMsgByCmd(cmd *service.CmdContext) {
    if connPtr, ok := ep.tables.Load(cmd.ConnID); ok {
        conn, _ := connPtr.(*connection)
        dp := tcp.DataPgk{Len: uint32(len(cmd.Payload)), Data: cmd.Payload}
        tcp.SendData(conn.conn, dp.Marshal())  // 写 TCP
    }
}
```

---

## gRPC 服务（state 调用的接口）

`gateway/rpc/service/service.go:23-48`：

```go
// state 调用 DelConn 告诉 gateway 关闭某连接
func (s *Service) DelConn(ctx context.Context, gr *GatewayRequest) (*GatewayResponse, error) {
    s.CmdChannel <- &CmdContext{Cmd: DelConnCmd, ConnID: gr.ConnID}
    return &GatewayResponse{Code: 0, Msg: "success"}, nil
}

// state 调用 Push 下发消息
func (s *Service) Push(ctx context.Context, gr *GatewayRequest) (*GatewayResponse, error) {
    s.CmdChannel <- &CmdContext{Cmd: PushCmd, ConnID: gr.ConnID, Payload: gr.GetData()}
    return &GatewayResponse{Code: 0, Msg: "success"}, nil
}
```

---

## 完整数据流总结

```
客户端 TCP 数据到达
    ↓
epoll_wait 返回可读事件
    ↓
runProc(conn, ep)
    ├── tcp.ReadData(conn) → protobuf bytes
    ├── wPool.Submit → client.SendMsg() → gRPC → state.SendMsg
    └── (或 io.EOF → ep.remove + CancelConn → state)

state → gRPC Push/DelConn → Service.Push/DelConn
    ↓
cmdChannel
    ↓
cmdHandler → wPool.Submit → closeConn / sendMsgByCmd
    ↓
TCP 写回客户端
```

---

## 🔑 epoll 触发模式

当前使用**水平触发(LT, Level Triggered)**——只要 socket buffer 有数据，epoll_wait 就会持续返回该 fd 的事件。优点是不会丢事件，缺点可能重复通知。

**边沿触发(ET, Edge Triggered)**——只在数据到达的瞬间通知一次。性能更好但需要非阻塞 IO + 循环读到 EAGAIN。代码中标注了 TODO：`gateway/epoll.go:146`。

---

## 学习目标

学完后能：画出 gateway 启动到处理一条消息的完整 goroutine/数据流图，解释 epoll 事件循环的层级结构，理解协程池、限流、fd 上限各自解决什么问题。

---

## 🔑 单机百万连接的关键配置

| 配置 | 值 | 作用 |
|------|-----|------|
| `tcp_max_num` | 70000 | 硬限制，超过拒绝 Accept |
| `epoll_num` | 4 | epoll 实例数，分摊到多个 CPU 核心 |
| `worker_pool_num` | 1024 | 协程池大小，限制并发消息处理 |
| ulimit -n | 调至 max | 进程级文件描述符上限 |
| `setLimit()` | — | 代码中调高 ulimit |

---

## 试一下

1. 画出 gateway epoll 的 4 层 goroutine 结构（Acceptor→子goroutine(A)→主循环），标注每层用什么 channel 通信
2. **修改**：把 `epoller.add` 中的 `EPOLLIN | EPOLLHUP` 改为只注册 `EPOLLIN`，客户端断开连接后，epoll_wait 是否还能感知？
3. 在 `runProc` 中，把 `wPool.Submit` 改为 `go func()`，用压测工具建 10000 连接同时发消息，观察 goroutine 数量变化（`runtime.NumGoroutine()`），理解协程池的作用
4. 把 `setLimit()` 注释掉，启动 gateway，用 `ulimit -a` 看当前 fd 上限，然后建 2000 个连接，观察是否能突破默认 1024 限制
