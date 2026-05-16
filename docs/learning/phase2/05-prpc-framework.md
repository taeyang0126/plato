# Phase 2-5: gRPC 封装框架（prpc）

## prpc 是什么

`common/prpc/` 是对 gRPC 的薄封装，提供：
- **Option 模式配置** — 服务名/IP/端口/权重
- **自动 etcd 注册** — 启动时注册，退出时注销
- **统一拦截器链** — recovery + trace + metric
- **服务发现解析器** — gRPC resolver 集成 etcd
- **负载均衡** — P2C + WRR

## 一、PServer — gRPC Server 封装

### 结构体

`common/prpc/server.go:23-27`：

```go
type PServer struct {
    serverOptions                   // 嵌入配置
    registers    []RegisterFn       // 注册的 gRPC service
    interceptors []grpc.UnaryServerInterceptor  // 自定义拦截器
}
```

**struct 嵌入**：`PServer` 嵌入了 `serverOptions`，所以可以直接用 `p.serviceName`、`p.port` 等字段。Go 的组合代替继承。

### Option 模式配置

`common/prpc/server.go:38-66`：

```go
type ServerOption func(opts *serverOptions)

func WithServiceName(serviceName string) ServerOption {
    return func(opts *serverOptions) { opts.serviceName = serviceName }
}
func WithIP(ip string) ServerOption { ... }
func WithPort(port int) ServerOption { ... }
func WithWeight(weight int) ServerOption { ... }
```

### NewPServer

`common/prpc/server.go:75-95`：

```go
func NewPServer(opts ...ServerOption) *PServer {
    opt := serverOptions{}
    for _, o := range opts {
        o(&opt)  // 逐个应用选项
    }
    if opt.d == nil {
        dis, _ := plugin.GetDiscovInstance()  // 从插件获取 etcd Discovery
        opt.d = dis
    }
    return &PServer{opt, make([]RegisterFn, 0), make([]grpc.UnaryServerInterceptor, 0)}
}
```

### Start — 核心启动流程

`common/prpc/server.go:112-172`：

```go
func (p *PServer) Start(ctx context.Context) {
    // 1. 构建服务描述
    service := discov.Service{
        Name: p.serviceName,
        Endpoints: []*discov.Endpoint{{
            ServerName: p.serviceName,
            IP:         p.ip,
            Port:       p.port,
            Weight:     p.weight,
        }},
    }

    // 2. 加载拦截器链
    interceptors := []grpc.UnaryServerInterceptor{
        serverinterceptor.RecoveryUnaryServerInterceptor(),  // panic recovery
        serverinterceptor.TraceUnaryServerInterceptor(),     // OpenTelemetry trace
        serverinterceptor.MetricUnaryServerInterceptor(p.serviceName),  // Prometheus
    }
    interceptors = append(interceptors, p.interceptors...)

    // 3. 创建 gRPC Server
    s := grpc.NewServer(grpc.ChainUnaryInterceptor(interceptors...))

    // 4. 注册业务 service
    for _, register := range p.registers {
        register(s)
    }

    // 5. 监听端口
    lis, _ := net.Listen("tcp", fmt.Sprintf("%s:%d", p.ip, p.port))
    go func() { s.Serve(lis) }()

    // 6. 注册到 etcd
    p.d.Register(ctx, &service)

    // 7. 等待退出信号
    c := make(chan os.Signal, 1)
    signal.Notify(c, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
    sig := <-c  // 阻塞等待信号
    s.Stop()
    p.d.UnRegister(ctx, &service)  // 优雅退出：先注销再停服
}
```

### 各模块如何使用

**gateway** (`gateway/server.go:33-46`)：

```go
s := prpc.NewPServer(
    prpc.WithServiceName(config.GetGatewayServiceName()),
    prpc.WithIP(config.GetGatewayServiceAddr()),
    prpc.WithPort(config.GetGatewayRPCServerPort()),
    prpc.WithWeight(config.GetGatewayRPCWeight()),
)
s.RegisterService(func(server *grpc.Server) {
    service.RegisterGatewayServer(server, &service.Service{CmdChannel: cmdChannel})
})
s.Start(context.TODO())
```

`RegisterService` 接收一个闭包，内部调用 protobuf 生成的 `RegisterGatewayServer`。这个闭包被保存到 `p.registers` 列表中，在 `Start` 时执行。

---

## 二、PClient — gRPC Client 封装

`common/prpc/client.go`（需要查看完整文件）：

```go
type PClient struct {
    serviceName string
    discov      discov.Discovery
}

func NewPClient(serviceName string) (*PClient, error) { ... }

// 通过 etcd 发现并负载均衡选择节点
func (p *PClient) Dial() (*grpc.ClientConn, error) { ... }

// 直连指定 endpoint
func (p *PClient) DialByEndPoint(endpoint string) (*grpc.ClientConn, error) { ... }
```

### 使用对比

**通过服务发现**（生产环境，当前未使用）：

```go
pCli, _ := prpc.NewPClient("plato.access.state")
conn, _ := pCli.Dial()  // etcd 发现 + P2C 负载均衡
```

**直连**（当前使用）：

`gateway/rpc/client/state.go:15-17`：

```go
pCli, _ := prpc.NewPClient(config.GetStateServiceName())
cli, _ := pCli.DialByEndPoint(config.GetGatewayStateServerEndPoint())
// config: gateway.state_server_endpoint: "127.0.0.1:8902"
```

---

## 三、拦截器链

### gRPC 拦截器模型

```
请求 → Interceptor1 → Interceptor2 → Interceptor3 → Handler → Interceptor3 → Interceptor2 → Interceptor1 → 响应
```

`grpc.ChainUnaryInterceptor` 将多个拦截器串联。

### Recovery 拦截器

防止 handler 中 panic 导致整个 gRPC server 崩溃。类似 HTTP 中间件的 recover。

### Trace 拦截器

`common/prpc/trace/` 集成 OpenTelemetry + Jaeger，自动为每个 gRPC 调用创建 Span。

### Metric 拦截器

`common/prpc/prome/` 集成 Prometheus，自动记录每个 RPC 方法的调用计数、延迟分位数。

---

## 四、负载均衡

`common/prpc/lb/`：

| 策略 | 文件 | 算法 |
|------|------|------|
| **P2C** (Power of Two Choices) | `p2c.go` | 随机选 2 个节点，选负载轻的。效果接近最优，开销远小于全局排序 |
| **WRR** (Weighted Round Robin) | `wrr.go` | 按权重比例分配请求。配合 ipconf 的权重评分 |

---

## 🔑 gRPC 的 Resolver 机制

gRPC 的标准 name resolver 机制：

1. 客户端 `Dial("etcd:///plato.access.state")` 
2. gRPC 根据 scheme(`etcd`) 查找对应的 `resolver.Builder`
3. `resolver.Builder` 调用 etcd API 获取节点列表
4. 通过 `resolver.ClientConn.UpdateState` 通知 gRPC 内部负载均衡器
5. 负载均衡器（P2C/WRR）选择一个节点发起请求

`common/prpc/resolver/discov_builder.go` 实现了 `resolver.Builder` 接口，将 etcd 节点变化同步到 gRPC。

---

## 🔑 Option 模式是 Go 标准模式

```go
// gRPC 官方
grpc.Dial("target", grpc.WithInsecure(), grpc.WithBlock())

// 标准库 net/http client
http.DefaultTransport

// plato 的运用
prpc.NewPServer(prpc.WithServiceName("x"), prpc.WithIP("127.0.0.1"), prpc.WithPort(8900))
```

每个 `WithXxx` 函数返回修改 options 的闭包，`NewPServer` 遍历应用。新增选项不影响已有调用方。

---

## 学习目标

学完后能：用 Option 模式配置 gRPC Server/Client，解释拦截器链的执行顺序，理解 etcd Resolver 如何与 gRPC 集成。

---

## 试一下

1. 读 `common/prpc/server.go` 的 `Start` 方法，画启动 7 步时间线；对比 gateway/state/user 的 RunMain 是否遵循这个模式
2. **修改**：在 `prpc/server.go:127` 的拦截器链中，注释掉 `TraceUnaryServerInterceptor`，启动服务后用 Jaeger UI 看 trace 是否消失
3. 搜索项目中所有 `prpc.NewPServer` 和 `prpc.NewPClient` 的调用，确认每个都传了哪些 Option
4. 思考：如果 `DialByEndPoint` 改为 `Dial`（LoadBalanced），Consistent Hashing 应该怎么实现才能保证同一 connID 的消息路由到同一 gateway？
