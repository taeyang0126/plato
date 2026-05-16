# Phase 2-2: 配置管理 + etcd 服务发现

## 一、配置管理：viper

### 初始化

`common/config/config.go:9-15`：

```go
func Init(path string) {
    viper.SetConfigFile(path)    // 指定配置文件路径
    viper.SetConfigType("yaml")  // 指定格式
    if err := viper.ReadInConfig(); err != nil {
        panic(err)
    }
}
```

每个服务的 `RunMain` 第一行都调用 `config.Init(path)`。

### 配置 getter 模式

每个模块的配置 getter 集中在一个文件，命名规则 `common/config/<module>.go`：

`common/config/gateway.go:7-9`：

```go
func GetGatewayMaxTcpNum() int32 {
    return viper.GetInt32("gateway.tcp_max_num")
}
```

**为什么每个值一个函数而非直接 `viper.GetInt`？**
1. 编译期类型检查——`GetGatewayMaxTcpNum()` 返回 `int32`，拼错 key 名会编译失败
2. 集中管理——key 的路径字符串只写一次，修改时只改一处
3. 可加缓存/计算逻辑——见 `GetStateServerLoginSlotRange()` 的 slot range 解析

### slot range 解析（带缓存的 getter）

`common/config/state.go:28-47`：

```go
var connStateSlotList []int

func GetStateServerLoginSlotRange() []int {
    if len(connStateSlotList) != 0 {  // 缓存命中
        return connStateSlotList
    }
    slotRnageStr := viper.GetString("state.conn_state_slot_range")
    slotRnage := strings.Split(slotRnageStr, ",")
    left, _ := strconv.Atoi(slotRnage[0])
    right, _ := strconv.Atoi(slotRnage[1])
    res := make([]int, right-left+1)
    for i := left; i <= right; i++ {
        res[i] = i   // ⚠️ 潜藏 bug
    }
    connStateSlotList = res  // 缓存
    return connStateSlotList
}
```

> ⚠️ 潜藏 bug：当前 `"0,1024"` 刚好正确（left=0 时 `res[i]` = `res[i-left]`），但如果配置改成 `"100,200"`：res 长度 101，`res[200]` 直接越界 panic。应改为 `res[i-left] = i`。详见 [Go 切片陷阱](../phase1/06-go-traps.md)。

---

## 二、服务发现：etcd

### 核心模型

`common/discovery/model.go:7-11`：

```go
type EndpointInfo struct {
    IP       string                 `json:"ip"`
    Port     string                 `json:"port"`
    MetaData map[string]interface{} `json:"meta"`
}
```

每个服务实例在 etcd 中存储为 JSON 序列化的 `EndpointInfo`。

### ServiceDiscovery

`common/discovery/discovery.go:14-18`：

```go
type ServiceDiscovery struct {
    cli  *clientv3.Client     // etcd v3 client
    lock sync.Mutex
    ctx  *context.Context
}
```

### WatchService — 监听 prefix 下的所有变更

`common/discovery/discovery.go:37-50`：

```go
func (s *ServiceDiscovery) WatchService(prefix string, set, del func(key, value string)) error {
    // Step 1: 获取现有 key（全量同步）
    resp, err := s.cli.Get(*s.ctx, prefix, clientv3.WithPrefix())
    for _, ev := range resp.Kvs {
        set(string(ev.Key), string(ev.Value))  // 逐条回调
    }
    // Step 2: 从 revision+1 开始 watch（增量更新）
    s.watcher(prefix, resp.Header.Revision+1, set, del)
    return nil
}
```

**revision+1 的作用**：etcd 的 revision 是全局递增的。watch 从 `revision+1` 开始，确保不会丢失 `Get` 和 `Watch` 之间的变更事件。

### watcher — 持续监听

`common/discovery/discovery.go:53-66`：

```go
func (s *ServiceDiscovery) watcher(prefix string, rev int64, set, del func(key, value string)) {
    rch := s.cli.Watch(*s.ctx, prefix, clientv3.WithPrefix(), clientv3.WithRev(rev))
    for wresp := range rch {
        for _, ev := range wresp.Events {
            switch ev.Type {
            case mvccpb.PUT:    // 新增或更新
                set(string(ev.Kv.Key), string(ev.Kv.Value))
            case mvccpb.DELETE: // 删除
                del(string(ev.Kv.Key), string(ev.Kv.Value))
            }
        }
    }
}
```

### ipconf 如何使用 WatchService

`ipconf/source/data.go:24-51`：

```go
func DataHandler(ctx *context.Context) {
    dis := discovery.NewServiceDiscovery(ctx)
    defer dis.Close()

    setFunc := func(key, value string) {
        if ed, err := discovery.UnMarshal([]byte(value)); err == nil {
            event := NewEvent(ed)
            event.Type = AddNodeEvent
            eventChan <- event   // ← 通知 Dispatcher
        }
    }
    delFunc := func(key, value string) {
        // 同理，发 DelNodeEvent
    }

    dis.WatchService(config.GetServicePathForIPConf(), setFunc, delFunc)
}
```

数据流：

```
etcd PUT/DELETE
    → ServiceDiscovery.watcher
    → setFunc / delFunc 回调
    → eventChan (channel)
    → Dispatcher.addNode / delNode (sync.Map)
    → Dispatch() 评分排序时使用
```

### prpc 的服务注册

`common/prpc/server.go:112-156`：

```go
func (p *PServer) Start(ctx context.Context) {
    service := discov.Service{
        Name: p.serviceName,
        Endpoints: []*discov.Endpoint{{
            ServerName: p.serviceName,
            IP:         p.ip,
            Port:       p.port,
            Weight:     p.weight,  // 权重（ipconf 排序时会考虑）
            Enable:     true,
        }},
    }
    // 启动 gRPC server ...
    // 注册到 etcd
    p.d.Register(ctx, &service)

    // 等待退出信号
    c := make(chan os.Signal, 1)
    signal.Notify(c, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
    for {
        sig := <-c
        switch sig {
        case syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
            s.Stop()
            p.d.UnRegister(ctx, &service)  // 退出时注销
            return
        }
    }
}
```

---

## 🔑 服务发现 vs 直连

| 方式 | 项目中的使用 | 为什么 |
|------|-------------|--------|
| etcd 服务发现 | ipconf → gateway 节点发现 | gateway 实例动态增删，ipconf 需要感知 |
| etcd 服务发现 | gateway/state 启动注册 | 服务上线通知 |
| **直连** | gateway RPC client → state (`gateway/rpc/client/state.go:15`) | `DialByEndPoint("127.0.0.1:8902")` 硬编码 |
| **直连** | state RPC client → gateway (`state/rpc/client/gateway.go:16`) | `DialByEndPoint("127.0.0.1:8901")` 硬编码 |

gateway ↔ state 之间目前是单点直连，没有负载均衡。生产环境应改为 etcd 发现。

---

## 🔑 etcd 的 revision 机制

etcd 每个写操作分配一个全局递增的 revision。Watch 从指定 revision 开始，保证**不丢事件**。
这类似 Kafka 的 offset 概念。

---

## 学习目标

学完后能：理解 viper 配置隔离模式，解释 etcd Watch 的 revision+1 机制，画出 etcd→event→Dispatcher 的完整事件流。

---

## 试一下

1. `grep -rn "config.Init" --include="*.go" .` 看每个服务启动第一步，总结统一的启动模式
2. **修改**：把 `common/config/state.go:45` 的 `res[i] = i` 改为 `res[i-left] = i`，然后修改 `plato.yaml` 中 `conn_state_slot_range` 为 `"100,200"`，验证是否有越界 panic
3. 启动 etcd，用 `etcdctl watch /plato --prefix` 监听，然后启动 gateway，观察 etcd 中注册了什么内容
4. 在 `ipconf/source/data.go:47` 的 `dis.WatchService` 调用前，用 `etcdctl put /plato/ip_dispatcher/test '{"ip":"1.2.3.4","port":"9999","meta":{"connect_num":100}}'` 写入数据，然后启动 ipconf，观察 `curl localhost:6789/ip/list` 是否返回这个 mock 节点
