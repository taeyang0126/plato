# Phase 3-1: IP 调度服务（ipconf）

## 模块职责

客户端连接 IM 系统时，首先调用 ipconf 的 HTTP API 获取最优 gateway 节点列表。ipconf 监听 etcd 中 gateway 节点的变化，为每个节点维护滑动窗口统计，按负载评分排序。

## 架构分层

```
HTTP API (api.go)          ← 对外接口层
    │
Domain 调度 (domain/)      ← 核心业务逻辑（不依赖 HTTP）
    │
Source 数据源 (source/)    ← 从 etcd 获取 gateway 节点数据
```

**分层原则**：Domain 层不感知 HTTP，Source 层不感知 Domain。每层通过接口/事件解耦。

## 启动流程

`ipconf/server.go:11-18`：

```go
func RunMain(path string) {
    config.Init(path)
    source.Init()      // ① 启动 etcd watch
    domain.Init()      // ② 初始化 Dispatcher
    s := server.Default(server.WithHostPorts(":6789"))  // ③ Hertz HTTP server
    s.GET("/ip/list", GetIpInfoList)
    s.Spin()           // ④ 阻塞，处理 HTTP 请求
}
```

**启动顺序很重要**：source.Init 在 domain.Init 之前，因为 domain 需要消费 source 的 event channel。

## Source 层：etcd → Event

### 数据模型

`ipconf/source/event.go:22-28`：

```go
type Event struct {
    Type         EventType  // AddNodeEvent / DelNodeEvent
    IP           string
    Port         string
    ConnectNum   float64    // 连接数（来自 gateway 上报的 meta）
    MessageBytes float64    // 消息字节数（来自 gateway 上报的 meta）
}
```

### Init：启动 etcd watch + mock 数据

`ipconf/source/data.go:11-21`：

```go
func Init() {
    eventChan = make(chan *Event)     // 创建事件 channel
    ctx := context.Background()
    go DataHandler(&ctx)              // 启动 etcd watch goroutine

    if config.IsDebug() {
        // 开发环境注册 mock 节点（因 etcd 中可能没有真实 gateway）
        ctx := context.Background()
        testServiceRegister(&ctx, "7896", "node1")
        testServiceRegister(&ctx, "7897", "node2")
        testServiceRegister(&ctx, "7898", "node3")
    }
}
```

### DataHandler：etcd → Event 转换

`ipconf/source/data.go:24-51`：

```go
func DataHandler(ctx *context.Context) {
    dis := discovery.NewServiceDiscovery(ctx)
    defer dis.Close()

    setFunc := func(key, value string) {
        if ed, err := discovery.UnMarshal([]byte(value)); err == nil {
            event := NewEvent(ed)         // etcd value → Event
            event.Type = AddNodeEvent
            eventChan <- event            // → 写入 event channel
        }
    }

    delFunc := func(key, value string) {
        // 同理，Type = DelNodeEvent → eventChan
    }

    // Watch etcd prefix: "/plato/ip_dispatcher"
    dis.WatchService(config.GetServicePathForIPConf(), setFunc, delFunc)
}
```

**etcd 中存储的 value 格式**：

```json
{
    "ip": "192.168.1.10",
    "port": "8900",
    "meta": {
        "connect_num": 50000.0,
        "message_bytes": 1024000000.0
    }
}
```

> ⚠️ `connect_num` 和 `message_bytes` 当前来自 mock 数据，真实环境中需要 gateway 定期上报。

---

## Domain 层：节点管理 + 评分排序

### Dispatcher — 节点管理

`ipconf/domain/dispatcher.go:10-13`：

```go
type Dispatcher struct {
    candidateTable map[string]*Endport  // key: "ip:port" → Endport
    sync.RWMutex
}
```

### Init：消费 event channel

`ipconf/domain/dispatcher.go:17-29`：

```go
func Init() {
    dp = &Dispatcher{}
    dp.candidateTable = make(map[string]*Endport)
    go func() {
        for event := range source.EventChan() {  // 阻塞读取 event channel
            switch event.Type {
            case source.AddNodeEvent:
                dp.addNode(event)
            case source.DelNodeEvent:
                dp.delNode(event)
            }
        }
    }()
}
```

### addNode

`ipconf/domain/dispatcher.go:70-86`：

```go
func (dp *Dispatcher) addNode(event *source.Event) {
    dp.Lock()
    defer dp.Unlock()
    var ed *Endport
    var ok bool
    if ed, ok = dp.candidateTable[event.Key()]; !ok {
        // 新节点：创建 Endport
        ed = NewEndport(event.IP, event.Port)
        dp.candidateTable[event.Key()] = ed
    }
    // 更新统计（不管新老节点，都更新最新的 Stat）
    ed.UpdateStat(&Stat{
        ConnectNum:   event.ConnectNum,
        MessageBytes: event.MessageBytes,
    })
}
```

### Dispatch — 评分排序

`ipconf/domain/dispatcher.go:31-54`：

```go
func Dispatch(ctx *IpConfContext) []*Endport {
    // Step 1: 获取所有候选节点
    eds := dp.getCandidateEndport(ctx)
    // Step 2: 每个节点计算分数
    for _, ed := range eds {
        ed.CalculateScore(ctx)
    }
    // Step 3: 排序——活跃分数优先，相同时静态分数决胜
    sort.Slice(eds, func(i, j int) bool {
        if eds[i].ActiveSorce > eds[j].ActiveSorce {
            return true
        }
        if eds[i].ActiveSorce == eds[j].ActiveSorce {
            return eds[i].StaticSorce > eds[j].StaticSorce
        }
        return false
    })
    return eds
}
```

---

## Endpoint + 滑动窗口

### Endport 结构

`ipconf/domain/endport.go:8-15`：

```go
type Endport struct {
    IP          string       `json:"ip"`
    Port        string       `json:"port"`
    ActiveSorce float64      `json:"-"`  // 活跃分数（基于消息字节数）
    StaticSorce float64      `json:"-"`  // 静态分数（基于连接数）
    Stats       *Stat        `json:"-"`  // 当前统计
    window      *stateWindow `json:"-"`  // 滑动窗口
}
```

### NewEndport

`ipconf/domain/endport.go:17-31`：

```go
func NewEndport(ip, port string) *Endport {
    ed := &Endport{IP: ip, Port: port}
    ed.window = newStateWindow()           // 创建滑动窗口
    ed.Stats = ed.window.getStat()         // 初始统计
    go func() {
        for stat := range ed.window.statChan {
            ed.window.appendStat(stat)     // 窗口追加新 stat
            newStat := ed.window.getStat() // 计算窗口平均值
            // 原子替换 Stats 指针（Phase 1-3 学过）
            atomic.SwapPointer(
                (*unsafe.Pointer)(unsafe.Pointer(&ed.Stats)),
                unsafe.Pointer(newStat),
            )
        }
    }()
    return ed
}
```

### 滑动窗口

`ipconf/domain/window.go:7-13`：

```go
const windowSize = 5  // 5 个采样窗口

type stateWindow struct {
    stateQueue []*Stat  // 环形队列（windowSize 个元素）
    statChan   chan *Stat
    sumStat    *Stat    // 当前窗口内 stat 之和
    idx        int64    // 环形队列写位置
}
```

**工作原理**：

```
stateQueue: [s0] [s1] [s2] [s3] [s4]    sumStat = s0+s1+s2+s3+s4
idx=5 → 替换 s0:
stateQueue: [s5] [s1] [s2] [s3] [s4]    sumStat = sumStat - s0 + s5
getStat(): sumStat / 5 → 平均 Stat
```

**为什么用窗口？** 避免瞬时波动影响评分。单次采样可能某节点刚处理完一批消息（瞬时低负载），窗口平滑能反映真实趋势。

### 评分计算

`ipconf/domain/stat.go:15-18,63-65`：

```go
// 活跃分数：基于消息字节数（带宽消耗）
func (s *Stat) CalculateActiveSorce() float64 {
    return getGB(s.MessageBytes)  // 剩余带宽(GB)
}

// 静态分数：基于连接数
func (s *Stat) CalculateStaticSorce() float64 {
    return s.ConnectNum  // 剩余连接容量
}
```

**评分逻辑**：
- **数值含义**：`MessageBytes` 和 `ConnectNum` 是**剩余值**（不是已用值）。所以越大越好（剩余资源越多，越应该接收新连接）
- **活跃分数优先**：带宽是瓶颈资源（网络 IO），比连接数更重要
- **5 个滑动窗口平均**：平滑瞬时波动

---

## API 层

`ipconf/api.go:19-31`：

```go
func GetIpInfoList(c context.Context, ctx *app.RequestContext) {
    defer func() {
        if err := recover(); err != nil {
            ctx.JSON(consts.StatusBadRequest, utils.H{"err": err})
        }
    }()
    ipConfCtx := domain.BuildIpConfContext(&c, ctx)
    eds := domain.Dispatch(ipConfCtx)
    ipConfCtx.AppCtx.JSON(consts.StatusOK, packRes(top5Endports(eds)))
}
```

返回 top5，客户端选择一个连接即可。

---

## 完整数据流

```
gateway 启动 → etcd PUT /plato/ip_dispatcher/xxx
    ↓
source.DataHandler watcher 捕获 PUT 事件
    ↓
setFunc → NewEvent(AddNodeEvent) → eventChan
    ↓
domain.Init goroutine → dp.addNode(event)
    ↓
dp.candidateTable["ip:port"] = NewEndport(ip, port)
    ↓
客户端 GET /ip/list
    ↓
GetIpInfoList → domain.Dispatch(ctx)
    ↓
遍历 candidateTable → CalculateScore → 排序 → top5
    ↓
JSON 响应 → 客户端获得最优 gateway 列表
```

---

## 🔑 为什么 IP 调度层独立

1. **关注点分离**：gateway 只管连接，ipconf 只管调度
2. **独立扩缩容**：ipconf 是 HTTP 无状态服务，可以水平扩展
3. **降低 gateway 耦合**：gateway 不需要感知其他 gateway 的负载

---

## 学习目标

学完后能：画出 etcd→event→Dispatcher→评分→HTTP 响应的完整数据流，解释滑动窗口平均算法，理解活跃/静态分数的分层排序策略。

---

## 试一下

1. 画出 etcd → source → event → dispatcher → API 的完整数据流，标注每个环节的文件:行号
2. **修改**：把 `domain/window.go:windowSize` 从 5 改为 1（即不滑动），修改 `domain/stat.go` 中 `MessageBytes` 的计算方式（给每个 gateway 不同的 mock 值），用 `curl localhost:6789/ip/list` 对比 windowSize=5 和 1 的返回排序差异
3. 在 `domain/dispatcher.go:185-193` 的排序逻辑中，把活跃分数和静态分数的排序顺序颠倒（静态优先），观察排序结果是否合理
4. 用 `etcdctl put /plato/ip_dispatcher/node1 '{"ip":"10.0.0.1","port":"8900","meta":{"connect_num":100,"message_bytes":1000000000}}'` 写入一个节点，观察 ipconf 是否感知并返回
