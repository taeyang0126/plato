# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述

plato — 纯 Go 编写的亿级通信 IM 系统。核心能力：单机百万长连接、万人群聊低延迟、4 个 9 可用性、超大规模消息推送、多场景消息中台（直播/聊天室/弹幕）。

## 构建与运行

```bash
go build -o plato .                    # 构建

# 各服务独立进程，cobra 子命令启动（需要 plato.yaml + etcd + Redis + MySQL）
./plato gateway             # TCP 长连接接入层
./plato state               # 连接状态管理 + 消息路由
./plato ipconf              # IP 调度服务（Hertz HTTP，:6789）
./plato user domain         # 用户领域服务（gRPC，:8903）
./plato client              # CLI IM 客户端（TUI，gocui）
./plato perf --tcp_conn_num=10000  # 性能压测

# 测试
go test ./...                          # 全部测试
go test -v ./common/timingwheel/       # 单包测试
go test -run TestXxx ./path/           # 单个用例
```

## 架构全貌

```
                    etcd（服务注册/发现）
                   ╱    │    ╲
                  ╱     │     ╲
       ┌──── ipconf  gateway  state ────┐
       │   (:6789)   ↓        ↓         │
       │             ↓        ↓         │
 client ──HTTP──► ipconf ──返回最优gateway列表──► client
       │
       └──TCP──► gateway ←──gRPC── state ←──gRPC── domain/user
                  (:8900)   (:8901↔:8902)         (:8903)
                     │                    │
                     ├── Redis ──────────┘
                     └── MySQL（仅 user domain）
```

### 服务分级与启动顺序

| 服务 | 命令 | 协议 | 端口 | 职责 |
|------|------|------|------|------|
| **ipconf** | `./plato ipconf` | HTTP | 6789 | 客户端首次获取 gateway 节点列表，按负载排序返回最优 top5 |
| **gateway** | `./plato gateway` | TCP + gRPC | 8900 / 8901 | 维持客户端 TCP 长连接，收发消息，将客户端数据转发给 state |
| **state** | `./plato state` | gRPC | 8902 | 连接状态机（心跳/重连/登出），消息可靠性保证（ACK/重推），业务路由 |
| **user** | `./plato user domain` | gRPC | 8903 | 用户领域服务，DDD 分层，GORM + MySQL + Redis 多级缓存 |

## 消息生命周期（完整链路）

以一条**上行消息**为例，走通全部模块：

### 1. 客户端获取 gateway 地址

```
client ──GET /ip/list──► ipconf (:6789)
```

`ipconf/server.go:RunMain` 初始化时：
- `source.Init()` 启动 etcd watch，监听 `/plato/ip_dispatcher` 前缀下的 gateway 节点变更
- etcd 中每个 gateway 节点 value 为 `{"ip":"x.x.x.x","port":"8900","meta":{"connect_num":...}}`
- 节点上线/下线事件通过 channel 推送给 `Dispatcher`

客户端请求 `/ip/list` 时：
- `api.go:GetIpInfoList` → `domain.Dispatch(ctx)` → 获取所有 `Endport` 候选 → 对每个 `Endport` 调用 `CalculateScore`
- **活跃分数(ActiveSorce)**：基于 `MessageBytes`（每秒消息字节数），剩余带宽越多分数越高
- **静态分数(StaticSorce)**：基于 `ConnectNum`（长连接数），连接越少分数越高
- 排序规则：活跃分数优先，相同时静态分数决胜
- 返回 top5 optimal gateway 节点给客户端

→ [下一步：客户端拿最优 IP:Port 建立 TCP 连接]

### 2. 客户端建立 TCP 长连接

```
client ──TCP──► gateway (:8900)
```

`gateway/server.go:RunMain` 启动流程：
1. `config.Init(path)` — 加载 yaml 配置
2. `net.ListenTCP` — 监听 TCP 8900
3. `initWorkPoll()` — 初始化 ants 协程池（`worker_pool_num: 1024`）
4. `initEpoll(ln, runProc)` — 初始化 epoll 池

**epoll 初始化细节（`gateway/epoll.go`）**：
- `setLimit()` — 调高进程文件描述符上限
- 创建 `ePool`，包含：
  - `eChan`（channel，缓冲大小=`epoll_channel_num`）— 新连接队列
  - `tables`（`sync.Map`）— connID → connection 映射
  - `eSize`（=`epoll_num: 4`）— epoll 实例数量
- `createAcceptProcess()` — 启动 `NumCPU` 个 Accept 协程，并发 Accept + 限流检查(`checkTcp`)
- `startEPool()` — 启动 `eSize` 个 epoll 轮询协程，每个协程内部：
  - 创建 epoll fd（`unix.EpollCreate1`）
  - goroutine 监听 `eChan`，收到新连接时 `ep.add(conn)` 注册到 epoll（水平触发，监听 `EPOLLIN|EPOLLHUP`）
  - 主循环 `ep.wait(200ms)` 等待可读事件，调用回调 `f(conn, ep)` 即 `runProc`

**连接 ID 生成（`gateway/connection.go`）**：
- 雪花算法：`(毫秒时间戳 - 2020-05-20 基准) << 16 | 序列号 | 版本位 << 63`
- 单毫秒最多 65536 个 ID，sequence 溢出时自旋等下一毫秒

**客户端 SDK 行为（`common/sdk/api.go`）**：
- `Chat` 结构体持有一个 `connect`（TCP 连接）+ 读写 channel
- `login()` → 发送 `CmdType_Login` 消息
- `heartbeat()` → 每秒发送 `CmdType_Heartbeat`
- `loop()` 协程持续读包 → 反序列化 protobuf `MsgCmd` → 按类型分发（ACK/Push）
- 重连：`ReConn()` 关闭旧连接 → 重新 DialTCP → 发送 `CmdType_ReConn`（携带旧 connID）

→ [下一步：gateway 读包后转发给 state]

### 3. gateway → state 消息转发

`gateway/server.go:runProc`（epoll 回调）：
1. `tcp.ReadData(conn)` — 读 TCP 包（4 字节 BigEndian 长度 + 数据体，`common/tcp/read.go`）
2. 若 `io.EOF`（客户端断开）→ `ep.remove(c)` + `client.CancelConn()` 异步通知 state
3. 否则 → `wPool.Submit` 提交到协程池 → `client.SendMsg()` gRPC 调用 state 的 `SendMsg`

**gateway RPC client（`gateway/rpc/client/state.go`）**：
- `initStateClient()` — 通过 `prpc.NewPClient` 创建 gRPC 客户端，`DialByEndPoint` 直连 state（`127.0.0.1:8902`）
- 每次 RPC 超时 100ms

→ [下一步：state 处理消息协议]

### 4. state 接收并处理消息

`state/server.go:cmdHandler()` goroutine 持续从 `cs.server.CmdChannel` 读取 `CmdContext`：

**state RPC service（`state/rpc/service/service.go`）**：
- `SendMsg` 和 `CancelConn` 两个 gRPC 方法
- 方法只做一件事：将请求数据封装为 `CmdContext`，写入 `CmdChannel`（channel），立即返回
- 异步解耦：gRPC handler 不处理业务逻辑，仅投递到 channel

**cmdHandler 消费 channel**：
```
cmdCtx.Cmd 分叉：
├── CancelConnCmd  → cs.connLogOut(connID)
└── SendMsgCmd    → protobuf 反序列化 MsgCmd → msgCmdHandler
```

**msgCmdHandler 消息类型路由**：

| 消息类型 | handler | 功能 |
|----------|---------|------|
| `CmdType_Login` | `loginMsgHandler` | 创建 connState → 写入登陆槽(Redis Set) → 添加路由记录(Redis String) → 启动心跳定时器 → 回复 ACK |
| `CmdType_Heartbeat` | `hearbeatMsgHandler` | 重置心跳定时器(5s) |
| `CmdType_ReConn` | `reConnMsgHandler` | 登出旧 connID → 用旧 did 重新 login 新 connID（保持路由不变） |
| `CmdType_UP` | `upMsgHandler` | 上行消息可靠性检查（Lua CAS clientID）→ 回复 ACK → 调用业务层 → pushMsg 下行 |
| `CmdType_ACK` | `ackMsgHandler` | 客户端对 Push 消息的确认 → 匹配 `msgTimerLock` → 删除 lastMsg → 停止重推定时器 |

### 5. 连接状态管理核心

`state/cache.go` 中的 `cacheState`：

**connLogin 流程**：
1. `newConnState(did, connID)` 创建 `connState` → 启动心跳定时器
2. `cache.SADD` — 写入登陆槽 Redis Set（`login_slot_set_{slot}`，value=`did|connID`）
3. `router.AddRecord` — 写入路由表 Redis String（`gateway_rotuer_{did}`=`endpoint-connID`）
4. `storeConnIDState` — 本地 `sync.Map` 存储 connID → connState

**connLogOut 流程**：
1. 停止心跳/重连/消息三个定时器
2. Redis：SREM 登陆槽、DEL max_client_id 相关 key、DEL lastMsg、DEL 路由记录
3. RPC 通知 gateway 关闭连接：`client.DelConn()`
4. 本地 `sync.Map` 删除

**登陆槽（Login Slot）**：
- 配置 `state.conn_state_slot_range: "0,1024"` 表示 0~1024 共 1025 个分片
- connID 取模分片：`slot = connID % slotSize`
- 作用：state 重启时遍历所有 slot 的 Redis Set，逐一 reLogin 恢复全部连接状态

### 6. 消息可靠性机制

**上行消息（UP）可靠性（防重复）**：
```
客户端发送 UPMsg(ClientID=N) → state 收到
│
├─ Lua 脚本 CAS 原子操作：
│   if redis key 不存在 → set key 0
│   if get(key) == oldClientID(N) → incr(key), expire(key, TTL7D), return 1 ✓
│   else → return -1 ✗（重复消息，丢弃）
│
├─ 通过检查 → 回复 ACK(N) → 调用业务层 → pushMsg 下行
└─ 未通过 → 丢弃（客户端已重试过）
```

**下行消息（Push）可靠性（ACK + 超时重推）**：
```
state pushMsg → gateway Push → 客户端
│
├─ 暂存 lastMsg 到 Redis（`last_msg_key_{slot}_{connID}`）
├─ 启动 msgTimer（100ms 时间轮定时器）
│
├─ 客户端收到 → 回复 ACK(connID, sessionID, msgID)
│   ├─ state 收到 ACK → 匹配 msgTimerLock(fmt.Sprintf("%d_%d", sessionID, msgID))
│   ├─ 匹配成功 → DEL lastMsg → Stop timer ✓
│   └─ 匹配失败 → 忽略（旧消息的 ACK 迟到了）
│
└─ 100ms 超时未收到 ACK → rePush(connID) → 从 Redis 读 lastMsg → 重发 → 重置 timer
    （最多重试到 10s 重连超时，connLogOut 清理 lastMsg）
```

### 7. 时间轮（Timing Wheel）

`common/timingwheel/timingwheel.go` — 层级时间轮实现：

- **参数**：tick=1ms，wheelSize=20（每轮覆盖 20ms）
- **层级溢出**：定时器超时时间超过当前轮间隔 → 递归放入上层 overflowWheel（间隔扩大 wheelSize 倍）
- `AfterFunc(d, f)` → 创建 Timer → 计算 expiration → `addOrRun` → 过期则直接 goroutine 执行，否则加入对应 bucket
- `Start()` → 两个 goroutine：一个轮询 DelayQueue 获取到期 bucket，一个消费到期 bucket 并 Flush 执行其中全部 timer
- state 中用途：心跳超时 5s、重连超时 10s、消息重推 100ms

### 8. domain/user 用户领域服务

`domain/user/server.go:RunMain`：
- `config.Init` → `client.Init`（空实现）→ `service.Init(isTest)` 初始化存储层 → `prpc.NewPServer` 启动 gRPC

**存储层（`domain/user/storage/storage.go`）**：
- `StorageManager` 聚合三个组件：
  - `cache.Manager` — 本地缓存 + Redis 远程缓存二级缓存
  - `gorm.DB` — MySQL 持久化
  - `event.Manager` — 领域事件（UserEvent → Kafka）
- **查询策略**：`QueryUsers` 先查缓存（MGet），miss 的查 DB，结果回写缓存
- **写入策略**：先写 DB，再删除缓存（Cache-Aside 模式），发送领域事件
- 测试模式（`isTest=true`）使用 SQLite 内存数据库 + AutoMigrate

## 目录详解

```
plato/
├── main.go                    # 入口：调用 cmd.Execute()
├── plato.yaml                 # 单一配置文件（viper 解析）
├── cmd/                       # cobra CLI 子命令定义
│   ├── plato.go               # 根命令（cobra.OnInitialize → initConfig）
│   ├── gateway.go             # ./plato gateway → gateway.RunMain
│   ├── state.go               # ./plato state → state.RunMain
│   ├── ipconf.go              # ./plato ipconf → ipconf.RunMain
│   ├── user.go                # ./plato user domain → user.RunMain
│   ├── client.go              # ./plato client → client.RunMain
│   └── perf.go                # ./plato perf → perf.RunMain（支持 --tcp_conn_num 参数）
│
├── gateway/                   # TCP 长连接接入层
│   ├── server.go              # RunMain + cmdHandler（接收 state 命令，Push/DelConn 写客户端）
│   ├── epoll.go               # epoll 池：Accept 协程池 + epoll 实例池 + 限流
│   ├── connection.go          # 雪花算法 ConnID 生成器 + connection 结构体
│   ├── workpool.go            # ants 协程池（处理读到的消息）
│   ├── table.go               # did→conn 映射表（sync.Map）
│   └── rpc/
│       ├── client/
│       │   ├── init.go        # 调用 initStateClient
│       │   └── state.go       # gRPC client → state.SendMsg / state.CancelConn
│       └── service/
│           ├── gateway.pb.go  # protobuf 生成：GatewayRequest/Response, DelConn/Push RPC
│           └── service.go     # gRPC service 实现：接收 Push/DelConn → 写入 CmdChannel
│
├── state/                     # 连接状态管理 + 消息路由
│   ├── server.go              # RunMain + cmdHandler + 消息协议路由（所有 msg*Handler）
│   ├── cache.go               # cacheState：登陆/登出/重连/心跳/消息可靠性
│   ├── state.go               # connState：心跳定时器/重连定时器/消息定时器/ACK 匹配
│   ├── timer.go               # 全局时间轮封装（tick=1ms, wheelSize=20）
│   └── rpc/
│       ├── client/
│       │   ├── init.go        # 调用 initGatewayClient
│       │   └── gateway.go     # gRPC client → gateway.DelConn / gateway.Push
│       └── service/
│           ├── state.pb.go    # protobuf 生成：StateRequest/Response, SendMsg/CancelConn RPC
│           └── service.go     # gRPC service 实现：接收 SendMsg/CancelConn → 写入 CmdChannel
│
├── ipconf/                    # IP 调度服务
│   ├── server.go              # RunMain：Hertz HTTP server + GET /ip/list
│   ├── api.go                 # GetIpInfoList HTTP handler
│   ├── utils.go               # top5Endports / packRes
│   ├── source/
│   │   ├── event.go           # Event 结构体 + AddNodeEvent/DelNodeEvent 常量
│   │   ├── data.go            # etcd Watch 回调 → 将节点变动转为 Event → 写入 eventChan
│   │   └── mock.go            # 开发环境 mock 注册 3 个测试节点
│   └── domain/
│       ├── context.go         # IpConfContext 请求上下文
│       ├── dispatcher.go      # Dispatcher：候选节点管理 + Dispatch 评分排序
│       ├── endport.go         # Endport：IP+端口+活跃/静态分数+滑动窗口统计
│       ├── stat.go            # Stat 统计值 + CalculateActiveSorce/StaticSorce
│       └── window.go          # stateWindow 滑动窗口（windowSize=5，原子更新）
│
├── domain/user/               # 用户领域服务（DDD）
│   ├── server.go              # RunMain → gRPC server
│   ├── server_test.go         # 集成测试
│   ├── storage/
│   │   ├── storage.go         # StorageManager：二级缓存 + GORM + 领域事件
│   │   ├── dao.go             # GORM UserDAO 模型
│   │   ├── options.go         # 查询选项
│   │   └── storage_test.go
│   └── rpc/
│       ├── client/init.go     # gRPC client 初始化（空实现）
│       └── service/service.go # UserService：QueryUsers/CreateUsers/UpdateUsers
│
├── client/                    # CLI IM 客户端（TUI）
│   └── cui.go                 # gocui 终端 UI
│
├── perf/                      # 性能压测
│   └── perf.go                # 创建大量 TCP 连接压测 gateway
│
└── common/                    # 公共基础设施
    ├── config/                # viper 配置读取
    │   ├── config.go          # Init + 通用配置（discovery/cache/ipconf）
    │   ├── gateway.go         # 所有 gateway.* 配置 getter
    │   ├── state.go           # 所有 state.* 配置 getter + slot range 解析
    │   └── user.go            # 所有 user_domain.* 配置 getter
    ├── prpc/                  # gRPC 封装框架
    │   ├── server.go          # PServer：Option 模式配置 → RegisterService → Start（自动注册 etcd）
    │   ├── client.go          # PClient：服务发现解析器 + 负载均衡（P2C/WRR）
    │   ├── discov/            # 服务注册/发现抽象 + etcd 插件
    │   ├── resolver/          # gRPC resolver.Builder 实现（etcd watcher）
    │   ├── lb/                # 负载均衡：p2c（Pick Two Choices）/ wrr（加权轮询）
    │   ├── trace/             # OpenTelemetry tracing 集成
    │   └── prome/             # Prometheus metrics 集成
    ├── discovery/             # etcd 服务发现
    │   ├── discovery.go       # ServiceDiscovery：etcd client + WatchService
    │   ├── register.go        # 服务注册（Register/UnRegister）
    │   └── model.go           # EndpointInfo 序列化/反序列化
    ├── cache/                 # Redis 操作封装
    │   ├── redis.go           # 底层：Get/Set/Del/SADD/SREM/Incr/Lua/Pipeline
    │   ├── lua.go             # Lua 脚本表（LuaCompareAndIncrClientID）
    │   ├── cosnt.go          # Key 模板常量（LoginSlotSetKey/MaxClientIDKey/LastMsgKey/TTL7D）
    │   ├── local.go           # 本地缓存（内存）
    │   └── manager.go         # 缓存管理器（二级缓存路由）
    ├── router/                # 设备路由表
    │   └── table.go           # Redis 操作：AddRecord/DelRecord/QueryRecord（deviceID→endpoint-connID）
    ├── timingwheel/           # 层级时间轮
    │   ├── timingwheel.go     # TimingWheel 核心（add/advanceClock/Start/AfterFunc）
    │   ├── bucket.go          # 时间轮桶（双向链表）
    │   ├── delayqueue.go      # 延迟队列（优先级队列）
    │   └── utils.go           # 时间工具函数
    ├── tcp/                   # 自定义 TCP 协议
    │   ├── coder.go           # DataPgk 编解码（4字节 BigEndian 长度 + 数据）
    │   ├── read.go            # ReadData（先读4字节头得长度，再读数据体，ReadDeadline 120s）
    │   └── write.go           # SendData（循环 write 直到全部写完）
    ├── idl/                   # protobuf 消息定义
    │   ├── message/message.pb.go  # MsgCmd/UPMsg/PushMsg/ACKMsg/LoginMsg 等
    │   └── base/base.pb.go        # 基础消息
    ├── sdk/                   # 客户端 SDK
    │   ├── api.go             # Chat：Login/Heartbeat/ReConn/Send/Recv/Loop
    │   └── net.go             # connect：TCP 连接 + send/recv/handAckMsg/handPushMsg
    ├── logger/                # zap 日志 + OpenTelemetry trace
    ├── bizflow/               # 业务流程引擎（DAG）
    └── bus/event/             # 事件总线（Kafka 集成）
```

## gRPC 服务调用关系

```
gateway ──SendMsg/CancelConn──► state (:8902)
state   ──Push/DelConn───────► gateway (:8901)
state   ──（预留）────────────► domain/user (:8903)
```

- gateway 和 state 之间是**点对点直连**（`DialByEndPoint`），不走服务发现负载均衡
- 原因：state 需要精准给持有某 connID 的 gateway 下发消息
- 路由信息存储在 Redis `gateway_rotuer_{deviceID}` 中
- 每个 gateway 注册到 etcd 时 value 携带自身 endpoint（`ip:port`）

## 核心依赖

| 依赖 | 用途 |
|------|------|
| `google.golang.org/grpc` | 服务间 RPC 通信 |
| `go.etcd.io/etcd/client/v3` | 服务注册与发现 |
| `github.com/go-redis/redis/v9` | 路由表、登陆槽、消息缓存、Lua CAS |
| `gorm.io/gorm` + `gorm.io/driver/mysql` | 用户领域 ORM |
| `github.com/cloudwego/hertz` | ipconf HTTP 框架 |
| `github.com/spf13/cobra` + `viper` | CLI + 配置文件 |
| `github.com/panjf2000/ants` | goroutine 协程池 |
| `github.com/prometheus/client_golang` | Prometheus 指标 |
| `go.opentelemetry.io/otel` | OpenTelemetry 链路追踪，Jaeger exporter |
| `golang.org/x/sys/unix` | epoll 系统调用（`EpollCreate1/EpollCtl/EpollWait`） |
| `github.com/goccy/go-json` | JSON 序列化（性能优于标准库） |
| `github.com/rocket049/gocui` | CLI 客户端 TUI |
| `github.com/gookit/color` | 终端颜色输出 |
| `golang.org/x/net` + `google.golang.org/protobuf` | 网络 + protobuf |
| `go.uber.org/zap` + `gopkg.in/natefinch/lumberjack.v2` | 结构化日志 + 日志轮转 |

## 配置（plato.yaml）

```yaml
global:
  env: debug                        # debug 模式下 ipconf 自动注册 mock 节点
discovery:
  endpoints: [localhost:2379]       # etcd 地址
  timeout: 5                        # etcd 连接超时(秒)
cache:
  redis:
    endpoints: [127.0.0.1:6379]     # Redis 地址（当前只支持单点）
ip_conf:
  service_path: /plato/ip_dispatcher  # etcd 中 gateway 节点注册前缀
gateway:
  service_name: "plato.access.gateway"
  service_addr: "127.0.0.1"
  tcp_max_num: 70000                # 最大 TCP 连接数
  epoll_channel_num: 100            # 新连接 channel 缓冲大小
  epoll_num: 4                      # epoll 实例数（每个实例一个轮询 goroutine）
  epoll_wait_queue_size: 100        # 单次 epoll_wait 最大事件数
  tcp_server_port: 8900             # 客户端 TCP 接入端口
  rpc_server_port: 8901             # gRPC 服务端口（供 state 调用 Push/DelConn）
  worker_pool_num: 1024             # 协程池大小
  cmd_channel_num: 2048             # 命令 channel 缓冲（state→gateway 的 Push/DelConn）
  weight: 100                       # etcd 注册权重
  state_server_endpoint: "127.0.0.1:8902"  # state gRPC 地址
state:
  service_name: "plato.access.state"
  service_addr: "127.0.0.1"
  cmd_channel_num: 2048             # 命令 channel 缓冲（gateway→state 的 SendMsg/CancelConn）
  server_port: 8902                 # gRPC 服务端口
  weight: 100
  conn_state_slot_range: "0,1024"   # 登陆槽范围，用于 connID 分片和重启恢复
  gateway_server_endpoint: "127.0.0.1:8901"  # gateway gRPC 地址
user_domain:
  service_name: "plato.domain.user"
  service_addr: "127.0.0.1"
  service_port: 8903
  weight: 100
  db_dns: "user:password@tcp(127.0.0.1:3306)/database?charset=utf8mb4&parseTime=True&loc=Local"
```

## 关键 Go 并发模式

| 模式 | 位置 | 说明 |
|------|------|------|
| **Channel 解耦** | `gateway/rpc/service/service.go`, `state/rpc/service/service.go` | gRPC handler 不执行业务，只将请求写入 buffered channel，后台 goroutine 消费，实现同步 RPC → 异步处理 |
| **epoll 事件循环** | `gateway/epoll.go` | 每个 epoll 实例一个 goroutine，`ep.wait()` 阻塞等待事件，拿到连接列表后同步调用回调 |
| **协程池** | `gateway/workpool.go` | ants 协程池，限制并发处理的消息数量，避免 goroutine 爆炸 |
| **sync.Map** | `gateway/epoll.go`, `state/cache.go` | Go 并发安全的 map，适合读多写少场景（ep.tables、connToStateTable、fdToConnTable） |
| **atomic + unsafe.Pointer** | `ipconf/domain/endport.go` | `atomic.SwapPointer` 原子更新 Stats 指针，无锁读取统计值 |
| **时间轮** | `common/timingwheel/` | 替代 `time.AfterFunc` 的大量定时器，O(1) 插入删除 |
| **Option 模式** | `common/prpc/server.go` | 函数式选项模式配置 gRPC Server，`prpc.WithServiceName/WithIP/WithPort/...` |

## 已知待完善（代码中 TODO 标记）

1. `gateway/epoll.go:146` — 默认水平触发，可优化为边沿触发 + 非阻塞 FD
2. `state/server.go` — `upMsgHandler` 中业务层调用未实现，`pushMsg` 只做了自收自发
3. `state/cache.go` — 上行消息 `max_client_id` 目前是 conn 维度，尚未调整到会话维度
4. `state/cache.go:89` — 重连时只 reLogin，未更新路由表
5. `domain/user/storage/storage.go` — 批量更新 DB 逐条 `UPDATE` 需要优化，领域事件异步处理待完善
6. `common/cache/redis.go` — `redisCache`（二级缓存中的 remote 部分）MGet/MSet/MDel 为 stub 未实现
7. `common/timingwheel/timingwheel.go` — 文档注释标记了 timer 停止的 gap 问题
