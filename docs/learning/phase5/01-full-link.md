# Phase 5: 完整链路串联

用一条消息走完 plato 全部模块，串联 Phase 1-4 所有知识点。

## 场景：用户 A 发送 "hello" 给用户 B

> 简化：当前 Push 是自收自发（TODO），但链路完整。

---

## 0. 启动依赖

```
etcd (:2379)             ← 服务发现
Redis (:6379)            ← 路由表 + 登陆槽 + 消息缓存
MySQL (:3306)            ← 用户数据

./plato gateway &        ← TCP :8900, gRPC :8901
./plato state &          ← gRPC :8902
./plato ipconf &         ← HTTP :6789
```

---

## 1. 客户端获取 gateway 地址

```
client ──GET /ip/list──► ipconf (:6789)
```

**代码链路**：
```
ipconf/api.go:19 GetIpInfoList
  → domain/context.go:19 BuildIpConfContext
  → domain/dispatcher.go:31 Dispatch
    → 遍历 dispatcher.candidateTable
    → endport.go:38 CalculateScore
      → stat.go:16 CalculateActiveSorce (基于 MessageBytes)
      → stat.go:63 CalculateStaticSorce (基于 ConnectNum)
    → sort.Slice (ActiveSorce 优先)
  → utils.go:7 top5Endports
  → api.go:31 packRes → JSON 响应
```

**技术点**：
- [Phase 1-3] atomic.SwapPointer 无锁读 Stats
- [Phase 2-2] etcd Watch → event channel → Dispatcher
- [Phase 3-1] 滑动窗口平滑评分

---

## 2. 客户端建立 TCP 连接 + 登陆

```
client ──TCP──► gateway (:8900)
  → 发送 LoginMsg { DeviceID: 123 }
```

**gateway 侧**：
```
gateway/epoll.go
  → createAcceptProcess (NumCPU 个 Accept goroutine)
    → AcceptTCP → NewConnection (雪花算法 connID) → ep.addTask → eChan
  → startEProc
    → 子 goroutine: eChan → ep.add(conn) → EpollCtl(EPOLL_CTL_ADD)
    → 主循环: EpollWait(200ms) → 可读事件 → runProc
```

**runProc** (`gateway/server.go:49-70`)：
```
tcp.ReadData(conn) → protobuf bytes
  → wPool.Submit → client.SendMsg RPC → state (:8902)
```

**技术点**：
- [Phase 2-1] TCP Length-Prefixed 拆包
- [Phase 3-2] epoll 事件循环 + 多 goroutine 分工
- [Phase 1-1] 协程池限制并发

**state 侧**：
```
state/rpc/service/service.go:39 SendMsg → CmdChannel
state/server.go:44 cmdHandler
  → proto.Unmarshal → MsgCmd{Type: Login}
  → msgCmdHandler → loginMsgHandler (server.go:78)
```

**loginMsgHandler**：
```
proto.Unmarshal → LoginMsg{Head.DeviceID: 123}
  → cs.connLogin(ctx, did=123, connID=xxx)  [cache.go:67]
    ├── newConnState(did, connID)            [cache.go:59]
    │     └── reSetHeartTimer()              [state.go:128]  → 5s 心跳定时器
    ├── SADD login_slot_set_{slot} "123|xxx" [Redis 登陆槽]
    ├── AddRecord(did, endpoint, connID)     [router/table.go:27] → Redis 路由表
    └── storeConnIDState(connID, state)      [本地 sync.Map]
  → sendACKMsg(Login, connID, 0, 0, "login ok") [server.go:174]
    → proto.Marshal ACKMsg{ConnID: xxx, Type: Login}
    → sendMsg(connID, CmdType_ACK, data)    [server.go:189]
      → state/rpc/client/gateway.go:33 Push RPC → gateway
```

**gateway 接收 Push**：
```
gateway/rpc/service/service.go:36 Push → CmdChannel
gateway/server.go:72 cmdHandler
  → sendMsgByCmd(cmd) [server.go:91]
    → ep.tables.Load(connID) → *connection
    → tcp.SendData(conn, dp.Marshal())
```

**客户端接收 ACK**：
```
common/sdk/net.go:39 handAckMsg
  → atomic.StoreUint64(&c.connID, ackMsg.ConnID)
```

**技术点**：
- [Phase 3-3] 登陆槽 + 路由表 Redis 设计
- [Phase 2-4] 时间轮心跳定时器
- [Phase 1-2] sync.Map 并发安全查表

---

## 3. 心跳维护

```
client ──Heartbeat──► gateway ──SendMsg RPC──► state
  → hearbeatMsgHandler (state/server.go:97)
    → cs.reSetHeartTimer(connID) [cache.go:116]
      → state.reSetHeartTimer() [state.go:128]
        → 停止旧 heartTimer → 创建新 5s 定时器
```

**两个定时器的配合**：
```
收到心跳 → reSetHeartTimer → 5s 后触发 → reSetReConnTimer → 10s 后触发 → connLogOut
                                                              ↑
                                          ReConn 消息可中断 ───┘
```

---

## 4. 用户 A 发送消息 "hello"

```
client.Send({Content: "hello"})
  → json.Marshal → {"Content":"hello", ...}
  → getClientID(sessionID) → clientID=0
  → proto.Marshal UPMsg{Head: {ClientID:0, ConnID:xxx}, UPMsgBody: json_bytes}
  → conn.send(CmdType_UP, payload)
  → tcp.DataPgk{Marshal} → TCP Write
```

**gateway → state（同上步骤 2 的链路）**

**state upMsgHandler** (`state/server.go:128`)：

```
proto.Unmarshal → UPMsg{Head.ClientID: 0, Head.SessionId: "xxx"}
  → cs.compareAndIncrClientID(ctx, connID, 0, "xxx")  [cache.go:152]
    → key = "max_client_id_{slot}_{connID}_xxx"
    → RunLuaInt("LuaCompareAndIncrClientID", [key], 0, TTL7D)  [redis.go:112]
      → EVALSHA: "if get(key) == 0 then incr(key); return 1 else return -1 end"
      → 返回 1（成功）
  → sendACKMsg(UP, connID, clientID=0, 0, "ok")
  → pushMsg(ctx, connID, sessionID, msgID, upMsg.UPMsgBody)  [server.go:155]
    → proto.Marshal PushMsg{Content: json_bytes, MsgID: msgID}
    → sendMsg(connID, CmdType_Push, data)  → gateway RPC Push
    → cs.appendLastMsg(ctx, connID, pushMsg)  [cache.go:168]
      → Redis SET last_msg_{slot}_{connID} = proto.Marshal(pushMsg)
      → 100ms 重推定时器启动
```

**技术点**：
- [Phase 2-3] Lua CAS 原子去重
- [Phase 3-3] 消息可靠性协议

---

## 5. 客户端收到 Push + 回复 ACK

```
client handPushMsg (net.go:54)
  → proto.Unmarshal → PushMsg{Content: json_bytes}
  → json.Unmarshal → Message{Content: "hello"}
  → conn.send(CmdType_ACK, ACKMsg{ConnID: xxx})
```

**ACK 到达 state** (`state/server.go:144`)：

```
ackMsgHandler
  → proto.Unmarshal → ACKMsg{ConnID: xxx, SessionID: xxx, MsgID: xxx}
  → cs.ackLastMsg(ctx, connID, sessionID, msgID)  [cache.go:188]
    → state.ackLastMsg(ctx, sessionID, msgID)  [state.go:155]
      → 匹配 msgTimerLock (sessionID_msgID)
      → 匹配成功 → DEL last_msg_{slot}_{connID} (Redis)
      → msgTimer.Stop() (停止重推定时器)
```

---

## 6. 异常路径：ACK 丢失 → 重推

```
如果 client 收到 Push 但 ACK 丢失了:
  state: 100ms 定时器触发 → rePush(connID)  [server.go:202]
    → cs.getLastMsg(ctx, connID)  [cache.go:199]
      → Redis GET last_msg_{slot}_{connID} → proto.Unmarshal → PushMsg
    → sendMsg(connID, CmdType_Push, msgData)  → 重新发送
    → state.reSetMsgTimer(connID, sessionID, msgID)  [state.go:102]
      → 重置 100ms 定时器
```

**最多重试到什么时候？** — 10s 重连超时 → connLogOut → state.close() 中 DEL lastMsg。所以最多重试 10s/0.1s = 100 次。

---

## 7. 连接断开

```
client TCP 断开 / 网络故障
  → gateway epoll_wait → EPOLLHUP
    → runProc: tcp.ReadData → io.EOF
      → ep.remove(conn) [EpollCtl EPOLL_CTL_DEL]
      → client.CancelConn(ctx, endpoint, connID, nil) RPC → state

state CancelConn:
  → cs.connLogOut(ctx, connID)  [cache.go:97]
    → state.close()  [state.go:25]
      ├── 停止三个定时器 (heartTimer/reConnTimer/msgTimer)
      ├── SREM login_slot_set_{slot} "did|connID"  [Redis]
      ├── KEYS max_client_id_*_{connID}_* → DEL  [Redis]
      ├── DEL last_msg_{slot}_{connID}  [Redis]
      └── DelRecord(did)  [Redis 路由表]
    → client.DelConn(ctx, connID) RPC → gateway 关闭 TCP
    → cs.deleteConnIDState(connID)  [本地 sync.Map]
```

---

## 完整链路图（标注所有交叉知识点）

```
┌─────────────────────────────────────────────────────────────┐
│ Phase 1: goroutine/channel/sync.Map/atomic                  │
│                                                             │
│  每个 "→ channel → goroutine" 都是一次 Phase 1-1 的实践     │
│  每个 "sync.Map" 都是 Phase 1-2                             │
│  atomic.SwapPointer 是 Phase 1-3                            │
│  Option 模式是 Phase 1-4                                    │
├─────────────────────────────────────────────────────────────┤
│ Phase 2: TCP编解码/config/etcd/Redis/Lua/时间轮/prpc        │
│                                                             │
│  tcp.ReadData/SendData 是 Phase 2-1                         │
│  config.GetXxx() 是 Phase 2-2                               │
│  Redis Lua CAS 是 Phase 2-3                                 │
│  心跳/重连/重推定时器 是 Phase 2-4                          │
│  prpc.NewPServer 启动/注册 是 Phase 2-5                     │
├─────────────────────────────────────────────────────────────┤
│ Phase 3: gateway/state/ipconf                               │
│                                                             │
│  ipconf 评分调度 是 Phase 3-1                               │
│  gateway epoll + runProc 是 Phase 3-2                       │
│  state 消息路由 + 状态机 是 Phase 3-3                       │
├─────────────────────────────────────────────────────────────┤
│ Phase 4: SDK/GORM                                            │
│                                                             │
│  SDK Chat/connect 是 Phase 4-2                              │
│  (user domain 本场景未涉及, 见 Phase 4-1)                   │
└─────────────────────────────────────────────────────────────┘
```

---

## 学习目标

学完整个学习路线后能：不查资料画出 plato 完整架构图 + 消息流转路径，解释每个设计决策的 trade-off，具备修改和扩展代码的能力。

---

## Checkpoint：自测问题

1. 客户端如何知道把消息发给哪个 gateway？ → ipconf HTTP API 返回最优节点
2. gateway 如何知道哪个连接对应哪个客户端？ → connID（雪花算法）全局唯一
3. state 如何知道消息应该推送到哪个 gateway 的哪个连接？ → Redis 路由表 `gateway_rotuer_{did}`
4. 上行消息如何防重复？ → 客户端递增 clientID + state Lua CAS 原子验证
5. 下行消息如何保证不丢？ → 缓存 lastMsg + ACK 确认 + 超时重推
6. state 重启后连接状态怎么恢复？ → 遍历 Redis 登陆槽 reLogin
7. 连接断开/心跳超时如何清理？ → 定时器驱动：heartTimer(5s) → reConnTimer(10s) → connLogOut

---

## 试一下 + 动手练习（难度递进）

### 🟢 入门：观察 + 验证
1. 启动 etcd + Redis + gateway + state，用 `./plato client` 连上，发一条消息
2. 用 `redis-cli` 观察 `login_slot_set_*`、`gateway_rotuer_*`、`last_msg_*` 的 key 变化

### 🟡 进阶：修改 + 预测
3. 把 `state/server.go` 中 `hearbeatMsgHandler` 的 `cs.reSetHeartTimer` 注释掉，重启 state，预测会发生什么？实验验证
4. 把 `reSetMsgTimer` 的 100ms 改成 10s，发一条消息后立即 `kill -9` 客户端，观察 Redis 中 lastMsg 什么时候被删
5. 修改 `state/server.go:139` 的 `pushMsg`，让它不推给发消息的 connID，而是推给另一个连接的 connID（模拟真正的点对点消息）

### 🔴 挑战：设计 + 实现
6. 新增 `CmdType_GroupMsg = 6`：在 protobuf、SDK、state handler 三端各加一小段代码，走通全链路
7. 补齐 `common/cache/redis.go:129-147` 的 `redisCache` 的 MGet/MSet/MDel 实现（当前是 stub），让 `domain/user/` 的二级缓存真正跑通
8. gateway ↔ state 去直连，改为 etcd 服务发现：改 `gateway/rpc/client/state.go` 的 `DialByEndPoint` 为 `Dial`，改 etcd key 和 watch 逻辑
