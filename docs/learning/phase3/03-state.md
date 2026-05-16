# Phase 3-3: 连接状态管理 + 消息路由（state）

## 模块职责

state 是 plato 的业务逻辑核心。三大职责：
1. **连接状态管理**：登陆/登出/重连/心跳 + 状态持久化到 Redis
2. **消息协议路由**：识别 Login/Heartbeat/ReConn/UP/ACK 协议类型 → 路由到对应 handler
3. **消息可靠性**：上行消息去重(Lua CAS) + 下行消息确认(ACK + 超时重推)

## 启动流程

`state/server.go:18-41`：

```go
func RunMain(path string) {
    ctx := context.TODO()
    config.Init(path)        // ①
    client.Init()            // ② 初始化 gRPC client（→ gateway）
    InitTimer()              // ③ 初始化时间轮
    InitCacheState(ctx)      // ④ 初始化缓存状态机（Redis + 路由表 + 恢复登陆槽）
    go cmdHandler()          // ⑤ 启动命令消费 goroutine
    s := prpc.NewPServer(...) // ⑥ gRPC server
    s.RegisterService(func(server *grpc.Server) {
        service.RegisterStateServer(server, cs.server)
    })
    s.Start(ctx)             // ⑦ 阻塞
}
```

**InitCacheState 做了什么？**

`state/cache.go:29-36`：

```go
func InitCacheState(ctx context.Context) {
    cs = &cacheState{}
    cache.InitRedis(ctx)          // Redis 初始化
    router.Init(ctx)              // 路由表初始化
    cs.connToStateTable = sync.Map{}
    cs.initLoginSlot(ctx)         // 恢复登陆槽中的全部连接
    cs.server = &service.Service{
        CmdChannel: make(chan *service.CmdContext, config.GetSateCmdChannelNum()),
    }
}
```

### initLoginSlot — 重启恢复

`state/cache.go:39-57`：

```go
func (cs *cacheState) initLoginSlot(ctx context.Context) error {
    loginSlotRange := config.GetStateServerLoginSlotRange()
    for _, slot := range loginSlotRange {
        loginSlotKey := fmt.Sprintf(cache.LoginSlotSetKey, slot)
        go func() {
            loginSlot, _ := cache.SmembersStrSlice(ctx, loginSlotKey)
            for _, mate := range loginSlot {
                did, connID := cs.loginSlotUnmarshal(mate)  // "did|connID" → did, connID
                cs.connReLogin(ctx, did, connID)             // 恢复连接状态
            }
        }()
    }
    return nil
}
```

**恢复流程**：
1. 遍历所有 slot（0~1024）
2. 从 Redis Set `login_slot_set_{slot}` 读取所有 `did|connID`
3. 逐条 `connReLogin`：创建 connState → 存入 connToStateTable → 加载 lastMsg → 启动消息重推定时器
4. 每个 slot 一个 goroutine 并发恢复

---

## 消息处理三层路由

### 第一层：cmdHandler（gateway RPC → 协议路由）

`state/server.go:44-58`：

```go
func cmdHandler() {
    for cmdCtx := range cs.server.CmdChannel {
        switch cmdCtx.Cmd {
        case service.CancelConnCmd:
            cs.connLogOut(*cmdCtx.Ctx, cmdCtx.ConnID)
        case service.SendMsgCmd:
            msgCmd := &message.MsgCmd{}
            proto.Unmarshal(cmdCtx.Payload, msgCmd)
            msgCmdHandler(cmdCtx, msgCmd)  // → 第二层
        }
    }
}
```

### 第二层：msgCmdHandler（protobuf 类型 → 业务 handler）

`state/server.go:62-75`：

```go
func msgCmdHandler(cmdCtx *service.CmdContext, msgCmd *message.MsgCmd) {
    switch msgCmd.Type {
    case message.CmdType_Login:     loginMsgHandler(cmdCtx, msgCmd)
    case message.CmdType_Heartbeat: hearbeatMsgHandler(cmdCtx, msgCmd)
    case message.CmdType_ReConn:    reConnMsgHandler(cmdCtx, msgCmd)
    case message.CmdType_UP:        upMsgHandler(cmdCtx, msgCmd)
    case message.CmdType_ACK:       ackMsgHandler(cmdCtx, msgCmd)
    }
}
```

---

## 各业务 handler 详解

### 1. Login

`state/server.go:78-93`：

```go
func loginMsgHandler(cmdCtx *service.CmdContext, msgCmd *message.MsgCmd) {
    loginMsg := &message.LoginMsg{}
    proto.Unmarshal(msgCmd.Payload, loginMsg)

    // 创建连接状态 + 写入登陆槽 + 写入路由表 + 启动心跳定时器
    err := cs.connLogin(*cmdCtx.Ctx, loginMsg.Head.DeviceID, cmdCtx.ConnID)

    // 回复 ACK（connID 通过 ACK 告知客户端）
    sendACKMsg(message.CmdType_Login, cmdCtx.ConnID, 0, 0, "login ok")
}
```

**connLogin 做了什么**（`state/cache.go:67-89`）：

```
connLogin(did, connID):
  1. newConnState(did, connID)  → 创建 connState + 启动心跳定时器 5s
  2. SADD login_slot_set_{slot} "did|connID"  → Redis 登陆槽
  3. AddRecord(did, endpoint, connID)          → Redis 路由表
  4. storeConnIDState(connID, state)           → 本地 sync.Map
```

### 2. Heartbeat

`state/server.go:97-107`：

```go
func hearbeatMsgHandler(cmdCtx *service.CmdContext, msgCmd *message.MsgCmd) {
    heartMsg := &message.HeartbeatMsg{}
    proto.Unmarshal(msgCmd.Payload, heartMsg)
    cs.reSetHeartTimer(cmdCtx.ConnID)  // 重置心跳定时器（1s 心跳 → 5s 超时）
    // 不回复 ACK——减少通信量
}
```

心跳定时器机制（`state/state.go:128-137`）：

```
收到心跳 → reSetHeartTimer → 重置 5s 定时器
5s 未收到心跳 → 定时器触发 → reSetReConnTimer → 设置 10s 重连定时器
10s 未重连 → 定时器触发 → connLogOut → 登出
```

### 3. ReConn（重连）

`state/server.go:110-124`：

```go
func reConnMsgHandler(cmdCtx *service.CmdContext, msgCmd *message.MsgCmd) {
    reConnMsg := &message.ReConnMsg{}
    proto.Unmarshal(msgCmd.Payload, reConnMsg)
    // reConnMsg.Head.ConnID 是断开前的旧 connID
    // cmdCtx.ConnID 是重连后的新 connID
    cs.reConn(*cmdCtx.Ctx, reConnMsg.Head.ConnID, cmdCtx.ConnID)
    sendACKMsg(message.CmdType_ReConn, cmdCtx.ConnID, 0, 0, "reconn ok")
}
```

**reConn 做了什么**（`state/cache.go:105-114`）：

```go
func (cs *cacheState) reConn(ctx context.Context, oldConnID, newConnID uint64) error {
    did, _ := cs.connLogOut(ctx, oldConnID)  // 登出旧连接（清理 Redis）
    return cs.connLogin(ctx, did, newConnID)  // 用旧 did 登陆新连接（路由表不变）
}
```

> ⚠️ 当前实现：重连时 connLogin 会再写一次路由表，虽然值相同，但有一次多余的 Redis 操作。TODO 标记需要优化。

### 4. UP（上行消息）— 核心可靠性逻辑

`state/server.go:128-141`：

```go
func upMsgHandler(cmdCtx *service.CmdContext, msgCmd *message.MsgCmd) {
    upMsg := &message.UPMsg{}
    proto.Unmarshal(msgCmd.Payload, upMsg)

    // ⭐ CAS 原子检查：clientID 是否正确
    if cs.compareAndIncrClientID(*cmdCtx.Ctx, cmdCtx.ConnID,
        upMsg.Head.ClientID, upMsg.Head.SessionId) {

        sendACKMsg(message.CmdType_UP, cmdCtx.ConnID, upMsg.Head.ClientID, 0, "ok")
        // TODO: 调用业务层处理上行消息
        pushMsg(*cmdCtx.Ctx, cmdCtx.ConnID, cs.msgID, 0, upMsg.UPMsgBody)
    }
    // 如果 CAS 失败：重复消息，不回复 ACK，消息被丢弃
}
```

**可靠性机制**：

```
客户端发送 UPMsg(clientID=N)
  → state Lua CAS: get(key) == N ? incr : return -1
  → 成功 → 回复 ACK(N) → 客户端收到 ACK 后 clientID=N+1
  → 失败 → 不回复 ACK → 客户端超时重发（带相同 clientID）→ state CAS 再次拒绝

如果 ACK 在网络丢失：
  客户端超时重发 UPMsg(clientID=N)
  → state: get(key) 已经被 incr 过 = N+1 ≠ N → 返回 -1 → 丢弃
  → 但消息已处理，安全！
```

### 5. ACK（下行消息确认）

`state/server.go:144-152`：

```go
func ackMsgHandler(cmdCtx *service.CmdContext, msgCmd *message.MsgCmd) {
    ackMsg := &message.ACKMsg{}
    proto.Unmarshal(msgCmd.Payload, ackMsg)
    cs.ackLastMsg(*cmdCtx.Ctx, ackMsg.ConnID, ackMsg.SessionID, ackMsg.MsgID)
}
```

**ackLastMsg 的匹配逻辑**（`state/state.go:155-171`）：

```go
func (c *connState) ackLastMsg(ctx context.Context, sessionID, msgID uint64) bool {
    c.Lock()
    defer c.Unlock()
    msgTimerLock := fmt.Sprintf("%d_%d", sessionID, msgID)
    if c.msgTimerLock != msgTimerLock {  // ⭐ 锁匹配：只接受最新消息的 ACK
        return false
    }
    // 删除 lastMsg（Redis），停止重推定时器
    slot := cs.getConnStateSlot(c.connID)
    key := fmt.Sprintf(cache.LastMsgKey, slot, c.connID)
    cache.Del(ctx, key)
    c.msgTimer.Stop()
    return true
}
```

### 6. pushMsg（下行消息发送+可靠性保证）

`state/server.go:155-171`：

```go
func pushMsg(ctx context.Context, connID, sessionID, msgID uint64, data []byte) {
    pushMsg := &message.PushMsg{Content: data, MsgID: cs.msgID}
    downLoad, _ := proto.Marshal(pushMsg)
    sendMsg(connID, message.CmdType_Push, downLoad)  // → gateway RPC Push

    // 可靠性：缓存 lastMsg + 启动重推定时器
    cs.appendLastMsg(ctx, connID, pushMsg)
}
```

**appendLastMsg**（`state/cache.go:168-186` + `state/state.go:83-100`）：

```go
func (c *connState) appendMsg(ctx context.Context, key, msgTimerLock string, msgData []byte) {
    c.Lock()
    defer c.Unlock()
    c.msgTimerLock = msgTimerLock
    if c.msgTimer != nil {
        c.msgTimer.Stop()  // 停止旧定时器（如果有未 ACK 的消息，被替换）
    }
    // 缓存到 Redis
    cache.SetBytes(ctx, key, msgData, cache.TTL7D)
    // 启动 100ms 重推定时器
    c.msgTimer = AfterFunc(100*time.Millisecond, func() {
        rePush(c.connID)  // 从 Redis 读 lastMsg → 重发
    })
}
```

---

## Login / Logout 状态机

```
                 ┌──────────────┐
                 │  未连接      │
                 └──────┬───────┘
                        │ Login
                 ┌──────▼───────┐
          ┌──────│  已连接      │◄──── 心跳重置 ────┐
          │      └──────┬───────┘                    │
          │             │ 5s 无心跳                   │
          │      ┌──────▼───────┐                    │
          │      │  等待重连    │                    │
          │      └──────┬───────┘                    │
          │             │ 10s 内 ReConn              │
          │             └────────────────────────────┘
          │             │ 10s 无重连
          │      ┌──────▼───────┐
          └──────│  登出        │
                 └──────────────┘
```

---

## Redis 数据结构汇总

| 结构 | Key | Value | 用途 |
|------|-----|-------|------|
| Set | `login_slot_set_{slot}` | `"did\|connID"` | 登陆槽，重启恢复 |
| String | `gateway_rotuer_{did}` | `"endpoint-connID"` | 设备→连接路由 |
| String | `max_client_id_{slot}_{connID}_{session}` | 数字（Lua CAS 维护） | 上行消息去重 |
| String | `last_msg_{slot}_{connID}` | protobuf 序列化的 PushMsg | 下行消息可靠性 |

---

## 学习目标

学完后能：画出 Login→Heartbeat→ReConn→Logout 完整状态机，解释 Lua CAS 如何防重复消息，讲清楚 ACK+重推 的可靠性保障机制，列出 state 重启后恢复连接状态的完整步骤。

---

## 🔑 消息 ID 设计（clientID vs msgID）

- **clientID**（上行消息 ID）：客户端(会话维度)递增。state 用 Lua CAS 原子比较+自增防重复。客户端收到 ACK 后 `clientID++`
- **msgID**（下行消息 ID）：state 递增。客户端根据 msgID 判断是否有消息丢失

这是经典的**滑动窗口协议**简化版：单条消息确认，而非批量窗口。

---

## 试一下

```bash
# 画 state 的消息处理状态机
# 输入：cmdChannel 中的 CmdContext
# 路由：CancelConnCmd → connLogOut
#       SendMsgCmd → proto.Unmarshal → msgCmdHandler
#         → Login/Heartbeat/ReConn/UP/ACK → 各自 handler
# 输出：sendMsg(connID, type, data) → state gRPC client → gateway Push/DelConn

# 找每个 handler 对应的 connState 方法
# loginMsgHandler → connLogin → newConnState, SADD, AddRecord, storeConnIDState
# hearbeatMsgHandler → reSetHeartTimer
# reConnMsgHandler → connLogOut + connLogin
# upMsgHandler → compareAndIncrClientID + pushMsg
# ackMsgHandler → ackLastMsg
```
