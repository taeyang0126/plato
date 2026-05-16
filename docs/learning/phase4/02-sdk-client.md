# Phase 4-2: 客户端 SDK + TUI

## 模块职责

- `common/sdk/`：客户端 SDK，封装 TCP 连接管理 + 消息协议
- `client/`：CLI 聊天客户端，基于 gocui TUI

## SDK 核心：Chat

### Chat 结构体

`common/sdk/api.go:22-30`：

```go
type Chat struct {
    Nick             string
    UserID           string
    SessionID        string
    conn             *connect              // TCP 连接
    closeChan        chan struct{}         // 关闭信号
    MsgClientIDTable map[string]uint64     // sessionID → clientID（上行消息计数）
    sync.RWMutex
}
```

### 生命周期

`common/sdk/api.go:41-54`：

```go
func NewChat(ip net.IP, port int, nick, userID, sessionID string) *Chat {
    chat := &Chat{
        Nick:             nick,
        UserID:           userID,
        SessionID:        sessionID,
        conn:             newConnet(ip, port),   // ① 建立 TCP 连接
        closeChan:        make(chan struct{}),
        MsgClientIDTable: make(map[string]uint64),
    }
    go chat.loop()        // ② 启动读循环
    chat.login()          // ③ 发送登录消息
    go chat.heartbeat()   // ④ 启动心跳（1s 间隔）
    return chat
}
```

### login — 发送登录消息

`common/sdk/api.go:139-150`：

```go
func (chat *Chat) login() {
    loginMsg := message.LoginMsg{
        Head: &message.LoginMsgHead{DeviceID: 123},
    }
    payload, _ := proto.Marshal(&loginMsg)
    chat.conn.send(message.CmdType_Login, payload)
}
```

### heartbeat — 定时心跳

`common/sdk/api.go:165-189`：

```go
func (chat *Chat) heartbeat() {
    tc := time.NewTicker(1 * time.Second)  // 每秒
    defer chat.heartbeat()  // ⚠️ 递归调用！退出后重新开始
    for {
        select {
        case <-chat.closeChan:
            return
        case <-tc.C:
            heartbeat := message.HeartbeatMsg{Head: &message.HeartbeatMsgHead{}}
            payload, _ := proto.Marshal(&heartbeat)
            chat.conn.send(message.CmdType_Heartbeat, payload)
        }
    }
}
```

> ⚠️ `defer chat.heartbeat()` 递归——函数退出后重启一个新的 heartbeat goroutine。不是标准做法，但功能正确。

### Send — 发送消息

`common/sdk/api.go:55-68`：

```go
func (chat *Chat) Send(msg *Message) {
    data, _ := json.Marshal(msg)
    upMsg := &message.UPMsg{
        Head: &message.UPMsgHead{
            ClientID:  chat.getClientID(chat.SessionID),  // 递增 clientID
            ConnID:    chat.conn.connID,
            SessionId: chat.SessionID,
        },
        UPMsgBody: data,
    }
    payload, _ := proto.Marshal(upMsg)
    chat.conn.send(message.CmdType_UP, payload)
}
```

### getClientID — 会话维度的 clientID 递增

`common/sdk/api.go:128-137`：

```go
func (chat *Chat) getClientID(sessionID string) uint64 {
    chat.Lock()
    defer chat.Unlock()
    var res uint64
    if id, ok := chat.MsgClientIDTable[sessionID]; ok {
        res = id
    }
    chat.MsgClientIDTable[sessionID] = res + 1  // 递增
    return res
}
```

clientID 从 0 开始：第一条消息 clientID=0，第二条 clientID=1，…… 服务端用 Lua CAS 验证连续性。

### loop — 读循环

`common/sdk/api.go:99-126`：

```go
func (chat *Chat) loop() {
    for {
        select {
        case <-chat.closeChan:
            return
        default:
            mc := &message.MsgCmd{}
            data, _ := tcp.ReadData(chat.conn.conn)
            proto.Unmarshal(data, mc)

            var msg *Message
            switch mc.Type {
            case message.CmdType_ACK:
                msg = handAckMsg(chat.conn, mc.Payload)
            case message.CmdType_Push:
                msg = handPushMsg(chat.conn, mc.Payload)
            }
            chat.conn.recvChan <- msg  // → 上层通过 Recv() 接收
        }
    }
}
```

### Recv — 上层接收接口

```go
func (chat *Chat) Recv() <-chan *Message {
    return chat.conn.recv()
}
```

只读 channel，调用方通过 `for msg := range chat.Recv()` 阻塞接收。

### ReConn — 重连

`common/sdk/api.go:85-92`：

```go
func (chat *Chat) ReConn() {
    chat.Lock()
    defer chat.Unlock()
    chat.MsgClientIDTable = make(map[string]uint64)  // 重置 clientID
    chat.conn.reConn()  // 关闭旧连接 → 重新 DialTCP
    chat.reConn()       // 发送 ReConn 消息（携带旧 connID）
}
```

---

## connect — TCP 连接层

`common/sdk/net.go:14-20`：

```go
type connect struct {
    sendChan, recvChan chan *Message
    conn               *net.TCPConn
    connID             uint64  // 服务端分配的 connID
    ip                 net.IP
    port               int
}
```

### send — 发送 protobuf 消息

`common/sdk/net.go:80-96`：

```go
func (c *connect) send(ty message.CmdType, palyload []byte) error {
    msgCmd := message.MsgCmd{Type: ty, Payload: palyload}
    msg, _ := proto.Marshal(&msgCmd)
    dataPgk := tcp.DataPgk{Data: msg, Len: uint32(len(msg))}
    _, err := c.conn.Write(dataPgk.Marshal())
    return err
}
```

### handAckMsg — 接收 ACK

`common/sdk/net.go:39-53`：

```go
func handAckMsg(c *connect, data []byte) *Message {
    ackMsg := &message.ACKMsg{}
    proto.Unmarshal(data, ackMsg)
    switch ackMsg.Type {
    case message.CmdType_Login, message.CmdType_ReConn:
        atomic.StoreUint64(&c.connID, ackMsg.ConnID)  // ⭐ 从 ACK 获取 connID
    }
    return &Message{Type: MsgTypeAck, Content: ackMsg.Msg, ...}
}
```

**connID 的获取**：客户端不生成 connID，服务端 login ACK 中返回 `ackMsg.ConnID`，客户端用 `atomic.StoreUint64` 存储。

### handPushMsg — 接收 Push

`common/sdk/net.go:54-69`：

```go
func handPushMsg(c *connect, data []byte) *Message {
    pushMsg := &message.PushMsg{}
    proto.Unmarshal(data, pushMsg)
    msg := &Message{}
    json.Unmarshal(pushMsg.Content, msg)
    // 回复 ACK
    ackMsg := &message.ACKMsg{Type: message.CmdType_UP, ConnID: c.connID}
    ackData, _ := proto.Marshal(ackMsg)
    c.send(message.CmdType_ACK, ackData)
    return msg
}
```

收到 Push 后回复带 `connID` 的 ACK，服务端据此停止重推。

---

## CLI 客户端（TUI）

`client/cui.go` — 基于 gocui 的终端 UI。功能：
- 登陆 → 输入消息 → Send → Recv 显示 → ReConn 按钮

---

## 完整客户端状态机

```
                         NewChat(ip, port)
                               │
                     ┌─────────▼─────────┐
                     │    TCP 已连接      │
                     │  send Login       │
                     └────────┬──────────┘
                              │ 收到 ACK(connID)
                     ┌────────▼──────────┐
                     │    已登录          │◄──────── 循环 ────────┐
                     │  loop + heartbeat │                      │
                     └────────┬──────────┘                      │
                              │                                  │
            ┌─────────────────┼──────────────────┐              │
            │                 │                   │              │
            ▼                 ▼                   ▼              │
       Send(msg)        heartbeat(1s)      收到 Push              │
       send UP         send Heartbeat     handPushMsg → ACK      │
            │                 │                   │              │
            └─────────────────┼───────────────────┘              │
                              │                                  │
                    连接断开   │                                  │
                              ▼                                  │
                     ┌────────────────┐                          │
                     │   ReConn()     │──────────────────────────┘
                     │ 重置 clientID  │     重连成功 → 已登录
                     │ send ReConn    │
                     └────────────────┘
```

---

## 🔑 protobuf + JSON 双层序列化

项目中有趣的设计：

```
JSON (应用层消息体) → protobuf UPMsg.Payload → protobuf MsgCmd.Payload → TCP
```

**原因**：
- protobuf 用于**协议层**（MsgCmd、ACK、Push 等控制消息），强类型、高效
- JSON 用于**应用层**（聊天文本、用户信息等），灵活、可读

解耦方式：protobuf 的 `Payload` 字段是 `[]byte`，任何二进制数据都可以塞进去。

---

## 学习目标

学完后能：画出 SDK 的 Login→Heartbeat→Send→Recv→ReConn 完整状态机，理解 connID 的获取流程和协议栈的双层序列化设计（JSON→protobuf→TCP）。

---

## 试一下

1. 编译 `./plato client`，启动 gateway+state 后连上，发一条消息，在 `common/sdk/net.go:80` 的 `send` 函数中加 `fmt.Printf` 打印每个发送包的 hex，对比 `tcp-protocol.md` 中定义的协议格式
2. **修改**：在 `api.go:165` 的 `heartbeat` 函数中，把 `defer chat.heartbeat()` 删掉，客户端网络断开一次看心跳 goroutine 是否永久退出（验证递归 defer 的作用）
3. 在 `api.go:85` 的 `ReConn` 中，把 `chat.MsgClientIDTable = make(map[string]uint64)` 注释掉（不重置 clientID），重连后发消息，观察 state 侧 `compareAndIncrClientID` 是否拒绝（理解为什么重连必须重置 clientID）
4. 搜索 SDK 中所有 `proto.Unmarshal` 和 `json.Marshal` 忽略 error 的地方，思考：如果服务端发送了损坏的 protobuf 数据，SDK 会发生什么？
