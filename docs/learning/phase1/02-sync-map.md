# Phase 1-2: sync.Map 并发安全 map

## Go 概念

Go 内置 `map` 不是并发安全的——并发读写会 panic。`sync.Map` 是标准库提供的并发安全 map，适合**读多写少**场景。

## sync.Map 核心方法

```go
var m sync.Map
m.Store(key, value)     // 写入
value, ok := m.Load(key) // 读取
m.Delete(key)           // 删除
m.Range(func(k, v interface{}) bool {}) // 遍历
```

## 项目中的使用

### 场景1：ep.tables — connID → connection 映射

`gateway/epoll.go:21-29`：

```go
type ePool struct {
    tables sync.Map  // key: connID(uint64), value: *connection
    // ...
```

**写入** — `gateway/epoll.go:155`（连接注册到 epoll 时）：

```go
func (e *epoller) add(conn *connection) error {
    // ...
    ep.tables.Store(conn.id, conn)     // connID → connection
    // ...
}
```

**读取** — `gateway/server.go:86-89`（state 下发的 Push/DelConn 命令）：

```go
func closeConn(cmd *service.CmdContext) {
    if connPtr, ok := ep.tables.Load(cmd.ConnID); ok {
        conn, _ := connPtr.(*connection)  // 类型断言
        conn.Close()
    }
}
```

**删除** — `gateway/epoll.go:166`（连接断开时）：

```go
func (e *epoller) remove(c *connection) error {
    ep.tables.Delete(c.id)
    e.fdToConnTable.Delete(c.fd)  // 同时删 fd 映射
    // ...
}
```

**场景分析**：ep.tables 的操作分布在：
- Acceptor goroutine（写 Store）
- epoll 事件 goroutine（读 Load）
- state 命令消费 goroutine（读写 Load/Delete）
- epoller.remove goroutine（写 Delete）

多 goroutine 并发访问，必须用 sync.Map。且读（Load）远多于写（Store/Delete）——每个消息都要 Load 一次，而 Store 只在建连时发生。

### 场景2：cs.connToStateTable — connID → connState 映射

`state/cache.go:22-26`：

```go
type cacheState struct {
    connToStateTable sync.Map  // key: connID(uint64), value: *connState
}
```

`state/cache.go:121,129,132`（三个函数不连续）：

```go
func (cs *cacheState) loadConnIDState(connID uint64) (*connState, bool) {  // :121
    if data, ok := cs.connToStateTable.Load(connID); ok {
        sate, _ := data.(*connState)  // 类型断言，提取 *connState
        return sate, true
    }
    return nil, false
}
func (cs *cacheState) storeConnIDState(connID uint64, state *connState) {
    cs.connToStateTable.Store(connID, state)
}
func (cs *cacheState) deleteConnIDState(ctx context.Context, connID uint64) {
    cs.connToStateTable.Delete(connID)
}
```

### 场景3：ep.fdToConnTable — fd → connection 映射

`gateway/epoll.go:131-134`：

```go
type epoller struct {
    fdToConnTable sync.Map  // key: fd(int), value: *connection
}
```

epoll_wait 返回的是 fd 列表，需要通过 fd 快速找到对应的 `*connection` 对象来读数据。

`gateway/epoll.go:177-178`：

```go
for i := 0; i < n; i++ {
    if conn, ok := e.fdToConnTable.Load(int(events[i].Fd)); ok {
        connections = append(connections, conn.(*connection))
    }
}
```

---

## sync.Map 的底层原理（简化版）

sync.Map 内部有两个 map：

```
                  ┌── read map (只读，atomic 访问)
写入 → dirty map  │
读取 ← read map   │   miss 次数达标 → dirty 提升为 read
                  └── read map 是 dirty 的快照
```

- **读**：先查 read map（无锁），命中则返回。miss 次数多时加锁从 dirty map 读
- **写**：加锁写 dirty map
- **删**：加锁删 dirty map

所以 **读多写少** 时性能接近无锁，**写多** 时退化为加锁 map。

---

## 🔑 何时用 sync.Map vs 普通 map + sync.RWMutex

| 场景 | 用哪个 | 原因 |
|------|--------|------|
| 读多写少，key 稳定 | `sync.Map` | 内部无锁读取 |
| 写多，或 key 频繁变化 | `map + sync.RWMutex` | sync.Map 写操作有额外开销 |
| 需要 `len()` | `map + sync.RWMutex` | sync.Map 没有 Len 方法，Range 计数很慢 |
| 需要批量操作 | `map + sync.RWMutex` | sync.Map 没有原子批量操作 |
| 类型安全 | `map + sync.RWMutex` | sync.Map 用 `interface{}`，需要到处类型断言 |

**项目中 ep.tables 为什么用 sync.Map？**
- 连接建立后 connID 不变（key 稳定）
- 每个消息都要 Load 一次（读远多于写）
- 不需要 len、不需要批量操作
- → sync.Map 最佳场景

---

## 🔑 类型断言安全

sync.Map 的 value 是 `interface{}`，取出需要类型断言：

```go
// ❌ 不安全——如果类型不对会 panic
conn := connPtr.(*connection)

// ✅ 逗号-ok 模式——类型不对返回 false
if conn, ok := connPtr.(*connection); ok {
    // ...
}
```

项目中 `gateway/server.go:87`、`gateway/epoll.go:178` 都用逗号-ok 模式。

---

## 学习目标

学完后能：判断什么场景用 sync.Map vs map+Mutex，解释 sync.Map 的 read/dirty 双层设计，安全地进行类型断言。

---

## 试一下

1. `grep -rn "sync.Map" --include="*.go" .` 找到全部 4 处使用，给每个标注 Store/Load/Delete 分别在哪个 goroutine 中执行
2. **修改**：把 `ep.tables` 从 `sync.Map` 改为 `map[uint64]*connection + sync.RWMutex`，自己写 get/set/delete 方法，对比两种实现的代码量差异
3. 在 `gateway/server.go:87` 的类型断言 `connPtr.(*connection)` 中，故意把 `*connection` 改成 `*int`，编译报什么错？运行时没报错说明什么？
