# Phase 1-3: atomic + unsafe.Pointer 无锁编程

## Go 概念

`sync/atomic` 提供无锁原子操作。`unsafe.Pointer` 可以绕过 Go 类型系统，但配合 atomic 可以实现高性能的无锁指针替换。

## 项目中的最佳范例：ipconf 滑动窗口无锁更新统计

### 问题场景

`ipconf/domain/endport.go` 中的 `Endport` 有一个 `Stats` 字段，存储 gateway 节点的连接数和消息字节数统计。这个字段需要：
- **被调度线程读取**（`CalculateScore`，高频读）
- **被数据源 goroutine 更新**（`UpdateStat`，低频写）

读写锁会阻塞评分计算，影响 HTTP 响应延迟。

### 解决方案：atomic.SwapPointer

`ipconf/domain/endport.go:17-37`（NewEndport 完整函数，atomic.SwapPointer 在 :29）：

```go
func NewEndport(ip, port string) *Endport {
    ed := &Endport{IP: ip, Port: port}
    ed.window = newStateWindow()
    ed.Stats = ed.window.getStat()               // 初始 Stat 指针
    go func() {
        for stat := range ed.window.statChan {
            ed.window.appendStat(stat)            // 更新滑动窗口
            newStat := ed.window.getStat()        // 计算新 Stat（创建新对象！）
            // ⭐ 原子替换指针——读写两边都无锁
            atomic.SwapPointer(
                (*unsafe.Pointer)(unsafe.Pointer(&ed.Stats)),
                unsafe.Pointer(newStat),
            )
        }
    }()
    return ed
}
```

**读侧** `ipconf/domain/endport.go:38-44`：

```go
func (ed *Endport) CalculateScore(ctx *IpConfContext) {
    if ed.Stats != nil {
        ed.ActiveSorce = ed.Stats.CalculateActiveSorce()
        ed.StaticSorce = ed.Stats.CalculateStaticSorce()
    }
}
```

**关键设计**：
- 写方不修改旧 Stat 对象的内容，而是创建新对象，再用 atomic 替换指针
- 读方直接读指针（`ed.Stats`），如果指针刚好在替换中，读到旧值或新值都是有效的完整对象——不会出现"读了一半"
- 前提：指针在 64 位系统上读写是原子的（Go 保证对齐）

### 为什么不用读写锁？

```go
// 锁方案的问题
func (ed *Endport) CalculateScore(ctx *IpConfContext) {
    ed.mu.RLock()
    // 这里的计算可能耗时（虽然本例中很快）
    score := ed.Stats.CalculateActiveSorce()
    ed.mu.RUnlock()
    // 如果 10000 个请求同时算分，锁竞争会排队
}
```

无锁方案：10000 个 goroutine 同时读指针，完全不互相阻塞。

---

## atomic 常用操作（对照项目代码）

### 1. AddInt32 — 原子加减

`gateway/epoll.go:205-212`（TCP 连接数计数器）：

```go
var tcpNum int32  // ⚠️ 必须是 int32/int64，不能用普通 int

func addTcpNum() {
    atomic.AddInt32(&tcpNum, 1)   // 原子 +1
}
func subTcpNum() {
    atomic.AddInt32(&tcpNum, -1)  // 原子 -1
}
func getTcpNum() int32 {
    return atomic.LoadInt32(&tcpNum)  // 原子读
}
```

**为什么？** 多个 Acceptor goroutine 同时 Accept 新连接、多个 epoll goroutine 同时断开连接。`tcpNum++` 不是原子的（读-改-写三步），并发时会丢计数。

### 2. Load/Store — 原子读写

`gateway/epoll.go:209-210` 中 `atomic.LoadInt32` 替代直接读 `tcpNum`。

### 3. CompareAndSwap — CAS

`common/timingwheel/timingwheel.go:91-101`（overflowWheel 懒初始化）：

```go
overflowWheel := atomic.LoadPointer(&tw.overflowWheel)
if overflowWheel == nil {
    // CAS: 如果 overflowWheel 还是 nil，就设置为新 wheel
    // 如果失败了（其他 goroutine 已经设置了），也不碍事
    atomic.CompareAndSwapPointer(
        &tw.overflowWheel,
        nil,                                      // 期望旧值
        unsafe.Pointer(newTimingWheel(...)),       // 新值
    )
    overflowWheel = atomic.LoadPointer(&tw.overflowWheel)
}
```

**为什么用 CAS？** 多个 goroutine 检测到 `overflowWheel == nil` 后会同时尝试创建，CAS 保证只有一个成功。这是**lock-free 懒加载**标准模式。

---

## unsafe.Pointer 的作用

Go 的类型系统中，`*Stat` 和 `unsafe.Pointer` 是不同的类型。`atomic.SwapPointer` 操作的是 `unsafe.Pointer`：

```go
// ed.Stats 类型是 *Stat
// 想原子替换它，需要将其视为 unsafe.Pointer

// 步骤：
// 1. *Stat → unsafe.Pointer
unsafe.Pointer(ed.Stats)

// 2. 取 *Stat 指针本身的地址 → *unsafe.Pointer
(*unsafe.Pointer)(unsafe.Pointer(&ed.Stats))

// 3. 原子替换
atomic.SwapPointer(
    (*unsafe.Pointer)(unsafe.Pointer(&ed.Stats)),
    unsafe.Pointer(newStat),
)
```

**解读**：`(*unsafe.Pointer)(unsafe.Pointer(&ed.Stats))` 就是把 `*Stat` 类型变量的地址，重新解释为 `*unsafe.Pointer` 类型，以便传给 `atomic.SwapPointer`。

---

## 🔑 无锁编程三原则

1. **复制而非修改**：写方创建新对象替换，读方总能看到完整状态
2. **指针大小在目标平台原子**：64 位系统上指针就是 64 位，硬件保证原子读写
3. **CAS 重试或容忍失败**：CAS 不保证成功，调用方要么重试，要么接受失败（如 overflowWheel 例子）

---

## 🔑 什么时候用 atomic，什么时候用 mutex

| 场景 | 方案 | 理由 |
|------|------|------|
| 单个 int/bool/pointer | atomic | 性能最好 |
| 结构体多个字段需要原子更新 | mutex | 无法用 atomic 同时更新多个值 |
| 需要保护一段代码而非一个变量 | mutex | atomic 只能保护单次操作 |
| 读极多写极少 | atomic（指针替换模式） | 读无锁 |

---

## 学习目标

学完后能：用 atomic 进行无锁计数器和指针替换，理解 CAS 的 lazy-init 模式，区分 atomic 和 mutex 的适用场景。

---

## 试一下

1. 看 `ipconf/domain/endport.go, window.go, stat.go` 三个文件，画 goroutine + channel + atomic 的完整数据流图
2. **修改**：在 `gateway/epoll.go:205` 中，把 `atomic.AddInt32` 改为普通 `tcpNum++`，启动 gateway 后疯狂 Accept+Close 连接，最终 `tcpNum` 的值还准确吗？
3. 在 `common/timingwheel/timingwheel.go:91-101` 的 CAS 处，如果去掉 `if overflowWheel == nil` 直接 CAS，功能还正确吗？看看 `newTimingWheel` 是否有副作用
