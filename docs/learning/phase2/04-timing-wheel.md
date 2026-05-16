# Phase 2-4: 层级时间轮

## 为什么需要时间轮

IM 系统需要大量定时器：
- 100 万连接 × 心跳定时器(5s) = 100 万个定时器
- 100 万连接 × 重连定时器(10s) = 100 万个定时器
- 每条消息 × 重推定时器(100ms) = 不确定数量

如果每个都用 `time.AfterFunc`（底层是 Go runtime timer 堆，O(log n)），内存和 CPU 无法承受。时间轮插入和删除都是 **O(1)**。

## 层级时间轮原理

### 单层时间轮

```
tick = 1ms, wheelSize = 20, 覆盖 20ms

┌───┬───┬───┬─────┬───┐
│ 0 │ 1 │ 2 │ ... │19 │  ← 20 个 bucket
└───┴───┴───┴─────┴───┘
  ↑
currentTime

每个 bucket 对应一个 tick 时间段
bucket[0]: [now, now+1ms)
bucket[1]: [now+1ms, now+2ms)
...
```

### 层级溢出

当定时器超时时间超过当前轮覆盖范围（interval = tick × wheelSize = 20ms）：

```
单轮覆盖 20ms
  → 溢出 → 上层轮覆盖 20ms × 20 = 400ms
  → 溢出 → 再上层覆盖 400ms × 20 = 8s
  → 溢出 → 再上层覆盖 8s × 20 = 160s
```

所以 4 层就覆盖了 160s，足够处理 10s 以内的重连超时。

## 核心实现

### 创建

`common/timingwheel/timingwheel.go:30-44`：

```go
func NewTimingWheel(tick time.Duration, wheelSize int64) *TimingWheel {
    tickMs := int64(tick / time.Millisecond)
    startMs := timeToMs(time.Now().UTC())
    return newTimingWheel(tickMs, wheelSize, startMs, NewDelayqueue(int(wheelSize)))
}

func newTimingWheel(tickMs, wheelSize, startMs int64, queue *DelayQueue) *TimingWheel {
    buckets := make([]*bucket, wheelSize)
    for i := range buckets {
        buckets[i] = newBucket()
    }
    return &TimingWheel{
        tick:        tickMs,
        wheelSize:   wheelSize,
        currentTime: truncate(startMs, tickMs),  // 向下对齐到 tick 边界
        interval:    tickMs * wheelSize,
        buckets:     buckets,
        queue:       queue,
        exitC:       make(chan struct{}),
    }
}
```

plato 配置：`tick=1ms, wheelSize=20`，单轮覆盖 20ms。

### add — 插入定时器

`common/timingwheel/timingwheel.go:64-105`：

```go
func (tw *TimingWheel) add(t *Timer) bool {
    currentTime := atomic.LoadInt64(&tw.currentTime)
    if t.expiration < currentTime+tw.tick {
        return false  // 已经过期了
    } else if t.expiration < currentTime+tw.interval {
        // 在本轮范围内：计算目标 bucket
        virtualID := t.expiration / tw.tick
        b := tw.buckets[virtualID % tw.wheelSize]
        b.Add(t)
        // 如果 bucket 的过期时间改变（说明 bucket 刚被轮转重用），重新入队
        if b.SetExpiration(virtualID * tw.tick) {
            tw.queue.Offer(b, b.Expiration())
        }
        return true
    } else {
        // 超出本轮范围：递归放入上层 overflowWheel
        overflowWheel := atomic.LoadPointer(&tw.overflowWheel)
        if overflowWheel == nil {
            // CAS 懒初始化 overflowWheel
            atomic.CompareAndSwapPointer(&tw.overflowWheel, nil, unsafe.Pointer(newTimingWheel(tw.interval, tw.wheelSize, currentTime, tw.queue)))
            overflowWheel = atomic.LoadPointer(&tw.overflowWheel)
        }
        return (*TimingWheel)(overflowWheel).add(t)
    }
}
```

**关键设计**：
1. 共享 `queue`（DelayQueue）——所有层级共享一个延迟队列
2. overflowWheel 懒初始化——只在需要时创建
3. `currentTime` 用 atomic 读写——多 goroutine 读

### Start — 驱动时间轮

`common/timingwheel/timingwheel.go:134-153`：

```go
func (tw *TimingWheel) Start() {
    // Goroutine 1: 轮询 DelayQueue，产生到期 bucket
    tw.waitGroup.Wrap(func() {
        tw.queue.Poll(tw.exitC, func() int64 {
            return timeToMs(time.Now().UTC())
        })
    })
    // Goroutine 2: 消费到期 bucket
    tw.waitGroup.Wrap(func() {
        for {
            select {
            case elem := <-tw.queue.C:
                b := elem.(*bucket)
                tw.advanceClock(b.Expiration())  // 推动时钟前进
                b.Flush(tw.addOrRun)             // 执行 bucket 中所有 timer
            case <-tw.exitC:
                return
            }
        }
    })
}
```

**两个 goroutine 的分工**：
- Poll goroutine：持续检查队列头部 bucket 是否到期，到期则写入 `queue.C`
- Consume goroutine：从 `queue.C` 读取到期 bucket，执行其中的 timer

### advanceClock — 推动时钟

`common/timingwheel/timingwheel.go:119-131`：

```go
func (tw *TimingWheel) advanceClock(expiration int64) {
    currentTime := atomic.LoadInt64(&tw.currentTime)
    if expiration >= currentTime+tw.tick {
        currentTime = truncate(expiration, tw.tick)
        atomic.StoreInt64(&tw.currentTime, currentTime)
        // 递归推动上层轮
        overflowWheel := atomic.LoadPointer(&tw.overflowWheel)
        if overflowWheel != nil {
            (*TimingWheel)(overflowWheel).advanceClock(currentTime)
        }
    }
}
```

每处理一个到期的 bucket，时钟前进到该 bucket 的过期时间。

---

## state 中的使用

`state/timer.go:11-21`：

```go
var wheel *timingwheel.TimingWheel

func InitTimer() {
    wheel = timingwheel.NewTimingWheel(time.Millisecond, 20)
    wheel.Start()
}

func AfterFunc(d time.Duration, f func()) *timingwheel.Timer {
    return wheel.AfterFunc(d, f)
}
```

### 三个定时器

| 定时器 | 间隔 | 回调 | 在 state.go |
|--------|------|------|------------|
| `heartTimer` | 5s | `reSetReConnTimer()`（启动重连定时器） | `connState.reSetHeartTimer:128` |
| `reConnTimer` | 10s | `cs.connLogOut()`（登出） | `connState.reSetReConnTimer:139` |
| `msgTimer` | 100ms | `rePush(connID)`（重推消息） | `connState.appendMsg:92` |

### 定时器重置模式

```go
func (c *connState) reSetHeartTimer() {
    c.Lock()
    defer c.Unlock()
    if c.heartTimer != nil {
        c.heartTimer.Stop()    // 先停掉旧的
    }
    c.heartTimer = AfterFunc(5*time.Second, func() {
        c.reSetReConnTimer()
    })
}
```

**每次收到心跳消息就重置定时器**。如果 5s 没收到心跳 → 启动重连定时器。如果 10s 没重连 → 登出。

---

## AfterFunc 流程（对照标准库 time.AfterFunc）

`common/timingwheel/timingwheel.go:167-174`：

```go
func (tw *TimingWheel) AfterFunc(d time.Duration, f func()) *Timer {
    t := &Timer{
        expiration: timeToMs(time.Now().UTC().Add(d)),
        task:       f,                          // 回调函数
    }
    tw.addOrRun(t)
    return t
}
```

与 `time.AfterFunc` 的最大区别：
- `time.AfterFunc`：每创建一个底层加入全局 timer 堆，大规模下性能差
- 时间轮：O(1) 插入时间轮 bucket，批量触发

---

## 🔑 时间轮 vs time.AfterFunc vs time.Ticker

| 方案 | 插入 | 内存 | 适用场景 |
|------|------|------|----------|
| `time.AfterFunc` | O(log n) | 每个 timer 独立 | 少量定时器（< 1000） |
| `time.Ticker` | O(log n) | 每 tick 一个 | 周期性任务但对精度要求不高 |
| **时间轮** | **O(1)** | bucket 共享 | 大量定时器（百万级） |

---

## 🔑 Kafka 也用时间轮

Kafka 的时间轮实现（`org.apache.kafka.server.util.timer.TimingWheel`）几乎一样：层级溢出、DelayQueue、bucket 批量执行。plato 的实现是 Go 移植版。

---

## 学习目标

学完后能：解释时间轮相比 `time.AfterFunc` 在大规模场景下的优势，理解层级溢出的设计，画出 plato 中三个定时器（心跳/重连/重推）的触发链。

---

## 试一下（必做）

1. 跑 `go test -v ./common/timingwheel/`，看 `TestTimingWheel` 是如何验证定时器回调被正确触发的
2. **修改测试**：把 tick 从 1ms 改为 500ms，观察测试是否还能通过（理解 tick 粒度与测试精度的关系）
3. 在 `state/state.go` 中找一个定时器回调（如 `reSetHeartTimer()`），追踪：回调触发后最终会走到哪个 Redis 操作？
4. 思考：如果 `msgTimer` 的 100ms 间隔改为一分钟，100万连接同时有一个待 ACK 的消息，时间轮内存占用多大？如果用 `time.AfterFunc` 呢？
