# Phase 1-6: Go 常见陷阱

每个陷阱都标注了"项目中是否出现"。

## 1. nil interface ≠ nil 具体类型

```go
// ❌ 陷阱——interface{} 的 nil 判断是双重检查
var p *connection = nil
var i interface{} = p
fmt.Println(i == nil)  // false！因为 interface 内部 (type=*connection, value=nil)
```

**项目中**：没有直接出现，但 sync.Map 的 `Load` 返回 `(interface{}, bool)`，第二个返回值才是可靠的 nil 检查。

---

## 2. Slice append 的容量陷阱

```go
// ❌ 陷阱——append 可能返回新底层数组
a := []int{1, 2, 3}
b := append(a, 4)
a[0] = 999
fmt.Println(b[0])  // 可能也是 999（如果 a 容量够），也可能不是（扩容了）
```

**规范做法**：永远不要对新旧 slice 的底层数组做共享假设。

---

## 3. range 循环是值拷贝

```go
// ❌ 陷阱——range 遍历的是副本，修改它不影响原 slice
type User struct{ Name string }
users := []User{{"a"}, {"b"}}
for _, u := range users {
    u.Name = "modified"  // 只改了局部变量 u，users 不变
}
// users 还是 [{"a"}, {"b"}]

// ✅ 用索引
for i := range users {
    users[i].Name = "modified"
}
```

**项目中**：Phase 1-1 已经讲了 goroutine 闭包捕获循环变量，这是同一个陷阱的不同表现形式。

---

## 4. defer 在循环中

```go
// ❌ 陷阱——defer 在函数结束时才执行，不是在循环迭代结束时
for _, file := range files {
    f, _ := os.Open(file)
    defer f.Close()  // 所有文件积累到函数结束时才关闭 → 资源泄漏
}

// ✅ 封装成一个函数
for _, file := range files {
    func() {
        f, _ := os.Open(file)
        defer f.Close()
        process(f)
    }()
}
```

**项目中**：没有出现。

---

## 5. map 并发读写会 panic

```go
var m = make(map[int]string)

// goroutine 1
go func() { m[1] = "a" }()  // 写

// goroutine 2
go func() { fmt.Println(m[1]) }()  // 读

// fatal error: concurrent map read and map write
```

Go runtime 会检测并发 map 访问并直接 crash（称为 fatal error，无法 recover）。

**项目中**：所有并发场景都用了 sync.Map（Phase 1-2）。如果用普通 map 加 sync.RWMutex 也行。

---

## 6. channel 关闭的规则

```go
// ❌ 陷阱 1：往已关闭的 channel 写 → panic
ch := make(chan int)
close(ch)
ch <- 1  // panic: send on closed channel

// ❌ 陷阱 2：关闭已关闭的 channel → panic  
close(ch)
close(ch)  // panic: close of closed channel

// ❌ 陷阱 3：关闭 nil channel → panic
var ch chan int
close(ch)  // panic
```

**安全关闭规则**（已在 Phase 1-1 提到）：只由发送方关闭 channel。

---

## 7. time.After 的内存泄漏

```go
// ❌ 陷阱——time.After 创建的 timer 在到期前不会被 GC
for {
    select {
    case <-time.After(1 * time.Second):  // 每次循环创建新 timer，但是如果一直有数据从其他 case 来，这个 timer 永远不会过期
        doSomething()
    }
}

// ✅ 复用 timer
t := time.NewTimer(1 * time.Second)
for {
    t.Reset(1 * time.Second)
    select {
    case <-t.C:
        doSomething()
    }
}
```

**项目中**：`gateway/epoll.go:111` 的 `ep.wait(200)` 返回 slice 复用，规避了这个问题。

---

## 8. nil channel 在 select 中的行为

```go
// select 中 nil channel 永远不满足
var nilCh chan int
select {
case <-nilCh:  // 永不触发
default:
    fmt.Println("default")
}
// 输出: default
```

这个可以是"陷阱"也可以是"技巧"——Phase 1-1 提到用 nil channel 禁用 select 某个分支。

---

## 9. string 和 []byte 转换有拷贝

```go
s := "hello"
b := []byte(s)  // 拷贝！分配新内存
s2 := string(b)  // 又拷贝！
```

在热路径上（如高频日志、消息处理），频繁的类型转换会带来内存分配压力。`unsafe` 可以零拷贝转换但风险极高。

**项目中**：没有明显的转换热点，但消息体的 protobuf 处理全在 `[]byte` 上，避免了转换。

---

## 10. GORM Updates 的零值陷阱

```go
// ❌ 陷阱——GORM Updates 忽略零值
db.Model(&User{}).Where("id = ?", 1).Updates(User{Age: 0})  
// Age 不会更新到 0，因为 GORM 跳过零值！

// ✅ 用 Select 或 map
db.Model(&User{}).Select("Age").Where("id = ?", 1).Updates(User{Age: 0})
db.Model(&User{}).Where("id = ?", 1).Updates(map[string]interface{}{"age": 0})
```

**项目中**：Phase 4-1 的 user-domain.md 已经指出了这一点。

---

## 11. goroutine 泄漏检测

用 `runtime.NumGoroutine()` 监控。更专业的是 `go.uber.org/goleak`：

```go
func TestMain(m *testing.M) {
    goleak.VerifyTestMain(m)
}
```

**项目中**：没有 goroutine 泄漏检测。

---

## 12. 结构体嵌入的字段提升

```go
type A struct{ Name string }
type B struct {
    A           // 嵌入 A，B 可以直接用 Name
    Name string // 同名时遮蔽 A 的 Name
}
b := B{}
b.Name  // B.Name
b.A.Name  // A.Name（需要显式访问）
```

**项目中**：`common/prpc/server.go:23` PServer 嵌入了 `serverOptions`，直接使用 `p.serviceName`。

---

## 学习目标

学完后能：识别 12 个 Go 常见陷阱，在阅读项目代码时主动判断"这里有没有踩坑"，编写新代码时避免重蹈覆辙。

---

## 试一下

打开项目源码，逐条检查：项目中有没有上面的陷阱？每找到一处打勾（如：sdk/api.go:56 的 `data, _ := json.Marshal(msg)` 属于忽略 error）。至少找出 5 处。

---

## 🔑 学习建议

这些陷阱不需要死记。每遇到一个，回来打勾。踩过一遍就记住了。
