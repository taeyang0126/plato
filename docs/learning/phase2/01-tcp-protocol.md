# Phase 2-1: 自定义 TCP 协议编解码

## 协议定义

plato 使用自定义**长度前缀(Length-Prefixed)**TCP 协议：

```
┌──────────────┬──────────────────┐
│  4 bytes     │   N bytes        │
│  DataLen     │   Data           │
│  (BigEndian) │   (protobuf)     │
└──────────────┴──────────────────┘
```

## 编解码实现

### DataPgk 结构体 + 序列化

`common/tcp/coder.go:8-16`：

```go
type DataPgk struct {
    Len  uint32   // 数据体长度
    Data []byte   // 数据体（已序列化的 protobuf）
}

func (d *DataPgk) Marshal() []byte {
    bytesBuffer := bytes.NewBuffer([]byte{})
    binary.Write(bytesBuffer, binary.BigEndian, d.Len)  // 写入4字节大端长度
    return append(bytesBuffer.Bytes(), d.Data...)        // 追加数据体
}
```

**大端(BigEndian)的含义**：数值的高位字节存在低地址。比如 `len=0x00000100`(256)：
```
地址:   [0]    [1]    [2]    [3]
大端:   0x00   0x00   0x01   0x00  ← Go 的 binary.BigEndian
小端:   0x00   0x01   0x00   0x00
```

网络字节序统一用大端。Java 的 `DataOutputStream.writeInt()` 也是大端。

### 读取（拆包）

`common/tcp/read.go:11-31`：

```go
func ReadData(conn *net.TCPConn) ([]byte, error) {
    // Step 1: 读 4 字节头
    dataLenBuf := make([]byte, 4)
    if err := readFixedData(conn, dataLenBuf); err != nil {
        return nil, err
    }
    // Step 2: 解析长度
    buffer := bytes.NewBuffer(dataLenBuf)
    if err := binary.Read(buffer, binary.BigEndian, &dataLen); err != nil {
        return nil, fmt.Errorf("read headlen error:%s", err.Error())
    }
    if dataLen <= 0 {
        return nil, fmt.Errorf("wrong headlen :%d", dataLen)
    }
    // Step 3: 读数据体
    dataBuf := make([]byte, dataLen)
    if err := readFixedData(conn, dataBuf); err != nil {
        return nil, fmt.Errorf("read headlen error:%s", err.Error())
    }
    return dataBuf, nil
}
```

### readFixedData — 循环读到指定长度

`common/tcp/read.go:34-49`：

```go
func readFixedData(conn *net.TCPConn, buf []byte) error {
    _ = (*conn).SetReadDeadline(time.Now().Add(120 * time.Second))
    var pos int = 0
    var totalSize int = len(buf)
    for {
        c, err := (*conn).Read(buf[pos:])
        if err != nil {
            return err
        }
        pos = pos + c
        if pos == totalSize {
            break
        }
    }
    return nil
}
```

**关键点**：TCP 是流式协议，一次 `Read` 不一定返回完整数据包。必须循环读直到读满目标长度。

**ReadDeadline 120s**：防止恶意客户端建立连接后不发数据，占用服务端资源。超时后 `Read` 返回 error，连接被关闭。

### 写入

`common/tcp/write.go:6-19`：

```go
func SendData(conn *net.TCPConn, data []byte) error {
    totalLen := len(data)
    writeLen := 0
    for {
        len, err := conn.Write(data[writeLen:])
        if err != nil {
            return err
        }
        writeLen = writeLen + len
        if writeLen >= totalLen {
            break
        }
    }
    return nil
}
```

同读一样，`Write` 可能只写部分数据，需要循环直到写完。

---

## 调用链：一个上行消息的完整编解码流程

```
客户端发送:
SDK: Chat.Send(msg)
  → json.Marshal(msg) → UPMsg
  → proto.Marshal(&upMsg) → protobuf bytes
  → tcp.DataPgk{Len: len(bytes), Data: bytes}.Marshal()
  → TCP Write

服务端接收:
gateway: tcp.ReadData(conn)
  → readFixedData(conn, 4) → dataLen
  → readFixedData(conn, dataLen) → protobuf bytes
  → SendMsg RPC 传给 state
state: proto.Unmarshal(cmdCtx.Payload, &msgCmd) → 解析消息类型 → 路由到具体 handler
```

---

## 🔑 为什么不用 HTTP/WebSocket，而是自定义 TCP 协议？

| 因素 | HTTP/WS | 自定义 TCP |
|------|---------|-----------|
| 协议开销 | 每帧 HTTP 头 + WS 帧头 | 仅 4 字节长度 |
| 灵活性 | 受限于 WS 消息模型 | 完全自定义 |
| 实现难度 | 标准库支持 | 需自行处理拆包/粘包/超时 |
| IM 场景 | 消息量大时协议开销显著 | 协议开销最小化 |

长连接 IM 系统中，每条消息省几十字节的协议头，乘上亿级消息量，节省的带宽非常可观。

---

## 🔑 TCP 粘包/拆包问题

TCP 是面向字节流的，不会保留消息边界：
- **粘包**：发送方连续 Write 两个包，接收方一次 Read 读到了两个包的数据
- **拆包**：发送方 Write 一个大包，接收方一次 Read 只读到部分数据

**解决方案**：Length-Prefixed 协议。先读长度，再按长度读数据，两个包之间不会混淆。

---

## 🔑 Java 对比

```java
// Java 中类似做法
DataInputStream dis = new DataInputStream(socket.getInputStream());
int length = dis.readInt();       // 读长度（大端）
byte[] data = new byte[length];
dis.readFully(data);              // 读完指定字节数
```

Go 没有 `readFully`，所以需要自己写 `readFixedData` 循环。

---

## 学习目标

学完后能：看懂自定义 TCP 协议格式，理解 Length-Prefixed 编解码原理，解释 TCP 粘包/拆包及解决方案。

---

## 试一下（必做）

1. 创建 `test_tcp.go`，运行上面 Marshal 例子，确认输出二进制格式
2. **修改** `ReadData` 中的 `dataLenBuf` 长度从 4 改到 2，跑测试看报错是什么（理解定长头的重要性）
3. 在 `readFixedData` 中故意把 `_ = (*conn).SetReadDeadline(...)` 删掉，思考：什么情况下服务端资源会被耗尽？
4. 搜索项目中所有调用 `tcp.ReadData` 和 `tcp.SendData` 的地方，确认：为什么写入时也要循环？
