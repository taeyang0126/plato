# Phase 2-3: Redis 操作 + Lua 脚本原子操作

## 一、Redis Client 初始化

`common/cache/redis.go:15-26`：

```go
var rdb *redis.Client  // 全局单例

func InitRedis(ctx context.Context) {
    if rdb != nil { return }  // 幂等初始化
    endpoints := config.GetCacheRedisEndpointList()
    opt := &redis.Options{
        Addr:     endpoints[0],     // 只取第一个 endpoint（当前不支持集群）
        PoolSize: 10000,            // 连接池大小
    }
    rdb = redis.NewClient(opt)
    if _, err := rdb.Ping(ctx).Result(); err != nil {
        panic(err)  // 启动时连不上 Redis 直接 panic，fail-fast
    }
    initLuaScript(ctx)  // 预加载 Lua 脚本
}
```

**连接池 10000**：每个请求可能涉及多次 Redis 操作（路由表查询 + 消息缓存 + clientID CAS），高并发下需要大连接池。

---

## 二、封装层设计

`common/cache/redis.go` 封装了 go-redis 的调用细节：

```go
// 每层封装做三件事：
//   1. 检查返回值是否为 nil
//   2. 处理 redis.Nil (key 不存在——不是错误)
//   3. 提取数据并返回

func GetBytes(ctx context.Context, key string) ([]byte, error) {
    cmd := rdb.Conn().Get(ctx, key)
    if cmd == nil { return nil, errors.New("redis GetBytes cmd is nil") }
    data, err := cmd.Bytes()
    if redis.Nil == err { return nil, nil }  // key 不存在，返回 nil, nil
    return data, err
}
```

### 各封装函数一览

| 函数 | Redis 命令 | 用途 |
|------|-----------|------|
| `GetBytes/SetBytes` | GET/SET | 路由表、lastMsg |
| `GetUInt64` | GET | max_client_id |
| `Del` | DEL | 登出清理 |
| `SADD/SREM/SmembersStrSlice` | SADD/SREM/SMEMBERS | 登陆槽(Set) |
| `Incr` | INCR + EXPIRE (pipeline) | clientID 自增 |
| `SetString/GetString` | SET/GET | 路由记录 |
| `RunLuaInt` | EVALSHA | CAS clientID |
| `GetKeys` | KEYS | 登出时清理 max_client_id 相关 key |

---

## 三、Key 命名规范

`common/cache/cosnt.go:5-9`（注意文件名拼写是 `cosnt.go`）：

```go
const (
    MaxClientIDKey  = "max_client_id_{%d}_%d_%s"     // 上行消息去重
    LastMsgKey      = "last_msg_{%d}_%d"             // 下行消息可靠性
    LoginSlotSetKey = "login_slot_set_{%d}"          // 登陆槽，hash tag 保证 cluster 模式下同 shard
    TTL7D           = 7 * 24 * time.Hour             // 7 天过期
)
```

**Hash Tag 注释**：`login_slot_set_{%d}` 中的 `{}` 是 Redis Cluster 的 hash tag 语法——只有 `{}` 内的部分参与 hash 计算，保证同一 slot 的 key 落在同一个 shard。

使用 fmt.Sprintf 填充：

```go
slotKey := fmt.Sprintf(cache.LoginSlotSetKey, slot)
// → "login_slot_set_500"
```

---

## 四、Lua 脚本：CAS 自增 clientID

### 为什么需要 Lua？

上行消息可靠性需要**原子地**比较 clientID 并自增。如果分两步做：

```
1. GET key → 比较 oldClientID
2. INCR key → 自增
```

两步之间有并发问题：另一个请求可能同时通过检查，导致重复消息。

用 Lua 脚本在 Redis 服务端原子执行，解决竞态。

### Lua 脚本定义

`common/cache/lua.go:17-21`：

```lua
-- LuaCompareAndIncrClientID
if redis.call('exists', KEYS[1]) == 0 then
    redis.call('set', KEYS[1], 0)  -- key 不存在则初始化为 0
end
if redis.call('get', KEYS[1]) == ARGV[1] then  -- 比较 oldClientID
    redis.call('incr', KEYS[1])                -- 相等则自增
    redis.call('expire', KEYS[1], ARGV[2])     -- 更新 TTL
    return 1                                   -- 成功
else
    return -1                                   -- 失败（重复消息）
end
```

### 脚本预加载

`common/cache/lua.go:24-35`：

```go
func initLuaScript(ctx context.Context) {
    for name, part := range luaScriptTable {
        cmd := rdb.ScriptLoad(ctx, part.LuaScript)  // SCRIPT LOAD 上传脚本
        part.Sha = cmd.Val()                         // 缓存 SHA1
    }
}
```

### 调用处

`state/cache.go:152-165`：

```go
func (cs *cacheState) compareAndIncrClientID(ctx context.Context, connID, oldMaxClientID uint64, sessionId string) bool {
    slot := cs.getConnStateSlot(connID)
    key := fmt.Sprintf(cache.MaxClientIDKey, slot, connID, sessionId)
    res, err := cache.RunLuaInt(ctx, cache.LuaCompareAndIncrClientID,
        []string{key},           // KEYS
        oldMaxClientID,          // ARGV[1]
        cache.TTL7D,             // ARGV[2]
    )
    return res > 0
}
```

`RunLuaInt` 用 `EVALSHA` 执行（只需要传 SHA1，节省带宽），而非 `EVAL`（传完整脚本）。

---

## 五、路由表存储设计

`common/router/table.go:27-51`：

```go
// 路由表 key: gateway_rotuer_{deviceID}
// 路由表 value: "{gateway_endpoint}-{connID}"
// 例如: "127.0.0.1:8901-12345678"

func AddRecord(ctx context.Context, did uint64, endpoint string, conndID uint64) error {
    key := fmt.Sprintf(gatewayRotuerKey, did)
    value := fmt.Sprintf("%s-%d", endpoint, conndID)
    return cache.SetString(ctx, key, value, ttl7D*time.Second)
}

func QueryRecord(ctx context.Context, did uint64) (*Record, error) {
    key := fmt.Sprintf(gatewayRotuerKey, did)
    data, err := cache.GetString(ctx, key)
    ec := strings.Split(data, "-")
    conndID, _ := strconv.ParseUint(ec[1], 10, 64)
    return &Record{Endpoint: ec[0], ConndID: conndID}, nil
}
```

**作用**：state 根据 deviceID 查询路由，找到该设备连接在哪个 gateway 的哪个 connID，然后精准下发消息。

---

## 🔑 go-redis 的 PoolSize 调优

go-redis 连接池模式类似数据库连接池。`PoolSize: 10000` 意味着最多 10000 个并发连接。

调优经验：
- `go-redis` 是并发安全的，不需要在客户端侧加锁
- 连接池耗尽时，`Ping/Get/Set` 等调用会阻塞等待
- 如果预期 QPS 高，`PoolSize` 至少设为 `QPS * 平均延迟(秒) * 2`

---

## 🔑 Lua 脚本的 SHAs 缓存

`EVALSHA` 比 `EVAL` 省带宽（SHA1 是 40 字节，脚本可能几 KB），但需要先 `SCRIPT LOAD`。
如果 Redis 重启 SHA 会丢失，此时 `EVALSHA` 返回 NOSCRIPT 错误。生产环境应在 `RunLuaInt` 中检测 NOSCRIPT 并 fallback 到 `EVAL`。

**当前项目的处理**：启动时 `initLuaScript` 加载，假设 Redis 不重启。如果重启则 panic。

---

## 学习目标

学完后能：使用 go-redis 进行常见操作，解释 Lua CAS 如何解决分布式并发竞态，理解 EVALSHA vs EVAL 的区别，画出 Redis 中 4 种数据结构的读写路径。

---

## 试一下

1. `redis-cli monitor` 监控所有 Redis 命令，然后启动 state + gateway + client，发一条消息，观察 monitor 输出的完整命令序列
2. **修改**：在 Lua CAS 脚本中，把 `return 1` 改为 `return 0`，发消息后观察 `upMsgHandler` 中的 `compareAndIncrClientID` 返回值变化，消息是否还能正常处理？
3. 模拟 Redis 重启场景：启动 state 后，`redis-cli SCRIPT FLUSH` 清除缓存，然后发一条消息，观察 `RunLuaInt` 是否 panic（验证 NOSCRIPT fallback 问题）
4. 画表：列出 `cosnt.go` 中 4 个 key 模板 → `state/cache.go` 中的填充调用 → 最终的 Redis key 示例
