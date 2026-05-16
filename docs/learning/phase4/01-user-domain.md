# Phase 4-1: 用户领域服务（domain/user）

## 模块职责

用户领域服务采用 DDD 分层架构，提供用户 CRUD。特点：
- **二级缓存**：本地内存缓存 + Redis 远程缓存
- **Cache-Aside 模式**：写 DB 后删缓存
- **领域事件**：数据变更时发送事件（预留 Kafka 集成）

## 启动流程

`domain/user/server.go:14-33`：

```go
func RunMain(path string) {
    ctx := context.TODO()
    config.Init(path)
    client.Init()       // 空实现（预留调用其他服务）
    service.Init(false) // 初始化存储层
    s := prpc.NewPServer(...)
    s.RegisterService(func(server *grpc.Server) {
        service.RegisterUserServer(server, &service.Service{})
    })
    s.Start(ctx)
}
```

---

## 分层架构

```
service.Service (gRPC handler)   ← 接口层：薄转发，参数转换
    │
storage.StorageManager            ← 存储层：缓存+DB+事件聚合
    ├── cache.Manager             ← 二级缓存管理
    ├── gorm.DB                   ← MySQL 持久化
    └── event.Manager             ← 领域事件

UserDAO (GORM model)             ← 数据模型
UserDTO (protobuf)               ← 传输对象
```

## 存储层：StorageManager

### 初始化

`domain/user/storage/storage.go:27-56`：

```go
func NewStorageManager(isTest bool) *StorageManager {
    if isTest {
        return newMockStorageManager()  // SQLite 内存数据库
    }
    cacheOpt := []*cache.Options{
        {Mode: cache.Local},   // 第1级：本地内存缓存
        {Mode: cache.Remote},  // 第2级：Redis 远程缓存
    }
    channelOpt := map[event.Channel]*event.Options{
        event.UserEvent: {},   // 领域事件 channel
    }
    sm := &StorageManager{
        cm:    cache.NewManager(cacheOpt),
        event: event.NewManager(channelOpt),
    }
    sm.db, _ = gorm.Open(mysql.Open(config.GetDomainUserDBDNS()), &gorm.Config{})
    return sm
}
```

### QueryUsers — 查询（内存 → Redis → MySQL）

`domain/user/storage/storage.go:59-98`：

```go
func (s *StorageManager) QueryUsers(ctx context.Context, querys map[uint64]*Options) map[uint64]*user.UserDTO {
    res := make(map[uint64]*user.UserDTO, len(querys))
    miss := make([]uint64, 0)

    // Step 1: 查二级缓存（本地 + Redis，由 cache.Manager 自动路由）
    keys := make([]string, 0, len(querys))
    for uid := range querys {
        keys = append(keys, fmt.Sprintf(userDomainCacheKey, uid))
    }
    cacheTable := s.cm.MGet(keys)  // 批量查缓存

    // Step 2: 缓存命中 → 直接返回；miss → 记录 miss 列表
    for uid := range querys {
        key := fmt.Sprintf(userDomainCacheKey, uid)
        if data, ok := cacheTable[key]; ok {
            userDTO, _ := data.(*user.UserDTO)
            res[uid] = userDTO
        } else {
            miss = append(miss, uid)
        }
    }

    if len(miss) == 0 { return res }

    // Step 3: 查 DB（批量）
    var users []UserDAO
    s.db.WithContext(ctx).Where("user_id IN (?)", miss).Find(&users)

    // Step 4: DB 结果写入 DTO + 回写缓存
    for _, userDAO := range users {
        userDTO := convertUserDAOToDTO(&userDAO)
        res[userDAO.UserID] = userDTO
        key := fmt.Sprintf(userDomainCacheKey, userDAO.UserID)
        missUsers[key] = userDTO
    }
    s.cm.MSet(missUsers)  // 批量回写

    // Step 5: 发送领域事件
    s.event.Send(event.UserEvent, nil)
    return res
}
```

**查询策略**：
1. 先查缓存（本地内存 → Redis），批量 MGet
2. 缓存未命中的 uid 批量查 DB（`WHERE user_id IN (?)`，一次查询）
3. 查到的结果批量回写缓存
4. 发送领域事件（触发后续异步处理）

### CreateUsers — 写 DB + 删缓存

`domain/user/storage/storage.go:100-122`：

```go
func (s *StorageManager) CreateUsers(ctx context.Context, users []*user.UserDTO, opt *Options) error {
    // Step 1: DTO → DAO 转换
    userDAOList := make([]UserDAO, 0, len(users))
    keys := make([]string, 0, len(users))
    for _, userDTO := range users {
        userDAO := convertUserDTOToDAO(userDTO)
        userDAOList = append(userDAOList, *userDAO)
        key := fmt.Sprintf(userDomainCacheKey, userDAO.UserID)
        keys = append(keys, key)
    }

    // Step 2: 批量写 DB
    ret := s.db.Create(&userDAOList)

    // Step 3: 删除缓存（Cache-Aside）
    s.cm.MDel(keys)

    // Step 4: 发送领域事件
    s.event.Send(event.UserEvent, nil)
    return nil
}
```

**为什么删缓存而非更新？** — Cache-Aside 模式：
- 更新缓存在并发下可能造成数据不一致（A 更新 DB → B 更新 DB → B 更新缓存 → A 更新缓存，A 的旧值覆盖）
- 删除缓存更安全：下次查询时从 DB 加载最新值

### batchUpdateUsers — 事务批量更新

`domain/user/storage/storage.go:209-224`：

```go
func (sm *StorageManager) batchUpdateUsers(users []UserDAO) error {
    tx := sm.db.Begin()  // 开启事务
    defer tx.Commit()
    for _, user := range users {
        result := tx.Model(&UserDAO{}).Where("user_id = ?", user.UserID).Updates(user)
        if result.Error != nil {
            tx.Rollback()
            return result.Error
        }
    }
    return nil
}
```

> ⚠️ TODO: 逐条 UPDATE 性能差，应改为批量 UPDATE。

---

## gRPC Service 层

`domain/user/rpc/service/service.go:18-41`：

```go
type Service struct{}

func (s *Service) QueryUsers(ctx context.Context, req *QueryUsersRequest) (*QueryUsersResponse, error) {
    userOpts := make(map[uint64]*storage.Options)
    for uid, opt := range req.Opts {
        userOpts[uid] = &storage.Options{AllDevice: opt.ActiveDevice}
    }
    users := sm.QueryUsers(ctx, userOpts)
    return &QueryUsersResponse{Users: users}, nil
}
```

Service 层只做**参数转换**（protobuf → storage 内部类型），业务逻辑在 StorageManager 中。

---

## DAO ↔ DTO 转换

`domain/user/storage/storage.go:146-206`：

```go
func convertUserDAOToDTO(dao *UserDAO) *user.UserDTO {
    return &user.UserDTO{
        UserID: dao.UserID,
        Setting: &user.SettingDTO{FontSize: dao.FontSize, DarkMode: dao.DarkMode, ...},
        Information: &user.InformationDTO{Nickname: dao.Nickname, Avatar: dao.Avatar, ...},
        Pprofile: &user.ProfileDTO{Location: dao.Location, Age: int32(dao.Age), ...},
    }
}
```

**为什么需要转换？**
- DAO（Data Access Object）：与 DB 表结构对应，可自由加减字段
- DTO（Data Transfer Object）：protobuf 生成，API 契约，变更需谨慎
- 中间层隔离：DB schema 变更不影响 API

---

## 测试模式

`domain/user/storage/storage.go:42-56`：

```go
func newMockStorageManager() *StorageManager {
    sm := &StorageManager{...}
    sm.db, _ = gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})  // SQLite 内存
    sm.db.AutoMigrate(&UserDAO{})  // 自动建表
    return sm
}
```

`domain/user/server_test.go` 测试时用 SQLite :memory: 替代 MySQL，无需外部依赖。这是 Go 测试的标准做法。

---

## 🔑 GORM 基础

```go
// 查询
db.Where("user_id IN (?)", []uint64{1,2,3}).Find(&users)
// 批量插入
db.Create(&userList)
// 更新（零值不会更新，需要用 map 或 Select）
db.Model(&UserDAO{}).Where("user_id = ?", id).Updates(user)
// 事务
tx := db.Begin()
// ...
tx.Commit() // 或 tx.Rollback()
```

`Updates` 陷阱：struct 的零值字段不会被更新。如果要把某个字段设为 `0` 或 `""`，需要用 `Select` 或 `map[string]interface{}`。

---

## 🔑 DDD 分层在本项目中的体现

```
domain/user/
├── server.go          # Application 层：编排流程
├── rpc/service/       # Interface 层：gRPC 适配
├── storage/           # Infrastructure 层：持久化
│   ├── storage.go     # Repository 实现
│   └── dao.go         # 数据模型
└── (无 domain 包)     # 纯业务逻辑暂未抽离，TODO

理想 DDD：
domain/user/domain/    # 实体、值对象、领域服务（操作实体）
domain/user/app/       # 应用服务（编排）
domain/user/infra/     # 基础设施实现
domain/user/interface/ # gRPC/HTTP 适配
```

当前项目尚未严格分层，业务逻辑(如数据校验)混在 storage 和 service 之间。

---

## 学习目标

学完后能：理解 DDD 分层在 Go 项目中的落地方式，解释 Cache-Aside 模式的一致性和并发安全性，使用 GORM 进行基本 CRUD，知道 Updates 零值陷阱。

---

## 试一下

1. `go test -v ./domain/user/` 跑测试，观察 SQLite :memory: 如何替代 MySQL
2. **修改**：在 `storage.go:100-122` 的 `CreateUsers` 中，把删缓存改为更新缓存，然后写一个并发测试：同时 Create 和 Query，观察是否出现脏读
3. 在 `batchUpdateUsers` 中，把 `tx.Model(&UserDAO{}).Where(...).Updates(user)` 的 struct 传参改为 `map[string]interface{}` 传参，验证零值字段是否被正确更新
4. 补齐 `common/cache/redis.go:129-147` 中 `redisCache` 的 MGet/MSet/MDel（当前是空实现），让 user domain 的二级缓存完整跑通
