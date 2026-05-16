# plato 学习路线图

目的：从 Go 并发基础到完整 IM 系统架构，逐模块深入。每个文件聚焦单一主题，源码行号 + 例子 + 最佳实践。

## 开始之前

**[00-environment.md](00-environment.md)** — 环境准备：Go/etcd/Redis/MySQL 安装 + 启动 + 验证

## 使用方式

1. **按阶段顺序学**——后面阶段依赖前面的 Go 概念
2. **每文件读源码 + 跑测试**——文件里标了每个关键函数的文件:行号
3. **每阶段末尾有 Checkpoint**——自查是否理解到位
4. **不要跳**——phase2 基础设施是 phase3 核心服务的基础

---

## Phase 1: Go 并发核心概念

**目标**：掌握 goroutine/channel/sync.Map/atomic/context 并发基石，用项目代码验证。

**如果你完全不熟 Go：从 [00-go-project-basics.md](phase1/00-go-project-basics.md) 开始。**

| # | 文件 | 主题 | 涉及源码 |
|---|------|------|----------|
| 0 | [00-go-project-basics.md](phase1/00-go-project-basics.md) | Go 项目结构/包/import/go.mod/常用命令 | — |
| 1 | [01-goroutine-channel.md](phase1/01-goroutine-channel.md) | goroutine + channel + WaitGroup | gateway/rpc/service/, state/rpc/service/, timingwheel |
| 2 | [02-sync-map.md](phase1/02-sync-map.md) | sync.Map 并发安全 map + sync.Mutex/RWMutex 对比 | gateway/epoll.go, state/cache.go |
| 3 | [03-atomic-pointer.md](phase1/03-atomic-pointer.md) | atomic + unsafe.Pointer 无锁编程 | ipconf/domain/endport.go, gateway/epoll.go |
| 4 | [04-go-idioms.md](phase1/04-go-idioms.md) | Go 惯用法：defer/error/init/Option/接口 | 贯穿全项目 |
| 5 | [05-context.md](phase1/05-context.md) | context.Context：超时/取消/传值 | 贯穿全项目 |
| 6 | [06-go-traps.md](phase1/06-go-traps.md) | Go 常见陷阱（nil interface/slice/for range/defer 等共 12 条） | 标注了项目中是否出现 |

**Checkpoint**：能解释 `gateway/rpc/service/service.go` 为什么 gRPC handler 只写 channel 就返回？那个 `context.TODO()` 有什么问题？

---

## Phase 2: 基础设施层（自底向上）

**目标**：理解每个 common 包的职责和实现原理，这是理解服务层的前提。

| # | 文件 | 主题 | 涉及源码 |
|---|------|------|----------|
| 1 | [01-tcp-protocol.md](phase2/01-tcp-protocol.md) | 自定义 TCP 协议编解码 | common/tcp/coder.go, read.go, write.go |
| 2 | [02-config-discovery.md](phase2/02-config-discovery.md) | 配置管理 + etcd 服务发现 | common/config/, common/discovery/ |
| 3 | [03-redis-lua.md](phase2/03-redis-lua.md) | Redis 操作 + Lua 脚本原子操作 | common/cache/redis.go, lua.go, cosnt.go, router/table.go |
| 4 | [04-timing-wheel.md](phase2/04-timing-wheel.md) | 层级时间轮实现 | common/timingwheel/ |
| 5 | [05-prpc-framework.md](phase2/05-prpc-framework.md) | gRPC 封装框架：Option 模式/拦截器/服务注册 | common/prpc/ |

**Checkpoint**：能画出 prpc 从 NewPServer → Start → etcd 注册的完整调用链。

---

## Phase 3: 核心服务

**目标**：逐服务理解架构设计和关键实现。

| # | 文件 | 主题 | 涉及源码 |
|---|------|------|----------|
| 1 | [01-ipconf.md](phase3/01-ipconf.md) | IP 调度服务：etcd watch + 滑动窗口 + 评分排序 | ipconf/ |
| 2 | [02-gateway.md](phase3/02-gateway.md) | TCP 接入层：epoll 事件循环 + 协程池 + 连接管理 | gateway/ |
| 3 | [03-state.md](phase3/03-state.md) | 状态管理：消息路由 + 可靠性协议 + 状态机 | state/ |

**Checkpoint**：能画出消息从客户端 TCP → gateway → state → gateway → 客户端的完整流转，包括异常路径。

---

## Phase 4: 周边模块

| # | 文件 | 主题 | 涉及源码 |
|---|------|------|----------|
| 1 | [01-user-domain.md](phase4/01-user-domain.md) | 用户领域服务：DDD + GORM 二级缓存 | domain/user/ |
| 2 | [02-sdk-client.md](phase4/02-sdk-client.md) | 客户端 SDK + TUI | common/sdk/, client/ |

**Checkpoint**：能解释 SDK 的 login → heartbeat → send → recv → reConn 完整状态机。

---

## Phase 5: 串联

| # | 文件 | 主题 |
|---|------|------|
| 1 | [01-full-link.md](phase5/01-full-link.md) | 完整链路：登陆 → 心跳 → 上行消息 → 下行消息 → ACK → 重推 → 断连 → 重连 |

---

## 学习方法

这套资料遵循以下学习设计原则（参考 deliberate practice + cognitive load theory）：

### 每文件的结构

1. **概念速览**（2-3 句话）→ 建立心理模型
2. **源码走读**（精确到行号）→ 在真实代码中验证概念
3. **最佳实践**（🔑 标记）→ 提取可迁移的模式
4. **试一下**（可执行的操作）→ 主动回忆而非被动阅读

### 怎样学效果最好

1. **不要只看不写**：每读完一个文件，打开对应的源码，修改一个小地方再跑测试看效果
2. **画图**：数据流（channel 传递路径）、状态机（connState 状态转换）、时间线（goroutine 生命周期）
3. **向自己解释**：学完每个 Phase 后，关上文件，用中文向自己解释这个模块做了什么、为什么这样设计
4. **对照列表**：Go traps 文件列了 12 个常见坑，每踩一个回来打勾
5. **不需要死记 API**：`go doc` 随时查，关键是理解设计模式和数据流

### 推荐节奏

每天 1-2 个文件，跑一遍对应的 `go test`，改动一点源码看效果。全部 20 个文件约 2 周完成。

---

## 配套命令

```bash
# 随时跑测试验证理解
go test -v ./common/timingwheel/  # Phase 2-4
go test -v ./common/discovery/    # Phase 2-2
go test -v ./domain/user/         # Phase 4-1

# 构建并看效果
go build -o plato .
./plato --help

# 查看文档
go doc sync.Map
go doc context.WithTimeout
```
