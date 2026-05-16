# 00: 学习环境准备

## 环境总览

| 组件 | 版本要求 | 用途 |
|------|----------|------|
| Go | **1.17+**（go.mod 指定，建议 1.19+） | 编译运行 |
| etcd | **3.5+** | 服务注册与发现 |
| Redis | **6.0+**（go-redis v9 需要 RESP3） | 路由表、登陆槽、消息缓存、Lua |
| MySQL | **5.7+ / 8.0** | 用户领域持久化 |
| Jaeger | **1.x**（OpenTelemetry exporter） | 链路追踪（可选） |
| Linux | — | epoll 仅 Linux 支持，macOS 需 Docker/VM |

## 1. 安装 Go

```bash
# macOS（推荐）
brew install go@1.21

# 或官网下载 https://go.dev/dl/
# 验证
go version  # go1.21.x darwin/arm64

# 配置 GOPROXY（国内加速）
go env -w GOPROXY=https://goproxy.cn,direct
```

**Go 环境变量**（`go env` 查看全部）：

| 变量 | 说明 |
|------|------|
| `GOROOT` | Go 安装目录 |
| `GOPATH` | 工作空间目录（默认 `~/go`） |
| `GO111MODULE` | 启用 Go Modules（1.17+ 默认 on） |
| `GOPROXY` | 模块代理 |

## 2. 安装 etcd

```bash
# macOS
brew install etcd

# 启动（开发模式，前台运行）
etcd

# 验证
etcdctl put foo bar
etcdctl get foo
# 默认监听 localhost:2379（client） + localhost:2380（peer）
```

## 3. 安装 Redis

```bash
# macOS
brew install redis

# 启动（前台运行）
redis-server

# 验证
redis-cli ping  # 返回 PONG
# 默认监听 localhost:6379
```

## 4. 安装 MySQL

```bash
# macOS
brew install mysql

# 启动服务
brew services start mysql

# 创建数据库和用户（对应 plato.yaml 中的 db_dns）
mysql -u root -e "
CREATE DATABASE IF NOT EXISTS plato;
CREATE USER IF NOT EXISTS 'user'@'127.0.0.1' IDENTIFIED BY 'password';
GRANT ALL PRIVILEGES ON plato.* TO 'user'@'127.0.0.1';
FLUSH PRIVILEGES;
"
```

## 5. 安装 Jaeger（可选，追踪用）

```bash
# Docker 方式——最简单
docker run -d --name jaeger \
  -p 14268:14268 \   # collector（项目上报端口）
  -p 16686:16686 \   # Web UI
  jaegertracing/all-in-one:latest

# 打开 http://localhost:16686 查看 trace
```

## 6. 克隆 + 编译项目

```bash
git clone https://github.com/hardcore-os/plato.git
cd plato
go mod download   # 下载依赖
go build -o plato .  # 编译
```

## 7. 修改配置文件

项目根目录 `plato.yaml` 中的关键配置核对：

```yaml
# etcd——确认端口
discovery:
  endpoints:
    - localhost:2379

# Redis——确认端口
cache:
  redis:
    endpoints:
    - 127.0.0.1:6379

# MySQL——确认 DSN
user_domain:
  db_dns: "user:password@tcp(127.0.0.1:3306)/plato?charset=utf8mb4&parseTime=True&loc=Local"
```

**注意：macOS 不支持 epoll，gateway 无法直接在 macOS 编译运行。推荐用 Docker。**

### 8a. Docker 方式（macOS 推荐）

```bash
# 在项目根目录启动 Linux 容器
docker run -it --rm \
  --name plato-dev \
  --network host \
  -v $(pwd):/plato \
  -w /plato \
  golang:1.21 bash

# 容器内编译运行
go build -o plato .
./plato gateway
```

etcd/Redis/MySQL 仍用本机 brew 启动的服务，`--network host` 让容器能访问宿主机端口。

### 8b. 直接启动（Linux）

推荐开 4 个终端分别启动：

```bash
# 终端 1: 启动 etcd（如未启动）
etcd

# 终端 2: 启动 Redis（如未启动）
redis-server

# 终端 3: 启动 gateway
cd /path/to/plato
./plato gateway

# 终端 4: 启动 state
cd /path/to/plato
./plato state
```

可选：

```bash
# 终端 5: ipconf
./plato ipconf

# 终端 6: 客户端（TUI 交互）
./plato client

# 终端 7: 用户领域服务
./plato user domain
```

## 9. 开发工具推荐

| 工具 | 用途 |
|------|------|
| **GoLand** / **VS Code + Go 插件** | IDE |
| `gopls` | Go 语言服务器（自动补全、跳转、重构） |
| `go test -v` | 运行测试 |
| `go vet ./...` | 静态分析 |
| `etcdctl` | etcd 命令行 |
| `redis-cli` | Redis 命令行 |
| **Another Redis Desktop Manager** | Redis GUI |
| **Jaeger UI** (localhost:16686) | Trace 查看 |
| `curl localhost:6789/ip/list` | 测试 ipconf API |

## 10. 快速验证环境

```bash
# 1. etcd 可用
etcdctl endpoint health
# 输出: localhost:2379 is healthy

# 2. Redis 可用
redis-cli ping
# 输出: PONG

# 3. MySQL 可用
mysql -u user -ppassword -h 127.0.0.1 plato -e "SELECT 1"
# 输出: 1

# 4. 项目编译成功
cd /path/to/plato
go build -o plato . && echo "build ok"

# 5. 运行测试
go test ./common/timingwheel/ -v
# 输出: ok ... (cached)
```

## 🔑 Go 模块管理基础

```bash
go mod download    # 下载 go.mod 中声明的所有依赖到本地缓存
go mod tidy        # 清理 go.mod（移除未使用的，添加缺失的）
go mod vendor      # 将依赖复制到 vendor/ 目录（离线构建）
go get pkg@v1.2.3  # 添加/升级依赖
```

plato 项目使用 Go Modules（`go.mod` + `go.sum`），不需要 `GOPATH`，任意目录都可编译。

## 🔑 如果不想装 MySQL

user domain 的测试使用 SQLite 内存数据库（`domain/user/storage/storage.go:46`），跑测试不需要 MySQL：

```bash
go test ./domain/user/ -v
```

如果只学 gateway + state + ipconf，完全不需要 MySQL。
