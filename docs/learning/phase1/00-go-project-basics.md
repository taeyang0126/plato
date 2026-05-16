# Phase 1-0: Go 项目基础（给 Go 新手）

如果你只写过 Java/Python，以下内容读完再进 Phase 1。

## Go 项目结构约定

Go 社区有一套非官方但广泛采用的布局约定（`golang-standards/project-layout`）。plato 遵循了其中大部分：

```
plato/
├── main.go              # 程序入口（极少代码，通常只调 cmd.Execute()）
├── cmd/                 # 子命令（每个子命令对应一个可执行入口）
│   ├── gateway.go       #   ./plato gateway
│   ├── state.go         #   ./plato state
│   └── ...
├── gateway/             # gateway 模块（包名 = 目录名 = gateway）
├── state/               # state 模块
├── ipconf/              # ipconf 模块
├── domain/user/         # 用户领域模块
├── common/              # 公共库（非可执行，只被 import）
│   ├── config/
│   ├── cache/
│   └── ...
└── go.mod               # 模块定义文件
```

**核心规则**：
- **目录名 = 包名**：`gateway/` 下所有 `.go` 文件第一行 `package gateway`
- **`main` 包**：`package main` + `func main()` = 可执行程序。只有一个。
- **`cmd/` 不是 `main` 包**：`cmd/` 下是 `package cmd`，被 `main.go` import。用 cobra 框架管理子命令。

## import 路径

```go
// go.mod: module github.com/hardcore-os/plato

// 导入本项目内的包（以 module 路径为根）
import "github.com/hardcore-os/plato/gateway"       // → gateway/ 目录
import "github.com/hardcore-os/plato/common/config"  // → common/config/ 目录

// 导入第三方包
import "github.com/spf13/cobra"                       // → 从 GOPATH/pkg/mod/ 或 vendor/ 加载
```

Go 没有相对路径 import。所有 import 路径从 module 根开始。

## 包和文件的关系

**Java：** 一个类一个文件，package 声明命名空间
**Go：** 一个包 = 一个目录，目录下所有 `.go` 文件都属于同一个包

```
gateway/
├── server.go       // package gateway
├── epoll.go        // package gateway  ← 同一个包，文件间自由引用，不需要 import
├── connection.go   // package gateway
└── rpc/            // 子目录 = 子包
    └── service/
        └── service.go  // package service  ← 不同包，需要 import
```

**关键区别**：同一个包内，所有函数/变量/类型相互可见（不需要 `public`）。首字母大写 = 对外可见（exported），小写 = 包内可见。

## `go.mod` 和 `go.sum`

```go
// go.mod
module github.com/hardcore-os/plato  // 模块路径（全局唯一标识）
go 1.17                              // Go 最低版本
require (
    github.com/spf13/viper v1.12.0   // 直接依赖
    google.golang.org/grpc v1.48.0
)
```

- `go.mod` = Maven 的 `pom.xml` / npm 的 `package.json`（但更简洁）
- `go.sum` = 依赖校验和（自动生成，勿手动编辑）
- `go mod tidy` = 自动清理未使用的依赖 / 添加缺失的依赖
- 所有依赖下载到 `$GOPATH/pkg/mod/`（全局缓存），非项目本地

## 基础命令

```bash
go build .               # 编译当前目录
go build -o plato .      # 编译并指定输出文件名
go run main.go           # 编译+运行（开发用，不生成二进制）
go test ./...            # 递归跑所有测试
go test ./gateway/ -v    # 跑指定包测试，-v 显示详细输出
go vet ./...             # 静态分析（检查可疑代码）
gofmt -w .               # 格式化所有代码（Go 有统一风格，无需争论）
go doc strings.HasPrefix # 查看标准库函数文档
go mod tidy              # 整理依赖
```

**注意**：
- `go build` 不会生成 `.class` 之类的中间文件，直接生成一个静态链接的二进制
- Go 编译速度极快（没有头文件、没有虚拟机），百万行代码也能秒级编译
- 没有 `clean` 命令——不需要，没有中间产物

## Go 编译产物的特殊性

plato 编译出来是一个**单一可执行文件** `plato`，包含了所有依赖（静态链接）：

```bash
go build -o plato .
./plato gateway    # 同一个二进制，不同子命令
./plato state      # 不同子命令 = 不同的启动流程
```

不需要 JVM、不需要 `.so` 动态库、不需要 Docker 来管理依赖。复制 `plato` 到另一台同架构 Linux 机器直接运行。

## 看的顺序

如果 Go 完全不熟，学习顺序：

1. 先看本文件 → 理解项目结构和基础命令
2. 再看 `phase1/01-goroutine-channel.md` → goroutine/channel
3. 然后按 README 顺序走

## 学习目标

学完后能：独立创建 Go 项目、理解包和目录的对应关系、正确 import、使用 `go build/test/vet/fmt` 命令。

---

## 什么时候需要查标准库文档

Go 标准库文档极好，`go doc` 或在浏览器访问 `pkg.go.dev/std` 即可。遇到不认识的包名时：

| import 路径 | 是什么 |
|-------------|--------|
| `fmt` | 格式化输出（`Println`, `Sprintf`） |
| `net` | 网络（`ListenTCP`, `Dial`） |
| `sync` | 并发同步（`Mutex`, `Map`, `WaitGroup`） |
| `context` | 上下文传递（超时、取消、传值） |
| `errors` | 错误处理（`New`, `Is`, `As`） |
| `encoding/json` | JSON 序列化 |
| `encoding/binary` | 二进制数据读写 |
| `io` | IO 接口（`Reader`, `Writer`） |
| `time` | 时间处理 |
| `reflect` | 反射（非必要勿用） |
| `unsafe` | 绕过类型系统（极其危险，仅性能关键路径使用） |

## 试一下

1. `go build -o plato .` 编译项目，用 `ls -lh plato` 看二进制大小
2. `go vet ./...` 跑静态检查，看有没有输出警告
3. `go doc github.com/hardcore-os/plato/gateway` 看自动生成的包文档
4. 在 `main.go` 中故意把 `import "github.com/hardcore-os/plato/cmd"` 写错一个字母，看 go build 报什么错（理解 import 路径是 how Go finds packages）
