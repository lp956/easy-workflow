# easy-workflow

`easy-workflow` 是可嵌入 Go 应用的人工审批流内核。它负责 Definition 校验与发布、Instance 状态流转、Task、乐观并发和不可变 Audit；业务节点、数据库、组织目录以及交互界面通过显式依赖组合。

## 分层与依赖

| 层 | Go package | 职责 | 依赖与边界 |
| --- | --- | --- | --- |
| core | `github.com/lvpeng/easy-workflow` | canonical Definition、Builder、发布、Engine、`Store` 契约、`MemoryStore` | 只使用 Go 标准库；导入时不读配置、不连接网络、不启动 goroutine |
| 官方 extension | `.../approval`、`.../condition` | 人工审批与受限 JSON 条件路由 | 只依赖 core；handler 必须显式注册 |
| durable adapter | `.../postgres`、`.../mysql` | PostgreSQL command Store、独立查询投影和 MySQL command Store | 数据库连接、迁移和 pool 生命周期由宿主负责；adapter 依赖按 package 显式选择 |
| 可选 transport / Web integration | 由宿主应用实现 | HTTP、鉴权、DTO、Web UI、设计器和 Definition JSON 传输 | 不属于 core，也没有隐式默认实现 |

组织目录同样不属于 core。`approval.OrganizationAdapter` 只是宿主可选实现的边界；静态 assignee 不需要目录。待办、已办和搜索属于 adapter 查询投影，不会扩张 command-side `workflow.Store`。

## 项目入口、注册点与流程创建位置

先给出最常用的定位结论：这个仓库是一个 **Go library**，不是可独立运行的服务，因此仓库内没有 `main.go`、HTTP server 或自动执行的启动函数。宿主应用需要在自己的 `main.go`、依赖注入模块或服务启动模块中显式装配它。

| 要找的能力 | 文件 | 关键类型或函数 | 说明 |
| --- | --- | --- | --- |
| 宿主装配示例 | [`example_test.go`](example_test.go) | `Example`、`ExampleDefinitionPublisher_versions` | 最接近完整应用入口的可执行示例，串起注册、Engine 创建、Definition 创建、启动和命令处理 |
| Engine 创建 | [`engine.go`](engine.go) | `NewEngine` | 把 `Store` 和 `Registry` 注入 Engine；构造本身不连接数据库、不启动 goroutine |
| 流程实例启动 | [`engine.go`](engine.go) | `Engine.Start`、`Engine.StartPublished` | `Start` 使用调用方提供的 Definition；`StartPublished` 先读取指定的不可变版本，再进入 `Start` |
| handler 注册中心 | [`handler.go`](handler.go) | `Registry`、`NewRegistry`、`Registry.Register` | `kind -> NodeHandler` 的唯一运行时映射；没有全局注册，也不会在 import 时自动注册 |
| Approval 注册实现 | [`approval/approval.go`](approval/approval.go) | `approval.Kind`、`approval.NewHandler`、`approval.NewHandlerWithOrganization` | 宿主创建 Approval handler 后，调用 `registry.Register(approval.Kind, handler)` |
| Condition 注册实现 | [`condition/condition.go`](condition/condition.go) | `condition.Kind`、`condition.NewHandler` | 宿主创建 Condition handler 后，调用 `registry.Register(condition.Kind, handler)` |
| 代码方式创建流程 | [`definition.go`](definition.go) | `NewBuilder`、`Start`、`Node`、`End`、`Connect`、`Build` | 构造 canonical `Definition`；`Build` 负责结构校验，但不持久化、不启动实例 |
| JSON 方式创建流程 | [`definition.go`](definition.go) | `ParseDefinition` | 将设计器或 Web 传入的 JSON 解码为同一个 canonical `Definition` 并做结构校验 |
| Definition 完整编译 | [`compiler.go`](compiler.go) | `CompileDefinition`、`compileDefinition` | 校验图、路由、节点 kind 是否已注册以及节点配置；生成的执行计划只在当前请求内使用 |
| Definition 发布 | [`publication.go`](publication.go) | `DefinitionPublisher`、`Publish`、`PublishJSON` | 在持久化之前完整编译，并通过 `DefinitionVersionWriter` 分配严格递增的不可变版本 |
| Instance 持久化接口 | [`store.go`](store.go) | `Store`、`MemoryStore` | Engine 的 command-side 存储端口；`Create` 创建实例，`Load` 读取快照，`Save` 做乐观并发 CAS |
| PostgreSQL 持久化实现 | [`postgres/store.go`](postgres/store.go) | `postgres.New`、`Store.Create`、`Store.Load`、`Store.Save` | 使用宿主传入的 `pgxpool.Pool` 持久化完整 Instance 聚合 |
| MySQL 持久化实现 | [`mysql/store.go`](mysql/store.go) | `mysql.New`、`Store.Create`、`Store.Load`、`Store.Save` | 使用宿主传入的 `*sql.DB` 持久化完整 Instance 聚合 |

需要特别区分三个“创建”动作：

1. `workflow.NewBuilder(...)` 创建的是 **流程定义 Definition**，位置在 [`definition.go`](definition.go)。
2. `DefinitionPublisher.Publish(...)` 创建的是 **Definition 的不可变发布版本**，位置在 [`publication.go`](publication.go)。
3. `engine.Start(...)` 创建的是 **一次实际运行的流程实例 Instance**，编排位置在 [`engine.go`](engine.go)，最终通过 [`store.go`](store.go) 的 `Store.Create` 落库。

## 完整文件结构

下面按运行职责列出仓库结构。`*_test.go` 不参与生产构建，但它们是各公开契约和调用方式的可执行说明。

```text
easy-workflow/
├─ go.mod / go.sum                         # Go module、Go 版本和 adapter 依赖
├─ README.md                               # 使用、架构、接入与运行说明
├─ LICENSE                                 # 许可证
│
├─ doc.go                                  # core package 总体边界与 package documentation
├─ definition.go                           # Definition、Node、Edge、Builder、JSON 解析、结构校验入口
├─ compiler.go                             # 图分析、路由索引、handler 解析、配置准备和请求内执行计划
├─ handler.go                              # NodeHandler 协议、NodeResult、Registry 和显式注册入口
├─ handler_preparation.go                  # 新旧 handler 的请求内 PreparedNodeHandler 适配
├─ runtime.go                              # Instance、Task、Audit、Command 和生命周期请求/策略类型
├─ engine.go                               # Start/Handle/Withdraw/Return/Transfer 的总编排与持久化边界
├─ instance_facts.go                       # Instance、Task、Audit 的唯一包内事实变更入口
├─ node_result_application.go              # NodeResult 的分阶段校验、归一化和事实应用
├─ store.go                                # command-side Store 契约及 MemoryStore 参考实现
├─ publication.go                          # Definition 发布、版本读写契约及 MemoryDefinitionStore
│
├─ approval/
│  ├─ approval.go                          # 官方人工审批节点、或签/会签、静态/动态分配
│  ├─ preparation_test.go                  # Approval 配置预编译与旧执行路径一致性
│  └─ assignment_integration_test.go       # 组织角色解析、冻结 assignee、失败原子性
├─ condition/
│  ├─ condition.go                         # 官方受限条件节点、JSON Pointer 和类型化操作符
│  ├─ condition_test.go                    # 条件匹配、类型、确定性和错误边界
│  ├─ preparation_test.go                  # Condition 配置预编译一致性
│  └─ integration_test.go                  # Builder、JSON 发布和运行时分支示例
│
├─ postgres/
│  ├─ doc.go                               # PostgreSQL adapter 的 package 边界
│  ├─ store.go                             # command Store 的事务、CAS、Create/Load/Save
│  ├─ codec.go                             # 聚合与数据库行之间的无损编解码
│  ├─ migration.go                         # 只暴露迁移文件，不自动执行迁移
│  ├─ projection.go                        # Projection 公共类型、查询契约和构造函数
│  ├─ projection_query.go                  # 查询族共享的 limit、scope、取消等边界校验
│  ├─ projection_task.go                   # 待办/已办 Task 查询、keyset 与页面构造
│  ├─ projection_instance.go               # 我发起的 Instance 查询、keyset 与页面构造
│  ├─ projection_continuation.go           # 版本化 opaque continuation 编解码
│  ├─ projection_write.go                  # Store 事务内派生并刷新查询投影行
│  ├─ migrations/
│  │  ├─ 0001_init.up.sql                  # Instance、Definition、Task、Audit 初始 schema
│  │  ├─ 0001_init.down.sql                # 初始 schema 回滚
│  │  ├─ 0002_query_projection.up.sql      # 查询投影 schema
│  │  └─ 0002_query_projection.down.sql    # 查询投影 schema 回滚
│  ├─ migration_test.go                    # 迁移暴露与 adapter 构造测试
│  ├─ store_integration_test.go            # Store 契约、事务回滚、CAS、重启恢复
│  ├─ query_integration_test.go            # Projection 排序、scope、分页和事务可见性
│  ├─ projection_validation_test.go        # 旧 Cursor API 的参数与家族校验
│  └─ projection_continuation_test.go      # 新 continuation API 的编码与分页校验
│
├─ definitiontest/
│  └─ contract.go                          # Definition writer/reader/repository adapter 契约测试套件
├─ storetest/
│  └─ contract.go                          # 任意 command Store adapter 都应通过的共享契约测试
│
├─ example_test.go                         # 端到端可执行示例；建议从这里开始阅读
├─ definition_test.go                      # Builder 与图结构校验
├─ definition_compile_test.go              # 编译、配置校验、路由确定性和失败原子性
├─ definition_seams_test.go                # Publisher/Reader capability seam
├─ definition_repository_contract_test.go  # MemoryDefinitionStore 契约入口
├─ publication_test.go                     # 发布版本、并发分配和快照冻结
├─ engine_test.go                          # Approval 主流程：或签、会签、拒绝路由
├─ engine_node_result_test.go              # 非法 NodeResult 的原子拒绝
├─ handler_preparation_test.go              # Prepared handler 和 legacy handler 兼容
├─ store_test.go                           # MemoryStore 契约入口
├─ withdraw_test.go                        # 撤回、授权、终态和并发冲突
├─ return_test.go                          # 退回历史节点、重新激活和授权
├─ transfer_test.go                        # 转办 active Task、授权和并发冲突
│
├─ docs/architecture/
│  ├─ definition-repository-seams.md       # Definition 读写 capability 分离决策
│  ├─ runtime-deep-modules.md              # NodeResult application 与配置准备决策
│  └─ postgres-projection-continuations.md # Projection continuation 兼容决策
└─ issues/                                 # 按依赖顺序保存的实现 issue 与交付记录
```

## 从宿主启动到流程运行的调用链

### 1. 宿主应用启动与依赖装配

宿主自己的入口文件通常是 `cmd/<service>/main.go` 或内部的 composition root；本仓库不替宿主提供该文件。推荐装配顺序如下：

```text
宿主 main.go / DI module
  ├─ 创建 pgxpool.Pool 或 workflow.NewMemoryStore()
  ├─ workflow.NewRegistry()
  ├─ registry.Register(approval.Kind, approval.NewHandler())
  ├─ registry.Register(condition.Kind, condition.NewHandler())   # 使用 Condition 时
  ├─ workflow.NewEngine(store, registry)
  └─ workflow.NewDefinitionPublisher(definitionWriter, registry) # 需要发布版本时
```

注册必须在服务开始接收启动或命令请求前完成。`Registry` 虽然支持并发访问，但 Engine 持有同一个 Registry 指针；运行期间临时增加 kind 会让不同时间的请求看到不同的可用行为。重复注册同一个 kind 会返回 `workflow.ErrHandlerExists`，不会覆盖旧 handler。

`start` 和 `end` 是 core 识别的控制节点，由 [`compiler.go`](compiler.go) 和 [`engine.go`](engine.go) 直接处理，不需要注册 handler。`approval`、`condition` 以及宿主自定义的业务节点都必须显式注册。

### 2. 创建 Definition

代码创建入口位于 [`definition.go`](definition.go)：

```text
NewBuilder(definitionID)
  -> Start(nodeID)
  -> Node(nodeID, registeredKind, config)
  -> Connect(from, to, outcome)
  -> End(nodeID)
  -> Build()
       -> Definition.Validate()
       -> compiler.go/analyzeDefinition()
```

`Build` 检查的是 canonical 图结构，例如唯一 start、节点引用、DAG、可达性、能否到达 end、路由是否确定等。业务节点的 kind 是否已经注册、其 config 是否符合 handler 规则，则由 [`compiler.go`](compiler.go) 的 `CompileDefinition` 在发布或启动前检查。

Web 设计器不需要另一套模型：JSON 通过 `ParseDefinition` 进入同一个 `Definition`。因此 Builder 与 Web JSON 最终共享完全相同的结构校验、编译、发布和运行路径。

### 3. 发布 Definition 版本

发布链路位于 [`publication.go`](publication.go)：

```text
DefinitionPublisher.Publish(definition)
  -> compiler.go/CompileDefinition(definition, registry)
       -> 校验图结构
       -> 按 NodeDefinition.Kind 从 handler.go/Registry 查找 handler
       -> Validate 或 PrepareConfig 校验每个业务节点配置
  -> DefinitionVersionWriter.CreateVersion(...)
       -> 为同一 Definition.ID 原子分配下一版本
       -> 保存不可变防御性快照
```

`PublishJSON` 只是在这条链路前增加 `ParseDefinition`。编译失败不会调用 writer，因此不会占用版本号。当前 core 提供的 `MemoryDefinitionStore` 同时实现 writer 和 reader；PostgreSQL command Store 不会自动充当 Definition repository。

### 4. 启动一次 Instance

运行入口位于 [`engine.go`](engine.go)：

```text
Engine.Start(ctx, definition, StartRequest)
  -> 校验 Engine 依赖、Instance ID、Initiator 和业务 JSON
  -> compiler.go/compileDefinition() 冻结请求内执行计划
  -> instance_facts.go/startInstanceFacts() 创建候选聚合
  -> engine.go/advance() 从 start 沿图推进
       -> 进入业务节点
       -> PreparedNodeHandler.ActivatePrepared()
       -> node_result_application.go 校验并应用 NodeResult
       -> waiting: 创建 active Task 并停止推进
       -> continue: 按 outcome 继续寻找下一节点
       -> reject: 终止或沿声明的拒绝 outcome 继续
       -> end: 标记 Instance completed
  -> Store.Create() 原子保存完整 Instance
  -> 返回与 Store 脱离的快照
```

`Engine.StartPublished` 位于同一文件，但会先调用 `DefinitionReader.Load(id, version)` 读取精确版本，然后复用 `Start`。它不会自动回退到最新版本；如需启动最新版本，应先 `LoadLatest` 固定版本，再按该精确版本调用 `StartPublished`。

### 5. 处理审批或其他节点命令

任务命令入口仍在 [`engine.go`](engine.go) 的 `Engine.Handle`：

```text
Engine.Handle(command)
  -> Store.Load(instanceID) 读取一个防御性快照
  -> compileDefinition(instance.Definition, registry)
  -> 找到 CurrentNodeID 对应的 PreparedNodeHandler
  -> handler.HandlePrepared(command + data + state + 当前节点完整 tasks)
  -> node_result_application.go 校验 handler 返回值
  -> instance_facts.go 写入 Task、NodeState 和 Audit 事实
  -> 如需跳转则 engine.go/advance() 继续推进
  -> Instance.Version + 1
  -> Store.Save(candidate, expectedVersion) 做原子 CAS
```

handler 不能直接访问 Store、修改 Instance 或任意指定下一个节点。它只能返回声明式 `NodeResult`；Engine 校验结果后，才通过 `instance_facts.go` 改变事实，并由编译后的路由表解释 outcome。这样 handler 错误、非法结果或版本冲突都不会留下部分持久化状态。

### 6. 撤回、退回与转办

三类生命周期命令也在 [`engine.go`](engine.go)：

| 操作 | Engine 入口 | 请求与授权接口 | 主要事实变化 |
| --- | --- | --- | --- |
| 撤回 | `Engine.Withdraw` | `runtime.go` 的 `WithdrawRequest`、`WithdrawalPolicy` | 关闭 active Task，状态改为 withdrawn，追加 Audit |
| 退回 | `Engine.Return` | `runtime.go` 的 `ReturnRequest`、`ReturnPolicy` | 校验目标曾经进入过，重新激活目标节点并创建新一轮 Task |
| 转办 | `Engine.Transfer` | `runtime.go` 的 `TransferRequest`、`TransferPolicy` | 只替换指定 active Task 的 assignee，保留历史并追加 Audit |

它们共享 `engine.go` 的加载、运行态校验、版本推进和 CAS 保存骨架；业务授权由宿主实现的 policy 决定，Engine 不从不可信请求体推断权限。

## 按需求快速定位文件

| 如果要修改…… | 首先查看 | 通常还要联动查看 |
| --- | --- | --- |
| Definition 字段、节点或边的数据模型 | [`definition.go`](definition.go) | `compiler.go`、`postgres/codec.go`、迁移、Definition 与发布测试 |
| 图校验、路由规则、节点配置编译 | [`compiler.go`](compiler.go) | `definition.go`、`handler_preparation.go`、`definition_compile_test.go` |
| 新的业务节点类型 | 新建独立 package | `handler.go` 的接口、宿主 `Registry.Register`、集成测试 |
| Approval 模式或人员分配 | [`approval/approval.go`](approval/approval.go) | Approval 的 preparation/assignment tests |
| Condition 操作符或 JSON 规则 | [`condition/condition.go`](condition/condition.go) | Condition 单元测试与 integration test |
| Instance/Task/Audit 公共字段 | [`runtime.go`](runtime.go) | `instance_facts.go`、`postgres/codec.go`、schema 与投影写入 |
| Start、审批、撤回、退回、转办编排 | [`engine.go`](engine.go) | `node_result_application.go`、`instance_facts.go`、对应 `*_test.go` |
| NodeResult 合法性或任务替换规则 | [`node_result_application.go`](node_result_application.go) | `handler.go`、`instance_facts.go`、`engine_node_result_test.go` |
| command Store 契约 | [`store.go`](store.go) | `storetest/contract.go`、`postgres/store.go` |
| Definition 发布与版本能力 | [`publication.go`](publication.go) | `definitiontest/contract.go`、`definition_seams_test.go` |
| PostgreSQL 事务或 CAS | [`postgres/store.go`](postgres/store.go) | `codec.go`、`projection_write.go`、迁移与 integration tests |
| 待办、已办、我发起的查询 | [`postgres/projection.go`](postgres/projection.go) | `projection_task.go`、`projection_instance.go`、`projection_continuation.go` |
| 数据库 schema | [`postgres/migrations`](postgres/migrations) | `codec.go`、`store.go`、`projection_write.go`、migration tests |

## 安装

core 和 Approval extension 可以使用标准 Go package 命令一起引入：

```bash
go get github.com/lvpeng/easy-workflow github.com/lvpeng/easy-workflow/approval
```

按需安装 Condition 或 PostgreSQL adapter：

```bash
go get github.com/lvpeng/easy-workflow/condition
go get github.com/lvpeng/easy-workflow/postgres
```

## Core-only 内存快速开始

下面是 [`example_test.go`](example_test.go) 中 `Example` 的可执行流程：注册 Approval、用 Builder 创建 Definition、以内存 Store 启动 Instance，再完成一次或签审批。它不需要配置文件、数据库、HTTP framework、Redis 或组织目录。

```go
package workflow_test

import (
	"context"
	"fmt"

	workflow "github.com/lvpeng/easy-workflow"
	"github.com/lvpeng/easy-workflow/approval"
)

// Example 演示只使用 core 和官方 Approval extension 完成一次内存审批。
func Example() {
	registry := workflow.NewRegistry()
	if err := registry.Register(approval.Kind, approval.NewHandler()); err != nil {
		panic(err)
	}
	engine := workflow.NewEngine(workflow.NewMemoryStore(), registry)

	// 构建最小完整流程：进入或签审批，并沿 approved outcome 完成。
	builder := workflow.NewBuilder("leave-request")
	builder.Start("start")
	builder.Node("manager-approval", approval.Kind, approval.Config{
		Mode:      approval.ModeAny,
		Assignees: []workflow.ActorID{"manager-a", "manager-b"},
	})
	builder.End("end")
	builder.Connect("start", "manager-approval", "")
	builder.Connect("manager-approval", "end", approval.OutcomeApproved)
	definition, err := builder.Build()
	if err != nil {
		panic(err)
	}

	// Start 会先校验并冻结 Definition，再原子创建内存 Instance。
	instance, err := engine.Start(context.Background(), definition, workflow.StartRequest{
		ID:        "leave-1",
		Initiator: "employee-a",
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(instance.Status, len(instance.Tasks))

	// 或签由第一个有效审批完成，同时关闭同节点的其他 active Task。
	instance, err = engine.Handle(context.Background(), workflow.Command{
		InstanceID: instance.ID,
		TaskID:     instance.Tasks[0].ID,
		ActorID:    instance.Tasks[0].Assignee,
		Name:       approval.CommandApprove,
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(instance.Status)

	// Output:
	// running 2
	// completed
}
```

直接验证：

```bash
go test . -run '^Example$' -count=1
```

`MemoryStore` 和 `MemoryDefinitionStore` 适用于示例、测试和单进程场景，进程退出后数据丢失；生产持久化应显式选择 durable adapter。

## Definition 发布与版本

Builder 和 Web JSON 汇入同一个 canonical `workflow.Definition`，并由同一个 `DefinitionPublisher` 完成编译、handler config 校验/请求内准备和版本分配：

1. `publisher.Publish(ctx, builderDefinition)` 发布代码定义并取得版本 1。
2. `publisher.PublishJSON(ctx, definitionJSON)` 解析相同 ID 的 JSON，通过同一发布路径取得版本 2。
3. `engine.StartPublished(ctx, reader, id, version, request)` 按指定版本启动。
4. “启动最新版本”先调用 `reader.LoadLatest(ctx, id)` 固定最新的 ID/Version，再把该精确版本传给 `StartPublished`。这样即使随后发布版本 3，本次启动目标也不会漂移。

Instance 会保存启动时 Definition 的完整快照。后续发布只增加不可变版本，不能改写运行中 Instance 的节点配置、路由或任务语义。

完整 Builder + JSON + 指定版本 + 最新版本示例是 [`ExampleDefinitionPublisher_versions`](example_test.go)，可执行验证：

```bash
go test . -run '^ExampleDefinitionPublisher_versions$' -count=1
```

## 官方 extensions

### Approval

`approval.NewHandler()` 支持静态 assignee 的或签与会签。需要角色解析时，宿主通过 `approval.NewHandlerWithOrganization` 显式注入 `OrganizationAdapter`；extension 不拥有目录连接、缓存、租户或身份映射。

### Condition

Condition 配置是纯 JSON 数据。规则使用 RFC 6901 JSON Pointer，显式选择 `all` 或 `any`；全部规则独立求值且只能命中一个。零命中时使用 `defaultOutcome`，未配置默认分支返回 `condition.ErrNoMatch`，多规则命中返回 `condition.ErrMultipleMatches`。

| 类型 | 操作符 | 值范围 |
| --- | --- | --- |
| `string` | `eq`, `neq`, `contains`, `starts_with`, `ends_with` | JSON 字符串 |
| `number` | `eq`, `neq`, `gt`, `gte`, `lt`, `lte` | 精确比较的 JSON 数值 |
| `boolean` | `eq`, `neq` | JSON 布尔值 |
| `collection` | `contains`, `contains_any`, `contains_all` | 字符串、数值或布尔基元数组 |

Condition 不做跨类型转换，也不执行脚本、模板、反射调用或外部 I/O。Web JSON 从发布到实际分支结束的可执行示例是 [`condition.ExampleHandler_webJSON`](condition/integration_test.go)：

```bash
go test ./condition -run '^ExampleHandler_webJSON$' -count=1
```

## PostgreSQL durable adapter

`postgres` 不会在 import、`postgres.New` 或 `postgres.Migrations` 时连接数据库或修改 schema。生产接入必须显式完成以下工作：

1. 宿主解析 DSN，创建并持有 `*pgxpool.Pool`，按自己的启动策略执行 `Ping`，退出时关闭 pool。
2. 宿主从 `postgres.Migrations()` 读取 `migrations/*.up.sql`，由选定的迁移工具负责版本顺序、迁移锁、事务和回滚；adapter 不自动初始化。
3. 宿主把 pool 传给 `postgres.New(pool)`。`Create` 和 `Save` 各自在单个数据库事务中提交 Instance、Definition 快照、business data、NodeState、Task、append-only Audit 和查询投影；任一步失败都会回滚。
4. `Save` 使用数据库条件版本更新实现跨进程 CAS。陈旧写入返回可由 `errors.Is` 识别的 `workflow.ErrVersionConflict`。

### 查询投影

`postgres.NewProjection(pool)` 借用同一个宿主所有的 pool，构造时不连接数据库、不执行迁移，且可被并发调用。Projection 只应用宿主已经计算出的 actor 和授权 scope，不发现租户、组织或权限。

| 方法 | 返回值 | 语义 |
| --- | --- | --- |
| `WorklistPage` | `ContinuationPage[TaskProjection]` | actor 在运行中 Instance 里的 active 冻结任务 |
| `ParticipatedPage` | `ContinuationPage[TaskProjection]` | actor 已 completed 或 closed 的冻结任务 |
| `InitiatedPage` | `ContinuationPage[InstanceProjection]` | actor 发起的运行中及终态 Instance |

```go
projection := postgres.NewProjection(pool)
const pageLimit = 50 // 使用公开契约的默认页大小，并在连续请求间保持不变。

// 首次查询应用宿主计算的完整授权 scope；Projection 不会补充或放宽它。
page, err := projection.WorklistPage(ctx, postgres.ContinuationQuery{
	ActorID: actorID,
	Scope: postgres.QueryScope{
		InstanceIDs: authorizedInstanceIDs,
	},
	Page: postgres.ContinuationPageRequest{Limit: pageLimit},
})
if err != nil {
	// 验证、取消或数据库错误都不会产生可继续使用的部分页面。
	return err
}

// page.Next 非空时，把 opaque token 原样传回同一个 Task 查询族以读取下一页。
if page.Next != "" {
	// 连续请求保持 actor、scope 和 limit 不变，只替换 Projection 返回的 continuation。
	nextPage, err := projection.WorklistPage(ctx, postgres.ContinuationQuery{
		ActorID: actorID,
		Scope: postgres.QueryScope{
			InstanceIDs: authorizedInstanceIDs,
		},
		Page: postgres.ContinuationPageRequest{Limit: pageLimit, After: page.Next},
	})
	if err != nil {
		// 下一页失败时保留原页，调用方可按错误原因决定是否重试。
		return err
	}
	page = nextPage
}
```

查询输入遵守以下兼容契约：

- `QueryScope.InstanceIDs == nil` 表示不附加 Instance 限制；非 nil 空 slice 表示拒绝全部 Instance，并直接返回非 nil 空 `Items`；非空 slice 只允许列出的 Instance。
- `ContinuationPageRequest.Limit == 0` 使用默认值 50；显式值必须位于 `[1, 200]`，否则返回可由 `errors.Is` 识别的 `postgres.ErrInvalidProjectionQuery`。
- Task 查询按审计时间降序、InstanceID 和 TaskID 升序稳定分页；`WorklistPage` 与 `ParticipatedPage` 共享 opaque Task continuation。`InitiatedPage` 使用 Instance continuation，跨家族或结构无效的 token 会在数据库访问前被拒绝。token 不是签名或授权凭据，scope 仍由每次查询显式提供。
- `Next` 只在同一查询快照中观察到后续行时返回，末页为空字符串；成功时 `Items` 始终非 nil。token 不携带授权，后续请求必须重新提供可信 actor 与完整 scope。
- actor、scope、continuation 解出的 keyset 和 limit 全部作为 PostgreSQL 参数传递，不参与 SQL 文本拼接。
- 旧 `Worklist`、`Participated`、`Initiated`、`ActorQuery`、`PageRequest`、`Cursor` 与 `Page` 已标记 Deprecated，并在当前 major version 内保持原分页行为；迁移只需改用对应 `*Page` 方法并把 `Next` 从 nil 判断改为空字符串判断。

内部实现按 Task 与 Instance 两个查询族分别拥有 continuation 映射与校验、keyset 参数、扫描和分页构造，只共享版本化 base64url 传输编码以及 limit、取消和 scope 值转换等完全一致的边界行为；新增查询族不需要引入通用 repository 或 mock-only executor。

集成测试要求调用方显式提供测试数据库，不会启动容器或使用隐式本机默认值：

```bash
EASY_WORKFLOW_POSTGRES_DSN='postgres://user:password@localhost:5432/easy_workflow_test?sslmode=disable' go test ./postgres -count=1
```

测试会为各场景创建随机隔离 schema，并覆盖公共 Store 契约、事务回滚、并发 CAS、pool 重启后的读取、完整快照恢复，以及 Projection 的 scope、稳定排序、opaque continuation、旧 Cursor 兼容、分页边界和事务可见性。未设置 `EASY_WORKFLOW_POSTGRES_DSN` 时，数据库相关用例会明确 skip。

## MySQL durable adapter

The optional `mysql` package implements the core command-side `workflow.Store` contract over a caller-owned
`*sql.DB`. It does not connect or migrate during construction. Import a MySQL `database/sql` driver in the host,
apply `mysql.Migrations()` with the host's migration tooling, then pass the opened handle to `mysql.New(db)`.

```go
import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"

	workflowmysql "github.com/lvpeng/easy-workflow/mysql"
)

db, err := sql.Open("mysql", dsn)
if err != nil {
	return err
}
store := workflowmysql.New(db)
```

The MySQL adapter requires MySQL 8.0.16 or later. It provides durable Instance, Task, and Audit snapshots with
transactional CAS and append-only audit semantics. Indexed identifiers use case-sensitive NO PAD utf8mb4
`VARCHAR(255)` columns; longer values and invalid child-row values are rejected before database I/O. It currently
does not provide the PostgreSQL package's query projection API.

MySQL integration tests require an explicit DSN and create isolated databases:

```bash
EASY_WORKFLOW_MYSQL_DSN='user:password@tcp(localhost:3306)/mysql?parseTime=true' go test ./mysql -count=1
```

## 公开契约

- `Store`：`Create` 仅插入，`Load` 返回调用方拥有的深拷贝，`Save` 以 `expectedVersion` 原子 CAS 完整聚合；Audit 只能追加。实现必须支持并发调用、传播 context cancellation，并通过 `errors.Is` 暴露稳定 sentinel。
- `NodeHandler`：`Validate` 在启动或发布前校验配置；`Activate` 和 `Handle` 只返回声明式 `NodeResult`，不能直接访问 Store 或任意跳转。handler 必须能被不同 Instance 并发调用，阻塞工作必须遵守 context cancellation。需要避免重复解析配置的实现可额外实现 `NodeHandlerConfigPreparer`；编译器每个节点、每次 executable plan 只调用一次 `PrepareConfig`，返回值只在该请求内复用且永不持久化。旧 handler 无需修改。
- Definition 发布：发布前完整编译；失败不占版本、不留部分记录。writer 为同一 ID 原子分配严格递增版本并保存防御性快照；reader 的 `Load` 只读指定版本且不 fallback，`LoadLatest` 读取当前最大版本。发布与读取必须支持并发。

`DefinitionVersionWriter` 与 `DefinitionReader` 是刻意分离的 capability seam：`DefinitionPublisher` 只依赖写入，`Engine.StartPublished` 只依赖调用方提供的精确版本读取。`MemoryDefinitionStore` 是当前参考 adapter，而不是唯一允许的实现。新 adapter 必须原样运行 `definitiontest.RunWriter`、`definitiontest.RunReader` 和 `definitiontest.RunRepository`；Definition repository 不并入 command-side `Store`。

Definition repository 的能力边界记录在 [Definition repository seam 架构决策](docs/architecture/definition-repository-seams.md)；NodeResult application、request-local handler preparation 与 Projection continuation 的 source of truth 和兼容决策记录在 [Runtime deep modules 架构决策](docs/architecture/runtime-deep-modules.md) 与 [Projection continuation 架构决策](docs/architecture/postgres-projection-continuations.md)。

这些契约的完整错误语义以相应公开类型的 Go package documentation 为准：

```bash
go doc github.com/lvpeng/easy-workflow.Store
go doc github.com/lvpeng/easy-workflow.NodeHandler
go doc github.com/lvpeng/easy-workflow.NodeHandlerConfigPreparer
go doc github.com/lvpeng/easy-workflow.DefinitionPublisher
go doc github.com/lvpeng/easy-workflow/definitiontest
go doc github.com/lvpeng/easy-workflow/postgres.Continuation
```

## 发布验证

```bash
go test ./...
go test -race ./...
go vet ./...
EASY_WORKFLOW_POSTGRES_DSN='...' go test ./postgres -count=1
EASY_WORKFLOW_MYSQL_DSN='...' go test ./mysql -count=1
```

The first three checks are local/static validation; the PostgreSQL and MySQL commands are explicit DSN-backed integration gates and are skipped when their DSN is not configured.

前三项验证所有无外部基础设施的 package；最后一项是 P1 durable adapter 的显式集成门禁。
