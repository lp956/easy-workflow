# easy-workflow

`easy-workflow` 是可嵌入 Go 应用的人工审批流内核。它负责 Definition 校验与发布、Instance 状态流转、Task、乐观并发和不可变 Audit；业务节点、数据库、组织目录以及交互界面通过显式依赖组合。

## 分层与依赖

| 层 | Go package | 职责 | 依赖与边界 |
| --- | --- | --- | --- |
| core | `github.com/lvpeng/easy-workflow` | canonical Definition、Builder、发布、Engine、`Store` 契约、`MemoryStore` | 只使用 Go 标准库；导入时不读配置、不连接网络、不启动 goroutine |
| 官方 extension | `.../approval`、`.../condition` | 人工审批与受限 JSON 条件路由 | 只依赖 core；handler 必须显式注册 |
| durable adapter | `.../postgres` | PostgreSQL command Store、迁移文件和独立查询投影 | 显式依赖 `pgx/v5`；连接、迁移和 pool 生命周期由宿主负责 |
| 可选 transport / Web integration | 由宿主应用实现 | HTTP、鉴权、DTO、Web UI、设计器和 Definition JSON 传输 | 不属于 core，也没有隐式默认实现 |

组织目录同样不属于 core。`approval.OrganizationAdapter` 只是宿主可选实现的边界；静态 assignee 不需要目录。待办、已办和搜索属于 adapter 查询投影，不会扩张 command-side `workflow.Store`。

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
```

前三项验证所有无外部基础设施的 package；最后一项是 P1 durable adapter 的显式集成门禁。
