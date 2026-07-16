# `go-workflow/go-workflow` 源码研究报告

> 研究日期：2026-07-15  
> 源码快照：[`85255031ec8d00773905b59e3989c1fb816dd881`](https://github.com/go-workflow/go-workflow/commit/85255031ec8d00773905b59e3989c1fb816dd881)  
> 证据范围：主仓库 README、示例、配置说明、全部 Go 源码与测试、提交/Release/Issue 元数据。未研究单独的 UI 仓库。

## 结论先行

这个项目值得参考的不是“后台任务执行框架”，而是一个很小的、数据驱动的**人工审批流微服务**原型：流程定义是嵌套 JSON；启动时根据变量一次性选择分支，并把结果编译成线性 `NodeInfo[]`；之后用 `step` 前后移动，MySQL 保存流程实例、任务、候选人/参与人和历史记录。项目自己也明确把它描述为微服务，并声明只记录流程流转、把用户与用户组解耦出去（[README L19-L29](https://github.com/go-workflow/go-workflow/blob/85255031ec8d00773905b59e3989c1fb816dd881/README.md#L19-L29)）。

因此：

- **适合借鉴**：极简 JSON 定义、定义与实例分离、启动时冻结实例执行计划、运行态/历史态分表、参与人/候选人关系模型、会签/或签的最小状态机。
- **不适合作为通用后台工作流框架底座直接改造**：没有可执行 activity/step 抽象，没有 worker/队列、重试、超时、取消、补偿、事件/信号、并行分支、幂等键或崩溃恢复协议；所谓“执行流”只是审批节点清单。
- **不能直接作为生产级库复用**：它是带全局配置、全局数据库和 HTTP 路由的完整进程，不是可嵌入库；没有 `go.mod`、tag 或 Release，默认分支最后一次提交停在 2020-01-22；自动化测试基本为空。

## 1. 整体架构

代码分成四层，但边界较薄：

1. `workflow-router`：`net/http.ServeMux` 注册 `/api/v1/workflow/...`，同时保留一套标为废弃的 `/workflow/...` 路由（[router.go](https://github.com/go-workflow/go-workflow/blob/85255031ec8d00773905b59e3989c1fb816dd881/workflow-router/router.go)）。
2. `workflow-controller`：解析 HTTP 请求、做少量字段校验、调用 service，并通过外部 `util` 包返回 JSON。
3. `workflow-engine/service`：流程定义部署、实例启动、审批、撤回、迁移历史等业务编排。
4. `workflow-engine/model`：GORM v1 模型与 SQL；启动时自动建表、索引和外键（[database.go L27-L73](https://github.com/go-workflow/go-workflow/blob/85255031ec8d00773905b59e3989c1fb816dd881/workflow-engine/model/database.go#L27-L73)）。

`main` 在启动时加载配置、建立 MySQL/Redis 连接、启动历史迁移 cron，然后启动 HTTP Server（[main.go](https://github.com/go-workflow/go-workflow/blob/85255031ec8d00773905b59e3989c1fb816dd881/main.go)）。这说明它的交付单元是一个微服务进程，而非 `workflow.New(...)` 形式的 Go SDK。

## 2. 公开 API 与使用模型

主要 HTTP 使用面如下（完整样例见 [EXAMPLE.md](https://github.com/go-workflow/go-workflow/blob/85255031ec8d00773905b59e3989c1fb816dd881/EXAMPLE.md)）：

| 能力 | 主要端点 | 核心输入 |
|---|---|---|
| 部署流程定义 | `POST /api/v1/workflow/procdef/save` | 名称、公司、用户、嵌套 JSON `resource` |
| 查询/删除定义 | `procdef/findAll`、`procdef/delById` | 分页条件或 ID |
| 启动实例 | `POST process/start` | `procName`、发起人、部门、公司、变量 `var` |
| 查询待办/实例 | `process/findTask`、`process/findById` | 用户、组、负责部门、公司 |
| 审批 | `POST task/complete` | `taskID`、`pass`、审批人、评论、可选下一候选人 |
| 撤回 | `POST task/withdraw` | 当前任务、流程实例、操作人 |
| 查询参与人/历史 | `identitylink/*`、`procHistory/*` | 流程实例或用户分页条件 |

部分端点有 `ByToken` 版本：token 只是用来从 Redis 读取 `UserInfo`，再补充公司、用户、角色和部门，不是独立的认证授权框架（[redisService.go](https://github.com/go-workflow/go-workflow/blob/85255031ec8d00773905b59e3989c1fb816dd881/workflow-engine/service/redisService.go)）。非 token 端点直接信任请求中的 `userID/company/groups`，所以调用方必须在网关或业务服务中承担身份鉴别与授权。

虽然 service 包函数是导出的，但导入链会触发全局配置初始化并从当前目录打开 `config.json`（[config.go L41-L77](https://github.com/go-workflow/go-workflow/blob/85255031ec8d00773905b59e3989c1fb816dd881/workflow-config/config.go#L41-L77)），数据库也通过包级全局变量访问。这使“嵌入单体应用”在工程上非常别扭。

## 3. 核心数据结构

### 3.1 定义结构

`Node` 是递归树：`ChildNode` 表示顺序后继，`ConditionNodes` 表示条件分支，`Properties` 保存条件和审批规则（[node.go L19-L29](https://github.com/go-workflow/go-workflow/blob/85255031ec8d00773905b59e3989c1fb816dd881/workflow-engine/flow/node.go#L19-L29)）。节点类型只有：`start`、`route`、`condition`、`approver`、`notifier`。审批人规则只有“主管”和“角色/标签”；条件只有整数范围和值集合（[node.go L30-L116](https://github.com/go-workflow/go-workflow/blob/85255031ec8d00773905b59e3989c1fb816dd881/workflow-engine/flow/node.go#L30-L116)）。

解析结果 `NodeInfo` 只保留节点 ID、审批人/类型、人数、层级和会签类型（[node.go L122-L130](https://github.com/go-workflow/go-workflow/blob/85255031ec8d00773905b59e3989c1fb816dd881/workflow-engine/flow/node.go#L122-L130)）。每个节点实际上只读取 `ActionerRules[0]`，所以数组形态没有带来多规则语义（[node.go L145-L163](https://github.com/go-workflow/go-workflow/blob/85255031ec8d00773905b59e3989c1fb816dd881/workflow-engine/flow/node.go#L145-L163)）。

### 3.2 持久化结构

- `Procdef`：名称、版本、定义 JSON、创建者和公司（[ACT_RE_PROCDEF.go](https://github.com/go-workflow/go-workflow/blob/85255031ec8d00773905b59e3989c1fb816dd881/workflow-engine/model/ACT_RE_PROCDEF.go)）。
- `ProcInst`：定义 ID、标题、发起人、当前节点、当前候选人、当前任务和结束状态（[ACT_HI_PROCINST.go L11-L34](https://github.com/go-workflow/go-workflow/blob/85255031ec8d00773905b59e3989c1fb816dd881/workflow-engine/model/ACT_HI_PROCINST.go#L11-L34)）。
- `Execution`：把实例会经过的整条 `NodeInfos` 作为 JSON 字符串保存（[ACT_RU_EXECUTION.go L14-L25](https://github.com/go-workflow/go-workflow/blob/85255031ec8d00773905b59e3989c1fb816dd881/workflow-engine/model/ACT_RU_EXECUTION.go#L14-L25)）。
- `Task`：节点、`step`、处理人、创建/处理时间、会签总人数、剩余人数、同意数和结束状态（[ACT_RU_TASK.go L15-L37](https://github.com/go-workflow/go-workflow/blob/85255031ec8d00773905b59e3989c1fb816dd881/workflow-engine/model/ACT_RU_TASK.go#L15-L37)）。
- `Identitylink`：candidate、participant、manager、notifier 四类关系，承载候选组/用户、评论和参与记录（[ACT_RU_IDENTITYLINK.go L8-L36](https://github.com/go-workflow/go-workflow/blob/85255031ec8d00773905b59e3989c1fb816dd881/workflow-engine/model/ACT_RU_IDENTITYLINK.go#L8-L36)）。

这种“实例摘要 + 当前任务 + 不可变执行计划 + 身份关系”的拆分很适合人工审批查询，但不足以表达后台任务的输入输出、尝试次数、租约、心跳、错误、超时、补偿和产物。

## 4. 执行语义

### 4.1 启动时编译，而不是运行时解释

`ParseProcessConfig` 深度遍历定义；遇到条件时用启动变量选出分支（[node.go L231-L275](https://github.com/go-workflow/go-workflow/blob/85255031ec8d00773905b59e3989c1fb816dd881/workflow-engine/flow/node.go#L231-L275)）。`GenerateExec` 再人工插入“开始/结束”，序列化后写入 `Execution.NodeInfos`（[executionService.go L58-L78](https://github.com/go-workflow/go-workflow/blob/85255031ec8d00773905b59e3989c1fb816dd881/workflow-engine/service/executionService.go#L58-L78)）。

这意味着：

- 分支只在启动时求值，流程中途变量变化不会重新选路。
- 定义升级不影响已启动实例；实例持有自己的线性快照。这是值得保留的确定性设计。
- 后续状态机只需 `step++/step--`，实现极简，但无法表达运行时动态分支、循环、并行、join 或子流程。

条件语义也有明显限制：只检查 `Conditions[0]`；如果多个分支同时满足，循环会让后匹配者覆盖前匹配者；变量为空或条件节点仅一个时直接走第一个子节点（[node.go L243-L306](https://github.com/go-workflow/go-workflow/blob/85255031ec8d00773905b59e3989c1fb816dd881/workflow-engine/flow/node.go#L243-L306)）。

### 4.2 事务内创建实例

启动流程在一个 GORM 事务内创建 `ProcInst`、冻结 `Execution`、创建已完成的“开始”任务，再调用 `MoveStage` 进入第一个审批节点（[procInstService.go L124-L203](https://github.com/go-workflow/go-workflow/blob/85255031ec8d00773905b59e3989c1fb816dd881/workflow-engine/service/procInstService.go#L124-L203)）。这是正确的原子性方向；但代码没有检查 `Begin`/`Commit` 的错误，创建实例后的错误也有被后续赋值覆盖的路径，生产实现不应照搬。

### 4.3 审批、会签与驳回

完成任务时先读取任务并更新计数：通过则 `AgreeNum++`，驳回立即结束当前任务，`UnCompleteNum--`，剩余数为零也结束（[taskService.go L98-L139](https://github.com/go-workflow/go-workflow/blob/85255031ec8d00773905b59e3989c1fb816dd881/workflow-engine/service/taskService.go#L98-L139)）。

- `and` 会签：通过且仍有人未审时只记录 participant；全通过后前进；任何一人驳回就向前一步回退。
- `or` 或签：默认人数通常为 1，一次审批即流转。
- 通过是 `step++`，驳回是 `step--`；驳回不是失败终止，而是退回相邻节点（[taskService.go L291-L338](https://github.com/go-workflow/go-workflow/blob/85255031ec8d00773905b59e3989c1fb816dd881/workflow-engine/service/taskService.go#L291-L338)）。
- notifier 节点自动创建已完成任务、记录抄送关系并递归跳过。
- `candidate` 可覆盖下一节点审批人，但没有策略接口、审计理由或授权校验。

撤回仅允许上一个实际处理人、当前任务尚无人处理且两个任务相邻时执行（[taskService.go L207-L276](https://github.com/go-workflow/go-workflow/blob/85255031ec8d00773905b59e3989c1fb816dd881/workflow-engine/service/taskService.go#L207-L276)）。这不是后台作业意义上的 cancellation：它不会向正在执行的工作发送取消信号。

## 5. 能力矩阵

| 维度 | 实际能力 | 结论 |
|---|---|---|
| 并发 | 分页查询偶尔并发查数据/计数；审批、定义保存、历史搬迁使用包级 `sync.Mutex` | 只对单进程有效；多副本没有数据库锁、乐观版本或分布式租约 |
| 异步 | cron goroutine 每 20 秒搬历史；无工作队列/worker | 不是异步任务编排引擎 |
| 错误处理 | service 返回 `error`，HTTP 包装响应；事务路径手工回滚 | 无稳定错误分类；cron 丢弃迁移错误（[cronJobService.go L11-L24](https://github.com/go-workflow/go-workflow/blob/85255031ec8d00773905b59e3989c1fb816dd881/workflow-engine/service/cronJobService.go#L11-L24)） |
| 重试 | 无 | 没有次数、退避、可重试错误、死信或人工重放 |
| 取消/超时 | 无 `context.Context`，无 deadline/heartbeat；只有人工“撤回” | 不适合长时间后台任务 |
| 持久化 | MySQL 保存运行态，结束后搬到 history 表 | 有基本耐久状态，但无事件日志/checkpoint/恢复协议 |
| 定义版本 | 新版本写入运行表，旧版本移动到 `procdef_history` | 实例快照是优点；只保留一个在线版本限制回滚与并行版本管理 |
| 幂等 | 防重复参与人的查询和单进程锁 | 没有业务幂等键；多实例竞争不安全 |
| 可观测性 | 日志、开始/结束时间、审批评论、最终 duration | 无结构化事件、指标、trace、卡住检测或管理面 |
| 扩展性 | 主要靠修改 `Node`、service、model 源码 | 没有注册式节点、存储、执行器、中间件或策略接口 |

## 6. 测试揭示的契约

仓库没有形成可执行的行为契约：`flow/node_test.go` 唯一启用的测试只打印 `NodeTypes`；真正解析流程的测试和 benchmark 被整段注释（[node_test.go](https://github.com/go-workflow/go-workflow/blob/85255031ec8d00773905b59e3989c1fb816dd881/workflow-engine/flow/node_test.go)）。Redis 测试也是空壳（[redis_test.go](https://github.com/go-workflow/go-workflow/blob/85255031ec8d00773905b59e3989c1fb816dd881/workflow-engine/model/redis_test.go)）。

仓库没有 `go.mod`，在当前 Go 工具链直接执行 `go test ./...` 失败：`directory prefix . does not contain main module or its selected dependencies`。依赖均未被模块文件锁定。因此不能把当前行为视为经过回归测试保护的稳定 API。

至少缺少以下关键测试：条件重叠/无匹配、多级分支、会签重复审批、并发完成同一任务、事务回滚、撤回边界、定义升级后旧实例、历史迁移重复执行、多服务实例竞争、错误恢复与 SQL 注入。

## 7. 生产风险

### 高风险

1. **多副本并发不安全**：`completeLock`、`saveLock`、`copyLock` 都是进程内互斥量，无法保护 Kubernetes 多副本。任务更新也没有 `SELECT ... FOR UPDATE`、版本号 CAS 或唯一约束协议。
2. **SQL 注入**：待办和抄送查询把 `company`、`procName`、`userID`、groups 拼进 SQL 字符串（[ACT_HI_PROCINST.go L80-L97](https://github.com/go-workflow/go-workflow/blob/85255031ec8d00773905b59e3989c1fb816dd881/workflow-engine/model/ACT_HI_PROCINST.go#L80-L97)，[L140-L162](https://github.com/go-workflow/go-workflow/blob/85255031ec8d00773905b59e3989c1fb816dd881/workflow-engine/model/ACT_HI_PROCINST.go#L140-L162)）。这些值直接来自 HTTP 请求时会形成注入面。
3. **授权边界薄弱**：非 token API 信任调用方提交的身份、公司和角色；按 ID 查询/删除也看不到租户与权限约束。
4. **历史迁移非单事务**：先在事务外创建 `proc_inst_history`，再在另一事务复制关联表并删除运行态，错误时用补偿删除；多实例并发下容易冲突（[procInstService.go L262-L316](https://github.com/go-workflow/go-workflow/blob/85255031ec8d00773905b59e3989c1fb816dd881/workflow-engine/service/procInstService.go#L262-L316)）。

### 中风险

- 配置加载会把完整配置 JSON 写日志，结构中包含数据库与 Redis 密码（[config.go L48-L58](https://github.com/go-workflow/go-workflow/blob/85255031ec8d00773905b59e3989c1fb816dd881/workflow-config/config.go#L48-L58)）。
- 定义校验不充分：可出现空 `ActionerRules` 导致索引 panic；不验证唯一 node ID、分支互斥、可达性或结构深度。
- 递归遍历和 notifier 递归跳转没有深度限制，恶意/错误配置可能导致栈问题。
- 无模块版本、无 Release/tag；仓库树没有 LICENSE 文件，README 虽展示 Apache 2 badge，但 GitHub 元数据未识别许可证。依赖和法律边界都不够清晰。

相关 Issue 也侧面体现了交付成熟度问题，例如数据库/Docker 启动困惑（[#2](https://github.com/go-workflow/go-workflow/issues/2)、[#3](https://github.com/go-workflow/go-workflow/issues/3)）、是否能嵌入单体（[#4](https://github.com/go-workflow/go-workflow/issues/4)）以及 PostgreSQL 支持（[#6](https://github.com/go-workflow/go-workflow/issues/6)）。Issue 只能作为使用反馈，以上技术结论仍以源码为准。

## 8. 对“极易使用的 Go 后台工作流框架”的启示

### 应该借鉴

1. **定义与实例分离，并在启动时固定定义版本**：保证运行中的实例不会因部署新定义而漂移。
2. **先编译、后运行**：将友好的 Builder/JSON 定义编译成经过校验的内部图；但不要像本项目一样过早压平成只能前后移动的数组。
3. **状态模型小而清楚**：Definition、Run、Step/Task、Assignment、History 分离，查询模型可单独优化。
4. **身份系统外置**：框架不维护用户目录，只保存 principal/role 引用；同时必须提供明确的授权注入点。
5. **默认体验极简**：最小场景可以 `workflow.New(...).Step(...).Run(ctx, input)`，复杂能力通过 option/interface 增量加入。

### 不应照搬

1. 不要把 HTTP、Redis、MySQL、全局配置绑死在核心包；核心应依赖 `Store`、`Executor`、`Clock`、`IDGenerator`、`Authorizer` 等窄接口。
2. 不要用进程内 mutex 保证持久状态一致性；采用事务行锁或乐观版本，并用唯一键保障幂等。
3. 不要把“审批节点”与“后台 activity”混为一谈。后台 activity 需要输入输出、attempt、lease、heartbeat、timeout、retry policy、idempotency key 和 cancellation。
4. 不要只存最终快照；至少记录可审计的状态转换事件，并保证“状态变化 + 待执行任务/outbox”原子提交。
5. 不要让条件表达式直接解释任意代码。应提供类型化、可验证、可序列化的条件 DSL，并在发布定义时检查分支互斥、可达性和 join 完整性。

## 9. 最终判断

把该仓库定位为“**审批流领域模型的最小参考实现**”是准确的；把它定位为“**通用后台工作流引擎的基础实现**”会误导架构方向。你的新框架可以保留它最有价值的三点——JSON/Builder 友好定义、实例版本冻结、显式任务/参与人状态——但执行内核应从 durable execution 的需求重新设计，而不是围绕 `step++/step--` 继续扩展。

下一阶段最先要澄清的不是 API 长什么样，而是目标到底是：

- 人工审批编排；
- 进程内函数流水线；
- 跨进程、可恢复的后台任务编排；
- 还是同时覆盖，但以两个不同执行器共享同一套定义/状态内核。

这个选择会决定持久化协议、并发模型和“极易使用”能做到什么程度。
