# Runtime deep modules

- 状态：Accepted
- 决策范围：`NodeResult` application 与 request-local handler config preparation

## 背景

Engine 的 activation、command 和 return 路径都消费 `NodeResult`，但三条路径的阶段规则不同：activation 创建新任务草稿，command 返回当前节点的完整任务视图，return 必须为历史目标创建新的非空 waiting 轮次。此前 Disposition 分派、Outcome 解释、State 写入和 Tasks 校验分散在 Engine 与 `instanceFacts` 中，新增约束容易只落在某一条路径。

Definition 编译此前只验证 handler config。官方 Approval 与 Condition 会在 Validate、Activate 和 Handle 中重复解析同一份 JSON；Engine 也会在编译后再次从 Registry 查找 handler。canonical Definition 和持久化 Instance 必须继续只保存 JSON 数据，因此不能把 handler、回调或解析对象写入快照，也不能用跨请求 cache 绕过持久化快照的防御性编译。

## 决策

### NodeResult application

- `node_result_application.go` 是 handler 结果规则的唯一 package-internal source of truth。
- 所有结果先完成 Disposition、Outcome、State JSON 和阶段专属 Tasks 校验，再产生不可变的 request-local application。
- activation 只允许 waiting 结果创建 task drafts；立即 continue/reject 的 activation 不得遗留 tasks。
- command 必须返回当前节点所有历史轮次和当前轮次的完整 task view；未知、遗漏或跨节点 identity 被拒绝。为兼容既有语义，重复 ID 仍采用最后一个值。
- return 只接受 outcome 为空、tasks 非空的 waiting 结果；错误同时保留 `ErrInvalidReturnTarget` 和 `ErrInvalidNodeResult` 分类。
- application 只通过 `instanceFacts` 写入任务、State、拒绝、节点进入与生命周期事实；`instanceFacts` 继续是 Instance 变更的唯一入口。
- Engine 只消费 application 产生的 wait、advance、stop 决策，不再解释原始 Disposition。
- 校验、handler、路由或持久化失败都丢弃完整候选；Store 不观察部分 task、audit、state、status 或 version 变更。

### Handler config preparation

- `NodeHandler` 保持不变，现有自定义 handler 无需修改。
- handler 可选实现 `NodeHandlerConfigPreparer`。`PrepareConfig` 在每次完整 Definition 编译中、每个业务节点恰好调用一次，并承担原 `Validate` 的完整校验职责。
- `PrepareConfig` 返回 `PreparedNodeHandler`；它接收不含 Config 的 `PreparedActivationInput` 和 `PreparedCommandInput`，因此运行时不能意外回退到重复解析 raw JSON。
- compiled Definition plan 同时拥有 canonical Definition、图索引和每节点 prepared executor。Engine 在同一 plan 中复用 executor，运行阶段不再查询 Registry。
- 未实现 preparer 的 handler 由 package-internal compatibility executor 适配：编译时仍调用一次 `Validate`，运行时仍按原接口获得防御性 raw Config 副本。
- Approval 与 Condition 实现 preparer，并把直接调用的旧 Activate/Handle 和 prepared 调用委托给同一 typed-config 业务函数。
- `CompileDefinition`、发布、Start、Handle 和 Return 每次都建立新的 plan；禁止进程级、Registry 级或跨请求 plan/config cache。
- plan、prepared config、handler 和回调永不进入 Definition JSON、Definition repository、Instance snapshot 或 PostgreSQL codec。

## 兼容与迁移

- `Engine`、`Store`、`Registry` 与 `NodeHandler` 的既有调用方式和错误分类保持兼容。
- 自定义 handler 可以永久使用原接口；需要消除重复解析时，再增量实现 `NodeHandlerConfigPreparer`。
- preparer 不应执行缺少 context 的阻塞 I/O。外部解析或目录访问仍应在 ActivatePrepared/HandlePrepared 中执行并遵守 cancellation。
- preparer 返回的 executor 只保证在一个 compiled plan 生命周期内使用；不得注册到全局状态或依赖跨请求可变数据。

## 验证

- activation、command、return 对称覆盖 malformed State、Disposition、Outcome 和 Tasks，并验证失败原子性。
- 自定义 prepared handler 验证每个 Engine 操作重新准备且同一 plan 复用；自定义 legacy handler 验证无需迁移。
- Approval 与 Condition 分别比较 legacy 和 prepared 的 activation/command 结果。
- canonical Definition 的 JSON round-trip 测试证明 prepared executor 未被持久化。

