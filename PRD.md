# easy-workflow 产品需求文档：可发布、可持久化、可视化定义的人工审批流框架

- 状态：Ready for agent
- 产品定位：可嵌入 Go 应用的人工审批流框架
- 交付方式：第三方 Go package，加可选官方 extension 与 adapter
- 目标定义方式：代码 Builder 与 Web 拖拽生成的 JSON 共用同一 canonical Definition

## Problem Statement

`easy-workflow` 已具备一个可靠的人工审批流内核：它能够校验 DAG、冻结实例定义、执行节点流转、生成和关闭任务、通过乐观版本处理并发，并记录不可变审计；官方 Approval extension 已支持或签、会签和终态拒绝。

当前骨架仍不足以形成易于安装和投入实际业务的第三方框架：

- Definition 可以被构建和解析，但缺少统一的发布语义、不可变版本分配和持久化管理。代码入口、Web 入口和未来 HTTP adapter 容易分别实现自己的发布流程。
- Definition 的图校验、handler 配置校验和运行期路由索引分散在 Definition module 与 Engine module，完整的“可执行定义”尚未形成一个高 leverage 的发布结果。
- Store seam 只有 MemoryStore 一个 adapter，尚未用 durable adapter 证明原子快照、任务、审计和 CAS 能在生产数据库中保持同一契约。
- Web JSON 可以表达 DAG，却没有统一、受限且可验证的条件语义。
- Instance 只接受 task command。撤回状态已经存在，但撤回、退回和转派尚无完整的领域操作、授权规则、任务关闭语义和审计语义。
- Approval definition 只能冻结具体 ActorID，不能在保持组织目录外置的前提下表达角色、主管、发起人或业务字段等 assignment policy。
- 缺少待办、已办、参与人和审计查询投影，但这些查询能力不应扩大核心 Store interface。

用户需要的不是绑定 HTTP、Redis、MySQL 和用户目录的工作流微服务，而是一个极易安装、默认体验简单、扩展路径明确的 Go package。框架应让最小审批流几分钟内可运行，同时让生产应用能够逐步接入定义发布、数据库持久化、条件路由、组织解析和 Web 设计器，而不重写核心执行模型。

## Solution

保持现有 Engine、NodeHandler、Store 和 Approval extension 的职责，围绕它们补齐四类深 module：

1. **Definition 发布与编译 module**
   - 接受代码 Builder 或 Web JSON 生成的同一 Definition。
   - 在发布时完成图校验、所有 node config 的 handler 校验、不可变版本分配和执行计划编译。
   - 发布成功后得到稳定的 Definition ID 与 Version；已发布版本不可原地覆盖。
   - Instance 启动时冻结已发布 Definition 的完整快照，运行中的 Instance 不受新版本影响。
   - Engine 消费可信执行计划，不让 Web、HTTP 或业务调用方重复解释路由规则。

2. **Durable Store adapter 与 Store 契约测试**
   - 保持 Store 的 command-side interface 小而稳定，只负责 Instance 聚合快照的 Create、Load 和 CAS Save。
   - 第一个官方 durable adapter 在一个数据库事务中保存 Instance、Task、NodeState 和 Audit。
   - 通过条件版本更新保证多进程、多副本并发安全，不使用进程内 mutex 代替数据库并发控制。
   - 提供所有 adapter 可复用的契约测试，MemoryStore 与 durable adapter 必须表现一致。
   - 定义仓库、待办查询和历史查询不加入核心 Store interface。

3. **官方 Condition extension**
   - 使用受限、可序列化、可静态验证的条件 DSL。
   - 条件只读取 Instance business data，不执行任意 Go、JavaScript、模板或脚本代码。
   - 条件求值产生一个 outcome，继续复用现有 DAG edge 路由。
   - 明确定义数据类型、操作符、默认分支、唯一命中、无匹配和错误语义。
   - Definition 发布时验证 Condition config，运行时只执行已验证规则。

4. **分阶段的人工审批生命周期能力**
   - 第一阶段保持现有终态拒绝，同时允许未来通过 rejected outcome 选择显式图路由。
   - 第二阶段先实现撤回，再实现退回；所有操作必须校验 Instance 状态、操作者身份和策略。
   - 撤回、退回和未来转派都必须关闭或替换相关 active task，并在同一次 CAS 提交中写入 Audit。
   - 只有第二种 Instance command 出现后，才提取公共 command 执行 module，避免提前制造 shallow module。
   - Approval assignment policy 和组织目录解析留在 Approval extension 与 adapter；核心只保存最终冻结的 ActorID task。

### 交付优先级

- **P1**：Definition 发布与编译、Definition 不可变版本、durable Store adapter、Store 契约测试。
- **P1**：官方 Condition extension，满足代码定义与 Web JSON 的条件路由需求。
- **P2**：撤回、rejected outcome 路由、退回及相应授权和审计。
- **P2**：assignment policy、参与人记录、待办/已办/抄送查询投影。
- **P3**：转派；只有在参与人和授权模型稳定后实施。

### 产品成功标准

- 新用户能够只依赖 core、MemoryStore 和官方 Approval extension 运行完整审批示例，无需数据库、HTTP 或组织目录。
- 同一份 Definition 可以从 Builder 构建，也可以由 Web JSON 解析并发布；二者产生相同的执行语义。
- 发布失败不会留下部分 Definition、错误版本或可启动的无效定义。
- 运行中的 Instance 始终使用启动时冻结的 Definition 版本。
- 两个并发命令针对同一 Instance 时最多一个提交成功，另一个得到稳定的 version conflict。
- durable adapter 中 Instance 状态、Task 和 Audit 不会部分提交。
- Condition 对相同输入始终得到确定结果；重叠、无匹配和类型错误具有明确行为。
- core package 不依赖 HTTP framework、具体数据库库、Redis、用户目录或表达式脚本运行时。

## User Stories

1. As a Go application developer, I want to install the workflow core as a normal Go package, so that I can embed approvals without deploying a separate workflow microservice.
2. As a Go application developer, I want a complete in-memory example, so that I can understand and run the framework without infrastructure setup.
3. As a workflow author, I want code Builder and JSON to share one Definition model, so that I can move between code-authored and Web-authored workflows without conversion logic.
4. As a workflow author, I want invalid graphs rejected before publication, so that cycles, unreachable nodes and dead-end branches never reach production.
5. As a workflow author, I want node configuration validated by its registered handler before publication, so that malformed Approval or Condition configuration cannot create a broken Instance.
6. As a workflow author, I want every published Definition to receive an immutable version, so that I can identify exactly which logic an Instance uses.
7. As a workflow author, I want to publish a new version without replacing the old version, so that existing Instances remain deterministic.
8. As a workflow administrator, I want to retrieve a Definition by ID and Version, so that I can inspect and reproduce historical execution semantics.
9. As a workflow administrator, I want to identify the latest published version, so that new Instances can start from the intended version without changing older Instances.
10. As a Web designer developer, I want the Definition JSON to contain data only, so that browser-produced definitions never embed executable Go callbacks.
11. As a Web designer developer, I want publication errors to identify the failing node and rule, so that the UI can present actionable validation feedback.
12. As a Web designer developer, I want deterministic JSON semantics independent of slice position, so that drag-and-drop editing and JSON reordering do not change execution.
13. As an application developer, I want the Engine to consume a validated published Definition, so that each application does not reimplement publication rules.
14. As an application developer, I want an Instance to freeze its Definition snapshot at start, so that later publication cannot change a running approval.
15. As an application developer, I want business data and node state to remain opaque JSON owned by business and extension modules, so that the core stays domain-neutral.
16. As an extension author, I want a constrained NodeHandler seam, so that I can add business nodes without accessing Store or forcing arbitrary graph jumps.
17. As an extension author, I want validation, activation and command handling to be separately defined behaviors, so that invalid configuration fails before durable state is created.
18. As an approval author, I want or-sign to close sibling tasks after the first decision, so that remaining actors cannot approve a completed node.
19. As an approval author, I want countersign to wait for every frozen assignee, so that the approval population cannot drift during execution.
20. As an approval author, I want one rejection to produce an auditable rejection result, so that terminal rejection remains simple by default.
21. As an approval author, I want an optional rejected outcome route, so that a Definition can send rejected work to a correction node instead of always terminating.
22. As a workflow author, I want a serializable Condition extension, so that Web JSON can express business routing without custom Go handlers for every branch.
23. As a workflow author, I want typed condition operands and operators, so that numeric, string, boolean and collection comparisons are not ambiguous.
24. As a workflow author, I want explicit default-branch behavior, so that missing condition matches do not silently select an arbitrary edge.
25. As a workflow author, I want overlapping condition matches rejected or resolved by a documented deterministic rule, so that branch order cannot accidentally change execution.
26. As a security reviewer, I want conditions unable to execute arbitrary code, so that workflow authors cannot turn configuration into remote code execution.
27. As an application operator, I want a durable Store adapter, so that Instance state survives process restarts.
28. As an application operator, I want all state changes, tasks and audit records committed atomically, so that operational failures cannot create contradictory workflow state.
29. As an application operator, I want optimistic concurrency enforced by the durable store, so that multiple application replicas can safely handle commands.
30. As an adapter author, I want reusable Store conformance tests, so that I can prove my adapter matches the core ownership, atomicity and CAS contract.
31. As an adapter author, I want Load to return caller-owned snapshots, so that callers cannot mutate durable state without Save.
32. As an adapter author, I want context cancellation propagated, so that database work stops when its request is cancelled.
33. As an Instance initiator, I want to withdraw a running request when policy permits, so that I can correct or abandon a request before further approval.
34. As an Instance initiator, I want withdrawal to close active tasks and record actor, time and affected node, so that no stale task remains actionable.
35. As an approver, I want a rejected item to return to an explicitly permitted earlier node when configured, so that correction flows do not rely on implicit “previous step” behavior.
36. As an auditor, I want every return to preserve previous tasks and create a new task round, so that historical decisions are never overwritten.
37. As a workflow administrator, I want return targets validated against policy and execution history, so that callers cannot jump to arbitrary nodes.
38. As a task owner, I want a future transfer action to identify the old and new assignee and reason, so that delegation remains attributable.
39. As a security reviewer, I want lifecycle actions authorized from server-established identity, so that request bodies cannot impersonate initiators or assignees.
40. As an organization adapter author, I want Approval assignment policy resolved outside the core, so that users, roles and departments remain owned by the host application.
41. As a workflow author, I want to use either explicit assignees or a supported assignment policy, so that simple workflows remain simple while organization-aware workflows remain portable.
42. As an approver, I want resolved assignees frozen when the node activates, so that later directory changes do not rewrite an in-flight approval population.
43. As a workflow administrator, I want candidate, participant and notifier information available as query projections, so that I can build inbox, completed-work and notification views.
44. As an application developer, I want query projections separate from command persistence, so that adding search and pagination does not enlarge the core Store interface.
45. As an auditor, I want audit order to be authoritative even when timestamps are equal, so that every accepted transition has a reproducible causal sequence.
46. As an auditor, I want completed Instances retained without cron-based table migration, so that history cannot be lost or duplicated by non-atomic copying.
47. As a security reviewer, I want database adapters to use parameterized queries, so that workflow identity and filter input cannot become SQL code.
48. As a library maintainer, I want HTTP, Web UI and database dependencies kept outside core, so that importing the package has no configuration or network side effects.
49. As a library maintainer, I want new seams added only when at least two behaviors or adapters genuinely vary, so that the codebase does not accumulate hypothetical abstractions.
50. As a library maintainer, I want public behavior tested through the highest stable seam, so that internal refactors do not require rewriting behavior tests.

## Implementation Decisions

- The product remains an artificial human-approval workflow framework, not a general durable background-job orchestrator.
- Definition, Instance, Task and Audit remain separate domain concepts.
- Code Builder and JSON remain two authoring paths into the same canonical Definition.
- Definition publication becomes the single source of truth for complete validation and immutable version assignment.
- Definition versions are monotonically increasing within one stable Definition ID and cannot be updated in place after publication.
- An Instance stores or can reconstruct the complete published Definition snapshot needed for deterministic execution.
- The execution graph remains a DAG with explicit edges selected by handler outcomes. The recursive tree and linear `step++/step--` execution model are rejected.
- The internal execution plan may maintain node and route indexes, but those indexes are implementation details and are not serialized as the public Definition format.
- NodeHandler remains the extension seam. Handlers receive defensive data and return declarative NodeResult values; they do not receive Store access or arbitrary target node control.
- Approval remains an official extension rather than becoming a hard-coded node type inside the Engine.
- Condition is implemented as an official extension that returns a normal outcome. The Engine receives no condition-language logic.
- The condition language is allow-listed and typed. It does not support arbitrary script evaluation, reflection-based method invocation or external I/O.
- Store remains the Instance command-side persistence seam with Create, Load and atomic CAS Save semantics.
- Definition persistence is a separate module from Instance Store because its lifecycle is publish/read, not command mutation.
- Query projections are separate from Store. Inbox, completed-work, participant, notifier and audit search can use database-specific read models without changing Engine persistence.
- A durable Store Save commits Instance state, Task state, NodeState and Audit in one database transaction.
- Durable concurrency uses database-supported conditional version updates or equivalent transactional protection. Process-local mutexes are not a correctness mechanism.
- Store adapters must return stable domain errors for duplicate Instance, missing Instance and version conflict.
- Audit remains append-only within accepted state transitions. Existing audit records are never edited to represent a later action.
- Terminal rejection remains the initial default. Rejected routing is an explicit Definition choice, not implicit movement to the previous array element.
- Withdrawal is the first new Instance lifecycle command. It is allowed only for a running Instance and only when host policy authorizes the actor.
- Return is explicit and target-based. It creates a new task round and preserves prior task and audit history.
- Transfer is deferred until assignment and authorization semantics are stable.
- The common Instance command execution implementation is extracted only when a second command requires the same load, policy, transition, audit and CAS sequence.
- Static assignees remain supported permanently as the simplest Approval configuration.
- Dynamic assignment policy resolves through an organization adapter owned by the host application or Approval extension. The core stores only frozen ActorID assignments.
- Authentication and tenant discovery belong to the host application's transport adapter. ActorID supplied to core operations must come from a trusted caller context.
- HTTP endpoints and the Web designer are separate deliverables that consume the package; they are not imported by core.
- No source code is copied from `go-workflow/go-workflow`. Its participation and history concepts may inform domain semantics, but its unlicensed, inactive implementation is not a dependency or code source.

## Testing Decisions

- Tests assert observable behavior through the highest stable seam. They do not assert private maps, traversal algorithms, database table names or helper call order.
- Definition publication tests cover Builder and JSON equivalence, graph validation, handler config validation, immutable version allocation and zero durable writes after failed publication.
- Definition compatibility tests confirm a running Instance is unaffected when a newer Definition version is published.
- Engine tests continue to use public Start and command behavior, covering task ownership, handler outcomes, terminal states, audit order and error propagation.
- Approval tests cover or-sign, countersign, duplicate assignee rejection, actor/task mismatch, repeated decisions, sibling task closure and rejection behavior.
- Condition tests are table-driven and cover every supported value type and operator, default branch, unique match, overlapping match, missing data, type mismatch and invalid config.
- Store contract tests run unchanged against MemoryStore and every durable adapter.
- Store contract tests cover duplicate Create, not-found Load, defensive snapshot ownership, context cancellation, stale Save, successful version increment and failure atomicity.
- Durable adapter integration tests verify Instance, Task, NodeState and Audit commit or roll back together.
- Concurrency tests issue competing commands from independently loaded snapshots and prove that at most one CAS write succeeds.
- Race tests remain part of normal verification for MemoryStore, Registry and stateless official handlers.
- Withdrawal tests cover authorized and unauthorized actors, non-running Instance, already-decided tasks, task closure, audit content and version conflict.
- Return tests cover permitted and forbidden targets, preserved historical tasks, new task rounds, branch history and concurrent commands.
- Assignment policy tests cover resolver errors, empty results, duplicates, frozen results and directory changes after activation.
- Security tests verify untrusted JSON cannot select an unregistered handler, invoke arbitrary code, bypass actor/task ownership or inject database query syntax.
- Existing public examples remain executable tests and continue to require no external infrastructure.
- Baseline repository verification remains `go test ./...`, `go test -race ./...` and `go vet ./...`; durable adapter integration checks run separately when external infrastructure is required.

## Out of Scope

- A bundled HTTP server or mandatory REST interface.
- A bundled Web drag-and-drop application; this PRD only guarantees the Definition JSON contract it will consume.
- User, role, department, tenant or organization-directory management inside core.
- General-purpose background activities, worker queues, retries, leases, heartbeat, timeout compensation or distributed job cancellation.
- Cyclic workflow graphs, arbitrary loops, parallel gateways, joins, subflows and BPMN compatibility.
- Arbitrary Go, JavaScript, CEL-like unrestricted programs, templates or shell expressions in Condition configuration.
- Redis as a required identity, token or persistence dependency.
- Moving completed runtime rows into history tables through periodic cron jobs.
- Event sourcing as the initial persistence model; append-only Audit plus atomic aggregate snapshots are sufficient for this scope.
- Multiple official durable database adapters in the first iteration.
- Transfer before withdrawal, return, assignment and authorization semantics are stable.
- Automatic retries after version conflict; the caller must reload and decide whether the original command remains meaningful.
- Copying source code from the reference `go-workflow` repository.

## Further Notes

- The first implementation iteration should stop after Definition publication/versioning and one durable Store adapter pass their contract tests. Condition can follow as a separate independently releasable extension.
- The specific database used by the first official durable adapter has not been selected in the current discussion. That choice affects packaging and integration tests but must not change the Store domain contract.
- The repository currently has no configured issue tracker, remote repository, domain glossary or ADR directory. This PRD is therefore stored locally with `Ready for agent` status instead of being published with a `ready-for-agent` tracker label.
- When the first lifecycle command beyond task handling is implemented, re-run the deletion test before extracting a common command module. Until then, the existing Engine implementation has better locality than a speculative abstraction.
- When the first dynamic assignment policy is implemented, require a real second adapter before introducing a new organization seam. Static assignees alone do not justify it.
- Release documentation should present capabilities in layers: core-only quick start, official extensions, durable adapters, then optional transport and Web integrations.
