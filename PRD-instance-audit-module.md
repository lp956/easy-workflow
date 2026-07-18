# easy-workflow 产品需求文档：深化 Instance、Task 与 Audit 事实模块

- 状态：Ready for agent
- 类型：内部架构深化
- 兼容性目标：保持现有公开行为、持久化格式和错误分类兼容

## Problem Statement

`easy-workflow` 已支持流程启动、任务处理、撤回、退回、转派、乐观并发、追加式 Audit，以及 Memory 和 PostgreSQL 两种 Store adapter。随着生命周期行为增加，运行中 Instance 的合法状态变化开始分散在 Engine 的多个操作、Task 处理逻辑、Audit 构造逻辑、Store adapter 防御性检查和 PostgreSQL 查询投影中。

从使用者角度看，现有功能可以工作，但继续增加生命周期行为时，维护者必须同时理解并正确修改 Instance 状态、当前节点、Task 历史、NodeState、Audit、Version 和查询投影。任何一个位置遗漏，都可能产生状态已经变化但 Audit、Task 历史或查询结果不同步的问题。这种风险会随着新命令数量增长，而不是被一个稳定 module 吸收。

当前公开 Instance 仍是可序列化 snapshot，这是嵌入式库和 Store adapter 所需要的兼容面；本需求不把它替换为新的公开模型。需要深化的是内部实现：让 Engine 产生的每一次状态变化都通过一个拥有明确不变量的 Instance facts module 完成，同时保留 Store adapter 在持久化 seam 上的防御性契约。

## Solution

建立一个内部 Instance facts module，集中处理 Engine 接受命令后的合法状态变化，包括节点进入、任务轮次变化、任务关闭与替换、实例终态、Audit 事实追加和 Version 递增所需的不变量。

Engine 继续提供现有 Start、Handle、Withdraw、Return 和 Transfer 行为，并继续负责依赖协调、输入校验、policy 调用、NodeHandler 调用、Store Load/Create/Save 和 CAS 顺序。内部 facts module 接收已经完成业务判断的候选变化，生成一致的 Instance snapshot；失败时不产生可持久化的部分变化。

Store interface、NodeHandler interface、公开请求类型、公开 Instance JSON、Audit JSON 和 PostgreSQL schema 保持兼容。Memory 与 PostgreSQL adapters 继续在各自持久化 seam 上执行 Store 契约要求的防御性检查，并通过同一套 Store contract tests 保持行为一致。

## User Stories

1. As a workflow application developer, I want existing Engine operations to keep their current behavior, so that an internal refactor does not require changes in my application.
2. As a workflow application developer, I want existing public request and result types to remain compatible, so that I do not need to rewrite integration code.
3. As a workflow application developer, I want existing serialized Instance data to remain readable, so that deployed snapshots survive the architecture change.
4. As a workflow application developer, I want stable errors to remain classifiable with `errors.Is`, so that current error handling continues to work.
5. As a workflow initiator, I want a newly started Instance to record its start, first node entry, tasks, status and version consistently, so that every view describes the same execution.
6. As a task assignee, I want an accepted task command to update the task, node result, Audit and version as one logical transition, so that I never observe a partial decision.
7. As a task assignee, I want rejected or invalid commands to leave the durable Instance unchanged, so that failed attempts do not corrupt workflow history.
8. As a workflow initiator, I want withdrawal to close every active task and append the corresponding Audit fact consistently, so that no withdrawn Instance retains actionable work.
9. As an authorized operator, I want return to preserve historical tasks while creating a fresh task round at the target node, so that prior work remains auditable.
10. As an authorized operator, I want transfer to close the old assignment and create a new assignment without rewriting history, so that ownership changes remain traceable.
11. As a former assignee, I want a transferred task to stop accepting my commands, so that historical ownership cannot be used as current authority.
12. As a new assignee, I want the replacement task to behave like any other active task, so that transfer does not create a special execution path.
13. As an auditor, I want Audit records to remain append-only and ordered, so that accepted history cannot be silently rewritten.
14. As an auditor, I want every lifecycle fact to retain its existing action name and required attribution, so that downstream consumers remain compatible.
15. As an auditor, I want return and transfer reasons to remain attached to the accepted transition, so that the durable explanation matches the state change.
16. As a workflow maintainer, I want node entry, completion, rejection, withdrawal, return and transfer facts to be constructed in one module, so that fact semantics have one source of truth.
17. As a workflow maintainer, I want Task lifecycle rules to be concentrated, so that active, completed and closed states are not independently reinterpreted by every Engine operation.
18. As a workflow maintainer, I want Version to advance exactly once for each accepted command, so that CAS behavior remains predictable.
19. As a workflow maintainer, I want transition failures to discard the complete candidate snapshot, so that mutation order cannot leak partial state to Store.
20. As a workflow maintainer, I want the existing command execution module to remain responsible for Load, running-state checks and CAS Save, so that already-centralized persistence ordering is not duplicated.
21. As a NodeHandler author, I want the NodeHandler contract to remain unchanged, so that existing Approval, Condition and host handlers continue to compile and run.
22. As a policy adapter author, I want policies to continue receiving defensive pre-transition snapshots, so that authorization cannot accidentally mutate the persisted candidate.
23. As a Store adapter author, I want Create, Load and Save contracts to remain unchanged, so that existing adapters do not require a public migration.
24. As a Store adapter author, I want shared contract tests to continue proving defensive ownership, append-only Audit and CAS behavior, so that adapters remain interchangeable at the Store seam.
25. As a PostgreSQL adapter user, I want aggregate snapshots and query projections to commit in the same transaction, so that command and query views remain consistent.
26. As a PostgreSQL adapter user, I want existing migrations and persisted rows to remain valid, so that the refactor does not require a database migration.
27. As a query consumer, I want worklist, participation and initiated projections to retain their existing results, so that internal fact handling does not change query semantics.
28. As a concurrent caller, I want competing commands with the same expected version to commit at most once, so that the refactor preserves optimistic concurrency safety.
29. As a library maintainer, I want MemoryStore and PostgreSQL Store to observe the same transition outcomes, so that development and production adapters do not diverge.
30. As a library maintainer, I want tests to exercise behavior through Engine and Store interfaces, so that internal module organization can evolve without rewriting tests.
31. As a library maintainer, I want race checks to cover concurrent Engine, Registry, handler and MemoryStore use, so that concentrating state logic does not introduce shared mutable state.
32. As a contributor, I want the internal facts module to have a narrow responsibility, so that future lifecycle commands can reuse it without becoming a generic command framework.
33. As a contributor, I want new lifecycle behavior to declare its domain-specific policy and transition while reusing common Instance facts, so that new work adds behavior rather than persistence choreography.
34. As a contributor, I want unrelated Definition compilation and Projection interface changes excluded, so that this refactor remains reviewable and independently verifiable.
35. As a release maintainer, I want all existing examples to remain executable, so that the documented quick start continues to represent supported behavior.

## Implementation Decisions

- Introduce one internal Instance facts module as the owner of Engine-generated aggregate mutations. It is an implementation module and does not add a new public interface.
- Preserve the existing Engine public operations and their domain-specific request and policy contracts.
- Preserve the existing internal command execution module as the owner of Load, running-state enforcement, preparation order, one Version increment, CAS Save and detached result ownership.
- Keep authorization outside the facts module. Host policies continue to run before candidate mutation and continue to receive defensive pre-transition snapshots.
- Keep NodeHandler behavior outside the facts module. Handlers continue to return declarative NodeResult values without accessing Store or choosing arbitrary graph targets.
- Concentrate Task state invariants used by Engine transitions: activation creates new identities, task decisions replace only the current node-owned task view, withdrawal closes active work, return preserves historical rounds, and transfer closes one assignment before appending its replacement.
- Concentrate Audit fact construction used by Engine transitions, including stable action names, actor attribution, node and task identity, transition target, assignment changes, reason, captured node state and UTC timestamp.
- Preserve every existing Audit action name and JSON field. This PRD does not introduce a new public event taxonomy or rewrite historical records.
- Preserve append-only Audit ordering. A successful candidate may only append facts; it may not remove, reorder or modify the durable prefix.
- Preserve the existing public Instance snapshot and JSON representation. Callers continue to receive detached snapshots and may mutate their copies without affecting Store-owned state.
- Preserve Store as the command-side persistence seam with Create, Load and CAS Save. Search, pagination and Definition publication remain outside this interface.
- Keep adapter-level defensive validation required by the Store contract. Centralizing Engine transitions does not allow adapters to trust arbitrary callers that invoke Store directly.
- Use the existing Store contract suite as the single behavioral specification for Memory and durable adapters; do not duplicate adapter-specific interpretations of the public contract in tests.
- Preserve PostgreSQL transactional behavior: aggregate parent data, Definition snapshot, business data, NodeState, Tasks, Audit and query projections commit or roll back together.
- Do not require a PostgreSQL schema migration. If implementation discovery proves a migration unavoidable, stop and revise this PRD before changing schema.
- Preserve projection semantics. Projection derivation may consume concentrated facts, but public query results, pagination and scope rules do not change in this work.
- Preserve stable error sentinels and wrapping behavior. Internal module errors must map to the existing public classifications at the current Engine seam.
- Do not add hidden retries after version conflict. Callers continue to reload and decide whether the original command is still meaningful.
- Do not introduce shared mutable Instance caches. Each Engine command continues to operate on one caller-owned loaded snapshot.
- Do not introduce a generic event bus, command bus or plugin protocol. The module exists to concentrate current Instance facts and invariants, not to create a new extension seam.
- Implement the change incrementally by transition family while keeping the full public test suite green after each step. No transition family is considered migrated until its direct mutation logic has been removed from the old location.
- Remove only code made unused by this refactor. Unrelated formatting, naming, Definition compilation and query-interface cleanup are excluded.

## Testing Decisions

- The highest test seam is the existing Engine public behavior. Tests exercise Start, Handle, Withdraw, Return and Transfer rather than calling internal facts functions directly.
- Good tests assert externally visible snapshots, Task history, Audit history, Version, stable errors and durable atomicity. They do not assert internal module names, helper call order or private data structures.
- Existing Engine approval tests remain the prior art for start, task decision, completion and rejection behavior.
- Existing withdrawal, return and transfer suites remain the prior art for lifecycle authorization, Task closure, historical preservation, Audit attribution, stale versions and terminal-state rejection.
- Existing Definition compilation tests continue to prove that invalid definitions and handler configuration fail before persistence. Compilation architecture itself is not changed by this PRD.
- The reusable Store contract suite remains the test surface for Create, Load, Save, defensive snapshot ownership, context cancellation, append-only Audit and CAS behavior.
- Run the Store contract unchanged against MemoryStore and PostgreSQL Store. A test must not branch on adapter implementation.
- Add or retain tests proving that an accepted transition increments Version exactly once and a rejected transition leaves the durable Version unchanged.
- Add or retain tests proving that transition errors do not persist partial Task, NodeState, status or Audit changes.
- Add or retain tests proving that withdrawal closes all active tasks without rewriting completed or historical tasks.
- Add or retain tests proving that return closes only source-active work, preserves prior rounds and creates a new target round with new Task identities.
- Add or retain tests proving that transfer preserves the old assignment as closed history and appends one active replacement.
- Add or retain tests proving that task decisions cannot introduce unknown Tasks, omit required current-node Tasks or mutate Tasks owned by another node.
- Add or retain tests proving that Audit prefixes cannot be removed, reordered or rewritten through either Store adapter.
- Add or retain tests proving that every existing Audit action and attribution field remains backward compatible.
- PostgreSQL integration tests continue to verify that aggregate and query projection changes commit and roll back in one transaction.
- PostgreSQL query tests continue to verify worklist, participation and initiated views after task decisions, withdrawal, return and transfer.
- Concurrency tests continue to issue competing commands from independently loaded snapshots and prove that at most one CAS write succeeds.
- Race tests continue to cover MemoryStore, Registry and stateless handlers. The internal facts module must not require package-global mutable state.
- Existing executable examples remain tests and must produce the same output.
- Baseline verification is `go test ./...`, `go test -race ./...` and `go vet ./...`.
- Durable PostgreSQL verification is run separately with an explicitly supplied test DSN; skipped integration tests must be reported rather than treated as executed coverage.

## Out of Scope

- Changing or removing the public Instance struct.
- Adding a new public transition, facts or aggregate interface.
- Changing Engine method signatures or lifecycle request and policy types.
- Changing the Store or NodeHandler interfaces.
- Redesigning Definition validation, compilation, publication or execution-plan persistence.
- Changing Projection cursor, scope, pagination or query interfaces.
- Adding a durable Definition repository adapter.
- Adding new workflow lifecycle features or new Task states.
- Changing authorization, organization-directory or assignment-policy semantics.
- Renaming Audit actions, changing Audit JSON fields or migrating historical Audit data.
- Event sourcing, event buses, command buses, general-purpose reducers or cross-process caches.
- PostgreSQL schema changes, automatic migrations or new infrastructure dependencies.
- HTTP transports, Web UI, authentication, tenancy discovery or organization management.
- Automatic retry after version conflict.
- Performance optimization unrelated to concentrating Instance facts and invariants.

## Further Notes

- The repository currently has no configured external issue tracker, domain glossary or ADR directory. This PRD is therefore stored locally with `Ready for agent` status.
- The existing internal command execution module already passed the deletion test by concentrating Load, status checks, Audit ordering, Version advancement and CAS Save. This PRD deepens Instance facts beneath that orchestration; it does not replace the command module.
- Store has two real adapters and a reusable contract test surface, so its seam is established and must remain stable.
- NodeHandler has multiple real adapters and remains the extension seam for Approval, Condition and host-defined node behavior.
- Adapter-level append-only checks remain necessary because Store is public and may be called outside Engine. The architecture improvement is one source of truth for Engine-generated transitions plus one shared behavioral contract for defensive adapter enforcement.
- The remaining architecture-review candidates—Definition compilation locality, Projection query depth and Definition repository seam validation—should be handled by separate PRDs if selected.
