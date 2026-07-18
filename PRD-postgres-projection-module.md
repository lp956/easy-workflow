# easy-workflow 产品需求文档：深化 PostgreSQL Projection 查询模块

- 状态：Ready for agent
- 类型：PostgreSQL adapter 内部架构深化
- 兼容性目标：保持 Worklist、Participated、Initiated、Cursor、Scope 和分页行为兼容

## Problem Statement

`easy-workflow/postgres` 已提供待办、参与记录和发起记录查询，并通过稳定 keyset pagination、参数化 scope 和事务内投影维护保证一致性。当前查询正确，但 Projection 的公开输入包含多项必须由调用者理解的隐含组合规则：nil scope 与空 scope 含义不同，同一个 Cursor 在 Task 查询和 Instance 查询中要求不同字段，错误组合只能在运行时识别。

在实现内部，Worklist 和 Participated 已共享部分 Task 查询编排，而 Initiated 仍拥有另一套相似的 limit、cursor、scope、SQL 参数、扫描和下一页构造过程。随着查询族增加，这些规则容易复制并产生细微差异。

本需求先深化 Projection module 的内部实现，在不破坏现有公开类型的前提下，把查询族规则、scope 规范化、keyset 编排和结果分页集中管理。公开接口的进一步收窄，例如完全不透明的 continuation token，需要单独的兼容与弃用设计，不属于本轮。

## Solution

在 PostgreSQL adapter 内建立清晰的查询族实现：Task projection family 统一服务 Worklist 与 Participated，Instance projection family 服务 Initiated。每个查询族在同一 module 内拥有输入规范化、cursor 语义、scope 参数化、稳定排序、limit+1 查询、扫描和 Next 构造。

Projection 继续是唯一公开查询入口，并继续借用调用方提供的 PostgreSQL pool。现有 ActorQuery、QueryScope、PageRequest、Cursor、Page、TaskProjection 和 InstanceProjection 保持兼容；现有 nil/empty scope 和 cursor 校验语义保持不变。

不为单元测试引入只有 mock 的新 adapter seam。真实查询行为继续通过 PostgreSQL integration tests 验证；无需数据库的输入校验通过现有公开 Projection 行为覆盖。

## User Stories

1. As an application developer, I want Worklist calls to keep their current inputs and outputs, so that the refactor does not break inbox integration.
2. As an application developer, I want Participated calls to keep their current inputs and outputs, so that completed-work views remain compatible.
3. As an application developer, I want Initiated calls to keep their current inputs and outputs, so that submitted-work views remain compatible.
4. As an application developer, I want existing Cursor values to remain usable with the same query family, so that active pagination sessions do not fail after upgrade.
5. As an application developer, I want invalid cross-family cursors rejected consistently, so that a Task cursor cannot silently corrupt Instance pagination.
6. As an application developer, I want a zero page limit to keep selecting the documented default, so that omitted pagination remains bounded.
7. As an application developer, I want explicit limits outside the supported range rejected, so that query work remains predictable.
8. As an application developer, I want a nil scope to retain its unrestricted meaning, so that applications without an additional scope filter remain compatible.
9. As an application developer, I want a non-nil empty scope to return no rows, so that an authorization result of no instances cannot broaden access.
10. As a security integrator, I want all scope values parameterized, so that trusted identities never become SQL text.
11. As a security integrator, I want Projection to apply but not discover authorization scope, so that tenancy decisions remain host-owned.
12. As a task assignee, I want Worklist to return only my active frozen assignments, so that I see actionable work only.
13. As a workflow participant, I want Participated to return my completed and closed assignments, so that my historical involvement is visible.
14. As a workflow initiator, I want Initiated to return my running and terminal instances, so that I can follow submitted workflows.
15. As a query consumer, I want Definition identity, Instance state, node, task and audit times to remain consistent, so that list rows describe one committed snapshot.
16. As a query consumer, I want stable ordering when audit times tie, so that pagination does not skip or duplicate rows.
17. As a query consumer, I want Next to be nil exactly when no later row exists, so that clients know when pagination is complete.
18. As a query consumer, I want returned Items to be non-nil on success, so that JSON and caller handling remain predictable.
19. As a concurrent caller, I want Projection queries to remain safe with a shared pool, so that host applications can serve requests concurrently.
20. As a PostgreSQL operator, I want Projection construction to perform no connection or migration work, so that lifecycle remains host-controlled.
21. As a PostgreSQL operator, I want command and projection changes committed atomically, so that queries never observe a partial transition.
22. As a PostgreSQL operator, I want existing migrations to remain sufficient, so that an internal query refactor requires no rollout migration.
23. As a workflow maintainer, I want Task query normalization owned by one query-family module, so that Worklist and Participated cannot drift.
24. As a workflow maintainer, I want Instance query normalization owned by one query-family module, so that Initiated has a clear source of truth.
25. As a workflow maintainer, I want cursor validation, SQL ordering and Next construction colocated, so that pagination invariants have locality.
26. As a workflow maintainer, I want scope normalization colocated with SQL argument construction, so that nil and empty meanings remain consistent.
27. As a workflow maintainer, I want shared limit and cancellation behavior, so that every query family has the same resource bounds.
28. As a workflow maintainer, I want scanning and cursor creation to use the same ordering keys, so that continuation cannot diverge from SQL order.
29. As a test maintainer, I want real PostgreSQL integration tests to remain the query test surface, so that tests cover actual SQL and driver behavior.
30. As a test maintainer, I want input validation covered without database I/O where possible, so that obvious failures stay fast.
31. As a contributor, I want a clear pattern for adding a future query family, so that new queries reuse depth instead of copying orchestration.
32. As a contributor, I want no generic repository or mock-only executor seam, so that testability does not fragment query locality.
33. As a release maintainer, I want public query documentation and examples to remain valid, so that users can upgrade without migration guidance.
34. As a release maintainer, I want skipped PostgreSQL tests reported explicitly, so that missing infrastructure is not mistaken for coverage.

## Implementation Decisions

- Keep Projection as the public PostgreSQL-specific read adapter and retain the caller-owned pool lifecycle.
- Preserve Worklist, Participated and Initiated method behavior and stable error classification.
- Preserve ActorQuery, QueryScope, PageRequest, Cursor, Page, TaskProjection and InstanceProjection public shapes.
- Preserve nil scope as no additional instance restriction and non-nil empty scope as deny all instances.
- Preserve current page limits, default limit and invalid-limit behavior.
- Preserve current cursor field semantics for compatibility: Task queries require all Task ordering keys, while Instance queries reject Task-specific keys.
- Deepen one internal Task query-family module shared by Worklist and Participated. It owns validation, scope normalization, keyset arguments, scanning and page construction for task rows.
- Deepen one internal Instance query-family module for Initiated with the same responsibility at the instance-row level.
- Share only behavior that is truly identical across query families, such as bounded limit normalization and context handling. Do not force incompatible cursor shapes through generic type erasure.
- Keep SQL parameterized. Instance IDs, actors, cursors and limits never participate in SQL string composition.
- Keep stable total ordering based on audit time plus deterministic identity tie-breakers.
- Keep limit+1 look-ahead as the mechanism for determining whether Next exists.
- Keep result values detached and caller-owned.
- Keep projection writes inside the Store command transaction; read-side refactoring must not alter write atomicity.
- Do not add a mock-only database executor seam. One adapter remains hypothetical; real PostgreSQL behavior is tested with PostgreSQL.
- Do not redesign public pagination into opaque tokens in this PRD. That requires a separate compatibility and deprecation decision.
- Do not change schema, migrations, projection tables or stored ordering columns.
- Remove only duplicated query orchestration and code made unused by the deeper query-family modules.

## Testing Decisions

- The highest test seams are Projection.Worklist, Projection.Participated and Projection.Initiated.
- Good tests assert returned rows, stable ordering, page boundaries, scope enforcement, error classification and transactional visibility. They do not assert private helper names or SQL formatting.
- Existing PostgreSQL query integration tests are prior art for active work, participation, initiated instances, equal-time pagination, withdrawal, return, transfer and dynamic assignments.
- Retain tests proving a Task cursor can continue Worklist and Participated only under their documented ordering.
- Retain tests proving Task cursors are rejected by Initiated and incomplete cursors are rejected before query execution.
- Add or retain tests for default, minimum, maximum and invalid limits across every query family.
- Add or retain tests proving nil scope applies no additional filter.
- Add or retain tests proving a non-nil empty scope returns an empty page without broadening access.
- Add or retain tests proving a populated scope returns only authorized Instance identities.
- Add or retain tests proving parameter values containing SQL metacharacters are handled as data.
- Add or retain tests proving equal timestamps use Instance and Task identities as deterministic tie-breakers.
- Add or retain tests proving no row is skipped or duplicated across consecutive pages.
- Add or retain tests proving Next is derived from the last returned row and is nil at the final page.
- Retain transaction rollback tests proving failed command persistence produces no projection changes.
- Retain lifecycle tests proving withdrawal, return and transfer update worklist and participation views consistently.
- Cover validation failures without a live database when the public call can reject before pool acquisition or SQL execution.
- Use real PostgreSQL integration tests for SQL, scanning, numeric version decoding and keyset behavior; do not replace these with mocked rows.
- Run `go test ./...`, `go test -race ./...` and `go vet ./...`.
- Run PostgreSQL integration tests with an explicit DSN and report them as skipped when the DSN is unavailable.

## Out of Scope

- Breaking or removing Cursor, QueryScope, PageRequest or ActorQuery fields.
- Introducing an opaque continuation-token public contract.
- Adding non-PostgreSQL Projection adapters.
- Adding a generic query repository interface or mock-only executor interface.
- Changing Store, Engine, Instance, Task or Audit contracts.
- Changing projection tables, indexes, migrations or transaction timing.
- Adding new query families, search syntax, full-text search or arbitrary sorting.
- Discovering tenants, authorizing actors or loading organization data inside Projection.
- Changing Definition compilation or Definition repository architecture.
- Performance tuning unrelated to consolidating query-family orchestration.

## Further Notes

- This PRD intentionally delivers internal depth before public interface reduction. A future opaque continuation design must include compatibility and deprecation policy.
- Worklist and Participated already demonstrate a real shared Task query family; the refactor should deepen that implementation rather than add another wrapper.
- PostgreSQL remains the only Projection adapter. Following the one-adapter rule, no new production seam is introduced solely for tests.
- The repository has no configured external issue tracker, domain glossary or ADR directory. This PRD is stored locally with `Ready for agent` status.

