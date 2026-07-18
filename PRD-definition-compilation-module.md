# easy-workflow 产品需求文档：深化 Definition 校验与编译模块

- 状态：Ready for agent
- 类型：内部架构深化
- 兼容性目标：保持 canonical Definition JSON、公开编译行为和 Engine 行为兼容
- 后续深化：request-local handler config preparation 已由 [Runtime deep modules 决策](docs/architecture/runtime-deep-modules.md) 接续；本 PRD 中将其列为 out of scope 仅描述当时交付边界。

## Problem Statement

`easy-workflow` 已经拥有统一的 Definition 类型、Builder、JSON 解析、图校验、handler config 校验和内部执行计划。当前实现能够正确工作，但“结构有效的 Definition”和“当前 Registry 下可执行的 Definition”分别由相邻入口表达，图索引和遍历数据又在多个校验阶段重复派生。

从使用者角度看，同一份 Definition 可能在 Builder、JSON 解析、发布、启动、任务处理和退回路径中经历不同层次的检查。调用者必须知道哪些入口只完成结构校验、哪些入口还检查 handler、哪些入口会生成执行索引。这增加了认知成本，也使未来修改图规则时更容易在某个入口遗漏相同语义。

本需求要深化现有 compiled Definition module，而不是新增一层包装。canonical Definition 继续保持纯数据和稳定 JSON；内部 module 统一拥有节点索引、路由索引、图遍历派生数据和完整可执行校验，使所有入口共享同一 source of truth。

## Solution

重构 Definition 校验与编译实现，让一次内部图分析产生后续结构校验和执行计划需要的全部索引。公开的 Definition.Validate、CompileDefinition、DefinitionPublisher 和 Engine 行为保持兼容，但它们委托给同一个内部 module，而不再分别重建或重新解释相同图结构。

结构校验与可执行校验仍保留清晰语义：结构校验不依赖 Registry；完整编译在结构校验成功后解析已注册 NodeHandler 并验证 config。二者共享相同图分析结果，但不会把 handler、回调或内部索引写入 canonical JSON。

执行计划继续是 package 内部、不可变且只读的数据。它的生命周期默认限定在一次发布验证或 Engine 操作中；本需求不引入跨请求缓存，也不持久化内部索引。

## User Stories

1. As a workflow author, I want Builder and JSON definitions to follow the same graph rules, so that authoring format never changes execution semantics.
2. As a workflow author, I want invalid node and edge structures rejected consistently, so that no entry point accepts a graph another entry point rejects.
3. As a workflow author, I want missing start routes rejected before persistence, so that a published workflow can always begin execution.
4. As a workflow author, I want ambiguous outcome routes rejected consistently, so that edge order never selects behavior.
5. As a workflow author, I want cycles, unreachable nodes and dead-end branches reported consistently, so that malformed graphs never become runnable.
6. As a workflow author, I want errors to identify the Definition, node or outcome involved, so that I can correct invalid workflows quickly.
7. As a workflow publisher, I want complete compilation to finish before version allocation, so that invalid attempts consume no version.
8. As a workflow publisher, I want code-authored and web-authored definitions to share one compilation path, so that publication has one meaning.
9. As a workflow publisher, I want published JSON to remain canonical data without runtime indexes, so that the stored format remains stable and portable.
10. As an application developer, I want existing CompileDefinition calls to keep working, so that the internal refactor does not break integration code.
11. As an application developer, I want existing Definition.Validate behavior to remain compatible, so that structural validation remains usable without a Registry.
12. As an application developer, I want Engine.Start to preserve its current validation and error behavior, so that startup integration does not change.
13. As an application developer, I want Engine.Handle and Engine.Return to preserve routing behavior, so that running instances remain compatible.
14. As an application developer, I want existing immutable Definition versions to remain readable, so that publication history survives the change.
15. As a NodeHandler author, I want config validation to run through the same Registry resolution rules, so that custom node kinds remain supported.
16. As a NodeHandler author, I want the NodeHandler interface to remain unchanged, so that existing handlers continue to compile.
17. As a NodeHandler author, I want control nodes to remain independent from registered handlers, so that start and end semantics remain owned by core.
18. As a workflow maintainer, I want node and route indexes derived once per compilation, so that graph knowledge has one source of truth.
19. As a workflow maintainer, I want acyclic, reachability and end-reachability checks to reuse one analyzed graph, so that invariant changes remain local.
20. As a workflow maintainer, I want structural and executable validation to share implementation without losing their distinct public meanings, so that reuse does not blur contracts.
21. As a workflow maintainer, I want Engine navigation to consume an immutable execution plan, so that runtime code does not scan raw node and edge slices.
22. As a workflow maintainer, I want the execution plan hidden from callers, so that internal index representation can change safely.
23. As a workflow maintainer, I want caller mutation after compilation to have no effect on a plan, so that one operation observes one frozen graph.
24. As a workflow maintainer, I want compilation errors to leave Instance and Definition stores unchanged, so that failure is atomic.
25. As a workflow maintainer, I want no package-global plan cache, so that the refactor does not introduce invalidation or concurrency state.
26. As a Store adapter author, I want Definition snapshot persistence unchanged, so that adapters do not need a schema or codec migration.
27. As a PostgreSQL adapter user, I want existing stored Instance definitions to resume successfully, so that durable workflows remain usable.
28. As a test maintainer, I want tests to assert compiler and Engine behavior rather than internal map layouts, so that implementation can deepen safely.
29. As a test maintainer, I want Builder and JSON fixtures to prove equivalent execution, so that the canonical model remains the shared seam.
30. As a concurrent caller, I want compilation to remain safe when definitions are not mutated by callers, so that Engine operations can run concurrently.
31. As a contributor, I want future graph invariants added in one module, so that new validation cannot be forgotten in publication or execution.
32. As a contributor, I want no extra shallow validation module introduced, so that deletion would not simply move graph logic between wrappers.
33. As a release maintainer, I want public examples to produce unchanged output, so that documentation remains valid.
34. As a release maintainer, I want baseline tests, race checks and vet to stay green, so that the refactor remains behavior-preserving.

## Implementation Decisions

- Deepen the existing internal compiled Definition module; do not introduce a second compiler or a wrapper around the current compiler.
- Preserve canonical Definition as JSON-serializable data containing identity, version, nodes, handler-owned config and edges only.
- Preserve the public distinction between structural validation and complete executable compilation.
- Route structural validation and complete compilation through one internal graph-analysis result containing the node index, start identity, route selectors and traversal relationships required by all validation passes.
- Build graph-derived data once per validation or compilation operation. Validation passes reuse that data rather than reconstructing adjacency, indegree or predecessor maps independently.
- Complete compilation runs structural validation first, then validates JSON config syntax, Registry membership, handler-owned config and the executable start route.
- Keep the compiled plan immutable and package-internal. It may retain a frozen canonical Definition plus deterministic lookup indexes.
- Keep Engine navigation based on compiled node and route lookups. Runtime behavior must not depend on node or edge slice order.
- Preserve Definition.Validate and CompileDefinition public behavior and stable error chains.
- Preserve DefinitionPublisher behavior: complete compilation succeeds before the version writer receives a candidate.
- Preserve Engine behavior: Start, Handle and Return obtain a complete execution plan before applying a transition that depends on Definition semantics.
- Do not persist internal indexes, handler implementations or compiled callbacks in Definition JSON or Instance snapshots.
- Do not add a cross-request or process-global plan cache. A future cache requires a separate design covering identity, Registry compatibility, invalidation and memory bounds.
- Do not change NodeHandler or Registry interfaces. Handler-specific config preparation is a separate architecture concern.
- Preserve defensive cloning so caller mutation cannot alter a plan or a frozen Instance Definition.
- Preserve existing error sentinels for invalid definitions, invalid node config, missing handlers, ambiguous routes and missing routes.
- Preserve publication versioning and Store contracts.
- Do not require PostgreSQL schema or codec changes.
- Remove only duplicated graph derivation and code made unused by consolidation; unrelated Definition features and formatting are excluded.

## Testing Decisions

- The highest test seams are DefinitionPublisher and Engine public behavior. Direct compiler tests remain for stable validation and error classification.
- Good tests assert accepted or rejected definitions, error chains, publication side effects and Engine outcomes. They do not inspect internal indexes or traversal structures.
- Existing Builder/JSON equivalence tests are prior art for canonical authoring behavior.
- Existing compiler tests are prior art for invalid handler config, missing handlers, ambiguous routes, graph context and missing start routes.
- Existing publication tests are prior art for validation before version allocation and immutable version behavior.
- Existing Engine tests are prior art for start, command routing, rejection routing and failure atomicity.
- Retain tests proving structural validation works without a Registry while complete compilation requires every business node handler.
- Retain tests proving control nodes require no registered handler.
- Add or retain tests proving every invalid graph is classified identically through Builder, JSON parsing, publication and Engine startup where applicable.
- Add or retain tests proving the compiled plan owns a frozen snapshot and is unaffected by later caller mutation.
- Add or retain tests proving valid outcome lookup is deterministic and independent from edge declaration order.
- Add or retain tests proving compilation failure performs no Store Create or Save and consumes no Definition version.
- Add or retain tests proving Engine.Handle and Engine.Return reject corrupted or no-longer-executable snapshots without durable mutation.
- Keep tests at public behavior seams; internal graph-analysis helpers receive direct tests only when a public failure cannot isolate a complex invariant.
- Run `go test ./...`, `go test -race ./...` and `go vet ./...`.
- Run PostgreSQL integration tests separately with an explicit DSN to confirm durable snapshots remain compatible.

## Out of Scope

- Changing canonical Definition JSON or adding compiled indexes to persisted data.
- Changing Definition, NodeDefinition or Edge public fields.
- Changing NodeHandler, Registry, Store, Definition reader or writer interfaces.
- Preparing or caching typed handler config across Engine operations.
- Cross-request, process-global or distributed execution-plan caching.
- Changing publication version allocation or Definition repository adapters.
- Changing Instance, Task, Audit or lifecycle command architecture.
- Changing PostgreSQL Projection query behavior.
- Adding new node kinds, graph constructs, cycles, joins, parallel gateways or subflows.
- Performance work unrelated to eliminating repeated graph derivation.

## Further Notes

- The current compiledDefinition already passes the deletion test: removing it would spread node and route lookup back into Engine. This PRD deepens it rather than replacing it.
- The repository has no configured external issue tracker, domain glossary or ADR directory. This PRD is stored locally with `Ready for agent` status.
- The original unified compiler requirement remains authoritative: Builder and JSON share canonical data, and Engine consumes deterministic execution semantics.
- Handler config parsing across Validate, Activate and Handle may still warrant a separate future PRD; it is intentionally excluded here to preserve NodeHandler compatibility.
