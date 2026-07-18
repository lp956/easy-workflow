# easy-workflow 产品需求文档：验证并加固 Definition Repository Seam

- 状态：Ready for agent
- 类型：契约加固与架构决策
- 兼容性目标：保持 DefinitionVersionWriter、DefinitionReader、Publisher 和 StartPublished 行为兼容

## Problem Statement

`easy-workflow` 通过 DefinitionVersionWriter 和 DefinitionReader 分离发布写入与版本读取。当前仓库中只有 MemoryDefinitionStore 同时实现这两个 interface；Publisher 持有 writer，而 Engine.StartPublished 在每次调用时接收 reader。

这两个 seam 为嵌入式库保留了宿主实现 durable Definition repository 的能力，但仓库内尚无第二个真实 adapter，也没有像 Store 那样可被任意 adapter 复用的完整 contract test suite。因此，当前无法仅凭仓库证据判断两个 interface 是有价值的扩展 seam，还是配置知识分散的过早抽象。

直接删除、合并或替换公开 interface 会给仓库外 adapter 带来兼容风险；直接新增 PostgreSQL adapter 又可能在没有需求证据时扩大产品范围。本需求先建立可执行契约并记录 seam 的保留条件，让未来第二个 adapter 有明确 test surface，同时不预设必须新增哪一种 adapter。

## Solution

建立可复用的 Definition repository contract tests，分别验证 writer 行为、reader 行为以及同时实现二者时的发布—读取闭环。MemoryDefinitionStore 必须通过完整契约；未来任何 durable adapter 也必须运行同一套测试。

保留 DefinitionVersionWriter 和 DefinitionReader 的公开行为，不在本轮合并或删除。补充清晰的架构说明：分离 interface 的价值是让 Publisher 只依赖写入能力、Engine 启动只依赖读取能力；MemoryDefinitionStore 是当前参考 adapter，而非唯一允许实现。

本轮产出 seam 验证、契约测试与决策记录，不实现新的 PostgreSQL Definition adapter。如果后续出现明确 durable publication 需求，应创建独立 PRD，并要求新 adapter 在不扩张 Instance Store interface 的前提下通过本契约。

## User Stories

1. As an application developer, I want existing DefinitionPublisher construction to remain compatible, so that publication integration does not change.
2. As an application developer, I want Engine.StartPublished to keep accepting an exact-version reader, so that startup integration remains compatible.
3. As an application developer, I want exact version loads never to fall back, so that workflow startup cannot drift to another definition.
4. As an application developer, I want LoadLatest to return the greatest published version, so that I can explicitly resolve a current version before pinning it.
5. As a workflow author, I want failed publication to consume no version, so that version sequences remain meaningful.
6. As a workflow author, I want successful versions to increase monotonically per Definition ID, so that published history is deterministic.
7. As a workflow author, I want published versions immutable, so that running and future instances can refer to stable snapshots.
8. As a workflow author, I want different Definition IDs to maintain independent version sequences, so that one workflow does not affect another.
9. As a workflow initiator, I want StartPublished to load the exact requested version, so that an Instance freezes the intended semantics.
10. As a workflow initiator, I want missing versions classified consistently, so that applications can distinguish absence from invalid execution.
11. As an application developer, I want returned Definition snapshots detached from repository storage, so that caller mutation cannot rewrite history.
12. As an application developer, I want repository operations to honor context cancellation, so that abandoned requests do not continue work.
13. As a concurrent publisher, I want simultaneous publishes for one Definition ID to receive unique versions, so that no version is reused.
14. As a concurrent reader, I want loads to remain safe during publication, so that readers observe complete immutable snapshots.
15. As an adapter author, I want an executable writer contract, so that I know how version allocation and failure atomicity must behave.
16. As an adapter author, I want an executable reader contract, so that exact and latest lookup semantics are unambiguous.
17. As an adapter author, I want a combined publication-read contract, so that I can prove one adapter supports the full lifecycle.
18. As an adapter author, I want stable error expectations, so that my adapter integrates with existing caller error handling.
19. As an adapter author, I want defensive ownership requirements tested, so that mutable slices and config bytes cannot leak across the seam.
20. As an adapter author, I want concurrent behavior tested without implementation-specific assertions, so that database and memory adapters can share coverage.
21. As a library maintainer, I want MemoryDefinitionStore to remain the reference adapter, so that examples and unit tests stay infrastructure-free.
22. As a library maintainer, I want public writer and reader interfaces retained during seam validation, so that repository-external adapters are not broken by assumption.
23. As a library maintainer, I want evidence before adding a second production adapter, so that speculative infrastructure does not expand maintenance cost.
24. As a library maintainer, I want evidence before merging or deleting interfaces, so that one in-repository adapter is not mistaken for one real-world adapter.
25. As a library maintainer, I want Publisher tested through the writer seam, so that invalid definitions never reach version allocation.
26. As a library maintainer, I want StartPublished tested through the reader seam, so that exact-version behavior remains independent from adapter implementation.
27. As a test maintainer, I want one reusable contract suite, so that every adapter is held to the same observable behavior.
28. As a test maintainer, I want contract tests to avoid inspecting internal maps, locks, SQL or schemas, so that adapter implementation remains free.
29. As a contributor, I want future durable adapter work to begin from a documented contract, so that Store semantics are not accidentally copied or expanded.
30. As a contributor, I want Definition persistence kept separate from Instance Store, so that the command-side Store interface remains small.
31. As a contributor, I want a recorded decision about the current hypothetical seam, so that future reviews do not repeatedly suggest deleting it without new evidence.
32. As a release maintainer, I want existing examples to remain executable with MemoryDefinitionStore, so that the quick start needs no infrastructure.
33. As a release maintainer, I want no migration or new dependency in this validation phase, so that contract hardening can ship independently.
34. As a future durable adapter user, I want any official adapter to pass the same contract before release, so that production behavior matches memory behavior.

## Implementation Decisions

- Preserve DefinitionVersionWriter and DefinitionReader as separate public interfaces during this PRD.
- Preserve DefinitionPublisher dependency on the writer capability only.
- Preserve Engine.StartPublished dependency on the exact-version reader capability supplied by the caller.
- Treat MemoryDefinitionStore as the current reference adapter, not proof that no external adapters exist.
- Create a reusable Definition repository contract test module that can be invoked by any adapter without depending on its implementation.
- Divide the contract into writer, reader and combined lifecycle behaviors so adapters may implement the capability they expose.
- Writer contract covers input validation, monotonic per-ID version allocation, concurrency, immutable storage, detached returns, context cancellation and failure atomicity.
- Reader contract covers exact lookup, latest lookup, missing identity, context cancellation, concurrent safety and detached returns.
- Combined contract covers publish then exact/latest read, multiple IDs, multiple versions, failed publication without gaps and caller mutation isolation.
- Test DefinitionPublisher separately to prove complete compilation happens before writer invocation and invalid publication consumes no version.
- Test Engine.StartPublished separately to prove exact lookup, error propagation and frozen Instance snapshots.
- Preserve stable error sentinels and `errors.Is` behavior.
- Preserve the canonical Definition format and Version semantics.
- Preserve the separation between Definition repository and command-side Instance Store.
- Do not add a PostgreSQL or other durable Definition adapter in this PRD.
- Do not remove, merge or deprecate the interfaces based only on the absence of a second in-repository adapter.
- Record the architecture decision that future seam changes require evidence from a second adapter need, external compatibility data or a planned major-version migration.
- Require any future official adapter to run the shared contract unchanged.
- Introduce no production seam solely to support mocks; the contract harness is test infrastructure, not a new runtime interface.
- Add no database schema, migration or external dependency.

## Testing Decisions

- The highest production seams are DefinitionVersionWriter, DefinitionReader, DefinitionPublisher and Engine.StartPublished.
- Good contract tests assert observable versions, snapshots, errors, concurrency and side effects. They do not inspect adapter storage, locks, SQL or transaction implementation.
- Existing MemoryDefinitionStore publication and immutability tests are prior art for the reusable contract.
- Existing concurrent publication tests are prior art for unique monotonic versions.
- Existing Engine published-start tests are prior art for exact version selection and snapshot freezing.
- Run the writer contract against MemoryDefinitionStore.
- Run the reader contract against MemoryDefinitionStore.
- Run the combined lifecycle contract against MemoryDefinitionStore.
- Test first publish as version 1 and subsequent publishes as strict increments for one ID.
- Test independent version sequences for multiple IDs.
- Test exact Load never returns a different or latest version.
- Test LoadLatest returns the greatest successfully published version.
- Test unknown IDs, zero versions and missing versions with stable error classification.
- Test caller mutation of input, writer result, exact-load result and latest-load result cannot alter stored snapshots.
- Test config bytes, nodes and edges are defensively owned across every seam.
- Test canceled CreateVersion, Load and LoadLatest calls leave state unchanged and preserve cancellation causes.
- Test concurrent CreateVersion calls produce unique gap-free versions for the reference adapter.
- Test concurrent reads during writes return complete snapshots and remain race-free.
- Test invalid publication invokes no successful version allocation and leaves latest unchanged.
- Test StartPublished propagates reader errors and performs no Store Create on load failure.
- Test StartPublished freezes the exact loaded version into the new Instance.
- Keep contract tests adapter-neutral so a future durable adapter can run them unchanged.
- Run `go test ./...`, `go test -race ./...` and `go vet ./...`.
- A future durable adapter must also run its explicit integration suite with required infrastructure.

## Out of Scope

- Adding a PostgreSQL, MongoDB, filesystem or remote Definition repository adapter.
- Combining Definition persistence with the Instance Store interface.
- Removing, merging, renaming or deprecating DefinitionVersionWriter or DefinitionReader.
- Changing DefinitionPublisher or Engine.StartPublished public signatures.
- Adding a new public repository interface.
- Changing Definition JSON, version numbering or immutability semantics.
- Changing Definition compilation internals beyond what is required for existing Publisher tests.
- Adding automatic latest-version startup or fallback behavior.
- Adding caching, replication, retention, deletion or version compaction.
- Adding database schema, migrations or external dependencies.
- Discovering or surveying private downstream repositories automatically.

## Further Notes

- One in-repository adapter means the seam is not proven by repository implementations alone. As an embeddable library, external adapters remain plausible, so compatibility is the safer default.
- The contract test module supplies evidence and leverage without creating another production abstraction.
- If a durable Definition repository becomes a confirmed requirement, create a separate PRD covering adapter choice, migrations, transaction semantics and integration gates. It must not expand the Instance Store interface.
- If a future major release proposes merging or removing the interfaces, it must include downstream compatibility evidence and a deprecation path.
- The repository has no configured external issue tracker, domain glossary or ADR directory. This PRD is stored locally with `Ready for agent` status.
