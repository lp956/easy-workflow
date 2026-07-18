# Definition repository capability seams

- 状态：Accepted
- 决策范围：Definition 发布版本的写入与读取

## 背景

`DefinitionPublisher` 需要在完整编译后原子分配并保存版本，`Engine.StartPublished` 只需按 ID 和正版本号读取一个精确快照。两条调用路径的依赖方向不同，但仓库内当前只有 `MemoryDefinitionStore` 同时实现 `DefinitionVersionWriter` 与 `DefinitionReader`。单一内置实现既不能证明这两个 seam 没有价值，也不足以支持在缺少需求证据时增加数据库 adapter。

Definition persistence 与 Instance command-side `Store` 的一致性边界也不同：前者保存不可变、按 Definition ID 递增的发布历史；后者保存可变 Instance 聚合并执行乐观并发 CAS。合并接口会迫使只需要其中一种能力的调用方承担无关依赖。

## 决策

保留 `DefinitionVersionWriter` 与 `DefinitionReader` 两个公开 capability interface：

- `DefinitionPublisher` 只依赖 `DefinitionVersionWriter`，并在调用 writer 前完成 Definition 编译。
- `Engine.StartPublished` 只调用 `DefinitionReader.Load` 读取请求的精确版本，不自动读取 latest，也不 fallback。
- `MemoryDefinitionStore` 继续作为无基础设施的参考 adapter，但不被视为唯一允许的实现。
- `definitiontest` 提供 writer、reader 和共享生命周期三组可复用契约；任何未来官方 adapter 发布前必须原样运行相应契约及其基础设施集成测试。
- Definition repository 保持独立于 Instance `Store`，不得为了 durable publication 扩张 command-side interface。
- 当前不增加 PostgreSQL、MongoDB、filesystem 或 remote Definition adapter，也不增加 schema、migration 或外部依赖。

契约只验证公开可观察行为：错误 sentinel、精确与 latest 查找、版本分配、失败原子性、防御性所有权、context cancellation 和并发安全。它不检查 map、锁、SQL、transaction 或 schema。

## 结果

分离 interface 的成本是维护两个小型公开契约；收益是 Publisher 与 Engine 保持最小依赖，仓库外 adapter 保持兼容，并且未来 durable adapter 有统一、可执行的验收面。参考 adapter 必须持续通过全部契约，示例和快速开始继续无需外部基础设施。

新增 durable Definition repository 需要独立 PRD，明确 adapter 选择、transaction 语义、migration 与集成门禁。该工作不得把 Definition persistence 合入 Instance `Store`。

## 重新评估条件

不得仅因仓库内只有一个 adapter 就合并、删除、重命名或废弃这两个 interface。未来改变 seam 至少需要以下一种证据，并提供兼容与迁移方案：

- 第二个 adapter 的实际需求表明当前 capability 边界无法实现或产生不可接受的复杂度；
- 仓库外兼容性数据证明接口没有实际使用，或证明另一边界更安全；
- 已规划 major-version migration，包含 deprecation 路径、下游影响和替代 API。
