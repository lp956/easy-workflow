# PostgreSQL Projection continuations

- 状态：Accepted
- 决策范围：Worklist、Participated、Initiated 的公开分页与 query-family ownership

## 背景

旧 `Cursor` 公开 `At`、`InstanceID` 和 `TaskID`。调用方虽然被要求原样回传，仍必须理解 Task 查询需要三个字段、Instance 查询只能使用两个字段，以及哪些组合属于错误家族。这把 PostgreSQL keyset 结构暴露为应用契约，也让分页规则可能在 public adapter、SQL 参数和 Next 构造之间漂移。

Task 与 Instance 查询族已经分别拥有 SQL、稳定排序、limit+1、扫描和分页构造，不应合并成 generic repository 或引入 mock-only database seam。本决策只深化各自的 continuation ownership，并保留旧公开 API 的明确迁移期。

## 决策

- 新的首选入口是 `WorklistPage`、`ParticipatedPage` 和 `InitiatedPage`，输入为 `ContinuationQuery`，输出为 `ContinuationPage[T]`。
- `Continuation` 是 opaque string。空值表示第一页或末页；非空值只能原样回传，调用方不得解析、拼装或修改。
- token 使用版本化 JSON 的 unpadded base64url 编码。它不是授权凭证、不包含 secret，也不替代可信 actor 或宿主计算的 scope。
- Task query family 拥有 `At + InstanceID + TaskID` keyset 的映射和完整性校验；WorklistPage 与 ParticipatedPage 因排序完全相同而共享 Task token。
- Instance query family 独立拥有 `At + InstanceID` keyset，并拒绝 Task family token。
- family、版本、字段完整性、未知 JSON 字段、trailing data、非法 base64url 和过长输入都在 pool acquisition 前返回 `ErrInvalidProjectionQuery`。
- token 只编码稳定位置。每一页都重新应用调用方提供的 ActorID 与 QueryScope；nil scope 仍表示不附加限制，non-nil empty scope 仍表示 deny all。
- 解码后的 keyset、actor、scope 和 limit 继续全部作为 pgx 参数，绝不拼接 SQL。
- 两个 query family 保持独立的 SQL、扫描、Task/Instance 类型和 Next 构造。只共享确实相同的 boundary normalization 与 transport envelope codec。

## 兼容与弃用

- `Worklist`、`Participated`、`Initiated`、`ActorQuery`、`PageRequest`、`Cursor` 和 `Page` 使用 Go 标准 `Deprecated:` 注释标记。
- 旧方法在当前 major version 内继续接受和返回原 Cursor，保留 nil Next、scope、limit、排序、参数化与跨家族拒绝行为。
- 旧方法与新方法都委托给同一 private Task/Instance query result；不存在两份 SQL 或 keyset 规则。
- 迁移对应关系：

| 旧入口 | 新入口 | 分页判断 |
| --- | --- | --- |
| `Worklist(ActorQuery)` | `WorklistPage(ContinuationQuery)` | `Next != nil` 改为 `Next != ""` |
| `Participated(ActorQuery)` | `ParticipatedPage(ContinuationQuery)` | `After: *Cursor` 改为 `After: Continuation` |
| `Initiated(ActorQuery)` | `InitiatedPage(ContinuationQuery)` | 不再构造或检查 TaskID |

- 删除旧 API 需要单独的 major-version 决策；本次不静默移除或改变现有分页会话行为。

## 验证

- 无数据库测试覆盖 deny-all scope、默认/最小/最大 limit、非法 limit、损坏 token 和 SQL 前拒绝。
- 真实 PostgreSQL 测试在随机隔离 schema 中覆盖等 timestamp 下的 InstanceID/TaskID tie-breaker、连续页面无重复无遗漏、Task token 跨 Worklist/Participated 兼容和 Task/Instance 跨家族拒绝。
- 既有 Cursor integration tests 原样保留，作为兼容路径的分页与事务可见性回归门禁。

