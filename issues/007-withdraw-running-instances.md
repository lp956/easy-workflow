# 实现 Instance 撤回

- Label: `ready-for-agent`
- Priority: P2
- User stories: 33–34, 39, 45

## Parent

[easy-workflow 项目说明](../README.md)

## What to build

提供显式 Instance 撤回行为。可信调用方提交 actor 和 Instance，Engine 校验 Instance 仍在运行、actor 通过 host policy 授权，并在一个 CAS 提交中把 Instance 标记为 withdrawn、关闭全部 active task、追加不可变 Audit。

这是第一种非 task command，不提前抽取通用 command module；实现应保持当前 Engine locality，并为后续退回复用积累实际证据。

## Acceptance criteria

- [ ] 只有 running Instance 可以撤回。
- [ ] 操作者必须通过显式 host policy 授权，不能仅因请求 body 声称是 initiator 而成功。
- [ ] 成功撤回将 Instance 置为 withdrawn，并关闭所有 active task。
- [ ] Audit 记录撤回 action、actor、当前 node 和时间，且旧记录不被修改。
- [ ] Instance、Task 和 Audit 通过一次 CAS Save 原子提交。
- [ ] stale version 返回 version conflict，不能部分关闭任务或追加 Audit。
- [ ] completed、rejected、withdrawn Instance 的撤回返回稳定错误。
- [ ] 撤回后旧 task command 不能再执行。
- [ ] MemoryStore 行为测试和 PostgreSQL 集成测试均覆盖撤回原子性。

## Blocked by

None - can start immediately.
