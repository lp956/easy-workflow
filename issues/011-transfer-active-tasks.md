# 实现任务转派

- Label: `ready-for-agent`
- Priority: P3
- User stories: 38–39, 43, 45

## Parent

[easy-workflow 项目说明](../README.md)

## What to build

提供显式 active Task 转派行为。可信调用方提交当前 Task、新 assignee 和原因，Engine 通过 host policy 验证操作者、Task 当前所有权和目标 assignee，然后关闭或标记原 assignment、创建新的 active assignment，并原子记录 Audit 与查询投影变化。

转派不得修改已发布 Definition 或 Approval config，也不能把历史 Task 的 assignee 原地改写成新人员。

## Acceptance criteria

- [ ] 只有 active Task 可以转派。
- [ ] 操作者和目标 assignee 必须通过 host policy 校验。
- [ ] 原 Task/assignment 历史保留，新 assignee 获得新的可操作 assignment。
- [ ] Audit 记录操作者、原 assignee、新 assignee、原因、Instance 和 node。
- [ ] 转派通过一次 CAS Save 原子提交，并与查询投影保持一致。
- [ ] stale version、无权限、无效目标和已结束 Task 不产生部分状态。
- [ ] 转派不修改冻结 Definition 或 Approval config。
- [ ] 新 assignee 能完成任务，旧 assignee 的后续命令被拒绝。
- [ ] 待办与已办查询正确反映转派前后的 assignment。
- [ ] 或签和会签中的转派均有行为测试。

## Blocked by

- [008 实现显式退回并深化 command module](008-return-instances-and-deepen-command-execution.md)
- [009 支持 Approval assignment policy](009-resolve-approval-assignment-policies.md)
- [010 建立待办、已办和参与人查询投影](010-add-worklist-and-participation-projections.md)
