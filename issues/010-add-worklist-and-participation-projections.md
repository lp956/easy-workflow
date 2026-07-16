# 建立待办、已办和参与人查询投影

- Label: `ready-for-agent`
- Priority: P2
- User stories: 43–47

## Parent

[easy-workflow 产品需求文档](../PRD.md)

## What to build

在 PostgreSQL adapter 旁建立独立查询投影，为 host application 提供按 ActorID 查询待办、已办、发起记录和参与记录的完整路径。投影从原子提交的 Instance、Task 和 Audit 事实生成，不改变 command-side Store interface，也不通过 cron 把完成数据搬到历史表。

查询必须支持稳定分页和租户/业务调用方提供的过滤约束，所有输入参数化。candidate、participant 和 notifier 只在确有对应事实时进入投影，不修改 core Task 模型来承载目录对象。

## Acceptance criteria

- [ ] 可以按 ActorID 查询当前 active task 待办列表。
- [ ] 可以按 ActorID 查询已完成或已关闭的参与记录。
- [ ] 可以按 initiator 查询其发起的运行中和已结束 Instance。
- [ ] 查询结果能关联 Definition ID/Version、Instance、node、Task 状态和关键 Audit 时间。
- [ ] 完成数据原地保留并可查询，不依赖 cron 历史迁移。
- [ ] 查询投影不向核心 Store interface 添加搜索或分页方法。
- [ ] 分页顺序稳定，相同排序字段时使用确定性 tie-breaker。
- [ ] 所有过滤条件使用参数化查询，并能应用 host 提供的租户约束。
- [ ] command 事务成功后投影可观察到一致结果；失败事务不产生查询记录。
- [ ] PostgreSQL 集成测试覆盖待办转已办、撤回、退回新轮次和 assignment policy 冻结结果。

## Blocked by

- [004 实现 PostgreSQL durable Store adapter](004-add-postgresql-durable-store-adapter.md)
- [009 支持 Approval assignment policy](009-resolve-approval-assignment-policies.md)
