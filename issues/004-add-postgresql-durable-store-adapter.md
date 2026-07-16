# 实现 PostgreSQL durable Store adapter

- Label: `ready-for-agent`
- Priority: P1
- User stories: 27–32, 45–48

## Parent

[easy-workflow 产品需求文档](../PRD.md)

## What to build

提供可选安装的 PostgreSQL Store adapter，使 Instance 在进程重启和多副本部署中保持耐久与一致。一次 Save 必须在单个数据库事务内原子保存 Instance 状态、冻结 Definition、business data、NodeState、Task 和 append-only Audit，并通过条件版本更新实现 CAS。

adapter 负责 schema、事务、参数化查询和数据库错误到稳定领域错误的映射。core 不依赖 PostgreSQL driver、迁移工具或数据库配置，导入 core 不产生连接或迁移副作用。

## Acceptance criteria

- [ ] PostgreSQL adapter 作为可选 package 安装，core 不引入 PostgreSQL 依赖。
- [ ] 提供显式 schema/migration 机制，不在 package 初始化时连接或自动迁移。
- [ ] Create、Load 和 Save 通过公共 Store 契约测试。
- [ ] Instance、Task、NodeState 和 Audit 在一个事务中提交或回滚。
- [ ] Save 使用数据库条件版本更新，多进程竞争时最多一个相同 expectedVersion 写入成功。
- [ ] stale write 返回稳定 version conflict，不能覆盖成功写入。
- [ ] 所有 SQL 使用参数化输入，ActorID、InstanceID 和业务 JSON 不参与 SQL 拼接。
- [ ] context cancellation 能中止等待或执行中的数据库操作。
- [ ] 集成测试覆盖事务回滚、并发 CAS、重启后 Load 和完整快照恢复。
- [ ] adapter 不提供待办、历史搜索或 Definition 发布方法，Store interface 不扩张。

## Blocked by

- [003 建立 Store adapter 契约测试](003-establish-store-adapter-contract-tests.md)
