# 支持 Approval assignment policy

- Label: `ready-for-agent`
- Priority: P2
- User stories: 40–42

## Parent

[easy-workflow 产品需求文档](../PRD.md)

## What to build

在 Approval extension 内支持可序列化 assignment policy，同时永久保留静态 assignees 作为最简单配置。交付至少一种真实的动态解析方式，通过 host-provided organization adapter 把 policy 解析为 ActorID，并在 node 激活时去重、校验和冻结为 Task。

core 不认识用户、角色、部门或组织目录。目录变化只影响未来激活，不能改写已经冻结的 Task。解析失败或结果为空时不得创建部分任务。

## Acceptance criteria

- [ ] 现有静态 assignees 配置和行为保持兼容。
- [ ] 至少一种动态 assignment policy 可以通过 JSON 和 Builder 配置。
- [ ] organization adapter 由 host 显式提供，导入 core 或 Approval 不产生目录连接副作用。
- [ ] policy 解析结果转换为非空、唯一且冻结的 ActorID Task。
- [ ] 空结果、重复 ActorID、adapter 错误和 context cancellation 返回可识别错误。
- [ ] 解析或任务创建失败时不留下部分 Task 或 Instance 更新。
- [ ] node 激活后组织目录变化不改变既有 assignee。
- [ ] 或签和会签对静态和动态 assignee 使用同一状态机。
- [ ] core Task 不增加角色、部门或目录对象字段。

## Blocked by

- [002 发布并运行不可变 Definition 版本](002-publish-immutable-definition-versions.md)
