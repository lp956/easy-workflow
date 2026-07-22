# 交付 P1 可安装版本与分层文档

- Label: `ready-for-agent`
- Priority: P1
- User stories: 1–2, 48

## Parent

[easy-workflow 项目说明](../README.md)

## What to build

把 P1 能力整理为可安装、可验证的首个产品化交付：core-only 内存快速开始、Definition 发布与版本示例、PostgreSQL durable adapter 集成说明，以及 Condition JSON 路由示例。文档按 core、官方 extension、durable adapter 和可选 transport/Web integration 分层，明确每层依赖和职责。

所有示例必须使用公开 interface 并作为可执行测试。导入 core 不需要配置文件、数据库、HTTP framework、Redis 或组织目录。

## Acceptance criteria

- [ ] 新用户可以通过标准 Go package 安装方式引入 core 和 Approval extension。
- [ ] core-only 快速开始无需外部基础设施即可执行完整审批。
- [ ] 文档展示 Builder 与 JSON 发布同一 Definition 的流程。
- [ ] 文档展示按指定版本和最新版本启动 Instance，并解释快照语义。
- [ ] PostgreSQL adapter 文档包含显式连接、迁移、事务和测试要求，不产生隐式初始化。
- [ ] Condition 示例从 JSON 发布到实际分支完成端到端执行。
- [ ] 所有示例是可执行测试，不引用内部 helper。
- [ ] 文档明确 HTTP、Web UI、组织目录和查询投影不属于 core。
- [ ] package 文档说明 Store、NodeHandler 和 Definition 发布的错误与并发契约。
- [ ] 全仓测试、race、vet 和 PostgreSQL adapter 集成测试通过。

## Blocked by

- [002 发布并运行不可变 Definition 版本](002-publish-immutable-definition-versions.md)
- [004 实现 PostgreSQL durable Store adapter](004-add-postgresql-durable-store-adapter.md)
- [005 实现受限 Condition extension](005-add-restricted-condition-extension.md)
