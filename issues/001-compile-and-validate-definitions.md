# 预编译并完整校验 Definition

- Label: `ready-for-agent`
- Priority: P1
- User stories: 4, 5, 11–17, 49–50

## Parent

[easy-workflow 项目说明](../README.md)

## What to build

建立统一的 Definition 编译路径，使代码 Builder 和 JSON Definition 在进入 Engine 前完成完整图校验、所有已注册 node handler 的配置校验，以及确定性的节点和 outcome 路由索引构建。编译结果只作为内部执行计划使用，canonical Definition JSON 保持数据化、稳定且不暴露内部索引。

Engine 启动和处理命令时消费可信执行计划，不再分别重复解释节点、边和 handler 配置。所有错误必须能关联到具体 Definition、node 或 outcome，并且失败不得产生 Instance 状态。

## Acceptance criteria

- [ ] Builder 和 JSON Definition 通过同一编译路径产生相同执行语义。
- [ ] 图结构、handler 是否注册及 node config 在创建 Instance 前全部校验。
- [ ] 编译结果包含确定性的节点和 outcome 路由索引，但 canonical JSON 不包含这些内部数据。
- [ ] 未注册 handler、无效 config、歧义路由和缺失路由返回可识别错误。
- [ ] Engine 的公开行为保持兼容，现有 Approval 流程继续通过。
- [ ] 编译失败时 Store 中不创建或修改 Instance。
- [ ] 测试只通过 Definition 编译和 Engine 的公开 interface 验证行为，不断言内部索引结构。
- [ ] `go test ./...`、`go test -race ./...` 和 `go vet ./...` 通过。

## Blocked by

None - can start immediately.
