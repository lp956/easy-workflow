# 发布并运行不可变 Definition 版本

- Label: `ready-for-agent`
- Priority: P1
- User stories: 3, 6–10, 13–14

## Parent

[easy-workflow 项目说明](../README.md)

## What to build

交付从代码 Builder 或 Web JSON 到可运行 Instance 的完整 Definition 发布路径。发布 module 负责为同一稳定 Definition ID 分配单调递增版本、保存不可变版本、读取指定版本和读取最新版本，并在发布前调用统一编译校验。

Engine 能从一个已发布版本启动 Instance。Instance 必须冻结该版本的完整 Definition 快照；发布新版本后，已运行 Instance 的路由、任务和审计语义不得改变。首个切片使用进程内 Definition adapter 完成端到端行为，不把 Definition 生命周期加入 Instance Store。

## Acceptance criteria

- [ ] Builder 和 JSON 都能发布为同一 canonical Definition 类型。
- [ ] 首次发布获得版本 1，后续发布为同一 ID 分配严格递增版本。
- [ ] 已发布版本不可被原地更新或覆盖。
- [ ] 可以读取指定 ID 和 Version，也可以读取指定 ID 的最新版本。
- [ ] 无效 Definition 发布失败且不占用版本、不留下部分记录。
- [ ] Engine 可以从指定已发布版本启动 Instance。
- [ ] Instance 保存启动版本的完整快照，新版本发布不影响运行中 Instance。
- [ ] 进程内 Definition adapter 返回防御性副本，调用方不能绕过发布修改已保存版本。
- [ ] 示例展示 Builder 发布、JSON 发布及按版本启动的完整流程。

## Blocked by

- [001 预编译并完整校验 Definition](001-compile-and-validate-definitions.md)
