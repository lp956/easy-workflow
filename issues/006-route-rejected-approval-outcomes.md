# 支持 rejected outcome 显式路由

- Label: `ready-for-agent`
- Priority: P2
- User stories: 18–21

## Parent

[easy-workflow 产品需求文档](../PRD.md)

## What to build

扩展 Approval extension，使拒绝既能保持当前默认终态语义，也能在 Definition 明确声明时产生 rejected outcome 并沿 DAG 路由到补件、复核或其他业务节点。

拒绝任务及其 sibling task 必须按既有规则完成或关闭，Audit 必须清楚区分“拒绝并终止”和“拒绝并路由”。不得使用隐式上一节点或数组位置决定目标。

## Acceptance criteria

- [ ] 未配置 rejected 路由时，现有终态拒绝行为保持兼容。
- [ ] 配置 rejected 路由时，拒绝沿显式 outcome edge 进入目标节点。
- [ ] 拒绝者的 Task 记录 rejected outcome，其他 active sibling task 被关闭。
- [ ] Audit 区分终态拒绝和拒绝后继续路由，并保持因果顺序。
- [ ] 缺失或歧义 rejected edge 在 Definition 校验或执行时返回可识别错误。
- [ ] 目标完全由 Definition edge 决定，Approval extension 不能选择任意 node ID。
- [ ] 或签和会签场景均覆盖终态与路由两种拒绝行为。
- [ ] 现有 Approval 公开行为测试继续通过。

## Blocked by

None - can start immediately.
