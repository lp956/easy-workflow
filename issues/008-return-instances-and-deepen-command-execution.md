# 实现显式退回并深化 command module

- Label: `ready-for-agent`
- Priority: P2
- User stories: 35–37, 39, 45, 49–50

## Parent

[easy-workflow 产品需求文档](../PRD.md)

## What to build

实现显式、目标化的 Instance 退回。调用方必须提供目标 node 和原因，Engine 校验 actor 授权、目标符合 Definition 与执行历史策略，关闭当前 active task，并在目标 node 创建新的任务轮次。历史 Task、NodeState 结果和 Audit 不得被覆盖。

撤回和退回形成第二种、第三种 command 行为后，应用 deletion test，把共同的 Load、状态校验、policy、transition、Audit、版本递增和 CAS Save 深化为 Engine 内部 command module；外部保持清晰的领域操作。

## Acceptance criteria

- [ ] 退回必须使用显式 node ID，不存在隐式“上一步”行为。
- [ ] 目标 node 必须属于冻结 Definition，并满足已执行历史和 host policy 的允许规则。
- [ ] 成功退回关闭当前 active task，并为目标 node 创建全新的任务轮次。
- [ ] 旧 Task、Outcome 和 Audit 保持不可变且仍可查询。
- [ ] Audit 记录 actor、来源 node、目标 node 和原因。
- [ ] 整个退回通过一次 CAS Save 原子提交，冲突时无部分状态。
- [ ] Engine 的公共 command 行为共享经过验证的内部执行骨架，不复制持久化和审计顺序。
- [ ] task decision、撤回和退回仍通过各自公开行为测试。
- [ ] 禁止退回到 start、end、未访问或策略禁止的 node，并返回稳定错误。
- [ ] 分支流程、会签节点和 rejected 路由之后的退回均有行为测试。

## Blocked by

- [007 实现 Instance 撤回](007-withdraw-running-instances.md)
