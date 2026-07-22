# 建立 Store adapter 契约测试

- Label: `ready-for-agent`
- Priority: P1
- User stories: 30–32, 50

## Parent

[easy-workflow 项目说明](../README.md)

## What to build

把 Store 的所有可观察契约整理为可复用测试套件，使 MemoryStore 和未来 durable adapter 接受完全相同的行为验证。测试覆盖创建、加载、快照所有权、context 传播、原子 CAS 和稳定领域错误，不依赖具体数据库或 adapter 内部结构。

该切片同时补齐 MemoryStore 当前未覆盖的契约缺口，确保第二个 adapter 出现前 Store seam 已有明确、可执行的 test surface。

## Acceptance criteria

- [ ] 契约套件可以由任意 Store adapter 复用，而不复制测试逻辑。
- [ ] 覆盖成功 Create、重复 ID、成功 Load 和 missing Instance。
- [ ] 覆盖 Load 和 Save 的防御性快照所有权，调用方修改不得越过 Store interface。
- [ ] 覆盖成功 CAS Save、stale version conflict 和 durable version 不被失败写入改变。
- [ ] 覆盖创建、读取和保存时的 context cancellation。
- [ ] 覆盖 Instance Data、Definition、NodeState、Task 和 Audit 的深复制语义。
- [ ] 稳定错误可以通过标准错误链识别。
- [ ] MemoryStore 通过完整契约套件和 race 检查。

## Blocked by

None - can start immediately.
