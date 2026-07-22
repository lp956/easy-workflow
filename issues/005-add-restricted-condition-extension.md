# 实现受限 Condition extension

- Label: `ready-for-agent`
- Priority: P1
- User stories: 22–26

## Parent

[easy-workflow 项目说明](../README.md)

## What to build

交付官方 Condition extension，使代码 Builder 和 Web JSON 能用同一受限 DSL 根据 Instance business data 选择一个 outcome，并继续复用现有 DAG edge 路由。Condition module 独占配置解析、类型校验和求值，Engine 不包含条件语言逻辑。

首个完整版本必须定义支持的数据类型和操作符、组合规则、默认分支、唯一匹配以及无匹配语义。配置不能执行任意 Go、JavaScript、模板、反射调用或外部 I/O。

## Acceptance criteria

- [ ] Condition config 完全可 JSON 序列化，并可由 Builder 直接使用。
- [ ] Definition 发布时校验字段引用、值类型、操作符和 outcome 配置。
- [ ] 激活时只读取防御性 business data，并返回正常的 Continue disposition 和 outcome。
- [ ] 字符串、数值、布尔和集合条件的支持范围及错误语义被明确记录并测试。
- [ ] 默认分支行为明确；无默认且无匹配时返回可识别错误。
- [ ] 多个规则同时命中时按明确的唯一性规则拒绝配置或返回错误，不依赖 slice 偶然顺序。
- [ ] 相同配置和输入始终产生相同 outcome。
- [ ] 恶意或畸形 JSON 不能触发代码执行、外部 I/O、panic 或静默 fallback。
- [ ] 示例覆盖 Web JSON 定义经过发布后执行条件分支的完整路径。

## Blocked by

- [001 预编译并完整校验 Definition](001-compile-and-validate-definitions.md)
- [002 发布并运行不可变 Definition 版本](002-publish-immutable-definition-versions.md)
