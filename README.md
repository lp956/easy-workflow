# easy-workflow

`easy-workflow` 是一个可嵌入 Go 项目的人工审批流内核。核心只负责流程图校验、状态流转、任务状态、并发版本和审计记录；具体业务节点通过显式注册的处理器扩展。

当前骨架已经提供：

- 代码 Builder 与 JSON 共用的流程定义；
- DAG 环路、不可达节点、死路和歧义路由校验；
- 可替换的 `Store` 接口和并发安全的 `MemoryStore`；
- 官方 `approval` 节点；
- 官方受限 `condition` 节点；
- 或签、会签和拒绝；
- 实例定义快照、乐观版本和不可变审计记录。

暂未提供数据库适配器、HTTP API、Web 设计器、撤回、退回和转派。

## 安装

```bash
go get github.com/lvpeng/easy-workflow
```

## 最小请假流程

```go
registry := workflow.NewRegistry()
if err := registry.Register(approval.Kind, approval.NewHandler()); err != nil {
    return err
}
engine := workflow.NewEngine(workflow.NewMemoryStore(), registry)

builder := workflow.NewBuilder("leave-request")
builder.Start("start")
builder.Node("manager-approval", approval.Kind, approval.Config{
    Mode:      approval.ModeAny,
    Assignees: []workflow.ActorID{"manager-a", "manager-b"},
})
builder.End("end")
builder.Connect("start", "manager-approval", "")
builder.Connect("manager-approval", "end", approval.OutcomeApproved)

definition, err := builder.Build()
```

完整的可执行闭环见 `example_test.go`。

## 条件路由

Condition 配置可直接传给 Builder，也可作为相同结构的 Web JSON 发布。字段使用 RFC 6901 JSON Pointer；每条规则显式选择 `all` 或 `any`，全部规则独立求值且只允许一个命中。零命中时使用 `defaultOutcome`，未配置默认分支则返回 `condition.ErrNoMatch`；多个规则命中返回 `condition.ErrMultipleMatches`。

```go
if err := registry.Register(condition.Kind, condition.NewHandler()); err != nil {
    return err
}

builder.Node("amount-condition", condition.Kind, condition.Config{
    Rules: []condition.Rule{{
        Match:   condition.MatchAll,
        Outcome: "review",
        Conditions: []condition.Expression{{
            Field:    "/expense/amount",
            Type:     condition.TypeNumber,
            Operator: condition.OperatorGreaterOrEqual,
            Value:    1000,
        }},
    }},
    DefaultOutcome: "automatic",
})
```

支持范围如下：

| 类型 | 操作符 | 值范围 |
| --- | --- | --- |
| `string` | `eq`, `neq`, `contains`, `starts_with`, `ends_with` | JSON 字符串 |
| `number` | `eq`, `neq`, `gt`, `gte`, `lt`, `lte` | 精确比较的 JSON 数值 |
| `boolean` | `eq`, `neq` | JSON 布尔值 |
| `collection` | `contains`, `contains_any`, `contains_all` | 字符串、数值或布尔基元数组 |

Condition 不进行跨类型转换，不支持 `null`、对象、嵌套集合、数组索引字段引用、脚本、模板、反射调用或外部 I/O。畸形业务数据、缺失字段和类型不符分别返回 `condition.ErrInvalidData`、`condition.ErrFieldNotFound` 和 `condition.ErrTypeMismatch`。Web JSON 发布到条件分支执行的完整示例见 `condition/integration_test.go`。
