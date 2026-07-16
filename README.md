# easy-workflow

`easy-workflow` 是一个可嵌入 Go 项目的人工审批流内核。核心只负责流程图校验、状态流转、任务状态、并发版本和审计记录；具体业务节点通过显式注册的处理器扩展。

当前骨架已经提供：

- 代码 Builder 与 JSON 共用的流程定义；
- DAG 环路、不可达节点、死路和歧义路由校验；
- 可替换的 `Store` 接口和并发安全的 `MemoryStore`；
- 官方 `approval` 节点；
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

