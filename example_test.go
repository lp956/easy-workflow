// Package workflow_test contains executable documentation for common library composition.
// Examples use only public APIs and the process-local MemoryStore, so they require no infrastructure.
package workflow_test

import (
	"context"
	"encoding/json"
	"fmt"

	workflow "github.com/lvpeng/easy-workflow"
	"github.com/lvpeng/easy-workflow/approval"
)

// Example demonstrates Builder and JSON publication followed by an exact-version approval run.
func Example() {
	registry := workflow.NewRegistry()
	if err := registry.Register(approval.Kind, approval.NewHandler()); err != nil {
		panic(err)
	}
	definitions := workflow.NewMemoryDefinitionStore()
	publisher := workflow.NewDefinitionPublisher(definitions, registry)
	engine := workflow.NewEngine(workflow.NewMemoryStore(), registry)

	// Code-authored and web-authored definitions converge on this same serializable graph model.
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
	if err != nil {
		panic(err)
	}
	published, err := publisher.Publish(context.Background(), definition)
	if err != nil {
		panic(err)
	}

	// Web-authored JSON enters the same canonical compiler and receives the next immutable version.
	data, err := json.Marshal(published)
	if err != nil {
		panic(err)
	}
	if _, err := publisher.PublishJSON(context.Background(), data); err != nil {
		panic(err)
	}

	// Startup selects version 2 explicitly; the instance freezes that full published Definition snapshot.
	instance, err := engine.StartPublished(context.Background(), definitions, "leave-request", 2, workflow.StartRequest{
		ID:        "leave-1",
		Initiator: "employee-a",
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(instance.Status, len(instance.Tasks))

	// Or-sign completes after the first assigned manager approves and closes the sibling task.
	instance, err = engine.Handle(context.Background(), workflow.Command{
		InstanceID: instance.ID,
		TaskID:     instance.Tasks[0].ID,
		ActorID:    instance.Tasks[0].Assignee,
		Name:       approval.CommandApprove,
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(instance.Status)

	// Output:
	// running 2
	// completed
}
