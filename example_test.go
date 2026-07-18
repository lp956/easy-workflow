// Package workflow_test contains executable documentation for common library composition.
// Examples use only public APIs and the process-local MemoryStore, so they require no infrastructure.
package workflow_test

import (
	"context"
	"fmt"

	workflow "github.com/lvpeng/easy-workflow"
	"github.com/lvpeng/easy-workflow/approval"
)

// Example demonstrates a complete in-memory approval using only the core package and official Approval extension.
//
// The example has no configuration-file, database, HTTP, Redis, or organization-directory dependency. All state
// lives in the process-local MemoryStore and disappears with the process; failures are surfaced as Go errors.
func Example() {
	registry := workflow.NewRegistry()
	if err := registry.Register(approval.Kind, approval.NewHandler()); err != nil {
		panic(err)
	}
	engine := workflow.NewEngine(workflow.NewMemoryStore(), registry)

	// Build the smallest useful flow: enter one or-sign approval and complete on its approved outcome.
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

	// Start compiles and freezes the supplied definition before MemoryStore atomically creates the instance.
	instance, err := engine.Start(context.Background(), definition, workflow.StartRequest{
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

// ExampleDefinitionPublisher_versions demonstrates Builder and JSON publication followed by exact and latest starts.
//
// Both authoring paths publish the canonical Definition type. LoadLatest selects a version once, after which
// StartPublished starts that exact immutable version; later publications cannot change either instance snapshot.
func ExampleDefinitionPublisher_versions() {
	registry := workflow.NewRegistry()
	if err := registry.Register(approval.Kind, approval.NewHandler()); err != nil {
		panic(err)
	}
	definitions := workflow.NewMemoryDefinitionStore()
	publisher := workflow.NewDefinitionPublisher(definitions, registry)
	engine := workflow.NewEngine(workflow.NewMemoryStore(), registry)

	// Publish version 1 from the code Builder and start it by its exact stable identity.
	builder := workflow.NewBuilder("leave-request")
	builder.Start("start")
	builder.Node("manager-approval", approval.Kind, approval.Config{
		Mode:      approval.ModeAny,
		Assignees: []workflow.ActorID{"manager-a"},
	})
	builder.End("end")
	builder.Connect("start", "manager-approval", "")
	builder.Connect("manager-approval", "end", approval.OutcomeApproved)
	definition, err := builder.Build()
	if err != nil {
		panic(err)
	}
	first, err := publisher.Publish(context.Background(), definition)
	if err != nil {
		panic(err)
	}
	exact, err := engine.StartPublished(context.Background(), definitions, first.ID, first.Version, workflow.StartRequest{
		ID:        "leave-v1",
		Initiator: "employee-a",
	})
	if err != nil {
		panic(err)
	}

	// Publish version 2 from web-style JSON using the same ID and canonical Definition schema.
	definitionJSON := []byte(`{
		"id":"leave-request",
		"nodes":[
			{"id":"start","kind":"start"},
			{"id":"manager-approval","kind":"approval","config":{"mode":"any","assignees":["manager-b"]}},
			{"id":"end","kind":"end"}
		],
		"edges":[
			{"from":"start","to":"manager-approval","outcome":""},
			{"from":"manager-approval","to":"end","outcome":"approved"}
		]
	}`)
	if _, err := publisher.PublishJSON(context.Background(), definitionJSON); err != nil {
		panic(err)
	}

	// Resolve "latest" to an immutable identity, then start exactly that version to avoid a moving target.
	latest, err := definitions.LoadLatest(context.Background(), "leave-request")
	if err != nil {
		panic(err)
	}
	latestInstance, err := engine.StartPublished(
		context.Background(),
		definitions,
		latest.ID,
		latest.Version,
		workflow.StartRequest{ID: "leave-latest", Initiator: "employee-b"},
	)
	if err != nil {
		panic(err)
	}

	fmt.Println(exact.Definition.Version, exact.Tasks[0].Assignee)
	fmt.Println(latestInstance.Definition.Version, latestInstance.Tasks[0].Assignee)
	// Output:
	// 1 manager-a
	// 2 manager-b
}
