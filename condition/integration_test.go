// Package condition_test verifies condition integration through public workflow publication and execution APIs.
// It does not inspect compiler plans, stores, or handler internals, so routing implementation remains replaceable.
package condition_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	workflow "github.com/lvpeng/easy-workflow"
	"github.com/lvpeng/easy-workflow/condition"
)

// ExampleHandler_webJSON demonstrates the complete web-authored publication and condition-routing path.
func ExampleHandler_webJSON() {
	registry := workflow.NewRegistry()
	if err := registry.Register(condition.Kind, condition.NewHandler()); err != nil {
		fmt.Printf("register: %v\n", err)
		return
	}
	definitions := workflow.NewMemoryDefinitionStore()
	publisher := workflow.NewDefinitionPublisher(definitions, registry)

	// This canonical JSON is representative of a web editor payload and contains no executable values.
	definitionJSON := []byte(`{
		"id":"expense-routing",
		"version":0,
		"nodes":[
			{"id":"start","kind":"start"},
			{"id":"amount-condition","kind":"condition","config":{
				"rules":[{"match":"all","conditions":[
					{"field":"/expense/amount","type":"number","operator":"gte","value":1000}
				],"outcome":"review"}],
				"defaultOutcome":"automatic"
			}},
			{"id":"automatic-end","kind":"end"},
			{"id":"review-end","kind":"end"}
		],
		"edges":[
			{"from":"start","to":"amount-condition","outcome":""},
			{"from":"amount-condition","to":"automatic-end","outcome":"automatic"},
			{"from":"amount-condition","to":"review-end","outcome":"review"}
		]
	}`)
	published, err := publisher.PublishJSON(context.Background(), definitionJSON)
	if err != nil {
		fmt.Printf("publish: %v\n", err)
		return
	}

	// Exact-version startup proves the persisted config is evaluated and routed by the existing DAG engine.
	engine := workflow.NewEngine(workflow.NewMemoryStore(), registry)
	instance, err := engine.StartPublished(
		context.Background(),
		definitions,
		published.ID,
		published.Version,
		workflow.StartRequest{
			ID:        "expense-001",
			Initiator: "alice",
			Data:      json.RawMessage(`{"expense":{"amount":1250}}`),
		},
	)
	if err != nil {
		fmt.Printf("start: %v\n", err)
		return
	}
	fmt.Println(instance.Status, instance.CurrentNodeID)
	// Output: completed review-end
}

// TestBuilderAcceptsConditionConfig verifies the same public Config value works without hand-authored JSON.
func TestBuilderAcceptsConditionConfig(t *testing.T) {
	t.Parallel()

	builder := workflow.NewBuilder("code-authored-condition")
	builder.Start("start")
	builder.Node("condition", condition.Kind, condition.Config{
		Rules: []condition.Rule{{
			Match:   condition.MatchAll,
			Outcome: "yes",
			Conditions: []condition.Expression{{
				Field: "/enabled", Type: condition.TypeBoolean, Operator: condition.OperatorEqual, Value: true,
			}},
		}},
		DefaultOutcome: "no",
	})
	builder.End("yes-end")
	builder.End("no-end")
	builder.Connect("start", "condition", "")
	builder.Connect("condition", "yes-end", "yes")
	builder.Connect("condition", "no-end", "no")
	definition, err := builder.Build()
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	registry := workflow.NewRegistry()
	if err := registry.Register(condition.Kind, condition.NewHandler()); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if err := workflow.CompileDefinition(definition, registry); err != nil {
		t.Fatalf("CompileDefinition() error = %v", err)
	}
}

// TestPublicationRejectsInvalidConditionConfig verifies compiler publication invokes the condition validator.
func TestPublicationRejectsInvalidConditionConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		config string
	}{
		{name: "field reference", config: `{"rules":[{"match":"all","conditions":[{"field":"amount","type":"number","operator":"gt","value":1}],"outcome":"yes"}]}`},
		{name: "value type", config: `{"rules":[{"match":"all","conditions":[{"field":"/amount","type":"number","operator":"gt","value":"1"}],"outcome":"yes"}]}`},
		{name: "operator", config: `{"rules":[{"match":"all","conditions":[{"field":"/amount","type":"number","operator":"contains","value":1}],"outcome":"yes"}]}`},
		{name: "outcome", config: `{"rules":[{"match":"all","conditions":[{"field":"/amount","type":"number","operator":"gt","value":1}],"outcome":""}]}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			registry := workflow.NewRegistry()
			if err := registry.Register(condition.Kind, condition.NewHandler()); err != nil {
				t.Fatalf("Register() error = %v", err)
			}
			definitions := workflow.NewMemoryDefinitionStore()
			publisher := workflow.NewDefinitionPublisher(definitions, registry)
			definition := &workflow.Definition{
				ID: "invalid-condition-" + test.name,
				Nodes: []workflow.NodeDefinition{
					{ID: "start", Kind: workflow.KindStart},
					{ID: "condition", Kind: condition.Kind, Config: json.RawMessage(test.config)},
					{ID: "end", Kind: workflow.KindEnd},
				},
				Edges: []workflow.Edge{
					{From: "start", To: "condition"},
					{From: "condition", To: "end", Outcome: "yes"},
				},
			}

			_, err := publisher.Publish(context.Background(), definition)
			if !errors.Is(err, condition.ErrInvalidConfig) {
				t.Fatalf("Publish() error = %v, want ErrInvalidConfig", err)
			}
			if _, err := definitions.LoadLatest(context.Background(), definition.ID); !errors.Is(err, workflow.ErrDefinitionNotFound) {
				t.Fatalf("LoadLatest() error = %v, want ErrDefinitionNotFound", err)
			}
		})
	}
}
