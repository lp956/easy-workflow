// This file verifies Condition's optional request-local prepared-config contract against its legacy public behavior.
// It compares public evaluation results and command errors without inspecting the evaluator's decoded rule structures.
package condition_test

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	workflow "github.com/lvpeng/easy-workflow"
	"github.com/lvpeng/easy-workflow/condition"
)

// TestConditionPreparedConfigMatchesLegacyExecution verifies one decoded rule set serves deterministic evaluation.
func TestConditionPreparedConfigMatchesLegacyExecution(t *testing.T) {
	t.Parallel()

	// Encode one restricted rule and require the official handler to expose the optional preparation seam.
	config, err := json.Marshal(condition.Config{
		Rules: []condition.Rule{{
			Match: condition.MatchAll,
			Conditions: []condition.Expression{{
				Field:    "/route",
				Type:     condition.TypeString,
				Operator: condition.OperatorEqual,
				Value:    "a",
			}},
			Outcome: "matched",
		}},
	})
	if err != nil {
		t.Fatalf("json.Marshal(config) error = %v", err)
	}
	handler := condition.NewHandler()
	preparer, ok := any(handler).(workflow.NodeHandlerConfigPreparer)
	if !ok {
		t.Fatal("Condition handler does not implement NodeHandlerConfigPreparer")
	}
	prepared, err := preparer.PrepareConfig(config)
	if err != nil {
		t.Fatalf("PrepareConfig() error = %v", err)
	}

	// Prepared and legacy activation must evaluate identical business data to the same declared outcome.
	data := json.RawMessage(`{"route":"a"}`)
	legacyResult, err := handler.Activate(t.Context(), workflow.ActivationInput{Config: config, Data: data})
	if err != nil {
		t.Fatalf("Activate() error = %v", err)
	}
	preparedResult, err := prepared.ActivatePrepared(t.Context(), workflow.PreparedActivationInput{Data: data})
	if err != nil {
		t.Fatalf("ActivatePrepared() error = %v", err)
	}
	if !reflect.DeepEqual(preparedResult, legacyResult) {
		t.Errorf("ActivatePrepared() = %#v, want legacy %#v", preparedResult, legacyResult)
	}

	// Condition remains synchronous and both command paths preserve the same public invalid-command classification.
	_, legacyErr := handler.Handle(t.Context(), workflow.CommandInput{})
	_, preparedErr := prepared.HandlePrepared(t.Context(), workflow.PreparedCommandInput{})
	if !errors.Is(legacyErr, workflow.ErrInvalidCommand) || !errors.Is(preparedErr, workflow.ErrInvalidCommand) {
		t.Errorf("command errors = legacy:%v prepared:%v, want ErrInvalidCommand", legacyErr, preparedErr)
	}
}
