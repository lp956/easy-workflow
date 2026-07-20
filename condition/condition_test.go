// Package condition_test verifies the public, serialization-safe condition handler contract.
// Tests exercise only Validate and Activate; parsing and evaluation internals remain replaceable.
package condition_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	workflow "github.com/lvpeng/easy-workflow"
	"github.com/lvpeng/easy-workflow/condition"
)

// TestActivateSelectsStringEqualityOutcome verifies the smallest code-authored condition route.
func TestActivateSelectsStringEqualityOutcome(t *testing.T) {
	t.Parallel()

	// Marshal the public configuration to exercise the same boundary used by workflow.Builder.
	config, err := json.Marshal(condition.Config{Rules: []condition.Rule{{
		Match:   condition.MatchAll,
		Outcome: "domestic",
		Conditions: []condition.Expression{{
			Field:    "/country",
			Type:     condition.TypeString,
			Operator: condition.OperatorEqual,
			Value:    "CN",
		}},
	}}})
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	// Activation evaluates a defensive JSON value and returns ordinary DAG-routing output.
	result, err := condition.NewHandler().Activate(context.Background(), workflow.ActivationInput{
		Config: config,
		Data:   json.RawMessage(`{"country":"CN"}`),
	})
	if err != nil {
		t.Fatalf("Activate() error = %v", err)
	}
	if result.Disposition != workflow.DispositionContinue || result.Outcome != "domestic" {
		t.Fatalf("Activate() result = %#v, want Continue outcome %q", result, "domestic")
	}
}

// TestActivateSupportsTypedOperators verifies every allow-listed type and operator through Activate.
func TestActivateSupportsTypedOperators(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		data      string
		valueType condition.ValueType
		operator  condition.Operator
		value     any
	}{
		{name: "string not equal", data: `{"value":"draft"}`, valueType: condition.TypeString, operator: condition.OperatorNotEqual, value: "final"},
		{name: "string contains", data: `{"value":"north-region"}`, valueType: condition.TypeString, operator: condition.OperatorContains, value: "region"},
		{name: "string starts with", data: `{"value":"north-region"}`, valueType: condition.TypeString, operator: condition.OperatorStartsWith, value: "north"},
		{name: "string ends with", data: `{"value":"north-region"}`, valueType: condition.TypeString, operator: condition.OperatorEndsWith, value: "region"},
		{name: "number equal", data: `{"value":9007199254740993}`, valueType: condition.TypeNumber, operator: condition.OperatorEqual, value: json.Number("9007199254740993")},
		{name: "number not equal", data: `{"value":2}`, valueType: condition.TypeNumber, operator: condition.OperatorNotEqual, value: 3},
		{name: "number greater", data: `{"value":3}`, valueType: condition.TypeNumber, operator: condition.OperatorGreaterThan, value: 2},
		{name: "number greater or equal", data: `{"value":3}`, valueType: condition.TypeNumber, operator: condition.OperatorGreaterOrEqual, value: 3},
		{name: "number less", data: `{"value":2}`, valueType: condition.TypeNumber, operator: condition.OperatorLessThan, value: 3},
		{name: "number less or equal", data: `{"value":2}`, valueType: condition.TypeNumber, operator: condition.OperatorLessOrEqual, value: 2},
		{name: "boolean equal", data: `{"value":true}`, valueType: condition.TypeBoolean, operator: condition.OperatorEqual, value: true},
		{name: "boolean not equal", data: `{"value":true}`, valueType: condition.TypeBoolean, operator: condition.OperatorNotEqual, value: false},
		{name: "collection contains", data: `{"value":["ops","finance"]}`, valueType: condition.TypeCollection, operator: condition.OperatorContains, value: "finance"},
		{name: "collection contains any", data: `{"value":["ops","finance"]}`, valueType: condition.TypeCollection, operator: condition.OperatorContainsAny, value: []string{"legal", "ops"}},
		{name: "collection contains all", data: `{"value":["ops","finance"]}`, valueType: condition.TypeCollection, operator: condition.OperatorContainsAll, value: []string{"finance", "ops"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			config, err := json.Marshal(condition.Config{Rules: []condition.Rule{{
				Match:   condition.MatchAll,
				Outcome: "matched",
				Conditions: []condition.Expression{{
					Field:    "/value",
					Type:     test.valueType,
					Operator: test.operator,
					Value:    test.value,
				}},
			}}})
			if err != nil {
				t.Fatalf("json.Marshal() error = %v", err)
			}

			result, err := condition.NewHandler().Activate(context.Background(), workflow.ActivationInput{
				Config: config,
				Data:   json.RawMessage(test.data),
			})
			if err != nil {
				t.Fatalf("Activate() error = %v", err)
			}
			if result.Disposition != workflow.DispositionContinue || result.Outcome != "matched" {
				t.Fatalf("Activate() result = %#v, want Continue outcome %q", result, "matched")
			}
		})
	}
}

// TestActivateSupportsExplicitMatchModes verifies deterministic all-sign and any-sign expression combination.
func TestActivateSupportsExplicitMatchModes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		match condition.MatchMode
	}{
		{name: "all expressions", match: condition.MatchAll},
		{name: "any expression", match: condition.MatchAny},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			// The second expression matches only in any mode; all mode receives an independently all-true value.
			secondExpected := "finance"
			if test.match == condition.MatchAny {
				secondExpected = "legal"
			}
			config, err := json.Marshal(condition.Config{Rules: []condition.Rule{{
				Match:   test.match,
				Outcome: "matched",
				Conditions: []condition.Expression{
					{Field: "/region", Type: condition.TypeString, Operator: condition.OperatorEqual, Value: "north"},
					{Field: "/department", Type: condition.TypeString, Operator: condition.OperatorEqual, Value: secondExpected},
				},
			}}})
			if err != nil {
				t.Fatalf("json.Marshal() error = %v", err)
			}

			result, err := condition.NewHandler().Activate(context.Background(), workflow.ActivationInput{
				Config: config,
				Data:   json.RawMessage(`{"region":"north","department":"finance"}`),
			})
			if err != nil {
				t.Fatalf("Activate() error = %v", err)
			}
			if result.Outcome != "matched" {
				t.Fatalf("Activate() outcome = %q, want %q", result.Outcome, "matched")
			}
		})
	}
}

// TestActivateReportsExpressionErrorsRegardlessOfOrder verifies match modes cannot hide invalid business data.
func TestActivateReportsExpressionErrorsRegardlessOfOrder(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		match      condition.MatchMode
		knownValue string
	}{
		{name: "any after true", match: condition.MatchAny, knownValue: "north"},
		{name: "all after false", match: condition.MatchAll, knownValue: "south"},
	}
	for _, test := range tests {
		for _, reverse := range []bool{false, true} {
			name := test.name + " forward"
			if reverse {
				name = test.name + " reversed"
			}
			t.Run(name, func(t *testing.T) {
				t.Parallel()

				expressions := []condition.Expression{
					{Field: "/region", Type: condition.TypeString, Operator: condition.OperatorEqual, Value: test.knownValue},
					{Field: "/missing", Type: condition.TypeString, Operator: condition.OperatorEqual, Value: "value"},
				}
				if reverse {
					expressions[0], expressions[1] = expressions[1], expressions[0]
				}
				config, err := json.Marshal(condition.Config{
					Rules:          []condition.Rule{{Match: test.match, Outcome: "matched", Conditions: expressions}},
					DefaultOutcome: "default",
				})
				if err != nil {
					t.Fatalf("json.Marshal() error = %v", err)
				}

				_, err = condition.NewHandler().Activate(t.Context(), workflow.ActivationInput{
					Config: config,
					Data:   json.RawMessage(`{"region":"north"}`),
				})
				if !errors.Is(err, condition.ErrFieldNotFound) {
					t.Fatalf("Activate() error = %v, want ErrFieldNotFound", err)
				}
			})
		}
	}
}

// TestActivateEnforcesUniqueRuleSelection verifies explicit default, no-match, and overlap behavior.
func TestActivateEnforcesUniqueRuleSelection(t *testing.T) {
	t.Parallel()

	rule := func(expected, outcome string) condition.Rule {
		return condition.Rule{
			Match:   condition.MatchAll,
			Outcome: outcome,
			Conditions: []condition.Expression{{
				Field: "/status", Type: condition.TypeString, Operator: condition.OperatorEqual, Value: expected,
			}},
		}
	}
	tests := []struct {
		name        string
		config      condition.Config
		wantOutcome string
		wantErr     error
	}{
		{
			name:        "explicit default",
			config:      condition.Config{Rules: []condition.Rule{rule("approved", "accepted")}, DefaultOutcome: "manual"},
			wantOutcome: "manual",
		},
		{
			name:    "no match without default",
			config:  condition.Config{Rules: []condition.Rule{rule("approved", "accepted")}},
			wantErr: condition.ErrNoMatch,
		},
		{
			name: "overlap in forward order",
			config: condition.Config{Rules: []condition.Rule{
				rule("pending", "first"),
				rule("pending", "second"),
			}},
			wantErr: condition.ErrMultipleMatches,
		},
		{
			name: "overlap in reverse order",
			config: condition.Config{Rules: []condition.Rule{
				rule("pending", "second"),
				rule("pending", "first"),
			}},
			wantErr: condition.ErrMultipleMatches,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			config, err := json.Marshal(test.config)
			if err != nil {
				t.Fatalf("json.Marshal() error = %v", err)
			}
			result, err := condition.NewHandler().Activate(context.Background(), workflow.ActivationInput{
				Config: config,
				Data:   json.RawMessage(`{"status":"pending"}`),
			})
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("Activate() error = %v, want %v", err, test.wantErr)
			}
			if test.wantErr == nil && result.Outcome != test.wantOutcome {
				t.Fatalf("Activate() outcome = %q, want %q", result.Outcome, test.wantOutcome)
			}
		})
	}
}

// TestActivateResolvesJSONPointerFields verifies nested object lookup and RFC 6901 token escaping.
func TestActivateResolvesJSONPointerFields(t *testing.T) {
	t.Parallel()

	config, err := json.Marshal(condition.Config{Rules: []condition.Rule{{
		Match:   condition.MatchAll,
		Outcome: "matched",
		Conditions: []condition.Expression{
			{Field: "/applicant/profile~1kind", Type: condition.TypeString, Operator: condition.OperatorEqual, Value: "employee"},
			{Field: "/applicant/~0priority", Type: condition.TypeBoolean, Operator: condition.OperatorEqual, Value: true},
		},
	}}})
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	result, err := condition.NewHandler().Activate(context.Background(), workflow.ActivationInput{
		Config: config,
		Data:   json.RawMessage(`{"applicant":{"profile/kind":"employee","~priority":true}}`),
	})
	if err != nil {
		t.Fatalf("Activate() error = %v", err)
	}
	if result.Outcome != "matched" {
		t.Fatalf("Activate() outcome = %q, want %q", result.Outcome, "matched")
	}
}

// TestValidateRejectsInvalidConfig verifies strict schema and allow-list enforcement at publication time.
func TestValidateRejectsInvalidConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		config string
	}{
		{name: "malformed JSON", config: `{"rules":[`},
		{name: "trailing JSON value", config: `{"rules":[]} {}`},
		{name: "unknown top-level field", config: `{"rules":[{"match":"all","conditions":[{"field":"/x","type":"string","operator":"eq","value":"a"}],"outcome":"yes"}],"script":"run()"}`},
		{name: "unknown expression field", config: `{"rules":[{"match":"all","conditions":[{"field":"/x","type":"string","operator":"eq","value":"a","template":"{{exec}}"}],"outcome":"yes"}]}`},
		{name: "duplicate key", config: `{"rules":[],"rules":[{"match":"all","conditions":[{"field":"/x","type":"string","operator":"eq","value":"a"}],"outcome":"yes"}]}`},
		{name: "empty rules", config: `{"rules":[]}`},
		{name: "implicit match mode", config: `{"rules":[{"conditions":[{"field":"/x","type":"string","operator":"eq","value":"a"}],"outcome":"yes"}]}`},
		{name: "empty conditions", config: `{"rules":[{"match":"all","conditions":[],"outcome":"yes"}]}`},
		{name: "empty outcome", config: `{"rules":[{"match":"all","conditions":[{"field":"/x","type":"string","operator":"eq","value":"a"}],"outcome":""}]}`},
		{name: "invalid pointer escape", config: `{"rules":[{"match":"all","conditions":[{"field":"/x~2y","type":"string","operator":"eq","value":"a"}],"outcome":"yes"}]}`},
		{name: "unsupported type", config: `{"rules":[{"match":"all","conditions":[{"field":"/x","type":"object","operator":"eq","value":{}}],"outcome":"yes"}]}`},
		{name: "operator outside type", config: `{"rules":[{"match":"all","conditions":[{"field":"/x","type":"boolean","operator":"contains","value":true}],"outcome":"yes"}]}`},
		{name: "wrong value type", config: `{"rules":[{"match":"all","conditions":[{"field":"/x","type":"number","operator":"gt","value":"1"}],"outcome":"yes"}]}`},
		{name: "empty collection query", config: `{"rules":[{"match":"all","conditions":[{"field":"/x","type":"collection","operator":"contains_any","value":[]}],"outcome":"yes"}]}`},
		{name: "object collection operand", config: `{"rules":[{"match":"all","conditions":[{"field":"/x","type":"collection","operator":"contains","value":{"call":"exec"}}],"outcome":"yes"}]}`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := condition.NewHandler().Validate(json.RawMessage(test.config))
			if !errors.Is(err, condition.ErrInvalidConfig) {
				t.Fatalf("Validate() error = %v, want ErrInvalidConfig", err)
			}
		})
	}
}

// TestActivateReportsBusinessDataErrors verifies malformed, missing, and mistyped data never become false matches.
func TestActivateReportsBusinessDataErrors(t *testing.T) {
	t.Parallel()

	config, err := json.Marshal(condition.Config{
		Rules: []condition.Rule{{
			Match:   condition.MatchAll,
			Outcome: "matched",
			Conditions: []condition.Expression{{
				Field: "/request/amount", Type: condition.TypeNumber, Operator: condition.OperatorGreaterThan, Value: 100,
			}},
		}},
		DefaultOutcome: "default",
	})
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	tests := []struct {
		name    string
		data    string
		wantErr error
	}{
		{name: "malformed JSON", data: `{"request":`, wantErr: condition.ErrInvalidData},
		{name: "non-object root", data: `[]`, wantErr: condition.ErrInvalidData},
		{name: "duplicate business field", data: `{"request":{"amount":100,"amount":200}}`, wantErr: condition.ErrInvalidData},
		{name: "missing field", data: `{"request":{}}`, wantErr: condition.ErrFieldNotFound},
		{name: "non-object path segment", data: `{"request":"unknown"}`, wantErr: condition.ErrFieldNotFound},
		{name: "type mismatch", data: `{"request":{"amount":"200"}}`, wantErr: condition.ErrTypeMismatch},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := condition.NewHandler().Activate(context.Background(), workflow.ActivationInput{
				Config: config,
				Data:   json.RawMessage(test.data),
			})
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("Activate() error = %v, want %v", err, test.wantErr)
			}
		})
	}
}

// TestActivateRejectsUnsupportedCollectionMembers verifies complex JSON values are not silently comparable.
func TestActivateRejectsUnsupportedCollectionMembers(t *testing.T) {
	t.Parallel()

	config, err := json.Marshal(condition.Config{
		Rules: []condition.Rule{{
			Match:   condition.MatchAll,
			Outcome: "matched",
			Conditions: []condition.Expression{{
				Field: "/values", Type: condition.TypeCollection, Operator: condition.OperatorContains, Value: "approved",
			}},
		}},
		DefaultOutcome: "default",
	})
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	for _, data := range []string{
		`{"values":[{"status":"approved"}]}`,
		`{"values":[["approved"]]}`,
		`{"values":[null]}`,
	} {
		_, err := condition.NewHandler().Activate(context.Background(), workflow.ActivationInput{
			Config: config,
			Data:   json.RawMessage(data),
		})
		if !errors.Is(err, condition.ErrTypeMismatch) {
			t.Fatalf("Activate(%s) error = %v, want ErrTypeMismatch", data, err)
		}
	}
}

// TestActivateIsDeterministic verifies repeated evaluation of identical config and data returns one stable result.
func TestActivateIsDeterministic(t *testing.T) {
	t.Parallel()

	config, err := json.Marshal(condition.Config{Rules: []condition.Rule{{
		Match:   condition.MatchAll,
		Outcome: "matched",
		Conditions: []condition.Expression{{
			Field: "/amount", Type: condition.TypeNumber, Operator: condition.OperatorGreaterOrEqual, Value: 100,
		}},
	}}})
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	input := workflow.ActivationInput{Config: config, Data: json.RawMessage(`{"amount":125}`)}
	handler := condition.NewHandler()

	first, err := handler.Activate(context.Background(), input)
	if err != nil {
		t.Fatalf("first Activate() error = %v", err)
	}
	for range 100 {
		result, err := handler.Activate(context.Background(), input)
		if err != nil {
			t.Fatalf("repeated Activate() error = %v", err)
		}
		if !reflect.DeepEqual(result, first) {
			t.Fatalf("repeated Activate() result = %#v, want %#v", result, first)
		}
	}
}
