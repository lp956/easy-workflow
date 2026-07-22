// This file verifies detached MySQL snapshots without external database I/O.
package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"

	workflow "github.com/lvpeng/easy-workflow"
)

// TestEncodeSnapshotDetachesTransactionAggregate verifies every later transaction write sees one caller-independent candidate.
func TestEncodeSnapshotDetachesTransactionAggregate(t *testing.T) {
	t.Parallel()

	source := &workflow.Instance{
		ID: "instance-a",
		Definition: workflow.Definition{
			ID:      "definition-a",
			Version: 2,
			Nodes: []workflow.NodeDefinition{{
				ID:     "node-a",
				Kind:   "test",
				Config: json.RawMessage(`{"role":"original"}`),
			}},
		},
		Data:  json.RawMessage(`{"source":"original"}`),
		Tasks: []workflow.Task{{ID: "task-a", NodeID: "node-a", Assignee: "actor-a", Status: workflow.TaskStatusActive}},
		Audit: []workflow.AuditRecord{{Action: "instance.started"}},
	}
	// Encode before mutating the source to model caller changes during later transaction work.
	snapshot, err := encodeSnapshot(source)
	if err != nil {
		t.Fatalf("encodeSnapshot() error = %v", err)
	}

	// Mutate every mutable category that the transaction could otherwise read after encoding.
	source.ID = "instance-b"
	source.Definition.ID = "definition-b"
	source.Definition.Nodes[0].Config[0] = 'X'
	source.Data[0] = 'X'
	source.Tasks[0].ID = "task-b"
	source.Audit[0].Action = "mutated"

	// Assert the persisted candidate remains the original detached aggregate.
	if snapshot.aggregate.ID != "instance-a" || snapshot.aggregate.Definition.ID != "definition-a" {
		t.Fatalf("snapshot identity = %q/%q, want instance-a/definition-a", snapshot.aggregate.ID, snapshot.aggregate.Definition.ID)
	}
	if string(snapshot.aggregate.Definition.Nodes[0].Config) != `{"role":"original"}` {
		t.Errorf("snapshot config = %s, want original JSON", snapshot.aggregate.Definition.Nodes[0].Config)
	}
	if string(snapshot.aggregate.Data) != `{"source":"original"}` {
		t.Errorf("snapshot data = %s, want original JSON", snapshot.aggregate.Data)
	}
	if snapshot.aggregate.Tasks[0].ID != "task-a" || snapshot.aggregate.Audit[0].Action != "instance.started" {
		t.Errorf("snapshot facts = task %q / audit %q, want task-a / instance.started", snapshot.aggregate.Tasks[0].ID, snapshot.aggregate.Audit[0].Action)
	}
}

// TestClassifyCommitErrorPreservesCancellation verifies the commit race maps an ended transaction to its context.
func TestClassifyCommitErrorPreservesCancellation(t *testing.T) {
	t.Parallel()

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := classifyCommitError(canceled, sql.ErrTxDone); !errors.Is(err, context.Canceled) {
		t.Fatalf("classifyCommitError(canceled, ErrTxDone) = %v, want context.Canceled", err)
	}
	if err := classifyCommitError(context.Background(), sql.ErrTxDone); !errors.Is(err, sql.ErrTxDone) {
		t.Fatalf("classifyCommitError(active, ErrTxDone) = %v, want sql.ErrTxDone", err)
	}
	driverErr := errors.New("driver commit failure")
	if !errors.Is(classifyCommitError(context.Background(), driverErr), driverErr) {
		t.Fatalf("classifyCommitError(driver error) did not preserve %v", driverErr)
	}
}

// TestDecodeJSONRejectsAmbiguousDurablePayloads verifies old readers cannot silently discard new or malformed fields.
func TestDecodeJSONRejectsAmbiguousDurablePayloads(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		payload string
	}{
		{name: "unknown field", payload: `{"id":"task-a","nodeId":"node-a","assignee":"actor-a","status":"active","future":true}`},
		{name: "case alias", payload: `{"ID":"task-a","nodeId":"node-a","assignee":"actor-a","status":"active"}`},
		{name: "duplicate field", payload: `{"id":"task-a","id":"task-b","nodeId":"node-a","assignee":"actor-a","status":"active"}`},
		{name: "lone surrogate", payload: `{"id":"\uD800"}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			var task workflow.Task
			if err := decodeJSON([]byte(test.payload), &task); err == nil {
				t.Fatalf("decodeJSON(%s) error = nil, want rejection", test.payload)
			}
		})
	}
}
