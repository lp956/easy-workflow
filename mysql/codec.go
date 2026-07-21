// This file encodes workflow aggregates into lossless MySQL row payloads and reconstructs caller-owned values.
// It contains no SQL or external I/O; persistence ordering and transaction boundaries remain in store.go.
package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"unicode/utf8"

	workflow "github.com/lvpeng/easy-workflow"
)

// storedDefinition preserves raw node configuration bytes instead of applying JSON normalization.
type storedDefinition struct {
	ID      string          `json:"id"`
	Version uint64          `json:"version"`
	Nodes   []storedNode    `json:"nodes"`
	Edges   []workflow.Edge `json:"edges"`
}

// storedNode serializes Config as ordinary bytes so nil, empty, whitespace, and content remain distinct.
type storedNode struct {
	ID     string `json:"id"`
	Kind   string `json:"kind"`
	Config []byte `json:"config"`
}

// encodedSnapshot contains detached parent and collection payloads ready for one database transaction.
type encodedSnapshot struct {
	id            workflow.InstanceID
	definition    []byte
	status        workflow.InstanceStatus
	initiator     workflow.ActorID
	currentNodeID string
	data          []byte
	nodeState     []byte
	tasksNil      bool
	auditNil      bool
	version       uint64
	tasks         [][]byte
	audit         [][]byte
}

const maxStoredStringLength = 255

// encodeSnapshot creates lossless byte payloads without retaining caller-owned mutable slices.
func encodeSnapshot(instance *workflow.Instance) (encodedSnapshot, error) {
	if err := validateStoredValues(instance); err != nil {
		return encodedSnapshot{}, err
	}
	if err := validateStoredStringLengths(instance); err != nil {
		return encodedSnapshot{}, err
	}
	definition, err := encodeDefinition(instance.Definition)
	if err != nil {
		return encodedSnapshot{}, err
	}
	tasks, err := encodeValues(instance.Tasks)
	if err != nil {
		return encodedSnapshot{}, fmt.Errorf("encode tasks: %w", err)
	}
	audit, err := encodeValues(instance.Audit)
	if err != nil {
		return encodedSnapshot{}, fmt.Errorf("encode audit: %w", err)
	}
	return encodedSnapshot{
		id:            instance.ID,
		definition:    definition,
		status:        instance.Status,
		initiator:     instance.Initiator,
		currentNodeID: instance.CurrentNodeID,
		data:          cloneBytes(instance.Data),
		nodeState:     cloneBytes(instance.NodeState),
		tasksNil:      instance.Tasks == nil,
		auditNil:      instance.Audit == nil,
		version:       instance.Version,
		tasks:         tasks,
		audit:         audit,
	}, nil
}

// validateStoredValues mirrors the child-row CHECK constraints so invalid aggregates are rejected even when a host
// connects to a MySQL version that does not enforce CHECK constraints.
func validateStoredValues(instance *workflow.Instance) error {
	for index, task := range instance.Tasks {
		if task.ID == "" {
			return fmt.Errorf("%w: mysql task %d ID cannot be empty", workflow.ErrInvalidStoreInput, index)
		}
		if task.Status == "" {
			return fmt.Errorf("%w: mysql task %d status cannot be empty", workflow.ErrInvalidStoreInput, index)
		}
	}
	for index, record := range instance.Audit {
		if record.Action == "" {
			return fmt.Errorf("%w: mysql audit %d action cannot be empty", workflow.ErrInvalidStoreInput, index)
		}
	}
	return nil
}

// validateStoredStringLengths rejects values that would exceed MySQL's indexed VARCHAR columns before any write.
func validateStoredStringLengths(instance *workflow.Instance) error {
	fields := []struct {
		name  string
		value string
	}{
		{name: "instance ID", value: string(instance.ID)},
		{name: "status", value: string(instance.Status)},
		{name: "initiator", value: string(instance.Initiator)},
		{name: "current node ID", value: instance.CurrentNodeID},
	}
	for index, task := range instance.Tasks {
		fields = append(fields,
			struct {
				name  string
				value string
			}{name: fmt.Sprintf("task %d ID", index), value: string(task.ID)},
			struct {
				name  string
				value string
			}{name: fmt.Sprintf("task %d status", index), value: string(task.Status)},
		)
	}
	for index, record := range instance.Audit {
		fields = append(fields, struct {
			name  string
			value string
		}{name: fmt.Sprintf("audit %d action", index), value: record.Action})
	}
	for _, field := range fields {
		if utf8.RuneCountInString(field.value) > maxStoredStringLength {
			return fmt.Errorf("%w: mysql %s exceeds %d characters", workflow.ErrInvalidStoreInput, field.name, maxStoredStringLength)
		}
	}
	return nil
}

// parentArguments returns parameter values for one insert without interpolating workflow data into SQL.
func (s encodedSnapshot) parentArguments() []any {
	return []any{
		s.id,
		s.definition,
		s.status,
		s.initiator,
		s.currentNodeID,
		nullableBytes(s.data),
		nullableBytes(s.nodeState),
		s.tasksNil,
		s.auditNil,
		strconv.FormatUint(s.version, 10),
	}
}

// nullableBytes maps nil byte slices to SQL NULL while keeping non-nil empty slices as empty BLOB values.
func nullableBytes(value []byte) any {
	if value == nil {
		return nil
	}
	return value
}

// updateArguments returns update fields followed by the identity and independently supplied CAS version.
func (s encodedSnapshot) updateArguments(expectedVersion uint64) []any {
	return []any{
		s.definition,
		s.status,
		s.initiator,
		s.currentNodeID,
		s.data,
		s.nodeState,
		s.tasksNil,
		s.auditNil,
		strconv.FormatUint(s.version, 10),
		s.id,
		strconv.FormatUint(expectedVersion, 10),
	}
}

// loadSnapshot reconstructs one aggregate using exactly three bounded queries in a repeatable-read transaction.
func loadSnapshot(ctx context.Context, tx *sql.Tx, id workflow.InstanceID) (*workflow.Instance, error) {
	var definitionData []byte
	var status workflow.InstanceStatus
	var initiator workflow.ActorID
	var currentNodeID string
	var data []byte
	var nodeState []byte
	var tasksNil bool
	var auditNil bool
	var versionData string
	if err := tx.QueryRowContext(ctx, loadInstanceSQL, id).Scan(
		&definitionData,
		&status,
		&initiator,
		&currentNodeID,
		&data,
		&nodeState,
		&tasksNil,
		&auditNil,
		&versionData,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("%w: %q", workflow.ErrInstanceNotFound, id)
		}
		return nil, fmt.Errorf("select instance row: %w", err)
	}

	// Decode the parent before child queries so corrupt durable metadata stops reconstruction immediately.
	definition, err := decodeDefinition(definitionData)
	if err != nil {
		return nil, fmt.Errorf("decode definition: %w", err)
	}
	version, err := parseVersion(versionData)
	if err != nil {
		return nil, err
	}
	tasks, err := loadTasks(ctx, tx, id, tasksNil)
	if err != nil {
		return nil, err
	}
	audit, err := loadAudit(ctx, tx, id, auditNil)
	if err != nil {
		return nil, err
	}
	return &workflow.Instance{
		ID:            id,
		Definition:    definition,
		Status:        status,
		Initiator:     initiator,
		CurrentNodeID: currentNodeID,
		Data:          cloneBytes(data),
		NodeState:     cloneBytes(nodeState),
		Tasks:         tasks,
		Audit:         audit,
		Version:       version,
	}, nil
}

// loadTasks reads the ordered task set in one query while preserving nil versus empty slice semantics.
func loadTasks(ctx context.Context, tx *sql.Tx, id workflow.InstanceID, isNil bool) ([]workflow.Task, error) {
	rows, err := tx.QueryContext(ctx, loadTasksSQL, id)
	if err != nil {
		return nil, fmt.Errorf("select instance tasks: %w", err)
	}
	defer rows.Close()

	var tasks []workflow.Task
	if !isNil {
		tasks = make([]workflow.Task, 0)
	}
	for rows.Next() {
		var payload []byte
		if err := rows.Scan(&payload); err != nil {
			return nil, fmt.Errorf("scan instance task: %w", err)
		}
		var task workflow.Task
		if err := decodeJSON(payload, &task); err != nil {
			return nil, fmt.Errorf("decode instance task: %w", err)
		}
		tasks = append(tasks, task)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate instance tasks: %w", err)
	}
	return tasks, nil
}

// loadAudit reads the ordered audit set in one query while preserving nil versus empty slice semantics.
func loadAudit(ctx context.Context, tx *sql.Tx, id workflow.InstanceID, isNil bool) ([]workflow.AuditRecord, error) {
	rows, err := tx.QueryContext(ctx, loadAuditSQL, id)
	if err != nil {
		return nil, fmt.Errorf("select instance audit: %w", err)
	}
	defer rows.Close()

	var audit []workflow.AuditRecord
	if !isNil {
		audit = make([]workflow.AuditRecord, 0)
	}
	for rows.Next() {
		var payload []byte
		if err := rows.Scan(&payload); err != nil {
			return nil, fmt.Errorf("scan instance audit: %w", err)
		}
		var record workflow.AuditRecord
		if err := decodeJSON(payload, &record); err != nil {
			return nil, fmt.Errorf("decode instance audit: %w", err)
		}
		audit = append(audit, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate instance audit: %w", err)
	}
	return audit, nil
}

// encodeDefinition converts RawMessage configuration into ordinary bytes before JSON serialization.
func encodeDefinition(definition workflow.Definition) ([]byte, error) {
	nodes := make([]storedNode, len(definition.Nodes))
	if definition.Nodes == nil {
		nodes = nil
	}
	for index, node := range definition.Nodes {
		nodes[index] = storedNode{ID: node.ID, Kind: node.Kind, Config: cloneBytes(node.Config)}
	}
	record := storedDefinition{
		ID:      definition.ID,
		Version: definition.Version,
		Nodes:   nodes,
		Edges:   append([]workflow.Edge(nil), definition.Edges...),
	}
	if definition.Edges != nil && len(definition.Edges) == 0 {
		record.Edges = make([]workflow.Edge, 0)
	}
	data, err := json.Marshal(record)
	if err != nil {
		return nil, fmt.Errorf("marshal definition: %w", err)
	}
	return data, nil
}

// decodeDefinition restores typed Definition values and detached raw configuration bytes.
func decodeDefinition(data []byte) (workflow.Definition, error) {
	var record storedDefinition
	if err := decodeJSON(data, &record); err != nil {
		return workflow.Definition{}, err
	}
	nodes := make([]workflow.NodeDefinition, len(record.Nodes))
	if record.Nodes == nil {
		nodes = nil
	}
	for index, node := range record.Nodes {
		nodes[index] = workflow.NodeDefinition{ID: node.ID, Kind: node.Kind, Config: cloneBytes(node.Config)}
	}
	edges := append([]workflow.Edge(nil), record.Edges...)
	if record.Edges != nil && len(record.Edges) == 0 {
		edges = make([]workflow.Edge, 0)
	}
	return workflow.Definition{
		ID:      record.ID,
		Version: record.Version,
		Nodes:   nodes,
		Edges:   edges,
	}, nil
}

// encodeValues serializes one ordered slice entirely in memory for later batched database writes.
func encodeValues[T any](values []T) ([][]byte, error) {
	if len(values) == 0 {
		return nil, nil
	}
	encoded := make([][]byte, len(values))
	for index := range values {
		data, err := json.Marshal(values[index])
		if err != nil {
			return nil, fmt.Errorf("marshal value %d: %w", index, err)
		}
		encoded[index] = data
	}
	return encoded, nil
}

// decodeJSON decodes one complete trusted durable payload and rejects trailing bytes.
func decodeJSON(data []byte, target any) error {
	if err := json.Unmarshal(data, target); err != nil {
		return fmt.Errorf("unmarshal durable payload: %w", err)
	}
	return nil
}

// parseVersion converts one lossless DECIMAL decimal value back to the public uint64 concurrency token.
func parseVersion(value string) (uint64, error) {
	version, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse durable version %q: %w", value, err)
	}
	return version, nil
}

// cloneBytes returns a caller-independent byte slice while preserving nil versus empty semantics.
func cloneBytes(source []byte) []byte {
	if source == nil {
		return nil
	}
	return append([]byte{}, source...)
}
