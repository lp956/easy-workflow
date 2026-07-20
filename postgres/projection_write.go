// This file derives PostgreSQL query rows from committed aggregate facts inside Store transactions.
// It performs no independent commit and does not expose query behavior or alter the core Store contract.
package postgres

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"

	workflow "github.com/lvpeng/easy-workflow"
)

// upsertInstanceProjectionSQL replaces one instance read row within its owning command transaction.
const upsertInstanceProjectionSQL = `
	INSERT INTO easy_workflow_instance_projection (
		instance_id, definition_id, definition_version, instance_status, initiator,
		current_node_id, started_at, last_audit_at, order_at
	) VALUES ($1, $2, $3::numeric, $4, $5, $6, $7, $8, $9)
	ON CONFLICT (instance_id) DO UPDATE SET
		definition_id = EXCLUDED.definition_id,
		definition_version = EXCLUDED.definition_version,
		instance_status = EXCLUDED.instance_status,
		initiator = EXCLUDED.initiator,
		current_node_id = EXCLUDED.current_node_id,
		started_at = EXCLUDED.started_at,
		last_audit_at = EXCLUDED.last_audit_at,
		order_at = EXCLUDED.order_at`

// deleteParticipationProjectionSQL clears one instance's prior task rows before the bulk replacement.
const deleteParticipationProjectionSQL = `
	DELETE FROM easy_workflow_participation_projection WHERE instance_id = $1`

// replaceQueryProjection rebuilds one instance's read model from its candidate aggregate inside tx.
//
// ctx and tx must belong to the Store Create or Save transaction, and instance must be the exact snapshot being
// committed. The function uses one parent upsert, one child delete, and at most one bulk copy; any error aborts the
// command transaction. It retains no caller data and issues no per-task database operations.
func replaceQueryProjection(ctx context.Context, tx pgx.Tx, instance *workflow.Instance) error {
	// Derive audit timestamps before writing either projection table so malformed inputs cannot leave partial rows.
	startedAt, lastAuditAt, orderAt := projectionAuditTimes(instance.Audit)
	// Preflight every row key in memory so the adapter never persists data its own continuation decoder cannot page.
	if err := validateProjectionContinuationKeys(instance, orderAt); err != nil {
		return err
	}
	// Upsert the instance row first because every task-derived row references it within the same transaction.
	if _, err := tx.Exec(ctx, upsertInstanceProjectionSQL,
		instance.ID,
		instance.Definition.ID,
		strconv.FormatUint(instance.Definition.Version, 10),
		instance.Status,
		instance.Initiator,
		instance.CurrentNodeID,
		startedAt,
		lastAuditAt,
		orderAt,
	); err != nil {
		// The command must roll back when its instance read row cannot represent the candidate aggregate.
		return fmt.Errorf("upsert instance query projection: %w", err)
	}

	// Replace task-derived rows after the parent succeeds; both operations still share the command transaction.
	if _, err := tx.Exec(ctx, deleteParticipationProjectionSQL, instance.ID); err != nil {
		// Keeping stale task rows would contradict the newly committed aggregate, so failure aborts the command.
		return fmt.Errorf("delete participation query projection: %w", err)
	}
	if len(instance.Tasks) == 0 {
		// The successful delete is the complete projection for an aggregate with no task facts.
		return nil
	}
	rows := make([][]any, 0, len(instance.Tasks))
	for index, task := range instance.Tasks {
		// A task without concrete ownership, node identity, or state is not yet a candidate or participant fact.
		if task.Assignee == "" || task.NodeID == "" || task.Status == workflow.TaskStatusUnknown {
			continue
		}
		// In-memory slice ordinals map losslessly to PostgreSQL BIGINT for every representable Go slice length.
		rows = append(rows, []any{instance.ID, int64(index), task.ID, task.Assignee, task.NodeID, task.Status, task.Outcome})
	}
	if len(rows) == 0 {
		// Tasks lacking concrete relation facts intentionally leave the participation projection empty.
		return nil
	}
	// Copy all concrete task relations in one database operation after deriving them entirely in memory.
	columns := []string{"instance_id", "task_ordinal", "task_id", "actor_id", "node_id", "task_status", "outcome"}
	if _, err := tx.CopyFrom(
		ctx,
		pgx.Identifier{"easy_workflow_participation_projection"},
		columns,
		pgx.CopyFromRows(rows),
	); err != nil {
		// Bulk-copy failure rolls back parent, facts, and projection together under the owning transaction.
		return fmt.Errorf("copy participation query projection: %w", err)
	}
	return nil
}

// validateProjectionContinuationKeys ensures every projected row can produce a reusable opaque continuation.
//
// instance must be the candidate aggregate and orderAt the timestamp derived for its instance projection. The function
// performs one bounded in-memory pass over aggregate tasks before any database I/O. Only concrete task relations mirror
// replaceQueryProjection rows; every failure wraps workflow.ErrInvalidStoreInput and no token escapes the preflight.
func validateProjectionContinuationKeys(instance *workflow.Instance, orderAt time.Time) error {
	if _, err := encodeInstanceContinuation(instanceProjectionKeyset{at: orderAt, instanceID: instance.ID}); err != nil {
		return fmt.Errorf("%w: instance projection continuation: %w", workflow.ErrInvalidStoreInput, err)
	}
	for _, task := range instance.Tasks {
		if task.Assignee == "" || task.NodeID == "" || task.Status == workflow.TaskStatusUnknown {
			continue
		}
		if _, err := encodeTaskContinuation(taskProjectionKeyset{
			at:         orderAt,
			instanceID: instance.ID,
			taskID:     task.ID,
		}); err != nil {
			return fmt.Errorf("%w: task %q projection continuation: %w", workflow.ErrInvalidStoreInput, task.ID, err)
		}
	}
	return nil
}

// projectionAuditTimes derives optional public audit times and a total-order timestamp from immutable audit order.
//
// audit may be nil or contain zero timestamps. The first instance.started timestamp becomes StartedAt, the final
// non-zero record timestamp becomes LastAuditAt, and Unix epoch is the deterministic ordering fallback when no
// usable time exists. Returned pointers reference fresh values and are safe for a database driver to retain.
func projectionAuditTimes(audit []workflow.AuditRecord) (*time.Time, *time.Time, time.Time) {
	var startedAt *time.Time
	var lastAuditAt *time.Time
	// Audit slice order is authoritative even when timestamps tie, so later non-zero records replace earlier ones.
	for index := range audit {
		record := audit[index]
		// Only the first explicit start fact supplies StartedAt; unrelated actor audit records cannot substitute.
		if record.Action == "instance.started" && startedAt == nil && !record.At.IsZero() {
			value := record.At.UTC()
			startedAt = &value
		}
		if !record.At.IsZero() {
			value := record.At.UTC()
			lastAuditAt = &value
		}
	}
	if lastAuditAt == nil {
		// Unix epoch provides a PostgreSQL-safe deterministic order key without inventing a public audit timestamp.
		return startedAt, nil, time.Unix(0, 0).UTC()
	}
	return startedAt, lastAuditAt, *lastAuditAt
}
