// This file implements the PostgreSQL workflow Store adapter over an explicitly supplied connection pool.
// It owns persistence transactions but not pool configuration, connectivity, migrations, or query projections.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	workflow "github.com/lvpeng/easy-workflow"
)

const (
	insertInstanceSQL = `
		INSERT INTO easy_workflow_instances (
			id, definition, status, initiator, current_node_id, data, node_state,
			tasks_is_nil, audit_is_nil, version
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::numeric)`
	loadInstanceSQL = `
		SELECT definition, status, initiator, current_node_id, data, node_state,
		       tasks_is_nil, audit_is_nil, version::text
		FROM easy_workflow_instances
		WHERE id = $1`
	updateInstanceSQL = `
		UPDATE easy_workflow_instances
		SET definition = $2, status = $3, initiator = $4, current_node_id = $5,
		    data = $6, node_state = $7, tasks_is_nil = $8, audit_is_nil = $9,
		    version = $10::numeric
		WHERE id = $1 AND version = $11::numeric`
	instanceExistsSQL = `SELECT EXISTS (SELECT 1 FROM easy_workflow_instances WHERE id = $1)`
	deleteTasksSQL    = `DELETE FROM easy_workflow_tasks WHERE instance_id = $1`
	loadTasksSQL      = `SELECT payload FROM easy_workflow_tasks WHERE instance_id = $1 ORDER BY ordinal`
	loadAuditSQL      = `SELECT payload FROM easy_workflow_audit WHERE instance_id = $1 ORDER BY ordinal`
)

// Store persists complete workflow instance snapshots in PostgreSQL.
//
// The adapter borrows a caller-owned pool for its full lifetime and is safe for concurrent use when that pool is.
// Construction performs no I/O; migrations and connection health checks remain explicit host responsibilities.
type Store struct {
	// pool provides transaction and query execution without transferring lifecycle ownership to Store.
	pool *pgxpool.Pool
}

var _ workflow.Store = (*Store)(nil)

// New constructs a PostgreSQL workflow Store without connecting or applying migrations.
//
// pool remains caller-owned and may be nil; operations report workflow.ErrInvalidStoreInput when no pool is
// configured. The returned adapter is safe to share across goroutines under pgxpool's concurrency contract.
func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// Create atomically inserts one detached workflow aggregate snapshot.
//
// instance and its ID must be non-empty, and ctx must remain active through commit. Definition, business data,
// node state, tasks, and audit are committed in one transaction. Duplicate IDs wrap workflow.ErrInstanceExists;
// every other database or encoding error retains its cause and leaves no partial rows.
func (s *Store) Create(ctx context.Context, instance *workflow.Instance) error {
	// Cancellation takes precedence because an abandoned caller cannot consume a successful commit.
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("postgres: create instance: %w", err)
	}
	// Pool, aggregate, and identity are the minimum inputs required to establish durable ownership.
	if s == nil || s.pool == nil || instance == nil || instance.ID == "" {
		return fmt.Errorf("%w: postgres Create requires pool and instance identity", workflow.ErrInvalidStoreInput)
	}
	snapshot, err := encodeSnapshot(instance)
	if err != nil {
		return fmt.Errorf("postgres: encode instance %q: %w", instance.ID, err)
	}

	// Parent and child rows share one transaction so no externally visible partial aggregate can remain.
	err = withTransaction(ctx, s.pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
		if _, execErr := tx.Exec(ctx, insertInstanceSQL, snapshot.parentArguments()...); execErr != nil {
			if isDuplicateInstanceError(execErr) {
				return fmt.Errorf("%w: %q", workflow.ErrInstanceExists, instance.ID)
			}
			return fmt.Errorf("insert instance row: %w", execErr)
		}
		if copyErr := copyTasks(ctx, tx, instance.ID, snapshot.tasks); copyErr != nil {
			return copyErr
		}
		if copyErr := copyAudit(ctx, tx, instance.ID, snapshot.audit, 0); copyErr != nil {
			return copyErr
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("postgres: create instance %q: %w", instance.ID, err)
	}
	return nil
}

// Load returns one caller-owned aggregate snapshot from a repeatable-read transaction.
//
// id selects one exact Instance. Parent, task, and audit rows are read from a single database snapshot and decoded
// into newly allocated Go values. Missing IDs wrap workflow.ErrInstanceNotFound; cancellation and database errors
// retain their causes. The method performs a bounded three queries regardless of task or audit counts.
func (s *Store) Load(ctx context.Context, id workflow.InstanceID) (*workflow.Instance, error) {
	// Avoid pool acquisition when the requested snapshot can no longer be consumed.
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("postgres: load instance: %w", err)
	}
	// A nil adapter or pool has no implicit connection fallback.
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("%w: postgres Load requires a pool", workflow.ErrInvalidStoreInput)
	}

	var instance *workflow.Instance
	options := pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}
	err := withTransaction(ctx, s.pool, options, func(tx pgx.Tx) error {
		loaded, loadErr := loadSnapshot(ctx, tx, id)
		if loadErr != nil {
			return loadErr
		}
		instance = loaded
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("postgres: load instance %q: %w", id, err)
	}
	return instance, nil
}

// Save atomically replaces one aggregate only when its durable version equals expectedVersion.
//
// instance must be non-nil and ctx must remain active through commit. The parent conditional update is the CAS
// authority across processes; task replacement and audit suffix insertion then occur in that same transaction. A missing ID wraps
// workflow.ErrInstanceNotFound, a stale expectedVersion wraps workflow.ErrVersionConflict, and every failure
// preserves the previously committed full aggregate.
func (s *Store) Save(ctx context.Context, instance *workflow.Instance, expectedVersion uint64) error {
	// Cancellation before encoding or pool acquisition must leave the durable version unchanged.
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("postgres: save instance: %w", err)
	}
	// CAS requires a configured pool and a concrete candidate snapshot.
	if s == nil || s.pool == nil || instance == nil {
		return fmt.Errorf("%w: postgres Save requires pool and instance", workflow.ErrInvalidStoreInput)
	}
	snapshot, err := encodeSnapshot(instance)
	if err != nil {
		return fmt.Errorf("postgres: encode instance %q: %w", instance.ID, err)
	}

	// The conditional parent update runs before child replacement and rolls back with every later failure.
	err = withTransaction(ctx, s.pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
		tag, execErr := tx.Exec(ctx, updateInstanceSQL, snapshot.updateArguments(expectedVersion)...)
		if execErr != nil {
			return fmt.Errorf("conditionally update instance row: %w", execErr)
		}
		if tag.RowsAffected() == 0 {
			return classifyFailedCAS(ctx, tx, instance.ID)
		}
		auditOffset, auditErr := validateAuditAppend(ctx, tx, instance.ID, instance.Audit)
		if auditErr != nil {
			return auditErr
		}
		if replaceErr := replaceCollections(ctx, tx, instance.ID, snapshot, auditOffset); replaceErr != nil {
			return replaceErr
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("postgres: save instance %q: %w", instance.ID, err)
	}
	return nil
}

// validateAuditAppend returns the durable prefix length after rejecting any removal or historical rewrite.
func validateAuditAppend(ctx context.Context, tx pgx.Tx, id workflow.InstanceID, candidate []workflow.AuditRecord) (int, error) {
	durable, err := loadAudit(ctx, tx, id, false)
	if err != nil {
		return 0, err
	}
	if len(candidate) < len(durable) || !slices.Equal(candidate[:len(durable)], durable) {
		return 0, fmt.Errorf("%w: save cannot rewrite audit history for %q", workflow.ErrInvalidStoreInput, id)
	}
	return len(durable), nil
}

// classifyFailedCAS distinguishes an absent aggregate from a stale version after a zero-row conditional update.
func classifyFailedCAS(ctx context.Context, tx pgx.Tx, id workflow.InstanceID) error {
	var exists bool
	if err := tx.QueryRow(ctx, instanceExistsSQL, id).Scan(&exists); err != nil {
		return fmt.Errorf("classify failed instance CAS: %w", err)
	}
	if !exists {
		return fmt.Errorf("%w: %q", workflow.ErrInstanceNotFound, id)
	}
	return fmt.Errorf("%w: %q", workflow.ErrVersionConflict, id)
}

// replaceCollections replaces tasks and appends the new audit suffix inside the already-open CAS transaction.
func replaceCollections(ctx context.Context, tx pgx.Tx, id workflow.InstanceID, snapshot encodedSnapshot, auditOffset int) error {
	// Tasks are current mutable state, so replace their ordered rows before appending immutable audit history.
	if _, err := tx.Exec(ctx, deleteTasksSQL, id); err != nil {
		return fmt.Errorf("delete instance tasks: %w", err)
	}
	if err := copyTasks(ctx, tx, id, snapshot.tasks); err != nil {
		return err
	}
	if err := copyAudit(ctx, tx, id, snapshot.audit[auditOffset:], auditOffset); err != nil {
		return err
	}
	return nil
}

// copyTasks bulk-inserts ordered task payloads without issuing one database operation per task.
func copyTasks(ctx context.Context, tx pgx.Tx, id workflow.InstanceID, tasks [][]byte) error {
	if len(tasks) == 0 {
		return nil
	}
	rows := make([][]any, len(tasks))
	for index, payload := range tasks {
		var task workflow.Task
		if err := decodeJSON(payload, &task); err != nil {
			return fmt.Errorf("decode task %d metadata: %w", index, err)
		}
		rows[index] = []any{id, int64(index), task.ID, task.Status, payload}
	}
	columns := []string{"instance_id", "ordinal", "task_id", "status", "payload"}
	if _, err := tx.CopyFrom(ctx, pgx.Identifier{"easy_workflow_tasks"}, columns, pgx.CopyFromRows(rows)); err != nil {
		return fmt.Errorf("copy instance tasks: %w", err)
	}
	return nil
}

// copyAudit bulk-inserts an ordered audit suffix beginning at the supplied durable ordinal.
func copyAudit(ctx context.Context, tx pgx.Tx, id workflow.InstanceID, audit [][]byte, ordinalOffset int) error {
	if len(audit) == 0 {
		return nil
	}
	rows := make([][]any, len(audit))
	for index, payload := range audit {
		var record workflow.AuditRecord
		if err := decodeJSON(payload, &record); err != nil {
			return fmt.Errorf("decode audit record %d metadata: %w", index, err)
		}
		rows[index] = []any{id, int64(ordinalOffset + index), record.Action, payload}
	}
	columns := []string{"instance_id", "ordinal", "action", "payload"}
	if _, err := tx.CopyFrom(ctx, pgx.Identifier{"easy_workflow_audit"}, columns, pgx.CopyFromRows(rows)); err != nil {
		return fmt.Errorf("copy instance audit: %w", err)
	}
	return nil
}

// withTransaction executes one operation and guarantees rollback after every failed or canceled path.
func withTransaction(ctx context.Context, pool *pgxpool.Pool, options pgx.TxOptions, operation func(pgx.Tx) error) (err error) {
	tx, err := pool.BeginTx(ctx, options)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		rollbackErr := tx.Rollback(context.WithoutCancel(ctx))
		if rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
			err = errors.Join(err, fmt.Errorf("rollback transaction: %w", rollbackErr))
		}
	}()

	if err := operation(tx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	return nil
}

// isDuplicateInstanceError reports whether PostgreSQL rejected the parent instance primary key specifically.
func isDuplicateInstanceError(err error) bool {
	var databaseError *pgconn.PgError
	return errors.As(err, &databaseError) && databaseError.Code == "23505" &&
		databaseError.ConstraintName == "easy_workflow_instances_pkey"
}

// parseVersion converts one lossless NUMERIC decimal value back to the public uint64 concurrency token.
func parseVersion(value string) (uint64, error) {
	version, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse durable version %q: %w", value, err)
	}
	return version, nil
}
