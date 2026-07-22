// This file implements MySQL command persistence over an explicitly supplied database handle.
// It owns aggregate transactions and CAS, but not pool configuration or migrations.
package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	workflow "github.com/lvpeng/easy-workflow"
)

const (
	insertInstanceSQL = `
		INSERT INTO easy_workflow_instances (
			id, definition, status, initiator, current_node_id, data, node_state,
			tasks_is_nil, audit_is_nil, version
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	loadInstanceSQL = `
		SELECT definition, status, initiator, current_node_id, data, node_state,
		tasks_is_nil, audit_is_nil, CAST(version AS CHAR)
		FROM easy_workflow_instances
		WHERE id = ?`
	updateInstanceSQL = `
		UPDATE easy_workflow_instances
		SET definition = ?, status = ?, initiator = ?, current_node_id = ?,
		data = ?, node_state = ?, tasks_is_nil = ?, audit_is_nil = ?,version = ?
		WHERE id = ? AND version = ?`
	instanceExistsSQL = `SELECT 1 FROM easy_workflow_instances WHERE id = ? FOR UPDATE`
	deleteTasksSQL    = `DELETE FROM easy_workflow_tasks WHERE instance_id = ?`
	loadTasksSQL      = `SELECT payload FROM easy_workflow_tasks WHERE instance_id = ? ORDER BY ordinal`
	loadAuditSQL      = `SELECT payload FROM easy_workflow_audit WHERE instance_id = ? ORDER BY ordinal`
	loadVersionSQL    = `SELECT CAST(version AS CHAR) FROM easy_workflow_instances WHERE id = ? FOR UPDATE`
)

const (
	maxInsertBatchSize         = 500
	transactionRollbackTimeout = 5 * time.Second
)

var errTransactionRollbackTimeout = errors.New("mysql: transaction rollback timed out")

// Store persists complete workflow instance snapshots in MySQL.
//
// The adapter borrows a caller-owned *sql.DB for its full lifetime and is safe for concurrent use when that handle
// is configured and used according to database/sql's connection-pool contract. Construction performs no I/O;
// migrations and connection health checks remain explicit host responsibilities.
type Store struct {
	db *sql.DB
}

var _ workflow.Store = (*Store)(nil)

// New constructs a MySQL workflow Store without connecting or applying migrations.
//
// db remains caller-owned and may be nil; operations report workflow.ErrInvalidStoreInput when no database handle is
// configured. The caller must import and register a MySQL database/sql driver before opening the handle.
func New(db *sql.DB) *Store {
	return &Store{db: db}
}

// Create atomically inserts one detached workflow aggregate snapshot.
//
// instance and its ID must be non-empty, and ctx must remain active through commit. Definition, business data,
// node state, tasks, and audit are committed in one transaction. Duplicate IDs wrap workflow.ErrInstanceExists;
// every other database or encoding error retains its cause and leaves no partial rows.
func (s *Store) Create(ctx context.Context, instance *workflow.Instance) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("mysql: create instance: %w", err)
	}
	if s == nil || s.db == nil || instance == nil || instance.ID == "" {
		return fmt.Errorf("%w: mysql Create requires database and instance identity", workflow.ErrInvalidStoreInput)
	}
	snapshot, err := encodeSnapshot(instance)
	if err != nil {
		return fmt.Errorf("mysql: encode instance %q: %w", instance.ID, err)
	}

	err = withTransaction(ctx, s.db, nil, func(tx *sql.Tx) error {
		if _, execErr := tx.ExecContext(ctx, insertInstanceSQL, snapshot.parentArguments()...); execErr != nil {
			if exists, classifyErr := instanceExists(ctx, tx, snapshot.aggregate.ID); classifyErr == nil && exists {
				return fmt.Errorf("%w: %q", workflow.ErrInstanceExists, snapshot.aggregate.ID)
			}
			return fmt.Errorf("insert instance row: %w", execErr)
		}
		if err := insertTasks(ctx, tx, snapshot.aggregate.ID, snapshot.tasks); err != nil {
			return err
		}
		if err := insertAudit(ctx, tx, snapshot.aggregate.ID, snapshot.audit, 0); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("mysql: create instance %q: %w", snapshot.aggregate.ID, err)
	}
	return nil
}

// Load returns one caller-owned aggregate snapshot from a repeatable-read transaction.
//
// id selects one exact Instance. Parent, task, and audit rows are read from one database snapshot and decoded into
// newly allocated Go values. Missing IDs wrap workflow.ErrInstanceNotFound; cancellation and database errors retain
// their causes. The method performs a bounded three queries regardless of task or audit counts.
func (s *Store) Load(ctx context.Context, id workflow.InstanceID) (*workflow.Instance, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("mysql: load instance: %w", err)
	}
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("%w: mysql Load requires a database", workflow.ErrInvalidStoreInput)
	}

	var instance *workflow.Instance
	err := withTransaction(ctx, s.db, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true}, func(tx *sql.Tx) error {
		loaded, loadErr := loadSnapshot(ctx, tx, id)
		if loadErr != nil {
			return loadErr
		}
		instance = loaded
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("mysql: load instance %q: %w", id, err)
	}
	return instance, nil
}

// Save atomically replaces one aggregate only when its durable version equals expectedVersion.
//
// instance must be non-nil and ctx must remain active through commit. The parent conditional update is the CAS
// authority across processes; task replacement and audit suffix insertion occur in that same transaction. A missing
// ID wraps workflow.ErrInstanceNotFound, a stale expectedVersion wraps workflow.ErrVersionConflict, and every
// failure preserves the previously committed aggregate.
func (s *Store) Save(ctx context.Context, instance *workflow.Instance, expectedVersion uint64) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("mysql: save instance: %w", err)
	}
	if s == nil || s.db == nil || instance == nil {
		return fmt.Errorf("%w: mysql Save requires database and instance", workflow.ErrInvalidStoreInput)
	}
	snapshot, err := encodeSnapshot(instance)
	if err != nil {
		return fmt.Errorf("mysql: encode instance %q: %w", instance.ID, err)
	}

	err = withTransaction(ctx, s.db, nil, func(tx *sql.Tx) error {
		result, execErr := tx.ExecContext(ctx, updateInstanceSQL, snapshot.updateArguments(expectedVersion)...)
		if execErr != nil {
			return fmt.Errorf("conditionally update instance row: %w", execErr)
		}
		rowsAffected, rowsErr := result.RowsAffected()
		if rowsErr != nil {
			return fmt.Errorf("read instance update count: %w", rowsErr)
		}
		if rowsAffected == 0 {
			if err := classifyFailedCAS(ctx, tx, snapshot.aggregate.ID, expectedVersion); err != nil {
				return err
			}
		}
		auditOffset, auditErr := validateAuditAppend(ctx, tx, snapshot.aggregate.ID, snapshot.aggregate.Audit)
		if auditErr != nil {
			return auditErr
		}
		if err := replaceCollections(ctx, tx, snapshot.aggregate.ID, snapshot, auditOffset); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("mysql: save instance %q: %w", snapshot.aggregate.ID, err)
	}
	return nil
}

// validateAuditAppend returns the durable prefix length after rejecting any removal or historical rewrite.
func validateAuditAppend(ctx context.Context, tx *sql.Tx, id workflow.InstanceID, candidate []workflow.AuditRecord) (int, error) {
	durable, err := loadAudit(ctx, tx, id, false)
	if err != nil {
		return 0, err
	}
	if len(candidate) < len(durable) || !slices.Equal(candidate[:len(durable)], durable) {
		return 0, fmt.Errorf("%w: save cannot rewrite audit history for %q", workflow.ErrInvalidStoreInput, id)
	}
	return len(durable), nil
}

// instanceExists reports whether one exact parent row exists inside the current transaction.
func instanceExists(ctx context.Context, tx *sql.Tx, id workflow.InstanceID) (bool, error) {
	var value int64
	if err := tx.QueryRowContext(ctx, instanceExistsSQL, id).Scan(&value); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("scan instance existence: %w", err)
	}
	return value == 1, nil
}

// classifyFailedCAS distinguishes an absent aggregate from a stale version after a zero-row conditional update.
func classifyFailedCAS(ctx context.Context, tx *sql.Tx, id workflow.InstanceID, expectedVersion uint64) error {
	var versionData string
	if err := tx.QueryRowContext(ctx, loadVersionSQL, id).Scan(&versionData); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%w: %q", workflow.ErrInstanceNotFound, id)
		}
		return fmt.Errorf("classify failed instance CAS: %w", err)
	}
	version, err := parseVersion(versionData)
	if err != nil {
		return fmt.Errorf("classify failed instance CAS: %w", err)
	}
	if version != expectedVersion {
		return fmt.Errorf("%w: %q", workflow.ErrVersionConflict, id)
	}
	// MySQL may report zero changed rows when the candidate is byte-for-byte equal to the stored row. The version
	// lookup above proves that the conditional predicate matched, so the transaction may continue with collection
	// validation and replacement just as PostgreSQL's matched-row count would allow.
	return nil
}

// replaceCollections replaces tasks and appends the new audit suffix inside the already-open CAS transaction.
func replaceCollections(ctx context.Context, tx *sql.Tx, id workflow.InstanceID, snapshot encodedSnapshot, auditOffset int) error {
	if _, err := tx.ExecContext(ctx, deleteTasksSQL, id); err != nil {
		return fmt.Errorf("delete instance tasks: %w", err)
	}
	if err := insertTasks(ctx, tx, id, snapshot.tasks); err != nil {
		return err
	}
	if err := insertAudit(ctx, tx, id, snapshot.audit[auditOffset:], auditOffset); err != nil {
		return err
	}
	return nil
}

// insertTasks inserts ordered task payloads in bounded multi-row batches without one database operation per task.
func insertTasks(ctx context.Context, tx *sql.Tx, id workflow.InstanceID, tasks [][]byte) error {
	if len(tasks) == 0 {
		return nil
	}
	rows := make([][]any, len(tasks))
	for index, payload := range tasks {
		var task workflow.Task
		if err := decodeJSON(payload, &task); err != nil {
			return fmt.Errorf("decode task %d metadata: %w", index, err)
		}
		rows[index] = []any{id, index, task.ID, task.Status, payload}
	}
	if err := insertRows(ctx, tx, "easy_workflow_tasks", []string{
		"instance_id", "ordinal", "task_id", "status", "payload",
	}, rows); err != nil {
		return fmt.Errorf("insert instance tasks: %w", err)
	}
	return nil
}

// insertAudit inserts an ordered audit suffix in bounded multi-row batches.
func insertAudit(ctx context.Context, tx *sql.Tx, id workflow.InstanceID, audit [][]byte, ordinalOffset int) error {
	if len(audit) == 0 {
		return nil
	}
	rows := make([][]any, len(audit))
	for index, payload := range audit {
		var record workflow.AuditRecord
		if err := decodeJSON(payload, &record); err != nil {
			return fmt.Errorf("decode audit record %d metadata: %w", index, err)
		}
		rows[index] = []any{id, ordinalOffset + index, record.Action, payload}
	}
	if err := insertRows(ctx, tx, "easy_workflow_audit", []string{"instance_id", "ordinal", "action", "payload"}, rows); err != nil {
		return fmt.Errorf("insert instance audit: %w", err)
	}
	return nil
}

// insertRows executes fixed-table multi-row inserts in bounded batches. Table and column names are package constants.
func insertRows(ctx context.Context, tx *sql.Tx, table string, columns []string, rows [][]any) error {
	for start := 0; start < len(rows); start += maxInsertBatchSize {
		end := min(start+maxInsertBatchSize, len(rows))
		placeholders := make([]string, end-start)
		args := make([]any, 0, (end-start)*len(columns))
		for index := start; index < end; index++ {
			groups := make([]string, len(columns))
			for column := range groups {
				groups[column] = "?"
			}
			placeholders[index-start] = "(" + strings.Join(groups, ", ") + ")"
			args = append(args, rows[index]...)
		}
		// The table and column values are package constants supplied only by insertTasks and insertAudit; workflow data
		// remains in args and is never interpolated into this SQL text.
		//nolint:gosec // table and columns are fixed adapter-owned identifiers, not external input.
		query := "INSERT INTO " + table + " (" + strings.Join(columns, ", ") + ") VALUES " + strings.Join(placeholders, ", ")
		if _, err := tx.ExecContext(ctx, query, args...); err != nil {
			return fmt.Errorf("execute batch insert: %w", err)
		}
	}
	return nil
}

// withTransaction executes one operation and rolls back every failed, canceled, or committed path. BeginTx binds ctx
// to the transaction, so database/sql also aborts a transaction whose context is canceled. The explicit rollback
// cleanup has a bounded wait because database/sql exposes Rollback without a context.
func withTransaction(ctx context.Context, db *sql.DB, options *sql.TxOptions, operation func(*sql.Tx) error) (err error) {
	tx, err := db.BeginTx(ctx, options)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		if rollbackErr := rollbackTransaction(tx); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			err = errors.Join(err, fmt.Errorf("rollback transaction: %w", rollbackErr))
		}
	}()

	if err := operation(tx); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", classifyCommitError(ctx, err))
	}
	return nil
}

// classifyCommitError preserves request cancellation when database/sql reports a transaction that ended concurrently.
//
// err is the direct Commit result. ErrTxDone is ambiguous by itself because the context may have canceled immediately
// before or during the driver commit; an observed context error is therefore the more useful stable public cause.
func classifyCommitError(ctx context.Context, err error) error {
	if errors.Is(err, sql.ErrTxDone) {
		if contextErr := ctx.Err(); contextErr != nil {
			return contextErr
		}
	}
	return err
}

// rollbackTransaction bounds how long a failed request waits for database/sql to finish driver cleanup. A driver that
// remains blocked after the timeout still owns its connection until it returns; the bounded caller path prevents one
// failed request from remaining blocked indefinitely while preserving the original operation error.
func rollbackTransaction(tx *sql.Tx) error {
	rollbackResult := make(chan error, 1)
	go func() {
		rollbackResult <- tx.Rollback()
	}()

	timer := time.NewTimer(transactionRollbackTimeout)
	defer timer.Stop()
	select {
	case err := <-rollbackResult:
		return err
	case <-timer.C:
		return fmt.Errorf("%w after %s", errTransactionRollbackTimeout, transactionRollbackTimeout)
	}
}
