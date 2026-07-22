// This file verifies PostgreSQL durability and transaction behavior through public adapter interfaces.
// Tests require an explicitly supplied database DSN and otherwise skip without starting infrastructure.
package postgres_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	workflow "github.com/lvpeng/easy-workflow"
	"github.com/lvpeng/easy-workflow/postgres"
	"github.com/lvpeng/easy-workflow/storetest"
)

const (
	integrationDSNEnvironment = "EASY_WORKFLOW_POSTGRES_DSN"
	// integrationCleanupTimeout prevents failed test cleanup from retaining a connection indefinitely.
	integrationCleanupTimeout = 5 * time.Second
)

// withdrawalPolicyFunc adapts an integration-test function to workflow.WithdrawalPolicy.
type withdrawalPolicyFunc func(context.Context, workflow.ActorID, *workflow.Instance) error

// failingWithdrawalStore delegates reads and creates but corrupts a withdrawal candidate before durable Save.
type failingWithdrawalStore struct {
	// Store is the real PostgreSQL adapter receiving the deliberately invalid candidate.
	workflow.Store
}

// AuthorizeWithdrawal delegates one host authorization decision and preserves its error identity.
func (f withdrawalPolicyFunc) AuthorizeWithdrawal(
	ctx context.Context,
	actor workflow.ActorID,
	instance *workflow.Instance,
) error {
	return f(ctx, actor, instance)
}

// Save duplicates one task in an isolated candidate so PostgreSQL must roll back after its parent CAS update.
func (s failingWithdrawalStore) Save(
	ctx context.Context,
	instance *workflow.Instance,
	expectedVersion uint64,
) error {
	// Copy the aggregate fields touched by this fault injection so Engine's candidate remains caller-owned.
	candidate := *instance
	candidate.Tasks = slices.Clone(instance.Tasks)
	if len(candidate.Tasks) > 0 {
		candidate.Tasks = append(candidate.Tasks, candidate.Tasks[0])
	}
	if err := s.Store.Save(ctx, &candidate, expectedVersion); err != nil {
		return fmt.Errorf("save injected withdrawal candidate: %w", err)
	}
	return nil
}

// TestStoreContract applies the shared adapter contract to isolated PostgreSQL schemas.
func TestStoreContract(t *testing.T) {
	dsn := requireIntegrationDSN(t)

	storetest.Run(t, func(t *testing.T) workflow.Store {
		t.Helper()

		return newIsolatedStore(t, dsn)
	})
}

// TestStoreRollsBackAggregateReplacement verifies that a child-row failure preserves the prior full snapshot.
func TestStoreRollsBackAggregateReplacement(t *testing.T) {
	dsn := requireIntegrationDSN(t)
	store := newIsolatedStore(t, dsn)
	original := integrationInstance("rollback-instance", 1)
	if err := store.Create(t.Context(), original); err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	// Duplicate task IDs violate the child-table uniqueness constraint after the parent CAS update executes.
	candidate := integrationInstance(original.ID, 2)
	candidate.Data = []byte(`{"state":"candidate"}`)
	candidate.Tasks = append(candidate.Tasks, candidate.Tasks[0])
	if err := store.Save(t.Context(), candidate, 1); err == nil {
		t.Fatal("Save() error = nil, want transaction failure")
	}
	assertIntegrationSnapshot(t, store, original)
}

// TestEngineWithdrawalAtomicity verifies successful and failed withdrawals against PostgreSQL transactions.
func TestEngineWithdrawalAtomicity(t *testing.T) {
	dsn := requireIntegrationDSN(t)
	policy := withdrawalPolicyFunc(func(context.Context, workflow.ActorID, *workflow.Instance) error { return nil })

	t.Run("commit full aggregate", func(t *testing.T) {
		// Persist a running aggregate with one active task before exercising Engine's public lifecycle operation.
		store := newIsolatedStore(t, dsn)
		original := integrationInstance("withdrawal-commit", 1)
		if err := store.Create(t.Context(), original); err != nil {
			t.Fatalf("Create() error = %v", err)
		}

		withdrawn, err := workflow.NewEngine(store, nil).Withdraw(t.Context(), workflow.WithdrawRequest{
			InstanceID: original.ID,
			ActorID:    "operator-a",
		}, policy)
		if err != nil {
			t.Fatalf("Withdraw() error = %v", err)
		}
		if withdrawn.Status != workflow.InstanceStatusWithdrawn || withdrawn.Version != 2 {
			t.Errorf("withdrawn status/version = %q/%d, want withdrawn/2", withdrawn.Status, withdrawn.Version)
		}
		if withdrawn.Tasks[0].Status != workflow.TaskStatusClosed {
			t.Errorf("withdrawn task status = %q, want %q", withdrawn.Tasks[0].Status, workflow.TaskStatusClosed)
		}
		lastAudit := withdrawn.Audit[len(withdrawn.Audit)-1]
		if lastAudit.Action != "instance.withdrawn" || lastAudit.ActorID != "operator-a" {
			t.Errorf("withdrawal audit = %#v, want attributed instance.withdrawn", lastAudit)
		}
		assertIntegrationSnapshot(t, store, withdrawn)
	})

	t.Run("roll back full aggregate", func(t *testing.T) {
		// The injected duplicate reaches child replacement only after PostgreSQL conditionally updates the parent row.
		store := newIsolatedStore(t, dsn)
		original := integrationInstance("withdrawal-rollback", 1)
		if err := store.Create(t.Context(), original); err != nil {
			t.Fatalf("Create() error = %v", err)
		}
		engine := workflow.NewEngine(failingWithdrawalStore{Store: store}, nil)

		_, err := engine.Withdraw(t.Context(), workflow.WithdrawRequest{
			InstanceID: original.ID,
			ActorID:    "operator-a",
		}, policy)
		if err == nil {
			t.Fatal("Withdraw() error = nil, want child replacement failure")
		}
		assertIntegrationSnapshot(t, store, original)
	})
}

// TestStoreLoadsAfterPoolRestart verifies that committed snapshots survive adapter and connection-pool lifetimes.
func TestStoreLoadsAfterPoolRestart(t *testing.T) {
	dsn := requireIntegrationDSN(t)
	schema := createIsolatedSchema(t, dsn)
	firstPool := openSchemaPool(t, dsn, schema)
	applyInitialMigration(t, firstPool)
	instance := integrationInstance("restart-instance", 1)
	if err := postgres.New(firstPool).Create(t.Context(), instance); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	firstPool.Close()

	// A new pool and adapter prove that no process-local cache participates in Load.
	secondPool := openSchemaPool(t, dsn, schema)
	assertIntegrationSnapshot(t, postgres.New(secondPool), instance)
}

// TestProjectionMigrationBackfillsLegacyInstances verifies 0003 upgrades command rows created before projections existed.
func TestProjectionMigrationBackfillsLegacyInstances(t *testing.T) {
	dsn := requireIntegrationDSN(t)
	schema := createIsolatedSchema(t, dsn)
	pool := openSchemaPool(t, dsn, schema)
	applyMigrationFile(t, pool, "migrations/0001_init.up.sql")

	definition := []byte(`{"id":"legacy-definition","version":7,"nodes":[],"edges":[]}`)
	task := []byte(`{"id":"legacy-task","nodeId":"review","assignee":"reviewer","status":"active"}`)
	started := []byte(`{"action":"instance.started","nodeId":"start","at":"2026-01-02T03:04:05Z"}`)
	lastAudit := []byte(`{"action":"node.entered","nodeId":"review","at":"2026-01-02T04:04:05Z"}`)
	if _, err := pool.Exec(t.Context(), `
		INSERT INTO easy_workflow_instances (
			id, definition, status, initiator, current_node_id, data, node_state,
			tasks_is_nil, audit_is_nil, version
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		"legacy-instance", definition, workflow.InstanceStatusRunning, "initiator-a", "review", nil, nil, false, false, "1"); err != nil {
		t.Fatalf("insert legacy instance error = %v", err)
	}
	if _, err := pool.Exec(t.Context(),
		"INSERT INTO easy_workflow_tasks (instance_id, ordinal, task_id, status, payload) VALUES ($1, $2, $3, $4, $5)",
		"legacy-instance", 0, "legacy-task", workflow.TaskStatusActive, task); err != nil {
		t.Fatalf("insert legacy task error = %v", err)
	}
	for ordinal, payload := range [][]byte{started, lastAudit} {
		if _, err := pool.Exec(t.Context(),
			"INSERT INTO easy_workflow_audit (instance_id, ordinal, action, payload) VALUES ($1, $2, $3, $4)",
			"legacy-instance", ordinal, []string{"instance.started", "node.entered"}[ordinal], payload); err != nil {
			t.Fatalf("insert legacy audit %d error = %v", ordinal, err)
		}
	}

	applyMigrationFile(t, pool, "migrations/0002_query_projection.up.sql")
	applyMigrationFile(t, pool, "migrations/0003_query_projection_backfill.up.sql")

	var definitionID, definitionVersion, status string
	var startedAt, lastAuditAt, orderAt time.Time
	if err := pool.QueryRow(t.Context(), `
		SELECT definition_id, definition_version::text, instance_status, started_at, last_audit_at, order_at
		FROM easy_workflow_instance_projection WHERE instance_id = $1`, "legacy-instance").Scan(
		&definitionID, &definitionVersion, &status, &startedAt, &lastAuditAt, &orderAt,
	); err != nil {
		t.Fatalf("query backfilled instance projection error = %v", err)
	}
	if definitionID != "legacy-definition" || definitionVersion != "7" || status != string(workflow.InstanceStatusRunning) ||
		!startedAt.Equal(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)) ||
		!lastAuditAt.Equal(time.Date(2026, 1, 2, 4, 4, 5, 0, time.UTC)) || !orderAt.Equal(lastAuditAt) {
		t.Fatalf("backfilled instance projection = %q/%q/%q %v/%v/%v, want legacy values", definitionID, definitionVersion, status, startedAt, lastAuditAt, orderAt)
	}
	var taskID, actorID, nodeID, taskStatus, outcome string
	if err := pool.QueryRow(t.Context(), `
		SELECT task_id, actor_id, node_id, task_status, outcome
		FROM easy_workflow_participation_projection WHERE instance_id = $1`, "legacy-instance").Scan(
		&taskID, &actorID, &nodeID, &taskStatus, &outcome,
	); err != nil {
		t.Fatalf("query backfilled participation projection error = %v", err)
	}
	if taskID != "legacy-task" || actorID != "reviewer" || nodeID != "review" || taskStatus != string(workflow.TaskStatusActive) || outcome != "" {
		t.Fatalf("backfilled participation projection = %q/%q/%q/%q/%q, want legacy task values", taskID, actorID, nodeID, taskStatus, outcome)
	}
}

// TestStorePreservesLosslessBoundaryValues verifies uint64 and nil-versus-empty snapshot fidelity.
func TestStorePreservesLosslessBoundaryValues(t *testing.T) {
	dsn := requireIntegrationDSN(t)
	store := newIsolatedStore(t, dsn)
	instance := &workflow.Instance{
		ID: "lossless-boundaries",
		Definition: workflow.Definition{
			ID:      "boundary-definition",
			Version: ^uint64(0),
			Nodes: []workflow.NodeDefinition{
				{ID: "node", Kind: "boundary", Config: []byte{}},
			},
			Edges: []workflow.Edge{},
		},
		Data:      []byte{},
		NodeState: nil,
		Tasks:     []workflow.Task{},
		Audit:     nil,
		Version:   ^uint64(0),
	}
	if err := store.Create(t.Context(), instance); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	assertIntegrationSnapshot(t, store, instance)
}

// TestSaveHonorsContextWhileWaitingForRowLock verifies cancellation of an in-flight conditional update.
func TestSaveHonorsContextWhileWaitingForRowLock(t *testing.T) {
	dsn := requireIntegrationDSN(t)
	schema := createIsolatedSchema(t, dsn)
	pool := openSchemaPool(t, dsn, schema)
	applyInitialMigration(t, pool)
	store := postgres.New(pool)
	instance := integrationInstance("locked-instance", 1)
	if err := store.Create(t.Context(), instance); err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	locker, err := pool.Begin(t.Context())
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	t.Cleanup(func() { rollbackIntegrationTransaction(t, locker) })
	if _, err := locker.Exec(t.Context(), "SELECT id FROM easy_workflow_instances WHERE id = $1 FOR UPDATE", instance.ID); err != nil {
		t.Fatalf("locking SELECT error = %v", err)
	}

	waitContext, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	replacement := integrationInstance(instance.ID, 2)
	if err := store.Save(waitContext, replacement, 1); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("blocked Save() error = %v, want context.DeadlineExceeded", err)
	}
	if err := locker.Rollback(t.Context()); err != nil {
		t.Fatalf("Rollback() error = %v", err)
	}
	assertIntegrationSnapshot(t, store, instance)
}

// requireIntegrationDSN returns the explicitly configured test DSN or skips PostgreSQL-dependent behavior.
func requireIntegrationDSN(t *testing.T) string {
	t.Helper()

	dsn := os.Getenv(integrationDSNEnvironment)
	if dsn == "" {
		t.Skipf("set %s to run PostgreSQL integration tests", integrationDSNEnvironment)
	}
	return dsn
}

// createIsolatedSchema allocates one random PostgreSQL schema and registers its eventual removal.
func createIsolatedSchema(t *testing.T, dsn string) string {
	t.Helper()

	admin, err := pgxpool.New(t.Context(), dsn)
	if err != nil {
		t.Fatalf("pgxpool.New() error = %v", err)
	}
	t.Cleanup(admin.Close)
	if err := admin.Ping(t.Context()); err != nil {
		t.Fatalf("Ping() error = %v", err)
	}
	random := make([]byte, 8)
	if _, err := rand.Read(random); err != nil {
		t.Fatalf("rand.Read() error = %v", err)
	}
	schema := "easy_workflow_test_" + hex.EncodeToString(random)
	identifier := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(t.Context(), "CREATE SCHEMA "+identifier); err != nil {
		t.Fatalf("CREATE SCHEMA error = %v", err)
	}
	t.Cleanup(func() {
		cleanupContext, cancelCleanup := context.WithTimeout(context.WithoutCancel(t.Context()), integrationCleanupTimeout)
		defer cancelCleanup()
		if _, err := admin.Exec(cleanupContext, "DROP SCHEMA "+identifier+" CASCADE"); err != nil {
			t.Errorf("DROP SCHEMA error = %v", err)
		}
	})
	return schema
}

// openSchemaPool creates a caller-owned pool whose unqualified adapter queries are confined to schema.
func openSchemaPool(t *testing.T, dsn, schema string) *pgxpool.Pool {
	t.Helper()

	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("ParseConfig() error = %v", err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(t.Context(), config)
	if err != nil {
		t.Fatalf("NewWithConfig() error = %v", err)
	}
	t.Cleanup(pool.Close)
	if err := pool.Ping(t.Context()); err != nil {
		t.Fatalf("Ping() error = %v", err)
	}
	return pool
}

// newIsolatedStore creates and migrates one schema-backed adapter for a contract subtest.
func newIsolatedStore(t *testing.T, dsn string) workflow.Store {
	t.Helper()

	schema := createIsolatedSchema(t, dsn)
	pool := openSchemaPool(t, dsn, schema)
	applyInitialMigration(t, pool)
	return postgres.New(pool)
}

// applyInitialMigration executes all embedded up migrations as one setup operation for an isolated test schema.
func applyInitialMigration(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()

	// Embedded paths sort lexically by version, matching the host-visible migration order used in production.
	paths, err := fs.Glob(postgres.Migrations(), "migrations/*.up.sql")
	if err != nil {
		t.Fatalf("Glob() error = %v", err)
	}
	var migrationSQL strings.Builder
	for _, path := range paths {
		data, readErr := fs.ReadFile(postgres.Migrations(), path)
		if readErr != nil {
			t.Fatalf("ReadFile(%q) error = %v", path, readErr)
		}
		migrationSQL.Write(data)
		migrationSQL.WriteString("\n")
	}
	// One setup call avoids per-migration database I/O while preserving SQL statement order in the combined text.
	if _, err := pool.Exec(t.Context(), migrationSQL.String()); err != nil {
		t.Fatalf("migration error = %v", err)
	}
}

// applyMigrationFile executes one embedded migration file for upgrade-path tests that must control version order.
func applyMigrationFile(t *testing.T, pool *pgxpool.Pool, path string) {
	t.Helper()

	data, err := fs.ReadFile(postgres.Migrations(), path)
	if err != nil {
		t.Fatalf("ReadFile(%q) error = %v", path, err)
	}
	if _, err := pool.Exec(t.Context(), string(data)); err != nil {
		t.Fatalf("migration %q error = %v", path, err)
	}
}

// integrationInstance returns a full independently allocated aggregate for durable round-trip assertions.
func integrationInstance(id workflow.InstanceID, version uint64) *workflow.Instance {
	return &workflow.Instance{
		ID: id,
		Definition: workflow.Definition{
			ID:      "durable-definition",
			Version: ^uint64(0),
			Nodes: []workflow.NodeDefinition{
				{ID: "start", Kind: workflow.KindStart},
				{ID: "approval", Kind: "approval", Config: []byte(`{"minimum":2}`)},
				{ID: "end", Kind: workflow.KindEnd},
			},
			Edges: []workflow.Edge{
				{From: "start", To: "approval"},
				{From: "approval", To: "end", Outcome: "approved"},
			},
		},
		Status:        workflow.InstanceStatusRunning,
		Initiator:     "initiator'; DROP TABLE easy_workflow_instances; --",
		CurrentNodeID: "approval",
		Data:          []byte("{\n  \"amount\": 42\n}"),
		NodeState:     []byte(`{"approved":1}`),
		Tasks: []workflow.Task{
			{ID: "task-1", NodeID: "approval", Assignee: "reviewer-1", Status: workflow.TaskStatusActive},
		},
		Audit: []workflow.AuditRecord{
			{Action: "started", NodeID: "start", ActorID: "initiator-1", At: time.Date(2026, 1, 2, 3, 4, 5, 678901234, time.UTC)},
			{
				Action:       "instance.returned",
				NodeID:       "review",
				TargetNodeID: "approval",
				ActorID:      "operator-1",
				Reason:       "durable return audit",
				NodeState:    `{"round":1}`,
				At:           time.Date(2026, 1, 2, 4, 5, 6, 789012345, time.UTC),
			},
		},
		Version: version,
	}
}

// assertIntegrationSnapshot loads an instance through Store and requires a field-for-field durable round trip.
func assertIntegrationSnapshot(t *testing.T, store workflow.Store, expected *workflow.Instance) {
	t.Helper()

	actual, err := store.Load(t.Context(), expected.ID)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("Load() = %#v, want %#v", actual, expected)
	}
}

// rollbackIntegrationTransaction releases a test lock unless the transaction has already closed.
func rollbackIntegrationTransaction(t *testing.T, tx pgx.Tx) {
	t.Helper()

	rollbackContext, cancelRollback := context.WithTimeout(context.WithoutCancel(t.Context()), integrationCleanupTimeout)
	defer cancelRollback()
	if err := tx.Rollback(rollbackContext); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
		t.Errorf("cleanup Rollback() error = %v", err)
	}
}
