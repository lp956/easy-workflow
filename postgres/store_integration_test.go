// This file verifies PostgreSQL durability and transaction behavior through public adapter interfaces.
// Tests require an explicitly supplied database DSN and otherwise skip without starting infrastructure.
package postgres_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io/fs"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	workflow "github.com/lvpeng/easy-workflow"
	"github.com/lvpeng/easy-workflow/postgres"
	"github.com/lvpeng/easy-workflow/storetest"
)

const integrationDSNEnvironment = "EASY_WORKFLOW_POSTGRES_DSN"

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
		if _, err := admin.Exec(context.WithoutCancel(t.Context()), "DROP SCHEMA "+identifier+" CASCADE"); err != nil {
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

// applyInitialMigration explicitly executes the first versioned schema artifact for an isolated test schema.
func applyInitialMigration(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()

	data, err := fs.ReadFile(postgres.Migrations(), "migrations/0001_init.up.sql")
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if _, err := pool.Exec(t.Context(), string(data)); err != nil {
		t.Fatalf("initial migration error = %v", err)
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

	if err := tx.Rollback(context.WithoutCancel(t.Context())); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
		t.Errorf("cleanup Rollback() error = %v", err)
	}
}
