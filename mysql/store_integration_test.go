// This file verifies MySQL durability through the public adapter and shared Store contract.
// Tests require an explicitly supplied database DSN and otherwise skip without starting infrastructure.
package mysql_test

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"io/fs"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	mysqldriver "github.com/go-sql-driver/mysql"

	workflow "github.com/lvpeng/easy-workflow"
	"github.com/lvpeng/easy-workflow/mysql"
	"github.com/lvpeng/easy-workflow/storetest"
)

const integrationDSNEnvironment = "EASY_WORKFLOW_MYSQL_DSN"

// TestStoreContract applies the shared adapter contract to isolated MySQL databases.
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

	candidate := integrationInstance(original.ID, 2)
	candidate.Data = []byte(`{"state":"candidate"}`)
	candidate.Tasks = append(candidate.Tasks, candidate.Tasks[0])
	if err := store.Save(t.Context(), candidate, 1); err == nil {
		t.Fatal("Save() error = nil, want transaction failure")
	}
	assertIntegrationSnapshot(t, store, original)
}

// TestStoreAllowsNoopSave verifies MySQL's zero-changed-row result is classified as a matched CAS.
func TestStoreAllowsNoopSave(t *testing.T) {
	dsn := requireIntegrationDSN(t)
	store := newIsolatedStore(t, dsn)
	instance := integrationInstance("noop-save", 1)
	if err := store.Create(t.Context(), instance); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if err := store.Save(t.Context(), instance, 1); err != nil {
		t.Fatalf("no-op Save() error = %v", err)
	}
	assertIntegrationSnapshot(t, store, instance)
}

// TestStoreRejectsEmptyChildValues verifies application-side validation does not depend on MySQL CHECK enforcement.
func TestStoreRejectsEmptyChildValues(t *testing.T) {
	dsn := requireIntegrationDSN(t)
	tests := []struct {
		name   string
		mutate func(*workflow.Instance)
	}{
		{
			name: "task id",
			mutate: func(instance *workflow.Instance) {
				instance.Tasks[0].ID = ""
			},
		},
		{
			name: "task status",
			mutate: func(instance *workflow.Instance) {
				instance.Tasks[0].Status = ""
			},
		},
		{
			name: "audit action",
			mutate: func(instance *workflow.Instance) {
				instance.Audit[0].Action = ""
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newIsolatedStore(t, dsn)
			instance := integrationInstance(workflow.InstanceID("invalid-child-"+test.name), 1)
			test.mutate(instance)
			if err := store.Create(t.Context(), instance); !errors.Is(err, workflow.ErrInvalidStoreInput) {
				t.Fatalf("Create() error = %v, want ErrInvalidStoreInput", err)
			}
		})
	}
}

// TestStoreDistinguishesTrailingSpaceIdentifiers verifies indexed identities use NO PAD comparison semantics.
func TestStoreDistinguishesTrailingSpaceIdentifiers(t *testing.T) {
	dsn := requireIntegrationDSN(t)
	store := newIsolatedStore(t, dsn)
	withoutSpace := integrationInstance("identity", 1)
	withSpace := integrationInstance("identity ", 1)
	if err := store.Create(t.Context(), withoutSpace); err != nil {
		t.Fatalf("Create(withoutSpace) error = %v", err)
	}
	if err := store.Create(t.Context(), withSpace); err != nil {
		t.Fatalf("Create(withSpace) error = %v", err)
	}
	assertIntegrationSnapshot(t, store, withoutSpace)
	assertIntegrationSnapshot(t, store, withSpace)
}

// TestStoreSaveHonorsContextWhileWaitingForCASLock verifies a canceled row-lock wait returns promptly.
func TestStoreSaveHonorsContextWhileWaitingForCASLock(t *testing.T) {
	dsn := requireIntegrationDSN(t)
	store, db := newIsolatedStoreAndDB(t, dsn)
	instance := integrationInstance("locked-save", 1)
	if err := store.Create(t.Context(), instance); err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	locker, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatalf("BeginTx() error = %v", err)
	}
	defer func() {
		if rollbackErr := locker.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			t.Errorf("lock transaction Rollback() error = %v", rollbackErr)
		}
	}()
	if _, err := locker.ExecContext(t.Context(), "SELECT id FROM easy_workflow_instances WHERE id = ? FOR UPDATE", instance.ID); err != nil {
		t.Fatalf("lock instance error = %v", err)
	}

	candidate := integrationInstance(instance.ID, 2)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	saveDone := make(chan error, 1)
	go func() {
		saveDone <- store.Save(ctx, candidate, 1)
	}()

	select {
	case err := <-saveDone:
		if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
			t.Fatalf("Save() error = %v, want context cancellation", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Save() did not return after context cancellation")
	}
}

// TestMigrationRoundTrip verifies the embedded down migration removes every initial table.
func TestMigrationRoundTrip(t *testing.T) {
	dsn := requireIntegrationDSN(t)
	_, db := newIsolatedStoreAndDB(t, dsn)
	downSQL, err := fs.ReadFile(mysql.Migrations(), "migrations/0001_init.down.sql")
	if err != nil {
		t.Fatalf("ReadFile(down migration) error = %v", err)
	}
	if _, err := db.ExecContext(t.Context(), string(downSQL)); err != nil {
		t.Fatalf("down migration error = %v", err)
	}
	for _, table := range []string{
		"easy_workflow_instances",
		"easy_workflow_tasks",
		"easy_workflow_audit",
	} {
		var found string
		err := db.QueryRowContext(t.Context(),
			"SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?",
			table,
		).Scan(&found)
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("table %q lookup error = %v, want sql.ErrNoRows", table, err)
		}
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
			Nodes:   []workflow.NodeDefinition{{ID: "node", Kind: "boundary", Config: []byte{}}},
			Edges:   []workflow.Edge{},
		},
		Status:        workflow.InstanceStatusRunning,
		Initiator:     "initiator",
		CurrentNodeID: "node",
		Data:          []byte{},
		NodeState:     nil,
		Tasks:         []workflow.Task{},
		Audit:         nil,
		Version:       ^uint64(0),
	}
	if err := store.Create(t.Context(), instance); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	assertIntegrationSnapshot(t, store, instance)
}

// requireIntegrationDSN returns the explicitly configured test DSN or skips MySQL-dependent behavior.
func requireIntegrationDSN(t *testing.T) string {
	t.Helper()

	dsn := os.Getenv(integrationDSNEnvironment)
	if dsn == "" {
		t.Skipf("set %s to run MySQL integration tests", integrationDSNEnvironment)
	}
	return dsn
}

// newIsolatedStore creates a random database, applies the embedded schema, and registers cleanup.
func newIsolatedStore(t *testing.T, dsn string) workflow.Store {
	t.Helper()
	store, _ := newIsolatedStoreAndDB(t, dsn)
	return store
}

// newIsolatedStoreAndDB creates an isolated Store and returns its database handle for database-specific tests.
func newIsolatedStoreAndDB(t *testing.T, dsn string) (workflow.Store, *sql.DB) {
	t.Helper()

	config, err := mysqldriver.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("ParseDSN() error = %v", err)
	}
	adminConfig := *config
	adminConfig.DBName = ""
	adminDB, err := sql.Open("mysql", adminConfig.FormatDSN())
	if err != nil {
		t.Fatalf("sql.Open(admin) error = %v", err)
	}
	if err := adminDB.PingContext(t.Context()); err != nil {
		adminDB.Close()
		t.Fatalf("admin PingContext() error = %v", err)
	}

	random := make([]byte, 8)
	if _, err := rand.Read(random); err != nil {
		adminDB.Close()
		t.Fatalf("rand.Read() error = %v", err)
	}
	databaseName := "easy_workflow_test_" + hex.EncodeToString(random)
	identifier := "`" + databaseName + "`"
	if _, err := adminDB.ExecContext(t.Context(), "CREATE DATABASE "+identifier+" CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin"); err != nil {
		adminDB.Close()
		t.Fatalf("CREATE DATABASE error = %v", err)
	}

	config.DBName = databaseName
	config.MultiStatements = true
	db, err := sql.Open("mysql", config.FormatDSN())
	if err != nil {
		_ = adminDB.Close()
		t.Fatalf("sql.Open() error = %v", err)
	}
	if err := db.PingContext(t.Context()); err != nil {
		db.Close()
		_, _ = adminDB.ExecContext(context.WithoutCancel(t.Context()), "DROP DATABASE "+identifier)
		adminDB.Close()
		t.Fatalf("PingContext() error = %v", err)
	}
	t.Cleanup(func() {
		db.Close()
		cleanupContext, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 5*time.Second)
		defer cancel()
		if _, err := adminDB.ExecContext(cleanupContext, "DROP DATABASE "+identifier); err != nil {
			t.Errorf("DROP DATABASE error = %v", err)
		}
		adminDB.Close()
	})

	applyInitialMigration(t, db)
	return mysql.New(db), db
}

// applyInitialMigration executes the embedded up migration as one setup operation for an isolated database.
func applyInitialMigration(t *testing.T, db *sql.DB) {
	t.Helper()

	paths, err := fs.Glob(mysql.Migrations(), "migrations/*.up.sql")
	if err != nil {
		t.Fatalf("Glob() error = %v", err)
	}
	var migrationSQL strings.Builder
	for _, path := range paths {
		data, readErr := fs.ReadFile(mysql.Migrations(), path)
		if readErr != nil {
			t.Fatalf("ReadFile(%q) error = %v", path, readErr)
		}
		migrationSQL.Write(data)
		migrationSQL.WriteString("\n")
	}
	if _, err := db.ExecContext(t.Context(), migrationSQL.String()); err != nil {
		t.Fatalf("migration error = %v", err)
	}
}

// integrationInstance returns a full independently allocated aggregate for durable round-trip assertions.
func integrationInstance(id workflow.InstanceID, version uint64) *workflow.Instance {
	return &workflow.Instance{
		ID: id,
		Definition: workflow.Definition{
			ID:      "durable-definition",
			Version: 1,
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
			{Action: "instance.returned", NodeID: "review", TargetNodeID: "approval", ActorID: "operator-1", Reason: "durable return audit", NodeState: `{"round":1}`, At: time.Date(2026, 1, 2, 4, 5, 6, 789012345, time.UTC)},
		},
		Version: version,
	}
}

// assertIntegrationSnapshot requires a field-for-field durable round trip through Store.Load.
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
