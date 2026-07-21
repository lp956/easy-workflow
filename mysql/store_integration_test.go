// This file verifies MySQL durability through the public adapter and shared Store contract.
// Tests require an explicitly supplied database DSN and otherwise skip without starting infrastructure.
package mysql_test

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
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
	if _, err := adminDB.ExecContext(t.Context(), "CREATE DATABASE "+identifier+" CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"); err != nil {
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
	return mysql.New(db)
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
