// This file verifies that PostgreSQL schema changes are explicit package artifacts.
// It does not apply migrations or connect to external infrastructure.
package postgres_test

import (
	"errors"
	"io/fs"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	workflow "github.com/lvpeng/easy-workflow"
	"github.com/lvpeng/easy-workflow/postgres"
)

// TestMigrationsExposeInitialSchema verifies that hosts can inspect and apply the versioned schema explicitly.
func TestMigrationsExposeInitialSchema(t *testing.T) {
	t.Parallel()

	data, err := fs.ReadFile(postgres.Migrations(), "migrations/0001_init.up.sql")
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	for _, table := range []string{
		"easy_workflow_instances",
		"easy_workflow_tasks",
		"easy_workflow_audit",
	} {
		if !strings.Contains(string(data), table) {
			t.Errorf("initial migration does not define %q", table)
		}
	}
}

// TestMigrationsExposeQueryProjection verifies hosts can apply and roll back the independent read-model schema.
func TestMigrationsExposeQueryProjection(t *testing.T) {
	t.Parallel()

	// The forward artifact must create both independent read-model tables available to host migration tooling.
	up, err := fs.ReadFile(postgres.Migrations(), "migrations/0002_query_projection.up.sql")
	if err != nil {
		t.Fatalf("ReadFile(up) error = %v", err)
	}
	for _, table := range []string{
		"easy_workflow_instance_projection",
		"easy_workflow_participation_projection",
	} {
		if !strings.Contains(string(up), table) {
			t.Errorf("query projection migration does not define %q", table)
		}
	}
	// The paired rollback artifact must remain explicitly discoverable rather than relying on runtime cleanup.
	down, err := fs.ReadFile(postgres.Migrations(), "migrations/0002_query_projection.down.sql")
	if err != nil {
		t.Fatalf("ReadFile(down) error = %v", err)
	}
	if !strings.Contains(string(down), "DROP TABLE") {
		t.Errorf("query projection down migration = %q, want table rollback", down)
	}
}

// TestNewReturnsStore verifies that explicit pool injection constructs the public workflow Store adapter.
func TestNewReturnsStore(t *testing.T) {
	t.Parallel()

	var pool *pgxpool.Pool
	var store workflow.Store = postgres.New(pool)
	if err := store.Create(t.Context(), &workflow.Instance{ID: "nil-pool"}); !errors.Is(err, workflow.ErrInvalidStoreInput) {
		t.Fatalf("Create() error = %v, want ErrInvalidStoreInput", err)
	}
}
