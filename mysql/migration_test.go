// This file verifies that MySQL schema changes are explicit package artifacts.
// It does not apply migrations or connect to external infrastructure.
package mysql_test

import (
	"database/sql"
	"errors"
	"io/fs"
	"strings"
	"testing"

	workflow "github.com/lvpeng/easy-workflow"
	"github.com/lvpeng/easy-workflow/mysql"
)

// TestMigrationsExposeInitialSchema verifies that hosts can inspect and apply the versioned schema explicitly.
func TestMigrationsExposeInitialSchema(t *testing.T) {
	t.Parallel()

	data, err := fs.ReadFile(mysql.Migrations(), "migrations/0001_init.up.sql")
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
	for _, fragment := range []string{
		"MySQL 8.0.16",
		"utf8mb4_0900_bin",
		"CHECK (task_id <> '')",
		"CHECK (action <> '')",
	} {
		if !strings.Contains(string(data), fragment) {
			t.Errorf("initial migration does not contain %q", fragment)
		}
	}
}

// TestNewReturnsStore verifies that explicit database injection constructs the public workflow Store adapter.
func TestNewReturnsStore(t *testing.T) {
	t.Parallel()

	var db *sql.DB
	var store workflow.Store = mysql.New(db)
	if err := store.Create(t.Context(), &workflow.Instance{ID: "nil-db"}); !errors.Is(err, workflow.ErrInvalidStoreInput) {
		t.Fatalf("Create() error = %v, want ErrInvalidStoreInput", err)
	}
}
