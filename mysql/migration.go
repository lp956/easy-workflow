// This file exposes versioned MySQL schema files without applying them.
// Hosts retain control of connection setup, migration locking, ordering, and rollback policy.
package mysql

import (
	"embed"
	"io/fs"
)

// migrationFiles is immutable package data embedded for explicit use by host migration tooling.
//
//go:embed migrations/*.sql
var migrationFiles embed.FS

// Migrations returns the adapter's versioned SQL files as a read-only filesystem.
//
// The returned filesystem contains files below the "migrations" directory. This function performs no database,
// filesystem, or network I/O; callers decide when and how migrations are applied and serialized.
func Migrations() fs.FS {
	return migrationFiles
}
