// Package mysql provides durable command persistence for workflow instances backed by MySQL.
//
// Hosts explicitly create and configure a *sql.DB, apply the SQL returned by Migrations through their chosen
// migration tooling, and pass the database handle to New. Importing this package never connects, migrates, or
// starts infrastructure. The adapter implements only the core workflow.Store contract; query projections remain
// outside this package until a MySQL-specific projection contract is defined.
//
// Store.Create and Store.Save own their transaction boundaries: the parent Instance, frozen Definition, business
// data, node state, tasks, and append-only audit suffix commit or roll back together. Save's conditional version
// update is the cross-process compare-and-swap authority and reports stale writes through workflow.ErrVersionConflict.
// The embedded schema requires MySQL 8.0.16 or later, uses case-sensitive NO PAD utf8mb4 VARCHAR(255) columns for
// indexed identifiers, and validates child-row values before writing; opaque Definition, business, task, and audit
// payloads use LONGBLOB storage.
package mysql
