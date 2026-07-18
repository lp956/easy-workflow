// Package postgres provides durable command persistence and independent workflow query projections backed by PostgreSQL.
//
// Hosts explicitly create and configure a pgxpool.Pool, apply the SQL returned by Migrations through their chosen
// migration tooling, and pass the pool to New and NewProjection. Importing this package never connects, migrates,
// starts goroutines, or registers global infrastructure. Projection search and pagination remain separate from the
// core workflow.Store interface, while Store transactions refresh read rows atomically with aggregate facts.
//
// Store.Create and Store.Save own their transaction boundaries: the parent Instance, frozen Definition, business
// data, node state, tasks, append-only audit suffix, and derived projection rows commit or roll back together. Save's
// conditional version update is the cross-process compare-and-swap authority and reports stale writes through
// workflow.ErrVersionConflict. Load reconstructs one caller-owned aggregate from a repeatable-read transaction.
//
// PostgreSQL integration tests run only when EASY_WORKFLOW_POSTGRES_DSN is explicitly set. They create isolated
// schemas and exercise the shared Store contract, rollback, concurrent CAS, pool restart, snapshot fidelity, and
// projections; the package never starts a test database or falls back to an implicit local connection.
package postgres
