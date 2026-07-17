// Package postgres provides durable command persistence and independent workflow query projections backed by PostgreSQL.
//
// Hosts explicitly create and configure a pgxpool.Pool, apply the SQL returned by Migrations through their chosen
// migration tooling, and pass the pool to New and NewProjection. Importing this package never connects, migrates,
// starts goroutines, or registers global infrastructure. Projection search and pagination remain separate from the
// core workflow.Store interface, while Store transactions refresh read rows atomically with aggregate facts.
package postgres
