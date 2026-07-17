// Package postgres provides an optional durable workflow Store backed by PostgreSQL.
//
// Hosts explicitly create and configure a pgxpool.Pool, apply the SQL returned by Migrations through their chosen
// migration tooling, and pass the pool to New. Importing this package never connects, migrates, starts goroutines,
// or registers global infrastructure. The adapter persists only command-side workflow snapshots; definition
// publication and query projections remain separate modules.
package postgres
