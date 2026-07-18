// Package workflow provides a persistence-agnostic engine for auditable human-approval flows.
//
// The package owns canonical Definition validation and publication, instance transitions, task state,
// optimistic concurrency, and append-only audit records. Importing it does not read configuration, connect
// to infrastructure, start goroutines, or register handlers. MemoryStore and MemoryDefinitionStore provide
// process-local defaults for examples, tests, and single-process applications.
//
// Store is the command-side persistence contract: Create is insert-only, Load returns a caller-owned snapshot,
// and Save atomically compares the durable version before replacing the complete aggregate. NodeHandler is the
// extension contract: implementations validate configuration and return declarative runtime results without
// controlling persistence or graph navigation. Both contracts require concurrency safety and context propagation;
// stable package errors are designed for errors.Is classification.
//
// DefinitionPublisher is the shared publication boundary for Builder and JSON definitions. It compiles before
// persistence, delegates atomic monotonically increasing version allocation to DefinitionVersionWriter, and leaves
// failed publication without a consumed version. DefinitionReader selects either one exact immutable version or
// the current latest snapshot; Engine.StartPublished always freezes the exact version passed to it.
//
// Official node behavior lives in the approval and condition packages. PostgreSQL durability and query projections
// live in the optional postgres package. HTTP transports, Web UI, organization directories, authorization, and
// projection presentation remain host responsibilities rather than core dependencies.
package workflow
