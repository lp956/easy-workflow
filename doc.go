// Package workflow provides a persistence-agnostic engine for auditable human-approval flows.
//
// The package owns graph validation, instance transitions, task state, and audit records. Business
// modules own node configuration and behavior through registered handlers; HTTP, databases, and
// organization directories remain outside the core library.
package workflow
