// This file owns only query-boundary behavior that is identical across PostgreSQL projection families.
// It does not erase family-specific cursor shapes, SQL parameters, row decoding, or page construction.
// Helpers perform no I/O, retain no caller data, and are safe for concurrent use.
// Source text is UTF-8 and contains no platform-specific encoding or generated regions.
package postgres

import (
	"context"
	"fmt"

	workflow "github.com/lvpeng/easy-workflow"
)

const (
	// projectionVersionBase decodes PostgreSQL NUMERIC text emitted in ordinary decimal notation.
	projectionVersionBase = 10
	// projectionVersionBits preserves the complete uint64 definition-version contract without truncation.
	projectionVersionBits = 64
)

// projectionQueryBoundary is the prepared behavior shared by every PostgreSQL projection query family.
//
// actorID, scope, and limit are detached and fully validated before pool acquisition. Family modules extend this value
// with their own typed keyset rather than erasing task and instance continuation differences.
type projectionQueryBoundary struct {
	// actorID is the trusted host principal sent as the first SQL parameter.
	actorID workflow.ActorID
	// scope is nil for unrestricted or a detached text array for the exact host-authorized instance set.
	scope []string
	// limit is the normalized caller-visible row count before each family adds one look-ahead row.
	limit int
}

// prepareProjectionQuery validates and detaches shared adapter, actor, scope, and bounded-limit semantics.
//
// ctx must be non-nil and usable by pgx; actorID must be non-empty. A zero limit becomes 50 and explicit limits must be
// in [1, 200]. scopeIDs preserves nil as unrestricted and non-nil empty as deny-all. The returned value is safe for one
// limit+1 look-ahead query, owns its scope storage, retains no other input, and errors before database I/O.
func prepareProjectionQuery(
	p *Projection,
	ctx context.Context,
	actorID workflow.ActorID,
	scopeIDs []workflow.InstanceID,
	requestedLimit int,
) (projectionQueryBoundary, error) {
	if err := ctx.Err(); err != nil {
		// Cancellation takes precedence because the caller can no longer consume validation or query results.
		return projectionQueryBoundary{}, fmt.Errorf("postgres: projection query: %w", err)
	}
	// Query execution requires a concrete adapter, borrowed pool, and trusted actor identity together.
	if p == nil || p.pool == nil || actorID == "" {
		return projectionQueryBoundary{}, fmt.Errorf("%w: projection, pool, or actor is empty", ErrInvalidProjectionQuery)
	}
	limit := requestedLimit
	if limit == 0 {
		// Zero is the documented request sentinel, not an empty-page request.
		limit = defaultPageLimit
	}
	// Explicit limits outside the bounded public range are rejected instead of silently clamped.
	if limit < 1 || limit > maximumPageLimit {
		return projectionQueryBoundary{}, fmt.Errorf(
			"%w: page limit %d is outside [1, %d]",
			ErrInvalidProjectionQuery,
			limit,
			maximumPageLimit,
		)
	}
	return projectionQueryBoundary{actorID: actorID, scope: projectionScope(scopeIDs), limit: limit}, nil
}

// projectionScope converts host-owned workflow identities into detached PostgreSQL text-array parameters.
//
// nil remains nil to mean no scope constraint, while a non-nil empty slice remains empty to deny every instance.
// The conversion performs no I/O and prevents driver encoding from depending on named Go string types.
func projectionScope(ids []workflow.InstanceID) []string {
	if ids == nil {
		// Preserve nil so SQL can distinguish unrestricted scope from an explicitly empty authorized set.
		return nil
	}
	// Copy concrete identities into the driver's native string representation without retaining host scope storage.
	values := make([]string, len(ids))
	for index := range ids {
		values[index] = string(ids[index])
	}
	return values
}
