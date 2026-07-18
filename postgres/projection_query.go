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

// normalizeProjectionQuery validates shared adapter, cancellation, actor, and bounded-limit semantics.
//
// ctx must be non-nil and usable by pgx; query.ActorID must be non-empty. A zero limit becomes 50 and explicit
// limits must be in [1, 200]. The returned limit is safe for limit+1 look-ahead. Errors occur without database I/O.
func normalizeProjectionQuery(p *Projection, ctx context.Context, query ActorQuery) (int, error) {
	if err := ctx.Err(); err != nil {
		// Cancellation takes precedence because the caller can no longer consume validation or query results.
		return 0, fmt.Errorf("postgres: projection query: %w", err)
	}
	// Query execution requires a concrete adapter, borrowed pool, and trusted actor identity together.
	if p == nil || p.pool == nil || query.ActorID == "" {
		return 0, fmt.Errorf("%w: projection, pool, or actor is empty", ErrInvalidProjectionQuery)
	}
	limit := query.Page.Limit
	if limit == 0 {
		// Zero is the documented request sentinel, not an empty-page request.
		limit = defaultPageLimit
	}
	// Explicit limits outside the bounded public range are rejected instead of silently clamped.
	if limit < 1 || limit > maximumPageLimit {
		return 0, fmt.Errorf("%w: page limit %d is outside [1, %d]", ErrInvalidProjectionQuery, limit, maximumPageLimit)
	}
	return limit, nil
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
