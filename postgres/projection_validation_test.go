// This file verifies Projection boundary behavior that must complete without PostgreSQL I/O.
// It exercises only public query methods and does not inspect SQL or adapter-private helpers.
// Tests use independent closed pools and retain no shared state, so they may run concurrently if enabled.
// Source text is UTF-8 and contains no platform-specific encoding or generated regions.
package postgres_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	workflow "github.com/lvpeng/easy-workflow"
	"github.com/lvpeng/easy-workflow/postgres"
)

// wrapProjectionQueryError adds the public query family to a non-nil adapter error while preserving nil success.
// operation is a stable test label and err is the exact public query result; wrapped causes remain available to errors.Is.
func wrapProjectionQueryError(operation string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s projection query: %w", operation, err)
}

// TestProjectionWorklistReturnsEmptyPageForEmptyScope verifies deny-all scope needs no usable database connection.
//
// t owns a closed pool sentinel and records any acquisition, error, nil slice, item, or cursor as a test failure.
func TestProjectionWorklistReturnsEmptyPageForEmptyScope(t *testing.T) {
	// A closed caller-owned pool makes any accidental database acquisition fail deterministically.
	pool := newClosedProjectionPool(t)

	// The host's explicit deny-all authorization result must produce a successful, allocation-stable empty page.
	page, err := postgres.NewProjection(pool).Worklist(t.Context(), postgres.ActorQuery{
		ActorID: "reviewer-a",
		Scope: postgres.QueryScope{
			InstanceIDs: []workflow.InstanceID{},
		},
	})
	if err != nil {
		t.Fatalf("Worklist() error = %v, want nil", err)
	}
	// All three empty-page sentinels are part of the public success contract.
	if page.Items == nil || len(page.Items) != 0 || page.Next != nil {
		t.Fatalf("Worklist() page = %#v, want non-nil empty items and nil cursor", page)
	}
}

// TestProjectionInitiatedReturnsEmptyPageForEmptyScope verifies deny-all scope needs no usable database connection.
//
// t owns a closed pool sentinel and records any acquisition, error, nil slice, item, or cursor as a test failure.
func TestProjectionInitiatedReturnsEmptyPageForEmptyScope(t *testing.T) {
	// A closed caller-owned pool proves the instance query family resolves explicit empty scope before acquisition.
	pool := newClosedProjectionPool(t)

	// The successful page keeps Items non-nil so callers do not need a deny-all serialization special case.
	page, err := postgres.NewProjection(pool).Initiated(t.Context(), postgres.ActorQuery{
		ActorID: "requester-a",
		Scope: postgres.QueryScope{
			InstanceIDs: []workflow.InstanceID{},
		},
	})
	if err != nil {
		t.Fatalf("Initiated() error = %v, want nil", err)
	}
	// All three empty-page sentinels are part of the public success contract.
	if page.Items == nil || len(page.Items) != 0 || page.Next != nil {
		t.Fatalf("Initiated() page = %#v, want non-nil empty items and nil cursor", page)
	}
}

// TestProjectionParticipatedReturnsEmptyPageForEmptyScope verifies both task views share deny-all scope behavior.
//
// t owns a closed pool sentinel and records any acquisition, error, nil slice, item, or cursor as a test failure.
func TestProjectionParticipatedReturnsEmptyPageForEmptyScope(t *testing.T) {
	// A closed pool detects any drift that makes Participated bypass task-family scope normalization.
	pool := newClosedProjectionPool(t)

	// Explicit deny-all scope succeeds with the same stable empty-page shape as Worklist.
	page, err := postgres.NewProjection(pool).Participated(t.Context(), postgres.ActorQuery{
		ActorID: "reviewer-a",
		Scope: postgres.QueryScope{
			InstanceIDs: []workflow.InstanceID{},
		},
	})
	if err != nil {
		t.Fatalf("Participated() error = %v, want nil", err)
	}
	// All three empty-page sentinels are part of the public success contract.
	if page.Items == nil || len(page.Items) != 0 || page.Next != nil {
		t.Fatalf("Participated() page = %#v, want non-nil empty items and nil cursor", page)
	}
}

// TestProjectionQueryFamiliesEnforcePageLimits verifies shared bounds without requiring PostgreSQL execution.
//
// t runs every public family against a closed pool and treats accepted boundary values or classified rejections as
// the only successful outcomes. Empty scope ensures valid requests complete without external state.
func TestProjectionQueryFamiliesEnforcePageLimits(t *testing.T) {
	// Build one public caller for all families so only their boundary-specific behavior can differ.
	pool := newClosedProjectionPool(t)
	projection := postgres.NewProjection(pool)
	queries := []struct {
		name string
		call func(postgres.ActorQuery) error
	}{
		{name: "worklist", call: func(query postgres.ActorQuery) error {
			_, err := projection.Worklist(t.Context(), query)
			return wrapProjectionQueryError("worklist", err)
		}},
		{name: "participated", call: func(query postgres.ActorQuery) error {
			_, err := projection.Participated(t.Context(), query)
			return wrapProjectionQueryError("participated", err)
		}},
		{name: "initiated", call: func(query postgres.ActorQuery) error {
			_, err := projection.Initiated(t.Context(), query)
			return wrapProjectionQueryError("initiated", err)
		}},
	}

	// Empty scope prevents database I/O after each family accepts default, minimum, and maximum public limits.
	for _, family := range queries {
		for _, limit := range []int{projectionOmittedLimit, projectionMinimumLimit, projectionMaximumLimit} {
			// This callback accepts one documented boundary and must finish before touching the closed pool.
			t.Run(fmt.Sprintf("%s/valid/%d", family.name, limit), func(t *testing.T) {
				err := family.call(postgres.ActorQuery{
					ActorID: "actor-a",
					Scope:   postgres.QueryScope{InstanceIDs: []workflow.InstanceID{}},
					Page:    postgres.PageRequest{Limit: limit},
				})
				if err != nil {
					t.Fatalf("query error = %v, want nil", err)
				}
			})
		}
	}

	// Values immediately outside the documented range remain classified before the deny-all shortcut.
	for _, family := range queries {
		for _, limit := range []int{projectionInvalidLowerLimit, projectionInvalidUpperLimit} {
			// This callback requires classified rejection for one out-of-range value without pool acquisition.
			t.Run(fmt.Sprintf("%s/invalid/%d", family.name, limit), func(t *testing.T) {
				err := family.call(postgres.ActorQuery{
					ActorID: "actor-a",
					Scope:   postgres.QueryScope{InstanceIDs: []workflow.InstanceID{}},
					Page:    postgres.PageRequest{Limit: limit},
				})
				if !errors.Is(err, postgres.ErrInvalidProjectionQuery) {
					t.Fatalf("query error = %v, want ErrInvalidProjectionQuery", err)
				}
			})
		}
	}
}

// TestProjectionQueryFamiliesRejectCrossFamilyCursors verifies cursor shape remains part of each public family contract.
//
// t supplies complete cursors to the wrong family through public methods and requires classified rejection before
// the closed pool can be acquired. The test retains no cursor or page data after completion.
func TestProjectionQueryFamiliesRejectCrossFamilyCursors(t *testing.T) {
	// Build stable non-database inputs shared by both cursor-family checks.
	pool := newClosedProjectionPool(t)
	projection := postgres.NewProjection(pool)
	orderAt := time.Date(2026, 7, 18, 0, 0, 0, 0, time.UTC) // Fixed non-zero UTC key; wall-clock time is irrelevant.
	emptyScope := postgres.QueryScope{InstanceIDs: []workflow.InstanceID{}}

	// Initiated rejects the TaskID ordering key even when the remaining cursor fields are complete.
	taskCursor := &postgres.Cursor{At: orderAt, InstanceID: "instance-a", TaskID: "task-a"}
	_, err := projection.Initiated(t.Context(), postgres.ActorQuery{
		ActorID: "actor-a",
		Scope:   emptyScope,
		Page:    postgres.PageRequest{After: taskCursor},
	})
	if !errors.Is(err, postgres.ErrInvalidProjectionQuery) {
		t.Fatalf("Initiated(task cursor) error = %v, want ErrInvalidProjectionQuery", err)
	}

	// Both task views reject an instance-only cursor because it cannot identify one stable assignment row.
	instanceCursor := &postgres.Cursor{At: orderAt, InstanceID: "instance-a"}
	for name, call := range map[string]func() error{
		"worklist": func() error {
			_, queryErr := projection.Worklist(t.Context(), postgres.ActorQuery{
				ActorID: "actor-a",
				Scope:   emptyScope,
				Page:    postgres.PageRequest{After: instanceCursor},
			})
			return wrapProjectionQueryError("worklist", queryErr)
		},
		"participated": func() error {
			_, queryErr := projection.Participated(t.Context(), postgres.ActorQuery{
				ActorID: "actor-a",
				Scope:   emptyScope,
				Page:    postgres.PageRequest{After: instanceCursor},
			})
			return wrapProjectionQueryError("participated", queryErr)
		},
	} {
		// This callback verifies one task-family method rejects the shared instance-only cursor.
		t.Run(name, func(t *testing.T) {
			if queryErr := call(); !errors.Is(queryErr, postgres.ErrInvalidProjectionQuery) {
				t.Fatalf("task query error = %v, want ErrInvalidProjectionQuery", queryErr)
			}
		})
	}
}

// newClosedProjectionPool returns a caller-owned pool that deterministically rejects every acquisition attempt.
//
// The pool parses a syntactically valid local DSN but never connects before Close. Tests must use it only for public
// behavior expected to finish before database I/O; returning it lets those tests detect accidental acquisition.
func newClosedProjectionPool(t *testing.T) *pgxpool.Pool {
	t.Helper()

	// Parse and close without acquisition so the helper never depends on local PostgreSQL availability.
	pool, err := pgxpool.New(t.Context(), "postgres://unused@localhost/unused")
	if err != nil {
		t.Fatalf("pgxpool.New() error = %v", err)
	}
	// Closing is the test sentinel: any later acquisition returns a deterministic pool-closed error.
	pool.Close()
	return pool
}
