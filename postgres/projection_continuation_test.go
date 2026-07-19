// This file verifies opaque Projection continuation behavior through public query methods and real PostgreSQL ordering.
// Unit cases stop before pool acquisition; integration cases use the package's random isolated-schema test harness.
package postgres_test

import (
	"errors"
	"slices"
	"testing"
	"time"

	workflow "github.com/lvpeng/easy-workflow"
	"github.com/lvpeng/easy-workflow/postgres"
)

// TestProjectionContinuationReturnsEmptyPageForEmptyScope verifies all token APIs preserve deny-all authorization.
func TestProjectionContinuationReturnsEmptyPageForEmptyScope(t *testing.T) {
	// A closed pool proves every family completes the explicit empty-scope result without database acquisition.
	projection := postgres.NewProjection(newClosedProjectionPool(t))
	emptyScope := postgres.QueryScope{InstanceIDs: []workflow.InstanceID{}}
	queries := []struct {
		name string
		call func() (int, postgres.Continuation, error)
	}{
		{name: "worklist", call: func() (int, postgres.Continuation, error) {
			page, err := projection.WorklistPage(t.Context(), postgres.ContinuationQuery{
				ActorID: "reviewer-a",
				Scope:   emptyScope,
			})
			return len(page.Items), page.Next, err
		}},
		{name: "participated", call: func() (int, postgres.Continuation, error) {
			page, err := projection.ParticipatedPage(t.Context(), postgres.ContinuationQuery{
				ActorID: "reviewer-a",
				Scope:   emptyScope,
			})
			return len(page.Items), page.Next, err
		}},
		{name: "initiated", call: func() (int, postgres.Continuation, error) {
			page, err := projection.InitiatedPage(t.Context(), postgres.ContinuationQuery{
				ActorID: "requester-a",
				Scope:   emptyScope,
			})
			return len(page.Items), page.Next, err
		}},
	}

	// Each family returns a non-error empty result and the empty continuation sentinel.
	for _, query := range queries {
		t.Run(query.name, func(t *testing.T) {
			count, next, err := query.call()
			if err != nil {
				t.Fatalf("query error = %v, want nil", err)
			}
			if count != 0 || next != "" {
				t.Fatalf("page count/next = %d/%q, want 0/empty", count, next)
			}
		})
	}
}

// TestProjectionContinuationRejectsMalformedTokensBeforeDatabase verifies callers cannot supply raw keyset fragments.
func TestProjectionContinuationRejectsMalformedTokensBeforeDatabase(t *testing.T) {
	// Invalid opaque text must be rejected before the deny-all shortcut can hide a malformed continuation.
	projection := postgres.NewProjection(newClosedProjectionPool(t))
	query := postgres.ContinuationQuery{
		ActorID: "actor-a",
		Scope:   postgres.QueryScope{InstanceIDs: []workflow.InstanceID{}},
		Page: postgres.ContinuationPageRequest{
			After: postgres.Continuation("not-an-encoded-continuation"),
		},
	}
	queries := []struct {
		name string
		call func() error
	}{
		{name: "worklist", call: func() error {
			_, err := projection.WorklistPage(t.Context(), query)
			return wrapProjectionQueryError("worklist page", err)
		}},
		{name: "participated", call: func() error {
			_, err := projection.ParticipatedPage(t.Context(), query)
			return wrapProjectionQueryError("participated page", err)
		}},
		{name: "initiated", call: func() error {
			_, err := projection.InitiatedPage(t.Context(), query)
			return wrapProjectionQueryError("initiated page", err)
		}},
	}

	// Every family owns token decoding and retains the shared public validation sentinel.
	for _, family := range queries {
		t.Run(family.name, func(t *testing.T) {
			if err := family.call(); !errors.Is(err, postgres.ErrInvalidProjectionQuery) {
				t.Fatalf("query error = %v, want ErrInvalidProjectionQuery", err)
			}
		})
	}
}

// TestProjectionContinuationEnforcesPageLimits verifies token APIs preserve the documented shared resource bounds.
func TestProjectionContinuationEnforcesPageLimits(t *testing.T) {
	// Empty scope makes every accepted boundary complete before touching the closed caller-owned pool.
	projection := postgres.NewProjection(newClosedProjectionPool(t))
	emptyScope := postgres.QueryScope{InstanceIDs: []workflow.InstanceID{}}
	queries := []struct {
		name string
		call func(postgres.ContinuationQuery) error
	}{
		{name: "worklist", call: func(query postgres.ContinuationQuery) error {
			_, err := projection.WorklistPage(t.Context(), query)
			return wrapProjectionQueryError("worklist page", err)
		}},
		{name: "participated", call: func(query postgres.ContinuationQuery) error {
			_, err := projection.ParticipatedPage(t.Context(), query)
			return wrapProjectionQueryError("participated page", err)
		}},
		{name: "initiated", call: func(query postgres.ContinuationQuery) error {
			_, err := projection.InitiatedPage(t.Context(), query)
			return wrapProjectionQueryError("initiated page", err)
		}},
	}

	// Zero, minimum, and maximum limits remain accepted without clamping or acquiring a connection.
	for _, family := range queries {
		for _, limit := range []int{projectionOmittedLimit, projectionMinimumLimit, projectionMaximumLimit} {
			t.Run(family.name+"/valid", func(t *testing.T) {
				err := family.call(postgres.ContinuationQuery{
					ActorID: "actor-a",
					Scope:   emptyScope,
					Page:    postgres.ContinuationPageRequest{Limit: limit},
				})
				if err != nil {
					t.Fatalf("limit %d error = %v, want nil", limit, err)
				}
			})
		}
	}

	// Values immediately outside the public range retain ErrInvalidProjectionQuery before the deny-all shortcut.
	for _, family := range queries {
		for _, limit := range []int{projectionInvalidLowerLimit, projectionInvalidUpperLimit} {
			t.Run(family.name+"/invalid", func(t *testing.T) {
				err := family.call(postgres.ContinuationQuery{
					ActorID: "actor-a",
					Scope:   emptyScope,
					Page:    postgres.ContinuationPageRequest{Limit: limit},
				})
				if !errors.Is(err, postgres.ErrInvalidProjectionQuery) {
					t.Fatalf("limit %d error = %v, want ErrInvalidProjectionQuery", limit, err)
				}
			})
		}
	}
}

// TestProjectionContinuationPagesStableFamilies verifies encoded tokens preserve each family's complete keyset order.
//
// The test owns a random migrated schema with equal-time task and instance rows. It requires every row exactly once,
// permits tokens across the two task views, and rejects task/instance cross-family continuation before SQL execution.
func TestProjectionContinuationPagesStableFamilies(t *testing.T) {
	dsn := requireIntegrationDSN(t)
	pool := newProjectionPool(t, dsn)
	store := postgres.New(pool)
	equalTime := time.Date(2026, 7, 18, 15, 0, 0, 0, time.UTC) // Equal times isolate deterministic identity tie-breakers.

	// One aggregate supplies three rows to each task view and the first of two initiated-instance rows.
	primary := &workflow.Instance{
		ID: "continuation-instance-a",
		Definition: workflow.Definition{
			ID:      "continuation-definition",
			Version: 1,
		},
		Status:        workflow.InstanceStatusRunning,
		Initiator:     "requester-a",
		CurrentNodeID: "review",
		Tasks: []workflow.Task{
			{ID: "active-a", NodeID: "review", Assignee: "reviewer-a", Status: workflow.TaskStatusActive},
			{ID: "active-b", NodeID: "review", Assignee: "reviewer-a", Status: workflow.TaskStatusActive},
			{ID: "active-c", NodeID: "review", Assignee: "reviewer-a", Status: workflow.TaskStatusActive},
			{ID: "completed-a", NodeID: "review", Assignee: "reviewer-a", Status: workflow.TaskStatusCompleted},
			{ID: "completed-b", NodeID: "review", Assignee: "reviewer-a", Status: workflow.TaskStatusCompleted},
			{ID: "completed-c", NodeID: "review", Assignee: "reviewer-a", Status: workflow.TaskStatusCompleted},
		},
		Audit: []workflow.AuditRecord{{
			Action:  "instance.started",
			NodeID:  "start",
			ActorID: "requester-a",
			At:      equalTime,
		}},
		Version: 1,
	}
	if err := store.Create(t.Context(), primary); err != nil {
		t.Fatalf("Create(primary) error = %v", err)
	}
	secondary := &workflow.Instance{
		ID: "continuation-instance-b",
		Definition: workflow.Definition{
			ID:      "continuation-definition",
			Version: 1,
		},
		Status:        workflow.InstanceStatusRunning,
		Initiator:     "requester-a",
		CurrentNodeID: "review",
		Audit: []workflow.AuditRecord{{
			Action:  "instance.started",
			NodeID:  "start",
			ActorID: "requester-a",
			At:      equalTime,
		}},
		Version: 1,
	}
	if err := store.Create(t.Context(), secondary); err != nil {
		t.Fatalf("Create(secondary) error = %v", err)
	}

	// Follow independent opaque token chains through both task views and record one token for family checks.
	projection := postgres.NewProjection(pool)
	taskFamilies := []struct {
		name string
		want []workflow.TaskID
		call func(postgres.Continuation) (postgres.ContinuationPage[postgres.TaskProjection], error)
	}{
		{name: "worklist", want: []workflow.TaskID{"active-a", "active-b", "active-c"}, call: func(after postgres.Continuation) (postgres.ContinuationPage[postgres.TaskProjection], error) {
			return projection.WorklistPage(t.Context(), postgres.ContinuationQuery{
				ActorID: "reviewer-a",
				Page:    postgres.ContinuationPageRequest{Limit: projectionMinimumLimit, After: after},
			})
		}},
		{name: "participated", want: []workflow.TaskID{"completed-a", "completed-b", "completed-c"}, call: func(after postgres.Continuation) (postgres.ContinuationPage[postgres.TaskProjection], error) {
			return projection.ParticipatedPage(t.Context(), postgres.ContinuationQuery{
				ActorID: "reviewer-a",
				Page:    postgres.ContinuationPageRequest{Limit: projectionMinimumLimit, After: after},
			})
		}},
	}
	var taskToken postgres.Continuation
	for _, family := range taskFamilies {
		t.Run(family.name, func(t *testing.T) {
			var after postgres.Continuation
			got := make([]workflow.TaskID, 0, len(family.want))
			for len(got) < len(family.want) {
				page, err := family.call(after)
				if err != nil {
					t.Fatalf("query error = %v", err)
				}
				if len(page.Items) != projectionMinimumLimit {
					t.Fatalf("page.Items = %#v, want one item", page.Items)
				}
				got = append(got, page.Items[0].TaskID)
				if len(got) < len(family.want) {
					if page.Next == "" {
						t.Fatalf("page %d continuation is empty before final item", len(got))
					}
					if taskToken == "" {
						taskToken = page.Next
					}
					after = page.Next
					continue
				}
				if page.Next != "" {
					t.Fatalf("final continuation = %q, want empty", page.Next)
				}
			}
			if !slices.Equal(got, family.want) {
				t.Fatalf("task order = %v, want %v", got, family.want)
			}
		})
	}

	// Page the two equal-time initiated rows and retain the task-free family token.
	firstInstance, err := projection.InitiatedPage(t.Context(), postgres.ContinuationQuery{
		ActorID: "requester-a",
		Page:    postgres.ContinuationPageRequest{Limit: projectionMinimumLimit},
	})
	if err != nil {
		t.Fatalf("first InitiatedPage() error = %v", err)
	}
	if len(firstInstance.Items) != 1 || firstInstance.Items[0].InstanceID != primary.ID || firstInstance.Next == "" {
		t.Fatalf("first InitiatedPage() = %#v, want primary and continuation", firstInstance)
	}
	instanceToken := firstInstance.Next
	secondInstance, err := projection.InitiatedPage(t.Context(), postgres.ContinuationQuery{
		ActorID: "requester-a",
		Page:    postgres.ContinuationPageRequest{Limit: projectionMinimumLimit, After: instanceToken},
	})
	if err != nil {
		t.Fatalf("second InitiatedPage() error = %v", err)
	}
	if len(secondInstance.Items) != 1 || secondInstance.Items[0].InstanceID != secondary.ID || secondInstance.Next != "" {
		t.Fatalf("second InitiatedPage() = %#v, want final secondary", secondInstance)
	}

	// Worklist and Participated share one task family, while the instance family rejects task-specific keysets and vice versa.
	if _, err := projection.ParticipatedPage(t.Context(), postgres.ContinuationQuery{
		ActorID: "reviewer-a",
		Page:    postgres.ContinuationPageRequest{Limit: projectionMinimumLimit, After: taskToken},
	}); err != nil {
		t.Fatalf("ParticipatedPage(task-family token) error = %v", err)
	}
	if _, err := projection.InitiatedPage(t.Context(), postgres.ContinuationQuery{
		ActorID: "requester-a",
		Page:    postgres.ContinuationPageRequest{After: taskToken},
	}); !errors.Is(err, postgres.ErrInvalidProjectionQuery) {
		t.Fatalf("InitiatedPage(task token) error = %v, want ErrInvalidProjectionQuery", err)
	}
	if _, err := projection.WorklistPage(t.Context(), postgres.ContinuationQuery{
		ActorID: "reviewer-a",
		Page:    postgres.ContinuationPageRequest{After: instanceToken},
	}); !errors.Is(err, postgres.ErrInvalidProjectionQuery) {
		t.Fatalf("WorklistPage(instance token) error = %v, want ErrInvalidProjectionQuery", err)
	}
}
