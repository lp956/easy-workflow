// This file verifies PostgreSQL query projections through Engine commands and the public Projection API.
// Tests use isolated schemas and never inspect projection tables or adapter-private query construction.
package postgres_test

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	workflow "github.com/lvpeng/easy-workflow"
	"github.com/lvpeng/easy-workflow/approval"
	"github.com/lvpeng/easy-workflow/postgres"
)

const (
	// projectionOmittedLimit is the PageRequest sentinel that selects the default page size.
	projectionOmittedLimit = 0
	// projectionMinimumLimit is the smallest documented explicit PageRequest limit.
	projectionMinimumLimit = 1
	// projectionDefaultLimit is the documented item count selected by a zero PageRequest limit.
	projectionDefaultLimit = 50
	// projectionMaximumLimit is the documented largest accepted explicit PageRequest limit.
	projectionMaximumLimit = 200
	// projectionInvalidLowerLimit is the first integer below the accepted explicit range.
	projectionInvalidLowerLimit = -1
	// projectionInvalidUpperLimit is the first integer above the accepted explicit range.
	projectionInvalidUpperLimit = projectionMaximumLimit + 1
	// projectionLimitFixtureCount leaves exactly one look-ahead row beyond the maximum visible page.
	projectionLimitFixtureCount = projectionInvalidUpperLimit
)

// projectionReturnPolicyFunc adapts one test authorization decision to workflow.ReturnPolicy.
type projectionReturnPolicyFunc func(context.Context, workflow.ReturnRequest, *workflow.Instance) error

// AuthorizeReturn delegates the host decision without changing its result or error identity.
func (f projectionReturnPolicyFunc) AuthorizeReturn(
	ctx context.Context,
	request workflow.ReturnRequest,
	instance *workflow.Instance,
) error {
	return f(ctx, request, instance)
}

// projectionTransferPolicyFunc adapts one test authorization decision to workflow.TransferPolicy.
type projectionTransferPolicyFunc func(context.Context, workflow.TransferRequest, workflow.Task, *workflow.Instance) error

// AuthorizeTransfer delegates the host decision without changing its result or error identity.
func (f projectionTransferPolicyFunc) AuthorizeTransfer(
	ctx context.Context,
	request workflow.TransferRequest,
	task workflow.Task,
	instance *workflow.Instance,
) error {
	return f(ctx, request, task, instance)
}

// projectionRoleResolverFunc adapts one host-owned role lookup to Approval's organization boundary.
type projectionRoleResolverFunc func(context.Context, string, json.RawMessage) ([]workflow.ActorID, error)

// ResolveRole delegates one lookup and preserves the caller-owned result contract.
func (f projectionRoleResolverFunc) ResolveRole(
	ctx context.Context,
	role string,
	data json.RawMessage,
) ([]workflow.ActorID, error) {
	return f(ctx, role, data)
}

// TestProjectionWorklistReturnsActiveFrozenAssignment verifies one committed activation is immediately queryable.
//
// t owns an isolated schema containing in-scope and out-of-scope assignments for the same actor. The public Worklist
// call must return only the authorized instance with complete frozen fields and no continuation cursor.
func TestProjectionWorklistReturnsActiveFrozenAssignment(t *testing.T) {
	// Build one engine whose commands and projection writes share the same caller-owned PostgreSQL pool.
	dsn := requireIntegrationDSN(t)
	pool := newProjectionPool(t, dsn)
	store := postgres.New(pool)
	registry := workflow.NewRegistry()
	// Register the only handler before either instance starts so both fixtures resolve identical assignments.
	if err := registry.Register(approval.Kind, approval.NewHandler()); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	definition := projectionApprovalDefinition(t, "expense", []workflow.ActorID{"reviewer-a"})
	engine := workflow.NewEngine(store, registry)

	// Start commits the aggregate and its query projection in the same PostgreSQL transaction.
	instance, err := engine.Start(t.Context(), definition, workflow.StartRequest{
		ID:        "expense-1",
		Initiator: "requester-a",
	})
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	// A second assignment for the same actor must remain invisible because the host does not include it in scope.
	if _, err := engine.Start(t.Context(), definition, workflow.StartRequest{
		ID:        "expense-2",
		Initiator: "requester-a",
	}); err != nil {
		t.Fatalf("Start(out-of-scope) error = %v", err)
	}

	// Query only the first instance so the item count detects any ignored or broadened populated scope.
	page, err := postgres.NewProjection(pool).Worklist(t.Context(), postgres.ActorQuery{
		ActorID: "reviewer-a",
		Scope: postgres.QueryScope{
			InstanceIDs: []workflow.InstanceID{instance.ID},
		},
	})
	if err != nil {
		t.Fatalf("Worklist() error = %v", err)
	}

	// The public row joins frozen definition identity, instance state, assignment state, and audit times.
	if len(page.Items) != projectionMinimumLimit {
		t.Fatalf("Worklist() item count = %d, want %d", len(page.Items), projectionMinimumLimit)
	}
	item := page.Items[0]
	// Every joined identity and status must come from the authorized committed aggregate.
	if item.DefinitionID != definition.ID || item.DefinitionVersion != definition.Version ||
		item.InstanceID != instance.ID || item.InstanceStatus != workflow.InstanceStatusRunning ||
		item.NodeID != "review" || item.TaskID != instance.Tasks[0].ID ||
		item.ActorID != "reviewer-a" || item.TaskStatus != workflow.TaskStatusActive {
		t.Errorf("Worklist() item = %#v, want active frozen assignment", item)
	}
	// Both public audit pointers must remain populated from the same committed projection snapshot.
	if item.StartedAt == nil || item.LastAuditAt == nil {
		t.Errorf("Worklist() audit times = (%v, %v), want both populated", item.StartedAt, item.LastAuditAt)
	}
	if page.Next != nil {
		t.Errorf("Worklist() next cursor = %#v, want nil", page.Next)
	}
}

// TestProjectionMovesFinishedRoundFromWorklistToParticipation verifies completed and closed tasks remain queryable.
func TestProjectionMovesFinishedRoundFromWorklistToParticipation(t *testing.T) {
	dsn := requireIntegrationDSN(t)
	pool := newProjectionPool(t, dsn)
	store := postgres.New(pool)
	registry := workflow.NewRegistry()
	if err := registry.Register(approval.Kind, approval.NewHandler()); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	definition := projectionApprovalDefinition(t, "purchase", []workflow.ActorID{"reviewer-a", "reviewer-b"})
	engine := workflow.NewEngine(store, registry)
	instance, err := engine.Start(t.Context(), definition, workflow.StartRequest{
		ID:        "purchase-1",
		Initiator: "requester-a",
	})
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	// Any-sign approval completes the acting task and closes its frozen sibling in one Store.Save transaction.
	finished, err := engine.Handle(t.Context(), workflow.Command{
		InstanceID: instance.ID,
		TaskID:     instance.Tasks[0].ID,
		ActorID:    "reviewer-a",
		Name:       approval.CommandApprove,
	})
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	projection := postgres.NewProjection(pool)
	worklist, err := projection.Worklist(t.Context(), postgres.ActorQuery{ActorID: "reviewer-a"})
	if err != nil {
		t.Fatalf("Worklist() error = %v", err)
	}
	if len(worklist.Items) != 0 {
		t.Errorf("Worklist() items = %#v, want empty after completion", worklist.Items)
	}

	// Each frozen actor keeps an independently queryable participation row with its final task status.
	for index, expected := range []struct {
		actor  workflow.ActorID
		status workflow.TaskStatus
	}{
		{actor: "reviewer-a", status: workflow.TaskStatusCompleted},
		{actor: "reviewer-b", status: workflow.TaskStatusClosed},
	} {
		page, queryErr := projection.Participated(t.Context(), postgres.ActorQuery{ActorID: expected.actor})
		if queryErr != nil {
			t.Fatalf("Participated(actor %q) error = %v", expected.actor, queryErr)
		}
		if len(page.Items) != 1 {
			t.Fatalf("Participated(actor %q) item count = %d, want 1", expected.actor, len(page.Items))
		}
		item := page.Items[0]
		if item.TaskID != finished.Tasks[index].ID || item.ActorID != expected.actor ||
			item.TaskStatus != expected.status || item.InstanceStatus != workflow.InstanceStatusCompleted {
			t.Errorf("Participated(actor %q) item = %#v, want final frozen task", expected.actor, item)
		}
	}
}

// TestProjectionInitiatedPagesRunningAndCompletedInstances verifies initiator history is retained and scoped.
func TestProjectionInitiatedPagesRunningAndCompletedInstances(t *testing.T) {
	dsn := requireIntegrationDSN(t)
	pool := newProjectionPool(t, dsn)
	store := postgres.New(pool)
	registry := workflow.NewRegistry()
	if err := registry.Register(approval.Kind, approval.NewHandler()); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	definition := projectionApprovalDefinition(t, "travel", []workflow.ActorID{"reviewer-a"})
	engine := workflow.NewEngine(store, registry)
	completed, err := engine.Start(t.Context(), definition, workflow.StartRequest{ID: "travel-1", Initiator: "requester-a"})
	if err != nil {
		t.Fatalf("first Start() error = %v", err)
	}
	if _, err := engine.Handle(t.Context(), workflow.Command{
		InstanceID: completed.ID,
		TaskID:     completed.Tasks[0].ID,
		ActorID:    "reviewer-a",
		Name:       approval.CommandApprove,
	}); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	running, err := engine.Start(t.Context(), definition, workflow.StartRequest{ID: "travel-2", Initiator: "requester-a"})
	if err != nil {
		t.Fatalf("second Start() error = %v", err)
	}

	// Follow the returned keyset cursor so the two instance states are observed once without offset pagination.
	projection := postgres.NewProjection(pool)
	first, err := projection.Initiated(t.Context(), postgres.ActorQuery{
		ActorID: "requester-a",
		Page:    postgres.PageRequest{Limit: 1},
	})
	if err != nil {
		t.Fatalf("first Initiated() error = %v", err)
	}
	if len(first.Items) != 1 || first.Next == nil {
		t.Fatalf("first Initiated() page = %#v, want one item and cursor", first)
	}
	second, err := projection.Initiated(t.Context(), postgres.ActorQuery{
		ActorID: "requester-a",
		Page:    postgres.PageRequest{Limit: 1, After: first.Next},
	})
	if err != nil {
		t.Fatalf("second Initiated() error = %v", err)
	}
	if len(second.Items) != 1 || second.Next != nil {
		t.Fatalf("second Initiated() page = %#v, want final single item", second)
	}
	items := make([]postgres.InstanceProjection, 0, len(first.Items)+len(second.Items))
	items = append(items, first.Items...)
	items = append(items, second.Items...)
	statuses := map[workflow.InstanceID]workflow.InstanceStatus{}
	for _, item := range items {
		statuses[item.InstanceID] = item.InstanceStatus
		if item.DefinitionID != definition.ID || item.DefinitionVersion != definition.Version ||
			item.Initiator != "requester-a" || item.StartedAt == nil || item.LastAuditAt == nil {
			t.Errorf("Initiated() item = %#v, want complete definition, initiator, and audit fields", item)
		}
	}
	if statuses[completed.ID] != workflow.InstanceStatusCompleted || statuses[running.ID] != workflow.InstanceStatusRunning {
		t.Errorf("Initiated() statuses = %v, want completed and running instances", statuses)
	}

	// A host scope with no authorized instances must stay empty, and SQL-like actor text remains ordinary data.
	for _, query := range []postgres.ActorQuery{
		{ActorID: "requester-a", Scope: postgres.QueryScope{InstanceIDs: []workflow.InstanceID{}}},
		{ActorID: "requester-a'; DROP TABLE easy_workflow_instance_projection; --"},
	} {
		page, queryErr := projection.Initiated(t.Context(), query)
		if queryErr != nil {
			t.Fatalf("scoped Initiated() error = %v", queryErr)
		}
		if len(page.Items) != 0 {
			t.Errorf("scoped Initiated() items = %#v, want empty", page.Items)
		}
	}
}

// TestProjectionTaskPaginationBreaksEqualTimesByIdentity verifies equal audit timestamps cannot duplicate or skip rows.
//
// t owns an isolated schema with two equal-time instances. Worklist and Initiated must page both identities exactly
// once in ascending tie-breaker order and expose nil Next only after the second committed row.
func TestProjectionTaskPaginationBreaksEqualTimesByIdentity(t *testing.T) {
	dsn := requireIntegrationDSN(t)
	pool := newProjectionPool(t, dsn)
	store := postgres.New(pool)
	equalTime := time.Date(2026, 7, 17, 12, 0, 0, 0, time.UTC) // Fixed UTC input makes identity the only changing key.
	for _, instanceID := range []workflow.InstanceID{"tie-a", "tie-b"} {
		instance := &workflow.Instance{
			ID: instanceID,
			Definition: workflow.Definition{
				ID:      "tie-definition",
				Version: 3, // A non-default frozen version also exercises projection version decoding.
			},
			Status:        workflow.InstanceStatusRunning,
			Initiator:     "requester-a",
			CurrentNodeID: "review",
			Tasks: []workflow.Task{{
				ID:       workflow.TaskID(instanceID + "-task"), // Instance-derived identity stays unique and ordered.
				NodeID:   "review",
				Assignee: "reviewer-a",
				Status:   workflow.TaskStatusActive,
			}},
			Audit: []workflow.AuditRecord{{
				Action:  "instance.started",
				NodeID:  "start",
				ActorID: "requester-a",
				At:      equalTime,
			}},
			Version: 1, // Each independent fixture is inserted once without a prior durable revision.
		}
		// Commit the complete aggregate and projection before advancing to the next identity.
		if err := store.Create(t.Context(), instance); err != nil {
			t.Fatalf("Create(%q) error = %v", instanceID, err)
		}
	}

	// The cursor must advance by ascending instance and task identity when the primary timestamp is identical.
	projection := postgres.NewProjection(pool)
	first, err := projection.Worklist(t.Context(), postgres.ActorQuery{
		ActorID: "reviewer-a",
		Page:    postgres.PageRequest{Limit: projectionMinimumLimit},
	})
	if err != nil {
		t.Fatalf("first Worklist() error = %v", err)
	}
	// The first equal-time task must expose a continuation to the only later instance identity.
	if len(first.Items) != projectionMinimumLimit || first.Items[0].InstanceID != "tie-a" || first.Next == nil {
		t.Fatalf("first Worklist() page = %#v, want tie-a and cursor", first)
	}
	second, err := projection.Worklist(t.Context(), postgres.ActorQuery{
		ActorID: "reviewer-a",
		Page:    postgres.PageRequest{Limit: projectionMinimumLimit, After: first.Next},
	})
	if err != nil {
		t.Fatalf("second Worklist() error = %v", err)
	}
	// The second equal-time task is final and therefore must not expose another continuation.
	if len(second.Items) != projectionMinimumLimit || second.Items[0].InstanceID != "tie-b" || second.Next != nil {
		t.Fatalf("second Worklist() page = %#v, want final tie-b", second)
	}

	// The instance family uses the same timestamp and InstanceID ordering without accepting a TaskID component.
	firstInstance, err := projection.Initiated(t.Context(), postgres.ActorQuery{
		ActorID: "requester-a",
		Page:    postgres.PageRequest{Limit: projectionMinimumLimit},
	})
	if err != nil {
		t.Fatalf("first Initiated() error = %v", err)
	}
	// The first equal-time instance must expose a continuation to the only later identity.
	if len(firstInstance.Items) != projectionMinimumLimit || firstInstance.Items[0].InstanceID != "tie-a" || firstInstance.Next == nil {
		t.Fatalf("first Initiated() page = %#v, want tie-a and cursor", firstInstance)
	}
	secondInstance, err := projection.Initiated(t.Context(), postgres.ActorQuery{
		ActorID: "requester-a",
		Page:    postgres.PageRequest{Limit: projectionMinimumLimit, After: firstInstance.Next},
	})
	if err != nil {
		t.Fatalf("second Initiated() error = %v", err)
	}
	// The second equal-time instance is the final identity and therefore must not expose continuation.
	if len(secondInstance.Items) != projectionMinimumLimit || secondInstance.Items[0].InstanceID != "tie-b" || secondInstance.Next != nil {
		t.Fatalf("second Initiated() page = %#v, want final tie-b", secondInstance)
	}
}

// TestProjectionTaskPaginationBreaksEqualTimesByTaskIdentity verifies both task views page by their final tie-breaker.
//
// t owns one isolated PostgreSQL schema containing equal-time active and completed assignments. Each public task view
// must return every expected TaskID once, preserve ascending identity order, and expose nil Next on its final item.
func TestProjectionTaskPaginationBreaksEqualTimesByTaskIdentity(t *testing.T) {
	dsn := requireIntegrationDSN(t)
	pool := newProjectionPool(t, dsn)
	store := postgres.New(pool)
	equalTime := time.Date(2026, 7, 18, 13, 0, 0, 0, time.UTC) // One instance makes TaskID the only differing order key.
	instance := &workflow.Instance{
		ID: "task-tie-instance",
		Definition: workflow.Definition{
			ID:      "task-tie-definition",
			Version: 1, // A concrete frozen version is required for projection decoding.
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
		Version: 1, // The fixture is inserted once and has no previous durable revision.
	}
	// Persist every assignment in one command transaction so both query views observe the same snapshot.
	if err := store.Create(t.Context(), instance); err != nil {
		t.Fatalf("Create(task tie fixture) error = %v", err)
	}

	// The same paging loop exercises valid continuation cursors returned independently by each task family.
	projection := postgres.NewProjection(pool)
	families := []struct {
		name string
		want []workflow.TaskID
		call func(*postgres.Cursor) (postgres.Page[postgres.TaskProjection], error)
	}{
		{
			name: "worklist",
			want: []workflow.TaskID{"active-a", "active-b", "active-c"},
			call: func(after *postgres.Cursor) (postgres.Page[postgres.TaskProjection], error) {
				return projection.Worklist(t.Context(), postgres.ActorQuery{
					ActorID: "reviewer-a",
					Page:    postgres.PageRequest{Limit: projectionMinimumLimit, After: after},
				})
			},
		},
		{
			name: "participated",
			want: []workflow.TaskID{"completed-a", "completed-b", "completed-c"},
			call: func(after *postgres.Cursor) (postgres.Page[postgres.TaskProjection], error) {
				return projection.Participated(t.Context(), postgres.ActorQuery{
					ActorID: "reviewer-a",
					Page:    postgres.PageRequest{Limit: projectionMinimumLimit, After: after},
				})
			},
		},
	}
	for _, family := range families {
		// This callback owns one family's cursor chain and must stop exactly on its final expected TaskID.
		t.Run(family.name, func(t *testing.T) {
			var after *postgres.Cursor
			got := make([]workflow.TaskID, 0, len(family.want))
			for len(got) < len(family.want) {
				page, err := family.call(after)
				if err != nil {
					t.Fatalf("query error = %v", err)
				}
				if len(page.Items) != projectionMinimumLimit {
					t.Fatalf("page items = %#v, want exactly one item", page.Items)
				}
				got = append(got, page.Items[0].TaskID)
				if len(got) < len(family.want) {
					if page.Next == nil {
						t.Fatalf("page %d Next = nil before final item", len(got))
					}
					// Resume from the exact item just observed so the next assertion detects gaps or duplication.
					after = page.Next
					continue
				}
				if page.Next != nil {
					t.Fatalf("final page Next = %#v, want nil", page.Next)
				}
			}
			if !slices.Equal(got, family.want) {
				t.Fatalf("task order = %v, want %v", got, family.want)
			}
		})
	}
}

// TestProjectionQueryFamiliesApplyDocumentedLimits verifies default, minimum, and maximum sizes against real rows.
//
// t owns an isolated migrated schema and creates maximum-plus-one rows for every public query family. Each subtest
// requires the documented count and a look-ahead cursor; PostgreSQL setup failures stop the test without shared state.
func TestProjectionQueryFamiliesApplyDocumentedLimits(t *testing.T) {
	dsn := requireIntegrationDSN(t)
	pool := newProjectionPool(t, dsn)
	store := postgres.New(pool)
	orderAt := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC) // Equal ordering keys force identity tie-breakers on every page.

	// One instance supplies enough active and completed assignments to exercise both task query views.
	tasks := make([]workflow.Task, 0, projectionLimitFixtureCount*2) // Two task families each need maximum plus look-ahead.
	for index := 0; index < projectionLimitFixtureCount; index++ {
		tasks = append(tasks,
			workflow.Task{
				ID:       workflow.TaskID(fmt.Sprintf("limit-active-%03d", index)), // Fixed width preserves identity order through 200.
				NodeID:   "review",
				Assignee: "limit-reviewer",
				Status:   workflow.TaskStatusActive,
			},
			workflow.Task{
				ID:       workflow.TaskID(fmt.Sprintf("limit-completed-%03d", index)), // Separate prefix prevents task identity overlap.
				NodeID:   "review",
				Assignee: "limit-reviewer",
				Status:   workflow.TaskStatusCompleted,
			},
		)
	}
	primary := &workflow.Instance{
		ID: "limit-instance-000",
		Definition: workflow.Definition{
			ID:      "limit-definition",
			Version: 1, // A concrete frozen version is required for lossless projection decoding.
		},
		Status:        workflow.InstanceStatusRunning,
		Initiator:     "limit-requester",
		CurrentNodeID: "review",
		Tasks:         tasks,
		Audit: []workflow.AuditRecord{{
			Action:  "instance.started",
			NodeID:  "start",
			ActorID: "limit-requester",
			At:      orderAt,
		}},
		Version: 1, // Store optimistic concurrency starts each independent fixture at version one.
	}
	// Persist the task-bearing instance first so every family has its maximum-plus-look-ahead fixture before querying.
	if err := store.Create(t.Context(), primary); err != nil {
		t.Fatalf("Create(primary limit fixture) error = %v", err)
	}

	// Additional taskless instances bring Initiated to maximum plus one without altering either task view.
	for index := 1; index < projectionLimitFixtureCount; index++ {
		instance := &workflow.Instance{
			ID: workflow.InstanceID(fmt.Sprintf("limit-instance-%03d", index)), // Fixed width matches ascending keyset identity order.
			Definition: workflow.Definition{
				ID:      "limit-definition",
				Version: 1, // Every fixture represents the same frozen definition revision.
			},
			Status:        workflow.InstanceStatusRunning,
			Initiator:     "limit-requester",
			CurrentNodeID: "review",
			Audit: []workflow.AuditRecord{{
				Action:  "instance.started",
				NodeID:  "start",
				ActorID: "limit-requester",
				At:      orderAt,
			}},
			Version: 1, // Each instance is inserted once and has no prior durable revision.
		}
		if err := store.Create(t.Context(), instance); err != nil {
			t.Fatalf("Create(limit fixture %d) error = %v", index, err)
		}
	}

	// Exercise each public family with independent spec-derived expected counts and a required look-ahead cursor.
	projection := postgres.NewProjection(pool)
	families := []struct {
		name string
		call func(int) (int, bool, error)
	}{
		{name: "worklist", call: func(limit int) (int, bool, error) {
			page, err := projection.Worklist(t.Context(), postgres.ActorQuery{
				ActorID: "limit-reviewer",
				Page:    postgres.PageRequest{Limit: limit},
			})
			return len(page.Items), page.Next != nil, err
		}},
		{name: "participated", call: func(limit int) (int, bool, error) {
			page, err := projection.Participated(t.Context(), postgres.ActorQuery{
				ActorID: "limit-reviewer",
				Page:    postgres.PageRequest{Limit: limit},
			})
			return len(page.Items), page.Next != nil, err
		}},
		{name: "initiated", call: func(limit int) (int, bool, error) {
			page, err := projection.Initiated(t.Context(), postgres.ActorQuery{
				ActorID: "limit-requester",
				Page:    postgres.PageRequest{Limit: limit},
			})
			return len(page.Items), page.Next != nil, err
		}},
	}
	limits := []struct {
		name    string
		request int
		want    int
	}{
		{name: "default", request: projectionOmittedLimit, want: projectionDefaultLimit},
		{name: "minimum", request: projectionMinimumLimit, want: projectionMinimumLimit},
		{name: "maximum", request: projectionMaximumLimit, want: projectionMaximumLimit},
	}
	for _, family := range families {
		for _, limit := range limits {
			// This callback checks one family and boundary against a fresh first page in the isolated schema.
			t.Run(family.name+"/"+limit.name, func(t *testing.T) {
				count, hasNext, err := family.call(limit.request)
				if err != nil {
					t.Fatalf("query error = %v", err)
				}
				// Every fixture set has one later row, so count and continuation must agree at each boundary.
				if count != limit.want || !hasNext {
					t.Fatalf("query count = %d, hasNext = %t, want %d and true", count, hasNext, limit.want)
				}
			})
		}
	}
}

// TestProjectionWithdrawalClosesCandidatesAsParticipants verifies lifecycle closure remains visible without migration.
func TestProjectionWithdrawalClosesCandidatesAsParticipants(t *testing.T) {
	dsn := requireIntegrationDSN(t)
	pool := newProjectionPool(t, dsn)
	store := postgres.New(pool)
	registry := workflow.NewRegistry()
	if err := registry.Register(approval.Kind, approval.NewHandler()); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	engine := workflow.NewEngine(store, registry)
	instance, err := engine.Start(
		t.Context(),
		projectionApprovalDefinition(t, "leave", []workflow.ActorID{"manager-a"}),
		workflow.StartRequest{ID: "leave-1", Initiator: "employee-a"},
	)
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	policy := withdrawalPolicyFunc(func(context.Context, workflow.ActorID, *workflow.Instance) error { return nil })
	if _, err := engine.Withdraw(t.Context(), workflow.WithdrawRequest{
		InstanceID: instance.ID,
		ActorID:    "employee-a",
	}, policy); err != nil {
		t.Fatalf("Withdraw() error = %v", err)
	}

	// Withdrawal atomically removes the active candidate view and retains the same assignment as participation.
	projection := postgres.NewProjection(pool)
	worklist, err := projection.Worklist(t.Context(), postgres.ActorQuery{ActorID: "manager-a"})
	if err != nil {
		t.Fatalf("Worklist() error = %v", err)
	}
	if len(worklist.Items) != 0 {
		t.Errorf("Worklist() items = %#v, want empty after withdrawal", worklist.Items)
	}
	participated, err := projection.Participated(t.Context(), postgres.ActorQuery{ActorID: "manager-a"})
	if err != nil {
		t.Fatalf("Participated() error = %v", err)
	}
	if len(participated.Items) != 1 || participated.Items[0].TaskStatus != workflow.TaskStatusClosed ||
		participated.Items[0].Relation != postgres.RelationParticipant {
		t.Errorf("Participated() items = %#v, want one closed participant", participated.Items)
	}
}

// TestProjectionReturnKeepsOldRoundAndAddsNewWorklist verifies repeated node entry creates distinct query records.
func TestProjectionReturnKeepsOldRoundAndAddsNewWorklist(t *testing.T) {
	dsn := requireIntegrationDSN(t)
	pool := newProjectionPool(t, dsn)
	store := postgres.New(pool)
	registry := workflow.NewRegistry()
	if err := registry.Register(approval.Kind, approval.NewHandler()); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	engine := workflow.NewEngine(store, registry)
	instance, err := engine.Start(t.Context(), projectionReturnDefinition(t), workflow.StartRequest{
		ID:        "return-1",
		Initiator: "requester-a",
	})
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	firstTaskID := instance.Tasks[0].ID
	instance, err = engine.Handle(t.Context(), workflow.Command{
		InstanceID: instance.ID,
		TaskID:     firstTaskID,
		ActorID:    "reviewer-a",
		Name:       approval.CommandApprove,
	})
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	policy := projectionReturnPolicyFunc(func(context.Context, workflow.ReturnRequest, *workflow.Instance) error { return nil })
	returned, err := engine.Return(t.Context(), workflow.ReturnRequest{
		InstanceID:   instance.ID,
		ActorID:      "operator-a",
		TargetNodeID: "first-review",
		Reason:       "missing evidence",
	}, policy)
	if err != nil {
		t.Fatalf("Return() error = %v", err)
	}

	// The first reviewer's completed old task and fresh active task must coexist under different task identities.
	projection := postgres.NewProjection(pool)
	worklist, err := projection.Worklist(t.Context(), postgres.ActorQuery{ActorID: "reviewer-a"})
	if err != nil {
		t.Fatalf("Worklist() error = %v", err)
	}
	participated, err := projection.Participated(t.Context(), postgres.ActorQuery{ActorID: "reviewer-a"})
	if err != nil {
		t.Fatalf("Participated() error = %v", err)
	}
	if len(worklist.Items) != 1 || worklist.Items[0].TaskID == firstTaskID ||
		worklist.Items[0].TaskID != returned.Tasks[len(returned.Tasks)-1].ID {
		t.Errorf("Worklist() items = %#v, want fresh returned round", worklist.Items)
	}
	if len(participated.Items) != 1 || participated.Items[0].TaskID != firstTaskID ||
		participated.Items[0].TaskStatus != workflow.TaskStatusCompleted {
		t.Errorf("Participated() items = %#v, want completed old round", participated.Items)
	}

	// Return closes the source node's active assignment and retains it as another participant fact.
	secondReviewer, err := projection.Participated(t.Context(), postgres.ActorQuery{ActorID: "reviewer-b"})
	if err != nil {
		t.Fatalf("Participated(reviewer-b) error = %v", err)
	}
	if len(secondReviewer.Items) != 1 || secondReviewer.Items[0].TaskStatus != workflow.TaskStatusClosed {
		t.Errorf("Participated(reviewer-b) items = %#v, want closed source task", secondReviewer.Items)
	}
}

// TestProjectionTransferMovesAssignmentBetweenActors verifies task history and candidate ownership commit together.
func TestProjectionTransferMovesAssignmentBetweenActors(t *testing.T) {
	dsn := requireIntegrationDSN(t)
	pool := newProjectionPool(t, dsn)
	store := postgres.New(pool)
	registry := workflow.NewRegistry()
	if err := registry.Register(approval.Kind, approval.NewHandler()); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	engine := workflow.NewEngine(store, registry)
	instance, err := engine.Start(
		t.Context(),
		projectionApprovalDefinition(t, "transfer-projection", []workflow.ActorID{"reviewer-a"}),
		workflow.StartRequest{ID: "transfer-projection-1", Initiator: "requester-a"},
	)
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	oldTaskID := instance.Tasks[0].ID
	policy := projectionTransferPolicyFunc(func(
		context.Context,
		workflow.TransferRequest,
		workflow.Task,
		*workflow.Instance,
	) error {
		return nil
	})
	transferred, err := engine.Transfer(t.Context(), workflow.TransferRequest{
		InstanceID:  instance.ID,
		TaskID:      oldTaskID,
		ActorID:     "reviewer-a",
		NewAssignee: "reviewer-b",
		Reason:      "projection ownership change",
	}, policy)
	if err != nil {
		t.Fatalf("Transfer() error = %v", err)
	}
	newTaskID := transferred.Tasks[len(transferred.Tasks)-1].ID

	// Reload through the durable Store seam before querying to verify audit and assignment metadata committed together.
	stored, err := store.Load(t.Context(), instance.ID)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	audit := stored.Audit[len(stored.Audit)-1]
	if audit.Action != "task.transferred" || audit.InstanceID != instance.ID || audit.NodeID != "review" ||
		audit.TaskID != oldTaskID || audit.ActorID != "reviewer-a" || audit.PreviousAssignee != "reviewer-a" ||
		audit.NewAssignee != "reviewer-b" || audit.Reason != "projection ownership change" {
		t.Errorf("durable transfer audit = %#v, want complete attribution", audit)
	}

	// The old actor loses its candidate and retains the closed assignment as participation in the same commit.
	projection := postgres.NewProjection(pool)
	oldWorklist, err := projection.Worklist(t.Context(), postgres.ActorQuery{ActorID: "reviewer-a"})
	if err != nil {
		t.Fatalf("Worklist(old actor) error = %v", err)
	}
	oldParticipation, err := projection.Participated(t.Context(), postgres.ActorQuery{ActorID: "reviewer-a"})
	if err != nil {
		t.Fatalf("Participated(old actor) error = %v", err)
	}
	if len(oldWorklist.Items) != 0 {
		t.Errorf("Worklist(old actor) items = %#v, want empty", oldWorklist.Items)
	}
	if len(oldParticipation.Items) != 1 || oldParticipation.Items[0].TaskID != oldTaskID ||
		oldParticipation.Items[0].TaskStatus != workflow.TaskStatusClosed {
		t.Errorf("Participated(old actor) items = %#v, want closed original assignment", oldParticipation.Items)
	}

	// The replacement actor sees only the fresh active assignment, not a rewritten historical task identity.
	newWorklist, err := projection.Worklist(t.Context(), postgres.ActorQuery{ActorID: "reviewer-b"})
	if err != nil {
		t.Fatalf("Worklist(new actor) error = %v", err)
	}
	newParticipation, err := projection.Participated(t.Context(), postgres.ActorQuery{ActorID: "reviewer-b"})
	if err != nil {
		t.Fatalf("Participated(new actor) error = %v", err)
	}
	if len(newWorklist.Items) != 1 || newWorklist.Items[0].TaskID != newTaskID ||
		newWorklist.Items[0].TaskStatus != workflow.TaskStatusActive {
		t.Errorf("Worklist(new actor) items = %#v, want fresh active assignment", newWorklist.Items)
	}
	if len(newParticipation.Items) != 0 {
		t.Errorf("Participated(new actor) items = %#v, want empty", newParticipation.Items)
	}
}

// TestProjectionUsesFrozenDynamicAssignments verifies later directory drift cannot rewrite candidate rows.
func TestProjectionUsesFrozenDynamicAssignments(t *testing.T) {
	dsn := requireIntegrationDSN(t)
	pool := newProjectionPool(t, dsn)
	members := []workflow.ActorID{"finance-a", "finance-b"}
	resolver := projectionRoleResolverFunc(func(context.Context, string, json.RawMessage) ([]workflow.ActorID, error) {
		return slices.Clone(members), nil
	})
	registry := workflow.NewRegistry()
	if err := registry.Register(approval.Kind, approval.NewHandlerWithOrganization(resolver)); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	definition := projectionDynamicDefinition(t)
	if _, err := workflow.NewEngine(postgres.New(pool), registry).Start(t.Context(), definition, workflow.StartRequest{
		ID:        "dynamic-1",
		Initiator: "requester-a",
	}); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	members = []workflow.ActorID{"replacement"}

	// Projection derives candidates from committed Task facts and never re-enters the host directory adapter.
	projection := postgres.NewProjection(pool)
	for _, actor := range []workflow.ActorID{"finance-a", "finance-b"} {
		page, err := projection.Worklist(t.Context(), postgres.ActorQuery{ActorID: actor})
		if err != nil {
			t.Fatalf("Worklist(actor %q) error = %v", actor, err)
		}
		if len(page.Items) != 1 || page.Items[0].ActorID != actor {
			t.Errorf("Worklist(actor %q) items = %#v, want frozen candidate", actor, page.Items)
		}
	}
	replacement, err := projection.Worklist(t.Context(), postgres.ActorQuery{ActorID: "replacement"})
	if err != nil {
		t.Fatalf("Worklist(replacement) error = %v", err)
	}
	if len(replacement.Items) != 0 {
		t.Errorf("Worklist(replacement) items = %#v, want empty", replacement.Items)
	}
}

// TestProjectionRollsBackWithFailedCommandTransaction verifies rejected persistence cannot leak read rows.
func TestProjectionRollsBackWithFailedCommandTransaction(t *testing.T) {
	dsn := requireIntegrationDSN(t)
	pool := newProjectionPool(t, dsn)
	store := postgres.New(pool)
	registry := workflow.NewRegistry()
	if err := registry.Register(approval.Kind, approval.NewHandler()); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	engine := workflow.NewEngine(store, registry)
	instance, err := engine.Start(
		t.Context(),
		projectionApprovalDefinition(t, "atomic", []workflow.ActorID{"reviewer-a"}),
		workflow.StartRequest{ID: "atomic-1", Initiator: "requester-a"},
	)
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	policy := withdrawalPolicyFunc(func(context.Context, workflow.ActorID, *workflow.Instance) error { return nil })
	failingEngine := workflow.NewEngine(failingWithdrawalStore{Store: store}, registry)
	if _, err := failingEngine.Withdraw(t.Context(), workflow.WithdrawRequest{
		InstanceID: instance.ID,
		ActorID:    "requester-a",
	}, policy); err == nil {
		t.Fatal("Withdraw() error = nil, want injected transaction failure")
	}

	// The pre-command candidate remains active and no closed participant row escapes the rolled-back Save.
	projection := postgres.NewProjection(pool)
	worklist, err := projection.Worklist(t.Context(), postgres.ActorQuery{ActorID: "reviewer-a"})
	if err != nil {
		t.Fatalf("Worklist() error = %v", err)
	}
	participated, err := projection.Participated(t.Context(), postgres.ActorQuery{ActorID: "reviewer-a"})
	if err != nil {
		t.Fatalf("Participated() error = %v", err)
	}
	if len(worklist.Items) != 1 || worklist.Items[0].TaskStatus != workflow.TaskStatusActive {
		t.Errorf("Worklist() items = %#v, want original active candidate", worklist.Items)
	}
	if len(participated.Items) != 0 {
		t.Errorf("Participated() items = %#v, want no leaked failed state", participated.Items)
	}
}

// newProjectionPool creates one fully migrated isolated PostgreSQL pool for projection scenarios.
//
// dsn must identify a test database where the caller may create schemas. The returned pool is test-owned and
// closed automatically; setup failures stop the current test before any workflow command executes.
func newProjectionPool(t *testing.T, dsn string) *pgxpool.Pool {
	t.Helper()

	schema := createIsolatedSchema(t, dsn)
	pool := openSchemaPool(t, dsn, schema)
	applyInitialMigration(t, pool)
	return pool
}

// projectionApprovalDefinition builds one static approval graph whose concrete assignees become frozen tasks.
//
// id must be non-empty and assignees must contain unique non-empty actors. Invalid graphs fail the current test;
// the returned definition is caller-owned and has one approval node followed by a successful end node.
func projectionApprovalDefinition(
	t *testing.T,
	id string,
	assignees []workflow.ActorID,
) *workflow.Definition {
	t.Helper()

	builder := workflow.NewBuilder(id)
	builder.Start("start")
	builder.Node("review", approval.Kind, approval.Config{Mode: approval.ModeAny, Assignees: assignees})
	builder.End("end")
	builder.Connect("start", "review", "")
	builder.Connect("review", "end", approval.OutcomeApproved)
	definition, err := builder.Build()
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	return definition
}

// projectionReturnDefinition builds two approval rounds so a running instance can return to the first node.
func projectionReturnDefinition(t *testing.T) *workflow.Definition {
	t.Helper()

	builder := workflow.NewBuilder("return-projection")
	builder.Start("start")
	builder.Node("first-review", approval.Kind, approval.Config{
		Mode:      approval.ModeAny,
		Assignees: []workflow.ActorID{"reviewer-a"},
	})
	builder.Node("second-review", approval.Kind, approval.Config{
		Mode:      approval.ModeAny,
		Assignees: []workflow.ActorID{"reviewer-b"},
	})
	builder.End("end")
	builder.Connect("start", "first-review", "")
	builder.Connect("first-review", "second-review", approval.OutcomeApproved)
	builder.Connect("second-review", "end", approval.OutcomeApproved)
	definition, err := builder.Build()
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	return definition
}

// projectionDynamicDefinition builds one role-assigned approval graph for frozen candidate verification.
func projectionDynamicDefinition(t *testing.T) *workflow.Definition {
	t.Helper()

	builder := workflow.NewBuilder("dynamic-projection")
	builder.Start("start")
	builder.Node("review", approval.Kind, approval.Config{
		Mode:   approval.ModeAny,
		Policy: &approval.AssignmentPolicy{Role: "finance-reviewer"},
	})
	builder.End("end")
	builder.Connect("start", "review", "")
	builder.Connect("review", "end", approval.OutcomeApproved)
	definition, err := builder.Build()
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	return definition
}
