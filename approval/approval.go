// Package approval provides the official human-approval node for the workflow core.
// It owns or-sign and countersign decisions but delegates graph transitions, persistence, and audit to Engine.
package approval

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	workflow "github.com/lvpeng/easy-workflow"
)

const (
	// Kind is the stable registry and JSON name of the official approval node.
	Kind = "approval"
	// CommandApprove records an assignee's affirmative decision.
	CommandApprove = "approve"
	// CommandReject records an assignee's rejection, which may terminate or follow an explicit Definition edge.
	CommandReject = "reject"
	// OutcomeApproved selects the successful outgoing edge after an approval node passes.
	OutcomeApproved = "approved"
	// OutcomeRejected records the decision and is the only configurable rejection-route outcome.
	OutcomeRejected = "rejected"
)

var (
	// ErrInvalidCommand means an approval command name is unsupported by the active handler.
	ErrInvalidCommand = errors.New("approval: invalid command")
	// ErrTaskNotActive means a command does not target an active task owned by its declared actor.
	ErrTaskNotActive = errors.New("approval: task is not active for actor")
	// ErrInvalidConfig means approval mode, assignment source, or rejection outcome cannot be activated safely.
	ErrInvalidConfig = errors.New("approval: invalid config")
	// ErrNoAssignees means assignment produced no concrete actor for an approval round.
	ErrNoAssignees = errors.New("approval: no assignees")
	// ErrInvalidAssignee means assignment produced an empty actor identity.
	ErrInvalidAssignee = errors.New("approval: invalid assignee")
	// ErrDuplicateAssignee means one resolved or static actor appears more than once in an approval round.
	ErrDuplicateAssignee = errors.New("approval: duplicate assignee")
	// ErrAssignmentResolution means a host organization adapter could not resolve a dynamic policy.
	ErrAssignmentResolution = errors.New("approval: assignment resolution failed")
	// ErrOrganizationAdapterRequired means a dynamic policy activated without an explicit host adapter.
	ErrOrganizationAdapterRequired = errors.New("approval: organization adapter required")
)

// Mode defines how affirmative decisions satisfy an approval node.
type Mode string

const (
	// ModeAny implements or-sign: the first valid decision closes every sibling task.
	ModeAny Mode = "any"
	// ModeAll implements countersign: every frozen assignee must approve, while one rejection ends immediately.
	ModeAll Mode = "all"
)

// Config is the JSON configuration owned by the official approval handler.
//
// Exactly one assignment source is required: Assignees supplies a non-empty unique static set, while Policy asks
// the host adapter to resolve concrete actors during activation. RejectedOutcome may be empty for terminal
// rejection or OutcomeRejected to require an explicit Definition edge.
type Config struct {
	Mode      Mode               `json:"mode"`
	Assignees []workflow.ActorID `json:"assignees,omitempty"`
	// Policy selects one host-resolved assignment source instead of explicit assignees.
	Policy *AssignmentPolicy `json:"assignmentPolicy,omitempty"`
	// RejectedOutcome is empty for terminal rejection or OutcomeRejected to require graph routing.
	RejectedOutcome string `json:"rejectedOutcome,omitempty"`
}

// AssignmentPolicy is the serialized dynamic assignment contract owned by Approval.
//
// Role names one host-defined organization role. Approval treats it as an opaque stable selector and delegates
// membership lookup to OrganizationAdapter when the node activates. Exactly one supported selector is required.
type AssignmentPolicy struct {
	// Role is the non-empty host-defined role selector resolved at activation time.
	Role string `json:"role"`
}

// OrganizationAdapter resolves Approval policies against a host-owned organization directory.
//
// Implementations own directory connectivity, tenancy, caching, and identity mapping. They must return concrete
// workflow ActorID values, honor cancellation for blocking work, and be safe for the host's Engine concurrency
// model. Approval never retains or mutates the returned slice.
type OrganizationAdapter interface {
	// ResolveRole returns the current concrete actors for one host-defined role.
	//
	// role is non-empty and opaque to Approval. data is a caller-owned, optional JSON value from the workflow start
	// request and must not be retained or mutated. A successful result must contain non-empty unique ActorID values;
	// Approval validates and copies them into task drafts. Any error aborts node activation without fallback or tasks.
	ResolveRole(ctx context.Context, role string, data json.RawMessage) ([]workflow.ActorID, error)
}

// Handler implements the official approval node with an optional host-provided organization adapter.
// It retains no instance-specific state and is safe for concurrent use when its adapter is also safe.
type Handler struct {
	// organization is nil for static-only handlers and is consulted only by dynamic policies during activation.
	organization OrganizationAdapter
}

var _ workflow.NodeHandler = (*Handler)(nil)

// NewHandler creates the stateless official approval handler.
func NewHandler() *Handler {
	return &Handler{}
}

// NewHandlerWithOrganization creates an Approval handler backed by an explicit host organization adapter.
//
// adapter resolves dynamic role policies during activation. The constructor performs no lookup or other external
// I/O; a nil adapter is retained and only affects dynamic configurations when they are activated.
func NewHandlerWithOrganization(adapter OrganizationAdapter) *Handler {
	return &Handler{organization: adapter}
}

// Validate rejects malformed mode, assignment source, or rejection outcome before an instance starts.
func (h *Handler) Validate(config json.RawMessage) error {
	_, err := parseConfig(config)
	return err
}

// Activate resolves and freezes configured assignees into one active task per actor and waits for decisions.
//
// Static assignees require no adapter. A dynamic role policy is resolved exactly once through the host adapter
// using the caller's context and opaque business data. Returned task drafts contain concrete ActorID values only;
// errors return no tasks, and the handler retains neither directory results nor activation input. ctx must remain
// active through resolution; input.Config must pass Validate and input.Data may be absent or valid opaque JSON.
func (h *Handler) Activate(ctx context.Context, input workflow.ActivationInput) (workflow.NodeResult, error) {
	// An abandoned activation cannot perform a directory lookup or propose tasks.
	if err := ctx.Err(); err != nil {
		return workflow.NodeResult{}, fmt.Errorf("approval: activate: %w", err)
	}
	// Re-validate persisted configuration before choosing its static or dynamic assignment source.
	config, err := parseConfig(input.Config)
	if err != nil {
		return workflow.NodeResult{}, err
	}

	// Resolve the selected assignment source before constructing any task draft.
	assignees := config.Assignees
	if config.Policy != nil {
		// Dynamic policies never fall back to a global or implicit organization directory.
		if h == nil || h.organization == nil {
			return workflow.NodeResult{}, fmt.Errorf("%w: %w", ErrInvalidConfig, ErrOrganizationAdapterRequired)
		}
		// The adapter receives detached business data so it cannot mutate Engine-owned activation input.
		assignees, err = h.organization.ResolveRole(ctx, config.Policy.Role, slices.Clone(input.Data))
		// Preserve the host cause under one stable Approval classification.
		if err != nil {
			return workflow.NodeResult{}, fmt.Errorf("%w: resolve role %q: %w", ErrAssignmentResolution, config.Policy.Role, err)
		}
		// Validate the full population before constructing the first task draft.
		if err := validateAssignees(assignees); err != nil {
			return workflow.NodeResult{}, err
		}
	}

	// Task drafts intentionally omit IDs and node ownership; Engine supplies both before persistence.
	tasks := make([]workflow.Task, 0, len(assignees))
	for _, assignee := range assignees {
		tasks = append(tasks, workflow.Task{Assignee: assignee, Status: workflow.TaskStatusActive})
	}
	return workflow.NodeResult{Disposition: workflow.DispositionWaiting, Tasks: tasks}, nil
}

// Handle applies one approve or reject command to the frozen task set.
//
// The command actor must own the active task. Or-sign completes on the first decision; countersign waits
// for every approval but rejects immediately. RejectedOutcome is either empty for terminal behavior or the
// fixed OutcomeRejected selector; Engine alone resolves that selector to a Definition edge target. Returned
// tasks are detached from input.Tasks.
func (h *Handler) Handle(ctx context.Context, input workflow.CommandInput) (workflow.NodeResult, error) {
	// Cancellation prevents an abandoned actor command from proposing any state transition.
	if err := ctx.Err(); err != nil {
		return workflow.NodeResult{}, fmt.Errorf("approval: handle: %w", err)
	}
	// Re-validate frozen configuration because persisted instances may outlive the publishing process.
	config, err := parseConfig(input.Config)
	if err != nil {
		return workflow.NodeResult{}, err
	}

	// Resolve the command task only within the defensive node-owned task snapshot supplied by Engine.
	tasks := slices.Clone(input.Tasks)
	taskIndex := -1
	for i := range tasks {
		if tasks[i].ID == input.TaskID {
			taskIndex = i
			break
		}
	}
	// Identity, ownership, and active status must all hold before the actor can change the task set.
	if taskIndex < 0 || tasks[taskIndex].Assignee != input.ActorID || tasks[taskIndex].Status != workflow.TaskStatusActive {
		return workflow.NodeResult{}, ErrTaskNotActive
	}

	// Apply the actor's decision before calculating whether sibling tasks must remain active or close.
	switch input.Name {
	case CommandApprove:
		tasks[taskIndex].Status = workflow.TaskStatusCompleted
		tasks[taskIndex].Outcome = OutcomeApproved
		if config.Mode == ModeAll && hasActiveTask(tasks) {
			return workflow.NodeResult{Disposition: workflow.DispositionWaiting, Tasks: tasks}, nil
		}
		closeActiveTasks(tasks)
		return workflow.NodeResult{
			Disposition: workflow.DispositionContinue,
			Outcome:     OutcomeApproved,
			Tasks:       tasks,
		}, nil
	case CommandReject:
		tasks[taskIndex].Status = workflow.TaskStatusCompleted
		tasks[taskIndex].Outcome = OutcomeRejected
		closeActiveTasks(tasks)
		return workflow.NodeResult{
			Disposition: workflow.DispositionReject,
			Outcome:     config.RejectedOutcome,
			Tasks:       tasks,
		}, nil
	default:
		return workflow.NodeResult{}, fmt.Errorf("%w: unsupported command %q", ErrInvalidCommand, input.Name)
	}
}

// parseConfig decodes and validates the complete approval configuration used by publication and execution.
//
// data must contain one Config JSON value with ModeAny or ModeAll, exactly one valid static or dynamic assignment
// source, and either no rejected outcome or OutcomeRejected. The returned Config owns its decoded assignee slice
// and policy. Errors retain JSON syntax causes or wrap ErrInvalidConfig; the function performs no I/O and is safe
// for concurrent calls.
func parseConfig(data json.RawMessage) (Config, error) {
	// Decode into fresh storage so handler calls never retain or mutate caller-owned JSON bytes.
	var config Config
	if err := json.Unmarshal(data, &config); err != nil {
		return Config{}, fmt.Errorf("approval: parse config: %w", err)
	}
	// Only the two documented decision policies have complete runtime semantics.
	if config.Mode != ModeAny && config.Mode != ModeAll {
		return Config{}, fmt.Errorf("%w: unsupported mode %q", ErrInvalidConfig, config.Mode)
	}
	// Approval exposes one stable rejection selector and never accepts a target node or arbitrary outcome.
	if config.RejectedOutcome != "" && config.RejectedOutcome != OutcomeRejected {
		return Config{}, fmt.Errorf("%w: unsupported rejected outcome %q", ErrInvalidConfig, config.RejectedOutcome)
	}
	// Exactly one source avoids ambiguous union or precedence semantics between static and dynamic assignment.
	if (len(config.Assignees) == 0) == (config.Policy == nil) {
		return Config{}, fmt.Errorf("%w: exactly one of assignees or assignment policy is required", ErrInvalidConfig)
	}
	if config.Policy != nil {
		// Role is the first supported policy selector and remains opaque to Approval validation.
		if config.Policy.Role == "" {
			return Config{}, fmt.Errorf("%w: assignment policy role is empty", ErrInvalidConfig)
		}
		return config, nil
	}
	// Static and resolved identities share one validation contract.
	if err := validateAssignees(config.Assignees); err != nil {
		return Config{}, err
	}
	return config, nil
}

// validateAssignees requires a non-empty set of unique concrete workflow identities.
//
// assignees may come from static JSON or a host adapter. Empty and duplicate identities wrap ErrInvalidConfig;
// validation is read-only, performs no I/O, and does not retain the caller-owned slice.
func validateAssignees(assignees []workflow.ActorID) error {
	// At least one frozen actor is required or the approval could never receive a valid command.
	if len(assignees) == 0 {
		return fmt.Errorf("%w: %w", ErrInvalidConfig, ErrNoAssignees)
	}

	// Reject duplicate actors because one person must never contribute multiple decisions to a frozen round.
	seen := make(map[workflow.ActorID]struct{}, len(assignees))
	for _, assignee := range assignees {
		// Empty actor identity cannot participate in task ownership checks.
		if assignee == "" {
			return fmt.Errorf("%w: %w", ErrInvalidConfig, ErrInvalidAssignee)
		}
		// Duplicate actors would let one person contribute more than one countersign decision.
		if _, exists := seen[assignee]; exists {
			return fmt.Errorf("%w: %w %q", ErrInvalidConfig, ErrDuplicateAssignee, assignee)
		}
		seen[assignee] = struct{}{}
	}
	return nil
}

// hasActiveTask reports whether countersign still waits for any frozen assignee.
func hasActiveTask(tasks []workflow.Task) bool {
	for _, task := range tasks {
		if task.Status == workflow.TaskStatusActive {
			return true
		}
	}
	return false
}

// closeActiveTasks marks every undecided sibling as closed while preserving completed decisions.
func closeActiveTasks(tasks []workflow.Task) {
	for i := range tasks {
		if tasks[i].Status == workflow.TaskStatusActive {
			tasks[i].Status = workflow.TaskStatusClosed
		}
	}
}
