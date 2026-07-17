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
	// CommandReject records an assignee's terminal rejection.
	CommandReject = "reject"
	// OutcomeApproved selects the successful outgoing edge after an approval node passes.
	OutcomeApproved = "approved"
	// OutcomeRejected is stored on the deciding task when an approval rejects the instance.
	OutcomeRejected = "rejected"
)

var (
	// ErrInvalidCommand means an approval command name is unsupported by the active handler.
	ErrInvalidCommand = errors.New("approval: invalid command")
	// ErrTaskNotActive means a command does not target an active task owned by its declared actor.
	ErrTaskNotActive = errors.New("approval: task is not active for actor")
	// ErrInvalidConfig means approval mode or assignee configuration cannot be activated safely.
	ErrInvalidConfig = errors.New("approval: invalid config")
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
// Assignees are frozen when the node activates. They must be non-empty and unique. A later resolver-based
// configuration can produce this same concrete list before activation without changing approval semantics.
type Config struct {
	Mode      Mode               `json:"mode"`
	Assignees []workflow.ActorID `json:"assignees"`
}

// Handler implements the official approval node without retaining instance-specific state.
// It is stateless and safe for concurrent use across workflow instances.
type Handler struct{}

var _ workflow.NodeHandler = (*Handler)(nil)

// NewHandler creates the stateless official approval handler.
func NewHandler() *Handler {
	return &Handler{}
}

// Validate rejects malformed mode and assignee configuration before an instance starts.
func (h *Handler) Validate(config json.RawMessage) error {
	_, err := parseConfig(config)
	return err
}

// Activate freezes configured assignees into one active task per actor and waits for decisions.
func (h *Handler) Activate(ctx context.Context, input workflow.ActivationInput) (workflow.NodeResult, error) {
	if err := ctx.Err(); err != nil {
		return workflow.NodeResult{}, fmt.Errorf("approval: activate: %w", err)
	}
	config, err := parseConfig(input.Config)
	if err != nil {
		return workflow.NodeResult{}, err
	}

	// Task drafts intentionally omit IDs and node ownership; Engine supplies both before persistence.
	tasks := make([]workflow.Task, 0, len(config.Assignees))
	for _, assignee := range config.Assignees {
		tasks = append(tasks, workflow.Task{Assignee: assignee, Status: workflow.TaskStatusActive})
	}
	return workflow.NodeResult{Disposition: workflow.DispositionWaiting, Tasks: tasks}, nil
}

// Handle applies one approve or reject command to the frozen task set.
//
// The command actor must own the active task. Or-sign completes on the first decision; countersign waits
// for every approval but rejects immediately. Returned tasks are detached from input.Tasks.
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
		return workflow.NodeResult{Disposition: workflow.DispositionReject, Tasks: tasks}, nil
	default:
		return workflow.NodeResult{}, fmt.Errorf("%w: unsupported command %q", ErrInvalidCommand, input.Name)
	}
}

// parseConfig decodes and validates the complete approval configuration used by publication and execution.
//
// data must contain one Config JSON value with ModeAny or ModeAll and a non-empty set of unique, non-empty
// assignees. The returned Config owns its decoded assignee slice. Errors retain JSON syntax causes or wrap
// ErrInvalidConfig; the function performs no I/O and is safe for concurrent calls.
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
	// At least one frozen actor is required or the approval could never receive a valid command.
	if len(config.Assignees) == 0 {
		return Config{}, fmt.Errorf("%w: assignees are empty", ErrInvalidConfig)
	}

	// Reject duplicate actors because one person must never contribute multiple decisions to a frozen round.
	seen := make(map[workflow.ActorID]struct{}, len(config.Assignees))
	for _, assignee := range config.Assignees {
		// Empty actor identity cannot participate in task ownership checks.
		if assignee == "" {
			return Config{}, fmt.Errorf("%w: assignee is empty", ErrInvalidConfig)
		}
		// Duplicate actors would let one person contribute more than one countersign decision.
		if _, exists := seen[assignee]; exists {
			return Config{}, fmt.Errorf("%w: duplicate assignee %q", ErrInvalidConfig, assignee)
		}
		seen[assignee] = struct{}{}
	}
	return config, nil
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
