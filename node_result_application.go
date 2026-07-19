// This file owns package-internal validation and factual application of handler NodeResult values.
// It does not invoke handlers, resolve graph routes, persist aggregates, or merge stage-specific task semantics.
// Applications are immutable request-local values and are never serialized, cached, or shared across Engine operations.
package workflow

import (
	"errors"
	"fmt"
	"slices"
)

var (
	// errInvalidActivationTaskDraft classifies handler-created tasks that cannot become durable active assignments.
	errInvalidActivationTaskDraft = errors.New("workflow: invalid activation task draft")
	// errInvalidCommandTaskSet classifies command task views that would rewrite history or violate node liveness.
	errInvalidCommandTaskSet = errors.New("workflow: invalid command task set")
)

// nodeResultStage identifies the Engine boundary that produced a handler result.
//
// Each stage has distinct task semantics: activation creates drafts, commands replace a complete task view, and return
// creates a mandatory fresh waiting round. Values are package-internal and live for one Engine operation.
type nodeResultStage uint8

const (
	// nodeResultActivation validates ordinary node-entry proposals, including synchronous routing with no task drafts.
	nodeResultActivation nodeResultStage = iota + 1
	// nodeResultCommand validates a handler's complete replacement view for every task owned by the current node.
	nodeResultCommand
	// nodeResultReturn validates a mandatory waiting reactivation round for one authorized historical target.
	nodeResultReturn
)

// nodeResultAction tells Engine whether accepted facts wait, route, or terminate without reinterpreting Disposition.
type nodeResultAction uint8

const (
	// nodeResultWait leaves the accepted current node active and ends graph advancement for this operation.
	nodeResultWait nodeResultAction = iota + 1
	// nodeResultAdvance asks Engine to resolve the application's validated outcome through the compiled Definition.
	nodeResultAdvance
	// nodeResultStop ends graph advancement after terminal rejection facts have been applied.
	nodeResultStop
)

// nodeResultDecision is the only post-application navigation signal consumed by Engine.
//
// outcome is meaningful only for nodeResultAdvance. The value owns no mutable data and remains valid only while the
// enclosing request-local compiled plan is executing.
type nodeResultDecision struct {
	// action identifies the next Engine navigation operation.
	action nodeResultAction
	// outcome is the handler-owned selector resolved only by the compiled Definition.
	outcome string
}

// navigation converts an applied decision into Engine's route-or-stop control signal.
//
// The returned outcome is meaningful only when advance is true. Waiting and terminal rejection both return false
// because their factual difference was already applied. Invalid internal actions return ErrInvalidNodeResult.
func (d nodeResultDecision) navigation() (outcome string, advance bool, err error) {
	switch d.action {
	case nodeResultWait, nodeResultStop:
		return "", false, nil
	case nodeResultAdvance:
		return d.outcome, true, nil
	default:
		return "", false, fmt.Errorf("%w: unsupported result navigation action %d", ErrInvalidNodeResult, d.action)
	}
}

// nodeTaskReplacement binds one validated aggregate position to its handler-proposed replacement value.
type nodeTaskReplacement struct {
	// index is the stable position of the current-node task in the candidate's complete historical task slice.
	index int
	// task is the detached replacement whose immutable identity and node ownership already match that position.
	task Task
}

// nodeResultApplication is a fully validated, detached proposal ready for the instanceFacts mutation boundary.
//
// Preparation owns all cross-field and stage rules before audit or transition facts run. Application delegates every
// aggregate mutation to instanceFacts and performs no Store I/O. A value is single-use within one Engine operation.
type nodeResultApplication struct {
	// stage selects the distinct factual task operation without erasing its domain semantics.
	stage nodeResultStage
	// nodeID is the current or return-target node that owns the result.
	nodeID string
	// result is a defensive copy of the handler proposal validated during preparation.
	result NodeResult
	// replacements contains command-only task writes resolved against the pre-transition aggregate.
	replacements []nodeTaskReplacement
}

// prepareActivationNodeResult validates and detaches one ordinary node-entry proposal.
//
// nodeID must identify the compiled business node just entered. result state must be absent or valid JSON; waiting
// outcomes are forbidden, routed results cannot create task drafts, and waiting drafts must satisfy activation task
// rules. Errors wrap ErrInvalidNodeResult and no aggregate facts are changed.
func prepareActivationNodeResult(nodeID string, result NodeResult) (*nodeResultApplication, error) {
	return prepareNodeResultApplication(nodeResultActivation, nil, nodeID, result)
}

// prepareCommandNodeResult validates and detaches one complete current-node task proposal.
//
// instance is the detached pre-transition aggregate and nodeID its compiled current node. Every existing task owned by
// nodeID must appear, no other identity may appear, and duplicate proposed IDs retain the established final-value-wins
// behavior. Errors wrap ErrInvalidNodeResult and leave instance unchanged.
func prepareCommandNodeResult(instance *Instance, nodeID string, result NodeResult) (*nodeResultApplication, error) {
	return prepareNodeResultApplication(nodeResultCommand, instance, nodeID, result)
}

// prepareReturnNodeResult validates and detaches one historical-target reactivation proposal.
//
// nodeID must identify the already authorized target. Only a waiting result with an empty outcome and at least one
// valid task draft is accepted. Errors wrap both ErrInvalidReturnTarget and ErrInvalidNodeResult for compatibility,
// and no return audit or aggregate fact is changed.
func prepareReturnNodeResult(nodeID string, result NodeResult) (*nodeResultApplication, error) {
	return prepareNodeResultApplication(nodeResultReturn, nil, nodeID, result)
}

// prepareNodeResultApplication enforces shared JSON/disposition rules and delegates distinct task semantics by stage.
//
// stage must be one of the three package constants. instance is required only for command results. The returned value
// owns cloned state, tasks, and replacements. All failures occur before instanceFacts receives the proposal.
func prepareNodeResultApplication(
	stage nodeResultStage,
	instance *Instance,
	nodeID string,
	result NodeResult,
) (*nodeResultApplication, error) {
	// Detach handler-owned buffers before validation so later handler mutation cannot alter accepted facts.
	application := &nodeResultApplication{
		stage:  stage,
		nodeID: nodeID,
		result: NodeResult{
			Disposition: result.Disposition,
			Outcome:     result.Outcome,
			State:       slices.Clone(result.State),
			Tasks:       slices.Clone(result.Tasks),
		},
	}

	// Shared fields must be sound before stage-specific task semantics can inspect the detached proposal.
	if err := validateNodeResultFields(application); err != nil {
		return nil, err
	}
	if err := application.prepareTasks(instance); err != nil {
		return nil, err
	}
	return application, nil
}

// validateNodeResultFields enforces node identity, state JSON, disposition, and outcome rules shared by every stage.
//
// application must be a freshly detached proposal. The function performs no aggregate mutation or I/O; every failure
// wraps ErrInvalidNodeResult with stage and node context through invalidNodeResult.
func validateNodeResultFields(application *nodeResultApplication) error {
	if application.nodeID == "" {
		return invalidNodeResult(application.stage, application.nodeID, "node identity is empty")
	}
	if !validJSON(application.result.State) {
		return invalidNodeResult(application.stage, application.nodeID, "state is not valid json")
	}
	switch application.result.Disposition {
	case DispositionWaiting, DispositionContinue, DispositionReject:
		// These are the complete transition decisions understood by the factual application boundary.
	case DispositionUnknown:
		return invalidNodeResult(application.stage, application.nodeID, "disposition is empty")
	default:
		return invalidNodeResult(
			application.stage,
			application.nodeID,
			"disposition %q is unsupported",
			application.result.Disposition,
		)
	}
	// Waiting has no route selector; accepting one would silently discard handler intent.
	if application.result.Disposition == DispositionWaiting && application.result.Outcome != "" {
		return invalidNodeResult(
			application.stage,
			application.nodeID,
			"waiting result has outcome %q",
			application.result.Outcome,
		)
	}
	return nil
}

// prepareTasks validates one proposal's stage-specific task contract and records command replacement positions.
//
// instance is required only for command results and remains unchanged. Activation and return validate task drafts, while
// commands protect the complete durable task view and liveness. Errors wrap ErrInvalidNodeResult with stage and node context.
func (a *nodeResultApplication) prepareTasks(instance *Instance) error {
	// Preserve the three intentionally different task contracts instead of forcing them through one generic merge.
	switch a.stage {
	case nodeResultActivation:
		// A node that routes immediately cannot leave active task drafts behind at a historical node.
		if a.result.Disposition != DispositionWaiting && len(a.result.Tasks) != 0 {
			return invalidNodeResult(a.stage, a.nodeID, "routed activation contains task drafts")
		}
		if err := validateActivationTaskDrafts(a.result.Tasks); err != nil {
			return invalidNodeResult(a.stage, a.nodeID, "%v", err)
		}
	case nodeResultCommand:
		replacements, err := prepareNodeTaskReplacements(instance, a.nodeID, a.result.Tasks)
		if err != nil {
			return invalidNodeResult(a.stage, a.nodeID, "%v", err)
		}
		if err := validateCommandTaskDisposition(a.result.Disposition, replacements); err != nil {
			return invalidNodeResult(a.stage, a.nodeID, "%v", err)
		}
		a.replacements = replacements
	case nodeResultReturn:
		// Explicit return is defined as a fresh actionable round and never as synchronous routing or terminal rejection.
		if a.result.Disposition != DispositionWaiting || len(a.result.Tasks) == 0 {
			return invalidNodeResult(a.stage, a.nodeID, "return target did not create a waiting task round")
		}
		if err := validateActivationTaskDrafts(a.result.Tasks); err != nil {
			return invalidNodeResult(a.stage, a.nodeID, "%v", err)
		}
	default:
		return invalidNodeResult(a.stage, a.nodeID, "application stage %d is unsupported", a.stage)
	}
	return nil
}

// validateActivationTaskDrafts checks handler-created assignments before instanceFacts allocates durable identities.
//
// Every task must omit ID and Outcome, provide a concrete assignee, and start active. NodeID remains ignored for
// compatibility because Engine has always overwritten draft ownership with the compiled node. The function is pure.
func validateActivationTaskDrafts(tasks []Task) error {
	for _, task := range tasks {
		// Draft identity, ownership outcome, assignee, and lifecycle status form one indivisible activation contract.
		if task.ID != "" || task.Assignee == "" || task.Status != TaskStatusActive || task.Outcome != "" {
			return errInvalidActivationTaskDraft
		}
	}
	return nil
}

// prepareNodeTaskReplacements resolves a command's complete node-owned task view without mutating the aggregate.
//
// instance must be non-nil and nodeID non-empty. Proposed duplicate IDs preserve legacy final-value-wins behavior.
// The returned replacements follow aggregate order and own their Task values; errors describe omissions or additions.
func prepareNodeTaskReplacements(
	instance *Instance,
	nodeID string,
	tasks []Task,
) ([]nodeTaskReplacement, error) {
	if instance == nil {
		return nil, fmt.Errorf("%w: command aggregate is nil", errInvalidCommandTaskSet)
	}

	// Index detached proposals by immutable identity while rejecting ownership that could rewrite another node's facts.
	updates := make(map[TaskID]Task, len(tasks))
	for _, task := range tasks {
		// Both a durable identity and exact current-node ownership are required before indexing the replacement.
		if task.ID == "" || task.NodeID != nodeID {
			return nil, fmt.Errorf("%w: replacement task identity is invalid", errInvalidCommandTaskSet)
		}
		updates[task.ID] = task
	}

	// Resolve every current-node aggregate position and consume every proposed identity exactly once.
	replacements := make([]nodeTaskReplacement, 0, len(updates))
	for index := range instance.Tasks {
		if instance.Tasks[index].NodeID != nodeID {
			continue // Other-node tasks remain immutable historical facts for this command.
		}
		updated, exists := updates[instance.Tasks[index].ID]
		if !exists {
			return nil, fmt.Errorf("%w: handler omitted task %q", errInvalidCommandTaskSet, instance.Tasks[index].ID)
		}
		if err := validateCommandTaskReplacement(instance.Tasks[index], updated); err != nil {
			return nil, err
		}
		replacements = append(replacements, nodeTaskReplacement{index: index, task: updated})
		delete(updates, updated.ID)
	}
	if len(updates) != 0 {
		return nil, fmt.Errorf("%w: handler introduced unknown task", errInvalidCommandTaskSet)
	}
	return replacements, nil
}

// validateCommandTaskReplacement protects immutable task facts and allows only forward transitions from active work.
//
// current is one durable current-node task and updated is the handler proposal with the same identity. Historical tasks
// must remain byte-for-byte equivalent. Active tasks retain ownership and may stay active, complete with a concrete
// outcome, or close without fabricating a decision. The function is pure and wraps errInvalidCommandTaskSet on failure.
func validateCommandTaskReplacement(current, updated Task) error {
	// Identity and ownership changes require dedicated Engine operations so their audit facts cannot be bypassed.
	if updated.ID != current.ID || updated.NodeID != current.NodeID || updated.Assignee != current.Assignee {
		return fmt.Errorf("%w: task %q identity or assignee changed", errInvalidCommandTaskSet, current.ID)
	}
	// A non-active task belongs to immutable history and cannot be reopened or have its recorded decision rewritten.
	if current.Status != TaskStatusActive {
		if updated != current {
			return fmt.Errorf("%w: historical task %q changed", errInvalidCommandTaskSet, current.ID)
		}
		return nil
	}

	// Active assignments may only advance within the documented lifecycle without rewriting an earlier outcome.
	switch updated.Status {
	case TaskStatusActive:
		if updated.Outcome != current.Outcome {
			return fmt.Errorf("%w: active task %q outcome changed", errInvalidCommandTaskSet, current.ID)
		}
	case TaskStatusCompleted:
		if updated.Outcome == "" {
			return fmt.Errorf("%w: completed task %q has no outcome", errInvalidCommandTaskSet, current.ID)
		}
	case TaskStatusClosed:
		if updated.Outcome != current.Outcome {
			return fmt.Errorf("%w: closed task %q fabricated an outcome", errInvalidCommandTaskSet, current.ID)
		}
	case TaskStatusUnknown:
		return fmt.Errorf("%w: task %q has an empty status", errInvalidCommandTaskSet, current.ID)
	default:
		return fmt.Errorf("%w: task %q has unsupported status %q", errInvalidCommandTaskSet, current.ID, updated.Status)
	}
	return nil
}

// validateCommandTaskDisposition keeps instance routing and actionable task state mutually consistent.
//
// replacements is the complete validated current-node task view. Waiting requires at least one active assignment because
// task commands are the only resumption seam. Continue and Reject must leave none, preventing stale worklist entries after
// navigation or termination. The function is pure and wraps errInvalidCommandTaskSet on failure.
func validateCommandTaskDisposition(disposition Disposition, replacements []nodeTaskReplacement) error {
	hasActiveTask := slices.ContainsFunc(replacements, func(replacement nodeTaskReplacement) bool {
		return replacement.task.Status == TaskStatusActive
	})
	if disposition == DispositionWaiting && !hasActiveTask {
		return fmt.Errorf("%w: waiting result has no active task", errInvalidCommandTaskSet)
	}
	if disposition != DispositionWaiting && hasActiveTask {
		return fmt.Errorf("%w: routed result retains an active task", errInvalidCommandTaskSet)
	}
	return nil
}

// apply delegates one validated proposal to instanceFacts and returns its normalized navigation decision.
//
// facts must wrap the candidate used during preparation. The method performs no routing or Store I/O. Entropy failure
// while creating activation identities is returned and the enclosing Engine operation discards the private candidate.
func (a *nodeResultApplication) apply(facts *instanceFacts) (nodeResultDecision, error) {
	// Application cannot safely mutate or navigate without both a prepared proposal and a live private candidate.
	if a == nil || facts == nil || facts.candidate() == nil {
		return nodeResultDecision{}, fmt.Errorf("%w: result application is incomplete", ErrInvalidNodeResult)
	}

	// Apply the stage's validated task and state facts through the aggregate's only mutation boundary.
	switch a.stage {
	case nodeResultActivation:
		if err := facts.activateTasks(a.nodeID, a.result.Tasks); err != nil {
			return nodeResultDecision{}, err
		}
		facts.setNodeState(a.result.State)
	case nodeResultCommand:
		facts.applyNodeTaskReplacements(a.replacements)
		facts.setNodeState(a.result.State)
	case nodeResultReturn:
		if err := facts.returnTo(a.nodeID, a.result); err != nil {
			return nodeResultDecision{}, err
		}
		return nodeResultDecision{action: nodeResultWait}, nil
	default:
		return nodeResultDecision{}, fmt.Errorf("%w: unsupported result application stage %d", ErrInvalidNodeResult, a.stage)
	}

	// Normalize Disposition once so Engine never reinterprets cross-field result rules.
	switch a.result.Disposition {
	case DispositionUnknown:
		return nodeResultDecision{}, fmt.Errorf(
			"%w: Disposition Unknown node %q has disposition %q",
			ErrInvalidNodeResult,
			a.nodeID,
			a.result.Disposition,
		)
	case DispositionWaiting:
		return nodeResultDecision{action: nodeResultWait}, nil
	case DispositionContinue:
		return nodeResultDecision{action: nodeResultAdvance, outcome: a.result.Outcome}, nil
	case DispositionReject:
		if a.result.Outcome != "" {
			facts.rejectNode()
			return nodeResultDecision{action: nodeResultAdvance, outcome: a.result.Outcome}, nil
		}
		facts.rejectInstance()
		return nodeResultDecision{action: nodeResultStop}, nil
	default:
		return nodeResultDecision{}, fmt.Errorf(
			"%w: validated node %q has disposition %q",
			ErrInvalidNodeResult,
			a.nodeID,
			a.result.Disposition,
		)
	}
}

// invalidNodeResult builds the stable error chain for one stage-specific malformed handler proposal.
//
// format and args describe the violated invariant without exposing internal types. Return-stage failures additionally
// wrap ErrInvalidReturnTarget so existing lifecycle callers retain their documented classification.
func invalidNodeResult(stage nodeResultStage, nodeID string, format string, args ...any) error {
	detail := fmt.Sprintf(format, args...)
	resultErr := fmt.Errorf("%w: node %q %s", ErrInvalidNodeResult, nodeID, detail)
	if stage == nodeResultReturn {
		return fmt.Errorf("%w: %w", ErrInvalidReturnTarget, resultErr)
	}
	return resultErr
}
