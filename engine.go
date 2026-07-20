// This file coordinates graph navigation, handlers, policies, and optimistic persistence for workflow commands.
// Business decisions stay in handlers; accepted aggregate changes pass through the internal instance-facts boundary.
package workflow

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/lvpeng/easy-workflow/internal/nilguard"
)

var (
	// ErrInvalidEngine means an operation cannot run because required Engine collaborators are absent.
	ErrInvalidEngine = errors.New("workflow: invalid engine")
	// ErrInvalidStartRequest means instance identity, initiator, business data, or published identity is invalid.
	ErrInvalidStartRequest = errors.New("workflow: invalid start request")
	// ErrInvalidCommand means a command violates the active instance, task, or handler contract.
	ErrInvalidCommand = errors.New("workflow: invalid command")
	// ErrInvalidWithdrawRequest means withdrawal lacks identity or an explicit host policy.
	ErrInvalidWithdrawRequest = errors.New("workflow: invalid withdraw request")
	// ErrInstanceNotRunning means a lifecycle operation targeted an already terminal instance.
	ErrInstanceNotRunning = errors.New("workflow: instance is not running")
	// ErrInvalidReturnRequest means return lacks identity, an explicit target, a reason, or host policy.
	ErrInvalidReturnRequest = errors.New("workflow: invalid return request")
	// ErrInvalidReturnTarget means return selected a control, current, absent, unvisited, or non-task node.
	ErrInvalidReturnTarget = errors.New("workflow: invalid return target")
	// ErrInvalidTransferRequest means transfer lacks identity, a reason, a replacement owner, or host policy.
	ErrInvalidTransferRequest = errors.New("workflow: invalid transfer request")
	// ErrTaskNotTransferable means transfer selected a missing, historical, inactive, or non-current assignment.
	ErrTaskNotTransferable = errors.New("workflow: task is not transferable")
	// ErrInvalidNodeResult means a handler returned state the engine cannot safely persist.
	ErrInvalidNodeResult = errors.New("workflow: invalid node result")
)

// Engine coordinates validated definitions, node handlers, storage, and audit records.
//
// Engine is safe for concurrent use when its Store and handlers honor their concurrency contracts. It
// owns no mutable instance cache; every command loads and conditionally saves one durable snapshot.
type Engine struct {
	store    Store
	registry *Registry
}

// instanceCommand defines the invariant execution phases shared by task and lifecycle commands.
//
// prepare validates operation-specific state and authorization against a defensive snapshot. audit asks the fact
// boundary to append its causal record before transition adds related changes. executeInstanceCommand owns loading,
// running-state enforcement, version advancement, CAS persistence, and detached result ownership.
type instanceCommand struct {
	// name identifies the operation in load and persistence error context.
	name string
	// instanceID selects the aggregate loaded exactly once for this command attempt.
	instanceID InstanceID
	// nonRunningError classifies terminal-state rejection for the public operation.
	nonRunningError error
	// prepare validates and authorizes against a detached pre-transition snapshot.
	prepare func(*Instance) error
	// audit appends the immutable causal record through the fact boundary before transition mutation.
	audit func(*instanceFacts)
	// transition applies operation-specific aggregate changes after audit append.
	transition func(*instanceFacts) error
}

// NewEngine constructs an engine with explicit persistence and handler dependencies.
//
// store and registry must be non-nil. Invalid dependencies are detected by Start or Handle and returned
// as errors rather than causing package initialization side effects.
func NewEngine(store Store, registry *Registry) *Engine {
	return &Engine{store: store, registry: registry}
}

// Start validates and freezes definition, enters its start path, and atomically creates one instance.
//
// request.ID and Initiator must be non-empty; Data must be absent or valid JSON. Handler activation runs
// before persistence and may observe context cancellation. The returned snapshot is detached from Store.
func (e *Engine) Start(ctx context.Context, definition *Definition, request StartRequest) (*Instance, error) {
	// Required collaborators must exist before validation can resolve handlers or persist the new aggregate.
	if e == nil || nilguard.IsNil(e.store) || e.registry == nil {
		return nil, fmt.Errorf("%w: start dependencies are nil", ErrInvalidEngine)
	}
	// External identity and JSON syntax are checked before potentially expensive Definition compilation.
	if request.ID == "" || request.Initiator == "" || !validJSON(request.Data) {
		return nil, fmt.Errorf("%w: id, initiator, or data is invalid", ErrInvalidStartRequest)
	}
	// Compilation freezes a trusted execution plan before any instance state becomes durable.
	plan, err := compileDefinition(definition, e.registry)
	if err != nil {
		return nil, err
	}

	// Use the compiler-owned snapshot so caller mutation cannot invalidate the execution plan or new instance.
	start, err := plan.startNode()
	if err != nil {
		return nil, err
	}
	facts := startInstanceFacts(plan.definition, request, start.ID)

	// Start nodes are control-only, so execution immediately follows their unconditional outgoing edge.
	if err := e.advance(ctx, facts, plan, ""); err != nil {
		return nil, err
	}
	if err := e.store.Create(ctx, facts.candidate()); err != nil {
		return nil, fmt.Errorf("workflow: persist new instance: %w", err)
	}
	return cloneInstance(facts.candidate()), nil
}

// StartPublished resolves one exact immutable Definition version and starts an instance from its snapshot.
//
// definitions must be a non-nil reader; definitionID must be non-empty and version must be positive. The
// reader's not-found and context errors remain in the returned error chain. Startup then follows Start's
// validation, handler, persistence, and ownership contract. No latest-version fallback is attempted.
func (e *Engine) StartPublished(
	ctx context.Context,
	definitions DefinitionReader,
	definitionID string,
	version uint64,
	request StartRequest,
) (*Instance, error) {
	// Exact-version startup requires a complete repository identity and never infers missing values.
	if nilguard.IsNil(definitions) || definitionID == "" || version == 0 {
		return nil, fmt.Errorf("%w: definition reader or identity is invalid", ErrInvalidStartRequest)
	}

	// Resolve through the immutable repository seam before Start freezes another copy into Instance storage.
	definition, err := definitions.Load(ctx, definitionID, version)
	// Preserve repository sentinels such as ErrDefinitionNotFound for caller error dispatch.
	if err != nil {
		return nil, fmt.Errorf("workflow: start published definition %q version %d: %w", definitionID, version, err)
	}
	return e.Start(ctx, definition, request)
}

// Handle processes one actor command against the current node and commits it with optimistic concurrency.
//
// The task must belong to the current node and actor. Handler errors leave durable state unchanged. A stale
// concurrent command returns ErrVersionConflict from Store; callers may reload before deciding whether to retry.
func (e *Engine) Handle(ctx context.Context, command Command) (*Instance, error) {
	// Reject dependency and boundary-input errors before loading or mutating durable instance state.
	if err := e.validateCommand(command); err != nil {
		return nil, err
	}

	// Prepare handler output from a detached snapshot, then let the shared command skeleton commit it once.
	var plan *compiledDefinition
	var node *NodeDefinition
	var application *nodeResultApplication
	return e.executeInstanceCommand(ctx, instanceCommand{
		name:            "task command",
		instanceID:      command.InstanceID,
		nonRunningError: ErrInvalidCommand,
		prepare: func(snapshot *Instance) error {
			// Authorize against durable current-node facts before any handler-owned compilation or execution runs.
			tasks := nodeTasks(snapshot.Tasks, snapshot.CurrentNodeID)
			if err := validateCommandTask(tasks, command); err != nil {
				return err
			}
			var err error
			plan, err = compileDefinition(&snapshot.Definition, e.registry)
			if err != nil {
				return err
			}
			node, err = plan.node(snapshot.CurrentNodeID)
			if err != nil {
				return err
			}
			handler, err := plan.preparedHandler(node.ID)
			if err != nil {
				return err
			}
			result, err := handler.HandlePrepared(ctx, PreparedCommandInput{
				Command: command,
				Data:    slices.Clone(snapshot.Data),
				State:   slices.Clone(snapshot.NodeState),
				Tasks:   tasks,
			})
			if err != nil {
				return fmt.Errorf("workflow: handle node %q command: %w", node.ID, err)
			}
			application, err = prepareCommandNodeResult(snapshot, node.ID, result)
			return err
		},
		audit: func(facts *instanceFacts) {
			facts.recordTaskCommand(command.Name, node.ID, command.TaskID, command.ActorID)
		},
		transition: func(facts *instanceFacts) error {
			decision, err := application.apply(facts)
			if err != nil {
				return err
			}
			outcome, advance, err := decision.navigation()
			if err != nil || !advance {
				return err
			}
			return e.advance(ctx, facts, plan, outcome)
		},
	})
}

// validateCommandTask authorizes one command against the complete current-node task view.
//
// tasks must contain only tasks owned by the active node. The command is accepted only when its identity resolves to an
// active task assigned to the actor. The function performs no I/O, retains no input, and wraps ErrInvalidCommand for
// unknown, inactive, or foreign-owned tasks without exposing handler-specific policy.
func validateCommandTask(tasks []Task, command Command) error {
	for _, task := range tasks {
		if task.ID != command.TaskID {
			continue
		}
		if task.Status != TaskStatusActive || task.Assignee != command.ActorID {
			return fmt.Errorf("%w: task %q is not active for actor", ErrInvalidCommand, command.TaskID)
		}
		return nil
	}
	return fmt.Errorf("%w: task %q does not belong to the current node", ErrInvalidCommand, command.TaskID)
}

// Withdraw authorizes and atomically ends one running instance without executing its active node handler.
//
// request identities and policy must be non-empty, and ActorID must originate from trusted host context. Policy
// receives a defensive pre-transition snapshot and may deny with any host-owned error. A successful withdrawal
// closes every active task, appends one actor-attributed audit record, increments Version once, and commits the
// full aggregate through Store.Save CAS. Load, policy, cancellation, terminal-state, and CAS errors leave durable
// state unchanged; returned snapshots are detached from Store ownership.
func (e *Engine) Withdraw(
	ctx context.Context,
	request WithdrawRequest,
	policy WithdrawalPolicy,
) (*Instance, error) {
	// Withdrawal needs persistence and complete trusted identities before any durable state is loaded.
	if e == nil || nilguard.IsNil(e.store) {
		return nil, fmt.Errorf("%w: withdraw store is nil", ErrInvalidEngine)
	}
	if request.InstanceID == "" || request.ActorID == "" || nilguard.IsNil(policy) {
		return nil, fmt.Errorf("%w: instance, actor, or policy is empty", ErrInvalidWithdrawRequest)
	}

	// Supply only withdrawal-specific policy, audit, and mutation to the shared command execution skeleton.
	return e.executeInstanceCommand(ctx, instanceCommand{
		name:            "withdrawal",
		instanceID:      request.InstanceID,
		nonRunningError: ErrInstanceNotRunning,
		prepare: func(snapshot *Instance) error {
			if err := policy.AuthorizeWithdrawal(ctx, request.ActorID, snapshot); err != nil {
				return fmt.Errorf("workflow: authorize withdrawal: %w", err)
			}
			return nil
		},
		audit: func(facts *instanceFacts) {
			facts.recordWithdrawal(request.ActorID)
		},
		transition: func(facts *instanceFacts) error {
			facts.withdraw()
			return nil
		},
	})
}

// Return authorizes and atomically moves one running instance to an explicit previously entered task node.
//
// request identities, target, non-blank reason, and policy are required. Engine validates the frozen Definition,
// rejects start, end, current, absent, and unvisited targets, then asks host policy to authorize a defensive
// pre-transition snapshot. Success closes active tasks at the source, preserves historical tasks and audit,
// activates a fresh waiting task round at the target, increments Version once, and commits through Store.Save
// CAS. Every error leaves durable state unchanged; returned snapshots are detached from Store ownership.
func (e *Engine) Return(
	ctx context.Context,
	request ReturnRequest,
	policy ReturnPolicy,
) (*Instance, error) {
	// Return needs the registry to validate and activate the explicit target through its registered handler.
	if e == nil || nilguard.IsNil(e.store) || e.registry == nil {
		return nil, fmt.Errorf("%w: return dependencies are nil", ErrInvalidEngine)
	}
	if request.InstanceID == "" || request.ActorID == "" || request.TargetNodeID == "" ||
		strings.TrimSpace(request.Reason) == "" || nilguard.IsNil(policy) {
		return nil, fmt.Errorf("%w: instance, actor, target, reason, or policy is empty", ErrInvalidReturnRequest)
	}

	// Prepare the exact historical target and task drafts before the shared skeleton mutates the loaded aggregate.
	var target *NodeDefinition
	var application *nodeResultApplication
	return e.executeInstanceCommand(ctx, instanceCommand{
		name:            "return",
		instanceID:      request.InstanceID,
		nonRunningError: ErrInstanceNotRunning,
		prepare: func(snapshot *Instance) error {
			var err error
			target, application, err = e.prepareReturn(ctx, request, policy, snapshot)
			return err
		},
		audit: func(facts *instanceFacts) {
			facts.recordReturn(target.ID, request.ActorID, request.Reason)
		},
		transition: func(facts *instanceFacts) error {
			_, err := application.apply(facts)
			return err
		},
	})
}

// Transfer authorizes and atomically replaces one active task assignment without rewriting its history.
//
// request identities, non-blank reason, and policy are required. Engine accepts only an active task owned by the
// current node, then supplies its pre-transition snapshot to host policy for operator and target validation. Success
// closes the old task, appends a fresh active task for NewAssignee, records fully attributed audit metadata, increments
// Version once, and commits the aggregate through one Store.Save CAS. Every error leaves durable state unchanged;
// Definition, Approval config, prior tasks, and the returned snapshot remain detached from caller mutation.
func (e *Engine) Transfer(
	ctx context.Context,
	request TransferRequest,
	policy TransferPolicy,
) (*Instance, error) {
	// Transfer needs persistence and complete trusted boundary input before loading durable state.
	if e == nil || nilguard.IsNil(e.store) {
		return nil, fmt.Errorf("%w: transfer store is nil", ErrInvalidEngine)
	}
	if request.InstanceID == "" || request.TaskID == "" || request.ActorID == "" || request.NewAssignee == "" ||
		strings.TrimSpace(request.Reason) == "" || nilguard.IsNil(policy) {
		return nil, fmt.Errorf("%w: instance, task, actor, assignee, reason, or policy is empty", ErrInvalidTransferRequest)
	}

	// Resolve and authorize the exact active assignment before constructing its replacement task.
	var currentTask Task
	return e.executeInstanceCommand(ctx, instanceCommand{
		name:            "task transfer",
		instanceID:      request.InstanceID,
		nonRunningError: ErrInstanceNotRunning,
		prepare: func(snapshot *Instance) error {
			var err error
			currentTask, err = prepareTransfer(ctx, request, policy, snapshot)
			return err
		},
		audit: func(facts *instanceFacts) {
			facts.recordTransfer(currentTask, request.ActorID, request.NewAssignee, request.Reason)
		},
		transition: func(facts *instanceFacts) error {
			return facts.transfer(currentTask, request.NewAssignee)
		},
	})
}

// prepareTransfer resolves and authorizes the active source assignment before the fact boundary creates its successor.
//
// request has passed boundary validation and snapshot is the detached running aggregate loaded for this command.
// policy receives that snapshot and the exact current task before any candidate mutation. Success returns the
// value-owned source task; errors preserve ErrTaskNotTransferable or host policy causes and leave snapshot unchanged.
func prepareTransfer(
	ctx context.Context,
	request TransferRequest,
	policy TransferPolicy,
	snapshot *Instance,
) (Task, error) {
	// Resolve the requested identity from the complete historical task set without accepting a prior node's task.
	var currentTask Task
	found := false
	for _, task := range snapshot.Tasks {
		if task.ID == request.TaskID {
			currentTask = task
			found = true
			// Task IDs are aggregate-unique, so later historical rows cannot change the resolved source.
			break
		}
	}
	// Only the current node's active assignment can acquire a replacement owner.
	if !found || currentTask.NodeID != snapshot.CurrentNodeID || currentTask.Status != TaskStatusActive {
		return Task{}, fmt.Errorf(
			"%w: task %q is not active at node %q",
			ErrTaskNotTransferable,
			request.TaskID,
			snapshot.CurrentNodeID,
		)
	}

	// Host policy owns operator authority and replacement identity validity for the resolved assignment.
	if err := policy.AuthorizeTransfer(ctx, request, currentTask, snapshot); err != nil {
		return Task{}, fmt.Errorf("workflow: authorize task transfer: %w", err)
	}
	return currentTask, nil
}

// prepareReturn validates one historical target, authorizes it, and activates detached task drafts.
//
// request has passed boundary validation and snapshot is a detached running aggregate. The method compiles only
// snapshot.Definition, rejects control, current, absent, and unvisited targets, then invokes policy with another
// defensive snapshot so policy mutation cannot affect activation. Success returns the compiled target and a fully
// prepared result application; errors leave the command candidate untouched and preserve stable sentinels.
func (e *Engine) prepareReturn(
	ctx context.Context,
	request ReturnRequest,
	policy ReturnPolicy,
	snapshot *Instance,
) (*NodeDefinition, *nodeResultApplication, error) {
	// Resolve the exact requested ID from the frozen plan rather than Definition slice position.
	plan, err := compileDefinition(&snapshot.Definition, e.registry)
	if err != nil {
		return nil, nil, err
	}
	target, err := plan.node(request.TargetNodeID)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: %w", ErrInvalidReturnTarget, err)
	}
	// Control, current, and unvisited nodes cannot represent an earlier executable task round.
	if target.Kind == KindStart || target.Kind == KindEnd || target.ID == snapshot.CurrentNodeID ||
		!hasEnteredNode(snapshot.Audit, target.ID) {
		return nil, nil, fmt.Errorf(
			"%w: node %q is not an eligible historical target",
			ErrInvalidReturnTarget,
			target.ID,
		)
	}
	if err := policy.AuthorizeReturn(ctx, request, cloneInstance(snapshot)); err != nil {
		return nil, nil, fmt.Errorf("workflow: authorize return: %w", err)
	}

	// Eligible return nodes must activate a concrete waiting task round before any aggregate mutation.
	handler, err := plan.preparedHandler(target.ID)
	if err != nil {
		return nil, nil, err
	}
	result, err := handler.ActivatePrepared(ctx, PreparedActivationInput{
		Data: slices.Clone(snapshot.Data),
	})
	if err != nil {
		return nil, nil, fmt.Errorf("workflow: reactivate return target %q: %w", target.ID, err)
	}
	application, err := prepareReturnNodeResult(target.ID, result)
	if err != nil {
		return nil, nil, err
	}
	return target, application, nil
}

// executeInstanceCommand applies the invariant command sequence around one operation-specific transition.
//
// command must provide an identity, stable terminal-state error, prepare, audit, and transition callbacks.
// prepare receives a detached snapshot, while audit and transition receive the private loaded aggregate only
// after preparation succeeds. The method performs exactly one Load and at most one Save; it increments Version
// once immediately before CAS persistence and returns a detached snapshot. Any error leaves Store unchanged.
func (e *Engine) executeInstanceCommand(
	ctx context.Context,
	command instanceCommand,
) (*Instance, error) {
	// Internal command construction must be complete before storage or callbacks can run.
	if e == nil || nilguard.IsNil(e.store) || command.instanceID == "" || command.nonRunningError == nil ||
		command.prepare == nil || command.audit == nil || command.transition == nil {
		return nil, fmt.Errorf("%w: instance command is incomplete", ErrInvalidEngine)
	}

	// Load exactly one caller-owned aggregate and apply the public operation's terminal-state classification.
	instance, err := e.store.Load(ctx, command.instanceID)
	if err != nil {
		return nil, fmt.Errorf("workflow: load %s: %w", command.name, err)
	}
	if instance.Status != InstanceStatusRunning {
		return nil, fmt.Errorf("%w: instance %q is %q", command.nonRunningError, instance.ID, instance.Status)
	}
	expectedVersion := instance.Version

	// Preparation and policy observe a detached snapshot, so denial or accidental mutation cannot alter candidate state.
	if err := command.prepare(cloneInstance(instance)); err != nil {
		return nil, err
	}
	facts := newInstanceFacts(instance)
	command.audit(facts)
	if err := command.transition(facts); err != nil {
		return nil, err
	}
	facts.advanceVersion()

	// One aggregate CAS commits every task, state, audit, status, and version change or none of them.
	if err := e.store.Save(ctx, facts.candidate(), expectedVersion); err != nil {
		return nil, fmt.Errorf("workflow: persist %s: %w", command.name, err)
	}
	return cloneInstance(instance), nil
}

// hasEnteredNode reports whether immutable audit history proves that nodeID executed before the current command.
//
// audit is the detached pre-transition history and nodeID is the explicit return target. The scan performs no
// mutation or I/O; false means no stable node-entry fact exists and therefore the target is not return-eligible.
func hasEnteredNode(audit []AuditRecord, nodeID string) bool {
	for _, record := range audit {
		// Both the stable action and exact node identity must match one previously accepted entry fact.
		if record.Action == auditNodeEntered && record.NodeID == nodeID {
			return true
		}
	}
	return false
}

// validateCommand checks Engine dependencies and the complete external command boundary before Store access.
//
// command identity fields must be non-empty and Payload must be absent or valid JSON. The method performs no
// I/O and returns ErrInvalidEngine or ErrInvalidCommand so callers can classify setup and request failures.
func (e *Engine) validateCommand(command Command) error {
	// Missing collaborators make every command unsafe regardless of its input fields.
	if e == nil || nilguard.IsNil(e.store) || e.registry == nil {
		return fmt.Errorf("%w: handle dependencies are nil", ErrInvalidEngine)
	}
	// All four identities jointly bind the command to one task, actor, and handler operation.
	if command.InstanceID == "" || command.TaskID == "" || command.ActorID == "" || command.Name == "" {
		return fmt.Errorf("%w: required command field is empty", ErrInvalidCommand)
	}
	// Invalid JSON cannot be delegated because handlers own schema validation only after syntax is trustworthy.
	if !validJSON(command.Payload) {
		return fmt.Errorf("%w: payload is not valid json", ErrInvalidCommand)
	}
	return nil
}

// advance follows compiled routes and asks the fact boundary to record nodes until execution waits or terminates.
//
// facts wraps the caller-owned aggregate being prepared for one atomic Store write. plan must be compiled from that
// aggregate's Definition, and outcome selects the first route; an empty outcome is unconditional. Handler or routing
// errors leave durable state unchanged because callers persist only after this method succeeds.
func (e *Engine) advance(ctx context.Context, facts *instanceFacts, plan *compiledDefinition, outcome string) error {
	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("workflow: advance instance: %w", err)
		}
		next, err := plan.nextNode(facts.candidate().CurrentNodeID, outcome)
		if err != nil {
			return err
		}
		facts.enterNode(next.ID)

		// End nodes complete immediately; other business nodes decide whether to wait or continue again.
		if next.Kind == KindEnd {
			facts.complete()
			return nil
		}
		handler, err := plan.preparedHandler(next.ID)
		if err != nil {
			return err
		}
		result, err := handler.ActivatePrepared(ctx, PreparedActivationInput{
			Data: slices.Clone(facts.candidate().Data),
		})
		if err != nil {
			return fmt.Errorf("workflow: activate node %q: %w", next.ID, err)
		}
		application, err := prepareActivationNodeResult(next.ID, result)
		if err != nil {
			return err
		}
		decision, err := application.apply(facts)
		if err != nil {
			return err
		}
		nextOutcome, shouldAdvance, err := decision.navigation()
		if err != nil {
			return err
		}
		if !shouldAdvance {
			return nil
		}
		outcome = nextOutcome
	}
}
