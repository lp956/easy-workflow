// This file owns the versioned transport encoding shared by PostgreSQL Projection continuation families.
// It does not decide task versus instance keyset validity, execute SQL, authorize scope, or expose ordering components.
// Encoding is deterministic UTF-8 JSON inside unpadded base64url and contains no secret or process-local state.
package postgres

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	workflow "github.com/lvpeng/easy-workflow"
	"github.com/lvpeng/easy-workflow/internal/jsonstrict"
)

const (
	// continuationEncodingVersion identifies the current wire schema and prevents silent reinterpretation after upgrades.
	continuationEncodingVersion = 1
	// maximumContinuationLength bounds both encoded output and untrusted decode allocation.
	maximumContinuationLength = 1024
	// taskContinuationFamily is shared by WorklistPage and ParticipatedPage because their ordering keys are identical.
	taskContinuationFamily = "task"
	// instanceContinuationFamily belongs only to InitiatedPage and excludes a task identity key.
	instanceContinuationFamily = "instance"
)

// continuationEnvelope is the private versioned transport form decoded before a query family validates its keyset.
//
// Field names are intentionally compact but stable. The value is never persisted server-side, carries no authorization,
// and must be combined with the caller's freshly supplied actor and scope on every query.
type continuationEnvelope struct {
	// Version selects the exact decoding and validation contract.
	Version int `json:"v"`
	// Family prevents a task keyset from being reinterpreted as an instance keyset or vice versa.
	Family string `json:"f"`
	// At is the PostgreSQL-normalized primary descending ordering timestamp.
	At time.Time `json:"a"`
	// InstanceID is the deterministic ascending identity tie-breaker.
	InstanceID workflow.InstanceID `json:"i"`
	// TaskID is the final task-family tie-breaker and remains empty for instance continuations.
	TaskID workflow.TaskID `json:"t,omitempty"`
}

// encodeContinuationEnvelope serializes one already validated family keyset into an opaque transport token.
//
// envelope must contain the current version, one known family, and that family's complete ordering keys. The function
// emits deterministic compact JSON inside unpadded base64url, retains no input, performs no I/O, and returns encoding
// errors without exposing a partial token.
func encodeContinuationEnvelope(envelope continuationEnvelope) (Continuation, error) {
	data, err := json.Marshal(envelope)
	if err != nil {
		return "", fmt.Errorf("postgres: encode projection continuation: %w", err)
	}
	token := Continuation(base64.RawURLEncoding.EncodeToString(data))
	if len(token) > maximumContinuationLength {
		return "", fmt.Errorf("%w: encoded continuation is too long", ErrInvalidProjectionQuery)
	}
	return token, nil
}

// decodeContinuationEnvelope strictly decodes one non-empty untrusted opaque token without choosing keyset semantics.
//
// token length is bounded before allocation. The decoder rejects invalid base64url, unknown JSON fields, trailing JSON,
// and malformed field encodings. Family and complete-key validation remain colocated with the Task or Instance query
// module. Every caller-controlled failure wraps ErrInvalidProjectionQuery and no database I/O occurs.
func decodeContinuationEnvelope(token Continuation) (continuationEnvelope, error) {
	// Empty tokens are handled as first-page sentinels by callers; the upper bound prevents oversized decode allocation.
	if token == "" || len(token) > maximumContinuationLength {
		return continuationEnvelope{}, fmt.Errorf("%w: continuation is empty or too long", ErrInvalidProjectionQuery)
	}
	data, err := base64.RawURLEncoding.DecodeString(string(token))
	if err != nil {
		return continuationEnvelope{}, fmt.Errorf("%w: continuation encoding: %w", ErrInvalidProjectionQuery, err)
	}

	// The shared strict boundary rejects duplicates, case aliases, invalid UTF-8, unknown fields, and trailing values.
	var envelope continuationEnvelope
	if err := jsonstrict.Decode(data, &envelope); err != nil {
		return continuationEnvelope{}, fmt.Errorf("%w: continuation payload: %w", ErrInvalidProjectionQuery, err)
	}
	return envelope, nil
}
