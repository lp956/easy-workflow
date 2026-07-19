// Package condition implements the official restricted condition node for easy-workflow.
//
// Config rules use explicit all or any combination. Every rule is evaluated, exactly one may match, and
// DefaultOutcome applies only when none matches. A missing default returns ErrNoMatch; overlaps return
// ErrMultipleMatches. Field references are RFC 6901 JSON Pointers traversed through object keys only.
//
// String expressions support eq, neq, contains, starts_with, and ends_with. Number expressions support eq,
// neq, gt, gte, lt, and lte using exact JSON decimal comparison. Boolean expressions support eq and neq.
// Collection expressions support contains, contains_any, and contains_all over string, number, and boolean
// members; null, objects, nested arrays, cross-type coercion, and array-index field traversal are unsupported.
//
// Validate rejects unknown fields, duplicate JSON keys, malformed references, and unsupported type/operator/value
// combinations. Activate distinguishes ErrInvalidData, ErrFieldNotFound, ErrTypeMismatch, ErrNoMatch, and
// ErrMultipleMatches. The package performs no external I/O and cannot execute Go, scripts, templates, reflection
// calls, or caller-supplied callbacks. Handlers are stateless and safe for concurrent activation.
package condition

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"slices"
	"strings"

	workflow "github.com/lvpeng/easy-workflow"
	"github.com/lvpeng/easy-workflow/internal/jsonstrict"
)

const (
	// Kind is the stable registry key for the official condition node handler.
	Kind = "condition"

	// MatchAll requires every expression in a rule to match.
	MatchAll MatchMode = "all"
	// MatchAny requires at least one expression in a rule to match.
	MatchAny MatchMode = "any"

	// TypeString requires both the referenced business value and comparison value to be strings.
	TypeString ValueType = "string"
	// TypeNumber compares JSON numbers without converting them to binary floating point.
	TypeNumber ValueType = "number"
	// TypeBoolean requires both the referenced business value and comparison value to be booleans.
	TypeBoolean ValueType = "boolean"
	// TypeCollection requires the referenced business value to be an array of JSON primitive values.
	TypeCollection ValueType = "collection"

	// OperatorEqual compares two values of the expression's declared type for equality.
	OperatorEqual Operator = "eq"
	// OperatorNotEqual compares two scalar values for inequality.
	OperatorNotEqual Operator = "neq"
	// OperatorContains checks a string substring or one collection member, depending on the declared type.
	OperatorContains Operator = "contains"
	// OperatorStartsWith checks a string prefix.
	OperatorStartsWith Operator = "starts_with"
	// OperatorEndsWith checks a string suffix.
	OperatorEndsWith Operator = "ends_with"
	// OperatorGreaterThan compares whether a number is greater than its configured value.
	OperatorGreaterThan Operator = "gt"
	// OperatorGreaterOrEqual compares whether a number is greater than or equal to its configured value.
	OperatorGreaterOrEqual Operator = "gte"
	// OperatorLessThan compares whether a number is less than its configured value.
	OperatorLessThan Operator = "lt"
	// OperatorLessOrEqual compares whether a number is less than or equal to its configured value.
	OperatorLessOrEqual Operator = "lte"
	// OperatorContainsAny checks whether a collection contains at least one configured member.
	OperatorContainsAny Operator = "contains_any"
	// OperatorContainsAll checks whether a collection contains every configured member.
	OperatorContainsAll Operator = "contains_all"
)

var (
	// ErrInvalidConfig identifies malformed or unsupported condition configuration.
	ErrInvalidConfig = errors.New("condition: invalid config")
	// ErrFieldNotFound identifies a configured field absent from activation business data.
	ErrFieldNotFound = errors.New("condition: field not found")
	// ErrInvalidData identifies malformed or non-object business data that cannot be evaluated safely.
	ErrInvalidData = errors.New("condition: invalid business data")
	// ErrTypeMismatch identifies a field whose runtime JSON type differs from its configured ValueType.
	ErrTypeMismatch = errors.New("condition: business data type mismatch")
	// ErrNoMatch identifies activation data that matches no rule when no default outcome is configured.
	ErrNoMatch = errors.New("condition: no matching rule")
	// ErrMultipleMatches identifies activation data that matches more than one independent rule.
	ErrMultipleMatches = errors.New("condition: multiple matching rules")
	// errInvalidJSON classifies strict token-stream failures before a public boundary adds its own sentinel.
	errInvalidJSON = errors.New("condition: invalid JSON")
	// errInvalidPointer classifies malformed RFC 6901 syntax before config validation adds rule context.
	errInvalidPointer = errors.New("condition: invalid JSON pointer")
)

// MatchMode defines how one rule combines its expressions.
type MatchMode string

// ValueType declares the JSON type expected for a field and comparison value.
type ValueType string

// Operator names one allow-listed comparison operation.
type Operator string

// Config is the fully JSON-serializable configuration accepted by Builder and web-authored definitions.
//
// Rules are evaluated independently and exactly one may match. DefaultOutcome is selected only when no rule
// matches; its empty value means unmatched input returns ErrNoMatch. Config contains data only and cannot retain
// callbacks, reflection targets, or external-service handles.
type Config struct {
	Rules          []Rule `json:"rules"`
	DefaultOutcome string `json:"defaultOutcome,omitempty"`
}

// Rule maps one explicit expression combination to a DAG outcome.
type Rule struct {
	Match      MatchMode    `json:"match"`
	Conditions []Expression `json:"conditions"`
	Outcome    string       `json:"outcome"`
}

// Expression compares one business-data field with one JSON-serializable literal.
type Expression struct {
	Field    string    `json:"field"`
	Type     ValueType `json:"type"`
	Operator Operator  `json:"operator"`
	Value    any       `json:"value"`
}

// Handler validates and evaluates condition nodes without retaining instance-specific state.
// It is safe for concurrent use because every call decodes into fresh local values.
type Handler struct{}

// preparedHandler owns one validated immutable Condition config for a request-local compiled Definition plan.
// It performs no I/O, cross-request caching, serialization, or persistence.
type preparedHandler struct {
	// config is the decoded restricted rule set reused by prepared activation.
	config Config
}

var (
	_ workflow.NodeHandler               = (*Handler)(nil)
	_ workflow.NodeHandlerConfigPreparer = (*Handler)(nil)
	_ workflow.PreparedNodeHandler       = (*preparedHandler)(nil)
)

// NewHandler creates the stateless official condition handler.
func NewHandler() *Handler {
	return &Handler{}
}

// Validate rejects configuration outside the restricted condition schema before publication.
//
// config must encode one Config value. Validation performs no I/O and returns ErrInvalidConfig for schema,
// field-reference, type, operator, or outcome violations.
func (h *Handler) Validate(config json.RawMessage) error {
	_, err := parseConfig(config)
	return err
}

// PrepareConfig decodes and validates one canonical Condition config for a request-local executable plan.
//
// config follows Validate's restricted schema and is not retained as raw bytes. The returned immutable executor reuses
// the decoded rules for activation, performs no I/O, and is never cached across Engine operations. Errors preserve
// ErrInvalidConfig and its detailed cause.
func (h *Handler) PrepareConfig(config json.RawMessage) (workflow.PreparedNodeHandler, error) {
	prepared, err := parseConfig(config)
	if err != nil {
		return nil, err
	}
	return &preparedHandler{config: prepared}, nil
}

// Activate evaluates validated rules against the supplied defensive business-data snapshot.
//
// input.Config must satisfy Validate and input.Data must be a JSON object. The method returns Continue with
// exactly one outcome on a match. Context cancellation and evaluation errors are returned without fallback;
// no state or tasks are produced, and the method performs no external I/O.
func (h *Handler) Activate(ctx context.Context, input workflow.ActivationInput) (workflow.NodeResult, error) {
	if err := ctx.Err(); err != nil {
		return workflow.NodeResult{}, fmt.Errorf("condition: activate: %w", err)
	}
	config, err := parseConfig(input.Config)
	if err != nil {
		return workflow.NodeResult{}, err
	}
	return activateConfig(config, input.Data)
}

// ActivatePrepared evaluates business data with rules decoded during executable-plan compilation.
//
// input.Data must be a JSON object and is decoded into request-local storage. The method reuses immutable Config, performs
// no external I/O, retains no input, and returns cancellation or Condition evaluation errors without fallback.
func (h *preparedHandler) ActivatePrepared(
	ctx context.Context,
	input workflow.PreparedActivationInput,
) (workflow.NodeResult, error) {
	if err := ctx.Err(); err != nil {
		return workflow.NodeResult{}, fmt.Errorf("condition: activate: %w", err)
	}
	return activateConfig(h.config, input.Data)
}

// activateConfig evaluates every validated rule against one detached business-data JSON object.
//
// config must come from parseConfig and data must encode a non-nil object. Exactly one match returns Continue; no match
// uses DefaultOutcome or ErrNoMatch, and overlaps return ErrMultipleMatches. The function retains no input and performs no I/O.
func activateConfig(config Config, dataJSON json.RawMessage) (workflow.NodeResult, error) {

	// Decode business data into fresh storage so evaluation cannot mutate or retain Engine-owned bytes.
	var data map[string]any
	if err := jsonstrict.Decode(dataJSON, &data); err != nil {
		return workflow.NodeResult{}, fmt.Errorf("%w: JSON object required: %w", ErrInvalidData, err)
	}
	if data == nil {
		return workflow.NodeResult{}, fmt.Errorf("%w: JSON object required", ErrInvalidData)
	}
	// Evaluate every rule before selecting an outcome so rule slice order can never choose the winner.
	matchedOutcome := ""
	matchCount := 0
	for _, rule := range config.Rules {
		matched, evalErr := evaluateRule(rule, data)
		if evalErr != nil {
			return workflow.NodeResult{}, evalErr
		}
		if matched {
			matchedOutcome = rule.Outcome
			matchCount++
		}
	}
	if matchCount > 1 {
		return workflow.NodeResult{}, fmt.Errorf("%w: %d rules matched", ErrMultipleMatches, matchCount)
	}
	if matchCount == 1 {
		return workflow.NodeResult{Disposition: workflow.DispositionContinue, Outcome: matchedOutcome}, nil
	}
	if config.DefaultOutcome != "" {
		return workflow.NodeResult{Disposition: workflow.DispositionContinue, Outcome: config.DefaultOutcome}, nil
	}
	return workflow.NodeResult{}, ErrNoMatch
}

// Handle rejects commands because a condition node completes synchronously during activation.
//
// The method ignores input data, creates no state, and always returns workflow.ErrInvalidCommand. Context
// cancellation is reported first so callers can consistently abandon work.
func (h *Handler) Handle(ctx context.Context, _ workflow.CommandInput) (workflow.NodeResult, error) {
	if err := ctx.Err(); err != nil {
		return workflow.NodeResult{}, fmt.Errorf("condition: handle: %w", err)
	}
	return workflow.NodeResult{}, fmt.Errorf("%w: condition nodes do not accept commands", workflow.ErrInvalidCommand)
}

// HandlePrepared rejects commands because a prepared Condition still completes synchronously during activation.
//
// The method ignores input, creates no state, performs no I/O, and returns workflow.ErrInvalidCommand. Context
// cancellation is reported first so callers can abandon work consistently with the legacy Handle path.
func (h *preparedHandler) HandlePrepared(
	ctx context.Context,
	_ workflow.PreparedCommandInput,
) (workflow.NodeResult, error) {
	if err := ctx.Err(); err != nil {
		return workflow.NodeResult{}, fmt.Errorf("condition: handle: %w", err)
	}
	return workflow.NodeResult{}, fmt.Errorf("%w: condition nodes do not accept commands", workflow.ErrInvalidCommand)
}

// parseConfig decodes and validates the complete restricted condition schema supported by the evaluator.
//
// data must contain at least one explicit all- or any-match rule with a non-empty outcome and typed expressions.
// The returned value owns all decoded slices and values; schema and allow-list errors wrap ErrInvalidConfig.
func parseConfig(data json.RawMessage) (Config, error) {
	// Strict decoding rejects ambiguous or extensible JSON before any rule acquires runtime meaning.
	var config Config
	if err := decodeConfig(data, &config); err != nil {
		return Config{}, fmt.Errorf("%w: decode: %w", ErrInvalidConfig, err)
	}
	if len(config.Rules) == 0 {
		return Config{}, fmt.Errorf("%w: rules are empty", ErrInvalidConfig)
	}
	// Validate every field reference and typed expression so Activate only executes a closed allow-list.
	for ruleIndex, rule := range config.Rules {
		if (rule.Match != MatchAll && rule.Match != MatchAny) || rule.Outcome == "" || len(rule.Conditions) == 0 {
			return Config{}, fmt.Errorf("%w: rule %d is incomplete", ErrInvalidConfig, ruleIndex)
		}
		for expressionIndex, expression := range rule.Conditions {
			if _, err := parsePointer(expression.Field); err != nil {
				return Config{}, fmt.Errorf("%w: rule %d expression %d field is invalid", ErrInvalidConfig, ruleIndex, expressionIndex)
			}
			if err := validateExpression(expression); err != nil {
				return Config{}, fmt.Errorf("%w: rule %d expression %d: %w", ErrInvalidConfig, ruleIndex, expressionIndex, err)
			}
		}
	}
	return config, nil
}

// decodeConfig applies the shared strict JSON boundary before returning typed configuration.
//
// data must contain exactly one Config JSON object. target receives fresh decoded values with numbers preserved
// as json.Number. Every syntax or schema error is returned to parseConfig for ErrInvalidConfig wrapping.
func decodeConfig(data []byte, target *Config) error {
	if err := jsonstrict.Decode(data, target); err != nil {
		return fmt.Errorf("%w: decode config: %w", errInvalidJSON, err)
	}
	return nil
}

// evaluateRule combines a validated rule's expressions under its explicit all or any mode.
//
// rule must have MatchAll or MatchAny and at least one expression. Missing fields and type mismatches are
// returned rather than treated as false; the boolean reports the selected combination result.
func evaluateRule(rule Rule, data map[string]any) (bool, error) {
	for _, expression := range rule.Conditions {
		matched, err := evaluateExpression(expression, data)
		if err != nil {
			return false, err
		}
		if rule.Match == MatchAny && matched {
			return true, nil
		}
		if rule.Match == MatchAll && !matched {
			return false, nil
		}
	}
	return rule.Match == MatchAll, nil
}

// evaluateExpression applies one validated typed expression against business data.
//
// data must be a decoded JSON object whose numbers remain json.Number values. Missing fields return
// ErrFieldNotFound; wrong runtime value types return ErrTypeMismatch rather than being coerced. The function
// is deterministic and has no side effects.
func evaluateExpression(expression Expression, data map[string]any) (bool, error) {
	value, exists := lookupField(data, expression.Field)
	if !exists {
		return false, fmt.Errorf("%w: %s", ErrFieldNotFound, expression.Field)
	}

	// Dispatch only on the declared allow-listed type; no runtime method or reflection lookup is performed.
	switch expression.Type {
	case TypeString:
		return evaluateString(value, expression)
	case TypeNumber:
		return evaluateNumber(value, expression)
	case TypeBoolean:
		return evaluateBoolean(value, expression)
	case TypeCollection:
		return evaluateCollection(value, expression)
	}
	return false, fmt.Errorf("%w: unsupported type or operator", ErrInvalidConfig)
}

// evaluateString applies one validated string operator without cross-type coercion.
//
// value must be a string and expression.Value must remain a string after validation. Type changes return a
// classified error; unsupported operators cannot fall through to a default comparison.
func evaluateString(value any, expression Expression) (bool, error) {
	actual, ok := value.(string)
	if !ok {
		return false, fmt.Errorf("%w: field %s is not a string", ErrTypeMismatch, expression.Field)
	}
	expected, ok := expression.Value.(string)
	if !ok {
		return false, fmt.Errorf("%w: string comparison value changed after validation", ErrInvalidConfig)
	}
	switch expression.Operator {
	case OperatorEqual:
		return actual == expected, nil
	case OperatorNotEqual:
		return actual != expected, nil
	case OperatorContains:
		return strings.Contains(actual, expected), nil
	case OperatorStartsWith:
		return strings.HasPrefix(actual, expected), nil
	case OperatorEndsWith:
		return strings.HasSuffix(actual, expected), nil
	case OperatorGreaterThan, OperatorGreaterOrEqual, OperatorLessThan, OperatorLessOrEqual,
		OperatorContainsAny, OperatorContainsAll:
		return false, fmt.Errorf("%w: operator %q is unsupported for string", ErrInvalidConfig, expression.Operator)
	}
	return false, fmt.Errorf("%w: unknown string operator %q", ErrInvalidConfig, expression.Operator)
}

// evaluateNumber applies one validated exact-decimal comparison.
//
// value and expression.Value must be json.Number values retained from decoding. Invalid text, changed types, and
// unsupported operators return errors; no binary floating-point conversion is performed.
func evaluateNumber(value any, expression Expression) (bool, error) {
	actual, ok := value.(json.Number)
	if !ok {
		return false, fmt.Errorf("%w: field %s is not a number", ErrTypeMismatch, expression.Field)
	}
	expected, ok := expression.Value.(json.Number)
	if !ok {
		return false, fmt.Errorf("%w: number comparison value changed after validation", ErrInvalidConfig)
	}
	comparison, err := compareNumbers(actual, expected)
	if err != nil {
		return false, fmt.Errorf("%w: field %s: %w", ErrInvalidConfig, expression.Field, err)
	}
	switch expression.Operator {
	case OperatorEqual:
		return comparison == 0, nil
	case OperatorNotEqual:
		return comparison != 0, nil
	case OperatorGreaterThan:
		return comparison > 0, nil
	case OperatorGreaterOrEqual:
		return comparison >= 0, nil
	case OperatorLessThan:
		return comparison < 0, nil
	case OperatorLessOrEqual:
		return comparison <= 0, nil
	case OperatorContains, OperatorStartsWith, OperatorEndsWith, OperatorContainsAny, OperatorContainsAll:
		return false, fmt.Errorf("%w: operator %q is unsupported for number", ErrInvalidConfig, expression.Operator)
	}
	return false, fmt.Errorf("%w: unknown number operator %q", ErrInvalidConfig, expression.Operator)
}

// evaluateBoolean applies equality or inequality to one validated boolean expression.
//
// value and expression.Value must both be booleans. Runtime or configuration type changes return classified
// errors; unknown operators are rejected rather than treated as inequality.
func evaluateBoolean(value any, expression Expression) (bool, error) {
	actual, ok := value.(bool)
	if !ok {
		return false, fmt.Errorf("%w: field %s is not a boolean", ErrTypeMismatch, expression.Field)
	}
	expected, ok := expression.Value.(bool)
	if !ok {
		return false, fmt.Errorf("%w: boolean comparison value changed after validation", ErrInvalidConfig)
	}
	switch expression.Operator {
	case OperatorEqual:
		return actual == expected, nil
	case OperatorNotEqual:
		return actual != expected, nil
	case OperatorContains, OperatorStartsWith, OperatorEndsWith, OperatorGreaterThan, OperatorGreaterOrEqual,
		OperatorLessThan, OperatorLessOrEqual, OperatorContainsAny, OperatorContainsAll:
		return false, fmt.Errorf("%w: operator %q is unsupported for boolean", ErrInvalidConfig, expression.Operator)
	}
	return false, fmt.Errorf("%w: unknown boolean operator %q", ErrInvalidConfig, expression.Operator)
}

// evaluateCollection validates a runtime array and applies one membership operator to JSON primitive values.
//
// value must be []any containing only supported primitives. expression.Value must retain its validated scalar or
// non-empty array shape. Comparisons are type-strict; malformed runtime or configuration values return errors.
func evaluateCollection(value any, expression Expression) (bool, error) {
	actual, ok := value.([]any)
	if !ok {
		return false, fmt.Errorf("%w: field %s is not a collection", ErrTypeMismatch, expression.Field)
	}
	if slices.ContainsFunc(actual, func(member any) bool { return !isPrimitive(member) }) {
		return false, fmt.Errorf("%w: field %s contains a non-primitive member", ErrTypeMismatch, expression.Field)
	}
	return compareCollection(actual, expression.Operator, expression.Value)
}

// parsePointer validates and decodes a non-root RFC 6901 JSON Pointer into object-key tokens.
//
// pointer must begin with '/'; '~0' denotes '~' and '~1' denotes '/'. Empty object-key tokens are preserved,
// while malformed escape sequences return an error. The function performs no data lookup.
func parsePointer(pointer string) ([]string, error) {
	if !strings.HasPrefix(pointer, "/") {
		return nil, fmt.Errorf("%w: must begin with '/'", errInvalidPointer)
	}
	rawTokens := strings.Split(pointer[1:], "/")
	tokens := make([]string, len(rawTokens))
	for index, rawToken := range rawTokens {
		var token strings.Builder
		for offset := 0; offset < len(rawToken); offset++ {
			if rawToken[offset] != '~' {
				token.WriteByte(rawToken[offset])
				continue
			}
			if offset+1 >= len(rawToken) {
				return nil, fmt.Errorf("%w: incomplete escape", errInvalidPointer)
			}
			offset++
			switch rawToken[offset] {
			case '0':
				token.WriteByte('~')
			case '1':
				token.WriteByte('/')
			default:
				return nil, fmt.Errorf("%w: invalid escape", errInvalidPointer)
			}
		}
		tokens[index] = token.String()
	}
	return tokens, nil
}

// lookupField resolves one validated JSON Pointer through object keys only.
//
// data is the decoded business-data object. Array indices are intentionally unsupported; encountering a
// non-object or absent key returns false. The returned value remains owned by the activation-local snapshot.
func lookupField(data map[string]any, pointer string) (any, bool) {
	tokens, err := parsePointer(pointer)
	if err != nil {
		return nil, false
	}
	var current any = data
	for _, token := range tokens {
		object, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		current, ok = object[token]
		if !ok {
			return nil, false
		}
	}
	return current, true
}

// validateExpression checks the value shape and operator allow-list for one configured type.
//
// expression.Value must use the JSON representation required by expression.Type. Collection operands contain
// only string, number, or boolean primitives. Errors describe unsupported combinations without executing data.
func validateExpression(expression Expression) error {
	switch expression.Type {
	case TypeString:
		return validateStringExpression(expression)
	case TypeNumber:
		return validateNumberExpression(expression)
	case TypeBoolean:
		return validateBooleanExpression(expression)
	case TypeCollection:
		return validateCollectionExpression(expression)
	default:
		return fmt.Errorf("%w: type is unsupported", ErrInvalidConfig)
	}
}

// validateStringExpression checks the literal type and complete operator allow-list for strings.
func validateStringExpression(expression Expression) error {
	if _, ok := expression.Value.(string); !ok {
		return fmt.Errorf("%w: value must be a string", ErrInvalidConfig)
	}
	if expression.Operator != OperatorEqual && expression.Operator != OperatorNotEqual &&
		expression.Operator != OperatorContains && expression.Operator != OperatorStartsWith &&
		expression.Operator != OperatorEndsWith {
		return fmt.Errorf("%w: operator is unsupported for string", ErrInvalidConfig)
	}
	return nil
}

// validateNumberExpression checks the literal type and ordered-comparison allow-list for exact JSON numbers.
func validateNumberExpression(expression Expression) error {
	if _, ok := expression.Value.(json.Number); !ok {
		return fmt.Errorf("%w: value must be a number", ErrInvalidConfig)
	}
	if expression.Operator != OperatorEqual && expression.Operator != OperatorNotEqual &&
		expression.Operator != OperatorGreaterThan && expression.Operator != OperatorGreaterOrEqual &&
		expression.Operator != OperatorLessThan && expression.Operator != OperatorLessOrEqual {
		return fmt.Errorf("%w: operator is unsupported for number", ErrInvalidConfig)
	}
	return nil
}

// validateBooleanExpression permits only boolean literals with equality or inequality.
func validateBooleanExpression(expression Expression) error {
	if _, ok := expression.Value.(bool); !ok {
		return fmt.Errorf("%w: value must be a boolean", ErrInvalidConfig)
	}
	if expression.Operator != OperatorEqual && expression.Operator != OperatorNotEqual {
		return fmt.Errorf("%w: operator is unsupported for boolean", ErrInvalidConfig)
	}
	return nil
}

// validateCollectionExpression checks membership operands without accepting objects, nested arrays, or null.
//
// contains requires one JSON primitive. contains_any and contains_all require a non-empty primitive array.
// Unsupported operators and member shapes return ErrInvalidConfig.
func validateCollectionExpression(expression Expression) error {
	if expression.Operator == OperatorContains {
		if !isPrimitive(expression.Value) {
			return fmt.Errorf("%w: contains value must be a JSON primitive", ErrInvalidConfig)
		}
		return nil
	}
	if expression.Operator != OperatorContainsAny && expression.Operator != OperatorContainsAll {
		return fmt.Errorf("%w: operator is unsupported for collection", ErrInvalidConfig)
	}
	values, ok := expression.Value.([]any)
	if !ok || len(values) == 0 {
		return fmt.Errorf("%w: collection comparison value must be a non-empty array", ErrInvalidConfig)
	}
	if slices.ContainsFunc(values, func(value any) bool { return !isPrimitive(value) }) {
		return fmt.Errorf("%w: collection comparison values must be JSON primitives", ErrInvalidConfig)
	}
	return nil
}

// compareNumbers compares two valid JSON numbers exactly as rational decimal values.
//
// left and right must retain their original JSON number text. The result is negative, zero, or positive when
// left is respectively less than, equal to, or greater than right. Invalid text returns an error.
func compareNumbers(left, right json.Number) (int, error) {
	leftValue, ok := new(big.Rat).SetString(left.String())
	if !ok {
		return 0, fmt.Errorf("%w: invalid number %q", ErrInvalidConfig, left)
	}
	rightValue, ok := new(big.Rat).SetString(right.String())
	if !ok {
		return 0, fmt.Errorf("%w: invalid number %q", ErrInvalidConfig, right)
	}
	return leftValue.Cmp(rightValue), nil
}

// compareCollection applies one validated membership operator to decoded JSON primitive values.
//
// actual contains decoded JSON values and expected matches the operator's validated scalar or non-empty array
// shape. Comparisons are type-strict and numeric values use exact decimal equality. A changed or malformed
// expected shape returns ErrInvalidConfig rather than panicking.
func compareCollection(actual []any, operator Operator, expected any) (bool, error) {
	contains := func(candidate any) bool {
		return slices.ContainsFunc(actual, func(value any) bool {
			return equalPrimitive(value, candidate)
		})
	}

	switch operator {
	case OperatorContains:
		return contains(expected), nil
	case OperatorContainsAny, OperatorContainsAll:
		// Multi-value membership requires the validated non-empty comparison array.
	case OperatorEqual, OperatorNotEqual, OperatorStartsWith, OperatorEndsWith, OperatorGreaterThan,
		OperatorGreaterOrEqual, OperatorLessThan, OperatorLessOrEqual:
		return false, fmt.Errorf("%w: operator %q is unsupported for collection", ErrInvalidConfig, operator)
	default:
		return false, fmt.Errorf("%w: unknown collection operator %q", ErrInvalidConfig, operator)
	}
	expectedValues, ok := expected.([]any)
	if !ok {
		return false, fmt.Errorf("%w: collection comparison value changed after validation", ErrInvalidConfig)
	}
	if operator == OperatorContainsAny {
		return slices.ContainsFunc(expectedValues, contains), nil
	}
	return !slices.ContainsFunc(expectedValues, func(candidate any) bool { return !contains(candidate) }), nil
}

// isPrimitive reports whether a decoded JSON value is supported as a collection member operand.
func isPrimitive(value any) bool {
	switch value.(type) {
	case string, json.Number, bool:
		return true
	default:
		return false
	}
}

// equalPrimitive compares two supported JSON primitives without cross-type coercion.
func equalPrimitive(left, right any) bool {
	switch leftValue := left.(type) {
	case string:
		rightValue, ok := right.(string)
		return ok && leftValue == rightValue
	case bool:
		rightValue, ok := right.(bool)
		return ok && leftValue == rightValue
	case json.Number:
		rightValue, ok := right.(json.Number)
		if !ok {
			return false
		}
		comparison, err := compareNumbers(leftValue, rightValue)
		return err == nil && comparison == 0
	default:
		return false
	}
}
