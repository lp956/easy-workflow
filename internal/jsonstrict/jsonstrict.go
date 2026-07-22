// Package jsonstrict decodes external JSON with one closed, unambiguous interpretation.
//
// It rejects duplicate object members, unknown typed fields, malformed nesting, and trailing values while preserving
// number text as encoding/json.Number. The package performs no schema-specific validation and retains no input or output
// state; callers remain responsible for domain rules after decoding.
package jsonstrict

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"reflect"
	"strings"
	"unicode/utf8"
)

var (
	// ErrInvalid classifies JSON that cannot be consumed without discarding or ambiguously replacing caller input.
	ErrInvalid = errors.New("jsonstrict: invalid JSON")
)

// Decode consumes exactly one unambiguous JSON value into target using a closed typed schema.
//
// data must contain one complete JSON value. target must be a non-nil pointer accepted by encoding/json; struct targets
// reject unknown fields, while map and interface targets retain their natural open shape. JSON numbers decode as
// json.Number. Errors wrap ErrInvalid, and neither input bytes nor decoded mutable values are retained by this package.
func Decode(data []byte, target any) error {
	if err := Validate(data); err != nil {
		return err
	}
	// Validate member names against the exact typed schema before encoding/json can apply case-insensitive field matching.
	if err := validateExactSchema(data, target); err != nil {
		return err
	}

	// Typed decoding closes struct schemas and preserves decimal text for domain-specific exact-number handling.
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("%w: decode value: %w", ErrInvalid, err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("%w: multiple values", ErrInvalid)
		}
		return fmt.Errorf("%w: read trailing data: %w", ErrInvalid, err)
	}
	return nil
}

// Validate checks one complete JSON value without applying a target schema.
//
// data must contain valid UTF-8 and exactly one JSON value. The validation rejects duplicate object members,
// malformed surrogate escape sequences, trailing values, and syntax errors. It performs no decoding into caller-owned
// values, so it is suitable for open JSON payload boundaries that still require one lossless interpretation.
func Validate(data []byte) error {
	// encoding/json replaces malformed source bytes and accepts lone UTF-16 surrogate escapes; both behaviors lose the
	// distinction between the original input and the decoded replacement character.
	if !utf8.Valid(data) {
		return fmt.Errorf("%w: source is not valid UTF-8", ErrInvalid)
	}
	if err := validateEscapedSurrogates(data); err != nil {
		return err
	}
	// Token validation also proves the value is syntactically complete and rejects last-member-wins ambiguity.
	return validateUniqueValue(data)
}

// validateEscapedSurrogates enforces JSON's UTF-16 escape pairing rules before encoding/json decodes strings.
//
// data must be valid UTF-8; the scan only interprets bytes inside JSON strings and leaves general syntax validation to
// encoding/json. A high surrogate must be immediately followed by a low-surrogate escape, and a low surrogate may
// never appear without that preceding pair. The scan allocates no decoded strings or runes.
func validateEscapedSurrogates(data []byte) error {
	insideString := false
	for index := 0; index < len(data); index++ {
		if !insideString {
			if data[index] == '"' {
				insideString = true
			}
			continue
		}
		if data[index] == '"' {
			insideString = false
			continue
		}
		if data[index] != '\\' {
			continue
		}
		// Skip the escaped byte itself; malformed escape syntax is reported by the token decoder later.
		if index+1 >= len(data) {
			continue
		}
		if data[index+1] != 'u' {
			index++
			continue
		}
		if index+5 >= len(data) {
			return fmt.Errorf("%w: incomplete Unicode escape", ErrInvalid)
		}
		value, ok := decodeHexEscape(data[index+2 : index+6])
		if !ok {
			return fmt.Errorf("%w: invalid Unicode escape", ErrInvalid)
		}
		switch {
		case isHighSurrogate(value):
			// A high surrogate is valid only as the first half of one adjacent escaped pair.
			if index+11 >= len(data) || data[index+6] != '\\' || data[index+7] != 'u' {
				return fmt.Errorf("%w: high surrogate is not paired", ErrInvalid)
			}
			low, lowOK := decodeHexEscape(data[index+8 : index+12])
			if !lowOK || !isLowSurrogate(low) {
				return fmt.Errorf("%w: high surrogate is followed by a non-low surrogate", ErrInvalid)
			}
			index += 11
		case isLowSurrogate(value):
			return fmt.Errorf("%w: low surrogate has no high-surrogate pair", ErrInvalid)
		default:
			index += 5
		}
	}
	return nil
}

// decodeHexEscape decodes exactly four hexadecimal bytes without accepting a partial escape.
func decodeHexEscape(data []byte) (uint16, bool) {
	if len(data) != 4 {
		return 0, false
	}
	var value uint16
	for _, digit := range data {
		value <<= 4
		switch {
		case digit >= '0' && digit <= '9':
			value |= uint16(digit - '0')
		case digit >= 'a' && digit <= 'f':
			value |= uint16(digit-'a') + 10
		case digit >= 'A' && digit <= 'F':
			value |= uint16(digit-'A') + 10
		default:
			return 0, false
		}
	}
	return value, true
}

// isHighSurrogate reports whether value is the first half of a UTF-16 surrogate pair.
func isHighSurrogate(value uint16) bool {
	return value >= 0xD800 && value <= 0xDBFF
}

// isLowSurrogate reports whether value is the second half of a UTF-16 surrogate pair.
func isLowSurrogate(value uint16) bool {
	return value >= 0xDC00 && value <= 0xDFFF
}

// validateExactSchema compares every closed-struct object member with its exact JSON field name.
//
// data must already be valid, unique-member JSON. target may be any value accepted or rejected later by encoding/json;
// non-pointer targets are left to the typed decoder. Maps, interfaces, RawMessage, and custom JSON unmarshallers retain
// their deliberately open shape. Errors wrap ErrInvalid and no decoded values escape this validation pass.
func validateExactSchema(data []byte, target any) error {
	targetType := reflect.TypeOf(target)
	if targetType == nil || targetType.Kind() != reflect.Pointer {
		return nil
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return fmt.Errorf("%w: decode schema value: %w", ErrInvalid, err)
	}
	if err := validateExactValue(value, targetType.Elem()); err != nil {
		return err
	}
	return nil
}

// validateExactValue recursively checks objects that decode into closed struct types.
//
// value is the generic representation of one already-valid JSON value and targetType is its typed destination. Container
// recursion mirrors slices, arrays, maps, and pointers; scalar type mismatches remain the typed decoder's responsibility.
func validateExactValue(value any, targetType reflect.Type) error {
	for targetType.Kind() == reflect.Pointer {
		targetType = targetType.Elem()
	}
	jsonUnmarshalerType := reflect.TypeFor[json.Unmarshaler]()
	if targetType == reflect.TypeFor[json.RawMessage]() || targetType.Implements(jsonUnmarshalerType) ||
		reflect.PointerTo(targetType).Implements(jsonUnmarshalerType) {
		return nil
	}

	switch targetType.Kind() { //nolint:exhaustive // Scalar and non-JSON-capable kinds intentionally share the default path.
	case reflect.Interface:
		return nil
	case reflect.Map:
		return validateExactMap(value, targetType.Elem())
	case reflect.Slice, reflect.Array:
		return validateExactArray(value, targetType.Elem())
	case reflect.Struct:
		return validateExactStruct(value, targetType)
	default:
		return nil
	}
}

// validateExactMap recursively validates values from one JSON object against an open typed map value schema.
func validateExactMap(value any, elementType reflect.Type) error {
	object, ok := value.(map[string]any)
	if !ok {
		return nil
	}
	for _, member := range object {
		if err := validateExactValue(member, elementType); err != nil {
			return err
		}
	}
	return nil
}

// validateExactArray recursively validates elements from one JSON array against its typed element schema.
func validateExactArray(value any, elementType reflect.Type) error {
	array, ok := value.([]any)
	if !ok {
		return nil
	}
	for _, member := range array {
		if err := validateExactValue(member, elementType); err != nil {
			return err
		}
	}
	return nil
}

// validateExactStruct rejects non-canonical member names and recursively validates each known field value.
func validateExactStruct(value any, targetType reflect.Type) error {
	object, ok := value.(map[string]any)
	if !ok {
		return nil
	}
	fields := exactJSONFields(targetType)
	for name, member := range object {
		fieldType, exists := fields[name]
		if !exists {
			return fmt.Errorf("%w: object member %q does not exactly match the typed schema", ErrInvalid, name)
		}
		if err := validateExactValue(member, fieldType); err != nil {
			return err
		}
	}
	return nil
}

// exactJSONFields returns the exact member names visible on one exported struct schema.
//
// Anonymous exported structs without an explicit name contribute promoted fields. Tagged exclusions are omitted, and
// collisions remain for encoding/json's typed decoder to classify. The returned map is request-local and read-only.
func exactJSONFields(targetType reflect.Type) map[string]reflect.Type {
	fields := make(map[string]reflect.Type)
	for field := range targetType.Fields() {
		if field.PkgPath != "" {
			continue
		}
		tagName, _, _ := strings.Cut(field.Tag.Get("json"), ",")
		if tagName == "-" {
			continue
		}
		promotedType := field.Type
		for promotedType.Kind() == reflect.Pointer {
			promotedType = promotedType.Elem()
		}
		if field.Anonymous && tagName == "" && promotedType.Kind() == reflect.Struct {
			maps.Copy(fields, exactJSONFields(promotedType))
			continue
		}
		if tagName == "" {
			tagName = field.Name
		}
		fields[tagName] = field.Type
	}
	return fields
}

// validateUniqueValue scans one JSON value and rejects duplicate member names at every object depth.
//
// data is interpreted only as JSON tokens. The scan consumes the complete top-level value, rejects trailing data, and
// allocates one short-lived member set per nested object. Errors wrap ErrInvalid and retain syntax causes where available.
func validateUniqueValue(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	first, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("%w: read first token: %w", ErrInvalid, err)
	}
	if err := scanValue(decoder, first); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("%w: multiple values", ErrInvalid)
		}
		return fmt.Errorf("%w: read trailing token: %w", ErrInvalid, err)
	}
	return nil
}

// scanValue consumes a token-started value and recursively validates every nested container.
//
// decoder must be positioned immediately after first. Scalars require no further work; arrays and objects consume their
// matching closing delimiters. Unexpected closing delimiters wrap ErrInvalid instead of being treated as scalar values.
func scanValue(decoder *json.Decoder, first json.Token) error {
	delimiter, isDelimiter := first.(json.Delim)
	if !isDelimiter {
		return nil
	}
	switch delimiter {
	case '{':
		return scanObject(decoder)
	case '[':
		return scanArray(decoder)
	default:
		return fmt.Errorf("%w: unexpected closing delimiter", ErrInvalid)
	}
}

// scanObject consumes one opened object while enforcing member-name uniqueness at that nesting depth.
//
// decoder must be positioned after an opening brace. Nested values delegate to scanValue, and the matching closing brace
// is consumed before return. The member set is request-local and discarded when this object has been validated.
func scanObject(decoder *json.Decoder) error {
	seen := make(map[string]struct{})
	for decoder.More() {
		nameToken, err := decoder.Token()
		if err != nil {
			return fmt.Errorf("%w: read object member name: %w", ErrInvalid, err)
		}
		name, ok := nameToken.(string)
		if !ok {
			return fmt.Errorf("%w: object member name is not a string", ErrInvalid)
		}
		if _, exists := seen[name]; exists {
			return fmt.Errorf("%w: duplicate object member %q", ErrInvalid, name)
		}
		seen[name] = struct{}{}
		valueToken, err := decoder.Token()
		if err != nil {
			return fmt.Errorf("%w: read object member value: %w", ErrInvalid, err)
		}
		if err := scanValue(decoder, valueToken); err != nil {
			return err
		}
	}
	closing, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("%w: read object closing delimiter: %w", ErrInvalid, err)
	}
	if closing != json.Delim('}') {
		return fmt.Errorf("%w: object has invalid closing delimiter", ErrInvalid)
	}
	return nil
}

// scanArray consumes one opened array while recursively validating objects in every element.
//
// decoder must be positioned after an opening bracket. Scalar elements allocate no additional state, nested containers
// delegate to scanValue, and the matching closing bracket is consumed before a successful return.
func scanArray(decoder *json.Decoder) error {
	for decoder.More() {
		valueToken, err := decoder.Token()
		if err != nil {
			return fmt.Errorf("%w: read array member: %w", ErrInvalid, err)
		}
		if err := scanValue(decoder, valueToken); err != nil {
			return err
		}
	}
	closing, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("%w: read array closing delimiter: %w", ErrInvalid, err)
	}
	if closing != json.Delim(']') {
		return fmt.Errorf("%w: array has invalid closing delimiter", ErrInvalid)
	}
	return nil
}
