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
	// Token validation runs first because encoding/json otherwise accepts duplicate object names with last-value-wins.
	if err := validateUniqueValue(data); err != nil {
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
