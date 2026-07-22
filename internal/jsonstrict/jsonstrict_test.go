// Package jsonstrict_test verifies the exported strict-decoding boundary without relying on scanner internals.
package jsonstrict_test

import (
	"errors"
	"testing"

	"github.com/lvpeng/easy-workflow/internal/jsonstrict"
)

// strictRecord is the smallest closed schema used to distinguish exact JSON names from case aliases.
type strictRecord struct {
	// ID is bound only by the explicitly tagged lower-case member name.
	ID string `json:"id"`
}

// TestDecodeRequiresExactStructFieldNames verifies encoding/json's case folding cannot create last-wins ambiguity.
func TestDecodeRequiresExactStructFieldNames(t *testing.T) {
	t.Parallel()

	for _, data := range []string{
		`{"ID":"alias"}`,
		`{"id":"canonical","ID":"alias"}`,
	} {
		var target strictRecord
		if err := jsonstrict.Decode([]byte(data), &target); !errors.Is(err, jsonstrict.ErrInvalid) {
			t.Fatalf("Decode(%s) error = %v, want ErrInvalid", data, err)
		}
	}
}

// TestDecodeRejectsInvalidUTF8 verifies malformed source bytes are not normalized into replacement characters.
func TestDecodeRejectsInvalidUTF8(t *testing.T) {
	t.Parallel()

	data := []byte{'{', '"', 'i', 'd', '"', ':', '"', 0xff, '"', '}'}
	var target strictRecord
	if err := jsonstrict.Decode(data, &target); !errors.Is(err, jsonstrict.ErrInvalid) {
		t.Fatalf("Decode(invalid UTF-8) error = %v, want ErrInvalid", err)
	}
}

// TestValidateRejectsUnpairedSurrogateEscapes verifies escaped UTF-16 values cannot be silently normalized.
func TestValidateRejectsUnpairedSurrogateEscapes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		data string
		want bool
	}{
		{name: "lone high", data: `{"value":"\uD800"}`, want: false},
		{name: "lone low", data: `{"value":"\uDC00"}`, want: false},
		{name: "mismatched pair", data: `{"value":"\uD800\uD800"}`, want: false},
		{name: "valid pair", data: `{"value":"\uD83D\uDE00"}`, want: true},
		{name: "surrogate object key", data: `{"\uD800":"value"}`, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := jsonstrict.Validate([]byte(test.data))
			if test.want {
				if err != nil {
					t.Fatalf("Validate() error = %v, want nil", err)
				}
				return
			}
			if !errors.Is(err, jsonstrict.ErrInvalid) {
				t.Fatalf("Validate() error = %v, want ErrInvalid", err)
			}
		})
	}
}
