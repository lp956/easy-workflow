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
