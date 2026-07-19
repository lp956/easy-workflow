// Package nilguard detects absent dependencies hidden inside interface values.
//
// It is limited to boundary validation for optional or injected collaborators. The package does not dereference values,
// recover panics, mutate inputs, or decide whether a non-nil dependency is otherwise usable. Calls are pure and safe for
// concurrent use.
package nilguard

import "reflect"

// IsNil reports whether value is nil either directly or through a nil-capable dynamic interface value.
//
// value may have any dynamic type. Pointers, maps, slices, functions, channels, and nested interface values are checked
// with reflection; all other concrete values are non-nil. The function performs no dereference and therefore cannot invoke
// methods on a typed-nil collaborator.
func IsNil(value any) bool {
	// A nil interface has no dynamic value and must not reach reflection.
	if value == nil {
		return true
	}

	// Reflection is restricted to kinds for which IsNil is defined; all ordinary concrete values are present.
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	case reflect.Invalid, reflect.Array, reflect.Bool, reflect.Complex64, reflect.Complex128, reflect.Float32,
		reflect.Float64, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.String,
		reflect.Struct, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.UnsafePointer:
		return false
	default:
		return false
	}
}
