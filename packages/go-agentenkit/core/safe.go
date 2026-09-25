package core

import (
	"fmt"
	"runtime/debug"
)

// PanicError is a panic caught by CallSafely. Stack is where it happened.
type PanicError struct {
	Value any
	Stack []byte
}

func (e *PanicError) Error() string { return fmt.Sprintf("panic: %v", e.Value) }

// CallSafely runs fn and turns a panic into a *PanicError. A worker calls
// user code (handlers, hooks, tools) on goroutines of its own; one panic
// there must fail that job, not end the process and every job beside it.
func CallSafely(fn func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = &PanicError{Value: r, Stack: debug.Stack()}
		}
	}()
	return fn()
}
