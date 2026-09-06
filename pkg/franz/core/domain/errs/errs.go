// Package errs is the domain error vocabulary. It has no framework or transport
// imports; the grpcgateway adapter maps these onto gRPC status codes and
// google.rpc.BadRequest details (003.1 "Errors").
package errs

import (
	"errors"
	"fmt"
)

// Kind classifies a domain error. It maps 1:1 onto the gRPC status codes in
// 003.1.
type Kind int

const (
	// Internal — unexpected; the caller should not see details.
	Internal Kind = iota
	// InvalidArgument — malformed request (bad selector, empty update_mask, …).
	InvalidArgument
	// NotFound — no resource with that name.
	NotFound
	// AlreadyExists — create with a name already taken.
	AlreadyExists
	// FailedPrecondition — operation invalid for the current state.
	FailedPrecondition
	// PermissionDenied — authorization check failed.
	PermissionDenied
	// ResourceExhausted — a quota was hit.
	ResourceExhausted
)

// FieldViolation names one bad request field (becomes a
// google.rpc.BadRequest.FieldViolation).
type FieldViolation struct {
	Field       string
	Description string
}

// Error is a domain error carrying a Kind, a message, optional field violations,
// and an optional wrapped cause.
type Error struct {
	Kind       Kind
	Msg        string
	Violations []FieldViolation
	cause      error
}

func (e *Error) Error() string {
	if e.cause != nil {
		return fmt.Sprintf("%s: %v", e.Msg, e.cause)
	}
	return e.Msg
}

// Unwrap exposes the wrapped cause to errors.Is / errors.As.
func (e *Error) Unwrap() error { return e.cause }

// Wrap attaches a cause and returns e (for chaining).
func (e *Error) Wrap(cause error) *Error {
	e.cause = cause
	return e
}

// AddViolation appends a field violation and returns e.
func (e *Error) AddViolation(field, desc string) *Error {
	e.Violations = append(e.Violations, FieldViolation{Field: field, Description: desc})
	return e
}

func newf(k Kind, format string, args ...any) *Error {
	return &Error{Kind: k, Msg: fmt.Sprintf(format, args...)}
}

// Constructors.

func Internalf(format string, args ...any) *Error { return newf(Internal, format, args...) }
func Invalidf(format string, args ...any) *Error  { return newf(InvalidArgument, format, args...) }
func NotFoundf(format string, args ...any) *Error { return newf(NotFound, format, args...) }
func Existsf(format string, args ...any) *Error   { return newf(AlreadyExists, format, args...) }
func Preconditionf(format string, args ...any) *Error {
	return newf(FailedPrecondition, format, args...)
}
func Deniedf(format string, args ...any) *Error    { return newf(PermissionDenied, format, args...) }
func Exhaustedf(format string, args ...any) *Error { return newf(ResourceExhausted, format, args...) }

// InvalidField is an INVALID_ARGUMENT error carrying a single field violation.
func InvalidField(field, desc string) *Error {
	return (&Error{Kind: InvalidArgument, Msg: "invalid request"}).AddViolation(field, desc)
}

// As extracts the *Error from an error chain, if present.
func As(err error) (*Error, bool) {
	var e *Error
	if errors.As(err, &e) {
		return e, true
	}
	return nil, false
}

// KindOf reports the Kind of err, or Internal if it is not a domain error.
func KindOf(err error) Kind {
	if e, ok := As(err); ok {
		return e.Kind
	}
	return Internal
}
