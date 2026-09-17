package uppgift

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"reflect"
	"time"
)

// ErrNoHandler indicates that no handler is registered for a job kind.
var ErrNoHandler = errors.New("uppgift: handler not registered")

// UnknownKindError indicates that the multiplexer received an unregistered job kind.
type UnknownKindError struct {
	kind string
}

func (e *UnknownKindError) Error() string {
	return fmt.Sprintf("uppgift: handler for kind %q is not registered", e.kind)
}

func (e *UnknownKindError) Unwrap() error {
	return ErrNoHandler
}

func (e *UnknownKindError) Kind() string {
	return e.kind
}

// DiscardError indicates that the worker must discard the job immediately without retries.
type DiscardError struct {
	cause error
}

func (e *DiscardError) Error() string {
	if e.cause != nil {
		return fmt.Sprintf("uppgift: job discarded: %v", e.cause)
	}

	return "uppgift: job discarded"
}

func (e *DiscardError) Unwrap() error {
	return e.cause
}

// DiscardJob returns a [DiscardError] that stops further retries for the job.
// If an error is provided, DiscardJob stores it as the underlying cause.
func DiscardJob(err ...error) error {
	var cause error
	if len(err) > 0 {
		cause = err[0]
	}
	return &DiscardError{cause: cause}
}

// SnoozeError indicates that the worker must postpone the job to a future time.
type SnoozeError struct {
	until time.Time
}

func (e *SnoozeError) Error() string {
	return fmt.Sprintf("uppgift: job will be snoozed until %s", e.until.Format(time.RFC3339))
}

func (e *SnoozeError) Until() time.Time {
	return e.until
}

// SnoozeJob returns a [SnoozeError] that reschedules the job at the given time
// without incrementing the attempt count.
func SnoozeJob(until time.Time) error {
	return &SnoozeError{until: until.UTC()}
}

// SnoozeJobFor returns a [SnoozeError] that reschedules the job after duration d
// without incrementing the attempt count.
func SnoozeJobFor(d time.Duration) error {
	return &SnoozeError{until: time.Now().UTC().Add(d)}
}

// Handler processes jobs of type T.
// Return nil for successful completion.
// Return an error to mark the attempt as failed.
// Return [SnoozeJob] or [DiscardJob] for explicit lifecycle control.
type Handler[T JobArgs] interface {
	Handle(ctx context.Context, job *Job[T]) error
}

// HandlerFunc adapts an ordinary function to implement [Handler].
type HandlerFunc[T JobArgs] func(ctx context.Context, job *Job[T]) error

func (f HandlerFunc[T]) Handle(ctx context.Context, job *Job[T]) error {
	return f(ctx, job)
}

type rawHandler func(ctx context.Context, meta Meta, payload []byte) error

// Mux routes jobs to registered [Handler] implementations by kind.
type Mux struct {
	handlers map[string]rawHandler
}

// NewMux creates a new [Mux].
func NewMux() *Mux {
	return &Mux{
		handlers: make(map[string]rawHandler),
	}
}

// Register registers a [Handler] for the kind defined by T. Only one handler can
// be registered per kind. Register is not safe for concurrent use. Call Register
// during application initialization before processing jobs.
func (m *Mux) Register[T JobArgs](h Handler[T]) {
	var t T
	if reflect.TypeFor[T]().Kind() == reflect.Pointer {
		panic(fmt.Sprintf("uppgift: JobArgs type parameter for kind %q must be a struct value, not a pointer (%T)", reflect.TypeFor[T]().String(), t))
	}

	kind := t.Kind()

	if h == nil {
		panic("uppgift: nil handler")
	}

	if _, exists := m.handlers[kind]; exists {
		panic(fmt.Sprintf("uppgift: handler already registered for kind %q", kind))
	}

	m.handlers[kind] = func(ctx context.Context, meta Meta, payload []byte) error {
		var args T
		err := json.Unmarshal(payload, &args)
		if err != nil {
			return fmt.Errorf("uppgift: unmarshal payload for kind %q: %w", kind, err)
		}

		return h.Handle(ctx, &Job[T]{
			Meta: meta,
			Args: args,
		})
	}
}

// Dispatch unmarshals the payload and executes the registered [Handler] for the specified kind.
func (m *Mux) Dispatch(ctx context.Context, meta Meta, kind string, payload []byte) error {
	h, ok := m.handlers[kind]
	if !ok {
		return &UnknownKindError{kind: kind}
	}

	return h(ctx, meta, payload)
}
