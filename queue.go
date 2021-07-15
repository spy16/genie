package genie

import (
	"context"
	"errors"
	"time"
)

// Status values an item on the queue can have.
const (
	StatusDone    = "DONE"    // fn finished successfully.
	StatusFailed  = "FAILED"  // all attempts failed or fn returned ErrFail.
	StatusPending = "PENDING" // attempts are still remaining.
	StatusSkipped = "SKIPPED" // fn returned ErrSkip
)

var (
	// ErrSkip can be returned by HandlerFn to indicate that the queued item
	// be skipped immediately.
	ErrSkip = errors.New("skip")

	// ErrFail can be returned by HandlerFn to indicate no further retries
	// should be attempted.
	ErrFail = errors.New("failed")
)

// Queue represents a priority or delay queue.
type Queue interface {
	ForEach(ctx context.Context, groupID, status string, fn Fn) error
	Push(ctx context.Context, items ...Item) error
	Run(ctx context.Context) error
	Stats() ([]Stats, error)
	JobTypes() []string
	Close() error
}

// Options represents optional queue configurations.
type Options struct {
	PollInt      time.Duration
	FnTimeout    time.Duration
	MaxAttempts  int
	RetryBackoff time.Duration
}

// HandlerFn is invoked by the queue instance when an item is available for
// execution or for validation when items are being enqueued.
type Handler interface {
	Handle(ctx context.Context, item Item) ([]byte, error)
	Sanitize(ctx context.Context, item *Item) error
}

type Fn func(ctx context.Context, item Item) error

// HandlerFn implements Handler using Go native func value.
type HandlerFn func(ctx context.Context, item Item) ([]byte, error)

func (h HandlerFn) Handle(ctx context.Context, item Item) ([]byte, error) { return h(ctx, item) }
func (h HandlerFn) Sanitize(_ context.Context, _ *Item) error             { return nil }

// Item represents an item on the queue.
type Item struct {
	ID          string    `json:"id"`
	Type        string    `json:"type"`
	Payload     string    `json:"payload"`
	GroupID     string    `json:"group_id"`
	Attempt     int       `json:"attempt"`
	MaxAttempts int       `json:"max_attempts"`
	NextAttempt time.Time `json:"next_attempt"`
	Result      string    `json:"result"`
}

// Stats represents queue status break down by type.
type Stats struct {
	GroupID string `json:"group_id" db:"group_id"`
	Type    string `json:"type"`
	Total   int    `json:"total"`
	Done    int    `json:"done"`
	Pending int    `json:"pending"`
	Failed  int    `json:"failed"`
	Skipped int    `json:"skipped"`
}
