package genie

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"time"

	"github.com/jmoiron/sqlx"
)

// Status values an item on the queue can have.
const (
	StatusDone    = "DONE"    // fn finished successfully.
	StatusFailed  = "FAILED"  // all attempts failed or fn returned ErrFail.
	StatusPending = "PENDING" // attempts are still remaining.
	StatusSkipped = "SKIPPED" // fn returned ErrSkip
)

var (
	// ErrSkip can be returned by ApplyFn to indicate that the queued item
	// be skipped immediately.
	ErrSkip = errors.New("skip")

	// ErrFail can be returned by ApplyFn to indicate no further retries
	// should be attempted.
	ErrFail = errors.New("failed")
)

// Open opens a queue based on the spec and returns it. If the keys/tables
// required for the queue are not present, they will be created as needed.
func Open(queueSpec string, opts *Options) (Queue, error) {
	u, err := url.Parse(queueSpec)
	if err != nil {
		return nil, err
	}

	if opts == nil {
		opts = &Options{
			PollInt:      1 * time.Second,
			FnTimeout:    1 * time.Second,
			MaxAttempts:  1,
			RetryBackoff: 10 * time.Second,
		}
	}

	switch u.Scheme {
	case "sqlite3":
		db, err := sqlx.Connect("sqlite3", u.Host)
		if err != nil {
			return nil, err
		}

		if _, err := db.Exec(schema); err != nil {
			_ = db.Close()
			return nil, err
		}

		return &sqlQueue{
			db:   db,
			file: u.Host,
			opts: *opts,
		}, nil

	default:
		return nil, fmt.Errorf("unknown queue type '%s'", u.Scheme)
	}
}

// Queue represents a priority or delay queue.
type Queue interface {
	Push(ctx context.Context, items ...Item) error
	Run(ctx context.Context, fn ApplyFn) error
	Stats() ([]Stats, error)
	Close() error
}

// Options represents optional queue configurations.
type Options struct {
	PollInt      time.Duration
	FnTimeout    time.Duration
	MaxAttempts  int
	RetryBackoff time.Duration
}

// ApplyFn is invoked by the queue instance when an item is available for
// execution.
type ApplyFn func(ctx context.Context, item Item) error

// Item represents an item on the queue.
type Item struct {
	ID          string `json:"id"`
	Type        string `json:"type"`
	Payload     string `json:"payload"`
	Attempt     int    `json:"attempt"`
	MaxAttempts int    `json:"max_attempts"`
}

// Stats represents queue status break down by type.
type Stats struct {
	Type    string `json:"type"`
	Total   int    `json:"total"`
	Done    int    `json:"done"`
	Pending int    `json:"pending"`
	Failed  int    `json:"failed"`
	Skipped int    `json:"skipped"`
}
