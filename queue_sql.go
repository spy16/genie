package genie

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3" // sqlite3 driver

)

// sqlQueue implements a simple disk-backed queue using SQLite3 database.
// A single table is used to store the queue items with their insertion
// timestamp defining the execution order.
type sqlQueue struct {
	db   *sqlx.DB
	file string
	opts Options
}

// Push enqueues all items into the queue with pending status.
func (q *sqlQueue) Push(ctx context.Context, items ...Item) error {
	const insertQuery = `INSERT INTO queue (id, type, status, created_at, updated_at, payload, next_attempt_at)
		VALUES (:id, :type, :status, :created_at, :updated_at, :payload, :next_attempt_at)`

	t := time.Now().UTC()

	qItems := make([]sqlQueueItem, len(items), len(items))
	for i, item := range items {
		// choose the maximum of default max attempts or item limit.
		maxAttempts := q.opts.MaxAttempts
		if item.MaxAttempts > 0 && item.MaxAttempts < maxAttempts {
			maxAttempts = item.MaxAttempts
		}

		qItems[i] = sqlQueueItem{
			ID:            item.ID,
			Type:          item.Type,
			Status:        StatusPending,
			Payload:       item.Payload,
			MaxAttempts:   maxAttempts,
			CreatedAt:     t,
			UpdatedAt:     t,
			NextAttemptAt: item.NextAttempt.UTC(),
		}
	}

	_, err := q.db.NamedExecContext(ctx, insertQuery, qItems)
	return err
}

// Run starts the worker loop that fetches next item from the queue and applies
// the given func. Runs until context is cancelled. fn can return nil, ErrFail,
// ErrSkipped to move to DONE, FAILED or SKIPPED terminal statuses directly. If
// fn returns any other error, it will remain in PENDING state and will be retried
// after sometime.
func (q *sqlQueue) Run(ctx context.Context, fn ApplyFn) error {
	timer := time.NewTimer(0)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil

		case <-timer.C:
			timer.Reset(q.opts.PollInt)

			records, err := q.getBatch(ctx)
			if err != nil {
				log.Printf("failed to read next batch: %v", err)
			} else if len(records) == 0 {
				continue
			}

			for _, rec := range records {
				if err := q.processRecord(ctx, rec, fn); err != nil {
					log.Printf("failed to process '%s': %v", rec.ID, err)
				}
			}
		}
	}
}

// Stats returns entire queue statistics broken down by type.
func (q *sqlQueue) Stats() ([]Stats, error) {
	const query = `SELECT type,
	       count(*)                                       AS total,
	       count(case when status = 'DONE' then 1 end)    AS done,
	       count(case when status = 'PENDING' then 1 end) AS pending,
	       count(case when status = 'SKIPPED' then 1 end) AS skipped,
	       count(case when status = 'FAILED' then 1 end)  AS failed
	FROM queue
	GROUP BY type;`
	var stats []Stats
	if err := q.db.Select(&stats, query); err != nil {
		return nil, err
	}
	return stats, nil
}

func (q *sqlQueue) getBatch(ctx context.Context) ([]sqlQueueItem, error) {
	const selectQuery = `SELECT * FROM queue
		WHERE status='PENDING' AND  next_attempt_at <= $1 
		ORDER BY next_attempt_at
		LIMIT 10;`

	var records []sqlQueueItem
	err := q.db.SelectContext(ctx, &records, selectQuery, time.Now().UTC())
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	return records, nil
}

func (q *sqlQueue) processRecord(ctx context.Context, rec sqlQueueItem, fn ApplyFn) error {
	fnCtx, cancel := context.WithTimeout(ctx, q.opts.FnTimeout)
	defer cancel()

	fnErr := fn(fnCtx, rec.Item())
	rec.Attempts++

	if fnErr == nil {
		rec.Status = StatusDone
	} else {
		if errors.Is(fnErr, ErrFail) || rec.Attempts >= rec.MaxAttempts {
			rec.Status = StatusFailed
		} else if errors.Is(fnErr, ErrSkip) {
			rec.Status = StatusSkipped
		} else {
			rec.Status = StatusPending
		}

		rec.NextAttemptAt = time.Now().Add(q.opts.RetryBackoff).UTC()
		rec.LastError = sql.NullString{
			Valid:  true,
			String: fnErr.Error(),
		}
	}

	const updateQuery = `UPDATE queue
		SET status=:status, 
		    last_error=:last_error, 
		    next_attempt_at=:next_attempt_at, 
		    attempts=:attempts,
		    updated_at=current_timestamp
		WHERE id=:id`
	_, err := q.db.NamedExecContext(ctx, updateQuery, rec)
	return err
}

func (q *sqlQueue) String() string { return fmt.Sprintf("sqlQueue<file='%s'>", q.file) }

func (q *sqlQueue) Close() error { return q.db.Close() }

const schema = `
	CREATE TABLE IF NOT EXISTS queue (
		id TEXT PRIMARY KEY,
		type TEXT NOT NULL,
		payload TEXT NOT NULL,
		status TEXT NOT NULL,
		max_attempts INTEGER NOT NULL,
		attempts INTEGER NOT NULL DEFAULT 0,
		next_attempt_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		last_error TEXT,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);
	
	CREATE INDEX IF NOT EXISTS index_type ON queue (type COLLATE binary);
	CREATE INDEX IF NOT EXISTS index_next_attempt_at ON queue (next_attempt_at);
`

// sqlQueueItem should always match the above schema.
type sqlQueueItem struct {
	// Item attributes.
	ID          string    `json:"id" db:"id"`
	Type        string    `json:"type" db:"type"`
	Status      string    `json:"status" db:"status"`
	Payload     string    `json:"payload" db:"payload"`
	CreatedAt   time.Time `json:"created_at" db:"created_at"`
	UpdatedAt   time.Time `json:"updated_at" db:"updated_at"`
	MaxAttempts int       `json:"max_attempts" db:"max_attempts"`

	// Execution info.
	Attempts      int            `json:"attempts" db:"attempts"`
	LastError     sql.NullString `json:"last_error" db:"last_error"`
	NextAttemptAt time.Time      `json:"next_attempt_at" db:"next_attempt_at"`
}

func (rec sqlQueueItem) Item() Item {
	return Item{
		ID:          rec.ID,
		Type:        rec.Type,
		Payload:     rec.Payload,
		Attempt:     rec.Attempts,
		MaxAttempts: rec.MaxAttempts,
		NextAttempt: rec.NextAttemptAt.Local(),
	}
}
