package genie

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net/url"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3" // sqlite3 driver

)

func newSQLQueue(u *url.URL, types []string, h Handler) (*sqlQueue, error) {
	db, err := sqlx.Connect("sqlite3", u.Host)
	if err != nil {
		return nil, err
	}

	if _, err := db.Exec(schema); err != nil {
		_ = db.Close()
		return nil, err
	}

	return &sqlQueue{
		db:     db,
		file:   u.Host,
		types:  types,
		handle: h,
		opts: Options{
			PollInt:      1 * time.Second,
			FnTimeout:    1 * time.Second,
			MaxAttempts:  1,
			RetryBackoff: 10 * time.Second,
		},
	}, nil
}

// sqlQueue implements a simple disk-backed queue using SQLite3 database.
// A single table is used to store the queue items with their insertion
// timestamp defining the execution order.
type sqlQueue struct {
	db     *sqlx.DB
	file   string
	opts   Options
	types  []string
	handle Handler
}

// Push enqueues all items into the queue with pending status.
func (q *sqlQueue) Push(ctx context.Context, items ...Item) error {
	const insertQuery = `
		INSERT INTO queue (id, type, group_id, status, created_at, updated_at, payload, max_attempts, next_attempt_at)
		VALUES (:id, :type, :group_id, :status, :created_at, :updated_at, :payload, :max_attempts, :next_attempt_at)`

	t := time.Now().UTC()

	qItems := make([]sqlQueueItem, len(items), len(items))
	for i, item := range items {
		if err := q.handle.Sanitize(ctx, &item); err != nil {
			return err
		}

		// choose the maximum of default max attempts or item limit.
		maxAttempts := q.opts.MaxAttempts
		if item.MaxAttempts > 0 && item.MaxAttempts < maxAttempts {
			maxAttempts = item.MaxAttempts
		}
		if maxAttempts <= 0 {
			maxAttempts = 1
		}

		qItems[i] = sqlQueueItem{
			ID:            item.ID,
			Type:          item.Type,
			Status:        StatusPending,
			Payload:       item.Payload,
			GroupID:       item.GroupID,
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
func (q *sqlQueue) Run(ctx context.Context) error {
	timer := time.NewTimer(0)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil

		case <-timer.C:
			timer.Reset(q.opts.PollInt)

			records, err := q.getBatch(ctx, q.types)
			if err != nil {
				log.Printf("failed to read next batch: %v", err)
			} else if len(records) == 0 {
				continue
			}

			for _, rec := range records {
				if err := q.processRecord(ctx, rec, q.handle); err != nil {
					log.Printf("failed to process '%s': %v", rec.ID, err)
				}
			}
		}
	}
}

// Stats returns entire queue statistics broken down by type.
func (q *sqlQueue) Stats() ([]Stats, error) {
	const query = `SELECT type, group_id, 
	       count(*)                                       AS total,
	       count(case when status = 'DONE' then 1 end)    AS done,
	       count(case when status = 'PENDING' then 1 end) AS pending,
	       count(case when status = 'SKIPPED' then 1 end) AS skipped,
	       count(case when status = 'FAILED' then 1 end)  AS failed
	FROM queue
	GROUP BY type, group_id;`
	var stats []Stats
	if err := q.db.Select(&stats, query); err != nil {
		return nil, err
	}
	return stats, nil
}

// ForEach enumerates all jobs of given type with given status and applies
// fn to them. Stops if fn returns error or when all jobs are considered.
func (q *sqlQueue) ForEach(ctx context.Context, groupID, status string, fn Fn) error {
	const query = `SELECT * FROM queue WHERE group_id=$1 AND status=$2`

	rows, err := q.db.QueryxContext(ctx, query, groupID, status)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		return err
	}

	var rec sqlQueueItem
	for rows.Next() {
		if err := rows.StructScan(&rec); err != nil {
			return err
		}
		if err := fn(ctx, rec.Item()); err != nil {
			return err
		}
	}

	return nil
}

func (q *sqlQueue) JobTypes() []string { return q.types }

func (q *sqlQueue) getBatch(ctx context.Context, supported []string) ([]sqlQueueItem, error) {
	const selectQuery = `SELECT * FROM queue
		WHERE status='PENDING' AND  next_attempt_at <= ? AND type IN (?)
		ORDER BY next_attempt_at
		LIMIT 10;`

	query, args, err := sqlx.In(selectQuery, time.Now().UTC(), supported)
	if err != nil {
		return nil, err
	}

	var records []sqlQueueItem
	err = q.db.SelectContext(ctx, &records, query, args...)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	return records, nil
}

func (q *sqlQueue) processRecord(ctx context.Context, rec sqlQueueItem, h Handler) error {
	fnCtx, cancel := context.WithTimeout(ctx, q.opts.FnTimeout)
	defer cancel()

	result, fnErr := h.Handle(fnCtx, rec.Item())
	rec.Attempts++

	if fnErr == nil {
		rec.Status = StatusDone
		rec.Result = sql.NullString{
			Valid:  true,
			String: string(result),
		}
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
		    updated_at=current_timestamp,
		    result=:result
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
		group_id TEXT NOT NULL,
		payload TEXT NOT NULL,
		status TEXT NOT NULL,
		max_attempts INTEGER NOT NULL,
		attempts INTEGER NOT NULL DEFAULT 0,
		next_attempt_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		result TEXT,
		last_error TEXT,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);
	
	CREATE INDEX IF NOT EXISTS index_type ON queue (type COLLATE binary);
	CREATE INDEX IF NOT EXISTS index_group_id ON queue (type COLLATE binary);
	CREATE INDEX IF NOT EXISTS index_next_attempt_at ON queue (next_attempt_at);
`

// sqlQueueItem should always match the above schema.
type sqlQueueItem struct {
	// Item attributes.
	ID          string         `json:"id" db:"id"`
	Type        string         `json:"type" db:"type"`
	Status      string         `json:"status" db:"status"`
	GroupID     string         `json:"group_id" db:"group_id"`
	Payload     string         `json:"payload" db:"payload"`
	CreatedAt   time.Time      `json:"created_at" db:"created_at"`
	UpdatedAt   time.Time      `json:"updated_at" db:"updated_at"`
	MaxAttempts int            `json:"max_attempts" db:"max_attempts"`
	Result      sql.NullString `json:"result" db:"result"`

	// Execution info.
	Attempts      int            `json:"attempts" db:"attempts"`
	LastError     sql.NullString `json:"last_error" db:"last_error"`
	NextAttemptAt time.Time      `json:"next_attempt_at" db:"next_attempt_at"`
}

func (rec sqlQueueItem) Item() Item {
	return Item{
		ID:          rec.ID,
		Type:        rec.Type,
		Result:      rec.Result.String,
		Payload:     rec.Payload,
		GroupID:     rec.GroupID,
		Attempt:     rec.Attempts,
		MaxAttempts: rec.MaxAttempts,
		NextAttempt: rec.NextAttemptAt.Local(),
	}
}
