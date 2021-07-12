package genie

import (
	"fmt"
	"net/url"
	"time"

	"github.com/jmoiron/sqlx"
)

// Open opens a queue based on the spec and returns it. If the keys/tables
// required for the queue are not present, they will be created as needed.
func Open(queueSpec string) (Queue, error) {
	u, err := url.Parse(queueSpec)
	if err != nil {
		return nil, err
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
			opts: Options{
				PollInt:      1 * time.Second,
				FnTimeout:    1 * time.Second,
				MaxAttempts:  1,
				RetryBackoff: 10 * time.Second,
			},
		}, nil

	default:
		return nil, fmt.Errorf("unknown queue type '%s'", u.Scheme)
	}
}
