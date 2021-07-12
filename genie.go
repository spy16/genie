package genie

import (
	"fmt"
	"net/url"
)

// Open opens a queue based on the spec and returns it. If the keys/tables
// required for the queue are not present, they will be created as needed.
func Open(queueSpec string, enableTypes []string, h Handler) (Queue, error) {
	u, err := url.Parse(queueSpec)
	if err != nil {
		return nil, err
	}

	switch u.Scheme {
	case "sqlite3":
		return newSQLQueue(u, enableTypes, h)

	default:
		return nil, fmt.Errorf("unknown queue type '%s'", u.Scheme)
	}
}
