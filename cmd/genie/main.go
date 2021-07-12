package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/spy16/genie"

	"github.com/spy16/genie/portal"
)

var (
	bindAddr  = flag.String("bind", "0.0.0.0:9090", "Bind address for portal")
	queueSpec = flag.String("spec", "sqlite3://genie.db", "Queue backend specification")
)

func main() {
	flag.Parse()

	q, err := genie.Open(*queueSpec, nil)
	if err != nil {
		fmt.Printf("failed to open file: %v\n", err)
		os.Exit(1)
	}
	defer q.Close()

	go func() {
		if err := q.Run(context.Background(), handlers); err != nil {
			log.Printf("queue.Run() exited: %v", err)
		}
	}()

	log.Printf("starting server on http://%s...", *bindAddr)
	if err := http.ListenAndServe(*bindAddr, portal.New(q)); err != nil {
		log.Fatalf("portal exited with error: %v", err)
	} else {
		log.Println("portal exited gracefully")
	}
}

var handlers = map[string]genie.ApplyFn{
	"WebHook": func(ctx context.Context, item genie.Item) ([]byte, error) {
		log.Printf("item: %v", item)
		return []byte("foo"), errors.New("failed")
	},
}
