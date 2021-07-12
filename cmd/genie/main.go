package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/spy16/genie"
)

var (
	bindAddr  = flag.String("bind", "0.0.0.0:9090", "Bind address for portal")
	jobTypes  = flag.String("types", "log,webhook", "Job types to enable")
	queueSpec = flag.String("spec", "sqlite3://genie.db", "Queue backend specification")
)

func main() {
	flag.Parse()

	q, err := genie.Open(*queueSpec, strings.Split(*jobTypes, ","))
	if err != nil {
		fmt.Printf("failed to open file: %v\n", err)
		os.Exit(1)
	}
	defer q.Close()

	go func() {
		if err := q.Run(context.Background(), logFn); err != nil {
			log.Printf("queue.Run() exited: %v", err)
		}
	}()

	log.Printf("starting server on http://%s...", *bindAddr)
	if err := http.ListenAndServe(*bindAddr, genie.Router(q)); err != nil {
		log.Fatalf("portal exited with error: %v", err)
	} else {
		log.Println("portal exited gracefully")
	}
}

func logFn(ctx context.Context, item genie.Item) ([]byte, error) {
	log.Printf("apply(%v)", item)
	return nil, nil
}
