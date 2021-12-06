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
	luaPaths  = flag.String("lua-dirs", "./", "Lua paths to detect and load")
	initLua   = flag.String("init", "genie.lua", "Lua file to use as entry point")
	queueSpec = flag.String("spec", "sqlite3://genie.db", "Queue backend specification")
)

func main() {
	flag.Parse()

	g, err := genie.New(*queueSpec, *initLua, strings.Split(*luaPaths, ","))
	if err != nil {
		fmt.Printf("failed to open file: %v\n", err)
		os.Exit(1)
	}

	go func() {
		if err := g.Run(context.Background()); err != nil {
			log.Printf("queue.Run() exited: %v", err)
		}
	}()

	log.Printf("starting server on http://%s...", *bindAddr)
	if err := http.ListenAndServe(*bindAddr, genie.Router(g)); err != nil {
		log.Fatalf("portal exited with error: %v", err)
	} else {
		log.Println("portal exited gracefully")
	}
}
