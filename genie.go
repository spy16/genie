package genie

import (
	"context"
	"fmt"
	libs "github.com/vadv/gopher-lua-libs"
	"io"
	"net/url"
	"reflect"
	"time"

	"github.com/spy16/genie/lua"

	gopherlua "github.com/yuin/gopher-lua"
)

// New returns a new initialised session of Genie.
func New(queueSpec string, initLua string, luaPaths []string) (*Genie, error) {
	var g Genie

	luaEngine, err := lua.New(
		lua.Path(luaPaths...),
		lua.Module("genie", genieAPI{g: &g}),
	)
	if err != nil {
		return nil, err
	}
	libs.Preload(luaEngine.State())

	if err := luaEngine.ExecuteFile(initLua); err != nil {
		return nil, err
	}

	q, err := createQueue(queueSpec)
	if err != nil {
		return nil, err
	}

	g.lua = luaEngine
	g.queue = q
	g.pollInt = 500 * time.Millisecond
	return &g, nil
}

func createQueue(queueSpec string) (Queue, error) {
	u, err := url.Parse(queueSpec)
	if err != nil {
		return nil, err
	}

	switch u.Scheme {
	case "sqlite3":
		return newSQLQueue(u)

	default:
		return nil, fmt.Errorf("unknown queue type '%s'", u.Scheme)
	}
}

// Genie represents a Genie session.
type Genie struct {
	lua         *lua.Lua
	queue       Queue
	pollInt     time.Duration
	jobTypes    []string
	jobHandlers map[string]Handler
}

// Stats returns stats about the current session and overall queue.
func (g *Genie) Stats() (Stats, error) {
	res := Stats{
		Queue:    reflect.TypeOf(g.queue).String(),
		JobTypes: g.jobTypes,
	}

	grpStats, err := g.queue.Stats()
	if err != nil {
		return res, err
	}
	res.Groups = grpStats

	return res, nil
}

// Push pushes all the items onto the queue.
func (g *Genie) Push(ctx context.Context, items []Item) error {
	for _, item := range items {
		if err := g.validate(item); err != nil {
			return err
		}
	}
	return g.queue.Push(ctx, items...)
}

// Run starts the genie worker threads that consume and execute jobs.
func (g *Genie) Run(ctx context.Context) error {
	defer func() {
		if closer, ok := g.queue.(io.Closer); ok {
			_ = closer.Close()
		}
	}()

	ticker := time.NewTicker(g.pollInt)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil

		case <-ticker.C:
			ticker.Reset(g.pollInt)

			if err := g.popAndProcess(ctx); err != nil {
				return err
			}
		}
	}
}

func (g *Genie) ForEach(ctx context.Context, groupID, status string, fn Fn) error {
	return g.queue.ForEach(ctx, groupID, status, fn)
}

func (g *Genie) popAndProcess(ctx context.Context) error {
	return g.queue.Pop(ctx, g.jobTypes, func(ctx context.Context, item Item) ([]byte, error) {
		h, found := g.jobHandlers[item.Type]
		if !found {
			return nil, fmt.Errorf("handler not found: %s", item.Type)
		}
		return h(ctx, item)
	})
}

func (g *Genie) validate(item Item) error {
	return nil
}

type genieAPI struct{ g *Genie }

func (api genieAPI) Register(name string, lf *gopherlua.LFunction) error {
	if api.g.jobHandlers == nil {
		api.g.jobHandlers = map[string]Handler{}
	}

	api.g.jobTypes = append(api.g.jobTypes, name)
	api.g.jobHandlers[name] = func(ctx context.Context, item Item) ([]byte, error) {
		ret, err := api.g.lua.CallFunc(lf, item)
		if err != nil {
			return nil, err
		}
		return []byte(ret.String()), nil
	}
	return nil
}
