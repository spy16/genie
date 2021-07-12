# 🧞 Genie

Genie is a dead simple job-queue library/tool.

## Usage

```go
import "github.com/spy16/genie"

func main() {
    // connect to sqlite3.
    q, err := genie.Open("sqlite3://my-queue.db", nil)
    if err != nil {
        panic(err)
    }
    defer q.Close()


    // enqueue items on the queue.
    // this can be exposed as http api or something.
    _ = q.Push(ctx, Item{
        ID: "job1",
        Type:"job-category",
        Payload:"arbitrary data for executing job",
    })

    // run the poll-excute loop
    log.Fatalf("exited: %v", q.Run(ctx, myExecutorFunc))
}

func myExecutorFunc(ctx context.Context, item genie.Item) error {
    // do your thing

    return nil
    // return genie.ErrFail to fail immediately
    // return genie.ErrSkip to skip this item
    // return any other error to signal retry.
}
```
