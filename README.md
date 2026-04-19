# go-corrosion

A Go client for [Corrosion](https://github.com/superfly/corrosion), Fly.io's
distributed SQLite database.

Supports:

- Parameterised queries over HTTP (`QueryContext`, `QueryRowContext`)
- Transactional writes (`ExecContext`, `ExecMultiContext`)
- Live subscriptions with `change_id` resume (`SubscribeContext`, `ResubscribeContext`)
- Bearer-token auth, custom `*http.Client`, structured logger injection
- NDJSON streaming with cancellation-aware read deadlines

## Install

```bash
go get github.com/iris-xyz/go-corrosion
```

## Quick start

```go
package main

import (
    "context"
    "log"

    "github.com/iris-xyz/go-corrosion"
)

func main() {
    c, err := corrosion.NewAPIClient("127.0.0.1:8080")
    if err != nil {
        log.Fatal(err)
    }

    rows, err := c.QueryContext(context.Background(),
        "SELECT id, name FROM nodes WHERE region = ?", "us-east")
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()

    for rows.Next() {
        var id, name string
        if err := rows.Scan(&id, &name); err != nil {
            log.Fatal(err)
        }
        log.Println(id, name)
    }
}
```

## Subscriptions

`SubscribeContext` returns an initial snapshot followed by live change events:

```go
sub, err := c.SubscribeContext(ctx, "SELECT id, name FROM nodes", nil, false)
if err != nil {
    log.Fatal(err)
}
defer sub.Close()

for ev := range sub.Changes() {
    if ev.Err != nil {
        log.Printf("subscription error: %v", ev.Err)
        break
    }
    log.Printf("change: %+v", ev.Change)
}
```

To resume a subscription after a disconnect, pass the last observed
`change_id` to `ResubscribeContext`:

```go
sub2, err := c.ResubscribeContext(ctx, sub.ID(), sub.LastChangeID())
```

## Options

```go
c, err := corrosion.NewAPIClient(addr,
    corrosion.WithBearerToken(token),
    corrosion.WithHTTPClient(customHTTPClient),
    corrosion.WithLogger(myLogger),
)
```

## Testing

Unit tests run against `httptest.Server` fixtures and require no external
dependencies:

```bash
go test -race ./...
```

Integration tests drive a real Corrosion agent via `testcontainers-go` and are
gated behind a build tag. Requires Docker:

```bash
go test -tags corrosion_integration -race ./corrosiontest/...
```

## License

Apache-2.0. See [LICENSE](LICENSE).
