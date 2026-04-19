//go:build corrosion_integration

package corrosiontest_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/iris-xyz/go-corrosion/corrosiontest"
)

func TestSmoke(t *testing.T) {
	ctx := context.Background()

	agent, err := corrosiontest.StartAgent(ctx)
	if err != nil {
		t.Fatalf("StartAgent: %v", err)
	}
	t.Cleanup(func() {
		if err := agent.Stop(context.Background()); err != nil {
			t.Logf("Stop: %v", err)
		}
	})

	// Apply schema.
	err = agent.ApplySchema(ctx,
		"CREATE TABLE IF NOT EXISTS test_t (k INTEGER NOT NULL PRIMARY KEY, v TEXT);")
	if err != nil {
		t.Fatalf("ApplySchema: %v", err)
	}

	// INSERT one row via /v1/transactions.
	insertBody, _ := json.Marshal([]string{
		`INSERT INTO test_t (k, v) VALUES (1, 'hello')`,
	})
	if err := doTx(ctx, agent.BaseURL(), insertBody); err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	// SELECT via /v1/queries and assert result.
	rows, err := doQuery(ctx, agent.BaseURL(), `SELECT k, v FROM test_t WHERE k = 1`)
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}

	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	// Corrosion row shape: [rowid, [values...]]
	vals, ok := rows[0][1].([]interface{})
	if !ok {
		t.Fatalf("unexpected row shape: %T %v", rows[0][1], rows[0][1])
	}
	if len(vals) < 2 {
		t.Fatalf("expected >=2 values, got %v", vals)
	}
	if v, _ := vals[1].(string); v != "hello" {
		t.Errorf("expected v='hello', got %q", vals[1])
	}
}

// doTx posts a transaction body to /v1/transactions.
func doTx(ctx context.Context, base string, body []byte) error {
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost,
		base+"/v1/transactions", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return &statusError{code: resp.StatusCode, body: string(b)}
	}
	return nil
}

// doQuery posts to /v1/queries and collects all "row" events.
func doQuery(ctx context.Context, base, sql string) ([][]interface{}, error) {
	body, _ := json.Marshal(sql)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost,
		base+"/v1/queries", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	dec := json.NewDecoder(resp.Body)
	var rows [][]interface{}
	for {
		var evt map[string]json.RawMessage
		if err := dec.Decode(&evt); err != nil {
			break
		}
		if raw, ok := evt["row"]; ok {
			var row []interface{}
			if err := json.Unmarshal(raw, &row); err == nil {
				rows = append(rows, row)
			}
		}
	}
	return rows, nil
}

type statusError struct {
	code int
	body string
}

func (e *statusError) Error() string {
	return "HTTP " + http.StatusText(e.code) + ": " + e.body
}
