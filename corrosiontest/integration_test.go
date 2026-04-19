//go:build corrosion_integration

package corrosiontest_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/iris-xyz/go-corrosion"
	"github.com/iris-xyz/go-corrosion/corrosiontest"
)

var (
	sharedAgentOnce sync.Once
	sharedAgentInst *corrosiontest.Agent
	sharedAgentErr  error
)

func sharedAgent(t *testing.T) *corrosiontest.Agent {
	t.Helper()
	sharedAgentOnce.Do(func() {
		ctx := context.Background()
		sharedAgentInst, sharedAgentErr = corrosiontest.StartAgent(ctx)
	})
	if sharedAgentErr != nil {
		t.Fatalf("StartAgent: %v", sharedAgentErr)
	}
	return sharedAgentInst
}

func newClient(t *testing.T, agent *corrosiontest.Agent) *corrosion.APIClient {
	t.Helper()
	addr := strings.TrimPrefix(agent.BaseURL(), "http://")
	c, err := corrosion.NewAPIClient(addr)
	if err != nil {
		t.Fatalf("NewAPIClient: %v", err)
	}
	return c
}

// SELECT with three positional args; verifies Scan decodes int64, string, and sql.NullString.
func TestQueryContext_BoundParams(t *testing.T) {
	ctx := context.Background()
	agent := sharedAgent(t)
	client := newClient(t, agent)

	if err := agent.ApplySchema(ctx, "CREATE TABLE IF NOT EXISTS t1 (a INTEGER NOT NULL PRIMARY KEY, b TEXT, c TEXT);"); err != nil {
		t.Fatalf("ApplySchema: %v", err)
	}
	if _, err := client.ExecContext(ctx, "DELETE FROM t1"); err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	if _, err := client.ExecContext(ctx, "INSERT INTO t1 (a, b, c) VALUES (42, 'hello', NULL)"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	rows, err := client.QueryContext(ctx, "SELECT ?, ?, ?", 42, "hello", nil)
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}
	t.Cleanup(func() { rows.Close() })

	if !rows.Next() {
		t.Fatalf("expected one row; Err: %v", rows.Err())
	}
	var a int64
	var b string
	var c sql.NullString
	if err := rows.Scan(&a, &b, &c); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if a != 42 {
		t.Errorf("a: want 42, got %d", a)
	}
	if b != "hello" {
		t.Errorf("b: want %q, got %q", "hello", b)
	}
	if c.Valid {
		t.Errorf("c: want NULL, got %q", c.String)
	}
	if rows.Next() {
		t.Error("expected only one row")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
}

// QueryRowContext on an empty table must return ErrNoRows from Scan.
func TestQueryRowContext_NoRows(t *testing.T) {
	ctx := context.Background()
	agent := sharedAgent(t)
	client := newClient(t, agent)

	if err := agent.ApplySchema(ctx, "CREATE TABLE IF NOT EXISTS t2 (k INTEGER NOT NULL PRIMARY KEY);"); err != nil {
		t.Fatalf("ApplySchema: %v", err)
	}
	if _, err := client.ExecContext(ctx, "DELETE FROM t2"); err != nil {
		t.Fatalf("DELETE: %v", err)
	}

	var k int64
	err := client.QueryRowContext(ctx, "SELECT k FROM t2 WHERE k = ?", 42).Scan(&k)
	if !errors.Is(err, corrosion.ErrNoRows) {
		t.Fatalf("expected ErrNoRows, got %v", err)
	}
}

// QueryContext on 1000 rows streams all of them; Close is idempotent.
func TestQuery_StreamsLargeResult(t *testing.T) {
	ctx := context.Background()
	agent := sharedAgent(t)
	client := newClient(t, agent)

	if err := agent.ApplySchema(ctx, "CREATE TABLE IF NOT EXISTS t3 (k INTEGER NOT NULL PRIMARY KEY);"); err != nil {
		t.Fatalf("ApplySchema: %v", err)
	}
	if _, err := client.ExecContext(ctx, "DELETE FROM t3"); err != nil {
		t.Fatalf("DELETE: %v", err)
	}

	const n = 1000
	var sb strings.Builder
	sb.WriteString("INSERT INTO t3 (k) VALUES ")
	for i := 0; i < n; i++ {
		if i > 0 {
			sb.WriteByte(',')
		}
		fmt.Fprintf(&sb, "(%d)", i+1)
	}
	if _, err := client.ExecContext(ctx, sb.String()); err != nil {
		t.Fatalf("INSERT 1000: %v", err)
	}

	rows, err := client.QueryContext(ctx, "SELECT k FROM t3 ORDER BY k")
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}
	t.Cleanup(func() { rows.Close() })

	count := 0
	for rows.Next() {
		var k int64
		if err := rows.Scan(&k); err != nil {
			t.Fatalf("Scan row %d: %v", count, err)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	if count != n {
		t.Fatalf("want %d rows, got %d", n, count)
	}
	// Close is idempotent.
	if err := rows.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// ExecContext INSERT must report RowsAffected == 1.
func TestExecContext_InsertAffected(t *testing.T) {
	ctx := context.Background()
	agent := sharedAgent(t)
	client := newClient(t, agent)

	if err := agent.ApplySchema(ctx, "CREATE TABLE IF NOT EXISTS t5 (k INTEGER NOT NULL PRIMARY KEY, v TEXT);"); err != nil {
		t.Fatalf("ApplySchema: %v", err)
	}
	if _, err := client.ExecContext(ctx, "DELETE FROM t5"); err != nil {
		t.Fatalf("DELETE: %v", err)
	}

	result, err := client.ExecContext(ctx, "INSERT INTO t5 (k, v) VALUES (1, 'hello')")
	if err != nil {
		t.Fatalf("ExecContext: %v", err)
	}
	if result.RowsAffected != 1 {
		t.Fatalf("RowsAffected: want 1, got %d", result.RowsAffected)
	}
}

// ExecMultiContext with an invalid middle statement surfaces a per-statement error.
func TestExecMultiContext_MiddleFails(t *testing.T) {
	ctx := context.Background()
	agent := sharedAgent(t)
	client := newClient(t, agent)

	if err := agent.ApplySchema(ctx, "CREATE TABLE IF NOT EXISTS t6 (k INTEGER NOT NULL PRIMARY KEY);"); err != nil {
		t.Fatalf("ApplySchema: %v", err)
	}
	if _, err := client.ExecContext(ctx, "DELETE FROM t6"); err != nil {
		t.Fatalf("DELETE: %v", err)
	}

	stmts := []corrosion.Statement{
		{Query: "INSERT INTO t6 (k) VALUES (1)"},
		{Query: "INSERT INTO nonexistent_table_t6 VALUES (1)"},
		{Query: "INSERT INTO t6 (k) VALUES (3)"},
	}
	resp, err := client.ExecMultiContext(ctx, stmts...)
	// ExecMultiContext joins per-statement errors; expect a non-nil error from the middle statement.
	if err == nil {
		t.Fatal("expected error from invalid middle statement, got nil")
	}
	// Corrosion returns an ExecResponse body on both 200 OK and 500 Internal Server
	// Error for failed transactions, so resp should never be nil when err is non-nil
	// on a protocol-level failure (as opposed to a network failure).
	if resp == nil {
		t.Fatal("expected ExecResponse to be non-nil even on partial failure")
	}
	// At least one result must carry the per-statement error; Corrosion may abort
	// the transaction on first failure, so we don't require exactly len(stmts) results.
	var seenErr bool
	for _, r := range resp.Results {
		if r.Error != nil {
			seenErr = true
			break
		}
	}
	if !seenErr {
		t.Errorf("expected at least one result with Error set; results=%+v", resp.Results)
	}
}

// SubscribeContext delivers a snapshot of 3 rows then an EOQ-equivalent marker;
// LastChangeID is non-zero after consuming the snapshot.
func TestSubscribeContext_InitialSnapshot(t *testing.T) {
	ctx := context.Background()
	agent := sharedAgent(t)
	client := newClient(t, agent)

	if err := agent.ApplySchema(ctx, "CREATE TABLE IF NOT EXISTS t7 (k INTEGER NOT NULL PRIMARY KEY);"); err != nil {
		t.Fatalf("ApplySchema: %v", err)
	}
	if _, err := client.ExecContext(ctx, "DELETE FROM t7"); err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	for _, k := range []int{1, 2, 3} {
		if _, err := client.ExecContext(ctx, fmt.Sprintf("INSERT INTO t7 (k) VALUES (%d)", k)); err != nil {
			t.Fatalf("INSERT %d: %v", k, err)
		}
	}

	sub, err := client.SubscribeContext(ctx, "SELECT k FROM t7", nil, false)
	if err != nil {
		t.Fatalf("SubscribeContext: %v", err)
	}
	t.Cleanup(func() { sub.Close() })

	// Drain the snapshot rows.
	rows := sub.Rows()
	if rows == nil {
		t.Fatal("expected non-nil Rows for skipRows=false")
	}
	t.Cleanup(func() { rows.Close() })

	count := 0
	for rows.Next() {
		var k int64
		if err := rows.Scan(&k); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	if count != 3 {
		t.Fatalf("snapshot: want 3 rows, got %d", count)
	}

	// After snapshot, open the changes channel. CRR change_id propagation is async
	// in Corrosion, so the snapshot's EOQ may arrive before the server has minted
	// change_ids for the existing rows. Verify the subscription is healthy by
	// committing one more INSERT and waiting for its change event — LastChangeID
	// must be non-zero after that.
	ch, err := sub.Changes()
	if err != nil {
		t.Fatalf("Changes: %v", err)
	}

	if _, err := client.ExecContext(ctx, "INSERT INTO t7 (k) VALUES (4)"); err != nil {
		t.Fatalf("INSERT 4: %v", err)
	}

	select {
	case evt, ok := <-ch:
		if !ok {
			t.Fatalf("changes channel closed unexpectedly; sub.Err: %v", sub.Err())
		}
		if evt.Type != corrosion.ChangeTypeInsert {
			t.Errorf("want insert event, got %q", evt.Type)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for post-snapshot INSERT change event")
	}

	if sub.LastChangeID() == 0 {
		t.Error("LastChangeID should be non-zero after observing a change event")
	}
}

// SubscribeContext on an empty table receives live INSERT events.
func TestSubscribeContext_LiveChanges(t *testing.T) {
	ctx := context.Background()
	agent := sharedAgent(t)
	client := newClient(t, agent)
	writer := newClient(t, agent)

	if err := agent.ApplySchema(ctx, "CREATE TABLE IF NOT EXISTS t8 (k INTEGER NOT NULL PRIMARY KEY);"); err != nil {
		t.Fatalf("ApplySchema: %v", err)
	}
	if _, err := client.ExecContext(ctx, "DELETE FROM t8"); err != nil {
		t.Fatalf("DELETE: %v", err)
	}

	// Subscribe with skipRows=true so we skip the (empty) snapshot immediately.
	sub, err := client.SubscribeContext(ctx, "SELECT k FROM t8", nil, true)
	if err != nil {
		t.Fatalf("SubscribeContext: %v", err)
	}
	t.Cleanup(func() { sub.Close() })

	ch, err := sub.Changes()
	if err != nil {
		t.Fatalf("Changes: %v", err)
	}

	// Insert a row via the second client.
	go func() {
		time.Sleep(100 * time.Millisecond)
		if _, err := writer.ExecContext(ctx, "INSERT INTO t8 (k) VALUES (99)"); err != nil {
			t.Logf("writer INSERT: %v", err)
		}
	}()

	select {
	case evt, ok := <-ch:
		if !ok {
			t.Fatalf("changes channel closed unexpectedly; sub.Err: %v", sub.Err())
		}
		if evt.Type != corrosion.ChangeTypeInsert {
			t.Errorf("want insert event, got %q", evt.Type)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for live INSERT change event")
	}
}

// Subscription can be resumed from a recorded change ID; replayed events start after that point.
func TestSubscription_ResumeFromChangeID(t *testing.T) {
	ctx := context.Background()
	agent := sharedAgent(t)
	client := newClient(t, agent)

	if err := agent.ApplySchema(ctx, "CREATE TABLE IF NOT EXISTS t9 (k INTEGER NOT NULL PRIMARY KEY);"); err != nil {
		t.Fatalf("ApplySchema: %v", err)
	}
	if _, err := client.ExecContext(ctx, "DELETE FROM t9"); err != nil {
		t.Fatalf("DELETE: %v", err)
	}

	// Subscribe to empty table (skipRows=true → immediate EOQ, no snapshot to drain).
	sub, err := client.SubscribeContext(ctx, "SELECT k FROM t9", nil, true)
	if err != nil {
		t.Fatalf("SubscribeContext: %v", err)
	}

	ch, err := sub.Changes()
	if err != nil {
		sub.Close()
		t.Fatalf("Changes: %v", err)
	}

	// Helper to receive one change event with a timeout.
	recv := func(label string) *corrosion.ChangeEvent {
		t.Helper()
		select {
		case evt, ok := <-ch:
			if !ok {
				t.Fatalf("%s: channel closed; sub.Err: %v", label, sub.Err())
			}
			return evt
		case <-time.After(5 * time.Second):
			t.Fatalf("%s: timed out", label)
			return nil
		}
	}

	// Insert rows 1, 2, 3 and collect their change events.
	for _, k := range []int{1, 2, 3} {
		if _, err := client.ExecContext(ctx, fmt.Sprintf("INSERT INTO t9 (k) VALUES (%d)", k)); err != nil {
			sub.Close()
			t.Fatalf("INSERT %d: %v", k, err)
		}
	}

	recv("event 1")
	evt2 := recv("event 2")
	changeAfterTwo := evt2.ChangeID
	recv("event 3")

	subID := sub.ID()
	sub.Close()

	// Resume from after event 2 — only event 3 should replay.
	sub2, err := client.ResubscribeContext(ctx, subID, changeAfterTwo)
	if err != nil {
		t.Fatalf("ResubscribeContext: %v", err)
	}
	t.Cleanup(func() { sub2.Close() })

	ch2, err := sub2.Changes()
	if err != nil {
		t.Fatalf("Changes (resumed): %v", err)
	}

	select {
	case evt, ok := <-ch2:
		if !ok {
			t.Fatalf("resumed channel closed; sub2.Err: %v", sub2.Err())
		}
		if evt.ChangeID <= changeAfterTwo {
			t.Errorf("expected change ID > %d, got %d", changeAfterTwo, evt.ChangeID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for resumed change event")
	}
}
