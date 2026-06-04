package corrosiontest_test

import (
	"context"
	"testing"

	corrosion "github.com/iris-xyz/go-corrosion"
	"github.com/iris-xyz/go-corrosion/corrosiontest"
)

// TestNewSubscription_ChangesOnly verifies the changes-only path: Rows() returns
// nil and events sent on the channel arrive through Changes().
func TestNewSubscription_ChangesOnly(t *testing.T) {
	ctx := context.Background()
	changes := make(chan *corrosion.ChangeEvent, 2)
	sub := corrosiontest.NewSubscription(ctx, "test", changes)

	if sub.Rows() != nil {
		t.Fatal("expected nil Rows for changes-only subscription")
	}

	ch, err := sub.Changes()
	if err != nil {
		t.Fatalf("Changes: %v", err)
	}

	want := corrosiontest.NewChangeEvent(corrosion.ChangeTypeInsert, 1, 1, "hello")
	changes <- want

	got := <-ch
	if got.Type != corrosion.ChangeTypeInsert {
		t.Errorf("type: got %v, want %v", got.Type, corrosion.ChangeTypeInsert)
	}
	if len(got.Values) != 1 {
		t.Fatalf("values len: got %d, want 1", len(got.Values))
	}
}

// TestNewSubscriptionWithRows_InitialLoad verifies that rows appear via Rows()
// with correct column values, and that change events arrive after via Changes().
func TestNewSubscriptionWithRows_InitialLoad(t *testing.T) {
	ctx := context.Background()

	columns := []string{"id", "name", "active"}
	rows := [][]any{
		{"row-1", "alice", true},
		{"row-2", "bob", false},
	}

	changes := make(chan *corrosion.ChangeEvent, 4)
	sub, err := corrosiontest.NewSubscriptionWithRows(ctx, "test", columns, rows, changes)
	if err != nil {
		t.Fatalf("NewSubscriptionWithRows: %v", err)
	}

	rs := sub.Rows()
	if rs == nil {
		t.Fatal("expected non-nil Rows")
	}

	// Consume initial rows.
	var scanned []struct {
		id     string
		name   string
		active bool
	}
	for rs.Next() {
		var id, name string
		var active bool
		if err := rs.Scan(&id, &name, &active); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		scanned = append(scanned, struct {
			id     string
			name   string
			active bool
		}{id, name, active})
	}
	if err := rs.Err(); err != nil {
		t.Fatalf("Rows.Err: %v", err)
	}

	if len(scanned) != 2 {
		t.Fatalf("row count: got %d, want 2", len(scanned))
	}
	if scanned[0].id != "row-1" || scanned[0].name != "alice" || !scanned[0].active {
		t.Errorf("row 0: got %+v", scanned[0])
	}
	if scanned[1].id != "row-2" || scanned[1].name != "bob" || scanned[1].active {
		t.Errorf("row 1: got %+v", scanned[1])
	}

	// Change events arrive after rows are consumed.
	ch, err := sub.Changes()
	if err != nil {
		t.Fatalf("Changes: %v", err)
	}

	changes <- corrosiontest.NewChangeEvent(corrosion.ChangeTypeInsert, 3, 1, "row-3", "carol", true)
	ev := <-ch
	if ev.Type != corrosion.ChangeTypeInsert {
		t.Errorf("event type: got %v, want insert", ev.Type)
	}
}

// TestNewSubscriptionWithRows_Empty verifies a subscription with no initial rows
// serves an empty Rows iterator and then delivers changes normally.
func TestNewSubscriptionWithRows_Empty(t *testing.T) {
	ctx := context.Background()
	columns := []string{"id", "value"}
	changes := make(chan *corrosion.ChangeEvent, 2)

	sub, err := corrosiontest.NewSubscriptionWithRows(ctx, "test", columns, nil, changes)
	if err != nil {
		t.Fatalf("NewSubscriptionWithRows: %v", err)
	}

	rs := sub.Rows()
	if rs == nil {
		t.Fatal("expected non-nil Rows even for empty snapshot")
	}
	if rs.Next() {
		t.Error("expected no rows")
	}
	if err := rs.Err(); err != nil {
		t.Fatalf("Rows.Err: %v", err)
	}

	ch, err := sub.Changes()
	if err != nil {
		t.Fatalf("Changes: %v", err)
	}

	changes <- corrosiontest.NewChangeEvent(corrosion.ChangeTypeInsert, 1, 1, "x", 42)
	ev := <-ch
	if ev.Type != corrosion.ChangeTypeInsert {
		t.Errorf("event type: got %v", ev.Type)
	}
}

// TestNewSubscriptionWithRows_ColumnCount verifies column count mismatch is caught
// at Scan time (not at construction time), matching database/sql behaviour.
func TestNewSubscriptionWithRows_ColumnCount(t *testing.T) {
	ctx := context.Background()
	columns := []string{"a", "b"}
	rows := [][]any{{"v1", "v2"}}
	sub, err := corrosiontest.NewSubscriptionWithRows(ctx, "test", columns, rows, make(chan *corrosion.ChangeEvent))
	if err != nil {
		t.Fatalf("NewSubscriptionWithRows: %v", err)
	}

	rs := sub.Rows()
	if !rs.Next() {
		t.Fatal("expected a row")
	}
	// Scanning into too few destinations must error.
	var a string
	if err := rs.Scan(&a); err == nil {
		t.Error("expected error when scanning 2 columns into 1 destination")
	}
}
