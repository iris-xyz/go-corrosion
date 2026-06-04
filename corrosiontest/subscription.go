// Subscription test helpers.
//
// These helpers build a [*corrosion.Subscription] whose rows and change events
// are delivered over caller-controlled channels, so tests can exercise code
// that consumes a [*corrosion.Subscription] without a real Corrosion server.

package corrosiontest

import (
	"context"
	"encoding/json"

	"github.com/iris-xyz/go-corrosion"
)

// NewSubscription creates a [*corrosion.Subscription] for testing whose change
// events are delivered over changes. Use this when the code under test only calls
// [corrosion.Subscription.Changes]; for code that also calls [corrosion.Subscription.Rows]
// use [NewSubscriptionWithRows].
func NewSubscription(ctx context.Context, id string, changes chan *corrosion.ChangeEvent) *corrosion.Subscription {
	return corrosion.NewSubscriptionForTesting(ctx, id, changes)
}

// NewSubscriptionWithRows creates a [*corrosion.Subscription] for testing that
// serves an initial row snapshot followed by change events.
//
// Use this when the code under test calls [corrosion.Subscription.Rows] to load
// initial state before switching to [corrosion.Subscription.Changes] for ongoing
// updates — the standard skip_rows=false subscription pattern.
//
// columns must list column names in the same order as the values in each row
// slice, matching the SELECT column order. Values are JSON-encoded automatically.
//
//	columns := []string{"id", "name", "active"}
//	rows := [][]any{
//	    {"row-1", "alice", true},
//	    {"row-2", "bob",   false},
//	}
//	changes := make(chan *corrosion.ChangeEvent, 4)
//	sub, err := corrosiontest.NewSubscriptionWithRows(ctx, "test-id", columns, rows, changes)
func NewSubscriptionWithRows(ctx context.Context, id string, columns []string, rows [][]any, changes chan *corrosion.ChangeEvent) (*corrosion.Subscription, error) {
	return corrosion.NewSubscriptionWithRowsForTesting(ctx, id, columns, rows, changes)
}

// NewChangeEvent creates a [*corrosion.ChangeEvent] for testing. Values are
// JSON-encoded automatically.
func NewChangeEvent(changeType corrosion.ChangeType, rowID, changeID uint64, values ...any) *corrosion.ChangeEvent {
	rawValues := make([]json.RawMessage, len(values))
	for i, v := range values {
		data, _ := json.Marshal(v)
		rawValues[i] = data
	}
	return &corrosion.ChangeEvent{
		Type:     changeType,
		RowID:    rowID,
		ChangeID: changeID,
		Values:   rawValues,
	}
}
