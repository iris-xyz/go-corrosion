// Subscription test helpers.
//
// These helpers build a [*corrosion.Subscription] whose change events are
// delivered over a caller-controlled channel, so tests can exercise code
// that consumes a [*corrosion.Subscription] without a real Corrosion server.

package corrosiontest

import (
	"context"
	"encoding/json"

	"github.com/iris-xyz/go-corrosion"
)

// NewSubscription creates a [*corrosion.Subscription] for testing.
//
// Events sent on changes are delivered verbatim through the subscription's
// Changes() channel. Close the changes channel or cancel ctx to terminate
// the subscription.
//
//	changes := make(chan *corrosion.ChangeEvent)
//	sub := corrosiontest.NewSubscription(ctx, "test-id", changes)
//	go func() {
//	    changes <- corrosiontest.NewChangeEvent(corrosion.ChangeTypeInsert, 1, 100, "v1", "v2")
//	    close(changes)
//	}()
func NewSubscription(ctx context.Context, id string, changes chan *corrosion.ChangeEvent) *corrosion.Subscription {
	return corrosion.NewSubscriptionForTesting(ctx, id, changes)
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
