package corrosion

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
)

// nopCloser is an [io.ReadCloser] that always reports EOF and is safe to close.
type nopCloser struct{}

func (nopCloser) Read(p []byte) (int, error) { return 0, io.EOF }
func (nopCloser) Close() error               { return nil }

// NewSubscriptionForTesting constructs a [*Subscription] whose change events
// are delivered over a caller-supplied channel, for use in unit tests that
// need to drive a subscription without a real Corrosion server.
//
// This entry point exists solely to let the [corrosiontest] subpackage build
// test subscriptions; application code should use [APIClient.SubscribeContext]
// instead. The API shape may change without notice.
func NewSubscriptionForTesting(ctx context.Context, id string, changes chan *ChangeEvent) *Subscription {
	ctx, cancel := context.WithCancel(ctx)
	return &Subscription{
		ctx:     ctx,
		cancel:  cancel,
		id:      id,
		changes: changes,
		body:    nopCloser{},
	}
}

// NewSubscriptionWithRowsForTesting constructs a [*Subscription] that serves
// the given rows as its initial snapshot and then delivers change events from
// changes, for use in unit tests that need to exercise both the [Subscription.Rows]
// and [Subscription.Changes] paths without a real Corrosion server.
//
// columns must list column names in the same order as the values in each row
// slice — identical to the SELECT column order the subscription would emit.
// Each value is JSON-encoded automatically; nil becomes JSON null.
//
// After all rows have been consumed via [Subscription.Rows], the caller must
// call [Subscription.Changes] to receive further events. Closing the changes
// channel or cancelling ctx terminates the subscription.
//
// Same stability caveats as [NewSubscriptionForTesting]: test-only entry point;
// shape may change without notice.
func NewSubscriptionWithRowsForTesting(ctx context.Context, id string, columns []string, rows [][]any, changes chan *ChangeEvent) (*Subscription, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)

	if err := enc.Encode(QueryEvent{Columns: columns}); err != nil {
		return nil, fmt.Errorf("encode columns frame: %w", err)
	}
	for i, row := range rows {
		rawVals := make([]json.RawMessage, len(row))
		for j, v := range row {
			b, err := json.Marshal(v)
			if err != nil {
				return nil, fmt.Errorf("marshal row %d col %d: %w", i, j, err)
			}
			rawVals[j] = b
		}
		if err := enc.Encode(QueryEvent{Row: &RowEvent{RowID: uint64(i + 1), Values: rawVals}}); err != nil {
			return nil, fmt.Errorf("encode row %d: %w", i, err)
		}
	}
	if err := enc.Encode(QueryEvent{EOQ: &EndOfQuery{Time: 0}}); err != nil {
		return nil, fmt.Errorf("encode eoq frame: %w", err)
	}

	// closeOnEOQ=false: Changes() never reads this body — s.changes is pre-set.
	rowsObj, err := newRows(ctx, io.NopCloser(&buf), false)
	if err != nil {
		return nil, fmt.Errorf("parse rows body: %w", err)
	}

	ctx, cancel := context.WithCancel(ctx)
	return &Subscription{
		ctx:     ctx,
		cancel:  cancel,
		id:      id,
		rows:    rowsObj,
		body:    rowsObj.body,
		changes: changes,
	}, nil
}

// NewChangeEventForTesting builds a [*ChangeEvent] for unit tests. Values are
// JSON-encoded for you; pass them in the same column order as the subscription
// would emit. Same caveats as [NewSubscriptionForTesting] — test-only entry
// point, shape may change.
func NewChangeEventForTesting(changeType ChangeType, rowID, changeID uint64, values ...any) *ChangeEvent {
	raw := make([]json.RawMessage, len(values))
	for i, v := range values {
		data, _ := json.Marshal(v)
		raw[i] = data
	}
	return &ChangeEvent{
		Type:     changeType,
		RowID:    rowID,
		ChangeID: changeID,
		Values:   raw,
	}
}
