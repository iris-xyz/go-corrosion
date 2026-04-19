package corrosion

import (
	"context"
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
