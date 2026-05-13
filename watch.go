package corrosion

import (
	"context"
	"log/slog"
	"time"
)

// DefaultWatchReconnectDelay is the delay between subscription reconnect attempts.
const DefaultWatchReconnectDelay = 2 * time.Second

// Subscriber is the interface required by [Watch]. [*APIClient] satisfies it.
type Subscriber interface {
	SubscribeContext(ctx context.Context, query string, args []any, skipRows bool) (*Subscription, error)
}

// WatchOptions configures a [Watch] loop.
type WatchOptions struct {
	Query string
	Args  []any

	// ReconnectDelay between reconnect attempts. Defaults to [DefaultWatchReconnectDelay].
	ReconnectDelay time.Duration

	// OnSubscribed is called after each (re)subscribe once initial rows are
	// drained. Use this to trigger reconciliation against the current row set.
	// May be nil.
	OnSubscribed func()

	// OnChange is called for each change event. Required.
	OnChange func(*ChangeEvent)

	// Logger for subscription lifecycle events. Defaults to [slog.Default].
	Logger *slog.Logger
}

// Watch runs a subscription loop until ctx is cancelled, reconnecting with
// backoff after failure. Initial rows from each (re)subscription are drained
// and discarded — [WatchOptions.OnSubscribed] is the hook for reconciling
// state after a fresh subscribe.
func Watch(ctx context.Context, sub Subscriber, opts WatchOptions) {
	if opts.ReconnectDelay <= 0 {
		opts.ReconnectDelay = DefaultWatchReconnectDelay
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}

	for ctx.Err() == nil {
		s, err := sub.SubscribeContext(ctx, opts.Query, opts.Args, false)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			logger.Warn("corrosion watch: subscribe failed, retrying", "error", err)
			if !sleepCtx(ctx, opts.ReconnectDelay) {
				return
			}
			continue
		}

		if rows := s.Rows(); rows != nil {
			for rows.Next() {
			}
		}

		if opts.OnSubscribed != nil {
			opts.OnSubscribed()
		}

		changes, err := s.Changes()
		if err != nil {
			logger.Warn("corrosion watch: changes channel error", "error", err)
			_ = s.Close()
			if !sleepCtx(ctx, opts.ReconnectDelay) {
				return
			}
			continue
		}

		for change := range changes {
			opts.OnChange(change)
		}

		if subErr := s.Err(); subErr != nil {
			logger.Warn("corrosion watch: subscription failed, reconnecting", "error", subErr)
		}
		_ = s.Close()
		if !sleepCtx(ctx, opts.ReconnectDelay) {
			return
		}
	}
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
