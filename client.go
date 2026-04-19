package corrosion

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"golang.org/x/net/http2"
)

// discardLogger is the default no-op logger, shared across clients.
var discardLogger = slog.New(slog.DiscardHandler)

const (
	defaultConnectTimeout        = 3 * time.Second
	defaultRetryMaxElapsed       = 10 * time.Second
	defaultResubscribeMaxElapsed = 60 * time.Second
	defaultBackoffInitial        = 100 * time.Millisecond
	defaultBackoffMax            = 1 * time.Second
)

// BackoffConfig configures exponential backoff retry behavior. Zero fields
// fall back to package defaults.
type BackoffConfig struct {
	InitialInterval time.Duration
	MaxInterval     time.Duration
	MaxElapsedTime  time.Duration
}

func (b BackoffConfig) newBackoff(defaultMaxElapsed time.Duration) backoff.BackOff {
	initial := b.InitialInterval
	if initial == 0 {
		initial = defaultBackoffInitial
	}
	maxInt := b.MaxInterval
	if maxInt == 0 {
		maxInt = defaultBackoffMax
	}
	maxElapsed := b.MaxElapsedTime
	if maxElapsed == 0 {
		maxElapsed = defaultMaxElapsed
	}
	return backoff.NewExponentialBackOff(
		backoff.WithInitialInterval(initial),
		backoff.WithMaxInterval(maxInt),
		backoff.WithMaxElapsedTime(maxElapsed),
	)
}

// APIClient is a client for the Corrosion API.
type APIClient struct {
	baseURL         *url.URL
	client          *http.Client
	newResubBackoff func() backoff.BackOff
	newRetryBackoff func() backoff.BackOff
	connectTimeout  time.Duration
	logger          *slog.Logger
	bearerToken     string
	subBufferSize   int

	// transport holds a caller-supplied base RoundTripper (via WithTransport).
	// When non-nil it replaces the default http2.Transport inside the retry wrapper.
	transport http.RoundTripper
}

// NewAPIClient creates a new Corrosion API client.
//
// addr accepts "host:port" (http by default), "http://host:port", or
// "https://host:port". The client retries transient network errors with
// exponential backoff and automatically resubscribes to subscriptions that
// fail. Tune behavior via [WithTransport], [WithConnectTimeout],
// [WithRetryBackoff], [WithResubscribeBackoff], [DisableRetry], and
// [DisableResubscribe].
func NewAPIClient(addr string, opts ...APIClientOption) (*APIClient, error) {
	baseURL, err := parseAddr(addr)
	if err != nil {
		return nil, err
	}

	c := &APIClient{
		baseURL:        baseURL,
		connectTimeout: defaultConnectTimeout,
		newResubBackoff: func() backoff.BackOff {
			return BackoffConfig{}.newBackoff(defaultResubscribeMaxElapsed)
		},
		newRetryBackoff: func() backoff.BackOff {
			return BackoffConfig{}.newBackoff(defaultRetryMaxElapsed)
		},
		logger: discardLogger,
	}
	for _, opt := range opts {
		opt(c)
	}

	base := c.transport
	if base == nil {
		timeout := c.connectTimeout
		base = &http2.Transport{
			AllowHTTP: true,
			DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
				dialer := &net.Dialer{Timeout: timeout}
				return dialer.DialContext(ctx, network, addr)
			},
		}
	}
	c.client = &http.Client{Transport: &retryRoundTripper{
		base:       base,
		newBackoff: c.newRetryBackoff,
		logger:     c.logger,
	}}
	return c, nil
}

func parseAddr(addr string) (*url.URL, error) {
	raw := addr
	if !strings.Contains(addr, "://") {
		raw = "http://" + addr
	}
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("invalid addr %q: %w", addr, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("invalid addr %q: scheme must be http or https", addr)
	}
	if u.Host == "" {
		return nil, fmt.Errorf("invalid addr %q: missing host", addr)
	}
	return u, nil
}

// APIClientOption configures an [APIClient].
type APIClientOption func(*APIClient)

// WithTransport replaces the base [http.RoundTripper] wrapped by the client's
// retry logic. Use this to customize TLS, proxying, or to inject a test
// transport. [WithConnectTimeout] has no effect once a custom transport is
// set; retry behavior and [WithRetryBackoff] still apply. Nil is a no-op.
func WithTransport(rt http.RoundTripper) APIClientOption {
	return func(c *APIClient) {
		if rt == nil {
			return
		}
		c.transport = rt
	}
}

// WithBearerToken sets a static bearer token attached to every outgoing
// request as `Authorization: Bearer <token>`.
func WithBearerToken(token string) APIClientOption {
	return func(c *APIClient) {
		c.bearerToken = token
	}
}

// WithConnectTimeout sets the dial timeout for the default transport.
// Ignored when [WithTransport] is used.
func WithConnectTimeout(d time.Duration) APIClientOption {
	return func(c *APIClient) {
		c.connectTimeout = d
	}
}

// WithRetryBackoff overrides the retry backoff policy for transient network
// errors. Zero fields fall back to package defaults.
func WithRetryBackoff(cfg BackoffConfig) APIClientOption {
	return func(c *APIClient) {
		c.newRetryBackoff = func() backoff.BackOff {
			return cfg.newBackoff(defaultRetryMaxElapsed)
		}
	}
}

// DisableRetry disables the built-in retry round-tripper; each request makes
// exactly one attempt.
func DisableRetry() APIClientOption {
	return func(c *APIClient) {
		c.newRetryBackoff = func() backoff.BackOff { return &backoff.StopBackOff{} }
	}
}

// WithResubscribeBackoff overrides the backoff policy used when automatically
// resubscribing after a stream error. Zero fields fall back to package defaults.
func WithResubscribeBackoff(cfg BackoffConfig) APIClientOption {
	return func(c *APIClient) {
		c.newResubBackoff = func() backoff.BackOff {
			return cfg.newBackoff(defaultResubscribeMaxElapsed)
		}
	}
}

// DisableResubscribe disables automatic resubscription. Stream errors are
// surfaced via [Subscription.Err] instead of being retried.
func DisableResubscribe() APIClientOption {
	return func(c *APIClient) {
		c.newResubBackoff = nil
	}
}

// WithLogger sets the [*slog.Logger] used for internal diagnostics. Nil restores
// the default discard logger.
func WithLogger(l *slog.Logger) APIClientOption {
	return func(c *APIClient) {
		if l == nil {
			c.logger = discardLogger
			return
		}
		c.logger = l
	}
}

// WithSubscriptionBufferSize sets the capacity of the channel returned by
// [Subscription.Changes]. The default is 0 (unbuffered): a slow consumer
// back-pressures the stream and Corrosion may drop the subscription. A non-zero
// buffer absorbs short bursts. Negative values are clamped to 0.
func WithSubscriptionBufferSize(n int) APIClientOption {
	return func(c *APIClient) {
		if n < 0 {
			n = 0
		}
		c.subBufferSize = n
	}
}

// retryRoundTripper wraps an [http.RoundTripper] with retry logic for
// transient network errors (*net.OpError).
type retryRoundTripper struct {
	base       http.RoundTripper
	newBackoff func() backoff.BackOff
	logger     *slog.Logger
}

func (rt *retryRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	roundTrip := func() (*http.Response, error) {
		resp, err := rt.base.RoundTrip(req)
		if err != nil {
			var opErr *net.OpError
			if errors.As(err, &opErr) {
				if rt.logger != nil {
					rt.logger.Debug("Retrying corrosion API request due to network error.", "error", err)
				}
				return nil, err
			}
			return nil, backoff.Permanent(err)
		}
		return resp, err
	}
	boff := backoff.WithContext(rt.newBackoff(), req.Context())
	return backoff.RetryWithData(roundTrip, boff)
}
