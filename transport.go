package corrosion

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sync"
)

// cancelableReadCloser wraps an [io.ReadCloser] so that canceling ctx
// closes the underlying body, unblocking any in-flight Read. This lets
// ctx-cancel interrupt a [json.Decoder.Decode] blocked on a silent
// server, instead of waiting for TCP keepalive or the server to
// eventually write.
//
// The watcher goroutine exits when either ctx is done or Close is
// called, whichever happens first. Close is idempotent.
type cancelableReadCloser struct {
	rc   io.ReadCloser
	done chan struct{}
	once sync.Once
}

func newCancelableReadCloser(ctx context.Context, rc io.ReadCloser) *cancelableReadCloser {
	c := &cancelableReadCloser{rc: rc, done: make(chan struct{})}
	go func() {
		select {
		case <-ctx.Done():
			_ = rc.Close()
		case <-c.done:
		}
	}()
	return c
}

func (c *cancelableReadCloser) Read(p []byte) (int, error) { return c.rc.Read(p) }

func (c *cancelableReadCloser) Close() error {
	c.once.Do(func() { close(c.done) })
	return c.rc.Close()
}

// wrapDoError classifies an error returned by [http.Client.Do]. Network-level
// failures ([*net.OpError]) are wrapped as [*TransientError]; all others are
// returned as a generic send-request error.
func wrapDoError(err error) error {
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return newTransientError(err)
	}
	return fmt.Errorf("send request: %w", err)
}

// newJSONRequest builds an HTTP request pointed at u with JSON content
// negotiation headers and the configured bearer token (if any). body may be
// nil; when non-nil it is sent with Content-Type: application/json.
func (c *APIClient) newJSONRequest(ctx context.Context, method string, u *url.URL, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, u.String(), body)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.bearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+c.bearerToken)
	}
	return req, nil
}
