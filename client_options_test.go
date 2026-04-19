package corrosion_test

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iris-xyz/go-corrosion"
)

// Test_WithConnectTimeout: shortens the dial deadline; retries disabled so
// elapsed time reflects a single dial attempt, not the retry budget.
func Test_WithConnectTimeout(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping: makes a real network dial")
	}

	// 10.255.255.1 is non-routable; the dialer will block until its timeout fires.
	const dialTimeout = 300 * time.Millisecond
	const upperBound = 2 * time.Second

	c, err := corrosion.NewAPIClient(
		"10.255.255.1:1",
		corrosion.WithConnectTimeout(dialTimeout),
		corrosion.DisableRetry(),
	)
	if err != nil {
		t.Fatalf("NewAPIClient: %v", err)
	}

	start := time.Now()
	_, execErr := c.ExecContext(context.Background(), "SELECT 1")
	elapsed := time.Since(start)

	if execErr == nil {
		t.Fatal("expected error dialing non-routable address, got nil")
	}
	if elapsed >= upperBound {
		t.Errorf("elapsed %v >= upper bound %v; timeout may not have applied", elapsed, upperBound)
	}
}

// Test_DisableRetry: each request makes exactly one attempt even when the
// transport returns a transient *net.OpError.
func Test_DisableRetry(t *testing.T) {
	var attempts atomic.Int32
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		attempts.Add(1)
		return nil, &net.OpError{Op: "dial", Net: "tcp", Err: fmt.Errorf("injected transient error")}
	})

	c, err := corrosion.NewAPIClient(
		"127.0.0.1:1",
		corrosion.WithTransport(transport),
		corrosion.DisableRetry(),
	)
	if err != nil {
		t.Fatalf("NewAPIClient: %v", err)
	}

	_, _ = c.ExecContext(context.Background(), "SELECT 1")

	if got := attempts.Load(); got != 1 {
		t.Errorf("attempts = %d, want 1 (retries disabled)", got)
	}
}

// Test_WithRetryBackoff_Retries: custom backoff with short intervals produces
// multiple attempts on transient errors.
func Test_WithRetryBackoff_Retries(t *testing.T) {
	var attempts atomic.Int32
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		n := attempts.Add(1)
		if n < 3 {
			return nil, &net.OpError{Op: "dial", Net: "tcp", Err: fmt.Errorf("injected transient error")}
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       http.NoBody,
		}, nil
	})

	c, err := corrosion.NewAPIClient(
		"127.0.0.1:1",
		corrosion.WithTransport(transport),
		corrosion.WithRetryBackoff(corrosion.BackoffConfig{
			InitialInterval: 5 * time.Millisecond,
			MaxInterval:     20 * time.Millisecond,
			MaxElapsedTime:  5 * time.Second,
		}),
	)
	if err != nil {
		t.Fatalf("NewAPIClient: %v", err)
	}

	_, _ = c.ExecContext(context.Background(), "SELECT 1")

	if got := attempts.Load(); got < 3 {
		t.Errorf("attempts = %d, want >= 3 (retries enabled)", got)
	}
}

// Test_NewAPIClient_SchemeParsing: addr accepts bare host:port, http://, and https://.
func Test_NewAPIClient_SchemeParsing(t *testing.T) {
	cases := []struct {
		in      string
		wantErr bool
	}{
		{"127.0.0.1:51002", false},
		{"http://127.0.0.1:51002", false},
		{"https://corrosion.example.com:8080", false},
		{"corrosion.example.com:8080", false},
		{"ftp://bad", true},
		{"", true},
	}
	for _, tc := range cases {
		_, err := corrosion.NewAPIClient(tc.in)
		gotErr := err != nil
		if gotErr != tc.wantErr {
			t.Errorf("NewAPIClient(%q) err=%v, wantErr=%v", tc.in, err, tc.wantErr)
		}
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
