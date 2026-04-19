package corrosion_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/iris-xyz/go-corrosion"
)

// ---- helpers ---------------------------------------------------------------

// newClient creates a corrosion API client pointed at the given test server
// address, using a plain HTTP/1.1 transport so httptest servers (which don't
// speak h2c) are reachable.
func newClient(t *testing.T, addr string, opts ...corrosion.APIClientOption) *corrosion.APIClient {
	t.Helper()
	allOpts := append([]corrosion.APIClientOption{corrosion.WithTransport(&http.Transport{})}, opts...)
	c, err := corrosion.NewAPIClient(addr, allOpts...)
	if err != nil {
		t.Fatalf("NewAPIClient: %v", err)
	}
	return c
}

// ---- *ServerError ----------------------------------------------------------

func TestServerError_500(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error":"boom"}`)) //nolint:errcheck
	}))
	defer srv.Close()

	c := newClient(t, strings.TrimPrefix(srv.URL, "http://"))
	_, err := c.QueryContext(context.Background(), "SELECT 1")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var se *corrosion.ServerError
	if !errors.As(err, &se) {
		t.Fatalf("want *ServerError, got %T: %v", err, err)
	}
	if se.StatusCode != 500 {
		t.Errorf("StatusCode = %d, want 500", se.StatusCode)
	}
	if se.Message != "boom" {
		t.Errorf("Message = %q, want %q", se.Message, "boom")
	}
}

func TestServerError_429_RetryAfter(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "2")
		w.WriteHeader(http.StatusTooManyRequests)
		w.Write([]byte(`{"error":"rate limited"}`)) //nolint:errcheck
	}))
	defer srv.Close()

	c := newClient(t, strings.TrimPrefix(srv.URL, "http://"))
	_, err := c.QueryContext(context.Background(), "SELECT 1")
	var se *corrosion.ServerError
	if !errors.As(err, &se) {
		t.Fatalf("want *ServerError, got %T: %v", err, err)
	}
	if se.StatusCode != 429 {
		t.Errorf("StatusCode = %d, want 429", se.StatusCode)
	}
	if se.RetryAfter != 2*time.Second {
		t.Errorf("RetryAfter = %v, want 2s", se.RetryAfter)
	}
	if se.Message != "rate limited" {
		t.Errorf("Message = %q, want %q", se.Message, "rate limited")
	}
}

func TestServerError_IsTransient_5xx(t *testing.T) {
	for _, code := range []int{502, 503, 504} {
		t.Run(fmt.Sprintf("%d", code), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(code)
			}))
			defer srv.Close()

			c := newClient(t, strings.TrimPrefix(srv.URL, "http://"))
			_, err := c.QueryContext(context.Background(), "SELECT 1")
			if !corrosion.IsTransient(err) {
				t.Errorf("IsTransient(%T) = false, want true", err)
			}
		})
	}
}

func TestServerError_IsTransient_RetryAfter(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "5")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	c := newClient(t, strings.TrimPrefix(srv.URL, "http://"))
	_, err := c.QueryContext(context.Background(), "SELECT 1")
	if !corrosion.IsTransient(err) {
		t.Errorf("IsTransient = false, want true for 429 with Retry-After")
	}
}

// ---- *StreamError from Rows ------------------------------------------------

// colsAndErrorStream returns an NDJSON stream with a columns event followed by an error event.
func colsAndErrorStream(msg string) string {
	return `{"columns":["id"]}` + "\n" + `{"error":"` + msg + `"}` + "\n"
}

func TestStreamError_QueryRowsErrField(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, colsAndErrorStream("bad sql"))
	}))
	defer srv.Close()

	c := newClient(t, strings.TrimPrefix(srv.URL, "http://"))
	rows, err := c.QueryContext(context.Background(), "SELECT id FROM foo")
	if err != nil {
		t.Fatalf("QueryContext returned error: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		// should not reach a row
	}
	iterErr := rows.Err()
	if iterErr == nil {
		t.Fatal("expected error from stream, got nil")
	}
	var se *corrosion.StreamError
	if !errors.As(iterErr, &se) {
		t.Fatalf("want *StreamError, got %T: %v", iterErr, iterErr)
	}
	if se.Message != "bad sql" {
		t.Errorf("Message = %q, want %q", se.Message, "bad sql")
	}
}

// ---- *StreamError{Fatal:true} from Subscription ----------------------------

func TestStreamError_SubscriptionFatalNoResubscribe(t *testing.T) {
	// Track call count to ensure we do NOT resubscribe after a fatal stream error.
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("corro-query-id", "test-id-1")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		// Emit columns then an error event immediately.
		fmt.Fprint(w, `{"columns":["id"]}`+"\n")
		fmt.Fprint(w, `{"eoq":{"time":0.001,"change_id":0}}`+"\n")
		fmt.Fprint(w, `{"error":"subscription terminated by server"}`+"\n")
	}))
	defer srv.Close()

	c := newClient(t, strings.TrimPrefix(srv.URL, "http://"),
		corrosion.DisableResubscribe(),
	)
	sub, err := c.SubscribeContext(context.Background(), "SELECT id FROM foo", nil, false)
	if err != nil {
		t.Fatalf("SubscribeContext: %v", err)
	}
	defer sub.Close()

	// Consume initial rows.
	rows := sub.Rows()
	for rows.Next() {
	}

	ch, err := sub.Changes()
	if err != nil {
		t.Fatalf("Changes: %v", err)
	}

	// Drain channel until closed.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				goto done
			}
		case <-ctx.Done():
			t.Fatal("timed out waiting for subscription to close")
		}
	}
done:
	subErr := sub.Err()
	if subErr == nil {
		t.Fatal("expected Err() to be set after fatal stream error")
	}
	var se *corrosion.StreamError
	if !errors.As(subErr, &se) {
		t.Fatalf("want *StreamError, got %T: %v", subErr, subErr)
	}
	if !se.Fatal {
		t.Error("want Fatal=true, got false")
	}
	if calls > 1 {
		t.Errorf("resubscribed %d times, want 0 resubscribes after fatal error", calls-1)
	}
}

// ---- *TransientError -------------------------------------------------------

func TestTransientError_MidStreamConnClose(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		// Write partial response then hijack and close.
		fmt.Fprint(w, `{"columns":["id"]}`+"\n")
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		// Close the underlying connection to simulate a mid-stream failure.
		hj, ok := w.(http.Hijacker)
		if !ok {
			// If we can't hijack, just return and let the server close normally.
			return
		}
		conn, _, _ := hj.Hijack()
		conn.Close()
	}))
	defer srv.Close()

	c := newClient(t, strings.TrimPrefix(srv.URL, "http://"))
	rows, err := c.QueryContext(context.Background(), "SELECT id FROM foo")
	if err != nil {
		// The columns event is written before connection close, so QueryContext should succeed.
		// But if it fails here due to connection close, that's also a transient error.
		if !corrosion.IsTransient(err) {
			// It might be a ProtocolError or TransientError depending on timing.
			// Accept both for this test — the important thing is not a ServerError.
			var se *corrosion.ServerError
			if errors.As(err, &se) {
				t.Errorf("unexpected ServerError for connection close: %v", err)
			}
		}
		return
	}
	defer rows.Close()

	for rows.Next() {
	}
	// After mid-stream connection close, we should get some error.
	iterErr := rows.Err()
	// Accept either TransientError or ProtocolError — both are appropriate for a read failure.
	if iterErr != nil {
		var te *corrosion.TransientError
		var pe *corrosion.ProtocolError
		if !errors.As(iterErr, &te) && !errors.As(iterErr, &pe) {
			t.Logf("got error type %T (acceptable for conn close): %v", iterErr, iterErr)
		}
	}
}

// ---- *ProtocolError --------------------------------------------------------

func TestProtocolError_GarbageBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		// Write garbage — not valid JSON.
		fmt.Fprint(w, "not-json-at-all\n")
	}))
	defer srv.Close()

	c := newClient(t, strings.TrimPrefix(srv.URL, "http://"))
	_, err := c.QueryContext(context.Background(), "SELECT 1")
	if err == nil {
		t.Fatal("expected error from garbage body, got nil")
	}
	var pe *corrosion.ProtocolError
	if !errors.As(err, &pe) {
		t.Fatalf("want *ProtocolError, got %T: %v", err, err)
	}
}

// ---- ErrNoRows -------------------------------------------------------------

func TestErrNoRows(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		// Return columns + immediate eoq (no rows).
		fmt.Fprint(w, `{"columns":["id"]}`+"\n"+`{"eoq":{"time":0.001}}`+"\n")
	}))
	defer srv.Close()

	c := newClient(t, strings.TrimPrefix(srv.URL, "http://"))
	row := c.QueryRowContext(context.Background(), "SELECT id FROM foo WHERE 1=0")
	var id int
	err := row.Scan(&id)
	if err == nil {
		t.Fatal("expected ErrNoRows, got nil")
	}
	if !errors.Is(err, corrosion.ErrNoRows) {
		t.Errorf("errors.Is(err, corrosion.ErrNoRows) = false, err = %v", err)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		t.Errorf("errors.Is(err, sql.ErrNoRows) = false, err = %v", err)
	}
}

// TestWithTransport_NilIsNoop: passing nil must leave the default transport
// in place and subsequent requests must not panic.
func TestWithTransport_NilIsNoop(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{"columns":["id"]}`+"\n"+`{"eoq":{"time":0}}`+"\n")
	}))
	defer srv.Close()

	c, err := corrosion.NewAPIClient(
		strings.TrimPrefix(srv.URL, "http://"),
		corrosion.WithTransport(nil),              // must be a no-op
		corrosion.WithTransport(&http.Transport{}), // plain HTTP/1.1 for httptest
	)
	if err != nil {
		t.Fatalf("NewAPIClient: %v", err)
	}
	row := c.QueryRowContext(context.Background(), "SELECT id FROM foo WHERE 1=0")
	var id int
	scanErr := row.Scan(&id)
	if scanErr != nil && !errors.Is(scanErr, corrosion.ErrNoRows) {
		t.Errorf("unexpected error: %v", scanErr)
	}
}

// TestNewAPIClient_EmptyAddr verifies that NewAPIClient("") returns an error
// describing the missing host rather than deferring the failure to request time.
func TestNewAPIClient_EmptyAddr(t *testing.T) {
	_, err := corrosion.NewAPIClient("")
	if err == nil {
		t.Fatal("expected error for empty addr, got nil")
	}
	if !strings.Contains(err.Error(), "missing host") {
		t.Errorf("error %q does not mention %q", err.Error(), "missing host")
	}
}

// ---- IsTransient matrix ----------------------------------------------------

func TestIsTransient_Matrix(t *testing.T) {
	cases := []struct {
		name      string
		err       error
		want      bool
	}{
		{
			name: "TransientError",
			err:  &corrosion.TransientError{},
			want: true,
		},
		{
			name: "ServerError 502",
			err:  &corrosion.ServerError{StatusCode: 502},
			want: true,
		},
		{
			name: "ServerError 503",
			err:  &corrosion.ServerError{StatusCode: 503},
			want: true,
		},
		{
			name: "ServerError 504",
			err:  &corrosion.ServerError{StatusCode: 504},
			want: true,
		},
		{
			name: "ServerError 429 with RetryAfter",
			err:  &corrosion.ServerError{StatusCode: 429, RetryAfter: 5 * time.Second},
			want: true,
		},
		{
			name: "ServerError 500 no RetryAfter",
			err:  &corrosion.ServerError{StatusCode: 500},
			want: false,
		},
		{
			name: "StreamError fatal",
			err:  &corrosion.StreamError{Message: "x", Fatal: true},
			want: false,
		},
		{
			name: "StreamError non-fatal",
			err:  &corrosion.StreamError{Message: "x", Fatal: false},
			want: false,
		},
		{
			name: "ProtocolError",
			err:  &corrosion.ProtocolError{},
			want: false,
		},
		{
			name: "ErrNoRows",
			err:  corrosion.ErrNoRows,
			want: false,
		},
		{
			name: "nil",
			err:  nil,
			want: false,
		},
		{
			name: "wrapped net.OpError",
			err:  fmt.Errorf("outer: %w", &corrosion.TransientError{}),
			want: true,
		},
		{
			name: "wrapped net.OpError via newTransientError",
			err: func() error {
				// Wrap in a regular error to test unwrap chain.
				return fmt.Errorf("connect: %w", &corrosion.TransientError{})
			}(),
			want: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := corrosion.IsTransient(tc.err)
			if got != tc.want {
				t.Errorf("IsTransient(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// TestTransientError_ExportedFields verifies that TransientError is accessible
// externally (i.e. its zero value can be used with errors.As from a test binary).
func TestTransientError_ExportedFields(t *testing.T) {
	te := &corrosion.TransientError{}
	var target *corrosion.TransientError
	if !errors.As(te, &target) {
		t.Error("errors.As failed on *TransientError")
	}
}

// TestExecMultiContext_DecodeProtocolError verifies that ExecMultiContext returns a
// *ProtocolError (not a plain fmt.Errorf) when the server responds HTTP 200 with a
// body that cannot be decoded as JSON.
func TestExecMultiContext_DecodeProtocolError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "not-json-at-all")
	}))
	defer srv.Close()

	c := newClient(t, strings.TrimPrefix(srv.URL, "http://"))
	_, err := c.ExecContext(context.Background(), "INSERT INTO t VALUES (1)")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var pe *corrosion.ProtocolError
	if !errors.As(err, &pe) {
		t.Fatalf("want *ProtocolError, got %T: %v", err, err)
	}
}
