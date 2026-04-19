package corrosion_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iris-xyz/go-corrosion"
)

// Server responds: columns event → one row → error event. Rows.Next() yields
// the row, then returns false with *StreamError; IsTransient is false (server-side
// SQL errors are not transient).
func TestQuery_MidStreamError(t *testing.T) {
	const errMsg = "table not found: machines"

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		// columns event
		fmt.Fprint(w, `{"columns":["id","name"]}`+"\n")
		// one valid row
		fmt.Fprint(w, `{"row":[1,["foo","bar"]]}`+"\n")
		// error event mid-stream
		fmt.Fprintf(w, `{"error":%q}`+"\n", errMsg)
	}))
	defer srv.Close()

	c := newClient(t, strings.TrimPrefix(srv.URL, "http://"),
		corrosion.DisableResubscribe())

	rows, err := c.QueryContext(context.Background(), "SELECT id, name FROM machines")
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}
	defer rows.Close()

	// First Next() should yield the row.
	if !rows.Next() {
		t.Fatalf("Next() returned false before any row; err = %v", rows.Err())
	}

	// Second Next() hits the error event.
	if rows.Next() {
		t.Fatal("Next() returned true after the error event, want false")
	}

	iterErr := rows.Err()
	if iterErr == nil {
		t.Fatal("Rows.Err() is nil after mid-stream error, want *StreamError")
	}

	var se *corrosion.StreamError
	if !errors.As(iterErr, &se) {
		t.Fatalf("want *StreamError, got %T: %v", iterErr, iterErr)
	}
	if !strings.Contains(se.Message, errMsg) {
		t.Errorf("StreamError.Message = %q, want it to contain %q", se.Message, errMsg)
	}

	// Server-side SQL errors are not transient — client should not retry.
	if corrosion.IsTransient(iterErr) {
		t.Error("IsTransient(mid-stream error) = true, want false")
	}
}

// Server sends: columns → eoq → one change event → error event. With
// resubscribe disabled, the *Subscription must not reconnect: Changes() closes,
// Err() returns *StreamError with Fatal=true, and the server sees exactly one
// request.
func TestSubscription_FatalError_NoResubscribe(t *testing.T) {
	var reqCount atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqCount.Add(1)
		w.Header().Set("corro-query-id", "sub-fatal")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		// snapshot: columns + eoq
		fmt.Fprint(w, `{"columns":["id"]}`+"\n")
		fmt.Fprint(w, `{"eoq":{"time":0.001,"change_id":1}}`+"\n")
		// one legitimate change
		fmt.Fprint(w, `{"change":["insert",1,["row-value"],2]}`+"\n")
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		// error terminates the stream
		fmt.Fprint(w, `{"error":"subscription killed by server"}`+"\n")
	}))
	defer srv.Close()

	// DisableResubscribe stops the internal resubscribe loop, so the raw
	// *Subscription never attempts a reconnect.
	c := newClient(t, strings.TrimPrefix(srv.URL, "http://"),
		corrosion.DisableResubscribe())

	sub, err := c.SubscribeContext(context.Background(), "SELECT id FROM t", nil, false)
	if err != nil {
		t.Fatalf("SubscribeContext: %v", err)
	}
	defer sub.Close()

	// Drain initial snapshot rows (empty snapshot).
	rows := sub.Rows()
	for rows.Next() {
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err after snapshot: %v", err)
	}

	ch, err := sub.Changes()
	if err != nil {
		t.Fatalf("Changes: %v", err)
	}

	// Receive the single change event before the error terminates the stream.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var changeCount int
	loop:
	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				// Channel closed — subscription ended.
				break loop
			}
			if ev != nil {
				changeCount++
			}
		case <-ctx.Done():
			t.Fatal("timed out waiting for subscription channel to close")
		}
	}

	// We expect exactly one change before the error closes the channel.
	if changeCount != 1 {
		t.Errorf("received %d change events, want 1", changeCount)
	}

	subErr := sub.Err()
	if subErr == nil {
		t.Fatal("Subscription.Err() is nil after fatal error, want *StreamError")
	}

	var se *corrosion.StreamError
	if !errors.As(subErr, &se) {
		t.Fatalf("want *StreamError, got %T: %v", subErr, subErr)
	}
	if !se.Fatal {
		t.Error("StreamError.Fatal = false, want true for subscription stream errors")
	}

	// No reconnect attempt: DisableResubscribe leaves resubscribe nil, so
	// handleChangeEvents returns immediately on fatal error. One request = the initial POST.
	if got := reqCount.Load(); got != 1 {
		t.Errorf("server received %d requests, want exactly 1 (no reconnect)", got)
	}
}

// bufio.Scanner has a default 64 KiB cap; the client uses json.Decoder instead,
// so a 2 MiB string value must decode without truncation.
func TestQuery_OversizedRowEvent(t *testing.T) {
	const mib2 = 2 * 1024 * 1024

	// Build a 2 MiB string value.
	bigValue := strings.Repeat("x", mib2)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{"columns":["payload"]}`+"\n")
		// row with a 2 MiB string value
		fmt.Fprintf(w, `{"row":[1,[%q]]}`+"\n", bigValue)
		fmt.Fprint(w, `{"eoq":{"time":0.001}}`+"\n")
	}))
	defer srv.Close()

	c := newClient(t, strings.TrimPrefix(srv.URL, "http://"))

	rows, err := c.QueryContext(context.Background(), "SELECT payload FROM blobs")
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatalf("Next() returned false; err = %v", rows.Err())
	}

	var got string
	if err := rows.Scan(&got); err != nil {
		t.Fatalf("Scan: %v", err)
	}

	if len(got) != mib2 {
		t.Errorf("decoded string length = %d, want %d (no truncation)", len(got), mib2)
	}
	if got != bigValue {
		t.Error("decoded string differs from the original — data was corrupted or truncated")
	}

	if rows.Next() {
		t.Error("Next() returned true after the only row, want false")
	}
	if err := rows.Err(); err != nil {
		t.Errorf("Rows.Err() = %v after eoq, want nil", err)
	}
}

// Server writes a valid columns event then closes the connection before any
// row or eoq. The json.Decoder hits EOF reading the next event, which surfaces
// as *ProtocolError.
func TestQuery_TruncatedStream(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{"columns":["id"]}`+"\n")
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		// Close the connection before sending any row or eoq.
		hj, ok := w.(http.Hijacker)
		if !ok {
			// Can't hijack: just return; server close will still trigger EOF.
			return
		}
		conn, _, _ := hj.Hijack()
		conn.Close()
	}))
	defer srv.Close()

	c := newClient(t, strings.TrimPrefix(srv.URL, "http://"))

	rows, err := c.QueryContext(context.Background(), "SELECT id FROM t")
	if err != nil {
		// If the connection close is fast enough, QueryContext itself may fail.
		// That's an acceptable outcome — the error must still be non-nil.
		t.Logf("QueryContext returned error (acceptable): %v", err)
		return
	}
	defer rows.Close()

	if rows.Next() {
		t.Error("Next() returned true on a truncated stream, want false")
	}

	iterErr := rows.Err()
	if iterErr == nil {
		t.Fatal("Rows.Err() is nil after truncated stream, want non-nil error")
	}

	// The json.Decoder returns io.EOF / io.ErrUnexpectedEOF on truncation, which
	// the Rows.Next() loop wraps as *ProtocolError.
	var pe *corrosion.ProtocolError
	if !errors.As(iterErr, &pe) {
		t.Errorf("want *ProtocolError after truncated stream, got %T: %v", iterErr, iterErr)
	}
}

// Server sends valid columns then a garbage (non-JSON) line. Rows.Err() is *ProtocolError.
func TestQuery_InvalidJSONMidStream(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{"columns":["id"]}`+"\n")
		// Not valid JSON.
		fmt.Fprint(w, "{not-json garbage}\n")
	}))
	defer srv.Close()

	c := newClient(t, strings.TrimPrefix(srv.URL, "http://"))

	rows, err := c.QueryContext(context.Background(), "SELECT id FROM t")
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		t.Error("Next() returned true after invalid JSON line, want false")
	}

	iterErr := rows.Err()
	if iterErr == nil {
		t.Fatal("Rows.Err() is nil after invalid JSON mid-stream, want non-nil error")
	}

	var pe *corrosion.ProtocolError
	if !errors.As(iterErr, &pe) {
		t.Fatalf("want *ProtocolError after invalid JSON, got %T: %v", iterErr, iterErr)
	}
}

// Server writes columns, flushes, then blocks forever. Canceling the context
// must close the body via [cancelableReadCloser], unblocking Decode and making
// Next() return false within 500ms.
func TestQuery_CtxCancelUnblocksRowsNext(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{"columns":["id"]}`+"\n")
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		// Block until the client disconnects.
		<-r.Context().Done()
	}))
	defer srv.Close()

	c := newClient(t, strings.TrimPrefix(srv.URL, "http://"),
		corrosion.DisableResubscribe())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rows, err := c.QueryContext(ctx, "SELECT id FROM t")
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}
	defer rows.Close()

	// Cancel the context after a short delay to let Next() enter Decode().
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	done := make(chan bool, 1)
	go func() {
		done <- rows.Next()
	}()

	select {
	case result := <-done:
		if result {
			t.Error("Next() returned true on a silent server, want false")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Next() did not unblock within 500ms after ctx cancel")
	}

	iterErr := rows.Err()
	if iterErr == nil {
		t.Fatal("Rows.Err() is nil after ctx cancel, want non-nil error")
	}
	if !errors.Is(iterErr, context.Canceled) {
		t.Errorf("Rows.Err() = %v, want errors.Is(err, context.Canceled) to be true", iterErr)
	}
}

// Server writes columns + eoq then blocks forever. Canceling the context
// closes the Changes() channel within 500ms; Err() is nil because ctx-cancel
// is a graceful shutdown for subscriptions.
func TestSubscription_CtxCancelClosesChanges(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("corro-query-id", "sub-silent")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{"columns":["id"]}`+"\n")
		fmt.Fprint(w, `{"eoq":{"time":0.001,"change_id":1}}`+"\n")
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		// Block forever — no change events coming.
		<-r.Context().Done()
	}))
	defer srv.Close()

	c := newClient(t, strings.TrimPrefix(srv.URL, "http://"),
		corrosion.DisableResubscribe())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sub, err := c.SubscribeContext(ctx, "SELECT id FROM t", nil, false)
	if err != nil {
		t.Fatalf("SubscribeContext: %v", err)
	}
	defer sub.Close()

	// Consume the (empty) initial snapshot.
	rows := sub.Rows()
	for rows.Next() {
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err after snapshot: %v", err)
	}

	ch, err := sub.Changes()
	if err != nil {
		t.Fatalf("Changes: %v", err)
	}

	// Cancel after a short delay.
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	timer := time.NewTimer(500 * time.Millisecond)
	defer timer.Stop()

	select {
	case _, ok := <-ch:
		if ok {
			t.Error("received an unexpected change event on a silent server")
		}
		// Channel closed — good.
	case <-timer.C:
		t.Fatal("Changes() channel did not close within 500ms after ctx cancel")
	}

	// ctx-cancel is graceful: Err() should be nil.
	if subErr := sub.Err(); subErr != nil {
		t.Errorf("Subscription.Err() = %v after ctx cancel, want nil", subErr)
	}
}
