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

// ndjsonSubscriptionStream is a helper that writes a full subscription stream:
// columns event, optional rows, eoq, and optional change events.
func ndjsonColumns(cols ...string) string {
	quoted := make([]string, len(cols))
	for i, c := range cols {
		quoted[i] = `"` + c + `"`
	}
	return `{"columns":[` + strings.Join(quoted, ",") + `]}` + "\n"
}

func ndjsonEoq(changeID uint64) string {
	return fmt.Sprintf(`{"eoq":{"time":0.001,"change_id":%d}}`, changeID) + "\n"
}

func ndjsonChange(op, rowID uint64, changeID uint64, vals ...string) string {
	quotedVals := make([]string, len(vals))
	for i, v := range vals {
		quotedVals[i] = `"` + v + `"`
	}
	return fmt.Sprintf(`{"change":["%s",%d,[%s],%d]}`,
		[]string{"insert", "update", "delete"}[op], rowID,
		strings.Join(quotedVals, ","), changeID) + "\n"
}

func ndjsonRow(rowID uint64, vals ...string) string {
	quotedVals := make([]string, len(vals))
	for i, v := range vals {
		quotedVals[i] = `"` + v + `"`
	}
	return fmt.Sprintf(`{"row":[%d,[%s]]}`, rowID, strings.Join(quotedVals, ",")) + "\n"
}

// drainChanges drains n change events from ch with a timeout.
func drainChanges(t *testing.T, ch <-chan *corrosion.ChangeEvent, n int, timeout time.Duration) []*corrosion.ChangeEvent {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var out []*corrosion.ChangeEvent
	for len(out) < n {
		select {
		case ev, ok := <-ch:
			if !ok {
				t.Fatalf("channel closed after %d events, wanted %d", len(out), n)
			}
			if ev == nil {
				t.Fatal("nil change event received")
			}
			out = append(out, ev)
		case <-ctx.Done():
			t.Fatalf("timed out after %d/%d events", len(out), n)
		}
	}
	return out
}

// waitChannelClosed waits for a channel to be closed within the given timeout.
func waitChannelClosed(t *testing.T, ch <-chan *corrosion.ChangeEvent, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return
			}
		case <-ctx.Done():
			t.Fatal("timed out waiting for channel to close")
		}
	}
}

// --- Test_CapturesQueryID ---------------------------------------------------

func Test_CapturesQueryID(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "expected POST", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("corro-query-id", "abc123")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, ndjsonColumns("id"))
		fmt.Fprint(w, ndjsonEoq(0))
		// Keep stream open so Changes() can block.
	}))
	defer srv.Close()

	c := newClient(t, strings.TrimPrefix(srv.URL, "http://"),
		corrosion.DisableResubscribe())

	sub, err := c.SubscribeContext(context.Background(), "SELECT id FROM foo", nil, false)
	if err != nil {
		t.Fatalf("SubscribeContext: %v", err)
	}
	defer sub.Close()

	if sub.ID() != "abc123" {
		t.Errorf("ID() = %q, want %q", sub.ID(), "abc123")
	}
}

// --- Test_TracksLastChangeID ------------------------------------------------

func Test_TracksLastChangeID(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("corro-query-id", "qid-track")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, ndjsonColumns("id"))
		fmt.Fprint(w, ndjsonRow(1, "val1"))
		fmt.Fprint(w, ndjsonRow(2, "val2"))
		fmt.Fprint(w, ndjsonEoq(2))
		fmt.Fprint(w, ndjsonChange(0, 3, 3, "val3")) // insert, change_id=3
		fmt.Fprint(w, ndjsonChange(1, 3, 4, "val4")) // update, change_id=4
		// Stream ends naturally (server closes connection).
	}))
	defer srv.Close()

	c := newClient(t, strings.TrimPrefix(srv.URL, "http://"),
		corrosion.DisableResubscribe())

	sub, err := c.SubscribeContext(context.Background(), "SELECT id FROM foo", nil, false)
	if err != nil {
		t.Fatalf("SubscribeContext: %v", err)
	}
	defer sub.Close()

	// Drain rows.
	rows := sub.Rows()
	for rows.Next() {
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}

	ch, err := sub.Changes()
	if err != nil {
		t.Fatalf("Changes: %v", err)
	}

	// Receive exactly 2 changes.
	drainChanges(t, ch, 2, 5*time.Second)

	// Wait for stream to close (EOF after 2 changes).
	waitChannelClosed(t, ch, 5*time.Second)

	if sub.LastChangeID() != 4 {
		t.Errorf("LastChangeID() = %d, want 4", sub.LastChangeID())
	}
}

// --- Test_ResumeOnTransientFailure ------------------------------------------

func Test_ResumeOnTransientFailure(t *testing.T) {
	var getCount atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			// Initial subscription.
			w.Header().Set("corro-query-id", "xyz-resume")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, ndjsonColumns("id"))
			fmt.Fprint(w, ndjsonEoq(5))
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			// Simulate network drop by hijacking and closing.
			hj, ok := w.(http.Hijacker)
			if !ok {
				return
			}
			conn, _, _ := hj.Hijack()
			conn.Close()
			return
		}

		// GET /v1/subscriptions/xyz-resume?from=5
		if r.Method == http.MethodGet {
			getCount.Add(1)
			// URL is /v1/subscriptions/xyz-resume?from=5
			from := r.URL.Query().Get("from")
			if from != "5" {
				http.Error(w, "wrong from: "+from, http.StatusBadRequest)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, ndjsonChange(0, 10, 6, "newval")) // change_id=6
			// Keep open to allow receipt.
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			// Pause a bit to ensure client receives the change.
			time.Sleep(200 * time.Millisecond)
		}
	}))
	defer srv.Close()

	// Use a fast backoff so test doesn't take too long.
	c := newClient(t, strings.TrimPrefix(srv.URL, "http://"))

	sub, err := c.SubscribeContext(context.Background(), "SELECT id FROM foo", nil, false)
	if err != nil {
		t.Fatalf("SubscribeContext: %v", err)
	}
	defer sub.Close()

	// Drain rows.
	rows := sub.Rows()
	for rows.Next() {
	}

	ch, err := sub.Changes()
	if err != nil {
		t.Fatalf("Changes: %v", err)
	}

	// Expect change_id=6 after seamless resume.
	events := drainChanges(t, ch, 1, 10*time.Second)
	if events[0].ChangeID != 6 {
		t.Errorf("ChangeID = %d, want 6", events[0].ChangeID)
	}

	if getCount.Load() < 1 {
		t.Error("expected at least one GET request for resume, got 0")
	}

	if sub.LastChangeID() != 6 {
		t.Errorf("LastChangeID() = %d, want 6", sub.LastChangeID())
	}
}

// --- Test_ResumeFallsBack_ToPOST_On_404 ------------------------------------

func Test_ResumeFallsBack_ToPOST_On_404(t *testing.T) {
	var postCount atomic.Int32
	var getCount atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			n := postCount.Add(1)
			if n == 1 {
				// Initial subscription: send eoq then close connection.
				w.Header().Set("corro-query-id", "forgotten-id")
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				fmt.Fprint(w, ndjsonColumns("id"))
				fmt.Fprint(w, ndjsonEoq(5))
				if f, ok := w.(http.Flusher); ok {
					f.Flush()
				}
				hj, ok := w.(http.Hijacker)
				if !ok {
					return
				}
				conn, _, _ := hj.Hijack()
				conn.Close()
				return
			}
			// Second POST (fallback after 404): skip_rows=true — changes only.
			w.Header().Set("corro-query-id", "new-post-id")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, ndjsonChange(0, 20, 1, "fallback-val")) // change_id=1
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			time.Sleep(200 * time.Millisecond)
			return
		}

		if r.Method == http.MethodGet {
			getCount.Add(1)
			// Server has forgotten — return 404.
			http.NotFound(w, r)
			return
		}
	}))
	defer srv.Close()

	c := newClient(t, strings.TrimPrefix(srv.URL, "http://"))

	sub, err := c.SubscribeContext(context.Background(), "SELECT id FROM foo", nil, false)
	if err != nil {
		t.Fatalf("SubscribeContext: %v", err)
	}
	defer sub.Close()

	rows := sub.Rows()
	for rows.Next() {
	}

	ch, err := sub.Changes()
	if err != nil {
		t.Fatalf("Changes: %v", err)
	}

	// Expect change from the fallback POST.
	events := drainChanges(t, ch, 1, 10*time.Second)
	if events[0].ChangeID != 1 {
		t.Errorf("ChangeID = %d, want 1", events[0].ChangeID)
	}

	if getCount.Load() < 1 {
		t.Errorf("expected at least 1 GET, got %d", getCount.Load())
	}
	if postCount.Load() < 2 {
		t.Errorf("expected 2 POSTs (initial + fallback), got %d", postCount.Load())
	}
}

// --- Test_DoesNotResubscribe_OnFatalStreamError -----------------------------

func Test_DoesNotResubscribe_OnFatalStreamError(t *testing.T) {
	var reqCount atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqCount.Add(1)
		w.Header().Set("corro-query-id", "fatal-id")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, ndjsonColumns("id"))
		fmt.Fprint(w, ndjsonEoq(0))
		fmt.Fprint(w, `{"error":"fatal bad sql"}`+"\n")
	}))
	defer srv.Close()

	c := newClient(t, strings.TrimPrefix(srv.URL, "http://"),
		corrosion.DisableResubscribe())

	sub, err := c.SubscribeContext(context.Background(), "SELECT id FROM bad_table", nil, false)
	if err != nil {
		t.Fatalf("SubscribeContext: %v", err)
	}
	defer sub.Close()

	rows := sub.Rows()
	for rows.Next() {
	}

	ch, err := sub.Changes()
	if err != nil {
		t.Fatalf("Changes: %v", err)
	}

	waitChannelClosed(t, ch, 5*time.Second)

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
	if reqCount.Load() > 1 {
		t.Errorf("resubscribed %d times, want 0 resubscribes after fatal error", reqCount.Load()-1)
	}
}

// --- Test_SubscribeResume ---------------------------------------------------

func Test_SubscribeResume(t *testing.T) {
	t.Run("issues GET not POST", func(t *testing.T) {
		var postCount atomic.Int32
		var getCount atomic.Int32

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPost {
				postCount.Add(1)
				http.Error(w, "unexpected POST", http.StatusBadRequest)
				return
			}
			if r.Method == http.MethodGet {
				getCount.Add(1)
				// Verify from param (URL: /v1/subscriptions/test-id?from=10).
				if r.URL.Query().Get("from") != "10" {
					http.Error(w, "wrong from: "+r.URL.Query().Get("from"), http.StatusBadRequest)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				fmt.Fprint(w, ndjsonChange(0, 1, 11, "v"))
				if f, ok := w.(http.Flusher); ok {
					f.Flush()
				}
				time.Sleep(100 * time.Millisecond)
			}
		}))
		defer srv.Close()

		c := newClient(t, strings.TrimPrefix(srv.URL, "http://"),
			corrosion.DisableResubscribe())

		sub, err := c.SubscribeResume(context.Background(), "test-id", 10)
		if err != nil {
			t.Fatalf("SubscribeResume: %v", err)
		}
		defer sub.Close()

		ch, err := sub.Changes()
		if err != nil {
			t.Fatalf("Changes: %v", err)
		}

		events := drainChanges(t, ch, 1, 5*time.Second)
		if events[0].ChangeID != 11 {
			t.Errorf("ChangeID = %d, want 11", events[0].ChangeID)
		}
		if postCount.Load() > 0 {
			t.Errorf("unexpected POST requests: %d", postCount.Load())
		}
		if getCount.Load() < 1 {
			t.Errorf("expected at least 1 GET, got %d", getCount.Load())
		}
	})

	t.Run("404 surfaces as ServerError without retry", func(t *testing.T) {
		var reqCount atomic.Int32

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			reqCount.Add(1)
			http.NotFound(w, r)
		}))
		defer srv.Close()

		c := newClient(t, strings.TrimPrefix(srv.URL, "http://"),
			corrosion.DisableResubscribe())

		_, err := c.SubscribeResume(context.Background(), "gone-id", 10)
		if err == nil {
			t.Fatal("expected error for 404, got nil")
		}

		var se *corrosion.ServerError
		if !errors.As(err, &se) {
			t.Fatalf("want *ServerError, got %T: %v", err, err)
		}
		if se.StatusCode != http.StatusNotFound {
			t.Errorf("StatusCode = %d, want 404", se.StatusCode)
		}
		if reqCount.Load() != 1 {
			t.Errorf("expected exactly 1 request (no retry), got %d", reqCount.Load())
		}
	})
}

// --- Test_SubscriptionBufferSize -------------------------------------------

// Test_SubscriptionBufferSize verifies that WithSubscriptionBufferSize sizes
// the Changes() channel. With a buffer of N, N change events must be
// receivable without a consumer goroutine present (the stream reader can
// enqueue them into the channel buffer). Without a buffer, the reader would
// block on the first send.
func Test_SubscriptionBufferSize(t *testing.T) {
	const bufSize = 4

	// Serve a snapshot (columns + eoq) followed by bufSize change events,
	// then hold the connection open so the reader goroutine fills the
	// buffer without further pressure.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("corro-query-id", "buf-test")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, ndjsonColumns("id"))
		fmt.Fprint(w, ndjsonEoq(0))
		for i := 1; i <= bufSize; i++ {
			fmt.Fprint(w, ndjsonChange(0, uint64(i), uint64(i), "v"))
		}
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		// Hold open so the stream stays alive while we drain.
		<-r.Context().Done()
	}))
	defer srv.Close()

	c := newClient(t, strings.TrimPrefix(srv.URL, "http://"),
		corrosion.DisableResubscribe(),
		corrosion.WithSubscriptionBufferSize(bufSize))

	sub, err := c.SubscribeContext(context.Background(), "SELECT id FROM foo", nil, false)
	if err != nil {
		t.Fatalf("SubscribeContext: %v", err)
	}
	defer sub.Close()

	// Drain rows (snapshot empty).
	for sub.Rows().Next() {
	}

	ch, err := sub.Changes()
	if err != nil {
		t.Fatalf("Changes: %v", err)
	}

	// cap(ch) is the authoritative signal that the option took effect.
	if got := cap(ch); got != bufSize {
		t.Errorf("cap(Changes()) = %d, want %d", got, bufSize)
	}

	// Sanity: all events should be drainable.
	drainChanges(t, ch, bufSize, 5*time.Second)
}

// Test_SubscriptionBufferSize_DefaultUnbuffered verifies that the default
// (no option set) preserves the historical unbuffered channel behavior.
func Test_SubscriptionBufferSize_DefaultUnbuffered(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("corro-query-id", "default-buf")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, ndjsonColumns("id"))
		fmt.Fprint(w, ndjsonEoq(0))
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		<-r.Context().Done()
	}))
	defer srv.Close()

	c := newClient(t, strings.TrimPrefix(srv.URL, "http://"),
		corrosion.DisableResubscribe())

	sub, err := c.SubscribeContext(context.Background(), "SELECT id FROM foo", nil, false)
	if err != nil {
		t.Fatalf("SubscribeContext: %v", err)
	}
	defer sub.Close()

	for sub.Rows().Next() {
	}
	ch, err := sub.Changes()
	if err != nil {
		t.Fatalf("Changes: %v", err)
	}
	if got := cap(ch); got != 0 {
		t.Errorf("default cap(Changes()) = %d, want 0 (unbuffered)", got)
	}
}

// Test_SkipRows_ChangesOnly: skip_rows=true stream delivers changes with no
// preamble; the first change is accepted at any change_id (last==0).
func Test_SkipRows_ChangesOnly(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("corro-query-id", "sr-changes-only")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		// No columns, no eoq — changes arrive directly. Use non-1
		// starting change_id to prove the first change is accepted with
		// lastChangeID==0.
		fmt.Fprint(w, ndjsonChange(0, 1, 7, "v7"))
		fmt.Fprint(w, ndjsonChange(0, 2, 8, "v8"))
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		<-r.Context().Done()
	}))
	defer srv.Close()

	c := newClient(t, strings.TrimPrefix(srv.URL, "http://"),
		corrosion.DisableResubscribe())

	sub, err := c.SubscribeContext(context.Background(), "SELECT id FROM foo", nil, true)
	if err != nil {
		t.Fatalf("SubscribeContext: %v", err)
	}
	defer sub.Close()

	if sub.Rows() != nil {
		t.Errorf("Rows() = %v, want nil for skip_rows=true", sub.Rows())
	}

	ch, err := sub.Changes()
	if err != nil {
		t.Fatalf("Changes: %v", err)
	}

	events := drainChanges(t, ch, 2, 5*time.Second)
	if events[0].ChangeID != 7 {
		t.Errorf("events[0].ChangeID = %d, want 7", events[0].ChangeID)
	}
	if events[1].ChangeID != 8 {
		t.Errorf("events[1].ChangeID = %d, want 8", events[1].ChangeID)
	}
	if got := sub.LastChangeID(); got != 8 {
		t.Errorf("LastChangeID() = %d, want 8", got)
	}
}

// Test_SkipRows_SilentThenChanges: skip_rows=true stream stays silent until
// the first change arrives; Changes() must not block on a preamble that never comes.
func Test_SkipRows_SilentThenChanges(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("corro-query-id", "sr-silent")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		// No preamble, no changes for a while — simulate an empty table.
		select {
		case <-time.After(150 * time.Millisecond):
		case <-r.Context().Done():
			return
		}
		fmt.Fprint(w, ndjsonChange(0, 1, 1, "v1"))
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		<-r.Context().Done()
	}))
	defer srv.Close()

	c := newClient(t, strings.TrimPrefix(srv.URL, "http://"),
		corrosion.DisableResubscribe())

	sub, err := c.SubscribeContext(context.Background(), "SELECT id FROM foo", nil, true)
	if err != nil {
		t.Fatalf("SubscribeContext: %v", err)
	}
	defer sub.Close()

	ch, err := sub.Changes()
	if err != nil {
		t.Fatalf("Changes: %v", err)
	}

	events := drainChanges(t, ch, 1, 5*time.Second)
	if events[0].ChangeID != 1 {
		t.Errorf("ChangeID = %d, want 1", events[0].ChangeID)
	}
	if got := sub.LastChangeID(); got != 1 {
		t.Errorf("LastChangeID() = %d, want 1", got)
	}
}

// Test_SkipRows_MonotonicityViolation_IsProtocolError: a gap in the change_id
// sequence surfaces as *ProtocolError via Err(), not silently accepted.
func Test_SkipRows_MonotonicityViolation_IsProtocolError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("corro-query-id", "sr-gap")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		// First change accepted (last==0), second has a gap: want 8, got 9.
		fmt.Fprint(w, ndjsonChange(0, 1, 7, "v7"))
		fmt.Fprint(w, ndjsonChange(0, 2, 9, "v9"))
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		<-r.Context().Done()
	}))
	defer srv.Close()

	c := newClient(t, strings.TrimPrefix(srv.URL, "http://"),
		corrosion.DisableResubscribe())

	sub, err := c.SubscribeContext(context.Background(), "SELECT id FROM foo", nil, true)
	if err != nil {
		t.Fatalf("SubscribeContext: %v", err)
	}
	defer sub.Close()

	ch, err := sub.Changes()
	if err != nil {
		t.Fatalf("Changes: %v", err)
	}

	// First event delivers normally.
	evts := drainChanges(t, ch, 1, 5*time.Second)
	if evts[0].ChangeID != 7 {
		t.Fatalf("first ChangeID = %d, want 7", evts[0].ChangeID)
	}
	// Second event triggers the monotonicity check and closes the channel.
	waitChannelClosed(t, ch, 5*time.Second)

	subErr := sub.Err()
	if subErr == nil {
		t.Fatal("expected Err() to be set after monotonicity violation")
	}
	var pe *corrosion.ProtocolError
	if !errors.As(subErr, &pe) {
		t.Fatalf("want *ProtocolError, got %T: %v", subErr, subErr)
	}
	if !strings.Contains(pe.Error(), "missed a change") {
		t.Errorf("Err() = %q, want it to mention 'missed a change'", pe.Error())
	}
}

// Test_SkipRows_UnexpectedPreamble_IsProtocolError: on skip_rows=true, any frame
// other than a change event (e.g. legacy columns preamble) surfaces as *ProtocolError.
func Test_SkipRows_UnexpectedPreamble_IsProtocolError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("corro-query-id", "sr-bad-preamble")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		// Unexpected on skip_rows=true: legacy snapshot preamble.
		fmt.Fprint(w, ndjsonColumns("id"))
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		<-r.Context().Done()
	}))
	defer srv.Close()

	c := newClient(t, strings.TrimPrefix(srv.URL, "http://"),
		corrosion.DisableResubscribe())

	sub, err := c.SubscribeContext(context.Background(), "SELECT id FROM foo", nil, true)
	if err != nil {
		t.Fatalf("SubscribeContext: %v", err)
	}
	defer sub.Close()

	ch, err := sub.Changes()
	if err != nil {
		t.Fatalf("Changes: %v", err)
	}

	waitChannelClosed(t, ch, 5*time.Second)

	subErr := sub.Err()
	if subErr == nil {
		t.Fatal("expected Err() to be set after unexpected preamble frame")
	}
	var pe *corrosion.ProtocolError
	if !errors.As(subErr, &pe) {
		t.Fatalf("want *ProtocolError, got %T: %v", subErr, subErr)
	}
}
