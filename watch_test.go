package corrosion_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iris-xyz/go-corrosion"
)

// streamServer serves NDJSON sub-stream content from a sequence of bodies,
// one body per HTTP request. Used to drive reconnect tests.
func streamServer(bodies ...string) *httptest.Server {
	var calls atomic.Int32
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		i := int(calls.Add(1)) - 1
		if i >= len(bodies) {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("corro-query-id", fmt.Sprintf("watch-q-%d", i))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, bodies[i])
	}))
}

func TestWatch_OnSubscribedFiresAfterRowsDrain(t *testing.T) {
	body := ndjsonColumns("c") +
		ndjsonRow(1, "a") +
		ndjsonRow(2, "b") +
		ndjsonEoq(0)
	srv := streamServer(body)
	defer srv.Close()

	c := newClient(t, strings.TrimPrefix(srv.URL, "http://"), corrosion.DisableResubscribe())

	var onSubscribed atomic.Int32
	onChange := make(chan *corrosion.ChangeEvent, 4)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		corrosion.Watch(ctx, c, corrosion.WatchOptions{
			Query:        "SELECT c FROM t",
			OnSubscribed: func() { onSubscribed.Add(1) },
			OnChange:     func(ev *corrosion.ChangeEvent) { onChange <- ev },
		})
		close(done)
	}()

	deadline := time.After(2 * time.Second)
	for onSubscribed.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("OnSubscribed never fired")
		case <-time.After(10 * time.Millisecond):
		}
	}
	if got := onSubscribed.Load(); got != 1 {
		t.Errorf("OnSubscribed fired %d times, want 1", got)
	}
	cancel()
	<-done
}

func TestWatch_OnChangeFiresForEachChange(t *testing.T) {
	body := ndjsonColumns("c") +
		ndjsonEoq(0) +
		ndjsonChange(0, 1, 5, "v5") +
		ndjsonChange(1, 1, 6, "v6") +
		ndjsonChange(2, 1, 7, "v7")
	srv := streamServer(body)
	defer srv.Close()

	c := newClient(t, strings.TrimPrefix(srv.URL, "http://"), corrosion.DisableResubscribe())

	received := make(chan *corrosion.ChangeEvent, 8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		corrosion.Watch(ctx, c, corrosion.WatchOptions{
			Query:    "SELECT c FROM t",
			OnChange: func(ev *corrosion.ChangeEvent) { received <- ev },
		})
		close(done)
	}()

	timeout := time.After(2 * time.Second)
	var seen []*corrosion.ChangeEvent
	for len(seen) < 3 {
		select {
		case ev := <-received:
			seen = append(seen, ev)
		case <-timeout:
			t.Fatalf("got %d events, want 3", len(seen))
		}
	}
	if seen[0].ChangeID != 5 || seen[1].ChangeID != 6 || seen[2].ChangeID != 7 {
		t.Errorf("ChangeIDs = [%d %d %d], want [5 6 7]",
			seen[0].ChangeID, seen[1].ChangeID, seen[2].ChangeID)
	}
	cancel()
	<-done
}

func TestWatch_ReconnectsAndCallsOnSubscribedAgain(t *testing.T) {
	first := ndjsonColumns("c") + ndjsonEoq(0)
	second := ndjsonColumns("c") + ndjsonRow(1, "x") + ndjsonEoq(1) + ndjsonChange(0, 1, 2, "x")
	srv := streamServer(first, second)
	defer srv.Close()

	c := newClient(t, strings.TrimPrefix(srv.URL, "http://"), corrosion.DisableResubscribe())

	var subCount atomic.Int32
	gotChange := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		corrosion.Watch(ctx, c, corrosion.WatchOptions{
			Query:          "SELECT c FROM t",
			ReconnectDelay: 50 * time.Millisecond,
			OnSubscribed:   func() { subCount.Add(1) },
			OnChange:       func(*corrosion.ChangeEvent) { gotChange <- struct{}{} },
		})
		close(done)
	}()

	select {
	case <-gotChange:
	case <-time.After(3 * time.Second):
		t.Fatal("never received change from second subscription")
	}
	if got := subCount.Load(); got < 2 {
		t.Errorf("OnSubscribed fired %d times, want at least 2 (reconnect)", got)
	}
	cancel()
	<-done
}

func TestWatch_StopsWhenContextCancelled(t *testing.T) {
	body := ndjsonColumns("c") + ndjsonEoq(0)
	srv := streamServer(body)
	defer srv.Close()

	c := newClient(t, strings.TrimPrefix(srv.URL, "http://"), corrosion.DisableResubscribe())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		corrosion.Watch(ctx, c, corrosion.WatchOptions{
			Query:    "SELECT c FROM t",
			OnChange: func(*corrosion.ChangeEvent) {},
		})
		close(done)
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("Watch did not exit after context cancel")
	}
}
