package corrosion_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/iris-xyz/go-corrosion"
)

func TestHealth_OK(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, ndjsonColumns("c"))
		fmt.Fprint(w, ndjsonRow(1, "1"))
		fmt.Fprint(w, ndjsonEoq(0))
	}))
	defer srv.Close()

	c := newClient(t, strings.TrimPrefix(srv.URL, "http://"))
	if err := c.Health(context.Background()); err != nil {
		t.Fatalf("Health: %v", err)
	}
}

func TestHealth_ServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error":"down"}`)) //nolint:errcheck
	}))
	defer srv.Close()

	c := newClient(t, strings.TrimPrefix(srv.URL, "http://"), corrosion.DisableRetry())
	if err := c.Health(context.Background()); err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestHealth_TimesOut(t *testing.T) {
	hold := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-hold:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(func() { close(hold); srv.Close() })

	c := newClient(t, strings.TrimPrefix(srv.URL, "http://"), corrosion.DisableRetry())

	start := time.Now()
	err := c.Health(context.Background())
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
	if elapsed > 4*time.Second {
		t.Errorf("elapsed = %s, want under 4s (internal Health timeout is 2s)", elapsed)
	}
}
