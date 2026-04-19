package corrosion_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/iris-xyz/go-corrosion"
)

func TestBearerToken_AttachedToQuery(t *testing.T) {
	var gotAuth atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth.Store(r.Header.Get("Authorization"))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"columns":["c"]}` + "\n" + `{"eoq":{"time":0.01}}` + "\n"))
	}))
	defer srv.Close()

	c := newClient(t, strings.TrimPrefix(srv.URL, "http://"),
		corrosion.WithBearerToken("s3cret"))

	rows, err := c.QueryContext(context.Background(), "SELECT 1")
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}
	rows.Close()

	if got := gotAuth.Load(); got != "Bearer s3cret" {
		t.Errorf("Authorization header = %q, want %q", got, "Bearer s3cret")
	}
}

func TestBearerToken_AttachedToExec(t *testing.T) {
	var gotAuth atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth.Store(r.Header.Get("Authorization"))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"results":[{"rows_affected":1,"time":0.01}],"time":0.01}`))
	}))
	defer srv.Close()

	c := newClient(t, strings.TrimPrefix(srv.URL, "http://"),
		corrosion.WithBearerToken("tok"))

	if _, err := c.ExecContext(context.Background(), "UPDATE t SET x=1"); err != nil {
		t.Fatalf("ExecContext: %v", err)
	}

	if got := gotAuth.Load(); got != "Bearer tok" {
		t.Errorf("Authorization header = %q, want %q", got, "Bearer tok")
	}
}

func TestBearerToken_NotSent_WhenEmpty(t *testing.T) {
	var gotAuth atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth.Store(r.Header.Get("Authorization"))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"results":[{"rows_affected":0,"time":0}],"time":0}`))
	}))
	defer srv.Close()

	c := newClient(t, strings.TrimPrefix(srv.URL, "http://"))
	if _, err := c.ExecContext(context.Background(), "SELECT 1"); err != nil {
		t.Fatalf("ExecContext: %v", err)
	}
	if got, _ := gotAuth.Load().(string); got != "" {
		t.Errorf("Authorization header = %q, want empty", got)
	}
}

func Test401_SurfacesAsServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("WWW-Authenticate", `Bearer realm="corrosion"`)
		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte(`{"error":"unauthorized"}`)) //nolint:errcheck
	}))
	defer srv.Close()

	c := newClient(t, strings.TrimPrefix(srv.URL, "http://"),
		corrosion.WithBearerToken("wrong"))

	_, err := c.QueryContext(context.Background(), "SELECT 1")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var se *corrosion.ServerError
	if !errors.As(err, &se) {
		t.Fatalf("want *ServerError, got %T: %v", err, err)
	}
	if se.StatusCode != http.StatusUnauthorized {
		t.Errorf("StatusCode = %d, want %d", se.StatusCode, http.StatusUnauthorized)
	}
	if corrosion.IsTransient(err) {
		t.Errorf("401 should not be transient")
	}
}
