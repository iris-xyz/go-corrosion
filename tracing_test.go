package corrosion

import (
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

type tracingFakeRT struct {
	responses []tracingFakeResp
	calls     int
}

type tracingFakeResp struct {
	resp *http.Response
	err  error
}

func (f *tracingFakeRT) RoundTrip(req *http.Request) (*http.Response, error) {
	if f.calls >= len(f.responses) {
		return nil, &net.OpError{Op: "exhausted", Err: net.ErrClosed}
	}
	r := f.responses[f.calls]
	f.calls++
	if r.err != nil {
		return nil, r.err
	}
	if r.resp != nil {
		r.resp.Request = req
	}
	return r.resp, nil
}

func TestWithTracing_OneSpanPerRetryAttempt(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { otel.SetTracerProvider(prev) })

	fake := &tracingFakeRT{
		responses: []tracingFakeResp{
			{err: &net.OpError{Op: "dial", Err: net.ErrClosed}},
			{err: &net.OpError{Op: "dial", Err: net.ErrClosed}},
			{resp: &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}},
		},
	}

	fastBackoff := BackoffConfig{
		InitialInterval: 1 * time.Millisecond,
		MaxInterval:     1 * time.Millisecond,
		MaxElapsedTime:  100 * time.Millisecond,
	}

	c, err := NewAPIClient("example.invalid:0",
		WithTransport(fake),
		WithRetryBackoff(fastBackoff),
		WithTracing(),
	)
	if err != nil {
		t.Fatalf("NewAPIClient: %v", err)
	}

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "http://example.invalid/v1/queries", http.NoBody)
	if err != nil {
		t.Fatalf("http.NewRequest: %v", err)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		t.Fatalf("Do: unexpected error after retries: %v", err)
	}
	_ = resp.Body.Close()

	if fake.calls != 3 {
		t.Errorf("fake transport calls = %d, want 3", fake.calls)
	}

	spans := sr.Ended()
	if len(spans) != 3 {
		t.Fatalf("span count = %d, want 3 (one per retry attempt). otelhttp wrapping the OUTSIDE of the retry round-tripper would produce 1.", len(spans))
	}
	for i, s := range spans {
		if !strings.HasPrefix(s.Name(), "corrosion ") {
			t.Errorf("span[%d] name = %q, want \"corrosion \" prefix", i, s.Name())
		}
	}
}

func TestSpanNameFormatter(t *testing.T) {
	cases := []struct {
		name string
		req  *http.Request
		want string
	}{
		{"queries POST", mustReq(t, "POST", "http://x/v1/queries"), "corrosion POST /v1/queries"},
		{"transactions POST", mustReq(t, "POST", "http://x/v1/transactions"), "corrosion POST /v1/transactions"},
		{"subscriptions GET", mustReq(t, "GET", "http://x/v1/subscriptions/abc"), "corrosion GET /v1/subscriptions/abc"},
		{"nil request", nil, "corrosion"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := SpanNameFormatter("", tc.req)
			if got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func mustReq(t *testing.T, method, url string) *http.Request {
	t.Helper()
	r, err := http.NewRequest(method, url, http.NoBody)
	if err != nil {
		t.Fatalf("http.NewRequest: %v", err)
	}
	return r
}
