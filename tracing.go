package corrosion

import (
	"net/http"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

// WithTracing wraps the base transport in otelhttp INSIDE the retry chain, so
// each retry attempt produces its own span. Span names follow
// [SpanNameFormatter]; extra options apply on top and may override it.
func WithTracing(extra ...otelhttp.Option) APIClientOption {
	return func(c *APIClient) {
		opts := append([]otelhttp.Option{otelhttp.WithSpanNameFormatter(SpanNameFormatter)}, extra...)
		c.tracingOpts = opts
		c.tracingEnabled = true
	}
}

// SpanNameFormatter produces span names like "corrosion POST /v1/queries".
func SpanNameFormatter(_ string, r *http.Request) string {
	if r == nil || r.URL == nil {
		return "corrosion"
	}
	return "corrosion " + r.Method + " " + r.URL.Path
}
