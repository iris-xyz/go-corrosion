package corrosion

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
)

// ServerError is returned when Corrosion responds with a non-2xx HTTP status or a structured
// JSON body containing an "error" field. Callers can use [errors.As] to inspect StatusCode,
// Code, Message, and RetryAfter.
type ServerError struct {
	// StatusCode is the HTTP status code.
	StatusCode int
	// Code is the machine-readable error code from the JSON body, if present.
	Code string
	// Message is the human-readable error text from Corrosion.
	Message string
	// RetryAfter is the duration parsed from the Retry-After response header. Zero if absent.
	RetryAfter time.Duration
}

func (e *ServerError) Error() string {
	if e.Code != "" {
		return fmt.Sprintf("corrosion server error %d (%s): %s", e.StatusCode, e.Code, e.Message)
	}
	if e.Message != "" {
		return fmt.Sprintf("corrosion server error %d: %s", e.StatusCode, e.Message)
	}
	return fmt.Sprintf("corrosion server error %d", e.StatusCode)
}

type serverErrorBody struct {
	Error   string `json:"error"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

// newServerError constructs a [*ServerError] from an HTTP response. The caller must not use
// resp.Body after this call; it is fully consumed and closed.
func newServerError(resp *http.Response) *ServerError {
	defer resp.Body.Close() //nolint:errcheck
	se := &ServerError{StatusCode: resp.StatusCode}

	// Parse Retry-After header.
	if ra := resp.Header.Get("Retry-After"); ra != "" {
		if secs, err := strconv.ParseFloat(ra, 64); err == nil {
			se.RetryAfter = time.Duration(secs * float64(time.Second))
		}
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil || len(body) == 0 {
		return se
	}

	var parsed serverErrorBody
	if err = json.Unmarshal(body, &parsed); err == nil {
		se.Code = parsed.Code
		// "error" field takes precedence over "message".
		if parsed.Error != "" {
			se.Message = parsed.Error
		} else {
			se.Message = parsed.Message
		}
	} else {
		se.Message = string(body)
	}
	return se
}

// StreamError is returned when Corrosion delivers an error event inside an NDJSON stream
// (e.g. {"error": "..."} while iterating rows or subscription changes). Fatal is always true
// for subscription streams because the upstream server terminates the stream on any error.
type StreamError struct {
	// Message is the error text from the stream event.
	Message string
	// Fatal indicates the stream is terminated and cannot be resumed without resubscribing.
	Fatal bool
}

func (e *StreamError) Error() string {
	if e.Fatal {
		return fmt.Sprintf("corrosion stream error (fatal): %s", e.Message)
	}
	return fmt.Sprintf("corrosion stream error: %s", e.Message)
}

func newStreamError(msg string, fatal bool) *StreamError {
	return &StreamError{Message: msg, Fatal: fatal}
}

// TransientError wraps a transport-layer failure (network unreachable, connection refused,
// read after EOF) that is likely worth retrying.
type TransientError struct {
	cause error
}

func (e *TransientError) Error() string {
	return fmt.Sprintf("corrosion transient error: %s", e.cause)
}

// Unwrap returns the underlying transport error.
func (e *TransientError) Unwrap() error {
	return e.cause
}

func newTransientError(cause error) *TransientError {
	return &TransientError{cause: cause}
}

// ProtocolError is returned when the NDJSON stream contains malformed data, unexpected event
// types, or JSON decode failures that prevent further reading.
type ProtocolError struct {
	// Message describes what was malformed.
	Message string
	cause   error
}

func (e *ProtocolError) Error() string {
	if e.cause != nil {
		return fmt.Sprintf("corrosion protocol error: %s: %s", e.Message, e.cause)
	}
	return fmt.Sprintf("corrosion protocol error: %s", e.Message)
}

// Unwrap returns the underlying parse/decode error.
func (e *ProtocolError) Unwrap() error {
	return e.cause
}

// newProtocolError constructs a [*ProtocolError] with an optional cause.
func newProtocolError(msg string, cause error) *ProtocolError {
	return &ProtocolError{Message: msg, cause: cause}
}

// IsTransient reports whether err is worth retrying. It returns true if:
//   - err is (or wraps) a [*TransientError], or
//   - err is (or wraps) a [*ServerError] with StatusCode ∈ {502, 503, 504}, or
//   - err is (or wraps) a [*ServerError] with RetryAfter > 0.
func IsTransient(err error) bool {
	if err == nil {
		return false
	}
	var te *TransientError
	if errors.As(err, &te) {
		return true
	}
	var se *ServerError
	if errors.As(err, &se) {
		switch se.StatusCode {
		case http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
			return true
		}
		if se.RetryAfter > 0 {
			return true
		}
	}
	return false
}
