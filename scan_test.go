package corrosion_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/iris-xyz/go-corrosion"
)

// buildQueryStream returns an NDJSON stream with a columns event, one row, and an eoq event.
// colNames and jsonValues must have the same length. jsonValues are literal JSON tokens
// (e.g. `null`, `42`, `"hello"`, `"2024-01-02T15:04:05Z"`).
func buildQueryStream(colNames []string, jsonValues []string) string {
	quotedCols := make([]string, len(colNames))
	for i, c := range colNames {
		quotedCols[i] = `"` + c + `"`
	}
	cols := `{"columns":[` + strings.Join(quotedCols, ",") + `]}` + "\n"

	// row format: [rowID, [val0, val1, ...]]
	row := fmt.Sprintf(`{"row":[1,[%s]]}`, strings.Join(jsonValues, ",")) + "\n"
	eoq := `{"eoq":{"time":0.001}}` + "\n"
	return cols + row + eoq
}

// scanOneRow is a helper that creates a test server serving a single-row NDJSON query
// result, then calls QueryContext + Next + Scan(dests...) and returns the scan error.
// The caller checks dest values after the call.
func scanOneRow(t *testing.T, colNames []string, jsonValues []string, dests ...any) error {
	t.Helper()
	stream := buildQueryStream(colNames, jsonValues)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, stream)
	}))
	defer srv.Close()

	c := newClient(t, strings.TrimPrefix(srv.URL, "http://"))
	rows, err := c.QueryContext(context.Background(), "SELECT 1")
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("expected one row, got none")
	}
	return rows.Scan(dests...)
}

// TestScan_NullIntoPointer verifies that a JSON null value is correctly scanned
// into a *int destination (resulting in nil pointer, no error).
func TestScan_NullIntoPointer(t *testing.T) {
	var v *int
	err := scanOneRow(t, []string{"col"}, []string{"null"}, &v)
	if err != nil {
		t.Fatalf("Scan returned unexpected error: %v", err)
	}
	if v != nil {
		t.Errorf("expected nil *int, got %v", *v)
	}
}

// TestScan_NullIntoValue documents the behavior of scanning JSON null into
// non-pointer types. encoding/json treats null as a no-op for concrete types:
// the destination value is left unchanged and no error is returned.
// This differs from database/sql, which returns an error for NULL into non-pointer.
// Callers should use pointer types (e.g. *int) for columns that may be NULL.
func TestScan_NullIntoValue(t *testing.T) {
	t.Run("int unchanged", func(t *testing.T) {
		v := 99
		err := scanOneRow(t, []string{"col"}, []string{"null"}, &v)
		// encoding/json leaves the destination unchanged on null for non-pointer types
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if v != 99 {
			t.Errorf("expected int unchanged (99) after null scan, got %d", v)
		}
	})

	t.Run("string unchanged", func(t *testing.T) {
		v := "non-empty"
		err := scanOneRow(t, []string{"col"}, []string{"null"}, &v)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if v != "non-empty" {
			t.Errorf("expected string unchanged after null scan, got %q", v)
		}
	})
}

// TestScan_NullString verifies that sql.NullString is handled correctly by Scan.
// Valid JSON strings set Valid=true; JSON null sets Valid=false.
func TestScan_NullString(t *testing.T) {
	t.Run("valid string", func(t *testing.T) {
		var ns sql.NullString
		err := scanOneRow(t, []string{"col"}, []string{`"hello"`}, &ns)
		if err != nil {
			t.Fatalf("Scan error: %v", err)
		}
		if !ns.Valid {
			t.Error("expected Valid=true for non-null string")
		}
		if ns.String != "hello" {
			t.Errorf("String = %q, want %q", ns.String, "hello")
		}
	})

	t.Run("null value", func(t *testing.T) {
		var ns sql.NullString
		err := scanOneRow(t, []string{"col"}, []string{"null"}, &ns)
		if err != nil {
			t.Fatalf("Scan error: %v", err)
		}
		if ns.Valid {
			t.Error("expected Valid=false for null")
		}
	})
}

// TestScan_NullInt64 verifies that sql.NullInt64 is handled correctly by Scan.
// Valid JSON numbers set Valid=true; JSON null sets Valid=false.
func TestScan_NullInt64(t *testing.T) {
	t.Run("valid number", func(t *testing.T) {
		var ni sql.NullInt64
		err := scanOneRow(t, []string{"col"}, []string{"42"}, &ni)
		if err != nil {
			t.Fatalf("Scan error: %v", err)
		}
		if !ni.Valid {
			t.Error("expected Valid=true for non-null int64")
		}
		if ni.Int64 != 42 {
			t.Errorf("Int64 = %d, want 42", ni.Int64)
		}
	})

	t.Run("null value", func(t *testing.T) {
		var ni sql.NullInt64
		err := scanOneRow(t, []string{"col"}, []string{"null"}, &ni)
		if err != nil {
			t.Fatalf("Scan error: %v", err)
		}
		if ni.Valid {
			t.Error("expected Valid=false for null")
		}
	})
}

// TestScan_TimeRFC3339 verifies that RFC3339 JSON strings are correctly scanned
// into time.Time destinations.
func TestScan_TimeRFC3339(t *testing.T) {
	var ts time.Time
	err := scanOneRow(t, []string{"col"}, []string{`"2024-01-02T15:04:05Z"`}, &ts)
	if err != nil {
		t.Fatalf("Scan error: %v", err)
	}
	want := time.Date(2024, 1, 2, 15, 4, 5, 0, time.UTC)
	if !ts.Equal(want) {
		t.Errorf("time = %v, want %v", ts, want)
	}
}

// TestScan_ByteSlice documents that []byte destinations use encoding/json semantics
// (base64 decode), NOT database/sql semantics (raw string bytes).
// A base64-encoded JSON string decodes correctly; a non-base64 string errors.
func TestScan_ByteSlice(t *testing.T) {
	t.Run("base64 string decodes to bytes", func(t *testing.T) {
		// "aGVsbG8=" is base64 for "hello"
		var b []byte
		err := scanOneRow(t, []string{"col"}, []string{`"aGVsbG8="`}, &b)
		if err != nil {
			t.Fatalf("Scan error: %v", err)
		}
		if string(b) != "hello" {
			t.Errorf("bytes = %q, want %q", b, "hello")
		}
	})

	t.Run("non-base64 string errors", func(t *testing.T) {
		// "hello" is not valid base64 (encoding/json base64 decodes []byte dests)
		var b []byte
		err := scanOneRow(t, []string{"col"}, []string{`"hello"`}, &b)
		// encoding/json returns an error for non-base64 strings into []byte
		if err == nil {
			t.Error("expected error scanning non-base64 string into []byte, got nil")
		}
	})

	t.Run("null sets nil slice", func(t *testing.T) {
		var b []byte
		err := scanOneRow(t, []string{"col"}, []string{"null"}, &b)
		if err != nil {
			t.Fatalf("Scan error: %v", err)
		}
		if b != nil {
			t.Errorf("expected nil []byte for null, got %v", b)
		}
	})
}

// TestScan_ColumnCountMismatch verifies that Scan returns an error when the
// number of dest arguments differs from the number of columns.
func TestScan_ColumnCountMismatch(t *testing.T) {
	t.Run("too few dests", func(t *testing.T) {
		var a, b string
		// 3 columns, 2 dests
		err := scanOneRow(t,
			[]string{"c1", "c2", "c3"},
			[]string{`"x"`, `"y"`, `"z"`},
			&a, &b,
		)
		if err == nil {
			t.Fatal("expected error for column count mismatch, got nil")
		}
		if !strings.Contains(err.Error(), "expected 3") {
			t.Errorf("error message %q should mention expected count", err.Error())
		}
	})

	t.Run("too many dests", func(t *testing.T) {
		var a, b, c string
		// 2 columns, 3 dests
		err := scanOneRow(t,
			[]string{"c1", "c2"},
			[]string{`"x"`, `"y"`},
			&a, &b, &c,
		)
		if err == nil {
			t.Fatal("expected error for column count mismatch, got nil")
		}
		if !strings.Contains(err.Error(), "expected 2") {
			t.Errorf("error message %q should mention expected count", err.Error())
		}
	})
}

// TestScan_Int64Precision verifies that large int64 values (near MaxInt64) are
// scanned without float64 coercion when the destination is explicitly typed int64.
func TestScan_Int64Precision(t *testing.T) {
	const maxMinus1 = int64(9223372036854775806)
	var v int64
	err := scanOneRow(t, []string{"col"}, []string{"9223372036854775806"}, &v)
	if err != nil {
		t.Fatalf("Scan error: %v", err)
	}
	if v != maxMinus1 {
		t.Errorf("int64 = %d, want %d (precision lost?)", v, maxMinus1)
	}
}

// TestChangeEventScan_NullString verifies that ChangeEvent.Scan handles sql.Scanner
// destinations the same way Rows.Scan does: JSON null → Valid=false, JSON string → Valid=true.
func TestChangeEventScan_NullString(t *testing.T) {
	t.Run("null value", func(t *testing.T) {
		ce := &corrosion.ChangeEvent{
			Values: mustRawMessages(t, "null", `"hello"`),
		}
		var nullCol, helloCol sql.NullString
		if err := ce.Scan(&nullCol, &helloCol); err != nil {
			t.Fatalf("Scan returned unexpected error: %v", err)
		}
		if nullCol.Valid {
			t.Error("expected Valid=false for JSON null")
		}
		if !helloCol.Valid {
			t.Error("expected Valid=true for JSON string")
		}
		if helloCol.String != "hello" {
			t.Errorf("String = %q, want %q", helloCol.String, "hello")
		}
	})
}

// mustRawMessages converts literal JSON token strings into []json.RawMessage.
func mustRawMessages(t *testing.T, tokens ...string) []json.RawMessage {
	t.Helper()
	out := make([]json.RawMessage, len(tokens))
	for i, tok := range tokens {
		out[i] = json.RawMessage(tok)
	}
	return out
}

// TestScan_Int64PrecisionViaInterface documents the float64 coercion hazard
// when scanning into interface{}. Users should use typed destinations for
// numeric columns where precision matters.
func TestScan_Int64PrecisionViaInterface(t *testing.T) {
	var v any
	err := scanOneRow(t, []string{"col"}, []string{"9223372036854775806"}, &v)
	if err != nil {
		t.Fatalf("Scan error: %v", err)
	}
	// encoding/json decodes numbers into interface{} as float64, which loses precision
	// for large integers. This is expected behavior that callers must work around
	// by using a typed destination (int64, json.Number, etc.) instead.
	switch tv := v.(type) {
	case float64:
		// Expected: float64 coercion. Document the precision loss.
		if tv == float64(9223372036854775806) {
			// float64 representation differs from exact int64 — precision lost
			t.Logf("NOTE: large int64 via interface{} becomes float64=%v (precision lost — use int64 dest)", tv)
		}
	case json.Number:
		// If Scan ever switches to UseNumber, this would be the safe path.
		if tv.String() != "9223372036854775806" {
			t.Errorf("json.Number = %q, want exact int64 string", tv.String())
		}
	case int64:
		if tv != 9223372036854775806 {
			t.Errorf("int64 = %d, want exact value", tv)
		}
	default:
		t.Errorf("unexpected type %T for large int64 via interface{}", v)
	}
}
