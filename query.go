package corrosion

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
)

// Statement is a SQL statement with optional positional parameters. It is the
// unit of work sent to /v1/queries and /v1/transactions.
type Statement struct {
	Query  string `json:"query"`
	Params []any  `json:"params"`
}

// ExecResponse is the decoded body of a /v1/transactions response.
type ExecResponse struct {
	Results []ExecResult `json:"results"`
	Time    float64      `json:"time"`
	Version *uint        `json:"version"`
}

// ExecResult is the per-statement outcome of a transaction. Error is non-nil
// when that statement failed; RowsAffected is then meaningless.
type ExecResult struct {
	RowsAffected uint    `json:"rows_affected"`
	Time         float64 `json:"time"`
	Error        *string `json:"error"`
}

// ExecContext writes changes to the Corrosion database for propagation through the cluster. The args are for any
// placeholder parameters in the query. Corrosion does not sync schema changes made using this method. Use Corrosion's
// schema_files to create and update the cluster's database schema.
func (c *APIClient) ExecContext(ctx context.Context, query string, args ...any) (*ExecResult, error) {
	statements := []Statement{
		{
			Query:  query,
			Params: args,
		},
	}
	resp, err := c.ExecMultiContext(ctx, statements...)
	if err != nil {
		return nil, err
	}

	if len(resp.Results) == 0 {
		return nil, fmt.Errorf("no results: %+v", resp)
	}
	return &resp.Results[0], nil
}

// ExecMultiContext writes changes to the Corrosion database for propagation through the cluster.
// Unlike ExecContext, this method allows multiple statements to be executed in a single transaction.
func (c *APIClient) ExecMultiContext(ctx context.Context, statements ...Statement) (*ExecResponse, error) {
	body, err := json.Marshal(statements)
	if err != nil {
		return nil, fmt.Errorf("marshal queries: %w", err)
	}

	req, err := c.newJSONRequest(ctx, "POST", c.baseURL.JoinPath("/v1/transactions"), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}

	resp, err := c.client.Do(req)
	if err != nil {
		var opErr *net.OpError
		if errors.As(err, &opErr) {
			return nil, newTransientError(err)
		}
		return nil, fmt.Errorf("send request: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck

	var execResp ExecResponse
	if resp.StatusCode == http.StatusOK {
		if err = json.NewDecoder(resp.Body).Decode(&execResp); err != nil {
			return nil, newProtocolError("decode exec response", err)
		}
		var errs []error
		for _, result := range execResp.Results {
			if result.Error != nil {
				errs = append(errs, errors.New(*result.Error))
			}
		}
		return &execResp, errors.Join(errs...)
	} else if resp.StatusCode == http.StatusInternalServerError {
		// Corrosion returns 500 when at least one statement in the transaction failed.
		// The body carries the same ExecResponse shape as the 200 OK path, so decode it
		// and return the partial results alongside the joined per-statement errors so
		// callers can inspect which statements succeeded and which failed.
		respBody, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("read response body: %w", err)
		}
		if err = json.Unmarshal(respBody, &execResp); err != nil {
			return nil, newServerError(&http.Response{
				StatusCode: http.StatusInternalServerError,
				Header:     resp.Header,
				Body:       io.NopCloser(bytes.NewReader(respBody)),
			})
		}
		var errs []error
		for _, result := range execResp.Results {
			if result.Error != nil {
				errs = append(errs, errors.New(*result.Error))
			}
		}
		if len(errs) == 0 {
			return nil, newServerError(&http.Response{
				StatusCode: http.StatusInternalServerError,
				Header:     resp.Header,
				Body:       io.NopCloser(bytes.NewReader(respBody)),
			})
		}
		return &execResp, errors.Join(errs...)
	}

	return nil, newServerError(resp)
}

// QueryEvent is a single NDJSON frame from /v1/queries or /v1/subscriptions.
// Exactly one of Columns, Row, EOQ, Change, or Error is set per frame.
type QueryEvent struct {
	Columns []string     `json:"columns"`
	Row     *RowEvent    `json:"row"`
	EOQ     *EndOfQuery  `json:"eoq"`
	Change  *ChangeEvent `json:"change"`
	// Error is a server-side error that occurred during query execution. It's considered fatal for the client
	// as it cannot be recovered from server-side.
	Error *string `json:"error"`
}

// EndOfQuery marks the last row of a query or snapshot. ChangeID is present
// only on subscription streams and names the watermark from which future
// change events will be numbered.
type EndOfQuery struct {
	Time     float64 `json:"time"`
	ChangeID *uint64 `json:"change_id"`
}

// RowEvent is a single row returned by a query or subscription snapshot.
// Values are JSON-encoded column values in column order.
type RowEvent struct {
	RowID  uint64
	Values []json.RawMessage
}

func (re *RowEvent) UnmarshalJSON(data []byte) error {
	var raw []json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("invalid row event: %w", err)
	}
	if len(raw) != 2 {
		return fmt.Errorf("invalid row event: expected an array of 2 elements")
	}
	if err := json.Unmarshal(raw[0], &re.RowID); err != nil {
		return fmt.Errorf("invalid row event: %w", err)
	}
	if err := json.Unmarshal(raw[1], &re.Values); err != nil {
		return fmt.Errorf("invalid row event: %w", err)
	}
	return nil
}

func (re *RowEvent) MarshalJSON() ([]byte, error) {
	return json.Marshal([]any{re.RowID, re.Values})
}

// QueryContext executes a query that returns rows, typically a SELECT.
// The args are for any placeholder parameters in the query.
func (c *APIClient) QueryContext(ctx context.Context, query string, args ...any) (*Rows, error) {
	statement := Statement{
		Query:  query,
		Params: args,
	}
	body, err := json.Marshal(statement)
	if err != nil {
		return nil, fmt.Errorf("marshal query: %w", err)
	}

	req, err := c.newJSONRequest(ctx, "POST", c.baseURL.JoinPath("/v1/queries"), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}

	resp, err := c.client.Do(req)
	if err != nil {
		var opErr *net.OpError
		if errors.As(err, &opErr) {
			return nil, newTransientError(err)
		}
		return nil, fmt.Errorf("send request: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, newServerError(resp)
	}

	rc := newCancelableReadCloser(ctx, resp.Body)
	rows, err := newRows(ctx, rc, true)
	if err != nil {
		_ = rc.Close()
		return nil, err
	}
	return rows, nil
}

// Rows is the result of a query. Its cursor starts before the first row of the result set.
// Use [Rows.Next] to advance from row to row.
type Rows struct {
	ctx        context.Context
	body       io.ReadCloser
	decoder    *json.Decoder
	eoq        *EndOfQuery
	closeOnEOQ bool

	columns []string
	row     RowEvent
	err     error
}

func newRows(ctx context.Context, body io.ReadCloser, closeOnEOQ bool) (*Rows, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	decoder := json.NewDecoder(body)
	var e QueryEvent
	if err := decoder.Decode(&e); err != nil {
		return nil, newProtocolError("decode initial columns event", err)
	}
	if e.Columns == nil {
		return nil, newProtocolError(fmt.Sprintf("expected columns event, got: %+v", e), nil)
	}

	return &Rows{
		ctx:        ctx,
		body:       body,
		decoder:    decoder,
		closeOnEOQ: closeOnEOQ,
		columns:    e.Columns,
	}, nil
}

// Columns returns the column names.
func (rs *Rows) Columns() []string {
	return rs.columns
}

// Next prepares the next result row for reading with the [Rows.Scan] method. It returns true on success, or false
// if there is no next result row or an error happened while preparing it. [Rows.Err] should be consulted to distinguish
// between the two cases.
//
// Every call to [Rows.Scan], even the first one, must be preceded by a call to [Rows.Next].
func (rs *Rows) Next() bool {
	select {
	case <-rs.ctx.Done():
		rs.err = rs.ctx.Err()
		_ = rs.Close()
		return false
	default:
	}

	var e QueryEvent
	if err := rs.decoder.Decode(&e); err != nil {
		rs.err = newProtocolError("decode query event", err)
		_ = rs.Close()
		return false
	}
	// Server-side query error event — not fatal for queries (stream ends here), but typed.
	if e.Error != nil {
		rs.err = newStreamError(*e.Error, false)
		_ = rs.Close()
		return false
	}

	if e.Row != nil {
		if len(e.Row.Values) != len(rs.columns) {
			rs.err = newProtocolError(
				fmt.Sprintf("expected %d column values, got %d", len(rs.columns), len(e.Row.Values)),
				nil,
			)
			_ = rs.Close()
			return false
		}
		rs.row = *e.Row
		return true
	}
	if e.EOQ != nil {
		rs.eoq = e.EOQ
		// Rows could be used as part of a subscription, so don't close the body if so.
		if rs.closeOnEOQ {
			_ = rs.Close()
		}
		return false
	}

	rs.err = newProtocolError(fmt.Sprintf("expected row or eoq event, got: %+v", e), nil)
	_ = rs.Close()
	return false
}

// Err returns the error, if any, that was encountered during iteration.
// Err may be called after an explicit or implicit [Rows.Close].
func (rs *Rows) Err() error {
	return rs.err
}

// Scan copies the columns in the current row into the values pointed at by dest.
// The number of values in dest must be the same as the number of columns in [Rows].
//
// Supported destination types:
//   - Standard Go types (bool, int*, float*, string, time.Time): decoded via [json.Unmarshal].
//   - Pointer types (e.g. *int): JSON null sets the pointer to nil; a value unmarshals normally.
//   - [sql.Scanner] implementors (e.g. [sql.NullString], [sql.NullInt64]): the JSON value is first
//     decoded to its native Go type (string, int64, float64, bool, nil for null), then forwarded
//     to the Scanner's Scan method — matching database/sql semantics.
//   - []byte: uses encoding/json base64 decoding (NOT raw string bytes as database/sql would).
//
// Note: JSON null scanned into a non-pointer value type is a no-op (the destination is unchanged),
// unlike database/sql which returns an error. Use pointer types for nullable columns.
func (rs *Rows) Scan(dest ...any) error {
	if rs.err != nil {
		return rs.err
	}
	if len(dest) != len(rs.columns) {
		return fmt.Errorf("expected %d values, got %d", len(rs.columns), len(dest))
	}
	return scanValues(rs.row.Values, dest)
}

// scanValues unmarshals JSON column values into dest. It handles [sql.Scanner]
// implementors by first decoding to a native Go type, then forwarding to Scan.
func scanValues(values []json.RawMessage, dest []any) error {
	for i, v := range values {
		if scanner, ok := dest[i].(sql.Scanner); ok {
			var raw any
			if err := json.Unmarshal(v, &raw); err != nil {
				return fmt.Errorf("unmarshal column value #%d: %w", i, err)
			}
			if err := scanner.Scan(raw); err != nil {
				return fmt.Errorf("scan column value #%d: %w", i, err)
			}
			continue
		}
		if err := json.Unmarshal(v, dest[i]); err != nil {
			return fmt.Errorf("unmarshal column value #%d: %w", i, err)
		}
	}
	return nil
}

// Time returns the time taken to execute the query in seconds. It's only available after all rows have been consumed.
// It doesn't include the time to send the query, receive the response, or iterate over the rows.
func (rs *Rows) Time() (float64, error) {
	if rs.eoq == nil {
		if rs.Err() != nil {
			return 0, fmt.Errorf("time is not available: %w", rs.Err())
		}
		return 0, errors.New("time is not available until all rows are consumed")
	}
	return rs.eoq.Time, nil
}

// Close closes the [Rows], preventing further enumeration. If [Rows.Next] is called and returns false,
// the [Rows] are closed automatically and it will suffice to check the result of [Rows.Err].
// Close is idempotent and does not affect the result of [Rows.Err].
func (rs *Rows) Close() error {
	return rs.body.Close()
}

// Row is the result of calling QueryRowContext to select a single row.
type Row struct {
	rows *Rows
	err  error
}

// Scan copies the columns from the matched row into the values pointed at by dest.
// If more than one row matches the query, Scan uses the first row and discards the rest.
// If no row matches the query, Scan returns ErrNoRows.
func (r *Row) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	defer r.rows.Close() //nolint:errcheck

	if !r.rows.Next() {
		if err := r.rows.Err(); err != nil {
			return err
		}
		return ErrNoRows
	}

	if err := r.rows.Scan(dest...); err != nil {
		return err
	}

	return nil
}

// Err returns the error, if any, that was encountered while running the query.
// If the query ran successfully but returned no rows, Err returns nil.
// Use Scan to check for ErrNoRows.
func (r *Row) Err() error {
	return r.err
}

// ErrNoRows is returned by Row.Scan when QueryRowContext doesn't return a row.
// It aliases [sql.ErrNoRows] so that errors.Is(err, sql.ErrNoRows) returns true.
var ErrNoRows = sql.ErrNoRows

// QueryRowContext executes a query that is expected to return at most one row.
// QueryRowContext always returns a non-nil value. Errors are deferred until Row's Scan method is called.
func (c *APIClient) QueryRowContext(ctx context.Context, query string, args ...any) *Row {
	rows, err := c.QueryContext(ctx, query, args...)
	return &Row{rows: rows, err: err}
}
