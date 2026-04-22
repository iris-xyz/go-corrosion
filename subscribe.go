package corrosion

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/cenkalti/backoff/v4"
)

// ChangeType names the kind of mutation a [ChangeEvent] represents.
type ChangeType string

// Change event kinds emitted by Corrosion subscriptions.
const (
	ChangeTypeInsert ChangeType = "insert"
	ChangeTypeUpdate ChangeType = "update"
	ChangeTypeDelete ChangeType = "delete"
)

// ChangeEvent is a row-level mutation delivered by a [Subscription]. Values
// hold the post-image (or last-known state, for deletes) in column order.
// ChangeID is monotonic per subscription and suitable for resume.
type ChangeEvent struct {
	Type     ChangeType
	RowID    uint64
	Values   []json.RawMessage
	ChangeID uint64
}

func (ce *ChangeEvent) UnmarshalJSON(data []byte) error {
	var raw []json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("invalid change event: %w", err)
	}
	if len(raw) != 4 {
		return fmt.Errorf("invalid change event: expected an array of 4 elements")
	}
	if err := json.Unmarshal(raw[0], &ce.Type); err != nil {
		return fmt.Errorf("invalid change event type: %w", err)
	}
	if err := json.Unmarshal(raw[1], &ce.RowID); err != nil {
		return fmt.Errorf("invalid change event row ID: %w", err)
	}
	if err := json.Unmarshal(raw[2], &ce.Values); err != nil {
		return fmt.Errorf("invalid change event values: %w", err)
	}
	if err := json.Unmarshal(raw[3], &ce.ChangeID); err != nil {
		return fmt.Errorf("invalid change event change ID: %w", err)
	}
	return nil
}

func (ce *ChangeEvent) MarshalJSON() ([]byte, error) {
	return json.Marshal([]any{ce.Type, ce.RowID, ce.Values, ce.ChangeID})
}

// Scan copies the column values in the change event into the values pointed at by dest.
// The number of values in dest must be the same as the number of columns in the change.
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
func (ce *ChangeEvent) Scan(dest ...any) error {
	if len(dest) != len(ce.Values) {
		return fmt.Errorf("expected %d values, got %d", len(ce.Values), len(dest))
	}

	for i, v := range ce.Values {
		if scanner, ok := dest[i].(sql.Scanner); ok {
			// Decode the JSON value to its native Go type first, then forward
			// to the Scanner so that sql.NullString, sql.NullInt64, etc. work correctly.
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

// Subscription receives updates from the Corrosion database for a desired SQL query.
type Subscription struct {
	ctx    context.Context
	cancel context.CancelFunc

	id          string
	rows        *Rows
	body        io.ReadCloser
	decoder     *json.Decoder
	resubscribe func(ctx context.Context, fromChange uint64) (*Subscription, error)
	changes     chan *ChangeEvent
	bufferSize  int
	// lastChangeID is the highest change_id observed. Seeded from the snapshot eoq
	// (skip_rows=false) and advanced by each change event.
	lastChangeID atomic.Uint64
	errMu        sync.Mutex
	err          error
	logger       *slog.Logger

	// sql and args hold the original query for POST fallback when the server has
	// forgotten the subscription (GET returns 404).
	sql  string
	args []any
}

func newSubscription(
	ctx context.Context,
	id string,
	rows *Rows,
	body io.ReadCloser,
	decoder *json.Decoder,
	resubscribe func(ctx context.Context, fromChange uint64) (*Subscription, error),
	logger *slog.Logger,
	bufferSize int,
) *Subscription {
	ctx, cancel := context.WithCancel(ctx)
	if decoder == nil {
		decoder = json.NewDecoder(body)
	}
	if logger == nil {
		logger = discardLogger
	}
	if bufferSize < 0 {
		bufferSize = 0
	}
	return &Subscription{
		ctx:         ctx,
		cancel:      cancel,
		id:          id,
		rows:        rows,
		body:        body,
		decoder:     decoder,
		resubscribe: resubscribe,
		logger:      logger,
		bufferSize:  bufferSize,
	}
}

// ID returns the server-assigned subscription ID; empty before the first successful POST.
func (s *Subscription) ID() string {
	return s.id
}

// LastChangeID returns the highest change_id observed so far. Callers can
// persist this across process restarts and pass it to [APIClient.SubscribeResume]
// for cross-process resume.
func (s *Subscription) LastChangeID() uint64 {
	return s.lastChangeID.Load()
}

// Rows returns the initial snapshot rows, or nil if skipRows was true or the
// subscription was created via [APIClient.ResubscribeContext].
func (s *Subscription) Rows() *Rows {
	return s.rows
}

// Changes returns a channel that receives change events for the query. Changes are not available until all rows
// are consumed. The channel is closed when the context is done, or an error occurs while reading the changes,
// or when the subscription is closed explicitly. If it's closed due to an error, [Subscription.Err] will return
// the error.
func (s *Subscription) Changes() (<-chan *ChangeEvent, error) {
	if s.changes != nil {
		return s.changes, nil
	}

	if s.rows != nil {
		if s.rows.eoq == nil {
			return nil, errors.New("changes are not available until all rows are consumed")
		}
		if s.rows.eoq.ChangeID != nil {
			s.lastChangeID.Store(*s.rows.eoq.ChangeID)
		}
	}
	s.changes = make(chan *ChangeEvent, s.bufferSize)

	go s.handleChangeEvents()

	return s.changes, nil
}

func (s *Subscription) handleChangeEvents() {
	defer s.cancel()
	defer close(s.changes)

	for {
		select {
		case <-s.ctx.Done():
			return
		default:
		}

		var e QueryEvent
		var err error
		if err = s.decoder.Decode(&e); err != nil {
			// Do not report an error that occurred due to context cancellation, just return.
			if s.ctx.Err() != nil {
				return
			}
			// Transport/read failure — wrap as transient so callers can retry.
			err = newTransientError(fmt.Errorf("decode query event: %w", err))
		} else if e.Error != nil {
			// Server sent an error event inside the stream. For subscriptions, any error event
			// terminates the stream (Fatal: true) and must not be retried automatically.
			err = newStreamError(*e.Error, true)
		} else if e.Change == nil {
			err = newProtocolError(fmt.Sprintf("expected change event, got: %+v", e), nil)
		} else {
			// last==0 on a fresh skip_rows=true stream: accept any first change_id.
			last := s.lastChangeID.Load()
			if last != 0 && e.Change.ChangeID != last+1 {
				err = newProtocolError(fmt.Sprintf("missed a change: expected change ID %d, got %d",
					last+1, e.Change.ChangeID), nil)
			}
		}

		if err == nil {
			s.lastChangeID.Store(e.Change.ChangeID)
			select {
			case s.changes <- e.Change:
			case <-s.ctx.Done():
				return
			}
		} else {
			// Fatal stream errors must not trigger resubscription — the subscription is dead.
			var se *StreamError
			if errors.As(err, &se) && se.Fatal {
				s.setErr(err)
				return
			}

			// Report the error if resubscribing is disabled.
			if s.resubscribe == nil {
				s.setErr(err)
				return
			}

			from := s.lastChangeID.Load()
			s.logger.Info("Resubscribing to Corrosion query due to an error.",
				"err", err, "id", s.id, "from_change", from)
			sub, sErr := s.resubscribe(s.ctx, from)
			if sErr != nil {
				// resubscribe returns a permanent error after unsuccessful retries.
				s.setErr(fmt.Errorf("resubscribe to query with backoff: %w", sErr))
				return
			}
			// On POST fallback the server issues a new ID; reset it and lastChangeID
			// so the monotonic change_id check doesn't reject the restarted sequence.
			s.rows = nil
			s.body = sub.body
			s.decoder = sub.decoder
			if sub.id != "" && sub.id != s.id {
				s.id = sub.id
				s.lastChangeID.Store(0)
			}
			if sub.resubscribe != nil {
				s.resubscribe = sub.resubscribe
			}
			// Do not close the sub to not close the body.
			sub.cancel()
		}
	}
}

// Err returns the error, if any, that was encountered during fetching changes.
// Err may be called after an explicit or implicit [Subscription.Close].
// It is safe to call concurrently with [Subscription.Changes].
func (s *Subscription) Err() error {
	s.errMu.Lock()
	defer s.errMu.Unlock()
	return s.err
}

func (s *Subscription) setErr(err error) {
	s.errMu.Lock()
	s.err = err
	s.errMu.Unlock()
}

// Close terminates the subscription, cancels any in-flight resubscribe, and
// releases the underlying response body. Close is idempotent.
func (s *Subscription) Close() error {
	s.cancel()
	return s.body.Close()
}

// SubscribeContext creates a subscription to receive updates for a desired SQL query. If skipRows is false,
// Subscription.Rows must be consumed before Subscription.Changes can be called. If skipRows is true, Subscription.Rows
// will return nil.
func (c *APIClient) SubscribeContext(
	ctx context.Context, query string, args []any, skipRows bool,
) (*Subscription, error) {
	sub, err := c.postSubscription(ctx, query, args, skipRows)
	if err != nil {
		return nil, err
	}
	sub.resubscribe = c.resubscribeWithBackoffFn(sub.id, query, args, skipRows)
	return sub, nil
}

// postSubscription issues POST /v1/subscriptions and returns a [*Subscription] with
// sql/args populated but resubscribe not yet set. The caller sets resubscribe after
// capturing the query ID.
func (c *APIClient) postSubscription(ctx context.Context, query string, args []any, skipRows bool) (*Subscription, error) {
	statement := Statement{
		Query:  query,
		Params: args,
	}
	body, err := json.Marshal(statement)
	if err != nil {
		return nil, fmt.Errorf("marshal query: %w", err)
	}

	subURL := c.baseURL.JoinPath("/v1/subscriptions")
	if skipRows {
		q := subURL.Query()
		q.Set("skip_rows", "true")
		subURL.RawQuery = q.Encode()
	}

	req, err := c.newJSONRequest(ctx, "POST", subURL, bytes.NewReader(body))
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

	id := resp.Header.Get("corro-query-id")
	if id == "" {
		_ = resp.Body.Close()
		return nil, errors.New("missing corro-query-id header in response")
	}

	wrappedBody := newCancelableReadCloser(ctx, resp.Body)
	var sub *Subscription
	if skipRows {
		// skip_rows=true: no snapshot preamble, body is fed straight to handleChangeEvents.
		dec := json.NewDecoder(wrappedBody)
		sub = newSubscription(ctx, id, nil, wrappedBody, dec, nil, c.logger, c.subBufferSize)
	} else {
		rows, err := newRows(ctx, wrappedBody, false)
		if err != nil {
			_ = wrappedBody.Close()
			return nil, err
		}
		sub = newSubscription(ctx, id, rows, rows.body, rows.decoder, nil, c.logger, c.subBufferSize)
	}
	sub.sql = query
	sub.args = args
	return sub, nil
}

// resubscribeWithBackoffFn returns a resubscribe function that:
//  1. Tries GET /v1/subscriptions/:id?from=<changeID> first (resume without re-snapshot).
//  2. Falls back to POST /v1/subscriptions if GET returns 404 (server forgot the subscription).
//  3. Returns a permanent error for non-transient server errors.
//
// The returned function retries with exponential backoff on transient failures.
func (c *APIClient) resubscribeWithBackoffFn(id, query string, args []any, skipRows bool) func(context.Context, uint64) (*Subscription, error) {
	if c.newResubBackoff == nil {
		return nil
	}
	return func(ctx context.Context, fromChange uint64) (*Subscription, error) {
		return backoff.RetryWithData(func() (*Subscription, error) {
			c.logger.Debug("Retrying to resubscribe to Corrosion query.", "id", id, "from_change", fromChange)
			sub, err := c.resumeOrRepost(ctx, id, query, args, fromChange, skipRows)
			if err != nil {
				if IsTransient(err) {
					return nil, err // retryable
				}
				return nil, backoff.Permanent(err)
			}
			return sub, nil
		}, backoff.WithContext(c.newResubBackoff(), ctx))
	}
}

// resumeOrRepost attempts GET resume first; on 404 falls back to POST with
// skip_rows=true (the caller is handleChangeEvents and cannot consume a snapshot).
func (c *APIClient) resumeOrRepost(ctx context.Context, id, query string, args []any, fromChange uint64, skipRows bool) (*Subscription, error) {
	sub, err := c.ResubscribeContext(ctx, id, fromChange)
	if err != nil {
		var se *ServerError
		if errors.As(err, &se) && se.StatusCode == http.StatusNotFound {
			c.logger.Debug("Subscription forgotten server-side, falling back to POST.", "id", id)
			sub, postErr := c.postSubscription(ctx, query, args, true)
			if postErr != nil {
				return nil, postErr
			}
			sub.resubscribe = c.resubscribeWithBackoffFn(sub.id, query, args, skipRows)
			return sub, nil
		}
		return nil, err
	}
	sub.sql = query
	sub.args = args
	sub.resubscribe = c.resubscribeWithBackoffFn(sub.id, query, args, skipRows)
	return sub, nil
}

// ResubscribeContext resumes an existing subscription by issuing
// GET /v1/subscriptions/:id?from=<fromChange>. If the server returns 404
// (subscription forgotten), a [*ServerError] with StatusCode 404 is returned.
// Callers that want automatic POST fallback should use [SubscribeContext], which
// handles this transparently on reconnect.
func (c *APIClient) ResubscribeContext(ctx context.Context, id string, fromChange uint64) (*Subscription, error) {
	subURL := c.baseURL.JoinPath("/v1/subscriptions", id)
	q := subURL.Query()
	q.Set("from", strconv.FormatUint(fromChange, 10))
	subURL.RawQuery = q.Encode()

	req, err := c.newJSONRequest(ctx, "GET", subURL, nil)
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

	wrappedBody := newCancelableReadCloser(ctx, resp.Body)
	return newSubscription(ctx, id, nil, wrappedBody, nil, nil, c.logger, c.subBufferSize), nil
}

// SubscribeResume starts receiving updates for an existing subscription identified
// by queryID, resuming from the given changeID. Use this to continue a subscription
// across process restarts.
//
// Unlike [SubscribeContext], there is no automatic POST fallback: if the server has
// forgotten the subscription, this returns a [*ServerError] with StatusCode 404 and
// the caller decides whether to restart from scratch.
func (c *APIClient) SubscribeResume(ctx context.Context, queryID string, fromChangeID uint64) (*Subscription, error) {
	sub, err := c.ResubscribeContext(ctx, queryID, fromChangeID)
	if err != nil {
		return nil, err
	}
	return sub, nil
}
