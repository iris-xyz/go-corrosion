package corrosion_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/iris-xyz/go-corrosion"
)

func TestRows_AsChangeEvent(t *testing.T) {
	body := `{"columns":["id","name"]}` + "\n" +
		`{"row":[42,[42,"alice"]]}` + "\n" +
		`{"row":[43,[43,"bob"]]}` + "\n" +
		`{"eoq":{"time":0.001}}` + "\n"

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, body)
	}))
	defer srv.Close()

	c := newClient(t, strings.TrimPrefix(srv.URL, "http://"))
	rows, err := c.QueryContext(context.Background(), "SELECT id, name FROM t")
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}
	defer rows.Close()

	var events []*corrosion.ChangeEvent
	for rows.Next() {
		events = append(events, rows.AsChangeEvent())
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("len(events) = %d, want 2", len(events))
	}

	for i, want := range []struct {
		rowID uint64
		id    int
		name  string
	}{
		{42, 42, "alice"},
		{43, 43, "bob"},
	} {
		ev := events[i]
		if ev.Type != corrosion.ChangeTypeInsert {
			t.Errorf("events[%d].Type = %v, want Insert", i, ev.Type)
		}
		if ev.RowID != want.rowID {
			t.Errorf("events[%d].RowID = %d, want %d", i, ev.RowID, want.rowID)
		}
		var id int
		var name string
		if err := ev.Scan(&id, &name); err != nil {
			t.Fatalf("events[%d].Scan: %v", i, err)
		}
		if id != want.id || name != want.name {
			t.Errorf("events[%d] = (%d, %q), want (%d, %q)", i, id, name, want.id, want.name)
		}
	}
}
