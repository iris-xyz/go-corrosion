package corrosion

import (
	"encoding/json"
	"testing"
)

// FuzzQueryEventDecode feeds arbitrary bytes into QueryEvent JSON decoding and
// verifies that no input causes a panic.
func FuzzQueryEventDecode(f *testing.F) {
	// columns event
	f.Add([]byte(`{"columns":["id","name","value"]}`))
	// row event
	f.Add([]byte(`{"row":[42,["hello","world",null]]}`))
	// eoq event without change_id
	f.Add([]byte(`{"eoq":{"time":0.001}}`))
	// eoq event with change_id
	f.Add([]byte(`{"eoq":{"time":0.042,"change_id":7}}`))
	// error event
	f.Add([]byte(`{"error":"something went wrong"}`))
	// change event (insert)
	f.Add([]byte(`{"change":["insert",1,[null,"alice"],3]}`))

	f.Fuzz(func(t *testing.T, data []byte) {
		var e QueryEvent
		// Must not panic; errors are acceptable.
		_ = json.Unmarshal(data, &e)
	})
}

// FuzzRowEventUnmarshal feeds arbitrary bytes into RowEvent.UnmarshalJSON and
// verifies that no input causes a panic. RowEvent uses custom unmarshal logic
// that manually parses a two-element JSON array, making it the highest-payoff
// target in this package.
func FuzzRowEventUnmarshal(f *testing.F) {
	// valid: rowID + value array
	f.Add([]byte(`[1,["foo","bar"]]`))
	// valid: rowID zero, empty values
	f.Add([]byte(`[0,[]]`))
	// valid: rowID + mixed-type values
	f.Add([]byte(`[99,[1,null,true,"hello",3.14]]`))
	// wrong element count — triggers the len-check error path
	f.Add([]byte(`[1]`))
	// not an array at all
	f.Add([]byte(`{"row_id":1}`))
	// nested arrays / deep nesting
	f.Add([]byte(`[1,[[1,2],[3,4]]]`))

	f.Fuzz(func(t *testing.T, data []byte) {
		var re RowEvent
		// Must not panic; errors are acceptable.
		_ = re.UnmarshalJSON(data)
	})
}

// FuzzExecResponseDecode feeds arbitrary bytes into ExecResponse JSON decoding
// and verifies that no input causes a panic.
func FuzzExecResponseDecode(f *testing.F) {
	// single successful result
	f.Add([]byte(`{"results":[{"rows_affected":1,"time":0.001}],"time":0.001}`))
	// multiple results
	f.Add([]byte(`{"results":[{"rows_affected":3,"time":0.001},{"rows_affected":0,"time":0.002}],"time":0.003,"version":5}`))
	// result with error
	f.Add([]byte(`{"results":[{"rows_affected":0,"time":0.0,"error":"UNIQUE constraint failed"}],"time":0.0}`))
	// empty results
	f.Add([]byte(`{"results":[],"time":0.0}`))
	// with version field
	f.Add([]byte(`{"results":[{"rows_affected":1,"time":0.005}],"time":0.005,"version":42}`))

	f.Fuzz(func(t *testing.T, data []byte) {
		var er ExecResponse
		// Must not panic; errors are acceptable.
		_ = json.Unmarshal(data, &er)
	})
}
