package dalgo2convex

import (
	"context"
	"errors"
	"math"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/dal-go/record"
)

type callbackRecord struct {
	record.Record
	key   func()
	data  func()
	state func(error)
}

func (r *callbackRecord) Key() *record.Key {
	if r.key != nil {
		r.key()
	}
	return r.Record.Key()
}
func (r *callbackRecord) Data() any {
	if r.data != nil {
		r.data()
	}
	return r.Record.Data()
}
func (r *callbackRecord) SetError(err error) record.Record {
	r.Record.SetError(err)
	if r.state != nil {
		r.state(err)
	}
	return r
}

func TestDALNativeObjectBridgeMissingAndNoJSONCoercion(t *testing.T) {
	c := testClient(t, fixtureOptions(), func(w http.ResponseWriter, r *http.Request) {
		args := requestArgs(t, r)
		id, _ := args["id"].String()
		if id == "missing" {
			reply(t, w, Null())
		} else {
			reply(t, w, document(id))
		}
	})
	target := Object{}
	r := record.NewRecordWithData(record.NewKeyWithID("tasks", "a"), target)
	if err := c.DB().Get(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	if !r.Exists() {
		t.Fatal("present record missing")
	}
	if n, ok := target["wide"].Int64(); !ok || n != math.MaxInt64 {
		t.Fatal("DALgo int64 coerced")
	}
	var pointer Object
	r2 := record.NewRecordWithData(record.NewKeyWithID("tasks", "b"), &pointer)
	if err := c.DB().Get(context.Background(), r2); err != nil || pointer["_id"].text != "b" {
		t.Fatal("pointer target", err)
	}
	missing := record.NewRecordWithData(record.NewKeyWithID("tasks", "missing"), Object{})
	if err := c.DB().Get(context.Background(), missing); !record.IsNotFound(err) {
		t.Fatal("missing sentinel lost", err)
	}
	if missing.Exists() {
		t.Fatal("missing retrieval state")
	}
	bad := record.NewRecordWithData(record.NewKeyWithID("tasks", "a"), map[string]any{})
	if err := c.DB().Get(context.Background(), bad); code(err) != "target" {
		t.Fatal("lossy generic data target accepted")
	}
}

func TestDALSnapshotsRecordSliceKeysAndTargetsBeforeCallbacks(t *testing.T) {
	for _, replacement := range []bool{false, true} {
		name := "nil"
		if replacement {
			name = "replacement"
		}
		t.Run(name, func(t *testing.T) {
			c := testClient(t, fixtureOptions(), func(w http.ResponseWriter, r *http.Request) {
				args := requestArgs(t, r)
				id, _ := args["id"].String()
				if id != "a" && id != "b" {
					t.Error("callback changed admitted native key")
				}
				reply(t, w, document(id))
			})
			a, b, substituted := Object{}, Object{}, Object{}
			key := record.NewKeyWithID("tasks", "a")
			originalB := record.NewRecordWithData(record.NewKeyWithID("tasks", "b"), b)
			records := []record.Record{nil, originalB}
			first := &callbackRecord{Record: record.NewRecordWithData(key, a)}
			first.data = func() {
				key.ID = "substituted"
				if replacement {
					records[1] = record.NewRecordWithData(record.NewKeyWithID("tasks", "substituted"), substituted)
				} else {
					records[1] = nil
				}
			}
			records[0] = first
			if err := c.DB().GetMulti(context.Background(), records); err != nil {
				t.Fatal(err)
			}
			if a["_id"].text != "a" || b["_id"].text != "b" || len(substituted) != 0 || !originalB.Exists() {
				t.Fatal("caller callback replaced original admitted target")
			}
		})
	}
}

func TestDALFinalStateCancellationAndDeadlineCallbackActuallyEntered(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		name := "caller"
		if deadline {
			name = "deadline"
		}
		t.Run(name, func(t *testing.T) {
			c := testClient(t, fixtureOptions(), func(w http.ResponseWriter, r *http.Request) { requestArgs(t, r); reply(t, w, document("a")) })
			var ctx context.Context
			var cancel context.CancelFunc
			if deadline {
				ctx, cancel = context.WithTimeout(context.Background(), 500*time.Millisecond)
			} else {
				ctx, cancel = context.WithCancel(context.Background())
			}
			defer cancel()
			target := Object{}
			entered := false
			r := &callbackRecord{Record: record.NewRecordWithData(record.NewKeyWithID("tasks", "a"), target)}
			r.state = func(err error) {
				if err == nil && len(target) > 0 {
					entered = true
					if deadline {
						<-ctx.Done()
					} else {
						cancel()
					}
				}
			}
			err := c.DB().Get(ctx, r)
			expected := context.Canceled
			if deadline {
				expected = context.DeadlineExceeded
			}
			if !entered {
				t.Fatal("final populated callback was not entered")
			}
			if !errors.Is(err, expected) || !errors.Is(r.Error(), expected) {
				t.Fatal("final state callback delivered success", err)
			}
		})
	}
}

func TestDALTransportErrorStateCallbackCancellationIsRedactedAndFinal(t *testing.T) {
	opts := fixtureOptions()
	opts.RoundTripper = roundTripFunc(func(*http.Request) (*http.Response, error) { return nil, errors.New("PRIVATE_TRANSPORT_TOKEN_URI") })
	c, err := New("https://owned.invalid", opts)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	observed := false
	r := &callbackRecord{Record: record.NewRecordWithData(record.NewKeyWithID("tasks", "a"), Object{})}
	r.state = func(err error) {
		if code(err) == "transport" {
			observed = true
			cancel(errors.New("PRIVATE_CALLBACK_CAUSE"))
		}
	}
	err = c.DB().Get(ctx, r)
	if !observed || !errors.Is(err, context.Canceled) || !errors.Is(r.Error(), context.Canceled) || strings.Contains(err.Error(), "PRIVATE") {
		t.Fatal("error restoration callback escaped final priority/redaction")
	}
}

func TestDALReclassificationCallbackCloseThenParentDeadlineConverges(t *testing.T) {
	c := testClient(t, fixtureOptions(), func(w http.ResponseWriter, r *http.Request) { requestArgs(t, r); reply(t, w, document("a")) })
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	target := Object{}
	closed, waited, classified := false, false, false
	r := &callbackRecord{Record: record.NewRecordWithData(record.NewKeyWithID("tasks", "a"), target)}
	r.state = func(err error) {
		if err == nil && len(target) > 0 && !closed {
			closed = true
			if closeErr := c.Close(); closeErr != nil {
				t.Error(closeErr)
			}
		} else if errors.Is(err, context.Canceled) && ctx.Err() == nil {
			waited = true
			<-ctx.Done()
		} else if errors.Is(err, context.DeadlineExceeded) {
			classified = true
		}
	}
	err := c.DB().Get(ctx, r)
	if !closed || !waited || !classified {
		t.Fatal("actual close/deadline callback sequence not entered")
	}
	if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(r.Error(), context.DeadlineExceeded) {
		t.Fatal("last error callback changed final result", err)
	}
}

func TestDALCancellationBetweenIndividualAdmissionCallbacks(t *testing.T) {
	for _, boundary := range []string{"key", "state", "data"} {
		t.Run(boundary, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			tokens := 0
			opts := fixtureOptions()
			opts.Token = func(context.Context) (string, error) { tokens++; return "owned-read-token", nil }
			c, err := New("https://owned.invalid", opts)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			firstKey, laterKey, firstData, laterData, laterSuccessState := false, false, false, false, false
			first := &callbackRecord{Record: record.NewRecordWithData(record.NewKeyWithID("tasks", "a"), Object{})}
			second := &callbackRecord{Record: record.NewRecordWithData(record.NewKeyWithID("tasks", "b"), Object{})}
			first.key = func() {
				firstKey = true
				if boundary == "key" {
					cancel()
				}
			}
			second.key = func() { laterKey = true }
			first.state = func(err error) {
				if err == nil && boundary == "state" {
					cancel()
				}
			}
			second.state = func(err error) {
				if err == nil {
					laterSuccessState = true
				}
			}
			first.data = func() {
				firstData = true
				if boundary == "data" {
					cancel()
				}
			}
			second.data = func() { laterData = true }
			// Test the provider entry directly: DALgo's external facade is not the
			// adapter's callback scheduler and may do its own key validation first.
			err = (&backend{client: c}).GetMulti(ctx, []record.Record{first, second})
			if !errors.Is(err, context.Canceled) || !errors.Is(first.Error(), context.Canceled) || !errors.Is(second.Error(), context.Canceled) {
				t.Fatal("callback cancellation not restored")
			}
			if !firstKey || laterData || laterSuccessState || tokens != 0 {
				t.Fatal("known cancellation invoked later getter/success state/auth")
			}
			if boundary == "key" && laterKey {
				t.Fatal("later Key invoked after first Key cancelled")
			}
			if boundary != "data" && firstData {
				t.Fatal("Data invoked after Key/SetError cancelled")
			}
			if boundary == "data" && !firstData {
				t.Fatal("actual Data boundary was not reached")
			}
		})
	}
}

func TestDALFinalStateRefreshBeforeNextRecordsCallback(t *testing.T) {
	c := testClient(t, fixtureOptions(), func(w http.ResponseWriter, r *http.Request) {
		args := requestArgs(t, r)
		id, _ := args["id"].String()
		reply(t, w, document(id))
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a, b := Object{}, Object{}
	first := &callbackRecord{Record: record.NewRecordWithData(record.NewKeyWithID("tasks", "a"), a)}
	second := &callbackRecord{Record: record.NewRecordWithData(record.NewKeyWithID("tasks", "b"), b)}
	cancelledAtFinal := false
	laterSawCancel := false
	first.state = func(err error) {
		if err == nil && len(a) > 0 {
			cancelledAtFinal = true
			cancel()
		}
	}
	second.state = func(err error) {
		if len(b) > 0 {
			if err == nil {
				t.Error("later success-state callback invoked after known cancellation")
			}
			if errors.Is(err, context.Canceled) {
				laterSawCancel = true
			}
		}
	}
	err := c.DB().GetMulti(ctx, []record.Record{first, second})
	if !cancelledAtFinal || !laterSawCancel || !errors.Is(err, context.Canceled) || !errors.Is(first.Error(), context.Canceled) || !errors.Is(second.Error(), context.Canceled) {
		t.Fatal("per-record final guard did not converge", err)
	}
}
