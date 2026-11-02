package dalgo2convex

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func fixtureOptions() Options {
	return Options{Token: func(context.Context) (string, error) { return "owned-read-token", nil }, Collections: map[string]Collection{"tasks": {GetFunction: "dalgo:getTask", StaticArgs: Object{"tenant": String("owned")}, Fields: []string{"_id", "wide", "binary"}}}, Queries: map[string]Query{"list": {Function: "dalgo:listTasks", Collection: "tasks", ArgumentNames: []string{"filter"}, StaticArgs: Object{"tenant": String("owned")}}}}
}
func testClient(t *testing.T, opts Options, handler http.HandlerFunc) *Client {
	t.Helper()
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	pool := x509.NewCertPool()
	pool.AddCert(server.Certificate())
	opts.RootCAs = pool
	c, err := New(server.URL, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Error(err)
		}
	})
	return c
}
func requestArgs(t *testing.T, r *http.Request) Object {
	t.Helper()
	if r.Method != "POST" || r.URL.Path != "/api/query" || r.URL.RawQuery != "" || r.Header.Get("Authorization") != "Bearer owned-read-token" {
		t.Error("wrong public-query request policy")
	}
	raw, err := io.ReadAll(io.LimitReader(r.Body, 65537))
	if err != nil {
		t.Error("owned request body read failed")
		return nil
	}
	var input struct {
		Path   string            `json:"path"`
		Format string            `json:"format"`
		Args   []json.RawMessage `json:"args"`
	}
	if json.Unmarshal(raw, &input) != nil || input.Format != "convex_encoded_json" || len(input.Args) != 1 {
		t.Error("wrong official query envelope")
		return nil
	}
	if input.Path != "dalgo:getTask" && input.Path != "dalgo:listTasks" {
		t.Error("unconfigured function dispatch")
	}
	v, err := DecodeValue(input.Args[0])
	if err != nil {
		t.Error(err)
		return nil
	}
	args, ok := v.Object()
	if !ok {
		t.Error("arguments not named object")
	}
	if tenant, _ := args["tenant"].String(); tenant != "owned" {
		t.Error("fixed auth scope overwritten")
	}
	return args
}
func reply(t *testing.T, w http.ResponseWriter, v Value) {
	t.Helper()
	raw, err := EncodeValue(v)
	if err != nil {
		t.Error(err)
		w.WriteHeader(500)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]any{"status": "success", "value": json.RawMessage(raw)}); err != nil {
		t.Error("owned response failed")
	}
}
func document(id string) Value {
	return Value{kind: ObjectKind, object: Object{"_id": String(id), "wide": Int64(math.MaxInt64), "binary": Bytes([]byte{0, 255}), "negativeZero": Float64(math.Copysign(0, -1)), "constructor": String("native"), "prototype": Bool(true), "__proto__": String("safe-own-field"), "": Null(), "_data": String("wire-field")}}
}
func page(docs []Value, done bool, cursor string) Value {
	return Value{kind: ObjectKind, object: Object{"page": Value{kind: ArrayKind, array: docs}, "isDone": Bool(done), "continueCursor": String(cursor)}}
}
func code(err error) string {
	var e *Error
	if errors.As(err, &e) {
		return e.Code
	}
	return ""
}
func await(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("owned fixture acknowledgement timed out")
	}
}
func waitIdle(t *testing.T, c *Client) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		c.mu.Lock()
		n := len(c.active)
		c.mu.Unlock()
		if n == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("physical work did not settle")
		}
		runtime.Gosched()
	}
}

func TestOwnedTLSGetManyNativeDuplicatesMissingAndMetadata(t *testing.T) {
	var requests atomic.Int32
	c := testClient(t, fixtureOptions(), func(w http.ResponseWriter, r *http.Request) {
		args := requestArgs(t, r)
		requests.Add(1)
		id, _ := args["id"].String()
		if id == "missing" {
			reply(t, w, Null())
			return
		}
		reply(t, w, document(id))
	})
	rows, err := c.GetMany(context.Background(), "tasks", []string{"b", "missing", "a", "a"})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 4 || rows[0].ID != "b" || rows[1] != nil || rows[2].ID != "a" || rows[3].ID != "a" {
		t.Fatal("order/duplicates/missing lost")
	}
	if n, _ := rows[2].Document["wide"].Int64(); n != math.MaxInt64 {
		t.Fatal("int64 lost")
	}
	if b, _ := rows[2].Document["binary"].Bytes(); len(b) != 2 || b[1] != 255 {
		t.Fatal("binary lost")
	}
	if rows[2].Document["__proto__"].text != "safe-own-field" || rows[2].Document["prototype"].Kind() != BoolKind || rows[2].Document["_data"].text != "wire-field" {
		t.Fatal("native own field names lost")
	}
	if _, present := rows[2].Document[""]; !present {
		t.Fatal("empty wire field lost")
	}
	rows[2].Document["wide"] = Null()
	if rows[3].Document["wide"].Kind() != Int64Kind {
		t.Fatal("duplicate result alias")
	}
	if exists, err := c.Exists(context.Background(), "tasks", "missing"); err != nil || exists {
		t.Fatal("missing exists")
	}
	if requests.Load() != 5 {
		t.Fatalf("requests %d", requests.Load())
	}
	m := c.Metadata()
	if m.Provenance != "trusted configuration" || m.DynamicDiscovery || m.AuthorizationVerified {
		t.Fatal("metadata overclaim")
	}
	m.Collections["tasks"][0] = "changed"
	if c.Metadata().Collections["tasks"][0] != "_id" {
		t.Fatal("metadata alias")
	}
}

func TestAdmissionAndTokenErrorsNoAnonymousFallback(t *testing.T) {
	var calls atomic.Int32
	opts := fixtureOptions()
	opts.Token = func(context.Context) (string, error) { calls.Add(1); return "", errors.New("PRIVATE_TOKEN_CAUSE") }
	c, err := New("https://owned.invalid", opts)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(errors.New("PRIVATE_CALLER_CAUSE"))
	if _, err := c.Get(ctx, "tasks", "a"); !errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "PRIVATE") {
		t.Fatal("caller classification/redaction")
	}
	if calls.Load() != 0 {
		t.Fatal("preabort token callback")
	}
	if _, err := c.GetMany(context.Background(), "tasks", []string{"a", ""}); code(err) != "key" {
		t.Fatal("full key admission")
	}
	if _, err := c.QueryFunction(context.Background(), "list", Object{"tenant": String("other")}, PageOptions{Size: 1}); code(err) != "input" {
		t.Fatal("scope override")
	}
	if calls.Load() != 0 {
		t.Fatal("invalid input invoked auth")
	}
	if _, err := c.Get(context.Background(), "tasks", "a"); code(err) != "auth" || strings.Contains(err.Error(), "PRIVATE") {
		t.Fatal("token error leaked")
	}
	if calls.Load() != 1 {
		t.Fatal("anonymous fallback or retry")
	}
}

func TestTrustedEndpointTLSRedirectAndBodyLimits(t *testing.T) {
	for _, endpoint := range []string{"http://example.com", "https://user:secret@example.com", "https://example.com/path", "https://example.com?token=secret", "https://example.com/#secret"} {
		if _, err := New(endpoint, fixtureOptions()); err == nil {
			t.Fatal("unsafe endpoint accepted")
		}
	}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("unverified CA request dispatched") }))
	defer server.Close()
	c, err := New(server.URL, fixtureOptions())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Get(context.Background(), "tasks", "a"); code(err) != "transport" {
		t.Fatal("unknown CA trusted")
	}
	c.Close()
	var followed atomic.Int32
	destination := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { followed.Add(1) }))
	defer destination.Close()
	redirect := testClient(t, fixtureOptions(), func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", destination.URL)
		w.WriteHeader(307)
	})
	if _, err := redirect.Get(context.Background(), "tasks", "a"); err == nil || followed.Load() != 0 {
		t.Fatal("redirect followed")
	}
	opts := fixtureOptions()
	opts.MaxResponseBytes = 128
	oversize := testClient(t, opts, func(w http.ResponseWriter, r *http.Request) {
		requestArgs(t, r)
		if _, err := io.WriteString(w, `{"status":"success","value":"`+strings.Repeat("x", 256)+`"}`); err != nil {
			t.Error("owned buffered oversize fixture write failed")
		}
	})
	if _, err := oversize.Get(context.Background(), "tasks", "a"); code(err) != "limit" {
		t.Fatal("oversize response delivered")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type callbackBody struct {
	io.Reader
	close func() error
}

func (b *callbackBody) Close() error { return b.close() }
func wireDocument() string {
	return `{"status":"success","value":{"_id":"a","wide":{"$integer":"/////////38="},"binary":{"$bytes":"AP8="}}}`
}

func TestFinalBodyCloseCancellationRefusesDelivery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	opts := fixtureOptions()
	opts.RoundTripper = roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: &callbackBody{Reader: strings.NewReader(wireDocument()), close: func() error { cancel(); return nil }}}, nil
	})
	c, err := New("https://owned.invalid", opts)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	row, err := c.Get(ctx, "tasks", "a")
	if row != nil || !errors.Is(err, context.Canceled) {
		t.Fatal("late body close cancellation delivered a row")
	}
	waitIdle(t, c)
}

func TestIgnoredForeignFetchRetainsAdmissionUntilSettlement(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		name := "caller"
		if deadline {
			name = "deadline"
		}
		t.Run(name, func(t *testing.T) {
			entered := make(chan struct{})
			release := make(chan struct{})
			lateClosed := make(chan struct{})
			var calls atomic.Int32
			opts := fixtureOptions()
			opts.MaxConcurrent = 1
			if deadline {
				opts.Timeout = 100 * time.Millisecond
			}
			opts.RoundTripper = roundTripFunc(func(*http.Request) (*http.Response, error) {
				n := calls.Add(1)
				if n == 1 {
					close(entered)
					<-release
				}
				return &http.Response{StatusCode: 200, Header: make(http.Header), Body: &callbackBody{Reader: strings.NewReader(wireDocument()), close: func() error {
					if n == 1 {
						close(lateClosed)
					}
					return nil
				}}}, nil
			})
			c, err := New("https://owned.invalid", opts)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			defer func() {
				select {
				case <-release:
				default:
					close(release)
				}
			}()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { _, err := c.Get(ctx, "tasks", "a"); done <- err }()
			await(t, entered)
			started := time.Now()
			if !deadline {
				cancel()
			}
			select {
			case err := <-done:
				expected := context.Canceled
				if deadline {
					expected = context.DeadlineExceeded
				}
				if !errors.Is(err, expected) {
					t.Fatalf("wrong cancellation %v", err)
				}
			case <-time.After(750 * time.Millisecond):
				t.Fatal("public cancellation waited for hostile fetch")
			}
			if time.Since(started) > 750*time.Millisecond {
				t.Fatal("late cancellation")
			}
			for i := 0; i < 3; i++ {
				if _, err := c.Get(context.Background(), "tasks", "a"); code(err) != "busy" {
					t.Fatal("physical lease bypass")
				}
			}
			if calls.Load() != 1 {
				t.Fatal("additional physical fetch admitted")
			}
			close(release)
			waitIdle(t, c)
			await(t, lateClosed)
			if row, err := c.Get(context.Background(), "tasks", "a"); err != nil || row == nil {
				t.Fatal("read after settlement failed", err)
			}
			waitIdle(t, c)
		})
	}
}

func TestIgnoredAuthRetainsAdmissionNoHTTP(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	var tokenCalls, fetchCalls atomic.Int32
	opts := fixtureOptions()
	opts.MaxConcurrent = 1
	opts.Token = func(context.Context) (string, error) {
		if tokenCalls.Add(1) == 1 {
			close(entered)
			<-release
		}
		return "owned-read-token", nil
	}
	opts.RoundTripper = roundTripFunc(func(*http.Request) (*http.Response, error) {
		fetchCalls.Add(1)
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(wireDocument()))}, nil
	})
	c, err := New("https://owned.invalid", opts)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := c.Get(ctx, "tasks", "a"); done <- err }()
	await(t, entered)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(750 * time.Millisecond):
		t.Fatal("auth cancellation waited")
	}
	if _, err := c.Get(context.Background(), "tasks", "a"); code(err) != "busy" {
		t.Fatal("auth lease bypass")
	}
	close(release)
	waitIdle(t, c)
	if fetchCalls.Load() != 0 || tokenCalls.Load() != 1 {
		t.Fatal("late auth dispatched anonymous/extra I/O")
	}
	if _, err := c.Get(context.Background(), "tasks", "a"); err != nil {
		t.Fatal(err)
	}
}

func TestOwnedTLSAfterDispatchCallerDeadlineDisconnectAndReopen(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		name := "caller"
		if deadline {
			name = "deadline"
		}
		t.Run(name, func(t *testing.T) {
			dispatched := make(chan struct{})
			disconnected := make(chan struct{})
			var count atomic.Int32
			opts := fixtureOptions()
			if deadline {
				opts.Timeout = 250 * time.Millisecond
			}
			c := testClient(t, opts, func(w http.ResponseWriter, r *http.Request) {
				requestArgs(t, r)
				if count.Add(1) == 1 {
					close(dispatched)
					<-r.Context().Done()
					close(disconnected)
					return
				}
				reply(t, w, document("a"))
			})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { _, err := c.Get(ctx, "tasks", "a"); done <- err }()
			await(t, dispatched)
			start := time.Now()
			if !deadline {
				cancel()
			}
			select {
			case err := <-done:
				expected := context.Canceled
				if deadline {
					expected = context.DeadlineExceeded
				}
				if !errors.Is(err, expected) {
					t.Fatal(err)
				}
			case <-time.After(750 * time.Millisecond):
				t.Fatal("after-dispatch cancellation was not prompt")
			}
			if time.Since(start) > 750*time.Millisecond {
				t.Fatal("cancel budget")
			}
			await(t, disconnected)
			waitIdle(t, c)
			if row, err := c.Get(context.Background(), "tasks", "a"); err != nil || row == nil {
				t.Fatal("reopen failed", err)
			}
		})
	}
}
