package dalgo2convex

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type blockingBody struct {
	readStarted  chan struct{}
	closeStarted chan struct{}
	readRelease  chan struct{}
	closeRelease chan struct{}
	onceRead     sync.Once
	onceClose    sync.Once
}

func (b *blockingBody) Read([]byte) (int, error) {
	b.onceRead.Do(func() { close(b.readStarted) })
	<-b.readRelease
	return 0, io.EOF
}
func (b *blockingBody) Close() error {
	b.onceClose.Do(func() { close(b.closeStarted) })
	<-b.closeRelease
	return nil
}

func TestHostileReaderAndCloseRemainBoundedWithoutDelayingPublicAbort(t *testing.T) {
	body := &blockingBody{readStarted: make(chan struct{}), closeStarted: make(chan struct{}), readRelease: make(chan struct{}), closeRelease: make(chan struct{})}
	opts := fixtureOptions()
	opts.MaxConcurrent = 1
	var fetches atomic.Int32
	opts.RoundTripper = roundTripFunc(func(*http.Request) (*http.Response, error) {
		fetches.Add(1)
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: body}, nil
	})
	c, err := New("https://owned.invalid", opts)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	defer func() {
		select {
		case <-body.readRelease:
		default:
			close(body.readRelease)
		}
		select {
		case <-body.closeRelease:
		default:
			close(body.closeRelease)
		}
	}()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := c.Get(ctx, "tasks", "a"); done <- err }()
	await(t, body.readStarted)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(750 * time.Millisecond):
		t.Fatal("public abort waited for body/cleanup")
	}
	await(t, body.closeStarted)
	for i := 0; i < 3; i++ {
		if _, err := c.Get(context.Background(), "tasks", "a"); code(err) != "busy" {
			t.Fatal("hostile read/close admission bypass")
		}
	}
	if fetches.Load() != 1 {
		t.Fatal("extra read admitted")
	}
	close(body.readRelease)
	if _, err := c.Get(context.Background(), "tasks", "a"); code(err) != "busy" {
		t.Fatal("unsettled close lost lease")
	}
	close(body.closeRelease)
	waitIdle(t, c)
}

func TestCloseCancelsActiveOwnedOperationWithoutClosingBorrowedTransport(t *testing.T) {
	dispatched := make(chan struct{})
	settled := make(chan struct{})
	var count atomic.Int32
	transport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if count.Add(1) == 1 {
			close(dispatched)
			<-r.Context().Done()
			close(settled)
			return nil, r.Context().Err()
		}
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(wireDocument()))}, nil
	})
	opts := fixtureOptions()
	opts.RoundTripper = transport
	c, err := New("https://owned.invalid", opts)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := c.Get(context.Background(), "tasks", "a"); done <- err }()
	await(t, dispatched)
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(750 * time.Millisecond):
		t.Fatal("Close left read active")
	}
	await(t, settled)
	waitIdle(t, c)
	if _, err := c.Get(context.Background(), "tasks", "a"); code(err) != "closed" {
		t.Fatal("closed client admitted read")
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	other, err := New("https://owned.invalid", opts)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if _, err := other.Get(context.Background(), "tasks", "a"); err != nil {
		t.Fatal("borrowed transport destroyed", err)
	}
}

func TestCallerDataAndConfigurationAreSnapshottedBeforeTokenCallback(t *testing.T) {
	ids := []string{"a", "b"}
	queryArgs := Object{"filter": String("original")}
	opts := fixtureOptions()
	static := opts.Collections["tasks"].StaticArgs
	opts.Token = func(context.Context) (string, error) {
		ids[0] = "substituted"
		queryArgs["filter"] = String("substituted")
		static["tenant"] = String("other")
		return "owned-read-token", nil
	}
	var calls atomic.Int32
	c := testClient(t, opts, func(w http.ResponseWriter, r *http.Request) {
		args := requestArgs(t, r)
		if id, ok := args["id"].String(); ok {
			if id != "a" && id != "b" {
				t.Error("token changed native key")
			}
			reply(t, w, document(id))
		} else {
			if s, _ := args["filter"].String(); s != "original" {
				t.Error("token changed query args")
			}
			reply(t, w, page(nil, true, ""))
		}
		calls.Add(1)
	})
	rows, err := c.GetMany(context.Background(), "tasks", ids)
	if err != nil || len(rows) != 2 || rows[0].ID != "a" || rows[1].ID != "b" {
		t.Fatal("keys not admitted before token", err)
	}
	queryArgs["filter"] = String("original")
	if _, err := c.QueryFunction(context.Background(), "list", queryArgs, PageOptions{Size: 1}); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 3 {
		t.Fatal("unexpected I/O")
	}
}

func TestActualEscapedRequestAndAggregateResponseBudgets(t *testing.T) {
	var requests atomic.Int32
	opts := fixtureOptions()
	opts.MaxRequestBytes = 160
	c := testClient(t, opts, func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		requestArgs(t, r)
		reply(t, w, document("a"))
	})
	if _, err := c.Get(context.Background(), "tasks", strings.Repeat("\\", 100)); code(err) != "limit" {
		t.Fatal("escaped JSON request cap missed")
	}
	if requests.Load() != 0 {
		t.Fatal("oversize escaped request dispatched")
	}
	opts = fixtureOptions()
	opts.MaxResponseBytes = 400
	batch := testClient(t, opts, func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		args := requestArgs(t, r)
		id, _ := args["id"].String()
		reply(t, w, document(id))
	})
	rows, err := batch.GetMany(context.Background(), "tasks", []string{"a", "b", "c"})
	if code(err) != "limit" || rows != nil {
		t.Fatal("aggregate batch response cap missed")
	}
	before := requests.Load()
	if before < 1 || before > 2 {
		t.Fatal("later reads dispatched after aggregate cap")
	}
}

func TestRepeatedNativeLeavesChargedPerEscapedWireOccurrenceBeforeAuth(t *testing.T) {
	var tokens atomic.Int32
	opts := fixtureOptions()
	opts.MaxRequestBytes = 512
	opts.Token = func(context.Context) (string, error) { tokens.Add(1); return "owned-read-token", nil }
	c, err := New("https://owned.invalid", opts)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	leaf := String(strings.Repeat("x", 180))
	repeated, err := Array([]Value{leaf, leaf, leaf})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.QueryFunction(context.Background(), "list", Object{"filter": repeated}, PageOptions{Size: 1}); code(err) != "limit" {
		t.Fatal("shared leaf references bypassed wire budget")
	}
	if _, err := c.QueryFunction(context.Background(), "list", Object{"filter": String(strings.Repeat("<", 100))}, PageOptions{Size: 1}); code(err) != "limit" {
		t.Fatal("HTML escaped expansion bypassed wire budget")
	}
	if tokens.Load() != 0 {
		t.Fatal("overbudget input invoked token provider")
	}
}

func TestIgnoredAuthDeadlineRetainsLeaseAndDoesNotDispatchLate(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	var tokens, requests atomic.Int32
	opts := fixtureOptions()
	opts.MaxConcurrent = 1
	opts.Timeout = 100 * time.Millisecond
	opts.Token = func(context.Context) (string, error) {
		if tokens.Add(1) == 1 {
			close(entered)
			<-release
		}
		return "owned-read-token", nil
	}
	opts.RoundTripper = roundTripFunc(func(*http.Request) (*http.Response, error) {
		requests.Add(1)
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(wireDocument()))}, nil
	})
	c, err := New("https://owned.invalid", opts)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	done := make(chan error, 1)
	go func() { _, err := c.Get(context.Background(), "tasks", "a"); done <- err }()
	await(t, entered)
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
	case <-time.After(750 * time.Millisecond):
		t.Fatal("auth deadline blocked")
	}
	if _, err := c.Get(context.Background(), "tasks", "a"); code(err) != "busy" {
		t.Fatal("deadline auth lease bypass")
	}
	close(release)
	waitIdle(t, c)
	if tokens.Load() != 1 || requests.Load() != 0 {
		t.Fatal("late expired auth dispatched")
	}
	if row, err := c.Get(context.Background(), "tasks", "a"); err != nil || row == nil {
		t.Fatal("read after auth settlement failed", err)
	}
}

func TestResponseAndErrorStillOwnsAndClosesExactBodyWithoutLeakingCause(t *testing.T) {
	closed := make(chan struct{})
	opts := fixtureOptions()
	opts.RoundTripper = roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: &callbackBody{Reader: strings.NewReader(wireDocument()), close: func() error { close(closed); return nil }}}, errors.New("PRIVATE_BORROWED_TRANSPORT_CAUSE")
	})
	c, err := New("https://owned.invalid", opts)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	row, err := c.Get(context.Background(), "tasks", "a")
	if row != nil || code(err) != "transport" || strings.Contains(err.Error(), "PRIVATE") {
		t.Fatal("transport response/error leaked or delivered")
	}
	await(t, closed)
	waitIdle(t, c)
}
