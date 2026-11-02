package dalgo2convex

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func retained(t *testing.T, c *Client) (int, int, int) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	sumBytes, sumNodes := 0, 0
	for _, state := range c.cursors {
		sumBytes += state.retainedBytes
		sumNodes += state.retainedNodes
	}
	if sumBytes != c.cursorBytes || sumNodes != c.cursorNodes || c.cursorBytes < 0 || c.cursorNodes < 0 || c.cursorBytes > c.options.MaxCursorBytes || c.cursorNodes > c.options.MaxCursorNodes {
		t.Fatal("aggregate retained ledger inconsistent")
	}
	return c.cursorBytes, c.cursorNodes, len(c.cursors)
}

func TestCursorByteQuotaRefusalPreservesPriorHandleAndSpentWork(t *testing.T) {
	opts := fixtureOptions()
	opts.MaxCursorBytes = 350
	var calls atomic.Int32
	c := testClient(t, opts, func(w http.ResponseWriter, r *http.Request) {
		args := requestArgs(t, r)
		paging, _ := args["paginationOpts"].Object()
		switch calls.Add(1) {
		case 1:
			reply(t, w, page([]Value{document("a")}, false, "owned-c1"))
		case 2:
			reply(t, w, page([]Value{document("b")}, false, "owned-c2"))
		case 3:
			if s, _ := paging["cursor"].String(); s != "owned-c1" {
				t.Error("quota refusal advanced server position")
			}
			reply(t, w, page([]Value{document("b")}, true, ""))
		default:
			t.Error("unexpected cursor request")
		}
	})
	first, err := c.QueryFunction(context.Background(), "list", nil, PageOptions{Size: 1})
	if err != nil {
		t.Fatal(err)
	}
	beforeBytes, beforeNodes, _ := retained(t, c)
	if beforeBytes != 333 || beforeNodes != 3 {
		t.Fatal("logical state accounting changed")
	}
	c.mu.Lock()
	state := c.cursors[first.NextCursor]
	oldResponse := state.bytes
	c.mu.Unlock()
	refused, err := c.QueryFunction(context.Background(), "list", nil, PageOptions{Size: 1, Cursor: first.NextCursor})
	if code(err) != "limit" || len(refused.Documents) != 0 || refused.NextCursor != "" {
		t.Fatal("overquota successor acknowledged data")
	}
	afterBytes, afterNodes, count := retained(t, c)
	if afterBytes != beforeBytes || afterNodes != beforeNodes || count != 1 {
		t.Fatal("quota refusal refunded/replaced prior state")
	}
	c.mu.Lock()
	if state.server != "owned-c1" || !state.seen["a"] || state.seen["b"] || state.pages != 2 || state.work != 2 || state.bytes <= oldResponse {
		t.Error("refusal lost position or incurred work")
	}
	c.mu.Unlock()
	last, err := c.QueryFunction(context.Background(), "list", nil, PageOptions{Size: 1, Cursor: first.NextCursor})
	if err != nil || !last.IsDone || len(last.Documents) != 1 || last.Documents[0].ID != "b" {
		t.Fatal("old handle cannot resume", err)
	}
	bytes, nodes, count := retained(t, c)
	if bytes != 0 || nodes != 0 || count != 0 {
		t.Fatal("terminal ACK did not refund retained state")
	}
}

func TestCursorNodeQuotaAdmissionAndTerminalRefund(t *testing.T) {
	opts := fixtureOptions()
	opts.MaxCursorNodes = 3
	var calls atomic.Int32
	c := testClient(t, opts, func(w http.ResponseWriter, r *http.Request) {
		requestArgs(t, r)
		switch calls.Add(1) {
		case 1:
			reply(t, w, page([]Value{document("a")}, false, "owned-c1"))
		case 2:
			reply(t, w, page([]Value{document("b")}, true, ""))
		case 3:
			reply(t, w, page(nil, false, "another-cursor"))
		default:
			t.Error("unexpected dispatch")
		}
	})
	first, err := c.QueryFunction(context.Background(), "list", nil, PageOptions{Size: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.QueryFunction(context.Background(), "list", nil, PageOptions{Size: 1}); code(err) != "limit" {
		t.Fatal("aggregate node admission bypass")
	}
	if calls.Load() != 1 {
		t.Fatal("node refusal dispatched")
	}
	if _, err := c.QueryFunction(context.Background(), "list", nil, PageOptions{Size: 1, Cursor: first.NextCursor}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.QueryFunction(context.Background(), "list", nil, PageOptions{Size: 1}); err != nil {
		t.Fatal("terminal credits not reusable", err)
	}
	_, nodes, count := retained(t, c)
	if nodes != 2 || count != 1 {
		t.Fatal("empty continuation accounting")
	}
}

func TestCursorNetACKReplacementDoesNotDoubleCharge(t *testing.T) {
	opts := fixtureOptions()
	opts.MaxCursorBytes = 670
	var calls atomic.Int32
	c := testClient(t, opts, func(w http.ResponseWriter, r *http.Request) {
		requestArgs(t, r)
		switch calls.Add(1) {
		case 1:
			reply(t, w, page([]Value{document("a")}, false, "c"))
		case 2:
			reply(t, w, page(nil, false, "r"))
		case 3:
			reply(t, w, page(nil, false, "replacement-cursor-long"))
		case 4:
			reply(t, w, page(nil, true, ""))
		default:
			t.Error("unexpected dispatch")
		}
	})
	first, err := c.QueryFunction(context.Background(), "list", Object{"filter": String("first")}, PageOptions{Size: 1})
	if err != nil {
		t.Fatal(err)
	}
	other, err := c.QueryFunction(context.Background(), "list", Object{"filter": String("other")}, PageOptions{Size: 1})
	if err != nil || other.NextCursor == "" {
		t.Fatal(err)
	}
	next, err := c.QueryFunction(context.Background(), "list", Object{"filter": String("first")}, PageOptions{Size: 1, Cursor: first.NextCursor})
	if err != nil || next.NextCursor == "" {
		t.Fatal("net ACK replacement double charged old+new", err)
	}
	_, nodes, count := retained(t, c)
	if nodes != 5 || count != 2 {
		t.Fatal("replacement lost another journal")
	}
	if _, err := c.QueryFunction(context.Background(), "list", Object{"filter": String("first")}, PageOptions{Size: 1, Cursor: next.NextCursor}); err != nil {
		t.Fatal(err)
	}
	bytes, nodes, count := retained(t, c)
	if bytes != 293 || nodes != 2 || count != 1 {
		t.Fatal("terminal retirement refunded wrong journal")
	}
}

func TestCursorExpiryAndIdempotentCloseRefundAggregateCredits(t *testing.T) {
	opts := fixtureOptions()
	opts.MaxCursorBytes = 350
	var calls atomic.Int32
	c := testClient(t, opts, func(w http.ResponseWriter, r *http.Request) {
		requestArgs(t, r)
		calls.Add(1)
		reply(t, w, page([]Value{document("a")}, false, "owned-c1"))
	})
	first, err := c.QueryFunction(context.Background(), "list", nil, PageOptions{Size: 1})
	if err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	c.cursors[first.NextCursor].expires = time.Now().Add(-time.Nanosecond)
	c.mu.Unlock()
	second, err := c.QueryFunction(context.Background(), "list", nil, PageOptions{Size: 1})
	if err != nil || second.NextCursor == "" {
		t.Fatal("expired credits not reclaimed", err)
	}
	bytes, nodes, count := retained(t, c)
	if bytes != 333 || nodes != 3 || count != 1 || calls.Load() != 2 {
		t.Fatal("expiry double charged")
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	bytes, nodes, count = retained(t, c)
	if bytes != 0 || nodes != 0 || count != 0 {
		t.Fatal("Close did not refund registry")
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	retained(t, c)
}

func TestConcurrentCursorACKRespectsAggregateByteAndNodeQuotas(t *testing.T) {
	for _, quota := range []string{"bytes", "nodes"} {
		t.Run(quota, func(t *testing.T) {
			opts := fixtureOptions()
			if quota == "bytes" {
				opts.MaxCursorBytes = 650
			} else {
				opts.MaxCursorNodes = 5
			}
			var dispatched atomic.Int32
			both := make(chan struct{})
			release := make(chan struct{})
			defer func() {
				select {
				case <-release:
				default:
					close(release)
				}
			}()
			c := testClient(t, opts, func(w http.ResponseWriter, r *http.Request) {
				requestArgs(t, r)
				if dispatched.Add(1) == 2 {
					close(both)
				}
				select {
				case <-release:
					reply(t, w, page([]Value{document("a")}, false, "s"))
				case <-r.Context().Done():
				}
			})
			type result struct {
				page Page
				err  error
			}
			results := make(chan result, 2)
			for _, label := range []string{"first", "other"} {
				go func(label string) {
					p, e := c.QueryFunction(context.Background(), "list", Object{"filter": String(label)}, PageOptions{Size: 1})
					results <- result{p, e}
				}(label)
			}
			await(t, both)
			close(release)
			success, limited := 0, 0
			for i := 0; i < 2; i++ {
				select {
				case got := <-results:
					if got.err == nil {
						if len(got.page.Documents) != 1 || got.page.NextCursor == "" {
							t.Fatal("success without native ACK")
						}
						success++
					} else if code(got.err) == "limit" {
						if len(got.page.Documents) != 0 || got.page.NextCursor != "" {
							t.Fatal("refusal acknowledged rows")
						}
						limited++
					} else {
						t.Fatal(got.err)
					}
				case <-time.After(2 * time.Second):
					t.Fatal("concurrent ACK did not finish")
				}
			}
			bytes, nodes, count := retained(t, c)
			if success != 1 || limited != 1 || count != 1 || bytes != 326 || nodes != 3 || dispatched.Load() != 2 {
				t.Fatal("concurrent ACK exceeded logical aggregate quota")
			}
		})
	}
}

func TestCancelledFirstPageRefundsReservationBeforeNewJourney(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opts := fixtureOptions()
	opts.MaxCursorBytes = 350
	var calls atomic.Int32
	opts.RoundTripper = roundTripFunc(func(*http.Request) (*http.Response, error) {
		n := calls.Add(1)
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: &callbackBody{Reader: strings.NewReader(`{"status":"success","value":{"page":[{"_id":"a"}],"isDone":false,"continueCursor":"c"}}`), close: func() error {
			if n == 1 {
				cancel()
			}
			return nil
		}}}, nil
	})
	c, err := New("https://owned.invalid", opts)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	refused, err := c.QueryFunction(ctx, "list", nil, PageOptions{Size: 1})
	if !errors.Is(err, context.Canceled) || len(refused.Documents) != 0 || refused.NextCursor != "" {
		t.Fatal("cancelled first page acknowledged data")
	}
	waitIdle(t, c)
	bytes, nodes, count := retained(t, c)
	if bytes != 0 || nodes != 0 || count != 0 {
		t.Fatal("cancelled reservation not refunded")
	}
	next, err := c.QueryFunction(context.Background(), "list", nil, PageOptions{Size: 1})
	if err != nil || len(next.Documents) != 1 || next.NextCursor == "" {
		t.Fatal("refunded credits not reusable", err)
	}
	bytes, nodes, count = retained(t, c)
	if bytes != 326 || nodes != 3 || count != 1 || calls.Load() != 2 {
		t.Fatal("first-page cancellation leaked/duplicated credits")
	}
}
