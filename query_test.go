package dalgo2convex

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestOwnedTLSOpaquePagingEmptyProgressAndReplay(t *testing.T) {
	var count atomic.Int32
	c := testClient(t, fixtureOptions(), func(w http.ResponseWriter, r *http.Request) {
		args := requestArgs(t, r)
		paging, _ := args["paginationOpts"].Object()
		if n, ok := paging["numItems"].Float64(); !ok || n != 1 {
			t.Error("page size not mapped")
		}
		if n, _ := paging["maximumRowsRead"].Float64(); n != 1 {
			t.Error("remote work cap not forwarded")
		}
		switch count.Add(1) {
		case 1:
			if paging["cursor"].Kind() != NullKind {
				t.Error("first cursor is not null")
			}
			reply(t, w, page(nil, false, "owned-c1"))
		case 2:
			if v, _ := paging["cursor"].String(); v != "owned-c1" {
				t.Error("wrong continuation")
			}
			reply(t, w, page([]Value{document("a")}, false, "owned-c2"))
		case 3:
			reply(t, w, page(nil, true, ""))
		default:
			t.Error("unexpected paging request")
		}
	})
	first, err := c.QueryFunction(context.Background(), "list", nil, PageOptions{Size: 1})
	if err != nil || first.IsDone || len(first.Documents) != 0 || first.NextCursor == "" || strings.Contains(first.NextCursor, "owned-c1") {
		t.Fatal("empty page or cursor exposure", err)
	}
	second, err := c.QueryFunction(context.Background(), "list", nil, PageOptions{Size: 1, Cursor: first.NextCursor})
	if err != nil || second.IsDone || len(second.Documents) != 1 || second.Documents[0].ID != "a" {
		t.Fatal("second page", err)
	}
	if _, err := c.QueryFunction(context.Background(), "list", nil, PageOptions{Size: 1, Cursor: first.NextCursor}); code(err) != "cursor" {
		t.Fatal("acknowledged handle replayed")
	}
	last, err := c.QueryFunction(context.Background(), "list", nil, PageOptions{Size: 1, Cursor: second.NextCursor})
	if err != nil || !last.IsDone || last.NextCursor != "" {
		t.Fatal("terminal page", err)
	}
	if count.Load() != 3 {
		t.Fatal("implicit retry")
	}
}

func TestMalformedPageDoesNotAdvanceAcknowledgedCursor(t *testing.T) {
	for _, kind := range []string{"repeat", "over-return", "split", "duplicate-id", "wrong-id-type"} {
		t.Run(kind, func(t *testing.T) {
			var count atomic.Int32
			c := testClient(t, fixtureOptions(), func(w http.ResponseWriter, r *http.Request) {
				args := requestArgs(t, r)
				paging, _ := args["paginationOpts"].Object()
				switch count.Add(1) {
				case 1:
					reply(t, w, page([]Value{document("a")}, false, "owned-c1"))
				case 2:
					bad := page([]Value{document("b")}, false, "owned-c2")
					switch kind {
					case "repeat":
						bad.object["continueCursor"] = String("owned-c1")
					case "over-return":
						bad.object["page"] = Value{kind: ArrayKind, array: []Value{document("b"), document("c")}}
					case "split":
						bad.object["pageStatus"] = String("SplitRequired")
					case "duplicate-id":
						bad.object["page"] = Value{kind: ArrayKind, array: []Value{document("a")}}
					case "wrong-id-type":
						bad.object["page"] = Value{kind: ArrayKind, array: []Value{{kind: ObjectKind, object: Object{"_id": Int64(1)}}}}
					}
					reply(t, w, bad)
				case 3:
					if v, _ := paging["cursor"].String(); v != "owned-c1" {
						t.Error("failed page advanced server position")
					}
					reply(t, w, page([]Value{document("b")}, true, ""))
				default:
					t.Error("unexpected retry")
				}
			})
			first, err := c.QueryFunction(context.Background(), "list", nil, PageOptions{Size: 1})
			if err != nil {
				t.Fatal(err)
			}
			bad, err := c.QueryFunction(context.Background(), "list", nil, PageOptions{Size: 1, Cursor: first.NextCursor})
			if err == nil || len(bad.Documents) != 0 || bad.NextCursor != "" {
				t.Fatal("malformed page delivered partial state")
			}
			retry, err := c.QueryFunction(context.Background(), "list", nil, PageOptions{Size: 1, Cursor: first.NextCursor})
			if err != nil || len(retry.Documents) != 1 || retry.Documents[0].ID != "b" || !retry.IsDone {
				t.Fatal("prior cursor not retryable", err)
			}
			if count.Load() != 3 {
				t.Fatal("hidden dispatch")
			}
		})
	}
}

func TestCursorAuthInstanceArgumentAndJourneyBudgets(t *testing.T) {
	var token atomic.Int32
	opts := fixtureOptions()
	opts.Token = func(context.Context) (string, error) {
		if token.Load() == 0 {
			return "owned-read-token", nil
		}
		return "rotated-user-token", nil
	}
	opts.MaxPages = 2
	var calls atomic.Int32
	c := testClient(t, opts, func(w http.ResponseWriter, r *http.Request) {
		requestArgs(t, r)
		n := calls.Add(1)
		reply(t, w, page(nil, false, "server-"+string(rune('0'+n))))
	})
	first, err := c.QueryFunction(context.Background(), "list", Object{"filter": String("a")}, PageOptions{Size: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.QueryFunction(context.Background(), "list", Object{"filter": String("b")}, PageOptions{Size: 1, Cursor: first.NextCursor}); code(err) != "cursor" {
		t.Fatal("cursor query replay")
	}
	token.Store(1)
	if _, err := c.QueryFunction(context.Background(), "list", Object{"filter": String("a")}, PageOptions{Size: 1, Cursor: first.NextCursor}); code(err) != "cursor" {
		t.Fatal("cursor auth replay")
	}
	token.Store(0)
	second, err := c.QueryFunction(context.Background(), "list", Object{"filter": String("a")}, PageOptions{Size: 1, Cursor: first.NextCursor})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.QueryFunction(context.Background(), "list", Object{"filter": String("a")}, PageOptions{Size: 1, Cursor: second.NextCursor}); code(err) != "limit" {
		t.Fatal("whole journey pages reset")
	}
	if calls.Load() != 2 {
		t.Fatal("budget refusal dispatched")
	}
	other, err := New("https://other.invalid", fixtureOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if _, err := other.QueryFunction(context.Background(), "list", Object{"filter": String("a")}, PageOptions{Size: 1, Cursor: second.NextCursor}); code(err) != "cursor" {
		t.Fatal("foreign instance cursor accepted")
	}
}

func TestFinalCloseAbortKeepsPriorCursorAndAttemptAccounting(t *testing.T) {
	var calls atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opts := fixtureOptions()
	opts.MaxPages = 3
	opts.MaxCursorBytes = 350
	opts.RoundTripper = roundTripFunc(func(*http.Request) (*http.Response, error) {
		n := calls.Add(1)
		raw := `{"status":"success","value":{"page":[],"isDone":false,"continueCursor":"server-c1"}}`
		if n > 1 {
			raw = `{"status":"success","value":{"page":[{"_id":"a"}],"isDone":true,"continueCursor":""}}`
		}
		closeFn := func() error {
			if n == 2 {
				cancel()
			}
			return nil
		}
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: &callbackBody{Reader: strings.NewReader(raw), close: closeFn}}, nil
	})
	c, err := New("https://owned.invalid", opts)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	first, err := c.QueryFunction(context.Background(), "list", nil, PageOptions{Size: 1})
	if err != nil {
		t.Fatal(err)
	}
	beforeBytes, beforeNodes, _ := retained(t, c)
	bad, err := c.QueryFunction(ctx, "list", nil, PageOptions{Size: 1, Cursor: first.NextCursor})
	if !errors.Is(err, context.Canceled) || len(bad.Documents) != 0 || bad.NextCursor != "" {
		t.Fatal("late abort acknowledged a page")
	}
	waitIdle(t, c)
	afterBytes, afterNodes, count := retained(t, c)
	if beforeBytes != afterBytes || beforeNodes != afterNodes || count != 1 {
		t.Fatal("cancelled successor refunded/lost prior retained credits")
	}
	retry, err := c.QueryFunction(context.Background(), "list", nil, PageOptions{Size: 1, Cursor: first.NextCursor})
	if err != nil || len(retry.Documents) != 1 || retry.Documents[0].ID != "a" {
		t.Fatal("late abort lost prior handle", err)
	}
	if calls.Load() != 3 {
		t.Fatal("request count changed")
	}
	afterBytes, afterNodes, count = retained(t, c)
	if afterBytes != 0 || afterNodes != 0 || count != 0 {
		t.Fatal("terminal retry leaked cursor credits")
	}
}

func TestJourneyExpiryDoesNotReplaceOriginalPerCallTimeout(t *testing.T) {
	opts := fixtureOptions()
	opts.CursorTTL = time.Minute
	opts.Timeout = 100 * time.Millisecond
	opts.RoundTripper = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		deadline, ok := r.Context().Deadline()
		if !ok || time.Until(deadline) > 150*time.Millisecond {
			t.Error("journey TTL replaced per-call deadline")
		}
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"status":"success","value":{"page":[],"isDone":false,"continueCursor":"owned-c1"}}`))}, nil
	})
	c, err := New("https://owned.invalid", opts)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	first, err := c.QueryFunction(context.Background(), "list", nil, PageOptions{Size: 1})
	if err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	state := c.cursors[first.NextCursor]
	expiry := state.expires
	state.expires = time.Now().Add(-time.Nanosecond)
	c.mu.Unlock()
	if _, err := c.QueryFunction(context.Background(), "list", nil, PageOptions{Size: 1, Cursor: first.NextCursor}); code(err) != "cursor" {
		t.Fatal("expired cursor accepted")
	}
	if expiry.Sub(time.Now()) > time.Minute {
		t.Fatal("expiry inflated")
	}
}

func TestOversizedEscapedServerCursorRefusesBeforeAcknowledgingRows(t *testing.T) {
	opts := fixtureOptions()
	opts.MaxRequestBytes = 512
	var calls atomic.Int32
	c := testClient(t, opts, func(w http.ResponseWriter, r *http.Request) {
		requestArgs(t, r)
		if calls.Add(1) == 1 {
			reply(t, w, page([]Value{document("a")}, false, strings.Repeat("\\", 400)))
		} else {
			reply(t, w, page([]Value{document("a")}, true, ""))
		}
	})
	bad, err := c.QueryFunction(context.Background(), "list", nil, PageOptions{Size: 1})
	if code(err) != "limit" || len(bad.Documents) != 0 || bad.NextCursor != "" {
		t.Fatal("unusable server cursor acknowledged rows")
	}
	retry, err := c.QueryFunction(context.Background(), "list", nil, PageOptions{Size: 1})
	if err != nil || len(retry.Documents) != 1 || retry.Documents[0].ID != "a" || !retry.IsDone {
		t.Fatal("first page refusal lost acknowledgement", err)
	}
	if calls.Load() != 2 {
		t.Fatal("hidden cursor retry")
	}
}
