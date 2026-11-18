package dalgo2convex

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"strconv"
	"time"
)

type PageOptions struct {
	Size   int
	Cursor string
}
type Page struct {
	Documents  []Row
	IsDone     bool
	NextCursor string
}

// A cursor is an expiring, instance-local acknowledgement journal, not a server
// snapshot or a transferable cursor. Its original expiry never extends.
type cursor struct {
	alias         string
	args          [32]byte
	auth          [32]byte
	size          int
	expires       time.Time
	server        string
	seen          map[string]bool
	busy          bool
	inflight      bool
	pages         int
	work          int
	rows          int
	bytes         int
	requestBytes  int
	retainedBytes int
	retainedNodes int
}

func newHandle() (string, error) {
	var b [24]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", failure("cursor")
	}
	return base64.RawURLEncoding.EncodeToString(b[:]), nil
}

func (c *Client) queryAdmission(alias string, args Object, size int) (Query, Object, [32]byte, error) {
	cfg, ok := c.options.Queries[alias]
	if !ok {
		return Query{}, nil, [32]byte{}, failure("mapping")
	}
	if size <= 0 || size > 1000 || size > c.options.MaxRows {
		return Query{}, nil, [32]byte{}, failure("input")
	}
	input, err := copyArgsLimit(args, c.options.MaxRequestBytes)
	if err != nil {
		return Query{}, nil, [32]byte{}, err
	}
	allowed := make(map[string]bool, len(cfg.ArgumentNames))
	for _, k := range cfg.ArgumentNames {
		allowed[k] = true
	}
	for k := range input {
		if !allowed[k] {
			return Query{}, nil, [32]byte{}, failure("input")
		}
	}
	merged, err := copyArgsLimit(cfg.StaticArgs, c.options.MaxRequestBytes)
	if err != nil {
		return Query{}, nil, [32]byte{}, err
	}
	for k, v := range input {
		merged[k] = v
	}
	snapshot, err := copyValue(Value{kind: ObjectKind, object: merged}, 0, &valueBudget{limit: c.options.MaxRequestBytes})
	if err != nil {
		return Query{}, nil, [32]byte{}, err
	}
	encoded, err := EncodeValue(snapshot)
	if err != nil {
		return Query{}, nil, [32]byte{}, err
	}
	if len(encoded) > c.options.MaxRequestBytes {
		return Query{}, nil, [32]byte{}, failure("limit")
	}
	return cfg, merged, hashScope("query", []byte(alias), []byte(cfg.Function), []byte(cfg.Collection), []byte(strconv.Itoa(size)), encoded), nil
}

// QueryFunction invokes only a configured public query. It never interprets SQL,
// chooses an arbitrary route, silently retries, splits, truncates, or returns a
// cursor for a partial/split page. Public query code must enforce authorization.
func (c *Client) QueryFunction(ctx context.Context, alias string, args Object, options PageOptions) (Page, error) {
	var zero Page
	o, err := c.begin(ctx)
	if err != nil {
		return zero, err
	}
	defer o.finish()
	cfg, admitted, fingerprint, err := c.queryAdmission(alias, args, options.Size)
	if err != nil {
		return zero, err
	}
	if len(options.Cursor) > 128 {
		return zero, failure("cursor")
	}
	token, digest, err := o.auth()
	if err != nil {
		return zero, err
	}
	handle := options.Cursor
	first := handle == ""
	if first {
		handle, err = newHandle()
		if err != nil {
			return zero, err
		}
	}
	c.mu.Lock()
	now := time.Now()
	for k, v := range c.cursors {
		if !v.busy && !v.inflight && !now.Before(v.expires) {
			c.dropCursorLocked(k, v)
		}
	}
	state := c.cursors[handle]
	if first {
		if len(c.cursors) >= c.options.MaxCursors {
			c.mu.Unlock()
			return zero, failure("limit")
		}
		state = &cursor{alias: alias, args: fingerprint, auth: digest, size: options.Size, expires: now.Add(c.options.CursorTTL), seen: make(map[string]bool)}
		retainedBytes, retainedNodes := cursorRetention(handle, state, "", nil)
		if retainedBytes > c.options.MaxCursorBytes-c.cursorBytes || retainedNodes > c.options.MaxCursorNodes-c.cursorNodes {
			c.mu.Unlock()
			return zero, failure("limit")
		}
		state.retainedBytes = retainedBytes
		state.retainedNodes = retainedNodes
		c.cursorBytes += retainedBytes
		c.cursorNodes += retainedNodes
		c.cursors[handle] = state
	}
	if state == nil || state.alias != alias || state.args != fingerprint || state.auth != digest || state.size != options.Size || !now.Before(state.expires) {
		c.mu.Unlock()
		return zero, failure("cursor")
	}
	if state.busy || state.inflight {
		c.mu.Unlock()
		return zero, failure("busy")
	}
	state.busy = true
	c.mu.Unlock()
	committed := false
	defer func() {
		c.mu.Lock()
		state.busy = false
		if first && !committed && c.cursors[handle] == state {
			c.dropCursorLocked(handle, state)
		}
		c.mu.Unlock()
	}()
	c.mu.Lock()
	if state.pages >= c.options.MaxPages || state.work > c.options.MaxWork-options.Size || state.bytes >= c.options.MaxResponseBytes || state.rows > c.options.MaxRows-options.Size {
		c.mu.Unlock()
		return zero, failure("limit")
	}
	server := state.server
	remaining := c.options.MaxResponseBytes - state.bytes
	c.mu.Unlock()
	serverCursor := Null()
	if server != "" {
		serverCursor = String(server)
	}
	admitted["paginationOpts"] = Value{kind: ObjectKind, object: Object{"numItems": Float64(float64(options.Size)), "cursor": serverCursor, "maximumRowsRead": Float64(float64(options.Size)), "maximumBytesRead": Float64(float64(remaining))}}
	body, err := o.body(cfg.Function, admitted)
	if err != nil {
		return zero, err
	}
	c.mu.Lock()
	if state.requestBytes > c.options.MaxTotalRequestBytes-len(body) {
		c.mu.Unlock()
		return zero, failure("limit")
	}
	state.pages++
	state.work += options.Size
	state.requestBytes += len(body)
	state.inflight = true
	c.mu.Unlock()
	o.responseLimit = remaining
	o.chargeResponse = func(n int) error {
		c.mu.Lock()
		defer c.mu.Unlock()
		state.bytes += n
		if state.bytes > c.options.MaxResponseBytes {
			return failure("limit")
		}
		return nil
	}
	o.responseSettled = func() { c.mu.Lock(); state.inflight = false; c.mu.Unlock() }
	value, _, err := o.send(body, token)
	if err != nil {
		return zero, err
	}
	if value.Kind() != ObjectKind {
		return zero, failure("response")
	}
	pageValue, ok := value.object["page"]
	if !ok || pageValue.Kind() != ArrayKind {
		return zero, failure("response")
	}
	done, ok := value.object["isDone"].Bool()
	if !ok {
		return zero, failure("response")
	}
	next, ok := value.object["continueCursor"].String()
	if !ok || len(next) > 64*1024 || !validCursorText(next) {
		return zero, failure("response")
	}
	if split, exists := value.object["splitCursor"]; exists && split.Kind() != NullKind {
		return zero, failure("split")
	}
	if status, exists := value.object["pageStatus"]; exists && status.Kind() != NullKind {
		return zero, failure("split")
	}
	if len(pageValue.array) > options.Size || !done && (next == "" || next == server) {
		return zero, failure("response")
	}
	c.mu.Lock()
	state.rows += len(pageValue.array)
	tooMany := state.rows > c.options.MaxRows
	c.mu.Unlock()
	if tooMany {
		return zero, failure("limit")
	}
	collection := c.options.Collections[cfg.Collection]
	rows := make([]Row, len(pageValue.array))
	ids := make(map[string]bool, len(rows))
	for i, item := range pageValue.array {
		if item.Kind() != ObjectKind {
			return zero, failure("response")
		}
		id, ok := item.object[collection.IDField].String()
		if !ok || id == "" || len(id) > 1024 || !validID(id) || ids[id] || state.seen[id] {
			return zero, failure("response")
		}
		ids[id] = true
		rows[i] = Row{ID: id, Document: item.object}
	}
	newCursor := ""
	if !done {
		// An opaque server cursor must fit the next request before we acknowledge
		// this page; never issue an unusable continuation after delivering rows.
		admitted["paginationOpts"] = Value{kind: ObjectKind, object: Object{"numItems": Float64(float64(options.Size)), "cursor": String(next), "maximumRowsRead": Float64(float64(options.Size)), "maximumBytesRead": Float64(float64(remaining))}}
		if _, err := c.buildBody(cfg.Function, admitted); err != nil {
			return zero, err
		}
		newCursor, err = newHandle()
		if err != nil {
			return zero, err
		}
	}
	// Final acknowledgement and journal mutation are in one critical section,
	// after physical body cleanup. There is no post-commit cancellation guard.
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := o.check(); err != nil {
		return zero, err
	}
	if !time.Now().Before(state.expires) || c.cursors[handle] != state {
		return zero, failure("cursor")
	}
	if !done {
		retainedBytes, retainedNodes := cursorRetention(newCursor, state, next, ids)
		if retainedBytes > c.options.MaxCursorBytes-(c.cursorBytes-state.retainedBytes) || retainedNodes > c.options.MaxCursorNodes-(c.cursorNodes-state.retainedNodes) {
			if err := o.check(); err != nil {
				return zero, err
			}
			return zero, failure("limit")
		}
		nextSeen := make(map[string]bool, len(state.seen)+len(ids))
		for id := range state.seen {
			nextSeen[id] = true
		}
		for id := range ids {
			nextSeen[id] = true
		}
		if err := o.check(); err != nil {
			return zero, err
		}
		if !time.Now().Before(state.expires) {
			return zero, failure("cursor")
		}
		// Net replacement under this same lock: do not charge old+successor twice,
		// and do not change position/IDs or refund anything on quota refusal.
		c.dropCursorLocked(handle, state)
		state.seen = nextSeen
		state.server = next
		state.retainedBytes = retainedBytes
		state.retainedNodes = retainedNodes
		c.cursorBytes += retainedBytes
		c.cursorNodes += retainedNodes
		c.cursors[newCursor] = state
	} else {
		c.dropCursorLocked(handle, state)
	}
	committed = true
	return Page{Documents: rows, IsDone: done, NextCursor: newCursor}, nil
}

func validCursorText(s string) bool {
	if len(s) > 64*1024 {
		return false
	}
	for _, r := range s {
		if r < 32 || r == 127 {
			return false
		}
	}
	return true
}

// Logical retained state, not an RSS estimate: journal+map/header/bookkeeping
// 256 bytes, handle/alias/server UTF8 bytes, 32 bytes plus ID bytes per seen
// entry, and two container nodes plus one node per ID. Fingerprints are fixed
// arrays included in the base; raw argument JSON and tokens are never retained.
// Caller holds c.mu. added IDs were already checked against seen and one another.
func cursorRetention(handle string, state *cursor, server string, added map[string]bool) (int, int) {
	bytes := 256 + len(handle) + len(state.alias) + len(server)
	nodes := 2
	for id := range state.seen {
		bytes += 32 + len(id)
		nodes++
	}
	for id := range added {
		bytes += 32 + len(id)
		nodes++
	}
	return bytes, nodes
}
func (c *Client) dropCursorLocked(handle string, state *cursor) {
	if c.cursors[handle] != state {
		return
	}
	delete(c.cursors, handle)
	c.cursorBytes -= state.retainedBytes
	c.cursorNodes -= state.retainedNodes
	state.retainedBytes = 0
	state.retainedNodes = 0
}
