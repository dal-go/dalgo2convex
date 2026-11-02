package dalgo2convex

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// Error contains a redacted classification, never a token, URL, body, remote log,
// caller cancellation cause, or underlying transport error.
type Error struct{ Code string }

func (e *Error) Error() string  { return "convex read: " + e.Code }
func failure(code string) error { return &Error{Code: code} }

type TokenProvider func(context.Context) (string, error)

type Collection struct {
	GetFunction string
	IDArgument  string
	IDField     string
	StaticArgs  Object
	Fields      []string
}
type Query struct {
	Function      string
	Collection    string
	ArgumentNames []string
	StaticArgs    Object
}

type Options struct {
	Token                TokenProvider
	Anonymous            bool
	Collections          map[string]Collection
	Queries              map[string]Query
	Timeout              time.Duration
	MaxConcurrent        int
	MaxRows              int
	MaxKeys              int
	MaxPages             int
	MaxResponseBytes     int
	MaxRequestBytes      int
	MaxTotalRequestBytes int
	MaxWork              int
	MaxCursorBytes       int
	MaxCursorNodes       int
	MaxCursors           int
	CursorTTL            time.Duration
	RootCAs              *x509.CertPool
	// RoundTripper is borrowed trusted infrastructure, primarily for owned
	// fixtures. It must provide verified TLS/no ambient proxy if those policies
	// are required; the adapter cannot attest a custom transport's internals.
	// Close never closes or destroys a borrowed transport.
	RoundTripper      http.RoundTripper
	AllowLoopbackHTTP bool
}

type Client struct {
	endpoint    string
	options     Options
	transport   http.RoundTripper
	owned       *http.Transport
	mu          sync.Mutex
	closed      bool
	active      map[*operation]struct{}
	cursors     map[string]*cursor
	cursorBytes int
	cursorNodes int
}

func controlName(s string) bool {
	if s == "" || s == "constructor" || s == "prototype" || s == "__proto__" || len(s) > 128 {
		return false
	}
	for _, c := range s {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_') {
			return false
		}
	}
	return true
}
func functionName(s string) bool {
	if len(s) > 512 || strings.Count(s, ":") != 1 {
		return false
	}
	parts := strings.Split(s, ":")
	if !controlName(parts[1]) {
		return false
	}
	for _, segment := range strings.Split(parts[0], "/") {
		if !controlName(segment) {
			return false
		}
	}
	return true
}
func defaultInt(v *int, def, max int) bool {
	if *v == 0 {
		*v = def
	}
	return *v > 0 && *v <= max
}

// New validates and snapshots trusted configuration without token/fetch I/O.
func New(endpoint string, options Options) (*Client, error) {
	u, err := url.Parse(endpoint)
	if err != nil || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || (u.Path != "" && u.Path != "/") || u.RawPath != "" || u.Host == "" {
		return nil, failure("config")
	}
	loopback := u.Hostname() == "127.0.0.1" || u.Hostname() == "::1"
	if u.Scheme != "https" && !(options.AllowLoopbackHTTP && loopback && u.Scheme == "http") {
		return nil, failure("config")
	}
	if options.Token == nil && !options.Anonymous || options.Token != nil && options.Anonymous {
		return nil, failure("config")
	}
	if options.Timeout == 0 {
		options.Timeout = 30 * time.Second
	}
	if options.Timeout <= 0 || options.Timeout > 120*time.Second {
		return nil, failure("config")
	}
	if options.CursorTTL == 0 {
		options.CursorTTL = 5 * time.Minute
	}
	if options.CursorTTL <= 0 || options.CursorTTL > 10*time.Minute {
		return nil, failure("config")
	}
	if !defaultInt(&options.MaxConcurrent, 4, 16) || !defaultInt(&options.MaxRows, 1000, 10000) || !defaultInt(&options.MaxKeys, 100, 1000) || !defaultInt(&options.MaxPages, 100, 1000) || !defaultInt(&options.MaxResponseBytes, 4*1024*1024, 16*1024*1024) || !defaultInt(&options.MaxRequestBytes, 64*1024, 1024*1024) || !defaultInt(&options.MaxTotalRequestBytes, 1024*1024, 16*1024*1024) || !defaultInt(&options.MaxWork, 10000, 100000) || !defaultInt(&options.MaxCursors, 128, 1024) || !defaultInt(&options.MaxCursorBytes, 16*1024*1024, 16*1024*1024) || !defaultInt(&options.MaxCursorNodes, 65536, 65536) {
		return nil, failure("config")
	}
	if len(options.Collections) > 128 || len(options.Queries) > 128 {
		return nil, failure("config")
	}
	collections := make(map[string]Collection, len(options.Collections))
	for alias, cfg := range options.Collections {
		if len(cfg.Fields) > 1024 || !controlName(alias) || !functionName(cfg.GetFunction) {
			return nil, failure("config")
		}
		if cfg.IDArgument == "" {
			cfg.IDArgument = "id"
		}
		if cfg.IDField == "" {
			cfg.IDField = "_id"
		}
		if !controlName(cfg.IDArgument) || !wireField(cfg.IDField) || cfg.IDField == "" {
			return nil, failure("config")
		}
		args, err := copyArgsLimit(cfg.StaticArgs, options.MaxRequestBytes)
		if err != nil {
			return nil, err
		}
		if _, exists := args[cfg.IDArgument]; exists {
			return nil, failure("config")
		}
		cfg.StaticArgs = args
		cfg.Fields = append([]string(nil), cfg.Fields...)
		for _, f := range cfg.Fields {
			if !wireField(f) {
				return nil, failure("config")
			}
		}
		collections[alias] = cfg
	}
	queries := make(map[string]Query, len(options.Queries))
	for alias, cfg := range options.Queries {
		if len(cfg.ArgumentNames) > 128 || !controlName(alias) || !functionName(cfg.Function) {
			return nil, failure("config")
		}
		if _, ok := collections[cfg.Collection]; !ok {
			return nil, failure("config")
		}
		args, err := copyArgsLimit(cfg.StaticArgs, options.MaxRequestBytes)
		if err != nil {
			return nil, err
		}
		if _, exists := args["paginationOpts"]; exists {
			return nil, failure("config")
		}
		cfg.StaticArgs = args
		names := make(map[string]bool)
		cfg.ArgumentNames = append([]string(nil), cfg.ArgumentNames...)
		for _, name := range cfg.ArgumentNames {
			if !controlName(name) || name == "paginationOpts" || names[name] {
				return nil, failure("config")
			}
			if _, fixed := args[name]; fixed {
				return nil, failure("config")
			}
			names[name] = true
		}
		queries[alias] = cfg
	}
	options.Collections = collections
	options.Queries = queries
	transport := options.RoundTripper
	var owned *http.Transport
	if transport == nil {
		tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
		if options.RootCAs != nil {
			tlsConfig.RootCAs = options.RootCAs.Clone()
		}
		owned = &http.Transport{Proxy: nil, DialContext: (&net.Dialer{Timeout: options.Timeout, KeepAlive: 30 * time.Second}).DialContext, TLSClientConfig: tlsConfig, TLSHandshakeTimeout: options.Timeout, ResponseHeaderTimeout: options.Timeout, MaxResponseHeaderBytes: 64 * 1024, DisableCompression: true, MaxIdleConns: options.MaxConcurrent, MaxIdleConnsPerHost: options.MaxConcurrent, MaxConnsPerHost: options.MaxConcurrent, IdleConnTimeout: 30 * time.Second}
		transport = owned
	}
	return &Client{endpoint: strings.TrimSuffix(endpoint, "/") + "/api/query", options: options, transport: transport, owned: owned, active: make(map[*operation]struct{}), cursors: make(map[string]*cursor)}, nil
}

func copyArgs(args Object) (Object, error) { return copyArgsLimit(args, 16*1024*1024) }
func copyArgsLimit(args Object, limit int) (Object, error) {
	v, err := copyValue(Value{kind: ObjectKind, object: args}, 0, &valueBudget{limit: limit})
	if err != nil {
		return nil, err
	}
	for name := range v.object {
		if !controlName(name) {
			return nil, failure("input")
		}
	}
	return v.object, nil
}

type operation struct {
	client          *Client
	parent          context.Context
	ctx             context.Context
	cancel          context.CancelFunc
	deadline        time.Time
	physical        int
	finished        bool
	responseBytes   int
	requestBytes    int
	rows            int
	responseLimit   int
	chargeResponse  func(int) error
	responseSettled func()
}

func (c *Client) begin(parent context.Context) (*operation, error) {
	if parent == nil {
		return nil, failure("input")
	}
	if err := parent.Err(); err != nil {
		if err == context.DeadlineExceeded {
			return nil, context.DeadlineExceeded
		}
		return nil, context.Canceled
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, failure("closed")
	}
	if len(c.active) >= c.options.MaxConcurrent {
		return nil, failure("busy")
	}
	ctx, cancel := context.WithTimeout(parent, c.options.Timeout)
	deadline, _ := ctx.Deadline()
	o := &operation{client: c, parent: parent, ctx: ctx, cancel: cancel, deadline: deadline}
	c.active[o] = struct{}{}
	return o, nil
}
func (o *operation) check() error {
	if err := o.parent.Err(); err != nil {
		if err == context.DeadlineExceeded {
			return context.DeadlineExceeded
		}
		return context.Canceled
	}
	if !time.Now().Before(o.deadline) {
		return context.DeadlineExceeded
	}
	if o.ctx.Err() != nil {
		return context.Canceled
	}
	return nil
}
func (o *operation) releaseLocked() {
	if o.finished && o.physical == 0 {
		delete(o.client.active, o)
	}
}
func (o *operation) finish() {
	o.client.mu.Lock()
	o.finished = true
	o.releaseLocked()
	o.client.mu.Unlock()
	o.cancel()
}
func (o *operation) track() { o.client.mu.Lock(); o.physical++; o.client.mu.Unlock() }
func (o *operation) settled() {
	o.client.mu.Lock()
	o.physical--
	o.releaseLocked()
	o.client.mu.Unlock()
}

type foreignResult[T any] struct {
	value T
	err   error
}

func foreign[T any](o *operation, fn func() (T, error)) (T, error) {
	var zero T
	o.track()
	ch := make(chan foreignResult[T], 1)
	go func() { v, err := fn(); o.settled(); ch <- foreignResult[T]{v, err} }()
	select {
	case <-o.ctx.Done():
		return zero, o.check()
	case r := <-ch:
		if err := o.check(); err != nil {
			return zero, err
		}
		return r.value, r.err
	}
}

func (c *Client) Close() error {
	c.mu.Lock()
	if !c.closed {
		c.closed = true
		for o := range c.active {
			o.cancel()
		}
		for handle, state := range c.cursors {
			c.dropCursorLocked(handle, state)
		}
	}
	c.mu.Unlock()
	if c.owned != nil {
		c.owned.CloseIdleConnections()
	}
	return nil
}

func (o *operation) auth() (string, [32]byte, error) {
	if err := o.check(); err != nil {
		return "", [32]byte{}, err
	}
	token := ""
	if o.client.options.Token != nil {
		v, err := foreign(o, func() (string, error) {
			if err := o.check(); err != nil {
				return "", err
			}
			return o.client.options.Token(o.ctx)
		})
		if err != nil {
			if ctxErr := o.check(); ctxErr != nil {
				return "", [32]byte{}, ctxErr
			}
			return "", [32]byte{}, failure("auth")
		}
		token = v
		if len(token) == 0 || len(token) > 8192 {
			return "", [32]byte{}, failure("auth")
		}
		for _, c := range token {
			if c < 33 || c > 126 {
				return "", [32]byte{}, failure("auth")
			}
		}
	}
	digest := hashScope("auth", []byte(token))
	if err := o.check(); err != nil {
		return "", [32]byte{}, err
	}
	return token, digest, nil
}

func (c *Client) buildBody(path string, args Object) ([]byte, error) {
	pathBytes, err := jsonStringSize(path)
	if err != nil {
		return nil, err
	}
	overhead := len(`{"args":[`) + len(`],"format":"convex_encoded_json","path":`) + pathBytes + 1
	available := c.options.MaxRequestBytes - overhead
	if available <= 0 {
		return nil, failure("limit")
	}
	snapshot, err := copyValue(Value{kind: ObjectKind, object: args}, 0, &valueBudget{limit: available})
	if err != nil {
		return nil, err
	}
	v, err := encodeNative(snapshot)
	if err != nil {
		return nil, err
	}
	data, err := json.Marshal(map[string]any{"path": path, "format": "convex_encoded_json", "args": []any{v}})
	if err != nil {
		return nil, failure("input")
	}
	if len(data) > c.options.MaxRequestBytes {
		return nil, failure("limit")
	}
	return data, nil
}
func (o *operation) body(path string, args Object) ([]byte, error) {
	data, err := o.client.buildBody(path, args)
	if err != nil {
		return nil, err
	}
	if o.requestBytes > o.client.options.MaxTotalRequestBytes-len(data) {
		return nil, failure("limit")
	}
	o.requestBytes += len(data)
	if err := o.check(); err != nil {
		return nil, err
	}
	return data, nil
}

func (o *operation) send(data []byte, token string) (Value, int, error) {
	result, err := foreign(o, func() (struct {
		v Value
		n int
	}, error) {
		var result struct {
			v Value
			n int
		}
		if o.responseSettled != nil {
			defer o.responseSettled()
		}
		if err := o.check(); err != nil {
			return result, err
		}
		req, err := http.NewRequestWithContext(o.ctx, http.MethodPost, o.client.endpoint, bytes.NewReader(data))
		if err != nil {
			return result, failure("transport")
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept-Encoding", "identity")
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		response, err := o.client.transport.RoundTrip(req)
		if err != nil {
			if response != nil && response.Body != nil {
				_ = response.Body.Close()
			}
			return result, failure("transport")
		}
		if response == nil || response.Body == nil {
			return result, failure("response")
		}
		// This watcher owns only this exact response body. Both read and close
		// physical work remain admitted after a prompt public cancellation.
		var once sync.Once
		var closeErr error
		closeBody := func() { once.Do(func() { closeErr = response.Body.Close() }) }
		done := make(chan struct{})
		watchDone := make(chan struct{})
		o.track()
		go func() {
			defer close(watchDone)
			defer o.settled()
			select {
			case <-o.ctx.Done():
				closeBody()
			case <-done:
			}
		}()
		defer func() { close(done); <-watchDone }()
		defer closeBody()
		if response.StatusCode != http.StatusOK {
			return result, failure("remote")
		}
		if response.Header.Get("Content-Encoding") != "" && response.Header.Get("Content-Encoding") != "identity" {
			return result, failure("response")
		}
		remaining := o.client.options.MaxResponseBytes - o.responseBytes
		if o.responseLimit > 0 && o.responseLimit < remaining {
			remaining = o.responseLimit
		}
		if remaining <= 0 {
			return result, failure("limit")
		}
		raw, err := io.ReadAll(io.LimitReader(response.Body, int64(remaining)+1))
		result.n = len(raw)
		if o.chargeResponse != nil {
			if charged := o.chargeResponse(result.n); charged != nil {
				return result, charged
			}
		}
		if err != nil {
			return result, failure("transport")
		}
		if len(raw) > remaining {
			return result, failure("limit")
		}
		closeBody()
		if closeErr != nil {
			return result, failure("transport")
		}
		if err := o.check(); err != nil {
			return result, err
		}
		envelope, err := strictJSON(raw)
		if err != nil {
			return result, err
		}
		obj, ok := envelope.(map[string]any)
		if !ok || obj["status"] != "success" {
			return result, failure("remote")
		}
		value, present := obj["value"]
		if !present {
			return result, failure("response")
		}
		result.v, err = decodeNative(value, 0)
		if err == nil {
			result.v, err = copyValue(result.v, 0, new(valueBudget))
		}
		return result, err
	})
	if err != nil {
		if ctxErr := o.check(); ctxErr != nil {
			return Value{}, 0, ctxErr
		}
		return Value{}, result.n, err
	}
	o.responseBytes += result.n
	if err := o.check(); err != nil {
		return Value{}, result.n, err
	}
	return result.v, result.n, nil
}

type Row struct {
	ID       string
	Document Object
}

func (c *Client) collection(alias string) (Collection, error) {
	cfg, ok := c.options.Collections[alias]
	if !ok {
		return Collection{}, failure("mapping")
	}
	return cfg, nil
}
func (o *operation) pointBody(cfg Collection, id string) ([]byte, error) {
	if id == "" || len(id) > 1024 || !validID(id) {
		return nil, failure("key")
	}
	args, err := copyArgsLimit(cfg.StaticArgs, o.client.options.MaxRequestBytes)
	if err != nil {
		return nil, err
	}
	args[cfg.IDArgument] = String(id)
	return o.body(cfg.GetFunction, args)
}
func (o *operation) readPoint(cfg Collection, id string, body []byte, token string) (*Row, error) {
	value, _, err := o.send(body, token)
	if err != nil {
		return nil, err
	}
	if value.Kind() == NullKind {
		return nil, nil
	}
	if value.Kind() != ObjectKind {
		return nil, failure("response")
	}
	nativeID, ok := value.object[cfg.IDField].String()
	if !ok || nativeID != id {
		return nil, failure("response")
	}
	o.rows++
	if o.rows > o.client.options.MaxRows {
		return nil, failure("limit")
	}
	return &Row{ID: id, Document: value.object}, nil
}
func validID(s string) bool {
	for _, r := range s {
		if r < 33 || r > 126 {
			return false
		}
	}
	return true
}

func (c *Client) Get(ctx context.Context, collection, id string) (*Row, error) {
	o, err := c.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer o.finish()
	cfg, err := c.collection(collection)
	if err != nil {
		return nil, err
	}
	if id == "" || len(id) > 1024 || !validID(id) {
		return nil, failure("key")
	}
	body, err := o.pointBody(cfg, id)
	if err != nil {
		return nil, err
	}
	token, _, err := o.auth()
	if err != nil {
		return nil, err
	}
	row, err := o.readPoint(cfg, id, body, token)
	if final := o.check(); final != nil {
		return nil, final
	}
	return row, err
}
func (c *Client) Exists(ctx context.Context, collection, id string) (bool, error) {
	row, err := c.Get(ctx, collection, id)
	return row != nil, err
}
func (c *Client) GetMany(ctx context.Context, collection string, ids []string) ([]*Row, error) {
	o, err := c.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer o.finish()
	cfg, err := c.collection(collection)
	if err != nil {
		return nil, err
	}
	if len(ids) > c.options.MaxKeys {
		return nil, failure("limit")
	}
	ids = append([]string(nil), ids...)
	for _, id := range ids {
		if id == "" || len(id) > 1024 || !validID(id) {
			return nil, failure("key")
		}
	}
	bodies := make([][]byte, len(ids))
	for i, id := range ids {
		bodies[i], err = o.pointBody(cfg, id)
		if err != nil {
			return nil, err
		}
	}
	token, _, err := o.auth()
	if err != nil {
		return nil, err
	}
	rows := make([]*Row, len(ids))
	for i, id := range ids {
		rows[i], err = o.readPoint(cfg, id, bodies[i], token)
		if err != nil {
			return nil, err
		}
	}
	if err := o.check(); err != nil {
		return nil, err
	}
	return rows, nil
}

type Metadata struct {
	Collections           map[string][]string
	Provenance            string
	DynamicDiscovery      bool
	AuthorizationVerified bool
}

func (c *Client) Metadata() Metadata {
	m := Metadata{Collections: make(map[string][]string), Provenance: "trusted configuration"}
	for alias, cfg := range c.options.Collections {
		m.Collections[alias] = append([]string(nil), cfg.Fields...)
	}
	return m
}

// TestConnection reads an explicitly configured key; it does not discover schema
// or assert any permissions beyond that public query invocation.
func (c *Client) TestConnection(ctx context.Context, collection, id string) error {
	_, err := c.Get(ctx, collection, id)
	return err
}

func hashScope(domain string, parts ...[]byte) [32]byte {
	h := sha256.New()
	_, _ = h.Write([]byte("dalgo-convex/v1/" + domain + "\x00"))
	var length [8]byte
	for _, part := range parts {
		binary.BigEndian.PutUint64(length[:], uint64(len(part)))
		_, _ = h.Write(length[:])
		_, _ = h.Write(part)
	}
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}
