# dalgo2convex

Native Go reads from existing Convex documents through explicitly configured
**public query functions**. No DALgo-owned document envelope, table recreation,
deployment admin key, mutation route, or hosted account is required by the SDK.

This is a source implementation. A real Convex backend, deployed query/auth
configuration, hosted permissions, installed CLI and release are separate gates.
The owned HTTPS tests are protocol tests; they do not prove a deployed backend.

## Configure existing public queries

```go
client, err := dalgo2convex.New("https://YOUR-DEPLOYMENT.convex.cloud", dalgo2convex.Options{
    Token: func(ctx context.Context) (string, error) {
        return userToken(ctx) // user JWT from your reviewed identity provider
    },
    Collections: map[string]dalgo2convex.Collection{
        "tasks": {
            GetFunction: "dalgo:getTask",
            StaticArgs: dalgo2convex.Object{"tenant": dalgo2convex.String("your-subject")},
            Fields: []string{"_id", "title", "amount", "bytes"},
        },
    },
    Queries: map[string]dalgo2convex.Query{
        "listTasks": {
            Function: "dalgo:listTasks", Collection: "tasks",
            StaticArgs: dalgo2convex.Object{"tenant": dalgo2convex.String("your-subject")},
        },
    },
})
if err != nil { return err }
defer client.Close()

row, err := client.Get(ctx, "tasks", existingID)
// A missing document is (nil, nil). IDs are opaque native strings.
rows, err := client.GetMany(ctx, "tasks", []string{idB, missingID, idA, idA})
// Input order, duplicates, missing positions and independent native values remain.
page, err := client.QueryFunction(ctx, "listTasks", nil, dalgo2convex.PageOptions{Size: 100})
if err == nil && !page.IsDone {
    next, err := client.QueryFunction(ctx, "listTasks", nil,
        dalgo2convex.PageOptions{Size: 100, Cursor: page.NextCursor})
    _, _ = next, err
}
_, _ = row, rows
```

`Get`, `Exists`, `GetMany`, `QueryFunction`, `TestConnection` and DALgo reads take
contexts. Constructors and `Metadata` perform no I/O. `TestConnection` invokes
only the supplied configured collection/key; it is not schema/permission discovery.
GetMany is sequential with one original deadline and aggregate byte/request/row
bounds. A batch error returns no partial result. There is no hidden bulk endpoint.

Collection aliases map a public `GetFunction`, an `IDArgument` (default `id`), an
`IDField` (default `_id`), immutable `StaticArgs` and optional configured `Fields`.
Get functions return a native object matching the requested ID, or null. Query
aliases specify the public function, collection, static args and an explicit list
of dynamic `ArgumentNames`. Runtime input cannot select arbitrary function paths,
override fixed static arguments or supply its own `paginationOpts`.

Alias/argument names and each module/function segment currently allow ASCII
letters, digits and underscores, excluding reserved control names. These are
adapter admission rules, not a claim about Convex's full function-name grammar.
Payload data-field names use the separate native wire rules below.

The ordinary public queries in [examples/convex/dalgo.ts](examples/convex/dalgo.ts)
show existing-table reads, `v.id("tasks")` validation, identity/tenant enforcement
and forwarding of bounded pagination options. Deploy these functions only with
your own reviewed schema, auth issuer and table access policy. Configuration is
not authorization. The example contains no deployment/admin credentials or seed
mutation. Local-backend deployment and its auth fixture are separate checks.

## Native values and DALgo

`Value` has closed kinds for null, bool, string, signed int64, IEEE754 float64,
binary, arrays and objects. Its zero value is null. Constructors include
`Int64`, `Float64`, `Bytes`, `Array` and `ObjectValue`; accessors return `(value,
kindMatches)` and copy mutable storage. Object/Array constructors return errors
for invalid, cyclic or oversized values. Input objects/configuration/keys are
snapshotted before async token/network work. Callers must not mutate a Go map or
slice concurrently with the initial synchronous admission itself.

`EncodeValue`, `DecodeValue` and `Value` JSON methods implement the pinned official
`convex_encoded_json` representation. Signed int64 uses an eight-byte
little-endian `$integer` tag, binary uses `$bytes`, and NaN/infinities/negative zero
use eight-byte little-endian `$float` tags. Finite ordinary numbers remain native
float64; an untagged large JSON number does not become an int64. No conversion
through `map[string]any`/JSON is used to populate DALgo targets. Canonical tags,
base64, Unicode, duplicate keys, trailing input, depth and node limits are checked.
Unresolved mutation commit timestamps are refused; resolved native int64 works.

Go maps safely preserve wire fields named `__proto__`, `constructor`, `prototype`,
empty names and underscore prefixes. Native wire field names are bounded to
1024 printable ASCII characters and cannot start with `$`. Stored custom-document
field/system-namespace restrictions are a separate backend concern; accepting a
wire value does not prove that it can be inserted as a custom stored field.

```go
target := dalgo2convex.Object{}
r := record.NewRecordWithData(record.NewKeyWithID("tasks", existingID), target)
err := client.DB().Get(ctx, r) // github.com/dal-go/dalgo v0.97.0
amount, isInt64 := target["amount"].Int64()
_, _ = amount, isInt64
```

DALgo `Get`/`Exists`/`GetMulti` use configured collections, native string IDs and
`Object` or `*Object` targets. Get returns the DALgo not-found sentinel; GetMulti
retains missing per-record state. Original record identities and keys are captured
before Data/SetError callbacks so a callback cannot substitute a later slice
element. Each data target is admitted once and is never re-read for delivery. All
results are validated before target delivery. Caller `SetError` callbacks run
synchronously, with a fresh cancellation check after each Key, SetError and Data
callback before another getter, success-state callback or I/O. Error-state
restoration still updates records using the current classification. They receive
fresh final caller/deadline classification, including
callbacks invoked while restoring an earlier error. Callbacks themselves must
return; they are not detached or forcibly terminated.

Generic DALgo structured queries/TugQL/SQL, writes, transactions, dynamic schema
discovery and remote-query kill are **not implemented**. Convex has writes and
transactions; these are adapter omissions, not engine-wide unsupported claims.
`Metadata` describes trusted configuration, with dynamic discovery and verified
authorization both false.

## Cancellation, budgets and cursor acknowledgement

Each public call starts one original monotonic timeout before input admission,
token retrieval, transport, decode and final callbacks. The default is 30 seconds
(maximum 120). Caller cancellation/deadline errors are safe standard context
sentinels, without raw cancellation causes. There is a fresh check after physical
body cleanup and the last user callback. Errors expose only a redacted `Error.Code`;
token, URI, server logs/errorData and transport causes are not forwarded.

The default verified HTTPS transport uses supplied system/custom CA roots, no
ambient proxy, no redirect following, no cookies, bounded response headers and no
automatic decompression. Only `/api/query` is called with user `Bearer` auth; no
admin `Convex` token or mutation/action/export fallback exists. Anonymous mode
requires explicit `Anonymous: true` and no token provider. Bad/missing supplied
tokens never fall back to anonymous. Root URLs cannot contain credentials, paths,
queries or fragments. Owned literal-loopback HTTP requires explicit fixture opt-in.

An injected `RoundTripper` is borrowed trusted infrastructure. Its TLS, proxy,
socket and remote authorization internals are not attested by the adapter. Close
does not close a borrowed transport. It cancels active operation contexts and
closes exact owned response bodies, and stops admitting new calls. HTTP abort
does not prove that remote query computation has stopped.

If a token provider, RoundTripper, body read or close ignores cancellation,
public cancellation remains prompt and its admission lease stays occupied until
that physical work settles. Repeated cancellation cannot create unlimited foreign
work. Body cleanup is rejection/error safe and is never awaited on the public
cancellation path. Successful reads wait for body cleanup; a cleanup error fails
closed. A permanently uncooperative borrowed function can keep a bounded lease
occupied indefinitely; the SDK does not kill arbitrary user goroutines.

Defaults (maximums):

| Bound | Default (maximum) |
| --- | --- |
| Concurrent physically active operations | 4 (16) |
| Rows / getMany keys | 1000 (10000) / 100 (1000) |
| Pages per cursor journey | 100 (1000) |
| Response bytes per call and per journey | 4 MiB (16 MiB) |
| One escaped JSON request | 64 KiB (1 MiB) |
| Total escaped request bytes per call/journey | 1 MiB (16 MiB) |
| Requested row-work per journey | 10000 (100000) |
| Rows requested per page | at most 1000, within row limit |
| Cursor registry / fixed journey TTL | 128 (1024) / 5 min (10 min) |
| Aggregate retained cursor bytes / nodes | 16 MiB (16 MiB) / 65536 (65536) |
| Native value/JSON depth and nodes | 32 / 100000 |

Escaped JSON—including repeated references, binary expansion, HTML escaping,
keys and framing—is budgeted during native admission before Marshal, with a final
actual-byte check. Native copying/standalone codec values have a 16 MiB hard
encoded-size bound. Response wire reads are capped before native parsing; parsed
values and copied object overhead remain distinct from wire-byte accounting.
The remote query must forward maximumRowsRead/maximumBytesRead. Requested work is
a client reservation, not measured remote CPU or observed database bytes.

Page cursors are bounded random handles owned by this Client, not exposed server
cursors or portable resumable tokens. Each binds the alias/function, collection,
canonical typed arguments, page size and a domain-separated token fingerprint.
Tokens are never stored in the journal or exposed in handles/metadata. Auth token
rotation invalidates old handles. A cursor has its own fixed journey TTL/counters;
each continuation call still has a fresh original per-call timeout, never the TTL
as its request deadline.

Empty unfinished pages require a new server cursor. Repeated cursors, duplicate
acknowledged IDs, over-return, split/incomplete pages, malformed/native types and
unusable oversized next cursors refuse the whole page. Nothing is silently
filtered, split, truncated or retried. Attempts retain charged pages/work/request
and response bytes even on failure. Position/acknowledged IDs advance atomically
only at final success; a failed continuation keeps the prior handle retryable
within remaining budgets. An unsettled physical read keeps the journey busy.
Original expiry never refreshes. This is not a snapshot consistency guarantee.

`MaxCursorBytes` and `MaxCursorNodes` limit aggregate retained registry state
across every journey, independently of per-call/per-journey response limits.
Logical byte cost is 256 bytes of journal/header/bookkeeping plus actual handle,
alias and server-cursor bytes, plus 32 bytes and native ID bytes per seen-ID entry.
Nodes count two containers plus one per seen ID. Fixed scope/auth hashes are in
the base; no raw argument JSON or token is retained. These are explicit logical
quotas, not an RSS estimate. New reservations and net ACK replacements are atomic;
refusal retains the old handle and incurred work without delivering rows.
Terminal ACK, idle expiry retirement and Close refund exactly once. Cleanup of
already-retired physical work remains independently bounded by active leases;
Close forbids new admission. Both quotas can be lowered for tighter consumers.

## Verification and upstream contracts

Normal root-controlled checks: `go test -race -count=1 -timeout=90s ./...`,
`go vet ./...`, `go mod verify`, and `gofmt`/normal `go mod tidy`. The single CI
job uses pinned checkout/setup-go revisions and Go 1.27.3, runs on pull requests
and main pushes (no duplicate feature-branch push), performs those checks and has
no tag/release workflow. Tests use owned TLS,
synthetic user tokens, fixed wire goldens, malformed/over-limit pages, physical
cancellation leases, callback mutation, real after-dispatch disconnect and reopen.
The tests do not contact a Convex account or deployment. No actual result is
claimed until checks run on this exact source.

The encoded transport/value contract is pinned to official convex-js 1.49.0 at
`fbb97090685455757c7f523646ed994884f3d6dd`:

- [HTTP client source](https://github.com/get-convex/convex-js/blob/fbb97090685455757c7f523646ed994884f3d6dd/src/browser/http_client.ts)
- [Native value codec](https://github.com/get-convex/convex-js/blob/fbb97090685455757c7f523646ed994884f3d6dd/src/values/value.ts)
- [Pagination types](https://github.com/get-convex/convex-js/blob/fbb97090685455757c7f523646ed994884f3d6dd/src/server/pagination.ts)
- [Public query/auth HTTP contract](https://docs.convex.dev/http-api/)
- [Server pagination contract](https://docs.convex.dev/database/pagination)

The HTTP API's documented plain `json` format overlaps native representations;
this adapter uses the official pinned encoded format instead. Actual deployed
function/identity policy, a real local backend, hosted scope, clean published-module
consumer and CLI integration must be proven separately.
