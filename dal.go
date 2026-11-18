package dalgo2convex

import (
	"context"
	"maps"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/recordset"
	"github.com/dal-go/record"
)

// DB exposes native Object/*Object targets through DALgo's read facade. There
// is no JSON/float64 round trip and no hidden managed document/key layout.
func (c *Client) DB() dal.DB { return dal.NewDB(&backend{client: c}) }

type backend struct {
	dal.ConcurrencyAvailable
	client *Client
}

var _ dal.Backend = (*backend)(nil)

func (*backend) ID() string           { return "convex:configured-public-queries" }
func (*backend) Adapter() dal.Adapter { return dal.NewAdapter("dalgo2convex", "unreleased") }
func (*backend) Schema() dal.Schema   { return nil }

type recordAdmission struct {
	record     record.Record
	collection Collection
	id         string
	target     Object
	pointer    *Object
}

func (c *Client) admitRecord(o *operation, r record.Record) (recordAdmission, error) {
	a := recordAdmission{record: r}
	if err := o.check(); err != nil {
		return a, err
	}
	if r == nil {
		return a, failure("input")
	}
	key := r.Key()
	if err := o.check(); err != nil {
		return a, err
	}
	if key == nil || key.Parent() != nil {
		return a, failure("key")
	}
	id, ok := key.ID.(string)
	if !ok || id == "" || len(id) > 1024 || !validID(id) {
		return a, failure("key")
	}
	cfg, err := c.collection(key.Collection())
	if err != nil {
		return a, err
	}
	a.id = id
	a.collection = cfg
	return a, nil
}
func (a *recordAdmission) admitTarget(o *operation) error {
	if err := o.check(); err != nil {
		return err
	}
	a.record.SetError(nil)
	if err := o.check(); err != nil {
		return err
	}
	data := a.record.Data()
	if err := o.check(); err != nil {
		return err
	}
	switch t := data.(type) {
	case Object:
		if t == nil {
			return failure("target")
		}
		a.target = t
	case *Object:
		if t == nil {
			return failure("target")
		}
		a.pointer = t
	default:
		return failure("target")
	}
	return nil
}

func (a *recordAdmission) put(row *Row) {
	if a.pointer != nil {
		*a.pointer = row.Document
		return
	}
	clear(a.target)
	maps.Copy(a.target, row.Document)
}
func (b *backend) Get(ctx context.Context, r record.Record) error {
	return b.getRecords(ctx, []record.Record{r}, true)
}
func (b *backend) Exists(ctx context.Context, key *record.Key) (bool, error) {
	o, err := b.client.begin(ctx)
	if err != nil {
		return false, err
	}
	defer o.finish()
	if key == nil || key.Parent() != nil {
		return false, failure("key")
	}
	collection := key.Collection()
	id, ok := key.ID.(string)
	if !ok || id == "" || len(id) > 1024 || !validID(id) {
		return false, failure("key")
	}
	cfg, err := b.client.collection(collection)
	if err != nil {
		return false, err
	}
	body, err := o.pointBody(cfg, id)
	if err != nil {
		return false, err
	}
	token, _, err := o.auth()
	if err != nil {
		return false, err
	}
	row, err := o.readPoint(cfg, id, body, token)
	if final := o.check(); final != nil {
		return false, final
	}
	return row != nil, err
}
func (b *backend) GetMulti(ctx context.Context, records []record.Record) error {
	return b.getRecords(ctx, records, false)
}
func (b *backend) getRecords(ctx context.Context, records []record.Record, single bool) error {
	// Copy the caller-owned slice before any Key/Data/SetError callback can
	// replace another admitted record or its native collection/id identity.
	o, err := b.client.begin(ctx)
	if len(records) > b.client.options.MaxKeys {
		if err != nil {
			return err
		}
		defer o.finish()
		if final := o.check(); final != nil {
			return final
		}
		return failure("limit")
	}
	records = append([]record.Record(nil), records...)
	if err != nil {
		return finalizeRecords(records, nil, err, func() error {
			if ctx == nil {
				return nil
			}
			if err := ctx.Err(); err != nil {
				if err == context.DeadlineExceeded {
					return context.DeadlineExceeded
				}
				return context.Canceled
			}
			return nil
		})
	}
	defer o.finish()
	finish := func(result error) error { return finalizeRecords(records, nil, result, o.check) }
	admitted := make([]recordAdmission, len(records))
	for i, r := range records {
		admitted[i], err = b.client.admitRecord(o, r)
		if err != nil {
			return finish(err)
		}
	}
	for i := range admitted {
		if err := admitted[i].admitTarget(o); err != nil {
			return finish(err)
		}
		if err := o.check(); err != nil {
			return finish(err)
		}
	}
	if len(admitted) == 0 {
		return finish(nil)
	}
	bodies := make([][]byte, len(admitted))
	for i, a := range admitted {
		bodies[i], err = o.pointBody(a.collection, a.id)
		if err != nil {
			return finish(err)
		}
	}
	token, _, err := o.auth()
	if err != nil {
		return finish(err)
	}
	rows := make([]*Row, len(admitted))
	for i, a := range admitted {
		rows[i], err = o.readPoint(a.collection, a.id, bodies[i], token)
		if err != nil {
			return finish(err)
		}
	}
	if err := o.check(); err != nil {
		return finish(err)
	}
	if single && rows[0] == nil {
		return finish(record.ErrRecordNotFound)
	}
	// Every native result is validated before delivering any target. Missing
	// keys remain per-record outcomes; GetMulti as a batch still succeeds.
	for i := range admitted {
		if rows[i] != nil {
			admitted[i].put(rows[i])
		}
	}
	states := make([]error, len(admitted))
	for i := range admitted {
		if rows[i] == nil {
			states[i] = record.ErrRecordNotFound
		}
	}
	return finalizeRecords(records, states, nil, o.check)
}
func (*backend) RunReadonlyTransaction(context.Context, dal.ROTxWorker, ...dal.TransactionOption) error {
	return dal.ErrNotSupported
}
func (*backend) RunReadwriteTransaction(context.Context, dal.RWTxWorker, ...dal.TransactionOption) error {
	return dal.ErrNotSupported
}
func (*backend) ExecuteQueryToRecordsReader(context.Context, dal.Query) (dal.RecordsReader, error) {
	return nil, dal.ErrNotSupported
}
func (*backend) ExecuteQueryToRecordsetReader(context.Context, dal.Query, ...recordset.Option) (dal.RecordsetReader, error) {
	return nil, dal.ErrNotSupported
}

// Real contexts have monotonic states. Priority may advance from local stop to
// local expiry to parent expiry/cancellation during a SetError callback. Repeat
// only for such a new classification, at most the four possible states plus the
// original result. No callbacks are detached and no Err-call-count policy exists.
func finalizeRecords(records []record.Record, states []error, result error, check func() error) error {
	for pass := 0; pass < 5; pass++ {
		if final := check(); final != nil {
			result = final
			states = nil
		}
		changed := false
		for i, r := range records {
			if r == nil {
				continue
			}
			if final := check(); final != nil && final != result {
				result = final
				states = nil
				changed = true
			}
			state := result
			if states != nil {
				state = states[i]
			}
			r.SetError(state)
			if final := check(); final != nil && final != result {
				result = final
				states = nil
				changed = true
			}
		}
		if final := check(); final != nil && final != result {
			result = final
			states = nil
			changed = true
		}
		if !changed {
			return result
		}
	}
	return result
}
