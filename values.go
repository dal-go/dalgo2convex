package dalgo2convex

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"io"
	"math"
	"strconv"
	"strings"
	"unicode/utf8"
)

// Object preserves native Convex fields. It is not a managed document envelope.
// Empty, underscore-prefixed, __proto__, constructor and prototype names are valid
// wire fields. This does not assert that every wire field is a stored custom field.
type Object map[string]Value

type Kind string

const (
	NullKind    Kind = "null"
	BoolKind    Kind = "bool"
	StringKind  Kind = "string"
	Int64Kind   Kind = "int64"
	Float64Kind Kind = "float64"
	BytesKind   Kind = "bytes"
	ArrayKind   Kind = "array"
	ObjectKind  Kind = "object"
)

// Value is a closed native value. The zero value is null. Constructors and
// accessors copy mutable storage; passing an Object to a read also snapshots it.
type Value struct {
	kind    Kind
	boolean bool
	text    string
	integer int64
	number  float64
	data    []byte
	array   []Value
	object  Object
}

func Null() Value             { return Value{} }
func Bool(v bool) Value       { return Value{kind: BoolKind, boolean: v} }
func String(v string) Value   { return Value{kind: StringKind, text: v} }
func Int64(v int64) Value     { return Value{kind: Int64Kind, integer: v} }
func Float64(v float64) Value { return Value{kind: Float64Kind, number: v} }
func Bytes(v []byte) Value    { return Value{kind: BytesKind, data: bytes.Clone(v)} }
func Array(v []Value) (Value, error) {
	return copyValue(Value{kind: ArrayKind, array: v}, 0, new(valueBudget))
}
func ObjectValue(v Object) (Value, error) {
	return copyValue(Value{kind: ObjectKind, object: v}, 0, new(valueBudget))
}
func (v Value) Kind() Kind {
	if v.kind == "" {
		return NullKind
	}
	return v.kind
}
func (v Value) Bool() (bool, bool)       { return v.boolean, v.Kind() == BoolKind }
func (v Value) String() (string, bool)   { return v.text, v.Kind() == StringKind }
func (v Value) Int64() (int64, bool)     { return v.integer, v.Kind() == Int64Kind }
func (v Value) Float64() (float64, bool) { return v.number, v.Kind() == Float64Kind }
func (v Value) Bytes() ([]byte, bool)    { return bytes.Clone(v.data), v.Kind() == BytesKind }
func (v Value) Array() ([]Value, bool) {
	c, err := copyValue(v, 0, new(valueBudget))
	return c.array, err == nil && v.Kind() == ArrayKind
}
func (v Value) Object() (Object, bool) {
	c, err := copyValue(v, 0, new(valueBudget))
	return c.object, err == nil && v.Kind() == ObjectKind
}

func wireField(s string) bool {
	if len(s) > 1024 || strings.HasPrefix(s, "$") {
		return false
	}
	for _, c := range s {
		if c < 32 || c > 126 {
			return false
		}
	}
	return true
}

type valueBudget struct {
	nodes int
	bytes int
	limit int
}

func (b *valueBudget) charge(n int) error {
	if b.limit == 0 {
		b.limit = 16 * 1024 * 1024
	}
	if n < 0 || n > b.limit-b.bytes {
		return failure("limit")
	}
	b.bytes += n
	return nil
}
func jsonStringSize(s string) (int, error) {
	if !utf8.ValidString(s) {
		return 0, failure("value")
	}
	n := 2
	for _, r := range s {
		switch r {
		case '"', '\\', '\n', '\r', '\t', '\b', '\f':
			n += 2
		case '<', '>', '&', '\u2028', '\u2029':
			n += 6
		default:
			if r < 32 {
				n += 6
			} else {
				n += utf8.RuneLen(r)
			}
		}
	}
	return n, nil
}
func finiteFloatJSONSize(v float64) int {
	format := byte('f')
	abs := math.Abs(v)
	if abs != 0 && (abs < 1e-6 || abs >= 1e21) {
		format = 'e'
	}
	b := strconv.AppendFloat(make([]byte, 0, 32), v, format, -1, 64)
	n := len(b)
	if format == 'e' && n >= 4 && b[n-4] == 'e' && b[n-3] == '-' && b[n-2] == '0' {
		n--
	}
	return n
}
func copyValue(v Value, depth int, budget *valueBudget) (Value, error) {
	if err := budget.charge(0); err != nil {
		return Value{}, err
	}
	budget.nodes++
	if depth > 32 || budget.nodes > 100000 {
		return Value{}, failure("limit")
	}
	switch v.Kind() {
	case NullKind:
		if err := budget.charge(4); err != nil {
			return Value{}, err
		}
	case BoolKind:
		n := 5
		if v.boolean {
			n = 4
		}
		if err := budget.charge(n); err != nil {
			return Value{}, err
		}
	case Int64Kind:
		if err := budget.charge(27); err != nil {
			return Value{}, err
		}
	case Float64Kind:
		n := 25
		if !math.IsNaN(v.number) && !math.IsInf(v.number, 0) && !(v.number == 0 && math.Signbit(v.number)) {
			n = finiteFloatJSONSize(v.number)
		}
		if err := budget.charge(n); err != nil {
			return Value{}, err
		}
	case StringKind:
		if len(v.text) > budget.limit-budget.bytes-2 {
			return Value{}, failure("limit")
		}
		n, err := jsonStringSize(v.text)
		if err != nil {
			return Value{}, err
		}
		if err = budget.charge(n); err != nil {
			return Value{}, err
		}
	case BytesKind:
		if len(v.data) > 16*1024*1024 {
			return Value{}, failure("limit")
		}
		if err := budget.charge(base64.StdEncoding.EncodedLen(len(v.data)) + 13); err != nil {
			return Value{}, err
		}
		v.data = bytes.Clone(v.data)
	case ArrayKind:
		if len(v.array) > 100000 {
			return Value{}, failure("limit")
		}
		if err := budget.charge(2 + max(0, len(v.array)-1)); err != nil {
			return Value{}, err
		}
		if len(v.array) > budget.limit-budget.bytes {
			return Value{}, failure("limit")
		}
		out := make([]Value, len(v.array))
		for i, item := range v.array {
			c, err := copyValue(item, depth+1, budget)
			if err != nil {
				return Value{}, err
			}
			out[i] = c
		}
		v.array = out
	case ObjectKind:
		if len(v.object) > 100000 {
			return Value{}, failure("limit")
		}
		if err := budget.charge(2 + max(0, len(v.object)-1)); err != nil {
			return Value{}, err
		}
		if len(v.object) > (budget.limit-budget.bytes)/4 {
			return Value{}, failure("limit")
		}
		out := make(Object, len(v.object))
		for k, item := range v.object {
			if !wireField(k) {
				return Value{}, failure("value")
			}
			n, err := jsonStringSize(k)
			if err != nil {
				return Value{}, err
			}
			if err := budget.charge(n + 1); err != nil {
				return Value{}, err
			}
			c, err := copyValue(item, depth+1, budget)
			if err != nil {
				return Value{}, err
			}
			out[k] = c
		}
		v.object = out
	default:
		return Value{}, failure("value")
	}
	return v, nil
}

func encodeNative(v Value) (any, error) {
	switch v.Kind() {
	case NullKind:
		return nil, nil
	case BoolKind:
		return v.boolean, nil
	case StringKind:
		return v.text, nil
	case Int64Kind:
		var b [8]byte
		binary.LittleEndian.PutUint64(b[:], uint64(v.integer))
		return map[string]string{"$integer": base64.StdEncoding.EncodeToString(b[:])}, nil
	case Float64Kind:
		if !math.IsNaN(v.number) && !math.IsInf(v.number, 0) && !(v.number == 0 && math.Signbit(v.number)) {
			return v.number, nil
		}
		var b [8]byte
		binary.LittleEndian.PutUint64(b[:], math.Float64bits(v.number))
		return map[string]string{"$float": base64.StdEncoding.EncodeToString(b[:])}, nil
	case BytesKind:
		return map[string]string{"$bytes": base64.StdEncoding.EncodeToString(v.data)}, nil
	case ArrayKind:
		out := make([]any, len(v.array))
		for i, item := range v.array {
			c, err := encodeNative(item)
			if err != nil {
				return nil, err
			}
			out[i] = c
		}
		return out, nil
	case ObjectKind:
		out := make(map[string]any, len(v.object))
		for k, item := range v.object {
			c, err := encodeNative(item)
			if err != nil {
				return nil, err
			}
			out[k] = c
		}
		return out, nil
	default:
		return nil, failure("value")
	}
}

// EncodeValue uses the pinned official convex_encoded_json representation,
// preserving int64, binary and IEEE values instead of overlapping plain JSON.
func EncodeValue(v Value) ([]byte, error) {
	c, err := copyValue(v, 0, new(valueBudget))
	if err != nil {
		return nil, err
	}
	x, err := encodeNative(c)
	if err != nil {
		return nil, err
	}
	data, err := json.Marshal(x)
	if err != nil {
		return nil, failure("value")
	}
	return data, nil
}

func decodeNative(x any, depth int) (Value, error) {
	if depth > 32 {
		return Value{}, failure("limit")
	}
	switch x := x.(type) {
	case nil:
		return Null(), nil
	case bool:
		return Bool(x), nil
	case string:
		return String(x), nil
	case json.Number:
		f, err := strconv.ParseFloat(string(x), 64)
		if err != nil || math.IsInf(f, 0) || (f == 0 && math.Signbit(f)) {
			return Value{}, failure("response")
		}
		return Float64(f), nil
	case []any:
		out := make([]Value, len(x))
		for i, item := range x {
			v, err := decodeNative(item, depth+1)
			if err != nil {
				return Value{}, err
			}
			out[i] = v
		}
		return Value{kind: ArrayKind, array: out}, nil
	case map[string]any:
		if len(x) == 1 {
			for k, raw := range x {
				if !strings.HasPrefix(k, "$") {
					break
				}
				s, ok := raw.(string)
				if !ok {
					return Value{}, failure("response")
				}
				b, err := base64.StdEncoding.Strict().DecodeString(s)
				if err != nil || base64.StdEncoding.EncodeToString(b) != s {
					return Value{}, failure("response")
				}
				switch k {
				case "$bytes":
					return Bytes(b), nil
				case "$integer":
					if len(b) != 8 {
						return Value{}, failure("response")
					}
					return Int64(int64(binary.LittleEndian.Uint64(b))), nil
				case "$float":
					if len(b) != 8 {
						return Value{}, failure("response")
					}
					f := math.Float64frombits(binary.LittleEndian.Uint64(b))
					if !math.IsNaN(f) && !math.IsInf(f, 0) && !(f == 0 && math.Signbit(f)) {
						return Value{}, failure("response")
					}
					return Float64(f), nil
				default:
					return Value{}, failure("response")
				}
			}
		}
		out := make(Object, len(x))
		for k, item := range x {
			if !wireField(k) {
				return Value{}, failure("response")
			}
			v, err := decodeNative(item, depth+1)
			if err != nil {
				return Value{}, err
			}
			out[k] = v
		}
		return Value{kind: ObjectKind, object: out}, nil
	default:
		return Value{}, failure("response")
	}
}

// validJSONStrings refuses malformed Unicode instead of encoding/json's lossy
// replacement of an unpaired UTF16 surrogate or malformed UTF8 sequence.
func validJSONStrings(b []byte) bool {
	if !utf8.Valid(b) {
		return false
	}
	for i := 0; i < len(b); i++ {
		if b[i] != '"' {
			continue
		}
		i++
		for ; i < len(b) && b[i] != '"'; i++ {
			if b[i] != '\\' {
				continue
			}
			i++
			if i >= len(b) {
				return false
			}
			if b[i] != 'u' {
				continue
			}
			if i+4 >= len(b) {
				return false
			}
			n, err := strconv.ParseUint(string(b[i+1:i+5]), 16, 16)
			if err != nil {
				return false
			}
			i += 4
			if n >= 0xdc00 && n <= 0xdfff {
				return false
			}
			if n >= 0xd800 && n <= 0xdbff {
				if i+6 >= len(b) || b[i+1] != '\\' || b[i+2] != 'u' {
					return false
				}
				m, err := strconv.ParseUint(string(b[i+3:i+7]), 16, 16)
				if err != nil || m < 0xdc00 || m > 0xdfff {
					return false
				}
				i += 6
			}
		}
	}
	return true
}

func strictJSON(data []byte) (any, error) {
	if !validJSONStrings(data) {
		return nil, failure("response")
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	nodes := 0
	var read func(int) (any, error)
	read = func(depth int) (any, error) {
		nodes++
		if depth > 40 || nodes > 100000 {
			return nil, failure("limit")
		}
		t, err := d.Token()
		if err != nil {
			return nil, failure("response")
		}
		delim, ok := t.(json.Delim)
		if !ok {
			return t, nil
		}
		switch delim {
		case '{':
			out := map[string]any{}
			for d.More() {
				key, err := d.Token()
				if err != nil {
					return nil, failure("response")
				}
				k, ok := key.(string)
				if !ok {
					return nil, failure("response")
				}
				if _, duplicate := out[k]; duplicate {
					return nil, failure("response")
				}
				v, err := read(depth + 1)
				if err != nil {
					return nil, err
				}
				out[k] = v
			}
			end, err := d.Token()
			if err != nil || end != json.Delim('}') {
				return nil, failure("response")
			}
			return out, nil
		case '[':
			out := []any{}
			for d.More() {
				v, err := read(depth + 1)
				if err != nil {
					return nil, err
				}
				out = append(out, v)
			}
			end, err := d.Token()
			if err != nil || end != json.Delim(']') {
				return nil, failure("response")
			}
			return out, nil
		default:
			return nil, failure("response")
		}
	}
	x, err := read(0)
	if err != nil {
		return nil, err
	}
	if _, err := d.Token(); err != io.EOF {
		return nil, failure("response")
	}
	return x, nil
}

func DecodeValue(data []byte) (Value, error) {
	if len(data) > 16*1024*1024 {
		return Value{}, failure("limit")
	}
	x, err := strictJSON(data)
	if err != nil {
		return Value{}, err
	}
	v, err := decodeNative(x, 0)
	if err != nil {
		return Value{}, err
	}
	return copyValue(v, 0, new(valueBudget))
}

// MarshalJSON preserves native tagged values for ordinary Go JSON consumers.
func (v Value) MarshalJSON() ([]byte, error) { return EncodeValue(v) }
func (v *Value) UnmarshalJSON(data []byte) error {
	if v == nil {
		return failure("value")
	}
	decoded, err := DecodeValue(data)
	if err != nil {
		return err
	}
	*v = decoded
	return nil
}
