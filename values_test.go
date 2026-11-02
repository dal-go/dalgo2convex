package dalgo2convex

import (
	"bytes"
	"math"
	"strings"
	"testing"
)

func TestOfficialEncodedGoldenValues(t *testing.T) {
	// Independently fixed convex-js1.49 little-endian base64 wire examples.
	cases := []struct {
		name  string
		value Value
		wire  string
	}{
		{"min-int64", Int64(math.MinInt64), `{"$integer":"AAAAAAAAAIA="}`},
		{"max-int64", Int64(math.MaxInt64), `{"$integer":"/////////38="}`},
		{"bytes", Bytes([]byte{0, 255, 128}), `{"$bytes":"AP+A"}`},
		{"negative-zero", Float64(math.Copysign(0, -1)), `{"$float":"AAAAAAAAAIA="}`},
		{"positive-infinity", Float64(math.Inf(1)), `{"$float":"AAAAAAAA8H8="}`},
		{"negative-infinity", Float64(math.Inf(-1)), `{"$float":"AAAAAAAA8P8="}`},
		{"float64", Float64(1.5), `1.5`},
		{"null", Null(), `null`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			encoded, err := EncodeValue(tc.value)
			if err != nil || string(encoded) != tc.wire {
				t.Fatalf("wire mismatch: %s %v", encoded, err)
			}
			decoded, err := DecodeValue([]byte(tc.wire))
			if err != nil {
				t.Fatal(err)
			}
			if decoded.Kind() != tc.value.Kind() {
				t.Fatalf("kind %s", decoded.Kind())
			}
			switch decoded.Kind() {
			case Int64Kind:
				if decoded.integer != tc.value.integer {
					t.Fatal("int64 changed")
				}
			case Float64Kind:
				if math.Float64bits(decoded.number) != math.Float64bits(tc.value.number) {
					t.Fatal("float bits changed")
				}
			case BytesKind:
				if !bytes.Equal(decoded.data, tc.value.data) {
					t.Fatal("bytes changed")
				}
			}
		})
	}
	value := Float64(math.Float64frombits(0x7ff8000000000001))
	encoded, err := EncodeValue(value)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeValue(encoded)
	if err != nil || !math.IsNaN(decoded.number) || math.Float64bits(decoded.number) != math.Float64bits(value.number) {
		t.Fatal("NaN payload lost")
	}
}

func TestNativeWireFieldNamesAndCopyIsolation(t *testing.T) {
	inner := Object{"__proto__": String("safe"), "constructor": Int64(9007199254740993), "prototype": Bytes([]byte{255}), "": Bool(true), "_data": Null(), "_id": String("a")}
	value, err := ObjectValue(Object{"nested": Value{kind: ObjectKind, object: inner}})
	if err != nil {
		t.Fatal(err)
	}
	inner["constructor"] = Null()
	wire, err := EncodeValue(value)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeValue(wire)
	if err != nil {
		t.Fatal(err)
	}
	obj, _ := decoded.Object()
	nested, _ := obj["nested"].Object()
	if v, ok := nested["constructor"].Int64(); !ok || v != 9007199254740993 {
		t.Fatal("constructor/int64 changed")
	}
	if s, _ := nested["__proto__"].String(); s != "safe" {
		t.Fatal("prototype field lost")
	}
	data, _ := nested["prototype"].Bytes()
	data[0] = 0
	again, _ := decoded.Object()
	stored, _ := again["nested"].Object()
	if b, _ := stored["prototype"].Bytes(); b[0] != 255 {
		t.Fatal("byte accessor alias")
	}
	nested["__proto__"] = Null()
	unchanged, _ := decoded.Object()
	a, _ := unchanged["nested"].Object()
	if a["__proto__"].Kind() != StringKind {
		t.Fatal("map accessor alias")
	}
}

func TestMalformedNativeEncodingRefusedWhole(t *testing.T) {
	for _, wire := range []string{`{"$integer":"AA=="}`, `{"$bytes":"AP+A="}`, `{"$float":"AAAAAAAA8D8="}`, `{"$commitTs":null}`, `{"$unknown":""}`, `{"a":1,"a":2}`, `{"$integer":"AAAAAAAAAAA=","x":1}`, `"\ud800"`, `"\udc00"`, `{"x":"\ud800z"}`, `{} true`, `1e999`, `{"$field":1}`, "\"\xff\"", strings.Repeat("[", 42) + "null" + strings.Repeat("]", 42)} {
		t.Run(wire[:min(len(wire), 30)], func(t *testing.T) {
			if _, err := DecodeValue([]byte(wire)); err == nil {
				t.Fatal("malformed value accepted")
			}
		})
	}
	if _, err := DecodeValue([]byte(`"\ud83d\ude00"`)); err != nil {
		t.Fatal("valid astral pair refused")
	}
	if _, err := ObjectValue(Object{"$bad": Null()}); err == nil {
		t.Fatal("reserved field accepted")
	}
	cyclic := Object{}
	cyclic["self"] = Value{kind: ObjectKind, object: cyclic}
	if _, err := ObjectValue(cyclic); err == nil {
		t.Fatal("cyclic native input accepted")
	}
}
