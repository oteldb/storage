package profile

import (
	"bytes"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

// retainFixture holds two stacks that share main's function, location and strings. Stack a also
// reaches a mapping with a filename and an inlined location of two lines.
func retainFixture() (d *Dictionary, a, b int32) {
	d = &Dictionary{}
	main := d.AddFunction(Function{NameStrindex: d.InternString([]byte("main")), FilenameStrindex: d.InternString([]byte("main.go"))})
	mainLoc := d.AddLocation(Location{Lines: []Line{{FunctionIndex: main, Line: 1}}})

	mapping := d.AddMapping(Mapping{MemoryStart: 0x1000, MemoryLimit: 0x2000, FilenameStrindex: d.InternString([]byte("app"))})
	inner := d.AddFunction(Function{NameStrindex: d.InternString([]byte("inner")), SystemNameStrindex: d.InternString([]byte("_inner"))})
	outer := d.AddFunction(Function{NameStrindex: d.InternString([]byte("outer"))})
	inlined := d.AddLocation(Location{MappingIndex: mapping, Address: 0x1234, Lines: []Line{
		{FunctionIndex: inner, Line: 7, Column: 3},
		{FunctionIndex: outer, Line: 9},
	}})

	leaf := d.AddLocation(Location{Lines: []Line{{FunctionIndex: d.AddFunction(Function{NameStrindex: d.InternString([]byte("leaf"))})}}})

	a = d.AddStack(inlined, mainLoc)
	b = d.AddStack(leaf, mainLoc)

	return d, a, b
}

func stackDelta(d *Dictionary, stacks ...int32) (symTables, [][]byte) {
	b := newBuilder(d)
	ids := make([][]byte, 0, len(stacks))

	for _, st := range stacks {
		ids = append(ids, b.stackID(st).AppendBinary(nil))
	}

	return b.tables, ids
}

func TestSymbolStoreRetain(t *testing.T) {
	t.Parallel()

	d, a, b := retainFixture()
	full, ids := stackDelta(d, a, b)
	onlyA, _ := stackDelta(d, a)
	onlyB, _ := stackDelta(d, b)
	unknown := signal.HashBytes([]byte("unknown")).AppendBinary(nil)

	for _, tt := range []struct {
		name string
		refs [][]byte
		want symTables
	}{
		{"Both", ids, full},
		{"OnlyA", [][]byte{ids[0], ids[0]}, onlyA},
		{"OnlyB", [][]byte{ids[1]}, onlyB},
		{"UnknownAndMalformed", [][]byte{unknown, {1, 2, 3}, nil}, newSymTables()},
		{"None", nil, newSymTables()},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := NewSymbolStore()
			require.NoError(t, s.Absorb(encodeDelta(full)))

			s.Retain(slices.Values(tt.refs))
			require.Equal(t, tt.want, s.acc)
		})
	}
}

// FuzzRetain replaces one table's entries with arbitrary bytes: following them must not panic and
// must keep only entries the accumulator held.
func FuzzRetain(f *testing.F) {
	d, a, b := retainFixture()
	full, ids := stackDelta(d, a, b)

	for i := range full.t {
		for _, entry := range full.t[i] {
			f.Add(uint8(i), entry)
			f.Add(uint8(i), entry[:len(entry)/2])
		}
	}

	f.Fuzz(func(t *testing.T, table uint8, data []byte) {
		s := NewSymbolStore()
		if err := s.Absorb(encodeDelta(full)); err != nil {
			t.Fatal(err)
		}

		held := s.acc.t[int(table)%len(s.acc.t)]
		for id := range held {
			held[id] = data
		}

		orig := s.acc
		s.Retain(slices.Values(ids))

		for i := range s.acc.t {
			for id, entry := range s.acc.t[i] {
				if prev, ok := orig.t[i][id]; !ok || !bytes.Equal(prev, entry) {
					t.Fatalf("table %d kept an entry it did not hold: %v", i, id)
				}
			}
		}
	})
}
