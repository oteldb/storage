package profile

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// pprofCorpus reads testdata/cpu.pprof, a CPU profile of this module's own benchmarks, into symbol
// tables: every sample's stack and the symbols it references, as one batch would deliver them.
func pprofCorpus(tb testing.TB) symTables {
	tb.Helper()

	raw, err := os.ReadFile(filepath.Join("testdata", "cpu.pprof"))
	require.NoError(tb, err)

	zr, err := gzip.NewReader(bytes.NewReader(raw))
	require.NoError(tb, err)

	data, err := io.ReadAll(zr)
	require.NoError(tb, err)

	d, stacks := parsePprof(tb, data)

	b := newBuilder(d)
	for _, st := range stacks {
		b.stackID(st)
	}

	return b.tables
}

// pbField is one protobuf wire field: a varint or fixed value in v, a length-delimited one in b.
type pbField struct {
	num int
	v   uint64
	b   []byte
}

func pbFields(tb testing.TB, p []byte) []pbField {
	tb.Helper()

	var out []pbField

	for len(p) > 0 {
		key, n := binary.Uvarint(p)
		require.Positive(tb, n, "field key")

		p = p[n:]
		f := pbField{num: int(key >> 3)}

		switch key & 7 {
		case 0:
			f.v, n = binary.Uvarint(p)
			require.Positive(tb, n, "varint")
			p = p[n:]
		case 1:
			f.v, p = binary.LittleEndian.Uint64(p), p[8:]
		case 2:
			ln, n := binary.Uvarint(p)
			require.Positive(tb, n, "length")
			f.b, p = p[n:n+int(ln)], p[n+int(ln):]
		case 5:
			f.v, p = uint64(binary.LittleEndian.Uint32(p)), p[4:]
		default:
			tb.Fatalf("wire type %d", key&7)
		}

		out = append(out, f)
	}

	return out
}

// pbUints decodes a repeated uint64 field, packed or not.
func pbUints(tb testing.TB, f pbField) []uint64 {
	tb.Helper()

	if f.b == nil {
		return []uint64{f.v}
	}

	var out []uint64

	for p := f.b; len(p) > 0; {
		v, n := binary.Uvarint(p)
		require.Positive(tb, n, "packed varint")
		out, p = append(out, v), p[n:]
	}

	return out
}

// parsePprof maps a pprof Profile message onto a [Dictionary], returning it with the stack index
// of every sample.
func parsePprof(tb testing.TB, data []byte) (*Dictionary, []int32) {
	tb.Helper()

	var (
		strtab                      [][]byte
		samples, mappings, locs, fs [][]pbField
	)

	for _, f := range pbFields(tb, data) {
		switch f.num {
		case 2:
			samples = append(samples, pbFields(tb, f.b))
		case 3:
			mappings = append(mappings, pbFields(tb, f.b))
		case 4:
			locs = append(locs, pbFields(tb, f.b))
		case 5:
			fs = append(fs, pbFields(tb, f.b))
		case 6:
			strtab = append(strtab, f.b)
		}
	}

	d := &Dictionary{}
	str := func(i uint64) int32 {
		require.Less(tb, i, uint64(len(strtab)), "string index")

		return d.InternString(strtab[i])
	}

	mappingIdx := map[uint64]int32{}

	for _, m := range mappings {
		var id uint64

		var mp Mapping

		for _, f := range m {
			switch f.num {
			case 1:
				id = f.v
			case 2:
				mp.MemoryStart = f.v
			case 3:
				mp.MemoryLimit = f.v
			case 4:
				mp.FileOffset = f.v
			case 5:
				mp.FilenameStrindex = str(f.v)
			}
		}

		mappingIdx[id] = d.AddMapping(mp)
	}

	funcIdx := map[uint64]int32{}

	for _, fn := range fs {
		var id uint64

		var def Function

		for _, f := range fn {
			switch f.num {
			case 1:
				id = f.v
			case 2:
				def.NameStrindex = str(f.v)
			case 3:
				def.SystemNameStrindex = str(f.v)
			case 4:
				def.FilenameStrindex = str(f.v)
			case 5:
				def.StartLine = int64(f.v)
			}
		}

		funcIdx[id] = d.AddFunction(def)
	}

	locIdx := map[uint64]int32{}

	for _, l := range locs {
		var id uint64

		loc := Location{MappingIndex: -1}

		for _, f := range l {
			switch f.num {
			case 1:
				id = f.v
			case 2:
				if mi, ok := mappingIdx[f.v]; ok {
					loc.MappingIndex = mi
				}
			case 3:
				loc.Address = f.v
			case 4:
				var ln Line

				for _, lf := range pbFields(tb, f.b) {
					switch lf.num {
					case 1:
						ln.FunctionIndex = funcIdx[lf.v]
					case 2:
						ln.Line = int64(lf.v)
					case 3:
						ln.Column = int64(lf.v)
					}
				}

				loc.Lines = append(loc.Lines, ln)
			}
		}

		locIdx[id] = d.AddLocation(loc)
	}

	stacks := make([]int32, 0, len(samples))

	for _, s := range samples {
		var st []int32

		for _, f := range s {
			if f.num != 1 {
				continue
			}

			for _, id := range pbUints(tb, f) {
				li, ok := locIdx[id]
				require.True(tb, ok, "sample location %d", id)

				st = append(st, li)
			}
		}

		stacks = append(stacks, d.AddStack(st...))
	}

	return d, stacks
}
