package chunk

import "testing"

func BenchmarkMetricCursorBlocks(b *testing.B) {
	var encoder FloatGroupEncoder
	floats := make([]float64, 16)
	regular, irregular := make([]int64, 16), make([]int64, 16)
	for i := range floats {
		floats[i] = float64(i) / 1000
		regular[i] = int64(i) * 15_000_000_000
		irregular[i] = regular[i] + int64(i*i)
	}
	floatData, err := encoder.Encode(nil, floats, []int{len(floats)})
	if err != nil {
		b.Fatal(err)
	}
	regularData, err := EncodeTimestampsAdaptive(nil, regular)
	if err != nil {
		b.Fatal(err)
	}
	irregularData, err := EncodeTimestampsAdaptive(nil, irregular)
	if err != nil {
		b.Fatal(err)
	}
	for _, reuse := range []bool{false, true} {
		name := "fresh"
		if reuse {
			name = "reuse"
		}
		b.Run("floats/"+name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(floats) * 8))
			var decoder *FloatGroupsDecoder
			for b.Loop() {
				var err error
				if decoder != nil && reuse {
					err = decoder.Reset(floatData)
				} else {
					decoder, err = NewFloatGroupsDecoder(floatData)
				}
				if err != nil {
					b.Fatal(err)
				}
				for range floats {
					if _, err := decoder.Next(); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
		for _, tc := range []struct {
			name string
			data []byte
		}{{"timestamps-regular", regularData}, {"timestamps-irregular", irregularData}} {
			b.Run(tc.name+"/"+name, func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(len(regular) * 8))
				var cursor TsCursor
				for b.Loop() {
					var err error
					if cursor != nil && reuse {
						reset, ok := cursor.(interface{ Reset(src []byte) error })
						if !ok {
							b.Fatal("cursor does not support reset")
						}
						err = reset.Reset(tc.data)
					} else {
						cursor, err = NewAdaptiveTimestampCursor(tc.data)
					}
					if err != nil {
						b.Fatal(err)
					}
					for range regular {
						if _, err := cursor.Next(); err != nil {
							b.Fatal(err)
						}
					}
				}
			})
		}
	}
}
