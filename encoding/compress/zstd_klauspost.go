//go:build !gozstd

package compress

import (
	"io"

	"github.com/klauspost/compress/zstd"
)

// The default ZSTD backend: pure-Go klauspost/compress. No cgo, so the module builds and
// cross-compiles as a static binary. Build with -tags gozstd to switch to libzstd (zstd_gozstd.go).

type kpEncoder struct{ enc *zstd.Encoder }

func (e kpEncoder) encodeAll(dst, src []byte) []byte { return e.enc.EncodeAll(src, dst) }

type kpDecoder struct{ dec *zstd.Decoder }

func (d kpDecoder) decodeAll(dst, src []byte) ([]byte, error) { return d.dec.DecodeAll(src, dst) }

func newZstdEncoder(level Level) zstdEncoder {
	// klauspost exposes four presets, not the full 1–22 range. LevelFast → Fastest, LevelBest →
	// BetterCompression (its BestCompression preset is slower with little/negative ratio gain on
	// log-shaped data — measured), else Default.
	l := zstd.SpeedDefault

	switch {
	case level == LevelFast:
		l = zstd.SpeedFastest
	case level >= LevelBest:
		l = zstd.SpeedBetterCompression
	}

	enc, err := zstd.NewWriter(io.Discard, zstd.WithEncoderLevel(l))
	if err != nil {
		panic(err)
	}

	return kpEncoder{enc}
}

func newZstdDecoder() zstdDecoder {
	dec, err := zstd.NewReader(io.NopCloser(nilReader{}))
	if err != nil {
		panic(err)
	}

	return kpDecoder{dec}
}

// nilReader is a zero-length reader for constructing a Decoder (DecodeAll doesn't use the reader, but
// NewReader requires a non-nil one).
type nilReader struct{}

func (nilReader) Read([]byte) (int, error) { return 0, io.EOF }
