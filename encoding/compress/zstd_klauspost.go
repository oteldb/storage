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

// encoderWindowBytes caps the ZSTD match window. This package only ever calls EncodeAll on a single
// block (a part column, bounded by the part size), so a huge window buys little — but klauspost sizes
// the encoder's hash tables to the window, so an unbounded one costs tens of MiB of resident state per
// encoder. 8 MiB covers a block's locality with negligible ratio loss.
const encoderWindowBytes = 8 << 20

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

	// WithEncoderConcurrency(1): this pool only does one-shot EncodeAll, so the default GOMAXPROCS
	// worker set (each preallocating window+hash buffers) is pure resident waste — bounding it to one
	// cuts encoder state ~6× with no effect on ratio. WithWindowSize bounds it further (see above).
	enc, err := zstd.NewWriter(io.Discard,
		zstd.WithEncoderLevel(l),
		zstd.WithEncoderConcurrency(1),
		zstd.WithWindowSize(encoderWindowBytes),
	)
	if err != nil {
		panic(err)
	}

	return kpEncoder{enc}
}

func newZstdDecoder() zstdDecoder {
	// DecodeAll never uses the streaming worker pool, so bound concurrency to one and take the
	// low-memory buffers.
	dec, err := zstd.NewReader(io.NopCloser(nilReader{}),
		zstd.WithDecoderConcurrency(1),
		zstd.WithDecoderLowmem(true),
	)
	if err != nil {
		panic(err)
	}

	return kpDecoder{dec}
}

// nilReader is a zero-length reader for constructing a Decoder (DecodeAll doesn't use the reader, but
// NewReader requires a non-nil one).
type nilReader struct{}

func (nilReader) Read([]byte) (int, error) { return 0, io.EOF }
