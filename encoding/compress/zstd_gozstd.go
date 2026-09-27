//go:build gozstd

package compress

import (
	"io"

	"github.com/valyala/gozstd"
)

// The libzstd ZSTD backend (build -tags gozstd): github.com/valyala/gozstd, a cgo binding of the C
// reference zstd. Higher ratio than pure-Go klauspost at high levels, full 1–22 range. The stateless
// CompressLevel/Decompress calls pool their C contexts internally, so no per-call native allocation
// escapes — but that C memory is invisible to the Go GC and GOMEMLIMIT, so do not spin up unbounded
// distinct Compressors. This build is not static/pure-Go; keep the default (klauspost) build for CI
// and cross-compilation.

type gzEncoder struct{ level int }

func (e gzEncoder) encodeAll(dst, src []byte) []byte { return gozstd.CompressLevel(dst, src, e.level) }

type gzDecoder struct{}

func (gzDecoder) decodeAll(dst, src []byte) ([]byte, error) { return gozstd.Decompress(dst, src) }

// minEncoderWindow is libzstd's smallest window.
const minEncoderWindow = 1 << 10

// encoderWindowBytes is the window [NewCompressor] asks for; libzstd sizes its own.
const encoderWindowBytes = 8 << 20

// newZstdEncoder ignores window: a one-shot libzstd compress of a known source size already fits
// the context's window and tables to the source.
func newZstdEncoder(level Level, _ int) zstdEncoder {
	// Map the abstract level to a real libzstd level: LevelFast → 1, LevelBest → 19 (the cold tier),
	// else → 12 (libzstd's sweet spot vs klauspost — comparable encode time, markedly better ratio on
	// structured data; L19 is far denser but ~40× slower, so it fits only cold recompression).
	l := 12

	switch {
	case level == LevelFast:
		l = 1
	case level >= LevelBest:
		l = 19
	}

	return gzEncoder{level: l}
}

// zstdEncodeWorkspace is a libzstd compression context at the mapped level. It is C memory, outside
// the Go heap and GOMEMLIMIT, which the heap measurements behind the klauspost figures cannot see, so
// these are round figures meant to sit above a context's window and tables at each level, unmeasured.
func zstdEncodeWorkspace(level Level, _ int) int64 {
	switch {
	case level == LevelFast:
		return 4 << 20
	case level >= LevelBest:
		return 128 << 20
	default:
		return 64 << 20
	}
}

func newZstdDecoder() zstdDecoder { return gzDecoder{} }

type gzStreamDecoder struct{ r *gozstd.Reader }

func (d gzStreamDecoder) Read(p []byte) (int, error) { return d.r.Read(p) }

func (d gzStreamDecoder) reset(r io.Reader) error {
	d.r.Reset(r, nil)

	return nil
}

func (d gzStreamDecoder) release() { d.r.Reset(nil, nil) }

func newZstdStreamDecoder() zstdStreamDecoder { return gzStreamDecoder{gozstd.NewReader(nil)} }
