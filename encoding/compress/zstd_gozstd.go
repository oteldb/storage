//go:build gozstd

package compress

import "github.com/valyala/gozstd"

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

func newZstdEncoder(level Level) zstdEncoder {
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

func newZstdDecoder() zstdDecoder { return gzDecoder{} }
