package compress

import (
	"io"
	"sync"
)

// ZSTDFramer is a pooled codec for bare zstd frames, without the [FlagRaw]/[FlagCompressed] prefix
// [Compressor] puts in front of a block. Two properties follow from that and neither is available
// from [Compressor]:
//
//   - the bytes are a standard zstd frame, so any zstd implementation reads them — an HTTP
//     `Content-Encoding: zstd` body, for one;
//   - [ZSTDFramer.Reader] decompresses as a stream, so a caller can bound the *decompressed* size.
//     A whole-buffer decode cannot: it has already allocated the output by the time the size is
//     known, which is exactly what a decompression bomb exploits.
//
// A ZSTDFramer is safe for concurrent use.
type ZSTDFramer struct {
	encPool sync.Pool // zstdEncoder
	decPool sync.Pool // zstdStreamDecoder
}

// NewZSTDFramer returns a [ZSTDFramer] compressing at the given level.
func NewZSTDFramer(level Level) *ZSTDFramer {
	f := &ZSTDFramer{}
	f.encPool = sync.Pool{New: func() any { return newZstdEncoder(level) }}
	f.decPool = sync.Pool{New: func() any { return newZstdStreamDecoder() }}

	return f
}

// Compress appends the zstd frame of src to dst and returns the extended slice. Unlike
// [Compressor.Compress] it always compresses, even when that grows the input; a caller that cares
// compares the lengths itself.
func (f *ZSTDFramer) Compress(dst, src []byte) []byte {
	enc, _ := f.encPool.Get().(zstdEncoder)
	defer f.encPool.Put(enc)

	return enc.encodeAll(dst, src)
}

// Reader returns a streaming decompressor over the zstd frame in r. Close returns the decoder to
// the pool and must be called; it does not close r.
func (f *ZSTDFramer) Reader(r io.Reader) (io.ReadCloser, error) {
	dec, _ := f.decPool.Get().(zstdStreamDecoder)
	if err := dec.reset(r); err != nil {
		f.decPool.Put(dec)

		return nil, err
	}

	return &framedReader{framer: f, dec: dec}, nil
}

type framedReader struct {
	framer *ZSTDFramer
	dec    zstdStreamDecoder
}

func (r *framedReader) Read(p []byte) (int, error) {
	if r.dec == nil {
		return 0, io.EOF
	}

	return r.dec.Read(p)
}

func (r *framedReader) Close() error {
	if r.dec == nil {
		return nil
	}

	dec := r.dec
	r.dec = nil
	// Drop the reference to the source before pooling: a decoder kept in the pool otherwise pins
	// the response body of the request that borrowed it.
	dec.release()
	r.framer.decPool.Put(dec)

	return nil
}

// zstdStreamDecoder is a reusable streaming zstd decoder; the backend is selected at build time
// alongside [zstdDecoder] (see zstd_klauspost.go / zstd_gozstd.go).
type zstdStreamDecoder interface {
	io.Reader
	reset(r io.Reader) error
	release()
}
