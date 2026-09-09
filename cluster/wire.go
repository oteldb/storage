package cluster

import (
	"net/http"
	"strings"

	"github.com/oteldb/storage/encoding/compress"
)

// Content coding negotiated on the read fan-out. The names and the negotiation are HTTP's own, so a
// node that predates this simply never advertises zstd and is answered in plaintext, and a node that
// does advertise it still handles a plaintext answer.
const (
	acceptEncodingHeader  = "Accept-Encoding"
	contentEncodingHeader = "Content-Encoding"
	encodingZSTD          = "zstd"
)

// wireCompressor compresses fan-out response payloads whole.
//
// LevelFast rather than the default: measured on a 17.6 MB log fan-out payload the two are within
// 1% of each other on ratio (4.29× vs 4.33×) while fast encodes 20% quicker (365 vs 303 MB/s), and
// on integral metric samples fast is the *denser* of the two (8.7× vs 6.1×). The asymmetry is the
// point — encoding is paid by the owners, which scale out, and decoding (~1.3 GB/s) by the single
// aggregator, which does not.
var wireCompressor = compress.NewZSTDFramer(compress.LevelFast)

// acceptsZSTD reports whether the request advertised zstd as an acceptable content coding. A coding
// listed with q=0 is explicitly refused, so it does not count.
func acceptsZSTD(h http.Header) bool {
	for coding := range strings.SplitSeq(h.Get(acceptEncodingHeader), ",") {
		name, params, _ := strings.Cut(coding, ";")
		if !strings.EqualFold(strings.TrimSpace(name), encodingZSTD) {
			continue
		}

		return !isRefused(params)
	}

	return false
}

func isRefused(params string) bool {
	for p := range strings.SplitSeq(params, ";") {
		k, v, ok := strings.Cut(p, "=")
		if !ok || !strings.EqualFold(strings.TrimSpace(k), "q") {
			continue
		}

		switch strings.TrimSpace(v) {
		case "0", "0.", "0.0", "0.00", "0.000":
			return true
		}
	}

	return false
}

// compressResponse compresses a fan-out payload when the caller advertised zstd, declaring the
// coding on w. It falls back to plaintext when compression fails to shrink the payload, so a
// pathological body never costs bandwidth.
func compressResponse(w http.ResponseWriter, h http.Header, out []byte) []byte {
	if !acceptsZSTD(h) {
		return out
	}

	enc := wireCompressor.Compress(nil, out)
	if len(enc) >= len(out) {
		return out
	}

	w.Header().Set(contentEncodingHeader, encodingZSTD)

	return enc
}

// zstdEncoded reports whether a response body is a zstd frame.
func zstdEncoded(h http.Header) bool {
	return strings.EqualFold(strings.TrimSpace(h.Get(contentEncodingHeader)), encodingZSTD)
}
