package seriesindex

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	backendfile "github.com/oteldb/storage/backend/file"
)

type packedPageCache struct {
	backend.Backend
	backend.PageViewer
}

type coldDecodedPages struct{ packedPageCache }

func (b coldDecodedPages) ReadDecodedPage(ctx context.Context, key string, off, n int64, _ string, decoder backend.PageDecoder) ([]byte, error) {
	data, err := b.ReadPageView(ctx, key, off, n)
	if err != nil {
		return nil, err
	}
	return decoder.DecodePage(data)
}

func BenchmarkTablePageTraversal(b *testing.B) {
	f, err := backendfile.New(b.TempDir())
	require.NoError(b, err)
	require.NoError(b, f.Write(b.Context(), "index", tableData(b, 4096, 64<<10)))
	cached := backend.Cached(f, 16<<20)
	packed := packedPageCache{Backend: cached, PageViewer: cached.(backend.PageViewer)}
	for _, mode := range []string{"packed", "decoded-cold", "decoded-warm"} {
		var source backend.Backend = packed
		if mode == "decoded-cold" {
			source = coldDecodedPages{packedPageCache: packed}
		}
		if mode == "decoded-warm" {
			source = cached
		}
		table, err := OpenTable(b.Context(), source, "index")
		require.NoError(b, err)
		keys := make([][]byte, 16)
		for i := range keys {
			keys[i] = fmt.Appendf(nil, "key-%08d", (i*7919)%4096)
		}
		// Warm raw cache for every mode; cold refers only to decoded-page admission.
		c := table.Cursor(b.Context())
		for c.Next() {
		}
		require.NoError(b, c.Err())
		for _, pattern := range []string{"random", "sequential"} {
			b.Run(mode+"/"+pattern, func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					c := table.Cursor(b.Context())
					if pattern == "random" {
						for _, key := range keys {
							if !c.Seek(key) {
								b.Fatal(c.Err())
							}
						}
					} else {
						for range 4096 {
							if !c.Next() {
								b.Fatal(c.Err())
							}
						}
					}
				}
			})
		}
	}
}

func BenchmarkTableOrderedPages(b *testing.B) {
	files, err := backendfile.New(b.TempDir())
	require.NoError(b, err)
	require.NoError(b, files.Write(b.Context(), "index", tableData(b, 65536, 64<<10)))
	keys := make([][]byte, 8192)
	for i := range keys {
		keys[i] = fmt.Appendf(nil, "key-%08d", i*8)
	}
	for _, mode := range []string{"expanded", "streaming"} {
		b.Run(mode, func(b *testing.B) {
			table, err := OpenTable(b.Context(), backend.Cached(files, 64<<10), "index")
			require.NoError(b, err)
			b.ReportAllocs()
			for b.Loop() {
				c := table.Cursor(b.Context())
				c.streaming = mode == "streaming"
				for _, key := range keys {
					if !c.Seek(key) {
						b.Fatal(c.Err())
					}
				}
			}
		})
	}
}
