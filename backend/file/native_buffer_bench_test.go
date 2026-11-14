package file

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"io/fs"
	"testing"
	"time"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/internal/sidecar"
	"github.com/oteldb/storage/internal/vfs"
)

type nativeBufferIO struct {
	writeCalls, writeBytes int64
	readCalls, readBytes   int64
	syncCalls              int64
	syncTime               time.Duration
}

type nativeBufferFS struct {
	vfs.FS

	stats *nativeBufferIO
}

func (f nativeBufferFS) OpenFile(name string, flag int, perm fs.FileMode) (vfs.File, error) {
	r, err := f.FS.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	return nativeBufferFile{File: r, stats: f.stats}, nil
}

type nativeBufferFile struct {
	vfs.File

	stats *nativeBufferIO
}

func (f nativeBufferFile) Write(p []byte) (int, error) {
	f.stats.writeCalls++
	n, err := f.File.Write(p)
	f.stats.writeBytes += int64(n)
	return n, err
}

func (f nativeBufferFile) ReadAt(p []byte, off int64) (int, error) {
	f.stats.readCalls++
	n, err := f.File.ReadAt(p, off)
	f.stats.readBytes += int64(n)
	return n, err
}

func (f nativeBufferFile) Sync() error {
	start := time.Now()
	err := f.File.Sync()
	f.stats.syncTime += time.Since(start)
	f.stats.syncCalls++
	return err
}

func nativeBufferBackend(b *testing.B) (*File, *nativeBufferIO) {
	b.Helper()
	f, err := New(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	stats := new(nativeBufferIO)
	open := f.open
	f.open = func() (vfs.FS, error) {
		r, err := open()
		if err != nil {
			return nil, err
		}
		return nativeBufferFS{FS: r, stats: stats}, nil
	}
	return f, stats
}

func appendBufferSIDX(dst []byte, i int) []byte {
	dst = binary.BigEndian.AppendUint64(dst, uint64(i>>16))
	dst = binary.BigEndian.AppendUint64(dst, uint64(i))
	return binary.BigEndian.AppendUint32(dst, uint32(i))
}

func appendBufferSMAX(dst []byte, i int) []byte {
	dst = binary.BigEndian.AppendUint64(dst, uint64(i>>16))
	dst = binary.BigEndian.AppendUint64(dst, uint64(i))
	return binary.BigEndian.AppendUint64(dst, uint64(i+1))
}

func appendBufferStats(dst []byte, i int) []byte {
	dst = binary.BigEndian.AppendUint64(dst, uint64(i>>16))
	dst = binary.BigEndian.AppendUint64(dst, uint64(i))
	dst = binary.AppendUvarint(dst, 1)
	dst = binary.BigEndian.AppendUint64(dst, uint64(i))
	dst = binary.BigEndian.AppendUint64(dst, uint64(i))
	return binary.BigEndian.AppendUint64(dst, uint64(i))
}

// Use a disk-backed TMPDIR. Counters measure VFS calls, not kernel syscalls.
func BenchmarkNativeSidecarBuffers(b *testing.B) {
	for _, count := range []int{512, 8192, 80000} {
		b.Run(fmt.Sprintf("series=%d", count), func(b *testing.B) {
			f, stats := nativeBufferBackend(b)
			be := backend.Cached(f, 8<<20)
			ctx := b.Context()
			header := []byte("sidecar-fixture")
			objects := []struct {
				key   string
				width int
				entry func([]byte, int) []byte
			}{
				{"part/sidx", 20, appendBufferSIDX},
				{"part/smax", 24, appendBufferSMAX},
				{"part/stats", 41, appendBufferStats},
			}
			write := func() {
				for _, obj := range objects {
					if err := sidecar.Write(ctx, be, obj.key, header, count, obj.width, obj.entry); err != nil {
						b.Fatal(err)
					}
				}
				if err := f.SyncPrefix(ctx, "part"); err != nil {
					b.Fatal(err)
				}
			}
			write()
			*stats = nativeBufferIO{}
			b.SetBytes(int64(count*85 + 3*(len(header)+4)))
			b.ReportAllocs()
			for b.Loop() {
				write()
			}
			b.ReportMetric(float64(stats.writeCalls)/float64(b.N), "file-writes/op")
			b.ReportMetric(float64(stats.writeBytes)/float64(stats.writeCalls), "B/file-write")
			b.ReportMetric(float64(stats.syncCalls)/float64(b.N), "file-syncs/op")
			b.ReportMetric(float64(stats.syncTime.Nanoseconds())/float64(b.N), "file-sync-ns/op")
			b.ReportMetric(float64(sidecar.ScratchBytes), "scratch-B")
			b.ReportMetric(float64(objectWriteBufferBytes), "file-buffer-B")
			for _, obj := range objects {
				got, err := f.Read(ctx, obj.key)
				if err != nil {
					b.Fatal(err)
				}
				if len(got) != len(header)+count*obj.width+4 {
					b.Fatal("unexpected object size")
				}
				if binary.BigEndian.Uint32(got[len(got)-4:]) != crc32.Checksum(got[:len(got)-4], crc32.MakeTable(crc32.Castagnoli)) {
					b.Fatal("invalid checksum")
				}
			}
		})
	}
}

func BenchmarkNativeSequentialReadBuffers(b *testing.B) {
	benchmarkNativeReadBuffers(b, false)
}

func BenchmarkNativeSeekReadBuffers(b *testing.B) {
	benchmarkNativeReadBuffers(b, true)
}

func benchmarkNativeReadBuffers(b *testing.B, seek bool) {
	b.Helper()
	const objectBytes = 16 << 20
	for _, readBytes := range []int{4 << 10, 64 << 10} {
		for _, buffered := range []int{0, 64 << 10, 128 << 10, 256 << 10, 1 << 20} {
			b.Run(fmt.Sprintf("read=%d/buffer=%d", readBytes, buffered), func(b *testing.B) {
				f, stats := nativeBufferBackend(b)
				payload := make([]byte, objectBytes)
				for i := range payload {
					payload[i] = byte(i*31 + i/4096)
				}
				if err := f.Write(b.Context(), "object", payload); err != nil {
					b.Fatal(err)
				}
				r, _, err := f.OpenReader(b.Context(), "object")
				if err != nil {
					b.Fatal(err)
				}
				b.Cleanup(func() { _ = r.Close() })
				var reader io.Reader = r
				var br *bufio.Reader
				if buffered > 0 {
					br = bufio.NewReaderSize(r, buffered)
					reader = br
				}
				buf := make([]byte, readBytes)
				reads := objectBytes / readBytes
				if seek {
					reads = 64
				}
				*stats = nativeBufferIO{}
				b.SetBytes(int64(reads * readBytes))
				b.ReportAllocs()
				for b.Loop() {
					if _, err := r.Seek(0, io.SeekStart); err != nil {
						b.Fatal(err)
					}
					if br != nil {
						br.Reset(r)
					}
					for i := range reads {
						offset := i * readBytes
						if seek {
							offset = ((i * 997) % (objectBytes / readBytes)) * readBytes
							if _, err := r.Seek(int64(offset), io.SeekStart); err != nil {
								b.Fatal(err)
							}
							if br != nil {
								br.Reset(r)
							}
						}
						if _, err := io.ReadFull(reader, buf); err != nil {
							b.Fatal(err)
						}
						if buf[0] != payload[offset] || buf[len(buf)-1] != payload[offset+readBytes-1] {
							b.Fatal("unexpected bytes")
						}
					}
				}
				b.ReportMetric(float64(stats.readCalls)/float64(b.N), "file-reads/op")
				b.ReportMetric(float64(stats.readBytes)/float64(stats.readCalls), "B/file-read")
				b.ReportMetric(float64(stats.readBytes)/float64(int64(b.N)*int64(reads*readBytes)), "read-amplification")
				b.ReportMetric(float64(buffered), "buffer-B")
			})
		}
	}
}
