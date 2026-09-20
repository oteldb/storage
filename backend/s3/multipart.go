package s3

import (
	"context"
	"time"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
)

// MultipartObjectStore is an optional [ObjectStore] capability: assemble one object from parts
// uploaded separately. It is what makes [backend.ObjectCreator] available over S3, so a merge
// producing a part column far larger than it wants resident hands finished bytes to the store as
// they are produced instead of holding the whole object.
//
// It is optional for the same reason [RangeObjectStore] is: every real S3-compatible store has it,
// but an embedder's narrow fake need not. A store without it makes the [Backend] buffer whole
// objects, which [backend.StreamsWrites] reports.
//
// Enabling it carries an operational precondition: a crashed writer leaves an incomplete upload,
// which is not an object — it appears in no listing, so no orphan sweep can ever reclaim it. The
// bucket needs an AbortIncompleteMultipartUpload lifecycle rule (see ADMIN.md).
type MultipartObjectStore interface {
	// CreateMultipartUpload starts an upload for key and returns its id. Nothing is visible under
	// key until CompleteMultipartUpload.
	CreateMultipartUpload(ctx context.Context, key string) (uploadID string, err error)

	// UploadPart stores data as the upload's partNum-th part (1-based) and returns its ETag.
	// Uploading the same part number again replaces it, so a retry by part number is safe.
	UploadPart(ctx context.Context, key, uploadID string, partNum int32, data []byte) (etag string, err error)

	// CompleteMultipartUpload publishes the parts, in etags order, as the object under key.
	CompleteMultipartUpload(ctx context.Context, key, uploadID string, etags []string) error

	// AbortMultipartUpload discards the upload and its parts. It is idempotent.
	AbortMultipartUpload(ctx context.Context, key, uploadID string) error
}

// uploadPartBytes is how much the writer accumulates before it starts an upload at all, and the
// size of every part but the last. S3's minimum part size is 5 MiB for every part but the last, so
// anything smaller cannot be a part; the margin above it keeps a large column's part count well
// inside the 10,000-part limit. Everything a part writes that is smaller — marks, manifest,
// watermark, small columns — therefore commits as a single PutObject and starts no upload at all.
const uploadPartBytes = 8 << 20

// abortTimeout bounds the abort of an upload whose caller has already given up. [ObjectWriter.Abort]
// reports nothing and is usually reached from a failing path, so it must not block on a store that
// has stopped answering.
const abortTimeout = 30 * time.Second

var _ backend.ObjectCreator = (*Backend)(nil)

// CreateObject returns a writer that uploads key's object in parts once it exceeds
// [uploadPartBytes], and issues a single PutObject below that — which is also what it does for the
// whole object when the store cannot upload in parts at all. Implements [backend.ObjectCreator].
func (b *Backend) CreateObject(ctx context.Context, key string) (backend.ObjectWriter, error) {
	return &objectWriter{ctx: ctx, store: b.store, mp: b.mp, key: b.key(key), name: key}, nil
}

// StreamsWrites reports whether the store can actually upload in parts. Without it the writer above
// still works — it just holds the object, which is what a caller sizing its output against memory
// needs to know. Implements [backend.ObjectCreator].
func (b *Backend) StreamsWrites() bool { return b.mp != nil }

// objectWriter builds one object out of multipart uploads. Bytes accumulate until a whole part is
// due, so an object that never reaches [uploadPartBytes] — marks, manifests, watermarks, small
// columns — creates no upload at all and commits as a plain put. A nil mp is the same path for
// every size: the store cannot upload in parts, so the object is held and put whole, which is what
// [Backend.StreamsWrites] reports.
type objectWriter struct {
	ctx   context.Context //nolint:containedctx // Abort takes none, and it must still reach the store
	store ObjectStore
	mp    MultipartObjectStore

	key  string // store key, root prefix included
	name string // backend key, for error messages

	buf      []byte
	uploadID string
	etags    []string
	done     bool
}

var _ backend.ObjectWriter = (*objectWriter)(nil)

func (w *objectWriter) Write(p []byte) (int, error) {
	if w.done {
		return 0, errors.New("backend/s3: write after commit or abort")
	}

	w.buf = append(w.buf, p...)
	if w.mp != nil && len(w.buf) >= uploadPartBytes {
		if err := w.flushPart(w.ctx); err != nil {
			return 0, err
		}
	}

	return len(p), nil
}

// Commit publishes everything written so far under the writer's key. Implements
// [backend.ObjectWriter].
func (w *objectWriter) Commit(ctx context.Context) error {
	if w.done {
		return errors.New("backend/s3: commit after commit or abort")
	}

	if w.uploadID == "" {
		if err := w.store.PutObject(ctx, w.key, w.buf); err != nil {
			return errors.Wrapf(err, "put %q", w.name)
		}

		w.finish()

		return nil
	}

	// The last part is the one S3 exempts from the minimum size, so whatever is left goes as-is.
	if len(w.buf) > 0 {
		if err := w.flushPart(ctx); err != nil {
			return err
		}
	}

	if err := w.mp.CompleteMultipartUpload(ctx, w.key, w.uploadID, w.etags); err != nil {
		return errors.Wrapf(err, "complete multipart upload %q", w.name)
	}

	w.finish()

	return nil
}

// Abort discards the writer's bytes and the upload behind them. Implements [backend.ObjectWriter].
func (w *objectWriter) Abort() {
	if w.done || w.uploadID == "" {
		w.finish()

		return
	}

	// An abort usually follows a cancellation, so the writer's own context is already dead. Without
	// stripping it the request never reaches the store, and the uploaded parts are billed until the
	// bucket's lifecycle rule reaps them — nothing else can, since an incomplete upload is not an
	// object and appears in no listing. There is nowhere to report a failure here; the lifecycle
	// rule is the backstop, which is why ADMIN.md makes it a precondition.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(w.ctx), abortTimeout)
	defer cancel()

	_ = w.mp.AbortMultipartUpload(ctx, w.key, w.uploadID)

	w.finish()
}

func (w *objectWriter) finish() {
	w.done, w.buf, w.uploadID, w.etags = true, nil, "", nil
}

func (w *objectWriter) flushPart(ctx context.Context) error {
	if w.uploadID == "" {
		id, err := w.mp.CreateMultipartUpload(ctx, w.key)
		if err != nil {
			return errors.Wrapf(err, "create multipart upload %q", w.name)
		}

		if id == "" {
			return errors.Errorf("store reported no upload id for %q", w.name)
		}

		w.uploadID = id
	}

	num := int32(len(w.etags) + 1)

	etag, err := w.mp.UploadPart(ctx, w.key, w.uploadID, num, w.buf)
	if err != nil {
		return errors.Wrapf(err, "upload part %d of %q", num, w.name)
	}

	// Completing an upload names each part by its ETag; an empty one would be rejected there, far
	// from the part that produced it.
	if etag == "" {
		return errors.Errorf("store reported no ETag for part %d of %q", num, w.name)
	}

	w.etags = append(w.etags, etag)
	w.buf = w.buf[:0]

	return nil
}
