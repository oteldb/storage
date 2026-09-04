package recordengine

import (
	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
)

// partGone reports whether err is the backend saying the object is not there, as opposed to
// saying nothing at all. Only that answer may turn an index entry into a repair obligation:
// every other failure — a timeout, a canceled context, a full disk, a denied request, a
// throttled bucket — leaves the part's existence unknown, and treating one as loss would strip
// live parts out of the index, shard-wide when the backend is shared.
func partGone(err error) bool { return errors.Is(err, backend.ErrNotExist) }
