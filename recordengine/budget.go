package recordengine

import (
	"github.com/oteldb/storage/query/fetch"
)

// budgetedIterator keeps accumulated result reservations until the consumer closes.
type budgetedIterator struct {
	fetch.Iterator

	release func()
}

func (it *budgetedIterator) Close() error {
	it.release()

	return it.Iterator.Close()
}
