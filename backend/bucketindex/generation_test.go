package bucketindex_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/oteldb/storage/backend/bucketindex"
)

func gen(term, counter uint64) bucketindex.Generation {
	return bucketindex.Generation{Term: term, Counter: counter}
}

func TestGenerationCompare(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name string
		a, b bucketindex.Generation
		want int
	}{
		{"Equal", gen(3, 7), gen(3, 7), 0},
		{"CounterAhead", gen(3, 8), gen(3, 7), 1},
		{"CounterBehind", gen(3, 6), gen(3, 7), -1},
		// The term dominates: a writer that reacquired the shard supersedes whatever the
		// previous one wrote, however far its counter had run.
		{"TermBeatsCounter", gen(4, 1), gen(3, 9999), 1},
		{"LowerTermLoses", gen(3, 9999), gen(4, 1), -1},
		// An index written before v3 carries no generation and loses to every real one, so the
		// first write that carries one supersedes it.
		{"ZeroLosesToAny", gen(0, 0), gen(0, 1), -1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.a.Compare(tt.b))
			assert.Equal(t, -tt.want, tt.b.Compare(tt.a))
		})
	}
}

func TestGenerationNext(t *testing.T) {
	t.Parallel()

	// Within a term, the counter advances.
	assert.Equal(t, gen(3, 8), gen(3, 7).Next(3))

	// A new term restarts it: the term already orders this writer above everything the previous
	// one wrote, so the counter is its own sequence and starts at one.
	assert.Equal(t, gen(4, 1), gen(3, 7).Next(4))

	// A superseded writer that has not noticed keeps its own state monotonic; the term is what
	// keeps its writes from being adopted, not a frozen counter.
	assert.Equal(t, gen(3, 8), gen(3, 7).Next(2))

	// No term at all (single node) still gives a monotonic sequence.
	assert.Equal(t, gen(0, 1), bucketindex.Generation{}.Next(0))
	assert.True(t, bucketindex.Generation{}.Zero())
	assert.False(t, gen(0, 1).Zero())
}
