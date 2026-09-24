package enginetest

import (
	"context"
	"slices"
	"sync"

	"github.com/oteldb/storage/backend/bucketindex"
)

// FetchResult mirrors the engines' identical FetchResult; an adapter converts it field for field.
type FetchResult struct {
	Entry   bucketindex.Entry
	Outcome bucketindex.WantOutcome
	Err     error
}

// PartFetcher mirrors the engines' PartFetcher over [FetchResult]; an adapter bridges it.
type PartFetcher interface {
	FetchWants(ctx context.Context, wants []bucketindex.Want) []FetchResult
}

// Answer is how a [Fetcher] answers one want.
type Answer func(w bucketindex.Want) (bucketindex.Entry, bucketindex.WantOutcome, error)

// Fetcher stands in for the cluster layer's part-sync pull: it records what repair asked for and
// answers with Answer, so the engine half of the seam runs without a peer, a transport or a syncer.
// A nil Answer reports every want absent.
type Fetcher struct {
	mu     sync.Mutex
	asked  []string
	calls  int
	answer Answer
}

// NewFetcher returns a [Fetcher] answering with a.
func NewFetcher(a Answer) *Fetcher { return &Fetcher{answer: a} }

// AnswerAlways returns a [Fetcher] giving every want the same outcome.
func AnswerAlways(outcome bucketindex.WantOutcome, err error) *Fetcher {
	return NewFetcher(func(bucketindex.Want) (bucketindex.Entry, bucketindex.WantOutcome, error) {
		return bucketindex.Entry{}, outcome, err
	})
}

// SetAnswer replaces the answer for later fetches.
func (f *Fetcher) SetAnswer(a Answer) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.answer = a
}

// FetchWants implements [PartFetcher].
func (f *Fetcher) FetchWants(_ context.Context, wants []bucketindex.Want) []FetchResult {
	f.mu.Lock()
	f.calls++

	for i := range wants {
		f.asked = append(f.asked, wants[i].Prefix)
	}

	answer := f.answer
	f.mu.Unlock()

	out := make([]FetchResult, len(wants))

	for i := range wants {
		if answer == nil {
			out[i].Outcome = bucketindex.WantAbsent

			continue
		}

		ent, outcome, err := answer(wants[i])
		out[i] = FetchResult{Entry: ent, Outcome: outcome, Err: err}
	}

	return out
}

// Asks reports what repair has asked for, in call order.
func (f *Fetcher) Asks() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return slices.Clone(f.asked)
}

// Calls reports how many batches repair issued: one per cycle, whatever the want count.
func (f *Fetcher) Calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.calls
}
