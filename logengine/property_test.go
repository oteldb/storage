package logengine_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/logengine"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/log"
)

// TestFetchSupersetOfBruteForce is the contract property: for random streams + records and a
// random label matcher + window, the engine's Fetch returns exactly the records a brute-force
// scan would — a superset that, for the label+window filter of M8a, is exact. Some data is
// flushed to parts and some left in the head, so it exercises the merge path too.
func TestFetchSupersetOfBruteForce(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	rng := rand.New(rand.NewPCG(1, 2))

	services := []string{"api", "web", "db"}

	type record struct {
		svc  string
		ts   int64
		body string
	}

	all := make([]record, 0, 120)

	e := logengine.New(logengine.Config{Backend: backend.Memory(), Prefix: "t/logs"})

	// Ingest several rounds, flushing some so data lives in both parts and the head.
	for round := range 6 {
		ld := map[string]*log.ScopeLogs{}
		var batch log.Logs

		for range 20 {
			svc := services[rng.IntN(len(services))]
			ts := int64(rng.IntN(1000))
			body := fmt.Sprintf("msg-%d", rng.IntN(50))
			all = append(all, record{svc, ts, body})

			sl := ld[svc]
			if sl == nil {
				rl := batch.AddResource()
				rl.Resource = signal.Resource{Attributes: signal.NewAttributes(
					signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte(svc))},
				)}
				sl = rl.AddScope()
				ld[svc] = sl
			}

			r := sl.AddRecord()
			r.Timestamp, r.Body = ts, []byte(body)
		}

		ingest(t, e, batch)

		if round%2 == 1 {
			require.NoError(t, e.Flush(ctx))
		}
	}

	// Random query: one service, a random window.
	for range 20 {
		svc := services[rng.IntN(len(services))]
		lo := int64(rng.IntN(1000))
		hi := lo + int64(rng.IntN(500))

		got := fetchAll(t, e, fetch.Request{
			Start: lo, End: hi, Matchers: []fetch.Matcher{eqMatcher("service.name", svc)},
		})

		gotBodies := map[string]int{}
		for _, b := range got {
			for _, body := range bodies(b) {
				gotBodies[body]++
			}
		}

		wantBodies := map[string]int{}
		for _, rec := range all {
			if rec.svc == svc && rec.ts >= lo && rec.ts <= hi {
				wantBodies[rec.body]++
			}
		}

		require.Equalf(t, wantBodies, gotBodies, "service %q window [%d,%d]", svc, lo, hi)
	}
}
