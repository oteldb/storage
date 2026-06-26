package cluster

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net/http"
	"slices"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// The enumeration/resolution fan-out: a non-owner node serves a record signal's series listing
// (profile types / labels) and side store (the profiles symbol store, for stack resolution) from an
// owner over HTTP. Both reuse [EncodeFetchRequest] for the request (signal + tenant + window +
// equality matchers); they differ only in the response payload. A single owner is a complete replica,
// so the caller fails over between owners rather than merging.

// SeriesPath and SidePath are the HTTP paths of the series-listing and side-store servers.
const (
	SeriesPath = "/internal/series"
	SidePath   = "/internal/side"
)

// SeriesFunc lists the local store's stream identities for a tenant matching matchers within the
// window (a zero window disables the time filter).
type SeriesFunc func(ctx context.Context, tenant string, start, end int64, matchers []fetch.Matcher) ([]signal.Series, error)

// SideFunc returns the local store's side-store tables (name → encoded payload) for a tenant.
type SideFunc func(ctx context.Context, tenant string) (map[string][]byte, error)

// EncodeSeriesList serializes stream identities as length-prefixed reversible hash pre-images.
func EncodeSeriesList(series []signal.Series) []byte {
	buf := binary.AppendUvarint(nil, uint64(len(series)))
	for i := range series {
		enc := series[i].AppendHashInput(nil)
		buf = binary.AppendUvarint(buf, uint64(len(enc)))
		buf = append(buf, enc...)
	}

	return buf
}

// DecodeSeriesList parses [EncodeSeriesList] output.
func DecodeSeriesList(data []byte) ([]signal.Series, error) {
	count, m := binary.Uvarint(data)
	if m <= 0 || count > uint64(len(data)) { // each series needs ≥1 downstream byte
		return nil, errors.New("cluster: malformed series list")
	}

	data = data[m:]

	out := make([]signal.Series, 0, count)
	for range count {
		sl, m := binary.Uvarint(data)
		if m <= 0 || sl > uint64(len(data)-m) {
			return nil, errors.New("cluster: malformed series identity")
		}

		data = data[m:]

		s, _, err := signal.DecodeSeries(data[:sl])
		if err != nil {
			return nil, errors.Wrap(err, "decode series")
		}

		data = data[sl:]
		out = append(out, s)
	}

	return out, nil
}

// EncodeSideTables serializes a side-store table set (sorted by name for determinism).
func EncodeSideTables(tables map[string][]byte) []byte {
	names := make([]string, 0, len(tables))
	for name := range tables {
		names = append(names, name)
	}

	slices.Sort(names)

	buf := binary.AppendUvarint(nil, uint64(len(names)))
	for _, name := range names {
		buf = appendString(buf, name)
		buf = appendString(buf, string(tables[name]))
	}

	return buf
}

// DecodeSideTables parses [EncodeSideTables] output.
func DecodeSideTables(data []byte) (map[string][]byte, error) {
	count, m := binary.Uvarint(data)
	if m <= 0 || count > uint64(len(data)) { // each table needs ≥1 downstream byte
		return nil, errors.New("cluster: malformed side tables")
	}

	data = data[m:]

	out := make(map[string][]byte, count)
	for range count {
		name, rest, err := takeString(data)
		if err != nil {
			return nil, errors.Wrap(err, "table name")
		}

		payload, rest2, err := takeString(rest)
		if err != nil {
			return nil, errors.Wrap(err, "table payload")
		}

		out[name] = []byte(payload)
		data = rest2
	}

	return out, nil
}

// SeriesHandler serves [SeriesPath]: it reconstructs the pushed-down equality matchers and lists the
// matching stream identities via fn.
func SeriesHandler(fn SeriesFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		tenant, start, end, matchers, err := decodeEnumRequest(req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)

			return
		}

		series, err := fn(req.Context(), tenant, start, end, matchers)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)

			return
		}

		_, _ = w.Write(EncodeSeriesList(series))
	})
}

// SideHandler serves [SidePath]: it returns the tenant's side-store tables via fn.
func SideHandler(fn SideFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		tenant, _, _, _, err := decodeEnumRequest(req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)

			return
		}

		tables, err := fn(req.Context(), tenant)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)

			return
		}

		_, _ = w.Write(EncodeSideTables(tables))
	})
}

// decodeEnumRequest reads an [EncodeFetchRequest] body and reconstructs the tenant, window, and
// equality matchers (the signal is fixed by the handler the request reaches).
func decodeEnumRequest(req *http.Request) (tenant string, start, end int64, matchers []fetch.Matcher, err error) {
	if req.Method != http.MethodPost {
		return "", 0, 0, nil, errors.New("method not allowed")
	}

	body, err := io.ReadAll(req.Body)
	if err != nil {
		return "", 0, 0, nil, err
	}

	_, tenant, start, end, eq, err := DecodeFetchRequest(body)
	if err != nil {
		return "", 0, 0, nil, err
	}

	matchers = make([]fetch.Matcher, len(eq))
	for i := range eq {
		matchers[i] = fetch.Matcher{Name: []byte(eq[i].Name), Match: eq[i].Predicate(), Spec: &eq[i]}
	}

	return tenant, start, end, matchers, nil
}

// FetchSeries lists a peer's stream identities for the signal+tenant+window, pushing down the
// serializable (equality) matchers; the caller re-applies any non-equality matchers.
func FetchSeries(
	ctx context.Context, client *http.Client, addr string, sig signal.Signal,
	tenant string, start, end int64, eq []fetch.EqualMatcher,
) ([]signal.Series, error) {
	body, err := postEnum(ctx, client, addr, SeriesPath, EncodeFetchRequest(sig, tenant, start, end, eq))
	if err != nil {
		return nil, err
	}

	return DecodeSeriesList(body)
}

// FetchSide returns a peer's side-store tables for the signal+tenant.
func FetchSide(ctx context.Context, client *http.Client, addr string, sig signal.Signal, tenant string) (map[string][]byte, error) {
	body, err := postEnum(ctx, client, addr, SidePath, EncodeFetchRequest(sig, tenant, 0, 0, nil))
	if err != nil {
		return nil, err
	}

	return DecodeSideTables(body)
}

func postEnum(ctx context.Context, client *http.Client, addr, path string, payload []byte) ([]byte, error) {
	if client == nil {
		client = http.DefaultClient
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+addr+path, bytes.NewReader(payload))
	if err != nil {
		return nil, errors.Wrap(err, "build request")
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, errors.Wrapf(err, "request to %q", addr)
	}

	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "read response")
	}

	if resp.StatusCode != http.StatusOK {
		return nil, errors.Errorf("cluster: %q returned %d: %s", addr, resp.StatusCode, bytes.TrimSpace(body))
	}

	return body, nil
}
