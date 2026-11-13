package series

import (
	"bytes"
	"compress/gzip"
	_ "embed"
	"encoding/base64"
	"encoding/json"
	"io"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

//go:embed testdata/node-exporter-series.json.gz
var exporterFixture []byte

func exporterSeed(tb testing.TB) []signal.Series {
	tb.Helper()
	z, err := gzip.NewReader(bytes.NewReader(exporterFixture))
	require.NoError(tb, err)
	defer func() { require.NoError(tb, z.Close()) }()
	var fixture struct {
		Source string   `json:"source"`
		Series []string `json:"series"`
	}
	require.NoError(tb, json.NewDecoder(io.LimitReader(z, 2<<20)).Decode(&fixture))
	seed := make([]signal.Series, len(fixture.Series))
	for i, encoded := range fixture.Series {
		raw, err := base64.StdEncoding.DecodeString(encoded)
		require.NoError(tb, err)
		s, used, err := signal.DecodeSeries(raw)
		require.NoError(tb, err)
		require.Equal(tb, len(raw), used)
		seed[i] = s
	}
	return seed
}

func exporterInput(tb testing.TB, hosts int) []signal.Series {
	tb.Helper()
	seed := exporterSeed(tb)
	input := make([]signal.Series, 0, len(seed)*hosts)
	for host := range hosts {
		instance := signal.StringValue([]byte("node-" + strconv.Itoa(host) + ":9100"))
		for i := range seed {
			s := seed[i]
			s.Attributes = append(signal.Attributes(nil), s.Attributes...)
			for j := range s.Attributes {
				if bytes.Equal(s.Attributes[j].Key, []byte("instance")) {
					s.Attributes[j].Value = instance
				}
			}
			input = append(input, s)
		}
	}
	return input
}

func TestCapturedExporterDistribution(t *testing.T) {
	t.Parallel()
	seed := exporterSeed(t)
	require.Len(t, seed, 1658)
	metrics, schemas, cpus, devices, quantiles := map[string]bool{}, map[string]bool{}, map[string]bool{}, map[string]bool{}, map[string]bool{}
	for _, s := range seed {
		var schema []byte
		for _, kv := range s.Attributes {
			schema = append(schema, kv.Key...)
			schema = append(schema, 0, byte(kv.Value.Kind()))
			switch string(kv.Key) {
			case "__name__":
				metrics[string(kv.Value.Str())] = true
			case "cpu":
				cpus[string(kv.Value.Str())] = true
			case "device":
				devices[string(kv.Value.Str())] = true
			case "quantile":
				quantiles[string(kv.Value.Str())] = true
			}
		}
		schemas[string(schema)] = true
	}
	require.Len(t, metrics, 227)
	require.Len(t, schemas, 26)
	require.Greater(t, len(cpus), 1)
	require.Greater(t, len(devices), 1)
	require.Greater(t, len(quantiles), 1)
}
