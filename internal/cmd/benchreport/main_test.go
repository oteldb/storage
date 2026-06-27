package main

import (
	"strings"
	"testing"
)

// fixtureCSV is benchstat -format csv output (n=6) where write/head regresses on latency and
// read/fetch_all + density improve. Throughput (B/s, Mpoints/s) mirrors the latency moves.
const fixtureCSV = `goos: linux
goarch: amd64
pkg: github.com/oteldb/storage
cpu: AMD EPYC 7763 64-Core Processor
,base.txt,,head.txt,,,
,sec/op,CI,sec/op,CI,vs base,P
Golden/write/head-4,1e-06,0%,1.3e-06,0%,+30.00%,p=0.002 n=6
Golden/read/fetch_all-4,2e-06,0%,1.6e-06,0%,-20.00%,p=0.002 n=6
geomean,1.414e-06,,1.442e-06,,+1.98%,

,base.txt,,head.txt,,,
,Mpoints/s,CI,Mpoints/s,CI,vs base,P
Golden/write/head-4,5,0%,3.85,0%,-23.00%,p=0.002 n=6
geomean,5,,3.85,,-23.00%,

,base.txt,,head.txt,,,
,allocs/op,CI,allocs/op,CI,vs base,P
Golden/write/head-4,2,0%,2,0%,~,p=1.000 n=6
Golden/read/fetch_all-4,4,0%,3,0%,-25.00%,p=0.002 n=6
geomean,2.82,,2.44,,-13.40%,

,base.txt,,head.txt,,,
,B/point,CI,B/point,CI,vs base,P
Golden/density-4,1.5,0%,1.2,0%,-20.00%,p=0.002 n=6
geomean,1.5,,1.2,,-20.00%,
`

func TestRenderVerdictAndTables(t *testing.T) {
	t.Parallel()

	out, err := render(strings.NewReader(fixtureCSV), "RAW-TABLE-PLACEHOLDER")
	if err != nil {
		t.Fatal(err)
	}

	want := []string{
		marker,
		"> [!WARNING]",
		// Distinct-benchmark counting (not per-metric): 1 regressed (write/head), 2 improved.
		"**1 benchmark regressed** and **2 improved**",
		"Largest regression: `write/head` latency **+30.0%**",
		"### ⏱️ Time — `sec/op` (lower is better)",
		"| `write/head` | 1.00µs | 1.30µs | **+30.0%** | 🔴 |",
		"| `read/fetch_all` | 2.00µs | 1.60µs | **−20.0%** | 🟢 |",
		"### 🗜️ Density — `B/point` (lower is better)",
		"| `density` | 1.500 | 1.200 | **−20.0%** | 🟢 |",
		"n=6/benchmark",
		"RAW-TABLE-PLACEHOLDER",
	}
	for _, w := range want {
		if !strings.Contains(out, w) {
			t.Errorf("report missing %q\n--- got ---\n%s", w, out)
		}
	}

	// allocs/op write/head is "~" (not significant) ⇒ neutral, must not be flagged.
	if strings.Contains(out, "| `write/head` | 2 | 2 | **") {
		t.Error("non-significant allocs row should not be bolded/flagged")
	}
}

func TestRenderNoChange(t *testing.T) {
	t.Parallel()

	csv := `goos: linux
goarch: amd64
,base.txt,,head.txt,,,
,sec/op,CI,sec/op,CI,vs base,P
Golden/write/head-4,1e-06,0%,1e-06,0%,~,p=1.000 n=6
geomean,1e-06,,1e-06,,~,
`
	out, err := render(strings.NewReader(csv), "")
	if err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(out, "No statistically significant change") {
		t.Errorf("expected a no-change verdict, got:\n%s", out)
	}
	if !strings.Contains(out, "[!NOTE]") {
		t.Error("no-change verdict should use a NOTE alert")
	}
}

func TestRenderEmpty(t *testing.T) {
	t.Parallel()

	out, err := render(strings.NewReader(""), "")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "[!CAUTION]") {
		t.Errorf("empty input should yield a CAUTION verdict, got:\n%s", out)
	}
}
