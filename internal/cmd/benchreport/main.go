// Command benchreport turns `benchstat -format csv` (read from stdin) into a rich,
// GitHub-flavored Markdown report: a generalized verdict alert, per-metric tables with
// direction-aware deltas and status emoji, and an optional collapsible raw table. It is used by
// .github/workflows/bench.yml to comment golden-benchmark results on a PR.
//
// Usage:
//
//	benchstat -format csv base.txt head.txt | benchreport -raw <(benchstat base.txt head.txt) > report.md
package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"slices"
	"strconv"
	"strings"
)

func main() {
	raw := flag.String("raw", "", "path to a plain-text benchstat table to embed in a <details> block")
	flag.Parse()

	var rawText string
	if *raw != "" {
		b, err := os.ReadFile(*raw)
		if err != nil {
			fmt.Fprintln(os.Stderr, "benchreport: read -raw:", err)
			os.Exit(1)
		}
		rawText = string(b)
	}

	md, err := render(os.Stdin, rawText)
	if err != nil {
		fmt.Fprintln(os.Stderr, "benchreport:", err)
		os.Exit(1)
	}

	fmt.Print(md)
}

// marker is the hidden HTML comment the PR workflow uses to find and update its sticky comment.
const marker = "<!-- golden-bench -->"

type benchRow struct {
	name        string
	base, head  float64
	hasHead     bool
	significant bool    // benchstat marked the change significant (the "vs base" column is a %, not "~")
	pct         float64 // (head-base)/base*100, computed from the raw values
}

type group struct {
	unit    string
	rows    []benchRow
	geomean string // the geomean "vs base" cell (a signed %), when present
}

type report struct {
	meta    []string // goos/goarch/pkg/cpu lines from the benchstat header
	groups  []group
	twoFile bool
	n       int // samples per benchmark, parsed from the "n=" suffix
}

func render(r io.Reader, rawText string) (string, error) {
	rep, err := parse(r)
	if err != nil {
		return "", err
	}

	var b strings.Builder
	b.WriteString(marker)
	b.WriteString("\n## 📊 Golden benchmarks\n\n")

	if len(rep.groups) == 0 {
		b.WriteString("> [!CAUTION]\n> No benchmark results were parsed — the benchmark step may have failed.\n")
		return b.String(), nil
	}

	writeVerdict(&b, rep)
	writeMeta(&b, rep)

	for _, g := range rep.groups {
		writeGroup(&b, g)
	}

	if strings.TrimSpace(rawText) != "" {
		b.WriteString("\n<details><summary>Raw <code>benchstat</code> table</summary>\n\n```\n")
		b.WriteString(strings.TrimRight(rawText, "\n"))
		b.WriteString("\n```\n</details>\n")
	}

	return b.String(), nil
}

// parse reads benchstat CSV into a report.
func parse(r io.Reader) (report, error) {
	cr := csv.NewReader(r)
	cr.FieldsPerRecord = -1

	var (
		rep report
		cur *group
	)

	for {
		rec, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return rep, err
		}

		switch {
		case len(rec) == 1: // header metadata: "goos: linux", "cpu: ...", etc.
			if s := strings.TrimSpace(rec[0]); s != "" {
				rep.meta = append(rep.meta, s)
			}
		case rec[0] == "" && len(rec) >= 3 && rec[2] == "CI": // unit header: ,unit,CI,[unit,CI,vs base,P]
			rep.groups = append(rep.groups, group{unit: rec[1]})
			cur = &rep.groups[len(rep.groups)-1]
			rep.twoFile = containsVsBase(rec)
		case rec[0] == "": // file-label row (,base.txt,,head.txt,...) — skip
			continue
		case rec[0] == "geomean":
			if cur != nil && rep.twoFile && len(rec) >= 6 {
				cur.geomean = rec[5]
			}
		default:
			if cur == nil {
				continue
			}
			row := benchRow{name: rec[0]}
			row.base, _ = strconv.ParseFloat(rec[1], 64)
			if rep.twoFile && len(rec) >= 7 {
				row.head, _ = strconv.ParseFloat(rec[3], 64)
				row.hasHead = true
				row.significant = strings.Contains(rec[5], "%")
				if row.base != 0 {
					row.pct = (row.head - row.base) / row.base * 100
				}
				if rep.n == 0 {
					rep.n = parseN(rec[6])
				}
			}
			cur.rows = append(cur.rows, row)
		}
	}

	return rep, nil
}

func containsVsBase(rec []string) bool {
	return slices.Contains(rec, "vs base")
}

func parseN(p string) int {
	if _, after, ok := strings.Cut(p, "n="); ok {
		n, _ := strconv.Atoi(strings.TrimSpace(after))
		return n
	}
	return 0
}

// direction reports whether lower (-1) or higher (+1) is better for a unit, or neutral (0).
func direction(unit string) int {
	switch {
	case unit == "rows/op": // a workload-invariant count, not a perf metric
		return 0
	case strings.HasSuffix(unit, "/op"), unit == "B/point":
		return -1
	case strings.HasSuffix(unit, "/s"):
		return 1
	default:
		return 0
	}
}

func isImprovement(r benchRow, unit string) bool {
	d := direction(unit)
	return r.significant && ((d < 0 && r.pct < 0) || (d > 0 && r.pct > 0))
}

func isRegression(r benchRow, unit string) bool {
	d := direction(unit)
	return r.significant && ((d < 0 && r.pct > 0) || (d > 0 && r.pct < 0))
}

func writeVerdict(b *strings.Builder, rep report) {
	if !rep.twoFile {
		b.WriteString("> [!NOTE]\n> No base revision to compare against — showing this revision's numbers only.\n\n")
		return
	}

	// Count DISTINCT benchmarks over the primary, lower-is-better metrics only (sec/op, B/op,
	// allocs/op, B/point). Throughput (B/s, Mpoints/s) is the inverse of latency, so counting it
	// too would triple-count a single physical regression; it is still shown in its own table.
	regressed, improved := map[string]bool{}, map[string]bool{}
	var worstReg, bestImp benchRow
	var worstUnit, bestUnit string
	var rowsChanged int

	for _, g := range rep.groups {
		if g.unit == "rows/op" {
			for _, r := range g.rows {
				if r.significant {
					rowsChanged++
				}
			}
			continue
		}
		if direction(g.unit) != -1 { // skip throughput / informational for the tally
			continue
		}
		for _, r := range g.rows {
			switch {
			case isRegression(r, g.unit):
				regressed[short(r.name)] = true
				if math.Abs(r.pct) > math.Abs(worstReg.pct) {
					worstReg, worstUnit = r, g.unit
				}
			case isImprovement(r, g.unit):
				improved[short(r.name)] = true
				if math.Abs(r.pct) > math.Abs(bestImp.pct) {
					bestImp, bestUnit = r, g.unit
				}
			}
		}
	}

	switch {
	case len(regressed) > 0:
		fmt.Fprintf(b, "> [!WARNING]\n> **%s** and **%s** across the golden set.\n",
			plural(len(regressed), "benchmark regressed", "benchmarks regressed"),
			plural(len(improved), "improved", "improved"))
		fmt.Fprintf(b, "> Largest regression: `%s` %s **%s**.\n", short(worstReg.name), metricWord(worstUnit), signedPct(worstReg.pct))
	case len(improved) > 0:
		fmt.Fprintf(b, "> [!TIP]\n> **%s**, no regressions. 🎉\n",
			plural(len(improved), "benchmark improved", "benchmarks improved"))
		fmt.Fprintf(b, "> Largest improvement: `%s` %s **%s**.\n", short(bestImp.name), metricWord(bestUnit), signedPct(bestImp.pct))
	default:
		b.WriteString("> [!NOTE]\n> **No statistically significant change.** Performance is on par with the base revision.\n")
	}

	if rowsChanged > 0 {
		fmt.Fprintf(b, ">\n> ⚠️ `rows/op` changed on %s — the benchmark scanned a different number of rows, so the comparison may not be apples-to-apples.\n", plural(rowsChanged, "benchmark", "benchmarks"))
	}

	if rep.n > 0 && rep.n < 6 {
		fmt.Fprintf(b, ">\n> ℹ️ Only `n=%d` samples/benchmark — below benchstat's 6-sample threshold for confidence intervals, so deltas read `~` (not significant) even when large. Treat as indicative.\n", rep.n)
	} else if noisyButInsignificant(rep) {
		b.WriteString(">\n> ℹ️ Some deltas are large but not statistically significant — likely shared-runner noise. Trust repeated, consistent runs.\n")
	}

	b.WriteString("\n")
}

// noisyButInsignificant reports whether any non-significant row moved >15% — a hint of noise.
func noisyButInsignificant(rep report) bool {
	for _, g := range rep.groups {
		for _, r := range g.rows {
			if r.hasHead && !r.significant && math.Abs(r.pct) > 15 {
				return true
			}
		}
	}
	return false
}

func writeMeta(b *strings.Builder, rep report) {
	var plat, cpu string
	var goos, goarch string
	for _, m := range rep.meta {
		switch {
		case strings.HasPrefix(m, "goos:"):
			goos = strings.TrimSpace(strings.TrimPrefix(m, "goos:"))
		case strings.HasPrefix(m, "goarch:"):
			goarch = strings.TrimSpace(strings.TrimPrefix(m, "goarch:"))
		case strings.HasPrefix(m, "cpu:"):
			cpu = strings.TrimSpace(strings.TrimPrefix(m, "cpu:"))
		}
	}
	if goos != "" {
		plat = goos + "/" + goarch
	}

	parts := []string{"`benchstat base → head`"}
	if plat != "" {
		parts = append(parts, plat)
	}
	if cpu != "" {
		parts = append(parts, cpu)
	}
	if rep.n > 0 {
		parts = append(parts, fmt.Sprintf("n=%d/benchmark", rep.n))
	}
	parts = append(parts, "`~` = not significant (p≥0.05)")

	fmt.Fprintf(b, "<sub>%s</sub>\n\n", strings.Join(parts, " · "))
}

func writeGroup(b *strings.Builder, g group) {
	emoji, label := unitTitle(g.unit)
	better := "lower is better"
	switch direction(g.unit) {
	case 1:
		better = "higher is better"
	case 0:
		better = "informational"
	}

	fmt.Fprintf(b, "### %s %s — `%s` (%s)\n\n", emoji, label, g.unit, better)
	b.WriteString("| Benchmark | base | head | Δ | |\n|:--|--:|--:|--:|:-:|\n")

	for _, r := range g.rows {
		base := fmtVal(g.unit, r.base)
		head, delta, status := "—", "·", "⚪"
		if r.hasHead {
			head = fmtVal(g.unit, r.head)
			d := signedPct(r.pct)
			switch {
			case isRegression(r, g.unit):
				delta, status = "**"+d+"**", "🔴"
			case isImprovement(r, g.unit):
				delta, status = "**"+d+"**", "🟢"
			default:
				delta = d // not significant: plain
			}
		}
		fmt.Fprintf(b, "| `%s` | %s | %s | %s | %s |\n", short(r.name), base, head, delta, status)
	}

	if g.geomean != "" && g.geomean != "~" {
		fmt.Fprintf(b, "| _geomean_ | | | _%s_ | |\n", g.geomean)
	}

	b.WriteString("\n")
}

func unitTitle(unit string) (emoji, label string) {
	switch unit {
	case "sec/op":
		return "⏱️", "Time"
	case "B/s", "MB/s", "Mpoints/s":
		return "🚀", "Throughput"
	case "B/op":
		return "📦", "Bytes per op"
	case "allocs/op":
		return "♻️", "Allocations"
	case "B/point":
		return "🗜️", "Density"
	case "rows/op":
		return "🔢", "Rows per op"
	default:
		return "📊", unit
	}
}

func metricWord(unit string) string {
	switch unit {
	case "sec/op":
		return "latency"
	case "B/s", "MB/s", "Mpoints/s":
		return "throughput"
	case "B/op":
		return "bytes/op"
	case "allocs/op":
		return "allocs"
	case "B/point":
		return "density"
	default:
		return unit
	}
}

// short trims the redundant "Golden/" prefix and the "-N" GOMAXPROCS suffix from a benchmark name.
func short(name string) string {
	name = strings.TrimPrefix(name, "Golden/")
	if i := strings.LastIndex(name, "-"); i >= 0 {
		if _, err := strconv.Atoi(name[i+1:]); err == nil {
			name = name[:i]
		}
	}
	return name
}

func plural(n int, one, many string) string {
	if n == 1 {
		return "1 " + one
	}
	return strconv.Itoa(n) + " " + many
}

// signedPct formats a percentage with a sign and a real Unicode minus for negatives.
func signedPct(p float64) string {
	if math.Abs(p) < 0.05 {
		return "≈0%"
	}
	if p < 0 {
		return fmt.Sprintf("−%.1f%%", -p) // U+2212 minus
	}
	return fmt.Sprintf("+%.1f%%", p)
}

// fmtVal renders a benchstat value in human units per the metric.
func fmtVal(unit string, v float64) string {
	switch {
	case unit == "sec/op":
		return fmtDuration(v)
	case unit == "B/s":
		return fmtBytes(v) + "/s"
	case unit == "B/op":
		return fmtBytes(v)
	case unit == "B/point":
		return fmt.Sprintf("%.3f", v)
	case unit == "allocs/op":
		return fmtCount(v)
	case unit == "rows/op":
		return fmtCount(v)
	case strings.HasSuffix(unit, "/s"): // Mpoints/s, MB/s, …
		return trimFloat(v)
	default:
		return trimFloat(v)
	}
}

func fmtDuration(sec float64) string {
	ns := sec * 1e9
	switch {
	case ns < 1e3:
		return fmt.Sprintf("%.1fns", ns)
	case ns < 1e6:
		return fmt.Sprintf("%.2fµs", ns/1e3)
	case ns < 1e9:
		return fmt.Sprintf("%.2fms", ns/1e6)
	default:
		return fmt.Sprintf("%.2fs", sec)
	}
}

func fmtBytes(b float64) string {
	const k = 1024
	switch {
	case b < k:
		return fmt.Sprintf("%.0fB", b)
	case b < k*k:
		return fmt.Sprintf("%.1fKiB", b/k)
	case b < k*k*k:
		return fmt.Sprintf("%.1fMiB", b/(k*k))
	default:
		return fmt.Sprintf("%.2fGiB", b/(k*k*k))
	}
}

func fmtCount(v float64) string {
	switch {
	case v < 1e3:
		return trimFloat(v)
	case v < 1e6:
		return fmt.Sprintf("%.1fk", v/1e3)
	default:
		return fmt.Sprintf("%.2fM", v/1e6)
	}
}

// trimFloat prints up to 3 significant digits without trailing-zero noise.
func trimFloat(v float64) string {
	s := strconv.FormatFloat(v, 'g', 3, 64)
	return s
}
