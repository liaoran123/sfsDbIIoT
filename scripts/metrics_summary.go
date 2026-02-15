package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"sort"
	"strconv"
	"time"
)

type MemSample struct {
	Time         string `json:"time"`
	NumGoroutine int    `json:"num_goroutine"`
	Alloc        uint64 `json:"alloc"`
	TotalAlloc   uint64 `json:"total_alloc"`
	Sys          uint64 `json:"sys"`
	HeapAlloc    uint64 `json:"heap_alloc"`
	HeapSys      uint64 `json:"heap_sys"`
	NumGC        uint32 `json:"num_gc"`
	PauseTotalNs uint64 `json:"pause_total_ns"`
}

func readSamples(path string) ([]MemSample, error) {
	b, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var s []MemSample
	if err := json.Unmarshal(b, &s); err != nil {
		return nil, err
	}
	return s, nil
}

func float64SliceFromUint64(a []uint64) []float64 {
	out := make([]float64, len(a))
	for i, v := range a {
		out[i] = float64(v)
	}
	return out
}

func percentile(sorted []float64, q float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	if q <= 0 {
		return sorted[0]
	}
	if q >= 1 {
		return sorted[len(sorted)-1]
	}
	// Nearest-rank method
	rank := int(math.Ceil(q*float64(len(sorted)))) - 1
	if rank < 0 {
		rank = 0
	}
	if rank >= len(sorted) {
		rank = len(sorted) - 1
	}
	return sorted[rank]
}

func summaryStatsFloats(a []float64) (min, max, mean, p50, p95, p99 float64) {
	if len(a) == 0 {
		return
	}
	sort.Float64s(a)
	min = a[0]
	max = a[len(a)-1]
	var sum float64
	for _, v := range a {
		sum += v
	}
	mean = sum / float64(len(a))
	p50 = percentile(a, 0.50)
	p95 = percentile(a, 0.95)
	p99 = percentile(a, 0.99)
	return
}

func writeCSV(samples []MemSample, path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	w.Write([]string{"time", "num_goroutine", "alloc", "heap_alloc", "num_gc", "pause_total_ns"})
	for _, s := range samples {
		w.Write([]string{s.Time, strconv.Itoa(s.NumGoroutine), strconv.FormatUint(s.Alloc, 10), strconv.FormatUint(s.HeapAlloc, 10), strconv.FormatUint(uint64(s.NumGC), 10), strconv.FormatUint(s.PauseTotalNs, 10)})
	}
	return nil
}

func formatBytes(f float64) string {
	units := []string{"B", "KB", "MB", "GB"}
	i := 0
	for f >= 1024 && i < len(units)-1 {
		f /= 1024
		i++
	}
	return fmt.Sprintf("%.2f %s", f, units[i])
}

func main() {
	jsonPath := "bench_sustained_metrics_round2.json"
	if len(os.Args) > 1 {
		jsonPath = os.Args[1]
	}
	samples, err := readSamples(jsonPath)
	if err != nil {
		fmt.Printf("failed to read samples: %v\n", err)
		os.Exit(1)
	}
	if len(samples) == 0 {
		fmt.Println("no samples found")
		os.Exit(0)
	}

	// Build arrays
	allocs := make([]float64, len(samples))
	heapAllocs := make([]float64, len(samples))
	numG := make([]float64, len(samples))
	pauseNs := make([]float64, len(samples))

	for i, s := range samples {
		allocs[i] = float64(s.Alloc)
		heapAllocs[i] = float64(s.HeapAlloc)
		numG[i] = float64(s.NumGoroutine)
		pauseNs[i] = float64(s.PauseTotalNs)
	}

	amin, amax, amean, ap50, ap95, ap99 := summaryStatsFloats(allocs)
	hmin, hmax, hmean, hp50, hp95, hp99 := summaryStatsFloats(heapAllocs)
	gmin, gmax, gmean, gp50, gp95, gp99 := summaryStatsFloats(numG)
	pmin, pmax, pmean, pp50, pp95, pp99 := summaryStatsFloats(pauseNs)

	summaryPath := "bench_sustained_metrics_round2_summary.txt"
	f, err := os.Create(summaryPath)
	if err != nil {
		fmt.Printf("failed to create summary file: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()

	fmt.Fprintf(f, "Sustained metrics summary (generated: %s)\n\n", time.Now().Format(time.RFC3339))
	fmt.Fprintln(f, "Alloc:")
	fmt.Fprintf(f, "  min: %s\n", formatBytes(amin))
	fmt.Fprintf(f, "  mean: %s\n", formatBytes(amean))
	fmt.Fprintf(f, "  p50: %s\n", formatBytes(ap50))
	fmt.Fprintf(f, "  p95: %s\n", formatBytes(ap95))
	fmt.Fprintf(f, "  p99: %s\n", formatBytes(ap99))
	fmt.Fprintf(f, "  max: %s\n\n", formatBytes(amax))

	fmt.Fprintln(f, "HeapAlloc:")
	fmt.Fprintf(f, "  min: %s\n", formatBytes(hmin))
	fmt.Fprintf(f, "  mean: %s\n", formatBytes(hmean))
	fmt.Fprintf(f, "  p50: %s\n", formatBytes(hp50))
	fmt.Fprintf(f, "  p95: %s\n", formatBytes(hp95))
	fmt.Fprintf(f, "  p99: %s\n", formatBytes(hp99))
	fmt.Fprintf(f, "  max: %s\n\n", formatBytes(hmax))

	fmt.Fprintln(f, "NumGoroutine:")
	fmt.Fprintf(f, "  min: %.0f\n", gmin)
	fmt.Fprintf(f, "  mean: %.2f\n", gmean)
	fmt.Fprintf(f, "  p50: %.0f\n", gp50)
	fmt.Fprintf(f, "  p95: %.0f\n", gp95)
	fmt.Fprintf(f, "  p99: %.0f\n", gp99)
	fmt.Fprintf(f, "  max: %.0f\n\n", gmax)

	fmt.Fprintln(f, "PauseTotalNs:")
	fmt.Fprintf(f, "  min: %.0f ns\n", pmin)
	fmt.Fprintf(f, "  mean: %.0f ns\n", pmean)
	fmt.Fprintf(f, "  p50: %.0f ns\n", pp50)
	fmt.Fprintf(f, "  p95: %.0f ns\n", pp95)
	fmt.Fprintf(f, "  p99: %.0f ns\n", pp99)
	fmt.Fprintf(f, "  max: %.0f ns\n\n", pmax)

	csvPath := "bench_sustained_metrics_round2.csv"
	if err := writeCSV(samples, csvPath); err != nil {
		fmt.Printf("failed to write csv: %v\n", err)
	} else {
		fmt.Printf("wrote CSV to %s\n", csvPath)
	}

	fmt.Printf("wrote summary to %s\n", summaryPath)
}
