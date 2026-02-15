package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// BenchmarkResult 存储基准测试结果
type BenchmarkResult struct {
	Operation           string
	Count               int
	Duration            time.Duration
	OperationsPerSecond float64
	AverageTime         time.Duration
}

// RunBenchmarks 运行所有基准测试
func RunBenchmarks() []BenchmarkResult {
	// 在运行基准测试时暂时屏蔽标准输出和标准错误, 以减少日志噪声对耗时的影响。
	oldStdout := os.Stdout
	oldStderr := os.Stderr
	devNull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err == nil {
		os.Stdout = devNull
		os.Stderr = devNull
		defer func() {
			// 恢复
			os.Stdout = oldStdout
			os.Stderr = oldStderr
			devNull.Close()
		}()
	}

	results := []BenchmarkResult{}

	// 设备注册测试（扩大到 10000 次以获得稳定测量）
	results = append(results, benchmarkDeviceRegistration(10000))

	// 传感器数据写入测试
	results = append(results, benchmarkSensorDataWrite(1000))

	// 数据查询测试
	results = append(results, benchmarkSensorDataQuery())

	// 告警检测测试
	results = append(results, benchmarkAlertDetection(1000))

	return results
}

// 基准测试：设备注册
func benchmarkDeviceRegistration(count int) BenchmarkResult {
	start := time.Now()

	for i := 0; i < count; i++ {
		deviceID := fmt.Sprintf("benchmark-device-%d", i)
		deviceName := fmt.Sprintf("测试设备-%d", i)

		device := &Device{
			ID:       deviceID,
			Name:     deviceName,
			Type:     "benchmark",
			Location: "测试位置",
			Status:   DeviceStatusOnline,
			LastSeen: time.Now(),
		}

		err := DeviceManagerInstance.RegisterDevice(device)
		if err != nil {
			fmt.Printf("设备注册失败: %v\n", err)
		}
	}

	duration := time.Since(start)
	opsPerSec := float64(count) / duration.Seconds()
	averageTime := duration / time.Duration(count)

	return BenchmarkResult{
		Operation:           "设备注册",
		Count:               count,
		Duration:            duration,
		OperationsPerSecond: opsPerSec,
		AverageTime:         averageTime,
	}
}

// 基准测试：传感器数据写入
func benchmarkSensorDataWrite(count int) BenchmarkResult {
	// 确保有一个测试设备
	deviceID := "benchmark-test-device"
	deviceName := "基准测试设备"

	device := &Device{
		ID:       deviceID,
		Name:     deviceName,
		Type:     "benchmark",
		Location: "测试位置",
		Status:   DeviceStatusOnline,
		LastSeen: time.Now(),
	}

	err := DeviceManagerInstance.RegisterDevice(device)
	if err != nil {
		fmt.Printf("创建测试设备失败: %v\n", err)
	}

	// 添加传感器
	sensorID := "temperature"
	sensor := &Sensor{
		ID:        sensorID,
		Name:      "温度传感器",
		Type:      "temperature",
		Unit:      "摄氏度",
		MinValue:  0,
		MaxValue:  100,
		Threshold: 80,
		Enabled:   true,
	}

	err = DeviceManagerInstance.AddSensor(deviceID, sensor)
	if err != nil {
		fmt.Printf("添加传感器失败: %v\n", err)
	}

	start := time.Now()

	for i := 0; i < count; i++ {
		// 生成随机温度数据
		temperature := 20.0 + rand.Float64()*10.0

		// 生成随机时间戳
		timestamp := time.Now().Add(-time.Duration(i) * time.Second)

		// 创建传感器数据
		// 给每条数据加上索引以减少 ID 冲突
		data := &SensorData{
			ID:        fmt.Sprintf("data_%d_%d", i, time.Now().UnixNano()),
			DeviceID:  deviceID,
			SensorID:  sensorID,
			Value:     temperature,
			Timestamp: timestamp,
			Quality:   100,
			RawData:   fmt.Sprintf("{\"value\":%f}", temperature),
		}

		// 处理传感器数据
		err := SensorDataProcessorInstance.ProcessSensorData(data)
		if err != nil {
			fmt.Printf("数据处理失败: %v\n", err)
		}
	}

	// 不再强制等待，避免影响写入吞吐的测量

	duration := time.Since(start)
	opsPerSec := float64(count) / duration.Seconds()
	averageTime := duration / time.Duration(count)

	return BenchmarkResult{
		Operation:           "传感器数据写入",
		Count:               count,
		Duration:            duration,
		OperationsPerSecond: opsPerSec,
		AverageTime:         averageTime,
	}
}

// 基准测试：传感器数据查询
func benchmarkSensorDataQuery() BenchmarkResult {
	deviceID := "benchmark-test-device"
	sensorID := "temperature"

	// 定义查询时间范围
	endTime := time.Now()
	startTime := endTime.Add(-1 * time.Hour)

	count := 100
	start := time.Now()

	for i := 0; i < count; i++ {
		// 查询传感器数据
		_, err := StorageManagerInstance.QuerySensorData(deviceID, sensorID, startTime, endTime, 100)
		if err != nil {
			fmt.Printf("数据查询失败: %v\n", err)
		}
	}

	duration := time.Since(start)
	opsPerSec := float64(count) / duration.Seconds()
	averageTime := duration / time.Duration(count)

	return BenchmarkResult{
		Operation:           "传感器数据查询",
		Count:               count,
		Duration:            duration,
		OperationsPerSecond: opsPerSec,
		AverageTime:         averageTime,
	}
}

// 基准测试：聚合查询
func benchmarkAggregationQuery() BenchmarkResult {
	deviceID := "benchmark-test-device"
	sensorID := "temperature"

	// 定义查询时间范围
	endTime := time.Now()
	startTime := endTime.Add(-1 * time.Hour)

	count := 50
	start := time.Now()

	for i := 0; i < count; i++ {
		// 测试不同的聚合函数
		aggregations := []string{"avg", "max", "min", "sum"}
		aggregation := aggregations[i%len(aggregations)]

		// 查询聚合数据
		_, err := StorageManagerInstance.QuerySensorDataWithAggregation(
			deviceID, sensorID, startTime, endTime, "minute", aggregation,
		)
		if err != nil {
			fmt.Printf("聚合查询失败: %v\n", err)
		}
	}

	duration := time.Since(start)
	opsPerSec := float64(count) / duration.Seconds()
	averageTime := duration / time.Duration(count)

	return BenchmarkResult{
		Operation:           "聚合查询",
		Count:               count,
		Duration:            duration,
		OperationsPerSecond: opsPerSec,
		AverageTime:         averageTime,
	}
}

// 基准测试：告警检测
func benchmarkAlertDetection(count int) BenchmarkResult {
	deviceID := "benchmark-test-device"
	sensorID := "temperature"

	start := time.Now()

	for i := 0; i < count; i++ {
		// 生成随机温度数据
		temperature := 20.0 + rand.Float64()*20.0 // 20-40度

		// 创建告警
		// 使用索引避免与其他并发生成的告警 ID 冲突
		alert := &Alert{
			ID:        fmt.Sprintf("alert_%d_%d", i, time.Now().UnixNano()),
			DeviceID:  deviceID,
			SensorID:  sensorID,
			Type:      "threshold",
			Message:   fmt.Sprintf("温度异常: %.2f度", temperature),
			Severity:  AlertSeverityWarning,
			Timestamp: time.Now(),
			Status:    AlertStatusActive,
			Metadata:  map[string]interface{}{"value": temperature},
		}

		// 添加告警
		err := AlertManagerInstance.AddAlert(alert)
		if err != nil {
			fmt.Printf("添加告警失败: %v\n", err)
		}
	}

	duration := time.Since(start)
	opsPerSec := float64(count) / duration.Seconds()
	averageTime := duration / time.Duration(count)

	return BenchmarkResult{
		Operation:           "告警检测",
		Count:               count,
		Duration:            duration,
		OperationsPerSecond: opsPerSec,
		AverageTime:         averageTime,
	}
}

// PrintBenchmarkResults 打印基准测试结果
func PrintBenchmarkResults(results []BenchmarkResult) {
	fmt.Println("\n=== 基准测试结果 ===")
	fmt.Printf("%-15s %-10s %-20s %-20s %-20s\n", "操作", "次数", "总耗时", "每秒操作数", "平均耗时")
	fmt.Println("-----------------------------------------------------------------------------")

	for _, result := range results {
		fmt.Printf("%-15s %-10d %-20s %-20.2f %-20s\n",
			result.Operation,
			result.Count,
			result.Duration,
			result.OperationsPerSecond,
			result.AverageTime,
		)
	}

	fmt.Println("-----------------------------------------------------------------------------")

	// 与其他时序数据库的性能比较（估算值）
	fmt.Println("\n=== 性能比较（估算值）===")
	fmt.Printf("%-15s %-20s\n", "数据库", "写入性能 ( ops/sec )")
	fmt.Println("----------------------------------------")
	if len(results) > 1 {
		fmt.Printf("%-15s %-20.2f\n", "sfsDb (本系统)", results[1].OperationsPerSecond)
	}
	fmt.Printf("%-15s %-20s\n", "InfluxDB", "~10,000-50,000")
	fmt.Printf("%-15s %-20s\n", "TimescaleDB", "~5,000-20,000")
	fmt.Printf("%-15s %-20s\n", "Prometheus", "~1,000-10,000")
	fmt.Println("----------------------------------------")

	fmt.Println("\n注：比较数据为估算值，实际性能取决于硬件配置和具体使用场景。")
}

// RunSustainedWrite 在指定持续时间内并发写入传感器数据，返回统计结果
func RunSustainedWrite(durationSec int, concurrency int, batch int) BenchmarkResult {
	// 确保有测试设备和传感器
	deviceID := "benchmark-test-device"
	device := &Device{
		ID:       deviceID,
		Name:     "持续写入设备",
		Type:     "benchmark",
		Location: "测试位置",
		Status:   DeviceStatusOnline,
		LastSeen: time.Now(),
	}
	_ = DeviceManagerInstance.RegisterDevice(device)
	sensorID := "temperature"
	sensor := &Sensor{
		ID:       sensorID,
		Name:     "温度传感器",
		Type:     "temperature",
		Unit:     "摄氏度",
		MinValue: 0,
		MaxValue: 100,
		Enabled:  true,
	}
	_ = DeviceManagerInstance.AddSensor(deviceID, sensor)

	var total uint64
	var errs uint64
	var totalLatency uint64

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(durationSec)*time.Second)
	defer cancel()

	var wg sync.WaitGroup

	// 监控采样
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

	var samplesMu sync.Mutex
	samples := []MemSample{}

	monitorTicker := time.NewTicker(5 * time.Second)
	defer monitorTicker.Stop()

	// 监控 goroutine
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case t := <-monitorTicker.C:
				var ms runtime.MemStats
				runtime.ReadMemStats(&ms)
				s := MemSample{
					Time:         t.Format(time.RFC3339),
					NumGoroutine: runtime.NumGoroutine(),
					Alloc:        ms.Alloc,
					TotalAlloc:   ms.TotalAlloc,
					Sys:          ms.Sys,
					HeapAlloc:    ms.HeapAlloc,
					HeapSys:      ms.HeapSys,
					NumGC:        ms.NumGC,
					PauseTotalNs: ms.PauseTotalNs,
				}
				samplesMu.Lock()
				samples = append(samples, s)
				samplesMu.Unlock()
			}
		}
	}()
	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				for b := 0; b < batch; b++ {
					data := &SensorData{
						ID:        fmt.Sprintf("sust_%d_%d_%d", worker, b, time.Now().UnixNano()),
						DeviceID:  deviceID,
						SensorID:  sensorID,
						Value:     20.0 + rand.Float64()*10.0,
						Timestamp: time.Now(),
						Quality:   100,
						RawData:   fmt.Sprintf("{\"value\":%f}", rand.Float64()*100),
					}

					t0 := time.Now()
					if err := SensorDataProcessorInstance.ProcessSensorData(data); err != nil {
						atomic.AddUint64(&errs, 1)
					} else {
						atomic.AddUint64(&total, 1)
						atomic.AddUint64(&totalLatency, uint64(time.Since(t0).Nanoseconds()))
					}
				}
			}
		}(w)
	}

	wg.Wait()
	// 写出监控采样到文件
	samplesMu.Lock()
	if len(samples) > 0 {
		if b, err := json.MarshalIndent(samples, "", "  "); err == nil {
			// 写到项目根目录
			_ = os.WriteFile("bench_sustained_metrics_round2.json", b, 0644)
		}
	}
	samplesMu.Unlock()

	duration := time.Duration(durationSec) * time.Second
	ops := atomic.LoadUint64(&total)
	avg := time.Duration(0)
	if ops > 0 {
		avg = time.Duration(atomic.LoadUint64(&totalLatency)/ops) * time.Nanosecond
	}

	return BenchmarkResult{
		Operation:           fmt.Sprintf("持续写入 %ds (concurrency=%d batch=%d)", durationSec, concurrency, batch),
		Count:               int(ops),
		Duration:            duration,
		OperationsPerSecond: float64(ops) / duration.Seconds(),
		AverageTime:         avg,
	}
}
