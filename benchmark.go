package main

import (
	"fmt"
	"math/rand"
	"time"
)

// BenchmarkResult 存储基准测试结果
type BenchmarkResult struct {
	Operation     string
	Count         int
	Duration      time.Duration
	OperationsPerSecond float64
	AverageTime   time.Duration
}

// RunBenchmarks 运行所有基准测试
func RunBenchmarks() []BenchmarkResult {
	results := []BenchmarkResult{}
	
	// 设备注册测试
	results = append(results, benchmarkDeviceRegistration(100))
	
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
			ID:              deviceID,
			Name:            deviceName,
			Type:            "benchmark",
			Location:        "测试位置",
			Status:          DeviceStatusOnline,
			LastSeen:        time.Now(),
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
		Operation:     "设备注册",
		Count:         count,
		Duration:      duration,
		OperationsPerSecond: opsPerSec,
		AverageTime:   averageTime,
	}
}

// 基准测试：传感器数据写入
func benchmarkSensorDataWrite(count int) BenchmarkResult {
	// 确保有一个测试设备
	deviceID := "benchmark-test-device"
	deviceName := "基准测试设备"
	
	device := &Device{
		ID:              deviceID,
		Name:            deviceName,
		Type:            "benchmark",
		Location:        "测试位置",
		Status:          DeviceStatusOnline,
		LastSeen:        time.Now(),
	}
	
	err := DeviceManagerInstance.RegisterDevice(device)
	if err != nil {
		fmt.Printf("创建测试设备失败: %v\n", err)
	}
	
	// 添加传感器
	sensorID := "temperature"
	sensor := &Sensor{
		ID:         sensorID,
		Name:       "温度传感器",
		Type:       "temperature",
		Unit:       "摄氏度",
		MinValue:   0,
		MaxValue:   100,
		Threshold:  80,
		Enabled:    true,
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
		data := &SensorData{
			ID:        fmt.Sprintf("data_%d", time.Now().UnixNano()),
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
	
	// 等待数据批处理完成
	time.Sleep(2 * time.Second)
	
	duration := time.Since(start)
	opsPerSec := float64(count) / duration.Seconds()
	averageTime := duration / time.Duration(count)
	
	return BenchmarkResult{
		Operation:     "传感器数据写入",
		Count:         count,
		Duration:      duration,
		OperationsPerSecond: opsPerSec,
		AverageTime:   averageTime,
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
		Operation:     "传感器数据查询",
		Count:         count,
		Duration:      duration,
		OperationsPerSecond: opsPerSec,
		AverageTime:   averageTime,
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
		Operation:     "聚合查询",
		Count:         count,
		Duration:      duration,
		OperationsPerSecond: opsPerSec,
		AverageTime:   averageTime,
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
		alert := &Alert{
			ID:        fmt.Sprintf("alert_%d", time.Now().UnixNano()),
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
		Operation:     "告警检测",
		Count:         count,
		Duration:      duration,
		OperationsPerSecond: opsPerSec,
		AverageTime:   averageTime,
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
