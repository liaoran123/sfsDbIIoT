package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// 全局实例
var (
	DeviceManagerInstance       *DeviceManager
	StorageManagerInstance      *StorageManager
	SensorDataProcessorInstance *SensorDataProcessor
	AlertManagerInstance        *AlertManager
	APIInstance                 *API
)

func main() {
	// 解析命令行参数
	var runBenchmark bool
	var runSustained bool
	var sustainedDuration int
	var sustainedConcurrency int
	var sustainedBatch int
	flag.BoolVar(&runBenchmark, "benchmark", false, "运行基准测试")
	flag.BoolVar(&runSustained, "sustained", false, "运行持续写入基准测试")
	flag.IntVar(&sustainedDuration, "sustained-duration", 300, "持续写入测试持续时间（秒），默认300s）")
	flag.IntVar(&sustainedConcurrency, "sustained-concurrency", 10, "持续写入并发数，默认10")
	flag.IntVar(&sustainedBatch, "sustained-batch", 1, "每次写入的批量大小，默认1")
	flag.Parse()

	fmt.Println("=== 智能工厂设备监控系统 ===")
	fmt.Println("正在初始化系统...")

	// 1. 加载配置
	err := LoadConfig()
	if err != nil {
		fmt.Printf("配置加载失败: %v\n", err)
		os.Exit(1)
	}

	config := GetConfig()
	fmt.Println("配置加载成功")

	// 2. 初始化存储管理器
	StorageManagerInstance, err = NewStorageManager(
		config.Database.Path,
		config.Database.CacheSize,
		config.Database.UseCompression,
		config.Database.CompressionType,
	)
	if err != nil {
		fmt.Printf("存储管理器初始化失败: %v\n", err)
		os.Exit(1)
	}
	defer StorageManagerInstance.Close()
	fmt.Println("存储管理器初始化成功")

	// 3. 初始化设备管理器
	DeviceManagerInstance = NewDeviceManager(
		config.Device.MaxDevices,
		config.Device.ScanInterval,
	)
	fmt.Println("设备管理器初始化成功")

	// 4. 初始化告警管理器
	AlertManagerInstance = NewAlertManager(
		config.Alert.CheckInterval,
		config.Alert.NotificationType,
	)
	AlertManagerInstance.Start()
	fmt.Println("告警管理器初始化成功")

	// 5. 初始化传感器数据处理器
	SensorDataProcessorInstance = NewSensorDataProcessor(
		config.Sensor.DataInterval,
		config.Sensor.BatchSize,
		DeviceManagerInstance,
		StorageManagerInstance,
	)
	err = SensorDataProcessorInstance.Start()
	if err != nil {
		fmt.Printf("传感器数据处理器启动失败: %v\n", err)
		os.Exit(1)
	}
	defer SensorDataProcessorInstance.Stop()
	fmt.Println("传感器数据处理器初始化成功")

	// 6. 初始化API
	if config.API.Enabled {
		APIInstance = NewAPI(config.API.Port, config.API.Cors)
		go func() {
			err := APIInstance.Start()
			if err != nil {
				fmt.Printf("API启动失败: %v\n", err)
			}
		}()
		fmt.Printf("API初始化成功，监听端口: %s\n", config.API.Port)
	}

	// 7. 启动设备扫描
	DeviceManagerInstance.StartDeviceScan()
	fmt.Println("设备扫描服务启动成功")

	// 8. 注册示例设备和传感器
	registerExampleDevices()

	// 9. 运行基准测试（如果请求）
	if runBenchmark {
		fmt.Println("\n=== 开始基准测试 ===")
		results := RunBenchmarks()
		PrintBenchmarkResults(results)
		fmt.Println("基准测试完成")
		os.Exit(0)
	}

	// 持续写入基准（例如 5 分钟并发 10）
	if runSustained {
		fmt.Println("\n=== 开始持续写入基准测试 ===")
		result := RunSustainedWrite(sustainedDuration, sustainedConcurrency, sustainedBatch)
		PrintBenchmarkResults([]BenchmarkResult{result})
		fmt.Println("持续写入基准测试完成")
		os.Exit(0)
	}

	// 10. 模拟传感器数据
	go simulateSensorData()

	// 11. 等待中断信号
	fmt.Println("系统初始化完成，正在运行...")
	fmt.Println("按 Ctrl+C 退出系统")
	fmt.Println("使用 -benchmark 参数运行基准测试")

	// 等待中断信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	// 12. 关闭系统
	fmt.Println("正在关闭系统...")

	if APIInstance != nil {
		APIInstance.Stop()
	}

	AlertManagerInstance.Stop()

	fmt.Println("系统已关闭")
}

// registerExampleDevices 注册示例设备和传感器
func registerExampleDevices() {
	// 注册示例设备1
	device1 := &Device{
		ID:              "device_001",
		Name:            "注塑机A",
		Type:            "injection_machine",
		Location:        "车间1-东区",
		Status:          DeviceStatusOnline,
		LastSeen:        time.Now(),
		IPAddress:       "192.168.1.101",
		MacAddress:      "00:11:22:33:44:55",
		FirmwareVersion: "v1.2.3",
	}

	err := DeviceManagerInstance.RegisterDevice(device1)
	if err != nil {
		fmt.Printf("示例设备1注册失败: %v\n", err)
	} else {
		fmt.Println("示例设备1注册成功: 注塑机A")

		// 添加传感器
		temperatureSensor := &Sensor{
			ID:        "sensor_001",
			Name:      "温度传感器",
			Type:      "temperature",
			Unit:      "°C",
			MinValue:  0,
			MaxValue:  200,
			Threshold: 150,
			Enabled:   true,
		}

		err := DeviceManagerInstance.AddSensor(device1.ID, temperatureSensor)
		if err != nil {
			fmt.Printf("温度传感器添加失败: %v\n", err)
		} else {
			fmt.Println("温度传感器添加成功")
		}

		pressureSensor := &Sensor{
			ID:        "sensor_002",
			Name:      "压力传感器",
			Type:      "pressure",
			Unit:      "bar",
			MinValue:  0,
			MaxValue:  200,
			Threshold: 180,
			Enabled:   true,
		}

		err = DeviceManagerInstance.AddSensor(device1.ID, pressureSensor)
		if err != nil {
			fmt.Printf("压力传感器添加失败: %v\n", err)
		} else {
			fmt.Println("压力传感器添加成功")
		}
	}

	// 注册示例设备2
	device2 := &Device{
		ID:              "device_002",
		Name:            "包装机B",
		Type:            "packaging_machine",
		Location:        "车间2-西区",
		Status:          DeviceStatusOnline,
		LastSeen:        time.Now(),
		IPAddress:       "192.168.1.102",
		MacAddress:      "00:11:22:33:44:66",
		FirmwareVersion: "v1.1.2",
	}

	err = DeviceManagerInstance.RegisterDevice(device2)
	if err != nil {
		fmt.Printf("示例设备2注册失败: %v\n", err)
	} else {
		fmt.Println("示例设备2注册成功: 包装机B")

		// 添加传感器
		speedSensor := &Sensor{
			ID:        "sensor_003",
			Name:      "速度传感器",
			Type:      "speed",
			Unit:      "rpm",
			MinValue:  0,
			MaxValue:  3000,
			Threshold: 2500,
			Enabled:   true,
		}

		err := DeviceManagerInstance.AddSensor(device2.ID, speedSensor)
		if err != nil {
			fmt.Printf("速度传感器添加失败: %v\n", err)
		} else {
			fmt.Println("速度传感器添加成功")
		}
	}
}

// simulateSensorData 模拟传感器数据
func simulateSensorData() {
	fmt.Println("开始模拟传感器数据...")

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	counter := 0

	for {
		<-ticker.C

		// 模拟注塑机A的温度数据
		temperature := 80.0 + float64(counter%30)
		tempData := GenerateTestSensorData("device_001", "sensor_001", temperature)
		err := SensorDataProcessorInstance.ProcessSensorData(tempData)
		if err != nil {
			fmt.Printf("温度数据处理失败: %v\n", err)
		}

		// 模拟注塑机A的压力数据
		pressure := 120.0 + float64(counter%50)
		pressData := GenerateTestSensorData("device_001", "sensor_002", pressure)
		err = SensorDataProcessorInstance.ProcessSensorData(pressData)
		if err != nil {
			fmt.Printf("压力数据处理失败: %v\n", err)
		}

		// 模拟包装机B的速度数据
		speed := 1500.0 + float64(counter%1000)
		speedData := GenerateTestSensorData("device_002", "sensor_003", speed)
		err = SensorDataProcessorInstance.ProcessSensorData(speedData)
		if err != nil {
			fmt.Printf("速度数据处理失败: %v\n", err)
		}

		counter++

		// 每10次模拟打印一次状态
		if counter%10 == 0 {
			fmt.Printf("已模拟 %d 次传感器数据\n", counter)
		}
	}
}

/*
智能工厂设备监控 ：需要处理高频传感器数据的时间序列

系统架构说明：
1. 配置管理模块 (config.go) - 负责加载和管理配置
2. 设备管理模块 (device.go) - 负责设备和传感器的注册与管理
3. 传感器数据处理模块 (sensor.go) - 负责传感器数据的采集、处理和批处理
4. 数据存储模块 (storage.go) - 负责使用 sfsDb 存储和查询数据
5. 数据分析模块 (analytics.go) - 负责数据分析和预测
6. 告警管理模块 (alert.go) - 负责告警的检测和通知
7. API接口模块 (api.go) - 提供RESTful API接口

核心功能：
- 设备自动发现和注册
- 传感器数据实时采集和处理
- 时序数据存储和查询
- 数据聚合和分析
- 阈值告警和通知
- 设备状态监控
- 历史数据分析

技术特点：
- 使用 sfsDb 作为时序数据库
- 支持高频数据采集和处理
- 支持数据压缩和存储优化
- 支持实时告警和通知
- 提供RESTful API接口
- 模块化设计，易于扩展
*/
