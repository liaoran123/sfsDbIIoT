package main

import (
	"fmt"
	"sync"
	"time"
)

// SensorData 传感器数据结构体
type SensorData struct {
	ID        string    `json:"id"`
	DeviceID  string    `json:"device_id"`
	SensorID  string    `json:"sensor_id"`
	Value     float64   `json:"value"`
	Timestamp time.Time `json:"timestamp"`
	Quality   int       `json:"quality"` // 0-100，数据质量
	RawData   string    `json:"raw_data"`
}

// SensorDataBatch 传感器数据批处理结构体
type SensorDataBatch struct {
	Data     []*SensorData
	BatchSize int
	mutex    sync.Mutex
}

// NewSensorDataBatch 创建传感器数据批处理实例
func NewSensorDataBatch(batchSize int) *SensorDataBatch {
	return &SensorDataBatch{
		Data:     make([]*SensorData, 0, batchSize),
		BatchSize: batchSize,
	}
}

// AddData 添加传感器数据
func (batch *SensorDataBatch) AddData(data *SensorData) bool {
	batch.mutex.Lock()
	defer batch.mutex.Unlock()
	
	batch.Data = append(batch.Data, data)
	return len(batch.Data) >= batch.BatchSize
}

// GetBatch 获取当前批次数据
func (batch *SensorDataBatch) GetBatch() []*SensorData {
	batch.mutex.Lock()
	defer batch.mutex.Unlock()
	
	data := batch.Data
	batch.Data = make([]*SensorData, 0, batch.BatchSize)
	return data
}

// GetSize 获取当前批次大小
func (batch *SensorDataBatch) GetSize() int {
	batch.mutex.Lock()
	defer batch.mutex.Unlock()
	return len(batch.Data)
}

// SensorDataProcessor 传感器数据处理器
type SensorDataProcessor struct {
	batch         *SensorDataBatch
	dataInterval  int
	deviceManager *DeviceManager
	storage       *StorageManager
	stopChan      chan struct{}
	isRunning     bool
	mutex         sync.Mutex
}

// NewSensorDataProcessor 创建传感器数据处理器
func NewSensorDataProcessor(dataInterval, batchSize int, deviceManager *DeviceManager, storage *StorageManager) *SensorDataProcessor {
	return &SensorDataProcessor{
		batch:         NewSensorDataBatch(batchSize),
		dataInterval:  dataInterval,
		deviceManager: deviceManager,
		storage:       storage,
		stopChan:      make(chan struct{}),
		isRunning:     false,
	}
}

// Start 启动传感器数据处理器
func (processor *SensorDataProcessor) Start() error {
	processor.mutex.Lock()
	if processor.isRunning {
		processor.mutex.Unlock()
		return fmt.Errorf("sensor data processor is already running")
	}
	processor.isRunning = true
	processor.mutex.Unlock()
	
	go processor.processLoop()
	fmt.Println("Sensor data processor started")
	return nil
}

// Stop 停止传感器数据处理器
func (processor *SensorDataProcessor) Stop() error {
	processor.mutex.Lock()
	if !processor.isRunning {
		processor.mutex.Unlock()
		return fmt.Errorf("sensor data processor is not running")
	}
	processor.isRunning = false
	processor.mutex.Unlock()
	
	close(processor.stopChan)
	fmt.Println("Sensor data processor stopped")
	return nil
}

// processLoop 处理循环
func (processor *SensorDataProcessor) processLoop() {
	ticker := time.NewTicker(time.Duration(processor.dataInterval) * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			processor.processBatch()
		case <-processor.stopChan:
			// 处理剩余数据
			processor.processBatch()
			return
		}
	}
}

// processBatch 处理批次数据
func (processor *SensorDataProcessor) processBatch() {
	batch := processor.batch.GetBatch()
	if len(batch) == 0 {
		return
	}
	
	// 处理数据
	processedData := processor.processData(batch)
	
	// 存储数据
	if processor.storage != nil {
		err := processor.storage.StoreSensorDataBatch(processedData)
		if err != nil {
			fmt.Printf("Error storing sensor data batch: %v\n", err)
		}
	}
	
	// 更新设备和传感器状态
	processor.updateDeviceSensorStatus(processedData)
}

// processData 处理传感器数据
func (processor *SensorDataProcessor) processData(data []*SensorData) []*SensorData {
	processedData := make([]*SensorData, 0, len(data))
	
	for _, item := range data {
		// 验证数据
		if !processor.validateData(item) {
			fmt.Printf("Invalid sensor data: %v\n", item)
			continue
		}
		
		// 数据转换和标准化
		processedItem := processor.normalizeData(item)
		
		// 数据质量检查
		processedItem.Quality = processor.checkDataQuality(processedItem)
		
		processedData = append(processedData, processedItem)
	}
	
	return processedData
}

// validateData 验证传感器数据
func (processor *SensorDataProcessor) validateData(data *SensorData) bool {
	// 检查必要字段
	if data.DeviceID == "" || data.SensorID == "" {
		return false
	}
	
	// 检查时间戳
	if data.Timestamp.IsZero() {
		data.Timestamp = time.Now()
	}
	
	// 检查设备是否存在
	_, err := processor.deviceManager.GetDevice(data.DeviceID)
	if err != nil {
		return false
	}
	
	// 检查传感器是否存在
	_, err = processor.deviceManager.GetSensor(data.DeviceID, data.SensorID)
	if err != nil {
		return false
	}
	
	return true
}

// normalizeData 标准化传感器数据
func (processor *SensorDataProcessor) normalizeData(data *SensorData) *SensorData {
	// 获取传感器信息
	sensor, err := processor.deviceManager.GetSensor(data.DeviceID, data.SensorID)
	if err != nil {
		return data
	}
	
	// 确保值在有效范围内
	if data.Value < sensor.MinValue {
		data.Value = sensor.MinValue
	}
	if data.Value > sensor.MaxValue {
		data.Value = sensor.MaxValue
	}
	
	return data
}

// checkDataQuality 检查数据质量
func (processor *SensorDataProcessor) checkDataQuality(data *SensorData) int {
	// 基础质量分数
	quality := 100
	
	// 获取传感器信息
	sensor, err := processor.deviceManager.GetSensor(data.DeviceID, data.SensorID)
	if err != nil {
		return 0
	}
	
	// 检查值是否在有效范围内
	if data.Value < sensor.MinValue || data.Value > sensor.MaxValue {
		quality -= 50
	}
	
	// 检查值是否接近阈值
	if abs(data.Value-sensor.Threshold) < (sensor.MaxValue-sensor.MinValue)*0.1 {
		quality -= 20
	}
	
	// 检查时间戳是否合理（不超过5分钟）
	if time.Since(data.Timestamp) > 5*time.Minute {
		quality -= 30
	}
	
	// 确保质量分数在0-100之间
	if quality < 0 {
		quality = 0
	}
	
	return quality
}

// updateDeviceSensorStatus 更新设备和传感器状态
func (processor *SensorDataProcessor) updateDeviceSensorStatus(data []*SensorData) {
	for _, item := range data {
		// 更新传感器值
		err := processor.deviceManager.UpdateSensorValue(item.DeviceID, item.SensorID, item.Value)
		if err != nil {
			fmt.Printf("Error updating sensor value: %v\n", err)
		}
		
		// 更新设备状态为在线
		err = processor.deviceManager.UpdateDeviceStatus(item.DeviceID, DeviceStatusOnline)
		if err != nil {
			fmt.Printf("Error updating device status: %v\n", err)
		}
	}
}

// ProcessSensorData 处理单个传感器数据
func (processor *SensorDataProcessor) ProcessSensorData(data *SensorData) error {
	// 添加到批次
	batchFull := processor.batch.AddData(data)
	
	// 如果批次满了，立即处理
	if batchFull {
		processor.processBatch()
	}
	
	return nil
}

// GetProcessingStats 获取处理统计信息
func (processor *SensorDataProcessor) GetProcessingStats() map[string]interface{} {
	return map[string]interface{}{
		"batch_size":      processor.batch.BatchSize,
		"current_batch":   processor.batch.GetSize(),
		"data_interval":   processor.dataInterval,
		"is_running":      processor.isRunning,
	}
}

// abs 返回浮点数的绝对值
func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}

// GenerateTestSensorData 生成测试传感器数据
func GenerateTestSensorData(deviceID, sensorID string, value float64) *SensorData {
	return &SensorData{
		ID:        fmt.Sprintf("data_%d", time.Now().UnixNano()),
		DeviceID:  deviceID,
		SensorID:  sensorID,
		Value:     value,
		Timestamp: time.Now(),
		Quality:   100,
		RawData:   fmt.Sprintf("{\"value\":%f}", value),
	}
}
