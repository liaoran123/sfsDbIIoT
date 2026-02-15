package main

import (
	"fmt"
	"os"
	"time"

	"github.com/liaoran123/sfsDb/engine"
	"github.com/liaoran123/sfsDb/storage"
	sfstime "github.com/liaoran123/sfsDb/time"
)

// StorageManager 存储管理器
type StorageManager struct {
	deviceTable     *engine.Table
	sensorTable     *engine.Table
	dataTable       *engine.Table
	path            string
	cacheSize       int
	useCompression  bool
	compressionType string
}

// NewStorageManager 创建存储管理器
func NewStorageManager(path string, cacheSize int, useCompression bool, compressionType string) (*StorageManager, error) {
	// 确保数据目录存在
	err := os.MkdirAll(path, 0755)
	if err != nil {
		return nil, fmt.Errorf("failed to create data directory: %v", err)
	}

	// 初始化 sfsDb 数据库
	dbManager := storage.GetDBManager()
	_, err = dbManager.OpenDB(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %v", err)
	}

	// 创建存储管理器
	manager := &StorageManager{
		path:            path,
		cacheSize:       cacheSize,
		useCompression:  useCompression,
		compressionType: compressionType,
	}

	// 初始化表结构
	err = manager.initTables()
	if err != nil {
		return nil, fmt.Errorf("failed to initialize tables: %v", err)
	}

	fmt.Printf("Storage manager initialized successfully at %s\n", path)
	return manager, nil
}

// initTables 初始化表结构
func (sm *StorageManager) initTables() error {
	// 创建设备表
	deviceTable, err := engine.TableNew("devices")
	if err != nil {
		return fmt.Errorf("failed to create devices table: %v", err)
	}

	// 设置设备表字段
	deviceFields := map[string]any{
		"id":               "",
		"name":             "",
		"type":             "",
		"location":         "",
		"status":           "",
		"last_seen":        time.Time{},
		"ip_address":       "",
		"mac_address":      "",
		"firmware_version": "",
	}
	err = deviceTable.SetFields(deviceFields)
	if err != nil {
		return fmt.Errorf("failed to set devices table fields: %v", err)
	}

	// 创建设备表主键
	devicePK, err := engine.DefaultPrimaryKeyNew("pk")
	if err != nil {
		return fmt.Errorf("failed to create devices table primary key: %v", err)
	}
	devicePK.AddFields("id")
	err = deviceTable.CreateIndex(devicePK)
	if err != nil {
		return fmt.Errorf("failed to create devices table index: %v", err)
	}
	sm.deviceTable = deviceTable

	// 创建传感器表
	sensorTable, err := engine.TableNew("sensors")
	if err != nil {
		return fmt.Errorf("failed to create sensors table: %v", err)
	}

	// 设置传感器表字段
	sensorFields := map[string]any{
		"id":           "",
		"device_id":    "",
		"name":         "",
		"type":         "",
		"unit":         "",
		"min_value":    0.0,
		"max_value":    0.0,
		"threshold":    0.0,
		"last_value":   0.0,
		"last_updated": time.Time{},
		"enabled":      false,
	}
	err = sensorTable.SetFields(sensorFields)
	if err != nil {
		return fmt.Errorf("failed to set sensors table fields: %v", err)
	}

	// 创建传感器表主键
	sensorPK, err := engine.DefaultPrimaryKeyNew("pk")
	if err != nil {
		return fmt.Errorf("failed to create sensors table primary key: %v", err)
	}
	sensorPK.AddFields("id")
	err = sensorTable.CreateIndex(sensorPK)
	if err != nil {
		return fmt.Errorf("failed to create sensors table index: %v", err)
	}
	sm.sensorTable = sensorTable

	// 创建传感器数据表（时序数据）
	dataTable, err := engine.TableNew("sensor_data")
	if err != nil {
		return fmt.Errorf("failed to create sensor_data table: %v", err)
	}

	// 设置传感器数据表字段
	dataFields := map[string]any{
		"id":               "",
		"device_id":        "",
		"sensor_id":        "",
		"value":            0.0,
		"timestamp":        time.Time{},
		"quality":          0,
		"raw_data":         "",
		"compressed_data":  []byte{},
		"start_time":       time.Time{},
		"interval":         time.Duration(0),
		"compression_type": "",
	}
	err = dataTable.SetFields(dataFields)
	if err != nil {
		return fmt.Errorf("failed to set sensor_data table fields: %v", err)
	}

	// 创建传感器数据表主键
	dataPK, err := engine.DefaultPrimaryKeyNew("pk")
	if err != nil {
		return fmt.Errorf("failed to create sensor_data table primary key: %v", err)
	}
	dataPK.AddFields("id")
	err = dataTable.CreateIndex(dataPK)
	if err != nil {
		return fmt.Errorf("failed to create sensor_data table index: %v", err)
	}

	// 创建device_id和sensor_id的索引，以提高查询性能
	deviceSensorIndex, err := engine.DefaultNormalIndexNew("device_sensor_idx")
	if err != nil {
		return fmt.Errorf("failed to create device_sensor index: %v", err)
	}
	deviceSensorIndex.AddFields("device_id")
	deviceSensorIndex.AddFields("sensor_id")
	err = dataTable.CreateIndex(deviceSensorIndex)
	if err != nil {
		return fmt.Errorf("failed to create device_sensor index: %v", err)
	}

	sm.dataTable = dataTable

	return nil
}

// StoreDevice 存储设备信息
func (sm *StorageManager) StoreDevice(device *Device) error {
	record := map[string]any{
		"id":               device.ID,
		"name":             device.Name,
		"type":             device.Type,
		"location":         device.Location,
		"status":           string(device.Status),
		"last_seen":        device.LastSeen,
		"ip_address":       device.IPAddress,
		"mac_address":      device.MacAddress,
		"firmware_version": device.FirmwareVersion,
	}

	_, err := sm.deviceTable.Insert(&record)
	if err != nil {
		return fmt.Errorf("failed to store device: %v", err)
	}

	return nil
}

// StoreSensor 存储传感器信息
func (sm *StorageManager) StoreSensor(sensor *Sensor) error {
	record := map[string]any{
		"id":           sensor.ID,
		"device_id":    sensor.DeviceID,
		"name":         sensor.Name,
		"type":         sensor.Type,
		"unit":         sensor.Unit,
		"min_value":    sensor.MinValue,
		"max_value":    sensor.MaxValue,
		"threshold":    sensor.Threshold,
		"last_value":   sensor.LastValue,
		"last_updated": sensor.LastUpdated,
		"enabled":      sensor.Enabled,
	}

	_, err := sm.sensorTable.Insert(&record)
	if err != nil {
		return fmt.Errorf("failed to store sensor: %v", err)
	}

	return nil
}

// StoreSensorData 存储单个传感器数据
func (sm *StorageManager) StoreSensorData(data *SensorData) error {
	record := map[string]any{
		"id":        data.ID,
		"device_id": data.DeviceID,
		"sensor_id": data.SensorID,
		"value":     data.Value,
		"timestamp": data.Timestamp,
		"quality":   data.Quality,
		"raw_data":  data.RawData,
	}

	_, err := sm.dataTable.Insert(&record)
	if err != nil {
		return fmt.Errorf("failed to store sensor data: %v", err)
	}

	return nil
}

// StoreSensorDataBatch 批量存储传感器数据
func (sm *StorageManager) StoreSensorDataBatch(data []*SensorData) error {
	if len(data) == 0 {
		return nil
	}

	// 构建批量插入记录
	records := make([]*map[string]any, len(data))
	for i, item := range data {
		record := map[string]any{
			"id":        item.ID,
			"device_id": item.DeviceID,
			"sensor_id": item.SensorID,
			"value":     item.Value,
			"timestamp": item.Timestamp,
			"quality":   item.Quality,
			"raw_data":  item.RawData,
		}
		records[i] = &record
	}

	// 使用 sfsDb 的批量插入 API
	_, err := sm.dataTable.BatchInsert(records) //_, err := sm.dataTable.BatchInsertNoInc(records,true) // 不自动递增主键，性能更好
	if err != nil {
		return fmt.Errorf("failed to batch store sensor data: %v", err)
	}

	fmt.Printf("Stored %d sensor data records in batch\n", len(data))
	return nil
}

// StoreSensorDataBatchWithSize 带大小参数的批量存储传感器数据
func (sm *StorageManager) StoreSensorDataBatchWithSize(data []*SensorData, batchSize int) error {
	if len(data) == 0 {
		return nil
	}

	// 构建批量插入记录
	records := make([]*map[string]any, len(data))
	for i, item := range data {
		record := map[string]any{
			"id":        item.ID,
			"device_id": item.DeviceID,
			"sensor_id": item.SensorID,
			"value":     item.Value,
			"timestamp": item.Timestamp,
			"quality":   item.Quality,
			"raw_data":  item.RawData,
		}
		records[i] = &record
	}

	// 使用 sfsDb 的带大小参数的批量插入 API
	_, err := sm.dataTable.BatchInsertWithSize(records, batchSize) //_, err := sm.dataTable.BatchInsertNoInc(records,true) // 不自动递增主键，性能更好
	if err != nil {
		return fmt.Errorf("failed to batch store sensor data with size: %v", err)
	}

	fmt.Printf("Stored %d sensor data records in batch with size %d\n", len(data), batchSize)
	return nil
}

// QuerySensorData 查询传感器数据
func (sm *StorageManager) QuerySensorData(deviceID, sensorID string, startTime, endTime time.Time, limit int) ([]*SensorData, error) {
	// 构建查询条件
	conditions := map[string]any{}

	if deviceID != "" {
		conditions["device_id"] = deviceID
	}

	if sensorID != "" {
		conditions["sensor_id"] = sensorID
	}

	// 执行查询
	iter, err := sm.dataTable.Search(&conditions)
	if err != nil {
		return nil, fmt.Errorf("failed to query sensor data: %v", err)
	}
	defer iter.Release()

	// 处理结果
	records := iter.GetRecords(true)
	defer records.Release()

	result := make([]*SensorData, 0, len(records))
	count := 0

	for _, record := range records {
		if count >= limit && limit > 0 {
			break
		}

		// 检查时间范围
		timestamp := record["timestamp"].(time.Time)
		if timestamp.Before(startTime) || timestamp.After(endTime) {
			continue
		}

		data := &SensorData{
			ID:        record["id"].(string),
			DeviceID:  record["device_id"].(string),
			SensorID:  record["sensor_id"].(string),
			Value:     record["value"].(float64),
			Timestamp: timestamp,
			Quality:   record["quality"].(int),
		}

		if rawData, ok := record["raw_data"].(string); ok {
			data.RawData = rawData
		}

		result = append(result, data)
		count++
	}

	return result, nil
}

// QuerySensorDataWithAggregation 带聚合的传感器数据查询
func (sm *StorageManager) QuerySensorDataWithAggregation(deviceID, sensorID string, startTime, endTime time.Time, granularity sfstime.TimeGranularity, aggregationType string) ([]sfstime.TimeAggregationResult, error) {
	// 构建时间范围查询选项
	options := sfstime.NewTimeRangeQueryOptions("timestamp", startTime, endTime, granularity)

	// 执行带聚合的查询
	results, err := sfstime.TimeRangeQueryWithAggregation(sm.dataTable, options, "value", aggregationType)
	if err != nil {
		return nil, fmt.Errorf("failed to query sensor data with aggregation: %v", err)
	}

	return results, nil
}

// GetDevice 获取设备信息
func (sm *StorageManager) GetDevice(deviceID string) (*Device, error) {
	// 查询设备
	conditions := map[string]any{
		"id": deviceID,
	}
	iter, err := sm.deviceTable.Search(&conditions)
	if err != nil {
		return nil, fmt.Errorf("failed to query device: %v", err)
	}
	defer iter.Release()

	// 处理结果
	records := iter.GetRecords(true)
	defer records.Release()

	if len(records) == 0 {
		return nil, fmt.Errorf("device not found: %s", deviceID)
	}

	record := records[0]
	device := &Device{
		ID:              record["id"].(string),
		Name:            record["name"].(string),
		Type:            record["type"].(string),
		Location:        record["location"].(string),
		Status:          DeviceStatus(record["status"].(string)),
		LastSeen:        record["last_seen"].(time.Time),
		IPAddress:       record["ip_address"].(string),
		MacAddress:      record["mac_address"].(string),
		FirmwareVersion: record["firmware_version"].(string),
		Sensors:         []*Sensor{},
	}

	return device, nil
}

// GetSensor 获取传感器信息
func (sm *StorageManager) GetSensor(sensorID string) (*Sensor, error) {
	// 查询传感器
	conditions := map[string]any{
		"id": sensorID,
	}
	iter, err := sm.sensorTable.Search(&conditions)
	if err != nil {
		return nil, fmt.Errorf("failed to query sensor: %v", err)
	}
	defer iter.Release()

	// 处理结果
	records := iter.GetRecords(true)
	defer records.Release()

	if len(records) == 0 {
		return nil, fmt.Errorf("sensor not found: %s", sensorID)
	}

	record := records[0]
	sensor := &Sensor{
		ID:          record["id"].(string),
		DeviceID:    record["device_id"].(string),
		Name:        record["name"].(string),
		Type:        record["type"].(string),
		Unit:        record["unit"].(string),
		MinValue:    record["min_value"].(float64),
		MaxValue:    record["max_value"].(float64),
		Threshold:   record["threshold"].(float64),
		LastValue:   record["last_value"].(float64),
		LastUpdated: record["last_updated"].(time.Time),
		Enabled:     record["enabled"].(bool),
	}

	return sensor, nil
}

// GetSensorsByDevice 获取设备的所有传感器
func (sm *StorageManager) GetSensorsByDevice(deviceID string) ([]*Sensor, error) {
	// 查询传感器
	conditions := map[string]any{
		"device_id": deviceID,
	}
	iter, err := sm.sensorTable.Search(&conditions)
	if err != nil {
		return nil, fmt.Errorf("failed to query sensors: %v", err)
	}
	defer iter.Release()

	// 处理结果
	records := iter.GetRecords(true)
	defer records.Release()

	result := make([]*Sensor, 0, len(records))
	for _, record := range records {
		sensor := &Sensor{
			ID:          record["id"].(string),
			DeviceID:    record["device_id"].(string),
			Name:        record["name"].(string),
			Type:        record["type"].(string),
			Unit:        record["unit"].(string),
			MinValue:    record["min_value"].(float64),
			MaxValue:    record["max_value"].(float64),
			Threshold:   record["threshold"].(float64),
			LastValue:   record["last_value"].(float64),
			LastUpdated: record["last_updated"].(time.Time),
			Enabled:     record["enabled"].(bool),
		}
		result = append(result, sensor)
	}

	return result, nil
}

// Close 关闭存储管理器
func (sm *StorageManager) Close() error {
	// 关闭数据库
	dbManager := storage.GetDBManager()
	err := dbManager.CloseDB()
	if err != nil {
		return fmt.Errorf("failed to close database: %v", err)
	}

	return nil
}

// GetStats 获取存储统计信息
func (sm *StorageManager) GetStats() (map[string]interface{}, error) {
	// 构建统计信息
	stats := map[string]interface{}{
		"path":             sm.path,
		"use_compression":  sm.useCompression,
		"compression_type": sm.compressionType,
		"tables": map[string]interface{}{
			"devices": map[string]interface{}{
				"name": "devices",
			},
			"sensors": map[string]interface{}{
				"name": "sensors",
			},
			"sensor_data": map[string]interface{}{
				"name": "sensor_data",
			},
		},
	}

	return stats, nil
}

// CompressSensorData 压缩传感器数据
func (sm *StorageManager) CompressSensorData(data []*SensorData) (*sfstime.CompressedTimeSeries, error) {
	if !sm.useCompression || len(data) == 0 {
		return nil, nil
	}

	// 转换为 TimeSeriesPoint
	points := make([]sfstime.TimeSeriesPoint, len(data))
	for i, item := range data {
		points[i] = sfstime.TimeSeriesPoint{
			Time:  item.Timestamp,
			Value: item.Value,
		}
	}

	// 计算时间间隔
	var interval time.Duration
	if len(points) > 1 {
		interval = points[1].Time.Sub(points[0].Time)
	}

	// 压缩数据
	compressed, err := sfstime.CompressTimeSeries(points, sm.compressionType, interval)
	if err != nil {
		return nil, fmt.Errorf("failed to compress sensor data: %v", err)
	}

	return compressed, nil
}

// DecompressSensorData 解压缩传感器数据
func (sm *StorageManager) DecompressSensorData(compressed *sfstime.CompressedTimeSeries, count int) ([]sfstime.TimeSeriesPoint, error) {
	if compressed == nil {
		return []sfstime.TimeSeriesPoint{}, nil
	}

	// 解压缩数据
	points, err := sfstime.DecompressTimeSeries(compressed, count)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress sensor data: %v", err)
	}

	return points, nil
}

// StoreCompressedSensorData 存储压缩后的传感器数据
func (sm *StorageManager) StoreCompressedSensorData(deviceID, sensorID string, compressed *sfstime.CompressedTimeSeries) error {
	if compressed == nil {
		return nil
	}

	// 构建记录
	record := map[string]any{
		"id":               fmt.Sprintf("%s_%s_%d", deviceID, sensorID, time.Now().UnixNano()),
		"device_id":        deviceID,
		"sensor_id":        sensorID,
		"compressed_data":  compressed.CompressedValues,
		"start_time":       compressed.StartTime,
		"interval":         compressed.Interval,
		"compression_type": compressed.CompressionType,
		"timestamp":        time.Now(),
	}

	// 插入记录
	_, err := sm.dataTable.Insert(&record)
	if err != nil {
		return fmt.Errorf("failed to store compressed sensor data: %v", err)
	}

	return nil
}

// QueryCompressedSensorData 查询压缩后的传感器数据
func (sm *StorageManager) QueryCompressedSensorData(deviceID, sensorID string, startTime, endTime time.Time) ([]*sfstime.CompressedTimeSeries, error) {
	// 构建查询条件
	conditions := map[string]any{
		"device_id": deviceID,
		"sensor_id": sensorID,
	}

	// 执行查询
	iter, err := sm.dataTable.Search(&conditions)
	if err != nil {
		return nil, fmt.Errorf("failed to query compressed sensor data: %v", err)
	}
	defer iter.Release()

	// 处理结果
	records := iter.GetRecords(true)
	defer records.Release()

	result := make([]*sfstime.CompressedTimeSeries, 0)

	for _, record := range records {
		// 检查时间范围
		timestamp := record["timestamp"].(time.Time)
		if timestamp.Before(startTime) || timestamp.After(endTime) {
			continue
		}

		// 提取压缩数据
		compressedValues := record["compressed_data"].([]byte)
		startTime := record["start_time"].(time.Time)
		interval := record["interval"].(time.Duration)
		compressionType := record["compression_type"].(string)

		// 构建压缩时间序列
		cts := &sfstime.CompressedTimeSeries{
			StartTime:        startTime,
			Interval:         interval,
			CompressedValues: compressedValues,
			CompressionType:  compressionType,
		}

		result = append(result, cts)
	}

	return result, nil
}
