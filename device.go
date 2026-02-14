package main

import (
	"fmt"
	"sync"
	"time"
)

// DeviceStatus 设备状态枚举
type DeviceStatus string

const (
	DeviceStatusOnline  DeviceStatus = "online"
	DeviceStatusOffline DeviceStatus = "offline"
	DeviceStatusError   DeviceStatus = "error"
	DeviceStatusUnknown DeviceStatus = "unknown"
)

// Device 设备结构体
type Device struct {
	ID          string       `json:"id"`
	Name        string       `json:"name"`
	Type        string       `json:"type"`
	Location    string       `json:"location"`
	Status      DeviceStatus `json:"status"`
	LastSeen    time.Time    `json:"last_seen"`
	IPAddress   string       `json:"ip_address"`
	MacAddress  string       `json:"mac_address"`
	FirmwareVersion string    `json:"firmware_version"`
	Sensors     []*Sensor    `json:"sensors"`
	sensorMutex sync.RWMutex
}

// Sensor 传感器结构体
type Sensor struct {
	ID          string    `json:"id"`
	DeviceID    string    `json:"device_id"`
	Name        string    `json:"name"`
	Type        string    `json:"type"`
	Unit        string    `json:"unit"`
	MinValue    float64   `json:"min_value"`
	MaxValue    float64   `json:"max_value"`
	Threshold   float64   `json:"threshold"`
	LastValue   float64   `json:"last_value"`
	LastUpdated time.Time `json:"last_updated"`
	Enabled     bool      `json:"enabled"`
}

// DeviceManager 设备管理器
type DeviceManager struct {
	devices     map[string]*Device
	devicesMutex sync.RWMutex
	maxDevices  int
	scanInterval int
}

// NewDeviceManager 创建设备管理器
func NewDeviceManager(maxDevices, scanInterval int) *DeviceManager {
	return &DeviceManager{
		devices:     make(map[string]*Device),
		maxDevices:  maxDevices,
		scanInterval: scanInterval,
	}
}

// RegisterDevice 注册新设备
func (dm *DeviceManager) RegisterDevice(device *Device) error {
	dm.devicesMutex.Lock()
	defer dm.devicesMutex.Unlock()
	
	// 检查设备数量是否超过限制
	if len(dm.devices) >= dm.maxDevices {
		return fmt.Errorf("maximum number of devices reached: %d", dm.maxDevices)
	}
	
	// 检查设备ID是否已存在
	if _, exists := dm.devices[device.ID]; exists {
		return fmt.Errorf("device with ID %s already exists", device.ID)
	}
	
	// 设置设备默认值
	if device.Status == "" {
		device.Status = DeviceStatusUnknown
	}
	
	if device.LastSeen.IsZero() {
		device.LastSeen = time.Now()
	}
	
	// 初始化传感器列表
	if device.Sensors == nil {
		device.Sensors = []*Sensor{}
	}
	
	// 注册设备
	dm.devices[device.ID] = device
	fmt.Printf("Device registered: %s (%s)\n", device.Name, device.ID)
	return nil
}

// GetDevice 获取设备
func (dm *DeviceManager) GetDevice(deviceID string) (*Device, error) {
	dm.devicesMutex.RLock()
	defer dm.devicesMutex.RUnlock()
	
	device, exists := dm.devices[deviceID]
	if !exists {
		return nil, fmt.Errorf("device not found: %s", deviceID)
	}
	
	return device, nil
}

// GetAllDevices 获取所有设备
func (dm *DeviceManager) GetAllDevices() []*Device {
	dm.devicesMutex.RLock()
	defer dm.devicesMutex.RUnlock()
	
	devices := make([]*Device, 0, len(dm.devices))
	for _, device := range dm.devices {
		devices = append(devices, device)
	}
	
	return devices
}

// UpdateDevice 更新设备信息
func (dm *DeviceManager) UpdateDevice(device *Device) error {
	dm.devicesMutex.Lock()
	defer dm.devicesMutex.Unlock()
	
	// 检查设备是否存在
	existingDevice, exists := dm.devices[device.ID]
	if !exists {
		return fmt.Errorf("device not found: %s", device.ID)
	}
	
	// 更新设备信息
	existingDevice.Name = device.Name
	existingDevice.Type = device.Type
	existingDevice.Location = device.Location
	existingDevice.Status = device.Status
	existingDevice.LastSeen = time.Now()
	existingDevice.IPAddress = device.IPAddress
	existingDevice.MacAddress = device.MacAddress
	existingDevice.FirmwareVersion = device.FirmwareVersion
	
	fmt.Printf("Device updated: %s (%s)\n", device.Name, device.ID)
	return nil
}

// DeleteDevice 删除设备
func (dm *DeviceManager) DeleteDevice(deviceID string) error {
	dm.devicesMutex.Lock()
	defer dm.devicesMutex.Unlock()
	
	// 检查设备是否存在
	_, exists := dm.devices[deviceID]
	if !exists {
		return fmt.Errorf("device not found: %s", deviceID)
	}
	
	// 删除设备
	delete(dm.devices, deviceID)
	fmt.Printf("Device deleted: %s\n", deviceID)
	return nil
}

// UpdateDeviceStatus 更新设备状态
func (dm *DeviceManager) UpdateDeviceStatus(deviceID string, status DeviceStatus) error {
	dm.devicesMutex.Lock()
	defer dm.devicesMutex.Unlock()
	
	// 检查设备是否存在
	device, exists := dm.devices[deviceID]
	if !exists {
		return fmt.Errorf("device not found: %s", deviceID)
	}
	
	// 更新设备状态
	device.Status = status
	device.LastSeen = time.Now()
	
	fmt.Printf("Device status updated: %s (%s) - %s\n", device.Name, device.ID, status)
	return nil
}

// AddSensor 向设备添加传感器
func (dm *DeviceManager) AddSensor(deviceID string, sensor *Sensor) error {
	dm.devicesMutex.Lock()
	defer dm.devicesMutex.Unlock()
	
	// 检查设备是否存在
	device, exists := dm.devices[deviceID]
	if !exists {
		return fmt.Errorf("device not found: %s", deviceID)
	}
	
	// 检查传感器数量是否超过限制
	config := GetConfig()
	if len(device.Sensors) >= config.Sensor.MaxSensorsPerDevice {
		return fmt.Errorf("maximum number of sensors per device reached: %d", config.Sensor.MaxSensorsPerDevice)
	}
	
	// 设置传感器默认值
	sensor.DeviceID = deviceID
	if sensor.Enabled == false {
		sensor.Enabled = true
	}
	
	// 添加传感器
	device.sensorMutex.Lock()
	device.Sensors = append(device.Sensors, sensor)
	device.sensorMutex.Unlock()
	
	fmt.Printf("Sensor added to device %s: %s (%s)\n", deviceID, sensor.Name, sensor.ID)
	return nil
}

// GetSensor 获取传感器
func (dm *DeviceManager) GetSensor(deviceID, sensorID string) (*Sensor, error) {
	dm.devicesMutex.RLock()
	defer dm.devicesMutex.RUnlock()
	
	// 检查设备是否存在
	device, exists := dm.devices[deviceID]
	if !exists {
		return nil, fmt.Errorf("device not found: %s", deviceID)
	}
	
	// 查找传感器
	device.sensorMutex.RLock()
	defer device.sensorMutex.RUnlock()
	
	for _, sensor := range device.Sensors {
		if sensor.ID == sensorID {
			return sensor, nil
		}
	}
	
	return nil, fmt.Errorf("sensor not found: %s on device %s", sensorID, deviceID)
}

// UpdateSensorValue 更新传感器值
func (dm *DeviceManager) UpdateSensorValue(deviceID, sensorID string, value float64) error {
	dm.devicesMutex.RLock()
	device, exists := dm.devices[deviceID]
	if !exists {
		dm.devicesMutex.RUnlock()
		return fmt.Errorf("device not found: %s", deviceID)
	}
	
	device.sensorMutex.Lock()
	defer func() {
		device.sensorMutex.Unlock()
		dm.devicesMutex.RUnlock()
	}()
	
	// 查找传感器
	for _, sensor := range device.Sensors {
		if sensor.ID == sensorID {
			// 更新传感器值
			sensor.LastValue = value
			sensor.LastUpdated = time.Now()
			
			// 检查是否超过阈值
			if sensor.Enabled && value > sensor.Threshold {
				// 触发告警
				go func() {
					alert := &Alert{
						ID:        fmt.Sprintf("alert_%d", time.Now().UnixNano()),
						DeviceID:  deviceID,
						SensorID:  sensorID,
						Type:      "threshold",
						Message:   fmt.Sprintf("Sensor %s on device %s exceeded threshold: %f > %f", sensor.Name, device.Name, value, sensor.Threshold),
						Severity:  "warning",
						Timestamp: time.Now(),
						Status:    "active",
					}
					AlertManagerInstance.AddAlert(alert)
				}()
			}
			
			return nil
		}
	}
	
	return fmt.Errorf("sensor not found: %s on device %s", sensorID, deviceID)
}

// RemoveSensor 从设备移除传感器
func (dm *DeviceManager) RemoveSensor(deviceID, sensorID string) error {
	dm.devicesMutex.Lock()
	defer dm.devicesMutex.Unlock()
	
	// 检查设备是否存在
	device, exists := dm.devices[deviceID]
	if !exists {
		return fmt.Errorf("device not found: %s", deviceID)
	}
	
	// 查找并移除传感器
	device.sensorMutex.Lock()
	defer device.sensorMutex.Unlock()
	
	for i, sensor := range device.Sensors {
		if sensor.ID == sensorID {
			// 移除传感器
			device.Sensors = append(device.Sensors[:i], device.Sensors[i+1:]...)
			fmt.Printf("Sensor removed from device %s: %s (%s)\n", deviceID, sensor.Name, sensor.ID)
			return nil
		}
	}
	
	return fmt.Errorf("sensor not found: %s on device %s", sensorID, deviceID)
}

// StartDeviceScan 启动设备扫描
func (dm *DeviceManager) StartDeviceScan() {
	go func() {
		ticker := time.NewTicker(time.Duration(dm.scanInterval) * time.Second)
		defer ticker.Stop()
		
		for {
			<-ticker.C
			dm.scanDevices()
		}
	}()
}

// scanDevices 扫描设备状态
func (dm *DeviceManager) scanDevices() {
	dm.devicesMutex.RLock()
	devices := make([]*Device, 0, len(dm.devices))
	for _, device := range dm.devices {
		devices = append(devices, device)
	}
	dm.devicesMutex.RUnlock()
	
	for _, device := range devices {
		// 检查设备是否离线
		if time.Since(device.LastSeen) > time.Duration(dm.scanInterval*2)*time.Second {
			dm.UpdateDeviceStatus(device.ID, DeviceStatusOffline)
		}
	}
}

// GetDeviceCount 获取设备数量
func (dm *DeviceManager) GetDeviceCount() int {
	dm.devicesMutex.RLock()
	defer dm.devicesMutex.RUnlock()
	return len(dm.devices)
}

// GetSensorCount 获取传感器数量
func (dm *DeviceManager) GetSensorCount() int {
	dm.devicesMutex.RLock()
	defer dm.devicesMutex.RUnlock()
	
	count := 0
	for _, device := range dm.devices {
		device.sensorMutex.RLock()
		count += len(device.Sensors)
		device.sensorMutex.RUnlock()
	}
	
	return count
}
