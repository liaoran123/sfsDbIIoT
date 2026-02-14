package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// API API服务结构体
type API struct {
	port   string
	cors   bool
	server *http.Server
}

// NewAPI 创建API服务
func NewAPI(port string, cors bool) *API {
	return &API{
		port: port,
		cors: cors,
	}
}

// Start 启动API服务
func (api *API) Start() error {
	mux := http.NewServeMux()

	// 注册路由
	mux.HandleFunc("/api/devices", api.handleDevices)
	mux.HandleFunc("/api/devices/", api.handleDevice)
	mux.HandleFunc("/api/sensors", api.handleSensors)
	mux.HandleFunc("/api/sensors/", api.handleSensor)
	mux.HandleFunc("/api/data", api.handleSensorData)
	mux.HandleFunc("/api/alerts", api.handleAlerts)
	mux.HandleFunc("/api/alerts/", api.handleAlert)
	mux.HandleFunc("/api/stats", api.handleStats)
	mux.HandleFunc("/api/health", api.handleHealth)

	// 创建服务器
	api.server = &http.Server{
		Addr:    fmt.Sprintf(":%s", api.port),
		Handler: mux,
	}

	fmt.Printf("API server starting on port %s\n", api.port)
	return api.server.ListenAndServe()
}

// Stop 停止API服务
func (api *API) Stop() error {
	if api.server != nil {
		return api.server.Close()
	}
	return nil
}

// handleDevices 处理设备列表请求
func (api *API) handleDevices(w http.ResponseWriter, r *http.Request) {
	api.setCORSHeaders(w)

	switch r.Method {
	case http.MethodGet:
		// 获取所有设备
		devices := DeviceManagerInstance.GetAllDevices()
		api.sendJSON(w, http.StatusOK, devices)

	case http.MethodPost:
		// 注册新设备
		var device Device
		if err := json.NewDecoder(r.Body).Decode(&device); err != nil {
			api.sendError(w, http.StatusBadRequest, fmt.Sprintf("Invalid request: %v", err))
			return
		}

		err := DeviceManagerInstance.RegisterDevice(&device)
		if err != nil {
			api.sendError(w, http.StatusBadRequest, fmt.Sprintf("Failed to register device: %v", err))
			return
		}

		api.sendJSON(w, http.StatusCreated, device)

	default:
		api.sendError(w, http.StatusMethodNotAllowed, "Method not allowed")
	}
}

// handleDevice 处理单个设备请求
func (api *API) handleDevice(w http.ResponseWriter, r *http.Request) {
	api.setCORSHeaders(w)

	// 提取设备ID
	deviceID := r.URL.Path[len("/api/devices/"):]
	if deviceID == "" {
		api.sendError(w, http.StatusBadRequest, "Device ID is required")
		return
	}

	switch r.Method {
	case http.MethodGet:
		// 获取设备信息
		device, err := DeviceManagerInstance.GetDevice(deviceID)
		if err != nil {
			api.sendError(w, http.StatusNotFound, fmt.Sprintf("Device not found: %v", err))
			return
		}
		api.sendJSON(w, http.StatusOK, device)

	case http.MethodPut:
		// 更新设备信息
		var device Device
		if err := json.NewDecoder(r.Body).Decode(&device); err != nil {
			api.sendError(w, http.StatusBadRequest, fmt.Sprintf("Invalid request: %v", err))
			return
		}

		device.ID = deviceID
		err := DeviceManagerInstance.UpdateDevice(&device)
		if err != nil {
			api.sendError(w, http.StatusBadRequest, fmt.Sprintf("Failed to update device: %v", err))
			return
		}

		api.sendJSON(w, http.StatusOK, device)

	case http.MethodDelete:
		// 删除设备
		err := DeviceManagerInstance.DeleteDevice(deviceID)
		if err != nil {
			api.sendError(w, http.StatusBadRequest, fmt.Sprintf("Failed to delete device: %v", err))
			return
		}

		api.sendJSON(w, http.StatusOK, map[string]string{"message": "Device deleted successfully"})

	default:
		api.sendError(w, http.StatusMethodNotAllowed, "Method not allowed")
	}
}

// handleSensors 处理传感器列表请求
func (api *API) handleSensors(w http.ResponseWriter, r *http.Request) {
	api.setCORSHeaders(w)

	if r.Method == http.MethodGet {
		// 获取所有传感器
		devices := DeviceManagerInstance.GetAllDevices()
		var sensors []*Sensor

		for _, device := range devices {
			device.sensorMutex.RLock()
			sensors = append(sensors, device.Sensors...)
			device.sensorMutex.RUnlock()
		}

		api.sendJSON(w, http.StatusOK, sensors)
	} else {
		api.sendError(w, http.StatusMethodNotAllowed, "Method not allowed")
	}
}

// handleSensor 处理单个传感器请求
func (api *API) handleSensor(w http.ResponseWriter, r *http.Request) {
	api.setCORSHeaders(w)

	// 提取传感器ID
	sensorID := r.URL.Path[len("/api/sensors/"):]
	if sensorID == "" {
		api.sendError(w, http.StatusBadRequest, "Sensor ID is required")
		return
	}

	if r.Method == http.MethodGet {
		// 查找传感器
		devices := DeviceManagerInstance.GetAllDevices()
		var foundSensor *Sensor

		for _, device := range devices {
			device.sensorMutex.RLock()
			for _, sensor := range device.Sensors {
				if sensor.ID == sensorID {
					foundSensor = sensor
					break
				}
			}
			device.sensorMutex.RUnlock()
			if foundSensor != nil {
				break
			}
		}

		if foundSensor == nil {
			api.sendError(w, http.StatusNotFound, "Sensor not found")
			return
		}

		api.sendJSON(w, http.StatusOK, foundSensor)
	} else {
		api.sendError(w, http.StatusMethodNotAllowed, "Method not allowed")
	}
}

// handleSensorData 处理传感器数据请求
func (api *API) handleSensorData(w http.ResponseWriter, r *http.Request) {
	api.setCORSHeaders(w)

	switch r.Method {
	case http.MethodGet:
		// 获取查询参数
		deviceID := r.URL.Query().Get("device_id")
		sensorID := r.URL.Query().Get("sensor_id")
		startTimeStr := r.URL.Query().Get("start_time")
		endTimeStr := r.URL.Query().Get("end_time")

		// 解析时间参数
		var startTime, endTime time.Time
		var err error

		if startTimeStr != "" {
			startTime, err = time.Parse(time.RFC3339, startTimeStr)
			if err != nil {
				api.sendError(w, http.StatusBadRequest, "Invalid start_time format")
				return
			}
		} else {
			startTime = time.Now().Add(-24 * time.Hour)
		}

		if endTimeStr != "" {
			endTime, err = time.Parse(time.RFC3339, endTimeStr)
			if err != nil {
				api.sendError(w, http.StatusBadRequest, "Invalid end_time format")
				return
			}
		} else {
			endTime = time.Now()
		}

		// 解析限制参数
		limit := 1000

		// 查询传感器数据
		data, err := StorageManagerInstance.QuerySensorData(deviceID, sensorID, startTime, endTime, limit)
		if err != nil {
			api.sendError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to query sensor data: %v", err))
			return
		}

		api.sendJSON(w, http.StatusOK, data)

	case http.MethodPost:
		// 提交传感器数据
		var data SensorData
		if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
			api.sendError(w, http.StatusBadRequest, fmt.Sprintf("Invalid request: %v", err))
			return
		}

		err := SensorDataProcessorInstance.ProcessSensorData(&data)
		if err != nil {
			api.sendError(w, http.StatusBadRequest, fmt.Sprintf("Failed to process sensor data: %v", err))
			return
		}

		api.sendJSON(w, http.StatusCreated, data)

	default:
		api.sendError(w, http.StatusMethodNotAllowed, "Method not allowed")
	}
}

// handleAlerts 处理告警列表请求
func (api *API) handleAlerts(w http.ResponseWriter, r *http.Request) {
	api.setCORSHeaders(w)

	switch r.Method {
	case http.MethodGet:
		// 获取所有告警
		status := r.URL.Query().Get("status")
		var alerts []*Alert

		if status != "" {
			alerts = AlertManagerInstance.GetAlerts(AlertStatus(status))
		} else {
			alerts = AlertManagerInstance.GetAlerts()
		}

		api.sendJSON(w, http.StatusOK, alerts)

	default:
		api.sendError(w, http.StatusMethodNotAllowed, "Method not allowed")
	}
}

// handleAlert 处理单个告警请求
func (api *API) handleAlert(w http.ResponseWriter, r *http.Request) {
	api.setCORSHeaders(w)

	// 提取告警ID
	alertID := r.URL.Path[len("/api/alerts/"):]
	if alertID == "" {
		api.sendError(w, http.StatusBadRequest, "Alert ID is required")
		return
	}

	switch r.Method {
	case http.MethodGet:
		// 获取告警信息
		alert, err := AlertManagerInstance.GetAlert(alertID)
		if err != nil {
			api.sendError(w, http.StatusNotFound, fmt.Sprintf("Alert not found: %v", err))
			return
		}
		api.sendJSON(w, http.StatusOK, alert)

	case http.MethodPut:
		// 解决告警
		err := AlertManagerInstance.ResolveAlert(alertID)
		if err != nil {
			api.sendError(w, http.StatusBadRequest, fmt.Sprintf("Failed to resolve alert: %v", err))
			return
		}

		api.sendJSON(w, http.StatusOK, map[string]string{"message": "Alert resolved successfully"})

	default:
		api.sendError(w, http.StatusMethodNotAllowed, "Method not allowed")
	}
}

// handleStats 处理统计信息请求
func (api *API) handleStats(w http.ResponseWriter, r *http.Request) {
	api.setCORSHeaders(w)

	if r.Method == http.MethodGet {
		// 获取设备统计
		deviceCount := DeviceManagerInstance.GetDeviceCount()
		sensorCount := DeviceManagerInstance.GetSensorCount()

		// 获取告警统计
		alertStats := AlertManagerInstance.GetAlertStats()

		// 获取存储统计
		storageStats, err := StorageManagerInstance.GetStats()
		if err != nil {
			storageStats = map[string]interface{}{}
		}

		// 获取处理统计
		processingStats := SensorDataProcessorInstance.GetProcessingStats()

		// 构建统计信息
		stats := map[string]interface{}{
			"devices":    deviceCount,
			"sensors":    sensorCount,
			"alerts":     alertStats,
			"storage":    storageStats,
			"processing": processingStats,
			"timestamp":  time.Now(),
		}

		api.sendJSON(w, http.StatusOK, stats)
	} else {
		api.sendError(w, http.StatusMethodNotAllowed, "Method not allowed")
	}
}

// handleHealth 处理健康检查请求
func (api *API) handleHealth(w http.ResponseWriter, r *http.Request) {
	api.setCORSHeaders(w)

	health := map[string]interface{}{
		"status":    "healthy",
		"timestamp": time.Now(),
		"service":   "sfsDbIIoT",
	}

	api.sendJSON(w, http.StatusOK, health)
}

// setCORSHeaders 设置CORS头
func (api *API) setCORSHeaders(w http.ResponseWriter) {
	if api.cors {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
	}
}

// sendJSON 发送JSON响应
func (api *API) sendJSON(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(data)
}

// sendError 发送错误响应
func (api *API) sendError(w http.ResponseWriter, statusCode int, message string) {
	api.sendJSON(w, statusCode, map[string]string{"error": message})
}
