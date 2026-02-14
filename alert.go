package main

import (
	"fmt"
	"sync"
	"time"
)

// AlertSeverity 告警级别枚举
type AlertSeverity string

const (
	AlertSeverityInfo    AlertSeverity = "info"
	AlertSeverityWarning AlertSeverity = "warning"
	AlertSeverityError   AlertSeverity = "error"
	AlertSeverityCritical AlertSeverity = "critical"
)

// AlertStatus 告警状态枚举
type AlertStatus string

const (
	AlertStatusActive   AlertStatus = "active"
	AlertStatusResolved AlertStatus = "resolved"
	AlertStatusSuppressed AlertStatus = "suppressed"
)

// Alert 告警结构体
type Alert struct {
	ID        string        `json:"id"`
	DeviceID  string        `json:"device_id"`
	SensorID  string        `json:"sensor_id"`
	Type      string        `json:"type"`
	Message   string        `json:"message"`
	Severity  AlertSeverity `json:"severity"`
	Timestamp time.Time     `json:"timestamp"`
	Status    AlertStatus   `json:"status"`
	ResolvedAt *time.Time   `json:"resolved_at"`
	Metadata  map[string]interface{} `json:"metadata"`
}

// AlertManager 告警管理器
type AlertManager struct {
	alerts        map[string]*Alert
	alertsMutex   sync.RWMutex
	checkInterval int
	notificationType string
	stopChan      chan struct{}
	isRunning     bool
	mutex         sync.Mutex
}

// NewAlertManager 创建告警管理器
func NewAlertManager(checkInterval int, notificationType string) *AlertManager {
	return &AlertManager{
		alerts:        make(map[string]*Alert),
		checkInterval: checkInterval,
		notificationType: notificationType,
		stopChan:      make(chan struct{}),
		isRunning:     false,
	}
}

// Start 启动告警管理器
func (am *AlertManager) Start() error {
	am.mutex.Lock()
	if am.isRunning {
		am.mutex.Unlock()
		return fmt.Errorf("alert manager is already running")
	}
	am.isRunning = true
	am.mutex.Unlock()
	
	go am.checkLoop()
	fmt.Println("Alert manager started")
	return nil
}

// Stop 停止告警管理器
func (am *AlertManager) Stop() error {
	am.mutex.Lock()
	if !am.isRunning {
		am.mutex.Unlock()
		return fmt.Errorf("alert manager is not running")
	}
	am.isRunning = false
	am.mutex.Unlock()
	
	close(am.stopChan)
	fmt.Println("Alert manager stopped")
	return nil
}

// checkLoop 检查循环
func (am *AlertManager) checkLoop() {
	ticker := time.NewTicker(time.Duration(am.checkInterval) * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			am.checkAlerts()
		case <-am.stopChan:
			return
		}
	}
}

// checkAlerts 检查告警状态
func (am *AlertManager) checkAlerts() {
	am.alertsMutex.RLock()
	alerts := make([]*Alert, 0, len(am.alerts))
	for _, alert := range am.alerts {
		alerts = append(alerts, alert)
	}
	am.alertsMutex.RUnlock()
	
	// 检查告警是否需要自动解决
	for _, alert := range alerts {
		if alert.Status == AlertStatusActive {
			// 这里可以添加自动解决逻辑
			// 例如：检查设备状态是否恢复正常
		}
	}
}

// AddAlert 添加新告警
func (am *AlertManager) AddAlert(alert *Alert) error {
	am.alertsMutex.Lock()
	defer am.alertsMutex.Unlock()
	
	// 检查告警是否已存在
	if _, exists := am.alerts[alert.ID]; exists {
		return fmt.Errorf("alert with ID %s already exists", alert.ID)
	}
	
	// 设置默认值
	if alert.Status == "" {
		alert.Status = AlertStatusActive
	}
	
	if alert.Timestamp.IsZero() {
		alert.Timestamp = time.Now()
	}
	
	if alert.Metadata == nil {
		alert.Metadata = make(map[string]interface{})
	}
	
	// 添加告警
	am.alerts[alert.ID] = alert
	
	// 发送通知
	am.notifyAlert(alert)
	
	fmt.Printf("Alert added: %s - %s (%s)\n", alert.ID, alert.Message, alert.Severity)
	return nil
}

// ResolveAlert 解决告警
func (am *AlertManager) ResolveAlert(alertID string) error {
	am.alertsMutex.Lock()
	defer am.alertsMutex.Unlock()
	
	// 检查告警是否存在
	alert, exists := am.alerts[alertID]
	if !exists {
		return fmt.Errorf("alert not found: %s", alertID)
	}
	
	// 检查告警状态
	if alert.Status != AlertStatusActive {
		return fmt.Errorf("alert is not active: %s", alertID)
	}
	
	// 更新告警状态
	alert.Status = AlertStatusResolved
	now := time.Now()
	alert.ResolvedAt = &now
	
	// 发送通知
	am.notifyAlertResolved(alert)
	
	fmt.Printf("Alert resolved: %s - %s\n", alertID, alert.Message)
	return nil
}

// SuppressAlert 抑制告警
func (am *AlertManager) SuppressAlert(alertID string) error {
	am.alertsMutex.Lock()
	defer am.alertsMutex.Unlock()
	
	// 检查告警是否存在
	alert, exists := am.alerts[alertID]
	if !exists {
		return fmt.Errorf("alert not found: %s", alertID)
	}
	
	// 检查告警状态
	if alert.Status != AlertStatusActive {
		return fmt.Errorf("alert is not active: %s", alertID)
	}
	
	// 更新告警状态
	alert.Status = AlertStatusSuppressed
	
	fmt.Printf("Alert suppressed: %s - %s\n", alertID, alert.Message)
	return nil
}

// GetAlert 获取告警
func (am *AlertManager) GetAlert(alertID string) (*Alert, error) {
	am.alertsMutex.RLock()
	defer am.alertsMutex.RUnlock()
	
	alert, exists := am.alerts[alertID]
	if !exists {
		return nil, fmt.Errorf("alert not found: %s", alertID)
	}
	
	return alert, nil
}

// GetAlerts 获取所有告警
func (am *AlertManager) GetAlerts(status ...AlertStatus) []*Alert {
	am.alertsMutex.RLock()
	defer am.alertsMutex.RUnlock()
	
	result := make([]*Alert, 0, len(am.alerts))
	
	if len(status) == 0 {
		// 返回所有告警
		for _, alert := range am.alerts {
			result = append(result, alert)
		}
	} else {
		// 返回指定状态的告警
		statusMap := make(map[AlertStatus]bool)
		for _, s := range status {
			statusMap[s] = true
		}
		
		for _, alert := range am.alerts {
			if statusMap[alert.Status] {
				result = append(result, alert)
			}
		}
	}
	
	return result
}

// GetActiveAlerts 获取活跃告警
func (am *AlertManager) GetActiveAlerts() []*Alert {
	return am.GetAlerts(AlertStatusActive)
}

// GetAlertCount 获取告警数量
func (am *AlertManager) GetAlertCount() int {
	am.alertsMutex.RLock()
	defer am.alertsMutex.RUnlock()
	return len(am.alerts)
}

// notifyAlert 发送告警通知
func (am *AlertManager) notifyAlert(alert *Alert) {
	switch am.notificationType {
	case "log":
		am.logNotification(alert)
	case "email":
		// 这里可以添加邮件通知逻辑
		fmt.Printf("Email notification would be sent for alert: %s\n", alert.ID)
	case "webhook":
		// 这里可以添加webhook通知逻辑
		fmt.Printf("Webhook notification would be sent for alert: %s\n", alert.ID)
	default:
		am.logNotification(alert)
	}
}

// notifyAlertResolved 发送告警解决通知
func (am *AlertManager) notifyAlertResolved(alert *Alert) {
	switch am.notificationType {
	case "log":
		fmt.Printf("[RESOLVED] %s - %s\n", alert.Severity, alert.Message)
	case "email":
		// 这里可以添加邮件通知逻辑
		fmt.Printf("Email notification would be sent for resolved alert: %s\n", alert.ID)
	case "webhook":
		// 这里可以添加webhook通知逻辑
		fmt.Printf("Webhook notification would be sent for resolved alert: %s\n", alert.ID)
	default:
		fmt.Printf("[RESOLVED] %s - %s\n", alert.Severity, alert.Message)
	}
}

// logNotification 记录告警通知
func (am *AlertManager) logNotification(alert *Alert) {
	fmt.Printf("[ALERT] %s - %s: %s\n", alert.Severity, alert.Type, alert.Message)
	if alert.DeviceID != "" {
		fmt.Printf("  Device: %s\n", alert.DeviceID)
	}
	if alert.SensorID != "" {
		fmt.Printf("  Sensor: %s\n", alert.SensorID)
	}
	if len(alert.Metadata) > 0 {
		fmt.Printf("  Metadata: %v\n", alert.Metadata)
	}
}

// GetAlertStats 获取告警统计信息
func (am *AlertManager) GetAlertStats() map[string]interface{} {
	am.alertsMutex.RLock()
	defer am.alertsMutex.RUnlock()
	
	stats := map[string]interface{}{
		"total":     len(am.alerts),
		"active":    0,
		"resolved":  0,
		"suppressed": 0,
		"by_severity": make(map[string]int),
	}
	
	bySeverity := stats["by_severity"].(map[string]int)
	
	for _, alert := range am.alerts {
		switch alert.Status {
		case AlertStatusActive:
			stats["active"] = stats["active"].(int) + 1
		case AlertStatusResolved:
			stats["resolved"] = stats["resolved"].(int) + 1
		case AlertStatusSuppressed:
			stats["suppressed"] = stats["suppressed"].(int) + 1
		}
		
		bySeverity[string(alert.Severity)]++
	}
	
	return stats
}
