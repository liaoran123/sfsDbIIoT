package main

import (
	"fmt"
	"math"
	"time"

	sfstime "github.com/liaoran123/sfsDb/time"
)

// AnalyticsManager 数据分析管理器
type AnalyticsManager struct {
	enabled           bool
	aggregationWindow string
	predictionEnabled bool
	storage           *StorageManager
}

// NewAnalyticsManager 创建数据分析管理器
func NewAnalyticsManager(enabled bool, aggregationWindow string, predictionEnabled bool, storage *StorageManager) *AnalyticsManager {
	return &AnalyticsManager{
		enabled:           enabled,
		aggregationWindow: aggregationWindow,
		predictionEnabled: predictionEnabled,
		storage:           storage,
	}
}

// AnalyzeSensorData 分析传感器数据
func (am *AnalyticsManager) AnalyzeSensorData(deviceID, sensorID string, startTime, endTime time.Time) (map[string]interface{}, error) {
	if !am.enabled {
		return nil, fmt.Errorf("analytics is disabled")
	}

	// 获取原始数据
	data, err := am.storage.QuerySensorData(deviceID, sensorID, startTime, endTime, 10000)
	if err != nil {
		return nil, fmt.Errorf("failed to query sensor data: %v", err)
	}

	if len(data) == 0 {
		return nil, fmt.Errorf("no sensor data found")
	}

	// 计算基本统计信息
	stats := am.calculateBasicStats(data)

	// 计算趋势
	trend := am.calculateTrend(data)

	// 计算异常值
	anomalies := am.detectAnomalies(data)

	// 预测未来值
	var prediction []map[string]interface{}
	if am.predictionEnabled {
		prediction, err = am.predictFutureValues(data, 10)
		if err != nil {
			fmt.Printf("Prediction failed: %v\n", err)
		}
	}

	// 构建分析结果
	result := map[string]interface{}{
		"device_id":   deviceID,
		"sensor_id":   sensorID,
		"start_time":  startTime,
		"end_time":    endTime,
		"data_points": len(data),
		"statistics":  stats,
		"trend":       trend,
		"anomalies":   anomalies,
		"prediction":  prediction,
		"timestamp":   time.Now(),
	}

	return result, nil
}

// calculateBasicStats 计算基本统计信息
func (am *AnalyticsManager) calculateBasicStats(data []*SensorData) map[string]interface{} {
	var sum, sumSquares float64
	var min, max float64
	count := len(data)

	if count == 0 {
		return map[string]interface{}{}
	}

	// 初始化最小值和最大值
	min = data[0].Value
	max = data[0].Value

	// 计算总和和平方和
	for _, item := range data {
		value := item.Value
		sum += value
		sumSquares += value * value

		if value < min {
			min = value
		}
		if value > max {
			max = value
		}
	}

	// 计算平均值
	mean := sum / float64(count)

	// 计算标准差
	variance := (sumSquares / float64(count)) - (mean * mean)
	stdDev := math.Sqrt(variance)

	return map[string]interface{}{
		"count":    count,
		"sum":      sum,
		"mean":     mean,
		"median":   am.calculateMedian(data),
		"min":      min,
		"max":      max,
		"std_dev":  stdDev,
		"variance": variance,
	}
}

// calculateMedian 计算中位数
func (am *AnalyticsManager) calculateMedian(data []*SensorData) float64 {
	count := len(data)
	if count == 0 {
		return 0
	}

	// 提取值并排序
	values := make([]float64, count)
	for i, item := range data {
		values[i] = item.Value
	}

	// 简单排序
	for i := 0; i < count; i++ {
		for j := i + 1; j < count; j++ {
			if values[i] > values[j] {
				values[i], values[j] = values[j], values[i]
			}
		}
	}

	// 计算中位数
	if count%2 == 0 {
		return (values[count/2-1] + values[count/2]) / 2
	} else {
		return values[count/2]
	}
}

// calculateTrend 计算趋势
func (am *AnalyticsManager) calculateTrend(data []*SensorData) map[string]interface{} {
	count := len(data)
	if count < 2 {
		return map[string]interface{}{
			"slope":     0,
			"direction": "stable",
			"strength":  0,
		}
	}

	// 线性回归计算斜率
	var sumX, sumY, sumXY, sumX2 float64

	for i, item := range data {
		x := float64(i)
		y := item.Value
		sumX += x
		sumY += y
		sumXY += x * y
		sumX2 += x * x
	}

	n := float64(count)
	slope := (n*sumXY - sumX*sumY) / (n*sumX2 - sumX*sumX)

	// 计算趋势方向
	direction := "stable"
	if slope > 0.1 {
		direction = "increasing"
	} else if slope < -0.1 {
		direction = "decreasing"
	}

	// 计算趋势强度
	strength := math.Abs(slope) / (math.Max(slope, 1))

	return map[string]interface{}{
		"slope":     slope,
		"direction": direction,
		"strength":  strength,
	}
}

// detectAnomalies 检测异常值
func (am *AnalyticsManager) detectAnomalies(data []*SensorData) []*SensorData {
	count := len(data)
	if count < 3 {
		return []*SensorData{}
	}

	// 计算平均值和标准差
	var sum, sumSquares float64
	for _, item := range data {
		sum += item.Value
		sumSquares += item.Value * item.Value
	}

	mean := sum / float64(count)
	variance := (sumSquares / float64(count)) - (mean * mean)
	stdDev := math.Sqrt(variance)

	// 检测异常值（使用3倍标准差法则）
	var anomalies []*SensorData
	threshold := 3.0 * stdDev

	for _, item := range data {
		if math.Abs(item.Value-mean) > threshold {
			anomalies = append(anomalies, item)
		}
	}

	return anomalies
}

// predictFutureValues 预测未来值
func (am *AnalyticsManager) predictFutureValues(data []*SensorData, steps int) ([]map[string]interface{}, error) {
	count := len(data)
	if count < 5 {
		return nil, fmt.Errorf("not enough data for prediction")
	}

	// 使用简单的线性回归预测
	var sumX, sumY, sumXY, sumX2 float64

	for i, item := range data {
		x := float64(i)
		y := item.Value
		sumX += x
		sumY += y
		sumXY += x * y
		sumX2 += x * x
	}

	n := float64(count)
	slope := (n*sumXY - sumX*sumY) / (n*sumX2 - sumX*sumX)
	intercept := (sumY - slope*sumX) / n

	// 预测未来值
	prediction := make([]map[string]interface{}, steps)
	lastTimestamp := data[count-1].Timestamp

	for i := 0; i < steps; i++ {
		// 计算预测值
		x := float64(count + i)
		predictedValue := slope*x + intercept

		// 计算时间戳
		predictedTimestamp := lastTimestamp.Add(time.Duration(i+1) * time.Minute)

		// 构建预测结果
		prediction[i] = map[string]interface{}{
			"step":      i + 1,
			"value":     predictedValue,
			"timestamp": predictedTimestamp,
			"method":    "linear_regression",
		}
	}

	return prediction, nil
}

// AggregateSensorData 聚合传感器数据
func (am *AnalyticsManager) AggregateSensorData(deviceID, sensorID string, startTime, endTime time.Time, granularity sfstime.TimeGranularity, aggregationType string) ([]sfstime.TimeAggregationResult, error) {
	if !am.enabled {
		return nil, fmt.Errorf("analytics is disabled")
	}

	// 使用 sfsDb 的 time 包进行聚合
	results, err := am.storage.QuerySensorDataWithAggregation(deviceID, sensorID, startTime, endTime, granularity, aggregationType)
	if err != nil {
		return nil, fmt.Errorf("failed to aggregate sensor data: %v", err)
	}

	return results, nil
}

// GetCorrelation 计算两个传感器之间的相关性
func (am *AnalyticsManager) GetCorrelation(deviceID1, sensorID1, deviceID2, sensorID2 string, startTime, endTime time.Time) (map[string]interface{}, error) {
	if !am.enabled {
		return nil, fmt.Errorf("analytics is disabled")
	}

	// 获取两个传感器的数据
	data1, err := am.storage.QuerySensorData(deviceID1, sensorID1, startTime, endTime, 10000)
	if err != nil {
		return nil, fmt.Errorf("failed to query sensor 1 data: %v", err)
	}

	data2, err := am.storage.QuerySensorData(deviceID2, sensorID2, startTime, endTime, 10000)
	if err != nil {
		return nil, fmt.Errorf("failed to query sensor 2 data: %v", err)
	}

	if len(data1) == 0 || len(data2) == 0 {
		return nil, fmt.Errorf("not enough data for correlation")
	}

	// 确保数据点数量相同（简单处理，取最小值）
	minCount := len(data1)
	if len(data2) < minCount {
		minCount = len(data2)
	}

	data1 = data1[:minCount]
	data2 = data2[:minCount]

	// 计算相关性
	correlation := am.calculateCorrelation(data1, data2)

	// 计算协方差
	covariance := am.calculateCovariance(data1, data2)

	// 构建结果
	result := map[string]interface{}{
		"sensor1": map[string]string{
			"device_id": deviceID1,
			"sensor_id": sensorID1,
		},
		"sensor2": map[string]string{
			"device_id": deviceID2,
			"sensor_id": sensorID2,
		},
		"start_time":  startTime,
		"end_time":    endTime,
		"data_points": minCount,
		"correlation": correlation,
		"covariance":  covariance,
		"timestamp":   time.Now(),
	}

	return result, nil
}

// calculateCorrelation 计算相关性
func (am *AnalyticsManager) calculateCorrelation(data1, data2 []*SensorData) float64 {
	count := len(data1)
	if count != len(data2) || count == 0 {
		return 0
	}

	// 计算平均值
	var sum1, sum2 float64
	for i := 0; i < count; i++ {
		sum1 += data1[i].Value
		sum2 += data2[i].Value
	}
	mean1 := sum1 / float64(count)
	mean2 := sum2 / float64(count)

	// 计算分子和分母
	var numerator, denominator1, denominator2 float64
	for i := 0; i < count; i++ {
		diff1 := data1[i].Value - mean1
		diff2 := data2[i].Value - mean2
		numerator += diff1 * diff2
		denominator1 += diff1 * diff1
		denominator2 += diff2 * diff2
	}

	// 计算相关性
	denominator := math.Sqrt(denominator1 * denominator2)
	if denominator == 0 {
		return 0
	}

	return numerator / denominator
}

// calculateCovariance 计算协方差
func (am *AnalyticsManager) calculateCovariance(data1, data2 []*SensorData) float64 {
	count := len(data1)
	if count != len(data2) || count == 0 {
		return 0
	}

	// 计算平均值
	var sum1, sum2 float64
	for i := 0; i < count; i++ {
		sum1 += data1[i].Value
		sum2 += data2[i].Value
	}
	mean1 := sum1 / float64(count)
	mean2 := sum2 / float64(count)

	// 计算协方差
	var covariance float64
	for i := 0; i < count; i++ {
		covariance += (data1[i].Value - mean1) * (data2[i].Value - mean2)
	}

	return covariance / float64(count-1)
}

// GetAnalyticsStats 获取分析统计信息
func (am *AnalyticsManager) GetAnalyticsStats() map[string]interface{} {
	return map[string]interface{}{
		"enabled":            am.enabled,
		"aggregation_window": am.aggregationWindow,
		"prediction_enabled": am.predictionEnabled,
		"timestamp":          time.Now(),
	}
}
