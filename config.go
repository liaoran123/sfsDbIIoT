package main

import (
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// Config 应用程序配置结构
type Config struct {
	Database struct {
		Path     string `yaml:"path"`
		MaxOpen  int    `yaml:"max_open"`
		MaxIdle  int    `yaml:"max_idle"`
		CacheSize int    `yaml:"cache_size"`
	} `yaml:"database"`
	Device struct {
		MaxDevices int `yaml:"max_devices"`
		ScanInterval int `yaml:"scan_interval"`
	} `yaml:"device"`
	Sensor struct {
		MaxSensorsPerDevice int `yaml:"max_sensors_per_device"`
		DataInterval        int `yaml:"data_interval"`
		BatchSize           int `yaml:"batch_size"`
	} `yaml:"sensor"`
	Analytics struct {
		Enabled           bool   `yaml:"enabled"`
		AggregationWindow string `yaml:"aggregation_window"`
		PredictionEnabled bool   `yaml:"prediction_enabled"`
	} `yaml:"analytics"`
	Alert struct {
		Enabled          bool   `yaml:"enabled"`
		CheckInterval    int    `yaml:"check_interval"`
		NotificationType string `yaml:"notification_type"`
	} `yaml:"alert"`
	API struct {
		Enabled bool   `yaml:"enabled"`
		Port    string `yaml:"port"`
		Cors    bool   `yaml:"cors"`
	} `yaml:"api"`
}

var AppConfig *Config

// LoadConfig 加载配置文件
func LoadConfig() error {
	configPath := "config.yaml"
	
	// 检查配置文件是否存在
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		// 检查当前目录和上级目录
		currentDir, err := os.Getwd()
		if err != nil {
			return fmt.Errorf("failed to get current directory: %v", err)
		}
		
		// 尝试在当前目录的不同位置查找配置文件
		possiblePaths := []string{
			configPath,
			filepath.Join(currentDir, configPath),
			filepath.Join(currentDir, "config", configPath),
		}
		
		found := false
		for _, path := range possiblePaths {
			if _, err := os.Stat(path); !os.IsNotExist(err) {
				configPath = path
				found = true
				break
			}
		}
		
		if !found {
			// 使用默认配置
			AppConfig = getDefaultConfig()
			fmt.Println("Config file not found, using default configuration")
			return nil
		}
	}
	
	// 读取配置文件
	data, err := os.ReadFile(configPath)
	if err != nil {
		return fmt.Errorf("failed to read config file: %v", err)
	}
	
	// 解析配置文件
	AppConfig = &Config{}
	err = yaml.Unmarshal(data, AppConfig)
	if err != nil {
		return fmt.Errorf("failed to parse config file: %v", err)
	}
	
	// 验证配置
	err = validateConfig(AppConfig)
	if err != nil {
		return fmt.Errorf("invalid configuration: %v", err)
	}
	
	fmt.Printf("Config loaded successfully from %s\n", configPath)
	return nil
}

// getDefaultConfig 获取默认配置
func getDefaultConfig() *Config {
	config := &Config{}
	
	// 数据库默认配置
	config.Database.Path = "./data"
	config.Database.MaxOpen = 10
	config.Database.MaxIdle = 5
	config.Database.CacheSize = 1024
	
	// 设备默认配置
	config.Device.MaxDevices = 1000
	config.Device.ScanInterval = 60
	
	// 传感器默认配置
	config.Sensor.MaxSensorsPerDevice = 20
	config.Sensor.DataInterval = 1
	config.Sensor.BatchSize = 100
	
	// 分析默认配置
	config.Analytics.Enabled = true
	config.Analytics.AggregationWindow = "5m"
	config.Analytics.PredictionEnabled = false
	
	// 告警默认配置
	config.Alert.Enabled = true
	config.Alert.CheckInterval = 30
	config.Alert.NotificationType = "log"
	
	// API默认配置
	config.API.Enabled = true
	config.API.Port = "8080"
	config.API.Cors = true
	
	return config
}

// validateConfig 验证配置
func validateConfig(config *Config) error {
	// 验证数据库配置
	if config.Database.Path == "" {
		return fmt.Errorf("database path is required")
	}
	
	// 验证设备配置
	if config.Device.MaxDevices <= 0 {
		return fmt.Errorf("max devices must be greater than 0")
	}
	
	// 验证传感器配置
	if config.Sensor.MaxSensorsPerDevice <= 0 {
		return fmt.Errorf("max sensors per device must be greater than 0")
	}
	
	// 验证API配置
	if config.API.Enabled && config.API.Port == "" {
		return fmt.Errorf("API port is required when API is enabled")
	}
	
	return nil
}

// GetConfig 获取配置实例
func GetConfig() *Config {
	if AppConfig == nil {
		AppConfig = getDefaultConfig()
	}
	return AppConfig
}
