package main

import (
	"fmt"
	"os"
	"strconv"
)

// Config содержит конфигурацию приложения
type Config struct {
	// Database
	DBHost     string
	DBPort     int
	DBName     string
	DBUser     string
	DBPassword string

	// RabbitMQ
	RabbitMQURL string

	// Worker settings
	BatchSize      int
	PrefetchCount  int
	PivotBatchSize int
	StoragePath    string
}

// LoadConfig загружает конфигурацию из переменных окружения
func LoadConfig() (*Config, error) {
	config := &Config{
		DBHost:         getEnv("DB_HOST", "mysql"),
		DBPort:         getEnvAsInt("DB_PORT", 3306),
		DBName:         getEnv("DB_NAME", "csv"),
		DBUser:         getEnv("DB_USER", "csv_user"),
		DBPassword:     getEnv("DB_PASSWORD", "csv_pass"),
		RabbitMQURL:    getEnv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/"),
		BatchSize:      getEnvAsInt("WORKER_BATCH_SIZE", 2000),
		PrefetchCount:  getEnvAsInt("WORKER_PREFETCH_COUNT", 1),
		PivotBatchSize: 5000,
		StoragePath:    getEnv("STORAGE_PATH", "/app/storage"),
	}

	if config.RabbitMQURL == "" {
		return nil, fmt.Errorf("RABBITMQ_URL не установлен")
	}

	return config, nil
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvAsInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}

