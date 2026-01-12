package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	// Загружаем конфигурацию
	config, err := LoadConfig()
	if err != nil {
		log.Fatalf("Ошибка загрузки конфигурации: %v", err)
	}

	// Подключаемся к БД
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		config.DBUser, config.DBPassword, config.DBHost, config.DBPort, config.DBName)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("Ошибка подключения к БД: %v", err)
	}
	defer db.Close()

	// Проверяем соединение
	if err := db.Ping(); err != nil {
		log.Fatalf("Ошибка проверки соединения с БД: %v", err)
	}

	// Устанавливаем параметры пула соединений
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * 60) // 5 минут

	log.Println("Подключение к БД установлено")

	// Определяем очереди для обработки
	queues := getQueuesToProcess()

	if len(queues) == 0 {
		log.Fatalf("Не указаны очереди для обработки. Установите переменную окружения WORKER_QUEUES или используйте значения по умолчанию")
	}

	log.Printf("Запуск воркеров для очередей: %s", strings.Join(queues, ", "))

	// Создаем воркеры для каждой очереди
	var workers []*Worker
	for _, queueName := range queues {
		worker, err := NewWorker(config, db, queueName)
		if err != nil {
			log.Fatalf("Ошибка создания воркера для очереди %s: %v", queueName, err)
		}
		workers = append(workers, worker)
		defer worker.Close()
	}

	// Обработка сигналов для graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Запускаем все воркеры в отдельных горутинах
	var wg sync.WaitGroup
	done := make(chan error, len(workers))

	for _, worker := range workers {
		wg.Add(1)
		go func(w *Worker) {
			defer wg.Done()
			if err := w.Start(); err != nil {
				done <- err
			}
		}(worker)
	}

	// Ждем сигнала завершения или ошибки
	select {
	case sig := <-sigChan:
		log.Printf("Получен сигнал: %v, завершаем работу...", sig)
		for _, worker := range workers {
			if err := worker.Close(); err != nil {
				log.Printf("Ошибка закрытия воркера: %v", err)
			}
		}
	case err := <-done:
		if err != nil {
			log.Fatalf("Воркер завершился с ошибкой: %v", err)
		}
	}

	wg.Wait()
	log.Println("Все воркеры остановлены")
}

// getQueuesToProcess возвращает список очередей для обработки
func getQueuesToProcess() []string {
	// Проверяем переменную окружения
	if queuesEnv := os.Getenv("WORKER_QUEUES"); queuesEnv != "" {
		queues := strings.Split(queuesEnv, ",")
		result := make([]string, 0, len(queues))
		for _, q := range queues {
			q = strings.TrimSpace(q)
			if q != "" {
				result = append(result, q)
			}
		}
		if len(result) > 0 {
			return result
		}
	}

	// Значения по умолчанию - все очереди
	return []string{
		"csv_import_high",
		"csv_import_normal",
		"csv_import_large",
	}
}

