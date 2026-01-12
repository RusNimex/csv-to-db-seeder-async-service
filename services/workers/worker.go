package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Worker обрабатывает задачи из RabbitMQ
type Worker struct {
	conn          *amqp.Connection
	channel       *amqp.Channel
	repository    *CompanyRepository
	queueName     string
	prefetchCount int
	csvParser     *CSVParser
	storagePath   string
	workerID      string
}

// NewWorker создает новый воркер
func NewWorker(config *Config, db *sql.DB, queueName string) (*Worker, error) {
	conn, err := amqp.Dial(config.RabbitMQURL)
	if err != nil {
		return nil, fmt.Errorf("ошибка подключения к RabbitMQ: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("ошибка создания канала: %w", err)
	}

	// Устанавливаем prefetch count
	if err := ch.Qos(config.PrefetchCount, 0, false); err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("ошибка установки Qos: %w", err)
	}

	// Определяем аргументы очереди в зависимости от её типа
	args := make(amqp.Table)
	
	// Устанавливаем x-max-priority в зависимости от очереди
	switch queueName {
	case "csv_import_high":
		args["x-max-priority"] = 10
	case "csv_import_normal":
		args["x-max-priority"] = 5
	case "csv_import_large":
		args["x-max-priority"] = 1
	}

	// Объявляем очередь (не создаем, если уже существует)
	_, err = ch.QueueDeclare(
		queueName, // name
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		args,      // arguments
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("ошибка объявления очереди: %w", err)
	}

	// Генерируем уникальный ID воркера
	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "unknown"
	}
	workerID := fmt.Sprintf("worker-%s-%s", queueName, hostname)

	return &Worker{
		conn:          conn,
		channel:       ch,
		repository:    NewCompanyRepository(db),
		queueName:     queueName,
		prefetchCount: config.PrefetchCount,
		csvParser:     NewCSVParser(),
		storagePath:   config.StoragePath,
		workerID:      workerID,
	}, nil
}

// Start запускает воркер для обработки задач
func (w *Worker) Start() error {
	msgs, err := w.channel.Consume(
		w.queueName, // queue
		"",          // consumer
		false,       // auto-ack
		false,       // exclusive - разрешаем несколько воркеров на очередь
		false,       // no-local
		false,       // no-wait
		nil,         // args
	)
		if err != nil {
			// Если очередь уже занята другим эксклюзивным потребителем, это нормально
			if strings.Contains(err.Error(), "exclusive") || strings.Contains(err.Error(), "RESOURCE_LOCKED") {
				log.Printf("[%s] Очередь %s уже обрабатывается другим воркером, пропускаем...", w.workerID, w.queueName)
				return nil
			}
			return fmt.Errorf("ошибка регистрации потребителя: %w", err)
		}

		log.Printf("[%s] Воркер успешно подключен к очереди %s (эксклюзивный режим)", w.workerID, w.queueName)

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			retryCount := getRetryCount(d.Headers)
			maxRetries := 10 // Максимальное количество попыток
			
			if err := w.processMessage(d); err != nil {
				// Проверяем, является ли ошибка deadlock или временной
				isRetryable := isRetryableError(err)
				
				if isRetryable && retryCount < maxRetries {
					newRetryCount := retryCount + 1
					
					// Задержка перед возвратом в очередь (экспоненциальная: 1s, 2s, 3s, 4s, 5s...)
					time.Sleep(time.Duration(newRetryCount) * time.Second)
					
					// Обновляем заголовки с новым счетчиком попыток
					newHeaders := make(amqp.Table)
					if d.Headers != nil {
						for k, v := range d.Headers {
							newHeaders[k] = v
						}
					}
					newHeaders["x-retry-count"] = newRetryCount
					
					// Перепубликуем сообщение с обновленным счетчиком попыток
					pubErr := w.channel.Publish(
						"",          // exchange (используем default exchange для прямой публикации в очередь)
						w.queueName, // routing key (имя очереди)
						false,       // mandatory
						false,       // immediate
						amqp.Publishing{
							Body:         d.Body,
							Headers:      newHeaders,
							DeliveryMode: amqp.Persistent, // 2 - persistent
							Priority:     d.Priority,
							MessageId:    d.MessageId,
							Timestamp:    time.Now(),
						},
					)
					
					if pubErr != nil {
						// Fallback к обычному requeue если перепубликация не удалась
						d.Nack(false, true)
					} else {
						// Подтверждаем старое сообщение после успешной перепубликации
						d.Ack(false)
					}
				} else {
					// Извлекаем название файла из сообщения или используем MessageId
					fileName := d.MessageId
					var task ImportTask
					if json.Unmarshal(d.Body, &task) == nil {
						fileName = task.FileName
					}
					log.Printf("[%s] %s: %v", w.workerID, fileName, err)
					// Отклоняем сообщение без возврата в очередь
					d.Nack(false, false)
				}
			} else {
				// Подтверждаем обработку
				d.Ack(false)
			}
		}
	}()

	<-forever
	return nil
}

// processMessage обрабатывает одно сообщение
func (w *Worker) processMessage(d amqp.Delivery) error {
	var task ImportTask
	if err := json.Unmarshal(d.Body, &task); err != nil {
		return fmt.Errorf("ошибка декодирования задачи: %w", err)
	}

	fileSizeKB := float64(task.FileSize) / 1024.0 / 1024.0
	log.Printf("[%s] Задача: %.4f МБ, файл: %s, старт...", w.workerID, fileSizeKB, task.FileName)

	// Определяем полный путь к файлу
	filePath := task.FilePath
	
	// Проверяем существование файла и пробуем разные варианты путей
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		// Если файл не найден по указанному пути, пробуем альтернативные варианты
		
		// Вариант 1: путь из PHP может быть /var/www/html/storage/csv/...
		// В контейнере воркера это должно быть /app/storage/csv/...
		if strings.Contains(filePath, "/var/www/html/storage") {
			filePath = strings.Replace(filePath, "/var/www/html/storage", w.storagePath, 1)
		}
		
		// Вариант 2: если путь относительный, добавляем storage/csv
		if !filepath.IsAbs(filePath) {
			filePath = filepath.Join(w.storagePath, "csv", filepath.Base(filePath))
		}
		
		// Проверяем существование файла
		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			return fmt.Errorf("%s: файл не найден", task.FileName)
		}
	}

	// Парсим CSV файл
	records, err := w.csvParser.ParseFile(filePath)
	if err != nil {
		return fmt.Errorf("%s: %w", task.FileName, err)
	}

	if len(records) == 0 {
		return nil
	}

	startTime := time.Now()
	if err := w.repository.Insert(records); err != nil {
		return fmt.Errorf("%s: %w", task.FileName, err)
	}

	duration := time.Since(startTime)
	summary := w.repository.GetSummary()

	log.Printf("[%s] Успешно: %.2fс, Строк: %d, Файл: %s, Компаний: %d, Городов: %d, Катег: %d, Подкатег: %d.",
		w.workerID, duration.Seconds(), len(records), task.FileName, summary.Company, summary.City, summary.Category, summary.Subcategory)

	// Удаляем обработанный файл
	os.Remove(filePath)

	return nil
}

// getRetryCount получает количество попыток из заголовков сообщения
func getRetryCount(headers amqp.Table) int {
	if headers == nil {
		return 0
	}
	if count, ok := headers["x-retry-count"].(int32); ok {
		return int(count)
	}
	if count, ok := headers["x-retry-count"].(int); ok {
		return count
	}
	return 0
}

// isRetryableError проверяет, можно ли повторить операцию при данной ошибке
func isRetryableError(err error) bool {
	if err == nil {
		return false
	}
	
	errStr := err.Error()
	
	// Deadlock - можно повторить
	if strings.Contains(errStr, "Deadlock") || strings.Contains(errStr, "deadlock") {
		return true
	}
	
	// Временные ошибки MySQL
	retryableErrors := []string{
		"Error 1213",      // Deadlock
		"Error 1205",      // Lock wait timeout
		"Error 2013",      // Lost connection
		"Error 2006",      // MySQL server has gone away
		"connection reset",
		"connection refused",
		"timeout",
		"temporary failure",
	}
	
	for _, retryableErr := range retryableErrors {
		if strings.Contains(errStr, retryableErr) {
			return true
		}
	}
	
	// Ошибки файловой системы (файл может быть временно заблокирован)
	if strings.Contains(errStr, "file") && 
	   (strings.Contains(errStr, "locked") || strings.Contains(errStr, "busy")) {
		return true
	}
	
	return false
}

// Close закрывает соединения
func (w *Worker) Close() error {
	if w.channel != nil {
		if err := w.channel.Close(); err != nil {
			return err
		}
	}
	if w.conn != nil {
		return w.conn.Close()
	}
	return nil
}

