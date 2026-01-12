# Infrastructure

Инфраструктура для проекта асинхронной загрузки CSV файлов.

## Структура

```
infra/
├── docker-compose.yml          # Основная конфигурация
├── docker-compose.prod.yml     # Production override
├── docker-compose.dev.yml      # Development override
├── rabbitmq/
│   ├── rabbitmq.conf           # Конфигурация RabbitMQ
│   └── definitions.json        # Очереди и exchange
├── mysql/
│   ├── my.cnf                  # Конфигурация MySQL
│   └── init/                   # SQL скрипты для инициализации
└── php/
    └── php.ini                  # Конфигурация PHP
```

## Быстрый старт

### 1. Копирование переменных окружения

```bash
cp .env.example .env
# Отредактируйте .env при необходимости
```

### 2. Запуск всех сервисов

```bash
# Development
docker-compose -f docker-compose.yml -f docker-compose.dev.yml up -d

# Production
docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d

# Базовый запуск
docker-compose up -d
```

### 3. Проверка статуса

```bash
docker-compose ps
```

### 4. Просмотр логов

```bash
# Все сервисы
docker-compose logs -f

# Конкретный сервис
docker-compose logs -f api
docker-compose logs -f worker
docker-compose logs -f rabbitmq
```

## Сервисы

### RabbitMQ
- **AMQP порт**: 5672
- **Management UI**: http://localhost:15672
- **Логин**: guest / guest (по умолчанию)

### MySQL
- **Порт**: 3306
- **База данных**: csv
- **Пользователь**: csv_user (настраивается в .env)

### API (PHP)
- **Порт**: 8001
- **URL**: http://localhost:8001

### Worker (Golang)
- Запускается в фоновом режиме
- Количество реплик настраивается через WORKER_REPLICAS

## Очереди RabbitMQ

Проект использует три очереди с разными приоритетами:

1. **csv_import_high** - высокий приоритет (маленькие файлы)
2. **csv_import_normal** - нормальный приоритет (средние файлы)
3. **csv_import_large** - низкий приоритет (большие файлы)

Exchange: **csv_import** (direct)

## Настройка воркеров

Количество воркеров настраивается через переменные окружения:

```env
WORKER_REPLICAS=3                    # Общее количество реплик контейнера
WORKER_QUEUE_HIGH_WORKERS=2         # Воркеры для high priority
WORKER_QUEUE_NORMAL_WORKERS=2       # Воркеры для normal priority
WORKER_QUEUE_LARGE_WORKERS=1        # Воркеры для large priority
WORKER_BATCH_SIZE=2000              # Размер батча для вставки
WORKER_PREFETCH_COUNT=1             # Prefetch для RabbitMQ
```

## Остановка и очистка

```bash
# Остановка сервисов
docker-compose down

# Остановка с удалением volumes (удалит данные!)
docker-compose down -v

# Перезапуск
docker-compose restart
```

## Мониторинг

### RabbitMQ Management
http://localhost:15672

### Проверка очередей
```bash
docker-compose exec rabbitmq rabbitmqctl list_queues
```

### Проверка соединений
```bash
docker-compose exec rabbitmq rabbitmqctl list_connections
```

## Troubleshooting

### Проблемы с подключением к RabbitMQ
```bash
docker-compose exec rabbitmq rabbitmqctl status
```

### Проблемы с MySQL
```bash
docker-compose exec mysql mysql -u root -p
```

### Просмотр логов воркеров
```bash
docker-compose logs -f worker
```

