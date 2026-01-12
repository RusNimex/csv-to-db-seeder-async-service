.PHONY: help up down restart logs ps clean build

help: ## Показать справку
	@echo "Доступные команды:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

up: ## Запустить все сервисы
	docker-compose up -d

up-dev: ## Запустить в режиме разработки
	docker-compose -f docker-compose.yml -f docker-compose.dev.yml up -d

up-prod: ## Запустить в режиме production
	docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d

down: ## Остановить все сервисы
	docker-compose down

down-v: ## Остановить и удалить volumes
	docker-compose down -v

restart: ## Перезапустить все сервисы
	docker-compose restart

logs: ## Показать логи всех сервисов
	docker-compose logs -f

logs-api: ## Показать логи API
	docker-compose logs -f api

logs-worker: ## Показать логи воркеров
	docker-compose logs -f worker

logs-rabbitmq: ## Показать логи RabbitMQ
	docker-compose logs -f rabbitmq

logs-mysql: ## Показать логи MySQL
	docker-compose logs -f mysql

ps: ## Показать статус сервисов
	docker-compose ps

build: ## Пересобрать образы
	docker-compose build --no-cache

build-api: ## Пересобрать образ API
	docker-compose build --no-cache api

build-worker: ## Пересобрать образ Worker
	docker-compose build --no-cache worker

clean: ## Очистить неиспользуемые ресурсы
	docker-compose down -v
	docker system prune -f

rabbitmq-status: ## Статус RabbitMQ
	docker-compose exec rabbitmq rabbitmqctl status

rabbitmq-queues: ## Список очередей RabbitMQ
	docker-compose exec rabbitmq rabbitmqctl list_queues

rabbitmq-connections: ## Список соединений RabbitMQ
	docker-compose exec rabbitmq rabbitmqctl list_connections
rabbitmq-queues-detail: ## Детальная информация об очередях
	docker-compose exec rabbitmq rabbitmqctl list_queues name messages consumers memory

rabbitmq-watch: ## Мониторинг очередей в реальном времени
	watch -n 2 'docker-compose exec -T rabbitmq rabbitmqctl list_queues name messages consumers'

rabbitmq-api-queues: ## Список очередей через API
	curl -s -u guest:guest http://localhost:15672/api/queues | jq '.[] | {name: .name, messages: .messages, consumers: .consumers, rate: .message_stats.publish_details.rate}'
	
mysql-shell: ## Подключиться к MySQL
	docker-compose exec mysql mysql -u root -p$(shell grep MYSQL_ROOT_PASSWORD .env | cut -d '=' -f2) csv

mysql-backup: ## Создать бэкап MySQL
	docker-compose exec mysql mysqldump -u root -p$(shell grep MYSQL_ROOT_PASSWORD .env | cut -d '=' -f2) csv > backup_$(shell date +%Y%m%d_%H%M%S).sql

mysql-restore: ## Восстановить из бэкапа (использовать: make mysql-restore FILE=backup.sql)
	docker-compose exec -T mysql mysql -u root -p$(shell grep MYSQL_ROOT_PASSWORD .env | cut -d '=' -f2) csv < $(FILE)

