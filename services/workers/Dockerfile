# Многоэтапная сборка для уменьшения размера образа
FROM golang:1.21-alpine AS builder

# Устанавливаем зависимости для сборки
RUN apk add --no-cache git

# Устанавливаем рабочую директорию
WORKDIR /app

# Копируем go mod файлы
COPY go.mod ./

# Устанавливаем зависимости явно
RUN go get github.com/go-sql-driver/mysql@v1.7.1 && \
    go get github.com/rabbitmq/amqp091-go@v1.9.0

# Копируем исходный код
COPY *.go ./

# Собираем приложение
RUN CGO_ENABLED=0 GOOS=linux go build -v -o worker .

# Финальный образ
FROM alpine:latest

# Устанавливаем ca-certificates для HTTPS запросов
RUN apk --no-cache add ca-certificates

WORKDIR /app

# Копируем бинарник из builder в /usr/local/bin (не будет перезаписан volume)
COPY --from=builder /app/worker /usr/local/bin/worker

# Запускаем воркер
CMD ["/usr/local/bin/worker"]

