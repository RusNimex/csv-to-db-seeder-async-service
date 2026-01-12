<?php

namespace App\Services;

use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Exception\AMQPException;

/**
 * Сервис для публикации задач в RabbitMQ
 */
class RabbitMQPublisher
{
    private AMQPStreamConnection $connection;
    private \PhpAmqpLib\Channel\AMQPChannel $channel;
    private string $exchange;
    private string $vhost;

    public function __construct()
    {
        $host = $_ENV['RABBITMQ_HOST'] ?? 'rabbitmq';
        $port = (int)($_ENV['RABBITMQ_PORT'] ?? 5672);
        $user = $_ENV['RABBITMQ_USER'] ?? 'guest';
        $pass = $_ENV['RABBITMQ_PASS'] ?? 'guest';
        $this->vhost = $_ENV['RABBITMQ_VHOST'] ?? '/';
        $this->exchange = 'csv_import';

        try {
            $this->connection = new AMQPStreamConnection($host, $port, $user, $pass, $this->vhost);
            $this->channel = $this->connection->channel();
            $this->channel->exchange_declare($this->exchange, 'direct', false, true, false);
        } catch (AMQPException $e) {
            throw new \RuntimeException("Не удалось подключиться к RabbitMQ: " . $e->getMessage());
        }
    }

    /**
     * Публикует задачу на импорт CSV файла
     *
     * @param string $filePath Путь к сохраненному файлу
     * @param string $fileName Имя файла
     * @param int $fileSize Размер файла в байтах
     * @param string $priority Приоритет: 'high', 'normal', 'large'
     * @return bool
     */
    public function publishImportTask(string $filePath, string $fileName, int $fileSize, string $priority = 'normal'): bool
    {
        // Определяем routing key на основе приоритета
        $routingKey = match($priority) {
            'high' => 'high',
            'large' => 'large',
            default => 'normal',
        };

        // Определяем приоритет сообщения (0-255, но в настройках очередей max 10)
        $messagePriority = match($priority) {
            'high' => 10,
            'large' => 1,
            default => 5,
        };

        $task = [
            'file_path' => $filePath,
            'file_name' => $fileName,
            'file_size' => $fileSize,
            'priority' => $priority,
            'created_at' => date('Y-m-d H:i:s'),
        ];

        $message = new AMQPMessage(
            json_encode($task, JSON_UNESCAPED_UNICODE),
            [
                'delivery_mode' => 2,
                'priority' => $messagePriority,
            ]
        );

        try {
            $this->channel->basic_publish($message, $this->exchange, $routingKey);
            return true;
        } catch (AMQPException $e) {
            throw new \RuntimeException("Не удалось опубликовать задачу: " . $e->getMessage());
        }
    }

    /**
     * Закрывает соединение
     */
    public function close(): void
    {
        if (isset($this->channel)) {
            $this->channel->close();
        }
        if (isset($this->connection)) {
            $this->connection->close();
        }
    }

    public function __destruct()
    {
        $this->close();
    }
}

