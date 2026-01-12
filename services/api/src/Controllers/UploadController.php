<?php

namespace App\Controllers;

use App\Http\ResponseTrait;
use App\Services\RabbitMQPublisher;

/**
 * Контроллер загрузки файлов
 */
class UploadController
{
    use ResponseTrait;

    /**
     * @var array|mixed Файлы из $_FILES['files']
     */
    protected array $files;

    /**
     * @var array|string[] Допустимые типы файлов
     */
    protected array $allowedTypes = ['text/csv'];

    /**
     * @var int|float Максимальный размер файла в байтах
     */
    protected int|float $maxSize = 100 * 1024 * 1024;

    /**
     * @var array Ошибки
     */
    protected array $errors = [];

    /**
     * @var string Путь для сохранения файлов
     */
    protected string $storagePath;

    /**
     * Необходимый минимум для работы с CSV файлами
     */
    public function __construct()
    {
        $this->files = $_FILES['files'] ?? [];
        $this->storagePath = $_ENV['FILES_STORAGE_PATH'] ?? ROOT_PATH . '/storage/csv';
    }

    /**
     * Обрабатываем загрузку файла
     * 
     * Основная логика в сервисе {@see CsvImportService}
     */
    public function upload(): void
    {
        if (!$this->validate()) {
            self::sendError($this->errors);
        }

        try {
            $publisher = new RabbitMQPublisher();
            $publishedFiles = [];
            $errors = [];

            foreach ($this->files['name'] as $key => $fileName) {
                // Сохраняем файл в storage
                $savedFilePath = $this->saveFile($key, $fileName);
                
                if (!$savedFilePath) {
                    $errors[] = "Не удалось сохранить файл: {$fileName}";
                    continue;
                }

                // Определяем приоритет на основе размера файла
                $fileSize = $this->files['size'][$key];
                $priority = $this->determinePriority($fileSize);

                // Публикуем задачу в RabbitMQ
                try {
                    $publisher->publishImportTask(
                        $savedFilePath,
                        $fileName,
                        $fileSize,
                        $priority
                    );
                    $publishedFiles[] = [
                        'file' => $fileName,
                        'priority' => $priority,
                        'size' => $fileSize,
                    ];
                } catch (\Exception $e) {
                    $errors[] = "Не удалось опубликовать задачу для файла {$fileName}: " . $e->getMessage();
                    // Удаляем сохраненный файл, если не удалось опубликовать задачу
                    if (file_exists($savedFilePath)) {
                        unlink($savedFilePath);
                    }
                }
            }

            $publisher->close();

            if (!empty($errors)) {
                self::sendError($errors);
            }

            self::sendResponse([
                'message' => 'Файлы успешно загружены и задачи отправлены в очередь',
                'published' => count($publishedFiles),
                'files' => $publishedFiles,
            ]);
        } catch (\Exception $e) {
            self::sendError('Ошибка при обработке файлов: ' . $e->getMessage());
        }
    }

    /**
     * Сохраняет загруженный файл в storage
     *
     * @param int $key Индекс файла в массиве
     * @param string $fileName Имя файла
     * @return string|null Путь к сохраненному файлу или null при ошибке
     */
    protected function saveFile(int $key, string $fileName): ?string
    {
        // Создаем директорию, если её нет
        if (!is_dir($this->storagePath)) {
            if (!mkdir($this->storagePath, 0755, true)) {
                $this->errors[] = "Не удалось создать директорию для сохранения файлов: {$this->storagePath}";
                return null;
            }
        }

        $tmpPath = $this->files['tmp_name'][$key];
        $uniqueName = uniqid('csv_', true) . '_' . basename($fileName);
        $destinationPath = $this->storagePath . '/' . $uniqueName;

        if (move_uploaded_file($tmpPath, $destinationPath)) {
            return $destinationPath;
        }

        return null;
    }
    
    /**
     * Определяет приоритет задачи на основе размера файла
     *
     * @param int $fileSize Размер файла в байтах
     * @return string Приоритет: 'high', 'normal', 'large'
     */
    protected function determinePriority(int $fileSize): string
    {
        // Файлы больше 20MB считаем большими
        if ($fileSize > 20 * 1024 * 1024) {
            return 'large';
        }
        
        // Файлы меньше 1MB считаем высокоприоритетными
        if ($fileSize < 1024 * 1024) {
            return 'high';
        }

        return 'normal';
    }

    /**
     * Валидация файлов
     *
     * @return bool
     */
    protected function validate(): bool
    {
        if (empty($this->files)) {
            $this->errors[] = "Необходимо выбрать один или множество файлов";
            return false;
        }

        foreach ($this->files['name'] as $key => $fileName) {
            if ($this->files['error'][$key] !== UPLOAD_ERR_OK) {
                $this->errors[] = "Файл `{$fileName}` загружен с ошибкой `{$this->files['error'][$key]}`";
                continue;
            }

            if (!in_array($this->files['type'][$key], $this->allowedTypes)) {
                $this->errors[] = "Файл `{$fileName}` должен быть в CSV формате";
                continue;
            }

            if ($this->files['size'][$key] > $this->maxSize) {
                $this->errors[] = "Файл `{$fileName}` не должен превышать {$this->maxSize} Мб";
            }
        }

        return empty($this->errors);
    }
}

