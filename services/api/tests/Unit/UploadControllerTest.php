<?php

namespace Tests\Unit;

use App\Controllers\UploadController;
use PHPUnit\Framework\TestCase;
use ReflectionClass;
use ReflectionMethod;

/**
 * Тестовый класс-наследник UploadController для переопределения saveFile
 * Использует copy() вместо move_uploaded_file() для тестирования
 */
class TestableUploadController extends UploadController
{
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

        // В тестах используем copy() вместо move_uploaded_file()
        // так как обычные файлы не проходят проверку move_uploaded_file()
        if (file_exists($tmpPath) && copy($tmpPath, $destinationPath)) {
            return $destinationPath;
        }

        return null;
    }
}

/**
 * Unit тесты для UploadController
 */
class UploadControllerTest extends TestCase
{
    /**
     * @var string Путь к временной директории для тестов
     */
    private string $testStoragePath;

    /**
     * @var array Исходное состояние $_FILES
     */
    private array $originalFiles;

    /**
     * @var array Исходное состояние $_ENV
     */
    private array $originalEnv;

    protected function setUp(): void
    {
        parent::setUp();

        // Сохраняем исходное состояние
        $this->originalFiles = $_FILES ?? [];
        $this->originalEnv = $_ENV ?? [];

        // Создаем временную директорию для тестов
        $this->testStoragePath = sys_get_temp_dir() . '/csv_upload_test_' . uniqid();
        if (!is_dir($this->testStoragePath)) {
            mkdir($this->testStoragePath, 0755, true);
        }

        // Устанавливаем тестовый путь в переменные окружения
        $_ENV['FILES_STORAGE_PATH'] = $this->testStoragePath;
    }

    protected function tearDown(): void
    {
        // Восстанавливаем исходное состояние
        $_FILES = $this->originalFiles;
        $_ENV = $this->originalEnv;

        // Удаляем временную директорию и все файлы в ней
        if (is_dir($this->testStoragePath)) {
            $files = glob($this->testStoragePath . '/*');
            foreach ($files as $file) {
                if (is_file($file)) {
                    unlink($file);
                }
            }
            rmdir($this->testStoragePath);
        }

        parent::tearDown();
    }

    /**
     * Получает доступ к protected методу через рефлексию
     */
    private function getProtectedMethod(string $methodName): ReflectionMethod
    {
        $reflection = new ReflectionClass(TestableUploadController::class);
        $method = $reflection->getMethod($methodName);
        $method->setAccessible(true);
        return $method;
    }

    /**
     * Получает доступ к protected свойству через рефлексию
     */
    private function getProtectedProperty(string $propertyName, object $object): mixed
    {
        $reflection = new ReflectionClass(TestableUploadController::class);
        $property = $reflection->getProperty($propertyName);
        $property->setAccessible(true);
        return $property->getValue($object);
    }

    /**
     * Устанавливает значение protected свойства через рефлексию
     */
    private function setProtectedProperty(string $propertyName, object $object, mixed $value): void
    {
        $reflection = new ReflectionClass(TestableUploadController::class);
        $property = $reflection->getProperty($propertyName);
        $property->setAccessible(true);
        $property->setValue($object, $value);
    }

    /**
     * Получает путь к тестовому CSV файлу
     * 
     * @param string $fileName Имя файла из папки tests/csv
     * @return string Путь к файлу
     */
    private function getTestCsvFile(string $fileName): string
    {
        $testCsvPath = __DIR__ . '/../csv/' . $fileName;
        if (!file_exists($testCsvPath)) {
            $this->fail("Тестовый CSV файл не найден: {$testCsvPath}");
        }
        return $testCsvPath;
    }

    /**
     * Копирует тестовый CSV файл во временную директорию для использования в тестах
     * 
     * @param string $fileName Имя файла из папки tests/csv
     * @return string Путь к скопированному файлу
     */
    private function copyTestCsvFile(string $fileName): string
    {
        $sourceFile = $this->getTestCsvFile($fileName);
        $tmpFile = tempnam(sys_get_temp_dir(), 'test_csv_');
        copy($sourceFile, $tmpFile);
        return $tmpFile;
    }

    /**
     * Тест: Валидация проходит для корректного CSV файла
     */
    public function testValidateWithValidCsvFile(): void
    {
        $tmpFile = $this->copyTestCsvFile('Кафе-кондитерские.csv');
        $fileSize = filesize($tmpFile);

        $_FILES['files'] = [
            'name' => ['Кафе-кондитерские.csv'],
            'type' => ['text/csv'],
            'tmp_name' => [$tmpFile],
            'error' => [UPLOAD_ERR_OK],
            'size' => [$fileSize],
        ];

        $controller = new UploadController();
        $validateMethod = $this->getProtectedMethod('validate');
        $result = $validateMethod->invoke($controller);

        $this->assertTrue($result, 'Валидация должна пройти для корректного CSV файла');
        
        $errors = $this->getProtectedProperty('errors', $controller);
        $this->assertEmpty($errors, 'Не должно быть ошибок валидации');

        unlink($tmpFile);
    }

    /**
     * Тест: Валидация не проходит для пустого массива файлов
     */
    public function testValidateWithEmptyFiles(): void
    {
        $_FILES = [];

        $controller = new UploadController();
        $validateMethod = $this->getProtectedMethod('validate');
        $result = $validateMethod->invoke($controller);

        $this->assertFalse($result, 'Валидация должна не пройти для пустого массива файлов');
        
        $errors = $this->getProtectedProperty('errors', $controller);
        $this->assertNotEmpty($errors, 'Должна быть ошибка о необходимости выбора файлов');
        $this->assertStringContainsString('Необходимо выбрать', $errors[0]);
    }

    /**
     * Тест: Валидация не проходит для файла с ошибкой загрузки
     */
    public function testValidateWithUploadError(): void
    {
        $_FILES['files'] = [
            'name' => ['test.csv'],
            'type' => ['text/csv'],
            'tmp_name' => [''],
            'error' => [UPLOAD_ERR_INI_SIZE],
            'size' => [0],
        ];

        $controller = new UploadController();
        $validateMethod = $this->getProtectedMethod('validate');
        $result = $validateMethod->invoke($controller);

        $this->assertFalse($result, 'Валидация должна не пройти для файла с ошибкой загрузки');
        
        $errors = $this->getProtectedProperty('errors', $controller);
        $this->assertNotEmpty($errors, 'Должна быть ошибка о проблеме загрузки файла');
        $this->assertStringContainsString('загружен с ошибкой', $errors[0]);
    }

    /**
     * Тест: Валидация не проходит для файла неправильного типа
     */
    public function testValidateWithInvalidFileType(): void
    {
        $tmpFile = $this->copyTestCsvFile('Кафе-кондитерские.csv');
        $fileSize = filesize($tmpFile);

        $_FILES['files'] = [
            'name' => ['test.txt'],
            'type' => ['text/plain'],
            'tmp_name' => [$tmpFile],
            'error' => [UPLOAD_ERR_OK],
            'size' => [$fileSize],
        ];

        $controller = new UploadController();
        $validateMethod = $this->getProtectedMethod('validate');
        $result = $validateMethod->invoke($controller);

        $this->assertFalse($result, 'Валидация должна не пройти для файла неправильного типа');
        
        $errors = $this->getProtectedProperty('errors', $controller);
        $this->assertNotEmpty($errors, 'Должна быть ошибка о неправильном типе файла');
        $this->assertStringContainsString('должен быть в CSV формате', $errors[0]);

        unlink($tmpFile);
    }

    /**
     * Тест: Валидация не проходит для файла превышающего максимальный размер
     */
    public function testValidateWithFileExceedingMaxSize(): void
    {
        $tmpFile = $this->copyTestCsvFile('Кафе-кондитерские.csv');

        $_FILES['files'] = [
            'name' => ['test.csv'],
            'type' => ['text/csv'],
            'tmp_name' => [$tmpFile],
            'error' => [UPLOAD_ERR_OK],
            'size' => [101 * 1024 * 1024], // 101 MB, больше чем maxSize (100 MB)
        ];

        $controller = new UploadController();
        $validateMethod = $this->getProtectedMethod('validate');
        $result = $validateMethod->invoke($controller);

        $this->assertFalse($result, 'Валидация должна не пройти для файла превышающего максимальный размер');
        
        $errors = $this->getProtectedProperty('errors', $controller);
        $this->assertNotEmpty($errors, 'Должна быть ошибка о превышении размера файла');
        $this->assertStringContainsString('не должен превышать', $errors[0]);

        unlink($tmpFile);
    }

    /**
     * Тест: Валидация проходит для нескольких корректных CSV файлов
     */
    public function testValidateWithMultipleValidFiles(): void
    {
        $tmpFile1 = $this->copyTestCsvFile('Кафе-кондитерские.csv');
        $tmpFile2 = $this->copyTestCsvFile('Пункты техосмотра.csv');
        $fileSize1 = filesize($tmpFile1);
        $fileSize2 = filesize($tmpFile2);

        $_FILES['files'] = [
            'name' => ['Кафе-кондитерские.csv', 'Пункты техосмотра.csv'],
            'type' => ['text/csv', 'text/csv'],
            'tmp_name' => [$tmpFile1, $tmpFile2],
            'error' => [UPLOAD_ERR_OK, UPLOAD_ERR_OK],
            'size' => [$fileSize1, $fileSize2],
        ];

        $controller = new UploadController();
        $validateMethod = $this->getProtectedMethod('validate');
        $result = $validateMethod->invoke($controller);

        $this->assertTrue($result, 'Валидация должна пройти для нескольких корректных CSV файлов');
        
        $errors = $this->getProtectedProperty('errors', $controller);
        $this->assertEmpty($errors, 'Не должно быть ошибок валидации');

        unlink($tmpFile1);
        unlink($tmpFile2);
    }

    /**
     * Тест: Валидация не проходит если хотя бы один файл невалиден
     */
    public function testValidateWithMixedValidAndInvalidFiles(): void
    {
        $tmpFile1 = $this->copyTestCsvFile('Кафе-кондитерские.csv');
        $tmpFile2 = $this->copyTestCsvFile('Пункты техосмотра.csv');
        $fileSize1 = filesize($tmpFile1);
        $fileSize2 = filesize($tmpFile2);

        $_FILES['files'] = [
            'name' => ['Кафе-кондитерские.csv', 'test2.txt'],
            'type' => ['text/csv', 'text/plain'],
            'tmp_name' => [$tmpFile1, $tmpFile2],
            'error' => [UPLOAD_ERR_OK, UPLOAD_ERR_OK],
            'size' => [$fileSize1, $fileSize2],
        ];

        $controller = new UploadController();
        $validateMethod = $this->getProtectedMethod('validate');
        $result = $validateMethod->invoke($controller);

        $this->assertFalse($result, 'Валидация должна не пройти если хотя бы один файл невалиден');
        
        $errors = $this->getProtectedProperty('errors', $controller);
        $this->assertNotEmpty($errors, 'Должна быть ошибка для невалидного файла');

        unlink($tmpFile1);
        unlink($tmpFile2);
    }

    /**
     * Тест: Файл успешно сохраняется в папку
     */
    public function testSaveFileSuccessfully(): void
    {
        $tmpFile = $this->copyTestCsvFile('Кафе-кондитерские.csv');
        $fileName = 'Кафе-кондитерские.csv';
        $originalContent = file_get_contents($tmpFile);
        $fileSize = filesize($tmpFile);

        $_FILES['files'] = [
            'name' => [$fileName],
            'type' => ['text/csv'],
            'tmp_name' => [$tmpFile],
            'error' => [UPLOAD_ERR_OK],
            'size' => [$fileSize],
        ];

        $controller = new TestableUploadController();
        
        // Устанавливаем тестовый путь через рефлексию
        $this->setProtectedProperty('storagePath', $controller, $this->testStoragePath);
        $this->setProtectedProperty('files', $controller, $_FILES['files']);

        $saveFileMethod = $this->getProtectedMethod('saveFile');
        $result = $saveFileMethod->invoke($controller, 0, $fileName);

        $this->assertNotNull($result, 'Метод saveFile должен вернуть путь к сохраненному файлу');
        $this->assertIsString($result, 'Результат должен быть строкой');
        $this->assertStringStartsWith($this->testStoragePath, $result, 'Файл должен быть сохранен в указанную директорию');
        $this->assertFileExists($result, 'Файл должен существовать на диске');
        $this->assertStringContainsString($fileName, $result, 'Имя файла должно содержать оригинальное имя');

        // Проверяем содержимое файла
        $this->assertEquals(
            $originalContent,
            file_get_contents($result),
            'Содержимое сохраненного файла должно совпадать с оригиналом'
        );

        // Очистка
        if (file_exists($tmpFile)) {
            unlink($tmpFile);
        }
        if (file_exists($result)) {
            unlink($result);
        }
    }

    /**
     * Тест: Директория создается если её не существует
     */
    public function testSaveFileCreatesDirectoryIfNotExists(): void
    {
        $nonExistentPath = sys_get_temp_dir() . '/csv_upload_new_dir_' . uniqid();
        $tmpFile = $this->copyTestCsvFile('Пункты техосмотра.csv');
        $fileName = 'Пункты техосмотра.csv';
        $fileSize = filesize($tmpFile);

        $_FILES['files'] = [
            'name' => [$fileName],
            'type' => ['text/csv'],
            'tmp_name' => [$tmpFile],
            'error' => [UPLOAD_ERR_OK],
            'size' => [$fileSize],
        ];

        $controller = new TestableUploadController();
        
        // Устанавливаем несуществующий путь
        $this->setProtectedProperty('storagePath', $controller, $nonExistentPath);
        $this->setProtectedProperty('files', $controller, $_FILES['files']);

        $saveFileMethod = $this->getProtectedMethod('saveFile');
        $result = $saveFileMethod->invoke($controller, 0, $fileName);

        $this->assertNotNull($result, 'Метод saveFile должен создать директорию и сохранить файл');
        $this->assertDirectoryExists($nonExistentPath, 'Директория должна быть создана');
        $this->assertFileExists($result, 'Файл должен быть сохранен');

        // Очистка
        if (file_exists($tmpFile)) {
            unlink($tmpFile);
        }
        if (file_exists($result)) {
            unlink($result);
        }
        if (is_dir($nonExistentPath)) {
            rmdir($nonExistentPath);
        }
    }

    /**
     * Тест: Сохранение файла возвращает null при ошибке перемещения
     */
    public function testSaveFileReturnsNullOnMoveError(): void
    {
        $fileName = 'test.csv';

        $_FILES['files'] = [
            'name' => [$fileName],
            'type' => ['text/csv'],
            'tmp_name' => ['/nonexistent/path/file.csv'], // Несуществующий файл
            'error' => [UPLOAD_ERR_OK],
            'size' => [1024],
        ];

        $controller = new TestableUploadController();
        
        $this->setProtectedProperty('storagePath', $controller, $this->testStoragePath);
        $this->setProtectedProperty('files', $controller, $_FILES['files']);

        $saveFileMethod = $this->getProtectedMethod('saveFile');
        $result = $saveFileMethod->invoke($controller, 0, $fileName);

        $this->assertNull($result, 'Метод saveFile должен вернуть null при ошибке перемещения файла');
    }

    /**
     * Тест: Сохраненный файл имеет уникальное имя
     */
    public function testSaveFileGeneratesUniqueFileName(): void
    {
        $tmpFile1 = $this->copyTestCsvFile('Кафе-кондитерские.csv');
        $tmpFile2 = $this->copyTestCsvFile('Пункты техосмотра.csv');
        $fileName = 'test.csv';
        $fileSize1 = filesize($tmpFile1);
        $fileSize2 = filesize($tmpFile2);

        $_FILES['files'] = [
            'name' => [$fileName, $fileName],
            'type' => ['text/csv', 'text/csv'],
            'tmp_name' => [$tmpFile1, $tmpFile2],
            'error' => [UPLOAD_ERR_OK, UPLOAD_ERR_OK],
            'size' => [$fileSize1, $fileSize2],
        ];

        $controller = new TestableUploadController();
        
        $this->setProtectedProperty('storagePath', $controller, $this->testStoragePath);
        $this->setProtectedProperty('files', $controller, $_FILES['files']);

        $saveFileMethod = $this->getProtectedMethod('saveFile');
        $result1 = $saveFileMethod->invoke($controller, 0, $fileName);
        $result2 = $saveFileMethod->invoke($controller, 1, $fileName);

        $this->assertNotEquals($result1, $result2, 'Каждый сохраненный файл должен иметь уникальное имя');
        $this->assertFileExists($result1);
        $this->assertFileExists($result2);

        // Очистка
        if (file_exists($tmpFile1)) {
            unlink($tmpFile1);
        }
        if (file_exists($tmpFile2)) {
            unlink($tmpFile2);
        }
        if (file_exists($result1)) {
            unlink($result1);
        }
        if (file_exists($result2)) {
            unlink($result2);
        }
    }
}
