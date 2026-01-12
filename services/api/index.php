<?php
/**
 * Точка входа в приложение
 */

require_once 'src/bootstrap.php';
require_once 'vendor/autoload.php';

// Загружаем .env файл если он существует
if (file_exists(ROOT_PATH . '/.env')) {
    $dotenv = Dotenv\Dotenv::createImmutable(ROOT_PATH);
    $dotenv->load();
}

// Загружаем переменные окружения из $_ENV
$_ENV = array_merge($_ENV, getenv());

header('Content-Type: application/json');

use App\Http\Router;

// Регистрируем маршруты
Router::get('/health', function() {
    Router::sendResponse(['message' => 'OK']);
});
Router::post('/upload', 'App\Controllers\UploadController::upload');
Router::end();
