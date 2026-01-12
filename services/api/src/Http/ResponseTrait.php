<?php

namespace App\Http;

/**
 * Однотипные методы для отправки ответа и ошибки
 */
trait ResponseTrait
{
    /**
     * Отправка ответа
     *
     * @param array $data
     * @param int $code
     * @return void
     */
    public static function sendResponse(array $data, int $code = 200): void
    {
        http_response_code($code);
        echo json_encode(['data' => $data]);
        exit();
    }

    /**
     * Отправка ошибки
     *
     * @param array|string $errors
     * @param int $code
     * @return void
     */
    public static function sendError(array|string $errors, int $code = 400): void
    {
        http_response_code($code);
        if (!is_array($errors)) {
            $errors = [$errors];
        }
        echo json_encode(['errors' => $errors]);
        exit();
    }
}

