#!/bin/bash
set -e

# Устанавливаем зависимости composer, если их нет
if [ ! -d "vendor" ] || [ ! -f "vendor/autoload.php" ]; then
    echo "Установка зависимостей Composer..."
    composer install --no-dev --optimize-autoloader --no-interaction
fi

# Запускаем PHP сервер
exec php -S 0.0.0.0:8001 index.php

