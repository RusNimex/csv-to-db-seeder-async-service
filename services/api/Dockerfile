FROM php:8.2-cli

# Устанавливаем системные зависимости и инструменты для компиляции
RUN apt-get update && apt-get install -y \
    libzip-dev \
    zip \
    unzip \
    libonig-dev \
    $PHPIZE_DEPS \
    && rm -rf /var/lib/apt/lists/*

# Устанавливаем PHP расширения
RUN docker-php-ext-install zip bcmath

# Устанавливаем Composer
COPY --from=composer:latest /usr/bin/composer /usr/bin/composer

# Устанавливаем рабочую директорию
WORKDIR /var/www/html

# Копируем файлы composer
COPY composer.json ./

# Устанавливаем зависимости (без dev зависимостей для production)
RUN composer install --no-dev --optimize-autoloader --no-interaction

# Копируем остальные файлы приложения
COPY . .

# Копируем и делаем исполняемым entrypoint скрипт
COPY docker-entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# Настраиваем PHP для загрузки больших файлов
RUN echo "upload_max_filesize = 100M" >> /usr/local/etc/php/conf.d/uploads.ini \
    && echo "post_max_size = 100M" >> /usr/local/etc/php/conf.d/uploads.ini \
    && echo "max_execution_time = 300" >> /usr/local/etc/php/conf.d/uploads.ini \
    && echo "memory_limit = 256M" >> /usr/local/etc/php/conf.d/uploads.ini

# Открываем порт 8001
EXPOSE 8001

# Запускаем через entrypoint скрипт
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]