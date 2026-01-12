-- Создание пользователя и предоставление прав
-- Пользователь может быть уже создан через переменные окружения MYSQL_USER/MYSQL_PASSWORD
-- Этот скрипт гарантирует, что права будут предоставлены корректно

CREATE USER IF NOT EXISTS 'csv_user'@'%' IDENTIFIED BY 'csv_pass';
GRANT ALL PRIVILEGES ON csv.* TO 'csv_user'@'%';
FLUSH PRIVILEGES;

