<?php

namespace App\Http;

use Closure;

/**
 * Класс маршрутизатора
 */
class Router 
{
    use ResponseTrait;

    /**
     * @var Router|null Создадим экземпляр класса
     */
    protected static ?Router $instance = null;

    /**
     * @var array Параметры и значения запроса
     */
    protected array $params;

    /**
     * @var array Параметры, которые нужно исключить
     */
    protected array $excludedParams = [
        'XDEBUG_SESSION',
        'OTHER_KEY'
    ];
    
    /**
     * Насытим параметрами
     *
     * @param array $params
     * @return void
     */
    public function setParams(array $params): void
    {
        $result = array_filter($params, function($param) {
            return !in_array($param, $this->excludedParams);
        }, ARRAY_FILTER_USE_KEY);

        $this->params = $result;
    }
    
    /**
     * Добавим обраотку GET - запросов
     *
     * @param string $route
     * @param callable|string $callback
     * @return void
     */
    public static function get(string $route, callable|string $callback): void
    {
        if ($_SERVER['REQUEST_METHOD'] !== 'GET') {
            return;
        }
        
        $uri =  parse_url($_SERVER['REQUEST_URI'], PHP_URL_PATH);
        if ($route === $uri) {
            $router = self::create();
            $router->setParams($_GET ?? []);
            $router->execute($callback);
        }
    }
    
    /**
     * Создадим экземпляр класса-одиночки
     *
     * @return self
     */
    protected static function create(): self
    {
        if (self::$instance !== null) {
            return self::$instance;
        }

        return new self();
    }

    /**
     * Получим параметры и передадим их в контроллер
     *
     * @param callable|string $callback
     * @return void
     */
    protected function execute(callable|string $callback): void
    {
        if ($callback instanceof Closure) {
            call_user_func_array($callback, $this->params);
            return;
        }

        if (strpos($callback, '::')) {
            [$controllerName, $actionName] = explode('::', $callback);
            if (!class_exists($controllerName)) {
                return;
            }
            if (!method_exists($controllerName, $actionName)) {
                return;
            }
            $controller = new $controllerName();
            $controller->$actionName($this->params);
        }
    }
    
    /**
     * Добавим обраотку POST - запросов
     *
     * @param string $route
     * @param callable|string $callback
     * @return void
     */
    public static function post(string $route, callable|string $callback): void
    {
        if ($_SERVER['REQUEST_METHOD'] !== 'POST') {
            return;
        }

        $uri = parse_url($_SERVER['REQUEST_URI'], PHP_URL_PATH);

        if ($uri === $route) {
            $router = self::create();
            $router->setParams($_POST ?? []);
            $router->execute($callback);
        }
    }

    /**
     * Отправим ошибку, если маршрут не найден
     *
     * @return void
     */
    public static function end(): void
    {
        self::sendError('Route not found', 404);
    }
}

