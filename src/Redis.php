<?php

namespace RedisStreamQueue;

class Redis
{
    private static $redis = null;
    private static $host = null;
    private static $password = null;
    private static $port = 6379;

    public static function setConnInfo($host, $password, $port = 6379)
    {
        self::$host = $host;
        self::$password = $password;
        self::$port = $port;
    }

    public static function get()
    {
        if (is_null(self::$host) || is_null(self::$password)) {
            echo 'Please provide connection info by calling setConnInfo() first.' . PHP_EOL;
        }
        if (is_null(self::$redis)) {
            self::$redis = new \Redis();
            self::$redis->pconnect(self::$host, self::$port);
            self::$redis->auth(self::$password);
        }
        return self::$redis;
    }
}
