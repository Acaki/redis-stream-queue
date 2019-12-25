<?php

namespace RedisStreamQueue;

class RedisConn
{
    private static $redis = null;

    public static function get()
    {
        if (!getenv('REDIS_HOST') || !getenv('REDIS_PASS')) {
            echo 'Please set REDIS_HOST and REDIS_PASS environment variables respectively.' . PHP_EOL;
        }
        if (is_null(self::$redis)) {
            self::$redis = new \Redis();
            self::$redis->pconnect(getenv('REDIS_HOST'), 6379);
            self::$redis->auth(getenv('REDIS_PASS'));
        }
        return self::$redis;
    }
}
