<?php

namespace RedisStreamQueue;

class QueueStats
{
    /**
     * Get jobs in the queue
     * @param string $key
     * @param null|int $limit
     * @param string $start
     * @return array
     */
    public static function getJobs(string $key, $limit = null, $start = '0-0')
    {
        $jobs = Redis::get()->xRead([$key => $start], $limit);
        return isset($jobs[$key]) ? $jobs[$key] : [];
    }

    /**
     * Get current job count in queue
     * @param string $key
     * @return mixed
     */
    public static function getJobCount(string $key)
    {
        /** @noinspection PhpParamsInspection */
        $jobs = Redis::get()->xInfo('STREAM', $key);
        return $jobs ? $jobs['length'] : 0;
    }

    public static function getFirstJob(string $key)
    {
        /** @noinspection PhpParamsInspection */
        $jobs = Redis::get()->xInfo('STREAM', $key);
        return $jobs ? $jobs['first-entry'] : [];
    }

    public static function getLastJob(string $key)
    {
        /** @noinspection PhpParamsInspection */
        $jobs = Redis::get()->xInfo('STREAM', $key);
        return $jobs ? $jobs['last-entry'] : [];
    }

    /**
     * Get pending jobs
     * @param string $key
     * @param string $group consumer group id
     * @param int $limit
     * @param string $start minimum job id to be get
     * @param string $end maximum job id to be get
     * @return array each array element has the format [job id, consumer id, idle time, retry count]
     */
    public static function getRunningJobs(string $key, string $group, int $limit, $start = '-', $end = '+')
    {
        $jobs = Redis::get()->xPending(
            $key,
            $group,
            $start,
            $end,
            $limit
        );
        return is_array($jobs) ? $jobs : [];
    }

    /**
     * Get number of pending jobs in queue
     * @param string $key
     * @param string $group
     * @return int
     */
    public static function getRunningCount(string $key, string $group)
    {
        $pendingJobs = Redis::get()->xPending($key, $group);
        return $pendingJobs ? $pendingJobs[0] : 0;
    }
}