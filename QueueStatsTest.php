<?php

namespace RedisStreamQueue;

use PHPUnit\Framework\TestCase;

class QueueStatsTest extends TestCase
{
    public static function setUpBeforeClass()
    {
        Redis::setConnInfo(getenv('REDIS_HOST'), getenv('REDIS_PASS'));
    }

    public function testGetJobs()
    {
        $jobs = QueueStats::getJobs(getenv('REDIS_KEY'), 100);
        $this->assertLessThanOrEqual(100, count($jobs));
        $this->assertIsArray($jobs);
    }

    public function testGetJobCount()
    {
        $jobCount = QueueStats::getJobCount(getenv('REDIS_KEY'));
        $this->assertIsInt($jobCount);
    }

    public function testGetFirstJob()
    {
        $job = QueueStats::getFirstJob(getenv('REDIS_KEY'));
        $this->assertIsArray($job);
    }

    public function testGetLastJob()
    {
        $job = QueueStats::getLastJob(getenv('REDIS_KEY'));
        $this->assertIsArray($job);
    }

    public function testGetPendingJobs()
    {
        $pendingJobs = QueueStats::getRunningJobs(getenv('REDIS_KEY'), getenv('REDIS_GROUP'), 1000);
        $this->assertIsArray($pendingJobs);
        $this->assertLessThanOrEqual(1000, count($pendingJobs));
    }

    public function testGetPendingCount()
    {
        $count = QueueStats::getRunningCount(getenv('REDIS_KEY'), getenv('REDIS_GROUP'));
        $this->assertIsInt($count);
    }
}
