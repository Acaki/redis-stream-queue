# Introduction
Reliable message queue implemented using Redis Stream, no message drops compares to pub/sub and lists.
* Support basic queue pop/push/ack operation
* Messages won't loss unless you explicitly delete them
* Support multiple clients read from a single queue using consumer groups from Redis Stream.
* Retrieve queue statistics with ease

# Usage

```php
<?php

use RedisStreamQueue\Group;
use RedisStreamQueue\Stream;

// Connect to redis
Group::setConnInfo(REDIS_HOST, REDIS_PASSWORD);

// Create an empty stream using given key name
Stream::create('test-key');

// Implicit create a consumer group for given stream key and group name
$queue = new Group('test-key', 'test-group', 'test-consumer');

// Push a message into queue
$queue->push(['hello' => 'world', 'key' => 'value']);

// Pop a message from queue, default returns only new messages
$item = $queue->pop();

// Claim a job that has been processed over 1 minute from other consumers
$expiredItem = $queue->claim(60000);

$messageId = key($item);
$message = reset($item);

// Do something to the message
// ...

$queue->ack([$messageId]);

// After you sent ack to the job, you can explicit delete the job if you like
$queue->deleteJobs([$messageId]);

// Clean up consumers that has idle over 1 hour
$queue->cleanConsumers(3600000);

// Move failed jobs (default: delivery count > 10) to another stream for further investigation
$queue->moveFailed('test-key-failed'); 

```


# Reference
[phpredis](https://github.com/phpredis/phpredis)  
[Redis Stream Introduction](https://redis.io/topics/streams-intro)

