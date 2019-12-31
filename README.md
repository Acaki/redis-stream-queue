# Basic usage

```php
<?php

use RedisStreamQueue\Group;

// Connect to redis
Group::setConnInfo(REDIS_HOST, REDIS_PASSWORD);
$queue = new Group('test-key', 'test-group', 'test-consumer');
$queue->push('hello world');
$item = $queue->pop();
$messageId = key($item);
$message = reset($item);

// Do something to the message
// ...

$queue->ack($messageId);

```
