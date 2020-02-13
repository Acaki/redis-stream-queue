<?php

namespace RedisStreamQueue;

class Group extends Stream
{
    private $group;
    private $consumerId;

    /**
     * RedisStreamQueue constructor.
     * @param $key string redis stream key, created automatically by redis
     * @param $group string consumer group name, will manually create it if not exists
     * @param $consumerId string the unique consumer id used in consumer group
     */
    public function __construct(string $key, string $group, string $consumerId)
    {
        parent::__construct($key);
        self::createGroupIfNotExists($key, $group);
        $this->group = $group;
        $this->consumerId = $consumerId;
    }

    public function setGroup($group)
    {
        $this->group = $group;
    }

    public function setConsumerId($consumerId)
    {
        $this->consumerId = $consumerId;
    }

    /**
     * Read given number of messages from stream, default read with id '>'
     * @param int $count how many messages to read
     * @param string $id the begin of message id to be read
     * @param null $block when there are no messages in stream, how many ms to block
     * @return mixed
     */
    public function pop(int $count = 1, string $id = '>', $block = null)
    {
        $newItem = Redis::get()->xReadGroup(
            $this->group,
            $this->consumerId,
            [$this->key => $id],
            $count,
            $block
        );

        return $newItem[$this->key] ?? [];
    }

    /**
     * Claim given number of messages from all pending queues, and increment delivery count of the message by 1
     * @param int $minIdleTime ms for the message to be considered expired
     * @param int $count how many messages to claim
     * @param int $maxDelivery the minimum delivery count for the message to be claimed
     * @param int $limit how many pending messages to look up for claiming
     * @return array
     */
    public function claim(int $minIdleTime, int $count = 1, int $maxDelivery = 10, int $limit = 100)
    {
        $claimedItems = [];
        $pendingItems = Redis::get()->xPending(
            $this->key,
            $this->group,
            '-',
            '+',
            $limit
        );
        if ($pendingItems) {
            foreach ($pendingItems as $pendingItem) {
                [$messageId, , $idleTime, $deliveryCnt] = $pendingItem;
                if ($idleTime < $minIdleTime || $deliveryCnt >= $maxDelivery) {
                    continue;
                }
                $claimedItem = Redis::get()->xClaim(
                    $this->key,
                    $this->group,
                    $this->consumerId,
                    $minIdleTime,
                    [$messageId],
                    ['RETRYCOUNT' => $deliveryCnt + 1]
                );
                if ($claimedItem) {
                    $claimedItems = array_merge($claimedItems, $claimedItem);
                    $count--;
                }
                if (!$count) {
                    break;
                }
            }
        }

        return $claimedItems;
    }

    /**
     * Delete the failed jobs and move them to the given stream for backup purpose
     * @param null $newKey stream key for backup, if not provided then delete only
     * @param int $maxDelivery minimum delivery count for the message to be considered failed
     * @param int $limit how many pending messages to look up for claiming
     * @return int
     */
    public function moveFailed($newKey = null, int $maxDelivery = 10, int $limit = 100)
    {
        $failedIds = [];
        $pendingItems = Redis::get()->xPending(
            $this->key,
            $this->group,
            '-',
            '+',
            $limit
        );
        if ($pendingItems) {
            foreach ($pendingItems as $pendingItem) {
                [$messageId, , , $deliveryCnt] = $pendingItem;
                if ($deliveryCnt < $maxDelivery) {
                    continue;
                }
                $failedIds[] = $messageId;
                if (!is_null($newKey)) {
                    $message = Redis::get()->xRange($this->key, $messageId, $messageId);
                    $job = reset($message);
                    if ($job) {
                        Redis::get()->xAdd($newKey, $messageId, $job);
                    }
                }
            }
            $this->ack($failedIds);
            $this->deleteJobs($failedIds);
        }

        return count($failedIds);
    }

    /**
     * Remove consumers that has been idling for too long only if their pending message queues are empty
     * @param $idleTime
     * @return int
     */
    public function cleanConsumers(int $idleTime)
    {
        $deletedConsumers = 0;
        $consumers = Redis::get()->xInfo('CONSUMERS', $this->key, $this->group);
        if ($consumers) {
            foreach ($consumers as $consumer) {
                if (!$consumer['pending'] && $consumer['idle'] >= $idleTime) {
                    Redis::get()->xGroup(
                        'DELCONSUMER',
                        $this->key,
                        $this->group,
                        $consumer['name']
                    );
                    $deletedConsumers++;
                }
            }
        }

        return $deletedConsumers;
    }

    /**
     * Send ACK for given messages, which removes them from the pending queue
     * @param $messageIds
     * @return int
     */
    public function ack(array $messageIds)
    {
        return Redis::get()->xAck($this->key, $this->group, $messageIds);
    }
}
