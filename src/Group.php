<?php

namespace RedisStreamQueue;

class Group extends Stream
{
    private $group;
    private $consumerId;

    /**
     * RedisStreamQueue constructor.
     * @param $key string redis stream key, 由redis自動創立該stream
     * @param $group string 群組名稱, 若不存在stream內的話則會手動建立
     * @param $consumerId string 用於consumer group內的名稱, 同一個群組內不可重複
     */
    public function __construct(string $key, string $group, string $consumerId)
    {
        parent::__construct($key);
        self::createGroupIfNotExists($key, $group);
        $this->group = $group;
        $this->consumerId = $consumerId;
    }

    /**
     * 從stream中拿出指定數量的訊息, 預設僅拿出沒有被拿過的訊息
     * @param int $count 拿出多少訊息
     * @param string $id 傳入0或其他id字串可以拿來查詢pending佇列中的訊息
     * @param null $block 當沒有訊息在stream內時, 要等待訊息出現的最大毫秒數
     * @return mixed
     */
    public function pop(int $count = 1, string $id = '>', $block = null)
    {
        $newItem = Conn::get()->xReadGroup(
            $this->group,
            $this->consumerId,
            [$this->key => $id],
            $count,
            $block
        );

        return $newItem[$this->key];
    }

    /**
     * 從所有consumer的pending佇列中的已過期的訊息, 認領指定數量的訊息並改由自己執行, 該訊息的delivery count會+1
     * @param int $minIdleTime 訊息需經過多少毫秒才算逾時
     * @param int $count 要認領幾筆訊息
     * @param int $maxDelivery 要認領已經被拿過幾次以內的訊息
     * @param int $limit 最多要看幾筆pending的訊息
     * @return array
     */
    public function claim(int $minIdleTime, int $count = 1, int $maxDelivery = 10, int $limit = 100)
    {
        $claimedItems = [];
        $pendingItems = Conn::get()->xPending(
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
                $claimedItem = Conn::get()->xClaim(
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
     * 將判斷成失敗的工作刪除並備份到別的stream
     * @param null $newKey 訊息備份的目標stream key, 不指定則不會備份
     * @param int $maxDelivery 訊息要被拿過幾次才判斷成失敗
     * @param int $limit 最多要看幾筆pending的訊息
     * @return int
     */
    public function moveFailed($newKey = null, int $maxDelivery = 10, int $limit = 100)
    {
        $failedIds = [];
        $pendingItems = Conn::get()->xPending(
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
                    $message = Conn::get()->xRange($this->key, $messageId, $messageId);
                    $job = reset($message);
                    Conn::get()->xAdd($newKey, $messageId, $job);
                }
            }
            $this->ack($failedIds);
            $this->deleteJobs($failedIds);
        }

        return count($failedIds);
    }

    /**
     * 將閒置過久的consumer從consumer group中移除, pending在該consumer的訊息也會一併被移出pending佇列,
     * 且不會再被其他consumer領取, 故確認沒有pending訊息時才會移除
     * @param $idleTime
     * @return int
     */
    public function cleanConsumers(int $idleTime)
    {
        $deletedConsumers = 0;
        $consumers = Conn::get()->xInfo('CONSUMERS', $this->key, $this->group);
        if ($consumers) {
            foreach ($consumers as $consumer) {
                if (!$consumer['pending'] && $consumer['idle'] >= $idleTime) {
                    Conn::get()->xGroup(
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
     * 發送ACK給指定的所有訊息, 會將那些訊息從pending佇列中移除, 等於回報該工作完成
     * @param $messageIds
     * @return int
     */
    public function ack(array $messageIds)
    {
        return Conn::get()->xAck($this->key, $this->group, $messageIds);
    }
}
