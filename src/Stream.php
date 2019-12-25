<?php

namespace RedisStreamQueue;

class Stream
{
    protected $key;

    public function __construct($key)
    {
        $this->key = $key;
    }

    /**
     * 將指定的訊息新增進stream內, message id預設由redis自動產生
     * @param array $message
     * @param string $id 若要指定id可自行傳入
     * @return string
     */
    public function push(array $message, string $id = '*')
    {
        return Conn::get()->xAdd($this->key, $id, $message);
    }

    /**
     * 在指定的stream key和名稱下建立consumer group
     * @param $key string redis stream key
     * @param $groupName string name of the group to be created
     */
    protected static function createGroupIfNotExists(string $key, string $groupName)
    {
        /** @noinspection PhpParamsInspection */
        $groups = Conn::get()->xInfo('GROUPS', $key);
        $exists = $groups ? (array_search($groupName, array_column($groups, 'name')) !== false) : false;
        if ($exists === false) {
            Conn::get()->xGroup('CREATE', $key, $groupName, 0);
        }
    }

    /**
     * 將指定的訊息群從stream中刪除
     * @param array $ids
     * @return int
     */
    public function deleteJobs(array $ids)
    {
        return Conn::get()->xDel($this->key, $ids);
    }
}
