<?php

namespace RedisStreamQueue;

class Stream extends Redis
{
    protected $key;

    public function __construct($key)
    {
        $this->key = $key;
    }

    /**
     * Add the given messages into stream, message id default to auto generated
     * @param array $message
     * @param string $id message id
     * @return string
     */
    public function push(array $message, string $id = '*')
    {
        return Redis::get()->xAdd($this->key, $id, $message);
    }

    /**
     * Create consumer group under given stream key and group name
     * @param $key string redis stream key
     * @param $groupName string name of the group to be created
     */
    protected static function createGroupIfNotExists(string $key, string $groupName)
    {
        /** @noinspection PhpParamsInspection */
        $groups = Redis::get()->xInfo('GROUPS', $key);
        $exists = $groups ? (array_search($groupName, array_column($groups, 'name')) !== false) : false;
        if ($exists === false) {
            Redis::get()->xGroup('CREATE', $key, $groupName, 0);
        }
    }

    /**
     * Delete messages from stream using given message ids
     * @param array $ids
     * @return int
     */
    public function deleteJobs(array $ids)
    {
        return Redis::get()->xDel($this->key, $ids);
    }
}
