package delay_queue

import (
	"context"
	"github.com/go-redis/redis/v9"
	my_redis "myProject/delay-queue/redis"
)

type DelayQueue struct {
	zsetName string // redis zset 中存储 时间戳 --》 id
	hashName string // redis hash 中 存储  id (field) --> 具体信息（json）
}

func NewDelayQueue(zsetName, hashName string) DelayQueue {
	return DelayQueue{
		zsetName: zsetName,
		hashName: hashName,
	}
}

var EnqueueLua = redis.NewScript(
	`
local zset_key = KEYS[1]
local hash_key = KEYS[2]
local zset_value = ARGV[1]
local zset_score = ARGV[2]
local hash_field = ARGV[3]
local hash_value = ARGV[4]
redis.call('ZADD', zset_key, zset_score, zset_value)
redis.call('HSET', hash_key, hash_field, hash_value)
return nil
`)

func (d DelayQueue) EnQueue(seconds int64, id string, content string) error {

	keys := []string{d.zsetName, d.hashName}
	values := []interface{}{id, seconds, id, content}
	_, err := EnqueueLua.Run(context.TODO(), my_redis.RDB, keys, values...).Result()
	if err != nil {
		return err
	}

	return nil
}

var DeQueueLua = redis.NewScript(
	`
-- /lua/dequeue.lua
-- 参考jesque的部分Lua脚本实现
local zset_key = KEYS[1]
local hash_key = KEYS[2]
local min_score = ARGV[1]
local max_score = ARGV[2]
local offset = ARGV[3]
local limit = ARGV[4]
-- TYPE命令的返回结果是{'ok':'zset'}这样子,这里利用next做一轮迭代
local status, type = next(redis.call('TYPE', zset_key))
if status ~= nil and status == 'ok' then
    if type == 'zset' then
        local list = redis.call('ZREVRANGEBYSCORE', zset_key, max_score, min_score, 'LIMIT', offset, limit)
        print(list)
		if list ~= nil and #list > 0 then
            -- unpack函数能把table转化为可变参数
            redis.call('ZREM', zset_key, unpack(list))
            local result = redis.call('HMGET', hash_key, unpack(list))
            redis.call('HDEL', hash_key, unpack(list))
            return result
        end
    end
end
return nil
`)


func (d DelayQueue) DeQueue() (interface{}, error) {

	keys := []string{d.zsetName, d.hashName}
	values := []interface{}{0, 1111111111111, 0, 10}
	result, err := DeQueueLua.Run(context.TODO(), my_redis.RDB, keys, values...).Result()
	if err != nil {
		return nil, err
	}

	return result, nil
}
