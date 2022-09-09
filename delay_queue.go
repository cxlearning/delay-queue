package delay_queue

import (
	"context"
	"errors"
	"fmt"
	"github.com/go-redis/redis/v9"
	"log"
	"reflect"
	"strings"
	"sync"
	"time"
)

type DelayQueue struct {
	Logger       loggerFunc
	zsetName     string        // redis zset 中存储 时间戳 --》 id
	hashName     string        // redis hash 中 存储  id (field) --> 具体信息（json）
	workerNum    int           // 工作协程数量
	workerTicker time.Duration // 工作协程中执行时间间隔
	fn           Process       // 处理逻辑
	batchSize    int           // 单次从redis中获取数据个数
	rdb          *redis.Client
}

type DelayQueueConfig struct {
	Logger       loggerFunc
	ZsetName     string        // redis zset 中存储 时间戳 --》 id
	HashName     string        // redis hash 中 存储  id (field) --> 具体信息（json）
	WorkerNum    int           // 工作协程数量
	WorkerTicker time.Duration // 工作协程中执行时间间隔
	Fn           Process       // 处理逻辑
	BatchSize    int           // 单次从redis中获取数据个数
	Rdb          *redis.Client
}

func NewDelayQueue(config DelayQueueConfig) DelayQueue {
	if config.ZsetName == "" || config.HashName == "" || config.Fn == nil || config.Rdb == nil {
		panic("missing required parameters")
	}
	if config.Logger == nil {
		config.Logger = func(level, format string, args ...interface{}) {
			level = strings.TrimSpace(strings.ToUpper(level))
			log.Printf("[%s] %s", level, fmt.Sprintf(format, args...))
		}
	}
	if config.WorkerNum == 0 {
		config.WorkerNum = 1
	}
	if config.WorkerTicker == 0 {
		config.WorkerTicker = time.Second
	}
	if config.BatchSize == 0 {
		config.BatchSize = 30
	}

	return DelayQueue{
		Logger:       config.Logger,
		zsetName:     config.ZsetName,
		hashName:     config.HashName,
		workerNum:    config.WorkerNum,
		workerTicker: config.WorkerTicker,
		fn:           config.Fn,
		batchSize:    config.BatchSize,
		rdb:          config.Rdb,
	}
}

//func NewDelayQueue(zsetName, hashName string, workerNum int, fn Process, rdb *redis.Client) DelayQueue {
//	return DelayQueue{
//		Logger: func(level, format string, args ...interface{}) {
//			level = strings.TrimSpace(strings.ToUpper(level))
//			log.Printf("[%s] %s", level, fmt.Sprintf(format, args...))
//		},
//		zsetName:  zsetName,
//		hashName:  hashName,
//		workerNum: workerNum,
//		fn:        fn,
//		batchSize: 10,
//		rdb:       rdb,
//	}
//}

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

type Item struct {
	ID      string `json:"id"`
	ExecTmp int64  `json:"exec_tmp"`
	Content string `json:"content"`
}

func (d DelayQueue) EnQueue(item Item) error {

	keys := []string{d.zsetName, d.hashName}
	values := []interface{}{item.ID, item.ExecTmp, item.ID, item.Content}
	_, err := EnqueueLua.Run(context.TODO(), d.rdb, keys, values...).Result()
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

func (d DelayQueue) DeQueue(maxScore int64, batchSize int) ([]interface{}, error) {

	keys := []string{d.zsetName, d.hashName}
	values := []interface{}{"-inf", maxScore, 0, batchSize}
	result, err := DeQueueLua.Run(context.TODO(), d.rdb, keys, values...).Result()
	if err != nil {
		return nil, err
	}

	list, ok := result.([]interface{})
	if !ok {
		panic(fmt.Errorf("expecting list, got %s", reflect.TypeOf(list)))
	}

	d.log("debug", "moved %d items", len(list))
	return list, nil
}

// 具体处理逻辑
type Process func(ctx context.Context, value interface{}) error

/**
该函数控制所有的工作线程
*/
func (d DelayQueue) Run(ctx context.Context) error {
	// 包装一层， 该函数退出， 监听改ctx的协程都退出
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	wg := sync.WaitGroup{}

	for i := 0; i < d.workerNum; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			if err := d.worker(ctx, id); err != nil && !errors.Is(err, context.Canceled) {
				d.log("warn", "worker-%d died: %v", id, err)
			} else {
				d.log("info", "worker-%d exited gracefully", id)
			}
		}(i)
	}

	// 确保工作线程存在
	wg.Wait()

	d.log("info", "all workers returned, DelayQueue shutting down")
	return nil

}

/**
工作线程， 定时启用workerDo
*/
func (d DelayQueue) worker(ctx context.Context, workerID int) (err error) {
	defer func() {
		if v := recover(); v != nil {
			err = fmt.Errorf("panic: %v", v)
		}
	}()

	pollTimer := time.NewTimer(d.workerTicker)
	defer pollTimer.Stop()

	for {
		select {
		case <-pollTimer.C:
			pollTimer.Reset(d.workerTicker)
			if err = d.workerDo(ctx, workerID); err != nil {
				d.log("warn", err.Error())
				return
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}

}

/**
具体的一次工作，
从redis中拿数据，并处理
*/
func (d DelayQueue) workerDo(ctx context.Context, workerID int) error {

	maxScore := time.Now().Unix()

	list, err := d.DeQueue(maxScore, d.batchSize)
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil
		}
		d.log("warn", err.Error())
		return err
	}

	for _, _bin := range list {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			_ = d.fn(ctx, _bin)
		}
	}
	return nil
}

type loggerFunc func(level, format string, args ...interface{})

func (d DelayQueue) log(level, format string, args ...interface{}) {
	if d.Logger == nil {
		return
	}
	d.Logger(level, format, args...)
}
