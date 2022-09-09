package delay_queue

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v9"
	my_redis "myProject/delay-queue/redis"
	"testing"
)

func TestDelayQueue_EnQueue(t *testing.T) {

	config := DelayQueueConfig{
		Logger:       nil,
		ZsetName:     "my_zset",
		HashName:     "my_hash",
		WorkerNum:    3,
		WorkerTicker: 0,
		Fn: func(ctx context.Context, value interface{}) error {
			return nil
		},
		BatchSize: 0,
		Rdb:       my_redis.RDB,
	}
	q := NewDelayQueue(config)

	for i := 0; i < 100; i++ {
		type Person struct {
			Name        string `json:"name"`
			ExecSeconds int64  `json:"exec_seconds"`
		}
		p := Person{Name: fmt.Sprintf("%v", i), ExecSeconds: int64(i)}

		bytelist, _ := json.Marshal(p)

		err := q.EnQueue(Item{
			ID:      fmt.Sprintf("%v", i),
			ExecTmp: p.ExecSeconds,
			Content: string(bytelist),
		})
		if err != redis.Nil {
			fmt.Println(err)
		}
	}

}

func TestDelayQueue_DeQueue(t *testing.T) {

	config := DelayQueueConfig{
		Logger:       nil,
		ZsetName:     "my_zset",
		HashName:     "my_hash",
		WorkerNum:    3,
		WorkerTicker: 0,
		Fn: func(ctx context.Context, value interface{}) error {
			return nil
		},
		BatchSize: 0,
		Rdb:       my_redis.RDB,
	}
	q := NewDelayQueue(config)

	res, err := q.DeQueue(221111112, 10)
	fmt.Printf("res-----%+v\n", res)
	fmt.Printf("err -----%+v\n", err)

}

func TestDelayQueue_Run(t *testing.T) {
	config := DelayQueueConfig{
		Logger:       nil,
		ZsetName:     "my_zset",
		HashName:     "my_hash",
		WorkerNum:    3,
		WorkerTicker: 0,
		Fn: func(ctx context.Context, value interface{}) error {
			return nil
		},
		BatchSize: 0,
		Rdb:       my_redis.RDB,
	}
	q := NewDelayQueue(config)
	q.Run(context.TODO())

}
