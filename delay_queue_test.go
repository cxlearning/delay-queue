package delay_queue

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
)

func TestDelayQueue_EnQueue(t *testing.T) {
	q := NewDelayQueue("my_zset", "my_hash",3, func(ctx context.Context, value interface{}) error {
		return nil
	})

	for i := 0; i < 100; i++ {
		type Person struct {
			Name        string `json:"name"`
			ExecSeconds int64  `json:"exec_seconds"`
		}
		p := Person{Name: fmt.Sprintf("%v", i), ExecSeconds: int64(i)}

		bytelist, _ := json.Marshal(p)

		_ = q.EnQueue(Item{
			ID:     fmt.Sprintf("%v", i),
			ExecTmp:p.ExecSeconds,
			Content: string(bytelist),
		})
	}

}

func TestDelayQueue_DeQueue(t *testing.T) {

	q := NewDelayQueue("my_zset", "my_hash",3, func(ctx context.Context, value interface{}) error {
		return nil
	})

	res, err := q.DeQueue(221111112, 10)
	fmt.Printf("res-----%+v\n", res)
	fmt.Printf("err -----%+v\n", err)

}

func TestDelayQueue_Run(t *testing.T) {
	q := NewDelayQueue("my_zset", "my_hash",3, func(ctx context.Context, value interface{}) error {
		return nil
	})
	q.Run(context.TODO())

}
