package delay_queue

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestDelayQueue_EnQueue(t *testing.T) {
	q := NewDelayQueue("my_zset", "my_hash")

	type Person struct {
		Name        string `json:"name"`
		ExecSeconds int64  `json:"exec_seconds"`
	}
	p := Person{Name: "李四", ExecSeconds: 22222}

	bytelist, _ := json.Marshal(p)

	_ = q.EnQueue(22222, "2", string(bytelist))

}

func TestDelayQueue_DeQueue(t *testing.T) {

	q := NewDelayQueue("my_zset", "my_hash")

	res, err := q.DeQueue()
	fmt.Printf("res-----%+v\n", res)
	fmt.Printf("err -----%+v\n", err)

}
