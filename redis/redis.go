package my_redis

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v9"
	"log"
	"time"
)

func init() {
	Init(&redis.Options{
		Addr:     "****",
		Password: "**",
		DB:       2,
		PoolSize: 10,
	})
}

var RDB *redis.Client

func Init(ops *redis.Options) {
	client := redis.NewClient(ops)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second) // 1s 做为缓存已经太长了
	defer cancel()
	_, err := client.Ping(ctx).Result()
	if err != nil {
		panic(fmt.Sprintf("Connect redis failed. Error : %v", err))
	}
	RDB = client
	log.Println("redis connect success")
}