// cmd/server/main.go

package main

import (
	"context"
	"log"
	"time"

	"github.com/potatocheng/Orchestrator/internal/service"
	"github.com/potatocheng/Orchestrator/internal/taskQ"

	"github.com/redis/go-redis/v9"
)

func main() {
	// 初始化 Redis 客户端
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer client.Close()

	rdb := taskQ.NewRDB(client)

	// 检查 Redis 连接
	if err := rdb.Ping(); err != nil {
		log.Fatalf("Failed to ping Redis: %v", err)
	}

	// 初始化任务注册表
	registry := service.TaskRegister

	// 注册任务处理器
	registry.Register("example_task", exampleTaskHandler)

	// 初始化 Scheduler
	scheduler := service.NewScheduler(rdb, registry, 5, time.Second*10, time.Second*30)

	// 运行 Scheduler
	ctx := context.Background()
	go scheduler.Start(ctx)

	// 阻塞主线程
	select {}
}

func exampleTaskHandler(ctx context.Context, task *taskQ.TaskMessage) error {
	// 处理任务逻辑
	log.Printf("Handling task: %s", task.ID)
	// 模拟任务处理
	time.Sleep(2 * time.Second)
	return nil
}
