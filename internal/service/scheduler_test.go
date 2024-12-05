// internal/service/scheduler_test.go

package service

import (
	"context"
	"fmt"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/potatocheng/Orchestrator/internal/taskQ"
	"github.com/redis/go-redis/v9"
)

// TestScheduler_Process1000Tasks 测试 Scheduler 处理1000个任务
func TestScheduler_Process1000Tasks(t *testing.T) {
	// 初始化 Redis 客户端
	client := redis.NewClient(&redis.Options{
		Addr: "0.0.0.0:6379",
		DB:   0,
	})
	defer client.Close()

	// 清空测试队列
	err := client.FlushDB(context.Background()).Err()
	if err != nil {
		t.Fatalf("Failed to flush Redis DB: %v", err)
	}

	// 初始化 RDB
	rdb := taskQ.NewRDB(client)

	// 初始化 TaskRegistry
	registry := NewTaskRegistry()

	// 使用 WaitGroup 来等待所有任务完成
	var wg sync.WaitGroup
	wg.Add(1000)

	// 注册任务处理器
	registry.Register("test_task", func(ctx context.Context, task *taskQ.TaskMessage) error {
		// 模拟任务处理时间
		time.Sleep(10 * time.Millisecond)
		// 任务完成后打印日志
		log.Printf("Processed task ID: %s", task.ID)
		wg.Done()
		return nil
	})

	// 初始化 Scheduler
	scheduler := NewScheduler(rdb, registry, 10, 2*time.Second, 30*time.Second)

	// 启动 Scheduler
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go scheduler.Run(ctx)

	// Enqueue 1000 tasks
	for i := 0; i < 1000; i++ {
		task := &taskQ.Task{
			Payload: []byte(fmt.Sprintf("payload-%d", i)),
			Handler: "test_task",
		}
		err := rdb.EnqueueTask(context.Background(), task)
		if err != nil {
			t.Fatalf("Failed to enqueue task %d: %v", i, err)
		}
	}

	// 等待所有任务完成或超时
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("All 1000 tasks have been processed.")
	case <-time.After(60 * time.Second):
		t.Fatal("Timeout waiting for tasks to be processed.")
	}

	// 验证所有任务的状态是否为完成
	tasks, err := rdb.GetActiveTasks(context.Background())
	if err != nil {
		t.Fatalf("Failed to get active tasks: %v", err)
	}

	if len(tasks) != 0 {
		t.Fatalf("Expected 0 active tasks, but got %d", len(tasks))
	}
}

// TestHeartbeatAndProcessorCoordination 测试 Heartbeat 和 Processor 之间的协调
// 模拟 Processor 崩溃后，Heartbeat 重新执行任务
func TestHeartbeatAndProcessorCoordination(t *testing.T) {
	// 初始化 Redis 客户端
	client := redis.NewClient(&redis.Options{
		Addr: "0.0.0.0:6379",
		DB:   0,
	})
	defer client.Close()

	// 清空测试队列
	err := client.FlushDB(context.Background()).Err()
	if err != nil {
		t.Fatalf("Failed to flush Redis DB: %v", err)
	}

	// 初始化 RDB
	rdb := taskQ.NewRDB(client)

	// 初始化 TaskRegistry
	registry := NewTaskRegistry()

	// 使用 WaitGroup 来等待所有任务完成
	var wg sync.WaitGroup
	totalTasks := 10
	wg.Add(totalTasks + 1) // task-5 将被重新执行

	// 注册任务处理器
	registry.Register("test_task", func(ctx context.Context, task *taskQ.TaskMessage) error {
		if task.ID == "task-5" {
			log.Printf("Processor is simulating crash for task ID: %s", task.ID)
		}
		// 模拟任务处理时间
		time.Sleep(10 * time.Millisecond)
		log.Printf("Processed task ID: %s", task.ID)
		wg.Done()
		return nil
	})

	// 初始化 Heartbeat，设置较短的频率和超时以加速测试
	heartbeat := NewHeartbeat(rdb, 500*time.Millisecond, 1*time.Second)

	// 初始化 Processor
	processor := NewProcessor(rdb, registry, heartbeat, 3)

	// 启动 Heartbeat
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	heartbeat.Start(ctx)

	// 启动 Processor
	processor.Start(ctx)

	// Enqueue 10 tasks
	for i := 0; i < totalTasks; i++ {
		taskID := fmt.Sprintf("task-%d", i)
		task := &taskQ.Task{
			Payload: []byte(fmt.Sprintf("payload-%d", i)),
			Handler: "test_task",
		}
		err := rdb.EnqueueTask(context.Background(), task, taskQ.TaskID(taskID))
		if err != nil {
			t.Fatalf("Failed to enqueue task %s: %v", taskID, err)
		}
	}

	// 捕获 Processor 的 panic，确保测试不会因为 panic 而中断
	go func() {
		// 让 Processor 运行一段时间后模拟崩溃
		time.Sleep(50 * time.Millisecond)
		log.Println("Simulating processor crash")
		processor.Stop() // 停止 Processor，模拟崩溃
	}()

	// 重启 Processor，让 Heartbeat 检测到超时任务并重新调度
	go func() {
		time.Sleep(2 * time.Second) // 等待 Heartbeat 检测并恢复任务
		log.Println("Restarting processor")
		processor.Start(ctx)
	}()

	// 等待所有任务完成或超时
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("All tasks have been processed.")
	case <-time.After(10 * time.Second):
		t.Fatal("Timeout waiting for tasks to be processed.")
	}

	// 验证所有任务的状态是否为完成
	for i := 0; i < totalTasks; i++ {
		taskID := fmt.Sprintf("task-%d", i)
		task, err := rdb.GetTaskMessage(context.Background(), "default", taskID)
		if err != nil {
			t.Errorf("Failed to get task %s: %v", taskID, err)
			continue
		}
		if task.Status != taskQ.StatusCompleted {
			t.Errorf("Task %s status is %d, expected %d", taskID, task.Status, taskQ.StatusCompleted)
		}
	}

	// 额外验证 task-5 确实被重新执行
	taskID := "task-5"
	task, err := rdb.GetTaskMessage(context.Background(), "default", taskID)
	if err != nil {
		t.Errorf("Failed to get task %s: %v", taskID, err)
	} else if task.Status != taskQ.StatusCompleted {
		t.Errorf("Task %s status is %d, expected %d", taskID, task.Status, taskQ.StatusCompleted)
	} else if task.RetryCount != 1 {
		t.Errorf("Task %s retry count is %d, expected %d", taskID, task.RetryCount, 1)
	}
}
