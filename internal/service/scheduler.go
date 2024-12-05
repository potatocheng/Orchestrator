// internal/service/scheduler.go

package service

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/potatocheng/Orchestrator/internal/taskQ"
)

// Scheduler 调度器负责启动和管理 Heartbeat 和 Processor
type Scheduler struct {
	storage       *taskQ.RDB
	registry      *TaskRegistry
	heartbeat     *Heartbeat
	processor     *Processor
	workerNum     int
	heartbeatFreq time.Duration
	leaseTimeout  time.Duration
	quit          chan struct{}
	wg            sync.WaitGroup
}

// NewScheduler 创建一个新的 Scheduler 实例
func NewScheduler(storage *taskQ.RDB, registry *TaskRegistry, workerNum int, heartbeatFreq, leaseTimeout time.Duration) *Scheduler {
	return &Scheduler{
		storage:       storage,
		registry:      registry,
		workerNum:     workerNum,
		heartbeatFreq: heartbeatFreq,
		leaseTimeout:  leaseTimeout,
		quit:          make(chan struct{}),
	}
}

// Start 启动 Scheduler，包含 Heartbeat 和 Processor
func (s *Scheduler) Start(ctx context.Context) {
	// 初始化并启动 Heartbeat
	s.heartbeat = NewHeartbeat(s.storage, s.heartbeatFreq, s.leaseTimeout)
	s.heartbeat.Start(ctx)

	// 初始化并启动 Processor
	s.processor = NewProcessor(s.storage, s.registry, s.heartbeat, s.workerNum)
	s.processor.Start(ctx)

	// 启动 Heartbeat 监控
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.monitorHeartbeat(ctx)
	}()
}

// Stop 停止 Scheduler，包含 Heartbeat 和 Processor
func (s *Scheduler) Stop() {
	close(s.quit)
	s.heartbeat.Stop()
	s.processor.Stop()
	s.wg.Wait()
}

// Run 启动 Scheduler 并处理系统信号以优雅停止
func (s *Scheduler) Run(ctx context.Context) {
	s.Start(ctx)

	// 处理系统信号以优雅停止
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan
	log.Println("Received shutdown signal. Stopping Scheduler...")
	s.Stop()
	log.Println("Scheduler stopped gracefully.")
}

// monitorHeartbeat 定期检查 Heartbeat 状态并恢复任务
func (s *Scheduler) monitorHeartbeat(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if !s.storage.CheckHeartbeat(ctx) {
				log.Println("Processor heartbeat not detected. Recovering tasks...")
				if err := s.recoverTasks(ctx); err != nil {
					log.Printf("Error recovering tasks: %v", err)
				}
			}
		case <-s.quit:
			return
		}
	}
}

// recoverTasks 获取所有 active 状态的任务并重新入队
func (s *Scheduler) recoverTasks(ctx context.Context) error {
	tasks, err := s.storage.GetActiveTasks(ctx)
	if err != nil {
		return err
	}

	for _, task := range tasks {
		log.Printf("Re-enqueueing task %s due to missing heartbeat", task.ID)
		task.Status = taskQ.StatusPending
		// 使用原始任务 ID 重新入队
		if err := s.storage.EnqueueTaskWithID(ctx, taskQ.DefaultQueue, task); err != nil {
			log.Printf("Failed to re-enqueue task %s: %v", task.ID, err)
		}
	}

	return nil
}
