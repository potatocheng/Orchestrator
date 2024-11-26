package service

import (
	"context"
	"log"
	"time"

	"github.com/potatocheng/Orchestrator/internal/model"
	"github.com/potatocheng/Orchestrator/internal/storage"
)

type Scheduler struct {
	storage    *storage.RDB
	workerPool int
	quit       chan struct{}
}

func NewScheduler(rdb *storage.RDB, workerPool int) *Scheduler {
	return &Scheduler{
		storage:    rdb,
		workerPool: workerPool,
		quit:       make(chan struct{}),
	}
}

func (s *Scheduler) Start(ctx context.Context) {
	for i := 0; i < s.workerPool; i++ {
		go s.worker(ctx, i)
	}
}

func (s *Scheduler) Stop() {
	close(s.quit)
}

func (s *Scheduler) worker(ctx context.Context, id int) {
	for {
		select {
		case <-s.quit:
			return
		default:
			task, err := s.storage.DequeueTask(ctx, "default_queue")
			if err != nil {
				log.Printf("Worker %d, failed to dequeue task: %v", id, err)
				time.Sleep(1 * time.Second)
				continue
			}
			if task != nil {
				s.executeTask(ctx, task)
			}
		}
	}
}

func (s *Scheduler) executeTask(ctx context.Context, task *model.Task) {
	log.Printf("Executing task: %s", task.ID)
	// 更新任务状态为处理中
	err := s.storage.UpdateTaskStatus(ctx, task, model.StatusProcessing)
	if err != nil {
		log.Printf("Failed to update task status to processing: %v", err)
		return
	}

	// 任务执行

	// 根据执行结果更新任务状态
	err = s.storage.UpdateTaskStatus(ctx, task, model.StatusCompleted)
	if err != nil {
		log.Printf("Failed to update task status to completed: %v", err)
	}
}
