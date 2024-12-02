package service

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/potatocheng/Orchestrator/internal/model"
	"github.com/potatocheng/Orchestrator/internal/storage"
)

type Scheduler struct {
	storage    *storage.RDB
	workerPool int32
	quit       chan struct{}
	wg         sync.WaitGroup
}

func NewScheduler(storage *storage.RDB, workPool int32) *Scheduler {
	return &Scheduler{
		storage:    storage,
		workerPool: workPool,
		quit:       make(chan struct{}),
	}
}

func (s *Scheduler) Start(ctx context.Context) {
	for i := int32(0); i < s.workerPool; i++ {
		s.wg.Add(1)
		go s.worker(ctx, i)
	}
	log.Printf("Scheduler started with %d workers", s.workerPool)
}

func (s *Scheduler) Stop(ctx context.Context) {
	close(s.quit)
	s.wg.Wait()
	log.Println("Scheduler stopped")
}

func (s *Scheduler) worker(ctx context.Context, id int32) {
	defer s.wg.Done()
	for {
		select {
		case <-s.quit:
			log.Printf("Worker %d received stop signal", id)
			return
		default:
			// 从redis中取出任务
			task, err := s.storage.DequeueTask(ctx, model.QueuePrefix)
			if err != nil {
				log.Printf("Worker %d failed to dequeue task: %v", id, err)
				continue
			}
			if task == nil {
				// 如果队列为空，短暂休息
				time.Sleep(500 * time.Millisecond)
			} else {
				if err := s.executeTask(ctx, task); err != nil {
					log.Printf("Worker %d failed to execute task %s: %v", id, task.ID, err)
				}
			}
		}
	}
}

func (s *Scheduler) executeTask(ctx context.Context, task *model.Task) error {
	log.Printf("Executing task %s", task.ID)
	err := s.storage.UpdateTaskStatus(ctx, model.QueuePrefix, task, model.StatusProcessing)
	if err != nil {
		return err
	}

	taskCtx, cancel := context.WithTimeout(ctx, time.Duration(task.Timeout)*time.Second)
	defer cancel()

	handler, exists := TaskRegister.GetTaskHandler(task.Handler)
	if !exists {
		return fmt.Errorf("task handler %s not found", task.Handler)
	}

	err = handler(taskCtx, task)
	if err != nil {
		log.Printf("Task %s failed: %v", task.ID, err)
		task.RetryCount++
		if task.RetryCount > task.MaxRetries {
			return s.storage.UpdateTaskStatus(ctx, model.QueuePrefix, task, model.StatusFailed)
		} else {
			return s.storage.EnqueueTask(ctx, model.QueuePrefix, task)
		}
	}

	return s.storage.UpdateTaskStatus(ctx, model.QueuePrefix, task, model.StatusCompleted)
}

func (s *Scheduler) Run(ctx context.Context) {
	s.Start(ctx)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan
	s.Stop(ctx)
}
