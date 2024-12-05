package service

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/potatocheng/Orchestrator/internal/taskQ"
)

type Processor struct {
	storage   *taskQ.RDB
	registry  *TaskRegistry
	heartbeat *Heartbeat
	workerNum int
	quit      chan struct{}
	wg        sync.WaitGroup
}

func NewProcessor(storage *taskQ.RDB, registry *TaskRegistry, heartbeat *Heartbeat, workerNum int) *Processor {
	return &Processor{
		storage:   storage,
		registry:  registry,
		heartbeat: heartbeat,
		workerNum: workerNum,
		quit:      make(chan struct{}),
	}
}

func (p *Processor) Start(ctx context.Context) {
	for i := 0; i < p.workerNum; i++ {
		p.wg.Add(1)
		go p.worker(ctx, i)
	}
}

func (p *Processor) Stop() {
	close(p.quit)
	p.wg.Wait()
}

func (p *Processor) worker(ctx context.Context, id int) {
	defer p.wg.Done()
	for {
		select {
		case <-p.quit:
			log.Printf("Processor worker %d stopping", id)
			return
		default:
			task, err := p.storage.DequeueTask(ctx, taskQ.DefaultQueue)
			if err != nil {
				log.Printf("Processor worker %d dequeue error: %v", id, err)
				time.Sleep(1 * time.Second)
				continue
			}
			if task == nil {
				time.Sleep(500 * time.Millisecond)
				continue
			}

			if err := p.executeTask(ctx, task); err != nil {
				log.Printf("Processor worker %d execute task %s error: %v", id, task.ID, err)
			}
		}
	}
}

func (p *Processor) executeTask(ctx context.Context, task *taskQ.TaskMessage) error {
	handler, exists := p.registry.GetTaskHandler(task.Handler)
	if !exists {
		return fmt.Errorf("handler for task type %s not found", task.Handler)
	}

	p.heartbeat.NotifyTaskStart(task.ID)

	err := handler(ctx, task)
	if err != nil {
		p.heartbeat.NotifyTaskFinish(task.ID)
		return p.handleTaskFailure(ctx, task, err)
	}

	p.heartbeat.NotifyTaskFinish(task.ID)

	return nil
}

func (p *Processor) handleTaskFailure(ctx context.Context, task *taskQ.TaskMessage, err error) error {
	log.Printf("Task %s failed: %v", task.ID, err)
	task.RetryCount++
	if task.RetryCount > task.MaxRetries {
		if err := p.storage.ChangeTaskQueue(ctx, task.Queue, "Failure"); err != nil {
			return err
		}
		return fmt.Errorf("task %s failed after %d retries", task.ID, task.RetryCount)
	}
	return p.storage.EnqueueTaskWithID(ctx, taskQ.DefaultQueue, task)
}
