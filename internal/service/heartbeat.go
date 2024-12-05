package service

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/potatocheng/Orchestrator/internal/taskQ"
)

type Heartbeat struct {
	storage        *taskQ.RDB
	heartbeatFreq  time.Duration
	leaseTimeout   time.Duration
	quit           chan struct{}
	wg             sync.WaitGroup
	taskStartChan  chan string
	taskFinishChan chan string
	activeTasks    map[string]time.Time
	mu             sync.RWMutex
}

func NewHeartbeat(storage *taskQ.RDB, heartbeatFreq, leaseTimeout time.Duration) *Heartbeat {
	return &Heartbeat{
		storage:        storage,
		heartbeatFreq:  heartbeatFreq,
		leaseTimeout:   leaseTimeout,
		quit:           make(chan struct{}),
		taskStartChan:  make(chan string, 100), // 缓冲通道以防止阻塞
		taskFinishChan: make(chan string, 100),
		activeTasks:    make(map[string]time.Time),
	}
}

func (h *Heartbeat) Start(ctx context.Context) {
	h.wg.Add(4)

	// Goroutine 1: 发送心跳信号
	go func() {
		defer h.wg.Done()
		ticker := time.NewTicker(h.heartbeatFreq)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := h.sendHeartbeat(ctx); err != nil {
					log.Printf("Heartbeat error: %v", err)
				}
			case <-h.quit:
				return
			}
		}
	}()

	// Goroutine 2: 处理任务开始通知
	go func() {
		defer h.wg.Done()
		for {
			select {
			case taskID := <-h.taskStartChan:
				h.mu.Lock()
				h.activeTasks[taskID] = time.Now()
				h.mu.Unlock()
				log.Printf("Heartbeat tracking task start: %s", taskID)
			case <-h.quit:
				return
			}
		}
	}()

	// Goroutine 3: 处理任务完成通知
	go func() {
		defer h.wg.Done()
		for {
			select {
			case taskID := <-h.taskFinishChan:
				h.mu.Lock()
				delete(h.activeTasks, taskID)
				h.mu.Unlock()
				log.Printf("Heartbeat tracking task finish: %s", taskID)
			case <-h.quit:
				return
			}
		}
	}()

	// Goroutine 4: 监控活动任务的超时
	go func() {
		defer h.wg.Done()
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				h.checkActiveTasks()
			case <-h.quit:
				return
			}
		}
	}()
}

func (h *Heartbeat) Stop() {
	close(h.quit)
	h.wg.Wait()
}

func (h *Heartbeat) sendHeartbeat(ctx context.Context) error {
	return h.storage.UpdateHeartbeat(ctx, h.leaseTimeout)
}

// NotifyTaskStart 通知 Heartbeat 任务开始
func (h *Heartbeat) NotifyTaskStart(taskID string) {
	select {
	case h.taskStartChan <- taskID:
		log.Printf("Heartbeat received start notification for task: %s", taskID)
	default:
		log.Printf("Heartbeat taskStartChan is full. Dropping start notification for task: %s", taskID)
	}
}

// NotifyTaskFinish 通知 Heartbeat 任务完成
func (h *Heartbeat) NotifyTaskFinish(taskID string) {
	select {
	case h.taskFinishChan <- taskID:
		log.Printf("Heartbeat received finish notification for task: %s", taskID)
	default:
		log.Printf("Heartbeat taskFinishChan is full. Dropping finish notification for task: %s", taskID)
	}
}

// checkActiveTasks 检查活动任务是否超时
func (h *Heartbeat) checkActiveTasks() {
	h.mu.RLock()
	defer h.mu.RUnlock()

	now := time.Now()
	for taskID, startTime := range h.activeTasks {
		if now.Sub(startTime) > h.leaseTimeout {
			log.Printf("Task %s has timed out. Initiating recovery.", taskID)
			// 这里可以调用 recoverTasks 或其他恢复逻辑
			go func(id string) {
				if err := h.storage.ChangeTaskQueue(context.Background(), "active", "pending"); err != nil {
					log.Printf("Failed to recover task %s: %v", id, err)
				}
			}(taskID)
		}
	}
}
