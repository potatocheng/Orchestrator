package service

import (
	"context"
	"fmt"
	"sync"

	"github.com/potatocheng/Orchestrator/internal/model"
)

// TaskRegister 全局任务handler注册器
var TaskRegister *TaskRegistry = NewTaskRegistry()

type TaskHandlerFunc func(ctx context.Context, task *model.Task) error

type TaskRegistry struct {
	taskHandlers map[string]TaskHandlerFunc
	mu           sync.RWMutex
}

func NewTaskRegistry() *TaskRegistry {
	return &TaskRegistry{
		taskHandlers: make(map[string]TaskHandlerFunc, 0),
	}
}

// Register 注册任务handler, 由于注册在服务器启动阶段，所以出错直接panic
func (r *TaskRegistry) Register(taskType string, f TaskHandlerFunc) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.taskHandlers[taskType]; exists {
		panic(fmt.Sprintf("task: %s handler already exists", taskType))
	}
	r.taskHandlers[taskType] = f
}

func (r *TaskRegistry) GetTaskHandler(taskType string) (TaskHandlerFunc, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	f, exists := r.taskHandlers[taskType]
	return f, exists
}
