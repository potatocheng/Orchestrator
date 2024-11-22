package service

// import (
// 	"context"

// 	"github.com/potatocheng/Orchestrator/internal/model"
// 	"github.com/potatocheng/Orchestrator/internal/storage"
// )

// type Scheduler struct {
// 	storage *storage.TaskStorage
// }

// func NewScheduler(storage *storage.TaskStorage) *Scheduler {
// 	return &Scheduler{storage: storage}
// }

// func (s *Scheduler) ScheduleTask(ctx context.Context, task *model.Task) error {
// 	return s.storage.SaveTask(ctx, task)
// }

// func (s *Scheduler) Start(ctx context.Context) error {

// }
