package model

import (
	"fmt"

	pb "github.com/potatocheng/Orchestrator/internal/proto"
	"google.golang.org/protobuf/proto"
)

type Status uint32

const (
	StatusPending Status = iota
	StatusProcessing
	StatusCompleted
	StatusFailed
)

const (
	TaskPrefix   = "task:"
	QueueuPrefix = "queue"
	LockPrefix   = "lock:"
)

type Task struct {
	ID          string
	Type        string
	Payload     []byte
	Queue       string // Queue is the name of the queue where the task is stored
	Status      Status
	Retry       int32 // Retry is the number of times the task can be retried
	Retried     int32 // Retried is the number of times the task has been retried
	Timeout     int64
	Deadline    int64
	CompletedAt int64 // CompletedAt is the time when the task was completed
}

func EncodeMessage(task *Task) ([]byte, error) {
	if task == nil {
		return nil, fmt.Errorf("cannot encode nil task")
	}

	return proto.Marshal(&pb.Task{
		Id:          task.ID,
		Type:        task.Type,
		Payload:     task.Payload,
		Queue:       task.Queue,
		Retry:       task.Retry,
		Retried:     task.Retried,
		CompletedAt: task.CompletedAt,
		Timeout:     task.Timeout,
		Deadline:    task.Deadline,
		Status:      uint32(task.Status),
	})
}

func DecodeMessage(msg []byte) (*Task, error) {
	task := &pb.Task{}
	if err := proto.Unmarshal(msg, task); err != nil {
		return nil, err
	}

	taskmeta := &Task{
		ID:          task.Id,
		Type:        task.Type,
		Payload:     task.Payload,
		Queue:       task.Queue,
		Status:      Status(task.Status),
		Retry:       task.Retry,
		Retried:     task.Retried,
		Timeout:     task.Timeout,
		Deadline:    task.Deadline,
		CompletedAt: task.CompletedAt,
	}
	return taskmeta, nil
}
