package model

import (
	"fmt"

	pb "github.com/potatocheng/Orchestrator/internal/proto"
	"google.golang.org/protobuf/proto"
)

const (
	TaskPrefix   = "task:"
	QueueuPrefix = "queue"
	LockPrefix   = "lock:"

	StatusPending    = "pending"
	StatusProcessing = "processing"
	StatusCompleted  = "completed"
	StatusFailed     = "failed"
)

type TaskMetadata struct {
	ID          string
	Type        string
	Payload     []byte
	Queue       string // Queue is the name of the queue where the task is stored
	Retry       int32
	Retried     int32
	Timeout     int64
	Deadline    int64
	CompletedAt int64 // CompletedAt is the time when the task was completed
}

func EncodeMessage(task *TaskMetadata) ([]byte, error) {
	if task == nil {
		return nil, fmt.Errorf("cannot encode nil task")
	}

	return proto.Marshal(&pb.TaskMetadata{
		Id:          task.ID,
		Type:        task.Type,
		Payload:     task.Payload,
		Queue:       task.Queue,
		Retry:       task.Retry,
		Retried:     task.Retried,
		CompletedAt: task.CompletedAt,
		Timeout:     task.Timeout,
		Deadline:    task.Deadline,
	})
}
