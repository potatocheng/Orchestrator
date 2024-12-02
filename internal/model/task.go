package model

import (
	"fmt"
	"time"

	pb "github.com/potatocheng/Orchestrator/internal/proto"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	StatusPending uint32 = iota
	StatusProcessing
	StatusCompleted
	StatusFailed
)

const (
	TaskPrefix  = "task:"
	QueuePrefix = "queue"
	LockPrefix  = "lock:"
)

type Task struct {
	ID          string
	Payload     []byte
	Status      uint32
	RetryCount  uint32
	MaxRetries  uint32
	NextRunTime time.Time
	Timeout     int64
	CreatedAt   time.Time
	UpdatedAt   time.Time
	Handler     string
}

func Time2ProtoTimestamp(t time.Time) *timestamppb.Timestamp {
	return &timestamppb.Timestamp{Seconds: t.Unix(), Nanos: int32(t.Nanosecond())}

}

func EncodeMessage(task *Task) ([]byte, error) {
	if task == nil {
		return nil, fmt.Errorf("cannot encode nil task")
	}

	return proto.Marshal(&pb.Task{
		Id:          task.ID,
		Payload:     task.Payload,
		Status:      uint32(task.Status),
		Retrycount:  task.RetryCount,
		Maxretries:  task.MaxRetries,
		Nextruntime: Time2ProtoTimestamp(task.NextRunTime),
		Timeout:     task.Timeout,
		Createdat:   Time2ProtoTimestamp(task.CreatedAt),
		Updatedat:   Time2ProtoTimestamp(task.UpdatedAt),
		Handler:     task.Handler,
	})
}

func DecodeMessage(msg []byte) (*Task, error) {
	task := &pb.Task{}
	if err := proto.Unmarshal(msg, task); err != nil {
		return nil, err
	}

	taskmeta := &Task{
		ID:          task.Id,
		Payload:     task.Payload,
		Status:      task.Status,
		RetryCount:  task.Retrycount,
		MaxRetries:  task.Maxretries,
		NextRunTime: task.Nextruntime.AsTime(),
		Timeout:     task.Timeout,
		CreatedAt:   task.Createdat.AsTime(),
		UpdatedAt:   task.Updatedat.AsTime(),
		Handler:     task.Handler,
	}
	return taskmeta, nil
}
