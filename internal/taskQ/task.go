package taskQ

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

func QueuePrefix(queue string) string {
	return fmt.Sprintf("{%s}:", queue)
}

func TaskPrefix(queue string) string {
	return QueuePrefix(queue) + "task:"
}

func TaskKey(taskId, queue string) string {
	return TaskPrefix(queue) + taskId
}

func PendingKey(queue string) string {
	return QueuePrefix(queue) + "pending"
}

func ActiveKey(queue string) string {
	return QueuePrefix(queue) + "active"
}

func LeaseKey(queue string) string {
	return QueuePrefix(queue) + "lease"
}

const (
	DefaultQueue = "default"
)

type Task struct {
	Payload []byte
	Opts    []Option
	Handler string
}

type TaskMessage struct {
	ID         string
	Payload    []byte
	Status     uint32
	Queue      string
	Handler    string
	RetryCount uint32
	MaxRetries uint32
	Timeout    int64
}

func Time2ProtoTimestamp(t time.Time) *timestamppb.Timestamp {
	return &timestamppb.Timestamp{Seconds: t.Unix(), Nanos: int32(t.Nanosecond())}

}

func EncodeMessage(task *TaskMessage) ([]byte, error) {
	if task == nil {
		return nil, fmt.Errorf("cannot encode nil task")
	}

	return proto.Marshal(&pb.Task{
		Id:         task.ID,
		Payload:    task.Payload,
		Status:     uint32(task.Status),
		Retrycount: task.RetryCount,
		Maxretries: task.MaxRetries,
		Timeout:    task.Timeout,
		Handler:    task.Handler,
	})
}

func DecodeMessage(msg []byte) (*TaskMessage, error) {
	task := &pb.Task{}
	if err := proto.Unmarshal(msg, task); err != nil {
		return nil, err
	}

	taskmeta := &TaskMessage{
		ID:         task.Id,
		Payload:    task.Payload,
		Status:     task.Status,
		RetryCount: task.Retrycount,
		MaxRetries: task.Maxretries,
		Timeout:    task.Timeout,
		Handler:    task.Handler,
	}
	return taskmeta, nil
}

func genTaskMessage(task *Task, opt option) *TaskMessage {
	return &TaskMessage{
		ID:         opt.id,
		Payload:    task.Payload,
		Status:     StatusPending,
		Handler:    task.Handler,
		RetryCount: 0,
		MaxRetries: opt.maxRetries,
		Timeout:    opt.timeout,
		Queue:      opt.queue,
	}
}
