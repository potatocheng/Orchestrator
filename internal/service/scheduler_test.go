package service

import (
	"context"
	"errors"
	"log"
	"testing"
	"time"

	"github.com/potatocheng/Orchestrator/internal/model"
	"github.com/potatocheng/Orchestrator/internal/storage"
	"github.com/redis/go-redis/v9"
)

func SendMessage(ctx context.Context, task *model.Task) error {
	if task.Payload == nil {
		return errors.New("payload is nil")
	}

	emailContent := string(task.Payload)
	log.Printf("Sending email: %s", emailContent)

	return nil
}

func TestScheduler_RegisterTaskHandler(t *testing.T) {
	rClient := redis.NewClient(&redis.Options{
		Addr: "0.0.0.0:6379",
		DB:   0,
	})
	rdb := storage.NewRDB(rClient)
	scheduler := NewScheduler(rdb, 10)
	TaskRegister.Register("sendemail", SendMessage)

	rdb.EnqueueTask(context.Background(), model.QueuePrefix, &model.Task{
		ID:          "1",
		Payload:     []byte("hello"),
		Status:      model.StatusPending,
		RetryCount:  0,
		MaxRetries:  3,
		NextRunTime: time.Now().Add(5 * time.Second),
		Timeout:     5,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
		Handler:     "sendemail",
	})
	testCases := []struct {
		name string
	}{
		{
			name: "Scheduler",
		},
	}

	scheduler.Run(context.Background())
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
		})
	}
}
