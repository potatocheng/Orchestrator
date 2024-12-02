package storage

import (
	"context"
	"testing"
	"time"

	"github.com/potatocheng/Orchestrator/internal/model"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRDB_EnqueueAndDequeueTask(t *testing.T) {
	rClient := redis.NewClient(&redis.Options{
		Addr: "0.0.0.0:6379",
		DB:   0,
	})

	rdb := NewRDB(rClient)
	assert.NotNil(t, rdb)

	err := rdb.Ping()
	require.NoError(t, err)

	testCases := []struct {
		name  string
		task  *model.Task
		after func()
	}{
		{
			name: "task enqueue successful",
			task: &model.Task{
				ID:          "1",
				Payload:     []byte("test"),
				Status:      model.StatusPending,
				RetryCount:  0,
				MaxRetries:  3,
				NextRunTime: time.Now(),
				Timeout:     10,
				CreatedAt:   time.Now(),
				UpdatedAt:   time.Now(),
				Handler:     "test",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 任务入队
			err := rdb.EnqueueTask(context.Background(), model.QueuePrefix, tc.task)
			require.NoError(t, err)

			// 任务出队
			tsk, err := rdb.DequeueTask(context.Background(), model.QueuePrefix)
			require.NoError(t, err)

			// 判断出入对是否同一个任务
			assert.Equal(t, tsk, tc.task)
		})
	}
}
