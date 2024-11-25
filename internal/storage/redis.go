package storage

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/potatocheng/Orchestrator/internal/model"
	"github.com/redis/go-redis/v9"
)

type RDB struct {
	client          redis.UniversalClient
	queuesPublished sync.Map
}

func NewRDB(client redis.UniversalClient) *RDB {
	return &RDB{
		client: client,
	}
}

func (r *RDB) Close() error {
	return r.client.Close()
}

// Ping checks the connection to the Redis server
func (r *RDB) Ping() error {
	return r.client.Ping(context.Background()).Err()
}

// AcquireLock tries to acquire a distributed lock
func (r *RDB) acquireLock(ctx context.Context, key string, val string, ttl time.Duration) (bool, error) {
	lockKey := model.LockPrefix + key
	return r.client.SetNX(ctx, lockKey, val, ttl).Result()
}

func (r *RDB) releaseLock(ctx context.Context, key string) error {
	lockKey := model.LockPrefix + key
	return r.client.Del(ctx, lockKey).Err()
}

// executeScript executes a Lua script on the Redis server
func (r *RDB) executeLUAScript(ctx context.Context, script *redis.Script, keys []string, args ...any) error {
	return script.Run(ctx, r.client, keys, args...).Err()
}

// executeScriptWithErrorCode executes a Lua script on the Redis server and return execute result
func (r *RDB) executeScriptWithErrorCode(ctx context.Context, script *redis.Script, keys []string, args ...any) (any, error) {
	return script.Run(ctx, r.client, keys, args...).Result()
}

// EnqueueTask adds a task to the queue
func (r *RDB) EnqueueTask(ctx context.Context, taskMeta *model.TaskMetadata) error {
	taskKey := model.TaskPrefix + taskMeta.ID
	queueKey := model.QueueuPrefix + taskMeta.Queue

	encode, err := model.EncodeMessage(taskMeta)
	if err != nil {
		return err
	}

	pipe := r.client.Pipeline()
	// Store task data
	pipe.HSet(ctx, taskKey, "data", encode)

	// Add to sorted set by NextRunTime
	pipe.ZAdd(ctx, queueKey, redis.Z{
		Member: taskMeta.ID,
		Score:  float64(taskMeta.Deadline),
	})

	_, err = pipe.Exec(ctx)
	return err
}

// DequeueTask retrieves a task from the queue
func (r *RDB) DequeueTask(ctx context.Context, queueName string) (*model.TaskMetadata, error) {
	queueKey := model.QueueuPrefix + queueName
	luaScript, err := os.ReadFile("../script/dequeue.lua")
	if err != nil {
		return nil, err
	}
	redisScript := redis.NewScript(string(luaScript))
	result, err := r.executeScriptWithErrorCode(ctx, redisScript, []string{queueKey}, model.TaskPrefix, time.Now().UnixMilli())
	if err != nil {
		return nil, err
	}
	taskEncoded, ok := result.(string)
	if !ok {
		return nil, fmt.Errorf("获取数据不符合预期")
	}

	return model.DecodeMessage([]byte(taskEncoded))
}

func (r *RDB) UpdateTaskStatus(ctx context.Context, task *model.TaskMetadata, status model.Status) error {
	if status == model.StatusFailed && task.Retried < task.Retry {
		task.Retried++
		task.Status = model.StatusPending
		task.Deadline = time.Now().Add(time.Duration(task.Retried) * time.Second).Unix()
		return r.EnqueueTask(ctx, task)
	}

	task.Status = status
	return r.EnqueueTask(ctx, task)
}
