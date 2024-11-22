package storage

import (
	"context"
	"fmt"
	"sync"

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

// executeScript executes a Lua script on the Redis server
func (r *RDB) executeLUAScript(ctx context.Context, script *redis.Script, keys []string, args ...any) error {
	return script.Run(ctx, r.client, keys, args...).Err()
}

// executeScriptWithErrorCode executes a Lua script on the Redis server and return execute result
func (r *RDB) executeScriptWithErrorCode(ctx context.Context, script *redis.Script, keys []string, args ...any) (int64, error) {
	res, err := script.Run(ctx, r.client, keys, args...).Result()
	if err != nil {
		return 0, err
	}

	n, ok := res.(int64)
	if !ok {
		return 0, fmt.Errorf("unexpected return value from lua script: %v", res)
	}
	return n, nil
}

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

func (r *RDB) DequeueTask(ctx context.Context, queueName string) (*model.TaskMetadata, error) {

}
