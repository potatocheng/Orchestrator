package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/potatocheng/Orchestrator/internal/model"
	"github.com/redis/go-redis/v9"
)

type RDB struct {
	client *redis.Client
}

func NewRDB(client *redis.Client) *RDB {
	return &RDB{
		client: client,
	}
}

func (r *RDB) Close() error {
	return r.client.Close()
}

// Ping 检查 Redis 服务器的连接
func (r *RDB) Ping() error {
	return r.client.Ping(context.Background()).Err()
}

// EnqueueTask 将任务添加到队列
func (r *RDB) EnqueueTask(ctx context.Context, queueName string, task *model.Task) error {
	taskKey := model.TaskPrefix + task.ID
	queueKey := model.QueuePrefix + queueName

	// 序列化任务
	taskEncodedData, err := model.EncodeMessage(task)
	if err != nil {
		return fmt.Errorf("failed to marshal task: %w", err)
	}

	pipe := r.client.Pipeline()
	// 存储任务数据
	pipe.HSet(ctx, taskKey, map[string]interface{}{
		"data":   taskEncodedData,
		"status": uint32(task.Status),
	})

	// 按 NextRunTime 将任务添加到排序集合
	pipe.ZAdd(ctx, queueKey, redis.Z{
		Score:  float64(task.Timeout),
		Member: task.ID,
	})

	_, err = pipe.Exec(ctx)
	return err
}

// DequeueTask 从队列中获取下一个任务
func (r *RDB) DequeueTask(ctx context.Context, queueName string) (*model.Task, error) {
	queueKey := model.QueuePrefix + queueName

	var resultTask *model.Task
	for {
		// 使用 WATCH 监视队列键
		err := r.client.Watch(ctx, func(tx *redis.Tx) error {
			// 获取可执行的任务
			now := float64(time.Now().Unix())
			taskIDs, err := tx.ZRangeByScore(ctx, queueKey, &redis.ZRangeBy{
				Min:    "0",
				Max:    fmt.Sprintf("%f", now),
				Offset: 0,
				Count:  1,
			}).Result()
			if err != nil {
				return err
			}
			if len(taskIDs) == 0 {
				return redis.Nil // 没有可执行任务
			}

			taskID := taskIDs[0]
			taskKey := model.TaskPrefix + taskID
			lockKey := model.LockPrefix + taskID

			// 尝试获取分布式锁
			lock := NewDistributedLock(r.client, lockKey, 30*time.Second)
			locked, err := lock.TryLock(ctx)
			if err != nil || !locked {
				// 未能获取锁，可能被其他消费者处理
				return nil
			}
			defer lock.Unlock(ctx)

			// 获取任务数据
			taskData, err := tx.HGet(ctx, taskKey, "data").Bytes()
			if err != nil {
				return err
			}

			task, err := model.DecodeMessage(taskData)
			if err != nil {
				return err
			}

			// 更新任务状态为处理中，并从队列中移除
			pipe := tx.TxPipeline()
			pipe.HSet(ctx, taskKey, "status", model.StatusProcessing)
			pipe.ZRem(ctx, queueKey, taskID)
			_, err = pipe.Exec(ctx)
			if err != nil {
				return err
			}

			// 将任务返回给外层变量
			resultTask = task
			return nil
		}, queueKey)

		if err == redis.TxFailedErr {
			// 事务失败，重试
			continue
		} else if err == redis.Nil {
			// 没有可执行任务
			return nil, nil
		} else if err != nil {
			return nil, err
		}

		// 成功获取任务
		return resultTask, nil
	}
}

// UpdateTaskStatus 更新任务状态并处理重试逻辑
func (r *RDB) UpdateTaskStatus(ctx context.Context, queueName string, task *model.Task, status uint32) error {
	taskKey := model.TaskPrefix + task.ID

	task.Status = status
	task.UpdatedAt = time.Now()

	if status == model.StatusFailed && task.RetryCount < task.MaxRetries {
		task.RetryCount++
		task.Status = model.StatusPending
		task.NextRunTime = time.Now().Add(time.Duration(task.RetryCount) * time.Minute)
		return r.EnqueueTask(ctx, queueName, task)
	}

	taskData, err := json.Marshal(task)
	if err != nil {
		return err
	}

	pipe := r.client.Pipeline()
	pipe.HSet(ctx, taskKey, "data", taskData)
	pipe.HSet(ctx, taskKey, "status", status)
	_, err = pipe.Exec(ctx)

	return err
}
