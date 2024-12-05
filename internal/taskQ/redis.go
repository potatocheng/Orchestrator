package taskQ

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

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

func (r *RDB) runScriptWithReturnValue(ctx context.Context, script *redis.Script, keys []string, args ...interface{}) (interface{}, error) {
	return script.Run(ctx, r.client, keys, args...).Result()
}

/*
Input:
KEYS[1] - {<queueName>}:task:<task_id>
KEYS[2] - {<queueName>}:pending

	--

ARGV[1] - task message
ARGV[2] - task id

Output:
0 - task already exists
1 - task added to queue
*/
var enqueueCmd = redis.NewScript(`
if redis.call("EXISTS", KEYS[1]) > 1 then
	return 0
end
redis.call("HSET", KEYS[1], "msg", ARGV[1], "status", "pending")
redis.call("LPUSH", KEYS[2], ARGV[2])
return 1
`)

// EnqueueTask 将任务添加到队列
func (r *RDB) EnqueueTask(ctx context.Context, task *Task, opts ...Option) error {
	opt := composeOpt(opts...)
	taskMsg := genTaskMessage(task, opt)

	taskEncodedData, err := EncodeMessage(taskMsg)
	if err != nil {
		return fmt.Errorf("failed to marshal task: %w", err)
	}

	KEYS := []string{TaskKey(taskMsg.ID, taskMsg.Queue), PendingKey(taskMsg.Queue)}
	res, err := r.runScriptWithReturnValue(ctx, enqueueCmd, KEYS, taskEncodedData, taskMsg.ID)
	if err != nil {
		return err
	}
	if res == int64(0) {
		return fmt.Errorf("task[%s] already exists", taskMsg.ID)
	}

	return nil
}

/*
Input:
KEYS[1] - {<queueName>}:pending
KEYS[2] - {<queueName>}:task:
KEYS[3] - {<queueName>}:active
KEYS[4] - {<queueName>}:lease

ARGV[1] - lease

Output:
nil - no task available
not nil - task message
*/
var dequeueCmd = redis.NewScript(`
	local taskId = redis.call("RPOPLPUSH", KEYS[1], KEYS[3])
	if taskId then
		local taskKey = KEYS[2] .. taskId
		redis.call("HSET", taskKey, "status", "active")
		redis.call("ZADD", KEYS[4], ARGV[1], taskId)
		return redis.call("HGET", taskKey, "msg")
	end
	return nil
`)

// DequeueTask 从队列中获取下一个任务
func (r *RDB) DequeueTask(ctx context.Context, queueName string) (*TaskMessage, error) {
	lease := time.Now().Add(time.Second * 30).Unix()
	KEYS := []string{PendingKey(queueName), TaskPrefix(queueName), ActiveKey(queueName), LeaseKey(queueName)}
	res, err := r.runScriptWithReturnValue(ctx, dequeueCmd, KEYS, lease)
	if err != nil {
		return nil, err
	}
	msg, ok := res.(string)
	if !ok || res == nil {
		return nil, errors.New("task message is nil")
	}

	taskMsg := &TaskMessage{}
	taskMsg, err = DecodeMessage([]byte(msg))
	if err != nil {
		return nil, err
	}

	return taskMsg, nil
}

func (r *RDB) UpdateHeartbeat(ctx context.Context, leaseTimeout time.Duration) error {
	heartbeatKey := "processor:heartbeat"
	return r.client.Set(ctx, heartbeatKey, time.Now().Unix()+int64(leaseTimeout), time.Second*30).Err()
}

/*
Input:
KEYS[1] - {<queueName>}:current queue
KEYS[2] - {<queueName>}:target queue
KEYS[3] - {<queueName>}:task:

ARGV[1] - task target status

Output:
> 0 - task status updated
= 0 - failed to update task status
*/
var changeQueueCmd = redis.NewScript(`
	local taskId = redis.call("RPOPLPUSH", KEYS[1], KEYS[2])
	if taskId then
		local taskKey = KEYS[3] .. taskId
		return redis.call("HSET", taskKey, "status", ARGV[1])
	end
	return 0
`)

// ChangeTaskQueue 将任务更新到指定队列
func (r *RDB) ChangeTaskQueue(ctx context.Context, currentQueue, targetQueue string) error {
	KEYS := []string{
		QueuePrefix(currentQueue),
		QueuePrefix(targetQueue),
		TaskPrefix(currentQueue),
	}

	res, err := changeQueueCmd.Run(ctx, r.client, KEYS, targetQueue).Result()
	if err != nil {
		return err
	}
	if res.(int64) == 0 {
		return errors.New("failed to update task status")
	}

	return nil
}

func (r *RDB) CheckHeartbeat(ctx context.Context) bool {
	heartbeatKey := "processor:heartbeat"
	ts, err := r.client.Get(ctx, heartbeatKey).Int64()
	if err != nil {
		log.Printf("Error checking heartbeat: %v", err)
		return false
	}

	// 40s内有心跳则认为正常
	return time.Now().Unix()-ts < 60
}

func (r *RDB) GetActiveTasks(ctx context.Context) ([]*TaskMessage, error) {
	activeKey := ActiveKey(DefaultQueue)
	taskIDs, err := r.client.LRange(ctx, activeKey, 0, -1).Result()
	if err != nil {
		return nil, err
	}

	tasks := make([]*TaskMessage, 0, len(taskIDs))
	for _, taskID := range taskIDs {
		taskKey := TaskKey(taskID, DefaultQueue)
		taskData, err := r.client.HGet(ctx, taskKey, "msg").Bytes()
		if err != nil {
			return nil, err
		}

		taskMsg, err := DecodeMessage(taskData)
		if err != nil {
			return nil, err
		}

		tasks = append(tasks, taskMsg)
	}

	return tasks, nil
}

// EnqueueTaskWithID 将任务添加到队列，并使用指定的任务 ID
func (r *RDB) EnqueueTaskWithID(ctx context.Context, queueName string, task *TaskMessage) error {
	taskID := task.ID
	if taskID == "" {
		return fmt.Errorf("task ID cannot be empty")
	}

	taskKey := TaskPrefix(queueName) + taskID
	pendingKey := PendingKey(queueName)

	taskData, err := json.Marshal(task)
	if err != nil {
		return err
	}

	// 使用 Lua 脚本确保原子操作
	var enqueueScript = redis.NewScript(`
        -- 设置任务数据
        redis.call("HSET", KEYS[1], "msg", ARGV[1])
        -- 将任务 ID 推入 pending 队列
        redis.call("LPUSH", KEYS[2], ARGV[2])
        return 1
    `)

	_, err = enqueueScript.Run(ctx, r.client, []string{taskKey, pendingKey}, taskData, taskID).Result()
	if err != nil {
		return err
	}

	return nil
}

// GetTaskMessage retrieves the detailed information of a specific task from Redis.
func (r *RDB) GetTaskMessage(ctx context.Context, queueName, taskID string) (*TaskMessage, error) {
	taskKey := TaskPrefix(queueName) + taskID

	// Retrieve the JSON-encoded task message from the task hash
	msg, err := r.client.HGet(ctx, taskKey, "msg").Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil, fmt.Errorf("task %s not found in queue %s", taskID, queueName)
		}
		return nil, fmt.Errorf("failed to get task message for task %s: %w", taskID, err)
	}

	// Decode the JSON message into a TaskMessage struct
	taskMsg, err := DecodeMessage(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to decode task message for task %s: %w", taskID, err)
	}

	return taskMsg, nil
}
