package storage

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type DistributedLock struct {
	client       *redis.Client
	lockScript   *redis.Script
	unlockScript *redis.Script
	lockKey      string
	lockValue    string        // lock value是一个随机生成的字符串，用于区分不同节点加的锁，防止误解除
	ttl          time.Duration // 锁的时间，防止服务崩溃后，锁一直得不到释放的情况
}

func NewDistributedLock(client *redis.Client, lockKey string, ttl time.Duration) *DistributedLock {
	return &DistributedLock{
		client:    client,
		lockKey:   lockKey,
		ttl:       ttl,
		lockValue: uuid.New().String(),
		lockScript: redis.NewScript(`
		if redis.call("setnx", KEYS[1], ARGV[1]) == 1 then
			redis.call("expire", KEYS[1], ARGV[2])
			return 1
		end
		return 0`),
		unlockScript: redis.NewScript(`
		if redis.call("get", KEYS[1]) == ARGV[1] then
			return redis.call("del", KEYS[1])
		else
			return 0
		end`),
	}
}

// TryLock 尝试获取锁
func (d *DistributedLock) TryLock(ctx context.Context) (bool, error) {
	result, err := d.lockScript.Run(ctx, d.client, []string{d.lockKey}, d.lockValue, d.ttl.Milliseconds()).Result()
	if err != nil {
		return false, err
	}
	return result.(int64) == 1, nil
}

func (d *DistributedLock) Unlock(ctx context.Context) (bool, error) {
	result, err := d.unlockScript.Run(ctx, d.client, []string{d.lockKey}, d.lockValue).Result()
	if err != nil {
		return false, err
	}

	return result.(int64) == 1, nil
}
