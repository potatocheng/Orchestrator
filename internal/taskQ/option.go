package taskQ

import "github.com/google/uuid"

type Option interface {
	Value() any
}

const (
	defaultMaxRetries uint32 = 3
)

type (
	taskidOpt     string
	maxRetriesOpt uint32
	queueOpt      string
	timeoutOpt    int64
)

func (t taskidOpt) Value() any {
	return string(t)
}
func TaskID(id string) Option {
	return taskidOpt(id)
}

func (m maxRetriesOpt) Value() any {
	return uint32(m)
}
func MaxRetries(n uint32) Option {
	return maxRetriesOpt(n)
}

func (q queueOpt) Value() any {
	return string(q)
}
func Queue(q string) Option {
	return queueOpt(q)
}

func (t timeoutOpt) Value() any {
	return int64(t)
}
func Timeout(t int64) Option {
	return timeoutOpt(t)
}

type option struct {
	id         string
	maxRetries uint32
	queue      string
	timeout    int64
}

func composeOpt(opts ...Option) option {
	res := option{
		id:         uuid.NewString(),
		maxRetries: defaultMaxRetries,
		queue:      "default",
		timeout:    0,
	}

	for _, opt := range opts {
		switch opt := opt.(type) {
		case taskidOpt:
			res.id = string(opt)
		case maxRetriesOpt:
			res.maxRetries = uint32(opt)
		case queueOpt:
			res.queue = string(opt)
		case timeoutOpt:
			res.timeout = int64(opt)
		}
	}

	return res
}
