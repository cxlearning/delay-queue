package DelayedTaskPool

import (
	"context"
	"fmt"
	"time"
)

// DelayedTaskPool 延迟任务池
type DelayedTaskPool struct {
	fn         func(ctx context.Context, value interface{}) error
	ticker     time.Duration
	dataSource dataSource
	ch         chan interface{}
	workerNum  int
}

func NewDelayedTaskPool() *DelayedTaskPool {
	return &DelayedTaskPool{
		fn: func(ctx context.Context, value interface{}) error {
			return nil
		},
		ticker:     time.Second,
		dataSource: &fakeDataSource{},
		ch:         make(chan interface{}, 10),
		workerNum:  2,
	}
}

func (p *DelayedTaskPool) Add(ctx context.Context, val interface{}) error {
	return p.dataSource.Add(ctx, val)
}

func (p *DelayedTaskPool) worker(ctx context.Context, workerID int) (err error) {
	defer func() {
		if v := recover(); v != nil {
			err = fmt.Errorf("panic: %v", v)
		}
		fmt.Println(fmt.Sprintf("workerID:%v end", workerID))
	}()
	fmt.Println(fmt.Sprintf("workerID:%v start", workerID))
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case v := <-p.ch:
			fmt.Println("workerID:", workerID, "get task:", v)
			err = p.fn(ctx, v)
			if err != nil {
				_ = p.dataSource.FailHandler(ctx, v, err)
			} else {
				_ = p.dataSource.SuccessHandler(ctx, v)
			}
		}

	}
}

// Run 启动多个工作协程，获取待执行的任务分发给worker
func (p *DelayedTaskPool) Run(ctx context.Context) error {
	defer func() {
		close(p.ch)
	}()

	for i := 0; i < p.workerNum; i++ {
		go p.worker(ctx, i)
	}

	pollTimer := time.NewTimer(p.ticker)
	defer pollTimer.Stop()

	for {
		select {
		case <-pollTimer.C:
			pollTimer.Reset(p.ticker)
			vals, err := p.dataSource.GetWaitTask(ctx)
			if err != nil {
				return err
			}
			for _, v := range vals {
				p.ch <- v
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
