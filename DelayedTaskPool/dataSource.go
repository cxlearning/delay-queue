package DelayedTaskPool

import "context"

type dataSource interface {
	Add(ctx context.Context, val interface{}) error                    // 添加任务
	GetWaitTask(ctx context.Context) (tasks []interface{}, err error)  // 获取待执行的任务
	FailHandler(ctx context.Context, val interface{}, err error) error // 任务执行失败回调
	SuccessHandler(ctx context.Context, val interface{}) error         // 任务执行成功回调
}
