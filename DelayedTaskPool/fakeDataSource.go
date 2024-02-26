package DelayedTaskPool

import (
	"context"
	"time"
)

type fakeDataSource struct {
}

func (f *fakeDataSource) GetWaitTask(ctx context.Context) (tasks []interface{}, err error) {
	tasks = append(tasks, time.Now().Unix())
	return tasks, nil
}

func (f *fakeDataSource) Add(ctx context.Context, val interface{}) error {
	return nil
}

func (f *fakeDataSource) FailHandler(ctx context.Context, val interface{}, err error) error {
	return nil
}

func (f *fakeDataSource) SuccessHandler(ctx context.Context, val interface{}) error {
	return nil
}
