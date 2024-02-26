package DelayedTaskPool

import (
	"context"
	"testing"
)

func TestDelayedTaskPool_Run(t *testing.T) {
	NewDelayedTaskPool().Run(context.Background())
}
