package task

import (
	"context"
	"fmt"
	"testing"
	"time"

	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"github.com/stretchr/testify/require"
)

func TestEmbeddingUpgrade_retry(t *testing.T) {
	e := &EmbeddingUpgradeScheduler{
		p: model.EmbeddingUpgradeParams{
			RetryTimes:    3,
			RetryInterval: 100,
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	t.Run("retry max", func(t *testing.T) {
		i := 0
		err := e.retry(ctx, "retry max", time.Second, func(ctx context.Context) error {
			i++
			return fmt.Errorf(fmt.Sprintf("%d", i))
		})
		require.ErrorContains(t, err, "4")
		require.NoError(t, ctx.Err())
	})
	t.Run("retry success", func(t *testing.T) {
		i := 0
		err := e.retry(ctx, "retry success", time.Second, func(ctx context.Context) error {
			i++
			return nil
		})
		require.NoError(t, err)
		require.NoError(t, ctx.Err())
		require.EqualValues(t, 1, i)
	})
}
