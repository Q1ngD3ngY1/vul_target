package dao

import (
	"context"
	"os"
	"testing"

	"git.code.oa.com/trpc-go/trpc-database/mysql"
	"git.code.oa.com/trpc-go/trpc-go/client"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"github.com/stretchr/testify/assert"
)

func Test_dao_GetGlobalKnowledges(t *testing.T) {
	d := &dao{
		db: mysql.NewClientProxy("test", client.WithTarget(os.Getenv("DB_LLM_ROBOT_ADDR"))),
	}
	_, err := d.GetGlobalKnowledges(context.Background(), []model.GlobalKnowledgeID{1, 2})
	assert.Nil(t, err)
}
