package llm

import (
	"context"

	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/config"
	llmDao "git.woa.com/adp/kb/kb-config/internal/dao/llm"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	"git.woa.com/adp/kb/kb-config/internal/rpc"
)

func NewLogic(rpc *rpc.RPC, llmDao llmDao.Dao) *Logic {
	return &Logic{
		rpc:    rpc,
		llmDao: llmDao,
	}
}

type Logic struct {
	rpc    *rpc.RPC
	llmDao llmDao.Dao
}

func (l *Logic) GetPrompt(ctx context.Context, app *entity.App, modelType string) (string, error) {
	logx.D(ctx, "GetPrompt, app:%+v, modelType:%s", app, modelType)
	if app == nil {
		logx.W(ctx, "GetPrompt, llDao is nil")
		return "", nil
	}
	return l.llmDao.GetPrompt(ctx, app, modelType)
}

func (l *Logic) GetModelPromptLimit(ctx context.Context, modelName string) int {
	defaultLength := config.GetDefaultTokenLimit()
	rsp, err := l.rpc.Resource.GetModelInfo(ctx, 0, modelName)
	if err != nil {
		logx.E(ctx, "GetModelInfo Failed modelName:%s, err:%+v", modelName, err)
		return defaultLength
	}
	if rsp == nil || rsp.Length == 0 {
		return defaultLength
	}
	return int(rsp.Length)
}
