package llm

import (
	"context"

	"git.woa.com/adp/common/llm/prompt"
	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/dao/types"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
)

type Dao interface {
	GetPrompt(ctx context.Context, app *entity.App, modelType string) (string, error)
}

func NewDao(mysql types.MySQLDB) Dao {
	promptProcessor, err := prompt.NewBasicPromptProcessor(mysql)
	if err != nil {
		logx.Errorf("init promptCli err:%v", err)
		panic(err)
	}
	return &daoImpl{
		promptProcessor: promptProcessor,
	}
}

type daoImpl struct {
	promptProcessor prompt.PromptProcessor
}

// GetPrompt 获取Prompt
func (l *daoImpl) GetPrompt(ctx context.Context, app *entity.App, modelType string) (string, error) {
	if app == nil {
		return "", errs.ErrAppNotFound
	}
	modelInfo, ok := app.QaConfig.Model[modelType]
	if !ok {
		logx.E(ctx, "GetPrompt, modelType:%s not found", modelType)
		return "", errs.ErrSystem
	}
	language := contextx.Metadata(ctx).CAPILanguage()
	if language == "" {
		language = "zh-CN"
	}
	promptStr, err := l.promptProcessor.Get(ctx, modelType, modelInfo.ModelName, language, modelInfo.PromptVersion)
	if err != nil {
		logx.E(ctx, "GetPrompt, modelType:%s, modelName:%s, language:%s, version:%s, err:%v",
			modelType, modelInfo.ModelName, language, modelInfo.PromptVersion, err)
		return "", errs.ErrSystem
	}
	return promptStr, nil
}
