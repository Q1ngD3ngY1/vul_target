package dao

import (
	"context"
	"fmt"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"github.com/jmoiron/sqlx"
)

const (
	corpCustomModelFields = `
		id,corp_id,model_type,model_name,app_type,alias,prompt,path,target,history_words_limit,
		history_limit,service_name,prompt_words_limit,note,expired_time,create_time,update_time
	`
	createCustomModel = `
		INSERT INTO
		    t_corp_custom_model (%s)
		VALUES 
		    (null,:corp_id,:model_type,:model_name,:app_type,:alias,:prompt,:path,:target,:history_words_limit,
			:history_limit,:service_name,:prompt_words_limit,:note,:expired_time,:create_time,:update_time)
	`
	getCustomModelList = `
		SELECT 
			%s 
		FROM
			t_corp_custom_model
		WHERE 
			 corp_id = ? %s
	`
	getCustomModelByModelName = `
		SELECT 
			%s 
		FROM
			t_corp_custom_model
		WHERE 
			 corp_id = ? AND model_name = ?
	`
)

// CreateCustomModel 创建自定义模型
func (d *dao) CreateCustomModel(ctx context.Context, corpID uint64, customModel *model.CorpCustomModel) error {
	if err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		now := time.Now()
		customModel.CreateTime = now
		customModel.UpdateTime = now
		customModel.CorpID = corpID
		querySQL := fmt.Sprintf(createCustomModel, corpCustomModelFields)
		if _, err := tx.NamedExecContext(ctx, querySQL, customModel); err != nil {
			log.ErrorContextf(ctx, "创建自定义模型失败 sql:%s args:%+v err:%+v", querySQL, customModel, err)
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

// GetCustomModelList 获取企业自定义模型信息列表
func (d *dao) GetCustomModelList(ctx context.Context, corpID uint64, appType string) ([]*model.CorpCustomModel, error) {
	var models []*model.CorpCustomModel
	var args []any
	args = append(args, corpID)
	var condition string
	if len(appType) > 0 {
		condition += " AND app_type = ?"
		args = append(args, appType)
	}
	querySQL := fmt.Sprintf(getCustomModelList, corpCustomModelFields, condition)
	// 查询
	if err := d.db.QueryToStructs(ctx, &models, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取自定义模型列表失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	if len(models) == 0 {
		return nil, nil
	}
	return models, nil
}

// GetCustomModelByModelName 通过模型别名获取企业自定义模型
func (d *dao) GetCustomModelByModelName(ctx context.Context, corpID uint64, modelName string) (
	*model.CorpCustomModel, error) {
	args := make([]any, 0, 1)
	args = append(args, corpID, modelName)
	querySQL := fmt.Sprintf(getCustomModelByModelName, corpCustomModelFields)
	models := make([]*model.CorpCustomModel, 0)
	if err := d.db.QueryToStructs(ctx, &models, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "通过别名获取企业自定义模型 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	if len(models) == 0 {
		return nil, nil
	}
	return models[0], nil
}
