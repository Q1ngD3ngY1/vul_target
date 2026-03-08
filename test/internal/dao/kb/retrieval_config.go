package kb

import (
	"context"
	"fmt"

	"git.woa.com/adp/common/x/encodingx/jsonx"
	"git.woa.com/adp/common/x/errx"
	"git.woa.com/adp/common/x/gox/convx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/model"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/mysqlquery"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	"github.com/redis/go-redis/v9"
	"gorm.io/gen"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// DescribeRetrievalConfig 通过RobotID查询检索配置
func (d *daoImpl) DescribeRetrievalConfig(ctx context.Context, appPrimaryId uint64) (*entity.RetrievalConfig, error) {
	conds := []gen.Condition{
		d.mysql.TRetrievalConfig.RobotID.Eq(appPrimaryId),
	}
	val, err := d.mysql.TRetrievalConfig.WithContext(ctx).Where(conds...).First()
	if err != nil {
		if errx.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		logx.E(ctx, "获取应用检索配置失败 appPrimaryId:%d err:%+v", appPrimaryId, err)
		return nil, err
	}
	return retrievalConfigPO2DO(val), nil
}

func (d *daoImpl) ModifyRetrievalConfig(ctx context.Context, retrievalConfig *entity.RetrievalConfig) error {
	data := retrievalConfigDO2PO(retrievalConfig)

	err := d.mysql.Transaction(func(tx *mysqlquery.Query) error {
		// 写 DB，Upsert
		db := tx.TRetrievalConfig.WithContext(ctx)
		e := db.Clauses(clause.OnConflict{UpdateAll: true}).Create(data)
		if e != nil {
			return e
		}
		// 写缓存
		e = d.ModifyRetrievalConfigCache(ctx, retrievalConfig)
		return e
	})
	if err != nil {
		logx.E(ctx, "保存应用检索配置失败 retrievalConfig:%+v err:%+v", retrievalConfig, err)
		return fmt.Errorf("SaveRetrievalConfig err:%w", err)
	}
	return nil
}

func retrievalConfigDO2PO(do *entity.RetrievalConfig) *model.TRetrievalConfig {
	if do == nil {
		return nil
	}
	return &model.TRetrievalConfig{
		ID:                 do.ID,
		RobotID:            do.RobotID,
		EnableVectorRecall: convx.BoolToInt[uint32](do.EnableVectorRecall),
		EnableEsRecall:     convx.BoolToInt[uint32](do.EnableEsRecall),
		EnableRrf:          convx.BoolToInt[uint32](do.EnableRrf),
		EnableText2sql:     convx.BoolToInt[uint32](do.EnableText2Sql),
		RerankThreshold:    do.ReRankThreshold,
		RrfVecWeight:       do.RRFVecWeight,
		RrfEsWeight:        do.RRFEsWeight,
		RrfRerankWeight:    do.RRFReRankWeight,
		DocVecRecallNum:    do.DocVecRecallNum,
		QaVecRecallNum:     do.QaVecRecallNum,
		EsRecallNum:        do.EsRecallNum,
		EsRerankMinNum:     do.EsReRankMinNum,
		RrfReciprocalConst: do.RRFReciprocalConst,
		Operator:           do.Operator,
		EsTopN:             do.EsTopN,
		Text2sqlModel:      do.Text2sqlModel,
		Text2sqlPrompt:     do.Text2sqlPrompt,
	}
}

func (d *daoImpl) ModifyRetrievalConfigCache(ctx context.Context, retrievalConfig *entity.RetrievalConfig) error {
	key := entity.GetRetrievalConfigKey(retrievalConfig.ID)
	reqJSON, _ := jsonx.MarshalToString(retrievalConfig)
	err := d.retrievalRdb.Do(ctx, "SET", key, reqJSON).Err()
	if err != nil {
		logx.E(ctx, "Redis SET failed err:%v", err)
		return err
	}
	return nil
}

func (d *daoImpl) DescribeRetrievalConfigCache(ctx context.Context, appPrimaryId uint64) (*entity.RetrievalConfig, error) {
	retrievalConfig := entity.RetrievalConfig{}
	key := entity.GetRetrievalConfigKey(appPrimaryId)
	defaultConfigKey := entity.GetRetrievalConfigKey(entity.DefaultConfigRobotID)
	logx.D(ctx, "GetRetrievalConfig redis key:%s", key)
	// robotRetrievalConfig, err := redis.Bytes(d.retrievalConfigRedis.Do(ctx, "GET", key))
	robotRetrievalConfig, err := d.retrievalRdb.Get(ctx, key).Result()
	if errx.Is(err, redis.Nil) { // 配置不存在取默认配置
		// defaultRetrievalConfig, err := redis.Bytes(d.retrievalConfigRedis.Do(ctx, "GET", defaultConfigKey))
		defaultRetrievalConfig, err := d.retrievalRdb.Get(ctx, defaultConfigKey).Result()
		if err != nil { // 默认配置手动写入,如果不存在返回异常
			logx.E(ctx, "defaultRetrievalConfig GET failed err:%v", err)
			return &retrievalConfig, err
		}
		err = jsonx.UnmarshalFromString(defaultRetrievalConfig, &retrievalConfig)
		if err != nil {
			logx.E(ctx, "defaultRetrievalConfig Unmarshal failed err:%v", err)
			return &retrievalConfig, err
		}
		logx.D(ctx, "GetRetrievalConfig defaultRetrievalConfig:%v", retrievalConfig)
		return &retrievalConfig, nil
	} else if err != nil {
		logx.E(ctx, "Redis SET failed err:%v", err)
		return &retrievalConfig, err
	}
	err = jsonx.UnmarshalFromString(robotRetrievalConfig, &retrievalConfig)
	if err != nil {
		logx.E(ctx, "retrievalConfig Unmarshal failed err:%v", err)
		return &retrievalConfig, err
	}
	return &retrievalConfig, nil
}

// DescribeRetrievalConfigList 通过RobotID批量查询检索配置，为空则查询所有
func (d *daoImpl) DescribeRetrievalConfigList(ctx context.Context, appPrimaryIds []uint64) ([]*entity.RetrievalConfig, error) {
	var conds []gen.Condition
	if len(appPrimaryIds) > 0 {
		conds = append(conds, d.mysql.TRetrievalConfig.RobotID.In(appPrimaryIds...))
	}
	vals, err := d.mysql.TRetrievalConfig.WithContext(ctx).Where(conds...).Find()
	if err != nil {
		if errx.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		logx.E(ctx, "获取应用检索配置失败 appPrimaryIds:%+v err:%+v", appPrimaryIds, err)
		return nil, fmt.Errorf("GetRetrievalConfigsFromDB err:%w", err)
	}
	logx.I(ctx, "GetRetrievalConfigsFromDB success, count:%d, appPrimaryIds:%+v", len(vals), appPrimaryIds)
	return retrievalConfigsPO2DO(vals), nil
}

func retrievalConfigsPO2DO(pos []*model.TRetrievalConfig) []*entity.RetrievalConfig {
	if pos == nil {
		return nil
	}
	dos := make([]*entity.RetrievalConfig, 0, len(pos))
	for _, po := range pos {
		dos = append(dos, retrievalConfigPO2DO(po))
	}
	return dos
}

func retrievalConfigPO2DO(po *model.TRetrievalConfig) *entity.RetrievalConfig {
	if po == nil {
		return nil
	}
	return &entity.RetrievalConfig{
		ID:                 po.ID,
		RobotID:            po.RobotID,
		EnableVectorRecall: convx.IntToBool(po.EnableVectorRecall),
		EnableEsRecall:     convx.IntToBool(po.EnableEsRecall),
		EnableRrf:          convx.IntToBool(po.EnableRrf),
		EnableText2Sql:     convx.IntToBool(po.EnableText2sql),
		ReRankThreshold:    po.RerankThreshold,
		RRFVecWeight:       po.RrfVecWeight,
		RRFEsWeight:        po.RrfEsWeight,
		RRFReRankWeight:    po.RrfRerankWeight,
		DocVecRecallNum:    po.DocVecRecallNum,
		QaVecRecallNum:     po.QaVecRecallNum,
		EsRecallNum:        po.EsRecallNum,
		EsReRankMinNum:     po.EsRerankMinNum,
		RRFReciprocalConst: po.RrfReciprocalConst,
		Operator:           po.Operator,
		CreateTime:         po.CreateTime,
		UpdateTime:         po.UpdateTime,
		EsTopN:             po.EsTopN,
		Text2sqlModel:      po.Text2sqlModel,
		Text2sqlPrompt:     po.Text2sqlPrompt,
	}
}
