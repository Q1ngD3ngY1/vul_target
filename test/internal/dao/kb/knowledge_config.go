package kb

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/model"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/tdsqlquery"
	kbe "git.woa.com/adp/kb/kb-config/internal/entity/kb"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm/clause"
)

func getKnowledgeConfigsPO2DO(pos []*model.TKnowledgeConfig) []*kbe.KnowledgeConfig {
	var res []*kbe.KnowledgeConfig
	for _, config := range pos {
		if config.Config == "" && config.PreviewConfig == "" {
			// 如果评测发布配置都为空（正常是应用下共享知识库被解除绑定），该配置项应该直接被忽略
			continue
		}
		res = append(res, getKnowledgeConfigPO2DO(config))
	}
	return res
}

func getKnowledgeConfigPO2DO(po *model.TKnowledgeConfig) *kbe.KnowledgeConfig {
	if po == nil {
		return nil
	}
	return &kbe.KnowledgeConfig{
		ID:             po.ID,
		CorpBizID:      po.CorpBizID,
		KnowledgeBizID: po.KnowledgeBizID,
		Type:           po.Type,
		Config:         po.Config,
		IsDeleted:      po.IsDeleted,
		CreateTime:     po.CreateTime,
		UpdateTime:     po.UpdateTime,
		AppBizID:       po.AppBizID,
		PreviewConfig:  po.PreviewConfig,
	}
}

func getKnowledgeConfigsDO2PO(dos []*kbe.KnowledgeConfig) []*model.TKnowledgeConfig {
	return slicex.Map(dos, func(do *kbe.KnowledgeConfig) *model.TKnowledgeConfig {
		return getKnowledgeConfigDO2PO(do)
	})
}

func getKnowledgeConfigDO2PO(do *kbe.KnowledgeConfig) *model.TKnowledgeConfig {
	if do == nil {
		return nil
	}
	return &model.TKnowledgeConfig{
		ID:             do.ID,
		CorpBizID:      do.CorpBizID,
		KnowledgeBizID: do.KnowledgeBizID,
		Type:           do.Type,
		Config:         do.Config,
		IsDeleted:      do.IsDeleted,
		CreateTime:     do.CreateTime,
		UpdateTime:     do.UpdateTime,
		AppBizID:       do.AppBizID,
		PreviewConfig:  do.PreviewConfig,
	}
}

// GetKnowledgeConfigsByModelAssociated 通过模型信息获取知识库配置（Gen风格）
func (d *daoImpl) GetKnowledgeConfigsByModelAssociated(
	ctx context.Context, corpBizId uint64, modelKeyword string) ([]*kbe.KnowledgeConfig, error) {
	// 1. 初始化Gen生成的Query
	q := d.tdsql.TKnowledgeConfig.WithContext(ctx)

	// 2. 构建类型安全查询条件
	configTypes := []uint32{
		uint32(pb.KnowledgeBaseConfigType_EMBEDDING_MODEL),
		uint32(pb.KnowledgeBaseConfigType_QA_EXTRACT_MODEL),
		uint32(pb.KnowledgeBaseConfigType_KNOWLEDGE_SCHEMA_MODEL),
		uint32(pb.KnowledgeBaseConfigType_FILE_PARSE_MODEL),
	}

	// 3. 执行查询
	configs, err := q.Where(
		d.tdsql.TKnowledgeConfig.CorpBizID.Eq(corpBizId),                          // corp_biz_id = ?
		d.tdsql.TKnowledgeConfig.Type.In(configTypes...),                          // type IN (?,?,?)
		d.tdsql.TKnowledgeConfig.IsDeleted.Is(false),                              // is_deleted = ?
		d.tdsql.TKnowledgeConfig.Config.Like(fmt.Sprintf("%%%s%%", modelKeyword)), // config LIKE %keyword%
	).Find()

	// 4. 错误处理
	if err != nil {
		logx.E(ctx, "查询失败: corpBizId=%d, keyword=%s, err=%v",
			corpBizId, modelKeyword, err)
		return nil, fmt.Errorf("查询知识库配置失败: %w", err)
	}
	return getKnowledgeConfigsPO2DO(configs), nil
}

// DeleteKnowledgeConfigs 删除知识库配置
func (d *daoImpl) DeleteKnowledgeConfigs(ctx context.Context, corpBizId uint64, knowledgeBizIds []uint64) error {
	_, err := d.tdsql.TKnowledgeConfig.WithContext(ctx).
		Where(d.tdsql.TKnowledgeConfig.CorpBizID.Eq(corpBizId)).
		Where(d.tdsql.TKnowledgeConfig.KnowledgeBizID.In(knowledgeBizIds...)).
		Where(d.tdsql.TKnowledgeConfig.AppBizID.Eq(0)). // AppBizID为0时，表示这是全局共享知识库，而非某个应用的知识库
		Where(d.tdsql.TKnowledgeConfig.IsDeleted.Is(false)).
		Updates(map[string]interface{}{
			d.tdsql.TKnowledgeConfig.IsDeleted.ColumnName().String():  true,
			d.tdsql.TKnowledgeConfig.UpdateTime.ColumnName().String(): time.Now(),
		})
	for _, knowledgeBizID := range knowledgeBizIds {
		d.DeleteShareKnowledgeConfigFromCache(ctx, corpBizId, knowledgeBizID)
	}
	return err
}

// convertKnowledgeModelConfigToJson 转换知识库模型模型配置为JSON格式
func convertKnowledgeModelConfigToJson(knowledgeConfig *kbe.KnowledgeConfig) {
	switch knowledgeConfig.Type {
	case uint32(pb.KnowledgeBaseConfigType_EMBEDDING_MODEL):
		fallthrough
	case uint32(pb.KnowledgeBaseConfigType_QA_EXTRACT_MODEL):
		fallthrough
	case uint32(pb.KnowledgeBaseConfigType_KNOWLEDGE_SCHEMA_MODEL):
		fallthrough
	case uint32(pb.KnowledgeBaseConfigType_FILE_PARSE_MODEL):
		// 只有非空且非法JSON的字符串才需要转换
		if knowledgeConfig.Config != "" && !json.Valid([]byte(knowledgeConfig.Config)) {
			// 使用json.Marshal确保正确转义特殊字符
			modelNameBytes, _ := json.Marshal(knowledgeConfig.Config)
			knowledgeConfig.Config = fmt.Sprintf(`{"model_name":%s}`, modelNameBytes)
		}
		if knowledgeConfig.PreviewConfig != "" && !json.Valid([]byte(knowledgeConfig.PreviewConfig)) {
			// 使用json.Marshal确保正确转义特殊字符
			modelNameBytes, _ := json.Marshal(knowledgeConfig.PreviewConfig)
			knowledgeConfig.PreviewConfig = fmt.Sprintf(`{"model_name":%s}`, modelNameBytes)
		}
	default:
		// 其他类型不需要转换，保持原样
	}
}

// GetShareKnowledgeConfigs 获取共享知识库配置
func (d *daoImpl) GetShareKnowledgeConfigs(ctx context.Context,
	corpBizId uint64, knowledgeBizIds []uint64, configTypes []uint32) ([]*kbe.KnowledgeConfig, error) {
	// Cache-Aside模式，先读缓存，缓存不存在再读数据库
	knowledgeBizID2Configs, err := d.describeShareKnowledgeConfigFromCache(ctx, corpBizId, knowledgeBizIds)
	if err != nil {
		knowledgeBizID2Configs = make(map[uint64][]*kbe.KnowledgeConfig)
	}
	configList := make([]*kbe.KnowledgeConfig, 0)
	uncachedKnowledgeBizIds := make([]uint64, 0)
	for _, knowledgeBizID := range knowledgeBizIds {
		if _, ok := knowledgeBizID2Configs[knowledgeBizID]; ok {
			for _, shareKBConfig := range knowledgeBizID2Configs[knowledgeBizID] {
				if slices.Contains(configTypes, shareKBConfig.Type) {
					// 创建副本，避免修改缓存中的原始数据
					configList = append(configList, shareKBConfig)
				}
			}
		} else {
			uncachedKnowledgeBizIds = append(uncachedKnowledgeBizIds, knowledgeBizID)
		}
	}
	if len(uncachedKnowledgeBizIds) == 0 {
		// 缓存全部命中，直接返回，无需再查TDSQL
		logx.D(ctx, "GetShareKnowledgeConfigs all cache hit, corpBizId:%d, knowledgeBizIds:%+v", corpBizId, knowledgeBizIds)
		return configList, nil
	}
	logx.D(ctx, "DescribeShareKnowledgeConfigList some cache miss, corpBizId:%d, uncachedKnowledgeBizIds:%+v",
		corpBizId, uncachedKnowledgeBizIds)

	res, err := d.tdsql.TKnowledgeConfig.WithContext(ctx).
		Where(d.tdsql.TKnowledgeConfig.CorpBizID.Eq(corpBizId)).
		Where(d.tdsql.TKnowledgeConfig.KnowledgeBizID.In(knowledgeBizIds...)).
		Where(d.tdsql.TKnowledgeConfig.IsDeleted.Is(false)).
		Where(d.tdsql.TKnowledgeConfig.AppBizID.Eq(0)). // AppBizID为0时，表示这是全局共享知识库，而非某个应用的知识库
		Find()
	if err != nil {
		logx.E(ctx, "GetShareKnowledgeConfigs err: %+v", err)
		return nil, err
	}
	if len(res) > 0 {
		uncachedConfigList := getKnowledgeConfigsPO2DO(res)
		// 先写缓存，保存原始数据（未经convertKnowledgeModelConfigToJson转换）
		d.modifyShareKnowledgeConfigToCache(ctx, uncachedConfigList)

		// 再转换副本并添加到结果列表
		for _, shareKBConfig := range uncachedConfigList {
			if slices.Contains(configTypes, shareKBConfig.Type) {
				// 创建副本，避免修改原始数据
				convertKnowledgeModelConfigToJson(shareKBConfig)
				configList = append(configList, shareKBConfig)
			}
		}
	}
	return configList, nil
}

// SetKnowledgeConfig 设置知识库配置
func (d *daoImpl) SetKnowledgeConfig(ctx context.Context, knowledgeConfig *kbe.KnowledgeConfig, tx *tdsqlquery.Query, updateReleaseConfig bool) error {
	// t_knowledge_config有唯一索引`uk_biz_type` (`app_biz_id`, `corp_biz_id`, `knowledge_biz_id`, `type`)
	updateColumns := []string{
		tx.TKnowledgeConfig.UpdateTime.ColumnName().String(),
		tx.TKnowledgeConfig.IsDeleted.ColumnName().String(),
	}
	needUpdate := false
	if knowledgeConfig.PreviewConfig != "" {
		updateColumns = append(updateColumns, tx.TKnowledgeConfig.PreviewConfig.ColumnName().String())
		needUpdate = true
	}
	if updateReleaseConfig {
		updateColumns = append(updateColumns, tx.TKnowledgeConfig.Config.ColumnName().String())
		needUpdate = true
	}
	if needUpdate {
		err := tx.TKnowledgeConfig.WithContext(ctx).Clauses(clause.OnConflict{
			Columns: []clause.Column{
				{Name: tx.TKnowledgeConfig.CorpBizID.ColumnName().String()},
				{Name: tx.TKnowledgeConfig.KnowledgeBizID.ColumnName().String()},
				{Name: tx.TKnowledgeConfig.Type.ColumnName().String()},
				{Name: tx.TKnowledgeConfig.AppBizID.ColumnName().String()},
			},
			DoUpdates: clause.AssignmentColumns(updateColumns), // 冲突时更新 config,update_time,is_deleted,preview_config 字段
		}).Create(getKnowledgeConfigDO2PO(knowledgeConfig))
		if err != nil {
			return err
		}
	}
	if !updateReleaseConfig {
		// 不允许直接更新发布配置的场景，如果DB中的config（对应发布配置）字段是空的，而入参中的发布配置不为空，则需要更新DB中的config字段
		// 此时对应的场景是，重构发布后，老应用第一次同步知识库配置，不仅需要更新评测配置，也需要更新发布配置
		if knowledgeConfig.Config != "" {
			res, err := tx.TKnowledgeConfig.WithContext(ctx).
				Where(tx.TKnowledgeConfig.CorpBizID.Eq(knowledgeConfig.CorpBizID)).
				Where(tx.TKnowledgeConfig.KnowledgeBizID.Eq(knowledgeConfig.KnowledgeBizID)).
				Where(tx.TKnowledgeConfig.Type.Eq(knowledgeConfig.Type)).
				Where(tx.TKnowledgeConfig.AppBizID.Eq(knowledgeConfig.AppBizID)).
				Where(tx.TKnowledgeConfig.Config.Eq("")).
				Updates(map[string]interface{}{
					tx.TKnowledgeConfig.Config.ColumnName().String():     knowledgeConfig.Config,
					tx.TKnowledgeConfig.UpdateTime.ColumnName().String(): knowledgeConfig.UpdateTime,
				})
			if err != nil {
				logx.E(ctx, "SetKnowledgeConfig(for old app) update release config err: %+v", err)
				return err
			}
			if res.RowsAffected > 0 {
				logx.I(ctx, "SetKnowledgeConfig(for old app) update release config success: %+v", res)
			}
		}
	}
	return nil
}

// ModifyKnowledgeConfigList 设置知识库配置
func (d *daoImpl) ModifyKnowledgeConfigList(ctx context.Context, configList []*kbe.KnowledgeConfig) error {
	if len(configList) == 0 {
		return nil
	}
	var appBizIDList []uint64
	corpBizID := configList[0].CorpBizID
	for _, kbConfig := range configList {
		if kbConfig.CorpBizID != corpBizID {
			return fmt.Errorf("Different corpBizID: expected %d, got %d", corpBizID, kbConfig.CorpBizID)
		}
		appBizIDList = append(appBizIDList, kbConfig.AppBizID)
	}
	// t_knowledge_config有唯一索引`uk_biz_type` (`app_biz_id`, `corp_biz_id`, `knowledge_biz_id`, `type`)
	err := d.tdsql.TKnowledgeConfig.WithContext(ctx).Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: d.tdsql.TKnowledgeConfig.CorpBizID.ColumnName().String()},
			{Name: d.tdsql.TKnowledgeConfig.KnowledgeBizID.ColumnName().String()},
			{Name: d.tdsql.TKnowledgeConfig.Type.ColumnName().String()},
			{Name: d.tdsql.TKnowledgeConfig.AppBizID.ColumnName().String()},
		},
		DoUpdates: clause.AssignmentColumns([]string{
			d.tdsql.TKnowledgeConfig.Config.ColumnName().String(),
			d.tdsql.TKnowledgeConfig.UpdateTime.ColumnName().String(),
			d.tdsql.TKnowledgeConfig.IsDeleted.ColumnName().String(),
			d.tdsql.TKnowledgeConfig.PreviewConfig.ColumnName().String(),
		}), // 冲突时更新 config,update_time,is_deleted,preview_config 字段
	}).CreateInBatches(getKnowledgeConfigsDO2PO(configList), 10)
	if err != nil {
		return err
	}
	appBizIDList = slicex.Unique(appBizIDList)
	for _, appBizID := range appBizIDList {
		// Cache-Aside模式，更新成功后，删除缓存
		d.DeleteAppKnowledgeConfigFromCache(ctx, corpBizID, appBizID)
	}
	return nil
}

func shareKnowLedgeConfigCacheKey(corpBizID, knowledgeBizID uint64) string {
	return fmt.Sprintf("share_kb_config:%d:%d", corpBizID, knowledgeBizID)
}

func (d *daoImpl) describeShareKnowledgeConfigFromCache(ctx context.Context, corpBizId uint64,
	knowledgeBizIds []uint64) (map[uint64][]*kbe.KnowledgeConfig, error) {
	res := make(map[uint64][]*kbe.KnowledgeConfig)
	for _, chunkKnowledgeBizIDs := range slicex.Chunk(knowledgeBizIds, 30) {
		pipe := d.rdb.Pipeline()
		cmds := make(map[uint64]*redis.MapStringStringCmd)
		for _, knowledgeBizID := range chunkKnowledgeBizIDs {
			cacheKey := shareKnowLedgeConfigCacheKey(corpBizId, knowledgeBizID)
			cmds[knowledgeBizID] = pipe.HGetAll(ctx, cacheKey)
		}
		if _, err := pipe.Exec(ctx); err != nil {
			logx.E(ctx, "Failed to exec shareKnowLedgeConfigCacheKey, err: %v", err)
			return nil, err
		}
		for knowledgeBizID, cmd := range cmds {
			values, err := cmd.Result()
			if err != nil {
				logx.E(ctx, "Failed to exec shareKnowLedgeConfigCacheKey, err: %v", err)
				return nil, err
			}
			if len(values) == 0 {
				logx.D(ctx, "shareKnowLedgeConfigCacheKey not hit, knowledgeBizID:%d", knowledgeBizID)
				continue
			}
			configs := make([]*kbe.KnowledgeConfig, 0)
			for key, val := range values {
				logx.D(ctx, "describeShareKnowledgeConfigFromCache corpBizID:%d, knowledgeBizID:%d, key:%s, configs: %s",
					corpBizId, knowledgeBizID, key, val)
				config := &kbe.KnowledgeConfig{}
				err = json.Unmarshal([]byte(val), config)
				if err != nil {
					logx.E(ctx, "Failed to unmarshal shareKnowLedgeConfigCacheKey, val:%s, err: %v", val, err)
					return nil, err
				}
				convertKnowledgeModelConfigToJson(config)
				configs = append(configs, config)
			}
			res[knowledgeBizID] = configs
		}
	}
	return res, nil
}

// modifyShareKnowledgeConfigToCache 修改共享知识库配置缓存
func (d *daoImpl) modifyShareKnowledgeConfigToCache(ctx context.Context, configs []*kbe.KnowledgeConfig) {
	if len(configs) == 0 {
		return
	}
	knowledgeBizId2Configs := make(map[uint64][]*kbe.KnowledgeConfig)
	for _, config := range configs {
		knowledgeBizId2Configs[config.KnowledgeBizID] = append(knowledgeBizId2Configs[config.KnowledgeBizID], config)
	}
	for _, knowledgeConfigs := range knowledgeBizId2Configs {
		cacheKey := shareKnowLedgeConfigCacheKey(knowledgeConfigs[0].CorpBizID, knowledgeConfigs[0].KnowledgeBizID)
		data := make(map[string]interface{})
		for _, config := range knowledgeConfigs {
			val, err := json.Marshal(config)
			if err != nil {
				logx.E(ctx, "Failed to marshal shareKnowLedgeConfigCacheKey %s, err: %v", cacheKey, err)
				return
			}
			data[fmt.Sprintf("%d", config.Type)] = val
		}
		err := d.rdb.HSet(ctx, cacheKey, data).Err()
		if err != nil {
			logx.E(ctx, "Failed to set shareKnowLedgeConfigCacheKey %s, err: %v", cacheKey, err)
			return
		}
		logx.D(ctx, "modifyShareKnowledgeConfigToCache key:%s, data: %+v, time:%s", cacheKey, data, time.Now().Format("2006-01-02 15:04:05.999999"))
		err = d.rdb.Expire(ctx, cacheKey, 3600*24*10*time.Second).Err()
		if err != nil {
			logx.E(ctx, "Failed to set appKnowLedgeConfigCacheKey %s, err: %v", cacheKey, err)
			return
		}
	}
}

func (d *daoImpl) DeleteShareKnowledgeConfigFromCache(ctx context.Context, corpBizId, knowledgeBizId uint64) {
	cacheKey := shareKnowLedgeConfigCacheKey(corpBizId, knowledgeBizId)
	err := d.rdb.Del(ctx, cacheKey).Err()
	if err != nil {
		logx.W(ctx, "Failed to delete appKnowLedgeConfigCacheKey %s, err: %v", cacheKey, err)
	}
	logx.I(ctx, "DeleteShareKnowledgeConfigFromCache key:%s, time:%s", cacheKey, time.Now().Format("2006-01-02 15:04:05.999999"))
}

func appKnowLedgeConfigCacheKey(corpBizID, appBizID uint64) string {
	return fmt.Sprintf("app_kb_config:%d:%d", corpBizID, appBizID)
}

func (d *daoImpl) describeAppKnowledgeConfigListFromCache(ctx context.Context, corpBizID uint64, appBizIDs []uint64) (map[uint64][]*kbe.KnowledgeConfig, error) {
	res := make(map[uint64][]*kbe.KnowledgeConfig)
	for _, chunkAppBizIDs := range slicex.Chunk(appBizIDs, 30) {
		pipe := d.rdb.Pipeline()
		cmds := make(map[uint64]*redis.MapStringStringCmd)
		for _, appBizID := range chunkAppBizIDs {
			cacheKey := appKnowLedgeConfigCacheKey(corpBizID, appBizID)
			cmds[appBizID] = pipe.HGetAll(ctx, cacheKey)
		}
		if _, err := pipe.Exec(ctx); err != nil {
			logx.E(ctx, "Failed to exec appKnowLedgeConfigCacheKey, err: %v", err)
			return nil, err
		}
		for appBizID, cmd := range cmds {
			values, err := cmd.Result()
			if err != nil {
				logx.E(ctx, "Failed to get appKnowLedgeConfigCacheKey, err: %v", err)
				continue
			}
			if len(values) == 0 {
				logx.D(ctx, "appKnowLedgeConfigCacheKey not hit, appBizID:%d", appBizID)
				continue
			}
			configs := make([]*kbe.KnowledgeConfig, 0)
			for key, val := range values {
				logx.D(ctx, "describeAppKnowledgeConfigListFromCache appBizID:%d, key:%s, configs: %s", appBizID, key, val)
				config := &kbe.KnowledgeConfig{}
				err = json.Unmarshal([]byte(val), config)
				if err != nil {
					logx.E(ctx, "Failed to unmarshal appKnowLedgeConfigCacheKey, val:%s, err: %v", val, err)
					return nil, err
				}
				convertKnowledgeModelConfigToJson(config)
				configs = append(configs, config)
			}
			res[appBizID] = configs
		}
	}
	return res, nil
}

func (d *daoImpl) modifyAppKnowledgeConfigToCache(ctx context.Context, configs []*kbe.KnowledgeConfig) {
	if len(configs) == 0 {
		return
	}
	appBizId2Configs := make(map[uint64][]*kbe.KnowledgeConfig)
	for _, config := range configs {
		appBizId2Configs[config.AppBizID] = append(appBizId2Configs[config.AppBizID], config)
	}
	for _, knowledgeConfigs := range appBizId2Configs {
		cacheKey := appKnowLedgeConfigCacheKey(knowledgeConfigs[0].CorpBizID, knowledgeConfigs[0].AppBizID)
		data := make(map[string]interface{})
		for _, config := range knowledgeConfigs {
			val, err := json.Marshal(config)
			if err != nil {
				logx.E(ctx, "Failed to marshal appKnowLedgeConfigCacheKey %s, err: %v", cacheKey, err)
				return
			}
			data[fmt.Sprintf("%d:%d", config.KnowledgeBizID, config.Type)] = val
		}
		err := d.rdb.HSet(ctx, cacheKey, data).Err()
		if err != nil {
			logx.E(ctx, "Failed to set appKnowLedgeConfigCacheKey %s, err: %v", cacheKey, err)
			return
		}
		logx.I(ctx, "modifyAppKnowledgeConfigToCache key:%s, data: %+v, time:%s", cacheKey, data, time.Now().Format("2006-01-02 15:04:05.999999"))
		err = d.rdb.Expire(ctx, cacheKey, 3600*24*10*time.Second).Err()
		if err != nil {
			logx.E(ctx, "Failed to set appKnowLedgeConfigCacheKey %s, err: %v", cacheKey, err)
			return
		}
	}
}

func (d *daoImpl) DeleteAppKnowledgeConfigFromCache(ctx context.Context, corpBizId, appBizId uint64) {
	cacheKey := appKnowLedgeConfigCacheKey(corpBizId, appBizId)
	err := d.rdb.Del(ctx, cacheKey).Err()
	if err != nil {
		logx.E(ctx, "Failed to delete appKnowLedgeConfigCacheKey %s, err: %v", cacheKey, err)
	}
	logx.I(ctx, "DeleteAppKnowledgeConfigFromCache key:%s, time:%s", cacheKey, time.Now().Format("2006-01-02 15:04:05.999999"))
}

// DescribeAppKnowledgeConfig 获取应用下知识库配置
func (d *daoImpl) DescribeAppKnowledgeConfig(ctx context.Context, corpBizID, appBizID, knowledgeBizID uint64) ([]*kbe.KnowledgeConfig, error) {
	configs, err := d.DescribeAppKnowledgeConfigList(ctx, corpBizID, []uint64{appBizID})
	if err != nil {
		return nil, err
	}
	var res []*kbe.KnowledgeConfig
	for _, config := range configs {
		if config.KnowledgeBizID == knowledgeBizID {
			res = append(res, config)
		}
	}
	return res, nil
}

// DescribeAppKnowledgeConfigList 获取应用下知识库配置
func (d *daoImpl) DescribeAppKnowledgeConfigList(ctx context.Context, corpBizID uint64, appBizIDs []uint64) ([]*kbe.KnowledgeConfig, error) {
	// Cache-Aside模式，先读缓存，缓存不存在再读数据库
	appBizID2Configs, err := d.describeAppKnowledgeConfigListFromCache(ctx, corpBizID, appBizIDs)
	if err != nil {
		appBizID2Configs = make(map[uint64][]*kbe.KnowledgeConfig)
	}
	configList := make([]*kbe.KnowledgeConfig, 0)
	uncachedAppBizIDs := make([]uint64, 0)
	for _, appBizID := range appBizIDs {
		if _, ok := appBizID2Configs[appBizID]; ok {
			configList = append(configList, appBizID2Configs[appBizID]...)
		} else {
			uncachedAppBizIDs = append(uncachedAppBizIDs, appBizID)
		}
	}
	if len(uncachedAppBizIDs) == 0 {
		// 缓存全部命中，直接返回，无需再查TDSQL
		logx.D(ctx, "DescribeAppKnowledgeConfigList all cache hit, corpBizID:%d, appBizIDs:%+v", corpBizID, appBizIDs)
		return configList, nil
	}
	logx.D(ctx, "DescribeAppKnowledgeConfigList some cache miss, corpBizID:%d, uncachedAppBizIDs:%+v", corpBizID, uncachedAppBizIDs)
	// 剩下缓存未命中的，再查TDSQL
	configTypes := []uint32{
		uint32(pb.KnowledgeBaseConfigType_EMBEDDING_MODEL),
		uint32(pb.KnowledgeBaseConfigType_QA_EXTRACT_MODEL),
		uint32(pb.KnowledgeBaseConfigType_KNOWLEDGE_SCHEMA_MODEL),
		uint32(pb.KnowledgeBaseConfigType_RETRIEVAL_SETTING),
		uint32(pb.KnowledgeBaseConfigType_FILE_PARSE_MODEL),
	}
	res, err := d.tdsql.TKnowledgeConfig.WithContext(ctx).
		Where(d.tdsql.TKnowledgeConfig.CorpBizID.Eq(corpBizID)).
		Where(d.tdsql.TKnowledgeConfig.Type.In(configTypes...)).
		Where(d.tdsql.TKnowledgeConfig.IsDeleted.Is(false)).
		Where(d.tdsql.TKnowledgeConfig.AppBizID.In(uncachedAppBizIDs...)).
		Find()
	if err != nil {
		return nil, err
	}
	if len(res) > 0 {
		uncachedConfigList := getKnowledgeConfigsPO2DO(res)
		for _, config := range uncachedConfigList {
			convertKnowledgeModelConfigToJson(config)
		}
		d.modifyAppKnowledgeConfigToCache(ctx, uncachedConfigList)
		configList = append(configList, uncachedConfigList...)
	}
	return configList, nil

}

// DeleteAppSharedKnowledgeConfigs 删除应用下共享知识库配置
func (d *daoImpl) DeleteAppSharedKnowledgeConfigs(ctx context.Context, corpBizId, appBizId uint64, knowledgeBizIds []uint64) error {
	_, err := d.tdsql.TKnowledgeConfig.WithContext(ctx).
		Where(d.tdsql.TKnowledgeConfig.CorpBizID.Eq(corpBizId)).
		Where(d.tdsql.TKnowledgeConfig.AppBizID.Eq(appBizId)).
		Where(d.tdsql.TKnowledgeConfig.KnowledgeBizID.In(knowledgeBizIds...)).
		Where(d.tdsql.TKnowledgeConfig.IsDeleted.Is(false)).
		Updates(map[string]interface{}{
			d.tdsql.TKnowledgeConfig.PreviewConfig.ColumnName().String(): "", // 将PreviewConfig置为空，如果应用已发布，检索配置应该仍然能搜到
			d.tdsql.TKnowledgeConfig.UpdateTime.ColumnName().String():    time.Now(),
		})
	if err != nil {
		return err
	}
	d.DeleteAppKnowledgeConfigFromCache(ctx, corpBizId, appBizId)
	return nil
}
