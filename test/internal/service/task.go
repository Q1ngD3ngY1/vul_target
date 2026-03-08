package service

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"git.code.oa.com/trpc-go/trpc-go"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/syncx/errgroupx"
	"git.woa.com/adp/pb-go/common"
	pb "git.woa.com/adp/pb-go/kb/kb_config"

	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/dao"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	labelEntity "git.woa.com/adp/kb/kb-config/internal/entity/label"
	"git.woa.com/adp/kb/kb-config/internal/entity/qa"
	logicDoc "git.woa.com/adp/kb/kb-config/internal/logic/document"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
)

// QASimilarTaskHandler 检索并保存Qa的相似问答
func (s *Service) QASimilarTaskHandler(ctx context.Context) error {
	logx.I(ctx, "QASimilarTaskHandler begin ...")
	qas, err := s.qaLogic.PollQaToSimilar(ctx)
	qas = slicex.UniqueFunc(qas, func(qa *qa.DocQA) string { return qa.Question + qa.Answer })
	if err != nil {
		return err
	}
	g := errgroupx.New()
	g.SetLimit(10)
	for _, qa := range qas {
		tmpQA := qa
		qaCtx := trpc.CloneContext(ctx)
		g.Go(func() error {
			// 加锁
			err = s.qaLogic.LockOneQa(qaCtx, tmpQA)
			if err != nil {
				logx.D(qaCtx, "QASimilarTaskHandler LockOneQa 未获取到锁")
				return nil
			}
			logx.D(qaCtx, "LockOneQa  tmpQA %+v", tmpQA)
			appDB, err := s.rpc.AppAdmin.DescribeAppByPrimaryIdWithoutNotFoundError(qaCtx, tmpQA.RobotID)
			if err != nil {
				return err
			}
			if appDB == nil {
				return errs.ErrRobotNotFound
			}
			if appDB.AppType != entity.KnowledgeQaAppType {
				return errs.ErrGetAppFail
			}
			if appDB.IsDeleted {
				logx.D(qaCtx, "QASimilarTaskHandler 机器人已经删除 机器人ID:%d", appDB.PrimaryId)
				return nil
			}
			if appDB.QaConfig == nil {
				return fmt.Errorf("qa config is nil")
			}
			newCtx := util.SetMultipleMetaData(ctx, appDB.SpaceId, appDB.Uin)

			embeddingVersion := appDB.Embedding.Version
			embeddingModel, err := s.kbLogic.GetKnowledgeEmbeddingModel(newCtx, appDB.CorpBizId, appDB.BizId, appDB.BizId, appDB.IsShared)
			if err != nil {
				logx.W(newCtx, "GetKnowledgeEmbeddingModel err:%v", err)
			} else {
				if embeddingModel != "" {
					embeddingVersion = entity.GetEmbeddingVersion(embeddingModel)
				}
			}
			logx.D(newCtx, "saveQaSimilar | embeddingVersion %d, embeddingModel:%s",
				embeddingVersion, embeddingModel)

			// 保存相似问答对
			err = s.saveQaSimilar(newCtx, tmpQA, embeddingModel, embeddingVersion, appDB.BizId)
			if err != nil {
				return err
			}
			// 解锁
			err = s.qaLogic.UnLockOneQa(qaCtx, tmpQA)
			logx.D(qaCtx, "UnLockOneQa  tmpQA %+v", tmpQA)
			if err != nil {
				logx.D(qaCtx, "QASimilarTaskHandler UnLockOneQa 解锁失败")
				return nil
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		logx.D(ctx, "g.Wait err %v", err)
		return err
	}
	return nil
}

// DeleteCharSizeExceededTaskHandler 定时删除超量失效的文档与问答
func (s *Service) DeleteCharSizeExceededTaskHandler(ctx context.Context) error {
	deadLine, ok := ctx.Deadline()
	logx.D(ctx, "DeleteCharSizeExceededTaskHandler deadLine:%v ok:%v", deadLine, ok)
	// selectColumn := []string{dao.RobotTblColId, dao.RobotTblColCorpId}
	// apps, err := dao.GetRobotDao().GetAllValidApps(ctx, selectColumn)
	apps, _, err := s.rpc.AppAdmin.ListAllAppBaseInfo(ctx, nil)
	if err != nil {
		logx.E(ctx, "获取机器人失败 err:%+v", err)
		return err
	}
	// 2.6.2需求从30天改为180天
	// https://tapd.woa.com/tapd_fe/70080800/story/detail/1070080800119977397?from_iteration_id=1070080800002062621
	reserveTime := 180 * 24 * time.Hour
	for _, app := range apps {
		if err := s.docLogic.DeleteDocsCharSizeExceeded(ctx, app.CorpPrimaryId, app.PrimaryId, reserveTime); err != nil {
			logx.E(ctx, "删除除应用下超量失效的文档失败 %+v err:%+v", app.PrimaryId, err)
			return err
		}
		if err := s.qaLogic.DeleteQAsCharSizeExceeded(ctx, app.CorpPrimaryId, app.PrimaryId, reserveTime); err != nil {
			logx.E(ctx, "删除除应用下超量失效的问答失败 %+v err:%+v", app.PrimaryId, err)
			return err
		}
	}
	return nil
}

// UpdateAttributeLabelsTaskPreview 定时刷新评测环境属性&标签缓存
func (s *Service) UpdateAttributeLabelsTaskPreview(ctx context.Context) error {
	return s.updateAttributeLabelsTask(ctx, labelEntity.AttributeLabelsPreview)
}

// UpdateAttributeLabelsTaskProd 定时刷新发布环境属性&标签缓存
func (s *Service) UpdateAttributeLabelsTaskProd(ctx context.Context) error {
	return s.updateAttributeLabelsTask(ctx, labelEntity.AttributeLabelsProd)
}

// updateAttributeLabelsTask 定时刷新属性&标签缓存
func (s *Service) updateAttributeLabelsTask(ctx context.Context, envType string) error {
	deadLine, ok := ctx.Deadline()
	logx.D(ctx, "UpdateAttributeLabelsTask env:%s deadLine:%v ok:%v", envType, deadLine, ok)
	var redisKey string
	if envType == labelEntity.AttributeLabelsPreview {
		redisKey = dao.UpdateAttributeLabelsTaskPreview
	} else {
		redisKey = dao.UpdateAttributeLabelsTaskProd
	}
	duration := time.Duration(12 * 60) // 12分钟
	err := s.dao.Lock(ctx, redisKey, duration)
	if errors.Is(err, errs.ErrAlreadyLocked) {
		return nil
	} else if err != nil {
		logx.E(ctx, "UpdateAttributeLabelsTask env:%s, err:%+v", envType, err)
		return err
	}
	defer func() { _ = s.dao.UnLock(ctx, redisKey) }()
	apps, _, err := s.rpc.AppAdmin.ListAllAppBaseInfo(ctx, nil)
	if err != nil {
		logx.E(ctx, "UpdateAttributeLabelsTask ListAllAppBaseInfo err:%+v", err)
		return err
	}
	appIDs := slicex.Pluck(apps, func(v *entity.AppBaseInfo) uint64 { return v.PrimaryId })
	// appIDs, err := s.dao.GetAllValidAppIDs(ctx)
	// if err != nil {
	// 	logx.E(ctx, "UpdateAttributeLabelsTask env:%s err:%+v", envType, err)
	// 	return err
	// }
	logx.D(ctx, "UpdateAttributeLabelsTask env:%s appID count:%v", envType, len(appIDs))
	for _, appID := range appIDs {
		var attrs []*labelEntity.AttributeKeyAndID // preview环境属性ID是"ID"字段；prod环境属性ID是"AttrID"字段
		var attrIDList []uint64
		if envType == labelEntity.AttributeLabelsPreview {
			attrs, err = s.labelDao.GetAttributeKeyAndIDsByRobotID(ctx, appID)
			for i, attr := range attrs {
				attrIDList = append(attrIDList, attr.ID)
				attrs[i].AttrID = attr.ID // 用ID覆盖AttrID，后续都用AttrID
			}
		} else {
			attrs, err = s.labelDao.GetAttributeKeyAndIDsByRobotIDProd(ctx, appID)
			for _, attr := range attrs {
				attrIDList = append(attrIDList, attr.AttrID)
			}
		}
		if err != nil {
			logx.E(ctx, "UpdateAttributeLabelsTask env:%s appID:%d err:%+v", envType, appID, err)
			return err
		}
		if len(attrIDList) == 0 {
			continue
		}

		mapAttr2Labels := make(map[uint64][]*labelEntity.AttributeLabel)
		if envType == labelEntity.AttributeLabelsPreview {
			notEmptySimilarLabel := true
			filter := &labelEntity.AttributeLabelFilter{
				RobotId:              appID,
				AttrIds:              attrIDList,
				NotEmptySimilarLabel: &notEmptySimilarLabel,
			}
			selectColumns := []string{labelEntity.AttributeLabelTblColId, labelEntity.AttributeLabelTblColBusinessId,
				labelEntity.AttributeLabelTblColAttrId, labelEntity.AttributeLabelTblColName, labelEntity.AttributeLabelTblColSimilarLabel}
			attrLabels, err := s.labelLogic.GetAttributeLabelList(ctx, selectColumns, filter)
			if err != nil {
				return err
			}
			for _, attrLabel := range attrLabels {
				if _, ok := mapAttr2Labels[attrLabel.AttrID]; !ok {
					mapAttr2Labels[attrLabel.AttrID] = make([]*labelEntity.AttributeLabel, 0)
				}
				mapAttr2Labels[attrLabel.AttrID] = append(mapAttr2Labels[attrLabel.AttrID], attrLabel)
			}
		} else {
			mapAttr2Labels, err = s.labelDao.GetAttributeLabelByAttrIDsWithNotEmptySimilarLabelProd(ctx, attrIDList, appID)
			if err != nil {
				return err
			}
		}
		if len(mapAttr2Labels) == 0 {
			continue
		}
		attrKey2RedisValue := make(map[string][]labelEntity.AttributeLabelRedisValue)
		for attrID, Labels := range mapAttr2Labels {
			var redisValue []labelEntity.AttributeLabelRedisValue
			for _, l := range Labels {
				redisValue = append(redisValue, labelEntity.AttributeLabelRedisValue{
					Name:          l.Name,
					BusinessID:    l.BusinessID,
					SimilarLabels: l.SimilarLabel,
				})
			}
			if len(redisValue) == 0 {
				continue
			}
			for _, attr := range attrs {
				if attr.AttrID == attrID {
					attrKey2RedisValue[attr.AttrKey] = redisValue
					break
				}
			}
		}
		err = s.labelDao.PipelineSetAttributeLabelRedis(ctx, appID, attrKey2RedisValue, envType)
		if err != nil {
			logx.E(ctx, "UpdateAttributeLabelsTask env:%s appID:%d attrKey2RedisValue:%v err:%+v",
				envType, appID, attrKey2RedisValue, err)
			return err
		}
	}
	return nil
}

// GetTaskStatus 查询属性标签任务状态
func (s *Service) GetTaskStatus(ctx context.Context, req *pb.GetTaskStatusReq) (
	*pb.GetTaskStatusRsp, error) {
	logx.D(ctx, "GetTaskStatus req:%+v", req)
	rsp := new(pb.GetTaskStatusRsp)
	var err error
	_, err = util.CheckReqBotBizIDUint64(ctx, req.GetBotBizId())
	if err != nil {
		return rsp, err
	}
	_, err = util.CheckReqParamsIsUint64(ctx, req.GetTaskId())
	if err != nil {
		return rsp, err
	}
	switch req.TaskType {
	case pb.TaskType_ModifyAttributeLabel.String():
		rsp, err = s.getModifyAttributeLabel(ctx, req)
		if err != nil {
			logx.E(ctx, "getModifyAttributeLabel err:%v", err)
			return rsp, err
		}
	case pb.TaskType_ExportAttributeLabel.String():
		rsp, err = s.getExportAttributeLabel(ctx, req)
		if err != nil {
			logx.E(ctx, "getExportAttributeLabel err:%v", err)
			return rsp, err
		}
	}
	return rsp, nil
}

// getExportAttributeLabel 查询导出标签状态
func (s *Service) getExportAttributeLabel(ctx context.Context, req *pb.GetTaskStatusReq) (*pb.GetTaskStatusRsp, error) {
	rsp := new(pb.GetTaskStatusRsp)
	app, err := s.DescribeAppAndCheckCorp(ctx, req.GetBotBizId())
	if err != nil {
		return rsp, errs.ErrSystem
	}
	taskID, err := strconv.ParseUint(req.GetTaskId(), 10, 64)
	if err != nil {
		logx.E(ctx, "taskID转换失败 req:%v", req)
		return rsp, errs.ErrSystem
	}
	exportInfo, err := s.exportLogic.DescribeExportTask(ctx, taskID, app.CorpPrimaryId, app.PrimaryId)
	if err != nil {
		logx.E(ctx, "GetExportTaskInfo err:%v", err)
		return nil, err
	}
	if exportInfo == nil {
		logx.E(ctx, "getExportAttributeLabel 导出标签任务不存在 req:%+v", req)
		return rsp, nil
	}
	rsp.TaskId = strconv.FormatUint(exportInfo.ID, 10)
	taskType := pb.TaskParams{}
	taskType.CosPath = exportInfo.CosURL
	rsp.Params = &taskType
	rsp.Status = exportInfo.GetStatusString()
	rsp.TaskType = pb.TaskType_ExportAttributeLabel.String()
	return rsp, nil
}

// getModifyAttributeLabel 查询编辑标签任务状态
func (s *Service) getModifyAttributeLabel(ctx context.Context, req *pb.GetTaskStatusReq) (*pb.GetTaskStatusRsp, error) {
	rsp := new(pb.GetTaskStatusRsp)
	app, err := s.DescribeAppAndCheckCorp(ctx, req.GetBotBizId())
	if err != nil {
		return rsp, errs.ErrSystem
	}
	taskID, err := strconv.ParseUint(req.TaskId, 10, 64)
	if err != nil {
		logx.E(ctx, "taskID转换失败:%v", req)
		return rsp, errs.ErrParams
	}
	labelTask, err := s.labelDao.GetUpdateAttributeTask(ctx, taskID, app.CorpPrimaryId, app.PrimaryId)
	if err != nil {
		logx.E(ctx, "GetModifyUpdateAttributeTask err:%v", err)
		return rsp, err
	}
	if labelTask == nil {
		logx.E(ctx, "getModifyAttributeLabel 编辑标签任务不存在 req:%+v", req)
		return rsp, nil
	}
	rsp.Status = labelTask.GetStatusString()
	rsp.TaskType = pb.TaskType_ModifyAttributeLabel.String()
	rsp.TaskId = req.TaskId
	rsp.Message = labelTask.Message
	rsp.Params = &pb.TaskParams{
		CosPath: labelTask.CosURL,
	}
	return rsp, nil
}

// CleanVectorSyncHistory 清除t_vector_sync_history表数据
func (s *Service) CleanVectorSyncHistory(ctx context.Context) error {
	var err error

	logx.I(ctx, "CleanVectorSyncHistory lock")
	lockKey := fmt.Sprintf(dao.LockCleanVectorSyncHistory, "CleanVectorSyncHistory")
	if err := s.dao.Lock(ctx, lockKey, 10*time.Minute); err != nil {
		logx.I(ctx, "CleanVectorSyncHistory has locked")
		return nil
	}
	defer func() { _ = s.dao.UnLock(ctx, lockKey) }()
	log.Infof("CleanVectorSyncHistory run,time:%s", time.Now().Format("2006-01-02 15:04:05"))

	// vector := vector.SyncVector{}
	cutooffTime := time.Now().AddDate(0, -3, 0) // 2个月之前的时间
	duration := config.GetMainConfig().CleanVectorSyncHistoryConfig.DeleteDuration
	if duration == 0 {
		duration = 60
	}

	limitSize := config.GetMainConfig().CleanVectorSyncHistoryConfig.Limit
	if limitSize == 0 {
		limitSize = 1000
	}

	// vector := vector.NewVectorSync(s.GetDB(), s.GetTdSqlDB())
	vector := s.qaLogic.GetVectorSyncLogic()

	var rowsAffected = limitSize
	for rowsAffected >= limitSize {
		rowsAffected, err = vector.DeleteVectorSyncHistory(ctx, cutooffTime, limitSize)
		if err != nil {
			logx.W(ctx, "CleanVectorSyncHistory Failed! result:%+v", err)
			return err
		}
		logx.I(ctx, "CleanVectorSyncHistory batchSuccess, rowsAffected:%d,limit：%+v，time:%+v", rowsAffected, limitSize, time.Now().Format("2006-01-02 15:04:05"))
		// sleep一下，避免锁死了数据库
		time.Sleep(time.Duration(duration) * time.Millisecond)
	}

	log.Infof("CleanVectorSyncHistory sccess!,time:%s", time.Now().Format("2006-01-02 15:04:05"))

	return nil
}

// AutoDocRefresh 文档刷新任务
func (s *Service) AutoDocRefresh(ctx context.Context) error {
	logx.I(ctx, "AutoDocRefresh lock")
	startTime := time.Now()
	if !config.GetMainConfig().AutoDocRefreshConfig.Enable {
		// 文档刷新任务被关闭
		logx.D(ctx, "AutoDocRefresh is not enable")
		return nil
	}
	contextx.Metadata(ctx).WithEnvSet(config.GetMainConfig().AutoDocRefreshConfig.EnvName)
	logx.I(ctx, "AutoDocRefresh getEnvSet:%s", contextx.Metadata(ctx).EnvSet())

	lockKey := fmt.Sprintf(dao.LockAutoDocRefresh, "AutoDocRefresh")
	if err := s.dao.Lock(ctx, lockKey, 360*time.Minute); err != nil {
		logx.I(ctx, "AutoDocRefresh has locked")
		return nil
	}
	defer func() { _ = s.dao.UnLock(ctx, lockKey) }()
	logx.I(ctx, "AutoDocRefresh run,time:%s", time.Now().Format("2006-01-02 15:04:05"))

	now := time.Now()
	t := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.Local)
	logx.D(ctx, "AutoDocRefresh GetDocAutoRefreshList time:%+v", t)
	docs, err := s.docLogic.GetDocAutoRefreshList(ctx, t)
	if err != nil {
		logx.E(ctx, "AutoDocRefresh GetDocAutoRefreshList err:%v", err)
		return err
	}
	logx.D(ctx, "AutoDocRefresh GetDocAutoRefreshList len(docs):%+d", len(docs))

	if len(docs) == 0 {
		return nil
	}

	// 按RobotID分组文档
	docsByRobotID := make(map[uint64][]*docEntity.Doc)
	for _, doc := range docs {
		docsByRobotID[doc.RobotID] = append(docsByRobotID[doc.RobotID], doc)
	}

	batchSize := config.GetAutoDocRefreshBatchSize() // 每个异步任务处理的文档数
	var successCount, failCount int
	for robotID, docList := range docsByRobotID {
		total := len(docList)
		nextUpdateTimeDocList := make(map[time.Time][]uint64)
		var updateCorpID uint64
		updateCorpID = docList[0].CorpID // 同一个robotID的文档，corpID相同
		// 处理所有文档，按batchSize条数分批创建异步任务
		for i := 0; i < total; i += batchSize {
			end := i + batchSize
			if end > total {
				end = total
			}
			batch := docList[i:end]
			if len(batch) == 0 {
				logx.W(ctx, "应用ID:%d 获取到空批次文档，跳过处理", robotID)
				continue
			}

			for _, doc := range batch {
				// 计算下次更新时间
				nextUpdateTime := logicDoc.GetDocNextUpdateTime(ctx, doc.UpdatePeriodH)
				// 不同更新频率的文档，下次执行时间不同
				nextUpdateTimeDocList[nextUpdateTime] = append(nextUpdateTimeDocList[nextUpdateTime], doc.ID)
			}

			logx.I(ctx, "应用ID:%d 正在处理第%d-%d条文档(共%d条)",
				robotID, i+1, end, total)
			sourceDocMap := slicex.Group(batch, func(d *docEntity.Doc) uint32 {
				return d.Source
			})
			for source, batchDoc := range sourceDocMap {
				switch source {
				case docEntity.SourceFromTxDoc:
					if err := s.docLogic.RefreshTxDoc(ctx, true, batchDoc); err != nil {
						logx.E(ctx, "应用ID:%d 刷新文档失败(第%d-%d条):%v",
							robotID, i+1, end, err)
						failCount += len(batchDoc)
					}
				case docEntity.SourceFromOnedrive:
					oneDriveLogic := s.thirdDocLogic.GetThirdDocLogic(common.SourceFromType_SOURCE_FROM_TYPE_ONEDRIVE)
					if err := oneDriveLogic.RefreshDoc(ctx, true, batchDoc); err != nil {
						logx.E(ctx, "应用ID:%d 刷新文档失败(第%d-%d条):%v", robotID, i+1, end, err)
						failCount += len(batchDoc)
					}
				}
			}
			successCount += len(batch)
			// 短暂休眠避免压力过大
			// time.Sleep(100 * time.Millisecond)
		}

		// 不同的下次执行时间,分开更新
		for nextUpdateTime, updateDocIDs := range nextUpdateTimeDocList {
			updateDocFilter := &docEntity.DocFilter{
				IDs:     updateDocIDs,
				CorpId:  updateCorpID,
				RobotId: robotID,
			}
			doc := docEntity.Doc{}
			doc.NextUpdateTime = nextUpdateTime
			updateDocColumns := []string{
				docEntity.DocTblColNextUpdateTime}
			_, err = s.docLogic.UpdateLogicByDao(ctx, updateDocColumns, updateDocFilter, &doc)
			if err != nil {
				logx.E(ctx, "腾讯文档自动刷新,更新下次执行时间错误 nextUpdateTime:%v,docIDs:%+v err: %+v",
					nextUpdateTime, updateDocIDs, err)
				// 不影响后续待执行的文档
				continue
			}
		}
	}
	duration := time.Since(startTime)
	logx.I(ctx, "AutoDocRefresh completed, success:%d, failed:%d, duration:%s,nowTime:%s",
		successCount, failCount, duration, time.Now().Format("2006-01-02 15:04:05"))
	return nil
}
