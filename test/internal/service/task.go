package service

import (
	"context"
	"errors"
	"fmt"
	"git.woa.com/baicaoyuan/moss/metadata"
	logicDoc "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/doc"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/baicaoyuan/moss/types/slicex"
	appImpl "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/app"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/vector"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	config2 "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/util/config"
	"git.woa.com/dialogue-platform/common/v3/sync/errgroupx"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
	"github.com/spf13/cast"
)

// QASimilarTaskHandler 检索并保存Qa的相似问答
func (s *Service) QASimilarTaskHandler(ctx context.Context) error {
	log.InfoContextf(ctx, "QASimilarTaskHandler begin ...")
	qas, err := s.dao.PollQaToSimilar(ctx)
	qas = slicex.UniqueFunc(qas, func(qa *model.DocQA) string { return qa.Question + qa.Answer })
	if err != nil {
		return err
	}
	g := errgroupx.Group{}
	g.SetLimit(10)
	for _, qa := range qas {
		tmpQA := qa
		g.Go(func() error {
			// 加锁
			err = s.dao.LockOneQa(ctx, tmpQA)
			if err != nil {
				log.DebugContextf(ctx, "QASimilarTaskHandler LockOneQa 未获取到锁")
				return nil
			}
			log.DebugContextf(ctx, "LockOneQa  tmpQA %+v", tmpQA)
			appDB, err := s.dao.GetAppByID(ctx, tmpQA.RobotID)
			if err != nil {
				return err
			}
			if appDB == nil {
				return errs.ErrRobotNotFound
			}
			instance := appImpl.GetApp(appDB.AppType)
			if instance == nil {
				return errs.ErrAppTypeInvalid
			}
			app, err := instance.AnalysisDescribeApp(ctx, appDB)
			if err != nil {
				return err
			}
			if app.HasDeleted() {
				log.DebugContextf(ctx, "QASimilarTaskHandler 机器人已经删除 机器人ID:%d", app.GetAppID())
				return nil
			}
			ctx = pkg.WithSpaceID(ctx, app.SpaceID)
			embeddingConf, _, err := app.GetEmbeddingConf()
			if err != nil {
				return err
			}
			searchVector, _, err := app.GetSearchVector(model.AppTestScenes)
			if err != nil {
				return err
			}
			// 保存相似问答对
			err = s.saveQaSimilar(ctx, tmpQA, searchVector, embeddingConf.Version, appDB.BusinessID)
			if err != nil {
				return err
			}
			// 解锁
			err = s.dao.UnLockOneQa(ctx, tmpQA)
			log.DebugContextf(ctx, "LockOneQa  tmpQA %+v", tmpQA)
			if err != nil {
				log.DebugContextf(ctx, "QASimilarTaskHandler UnLockOneQa 解锁失败")
				return nil
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		log.DebugContextf(ctx, "g.Wait err %v", err)
		return err
	}
	return nil
}

// RebuildVectorIndex 脚本 用于存量企业创建发布向量库（已存在的 index 不会返回异常）
func (s *Service) RebuildVectorIndex(w http.ResponseWriter, r *http.Request) {
	log.ErrorContextf(r.Context(), "准备删除的接口收到了请求 deprecated interface req:%+v", r)

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	if strings.ToUpper(r.Method) != "POST" {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("only POST is allowed"))
		return
	}
	if config.App().RobotDefault.Embedding.Version != config.App().RobotDefault.Embedding.UpgradeVersion {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(fmt.Sprintf("err: embedding upgrading")))
		return
	}
	ctx := r.Context()
	BusinessID := r.PostFormValue("business_id")
	var (
		apps []*model.AppDB
		app  *model.AppDB
		err  error
	)
	if BusinessID != "" {
		app, err = s.dao.GetAppByAppBizID(ctx, cast.ToUint64(BusinessID))
		if err != nil {
			log.ErrorContextf(ctx, "获取机器人失败 err:%+v", err)
			return
		}
		apps = append(apps, app)
	} else {
		selectColumn := []string{dao.RobotTblColId, dao.RobotTblColCorpId, dao.RobotTblColQaVersion}
		apps, err = dao.GetRobotDao().GetAllValidApps(ctx, selectColumn, nil)
	}
	if err != nil {
		log.ErrorContextf(ctx, "获取机器人失败 err:%+v", err)
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(fmt.Sprintf("获取机器人失败 err:%+v", err)))
		return
	}
	for _, robot := range apps {
		log.InfoContextf(ctx, "corpID:%d 开始创建机器人:%d 发布向量库", robot.CorpID, robot.ID)
		if robot.QAVersion == 0 {
			log.InfoContextf(ctx, "corpID:%d 开始创建机器人:%d 版本:%d 无需创建发布向量库", robot.CorpID, robot.ID, robot.QAVersion)
			continue
		}
		if err = s.dao.IndexRebuild(ctx, robot.ID, robot.QAVersion); err != nil {
			log.ErrorContextf(ctx, "corpID:%d 创建机器人:%d 发布向量库失败 err:%+v", robot.CorpID, robot.ID, err)
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(fmt.Sprintf("corpID:%d 创建机器人:%d 发布向量库失败 err:%+v", robot.CorpID, robot.ID,
				err)))
			return
		}
		log.InfoContextf(ctx, "corpID:%d 创建机器人:%d 发布向量库成功", robot.CorpID, robot.ID)
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK"))
}

// CreateVectorIndex 脚本 用于存量企业创建向量库
func (s *Service) CreateVectorIndex(w http.ResponseWriter, r *http.Request) {
	log.ErrorContextf(r.Context(), "准备删除的接口收到了请求 deprecated interface req:%+v", r)

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	if strings.ToUpper(r.Method) != "POST" {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("only POST is allowed"))
		return
	}
	if config.App().RobotDefault.Embedding.Version != config.App().RobotDefault.Embedding.UpgradeVersion {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(fmt.Sprintf("err: embedding upgrading")))
		return
	}
	ctx := r.Context()
	corpID := r.PostFormValue("corp_id")
	var (
		apps []*model.AppDB
		err  error
	)
	if corpID != "" {
		apps, err = s.dao.GetAppsByCorpID(ctx, cast.ToUint64(corpID))
	} else {
		selectColumn := []string{dao.RobotTblColId, dao.RobotTblColCorpId,
			dao.RobotTblColIsCreateVectorIndex, dao.RobotTblColEmbedding}
		apps, err = dao.GetRobotDao().GetAllValidApps(ctx, selectColumn, nil)
	}
	if err != nil {
		log.ErrorContextf(ctx, "获取机器人失败 err:%+v", err)
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(fmt.Sprintf("获取机器人失败 err:%+v", err)))
		return
	}
	for _, app := range apps {
		log.InfoContextf(ctx, "corpID:%d 开始创建机器人:%d 向量库", app.CorpID, app.ID)
		if err = s.dao.CreateAppVectorIndex(ctx, app); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(fmt.Sprintf("corpID:%d 创建机器人:%d 向量库失败 err:%+v", app.CorpID, app.ID,
				err)))
			return
		}
		log.InfoContextf(ctx, "corpID:%d 创建机器人:%d 向量库成功", app.CorpID, app.ID)
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK"))
}

// CreateVector 创建用户相似、评测向量
func (s *Service) CreateVector(w http.ResponseWriter, r *http.Request) {
	log.ErrorContextf(r.Context(), "准备删除的接口收到了请求 deprecated interface req:%+v", r)

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	if strings.ToUpper(r.Method) != "POST" {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("only POST is allowed"))
		return
	}
	if config.App().RobotDefault.Embedding.Version != config.App().RobotDefault.Embedding.UpgradeVersion {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(fmt.Sprintf("err: embedding upgrading")))
		return
	}
	ctx := r.Context()
	corpID := cast.ToUint64(r.PostFormValue("corp_id"))
	// 评测库 & 相似库
	acceptStatus := []uint32{model.AcceptYes, model.AcceptInit}
	total, err := s.dao.CmdGetQAListCount(ctx, corpID, acceptStatus)
	if err != nil {
		log.ErrorContextf(ctx, "CreateVector CmdGetQAListCount fail corpID:%d err:%+v", corpID, err)
		return
	}
	pageSize := 500
	pages := int(math.Ceil(float64(total) / float64(pageSize)))
	g := errgroupx.Group{}
	g.SetLimit(10)
	for i := 1; i <= pages; i++ {
		page := i
		g.Go(func() error {
			qas, err := s.dao.CmdGetQAList(ctx, corpID, acceptStatus, uint32(page), uint32(pageSize))
			if err != nil {
				log.ErrorContextf(ctx, "CreateVector CmdGetQAList fail corpID:%d err:%+v", corpID, err)
				return err
			}
			for _, qa := range qas {
				err = s.dao.AddQAVector(ctx, qa)
				log.InfoContextf(ctx, "CreateVector 新增相似｜评测库 corpID:%d qaID:%d robotID:%d err:%+v",
					qa.CorpID, qa.ID, qa.RobotID, err)
			}
			return nil
		})
	}
	if err = g.Wait(); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(err.Error()))
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("success"))
}

// ReleaseDocRebuild 发布文档数据重建
func (s *Service) ReleaseDocRebuild(w http.ResponseWriter, r *http.Request) {
	log.ErrorContextf(r.Context(), "准备删除的接口收到了请求 deprecated interface req:%+v", r)

	// 文档状态、next_action、t_release_doc
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	if strings.ToUpper(r.Method) != "POST" {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("only POST is allowed"))
		return
	}
	if config.App().RobotDefault.Embedding.Version != config.App().RobotDefault.Embedding.UpgradeVersion {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(fmt.Sprintf("err: embedding upgrading")))
		return
	}
	ctx := r.Context()
	versionID := cast.ToUint64(r.PostFormValue("version_id"))
	if versionID == 0 {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("versionID is 0"))
		return
	}
	s.dao.ReleaseDocRebuild(ctx, versionID)
}

// DeleteCharSizeExceededTaskHandler 定时删除超量失效的文档与问答
func (s *Service) DeleteCharSizeExceededTaskHandler(ctx context.Context) error {
	deadLine, ok := ctx.Deadline()
	log.DebugContextf(ctx, "DeleteCharSizeExceededTaskHandler deadLine:%v ok:%v", deadLine, ok)
	selectColumn := []string{dao.RobotTblColId, dao.RobotTblColCorpId}
	apps, err := dao.GetRobotDao().GetAllValidApps(ctx, selectColumn, nil)
	if err != nil {
		log.ErrorContextf(ctx, "获取机器人失败 err:%+v", err)
		return err
	}
	// 2.6.2需求从30天改为180天
	// https://tapd.woa.com/tapd_fe/70080800/story/detail/1070080800119977397?from_iteration_id=1070080800002062621
	reserveTime := 180 * 24 * time.Hour
	for _, app := range apps {
		if err := s.dao.DeleteDocsCharSizeExceeded(ctx, app.CorpID, app.ID, reserveTime); err != nil {
			log.ErrorContextf(ctx, "删除除应用下超量失效的文档失败 %+v err:%+v", app.ID, err)
			return err
		}
		if err := s.dao.DeleteQAsCharSizeExceeded(ctx, app.CorpID, app.ID, reserveTime); err != nil {
			log.ErrorContextf(ctx, "删除除应用下超量失效的问答失败 %+v err:%+v", app.ID, err)
			return err
		}
	}
	return nil
}

// UpdateAttributeLabelsTaskPreview 定时刷新评测环境属性&标签缓存
func (s *Service) UpdateAttributeLabelsTaskPreview(ctx context.Context) error {
	return s.updateAttributeLabelsTask(ctx, model.AttributeLabelsPreview)
}

// UpdateAttributeLabelsTaskProd 定时刷新发布环境属性&标签缓存
func (s *Service) UpdateAttributeLabelsTaskProd(ctx context.Context) error {
	return s.updateAttributeLabelsTask(ctx, model.AttributeLabelsProd)
}

// updateAttributeLabelsTask 定时刷新属性&标签缓存
func (s *Service) updateAttributeLabelsTask(ctx context.Context, envType string) error {
	deadLine, ok := ctx.Deadline()
	log.DebugContextf(ctx, "UpdateAttributeLabelsTask env:%s deadLine:%v ok:%v", envType, deadLine, ok)
	var redisKey string
	if envType == model.AttributeLabelsPreview {
		redisKey = dao.UpdateAttributeLabelsTaskPreview
	} else {
		redisKey = dao.UpdateAttributeLabelsTaskProd
	}
	duration := time.Duration(12 * 60) // 12分钟
	err := s.dao.Lock(ctx, redisKey, duration)
	if errors.Is(err, errs.ErrAlreadyLocked) {
		return nil
	} else if err != nil {
		log.ErrorContextf(ctx, "UpdateAttributeLabelsTask env:%s, err:%+v", envType, err)
		return err
	}
	defer func() { _ = s.dao.UnLock(ctx, redisKey) }()
	appIDs, err := s.dao.GetAllValidAppIDs(ctx)
	if err != nil {
		log.ErrorContextf(ctx, "UpdateAttributeLabelsTask env:%s err:%+v", envType, err)
		return err
	}
	log.DebugContextf(ctx, "UpdateAttributeLabelsTask env:%s appID count:%v", envType, len(appIDs))
	for _, appID := range appIDs {
		var attrs []*model.AttributeKeyAndID // preview环境属性ID是"ID"字段；prod环境属性ID是"AttrID"字段
		var attrIDList []uint64
		if envType == model.AttributeLabelsPreview {
			attrs, err = s.dao.GetAttributeKeyAndIDsByRobotID(ctx, appID)
			for i, attr := range attrs {
				attrIDList = append(attrIDList, attr.ID)
				attrs[i].AttrID = attr.ID // 用ID覆盖AttrID，后续都用AttrID
			}
		} else {
			attrs, err = s.dao.GetAttributeKeyAndIDsByRobotIDProd(ctx, appID)
			for _, attr := range attrs {
				attrIDList = append(attrIDList, attr.AttrID)
			}
		}
		if err != nil {
			log.ErrorContextf(ctx, "UpdateAttributeLabelsTask env:%s appID:%d err:%+v", envType, appID, err)
			return err
		}
		if len(attrIDList) == 0 {
			continue
		}

		mapAttr2Labels := make(map[uint64][]*model.AttributeLabel)
		if envType == model.AttributeLabelsPreview {
			notEmptySimilarLabel := true
			filter := &dao.AttributeLabelFilter{
				RobotId:              appID,
				AttrIds:              attrIDList,
				NotEmptySimilarLabel: &notEmptySimilarLabel,
			}
			selectColumns := []string{dao.AttributeLabelTblColId, dao.AttributeLabelTblColBusinessId,
				dao.AttributeLabelTblColAttrId, dao.AttributeLabelTblColName, dao.AttributeLabelTblColSimilarLabel}
			attrLabels, err := dao.GetAttributeLabelDao().GetAttributeLabelList(ctx, selectColumns, filter)
			if err != nil {
				return err
			}
			for _, attrLabel := range attrLabels {
				if _, ok := mapAttr2Labels[attrLabel.AttrID]; !ok {
					mapAttr2Labels[attrLabel.AttrID] = make([]*model.AttributeLabel, 0)
				}
				mapAttr2Labels[attrLabel.AttrID] = append(mapAttr2Labels[attrLabel.AttrID], attrLabel)
			}
		} else {
			mapAttr2Labels, err = s.dao.GetAttributeLabelByAttrIDsWithNotEmptySimilarLabelProd(ctx, attrIDList, appID)
			if err != nil {
				return err
			}
		}
		if len(mapAttr2Labels) == 0 {
			continue
		}
		attrKey2RedisValue := make(map[string][]model.AttributeLabelRedisValue)
		for attrID, Labels := range mapAttr2Labels {
			var redisValue []model.AttributeLabelRedisValue
			for _, l := range Labels {
				redisValue = append(redisValue, model.AttributeLabelRedisValue{
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
		err = s.dao.PiplineSetAttributeLabelRedis(ctx, appID, attrKey2RedisValue, envType)
		if err != nil {
			log.ErrorContextf(ctx, "UpdateAttributeLabelsTask env:%s appID:%d attrKey2RedisValue:%v err:%+v",
				envType, appID, attrKey2RedisValue, err)
			return err
		}
	}
	return nil
}

// GetTaskStatus 查询任务状态
func (s *Service) GetTaskStatus(ctx context.Context, req *pb.GetTaskStatusReq) (
	*pb.GetTaskStatusRsp, error) {
	log.DebugContextf(ctx, "GetTaskStatus req:%+v", req)
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
			log.ErrorContextf(ctx, "getModifyAttributeLabel err:%v", err)
			return rsp, err
		}
	case pb.TaskType_ExportAttributeLabel.String():
		rsp, err = s.getExportAttributeLabel(ctx, req)
		if err != nil {
			log.ErrorContextf(ctx, "getExportAttributeLabel err:%v", err)
			return rsp, err
		}
	}
	return rsp, nil
}

// getExportAttributeLabel 查询导出标签状态
func (s *Service) getExportAttributeLabel(ctx context.Context, req *pb.GetTaskStatusReq) (*pb.GetTaskStatusRsp, error) {
	rsp := new(pb.GetTaskStatusRsp)
	corpID := pkg.CorpID(ctx)
	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetBotBizId())
	if err != nil {
		return nil, err
	}

	app, err := s.getAppByAppBizID(ctx, botBizID)
	if err != nil {
		return rsp, errs.ErrSystem
	}
	taskID, err := strconv.ParseUint(req.GetTaskId(), 10, 64)
	if err != nil {
		log.ErrorContextf(ctx, "taskID转换失败 req:%v", req)
		return rsp, errs.ErrSystem
	}
	exportInfo, err := s.dao.GetExportTaskInfo(ctx, taskID, corpID, app.ID)
	if err != nil {
		log.ErrorContextf(ctx, "GetExportTaskInfo err:%v", err)
		return nil, err
	}
	if exportInfo == nil {
		log.ErrorContextf(ctx, "getExportAttributeLabel 导出标签任务不存在 req:%+v", req)
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
	corpID := pkg.CorpID(ctx)
	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetBotBizId())
	if err != nil {
		return nil, err
	}
	app, err := s.getAppByAppBizID(ctx, botBizID)
	if err != nil {
		return rsp, errs.ErrSystem
	}
	taskID, err := strconv.ParseUint(req.TaskId, 10, 64)
	if err != nil {
		log.ErrorContextf(ctx, "taskID转换失败:%v", req)
		return rsp, errs.ErrParams
	}
	labelTask, err := s.dao.GetUpdateAttributeTask(ctx, taskID, corpID, app.ID)
	if err != nil {
		log.ErrorContextf(ctx, "GetModifyUpdateAttributeTask err:%v", err)
		return rsp, err
	}
	if labelTask == nil {
		log.ErrorContextf(ctx, "getModifyAttributeLabel 编辑标签任务不存在 req:%+v", req)
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

// TMsgDataCount 统计前一小时t_msg_record消息数据
func (s *Service) TMsgDataCount(ctx context.Context) error {
	log.InfoContextf(ctx, "CountTMsgData dataCount lock")
	lockKey := fmt.Sprintf(dao.LockTMsgDataCount, "TMsgDataCount")
	if err := s.dao.Lock(ctx, lockKey, 10*time.Second); err != nil {
		log.InfoContextf(ctx, "CountTMsgData dataCount has locked")
		return nil
	}
	defer func() { _ = s.dao.UnLock(ctx, lockKey) }()
	log.InfoContextf(ctx, "CountTMsgData dataCount start")
	startTime := time.Now()
	// 获取前一小时统计数据
	likeDataList, err := s.dao.LikeDataCount(ctx)
	if err != nil {
		log.ErrorContextf(ctx, "CountTMsgData dataCount LikeDataCount select error|%v", err)
		return err
	}
	log.InfoContextf(ctx, "CountTMsgData dataCount LikeDataCount success len|%", len(likeDataList))
	if len(likeDataList) > 0 {
		for i := range likeDataList {
			if !likeDataList[i].Day.IsZero() {
				likeDataList[i].FormatDay = likeDataList[i].Day.Format("2006-01-02")
			}
		}
		err = s.dao.UpdateLikeDataCount(ctx, likeDataList)
		if err != nil {
			log.ErrorContextf(ctx, "CountTMsgData dataCount UpdateLikeDataCount update error|%v", err)
			return err
		}
		log.InfoContextf(ctx, "CountTMsgData dataCount UpdateLikeDataCount success")
	}
	log.InfoContextf(ctx, "CountTMsgData dataCount LikeDataCount end")

	// 获取前一小时统计数据
	answerTypeDataList, err := s.dao.AnswerTypeDataCount(ctx)
	if err != nil {
		log.ErrorContextf(ctx, "CountTMsgData dataCount AnswerTypeDataCount select error|%v", err)
		return err
	}
	log.InfoContextf(ctx, "CountTMsgData dataCount AnswerTypeDataCount success len|%", len(answerTypeDataList))
	if len(answerTypeDataList) > 0 {
		for i := range answerTypeDataList {
			if !answerTypeDataList[i].Day.IsZero() {
				answerTypeDataList[i].FormatDay = answerTypeDataList[i].Day.Format("2006-01-02")
			}
		}
		err = s.dao.UpdateAnswerTypeDataCount(ctx, answerTypeDataList)
		if err != nil {
			log.ErrorContextf(ctx, "CountTMsgData dataCount UpdateAnswerTypeDataCount update error|%v", err)
			return err
		}
		log.InfoContextf(ctx, "CountTMsgData dataCount UpdateAnswerTypeDataCount success")
	}
	endTime := time.Now()
	// 计算执行时间
	duration := endTime.Sub(startTime)
	log.InfoContextf(ctx, "CountTMsgData dataCount end startTime|%s endTime|%s duration|%s",
		startTime.Format("2006-01-02 15:04:05"), endTime.Format("2006-01-02 15:04:05"), duration)
	return nil
}

// CleanVectorSyncHistory 清除t_vector_sync_history表数据
func (s *Service) CleanVectorSyncHistory(ctx context.Context) error {
	var err error

	log.InfoContextf(ctx, "CleanVectorSyncHistory lock")
	lockKey := fmt.Sprintf(dao.LockCleanVectorSyncHistory, "CleanVectorSyncHistory")
	if err := s.dao.Lock(ctx, lockKey, 10*time.Minute); err != nil {
		log.InfoContextf(ctx, "CleanVectorSyncHistory has locked")
		return nil
	}
	defer func() { _ = s.dao.UnLock(ctx, lockKey) }()
	log.Infof("CleanVectorSyncHistory run,time:%s", time.Now().Format("2006-01-02 15:04:05"))

	// vector := vector.SyncVector{}
	cutooffTime := time.Now().AddDate(0, -3, 0) // 2个月之前的时间
	duration := config2.GetMainConfig().CleanVectorSyncHistoryConfig.DeleteDuration
	if duration == 0 {
		duration = 60
	}

	limitSize := config2.GetMainConfig().CleanVectorSyncHistoryConfig.Limit
	if limitSize == 0 {
		limitSize = 1000
	}

	vector := vector.NewVectorSync(s.GetDB(), s.GetTdSqlDB())

	var rowsAffected = limitSize
	for rowsAffected >= limitSize {
		rowsAffected, err = vector.DeleteVectorSyncHistory(ctx, cutooffTime, limitSize)
		if err != nil {
			log.WarnContextf(ctx, "CleanVectorSyncHistory Failed! result:%+v", err)
			return err
		}
		log.InfoContextf(ctx, "CleanVectorSyncHistory batchSuccess, rowsAffected:%d,limit：%+v，time:%+v", rowsAffected, limitSize, time.Now().Format("2006-01-02 15:04:05"))
		// sleep一下，避免锁死了数据库
		time.Sleep(time.Duration(duration) * time.Millisecond)
	}

	log.Infof("CleanVectorSyncHistory sccess!,time:%s", time.Now().Format("2006-01-02 15:04:05"))

	return nil
}

// AutoDocRefresh 文档刷新任务
func (s *Service) AutoDocRefresh(ctx context.Context) error {
	log.InfoContextf(ctx, "AutoDocRefresh lock")
	startTime := time.Now()
	if !config2.GetMainConfig().AutoDocRefreshConfig.Enable {
		// 文档刷新任务被关闭
		log.DebugContextf(ctx, "AutoDocRefresh is not enable")
		return nil
	}
	ctx = pkg.WithEnvSet(ctx, config2.GetMainConfig().AutoDocRefreshConfig.EnvName)
	log.InfoContextf(ctx, "AutoDocRefresh getEnvSet:%s", metadata.Metadata(ctx).EnvSet())

	lockKey := fmt.Sprintf(dao.LockAutoDocRefresh, "AutoDocRefresh")
	if err := s.dao.Lock(ctx, lockKey, 360*time.Minute); err != nil {
		log.InfoContextf(ctx, "AutoDocRefresh has locked")
		return nil
	}
	defer func() { _ = s.dao.UnLock(ctx, lockKey) }()
	log.InfoContextf(ctx, "AutoDocRefresh run,time:%s", time.Now().Format("2006-01-02 15:04:05"))

	now := time.Now()
	t := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.Local)
	log.DebugContextf(ctx, "AutoDocRefresh GetTxDocAutoRefreshList time:%+v", t)
	docs, err := dao.GetDocDao().GetTxDocAutoRefreshList(ctx, t)
	if err != nil {
		log.ErrorContextf(ctx, "AutoDocRefresh GetTxDocAutoRefreshList err:%v", err)
		return err
	}
	log.DebugContextf(ctx, "AutoDocRefresh GetTxDocAutoRefreshList len(docs):%+d", len(docs))

	if len(docs) == 0 {
		return nil
	}

	// 按RobotID分组文档
	docsByRobotID := make(map[uint64][]*model.Doc)
	for _, doc := range docs {
		docsByRobotID[doc.RobotID] = append(docsByRobotID[doc.RobotID], doc)
	}

	batchSize := config2.GetAutoDocRefreshBatchSize() // 每个异步任务处理的文档数
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
				log.WarnContextf(ctx, "应用ID:%d 获取到空批次文档，跳过处理", robotID)
				continue
			}
			for _, doc := range batch {
				// 计算下次更新时间
				nextUpdateTime := logicDoc.GetDocNextUpdateTime(ctx, doc.UpdatePeriodH)
				// 不同更新频率的文档，下次执行时间不同
				nextUpdateTimeDocList[nextUpdateTime] = append(nextUpdateTimeDocList[nextUpdateTime], doc.ID)
			}

			log.InfoContextf(ctx, "应用ID:%d 正在处理第%d-%d条文档(共%d条)",
				robotID, i+1, end, total)
			if err := logicDoc.RefreshTxDoc(ctx, true, batch, s.dao); err != nil {
				log.ErrorContextf(ctx, "应用ID:%d 刷新文档失败(第%d-%d条):%v",
					robotID, i+1, end, err)
				failCount += len(batch)
				// 柔性放过，更新下次执行时间，等待下次执行
			}
			successCount += len(batch)
			// 短暂休眠避免压力过大
			//time.Sleep(100 * time.Millisecond)
		}

		// 不同的下次执行时间,分开更新
		for nextUpdateTime, updateDocIDs := range nextUpdateTimeDocList {
			updateDocFilter := &dao.DocFilter{
				IDs:     updateDocIDs,
				CorpId:  updateCorpID,
				RobotId: robotID,
			}
			doc := model.Doc{}
			doc.NextUpdateTime = nextUpdateTime
			updateDocColumns := []string{
				dao.DocTblColNextUpdateTime}
			_, err = dao.GetDocDao().UpdateDoc(ctx, updateDocColumns, updateDocFilter, &doc)
			if err != nil {
				log.ErrorContextf(ctx, "腾讯文档自动刷新,更新下次执行时间错误 nextUpdateTime:%v,docIDs:%+v err: %+v",
					nextUpdateTime, updateDocIDs, err)
				// 不影响后续待执行的文档
				continue
			}
		}
	}
	duration := time.Since(startTime)
	log.InfoContextf(ctx, "AutoDocRefresh completed, success:%d, failed:%d, duration:%s,nowTime:%s",
		successCount, failCount, duration, time.Now().Format("2006-01-02 15:04:05"))
	return nil
}
