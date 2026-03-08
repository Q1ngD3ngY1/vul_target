package api

import (
	"context"
	"errors"
	"fmt"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/i18nkey"
	"strconv"
	"time"

	jsoniter "github.com/json-iterator/go"

	"git.code.oa.com/trpc-go/trpc-database/redis"
	"git.code.oa.com/trpc-go/trpc-go"
	terrs "git.code.oa.com/trpc-go/trpc-go/errs"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/baicaoyuan/moss/metadata"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	logicDoc "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/doc"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/service"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	pb1 "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/knowledge"
)

// FileParserCallback 解析结果回调
func (s *Service) FileParserCallback(ctx context.Context, req *pb.FileParserCallbackReq) (
	*pb.FileParserCallbackRes, error) {
	log.DebugContextf(ctx, "解析结果回调 req:%+v", req)
	rsp := new(pb.FileParserCallbackRes)
	currentOpType := req.GetCurrentOpType()
	docParse, err := s.dao.GetDocParseByTaskIDAndOpType(ctx, req.GetTaskId(), int32(currentOpType))
	if err != nil {
		if errors.Is(err, errs.ErrDocParseTaskNotFound) {
			log.DebugContextf(ctx, "解析结果回调响应 req:%+v, rsp:%+v,info:%+v", req, rsp, err)
			return successFileParserCallbackRsp()
		}
		rsp.StatusCode = model.DocParseResultCallBackFail
		rsp.Message = terrs.Msg(errs.ErrDocParseTaskNotFound)
		log.DebugContextf(ctx, "解析结果回调响应 req:%+v, rsp:%+v", req, rsp)
		return rsp, errs.ErrDocParseTaskNotFound
	}
	result, err := jsoniter.MarshalToString(req)
	if err != nil {
		rsp.StatusCode = model.DocParseResultCallBackFail
		rsp.Message = terrs.Msg(errs.ErrParams)
		log.ErrorContextf(ctx, "解析结果回调响应 MarshalToString err:%+v, req:%+v,  rsp:%+v, result:%+v",
			errs.ErrParams, req, rsp, result)
		return rsp, errs.ErrParams
	}
	docParse.Result = result
	app, doc, err := s.getRobotAndDoc(ctx, docParse)
	if err != nil {
		if errors.Is(err, errs.ErrRobotNotFound) || errors.Is(err, errs.ErrRobotOrDocNotFound) ||
			errors.Is(err, errs.ErrDocNotFound) {
			log.DebugContextf(ctx, "解析结果回调响应 req:%+v, rsp:%+v,info:%+v", req, rsp, err)
			return successFileParserCallbackRsp()
		}
		rsp.StatusCode = model.DocParseResultCallBackFail
		rsp.Message = terrs.Msg(errs.ErrRobotOrDocNotFound)
		log.DebugContextf(ctx, "解析结果回调响应 req:%+v, rsp:%+v", req, rsp)
		return rsp, errs.ErrRobotOrDocNotFound
	}
	if err = app.IsWriteable(); err != nil {
		rsp.StatusCode = model.DocParseResultCallBackFail
		rsp.Message = terrs.Msg(err)
		return rsp, err
	}

	if req.Status != model.DocParseCallBackFinish {
		doc.Message = s.getDocMessage(req)
	}
	if currentOpType == model.DocParseOpTypeWordCount {
		// for https://tapd.woa.com/tapd_fe/70080800/story/detail/1070080800118931920
		// 干预文档解析/拆分结果，第一次FileParserCallback时，用新文档cos信息覆盖旧文档的cos信息，给下游传递新的cos信息
		botBizID := strconv.FormatUint(app.AppDB.BusinessID, 10)
		interventionType := uint32(0)
		// 获取op干预存储信息
		doc1, opRedisValue, err1 := s.getOldDocCosHashToNewDocRedisValue(ctx, doc.CorpID, botBizID, doc.CosHash)
		// op干预替换文档链接
		if err1 == nil && doc1 != nil && opRedisValue != nil {
			if opRedisValue.InterventionType == model.InterventionTypeOP {
				doc.CosURL = doc1.CosURL
				doc.FileName = doc1.FileName
				doc.FileType = doc1.FileType
				doc.CosHash = doc1.CosHash
				doc.FileSize = doc1.FileSize
			}
		}
		// 获取非op干预存储信息
		doc2, redisValue, err2 := s.dao.GetInterveneOldDocCosHashToNewDocRedisValue(ctx, doc.CorpID,
			app.AppDB.BusinessID, doc.BusinessID, doc.CosHash)
		if err2 == nil && doc2 != nil && redisValue != nil && (redisValue.InterventionType ==
			model.InterventionTypeOrgData || redisValue.InterventionType == model.InterventionTypeSheet) {
			interventionType = redisValue.InterventionType
		}
		err = s.parseWordCount(ctx, req, doc, app, docParse, interventionType)
		if err != nil {
			rsp.StatusCode = model.DocParseResultCallBackFail
			rsp.Message = terrs.Msg(errs.ErrDocParseResultCallBackErr)
			log.ErrorContextf(ctx, "解析结果回调响应 req:%+v, rsp:%+v, err:%+v", req, rsp, err)
			return rsp, errs.ErrDocParseResultCallBackErr
		}
		rsp, err = successFileParserCallbackRsp()
		log.DebugContextf(ctx, "解析结果回调 成功 docParse.Type:%d rsp:%+v", docParse.Type, rsp)
		return rsp, nil
	}

	if currentOpType == model.DocParseOpTypeSplit && docParse.Type == model.DocParseTaskTypeSplitSegment {
		// for https://tapd.woa.com/tapd_fe/70080800/story/detail/1070080800118931920
		// 干预文档解析/拆分结果，第二次FileParserCallback时，删除redis数据(数据库中的数据一直都为原始数据)
		botBizID := strconv.FormatUint(app.AppDB.BusinessID, 10)
		var originDocBizID uint64
		// 对于非op干预需获取一次原始id，用于判断是否为干预文档
		doc1, redisValue, err1 := s.dao.GetInterveneOldDocCosHashToNewDocRedisValue(ctx, doc.CorpID,
			app.AppDB.BusinessID, doc.BusinessID, doc.CosHash)
		if err1 == nil && doc1 != nil && redisValue != nil {
			if redisValue.InterventionType == model.InterventionTypeOrgData ||
				redisValue.InterventionType == model.InterventionTypeSheet {
				originDocBizID = redisValue.OriginDocBizID
			}
		}
		// 删除op干预存储的redis信息
		_ = s.delOldDocCosHashToNewDocRedisValue(ctx, doc.CorpID, botBizID, doc.CosHash)
		// 删除非op干预存储的redis信息
		_ = s.dao.DeleteInterveneOldDocCosHashToNewDocRedisValue(ctx, doc.CorpID,
			app.AppDB.BusinessID, doc.BusinessID, doc.CosHash)
		err = s.splitSegment(ctx, req, doc, docParse, originDocBizID)
		if err != nil {
			rsp.StatusCode = model.DocParseResultCallBackFail
			rsp.Message = terrs.Msg(err)
			log.DebugContextf(ctx, "解析结果回调响应 req:%+v, rsp:%+v", req, rsp)
			return rsp, errs.ErrDocParseResultCallBackErr
		}
		log.DebugContextf(ctx, "解析结果回调 成功 docParse.Type:%d rsp:%+v", docParse.Type, rsp)
		return successFileParserCallbackRsp()
	}

	if currentOpType == model.DocParseOpTypeSplit && docParse.Type == model.DocParseTaskTypeSplitQA {
		err = s.splitQa(ctx, req, doc, docParse, app.BusinessID)
		if err != nil {
			rsp.StatusCode = model.DocParseResultCallBackFail
			rsp.Message = terrs.Msg(err)
			log.DebugContextf(ctx, "解析结果回调响应 req:%+v, rsp:%+v", req, rsp)
			return rsp, err
		}
		log.DebugContextf(ctx, "解析结果回调 成功 docParse.Type:%d rsp:%+v", docParse.Type, rsp)
		return successFileParserCallbackRsp()
	}
	log.DebugContextf(ctx, "解析结果回调完成 docParse.Type:%d req:%+v, rsp:%+v", docParse.Type, req, rsp)
	return rsp, nil
}

// successFileParserCallbackRsp 解析任务成功或不需要重试回包
func successFileParserCallbackRsp() (*pb.FileParserCallbackRes, error) {
	rsp := new(pb.FileParserCallbackRes)
	rsp.StatusCode = model.DocParseResultCallBackSuccess
	rsp.Message = "success"
	return rsp, nil
}

func (s *Service) getDocMessage(req *pb.FileParserCallbackReq) string {
	if conf, ok := config.App().DocParseError[req.ErrorCode]; ok {
		return conf.Msg
	}
	return config.App().DocParseErrorDefault.Msg
}

func (s *Service) parseWordCount(ctx context.Context, req *pb.FileParserCallbackReq, doc *model.Doc, app *model.App,
	docParse model.DocParse, interventionType uint32) error {
	var err error
	var event string
	isNeedNotice := false
	if req.Status != model.DocParseCallBackFinish {
		// 解析失败
		event = model.EventProcessFailed
		docParse.Status = model.DocParseCallBackFailed
	} else {
		// 解析成功
		event = model.EventProcessSuccess
		docParse.Status = model.DocParseCallBackFinish
		if err = service.CheckIsCharSizeExceeded(ctx, s.dao, app.BusinessID, app.CorpID, int64(req.GetTextLength())); err != nil {
			// 字符超限
			event = model.EventUsedCharSizeExceeded
			doc.Message = terrs.Msg(err)
			docParse.Status = model.DocParseCallBackCharSizeExceeded
			isNeedNotice = true
		} else {
			// 字符未超限
			if config.App().AuditSwitch {
				switch {
				case interventionType == model.InterventionTypeOrgData || interventionType == model.InterventionTypeSheet:
					if err = s.dao.CreateInterveneDocAudit(ctx, doc, interventionType, docParse.SourceEnvSet); err != nil {
						return errs.ErrDocParseResultCallBackErr
					}
				case !doc.NeedAudit():
					event = model.EventCloseAudit
					s.asyncParseDoc(trpc.CloneContext(ctx), doc, interventionType)
				default:
					if err = s.dao.CreateDocAudit(ctx, doc, docParse.SourceEnvSet); err != nil {
						return errs.ErrDocParseResultCallBackErr
					}
				}
			} else {
				event = model.EventCloseAudit
				s.asyncParseDoc(trpc.CloneContext(ctx), doc, interventionType)
			}
		}
	}
	// 更新文档状态和字符数等信息
	doc.CharSize = uint64(req.GetTextLength())
	doc.AuditFlag = model.AuditFlagWait
	docFilter := &dao.DocFilter{
		IDs:     []uint64{doc.ID},
		RobotId: doc.RobotID,
	}
	updateCols := []string{dao.DocTblColCharSize, dao.DocTblColMessage, dao.DocTblColAuditFlag, dao.DocTblColStatus}
	err = logicDoc.UpdateDoc(ctx, updateCols, docFilter, doc, event)
	if err != nil {
		return errs.ErrUpdateDocStatusAndCharSizeFail
	}
	if doc.CharSize != 0 {
		if err := s.dao.UpdateAppUsedCharSizeNotTx(ctx, int64(doc.CharSize), doc.RobotID); err != nil {
			return errs.ErrUpdateRobotUsedCharSizeFail
		}
	}
	err = s.dao.UpdateDocParseTask(ctx, docParse)
	if err != nil {
		return errs.ErrUpdateDocParseTaskStatusFail
	}
	if isNeedNotice {
		if err = s.failNotice(ctx, doc); err != nil {
			return err
		}
	}
	log.DebugContextf(ctx, "parseWordCount: update doc status and char size succeeded, doc:%+v", doc)
	return err
}

func (s *Service) failNotice(ctx context.Context, doc *model.Doc) error {
	log.DebugContextf(ctx, "failNotice , doc: %+v", doc)
	operations := []model.Operation{{Typ: model.OpTypeViewDetail, Params: model.OpParams{}}}
	noticeOptions := []model.NoticeOption{
		model.WithGlobalFlag(),
		model.WithPageID(model.NoticeDocPageID),
		model.WithLevel(model.LevelError),
		model.WithSubject(i18n.Translate(ctx, i18nkey.KeyDocumentImportFailure)),
		model.WithContent(i18n.Translate(ctx, i18nkey.KeyDocumentImportFailureExceedCharLimitWithName, doc.FileName)),
	}
	notice := model.NewNotice(model.NoticeTypeDocToQA, doc.ID, doc.CorpID, doc.RobotID, doc.StaffID, noticeOptions...)
	if err := notice.SetOperation(operations); err != nil {
		log.ErrorContextf(ctx, "序列化通知操作参数失败 operations:%+v err:%+v", operations, err)
		return err
	}
	if err := s.dao.CreateNotice(ctx, notice); err != nil {
		log.ErrorContextf(ctx, "CreateNotice err:%+v err:%+v", notice, err)
		return err
	}
	return nil
}

func (s *Service) splitQa(ctx context.Context, req *pb.FileParserCallbackReq, doc *model.Doc,
	docParse model.DocParse, appBizID uint64) error {
	if req.Status == model.DocParseCallBackFinish {
		docParse.Status = model.DocParseCallBackFinish
	} else {
		docParse.Status = model.DocParseCallBackFailed
	}
	if err := s.dao.UpdateDocParseTask(ctx, docParse); err != nil {
		return errs.ErrUpdateDocParseTaskStatusFail
	}
	if req.Status == model.DocParseCallBackFinish {
		if _, err := s.dao.CreateDocToQATask(ctx, doc, nil, appBizID); err != nil {
			return errs.ErrCreateDocToQATaskFail
		}
	}
	return nil
}

func (s *Service) splitSegment(ctx context.Context, req *pb.FileParserCallbackReq, doc *model.Doc,
	docParse model.DocParse, originDocBizID uint64) error {
	if req.Status == model.DocParseCallBackFinish {
		docParse.Status = model.DocParseCallBackFinish
	} else {
		docParse.Status = model.DocParseCallBackFailed
		// 更新文档状态
		event := model.EventProcessFailed
		docFilter := &dao.DocFilter{
			RobotId: doc.RobotID,
			IDs:     []uint64{doc.ID},
		}
		updateCols := []string{dao.DocTblColStatus}
		err := logicDoc.UpdateDoc(ctx, updateCols, docFilter, doc, event)
		if err != nil {
			return errs.ErrUpdateDocStatusFail
		}
	}

	err := s.dao.UpdateDocParseTask(ctx, docParse)
	if err != nil {
		return errs.ErrUpdateDocParseTaskStatusFail
	}
	if req.Status == model.DocParseCallBackFinish {
		if err := s.dao.CreateDocToIndexTask(ctx, doc, originDocBizID); err != nil {
			return errs.ErrCreateDocToIndexTaskFail
		}
	}
	return nil
}

func (s *Service) getRobotAndDoc(ctx context.Context, docParse model.DocParse) (*model.App, *model.Doc, error) {
	appDB, err := s.dao.GetAppByID(ctx, docParse.RobotID)
	if err != nil {
		return nil, nil, err
	}
	if appDB == nil {
		return nil, nil, errs.ErrRobotNotFound
	}
	app, err := appDB.ToApp()
	if err != nil {
		return nil, nil, errs.ErrRobotOrDocNotFound
	}

	doc, err := s.dao.GetDocByID(ctx, docParse.DocID, app.ID)
	if err != nil {
		return nil, nil, errs.ErrDocNotFound
	}
	return app, doc, nil
}

// DocParsingIntervention 干预文档解析/拆分结果
func (s *Service) DocParsingIntervention(ctx context.Context, req *pb1.DocParsingInterventionReq) (
	*pb1.DocParsingInterventionRsp, error) {
	// for https://tapd.woa.com/tapd_fe/70080800/story/detail/1070080800118931920  干预文档解析/拆分结果
	log.InfoContextf(ctx, "DocParsingIntervention, env-set:%s, req: %+v", metadata.Metadata(ctx).EnvSet(), req)
	rsp := new(pb1.DocParsingInterventionRsp)
	adminService := service.New()
	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetBotBizId())
	if err != nil {
		return nil, err
	}
	app, err := s.getAppByAppBizID(ctx, botBizID)
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	// 1. 获取旧文档数据
	oldDoc, attlLabelList, isFromRedis, err := s.getOldDoc(ctx, req, app.ID)
	if err != nil {
		log.ErrorContextf(ctx, "DocParsingIntervention: get old doc failed, %+v", err)
		return rsp, err
	}
	log.InfoContextf(ctx, "DocParsingIntervention: get old doc succeeded, ifFromRedis:%v, oldDoc: %+v, "+
		"attlLabelList:%+v", isFromRedis, oldDoc, attlLabelList)
	var sid int // 集成商id
	if sid, err = s.dao.GetSidByCorpID(ctx, oldDoc.CorpID); err != nil {
		log.ErrorContextf(ctx, "DocParsingIntervention: get sid failed, %+v", err)
		return rsp, err
	}
	ctx = pkg.WithStaffID(ctx, oldDoc.StaffID)
	ctx = pkg.WithCorpID(ctx, oldDoc.CorpID)
	ctx = pkg.WithSID(ctx, uint64(sid))
	if !isFromRedis { // 旧文档不在redis中
		// 2. 把旧文档的数据保存到redis，以便重试时找回
		if err = s.setOldDocParsingInterventionRedisValue(ctx, req, oldDoc, attlLabelList); err != nil {
			return rsp, err
		}
	}
	// 3. 把新文档的coshash  url，文件名，文件类型，保存到redis，redis的key是旧文档的coshash
	if err = s.setOldDocCosHashToNewDocRedisValue(ctx, req, oldDoc); err != nil {
		return rsp, err
	}
	// 4. 删除旧文档
	delDeq := &pb1.DeleteDocReq{
		Ids:      []string{strconv.FormatUint(req.GetOriginDocId(), 10)},
		BotBizId: req.GetBotBizId(),
	}
	_, err = adminService.DeleteDoc(ctx, delDeq)
	if err != nil {
		log.ErrorContextf(ctx, "DocParsingIntervention: delete old doc failed, %+v", err)
		return rsp, err
	}
	log.InfoContextf(ctx, "DocParsingIntervention: delete old doc succeeded.")
	// 5. 创建新文档
	saveRsp, err := s.saveNewDoc(ctx, adminService, req, oldDoc, attlLabelList)
	if err != nil {
		log.ErrorContextf(ctx, "DocParsingIntervention: save new doc failed, %+v", err)
		return rsp, err
	}
	log.InfoContextf(ctx, "DocParsingIntervention: save new doc succeeded.")
	// 6. 返回新文档id
	newDocID, err := s.dao.GetDocIDByBusinessID(ctx, saveRsp.GetDocBizId(), app.ID)
	if err != nil {
		log.ErrorContextf(ctx, "DocParsingIntervention: get new doc id failed, %+v", err)
		return rsp, err
	}
	rsp.OriginDocId = oldDoc.ID
	rsp.DocId = newDocID
	// 7. 删除旧文档redis数据，非关键路径，不报错
	_ = s.delOldDocParsingInterventionRedisValue(ctx, req, oldDoc)
	log.InfoContextf(ctx, "DocParsingIntervention: all succeeded.")
	return rsp, nil
}

func (s *Service) getOldDoc(ctx context.Context, req *pb1.DocParsingInterventionReq, robotID uint64) (
	*model.Doc, []*model.AttrLabel, bool, error) {
	// 1. 获取redis数据，如果redis中存在数据，说明是重试场景
	redisKey := oldDocParsingInterventionRedisKey(req.GetBotBizId(), req.GetOriginDocId())
	redisValue, err := redis.String(s.dao.RedisCli().Do(ctx, "GET", redisKey))
	if err == nil {
		if redisValue == "" {
			log.ErrorContextf(ctx, "DocParsingIntervention: redis value is empty")
			return nil, nil, false, fmt.Errorf("redis value is empty")
		}
		var redisValueDoc = &model.DocParsingInterventionRedisValue{}
		if err1 := jsoniter.UnmarshalFromString(redisValue, redisValueDoc); err1 != nil {
			log.ErrorContextf(ctx, "DocParsingIntervention: unmarshal redis value failed, %+v", err1)
			return nil, nil, false, err1
		}
		if redisValueDoc.OldDoc == nil {
			log.ErrorContextf(ctx, "DocParsingIntervention: redisValueDoc.OldDoc is nil")
			return nil, nil, false, errs.ErrDocNotFound
		}
		for _, v := range redisValueDoc.AttrLabels {
			if v == nil {
				log.ErrorContextf(ctx, "DocParsingIntervention: redisValueDoc.AttrLabels has nil member")
				return nil, nil, false, errs.ErrAttributeLabelNotFound
			}
		}
		// 校验是否有权限操作文档
		if err = verifyDocParsingInterventionPermission(ctx, req, s.dao, redisValueDoc.OldDoc); err != nil {
			log.ErrorContextf(ctx, "DocParsingIntervention: verify permission failed, %+v", err)
			return nil, nil, false, err
		}
		return redisValueDoc.OldDoc, redisValueDoc.AttrLabels, true, nil
	} else if !errors.Is(err, redis.ErrNil) { // 非key不存在错误
		log.ErrorContextf(ctx, "DocParsingIntervention: get redis value failed, %+v", err)
		return nil, nil, false, err
	}

	// redis key不存在，从数据库获取
	// 2. 获取旧文档
	oldDoc, err := s.dao.GetDocByID(ctx, req.GetOriginDocId(), robotID)
	if err != nil {
		log.ErrorContextf(ctx, "DocParsingIntervention: get old doc failed, %+v", err)
		return nil, nil, false, err
	}
	if oldDoc == nil {
		log.ErrorContextf(ctx, "DocParsingIntervention: oldDoc is nil")
		return nil, nil, false, errs.ErrDocNotFound
	}

	// 3. 校验是否有权限操作文档
	if err = verifyDocParsingInterventionPermission(ctx, req, s.dao, oldDoc); err != nil {
		log.ErrorContextf(ctx, "DocParsingIntervention: verify permission failed, %+v", err)
		return nil, nil, false, err
	}
	// 4. 获取旧文档的属性&标签
	mapAttrLabel, err := s.dao.GetDocAttributeLabelDetail(ctx, oldDoc.RobotID, []uint64{oldDoc.ID})
	if err != nil {
		log.ErrorContextf(ctx, "DocParsingIntervention: get old doc attribute&label failed, %+v", err)
		return nil, nil, false, err
	}
	for _, v := range mapAttrLabel[oldDoc.ID] {
		if v == nil {
			log.ErrorContextf(ctx, "DocParsingIntervention: AttrLabels has nil member")
			return nil, nil, false, errs.ErrAttributeLabelNotFound
		}
	}
	return oldDoc, mapAttrLabel[oldDoc.ID], false, nil
}

// 校验是否有权限操作文档
func verifyDocParsingInterventionPermission(ctx context.Context, req *pb1.DocParsingInterventionReq,
	dao dao.Dao, oldDoc *model.Doc) error {
	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetBotBizId())
	if err != nil {
		log.ErrorContextf(ctx, "DocParsingIntervention: BotBizId can not be converted to uint64")
		return err
	}
	dbBotBizID, err := dao.GetBotBizIDByID(ctx, oldDoc.RobotID)
	if err != nil {
		log.ErrorContextf(ctx, "DocParsingIntervention: GetBotBizIDByID failed")
		return err
	}
	if botBizID != dbBotBizID {
		log.ErrorContextf(ctx, "DocParsingIntervention: ErrPermissionDenied")
		return errs.ErrPermissionDenied
	}
	return nil
}

func (s *Service) saveNewDoc(ctx context.Context, adminService *service.Service, req *pb1.DocParsingInterventionReq,
	oldDoc *model.Doc, attlLabelList []*model.AttrLabel) (*pb1.SaveDocRsp, error) {
	// 1. 设置文档关联的属性和标签
	var attrLabelReferList []*pb1.AttrLabelRefer
	for _, v := range attlLabelList {
		var labelBizIDs []string
		for _, l := range v.Labels {
			labelBizIDs = append(labelBizIDs, strconv.FormatUint(l.BusinessID, 10))
		}
		attrLabel := &pb1.AttrLabelRefer{
			Source:         v.Source,
			AttributeBizId: strconv.FormatUint(v.BusinessID, 10),
			LabelBizIds:    labelBizIDs,
		}
		attrLabelReferList = append(attrLabelReferList, attrLabel)
	}
	// 2. 保存新文档
	saveReq := &pb1.SaveDocReq{
		FileName:     req.GetFileName(),
		CosUrl:       req.GetCosUrl(),
		FileType:     req.GetFileType(),
		BotBizId:     req.GetBotBizId(),
		ETag:         req.GetETag(),
		CosHash:      req.GetCosHash(),
		Size:         req.GetSize(),
		Source:       oldDoc.Source,
		WebUrl:       oldDoc.WebURL,
		IsRefer:      oldDoc.IsRefer,
		AttrRange:    oldDoc.AttrRange,
		AttrLabels:   attrLabelReferList,
		ReferUrlType: oldDoc.ReferURLType,
		ExpireStart:  strconv.FormatInt(oldDoc.ExpireStart.Unix(), 10),
		ExpireEnd:    strconv.FormatInt(oldDoc.ExpireEnd.Unix(), 10),
		Opt:          oldDoc.Opt,
	}
	saveRsp, err := adminService.SaveDoc(ctx, saveReq)
	if err != nil {
		log.ErrorContextf(ctx, "DocParsingIntervention: save new doc failed, %+v", err)
		return nil, err
	}
	newDocID, err := s.dao.GetDocIDByBusinessID(ctx, saveRsp.GetDocBizId(), oldDoc.RobotID)
	if err != nil {
		log.ErrorContextf(ctx, "DocParsingIntervention: GetDocIDByBusinessID failed, %+v", err)
		return nil, err
	}
	// 用旧文档的cos信息覆盖新文档cos信息。这样干预过程中，预览文档或者拉文档列表时，获取到的是原始文档信息。
	newDoc := &model.Doc{
		ID:       newDocID,
		CosURL:   oldDoc.CosURL,
		FileName: oldDoc.FileName,
		FileType: oldDoc.FileType,
		FileSize: oldDoc.FileSize,
		CosHash:  oldDoc.CosHash,
	}
	err = s.dao.UpdateCosInfo(ctx, newDoc)
	if err != nil {
		log.ErrorContextf(ctx, "DocParsingIntervention: UpdateCosInfo failed, %+v", err)
		return nil, err
	}
	return saveRsp, nil
}

func (s *Service) setOldDocParsingInterventionRedisValue(ctx context.Context, req *pb1.DocParsingInterventionReq,
	oldDoc *model.Doc, attlLabelList []*model.AttrLabel) error {
	redisKey := oldDocParsingInterventionRedisKey(req.GetBotBizId(), oldDoc.ID)
	redisValue := &model.DocParsingInterventionRedisValue{
		OldDoc:     oldDoc,
		AttrLabels: attlLabelList,
	}
	var redisValueStr string
	redisValueStr, err := jsoniter.MarshalToString(redisValue)
	if err != nil {
		log.ErrorContextf(ctx, "DocParsingIntervention: marshal redisValue failed, key:%s, %+v", redisKey, err)
		return err
	}
	// 最多保存7天
	if _, err = s.dao.RedisCli().Do(ctx, "SET", redisKey, redisValueStr, "EX", 86400*7); err != nil {
		log.ErrorContextf(ctx, "DocParsingIntervention: set redis value failed, key:%s, %+v", redisKey, err)
		return err
	}
	log.InfoContextf(ctx, "DocParsingIntervention: setOldDocParsingInterventionRedisValue ok, key:%s, %+v",
		redisKey, redisValueStr)
	return nil
}

func (s *Service) delOldDocParsingInterventionRedisValue(ctx context.Context, req *pb1.DocParsingInterventionReq,
	oldDoc *model.Doc) error {
	redisKey := oldDocParsingInterventionRedisKey(req.GetBotBizId(), oldDoc.ID)
	if _, err := s.dao.RedisCli().Do(ctx, "DEL", redisKey); err != nil {
		log.InfoContextf(ctx, "DocParsingIntervention: del redis key failed, key:%s, %+v", redisKey, err)
		return err
	}
	log.InfoContextf(ctx, "DocParsingIntervention: delOldDocParsingInterventionRedisValue ok, key:%s", redisKey)
	return nil
}

func (s *Service) setOldDocCosHashToNewDocRedisValue(ctx context.Context, req *pb1.DocParsingInterventionReq,
	oldDoc *model.Doc) error {
	redisKey := oldDocCosHashToNewDocRedisKey(oldDoc.CorpID, req.GetBotBizId(), oldDoc.CosHash)
	fileSize, err := util.CheckReqParamsIsUint64(ctx, req.GetSize())
	if err != nil {
		return err
	}
	newDoc := &model.Doc{
		FileName: req.GetFileName(),
		FileType: req.GetFileType(),
		FileSize: fileSize,
		CosURL:   req.GetCosUrl(),
		CosHash:  req.GetCosHash(),
	}
	redisValue := &model.DocParsingInterventionRedisValue{
		OldDoc: newDoc,
	}
	var redisValueStr string
	redisValueStr, err = jsoniter.MarshalToString(redisValue)
	if err != nil {
		log.ErrorContextf(ctx, "DocParsingIntervention: marshal redisValue failed, key:%s, %+v", redisKey, err)
		return err
	}
	// 最多保存7天
	if _, err = s.dao.RedisCli().Do(ctx, "SET", redisKey, redisValueStr, "EX", 86400*7); err != nil {
		log.ErrorContextf(ctx, "DocParsingIntervention: set redis value1 failed, key:%s, %+v", redisKey, err)
		return err
	}
	log.InfoContextf(ctx, "DocParsingIntervention: setNewDocCosHashToOldDocRedisValue ok, key:%s", redisKey)
	return nil
}

func (s *Service) getOldDocCosHashToNewDocRedisValue(ctx context.Context, corpID uint64, botBizID,
	oldDocCosHash string) (*model.Doc, *model.DocParsingInterventionRedisValue, error) {
	redisKey := oldDocCosHashToNewDocRedisKey(corpID, botBizID, oldDocCosHash)
	redisValue, err := redis.String(s.dao.RedisCli().Do(ctx, "GET", redisKey))
	if err == nil {
		if redisValue == "" {
			log.ErrorContextf(ctx, "getOldDocCosHashToNewDocRedisValue: redis value is empty")
			return nil, nil, fmt.Errorf("redis value is empty")
		}
		var redisValueDoc = &model.DocParsingInterventionRedisValue{}
		if err1 := jsoniter.UnmarshalFromString(redisValue, redisValueDoc); err1 != nil {
			log.ErrorContextf(ctx, "getOldDocCosHashToNewDocRedisValue: unmarshal redis value failed, %+v", err1)
			return nil, nil, err1
		}
		log.InfoContextf(ctx, "getOldDocCosHashToNewDocRedisValue result: redisKey:%s, value: %+v", redisKey, redisValueDoc.OldDoc)
		return redisValueDoc.OldDoc, redisValueDoc, nil
	}
	if errors.Is(err, redis.ErrNil) { // key不存在
		return nil, nil, nil
	}
	log.ErrorContextf(ctx, "getOldDocCosHashToNewDocRedisValue failed: redisKey:%s, %+v", redisKey, err)
	return nil, nil, err
}

func (s *Service) delOldDocCosHashToNewDocRedisValue(ctx context.Context, corpID uint64, botBizID,
	oldDocCosHash string) error {
	redisKey := oldDocCosHashToNewDocRedisKey(corpID, botBizID, oldDocCosHash)
	if _, err := s.dao.RedisCli().Do(ctx, "DEL", redisKey); err != nil {
		log.ErrorContextf(ctx, "delOldDocCosHashToNewDocRedisValue: del redis key failed, key:%s, %+v",
			redisKey, err)
		return err
	}
	log.InfoContextf(ctx, "delOldDocCosHashToNewDocRedisValue ok, redisKey: %s", redisKey)
	return nil
}

func oldDocParsingInterventionRedisKey(botBizID string, oldDocID uint64) string {
	return fmt.Sprintf("OldDocParsingIntervention:%s:%d", botBizID, oldDocID)
}

func oldDocCosHashToNewDocRedisKey(corpID uint64, botBizID, oldDocCosHash string) string {
	return fmt.Sprintf("OldDocCosHashToNewDoc:%d:%s:%s", corpID, botBizID, oldDocCosHash)
}

// 异步解析文档
func (s *Service) asyncParseDoc(ctx context.Context, doc *model.Doc, interventionType uint32) {
	go func(rCtx context.Context) {
		// 需要sleep 1秒，等字数统计的回调接口先正常返回，再请求解析拆分
		time.Sleep(1 * time.Second)
		intervene := interventionType == model.InterventionTypeOrgData || interventionType == model.InterventionTypeSheet
		if err := s.dao.DocParseSegment(rCtx, nil, doc, intervene); err != nil {
			return
		}
	}(ctx)
}
