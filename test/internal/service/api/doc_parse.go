package api

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"git.woa.com/adp/common/x/gox"
	"git.woa.com/adp/kb/kb-config/internal/entity/finance"
	"github.com/redis/go-redis/v9"

	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/encodingx/jsonx"
	"git.woa.com/adp/common/x/errx"
	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/syncx/distributedlockx"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/dao"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	attrEntity "git.woa.com/adp/kb/kb-config/internal/entity/label"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	"git.woa.com/adp/kb/kb-config/pkg/i18nkey"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
	pb1 "git.woa.com/adp/pb-go/kb/kb_config"
	"git.woa.com/adp/pb-go/kb/parse_engine/file_parse_common"
)

const (
	// FileParserProcessLockKey 文档解析结果处理分布式锁
	FileParserProcessLockKey = "file_parser_process_lock_%d"
)

// FileParserCallback 解析结果回调
func (s *Service) FileParserCallback(ctx context.Context, req *pb.FileParserCallbackReq) (
	*pb.FileParserCallbackRes, error) {
	logx.I(ctx, "FileParserCallback req:%+v", req)
	if config.GetMainConfig().UnfinishedDocParseRefreshConfig.IgnoreFileParseCallbackForDebug {
		logx.W(ctx, "仅限测试环境，忽略解析结果回调")
		return successFileParserCallbackRsp()
	}
	rsp := new(pb.FileParserCallbackRes)
	// 分布式锁
	key := fmt.Sprintf(FileParserProcessLockKey, req.GetTaskId())
	lock := distributedlockx.NewRedisLock(s.docLogic.GetDao().RedisCli(), key, distributedlockx.WithTTL(time.Second*30))
	success, err := lock.Lock(ctx)
	if err != nil {
		logx.E(ctx, "Lock err:%v", err)
		return rsp, errs.ErrAlreadyLocked
	}
	if success { // 加锁成功
		defer func() {
			if err = lock.Unlock(ctx); err != nil {
				logx.E(ctx, "Unlock err:%v", err)
			}
		}()
	} else {
		logx.W(ctx, "FileParserCallback lock failed, taskId:%d", req.GetTaskId())
		return rsp, errs.ErrAlreadyLocked
	}
	currentOpType := req.GetCurrentOpType()
	docParse, err := s.docLogic.GetDocParseByTaskIDAndOpType(ctx, req.GetTaskId(), int32(currentOpType))
	if err != nil {
		if errors.Is(err, errs.ErrDocParseTaskNotFound) {
			logx.D(ctx, "FileParserCallback Response req:%+v, rsp:%+v,info:%+v", req, rsp, err)
			return successFileParserCallbackRsp()
		}
		rsp.StatusCode = docEntity.DocParseResultCallBackFail
		rsp.Message = errx.Msg(errs.ErrDocParseTaskNotFound)
		logx.D(ctx, "FileParserCallback Response req:%+v, rsp:%+v", req, rsp)
		return rsp, errs.ErrDocParseTaskNotFound
	}
	result, err := jsonx.MarshalToString(req)
	if err != nil {
		rsp.StatusCode = docEntity.DocParseResultCallBackFail
		rsp.Message = errx.Msg(errs.ErrParams)
		logx.E(ctx, "FileParserCallback Response MarshalToString err:%+v, req:%+v,  rsp:%+v, result:%+v",
			errs.ErrParams, req, rsp, result)
		return rsp, errs.ErrParams
	}
	docParse.Result = result
	app, doc, err := s.getRobotAndDoc(ctx, docParse)
	if err != nil {
		if errors.Is(err, errs.ErrRobotNotFound) || errors.Is(err, errs.ErrRobotOrDocNotFound) ||
			errors.Is(err, errs.ErrDocNotFound) {
			logx.D(ctx, "FileParserCallback Response req:%+v, rsp:%+v,info:%+v", req, rsp, err)
			return successFileParserCallbackRsp()
		}
		rsp.StatusCode = docEntity.DocParseResultCallBackFail
		rsp.Message = errx.Msg(errs.ErrRobotOrDocNotFound)
		logx.D(ctx, "FileParserCallback Response req:%+v, rsp:%+v", req, rsp)
		return rsp, errs.ErrRobotOrDocNotFound
	}
	if doc.HasDeleted() {
		logx.D(ctx, "FileParserCallback Response req:%+v, rsp:%+v,info:%+v", req, rsp, err)
		return successFileParserCallbackRsp()
	}
	if err = app.IsWriteable(); err != nil {
		rsp.StatusCode = docEntity.DocParseResultCallBackFail
		rsp.Message = errx.Msg(err)
		return rsp, err
	}

	logx.D(ctx, "FileParserCallback. prepare for doc: %+v", doc)

	if req.Status != docEntity.DocParseCallBackFinish {
		doc.Message = s.getDocMessage(req)
	}
	if currentOpType == docEntity.DocParseOpTypeWordCount {
		// for https://tapd.woa.com/tapd_fe/70080800/story/detail/1070080800118931920
		// 干预文档解析/拆分结果，第一次FileParserCallback时，用新文档cos信息覆盖旧文档的cos信息，给下游传递新的cos信息
		botBizID := strconv.FormatUint(app.BizId, 10)
		interventionType := uint32(0)
		// 获取op干预存储信息
		doc1, opRedisValue, err1 := s.getOldDocCosHashToNewDocRedisValue(ctx, doc.CorpID, botBizID, doc.CosHash)
		// op干预替换文档链接
		if err1 == nil && doc1 != nil && opRedisValue != nil {
			if opRedisValue.InterventionType == docEntity.InterventionTypeOP {
				doc.CosURL = doc1.CosURL
				doc.FileName = doc1.FileName
				doc.FileType = doc1.FileType
				doc.CosHash = doc1.CosHash
				doc.FileSize = doc1.FileSize
			}
		}
		// 获取非op干预存储信息
		doc2, redisValue, err2 := s.docLogic.GetInterveneOldDocCosHashToNewDocRedisValue(ctx, doc.CorpID,
			app.BizId, doc.BusinessID, doc.CosHash)
		if err2 == nil && doc2 != nil && redisValue != nil && (redisValue.InterventionType ==
			docEntity.InterventionTypeOrgData || redisValue.InterventionType == docEntity.InterventionTypeSheet) {
			interventionType = redisValue.InterventionType
		}
		err = s.parseWordCount(ctx, req, doc, app, docParse, interventionType)
		if err != nil {
			rsp.StatusCode = docEntity.DocParseResultCallBackFail
			rsp.Message = errx.Msg(errs.ErrDocParseResultCallBackErr)
			logx.D(ctx, "FileParserCallback Response req:%+v, rsp:%+v", req, rsp)
			return rsp, errs.ErrDocParseResultCallBackErr
		}
		rsp, err = successFileParserCallbackRsp()
		logx.D(ctx, "FileParserCallback Response Success docParse.Type:%d rsp:%+v", docParse.Type, rsp)
		return rsp, nil
	}

	if currentOpType == docEntity.DocParseOpTypeSplit && docParse.Type == docEntity.DocParseTaskTypeSplitSegment {
		// for https://tapd.woa.com/tapd_fe/70080800/story/detail/1070080800118931920
		// 干预文档解析/拆分结果，第二次FileParserCallback时，删除redis数据(数据库中的数据一直都为原始数据)
		botBizID := strconv.FormatUint(app.BizId, 10)
		var originDocBizID uint64
		// 对于非op干预需获取一次原始id，用于判断是否为干预文档
		doc1, redisValue, err1 := s.docLogic.GetInterveneOldDocCosHashToNewDocRedisValue(ctx, doc.CorpID,
			app.BizId, doc.BusinessID, doc.CosHash)
		if err1 == nil && doc1 != nil && redisValue != nil {
			if redisValue.InterventionType == docEntity.InterventionTypeOrgData ||
				redisValue.InterventionType == docEntity.InterventionTypeSheet {
				originDocBizID = redisValue.OriginDocBizID
			}
		}
		// 删除op干预存储的redis信息
		_ = s.delOldDocCosHashToNewDocRedisValue(ctx, doc.CorpID, botBizID, doc.CosHash)
		// 删除非op干预存储的redis信息
		_ = s.docLogic.DeleteInterveneOldDocCosHashToNewDocRedisValue(ctx, doc.CorpID,
			app.BizId, doc.BusinessID, doc.CosHash)
		err = s.splitSegment(ctx, req, doc, docParse, originDocBizID)
		if err != nil {
			rsp.StatusCode = docEntity.DocParseResultCallBackFail
			rsp.Message = errx.Msg(err)
			logx.D(ctx, "解析结果回调响应 req:%+v, rsp:%+v", req, rsp)
			return rsp, errs.ErrDocParseResultCallBackErr
		}
		logx.D(ctx, "解析结果回调 成功 docParse.Type:%d rsp:%+v", docParse.Type, rsp)
		return successFileParserCallbackRsp()
	}

	if currentOpType == docEntity.DocParseOpTypeSplit && docParse.Type == docEntity.DocParseTaskTypeSplitQA {
		err = s.splitQa(ctx, req, doc, docParse)
		if err != nil {
			rsp.StatusCode = docEntity.DocParseResultCallBackFail
			rsp.Message = errx.Msg(err)
			logx.D(ctx, "解析结果回调响应 req:%+v, rsp:%+v", req, rsp)
			return rsp, err
		}
		logx.D(ctx, "解析结果回调 成功 docParse.Type:%d rsp:%+v", docParse.Type, rsp)
		return successFileParserCallbackRsp()
	}
	logx.D(ctx, "解析结果回调完成 docParse.Type:%d req:%+v, rsp:%+v", docParse.Type, req, rsp)
	return rsp, nil
}

// successFileParserCallbackRsp 解析任务成功或不需要重试回包
func successFileParserCallbackRsp() (*pb.FileParserCallbackRes, error) {
	rsp := new(pb.FileParserCallbackRes)
	rsp.StatusCode = docEntity.DocParseResultCallBackSuccess
	rsp.Message = "success"
	return rsp, nil
}

func (s *Service) getDocMessage(req *pb.FileParserCallbackReq) string {
	if conf, ok := config.App().DocParseError[req.ErrorCode]; ok {
		return conf.Msg
	}
	return config.App().DocParseErrorDefault.Msg
}

func (s *Service) parseWordCount(ctx context.Context, req *pb.FileParserCallbackReq, doc *docEntity.Doc, app *entity.App,
	docParse *docEntity.DocParse, interventionType uint32) error {
	logx.I(ctx, "parseWordCount req:%+v, doc:%+v, app:%+v, docParse:%+v, interventionType:%+v",
		req, doc, app, docParse, interventionType)
	var err error
	var event string
	isNeedNotice := false
	if req.Status != docEntity.DocParseCallBackFinish {
		// 解析失败
		event = docEntity.EventProcessFailed
		docParse.Status = docEntity.DocParseCallBackFailed
	} else {
		// 解析成功
		event = docEntity.EventProcessSuccess
		docParse.Status = docEntity.DocParseCallBackFinish

		if err = s.financeLogic.CheckKnowledgeBaseQuota(ctx, finance.CheckQuotaReq{
			App:                  app,
			NewCharSize:          uint64(req.GetTextLength()),
			NewKnowledgeCapacity: doc.FileSize,
			NewStorageCapacity:   gox.IfElse(doc.Source == docEntity.SourceFromCorpCOSDoc, 0, doc.FileSize),
			NewComputeCapacity:   doc.FileSize,
		}); err != nil {
			// 字符超限
			event = docEntity.EventUsedCharSizeExceeded
			doc.Message = errs.ConvertErr2i18nKeyMsg(err)
			docParse.Status = docEntity.DocParseCallBackCharSizeExceeded
			isNeedNotice = true
		} else {
			// 字符未超限
			if config.FileAuditSwitch() {
				switch {
				case interventionType == docEntity.InterventionTypeOrgData || interventionType == docEntity.InterventionTypeSheet:
					if err = s.docLogic.CreateInterveneDocAudit(ctx, doc, interventionType, docParse.SourceEnvSet); err != nil {
						return errs.ErrDocParseResultCallBackErr
					}
				case !doc.NeedAudit():
					event = docEntity.EventCloseAudit
					s.docLogic.AsyncParseDoc(ctx, doc, interventionType)
				default:
					if err = s.docLogic.CreateDocAudit(ctx, doc, docParse.SourceEnvSet); err != nil {
						return errs.ErrDocParseResultCallBackErr
					}
				}
			} else {
				event = docEntity.EventCloseAudit
				s.docLogic.AsyncParseDoc(ctx, doc, interventionType)
			}
		}
	}
	// 更新文档状态和字符数等信息
	doc.CharSize = uint64(req.GetTextLength())
	if doc.AuditFlag != docEntity.AuditFlagNoNeed {
		doc.AuditFlag = docEntity.AuditFlagWait
	}
	docFilter := &docEntity.DocFilter{
		IDs:     []uint64{doc.ID},
		RobotId: doc.RobotID,
	}
	updateCols := []string{docEntity.DocTblColCharSize, docEntity.DocTblColMessage, docEntity.DocTblColAuditFlag,
		docEntity.DocTblColStatus, docEntity.DocTblColUpdateTime}
	err = s.docLogic.UpdateDocStatusMachineByEvent(ctx, updateCols, docFilter, doc, event)
	if err != nil {
		return errs.ErrUpdateDocStatusAndCharSizeFail
	}
	if doc.CharSize != 0 {
		err := s.financeLogic.UpdateAppCapacityUsage(ctx, entity.CapacityUsage{
			CharSize: int64(doc.CharSize),
		}, doc.RobotID, doc.CorpID)
		if err != nil {
			return errs.ErrUpdateRobotUsedCharSizeFail
		}
	}
	docParse.UpdateTime = time.Now()
	err = s.docLogic.UpdateDocParseTask(ctx, docEntity.DocParseUpdateColList, docParse)
	if err != nil {
		return errs.ErrUpdateDocParseTaskStatusFail
	}
	if isNeedNotice {
		if err = s.failNotice(ctx, doc); err != nil {
			return err
		}
	}
	logx.D(ctx, "parseWordCount: update doc status and char size succeeded, doc:%+v", doc)
	return err
}

func (s *Service) failNotice(ctx context.Context, doc *docEntity.Doc) error {
	logx.D(ctx, "failNotice , doc: %+v", doc)
	operations := []releaseEntity.Operation{{Type: releaseEntity.OpTypeViewDetail, Params: releaseEntity.OpParams{}}}
	noticeOptions := []releaseEntity.NoticeOption{
		releaseEntity.WithGlobalFlag(),
		releaseEntity.WithPageID(releaseEntity.NoticeDocPageID),
		releaseEntity.WithLevel(releaseEntity.LevelError),
		releaseEntity.WithSubject(i18n.Translate(ctx, i18nkey.KeyDocumentImportFailure)),
		releaseEntity.WithContent(i18n.Translate(ctx, i18nkey.KeyDocumentImportFailureExceedCharLimitWithName, doc.FileName)),
	}
	notice := releaseEntity.NewNotice(releaseEntity.NoticeTypeDocToQA, doc.ID, doc.CorpID, doc.RobotID, doc.StaffID, noticeOptions...)
	if err := notice.SetOperation(operations); err != nil {
		logx.E(ctx, "序列化通知操作参数失败 operations:%+v err:%+v", operations, err)
		return err
	}
	if err := s.rpc.PlatformAdmin.CreateNotice(ctx, notice); err != nil {
		logx.E(ctx, "CreateNotice err:%+v err:%+v", notice, err)
		return err
	}
	return nil
}

func (s *Service) splitQa(ctx context.Context, req *pb.FileParserCallbackReq, doc *docEntity.Doc,
	docParse *docEntity.DocParse) error {
	if req.Status == docEntity.DocParseCallBackFinish {
		docParse.Status = docEntity.DocParseCallBackFinish
	} else {
		docParse.Status = docEntity.DocParseCallBackFailed
	}
	docParse.UpdateTime = time.Now()
	if err := s.docLogic.UpdateDocParseTask(ctx, docEntity.DocParseUpdateColList, docParse); err != nil {
		return errs.ErrUpdateDocParseTaskStatusFail
	}
	if req.Status == docEntity.DocParseCallBackFinish {
		if _, err := s.docLogic.CreateDocToQATask(ctx, doc, nil); err != nil {
			return errs.ErrCreateDocToQATaskFail
		}
	}
	return nil
}

func (s *Service) splitSegment(ctx context.Context, req *pb.FileParserCallbackReq, doc *docEntity.Doc,
	docParse *docEntity.DocParse, originDocBizID uint64) error {
	if req.Status == docEntity.DocParseCallBackFinish {
		docParse.Status = docEntity.DocParseCallBackFinish
	} else {
		docParse.Status = docEntity.DocParseCallBackFailed
		// 更新文档状态
		event := docEntity.EventProcessFailed
		docFilter := &docEntity.DocFilter{
			RobotId: doc.RobotID,
			IDs:     []uint64{doc.ID},
		}
		updateCols := []string{docEntity.DocTblColStatus}
		err := s.docLogic.UpdateDocStatusMachineByEvent(ctx, updateCols, docFilter, doc, event)
		if err != nil {
			return errs.ErrUpdateDocStatusFail
		}
	}
	docParse.UpdateTime = time.Now()

	err := s.docLogic.UpdateDocParseTask(ctx, docEntity.DocParseUpdateColList, docParse)
	if err != nil {
		return errs.ErrUpdateDocParseTaskStatusFail
	}
	if req.Status == docEntity.DocParseCallBackFinish {
		if err := s.docLogic.CreateDocToIndexTask(ctx, doc, originDocBizID); err != nil {
			return errs.ErrCreateDocToIndexTaskFail
		}
	}
	return nil
}

func (s *Service) getRobotAndDoc(ctx context.Context, docParse *docEntity.DocParse) (*entity.App, *docEntity.Doc, error) {
	logx.I(ctx, "getRobotAndDoc, docParse: (appID:%d, docID:%d)", docParse.RobotID, docParse.DocID)
	appDB, err := s.rpc.AppAdmin.DescribeAppByPrimaryIdWithoutNotFoundError(ctx, docParse.RobotID)
	if err != nil {
		return nil, nil, err
	}
	if appDB == nil {
		return nil, nil, errs.ErrRobotNotFound
	}

	doc, err := s.docLogic.GetDocByID(ctx, docParse.DocID, appDB.PrimaryId)
	if err != nil {
		return nil, nil, errs.ErrDocNotFound
	}
	return appDB, doc, nil
}

// DocParsingIntervention 干预文档解析/拆分结果
func (s *Service) DocParsingIntervention(ctx context.Context, req *pb1.DocParsingInterventionReq) (
	*pb1.DocParsingInterventionRsp, error) {
	// for https://tapd.woa.com/tapd_fe/70080800/story/detail/1070080800118931920  干预文档解析/拆分结果
	logx.I(ctx, "DocParsingIntervention, env-set:%s, req: %+v", contextx.Metadata(ctx).EnvSet(), req)
	rsp := new(pb1.DocParsingInterventionRsp)
	app, err := s.svc.DescribeAppByScene(ctx, req.GetBotBizId(), entity.AppTestScenes)
	if err != nil {
		return nil, errs.ErrRobotNotFound
	}
	// 1. 获取旧文档数据
	oldDoc, attlLabelList, isFromRedis, err := s.getOldDoc(ctx, req, app.PrimaryId)
	if err != nil {
		logx.E(ctx, "DocParsingIntervention: get old doc failed, %+v", err)
		return rsp, err
	}
	logx.I(ctx, "DocParsingIntervention: get old doc succeeded, ifFromRedis:%v, oldDoc: %+v, attlLabelList:%+v", isFromRedis, oldDoc, attlLabelList)

	corp, err := s.rpc.PlatformAdmin.DescribeCorpByPrimaryId(ctx, oldDoc.CorpID)
	if err != nil {
		logx.E(ctx, "DocParsingIntervention: DescribeCorpByPrimaryId failed, %+v", err)
		return rsp, err
	}
	md := contextx.Metadata(ctx)
	md.WithStaffID(oldDoc.StaffID)
	md.WithCorpID(oldDoc.CorpID)
	md.WithSID(corp.GetSid())
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
	_, err = s.svc.DeleteDoc(ctx, delDeq)
	if err != nil {
		logx.E(ctx, "DocParsingIntervention: delete old doc failed, %+v", err)
		return rsp, err
	}
	logx.I(ctx, "DocParsingIntervention: delete old doc succeeded.")
	// 5. 创建新文档
	saveRsp, err := s.saveNewDoc(ctx, req, oldDoc, attlLabelList)
	if err != nil {
		logx.E(ctx, "DocParsingIntervention: save new doc failed, %+v", err)
		return rsp, err
	}
	logx.I(ctx, "DocParsingIntervention: save new doc succeeded.")
	// 6. 返回新文档id
	newDocID, err := s.docLogic.GetDocIDByBusinessID(ctx, saveRsp.GetDocBizId(), app.PrimaryId)
	if err != nil {
		logx.E(ctx, "DocParsingIntervention: get new doc id failed, %+v", err)
		return rsp, err
	}
	rsp.OriginDocId = oldDoc.ID
	rsp.DocId = newDocID
	// 7. 删除旧文档redis数据，非关键路径，不报错
	_ = s.delOldDocParsingInterventionRedisValue(ctx, req, oldDoc)
	logx.I(ctx, "DocParsingIntervention: all succeeded.")
	return rsp, nil
}

func (s *Service) getOldDoc(ctx context.Context, req *pb1.DocParsingInterventionReq, robotID uint64) (
	*docEntity.Doc, []*attrEntity.AttrLabel, bool, error) {
	// 1. 获取redis数据，如果redis中存在数据，说明是重试场景
	redisKey := oldDocParsingInterventionRedisKey(req.GetBotBizId(), req.GetOriginDocId())
	// redisValue, err := redis.String(s.dao.RedisCli().Do(ctx, "GET", redisKey))
	redisValue, err := s.svc.AdminRdb.Get(ctx, redisKey).Result()
	if err == nil {
		if redisValue == "" {
			logx.E(ctx, "DocParsingIntervention: redis value is empty")
			return nil, nil, false, fmt.Errorf("redis value is empty")
		}
		var redisValueDoc = &docEntity.DocParsingInterventionRedisValue{}
		if err1 := jsonx.UnmarshalFromString(redisValue, redisValueDoc); err1 != nil {
			logx.E(ctx, "DocParsingIntervention: unmarshal redis value failed, %+v", err1)
			return nil, nil, false, err1
		}
		if redisValueDoc.OldDoc == nil {
			logx.E(ctx, "DocParsingIntervention: redisValueDoc.OldDoc is nil")
			return nil, nil, false, errs.ErrDocNotFound
		}
		for _, v := range redisValueDoc.AttrLabels {
			if v == nil {
				logx.E(ctx, "DocParsingIntervention: redisValueDoc.AttrLabels has nil member")
				return nil, nil, false, errs.ErrAttributeLabelNotFound
			}
		}
		// 校验是否有权限操作文档
		if err = verifyDocParsingInterventionPermission(ctx, req, s.dao, redisValueDoc.OldDoc); err != nil {
			logx.E(ctx, "DocParsingIntervention: verify permission failed, %+v", err)
			return nil, nil, false, err
		}
		return redisValueDoc.OldDoc, redisValueDoc.AttrLabels, true, nil
	} else if !errors.Is(err, redis.Nil) { // 非key不存在错误
		logx.E(ctx, "DocParsingIntervention: get redis value failed, %+v", err)
		return nil, nil, false, err
	}

	// redis key不存在，从数据库获取
	// 2. 获取旧文档
	oldDoc, err := s.docLogic.GetDocByID(ctx, req.GetOriginDocId(), robotID)
	if err != nil {
		logx.E(ctx, "DocParsingIntervention: get old doc failed, %+v", err)
		return nil, nil, false, err
	}
	if oldDoc == nil {
		logx.E(ctx, "DocParsingIntervention: oldDoc is nil")
		return nil, nil, false, errs.ErrDocNotFound
	}

	// 3. 校验是否有权限操作文档
	if err = verifyDocParsingInterventionPermission(ctx, req, s.dao, oldDoc); err != nil {
		logx.E(ctx, "DocParsingIntervention: verify permission failed, %+v", err)
		return nil, nil, false, err
	}
	// 4. 获取旧文档的属性&标签
	mapAttrLabel, err := s.labelLogic.GetDocAttributeLabelDetail(ctx, oldDoc.RobotID, []uint64{oldDoc.ID})
	if err != nil {
		logx.E(ctx, "DocParsingIntervention: get old doc attribute&label failed, %+v", err)
		return nil, nil, false, err
	}
	for _, v := range mapAttrLabel[oldDoc.ID] {
		if v == nil {
			logx.E(ctx, "DocParsingIntervention: AttrLabels has nil member")
			return nil, nil, false, errs.ErrAttributeLabelNotFound
		}
	}
	return oldDoc, mapAttrLabel[oldDoc.ID], false, nil
}

// 校验是否有权限操作文档
func verifyDocParsingInterventionPermission(ctx context.Context, req *pb1.DocParsingInterventionReq,
	dao dao.Dao, oldDoc *docEntity.Doc) error {
	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetBotBizId())
	if err != nil {
		logx.E(ctx, "DocParsingIntervention: BotBizId can not be converted to uint64")
		return err
	}
	dbBotBizID, err := dao.GetBotBizIDByID(ctx, oldDoc.RobotID)
	if err != nil {
		logx.E(ctx, "DocParsingIntervention: GetBotBizIDByID failed")
		return err
	}
	if botBizID != dbBotBizID {
		logx.E(ctx, "DocParsingIntervention: ErrPermissionDenied")
		return errs.ErrPermissionDenied
	}
	return nil
}

func (s *Service) saveNewDoc(ctx context.Context, req *pb1.DocParsingInterventionReq,
	oldDoc *docEntity.Doc, attlLabelList []*attrEntity.AttrLabel) (*pb1.SaveDocRsp, error) {
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
	saveRsp, err := s.svc.SaveDoc(ctx, saveReq)
	if err != nil {
		logx.E(ctx, "DocParsingIntervention: save new doc failed, %+v", err)
		return nil, err
	}
	newDocID, err := s.docLogic.GetDocIDByBusinessID(ctx, saveRsp.GetDocBizId(), oldDoc.RobotID)
	if err != nil {
		logx.E(ctx, "DocParsingIntervention: GetDocIDByBusinessID failed, %+v", err)
		return nil, err
	}
	// 用旧文档的cos信息覆盖新文档cos信息。这样干预过程中，预览文档或者拉文档列表时，获取到的是原始文档信息。
	newDoc := &docEntity.Doc{
		ID:       newDocID,
		CosURL:   oldDoc.CosURL,
		FileName: oldDoc.FileName,
		FileType: oldDoc.FileType,
		FileSize: oldDoc.FileSize,
		CosHash:  oldDoc.CosHash,
	}
	err = s.docLogic.UpdateCosInfo(ctx, newDoc)
	if err != nil {
		logx.E(ctx, "DocParsingIntervention: UpdateCosInfo failed, %+v", err)
		return nil, err
	}
	return saveRsp, nil
}

func (s *Service) setOldDocParsingInterventionRedisValue(ctx context.Context, req *pb1.DocParsingInterventionReq,
	oldDoc *docEntity.Doc, attlLabelList []*attrEntity.AttrLabel) error {
	redisKey := oldDocParsingInterventionRedisKey(req.GetBotBizId(), oldDoc.ID)
	redisValue := &docEntity.DocParsingInterventionRedisValue{
		OldDoc:     oldDoc,
		AttrLabels: attlLabelList,
	}
	var redisValueStr string
	redisValueStr, err := jsonx.MarshalToString(redisValue)
	if err != nil {
		logx.E(ctx, "DocParsingIntervention: marshal redisValue failed, key:%s, %+v", redisKey, err)
		return err
	}
	// 最多保存7天
	// if _, err = s.dao.RedisCli().Do(ctx, "SET", redisKey, redisValueStr, "EX", 86400*7); err != nil {
	if _, err = s.docLogic.GetDao().RedisCli().Set(ctx, redisKey, redisValueStr, 86400*7*time.Second).Result(); err != nil {
		logx.E(ctx, "DocParsingIntervention: set redis value failed, key:%s, %+v", redisKey, err)
		return err
	}
	logx.I(ctx, "DocParsingIntervention: setOldDocParsingInterventionRedisValue ok, key:%s, %+v",
		redisKey, redisValueStr)
	return nil
}

func (s *Service) delOldDocParsingInterventionRedisValue(ctx context.Context, req *pb1.DocParsingInterventionReq,
	oldDoc *docEntity.Doc) error {
	redisKey := oldDocParsingInterventionRedisKey(req.GetBotBizId(), oldDoc.ID)
	// if _, err := s.dao.RedisCli().Do(ctx, "DEL", redisKey); err != nil {
	if _, err := s.docLogic.GetDao().RedisCli().Del(ctx, redisKey).Result(); err != nil {
		logx.I(ctx, "DocParsingIntervention: del redis key failed, key:%s, %+v", redisKey, err)
		return err
	}
	logx.I(ctx, "DocParsingIntervention: delOldDocParsingInterventionRedisValue ok, key:%s", redisKey)
	return nil
}

func (s *Service) setOldDocCosHashToNewDocRedisValue(ctx context.Context, req *pb1.DocParsingInterventionReq,
	oldDoc *docEntity.Doc) error {
	redisKey := oldDocCosHashToNewDocRedisKey(oldDoc.CorpID, req.GetBotBizId(), oldDoc.CosHash)
	fileSize, err := util.CheckReqParamsIsUint64(ctx, req.GetSize())
	if err != nil {
		return err
	}
	newDoc := &docEntity.Doc{
		FileName: req.GetFileName(),
		FileType: req.GetFileType(),
		FileSize: fileSize,
		CosURL:   req.GetCosUrl(),
		CosHash:  req.GetCosHash(),
	}
	redisValue := &docEntity.DocParsingInterventionRedisValue{
		OldDoc: newDoc,
	}
	var redisValueStr string
	redisValueStr, err = jsonx.MarshalToString(redisValue)
	if err != nil {
		logx.E(ctx, "DocParsingIntervention: marshal redisValue failed, key:%s, %+v", redisKey, err)
		return err
	}
	// 最多保存7天
	// if _, err = s.dao.RedisCli().Do(ctx, "SET", redisKey, redisValueStr, "EX", 86400*7); err != nil {
	if _, err = s.docLogic.GetDao().RedisCli().Set(ctx, redisKey, redisValueStr, 86400*7*time.Second).Result(); err != nil {
		logx.E(ctx, "DocParsingIntervention: set redis value1 failed, key:%s, %+v", redisKey, err)
		return err
	}
	logx.I(ctx, "DocParsingIntervention: setNewDocCosHashToOldDocRedisValue ok, key:%s", redisKey)
	return nil
}

func (s *Service) getOldDocCosHashToNewDocRedisValue(ctx context.Context, corpID uint64, botBizID,
	oldDocCosHash string) (*docEntity.Doc, *docEntity.DocParsingInterventionRedisValue, error) {
	redisKey := oldDocCosHashToNewDocRedisKey(corpID, botBizID, oldDocCosHash)
	// redisValue, err := redis.String(s.dao.RedisCli().Do(ctx, "GET", redisKey))
	redisValue, err := s.svc.AdminRdb.Get(ctx, redisKey).Result()
	if err == nil {
		if redisValue == "" {
			logx.E(ctx, "getOldDocCosHashToNewDocRedisValue: redis value is empty")
			return nil, nil, fmt.Errorf("redis value is empty")
		}
		var redisValueDoc = &docEntity.DocParsingInterventionRedisValue{}
		if err1 := jsonx.UnmarshalFromString(redisValue, redisValueDoc); err1 != nil {
			logx.E(ctx, "getOldDocCosHashToNewDocRedisValue: unmarshal redis value failed, %+v", err1)
			return nil, nil, err1
		}
		logx.I(ctx, "getOldDocCosHashToNewDocRedisValue result: redisKey:%s, value: %+v", redisKey, redisValueDoc.OldDoc)
		return redisValueDoc.OldDoc, redisValueDoc, nil
	}
	if errors.Is(err, redis.Nil) { // key不存在
		return nil, nil, nil
	}
	logx.E(ctx, "getOldDocCosHashToNewDocRedisValue failed: redisKey:%s, %+v", redisKey, err)
	return nil, nil, err
}

func (s *Service) delOldDocCosHashToNewDocRedisValue(ctx context.Context, corpID uint64, botBizID,
	oldDocCosHash string) error {
	redisKey := oldDocCosHashToNewDocRedisKey(corpID, botBizID, oldDocCosHash)
	// if _, err := s.dao.RedisCli().Do(ctx, "DEL", redisKey); err != nil {
	if _, err := s.docLogic.GetDao().RedisCli().Del(ctx, redisKey).Result(); err != nil {
		logx.E(ctx, "delOldDocCosHashToNewDocRedisValue: del redis key failed, key:%s, %+v",
			redisKey, err)
		return err
	}
	logx.I(ctx, "delOldDocCosHashToNewDocRedisValue ok, redisKey: %s", redisKey)
	return nil
}

func oldDocParsingInterventionRedisKey(botBizID string, oldDocID uint64) string {
	return fmt.Sprintf("OldDocParsingIntervention:%s:%d", botBizID, oldDocID)
}

func oldDocCosHashToNewDocRedisKey(corpID uint64, botBizID, oldDocCosHash string) string {
	return fmt.Sprintf("OldDocCosHashToNewDoc:%d:%s:%s", corpID, botBizID, oldDocCosHash)
}

// UnfinishedDocParseRefresh 未完成的文档解析刷新
func (s *Service) UnfinishedDocParseRefresh(ctx context.Context) error {
	startTime := time.Now()
	logx.I(ctx, "-----------------Do UnfinishedDocParseRefresh!!!!!!!!!!!!!!!!")
	if !config.GetMainConfig().UnfinishedDocParseRefreshConfig.Enable {
		// 未完成的文档解析刷新任务被关闭
		logx.I(ctx, "UnfinishedDocParseRefresh is not enable")
		return nil
	}
	docParses, err := s.docLogic.DescribeAllUnfinishedDocParse(ctx,
		config.GetMainConfig().UnfinishedDocParseRefreshConfig.ParseStatusStoreTimeSecond,
		config.GetMainConfig().UnfinishedDocParseRefreshConfig.RefreshBatchSize)
	if err != nil {
		logx.E(ctx, "DescribeAllUnfinishedDocParse failed, err: %+v", err)
		return err
	}
	for _, docParse := range docParses {
		logx.D(ctx, "-----------------docParse: %+v", docParse)
	}
	describeTaskStatusListRsp, err := s.docLogic.DescribeDocParseStatus(ctx, docParses,
		config.GetMainConfig().UnfinishedDocParseRefreshConfig.RefreshBatchSize)
	if err != nil {
		logx.E(ctx, "DescribeDocParseStatus failed, err: %+v", err)
		return err
	}
	// 统计变量
	totalProcessCount := 0
	successCount := 0
	failCount := 0

	for _, parseRsp := range describeTaskStatusListRsp.GetTaskStatusList() {
		if parseRsp.GetStatus() == file_parse_common.TaskStatus_TASK_STATUS_UNKNOWN ||
			parseRsp.GetStatus() == file_parse_common.TaskStatus_TASK_STATUS_PENDING ||
			parseRsp.GetStatus() == file_parse_common.TaskStatus_TASK_STATUS_RUNNING {
			continue
		}
		totalProcessCount++
		logx.D(ctx, "-----------------UnfinishedDocParseRefresh, do refresh: %+v", parseRsp)
		results := make(map[int32]*pb.FileParserCallbackReq_DataResult)
		for k, v := range parseRsp.GetResults() {
			results[k] = &pb.FileParserCallbackReq_DataResult{
				TotalFileNumber: v.GetTotalFileNumber(),
				Result: slicex.Map(v.GetResults(), func(r *file_parse_common.Result) *pb.FileParserCallbackReq_Result {
					return &pb.FileParserCallbackReq_Result{
						CurrentFileIndex: r.GetCurrentFileIndex(),
						Result:           r.GetResult(),
						ResultMd5:        r.GetResultMd5(),
					}
				}),
				FailedPages: slicex.Map(v.GetFailedPages(), func(r *file_parse_common.FailedPages) *pb.FileParserCallbackReq_FailedPages {
					return &pb.FileParserCallbackReq_FailedPages{
						PageNum: r.GetPageNum(),
					}
				}),
			}
		}
		req := &pb.FileParserCallbackReq{
			TaskId:        parseRsp.GetTaskId(),
			CurrentOpType: pb.FileParserCallbackReq_OpType(parseRsp.GetCurrentOpType()),
			Status:        pb.FileParserCallbackReq_TaskStatus(parseRsp.GetStatus()),
			FType:         pb.FileParserCallbackReq_FileType(parseRsp.GetFType()),
			Progress:      parseRsp.GetProgress().GetProgress(),
			ResultCosUrl:  "",
			TextLength:    parseRsp.GetTextLength(),
			Message:       parseRsp.GetMessage(),
			RequestId:     parseRsp.GetRequestId(),
			ResultMd5:     "",
			ErrorCode:     fmt.Sprintf("%d", parseRsp.GetErrorCode()),
			PageNum:       parseRsp.GetPageNum(),
			DebugInfo:     nil,
			Results:       results,
			Version:       0,
		}
		rsp, err := s.FileParserCallback(ctx, req)
		logx.D(ctx, "FileParserCallback req: %+v, rsp: %+v, err:%+v", req, rsp, err)
		if err != nil {
			logx.E(ctx, "FileParserCallback failed, err: %+v", err)
			failCount++
		} else {
			successCount++
		}
	}

	// 性能分析日志：统计处理的任务数量和总耗时
	duration := time.Since(startTime)
	logx.I(ctx, "UnfinishedDocParseRefresh定时任务完成 - 所有未完成的任务数: %d, 已完成需处理任务数: %d, 成功: %d, 失败: %d, 总耗时: %v",
		len(docParses), totalProcessCount, successCount, failCount, duration)
	return nil
}
