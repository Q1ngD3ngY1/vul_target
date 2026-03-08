package audit

import (
	"context"
	terrs "git.code.oa.com/trpc-go/trpc-go/errs"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/baicaoyuan/moss/types/slicex"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	logicDoc "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/doc"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/i18nkey"
	"github.com/jmoiron/sqlx"
	jsoniter "github.com/json-iterator/go"
	"sync"
	"time"
)

// getAuditStatus 获取审核状态
func getAuditStatus(pass, isAppeal bool) uint32 {
	if pass {
		if isAppeal {
			return model.AuditStatusAppealSuccess
		}
		return model.AuditStatusPass
	} else {
		if isAppeal {
			return model.AuditStatusAppealFail
		}
		return model.AuditStatusFail
	}
}

// getChildAudits 获取子审核任务列表
func getChildAudits(ctx context.Context, audit *model.Audit) ([]*model.AuditStatusSourceList, error) {
	sourceList := make([]*model.AuditStatusSourceList, 0)
	filter := &dao.AuditFilter{
		CorpID:   audit.CorpID,
		RobotID:  audit.RobotID,
		ParentID: audit.ID,
	}
	selectColumns := []string{dao.AuditTblColStatus, dao.AuditTblColType, dao.AuditTblColParams}
	childAudits, err := dao.GetAuditDao().GetAuditList(ctx, selectColumns, filter)
	if err != nil {
		return nil, err
	}
	for _, v := range childAudits {
		auditItem := model.AuditItem{}
		if err := jsoniter.UnmarshalFromString(v.Params, &auditItem); err != nil {
			log.ErrorContextf(ctx, "任务参数解析失败 v.Params:%s,err:%+v",
				v.Params, err)
			return nil, err
		}
		sourceList = append(sourceList, &model.AuditStatusSourceList{
			Status:   v.Status,
			Source:   auditItem.Source,
			Avatar:   auditItem.HeadURL,
			Name:     auditItem.Nick,
			Greeting: auditItem.Greeting,
			Content:  auditItem.Content,
		})
	}
	return sourceList, nil
}

// getChildAuditStatusMap 获取子审核任务状态信息
func getChildAuditStatusMap(ctx context.Context, audit *model.Audit) (map[uint32][]*model.AuditStatusSourceList,
	error) {
	auditStatusSourceMap := make(map[uint32][]*model.AuditStatusSourceList)
	lists, err := getChildAudits(ctx, audit)
	if err != nil {
		return auditStatusSourceMap, err
	}
	for _, v := range lists {
		if _, ok := auditStatusSourceMap[v.Status]; !ok {
			auditStatusSourceMap[v.Status] = make([]*model.AuditStatusSourceList, 0)
		}
		auditStatusSourceMap[v.Status] = append(auditStatusSourceMap[v.Status], v)
	}
	return auditStatusSourceMap, nil
}

func updateDocAuditResult(ctx context.Context, d dao.Dao, doc *model.Doc, audit *model.Audit,
	auditsMap map[uint32][]*model.AuditStatusSourceList, pass, isAppeal bool, event string) (*model.Doc, *model.Audit, error) {
	log.InfoContextf(ctx, "updateDocAuditResult|start|DocID:%d|event:%s|Audit.RelateID:%d|Audit.Type:%d",
		doc.ID, event, audit.RelateID, audit.Type)
	audit.Status = getAuditStatus(pass, isAppeal)
	docSegmentAuditStatusMap := &sync.Map{} // DocSegmentAuditStatus:["SegmentBizID"]
	docSegmentAuditStatusFailedMap := make(map[string]model.DocSegmentAuditStatus)
	if !pass {
		filter := &dao.AuditFilter{
			CorpID:   audit.CorpID,
			RobotID:  audit.RobotID,
			ParentID: audit.ID,
		}
		childAudits, err := dao.GetAuditDao().GetAuditList(ctx, dao.AuditTblColList, filter)
		if err != nil {
			return nil, nil, err
		}
		contentFailed := false
		nameFailed := false
		segmentFailed := false
		segmentPictureFailed := false
		for _, ca := range childAudits {
			key := model.DocSegmentAuditStatusPass
			// 解析params
			p := model.AuditItem{}
			if err := jsoniter.UnmarshalFromString(ca.Params, &p); err != nil {
				continue
			}
			if p.Source == model.AuditSourceDocName && (ca.Status == model.AuditStatusFail || ca.Status == model.AuditStatusAppealFail) {
				nameFailed = true
			}
			if p.Source == model.AuditSourceDoc && (ca.Status == model.AuditStatusFail || ca.Status == model.AuditStatusAppealFail) {
				contentFailed = true
				key = model.DocSegmentAuditStatusContentFailed
			}
			if p.Source == model.AuditSourceDocSegment && (ca.Status == model.AuditStatusFail || ca.Status == model.AuditStatusAppealFail) {
				segmentFailed = true
				if _, ok := docSegmentAuditStatusFailedMap[p.SegmentBizID]; ok {
					key = model.DocSegmentAuditStatusContentAndPictureFailed
				} else {
					key = model.DocSegmentAuditStatusContentFailed
				}
				docSegmentAuditStatusFailedMap[p.SegmentBizID] = key
			}
			if p.Source == model.AuditSourceDocSegmentPic && (ca.Status == model.AuditStatusFail || ca.Status == model.AuditStatusAppealFail) {
				segmentPictureFailed = true
				if _, ok := docSegmentAuditStatusFailedMap[p.SegmentBizID]; ok {
					key = model.DocSegmentAuditStatusContentAndPictureFailed
				} else {
					key = model.DocSegmentAuditStatusPictureFailed
				}
				docSegmentAuditStatusFailedMap[p.SegmentBizID] = key
			}
			if p.SegmentBizID != "" && key != model.DocSegmentAuditStatusPass &&
				(ca.Status == model.AuditStatusFail || ca.Status == model.AuditStatusAppealFail) {
				if ids, ok := docSegmentAuditStatusMap.Load(key); ok {
					idList, ok1 := ids.([]string)
					if !ok1 {
						log.ErrorContextf(ctx, "updateDocAuditResult|SegmentBizID:%s|type assertion failed for ids", p.SegmentBizID)
						continue
					}
					docSegmentAuditStatusMap.Store(key, append(idList, p.SegmentBizID))
				} else {
					docSegmentAuditStatusMap.Store(key, []string{p.SegmentBizID})
				}
			}
		}
		if isAppeal {
			event = model.EventAppealFailed
			if audit.Type == model.AuditBizTypeDocSegment || audit.Type == model.AuditBizTypeDocTableSheet {
				if segmentFailed || segmentPictureFailed || contentFailed {
					doc.Message = i18nkey.KeyParseSegmentationInterventionReviewFailed
				}
			} else {
				if contentFailed && nameFailed {
					doc.Message = i18nkey.KeyFileNameAndContentReviewFailed
				} else if nameFailed {
					doc.Message = i18nkey.KeyFileNameReviewFailed
				} else {
					doc.Message = i18nkey.KeyFileContentReviewFailed
				}
			}
		} else {
			event = model.EventProcessFailed
			if _, ok := auditsMap[model.AuditStatusTimeoutFail]; ok {
				audit.Status = model.AuditStatusTimeoutFail
				if audit.Type == model.AuditBizTypeDocSegment || audit.Type == model.AuditBizTypeDocTableSheet {
					// todo 重试/申诉 功能待开发
					doc.Message = i18nkey.KeyDocumentReviewTimeout
				} else {
					doc.Message = i18nkey.KeyFileReviewTimeout
				}
			} else {
				if audit.Type == model.AuditBizTypeDocSegment || audit.Type == model.AuditBizTypeDocTableSheet {
					if segmentFailed || segmentPictureFailed || contentFailed {
						doc.Message = i18nkey.KeyParseSegmentationInterventionReviewFailed
					}
				} else {
					if contentFailed && nameFailed {
						doc.Message = i18nkey.KeyFileNameAndContentReviewFailedWithOption
					} else if nameFailed {
						doc.Message = i18nkey.KeyFileNameReviewFailedWithOption
					} else {
						doc.Message = i18nkey.KeyFileContentReviewFailedWithOption
					}
				}
			}
		}
	}
	if event == model.EventUsedCharSizeExceeded && (audit.Type == model.AuditBizTypeDocSegment ||
		audit.Type == model.AuditBizTypeDocTableSheet) {
		doc.Message = i18nkey.KeyDocumentInterventionCharacterExceeded
	}
	// 更新干预切片审核失败的状态
	if audit.Type == model.AuditBizTypeDocSegment || audit.Type == model.AuditBizTypeDocTableSheet {
		err := updateDocSegmentAuditResult(ctx, d, doc, audit.Type, docSegmentAuditStatusMap)
		if err != nil {
			log.ErrorContextf(ctx, "updateDocSegmentAuditResult|err:%+v", err)
			return nil, nil, err
		}
	}
	docFilter := &dao.DocFilter{
		IDs:     []uint64{doc.ID},
		RobotId: doc.RobotID,
	}
	doc.AuditFlag = model.AuditFlagDone
	updateCols := []string{dao.DocTblColStatus, dao.DocTblColMessage, dao.DocTblColAuditFlag}
	err := logicDoc.UpdateDoc(ctx, updateCols, docFilter, doc, event)
	if err != nil {
		return nil, nil, err
	}
	return doc, audit, nil
}

// updateDocSegmentAuditResult 更新切片审核状态
func updateDocSegmentAuditResult(ctx context.Context, d dao.Dao, doc *model.Doc, auditType uint32,
	docSegmentAuditStatusMap *sync.Map) error {
	log.InfoContextf(ctx, "updateDocSegmentAuditResult|start|DocID:%d|AuditType:%d", doc.ID, auditType)
	batchSize := 200
	corpBizID, appBizID, _, _, err := d.SegmentCommonIDsToBizIDs(ctx, doc.CorpID,
		doc.RobotID, 0, 0)
	if err != nil {
		log.ErrorContextf(ctx, "updateDocSegmentAuditResult|SegmentCommonIDsToBizIDs|err:%+v", err)
		return err
	}
	auditFailedStatus := []model.DocSegmentAuditStatus{
		model.DocSegmentAuditStatusContentFailed,
		model.DocSegmentAuditStatusPictureFailed,
		model.DocSegmentAuditStatusContentAndPictureFailed}
	for _, auditStatus := range auditFailedStatus {
		if ids, ok := docSegmentAuditStatusMap.Load(auditStatus); ok {
			idList, ok1 := ids.([]string)
			if !ok1 {
				log.ErrorContextf(ctx, "updateDocSegmentAuditResult|auditStatus:%d|type assertion failed for ids", auditStatus)
				continue
			}
			// 分批更新审核状态
			log.InfoContextf(ctx, "updateDocSegmentAuditResult|auditStatus:%d|len(ids):%d", auditStatus, len(idList))
			for _, idChunks := range slicex.Chunk(idList, batchSize) {
				if auditType == model.AuditBizTypeDocSegment {
					err = dao.GetDocSegmentOrgDataTemporaryDao().UpdateDocSegmentAuditStatus(ctx, nil, corpBizID,
						appBizID, doc.BusinessID, idChunks, uint32(auditStatus))
					if err != nil {
						log.ErrorContextf(ctx, "updateDocSegmentAuditResult|UpdateDocSegmentAuditStatus|err:%+v", err)
						return err
					}
				} else if auditType == model.AuditBizTypeDocTableSheet {
					var sheetIDs []uint64
					sheetIDs, err = util.BatchCheckReqParamsIsUint64(ctx, idChunks)
					if err != nil {
						log.ErrorContextf(ctx, "updateDocSegmentAuditResult|BatchCheckReqParamsIsUint64|err:%+v", err)
						return err
					}
					err = dao.GetDocSegmentSheetTemporaryDao().UpdateDocSegmentSheetAuditStatus(ctx, nil, corpBizID,
						appBizID, doc.BusinessID, sheetIDs, uint32(auditStatus))
					if err != nil {
						log.ErrorContextf(ctx, "updateDocSegmentAuditResult|UpdateDocSegmentAuditStatus|err:%+v", err)
						return err
					}
				}
			}
		}
	}
	return nil
}

// ProcessDocAuditParentTask 文档审核或者申诉回调处理函数，audit是父审核任务
func ProcessDocAuditParentTask(ctx context.Context, d dao.Dao, audit *model.Audit, pass, isAppeal bool,
	rejectReason string, params model.AuditCheckParams) error {
	log.DebugContextf(ctx, "ProcessDocAuditParentTask audit:%+v pass:%v isAppeal:%v rejectReason:%s",
		audit, pass, isAppeal, rejectReason)
	intervene := false
	if audit.Type == model.AuditBizTypeDocSegment || audit.Type == model.AuditBizTypeDocTableSheet {
		intervene = true
	}
	doc, err := d.GetDocByID(ctx, audit.RelateID, audit.RobotID)
	if err != nil {
		return err
	}
	if doc == nil {
		return errs.ErrDocNotFound
	}
	if doc.HasDeleted() {
		audit.UpdateTime = time.Now()
		audit.Status = getAuditStatus(true, isAppeal) // 直接把审核状态改成成功
		_ = d.UpdateAuditStatus(ctx, audit)
		log.InfoContextf(ctx, "文档已经被删除，不再走审核逻辑，doc:%+v", doc)
		return nil
	}
	if !isAppeal && !doc.NeedAudit() {
		return nil
	}
	log.DebugContextf(ctx, "ProcessDocAuditParentTask|current DocID:%d", doc.ID)
	auditsMap, err := getChildAuditStatusMap(ctx, audit)
	if err != nil || len(auditsMap) == 0 {
		return errs.ErrAuditNotFound
	}
	isNeedCharSizeNotice := false
	if err = d.GetDB().Transactionx(ctx, func(tx *sqlx.Tx) error {
		// 更新审核任务状态
		filter := &dao.AuditFilter{
			IDs: []uint64{audit.ID},
		}
		audit.Status = getAuditStatus(pass, isAppeal) // 直接把审核状态改成成功
		updateCols := []string{dao.AuditTblColRetryTimes, dao.AuditTblColStatus, dao.AuditTblColMessage}
		_, err = dao.GetAuditDao().UpdateAudit(ctx, nil, updateCols, filter, audit)
		if err != nil {
			return err
		}
		event := model.EventProcessSuccess
		if pass {
			// 审核通过需要校验字符数是否超限
			if err = d.IsUsedCharSizeExceeded(ctx, doc.CorpID, doc.RobotID); err != nil {
				isNeedCharSizeNotice = true
				event = model.EventUsedCharSizeExceeded
				doc.Message = terrs.Msg(d.ConvertErrMsg(ctx, 0, doc.CorpID, errs.ErrDocParseCharSizeExceeded))
			} else {
				if isAppeal {
					// 人工申诉成功发送通知，如果发通知失败，不报错
					_ = d.SendNoticeIfDocAppealPass(ctx, tx, doc, audit)
				}
				if err = d.DocParseSegment(ctx, tx, doc, intervene); err != nil {
					return err
				}
			}
		} else {
			event = model.EventProcessFailed
			// 审核不通过发送通知，如果发通知失败，不报错
			_ = sendAuditNotPassNotice(ctx, d, doc, audit, auditsMap, isAppeal, rejectReason)
		}
		// 更新文档状态
		doc, audit, err = updateDocAuditResult(ctx, d, doc, audit, auditsMap, pass, isAppeal, event)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "审核文档失败 err:%+v", err)
		return err
	}
	if isNeedCharSizeNotice {
		var docParses model.DocParse
		if docParses, err = d.GetDocParseByDocID(ctx, doc.ID, doc.RobotID); err != nil {
			log.ErrorContextf(ctx, "查询 文档解析任务失败 args:%+v err:%+v", doc, err)
			return err
		}
		docParses.Status = model.DocParseCallBackCharSizeExceeded
		err = d.UpdateDocParseTask(ctx, docParses) // 更新解析字符状态,重试的时候不会重新解析
		if err != nil {
			return errs.ErrUpdateDocParseTaskStatusFail
		}
		if err = d.FailCharSizeNotice(ctx, doc); err != nil {
			return err
		}
	}
	return nil
}
