package api

import (
	"context"
	"time"

	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	kbEntity "git.woa.com/adp/kb/kb-config/internal/entity/kb"
	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"git.woa.com/adp/kb/kb-config/pkg/errs"

	pb "git.woa.com/adp/pb-go/kb/kb_config"
)

// GetWaitReleaseCount 获取待发布数量,用于上游业务 CheckRelease 发布前校验
// reference:https://git.woa.com/dialogue-platform/bot-config/bot-admin-config-server/blob/master/internal/app/knowledge_qa.go#L54
func (s *Service) GetWaitReleaseCount(ctx context.Context, req *pb.GetWaitReleaseCountReq) (*pb.GetWaitReleaseCountRsp, error) {
	logx.I(ctx, "[GetWaitReleaseCount] req:%+v", req)
	rsp := new(pb.GetWaitReleaseCountRsp)
	app, err := s.rpc.AppAdmin.DescribeAppById(ctx, req.GetAppBizId())
	if err != nil {
		logx.E(ctx, "Failed to get app (appBizId:%d). err:%+v,", req.GetAppBizId(), err)
		return rsp, errs.ErrRobotNotFound
	}
	corpID, robotID := app.CorpPrimaryId, app.PrimaryId
	logx.I(ctx, "corpInfo: corpID:%d, appId:%d", corpID, robotID)
	corp, err := s.rpc.PlatformAdmin.DescribeCorpByPrimaryId(ctx, corpID)
	if err != nil {
		logx.E(ctx, "Failed to get corp (corpID:%d). err:%+v,", corpID, err)
		return rsp, errs.ErrCorpNotFound
	}
	zeroTime := time.Time{}

	var count uint64
	logx.I(ctx, "[GetWaitReleaseCount] Prepare to get wait release count (appBizID:%d). releaseType:%s",
		req.AppBizId, req.ReleaseType.String())

	switch req.ReleaseType {
	case pb.ReleaseType_ReleaseTypeConfig:
		newCtx := util.SetMultipleMetaData(ctx, app.SpaceId, app.Uin)
		kbConfigDiff, referShareKBChanged, err := s.kbLogic.AppKnowledgeConfigRetrievalDetailDiff(
			newCtx, corp.CorpId, req.GetAppBizId(), app.SpaceId)
		if err != nil {
			logx.E(newCtx, "Failed to get wait release config count (appBizID:%d). releaseType:%s err:%+v,",
				req.AppBizId, req.ReleaseType.String(), err)
			return nil, err
		}
		count = uint64(len(kbConfigDiff))
		if count == 0 && referShareKBChanged {
			count = 1 // 仅存在共享知识库引用关系的变更，此时前端不需要展示共享知识库检索配置的详情，但需要返回的count>0，以便发布流程能正常进行
		}
		rsp.ConfigDiff = convertKbConfigDiffPO2Pb(kbConfigDiff)
	case pb.ReleaseType_ReleaseTypeDocument:
		rsp.Count = 0
		//count, err = s.docLogic.GetWaitReleaseDocCount(ctx, corpID, robotID, "",
		//	zeroTime, zeroTime, nil)
	case pb.ReleaseType_ReleaseTypeQA:
		rsp.Count = 0
		//count, err = s.qaLogic.GetReleaseQACount(ctx, corpID, robotID, "", zeroTime, zeroTime, nil)
	case pb.ReleaseType_ReleaseTypeRejectedQuestion:
		count, err = s.releaseLogic.GetReleaseRejectedQuestionCount(ctx, corpID, robotID, "", zeroTime, zeroTime, nil)
	case pb.ReleaseType_ReleaseTypeLabel:
		rsp.Count = 0
		//count, err = s.labelLogic.GetWaitReleaseAttributeCount(ctx, robotID, "", nil, zeroTime, zeroTime)
	case pb.ReleaseType_ReleaseDBType:
		//dbSources, err := s.dbLogic.GetUnreleasedDBSource(ctx, req.GetAppBizId())
		//if err != nil {
		//	return nil, err
		//}
		//dbTables, err := s.dbLogic.GetUnreleasedDBTable(ctx, req.GetAppBizId())
		//if err != nil {
		//	return nil, err
		//}
		//count = uint64(len(dbSources) + len(dbTables))
		rsp.Count = 0
	default:
		err = errs.ErrParamsNotExpected
		logx.E(ctx, "Failed to get wait release count (appBizID:%d). releaseType:%s err:%+v,",
			req.AppBizId, req.ReleaseType.String(), err)
		return nil, err
	}

	rsp.Count = int32(count)
	logx.I(ctx, "[GetWaitReleaseCount] Get wait release count (appBizID:%d). releaseType:%s count:%d",
		req.AppBizId, req.ReleaseType.String(), count)
	return rsp, nil
}

func convertKbConfigDiffPO2Pb(kbConfigDiff []*kbEntity.KnowledgeConfigDiff) []*pb.KnowledgeConfigDiff {
	diffs := make([]*pb.KnowledgeConfigDiff, 0, len(kbConfigDiff))
	for _, diff := range kbConfigDiff {
		diffs = append(diffs, &pb.KnowledgeConfigDiff{
			ConfigItem: diff.ConfigItem,
			LastValue:  diff.LastValue,
			NewValue:   diff.NewValue,
			Content:    diff.Content,
			Action:     diff.Action,
		})
	}
	return diffs
}

func (s *Service) GetReleasedCount(ctx context.Context, req *pb.GetReleasedCountReq) (*pb.GetReleasedCountRsp, error) {
	rsp := new(pb.GetReleasedCountRsp)
	logx.I(ctx, "[GetReleasedCount] req:%+v", req)
	rsp.VersionId = req.GetVersionId()

	app, err := s.rpc.AppAdmin.DescribeAppById(ctx, req.GetAppBizId())
	if err != nil {
		logx.E(ctx, "Failed to get app (appBizId:%d). err:%+v,", req.GetAppBizId(), err)
		return rsp, errs.ErrRobotNotFound

	}
	robotID := app.PrimaryId
	versionID := req.GetVersionId()
	logx.I(ctx, "Prepare to get released count (versionID:%d). releaseType:%s",
		versionID, req.ReleaseType.String())
	switch req.ReleaseType {
	case pb.ReleaseType_ReleaseTypeQA:
		logx.I(ctx, "Release QA is deprecated. for req:%+v", req)
		//if qaSuccCount, err := s.releaseLogic.GetModifyQACount(ctx, robotID, versionID, "", nil,
		//	[]uint32{qaEntity.QAReleaseStatusSuccess}); err != nil {
		//	logx.E(ctx, "Failed to get success released QAs (versionID:%d). err:%+v,", versionID, err)
		//	return nil, err
		//} else {
		//	rsp.SuccessCount = uint32(qaSuccCount)
		//}
		//
		//if qaFailCount, err := s.releaseLogic.GetModifyQACount(ctx, robotID, versionID, "", nil,
		//	[]uint32{qaEntity.QAReleaseStatusFail}); err != nil {
		//	logx.E(ctx, "Failed to get fail released QAs (versionID:%d). err:%+v,", versionID, err)
		//	return nil, err
		//} else {
		//	rsp.FailCount = uint32(qaFailCount)
		//}
		return rsp, nil
	case pb.ReleaseType_ReleaseTypeDocument:
		logx.I(ctx, "Release document is deprecated. for req:%+v", req)
		// 文档不会有失败的情况
		//if docCount, err := s.releaseLogic.GetModifyDocCount(ctx, robotID, versionID,
		//	"", nil, nil); err != nil {
		//	logx.E(ctx, "Failed to get success released Documents (versionID:%d). err:%+v,", versionID, err)
		//	return rsp, err
		//} else {
		//	rsp.SuccessCount = uint32(docCount)
		//	rsp.FailCount = 0
		//}
		return rsp, nil
	case pb.ReleaseType_ReleaseTypeRejectedQuestion:
		corpID := app.CorpPrimaryId
		if rejectQASuccCount, err := s.releaseLogic.GetModifyRejectedQuestionCount(ctx, corpID, robotID, versionID, "",
			[]uint32{qaEntity.RejectedQuestionReleaseStatusSuccess}); err != nil {
			logx.E(ctx, "Failed to get success released rejected questions (versionID:%d). err:%+v,", versionID, err)
			return rsp, err
		} else {
			rsp.SuccessCount = uint32(rejectQASuccCount)
		}

		if rejectQAFailCount, err := s.releaseLogic.GetModifyRejectedQuestionCount(ctx, corpID, robotID, versionID, "",
			[]uint32{qaEntity.RejectedQuestionReleaseStatusFail}); err != nil {
			logx.E(ctx, "Failed to get fail released rejected questions (versionID:%d). err:%+v,", versionID, err)
			return rsp, err
		} else {
			rsp.FailCount = uint32(rejectQAFailCount)
		}
		return rsp, nil
	case pb.ReleaseType_ReleaseTypeLabel:
		if labelCount, err := s.releaseLogic.GetReleaseAttributeCount(ctx, robotID, versionID, "", nil); err != nil {
			logx.E(ctx, "Failed to get success released labels (versionID:%d). err:%+v,", versionID, err)
			return rsp, err
		} else {
			rsp.SuccessCount = uint32(labelCount)
			rsp.FailCount = 0
		}
		return rsp, nil

	case pb.ReleaseType_ReleaseDBType:
		logx.I(ctx, "Release db is deprecated. for req:%+v", req)
		// 数据库发布任务，需要获取数据库发布任务的总数 请求中的VersionID实际是releaseBizID
		//releaseDBSource, err := s.dbLogic.GetAllReleaseDBSources(ctx, req.GetAppBizId(), versionID)
		//if err != nil {
		//	logx.E(ctx, "Failed to get released DB sources (versionBizID:%d). err:%+v,", versionID, err)
		//	return nil, err
		//}
		//releaseTable, err := s.dbLogic.GetAllReleaseDBTables(ctx, req.GetAppBizId(), versionID, true)
		//if err != nil {
		//	logx.E(ctx, "Failed to get released DB tables (versionBizID:%d). err:%+v,", versionID, err)
		//	return nil, err
		//}
		//count := len(releaseDBSource) + len(releaseTable)
		//rsp.SuccessCount = uint32(count)
		//rsp.FailCount = 0
		return rsp, nil
	default:
		logx.E(ctx, "Failed to get released count (versionID:%d). releaseType:%s err:%+v,",
			versionID, req.ReleaseType.String(), err)
		return nil, errs.ErrParamsNotExpected
	}
}

func (s *Service) SendReleaseTaskEvent(ctx context.Context, req *pb.SendReleaseTaskEventReq) (*pb.SendReleaseTaskEventRsp, error) {

	logx.I(ctx, "Prepare to send release task event (req:%+v)", req)
	app, err := s.rpc.AppAdmin.DescribeAppById(ctx, req.GetAppBizId())
	if err != nil {
		return nil, errs.ErrRobotNotFound
	}
	corpID := app.CorpPrimaryId
	corp, err := s.rpc.PlatformAdmin.DescribeCorpByPrimaryId(ctx, corpID)
	if err != nil {
		return nil, errs.ErrCorpNotFound
	}

	event := req.GetEvent()
	releaseType := req.GetReleaseType()
	versionId := req.GetVersionId()
	appBizId := req.GetAppBizId()

	if versionId <= 0 {
		logx.E(ctx, "[SendReleaseTaskEvent] Failed to send release task event (appBizID:%d). versionId:%d caused invalid versionId.", appBizId, versionId)
		return nil, errs.ErrParamsNotExpected
	}

	switch event {
	case entity.TaskConfigEventPrepare:
		if err := s.releaseLogic.ReleasePrepare(ctx, uint32(releaseType), corp.CorpId, app.BizId, req.GetVersionId()); err != nil {
			logx.E(ctx, "[SendReleaseTaskEvent] Failed to Release_Prepare(releaseType:%s)"+
				" (appBizID:%d, versionId:%d). err:%+v,", releaseType.String(), appBizId, versionId, err)
			return nil, err
		}
	case entity.TaskConfigEventCollect:
		// 收到COLLECT事件，生成发布任务
		if err := s.releaseLogic.ReleaseCollect(ctx, uint32(releaseType), app.CorpPrimaryId, app.PrimaryId, app.BizId, req.GetVersionId()); err != nil {
			logx.E(ctx, "[SendReleaseTaskEvent] Failed to Release_Collect(releaseType:%s)"+
				" (appBizID:%d, versionId:%d). err:%+v,", releaseType.String(), appBizId, versionId, err)
			return nil, err
		}
	case entity.TaskConfigEventRelease, entity.TaskConfigEventRetry:
		// 收到RELEASE事件，执行发布任务
		if err := s.releaseLogic.ReleaseRelease(ctx, uint32(releaseType), app.CorpPrimaryId, corp.CorpId,
			app.PrimaryId, app.BizId, req.GetVersionId()); err != nil {
			logx.E(ctx, "[SendReleaseTaskEvent] event:%s Failed to Release_Release(releaseType:%s)"+
				" (appBizID:%d, versionId:%d). err:%+v,", req.Event, releaseType.String(), appBizId, versionId, err)
			return nil, err
		}
	case entity.TaskConfigEventRollback:
		// 收到ROLLBACK事件，回滚知识库配置到指定版本 versionId
		logx.I(ctx, "[SendReleaseTaskEvent] ROLLBACK config appBizID:%d, versionId:%d", appBizId, versionId)

		if err := s.releaseLogic.RollbackKbConfig(ctx, corp.CorpPrimaryId, corp.CorpId, appBizId, versionId); err != nil {
			logx.E(ctx, "[SendReleaseTaskEvent] ROLLBACK config appBizID:%d, versionId:%d, error:%v", appBizId, versionId, err)
			return nil, err
		}
		logx.I(ctx, "[SendReleaseTaskEvent] ROLLBACK config success, appBizID:%d, versionId:%d", appBizId, versionId)
	default:
		// 暂停事件不处理
		logx.I(ctx, "[SendReleaseTaskEvent] receive event %v, ignore", req.Event)

	}
	return &pb.SendReleaseTaskEventRsp{}, nil
}

func (s *Service) DescribeNotifyNum(ctx context.Context, req *pb.DescribeNotifyNumReq) (*pb.DescribeNotifyNumRsp, error) {
	logx.I(ctx, "[DescribeNotifyNum] DescribeNotifyNumReq: %+v", req)
	rsp := new(pb.DescribeNotifyNumRsp)
	corpPrimaryId := contextx.Metadata(ctx).CorpID()
	staffID := contextx.Metadata(ctx).StaffID()
	appBizId := req.GetAppId()
	app, err := s.rpc.AppAdmin.DescribeAppById(ctx, appBizId)
	if err != nil {
		return nil, err
	}

	if app.CorpPrimaryId != corpPrimaryId || app.StaffID != staffID {
		logx.I(ctx, "[DescribeNotifyNum] mdCorp:%d appCorp:%d mdStaff:%d, appStaff:%d no permission to access appBizId:%d", corpPrimaryId, app.CorpPrimaryId, staffID, app.StaffID, appBizId)
		// return nil, errs.ErrPermissionDenied
	}

	switch req.GetType() {
	case pb.NotifyType_NotifyType_Qa:
		isExisted, err := s.qaLogic.HasUnconfirmedQa(ctx, corpPrimaryId, staffID, app.PrimaryId)
		if err != nil {
			return nil, err
		}
		total, err := s.qaLogic.GetUnconfirmedQaNum(ctx, corpPrimaryId, app.PrimaryId)
		if err != nil {
			return nil, err
		}
		rsp.Count = uint32(total)
		rsp.Existed = isExisted
	case pb.NotifyType_NotifyType_QaTask:
		total, err := s.taskLogic.GetNoticeQATaskNum(ctx, corpPrimaryId, app.PrimaryId)
		if err != nil {
			return nil, err
		}
		rsp.Count = uint32(total)
		rsp.Existed = total > 0
	default:
		return nil, errs.ErrParameterInvalid
	}
	return rsp, nil
}

func (s *Service) GetAuditQAFailByQaID(ctx context.Context, req *pb.GetAuditQAFailByQaIDReq) (*pb.GetAuditQAFailByQaIDRsp, error) {
	rsp := new(pb.GetAuditQAFailByQaIDRsp)
	qaIds, err := s.releaseDao.GetAuditQAFailByQaID(ctx, req.GetCorpId(), req.GetQaId())
	if err != nil {
		logx.E(ctx, "GetAuditQAFailByQaID failed  (qaID:%d). err:%+v,", req.GetQaId(), err)
		return rsp, err
	}
	rsp.FailIds = qaIds
	logx.I(ctx, "GetAuditQAFailByQaID rsp = %+v", rsp)
	return rsp, nil
}

func (s *Service) GetAuditQAFailByVersion(ctx context.Context, req *pb.GetAuditQAFailByVersionReq) (*pb.GetAuditQAFailByVersionRsp, error) {
	rsp := new(pb.GetAuditQAFailByVersionRsp)
	modifyQas, err := s.releaseDao.GetAuditQAFailByVersion(ctx, req.GetCorpId(), req.GetVersionId())
	if err != nil {
		return nil, err
	}
	qaIDs := make([]uint64, 0, len(modifyQas))
	for _, v := range modifyQas {
		qaIDs = append(qaIDs, v.QaID)
	}
	docQAs, err := s.qaLogic.GetQADetailsByReleaseStatus(ctx, req.GetCorpId(), req.GetRobotId(), qaIDs, qaEntity.QAReleaseStatusAuditNotPass)
	if err != nil {
		logx.E(ctx, "GetQADetailsByReleaseStatus  failed  err:%+v", err)
		return rsp, err
	}
	rsp.QaFailMap = docQAMapToPb(docQAs)
	details, err := s.qaLogic.GetSimilarQADetailsByReleaseStatus(ctx, req.GetCorpId(), req.GetRobotId(), qaIDs, qaEntity.QAReleaseStatusAuditNotPass)
	rsp.SimilarQaFailMap = similarQuestionMapToPb(details)
	logx.I(ctx, "GetAuditQAFailByQaID rsp = %+v", rsp)
	return rsp, nil
}

func similarQuestionMapToPb(sqMap map[uint64]*qaEntity.SimilarQuestion) map[uint64]*pb.DocSimilarQA {
	if len(sqMap) == 0 {
		return nil
	}
	result := make(map[uint64]*pb.DocSimilarQA, len(sqMap))
	for id, sq := range sqMap {
		if sq == nil {
			continue
		}
		if docSQ := SimilarQuestionToDocSimilarQA(sq); docSQ != nil {
			result[id] = docSQ
		}
	}
	return result
}

// SimilarQuestionToDocSimilarQA 将SimilarQuestion结构体转换为DocSimilarQA proto消息
func SimilarQuestionToDocSimilarQA(sq *qaEntity.SimilarQuestion) *pb.DocSimilarQA {
	if sq == nil {
		return nil
	}
	return &pb.DocSimilarQA{
		Id:            sq.ID,
		SimilarId:     sq.SimilarID,
		CorpId:        sq.CorpID,
		StaffId:       sq.StaffID,
		RobotId:       sq.RobotID,
		RelatedQaId:   sq.RelatedQAID,
		Source:        sq.Source,
		Question:      sq.Question,
		ReleaseStatus: sq.ReleaseStatus,
		NextAction:    sq.NextAction,
		CharSize:      sq.CharSize,
		IsAuditFree:   sq.IsAuditFree,
		Message:       sq.Message,
		IsDeleted:     int32(sq.IsDeleted), // uint32转int32
		CreateTime:    sq.CreateTime.Unix(),
		UpdateTime:    sq.UpdateTime.Unix(),
	}
}

// DocQAMapToQAInfoMap 将map[uint64]*DocQA转换为map[uint64]*QAInfo
func docQAMapToPb(docQAMap map[uint64]*qaEntity.DocQA) map[uint64]*pb.QAInfo {
	if len(docQAMap) == 0 {
		return nil
	}
	result := make(map[uint64]*pb.QAInfo, len(docQAMap))
	for id, docQA := range docQAMap {
		if docQA == nil {
			continue
		}
		if qaInfo := docQAToPbInfo(docQA); qaInfo != nil {
			result[id] = qaInfo
		}
	}
	return result
}

// DocQAToPbInfo 将DocQA结构体转换为QAInfo proto消息
func docQAToPbInfo(docQA *qaEntity.DocQA) *pb.QAInfo {
	if docQA == nil {
		return nil
	}
	return &pb.QAInfo{
		Id:            docQA.ID,
		BusinessId:    docQA.BusinessID,
		RobotId:       docQA.RobotID,
		CorpId:        docQA.CorpID,
		StaffId:       docQA.StaffID,
		DocId:         docQA.DocID,
		OriginDocId:   docQA.OriginDocID,
		SegmentId:     docQA.SegmentID,
		CategoryId:    docQA.CategoryID,
		Source:        docQA.Source,
		Question:      docQA.Question,
		Answer:        docQA.Answer,
		CustomParam:   docQA.CustomParam,
		QuestionDesc:  docQA.QuestionDesc,
		ReleaseStatus: docQA.ReleaseStatus,
		IsAuditFree:   docQA.IsAuditFree,
		IsDeleted:     int32(docQA.IsDeleted), // uint32转int32
		Message:       docQA.Message,
		AcceptStatus:  docQA.AcceptStatus,
		SimilarStatus: docQA.SimilarStatus,
		NextAction:    docQA.NextAction,
		CharSize:      docQA.CharSize,
		AttrRange:     docQA.AttrRange,
		CreateTime:    docQA.CreateTime.Unix(),
		UpdateTime:    docQA.UpdateTime.Unix(),
		ExpireStart:   docQA.ExpireStart.Unix(),
		ExpireEnd:     docQA.ExpireEnd.Unix(),
	}
}
