package service

import (
	"context"
	"errors"
	"fmt"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/knowledge_base"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_common"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
	"strconv"
	"sync"
)

var (
	docExceedStatus = []uint32{
		model.DocStatusCharExceeded,
		model.DocStatusResuming,
		model.DocStatusParseImportFailCharExceeded,
		model.DocStatusAuditFailCharExceeded,
		model.DocStatusUpdateFailCharExceeded,
		model.DocStatusCreateIndexFailCharExceeded,
		model.DocStatusParseImportFailResuming,
		model.DocStatusAuditFailResuming,
		model.DocStatusUpdateFailResuming,
		model.DocStatusCreateIndexFailResuming,
		model.DocStatusExpiredCharExceeded,
		model.DocStatusExpiredResuming,
		model.DocStatusAppealFailedCharExceeded,
		model.DocStatusAppealFailedResuming,
	}
	qaExceedStatus = []uint32{
		model.QAReleaseStatusCharExceeded,
		model.QAReleaseStatusResuming,
		model.QAReleaseStatusAppealFailCharExceeded,
		model.QAReleaseStatusAppealFailResuming,
		model.QAReleaseStatusAuditNotPassCharExceeded,
		model.QAReleaseStatusAuditNotPassResuming,
		model.QAReleaseStatusLearnFailCharExceeded,
		model.QAReleaseStatusLearnFailResuming}

	docExceedResumingStatus = []uint32{
		model.DocStatusResuming,
		model.DocStatusParseImportFailResuming,
		model.DocStatusAuditFailResuming,
		model.DocStatusUpdateFailResuming,
		model.DocStatusCreateIndexFailResuming,
		model.DocStatusExpiredResuming,
		model.DocStatusAppealFailedResuming,
	}
	qaExceedResumingStatus = []uint32{
		model.QAReleaseStatusResuming,
		model.QAReleaseStatusAppealFailResuming,
		model.QAReleaseStatusAuditNotPassResuming,
		model.QAReleaseStatusLearnFailResuming}
	allKeyPermission = uint64(1)
)

// DescribeKnowledgeBase 查询知识库信息
func (s *Service) DescribeKnowledgeBase(ctx context.Context, req *pb.DescribeKnowledgeBaseReq) (
	*pb.DescribeKnowledgeBaseRsp, error) {
	return knowledge_base.DescribeKnowledgeBase(ctx, req)
}

func (s *Service) DescribeExceededKnowledgeList(ctx context.Context,
	req *pb.DescribeExceededKnowledgeListReq) (*pb.DescribeExceededKnowledgeListRsp, error) {
	// 获取SpaceID下的所有DOC
	corpID := pkg.CorpID(ctx)
	// 拉取所有的robot id
	apps, err := dao.GetRobotDao().GetAllValidApps(ctx, []string{dao.RobotTblColId, dao.RobotTblColBusinessId,
		dao.RobotTblColName, dao.RobotTblColIsShared},
		&dao.RobotFilter{
			CorpId:    corpID,
			SpaceID:   req.GetSpaceId(),
			IsDeleted: pkg.GetIntPtr(dao.IsNotDeleted),
		},
	)
	if err != nil {
		log.ErrorContextf(ctx, "GetAllValidApps failed, err: %+v", err)
		return nil, errs.ErrDescribeExceededKnowledgeListFailed
	}
	var robotIDs []uint64
	for _, app := range apps {
		robotIDs = append(robotIDs, app.ID)
	}
	// 从t_doc表拿到corpsID对应的所有的超量文件对应的robotID及对应的大小
	appDocExceedCharSizeMap, err := s.dao.GetRobotDocExceedCharSize(ctx, corpID, robotIDs)
	if err != nil {
		log.ErrorContextf(ctx, "GetRobotDocExceedCharSize failed, err: %+v", err)
		return nil, errs.ErrDescribeExceededKnowledgeListFailed
	}
	appQAExceedCharSizeMap, err := s.dao.GetRobotQAExceedCharSize(ctx, corpID, robotIDs)
	if err != nil {
		log.ErrorContextf(ctx, "GetRobotQAExceedCharSize failed, err: %+v", err)
		return nil, errs.ErrDescribeExceededKnowledgeListFailed
	}
	isResuming, err := s.isResuming(ctx, robotIDs, corpID)
	if err != nil {
		log.ErrorContextf(ctx, "isResuming failed, err: %+v", err)
		return nil, errs.ErrDescribeExceededKnowledgeListFailed
	}
	// 获取所有应用的权限信息
	resPermissionMap, err := s.describeAllResPermission(ctx, req.GetSpaceId())
	if err != nil {
		log.ErrorContextf(ctx, "describeAllResPermission failed, err: %+v", err)
		return nil, errs.ErrDescribeExceededKnowledgeListFailed
	}
	var fApps []*pb.ExceededKnowledgeDetail
	for _, app := range apps {
		// 过滤掉没有编辑权限的应用
		if !s.hasPermission(app.BusinessID, resPermissionMap) {
			log.InfoContextf(ctx, "app.BusinessID has no edit permission: %d", app.BusinessID)
			continue
		}
		size := appDocExceedCharSizeMap[app.ID] + appQAExceedCharSizeMap[app.ID]
		if size == 0 {
			log.InfoContextf(ctx, "app.BusinessID has no exceeded knowledge: %d", app.BusinessID)
			continue
		}
		kd := pb.ExceededKnowledgeDetail{
			AppName:        app.Name,
			ExceedCharSize: size,
			Id:             strconv.FormatUint(app.BusinessID, 10),
		}
		if appDocExceedCharSizeMap[app.ID] != 0 {
			kd.KnowledgeSubType = pb.ResumeKnowledgeType_RESUME_KNOWLEDGE_TYPE_DOC
		} else {
			kd.KnowledgeSubType = pb.ResumeKnowledgeType_RESUME_KNOWLEDGE_TYPE_QA
		}
		if app.IsShared {
			kd.KnowledgeType = pb.KnowledgeType_SharedKnowledge
		} else {
			kd.KnowledgeType = pb.KnowledgeType_AppDefaultKnowledge
		}
		if isResuming[app.ID] {
			kd.State = pb.ExceededKnowledgeTypeState_EXCEEDED_KNOWLEDGE_TYPE_STATE_RESUMING
		} else {
			kd.State = pb.ExceededKnowledgeTypeState_EXCEEDED_KNOWLEDGE_TYPE_STATE_EXCEED
		}
		fApps = append(fApps, &kd)
	}

	rsp := pb.DescribeExceededKnowledgeListRsp{}
	rsp.Total = uint32(len(fApps))
	if rsp.Total != 0 {
		pApps, err := s.getPaginatedData(int(req.GetPageNumber()), int(req.GetPageSize()), fApps)
		if err != nil {
			log.ErrorContextf(ctx, "getPaginatedData failed, err: %+v", err)
			return &rsp, errs.ErrDescribeExceededKnowledgeListFailed
		}
		rsp.List = append(rsp.List, pApps...)
	}
	return &rsp, nil
}

func (s *Service) getPaginatedData(pageNumber, pageSize int, app []*pb.ExceededKnowledgeDetail) ([]*pb.
	ExceededKnowledgeDetail, error) {
	startIndex := (pageNumber - 1) * pageSize
	if startIndex >= len(app) {
		return nil, fmt.Errorf("startIndex >= len(app), startIndex: %d, len(app): %d", startIndex, len(app))
	}
	endIndex := startIndex + pageSize
	if endIndex > len(app) {
		endIndex = len(app)
	}
	if endIndex < startIndex {
		return nil, fmt.Errorf("endIndex < startIndex, endIndex: %d, startIndex: %d", endIndex, startIndex)
	}
	paginatedData := app[startIndex:endIndex]
	return paginatedData, nil
}

func (s *Service) isResuming(ctx context.Context, appIDs []uint64, corpID uint64) (map[uint64]bool, error) {
	docs, err := dao.GetDocDao().GetAllDocs(ctx, []string{dao.DocTblColRobotId},
		&dao.DocFilter{
			RobotIDs: appIDs,
			CorpId:   corpID,
			Limit:    1,
			Status:   docExceedResumingStatus,
		})
	if err != nil {
		return nil, fmt.Errorf("GetAllDocs failed, err: %w", err)
	}
	isResumingMap := make(map[uint64]bool)
	for _, doc := range docs {
		if !isResumingMap[doc.RobotID] {
			isResumingMap[doc.RobotID] = true
		}
	}
	qas, err := dao.GetDocQaDao().GetAllDocQas(ctx, []string{dao.DocQaTblColRobotId},
		&dao.DocQaFilter{
			RobotIDs:      appIDs,
			CorpId:        corpID,
			Limit:         1,
			ReleaseStatus: qaExceedResumingStatus,
		})
	if err != nil {
		return nil, fmt.Errorf("GetAllQas failed, err: %w", err)
	}
	for _, qa := range qas {
		if !isResumingMap[qa.RobotID] {
			isResumingMap[qa.RobotID] = true
		}
	}
	return isResumingMap, nil
}

func (s *Service) describeDocsByAppId(ctx context.Context, appID string, corpID uint64) (*pb.ResumeDocReq, error) {
	// 获取knowledge_base_id对应的robot_id
	baseID, err := util.CheckReqParamsIsUint64(ctx, appID)
	if err != nil {
		return nil, fmt.Errorf("CheckReqParamsIsUint64 failed, err: %w", err)
	}
	app, err := s.dao.GetAppByAppBizID(ctx, baseID)
	if err != nil {
		return nil, fmt.Errorf("GetAppByAppBizID failed, err: %w", err)
	}
	if app == nil {
		return nil, errors.New("GetAppByAppBizID failed, app == nil")
	}
	// 知识库文件ID为空，知识库ID不为空，则恢复知识库下面所有的超量失效的文件
	// 获取知识库ID对应的所有doc_biz_id
	docs, err := dao.GetDocDao().GetAllDocs(ctx, []string{dao.DocTblColBusinessId},
		&dao.DocFilter{
			RobotId:        app.ID,
			CorpId:         corpID,
			Limit:          0,
			Status:         docExceedStatus,
			OrderColumn:    []string{dao.RobotTblColId},
			OrderDirection: []string{dao.SqlOrderByAsc},
		})
	if err != nil {
		return nil, fmt.Errorf("GetAllDocs failed, err: %w", err)
	}
	var docIDs []string
	for _, doc := range docs {
		docIDs = append(docIDs, strconv.FormatUint(doc.BusinessID, 10))
	}
	return &pb.ResumeDocReq{
		BotBizId:  appID,
		DocBizIds: docIDs,
	}, nil
}

func (s *Service) describeDocQasByAppId(ctx context.Context, appID string, corpID uint64) (*pb.ResumeQAReq, error) {
	// 获取knowledge_base_id对应的robot_id
	baseID, err := util.CheckReqParamsIsUint64(ctx, appID)
	if err != nil {
		return nil, fmt.Errorf("CheckReqParamsIsUint64 failed, err: %w", err)
	}
	app, err := s.dao.GetAppByAppBizID(ctx, baseID)
	if err != nil {
		return nil, fmt.Errorf("GetAppByAppBizID failed, err: %w", err)
	}
	if app == nil {
		return nil, errors.New("GetAppByAppBizID failed, app == nil")
	}
	// 获取问答对应的所有qa_biz_id
	docQas, err := dao.GetDocQaDao().GetAllDocQas(ctx, []string{dao.DocQaTblColBusinessId},
		&dao.DocQaFilter{
			RobotId:        app.ID,
			CorpId:         corpID,
			Limit:          0,
			ReleaseStatus:  qaExceedStatus,
			OrderColumn:    []string{dao.DocQaTblColId},
			OrderDirection: []string{dao.SqlOrderByAsc},
			IsDeleted:      pkg.GetIntPtr(model.QAIsNotDeleted),
		})
	if err != nil {
		return nil, fmt.Errorf("GetAllDocQas failed, err: %w", err)
	}
	var qaIDs []string
	for _, docQa := range docQas {
		qaIDs = append(qaIDs, strconv.FormatUint(docQa.BusinessID, 10))
	}
	return &pb.ResumeQAReq{
		BotBizId: appID,
		QaBizIds: qaIDs,
	}, nil
}

func (s *Service) describeDocBySpaceID(ctx context.Context, spaceID string, corpID uint64) ([]*pb.ResumeDocReq, error) {
	// 拉取所有的robot id
	apps, err := dao.GetRobotDao().GetAllValidApps(ctx, []string{dao.RobotTblColId, dao.RobotTblColBusinessId},
		&dao.RobotFilter{
			CorpId:    corpID,
			SpaceID:   spaceID,
			IsDeleted: pkg.GetIntPtr(dao.IsNotDeleted),
		},
	)
	if err != nil {
		return nil, fmt.Errorf("GetAllValidApps failed, err: %w", err)
	}
	botIDToBizID := make(map[uint64]uint64)
	var robotIDs []uint64

	resPermissionMap, err := s.describeAllResPermission(ctx, spaceID)
	if err != nil {
		return nil, fmt.Errorf("describeAllResPermission failed, err: %w", err)
	}
	for _, app := range apps {
		// 过滤掉没有编辑权限的应用
		if !s.hasPermission(app.BusinessID, resPermissionMap) {
			log.InfoContextf(ctx, "app.BusinessID has no edit permission: %d", app.BusinessID)
			continue
		}
		robotIDs = append(robotIDs, app.ID)
		botIDToBizID[app.ID] = app.BusinessID
	}
	// 拉取当前空间下所有超量的DOC
	docs, err := dao.GetDocDao().GetAllDocs(ctx, []string{dao.DocTblColBusinessId, dao.DocTblColRobotId},
		&dao.DocFilter{
			RobotIDs:       robotIDs,
			Limit:          0,
			Status:         docExceedStatus,
			OrderColumn:    []string{dao.RobotTblColId},
			OrderDirection: []string{dao.SqlOrderByAsc},
			IsDeleted:      pkg.GetIntPtr(dao.IsNotDeleted),
		})
	if err != nil {
		return nil, fmt.Errorf("GetAllDocs failed, err: %w", err)
	}
	appBizDocIDs := make(map[uint64][]string)
	for _, doc := range docs {
		if botBizID, ok := botIDToBizID[doc.RobotID]; ok {
			appBizDocIDs[botBizID] = append(appBizDocIDs[botBizID], strconv.FormatUint(doc.BusinessID, 10))
		}
	}
	var docsInfo []*pb.ResumeDocReq
	for k, v := range appBizDocIDs {
		docsInfo = append(docsInfo, &pb.ResumeDocReq{
			BotBizId:  strconv.FormatUint(k, 10),
			DocBizIds: v,
		})
	}
	return docsInfo, nil
}

func (s *Service) describeDocQasBySpaceID(ctx context.Context, spaceID string, corpID uint64) ([]*pb.ResumeQAReq,
	error) {
	// 拉取所有的robot id
	apps, err := dao.GetRobotDao().GetAllValidApps(ctx, []string{dao.RobotTblColId, dao.RobotTblColBusinessId},
		&dao.RobotFilter{
			CorpId:    corpID,
			SpaceID:   spaceID,
			IsDeleted: pkg.GetIntPtr(dao.IsNotDeleted),
		},
	)
	if err != nil {
		return nil, fmt.Errorf("GetAllValidApps failed, err: %w", err)
	}

	resPermissionMap, err := s.describeAllResPermission(ctx, spaceID)
	if err != nil {
		return nil, fmt.Errorf("describeAllResPermission failed, err: %w", err)
	}

	botIDToBizID := make(map[uint64]uint64)
	var robotIDs []uint64
	for _, app := range apps {
		// 过滤掉没有编辑权限的应用
		if !s.hasPermission(app.BusinessID, resPermissionMap) {
			log.InfoContextf(ctx, "app.BusinessID has no edit permission: %d", app.BusinessID)
			continue
		}
		robotIDs = append(robotIDs, app.ID)
		botIDToBizID[app.ID] = app.BusinessID
	}
	// 拉取当前空间下所有超量的QA
	docQas, err := dao.GetDocQaDao().GetAllDocQas(ctx, []string{dao.DocQaTblColBusinessId, dao.DocQaTblColRobotId},
		&dao.DocQaFilter{
			RobotIDs:       robotIDs,
			Limit:          0,
			ReleaseStatus:  qaExceedStatus,
			OrderColumn:    []string{dao.DocQaTblColId},
			OrderDirection: []string{dao.SqlOrderByAsc},
			IsDeleted:      pkg.GetIntPtr(model.QAIsNotDeleted),
		})
	if err != nil {
		return nil, fmt.Errorf("GetAllDocQas failed, err: %w", err)
	}
	appBizQaIDs := make(map[uint64][]string)
	for _, qa := range docQas {
		if botBizID, ok := botIDToBizID[qa.RobotID]; ok {
			appBizQaIDs[botBizID] = append(appBizQaIDs[botBizID], strconv.FormatUint(qa.BusinessID, 10))
		}
	}
	var qasInfo []*pb.ResumeQAReq
	for k, v := range appBizQaIDs {
		qasInfo = append(qasInfo, &pb.ResumeQAReq{
			BotBizId: strconv.FormatUint(k, 10),
			QaBizIds: v,
		})
	}
	return qasInfo, nil
}

// ResumeExceedKnowledge 恢复知识库
func (s *Service) ResumeExceedKnowledge(ctx context.Context, req *pb.ResumeExceedKnowledgeReq) (
	*pb.ResumeExceedKnowledgeRsp, error) {
	corpID := pkg.CorpID(ctx)
	// 知识库文件ID不为空，则只恢复知识库文件
	var docsInfo []*pb.ResumeDocReq
	var qasInfo []*pb.ResumeQAReq
	for _, knowledgeBizID := range req.GetKnowledgeInfos() {
		if len(knowledgeBizID.GetIds()) == 0 {
			// 知识库文件ID为空，则恢复知识库下的所有文件
			if len(knowledgeBizID.GetKnowledgeBaseId()) == 0 {
				log.WarnContextf(ctx, "knowledgeBizID.GetKnowledgeBaseId() is empty, knowledgeBizID:%+v",
					knowledgeBizID)
				continue
			}
			docsI, docQasI, err := s.describeReqKnowledgeIDs(ctx, knowledgeBizID.GetKnowledgeBaseId(), corpID)
			if err != nil {
				log.ErrorContextf(ctx, "describeReqKnowledgeIDs failed, err: %+v", err)
				return nil, errs.ErrResumeExceedKnowledgeFailed
			}
			if docsI != nil {
				docsInfo = append(docsInfo, docsI)
			}
			if docQasI != nil {
				qasInfo = append(qasInfo, docQasI)
			}
		} else {
			// 知识库文件ID不为空，则只恢复指定知识库文件
			switch knowledgeBizID.Type {
			case pb.ResumeKnowledgeType_RESUME_KNOWLEDGE_TYPE_DOC:
				docsInfo = append(docsInfo, &pb.ResumeDocReq{
					BotBizId:  knowledgeBizID.GetKnowledgeBaseId(),
					DocBizIds: knowledgeBizID.GetIds(),
				})
			case pb.ResumeKnowledgeType_RESUME_KNOWLEDGE_TYPE_QA:
				qasInfo = append(qasInfo, &pb.ResumeQAReq{
					BotBizId: knowledgeBizID.GetKnowledgeBaseId(),
					QaBizIds: knowledgeBizID.GetIds(),
				})
			}
		}
	}
	if len(req.GetKnowledgeInfos()) == 0 {
		// 拉取当前空间下所有超量的DOC
		dI, err := s.describeDocBySpaceID(ctx, req.GetSpaceId(), corpID)
		if err != nil {
			log.ErrorContextf(ctx, "getRobotIDsBySpaceID failed, err: %+v", err)
			return nil, errs.ErrResumeExceedKnowledgeFailed
		}
		docsInfo = append(docsInfo, dI...)
		// 拉取当前空间下所有超量的QA
		qI, err := s.describeDocQasBySpaceID(ctx, req.GetSpaceId(), corpID)
		if err != nil {
			log.ErrorContextf(ctx, "getRobotIDsBySpaceID failed, err: %+v", err)
			return nil, errs.ErrResumeExceedKnowledgeFailed
		}
		qasInfo = append(qasInfo, qI...)
	}
	// 如果没有可恢复的知识库文件，则直接返回
	if len(docsInfo) == 0 && len(qasInfo) == 0 {
		log.ErrorContextf(ctx, "docsInfo and qasInfo is empty, req:%+v", req)
		rsp := new(pb.ResumeExceedKnowledgeRsp)
		return rsp, nil
	}

	// 对docBizIds做拆分，防止单个docBizIds过大
	splitDocsInfo := processDocsInfoEnhanced(docsInfo, config.DescribeResumeBatchSize())

	// 分批调用ResumeDoc
	if err := s.processResumeDocReqs(ctx, splitDocsInfo); err != nil {
		log.ErrorContextf(ctx, "ProcessResumeDocReqs failed, err: %+v", err)
		return nil, errs.ErrResumeExceedKnowledgeFailed
	}
	// 对qaBizIds做拆分，防止单个qaBizIds过大
	splitQasInfo := processQAsInfoWithMaxLength(qasInfo, config.DescribeResumeBatchSize())
	// 分批调用ResumeQA
	if err := s.processResumeDocQaReqs(ctx, splitQasInfo); err != nil {
		log.ErrorContextf(ctx, "ProcessResumeDocQaReqs failed, err: %+v", err)
		return nil, errs.ErrResumeExceedKnowledgeFailed
	}
	rsp := new(pb.ResumeExceedKnowledgeRsp)
	return rsp, nil
}

func (s *Service) hasPermission(bizID uint64, resPermissionMap map[uint64]bool) bool {
	if _, ok := resPermissionMap[bizID]; ok {
		return true
	}
	if _, ok := resPermissionMap[allKeyPermission]; ok {
		return true
	}
	return false
}

// 获取所有超量知识库的权限信息
func (s *Service) describeAllResPermission(ctx context.Context, spaceID string) (map[uint64]bool, error) {
	resPermissionMap := make(map[uint64]bool)
	app, err := s.describeResPermission(ctx, spaceID, bot_common.ResourceType_ResourceTypeApp)
	if err != nil {
		return resPermissionMap, err
	}
	for k, v := range app {
		resPermissionMap[k] = v
	}
	knowledge, err := s.describeResPermission(ctx, spaceID, bot_common.ResourceType_ResourceTypeKnowledge)
	if err != nil {
		return resPermissionMap, err
	}
	for k, v := range knowledge {
		resPermissionMap[k] = v
	}
	return resPermissionMap, nil
}

// 获取知识库的权限信息
func (s *Service) describeResPermission(ctx context.Context, spaceID string, resourceType bot_common.ResourceType) (map[uint64]bool, error) {
	resPermissionMap := make(map[uint64]bool)
	// 获取权限信息
	hasAllResourcePerm, otherAllPermissionIDs, shareKnowledgeBizIDList, mapShareKnowledgeBizIDs, err :=
		s.getUserResource(ctx, spaceID, resourceType)
	if err != nil {
		return resPermissionMap, err
	}
	permissions := config.DescribePermissionIDs()
	if hasAllResourcePerm {
		if len(otherAllPermissionIDs) != 0 {
			for _, permissionID := range otherAllPermissionIDs {
				if ok, _ := permissions[permissionID]; ok {
					resPermissionMap[allKeyPermission] = true
				}
			}
		}
	}
	for k, v := range mapShareKnowledgeBizIDs {
		for _, i := range v {
			if _, ok := permissions[i]; ok {
				resPermissionMap[k] = true
			}
		}
	}
	log.DebugContextf(ctx, "hasAllResourcePerm:%v, otherAllPermissionIDs:%v, "+
		"shareKnowledgeBizIDList:%v, mapShareKnowledgeBizIDs:%v", hasAllResourcePerm,
		otherAllPermissionIDs, shareKnowledgeBizIDList, mapShareKnowledgeBizIDs)

	log.DebugContextf(ctx, "resPermissionMap:%v", resPermissionMap)
	return resPermissionMap, nil
}

func (s *Service) describeReqKnowledgeIDs(ctx context.Context, knowledgeBaseID string,
	corpID uint64) (*pb.ResumeDocReq, *pb.ResumeQAReq, error) {
	docInfo, err := s.describeDocsByAppId(ctx, knowledgeBaseID, corpID)
	if err != nil {
		return nil, nil, fmt.Errorf("describeDocByAppId failed, err %w", err)
	}
	qaInfo, err := s.describeDocQasByAppId(ctx, knowledgeBaseID, corpID)
	if err != nil {
		return nil, nil, fmt.Errorf("describeDocQasByAppId failed, err %w", err)
	}
	return docInfo, qaInfo, nil
}

func (s *Service) processResumeDocReqs(ctx context.Context, requests []*pb.ResumeDocReq) error {
	// 创建带缓冲的通道，用于控制并发数量
	semaphore := make(chan struct{}, config.DescribeExceedKnowledgeResumeCon())
	var wg sync.WaitGroup
	var mu sync.Mutex
	var errors []error
	// 处理单个请求的协程函数
	processOne := func(req *pb.ResumeDocReq) {
		defer wg.Done()
		// 获取信号量（如果缓冲区满，则阻塞等待）
		semaphore <- struct{}{}
		defer func() {
			// 释放信号量
			<-semaphore
		}()
		// 执行实际处理
		if _, err := s.ResumeDoc(ctx, req); err != nil {
			mu.Lock()
			errors = append(errors, err)
			mu.Unlock()
		}
	}
	// 启动协程处理每个请求
	for _, req := range requests {
		wg.Add(1)
		go processOne(req)
	}
	// 等待所有协程完成
	wg.Wait()
	// 如果有错误，返回第一个错误
	if len(errors) > 0 {
		return errors[0]
	}
	return nil
}

func (s *Service) processResumeDocQaReqs(ctx context.Context, requests []*pb.ResumeQAReq) error {
	// 创建带缓冲的通道，用于控制并发数量
	semaphore := make(chan struct{}, config.DescribeExceedKnowledgeResumeCon())
	var wg sync.WaitGroup
	var mu sync.Mutex
	var errors []error
	// 处理单个请求的协程函数
	processOne := func(req *pb.ResumeQAReq) {
		defer wg.Done()
		// 获取信号量（如果缓冲区满，则阻塞等待）
		semaphore <- struct{}{}
		defer func() {
			// 释放信号量
			<-semaphore
		}()
		// 执行实际处理
		if _, err := s.ResumeQA(ctx, req); err != nil {
			mu.Lock()
			errors = append(errors, err)
			mu.Unlock()
		}
	}
	// 启动协程处理每个请求
	for _, req := range requests {
		wg.Add(1)
		go processOne(req)
	}
	// 等待所有协程完成
	wg.Wait()
	// 如果有错误，返回第一个错误
	if len(errors) > 0 {
		return errors[0]
	}
	return nil
}

func processDocsInfoEnhanced(docsInfo []*pb.ResumeDocReq, maxLength int) []*pb.ResumeDocReq {
	if maxLength == 0 {
		maxLength = 1000
	}
	var result []*pb.ResumeDocReq

	for _, doc := range docsInfo {
		if len(doc.DocBizIds) <= maxLength {
			result = append(result, doc)
			continue
		}
		// 拆分逻辑
		chunkCount := (len(doc.DocBizIds) + maxLength - 1) / maxLength // 计算需要拆分成几份
		for i := 0; i < chunkCount; i++ {
			start := i * maxLength
			end := start + maxLength
			if end > len(doc.DocBizIds) {
				end = len(doc.DocBizIds)
			}
			newDoc := &pb.ResumeDocReq{
				BotBizId:  doc.BotBizId,
				DocBizIds: make([]string, end-start),
			}
			copy(newDoc.DocBizIds, doc.DocBizIds[start:end])
			result = append(result, newDoc)
		}
	}
	return result
}

// 可配置最大长度的处理函数
func processQAsInfoWithMaxLength(qasInfo []*pb.ResumeQAReq, maxLength int) []*pb.ResumeQAReq {
	if maxLength == 0 {
		maxLength = 1000
	}
	var result []*pb.ResumeQAReq
	for _, qa := range qasInfo {
		if len(qa.QaBizIds) <= maxLength {
			result = append(result, qa)
			continue
		}

		chunkCount := (len(qa.QaBizIds) + maxLength - 1) / maxLength

		for i := 0; i < chunkCount; i++ {
			start := i * maxLength
			end := start + maxLength
			if end > len(qa.QaBizIds) {
				end = len(qa.QaBizIds)
			}

			newQA := &pb.ResumeQAReq{
				BotBizId: qa.BotBizId,
				QaBizIds: make([]string, end-start),
			}
			copy(newQA.QaBizIds, qa.QaBizIds[start:end])

			result = append(result, newQA)
		}
	}
	return result
}
