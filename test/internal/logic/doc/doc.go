package doc

import (
	"context"
	"errors"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/i18nkey"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/baicaoyuan/moss/types/slicex"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/attribute"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/common"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	utilConfig "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/util/config"
	"git.woa.com/dialogue-platform/common/v3/utils"
	"git.woa.com/dialogue-platform/lke_proto/pb-protocol/KEP_WF"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
	"git.woa.com/dialogue-platform/lke_proto/pb-protocol/knowledge"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/exp/slices"
)

// GetDocList 获取文档列表
func GetDocList(ctx context.Context, req *model.DocListReq) (uint64, []*model.Doc, error) {
	log.InfoContextf(ctx, "GetDocList, req: %+v", req)
	docs := make([]*model.Doc, 0)
	docIds := make([]uint64, 0)
	notDocIds := make([]uint64, 0)
	var err error
	fileNameSubStrOrAuditNameSubStr := ""
	if req.FileName != "" {
		if req.QueryType == model.DocQueryTypeAttribute && req.FileName != model.DocQuerySystemTypeUntagged {
			// 属性标签名检索
			docIds, err = attribute.GetDocIdsByAttrSubStr(ctx, req.RobotID, req.FileName)
			if err != nil {
				log.ErrorContextf(ctx, "GetDocList failed, err: %+v", err)
				return 0, docs, err
			}
			if len(docIds) == 0 {
				log.InfoContextf(ctx, "GetDocIdsByAttrSubStr, no doc found")
				return 0, docs, nil
			}
		}
		if req.QueryType == model.DocQueryTypeFileName {
			// 文件名检索
			fileNameSubStrOrAuditNameSubStr = req.FileName
		}
		if req.FileName == model.DocQuerySystemTypeUntagged {
			// 已有标签的文档id
			notDocIds, err = attribute.GetDocIdsByAttr(ctx, req.RobotID)
			if err != nil {
				log.ErrorContextf(ctx, "GetDocIdsByAttr failed, err: %+v", err)
				return 0, docs, err
			}
			log.InfoContextf(ctx, "GetDocIdsByAttr|notDocIds:%+v", notDocIds)
			// 查询没有标签的文档,不支持FileName查询
			fileNameSubStrOrAuditNameSubStr = ""
		}
	}

	expandStatus(req)
	isDeleted := dao.IsNotDeleted
	docFilter := &dao.DocFilter{
		CorpId:                          req.CorpID,
		RobotId:                         req.RobotID,
		IDs:                             docIds,
		FileNameSubStrOrAuditNameSubStr: fileNameSubStrOrAuditNameSubStr,
		FileTypes:                       req.FileTypes,
		FilterFlag:                      req.FilterFlag,
		ValidityStatus:                  req.ValidityStatus,
		Status:                          req.Status,
		Opts:                            req.Opts,
		CategoryIds:                     req.CateIDs,
		IsDeleted:                       &isDeleted,
		Offset:                          common.GetOffsetByPage(req.Page, req.PageSize),
		Limit:                           req.PageSize,
		OrderColumn:                     []string{dao.DocTblColCreateTime, dao.DocTblColId},
		OrderDirection:                  []string{dao.SqlOrderByDesc, dao.SqlOrderByDesc},
		NotInIDs:                        notDocIds,
	}
	docs, total, err := dao.GetDocDao().GetDocCountAndList(ctx, dao.DocTblColList, docFilter)
	if err != nil {
		log.ErrorContextf(ctx, "GetDocList failed, err: %+v", err)
		return 0, docs, err
	}
	for i := range docs {
		if docs[i].Status == model.DocStatusUpdating {
			docs[i].Status = model.DocStatusCreatingIndex
		} else if docs[i].Status == model.DocStatusUpdateFail {
			docs[i].Status = model.DocStatusCreateIndexFail
		}
	}
	return uint64(total), docs, nil
}

func expandStatus(req *model.DocListReq) {
	if len(req.Status) > 0 {
		status := slicex.Unique(req.Status)
		for _, stat := range status {
			switch stat {
			case model.DocStatusUpdating:
				// 更新中合并到学习中
				req.Status = append(req.Status, model.DocStatusCreatingIndex)
			case model.DocStatusUpdateFail:
				// 更新失败合并到学习失败
				req.Status = append(req.Status, model.DocStatusCreateIndexFail)
			case model.DocStatusAuditFail:
				// 扩展审核失败的过滤条件
				req.Status = append(req.Status, model.DocStatusDocNameAndContentAuditFail,
					model.DocStatusImportDocNameAuditFail)
			}
		}
		req.Status = slicex.Unique(req.Status)
	}
}

// GetDocParseResUrl 获取文档解析md结果地址
func GetDocParseResUrl(ctx context.Context, dao dao.Dao, docId uint64, robotID uint64) (string, error) {
	docParse, err := dao.GetDocParseByDocIDAndTypeAndStatus(ctx, docId, model.DocParseTaskTypeSplitSegment,
		model.DocParseSuccess, robotID)
	if err != nil {
		if errors.Is(err, errs.ErrDocParseTaskNotFound) {
			log.WarnContextf(ctx, "GetDocParseResUrl failed, err: %+v", err)
			return "", nil
		}
		return "", err
	}
	result := &knowledge.FileParserCallbackReq{}
	err = jsoniter.UnmarshalFromString(docParse.Result, result)
	if err != nil {
		log.ErrorContextf(ctx, "getDocParseContent|jsoniter.UnmarshalFromString failed, err:%+v", err)
		return "", err
	}
	log.InfoContextf(ctx, "getDocParseContent|file parse result:%+v", result)
	resultDataMap := result.GetResults()
	docParseRes := resultDataMap[int32(knowledge.FileParserSubDataType_FILE_PARSER_SUB_DATA_TYPE_PARSE)]
	//fileData := ""
	parseUrl := ""
	for _, res := range docParseRes.GetResult() {
		// 暂不返回文件内容，防止文件过大
		//data, err := dao.GetFileDataFromCosURL(ctx, res.GetResult())
		//if err != nil {
		// return "", err
		//}
		//fileData += data
		parseUrl, err = dao.GetPresignedURLWithTypeKey(ctx, model.OfflineStorageTypeKey, res.GetResult())
		if err != nil {
			return "", err
		}
	}
	return parseUrl, nil
}

// UpdateDoc 更新文档，如果涉及到status的变更，会通过有限状态机校验
func UpdateDoc(ctx context.Context, updateColumns []string, filter *dao.DocFilter, doc *model.Doc, event string) error {
	var err error
	updateStatusFlag := false
	for _, col := range updateColumns {
		// 先判断是否需要更新状态
		if col == dao.DocTblColStatus {
			updateStatusFlag = true
			break
		}
	}

	// 需要先初始化状态机
	doc.Init()
	fromStatus, err := utils.StringToInt32(doc.FSM.Current())
	if err != nil {
		log.ErrorContextf(ctx, "UpdateDoc failed, err: %+v", err)
		return err
	}
	if updateStatusFlag {
		// 强制增加当前状态作为过滤条件，乐观锁
		filter.Status = []uint32{uint32(fromStatus)}

		if event == "" {
			err = errors.New("UpdateDoc failed, FSM event is empty")
			log.ErrorContextf(ctx, "%+v", err)
			return err
		}

		// 状态变更，需要通过有限状态机校验，校验通过后会自动更新doc结构体中的状态
		// 【注意】这里会覆盖doc中传过来的状态，更新成状态机里限定的状态
		err = doc.FSM.Event(ctx, event)
		if err != nil {
			log.ErrorContextf(ctx, "UpdateDoc failed, FSM err: %+v", err)
			return err
		}

	}

	rowsAffected, err := dao.GetDocDao().UpdateDoc(ctx, updateColumns, filter, doc)
	if err != nil {
		log.ErrorContextf(ctx, "UpdateDoc failed, err: %+v", err)
		// 如果更新失败，需要回滚文档结构体状态，状态机每次都会根据文档状态重新初始化，不需要回滚
		doc.Status = uint32(fromStatus)
		return err
	}
	if rowsAffected == 0 {
		err = errors.New("UpdateDoc failed, rowsAffected is 0")
		log.ErrorContextf(ctx, "%+v", err)
		// 如果更新失败，需要回滚文档结构体状态，状态机每次都会根据文档状态重新初始化，不需要回滚
		doc.Status = uint32(fromStatus)
		return err
	}

	return nil
}

// ProcessUnstableStatusDoc 处理文档非稳定状态
func ProcessUnstableStatusDoc(ctx context.Context, doc *model.Doc) {
	docUnstableTimeoutMinutes := utilConfig.GetMainConfig().DefaultDocUnstableTimeoutMinutes
	if value, ok := utilConfig.GetMainConfig().DocUnstableTimeoutMinutes[doc.Status]; ok {
		// 定制配置优先级更高
		docUnstableTimeoutMinutes = value
	}
	if docUnstableTimeoutMinutes == 0 {
		// 如果没配置就不做处理，兼容旧逻辑
		return
	}
	if doc.UpdateTime.Add(time.Duration(docUnstableTimeoutMinutes) * time.Minute).After(time.Now()) {
		// 非稳定状态时间未过期，不做处理
		return
	}
	log.ErrorContextf(ctx, "ProcessUnstableStatusDoc unstable status docID:%d status:%d updateTime:%s",
		doc.ID, doc.Status, doc.UpdateTime.Format("2006-01-02 15:04:05"))
	// 将状态更新为失败，方便客户删除或者重试
	event := model.EventProcessFailed
	docFilter := &dao.DocFilter{
		RobotId: doc.RobotID,
		IDs:     []uint64{doc.ID},
	}
	doc.Message = i18nkey.KeyProcessingTimeout
	updateCols := []string{dao.DocTblColStatus, dao.DocTblColMessage}
	err := UpdateDoc(ctx, updateCols, docFilter, doc, event)
	if err != nil {
		log.ErrorContextf(ctx, "ProcessDocUnstable UpdateDoc failed, doc:%+v err:%+v", doc, err)
		return
	}
}

// GetDocByBizID 通过文档BusinessID获取文档详情
func GetDocByBizID(ctx context.Context, corpID, robotID, businessID uint64,
	selectColumns []string) (*model.Doc, error) {
	if corpID == 0 || robotID == 0 || businessID == 0 || len(selectColumns) == 0 {
		return nil, errs.ErrParams
	}
	return dao.GetDocDao().GetDocByID(ctx, corpID, robotID, businessID, 0, selectColumns)
}

// GetDocReleaseCount 获取文档未发布状态总数
func GetDocReleaseCount(ctx context.Context, corpID, robotID uint64) (int64, error) {
	//isDeleted := dao.IsNotDeleted
	filter := &dao.DocFilter{
		CorpId:  corpID,
		RobotId: robotID,
		Status: []uint32{model.DocStatusWaitRelease, model.DocStatusCreatingIndex, model.DocStatusUpdating,
			model.DocStatusParseIng, model.DocStatusAuditIng, model.DocStatusUnderAppeal},
		Opts: []uint32{model.DocOptDocImport},
		//IsDeleted: &isDeleted,
	}
	count, err := dao.GetDocDao().GetDocCount(ctx, []string{dao.DocTblColId}, filter)
	if err != nil {
		return 0, err
	}
	return count, nil
}

// DbDoc2PbDoc 将文档结构体转换为pb结构体
func DbDoc2PbDoc(ctx context.Context, releasingDocIdMap map[uint64]struct{}, doc *model.Doc,
	latestRelease *model.Release, qaNums map[uint64]map[uint32]uint32,
	mapDocID2AttrLabels map[uint64][]*model.AttrLabel, docParsesFailMap map[uint64]model.DocParse,
	docAuditFailMap map[uint64]model.AuditStatus, cateMap map[uint64]*model.CateInfo,
	isSharedKnowledge bool) *pb.ListDocRsp_Doc {
	_, ok := releasingDocIdMap[doc.ID]
	pbDoc := &pb.ListDocRsp_Doc{
		DocBizId:            doc.BusinessID,
		FileName:            doc.FileName,
		NewName:             doc.FileNameInAudit,
		CosUrl:              doc.CosURL,
		Reason:              i18n.Translate(ctx, doc.Message),
		UpdateTime:          doc.UpdateTime.Unix(),
		Status:              doc.StatusCorrect(),
		StatusDesc:          i18n.Translate(ctx, doc.StatusDesc(latestRelease.IsPublishPause())),
		FileType:            doc.FileType,
		IsRefer:             doc.IsRefer,
		QaNum:               qaNums[doc.ID][model.QAIsNotDeleted],
		IsDeleted:           doc.HasDeleted(),
		Source:              doc.Source,
		SourceDesc:          doc.DocSourceDesc(),
		IsAllowRestart:      !ok && doc.IsAllowCreateQA(),
		IsDeletedQa:         qaNums[doc.ID][model.QAIsNotDeleted] == 0 && qaNums[doc.ID][model.QAIsDeleted] != 0,
		IsCreatingQa:        doc.IsCreatingQaV1(),
		IsAllowDelete:       !ok && doc.IsAllowDelete(),
		IsAllowRefer:        doc.IsAllowRefer(),
		IsCreatedQa:         doc.IsCreatedQA,
		DocCharSize:         doc.CharSize,
		IsAllowEdit:         !ok && doc.IsAllowEdit(),
		AttrRange:           doc.AttrRange,
		AttrLabels:          fillPBAttrLabels(mapDocID2AttrLabels[doc.ID]),
		ReferUrlType:        doc.ReferURLType,
		WebUrl:              doc.WebURL,
		ExpireStart:         uint64(doc.ExpireStart.Unix()),
		ExpireEnd:           uint64(doc.ExpireEnd.Unix()),
		IsAllowRetry:        isAllowRetry(ctx, doc.ID, doc.Status, docParsesFailMap, docAuditFailMap),
		CreateTime:          doc.CreateTime.Unix(),
		CustomerKnowledgeId: doc.CustomerKnowledgeId,
		IsDisabled:          doc.IsDisable(),
	}
	if isSharedKnowledge {
		if pbDoc.Status == model.DocStatusReleaseSuccess {
			// 共享知识库，需要兼容从应用知识库人工转换成共享知识库的情况
			pbDoc.Status = model.DocStatusWaitRelease
		}
		if pbDoc.Status == model.DocStatusWaitRelease {
			// 共享知识库不需要发布，所以将待发布、已发布状态的文档显示为导入完成
			pbDoc.StatusDesc = i18n.Translate(ctx, i18nkey.KeyImportComplete)
		}
	}
	if doc.CategoryID != 0 {
		if cate, ok := cateMap[uint64(doc.CategoryID)]; ok {
			pbDoc.CateBizId = cate.BusinessID
		}
	}
	for k, v := range model.IsProcessingMap {
		if doc.IsProcessing([]uint64{k}) {
			pbDoc.Processing = append(pbDoc.Processing, v)
		}
	}
	for k, v := range model.AttributeFlagMap {
		if doc.HasAttributeFlag(k) {
			pbDoc.AttributeFlags = append(pbDoc.AttributeFlags, v)
		}
	}
	return pbDoc
}

func isAllowRetry(ctx context.Context, docID uint64, docStatus uint32,
	docParsesFailMap map[uint64]model.DocParse,
	docAuditFailMap map[uint64]model.AuditStatus) bool {
	if docParsesFailMap == nil {
		return false
	}
	if docStatus == model.DocStatusAuditFail {
		if docAuditFail, ok := docAuditFailMap[docID]; ok && docAuditFail.Status == model.AuditStatusTimeoutFail {
			return true
		}
		return false
	}
	if docStatus == model.DocStatusParseImportFail {
		return true
	}
	log.DebugContextf(ctx, "isAllowRetry docParsesFailMap:%+v, "+
		"docID:%d, docStatus:%d", docParsesFailMap, docID, docStatus)
	if len(docParsesFailMap) == 0 {
		return false
	}
	docParsesFail := model.DocParse{}
	ok := false
	if docParsesFail, ok = docParsesFailMap[docID]; !ok {
		return false
	}
	if docParsesFail.Status == model.DocParseCallBackCancel {
		return true
	}
	result := &knowledge.FileParserCallbackReq{}
	err := jsoniter.UnmarshalFromString(docParsesFail.Result, result)
	if err != nil {
		log.ErrorContextf(ctx, "isAllowRetry UnmarshalFromString err:%+v, "+
			"docID:%d, docStatus:%d", err, docID, docStatus)
		return true
	}
	return getIsAllowRetry(result.ErrorCode)
}

func getIsAllowRetry(errorCode string) bool {
	if conf, ok := config.App().DocParseError[errorCode]; ok {
		return conf.IsAllowRetry
	}
	return config.App().DocParseErrorDefault.IsAllowRetry
}

// fillPBAttrLabels 转成成PB的属性标签
func fillPBAttrLabels(attrLabels []*model.AttrLabel) []*pb.AttrLabel {
	list := make([]*pb.AttrLabel, 0)
	for _, v := range attrLabels {
		attrLabel := &pb.AttrLabel{
			Source:    v.Source,
			AttrBizId: v.BusinessID,
			AttrKey:   v.AttrKey,
			AttrName:  v.AttrName,
		}
		for _, label := range v.Labels {
			labelName := label.LabelName
			if label.LabelID == 0 {
				labelName = config.App().AttributeLabel.FullLabelDesc
			}
			attrLabel.Labels = append(attrLabel.Labels, &pb.AttrLabel_Label{
				LabelBizId: label.BusinessID,
				LabelName:  labelName,
			})
		}
		list = append(list, attrLabel)
	}
	return list
}

// GetWorkflowListByDoc 获取文档被引用的工作流列表
func GetWorkflowListByDoc(ctx context.Context, req *pb.CheckDocReferWorkFlowReq) (
	[]*KEP_WF.DocRefByWorkflow, error) {
	rsp, err := client.GetWorkflowListByDoc(ctx, req.BotBizId, req.GetDocBizIds())
	if err != nil {
		log.ErrorContextf(ctx, "GetWorkflowListByDoc failed, err: %+v", err)
		return nil, err
	}
	if rsp == nil || len(rsp.GetList()) == 0 {
		return []*KEP_WF.DocRefByWorkflow{}, nil
	}
	return rsp.GetList(), nil
}

// BatchDownloadDoc 批量下载文档
func BatchDownloadDoc(ctx context.Context, robotID uint64, docIDs []uint64, d dao.Dao) (
	*pb.BatchDownloadDocRsp, error) {
	docFilter := &dao.DocFilter{
		RobotId:     robotID,
		BusinessIds: docIDs,
	}
	selectColumns := []string{dao.DocTblColId, dao.DocTblColBusinessId, dao.DocTblColFileName,
		dao.DocTblColFileNameInAudit, dao.DocTblColFileType, dao.DocTblColCosURL}
	docs, err := dao.GetDocDao().GetDocList(ctx, selectColumns, docFilter)
	if err != nil {
		return nil, err
	}
	if len(docs) != len(docFilter.BusinessIds) {
		log.ErrorContextf(ctx, "BatchDownloadDoc Check docs not found, len(docs):%d docIDs:%+v",
			len(docs), docFilter.BusinessIds)
		return nil, errs.ErrDocNotFound
	}
	rsp := &pb.BatchDownloadDocRsp{}
	for _, doc := range docs {
		signURL, err := d.GetPresignedURLWithTypeKey(ctx, model.OfflineStorageTypeKey, doc.CosURL)
		if err != nil {
			log.ErrorContextf(ctx, "BatchDownloadDoc GetPresignedURLWithTypeKey failed, err:%+v", err)
			return nil, errs.ErrSystem
		}
		rsp.DocList = append(rsp.DocList, &pb.BatchDownloadDocRsp_DocList{
			FileName: doc.GetRealFileName(),
			FileType: doc.FileType,
			CosUrl:   doc.CosURL,
			Url:      signURL,
			DocBizId: doc.BusinessID,
		})
	}
	return rsp, nil
}

// GetDocNextUpdateTime 获取文档下次更新时间
func GetDocNextUpdateTime(ctx context.Context, updatePeriodH uint32) time.Time {
	// 如果updatePeriodH为0，返回1970-01-01 08:00:00
	if updatePeriodH == 0 {
		return time.Unix(0, 0).Add(8 * time.Hour) // 1970-01-01 08:00:00
	}
	now := time.Now()
	var daysToAdd int
	// 根据updatePeriodH的值决定增加的天数
	switch {
	case updatePeriodH > 72: // 大于72小时加7天
		daysToAdd = 7
	case updatePeriodH > 24: // 大于24小时加3天
		daysToAdd = 3
	default: // 其他情况(>0)加1天
		daysToAdd = 1
	}
	// 计算增加天数后的日期  取整到当天的0点
	nextUpdateTimeDay := now.AddDate(0, 0, daysToAdd) // 先加天数
	nextUpdateTimeDay = time.Date(                    // 再重置时间部分
		nextUpdateTimeDay.Year(),
		nextUpdateTimeDay.Month(),
		nextUpdateTimeDay.Day(),
		0, 0, 0, 0,
		time.Local,
	)
	log.DebugContextf(ctx, "GetDocNextUpdateTime: nextUpdateTimeDay: %v", nextUpdateTimeDay)
	return nextUpdateTimeDay
}

// RefreshTxDoc 刷新腾讯文档
func RefreshTxDoc(ctx context.Context, isAuto bool, docs []*model.Doc, d dao.Dao) error {
	var tFileInfo []model.TxDocRefreshTFileInfo
	for _, doc := range docs {
		if doc.Status != model.DocStatusWaitRelease && doc.Status != model.DocStatusReleaseSuccess {
			if isAuto {
				log.WarnContextf(ctx, "RefreshTxDoc doc status is not wait release or release success, doc: %+v", doc)
				continue
			}
			return errs.ErrRefreshTxDocStatusFail
		}
		if time.Unix(0, 0).Before(doc.ExpireEnd) && time.Now().After(doc.ExpireEnd) {
			log.WarnContextf(ctx, "RefreshTxDoc status Expire, doc: %+v", doc)
			continue
		}
		uin := pkg.Uin(ctx)
		if uin == "" {
			appDB, err := d.GetAppByID(ctx, doc.RobotID)
			if err != nil {
				return err
			}
			appInfo, err := client.GetAppInfo(ctx, appDB.BusinessID, model.AppTestScenes)
			if err != nil {
				log.ErrorContextf(ctx, "RefreshTxDoc GetAppInfo err: %+v", err)
				return err
			}
			uin = appInfo.GetUin()
		}
		rsp, err := client.CheckUserAuth(ctx, uin, uin)
		if err != nil {
			if isAuto {
				// 定时任务自动刷新,未授权跳过
				log.DebugContextf(ctx, "RefreshTxDoc CheckUserAuth rsp.Response.Code != 200, isAuto: %v uin: %s",
					isAuto, uin)
				continue
			}
			log.ErrorContextf(ctx, "RefreshTxDoc ImportTFile err: %+v", err)
			return err
		}
		log.DebugContextf(ctx, "RefreshTxDoc CheckUserAuth rsp: %+v", rsp)
		if rsp.Response.Code != 200 {
			if isAuto {
				// 定时任务自动刷新,未授权跳过
				log.DebugContextf(ctx, "RefreshTxDoc CheckUserAuth rsp.Response.Code != 200, isAuto: %v uin: %s",
					isAuto, uin)
				continue
			}
			return errs.ErrRefreshTxDocUserAuthFail
		}

		operationID, err := client.ImportTFile(ctx, uin, uin, doc.CustomerKnowledgeId)
		if err != nil {
			log.ErrorContextf(ctx, "RefreshTxDoc ImportTFile err: %+v", err)
			return err
		}
		if operationID == "" {
			return errors.New("ImportTFile operationID is empty")
		}

		tFileInfo = append(tFileInfo, model.TxDocRefreshTFileInfo{
			DocID:       doc.ID,
			CorpID:      doc.CorpID,
			StaffID:     doc.StaffID,
			RobotID:     doc.RobotID,
			FileID:      doc.CustomerKnowledgeId,
			OperationID: operationID,
		})
	}

	// isAuto 如果是自动刷新任务，需要更新所有文档下次执行时间
	if isAuto {
		log.DebugContextf(ctx, "RefreshTxDoc isAuto: %v", isAuto)
	}

	taskID, err := dao.NewTxDocRefreshTask(ctx, tFileInfo)
	if err != nil {
		log.ErrorContextf(ctx, "RefreshTxDoc NewTxDocRefreshTask err: %+v", err)
		return err
	}
	log.DebugContextf(ctx, "RefreshTxDoc NewTxDocRefreshTask taskID: %v", taskID)
	return nil
}

// RefreshCorpCOSDoc 刷新客户cos文档
func RefreshCorpCOSDoc(ctx context.Context, isAuto bool, docs []*model.Doc, d dao.Dao) error {
	for _, doc := range docs {
		if doc.Status != model.DocStatusWaitRelease && doc.Status != model.DocStatusReleaseSuccess {
			if isAuto {
				log.WarnContextf(ctx, "RefreshCorpCOSDoc doc status is not wait release or release success, doc: %+v", doc)
				continue
			}
			return errs.ErrRefreshCorpCOSDocStatusFail
		}
		if time.Unix(0, 0).Before(doc.ExpireEnd) && time.Now().After(doc.ExpireEnd) {
			log.WarnContextf(ctx, "RefreshCorpCOSDoc status Expire, doc: %+v", doc)
			continue
		}
		uin := pkg.Uin(ctx)
		if uin == "" {
			appDB, err := d.GetAppByID(ctx, doc.RobotID)
			if err != nil {
				return err
			}
			appInfo, err := client.GetAppInfo(ctx, appDB.BusinessID, model.AppTestScenes)
			if err != nil {
				log.ErrorContextf(ctx, "RefreshCorpCOSDoc GetAppInfo err: %+v", err)
				return err
			}
			uin = appInfo.GetUin()
		}
		// 校验授权信息
		_, status, err := d.AssumeServiceRole(ctx, uin,
			config.App().COSDocumentConfig.ServiceRole, 0, nil)
		if err != nil {
			return err
		}
		if status != knowledge.RoleStatusType_RoleStatusAvailable {
			return errs.ErrRefreshCorpCOSDocUserAuthFail
		}
	}

	taskID, err := dao.NewCorpCOSDocRefreshTask(ctx, docs)
	if err != nil {
		log.ErrorContextf(ctx, "RefreshCorpCOSDoc NewTxDocRefreshTask err: %+v", err)
		return err
	}
	log.DebugContextf(ctx, "RefreshCorpCOSDoc NewTxDocRefreshTask taskID: %v", taskID)
	return nil
}

// ModifyItemsActionUpdateDoc 更新文档指定db字段内容
func ModifyItemsActionUpdateDoc(ctx context.Context, d dao.Dao, doc *model.Doc, app *model.App,
	updateDocColumns []string, update *model.Doc, isReloadDoc, isModifySplitRule bool) error {
	updateDocFilter := &dao.DocFilter{
		IDs: []uint64{doc.ID}, CorpId: doc.CorpID, RobotId: doc.RobotID,
	}
	_, err := dao.GetDocDao().UpdateDoc(ctx, updateDocColumns, updateDocFilter, update)
	if err != nil {
		log.ErrorContextf(ctx, "ReloadUpdateDoc|UpdateDocStatus|err:%+v", err)
		return err
	}
	if isModifySplitRule {
		corp, err := d.GetCorpByID(ctx, app.CorpID)
		if err != nil || corp == nil {
			log.ErrorContextf(ctx, "ModifyItemsActionUpdateDoc GetCorpByID err:%+v", err)
			return errs.ErrCorpNotFound
		}
		err = dao.NewDocDocSplitRuleModifyTask(ctx, model.DocSplitRuleModifyParams{
			CorpBizID: corp.BusinessID,
			AppBizID:  app.BusinessID,
			AppID:     app.ID,
			DocBizID:  doc.BusinessID,
		})
		if err != nil {
			log.ErrorContextf(ctx, "ModifyItemsActionUpdateDoc NewDocDocSplitRuleModifyTask err:%+v", err)
			return err
		}
		log.DebugContextf(ctx, "ModifyItemsActionUpdateDoc newtask success")
	}
	// 需要重新解析
	if isReloadDoc {
		doc.CosURL = update.CosURL
		doc.CosHash = update.CosHash
		requestID := trace.SpanContextFromContext(ctx).TraceID().String()
		taskID, err := d.SendDocParseWordCount(ctx, doc, requestID, "")
		if err != nil {
			return err
		}
		docParse := model.DocParse{
			DocID:     doc.ID,
			CorpID:    doc.CorpID,
			RobotID:   doc.RobotID,
			StaffID:   doc.StaffID,
			RequestID: requestID,
			Type:      model.DocParseTaskTypeWordCount,
			OpType:    model.DocParseOpTypeWordCount,
			Status:    model.DocParseIng,
			TaskID:    taskID,
		}
		err = d.CreateDocParse(ctx, nil, docParse)
		if err != nil {
			return err
		}
	}
	return nil
}

// CheckReloadUpdateDocFileType 校验更新文件类型
func CheckReloadUpdateDocFileType(ctx context.Context, doc *model.Doc, url string) (err error) {
	ext := util.GetFileExt(url)
	log.DebugContextf(ctx, "checkReloadUpdateDocFileType|GetFileExt|ext:%+v", ext)
	if strings.ToLower(doc.FileType) != strings.ToLower(ext) {
		log.WarnContextf(ctx, "checkReloadUpdateDocFileType|FileType not match|doc.FileType:%s fileType:%s",
			doc.FileType, ext)
		return errs.ErrFileExtNotMatch
	}
	return nil
}

// ModifyItemsAction 更新文档指定内容
func ModifyItemsAction(ctx context.Context, d dao.Dao, app *model.App, doc *model.Doc, req *pb.ModifyDocReq) error {
	// 校验修改类型是否合法
	validModifyTypes := map[pb.ModifyDocReq_ModifyType]bool{
		pb.ModifyDocReq_COS_INFO:              true,
		pb.ModifyDocReq_REFER_INFO:            true,
		pb.ModifyDocReq_UPDATE_PERIOD:         true,
		pb.ModifyDocReq_UPDATE_TX_DOC_REFRESH: true,
		pb.ModifyDocReq_UPDATE_SPLIT_RULE:     true,
		pb.ModifyDocReq_UPDATE_CORP_COS_INFO:  true,
	}
	for _, modifyType := range req.GetModifyTypes() {
		if !validModifyTypes[modifyType] {
			log.WarnContextf(ctx, "ModifyItemsAction| invalid modify type:%v", req.GetModifyTypes())
			return errs.ErrParams
		}
	}
	// 重新解析文档
	isReloadDoc := false
	isModifySplitRule := false
	updateDocColumns := []string{dao.DocTblColStaffId}
	update := &model.Doc{
		StaffID: pkg.StaffID(ctx),
	}

	// 腾讯文档刷新
	if slices.Contains(req.GetModifyTypes(), pb.ModifyDocReq_UPDATE_TX_DOC_REFRESH) {
		if doc.Source != model.SourceFromTxDoc {
			log.WarnContextf(ctx, "ModifyItemsAction|RefreshTxDoc invalid modify type:%v doc %v",
				req.GetModifyTypes(), doc)
			return errs.ErrParams
		}
		err := RefreshTxDoc(ctx, false, []*model.Doc{doc}, d)
		if err != nil {
			log.ErrorContextf(ctx, "ModifyItemsAction|RefreshTxDoc|failed, err:%v", err)
			return errs.ErrRefreshTxDocFail
		}
		return nil
	}

	if slices.Contains(req.GetModifyTypes(), pb.ModifyDocReq_UPDATE_PERIOD) {
		if doc.Source != model.SourceFromTxDoc {
			log.WarnContextf(ctx, "ModifyItemsAction|UpdatePeriodInfo invalid modify type:%v doc:%v",
				req.GetModifyTypes(), doc)
			return errs.ErrParams
		}
		if req.GetUpdatePeriodInfo() == nil {
			return errs.ErrParams
		}
		nextUpdateTime := GetDocNextUpdateTime(ctx, req.GetUpdatePeriodInfo().GetUpdatePeriodH())

		update.UpdatePeriodH = req.GetUpdatePeriodInfo().GetUpdatePeriodH()
		update.NextUpdateTime = nextUpdateTime
		updateDocColumns = append(updateDocColumns, dao.DocTblColUpdatePeriodH, dao.DocTblColNextUpdateTime)
	}

	// 刷新需要解析的操作
	if slices.Contains(req.GetModifyTypes(), pb.ModifyDocReq_COS_INFO) {
		if req.GetCosInfo() == nil {
			return errs.ErrParams
		}
		fileSize, err := util.CheckReqBotBizIDUint64(ctx, req.GetCosInfo().GetSize())
		if err != nil {
			return err
		}
		corp, err := d.GetCorpByID(ctx, app.CorpID)
		if err != nil || corp == nil {
			return errs.ErrCorpNotFound
		}
		if err = d.CheckURLFile(ctx, app.CorpID, corp.BusinessID, app.BusinessID,
			req.GetCosInfo().GetCosUrl(), req.GetCosInfo().GetETag()); err != nil {
			log.ErrorContextf(ctx, "ModifyDoc|CheckURLFile failed, err:%+v", err)
			return errs.ErrInvalidURL
		}
		if err = CheckReloadUpdateDocFileType(ctx, doc, req.GetCosInfo().GetCosUrl()); err != nil {
			log.ErrorContextf(ctx, "ModifyDoc|CheckReloadUpdateDocFileType failed, err:%+v", err)
			return err
		}
		isReloadDoc = true
		update.Status = model.DocStatusParseIng
		update.UpdateTime = time.Now()
		update.CosURL = req.GetCosInfo().GetCosUrl()
		update.CosHash = req.GetCosInfo().GetETag()
		update.FileSize = fileSize
		updateDocColumns = append(updateDocColumns, dao.DocTblColStatus, dao.DocTblColUpdateTime,
			dao.DocTblColCosURL, dao.DocTblColCosHash, dao.DocTblColFileSize)
	}

	if slices.Contains(req.GetModifyTypes(), pb.ModifyDocReq_REFER_INFO) {
		if req.GetReferInfo() == nil {
			return errs.ErrParams
		}
		update.IsRefer = req.GetReferInfo().GetIsRefer()
		update.ReferURLType = req.GetReferInfo().GetReferUrlType()
		update.WebURL = req.GetReferInfo().GetWebUrl()
		updateDocColumns = append(updateDocColumns, dao.DocTblColIsRefer, dao.DocTblColReferURLType,
			dao.DocTblColWebURL)
	}

	// 自定义拆分规则更新
	if slices.Contains(req.GetModifyTypes(), pb.ModifyDocReq_UPDATE_SPLIT_RULE) {
		if len(req.GetSplitRule()) == 0 {
			log.ErrorContextf(ctx, "ModifyItemsAction invalid modify type:%+v doc %+v",
				req.GetModifyTypes(), doc)
			return errs.ErrParams
		}
		update.StaffID = pkg.StaffID(ctx)
		update.Status = model.DocStatusParseIng
		update.SplitRule = req.GetSplitRule()
		update.UpdateTime = time.Now()
		update.CharSize = 0
		update.BatchID = 0
		update.NextAction = model.DocNextActionAdd
		update.Message = ""
		updateDocColumns = append(updateDocColumns, dao.DocTblColStatus, dao.DocTblColUpdateTime, dao.DocTblColSplitRule,
			dao.DocTblColStaffId, dao.DocTblColCharSize, dao.DocTblColBatchId, dao.DocTblColNextAction, dao.DocTblColMessage)
		isModifySplitRule = true
		// 减少已使用字符数
		if err := d.UpdateAppUsedCharSizeTx(ctx, -int64(doc.CharSize), doc.RobotID); err != nil {
			log.ErrorContextf(ctx, "CreateDocParsingIntervention|UpdateAppUsedCharSizeTx|err:%+v", err)
			return err
		}
	}

	// 客户cos文件刷新
	if slices.Contains(req.GetModifyTypes(), pb.ModifyDocReq_UPDATE_CORP_COS_INFO) {
		if doc.Source != model.SourceFromCorpCOSDoc {
			log.WarnContextf(ctx, "ModifyItemsAction|RefreshCorpCOS invalid modify type:%v doc %v",
				req.GetModifyTypes(), doc)
			return errs.ErrParams
		}
		err := RefreshCorpCOSDoc(ctx, false, []*model.Doc{doc}, d)
		if err != nil {
			log.ErrorContextf(ctx, "ModifyItemsAction|RefreshCorpCOS|failed, err:%v", err)
			return errs.ErrRefreshCorpCOSDocFail
		}
		return nil
	}

	// 执行文档更新
	if err := ModifyItemsActionUpdateDoc(ctx, d, doc, app, updateDocColumns, update,
		isReloadDoc, isModifySplitRule); err != nil {
		log.ErrorContextf(ctx, "ModifyItemsActionUpdateDoc failed, err:%v", err)
		return err
	}
	return nil
}

// CheckDuplicateFile 处理文档重复的情况
func CheckDuplicateFile(ctx context.Context, dao dao.Dao, req *pb.SaveDocReq, corpID uint64, appID uint64) (bool,
	*pb.SaveDocRsp, error) {
	duplicateFileHandles := req.GetDuplicateFileHandles()
	if len(duplicateFileHandles) == 0 {
		// 默认策略
		duplicateFileHandles = make([]*pb.DuplicateFileHandle, 0)
		duplicateFileHandles = append(duplicateFileHandles, &pb.DuplicateFileHandle{
			CheckType:  pb.DuplicateFileCheckType_CHECK_TYPE_COS_HASH,
			HandleType: pb.DuplicateFileHandleType_HANDLE_TYPE_RETURN_ERR,
		})
	}
	var err error
	for i, fileHandle := range duplicateFileHandles {
		// 依次按照重复判断方式检查是否有重复文档
		// 只要有一个重复判断方式检查出重复文档，即可按照重复处理方式处理完，返回结果，不再继续检查
		var doc *model.Doc
		switch fileHandle.GetCheckType() {
		case pb.DuplicateFileCheckType_CHECK_TYPE_INVALID:
			return false, nil, errs.ErrWrapf(errs.ErrParameterInvalid,
				i18n.Translate(ctx, i18nkey.KeyInvalidParamDuplicateFileHandleCheckType), i, fileHandle.GetCheckType())
		case pb.DuplicateFileCheckType_CHECK_TYPE_COS_HASH:
			doc, err = dao.GetDocByCosHash(ctx, corpID, appID, req.GetCosHash())
			if err != nil {
				log.ErrorContextf(ctx, "CheckDuplicateFile GetDocByCosHash failed, err:%+v", err)
				return false, nil, errs.ErrWrapf(errs.ErrSystem, "err:%+v", err.Error())
			}
			if doc == nil {
				// 未找到重复文档，继续下一个检查类型
				continue
			}
		default:
			return false, nil, errs.ErrWrapf(errs.ErrParameterInvalid,
				i18n.Translate(ctx, i18nkey.KeyInvalidParamDuplicateFileHandleCheckType), i, fileHandle.GetCheckType())
		}
		rsp, err := handleDuplicateFile(ctx, fileHandle.GetHandleType(), doc)
		if rsp != nil {
			// 返回重复判断方式
			rsp.DuplicateFileCheckType = fileHandle.GetCheckType()
		}
		return true, rsp, err
	}
	rsp := &pb.SaveDocRsp{}
	return false, rsp, nil
}

// handleDuplicateFile 处理文档重复的情况
func handleDuplicateFile(ctx context.Context, handleType pb.DuplicateFileHandleType,
	doc *model.Doc) (*pb.SaveDocRsp, error) {
	if doc == nil {
		log.ErrorContextf(ctx, "handleDuplicateFile doc is nil")
		return nil, errs.ErrWrapf(errs.ErrSystem, "handleDuplicateFile doc is nil")
	}
	switch handleType {
	case pb.DuplicateFileHandleType_HANDLE_TYPE_INVALID:
		return nil, errs.ErrWrapf(errs.ErrParameterInvalid,
			i18n.Translate(ctx, i18nkey.KeyInvalidParamHandleType), handleType)
	case pb.DuplicateFileHandleType_HANDLE_TYPE_RETURN_ERR:
		log.WarnContextf(ctx, "handleDuplicateFile doc is duplicate, doc:%+v", doc)
		return nil, errs.ErrWrapf(errs.ErrDocExist,
			i18n.Translate(ctx, i18nkey.KeyDocumentAlreadyExistsWithNameAndID), doc.FileName, doc.BusinessID)
	case pb.DuplicateFileHandleType_HANDLE_TYPE_SKIP:
		return &pb.SaveDocRsp{
			DocBizId: doc.BusinessID,
		}, nil
	default:
		return nil, errs.ErrWrapf(errs.ErrParameterInvalid,
			i18n.Translate(ctx, i18nkey.KeyInvalidParamHandleType), handleType)
	}
}

func GetDataSource(ctx context.Context, splitRuleJSON string) int {
	// 如果没有拆分规则，则使用COS数据源
	if splitRuleJSON == "" {
		return model.DataSourceCOS
	}

	splitRule := model.SplitRule{}
	err := jsoniter.Unmarshal([]byte(splitRuleJSON), &splitRule)
	if err != nil {
		log.WarnContextf(ctx, "getDataSource|Unmarshal oldSplitRule failed, err:%v", err)
		return model.DataSourceCOS
	}
	// 如果为默认拆分规则，则使用COS数据源
	if splitRule.SplitConfigNew.XlsxSplitter.SplitRow == 0 {
		return model.DataSourceCOS
	}
	// 如果为自定义拆分规则，则使用DB数据源
	if splitRule.SplitConfigNew.XlsxSplitter.SplitRow > 0 {
		return model.DataSourceDB
	}

	return model.DataSourceCOS
}
