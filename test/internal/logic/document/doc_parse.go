package document

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"git.code.oa.com/trpc-go/trpc-go"
	"git.code.oa.com/trpc-go/trpc-go/client"
	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/encodingx/jsonx"
	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/model"
	async "git.woa.com/adp/kb/kb-config/internal/async/scheduler"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
	knowClient "git.woa.com/adp/kb/kb-config/internal/rpc"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	fileManagerServer "git.woa.com/adp/pb-go/kb/parse_engine/file_manager_server"
	"git.woa.com/adp/pb-go/kb/parse_engine/file_parse_common"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

const (
	// fileManagerBiz 知识问答文档解析Biz
	fileManagerBiz = "knowledge"

	// 默认离线文档解析服务版本号 2
	defaultOfflineFileManagerVersion = 2
)

func interveneOldDocCosHashToNewDocRedisKey(corpID, botBizID, docBizID uint64, oldDocCosHash string) string {
	return fmt.Sprintf("InterveneOldDocCosHashToNewDoc:%d:%d:%d:%s", corpID, botBizID, docBizID, oldDocCosHash)
}

// GetDocParseByTaskIDAndOpType 获取文档解析任务（指定 TaskID 和 Type）
func (l *Logic) GetDocParseByTaskIDAndOpType(ctx context.Context, taskID string, opType int32) (*docEntity.DocParse, error) {
	/*
			`
				SELECT
		    		` + docParseFields + `
				FROM
				    t_doc_parse
				WHERE
				    task_id = ? AND op_type = ? AND status = ?
				`
	*/

	parsingStatus := docEntity.DocParseIng
	filter := &docEntity.DocParseFilter{
		TaskID: taskID,
		OpType: opType,
		Status: []int32{int32(parsingStatus)},
		Limit:  1,
	}

	dbClients := knowClient.GetAllGormClients(ctx, docEntity.DocParseTableName, []client.Option{}...)
	for _, db := range dbClients {
		docParses, err := l.docDao.GetDocParseListWithTx(ctx, []string{}, filter, db)
		if err != nil {
			logx.E(ctx, "GetDocParseByTaskIDAndOpType error. err:%+v", err)
			continue
		}
		if len(docParses) > 0 {
			return docParses[0], nil
		}

	}
	return nil, errs.ErrDocParseTaskNotFound
}

// GetDocParseByDocIDAndType 获取文档解析任务（指定 DocID 和 Type）
func (l *Logic) GetDocParseByDocIDAndType(ctx context.Context, docID uint64, fType int32, robotID uint64) (*docEntity.DocParse, error) {
	/*
			`
				SELECT
		    		` + docParseFields + `
				FROM
				    t_doc_parse
				WHERE
				    doc_id = ? AND type = ?
				ORDER BY
					id DESC
				LIMIT 1
				`
	*/
	filter := &docEntity.DocParseFilter{
		DocID:          docID,
		Type:           fType,
		Limit:          1,
		OrderColumn:    []string{"id"},
		OrderDirection: []string{"DESC"},
	}
	db, err := knowClient.GormClient(ctx, docEntity.DocParseTableName, robotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "Get DB Client err:%+v", err)
		return nil, err
	}

	docParses, err := l.docDao.GetDocParseListWithTx(ctx, []string{}, filter, db)
	if err != nil {
		logx.E(ctx, "GetDocParseByDocIDAndType error. err:%+v", err)
		return nil, err
	}
	if len(docParses) > 0 {
		return docParses[0], nil
	}
	return nil, errs.ErrDocParseTaskNotFound
}

// GetDocParseByDocID 获取文档解析任务（指定 DocID）
func (l *Logic) GetDocParseByDocID(ctx context.Context, docID uint64, robotID uint64) (*docEntity.DocParse, error) {
	/*
				`
				SELECT
		    		` + docParseFields + `
				FROM
				    t_doc_parse
				WHERE
				    doc_id = ?
				ORDER BY
					id DESC
				LIMIT 1
				`
	*/
	filter := &docEntity.DocParseFilter{
		DocID:          docID,
		Limit:          1,
		OrderColumn:    []string{"id"},
		OrderDirection: []string{"DESC"},
	}
	db, err := knowClient.GormClient(ctx, docEntity.DocParseTableName, robotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "Get DB Client err:%+v", err)
		return nil, err
	}

	docParses, err := l.docDao.GetDocParseListWithTx(ctx, []string{}, filter, db)
	if err != nil {
		logx.E(ctx, "GetDocParseByDocID error. err:%+v", err)
		return nil, err
	}
	if len(docParses) > 0 {
		return docParses[0], nil
	}
	return nil, errs.ErrDocParseTaskNotFound
}

// GetDocParseByDocIDs 获取文档解析任务（指定 DocIDs）
func (l *Logic) GetDocParseByDocIDs(ctx context.Context, docIDs []uint64, robotID uint64) ([]*docEntity.DocParse, error) {
	/*
				 `
				SELECT
		    		` + docParseFields + `
				FROM
				    t_doc_parse
				WHERE
				    type =? AND status IN ( ? , ? ) AND doc_id IN (%s)
				ORDER BY
					id DESC
				`
	*/
	if len(docIDs) == 0 {
		return nil, errs.ErrDocParseTaskNotFound
	}

	filter := &docEntity.DocParseFilter{
		DocIDs:         docIDs,
		Type:           docEntity.DocParseTaskTypeWordCount,
		Status:         []int32{docEntity.DocParseCallBackFailed, docEntity.DocParseCallBackCancel},
		OrderColumn:    []string{"id"},
		OrderDirection: []string{"DESC"},
	}
	db, err := knowClient.GormClient(ctx, docEntity.DocParseTableName, robotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "Get DB Client err:%+v", err)
		return nil, err
	}

	docParses, err := l.docDao.GetDocParseListWithTx(ctx, []string{}, filter, db)
	if err != nil {
		logx.E(ctx, "GetDocParseByDocID error. err:%+v", err)
		return nil, err
	}
	return docParses, nil
}

// DocParseCanBeRetried 获取重试文档列表 获取文档解析任务（指定 DocID、type、status）
func (l *Logic) DocParseCanBeRetried(ctx context.Context, docID uint64, fType uint32, status []uint32, robotID uint64) (
	[]*docEntity.DocParse, error) {
	/*
				 `
				SELECT
		    		` + docParseFields + `
				FROM
				    t_doc_parse
				WHERE
				    doc_id = ? AND type =? %s
				ORDER BY
					id DESC
				`
	*/
	statusList := []int32{}
	for _, v := range status {
		statusList = append(statusList, int32(v))
	}
	filter := &docEntity.DocParseFilter{
		DocID:          docID,
		Type:           int32(fType),
		Status:         statusList,
		OrderColumn:    []string{"id"},
		OrderDirection: []string{"DESC"},
	}
	db, err := knowClient.GormClient(ctx, docEntity.DocParseTableName, robotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "Get DB Client err:%+v", err)
		return nil, err
	}

	docParses, err := l.docDao.GetDocParseListWithTx(ctx, []string{}, filter, db)
	if err != nil {
		logx.E(ctx, "GetDocParseByDocID error. err:%+v", err)
		return nil, err
	}
	return docParses, nil
}

// GetDocParseByDocIDAndTypeAndStatus 获取文档解析任务（指定 DocID、type、status）
func (l *Logic) GetDocParseByDocIDAndTypeAndStatus(ctx context.Context, docID uint64, fType, status uint32, robotID uint64) (
	*docEntity.DocParse, error) {
	/*
				`
				SELECT
		    		` + docParseFields + `
				FROM
				    t_doc_parse
				WHERE
				    doc_id = ? AND type =? AND status = ?
				ORDER BY
					id DESC
				LIMIT 1
				`
	*/

	filter := &docEntity.DocParseFilter{
		DocID:          docID,
		Type:           int32(fType),
		Status:         []int32{int32(status)},
		Limit:          1,
		OrderColumn:    []string{"id"},
		OrderDirection: []string{"DESC"},
	}
	db, err := knowClient.GormClient(ctx, docEntity.DocParseTableName, robotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "Get DB Client err:%+v", err)
		return nil, err
	}

	docParses, err := l.docDao.GetDocParseListWithTx(ctx, []string{}, filter, db)
	if err != nil {
		logx.E(ctx, "GetDocParseByDocIDAndTypeAndStatus error. err:%+v", err)
		return nil, err
	}
	if len(docParses) > 0 {
		return docParses[0], nil
	}
	return nil, errs.ErrDocParseTaskNotFound
}

func (l *Logic) GetDocParseList(ctx context.Context, selectColumns []string, filter *docEntity.DocParseFilter) (
	[]*docEntity.DocParse, error) {
	return l.docDao.GetDocParseList(ctx, selectColumns, filter)
}

func (l *Logic) DeleteDocParseByDocID(ctx context.Context, corpID, robotID, docID uint64) error {
	return l.docDao.DeleteDocParseByDocID(ctx, corpID, robotID, docID)
}

// UpdateDocParseTask 更新文档解析任务信息
func (l *Logic) UpdateDocParseTask(ctx context.Context, updateColumns []string, docParse *docEntity.DocParse) error {
	logx.I(ctx, "Prepare to UpdateDocParseTask docParse:%+v", docParse)
	db, err := knowClient.GormClient(ctx, model.TableNameTDocParse, docParse.RobotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "get GormClient failed, err: %+v", err)
		return err
	}
	return l.docDao.UpdateDocParseTaskByTx(ctx, updateColumns, docParse, db)
}

// CreateDocParseTask 创建文档解析任务
func (l *Logic) CreateDocParseTask(ctx context.Context, docParse *docEntity.DocParse) error {
	logx.I(ctx, "Prepare to CreateDocParseTask docParse:%+v", docParse)
	return l.docDao.CreateDocParseTask(ctx, docParse)
}

// StopDocParseTask 终止 文档解析任务
func (l *Logic) StopDocParseTask(ctx context.Context, taskID string, requestID string, robotBizID uint64) error {
	req := &fileManagerServer.CancelTaskReq{
		RequestId: requestID,
		AppInfo: &fileManagerServer.AppInfo{
			AppKey: fmt.Sprintf("%d", robotBizID),
			Biz:    fileManagerBiz,
		},
		TaskId: taskID,
	}
	logx.D(ctx, "Prepare to stop doc parse task req:%+v", req)
	rsp, err := l.rpc.FileManager.CancelTask(ctx, req)

	if err != nil || rsp.StatusCode != 0 {
		logx.E(ctx, "Failed to stop doc parse task  err:%+v, req:%+v, rsp:%+v", err, req, rsp)
		return errs.ErrStopDocParseFail
	}
	return nil
}

// CreateDocAudit 创建文档送审任务
func (l *Logic) CreateDocAudit(ctx context.Context, doc *docEntity.Doc, envSet string) error {
	logx.I(ctx, "Prepare to CreateDocAudit doc:%+v", doc)
	if !config.AuditSwitch() {
		logx.I(ctx, "AuditSwitch is off, skip to CreateDocAudit")
		return nil
	}
	sendParams := entity.AuditSendParams{
		CorpID: doc.CorpID, StaffID: doc.StaffID, RobotID: doc.RobotID, Type: releaseEntity.AuditBizTypeDoc,
		RelateID: doc.ID, EnvSet: envSet,
	}

	if err := l.releaseDao.MysqlQuery().TAudit.WithContext(ctx).UnderlyingDB().Transaction(func(tx *gorm.DB) error {
		audit, err := l.releaseDao.CreateAuditByAuditSendParams(ctx, sendParams, tx)

		if err != nil {
			logx.E(ctx, "Failed to create audit record error. err:%+v", err)
			return err
		}

		sendParams.ParentAuditBizID = audit.BusinessID

		return async.NewAuditSendTask(ctx, audit.RobotID, sendParams)

	}); err != nil {
		logx.E(ctx, "Failed to create audit error. err:%+v", err)
		return err
	}
	return nil
}

// CreateInterveneDocAudit 创建文档干预内容送审任务
func (l *Logic) CreateInterveneDocAudit(ctx context.Context, doc *docEntity.Doc,
	interventionType uint32, envSet string) error {

	logx.I(ctx, "Prepare to CreateInterveneDocAudit doc:%+v", doc)
	if !config.AuditSwitch() {
		logx.I(ctx, "AuditSwitch is off, skip to CreateInterveneDocAudit")
		return nil
	}

	auditType := releaseEntity.AuditBizTypeDoc
	switch interventionType {
	case docEntity.InterventionTypeOrgData:
		auditType = releaseEntity.AuditBizTypeDocSegment
	case docEntity.InterventionTypeSheet:
		auditType = releaseEntity.AuditBizTypeDocTableSheet
	}

	sendParams := entity.AuditSendParams{
		CorpID: doc.CorpID, StaffID: doc.StaffID, RobotID: doc.RobotID, Type: auditType,
		RelateID: doc.ID, EnvSet: envSet, OriginDocBizID: doc.BusinessID,
	}

	if err := l.releaseDao.MysqlQuery().TAudit.WithContext(ctx).UnderlyingDB().Transaction(func(tx *gorm.DB) error {
		audit, err := l.releaseDao.CreateAuditByAuditSendParams(ctx, sendParams, tx)

		if err != nil {
			logx.E(ctx, "Failed to create audit record error. err:%+v", err)
			return err
		}

		sendParams.ParentAuditBizID = audit.BusinessID

		return async.NewAuditSendTask(ctx, audit.RobotID, sendParams)

	}); err != nil {
		logx.E(ctx, "Failed to create audit error. err:%+v", err)
		return err
	}
	return nil
}

func (l *Logic) DocParseSegment(ctx context.Context, tx *gorm.DB, doc *docEntity.Doc, intervene bool) error {
	taskID := ""
	docParse, err := l.GetDocParseByDocIDAndType(ctx, doc.ID, docEntity.DocParseTaskTypeWordCount, doc.RobotID)
	if err != nil {
		taskID = ""
	}
	if docParse.TaskID != "" {
		taskID = docParse.TaskID
	}
	if intervene {
		// 获取redis中的新文档cos信息(解析切分干预使用)
		app, err := l.rpc.AppAdmin.DescribeAppByPrimaryIdWithoutNotFoundError(ctx, doc.RobotID)
		if err != nil {
			logx.E(ctx, "DocParseSegment|getAppByAppBizID|err:%+v", err)
			return errs.ErrRobotNotFound
		}
		newDoc, redisValue, err := l.GetInterveneOldDocCosHashToNewDocRedisValue(ctx, doc.CorpID, app.BizId, doc.BusinessID, doc.CosHash)
		if err == nil && newDoc != nil && redisValue != nil && (redisValue.InterventionType == docEntity.InterventionTypeSheet ||
			redisValue.InterventionType == docEntity.InterventionTypeOrgData) {
			logx.I(ctx, "DocParseSegment|GetOldDocCosHashToNewDocRedisValue|docBizID:%d", doc.BusinessID)
			doc.CosURL = newDoc.CosURL
			doc.CosHash = newDoc.CosHash
		}
	}
	requestID := contextx.TraceID(ctx)
	if _, err = l.SendDocParseCreateSegment(ctx, taskID, doc, requestID, intervene); err != nil {
		return err
	}
	newDocParse := &docEntity.DocParse{
		DocID:        doc.ID,
		CorpID:       doc.CorpID,
		RobotID:      doc.RobotID,
		StaffID:      doc.StaffID,
		RequestID:    requestID,
		Type:         docEntity.DocParseTaskTypeSplitSegment,
		OpType:       docEntity.DocParseOpTypeSplit,
		Status:       docEntity.DocParseIng,
		TaskID:       taskID,
		SourceEnvSet: docParse.SourceEnvSet,
	}

	err = l.CreateDocParseTask(ctx, newDocParse)
	if err != nil {
		return err
	}
	return nil
}

// SendDocParseCreateSegment 文档提交解析生成文档分段
func (l *Logic) SendDocParseCreateSegment(ctx context.Context, taskID string, doc *docEntity.Doc,
	requestID string, intervene bool) (string, error) {
	logx.I(ctx, "SendDocParseCreateSegment|intervene:%t", intervene)
	appBaseInfo, err := l.rpc.AppAdmin.GetAppBaseInfoByPrimaryId(ctx, doc.RobotID)
	if err != nil {
		return "", err
	}
	if appBaseInfo == nil {
		return "", errs.ErrRobotNotFound
	}
	thirdModelConfig, err := l.getThirdModelConfig(ctx, doc.CorpID, appBaseInfo)
	if err != nil {
		// 如果获取不到第三方模型，柔性放过
		logx.E(ctx, "SendDocParseWordCount|getThirdModelConfig err:%v", err)
	}
	prefix := strings.TrimSuffix(doc.FileName, filepath.Ext(doc.FileName)) + ": \n"
	splitStrategy, err := config.App().RobotDefault.DocSplit.GetSplitStrategy(ctx, prefix, docEntity.DocSplitTypeDoc)
	if err != nil {
		return "", err
	}

	splitJSON, err := util.MergeJsonString(splitStrategy, doc.SplitRule)
	if err != nil {
		splitJSON = splitStrategy
		logx.W(ctx, "SendDocParseCreateSegment|MergeJsonString err:%v", err)
	}
	req := &fileManagerServer.TaskReq{
		RequestId: requestID,
		AppInfo: &fileManagerServer.AppInfo{
			AppKey: fmt.Sprintf("%d", appBaseInfo.BizId),
			Biz:    fileManagerBiz,
		},
		OpInfo: &fileManagerServer.TaskReq_OpInfo{
			FinalOpType:   docEntity.DocParseOpTypeSplit,
			CurrentOpType: docEntity.DocParseOpTypeSplit,
		},
		ParseStrategy: fileManagerServer.TaskReq_ParseStrategy(config.App().RobotDefault.ParseStrategy),
		ParseSetting: &fileManagerServer.TaskReq_ParseSetting{
			ParseStrategy:    fileManagerServer.TaskReq_ParseStrategy(config.App().RobotDefault.ParseStrategy),
			ThirdModelConfig: thirdModelConfig,
		},
		SplitStrategy:   splitJSON,
		FCosUrl:         doc.CosURL,
		FMd5:            doc.CosHash,
		BRecallProgress: docEntity.BRecallProgressFalse,
		Priority:        docEntity.DocParseTaskNorMal,
		MetaData: &fileManagerServer.TaskReq_MetaData{
			Version: l.getOfflineFileManagerVersion(),
		},
	}
	if intervene {
		fileType := docEntity.ConvertFileTypeToFileManagerServerFileType(doc.FileType)
		req.ParseSetting.OriginFileType = fileType
		logx.I(ctx, "SendDocParseCreateSegment|fileType:%d|RequestId:%s", fileType, requestID)
	}
	if taskID != "" {
		req.OpInfo.TaskId = taskID
	}
	if contextx.Metadata(ctx).Uin() == "" {
		// 文档解析需要在context里带上uin
		ctx = contextx.SetServerMetaData(ctx, contextx.MDUin, appBaseInfo.Uin)
	}
	rsp, err := l.rpc.FileManager.AddTask(ctx, req)
	logx.D(ctx, "提交解析任务 文档拆分文档分段 err:%+v, req:%+v, rsp:%+v", err, req, rsp)
	if err != nil {
		logx.E(ctx, "创建文档拆分文档提取分段任务，解析服务失败 err:%+v, req:%+v, rsp:%+v", err, req, rsp)
		return "", errs.ErrCreateDocParseSplitSegmentTaskFail
	}
	if rsp.StatusCode != 0 {
		logx.E(ctx, "创建文档拆分文档提取分段任务，解析服务失败 err:%+v, req:%+v, rsp:%+v", err, req, rsp)
		return "", errs.ErrCreateDocParseSplitSegmentTaskFail
	}
	taskID = rsp.TaskId
	return taskID, nil
}

// SendDocParse 文档提交解析统计字符数
func (l *Logic) SendDocParse(ctx context.Context, taskID string, doc *docEntity.Doc) (string, error) {
	robot, err := l.rpc.AppAdmin.DescribeAppByPrimaryIdWithoutNotFoundError(ctx, doc.RobotID)
	if err != nil {
		return "", err
	}
	if robot == nil {
		return "", errs.ErrRobotNotFound
	}
	splitStrategy, err := l.getRobotSplitStrategy(ctx, doc.FileName)
	if err != nil {
		return "", err
	}
	splitJSON, err := util.MergeJsonString(splitStrategy, doc.SplitRule)
	if err != nil {
		splitJSON = splitStrategy
		logx.W(ctx, "SendDocParseCreateSegment|MergeJsonString err:%v", err)
	}
	req := &fileManagerServer.TaskReq{
		RequestId: contextx.TraceID(ctx),
		AppInfo: &fileManagerServer.AppInfo{
			AppKey: fmt.Sprintf("%d", robot.BizId),
			Biz:    fileManagerBiz,
		},
		OpInfo: &fileManagerServer.TaskReq_OpInfo{
			FinalOpType:   docEntity.DocParseOpTypeSplit,
			CurrentOpType: docEntity.DocParseOpTypeWordCount,
		},
		ParseStrategy: fileManagerServer.TaskReq_ParseStrategy(config.App().RobotDefault.ParseStrategy),
		ParseSetting: &fileManagerServer.TaskReq_ParseSetting{
			ParseStrategy: fileManagerServer.TaskReq_ParseStrategy(config.App().RobotDefault.ParseStrategy),
		},
		SplitStrategy:   splitJSON,
		FCosUrl:         doc.CosURL,
		FMd5:            doc.CosHash,
		BRecallProgress: docEntity.BRecallProgressFalse,
		Priority:        docEntity.DocParseTaskNorMal,
		MetaData: &fileManagerServer.TaskReq_MetaData{
			Version: l.getOfflineFileManagerVersion(),
		},
	}
	if taskID != "" {
		req.OpInfo.TaskId = taskID
	}
	rsp, err := l.rpc.FileManager.AddTask(ctx, req)
	logx.D(ctx, "提交解析任务 统计字符数 err:%+v, req:%+v, rsp:%+v", err, req, rsp)
	if err != nil {
		logx.E(ctx, "提交解析文档统计字符数失败 err:%+v, req:%+v, rsp:%+v", err, req, rsp)
		return "", errs.ErrCreateDocToIndexTaskFail
	}
	taskID = rsp.TaskId
	return taskID, nil
}

// getRobotSplitStrategy 获取拆分策略配置
func (l *Logic) getRobotSplitStrategy(ctx context.Context, fileName string) (string, error) {
	prefix := strings.TrimSuffix(fileName, filepath.Ext(fileName)) + ": \n"
	splitStrategy, err := config.App().RobotDefault.DocSplit.GetSplitStrategy(ctx, prefix, docEntity.DocSplitTypeDoc)
	if err != nil {
		return "", err
	}
	return splitStrategy, nil
}

// RetryDocParseTask 重试 文档解析任务
func (l *Logic) RetryDocParseTask(ctx context.Context, taskID string, requestID string, robotBizID uint64) error {
	req := &fileManagerServer.RetryTaskReq{
		RequestId: requestID,
		AppInfo: &fileManagerServer.AppInfo{
			AppKey: fmt.Sprintf("%d", robotBizID),
			Biz:    fileManagerBiz,
		},
		TaskId: taskID,
	}
	rsp, err := l.rpc.FileManager.RetryTask(ctx, req)
	logx.D(ctx, "重试解析任务 err:%+v, req:%+v, rsp:%+v", err, req, rsp)
	if err != nil || rsp.StatusCode != 0 {
		logx.E(ctx, "重试解析文档任务失败 err:%+v, req:%+v, rsp:%+v", err, req, rsp)
		return errs.ErrRetryDocParseTaskFail
	}
	return nil
}

// SendDocParseCreateQA 文档提交解析生成问答对
func (l *Logic) SendDocParseCreateQA(ctx context.Context, doc *docEntity.Doc, splitStrategy, requestID string,
	robotBizID uint64) (
	string, error) {
	req := &fileManagerServer.TaskReq{
		RequestId: requestID,
		AppInfo: &fileManagerServer.AppInfo{
			AppKey: fmt.Sprintf("%d", robotBizID),
			Biz:    fileManagerBiz,
		},
		OpInfo: &fileManagerServer.TaskReq_OpInfo{
			FinalOpType:   docEntity.DocParseOpTypeSplit,
			CurrentOpType: docEntity.DocParseOpTypeSplit,
		},
		ParseStrategy: fileManagerServer.TaskReq_ParseStrategy(config.App().RobotDefault.ParseStrategy),
		ParseSetting: &fileManagerServer.TaskReq_ParseSetting{
			ParseStrategy: fileManagerServer.TaskReq_ParseStrategy(config.App().RobotDefault.ParseStrategy),
		},
		SplitStrategy:   splitStrategy,
		FCosUrl:         doc.CosURL,
		FMd5:            doc.CosHash,
		BRecallProgress: docEntity.BRecallProgressFalse,
		Priority:        docEntity.DocParseTaskNorMal,
		MetaData: &fileManagerServer.TaskReq_MetaData{
			Version: l.getOfflineFileManagerVersion(),
		},
	}
	rsp, err := l.rpc.FileManager.AddTask(ctx, req)
	logx.D(ctx, "提交解析任务 文档拆分问答对 err:%+v, req:%+v, rsp:%+v", err, req, rsp)
	if err != nil {
		logx.E(ctx, "创建文档拆分文档提取问答对任务，解析服务失败 err:%+v, req:%+v, rsp:%+v", err, req, rsp)
		return "", errs.ErrCreateDocParseSplitQATaskFail
	}
	if rsp.StatusCode != 0 {
		logx.E(ctx, "创建文档拆分文档提取问答对任务，解析服务失败 err:%+v, req:%+v, rsp:%+v", err, req, rsp)
		return "", errs.ErrCreateDocParseSplitQATaskFail
	}
	return rsp.TaskId, nil
}

func (l *Logic) GetInterveneOldDocCosHashToNewDocRedisValue(ctx context.Context, corpID, botBizID, docBizID uint64,
	oldDocCosHash string) (*docEntity.Doc, *docEntity.DocParsingInterventionRedisValue, error) {
	redisKey := interveneOldDocCosHashToNewDocRedisKey(corpID, botBizID, docBizID, oldDocCosHash)
	// redisValue, err := redis.String(l.rawSqlDao.RedisCli().Do(ctx, "GET", redisKey))
	redisValue, err := l.docDao.RedisCli().Get(ctx, redisKey).Result()
	if err == nil {
		if redisValue == "" {
			logx.E(ctx, "GetInterveneOldDocCosHashToNewDocRedisValue: redis value is empty")
			return nil, nil, fmt.Errorf("redis value is empty")
		}
		var redisValueDoc = &docEntity.DocParsingInterventionRedisValue{}
		if err1 := jsonx.UnmarshalFromString(redisValue, redisValueDoc); err1 != nil {
			logx.E(ctx, "GetInterveneOldDocCosHashToNewDocRedisValue: unmarshal redis value failed, %+v", err1)
			return nil, nil, err1
		}
		logx.I(ctx, "GetInterveneOldDocCosHashToNewDocRedisValue result: redisKey:%s, value: %+v", redisKey, redisValueDoc.OldDoc)
		return redisValueDoc.OldDoc, redisValueDoc, nil
	}
	// if errors.Is(err, redis.Nil) { // key不存在
	//	return nil, nil, nil
	// }
	logx.W(ctx, "GetInterveneOldDocCosHashToNewDocRedisValue failed: redisKey:%s, %+v", redisKey, err)
	return nil, nil, err
}

func (l *Logic) SetInterveneOldDocCosHashToNewDocRedisValueByDoc(ctx context.Context, appBizID uint64, cosPath,
	cosHash string, oldDoc *docEntity.Doc, interventionType uint32) error {
	if oldDoc == nil {
		return errors.New("oldDoc is nil")
	}
	redisKey := interveneOldDocCosHashToNewDocRedisKey(oldDoc.CorpID, appBizID, oldDoc.BusinessID, oldDoc.CosHash)
	doc := &docEntity.Doc{
		FileName: oldDoc.FileName,
		FileType: oldDoc.FileType,
		FileSize: oldDoc.FileSize,
		CosURL:   cosPath,
		CosHash:  cosHash,
	}
	redisValue := &docEntity.DocParsingInterventionRedisValue{
		OldDoc:           doc,
		InterventionType: interventionType,
		OriginDocBizID:   oldDoc.BusinessID,
	}
	var redisValueStr string
	redisValueStr, err := jsonx.MarshalToString(redisValue)
	if err != nil {
		logx.E(ctx, "SetInterveneOldDocCosHashToNewDocRedisValueByDoc|marshal redisValue failed, key:%s, %+v", redisKey, err)
		return err
	}
	// 最多保存7天
	// if _, err = l.rawSqlDao.RedisCli().Do(ctx, "SET", redisKey, redisValueStr, "EX", 86400*7); err != nil {
	if _, err = l.docDao.RedisCli().Set(ctx, redisKey, redisValueStr, 86400*7*time.Second).Result(); err != nil {
		logx.E(ctx, "SetInterveneOldDocCosHashToNewDocRedisValueByDoc|set redis value1 failed, key:%s, %+v", redisKey, err)
		return err
	}
	logx.I(ctx, "SetInterveneOldDocCosHashToNewDocRedisValueByDoc|set ok, key:%s", redisKey)
	return nil
}

func (l *Logic) DeleteInterveneOldDocCosHashToNewDocRedisValue(ctx context.Context, corpID, botBizID, docBizID uint64,
	oldDocCosHash string) error {
	redisKey := interveneOldDocCosHashToNewDocRedisKey(corpID, botBizID, docBizID, oldDocCosHash)
	// if _, err := l.rawSqlDao.RedisCli().Do(ctx, "DEL", redisKey); err != nil {
	if _, err := l.docDao.RedisCli().Del(ctx, redisKey).Result(); err != nil {
		logx.E(ctx, "DeleteInterveneOldDocCosHashToNewDocRedisValue: del redis key failed, key:%s, %+v",
			redisKey, err)
		return err
	}
	logx.I(ctx, "DeleteInterveneOldDocCosHashToNewDocRedisValue ok, redisKey: %s", redisKey)
	return nil
}

// 异步解析文档
func (l *Logic) AsyncParseDoc(ctx context.Context, doc *docEntity.Doc, interventionType uint32) {
	go func(rCtx context.Context) {
		// 需要sleep 1秒，等字数统计的回调接口先正常返回，再请求解析拆分
		time.Sleep(1 * time.Second)
		intervene := interventionType == docEntity.InterventionTypeOrgData || interventionType == docEntity.InterventionTypeSheet
		if err := l.DocParseSegment(rCtx, nil, doc, intervene); err != nil {
			return
		}
	}(trpc.CloneContext(ctx))
}

func (l *Logic) DescribeAllUnfinishedDocParse(ctx context.Context, parseStatusStoreTimeSecond,
	refreshBatchSize int) ([]*docEntity.DocParse, error) {
	res := make([]*docEntity.DocParse, 0)
	parsingStatus := docEntity.DocParseIng
	if refreshBatchSize <= 0 || refreshBatchSize > 1000 {
		refreshBatchSize = 100
	}
	filter := &docEntity.DocParseFilter{
		Status:              []int32{int32(parsingStatus)},
		DeadlineForCreation: time.Now().Add(-time.Duration(parseStatusStoreTimeSecond) * time.Second),
		Limit:               refreshBatchSize,
	}

	dbClients := knowClient.GetAllGormClients(ctx, docEntity.DocParseTableName, []client.Option{}...)
	for _, db := range dbClients {
		offset := 0
		for {
			filter.Offset = offset
			unfinishedDocParses, err := l.docDao.GetDocParseListWithTx(ctx, []string{}, filter, db)
			if err != nil {
				logx.E(ctx, "GetDocParseListWithTx error. err:%+v", err)
				return nil, err
			}
			res = append(res, unfinishedDocParses...)
			if len(unfinishedDocParses) < filter.Limit {
				break
			}
			offset += filter.Limit
		}
	}
	return res, nil
}

func (l *Logic) DescribeDocParseStatus(ctx context.Context, docParses []*docEntity.DocParse,
	refreshBatchSize int) (*file_parse_common.DescribeTaskStatusListRsp, error) {
	if refreshBatchSize <= 0 || refreshBatchSize > 1000 {
		refreshBatchSize = 100
	}
	taskIds := slicex.Map(docParses, func(docParse *docEntity.DocParse) string {
		return docParse.TaskID
	})
	taskIds = slicex.Unique(taskIds)
	logx.D(ctx, "-----------------taskIds: %+v", taskIds)
	res := &file_parse_common.DescribeTaskStatusListRsp{}
	for _, batchTaskIds := range slicex.Chunk(taskIds, refreshBatchSize) {
		req := &file_parse_common.DescribeTaskStatusListReq{
			RequestId: uuid.NewString(),
			TaskIds:   batchTaskIds,
		}
		rsp, err := l.rpc.ParseRouter.DescribeTaskStatusList(ctx, req)
		logx.D(ctx, "-----------------DescribeTaskStatusList req=%+v\n rsp=%+v", req, rsp)
		if err != nil {
			logx.E(ctx, "DescribeTaskStatusList error. err:%+v", err)
			return nil, err
		}
		res.TaskStatusList = append(res.TaskStatusList, rsp.TaskStatusList...)
	}
	return res, nil
}
