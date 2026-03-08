package dao

import (
	"context"
	"errors"
	"fmt"
	"git.code.oa.com/trpc-go/trpc-database/redis"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	jsoniter "github.com/json-iterator/go"
	"time"

	"git.code.oa.com/trpc-go/trpc-database/mysql"
	"git.code.oa.com/trpc-go/trpc-go/client"
	"git.code.oa.com/trpc-go/trpc-go/log"
	knowClient "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	utilConfig "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/util/config"
	fileManagerServer "git.woa.com/dialogue-platform/proto/pb-stub/file_manager_server"
	"github.com/jmoiron/sqlx"
	"go.opentelemetry.io/otel/trace"
)

const (
	// fileManagerBiz 知识问答文档解析Biz
	fileManagerBiz = "knowledge"

	// 默认离线文档解析服务版本号 2
	defaultOfflineFileManagerVersion = 2
)

const (
	docParseFields = `
		id,robot_id,corp_id,request_id,doc_id,source_env_set,task_id,type,op_type,result,status,create_time,update_time
	`
	createDocParse = `
		INSERT INTO 
		    t_doc_parse (%s) 
		VALUES 
		    (null,:robot_id,:corp_id,:request_id,:doc_id,:source_env_set,:task_id,:type,:op_type,:result,:status,
			:create_time,:update_time)
	`
	updateDocParse = `
		UPDATE 
			t_doc_parse 
		SET
		    result = :result,
		    status = :status,
			request_id = :request_id,
		    update_time = :update_time 
		WHERE 
		    id = :id
	`
	getDocParses = `
		SELECT 
    		` + docParseFields + ` 
		FROM 
		    t_doc_parse 
		WHERE 
		    corp_id = ? AND robot_id = ?
		ORDER BY 
		    create_time DESC,id DESC 
		LIMIT ?,?
		`
	getDocParseByTaskIDAndOpType = `
		SELECT 
    		` + docParseFields + ` 
		FROM 
		    t_doc_parse 
		WHERE 
		    task_id = ? AND op_type = ? AND status = ?
		`
	getDocParseByDocIDAndOpType = `
		SELECT 
    		` + docParseFields + ` 
		FROM 
		    t_doc_parse 
		WHERE 
		    doc_id = ? and op_type = ? LIMIT 1
		`
	getDocParseByDocIDAndType = `
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
	getDocParseByDocID = `
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
	getDocParseByDocIDs = `
		SELECT 
    		` + docParseFields + ` 
		FROM 
		    t_doc_parse 
		WHERE 
		    type =? AND status IN ( ? , ? ) AND doc_id IN (%s) 
		ORDER BY 
			id DESC
		`
	getDocParseByDocIDAndTypeAndStaus = `
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
	getDocParseCanBeRetried = `
		SELECT 
    		` + docParseFields + ` 
		FROM 
		    t_doc_parse 
		WHERE 
		    doc_id = ? AND type =? %s
		ORDER BY 
			id DESC
		`
)

const (
	docParseTableName = "t_doc_parse"
)

// GetDocParses 获取文档解析任务列表
func (d *dao) GetDocParses(ctx context.Context, corpID, robotID uint64) ([]model.DocParse, error) {
	docParses := make([]model.DocParse, 0)
	page := 1
	pageSize := 50
	start := pageSize * (page - 1)
	end := pageSize * page
	args := []any{corpID, robotID, start, end}
	db := knowClient.DBClient(ctx, docParseTableName, robotID, []client.Option{}...)
	if err := db.QueryToStructs(ctx, &docParses, getDocParses, args...); err != nil {
		log.ErrorContextf(ctx, "获取解析文档任务失败 sql:%s args:%+v err:%+v", getDocParses, args, err)
		return nil, err
	}
	return docParses, nil
}

// GetDocParseByTaskIDAndOpType 获取文档解析任务（指定 TaskID 和 Type）
func (d *dao) GetDocParseByTaskIDAndOpType(ctx context.Context, taskID string, opType int32) (model.DocParse, error) {
	docParses := make([]model.DocParse, 0)
	args := []any{taskID, opType, model.DocParseIng}
	dbClients := knowClient.GetAllDbClients(ctx, docParseTableName, []client.Option{}...)
	for _, db := range dbClients {
		err := db.QueryToStructs(ctx, &docParses, getDocParseByTaskIDAndOpType, args...)
		if err != nil {
			log.ErrorContextf(ctx, "获取解析文档任务失败 sql:%s args:%+v err:%+v",
				getDocParseByTaskIDAndOpType, args, err)
			continue
		}
		if len(docParses) > 0 {
			return docParses[0], nil
		}
	}
	return model.DocParse{}, errs.ErrDocParseTaskNotFound
}

// GetDocParseByDocIDAndType 获取文档解析任务（指定 DocID 和 Type）
func (d *dao) GetDocParseByDocIDAndType(ctx context.Context, docID uint64, fType int32, robotID uint64) (model.DocParse, error) {
	docParses := make([]model.DocParse, 0)
	args := []any{docID, fType}
	db := knowClient.DBClient(ctx, docParseTableName, robotID, []client.Option{}...)
	if err := db.QueryToStructs(ctx, &docParses, getDocParseByDocIDAndType, args...); err != nil {
		log.ErrorContextf(ctx, "获取解析文档任务失败 sql:%s args:%+v err:%+v", getDocParseByDocIDAndType, args, err)
		return model.DocParse{}, err
	}
	if len(docParses) == 0 {
		return model.DocParse{}, errs.ErrDocParseTaskNotFound
	}
	return docParses[0], nil
}

// GetDocParseByDocID 获取文档解析任务（指定 DocID）
func (d *dao) GetDocParseByDocID(ctx context.Context, docID uint64, robotID uint64) (model.DocParse, error) {
	docParse := model.DocParse{}
	args := []any{docID}
	db := knowClient.DBClient(ctx, docParseTableName, robotID, []client.Option{}...)
	if err := db.QueryToStruct(ctx, &docParse, getDocParseByDocID, args...); err != nil {
		log.ErrorContextf(ctx, "获取解析文档任务失败 sql:%s args:%+v err:%+v", getDocParseByDocID, args, err)
		return docParse, err
	}
	return docParse, nil
}

// GetDocParseByDocIDs 获取文档解析任务（指定 DocIDs）
func (d *dao) GetDocParseByDocIDs(ctx context.Context, docIDs []uint64, robotID uint64) ([]model.DocParse, error) {
	docParses := make([]model.DocParse, 0)
	if len(docIDs) == 0 {
		return docParses, errs.ErrDocParseTaskNotFound
	}
	querySQL := fmt.Sprintf(getDocParseByDocIDs, placeholder(len(docIDs)))
	args := []any{model.DocParseTaskTypeWordCount, model.DocParseCallBackFailed, model.DocParseCallBackCancel}
	for _, docID := range docIDs {
		args = append(args, docID)
	}
	db := knowClient.DBClient(ctx, docParseTableName, robotID, []client.Option{}...)
	if err := db.QueryToStructs(ctx, &docParses, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取解析文档任务失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return docParses, errs.ErrDocParseTaskNotFound
	}
	return docParses, nil
}

// GetDocParseByDocIDAndTypeAndStatus 获取文档解析任务（指定 DocID、type、status）
func (d *dao) GetDocParseByDocIDAndTypeAndStatus(ctx context.Context, docID uint64, fType, status uint32, robotID uint64) (
	model.DocParse, error) {
	docParse := model.DocParse{}
	docParses := make([]model.DocParse, 0)
	args := []any{docID, fType, status}
	db := knowClient.DBClient(ctx, docParseTableName, robotID, []client.Option{}...)
	if err := db.QueryToStructs(ctx, &docParses, getDocParseByDocIDAndTypeAndStaus, args...); err != nil {
		log.ErrorContextf(ctx, "获取解析文档任务失败 sql:%s args:%+v err:%+v", getDocParseByDocIDAndTypeAndStaus, args, err)
		return docParse, errs.ErrDocParseTaskNotFound
	}
	if len(docParses) == 0 {
		return docParse, errs.ErrDocParseTaskNotFound
	}
	return docParses[0], nil
}

// GetDocParseByDocIDAndOpType 获取解析结果(DocID,op_type,robot_id)
func (d *dao) GetDocParseByDocIDAndOpType(ctx context.Context, docID uint64, opType uint32, robotID uint64) (model.DocParse, error) {
	var docParse model.DocParse
	args := []any{docID, opType}
	db := knowClient.DBClient(ctx, docParseTableName, robotID, []client.Option{}...)
	if err := db.QueryToStruct(ctx, &docParse, getDocParseByDocIDAndOpType, args...); err != nil {
		if errors.Is(err, mysql.ErrNoRows) {
			return docParse, nil
		}
		log.ErrorContextf(ctx, "获取解析文档解析结果失败 sql:%s args:%+v err:%+v", getDocParseByDocIDAndOpType, args, err)
		return docParse, err
	}
	return docParse, nil
}

// DocParseCanBeRetried 获取重试文档列表 获取文档解析任务（指定 DocID、type、status）
func (d *dao) DocParseCanBeRetried(ctx context.Context, docID uint64, fType uint32, status []uint32, robotID uint64) (
	[]model.DocParse, error) {
	docParses := make([]model.DocParse, 0)
	args := []any{docID, fType}
	var condition string
	if len(status) > 0 {
		condition += fmt.Sprintf(" AND status IN (%s)", placeholder(len(status)))
		for _, v := range status {
			args = append(args, v)
		}
	}
	querySQL := fmt.Sprintf(getDocParseCanBeRetried, condition)
	db := knowClient.DBClient(ctx, docParseTableName, robotID, []client.Option{}...)
	if err := db.QueryToStructs(ctx, &docParses, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取解析文档任务失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return docParses, err
	}
	return docParses, nil
}

// CreateDocParse 新建文档解析任务
func (d *dao) CreateDocParse(ctx context.Context, tx *sqlx.Tx, docParse model.DocParse) error {
	now := time.Now()
	docParse.UpdateTime = now
	docParse.CreateTime = now
	docParse.SourceEnvSet = getEnvSet(ctx)
	querySQL := fmt.Sprintf(createDocParse, docParseFields)
	db := knowClient.DBClient(ctx, docParseTableName, docParse.RobotID, []client.Option{}...)
	if err := db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		if _, err := tx.NamedExecContext(ctx, querySQL, docParse); err != nil {
			log.ErrorContextf(ctx, "新建文档解析任务 sql:%s docParse:%+v err:%+v", querySQL, docParse, err)
			return err
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "新建文档解析失败 err:%+v", err)
		return err
	}
	return nil
}

// CreateDocParseWithSourceEnvSet 新建文档解析任务
func (d *dao) CreateDocParseWithSourceEnvSet(ctx context.Context, tx *sqlx.Tx, docParse model.DocParse, sourceEnvSet string) error {
	now := time.Now()
	docParse.UpdateTime = now
	docParse.CreateTime = now
	docParse.SourceEnvSet = sourceEnvSet
	querySQL := fmt.Sprintf(createDocParse, docParseFields)
	db := knowClient.DBClient(ctx, docParseTableName, docParse.RobotID, []client.Option{}...)
	if err := db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		if _, err := tx.NamedExecContext(ctx, querySQL, docParse); err != nil {
			log.ErrorContextf(ctx, "新建文档解析任务 sql:%s docParse:%+v err:%+v", querySQL, docParse, err)
			return err
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "新建文档解析失败 err:%+v", err)
		return err
	}
	return nil
}

// CreateDocParseTask 新建文档解析任务
func (d *dao) CreateDocParseTask(ctx context.Context, docParse model.DocParse) error {
	db := knowClient.DBClient(ctx, docParseTableName, docParse.RobotID, []client.Option{}...)
	if err := db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		err := d.CreateDocParse(ctx, tx, docParse)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "创建文档失败 err:%+v", err)
		return err
	}
	return nil
}

// UpdateDocParseTaskTx 更新文档解析任务信息
func (d *dao) UpdateDocParseTaskTx(ctx context.Context, docParse model.DocParse) error {
	now := time.Now()
	db := knowClient.DBClient(ctx, docParseTableName, docParse.RobotID, []client.Option{}...)
	if err := db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		docParse.UpdateTime = now
		querySQL := updateDocParse
		if _, err := tx.NamedExecContext(ctx, querySQL, docParse); err != nil {
			log.ErrorContextf(ctx, "更新文档结果失败 sql:%s docParse:%+v err:%+v", querySQL, docParse, err)
			return err
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "更新文档结果失败 err:%+v", err)
		return err
	}
	return nil
}

// UpdateDocParseTask 更新文档解析任务信息
func (d *dao) UpdateDocParseTask(ctx context.Context, docParse model.DocParse) error {
	now := time.Now()
	docParse.UpdateTime = now
	querySQL := updateDocParse
	db := knowClient.DBClient(ctx, docParseTableName, docParse.RobotID, []client.Option{}...)
	if _, err := db.NamedExec(ctx, querySQL, docParse); err != nil {
		log.ErrorContextf(ctx, "更新文档结果失败 sql:%s docParse:%+v err:%+v", querySQL, docParse, err)
		return err
	}
	return nil
}

// SendDocParse 文档提交解析统计字符数
func (d *dao) SendDocParse(ctx context.Context, taskID string, doc *model.Doc) (string, error) {
	robot, err := d.GetAppByID(ctx, doc.RobotID)
	if err != nil {
		return "", err
	}
	if robot == nil {
		return "", errs.ErrRobotNotFound
	}
	splitStrategy, err := d.getRobotSplitStrategy(ctx, robot, doc.FileName)
	if err != nil {
		return "", err
	}
	splitJSON, err := util.MergeJsonString(splitStrategy, doc.SplitRule)
	if err != nil {
		splitJSON = splitStrategy
		log.WarnContextf(ctx, "SendDocParseCreateSegment|MergeJsonString err:%v", err)
	}
	req := &fileManagerServer.TaskReq{
		RequestId: trace.SpanContextFromContext(ctx).TraceID().String(),
		AppInfo: &fileManagerServer.AppInfo{
			AppKey: fmt.Sprintf("%d", robot.BusinessID),
			Biz:    fileManagerBiz,
		},
		OpInfo: &fileManagerServer.TaskReq_OpInfo{
			FinalOpType:   model.DocParseOpTypeSplit,
			CurrentOpType: model.DocParseOpTypeWordCount,
		},
		ParseStrategy: fileManagerServer.TaskReq_ParseStrategy(config.App().RobotDefault.ParseStrategy),
		ParseSetting: &fileManagerServer.TaskReq_ParseSetting{
			ParseStrategy: fileManagerServer.TaskReq_ParseStrategy(config.App().RobotDefault.ParseStrategy),
		},
		SplitStrategy:   splitJSON,
		FCosUrl:         doc.CosURL,
		FMd5:            doc.CosHash,
		BRecallProgress: model.BRecallProgressFalse,
		Priority:        model.DocParseTaskNorMal,
		MetaData: &fileManagerServer.TaskReq_MetaData{
			Version: d.getOfflineFileManagerVersion(),
		},
	}
	if taskID != "" {
		req.OpInfo.TaskId = taskID
	}
	rsp, err := knowClient.AddTask(ctx, req)
	log.DebugContextf(ctx, "提交解析任务 统计字符数 err:%+v, req:%+v, rsp:%+v", err, req, rsp)
	if err != nil {
		log.ErrorContextf(ctx, "提交解析文档统计字符数失败 err:%+v, req:%+v, rsp:%+v", err, req, rsp)
		return "", errs.ErrCreateDocToIndexTaskFail
	}
	taskID = rsp.TaskId
	return taskID, nil
}

// SendDocParseWordCount 文档提交解析统计字符数
func (d *dao) SendDocParseWordCount(ctx context.Context, doc *model.Doc,
	requestID string, originFileType string) (string, error) {
	robot, err := d.GetAppByID(ctx, doc.RobotID)
	if err != nil {
		return "", err
	}
	if robot == nil {
		return "", errs.ErrRobotNotFound
	}
	splitStrategy, err := d.getRobotSplitStrategy(ctx, robot, doc.FileName)
	if err != nil {
		return "", err
	}
	splitJSON, err := util.MergeJsonString(splitStrategy, doc.SplitRule)
	if err != nil {
		splitJSON = splitStrategy
		log.WarnContextf(ctx, "SendDocParseCreateSegment|MergeJsonString err:%v", err)
	}
	req := &fileManagerServer.TaskReq{
		RequestId: requestID,
		AppInfo: &fileManagerServer.AppInfo{
			AppKey: fmt.Sprintf("%d", robot.BusinessID),
			Biz:    fileManagerBiz,
		},
		OpInfo: &fileManagerServer.TaskReq_OpInfo{
			FinalOpType:   model.DocParseOpTypeSplit,
			CurrentOpType: model.DocParseOpTypeWordCount,
		},
		ParseStrategy: fileManagerServer.TaskReq_ParseStrategy(config.App().RobotDefault.ParseStrategy),
		ParseSetting: &fileManagerServer.TaskReq_ParseSetting{
			ParseStrategy: fileManagerServer.TaskReq_ParseStrategy(config.App().RobotDefault.ParseStrategy),
		},
		SplitStrategy:   splitJSON,
		FCosUrl:         doc.CosURL,
		FMd5:            doc.CosHash,
		BRecallProgress: model.BRecallProgressFalse,
		Priority:        model.DocParseTaskNorMal,
		MetaData: &fileManagerServer.TaskReq_MetaData{
			Version: d.getOfflineFileManagerVersion(),
		},
	}
	if originFileType != "" {
		fileType := ConvertFileTypeToFileManagerServerFileType(originFileType)
		req.ParseSetting.OriginFileType = fileType
	}
	rsp, err := knowClient.AddTask(ctx, req)
	log.DebugContextf(ctx, "提交解析任务 统计字符数 err:%+v, req:%+v, rsp:%+v", err, req, rsp)
	if err != nil || rsp.StatusCode != 0 {
		log.ErrorContextf(ctx, "提交解析文档统计字符数失败 err:%+v, req:%+v, rsp:%+v", err, req, rsp)
		return "", errs.ErrCreateDocToIndexTaskFail
	}
	return rsp.TaskId, nil
}

// SendDocParseCreateSegment 文档提交解析生成文档分段
func (d *dao) SendDocParseCreateSegment(ctx context.Context, taskID string, doc *model.Doc,
	requestID string, intervene bool) (string, error) {
	log.InfoContextf(ctx, "SendDocParseCreateSegment|intervene:%t", intervene)
	robot, err := d.GetAppByID(ctx, doc.RobotID)
	if err != nil {
		return "", err
	}
	if robot == nil {
		return "", errs.ErrRobotNotFound
	}
	splitStrategy, err := d.getRobotSplitStrategy(ctx, robot, doc.FileName)
	if err != nil {
		return "", err
	}

	splitJSON, err := util.MergeJsonString(splitStrategy, doc.SplitRule)
	if err != nil {
		splitJSON = splitStrategy
		log.WarnContextf(ctx, "SendDocParseCreateSegment|MergeJsonString err:%v", err)
	}
	req := &fileManagerServer.TaskReq{
		RequestId: requestID,
		AppInfo: &fileManagerServer.AppInfo{
			AppKey: fmt.Sprintf("%d", robot.BusinessID),
			Biz:    fileManagerBiz,
		},
		OpInfo: &fileManagerServer.TaskReq_OpInfo{
			FinalOpType:   model.DocParseOpTypeSplit,
			CurrentOpType: model.DocParseOpTypeSplit,
		},
		ParseStrategy: fileManagerServer.TaskReq_ParseStrategy(config.App().RobotDefault.ParseStrategy),
		ParseSetting: &fileManagerServer.TaskReq_ParseSetting{
			ParseStrategy: fileManagerServer.TaskReq_ParseStrategy(config.App().RobotDefault.ParseStrategy),
		},
		SplitStrategy:   splitJSON,
		FCosUrl:         doc.CosURL,
		FMd5:            doc.CosHash,
		BRecallProgress: model.BRecallProgressFalse,
		Priority:        model.DocParseTaskNorMal,
		MetaData: &fileManagerServer.TaskReq_MetaData{
			Version: d.getOfflineFileManagerVersion(),
		},
	}
	if intervene {
		fileType := ConvertFileTypeToFileManagerServerFileType(doc.FileType)
		req.ParseSetting.OriginFileType = fileType
		log.InfoContextf(ctx, "SendDocParseCreateSegment|fileType:%d|RequestId:%s", fileType, requestID)
	}
	if taskID != "" {
		req.OpInfo.TaskId = taskID
	}
	rsp, err := knowClient.AddTask(ctx, req)
	log.DebugContextf(ctx, "提交解析任务 文档拆分文档分段 err:%+v, req:%+v, rsp:%+v", err, req, rsp)
	if err != nil {
		log.ErrorContextf(ctx, "创建文档拆分文档提取分段任务，解析服务失败 err:%+v, req:%+v, rsp:%+v", err, req, rsp)
		return "", errs.ErrCreateDocParseSplitSegmentTaskFail
	}
	if rsp.StatusCode != 0 {
		log.ErrorContextf(ctx, "创建文档拆分文档提取分段任务，解析服务失败 err:%+v, req:%+v, rsp:%+v", err, req, rsp)
		return "", errs.ErrCreateDocParseSplitSegmentTaskFail
	}
	taskID = rsp.TaskId
	return taskID, nil
}

// SendDocParseCreateQA 文档提交解析生成问答对
func (d *dao) SendDocParseCreateQA(ctx context.Context, doc *model.Doc, splitStrategy, requestID string,
	robotBizID uint64) (
	string, error) {
	req := &fileManagerServer.TaskReq{
		RequestId: requestID,
		AppInfo: &fileManagerServer.AppInfo{
			AppKey: fmt.Sprintf("%d", robotBizID),
			Biz:    fileManagerBiz,
		},
		OpInfo: &fileManagerServer.TaskReq_OpInfo{
			FinalOpType:   model.DocParseOpTypeSplit,
			CurrentOpType: model.DocParseOpTypeSplit,
		},
		ParseStrategy: fileManagerServer.TaskReq_ParseStrategy(config.App().RobotDefault.ParseStrategy),
		ParseSetting: &fileManagerServer.TaskReq_ParseSetting{
			ParseStrategy: fileManagerServer.TaskReq_ParseStrategy(config.App().RobotDefault.ParseStrategy),
		},
		SplitStrategy:   splitStrategy,
		FCosUrl:         doc.CosURL,
		FMd5:            doc.CosHash,
		BRecallProgress: model.BRecallProgressFalse,
		Priority:        model.DocParseTaskNorMal,
		MetaData: &fileManagerServer.TaskReq_MetaData{
			Version: d.getOfflineFileManagerVersion(),
		},
	}
	opts := []client.Option{WithTrpcSelector()}
	rsp, err := d.docParseCli.AddTask(ctx, req, opts...)
	log.DebugContextf(ctx, "提交解析任务 文档拆分问答对 err:%+v, req:%+v, rsp:%+v", err, req, rsp)
	if err != nil {
		log.ErrorContextf(ctx, "创建文档拆分文档提取问答对任务，解析服务失败 err:%+v, req:%+v, rsp:%+v", err, req, rsp)
		return "", errs.ErrCreateDocParseSplitQATaskFail
	}
	if rsp.StatusCode != 0 {
		log.ErrorContextf(ctx, "创建文档拆分文档提取问答对任务，解析服务失败 err:%+v, req:%+v, rsp:%+v", err, req, rsp)
		return "", errs.ErrCreateDocParseSplitQATaskFail
	}
	return rsp.TaskId, nil
}

// StopDocParseTask 终止 文档解析任务
func (d *dao) StopDocParseTask(ctx context.Context, taskID string, requestID string, robotBizID uint64) error {
	req := &fileManagerServer.CancelTaskReq{
		RequestId: requestID,
		AppInfo: &fileManagerServer.AppInfo{
			AppKey: fmt.Sprintf("%d", robotBizID),
			Biz:    fileManagerBiz,
		},
		TaskId: taskID,
	}
	opts := []client.Option{WithTrpcSelector()}
	rsp, err := d.docParseCli.CancelTask(ctx, req, opts...)
	log.DebugContextf(ctx, "终止解析文档任务 err:%+v, req:%+v, rsp:%+v", err, req, rsp)
	if err != nil || rsp.StatusCode != 0 {
		log.ErrorContextf(ctx, "终止解析文档任务失败  err:%+v, req:%+v, rsp:%+v", err, req, rsp)
		return errs.ErrStopDocParseFail
	}
	return nil
}

// RetryDocParseTask 重试 文档解析任务
func (d *dao) RetryDocParseTask(ctx context.Context, taskID string, requestID string, robotBizID uint64) error {
	req := &fileManagerServer.RetryTaskReq{
		RequestId: requestID,
		AppInfo: &fileManagerServer.AppInfo{
			AppKey: fmt.Sprintf("%d", robotBizID),
			Biz:    fileManagerBiz,
		},
		TaskId: taskID,
	}
	opts := []client.Option{WithTrpcSelector()}
	rsp, err := d.docParseCli.RetryTask(ctx, req, opts...)
	log.DebugContextf(ctx, "重试解析任务 err:%+v, req:%+v, rsp:%+v", err, req, rsp)
	if err != nil || rsp.StatusCode != 0 {
		log.ErrorContextf(ctx, "重试解析文档任务失败 err:%+v, req:%+v, rsp:%+v", err, req, rsp)
		return errs.ErrRetryDocParseTaskFail
	}
	return nil
}

// getOfflineFileManagerVersion 离线文档解析服务版本号
func (d *dao) getOfflineFileManagerVersion() int32 {
	// 实时文档解析版本号
	fileManagerVersion := utilConfig.GetMainConfig().FileParseConfig.OfflineFileManagerVersion
	if fileManagerVersion <= 0 {
		fileManagerVersion = defaultOfflineFileManagerVersion
	}
	return int32(fileManagerVersion)
}

// CreateDocAudit 创建文档送审任务
func (d *dao) CreateDocAudit(ctx context.Context, doc *model.Doc, envSet string) error {
	if err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		if err := d.createAudit(ctx, model.AuditSendParams{
			CorpID: doc.CorpID, StaffID: doc.StaffID, RobotID: doc.RobotID, Type: model.AuditBizTypeDoc,
			RelateID: doc.ID, EnvSet: envSet,
		}); err != nil {
			log.ErrorContextf(ctx, "创建文档送审任务失败 err:%+v", err)
			return err
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "创建发布送审失败 err:%+v", err)
		return err
	}
	return nil
}

// CreateInterveneDocAudit 创建文档干预内容送审任务
func (d *dao) CreateInterveneDocAudit(ctx context.Context, doc *model.Doc, interventionType uint32, envSet string) error {
	if err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		auditType := model.AuditBizTypeDoc
		switch interventionType {
		case model.InterventionTypeOrgData:
			auditType = model.AuditBizTypeDocSegment
		case model.InterventionTypeSheet:
			auditType = model.AuditBizTypeDocTableSheet
		}
		if err := d.createAudit(ctx, model.AuditSendParams{
			CorpID: doc.CorpID, StaffID: doc.StaffID, RobotID: doc.RobotID, Type: auditType,
			RelateID: doc.ID, EnvSet: envSet, OriginDocBizID: doc.BusinessID,
		}); err != nil {
			log.ErrorContextf(ctx, "创建文档送审任务失败 err:%+v", err)
			return err
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "创建发布送审失败 err:%+v", err)
		return err
	}
	return nil
}

func (d *dao) GetInterveneOldDocCosHashToNewDocRedisValue(ctx context.Context, corpID, botBizID, docBizID uint64,
	oldDocCosHash string) (*model.Doc, *model.DocParsingInterventionRedisValue, error) {
	redisKey := util.InterveneOldDocCosHashToNewDocRedisKey(corpID, botBizID, docBizID, oldDocCosHash)
	redisValue, err := redis.String(d.RedisCli().Do(ctx, "GET", redisKey))
	if err == nil {
		if redisValue == "" {
			log.ErrorContextf(ctx, "GetInterveneOldDocCosHashToNewDocRedisValue: redis value is empty")
			return nil, nil, fmt.Errorf("redis value is empty")
		}
		var redisValueDoc = &model.DocParsingInterventionRedisValue{}
		if err1 := jsoniter.UnmarshalFromString(redisValue, redisValueDoc); err1 != nil {
			log.ErrorContextf(ctx, "GetInterveneOldDocCosHashToNewDocRedisValue: unmarshal redis value failed, %+v", err1)
			return nil, nil, err1
		}
		log.InfoContextf(ctx, "GetInterveneOldDocCosHashToNewDocRedisValue result: redisKey:%s, value: %+v", redisKey, redisValueDoc.OldDoc)
		return redisValueDoc.OldDoc, redisValueDoc, nil
	}
	//if errors.Is(err, redis.ErrNil) { // key不存在
	//	return nil, nil, nil
	//}
	log.WarnContextf(ctx, "GetInterveneOldDocCosHashToNewDocRedisValue failed: redisKey:%s, %+v", redisKey, err)
	return nil, nil, err
}

func (d *dao) SetInterveneOldDocCosHashToNewDocRedisValueByDoc(ctx context.Context, appBizID uint64, cosPath,
	cosHash string, oldDoc *model.Doc, interventionType uint32) error {
	if oldDoc == nil {
		return errors.New("oldDoc is nil")
	}
	redisKey := util.InterveneOldDocCosHashToNewDocRedisKey(oldDoc.CorpID, appBizID, oldDoc.BusinessID, oldDoc.CosHash)
	doc := &model.Doc{
		FileName: oldDoc.FileName,
		FileType: oldDoc.FileType,
		FileSize: oldDoc.FileSize,
		CosURL:   cosPath,
		CosHash:  cosHash,
	}
	redisValue := &model.DocParsingInterventionRedisValue{
		OldDoc:           doc,
		InterventionType: interventionType,
		OriginDocBizID:   oldDoc.BusinessID,
	}
	var redisValueStr string
	redisValueStr, err := jsoniter.MarshalToString(redisValue)
	if err != nil {
		log.ErrorContextf(ctx, "SetInterveneOldDocCosHashToNewDocRedisValueByDoc|marshal redisValue failed, key:%s, %+v", redisKey, err)
		return err
	}
	// 最多保存7天
	if _, err = d.RedisCli().Do(ctx, "SET", redisKey, redisValueStr, "EX", 86400*7); err != nil {
		log.ErrorContextf(ctx, "SetInterveneOldDocCosHashToNewDocRedisValueByDoc|set redis value1 failed, key:%s, %+v", redisKey, err)
		return err
	}
	log.InfoContextf(ctx, "SetInterveneOldDocCosHashToNewDocRedisValueByDoc|set ok, key:%s", redisKey)
	return nil
}

func (d *dao) DeleteInterveneOldDocCosHashToNewDocRedisValue(ctx context.Context, corpID, botBizID, docBizID uint64,
	oldDocCosHash string) error {
	redisKey := util.InterveneOldDocCosHashToNewDocRedisKey(corpID, botBizID, docBizID, oldDocCosHash)
	if _, err := d.RedisCli().Do(ctx, "DEL", redisKey); err != nil {
		log.ErrorContextf(ctx, "DeleteInterveneOldDocCosHashToNewDocRedisValue: del redis key failed, key:%s, %+v",
			redisKey, err)
		return err
	}
	log.InfoContextf(ctx, "DeleteInterveneOldDocCosHashToNewDocRedisValue ok, redisKey: %s", redisKey)
	return nil
}

func ConvertFileTypeToFileManagerServerFileType(fileType string) fileManagerServer.FileType {
	newFileType := fileManagerServer.FileType_UNKNOWN_FILE_TYPE
	switch fileType {
	case model.FileTypePdf:
		newFileType = fileManagerServer.FileType_PDF
	case model.FileTypeDoc:
		newFileType = fileManagerServer.FileType_DOC
	case model.FileTypeDocx:
		newFileType = fileManagerServer.FileType_DOCX
	case model.FileTypeXls:
		newFileType = fileManagerServer.FileType_XLS
	case model.FileTypeXlsx:
		newFileType = fileManagerServer.FileType_XLSX
	case model.FileTypePpt:
		newFileType = fileManagerServer.FileType_PPT
	case model.FileTypePptx:
		newFileType = fileManagerServer.FileType_PPTX
	case model.FileTypeMD:
		newFileType = fileManagerServer.FileType_MD
	case model.FileTypeTxt:
		newFileType = fileManagerServer.FileType_TXT
	case model.FileTypePng:
		newFileType = fileManagerServer.FileType_PNG
	case model.FileTypeJpg:
		newFileType = fileManagerServer.FileType_JPG
	case model.FileTypeJpeg:
		newFileType = fileManagerServer.FileType_JPEG
	case model.FileTypeCsv:
		newFileType = fileManagerServer.FileType_CSV
	case model.FileTypeBmp:
		newFileType = fileManagerServer.FileType_BMP
	case model.FileTypeGif:
		newFileType = fileManagerServer.FileType_GIF
	case model.FileTypeWebp:
		newFileType = fileManagerServer.FileType_WEBP
	case model.FileTypeHeif:
		newFileType = fileManagerServer.FileType_HEIF
	case model.FileTypeHeic:
		newFileType = fileManagerServer.FileType_HEIC
	case model.FileTypeJp2:
		newFileType = fileManagerServer.FileType_JP2
	case model.FileTypeEps:
		newFileType = fileManagerServer.FileType_EPS
	case model.FileTypeIcns:
		newFileType = fileManagerServer.FileType_ICNS
	case model.FileTypeIm:
		newFileType = fileManagerServer.FileType_IM
	case model.FileTypePcx:
		newFileType = fileManagerServer.FileType_PCX
	case model.FileTypePpm:
		newFileType = fileManagerServer.FileType_PPM
	case model.FileTypeTiff:
		newFileType = fileManagerServer.FileType_TIFF
	case model.FileTypeXbm:
		newFileType = fileManagerServer.FileType_XBM
	case model.FileTypePpsm:
		newFileType = fileManagerServer.FileType_PPSM
	case model.FileTypePPsx:
		newFileType = fileManagerServer.FileType_PPSX
	case model.FileTypeWps:
		newFileType = fileManagerServer.FileType_WPS
	case model.FileTypeEpub:
		newFileType = fileManagerServer.FileType_EPUB
	case model.FileTypeTsv:
		newFileType = fileManagerServer.FileType_TSV
	case model.FileTypeHtml:
		newFileType = fileManagerServer.FileType_HTML
	}
	return newFileType
}
