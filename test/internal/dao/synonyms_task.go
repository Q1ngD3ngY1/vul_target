package dao

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/i18nkey"
	"strings"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
	"github.com/jmoiron/sqlx"
	"github.com/xuri/excelize/v2"
)

const (
	synonymsTaskFields = `
        id,corp_id,robot_id,create_staff_id,params,status,message,file_name,cos_url,error_cos_url, update_time,create_time
    `

	createSynonymsTask = `
        INSERT INTO
            t_synonyms_task (%s)
        VALUES 
            (null,:corp_id,:robot_id,:create_staff_id,:params,:status,:message,:file_name,:cos_url,:error_cos_url,NOW(),NOW())   
    `

	getSynonymsTaskInfo = `
	    SELECT
      		%s
    	FROM
      		t_synonyms_task 
		WHERE
			id = ? AND corp_id = ? and robot_id = ? 
	`

	updateSynonymsTaskStatus = `
        UPDATE 
            t_synonyms_task
        SET 
            status = :status,
			message = :message,
            update_time = :update_time
        WHERE
            id = :id AND corp_id = :corp_id AND robot_id = :robot_id
    `

	updateSynonymsTaskErrorCosUrl = `
        UPDATE 
            t_synonyms_task
        SET 
            error_cos_url = :error_cos_url,
            update_time = :update_time
        WHERE
            id = :id AND corp_id = :corp_id AND robot_id = :robot_id
    `
)

type SynonymsTaskType int

const (
	SynonymsTaskTypeImport SynonymsTaskType = iota
)

// UpdateSynonymsTaskErrorCosUrl 更新导入任务ErrorCosUrl信息
func (d *dao) UpdateSynonymsTaskErrorCosUrl(ctx context.Context, synonymsTask *model.SynonymsTask) error {
	log.DebugContextf(ctx, "updateSynonymsTaskErrorCosUrl,  synonymsTaskID: %+v", synonymsTask.ID)
	synonymsTask.UpdateTime = time.Now()
	_, err := d.db.NamedExec(ctx, updateSynonymsTaskErrorCosUrl, synonymsTask)
	if err != nil {
		log.ErrorContextf(ctx, "更新同义词任务errorCosUrl失败 sql:%s synonymsTask:%+v err:%+v",
			updateSynonymsTaskErrorCosUrl,
			synonymsTask, err)
		return err
	}

	return nil
}

// GetSynonymsTaskInfo 获取更新同义词异步任务信息
func (d *dao) GetSynonymsTaskInfo(ctx context.Context, taskID, corpID, robotID uint64) (*model.SynonymsTask, error) {
	log.DebugContextf(ctx, "GetSynonymsTaskInfo,params: %d, %d, %d", taskID, corpID, robotID)
	sql := fmt.Sprintf(getSynonymsTaskInfo, synonymsTaskFields)
	args := []any{taskID, corpID, robotID}
	synonymsTask := make([]*model.SynonymsTask, 0)
	if err := d.db.QueryToStructs(ctx, &synonymsTask, sql, args...); err != nil {
		log.ErrorContextf(ctx, "获取同义词异步任务信息--失败 sql:%+v, err:%+v", sql, err)
		return nil, err
	}
	if len(synonymsTask) == 0 {
		log.ErrorContextf(ctx, "获取同义词异步任务信息--为空 , sql: %s, args: %+v ", sql, args)
		return nil, nil
	}
	return synonymsTask[0], nil
}

// createSynonymsTask 创建同义词任务
func (d *dao) createSynonymsTask(ctx context.Context, t *model.SynonymsTask) (uint64,
	error) {
	if t.CorpID <= 0 || t.CreateStaffID <= 0 || t.RobotID <= 0 {
		log.ErrorContextf(ctx, "createSynonymsTask args err:%+v", t)
		return 0, errs.ErrParams
	}
	execSQL := fmt.Sprintf(createSynonymsTask, synonymsTaskFields)
	res, err := d.db.NamedExec(ctx, execSQL, t)
	if err != nil {
		log.ErrorContextf(ctx, "创建同义词任务失败 sql:%s args:%+v err:%+v", execSQL, t, err)
		return 0, err
	}
	taskID, _ := res.LastInsertId()
	return uint64(taskID), nil
}

// UpdateSynonymsImportTaskStatus 更新同义词导入任务状态,并发送Notice
func (d *dao) UpdateSynonymsImportTaskStatus(ctx context.Context, t *model.SynonymsTask, status int) error {
	switch status {
	case model.SynonymsTaskStatusFailed:
		t.Status = model.SynonymsTaskStatusFailed
		t.Message = i18nkey.KeyParseImportFail
	case model.SynonymsTaskStatusSuccess:
		t.Status = model.SynonymsTaskStatusSuccess
		t.Message = "导入成功"
	case model.SynonymsTaskStatusRunning:
		t.Status = model.SynonymsTaskStatusRunning
		t.Message = "导入中"
	}
	t.UpdateTime = time.Now()
	err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		if _, err := tx.NamedExecContext(ctx, updateSynonymsTaskStatus, t); err != nil {
			log.ErrorContextf(ctx, "更新同义词任务状态 sql:%s taskInfo:%+v err:%+v", updateSynonymsTaskStatus, t, err)
			return err
		}
		if err := d.sendSynonymsImportNotice(ctx, tx, t); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		log.ErrorContextf(ctx, "UpdateSynonymsImportTaskStatus fail err:%+v", err)
		return err
	}
	return nil
}

// createImportSynonymsTask 添加同义词导入异步任务
func (d *dao) createSynonymsImportTask(ctx context.Context, req *pb.UploadSynonymsListReq,
	corpID, staffID, robotID uint64) (uint64, error) {
	log.DebugContextf(ctx, "createSynonymsImportTask req:%v", req)
	now := time.Now()
	synonymsTask := model.SynonymsTask{
		CorpID:        corpID,
		RobotID:       robotID,
		CreateStaffID: staffID,
		Status:        model.SynonymsTaskStatusPending,
		Message:       "未启动",
		FileName:      req.FileName,
		CosURL:        req.CosUrl,
		CreateTime:    now,
		UpdateTime:    now,
	}
	taskID, err := d.createSynonymsTask(ctx, &synonymsTask)
	if err != nil {
		log.ErrorContextf(ctx, "添加同义词异步任务失败 err:%+v", err)
		return 0, err
	}
	log.InfoContextf(ctx, "createSynonymsImportTask taskID: %d", taskID)
	return taskID, nil
}

// CreateSynonymsImportTask 创建同义词导入任务
func (d *dao) CreateSynonymsImportTask(ctx context.Context, req *pb.UploadSynonymsListReq,
	corpID, staffID, robotID uint64) (uint64, error) {
	taskID, err := d.createSynonymsImportTask(ctx, req, corpID, staffID, robotID)
	if err != nil {
		log.ErrorContextf(ctx, "同义词导入任务|添加任务记录失败 err:%+v", err)
		return 0, err
	}
	taskParams := model.SynonymsImportParams{
		CorpID:  corpID,
		StaffID: staffID,
		RobotID: robotID,
		TaskID:  taskID,
	}
	if err = newSynonymsImportTask(ctx, robotID, taskParams); err != nil {
		log.ErrorContextf(ctx, "同义词导入任务|创建任务失败 taskId:%d, err:%+v", taskID, err)
		return 0, err
	}
	return taskID, nil
}

// sendSynonymsImportNotice 发送同义词任务通知
func (d *dao) sendSynonymsImportNotice(ctx context.Context, tx *sqlx.Tx, task *model.SynonymsTask) error {
	if task == nil {
		log.ErrorContextf(ctx, "SynonymsTask is nil")
		return errs.ErrSystem
	}
	var noticeContent, noticeSubject, noticeLevel string
	operations := make([]model.Operation, 0)
	// 前端没有独立的【同义词页面】,去掉详情
	/*
		operations = append(operations, model.Operation{
			Typ:    model.OpTypeViewDetail,
			Params: model.OpParams{},
		})
	*/
	switch task.Status {
	case model.SynonymsTaskStatusSuccess:
		noticeLevel = model.LevelSuccess
		noticeContent = i18n.Translate(ctx, i18nkey.KeySynonymImportSuccessWithName, task.FileName)
		noticeSubject = i18n.Translate(ctx, i18nkey.KeySynonymImportSuccess)
	case model.SynonymsTaskStatusFailed:
		noticeLevel = model.LevelError
		noticeContent = i18n.Translate(ctx, i18nkey.KeySynonymImportFailureWithName, task.FileName)
		noticeSubject = i18n.Translate(ctx, i18nkey.KeySynonymImportFailure)
		if len(task.ErrorCosURL) > 0 {
			noticeContent = i18n.Translate(ctx, i18nkey.KeySynonymImportFailureWithNameDownloadErrorFile, task.FileName)
			operations = append(operations, model.Operation{
				Typ:    model.OpTypeExportQADownload,
				Params: model.OpParams{CosPath: task.ErrorCosURL},
			})
		}
	case model.SynonymsTaskStatusRunning:
		noticeLevel = model.LevelInfo
		noticeContent = i18n.Translate(ctx, i18nkey.KeySynonymImportStartWithName, task.FileName)
		noticeSubject = i18n.Translate(ctx, i18nkey.KeySynonymImportStart)
	default:
		log.InfoContextf(ctx, "存在未知任务状态 %+v, 不发送Noice", task)
		return nil
	}
	noticeOptions := []model.NoticeOption{
		model.WithPageID(model.NoticeSynonymsPageID),
		model.WithLevel(noticeLevel),
		model.WithSubject(noticeSubject),
		model.WithContent(noticeContent),
		model.WithGlobalFlag(),
	}
	notice := model.NewNotice(model.NoticeTypeSynonymsImport, task.ID, task.CorpID, task.RobotID, task.CreateStaffID,
		noticeOptions...)
	if err := notice.SetOperation(operations); err != nil {
		log.ErrorContextf(ctx, "导出任务通知操作序列化失败 taskID:%+v err:%+v", task.ID, err)
		return err
	}
	if err := d.createNotice(ctx, tx, notice); err != nil {
		return err
	}
	return nil
}

// ParseExcelAndImportSynonyms  解析同义词并导入到DB,若存在错误,则回写excel并上传到COS(异步任务,文件校验前期已完成)
func (d *dao) ParseExcelAndImportSynonyms(ctx context.Context, cosURL string,
	fileName string, robotID, corpID uint64) (string,
	error) {
	body, err := d.GetObject(ctx, cosURL)
	if err != nil {
		log.ErrorContextf(ctx, "读取 xlsx cos 文件失败, filename:%+v, cos_url:%+v, err: %+v", fileName, cosURL, err)
		return "", err
	}
	f, err := excelize.OpenReader(bytes.NewReader(body))
	if err != nil {
		log.ErrorContextf(ctx, "读取 xlsx 文件失败, filename: %+v, err: %+v", fileName, err)
		return "", err
	}
	sheet := f.GetSheetName(0)
	rows, err := f.Rows(sheet)
	if err != nil {
		log.ErrorContextf(ctx, "解析 xlsx 文件失败, filename: %+v, err: %+v", fileName, err)
		return "", err
	}
	cates, err := d.GetCateList(ctx, model.SynonymsCate, corpID, robotID)
	if err != nil {
		log.ErrorContextf(ctx, "获取同义词分类失败, err: %+v", err)
		return "", err
	}
	cateTree := model.BuildCateTree(cates)
	i := -1
	errMap := map[int]string{}
	// 一次大事务里
	err = d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		for rows.Next() {
			i++
			if i == 0 { // 跳过表头行
				continue
			}
			row, err := rows.Columns()
			if err != nil {
				log.ErrorContextf(ctx, "解析 xlsx 文件失败, filename: %+v, err: %+v", fileName, err)
				return err
			}
			if len(row) == 0 {
				continue
			}
			if len(row) < model.SynonymsExcelTplHeadLen {
				log.ErrorContextf(ctx, "同义词文件:%+v, 第 %d 行,字段长度不对", fileName, i)
				errMap[i] = "缺少标准词/同义词"
				continue
			}
			standard := strings.TrimSpace(row[model.SynonymsExcelTplStandardIndex])
			synonyms := strings.TrimSpace(row[model.SynonymsExcelTplSynonymsIndex])
			synonymsList := pkg.SplitAndTrimString(synonyms, "\n")
			if len(synonymsList) == 0 {
				errMap[i] = "同义词为空"
				continue
			}
			// 新建分类
			_, catePath := model.GetCatePath(row)
			if len(catePath) == 0 {
				catePath = []string{model.UncategorizedCateName}
			}
			cateTree.Create(catePath)
			if err := d.createCates(ctx, tx, model.SynonymsCate, corpID, robotID, cateTree); err != nil {
				return err
			}
			cateTree.Find(catePath)
			// 新建同义词
			req := &model.SynonymsCreateReq{
				RobotID:      robotID,
				CorpID:       corpID,
				CateID:       uint64(cateTree.Find(catePath)),
				StandardWord: standard,
				Synonyms:     synonymsList,
			}
			log.DebugContextf(ctx, "导入同义词[idx:%d], req: %+v", i, req)
			rsp, err := d.createSynonymsWithSqlx(ctx, req, tx)
			if err != nil {
				return err
			}
			// 插入失败,有冲突
			if rsp.ConflictType != 0 {
				log.DebugContextf(ctx, "导入标准词有冲突: %s -- %s", standard, rsp.ConflictContent)
				errMsg := fmt.Sprintf("【标准词/%s】同义词冲突", rsp.ConflictContent)
				switch rsp.ConflictType {
				case model.SynonymsConflictTypeStandard:
					errMsg = fmt.Sprintf("【标准词/%s】当前标准词与已有标准词/同义词冲突", rsp.ConflictContent)
				case model.SynonymsConflictTypeSynonymsAndStandard:
					errMsg = fmt.Sprintf("【标准词/%s】当前同义词与已有标准词冲突", rsp.ConflictContent)
				case model.SynonymsConflictTypeSynonymsAndSynonyms:
					// 当前同义词与已有同义词的同义词冲突
					errMsg = fmt.Sprintf("【标准词/%s】当前同义词与已有同义词冲突", rsp.ConflictContent)
				default:
					log.ErrorContextf(ctx, "存在未知的冲突类型 %+v", rsp)
				}
				errMap[i] = errMsg
				return errs.ErrSynonymsTaskImportFailWithConflict
			}
		}
		return nil
	})
	if err != nil && !errors.Is(err, errs.ErrSynonymsTaskImportFailWithConflict) {
		log.ErrorContextf(ctx, "同义词导入失败, error: %+v", err)
		return "", err
	}
	// 数据全部导入成功
	if len(errMap) == 0 {
		log.InfoContextf(ctx, "同义词导入成功, fileName: %s", fileName)
		return "", nil
	}
	// 导入失败, 回写excel
	for i, msg := range errMap {
		cell, _ := excelize.CoordinatesToCellName(model.SynonymsExcelTplHeadLen+1, i+1, false)
		if err := f.SetCellValue(sheet, cell, msg); err != nil {
			log.ErrorContextf(ctx, "设置单元格值错误, fileName: %s, err: %+v", fileName, err)
			return "", errs.ErrSystem
		}
	}
	styleID, err := f.NewStyle(&excelize.Style{Font: &excelize.Font{Color: "FF0000"}})
	if err != nil {
		log.ErrorContextf(ctx, "创建样式错误, docName: %s, err: %+v", fileName, err)
		return "", errs.ErrSystem
	}
	col, _ := excelize.ColumnNumberToName(model.SynonymsExcelTplHeadLen + 1)
	if err := f.SetColStyle(sheet, col, styleID); err != nil {
		log.ErrorContextf(ctx, "设置列样式错误, docName: %s, err: %+v", fileName, err)
		return "", errs.ErrSystem
	}
	buf, err := f.WriteToBuffer()
	if err != nil {
		log.ErrorContextf(ctx, "回写 xlsx 文件失败, docName: %s, err: %+v", fileName, err)
		return "", errs.ErrSystem
	}

	// 上传到COS
	filename := fmt.Sprintf("synonyms-import-error-%d-%d.xlsx", corpID, time.Now().Unix())
	cosPath := d.GetCorpCOSFilePath(ctx, corpID, filename)
	if err = d.PutObject(ctx, buf.Bytes(), cosPath); err != nil {
		log.ErrorContextf(ctx, "上传 error xlsx 文件失败, corpID:%+v, robotID:%+v, cosPath:%+v err:%+v", corpID,
			robotID, cosPath, err)
		return "", err
	}
	log.InfoContextf(ctx, "错误标注文件上传成功, fileName: %s, errorCosURL: %s", fileName, cosPath)
	return cosPath, errs.ErrSynonymsTaskImportFailWithConflict
}
