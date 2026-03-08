// bot-knowledge-config-server
//
// @(#)export_evaluate_task.go  星期二, 五月 21, 2024
// Copyright(c) 2024, zrwang@Tencent. All rights reserved.

package dao

import (
	"context"
	"fmt"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"time"

	"github.com/xuri/excelize/v2"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util/sse/event"
)

// ExportEvaluateTask 导出 评测任务
type ExportEvaluateTask struct {
	Dao Dao
}

// getExportHeader 获取 评测任务 导出表头信息
func getExportHeader(ctx context.Context) []string {
	heads := make([]string, 0, len(config.App().SampleRule.ExportExcelHead))
	for _, v := range config.App().SampleRule.ExportExcelHead {
		heads = append(heads, i18n.Translate(ctx, v))
	}
	return heads
}

// CreateExportEvaluateTask 创建评测任务导出
func (d *dao) CreateExportEvaluateTask(ctx context.Context, corpID, bizID, robotID uint64) (string, error) {
	robotTest, err := d.GetTestByTestBizIDs(ctx, corpID, robotID, []uint64{bizID})
	if err != nil {
		return "", errs.ErrSystem
	}
	if len(robotTest) == 0 {
		return "", errs.ErrTestNotExist
	}
	record, err := d.GetTestRecordByTestID(ctx, robotTest[0].ID)
	if err != nil {
		return "", errs.ErrSystem
	}
	if len(record) == 0 {
		return "", errs.ErrRecords
	}
	var rows [][]string
	rows = append([][]string{getExportHeader(ctx)}, rows...)
	row := d.getExportEvaluateData(ctx, record)
	rows = append(rows, row...)
	// 有超过 MaxExportCount 的数据, 限制回 MaxExportCount
	if len(rows) > int(config.App().MaxExportCount) {
		rows = rows[:config.App().MaxExportCount]
	}
	f := excelize.NewFile()
	sheet := "Sheet1"
	for x, row := range rows {
		for y, cell := range row {
			cellName, err := excelize.CoordinatesToCellName(y+1, x+1)
			if err != nil {
				return "", err
			}
			if err = f.SetCellStr(sheet, cellName, cell); err != nil {
				return "", err
			}
		}
	}
	b, err := f.WriteToBuffer()
	if err != nil {
		return "", err
	}
	filename := fmt.Sprintf("export-%s-%d.xlsx", "evaluate-task", time.Now().Unix())
	cosPath := d.GetCorpCOSFilePath(ctx, corpID, filename)
	if err = d.PutObject(ctx, b.Bytes(), cosPath); err != nil {
		log.ErrorContextf(ctx, "导出任务上传cos失败, corpID:%+v,  cosPath:%+v err:%+v", corpID, cosPath, err)
		return "", err
	}
	return cosPath, nil
}

// getExportEvaluateData 创建评测任务导出 数据
func (d *dao) getExportEvaluateData(ctx context.Context, record []model.RobotTestRecord) [][]string {
	rows := make([][]string, 0)
	for _, r := range record {
		answerJudge, roleDescription, traceID := "", "", ""
		switch r.AnswerJudge {
		case 0:
			answerJudge = "未标注"
		case 1:
			answerJudge = "正确"
		case 2:
			answerJudge = "错误"
		}
		if r.RoleDescription.Valid {
			roleDescription = r.RoleDescription.String
		}
		if r.TraceID.Valid {
			traceID = r.TraceID.String
		}
		replyMethod := event.ReplyMethodDescription(event.ReplyMethod(r.ReplyMethod))
		rows = append(rows, []string{r.Question, r.Answer, answerJudge, traceID, replyMethod, roleDescription})
	}
	return rows
}
