// Package util 工具
package util

import (
	"bytes"
	"context"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/i18nkey"
	"reflect"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"git.woa.com/dialogue-platform/common/v3/sync/errgroupx"
	"github.com/xuri/excelize/v2"
)

// CheckFunc i 被检查行 row 被检查行的列值 uniqueKeys 用于判断行唯一性的集合
type CheckFunc func(ctx context.Context, i int, row []string, uniqueKeys map[string]int) string

// CheckXlsxFunc i 被检查行 row 被检查行的列值 uniqueKeys 用于判断行唯一性的集合
type CheckXlsxFunc func(ctx context.Context, i int, row []string, uniqueKeys *sync.Map, uin string, appBizID uint64,
	uniqueImgHost *sync.Map,
) string

// CheckContent 检查Excel模板文件内容是否符合要求
func CheckContent(ctx context.Context, fileName string, minRow, maxRow int, head []string, body []byte,
	check CheckFunc) ([][]string, []byte, error) {
	f, err := excelize.OpenReader(bytes.NewReader(body))
	if err != nil {
		log.ErrorContextf(ctx, "读取 xlsx 文件失败, docName: %s, err: %+v", fileName, err)
		return nil, nil, errs.ErrSystem
	}
	if f.SheetCount == 0 {
		return nil, nil, errs.ErrExcelIsEmpty
	}
	sheet := f.GetSheetName(0)
	rows, err := f.Rows(sheet)
	if err != nil {
		log.ErrorContextf(ctx, "读取 xlsx 文件失败, docName: %s, err: %+v", fileName, err)
		return nil, nil, errs.ErrSystem
	}
	i, max, rowList, errors, uniqueKeys := -1, -1, make([][]string, 0), map[int]string{}, map[string]int{}
	for rows.Next() {
		i++
		row, err := rows.Columns()
		if err != nil {
			log.ErrorContextf(ctx, "读取 xlsx 文件失败, docName: %s, err: %+v", fileName, err)
			return nil, nil, errs.ErrInvalidDocQaExcel
		}
		// 空行跳过
		if isRowEmpty(head, row) {
			continue
		}
		rowList = append(rowList, row)
		if len(row) > 0 {
			max = i
			if max > maxRow {
				return nil, nil, errs.ErrWrapf(errs.ErrCodeExcelNumTooMany, i18n.Translate(ctx, i18nkey.KeyTableRowCountExceeded), maxRow)
			}
		}
		if i == 0 {
			if !reflect.DeepEqual(row, head) {
				return nil, nil, errs.ErrExcelHead
			}
		} else {
			if msg := check(ctx, i, row, uniqueKeys); msg != "" {
				errors[i] = msg
			}
		}
	}
	if len(rowList) == 0 {
		return nil, nil, errs.ErrWrapf(errs.ErrCodeExcelNumTooFew, i18n.Translate(ctx, i18nkey.KeyEmptyExcelFile))
	}
	if minRow > 0 && max < minRow {
		return nil, nil, errs.ErrWrapf(errs.ErrCodeExcelNumTooFew, i18n.Translate(ctx, i18nkey.KeyTableRowCountInsufficient), minRow)
	}
	var hasError bool
	for i, msg := range errors {
		if i > max {
			continue
		}
		hasError = true
		cell, _ := excelize.CoordinatesToCellName(len(head)+1, i+1, false)
		if err := f.SetCellValue(sheet, cell, msg); err != nil {
			log.ErrorContextf(ctx, "设置单元格值错误, docName: %s, err: %+v", fileName, err)
			return nil, nil, errs.ErrInvalidDocQaExcel
		}
	}
	if !hasError {
		return rowList[1:], nil, nil
	}
	return checkResColumns(ctx, fileName, sheet, head, f)
}

// CheckXlsxContent 检查Excel模板文件内容是否符合要求
func CheckXlsxContent(ctx context.Context, fileName string, minRow, maxRow int, head []string, body []byte,
	check CheckXlsxFunc, uin string, appBizID uint64) ([][]string, []byte, error) {
	f, err := excelize.OpenReader(bytes.NewReader(body))
	if err != nil {
		log.ErrorContextf(ctx, "读取 xlsx 文件失败, docName: %s, err: %+v", fileName, err)
		return nil, nil, errs.ErrSystem
	}
	if f.SheetCount == 0 {
		return nil, nil, errs.ErrExcelIsEmpty
	}
	sheet := f.GetSheetName(0)
	rows, err := f.Rows(sheet)
	if err != nil {
		log.ErrorContextf(ctx, "读取 xlsx 文件失败, docName: %s, err: %+v", fileName, err)
		return nil, nil, errs.ErrSystem
	}
	i, max, rowList, errors := -1, -1, make([][]string, 0), map[int]string{}
	var uniqueKeys sync.Map
	var uniqueImgHost sync.Map
	type checkResult struct {
		index int
		row   []string
		msg   string
		err   error
	}
	// 创建并发控制
	workerCount := 10
	ch := make(chan checkResult, workerCount*2)
	done := make(chan struct{})
	// check校验协程
	wg := errgroupx.Group{}
	wg.SetLimit(10)
	// 启动结果收集协程
	processWg := errgroupx.Group{}
	processWg.SetLimit(1)
	processWg.Go(func() error {
		for res := range ch {
			if res.err != nil {
				close(done)
				return res.err
			}
			if res.msg != "" {
				errors[res.index] = res.msg
				log.InfoContextf(ctx, "CheckXlsxContent ch写入errors i:%d msg:%s", res.index, res.msg)
			} else {
				log.InfoContextf(ctx, "CheckXlsxContent ch收到空msg i:%d", res.index)
			}
		}
		return nil
	})
	// 处理行数据
	for rows.Next() {
		i++
		row, err := rows.Columns()
		if err != nil {
			log.ErrorContextf(ctx, "读取 xlsx 文件失败, docName: %s, err: %+v", fileName, err)
			close(done)
			return nil, nil, errs.ErrInvalidDocQaExcel
		}
		// 空行跳过
		if isRowEmpty(head, row) {
			continue
		}
		rowList = append(rowList, row)
		if len(row) > 0 {
			max = i
			if max > maxRow {
				close(done)
				return nil, nil, errs.ErrWrapf(errs.ErrCodeExcelNumTooMany, i18n.Translate(ctx, i18nkey.KeyTableRowCountExceeded), maxRow)
			}
		}
		if i == 0 {
			if !reflect.DeepEqual(row, head) {
				log.WarnContextf(ctx, "表头错误, row: %v, head: %v", row, head)
				close(done)
				return nil, nil, errs.ErrExcelHead
			}
		} else {
			idx := i
			r := row
			wg.Go(func() error {
				msg := check(ctx, idx, r, &uniqueKeys, uin, appBizID, &uniqueImgHost)
				// 无论 msg 是否为空，都传递到 checkResult
				if msg != "" {
					ch <- checkResult{index: idx, row: r, msg: msg}
				}
				return nil
			})
		}
	}
	// 等待所有检查完成
	if err := wg.Wait(); err != nil {
		log.ErrorContextf(ctx, "group wait() error:%+v", err)
		return nil, nil, errs.ErrSystem
	}
	close(ch)
	if err := processWg.Wait(); err != nil {
		log.ErrorContextf(ctx, "processWg group wait() error:%+v", err)
		return nil, nil, errs.ErrSystem
	}
	if len(rowList) == 0 {
		return nil, nil, errs.ErrWrapf(errs.ErrCodeExcelNumTooFew, i18n.Translate(ctx, i18nkey.KeyEmptyExcelFile))
	}
	if minRow > 0 && max < minRow {
		return nil, nil, errs.ErrWrapf(errs.ErrCodeExcelNumTooFew, i18n.Translate(ctx, i18nkey.KeyTableRowCountInsufficient), minRow)
	}
	log.InfoContextf(ctx, "CheckXlsxContent 主流程开始处理errors，当前errors:%v ", errors)
	// 收集所有错误信息
	var hasError bool
	for i, msg := range errors {
		if i > max {
			continue
		}
		hasError = true
		cell, _ := excelize.CoordinatesToCellName(len(head)+1, i+1, false)
		if err := f.SetCellValue(sheet, cell, msg); err != nil {
			log.ErrorContextf(ctx, "设置单元格值错误, docName: %s, err: %+v", fileName, err)
			return nil, nil, errs.ErrInvalidDocQaExcel
		}
	}
	if !hasError {
		return rowList[1:], nil, nil
	}
	return checkResColumns(ctx, fileName, sheet, head, f)
}

// CheckSampleContent 检查Excel模板文件内容是否符合要求
func CheckSampleContent(ctx context.Context, fileName string, minRow, maxRow int, head []string,
	body []byte, check CheckFunc) ([][]string, []byte, error, bool, bool) {
	f, err := excelize.OpenReader(bytes.NewReader(body))
	if err != nil {
		log.WarnContextf(ctx, "读取 xlsx 文件失败, docName: %s, err: %+v", fileName, err)
		return nil, nil, errs.ErrInvalidSampleSetExcel, false, false
	}
	if f.SheetCount == 0 {
		return nil, nil, errs.ErrExcelIsEmpty, false, false
	}
	sheet := f.GetSheetName(0)
	rows, err := f.Rows(sheet)
	if err != nil {
		log.WarnContextf(ctx, "读取 xlsx 文件失败, docName: %s, err: %+v", fileName, err)
		return nil, nil, errs.ErrInvalidSampleSetExcel, false, false
	}
	i, max, rowList, maxTips, contentTips, errLine, errors, uniqueKeys := -1, -1, make([][]string, 0), false, false, 0,
		map[int]string{}, map[string]int{}
	for rows.Next() {
		i++
		row, err := rows.Columns()
		if err != nil {
			log.WarnContextf(ctx, "读取 xlsx 文件失败, docName: %s, err: %+v", fileName, err)
			return nil, nil, errs.ErrInvalidSampleSetExcel, false, false
		}
		// 空行跳过
		if isRowEmpty(head, row) {
			continue
		}
		if len(row) > 0 {
			max = i
			if max > maxRow {
				maxTips = true
			}
			if max > maxRow+errLine {
				break // 仅导入合规的前maxRow条语料
			}
		}
		rowList = append(rowList, row)
		if i == 0 {
			log.DebugContextf(ctx, "CheckContent head:%v row:%v", head, row)
			if len(row) == 0 || row[0] != head[0] {
				return nil, nil, errs.ErrExcelHead, false, false
			}
			if len(row) > 1 && row[1] != "" && row[1] != head[1] { // 兼容无角色设定表头的接口调用
				return nil, nil, errs.ErrExcelHead, false, false
			}
		} else {
			question := strings.TrimSpace(row[0])
			if question == "" {
				errLine++
			}
			// 单条语料不能超过12000字符；这里先不截断，getSampleSetContents会自动截断
			// https://tapd.woa.com/tapd_fe/70080800/story/detail/1070080800118892670
			if utf8.RuneCountInString(question) > config.App().SampleRule.Question.MaxLength {
				contentTips = true
				//errLine++
				//continue
			}
			if len(row) > 1 && row[1] != "" {
				if utf8.RuneCountInString(row[1]) > config.App().SampleRule.RoleLength {
					contentTips = true
					errLine++
					continue
				}
			}
			if msg := check(ctx, i, row, uniqueKeys); msg != "" {
				errors[i] = msg
			}
		}
	}
	log.DebugContextf(ctx, "CheckSampleContent errLine:%d", errLine)
	if len(rowList) == 0 {
		return nil, nil, errs.ErrWrapf(errs.ErrCodeExcelNumTooFew, i18n.Translate(ctx, i18nkey.KeyTableValidCorpusLessThanOne)), false, false
	}
	if minRow > 0 && max < minRow {
		return nil, nil, errs.ErrWrapf(errs.ErrCodeExcelNumTooFew, i18n.Translate(ctx, i18nkey.KeyTableValidCorpusInsufficient), minRow),
			false, false
	}
	var hasError bool
	log.DebugContextf(ctx, "CheckSampleContent errors:%v", errors)
	for i, msg := range errors {
		if i > max {
			continue
		}
		hasError = true
		cell, _ := excelize.CoordinatesToCellName(len(head)+1, i+1, false)
		if err := f.SetCellValue(sheet, cell, msg); err != nil {
			log.ErrorContextf(ctx, "设置单元格值错误, docName: %s, err: %+v", fileName, err)
			return nil, nil, errs.ErrInvalidDocQaExcel, maxTips, contentTips
		}
	}
	if !hasError {
		return rowList[1:], nil, nil, maxTips, contentTips
	}
	log.DebugContextf(ctx, "CheckSampleContent rowList:%d", len(rowList))
	k, fs, err := checkResColumns(ctx, fileName, sheet, head, f)
	return k, fs, err, maxTips, contentTips
	//return rowList[1:], nil, nil, maxTips, contentTips
}

// GetTimeFromString 字符串转时间
func GetTimeFromString(ctx context.Context, layout string, row []string) (time.Time, error) {
	if model.ExcelTplExpireTimeIndex+1 > len(row) {
		return time.Unix(0, 0), nil
	}
	s := string(pkg.ToUTF8([]byte(strings.TrimSpace(row[model.ExcelTplExpireTimeIndex]))))
	if len(s) == 0 || s == model.ExcelNoExpireTime || s == i18n.Translate(ctx, model.ExcelNoExpireTime) {
		return time.Unix(0, 0), nil
	}
	t, err := time.ParseInLocation(layout, s, time.Local)
	if err != nil {
		log.WarnContextf(ctx, "解析文件时间字段失败 err:%+v", err)
		return time.Unix(0, 0), err
	}
	return t, nil
}

// GetStringFromTime 时间转字符串
func GetStringFromTime(ctx context.Context, layout string, t time.Time) string {
	if uint64(t.Unix()) == 0 {
		return i18n.Translate(ctx, model.ExcelNoExpireTime)
	}
	return t.Format(layout)
}

// isRowEmpty 有头部设置的时候仅判断头部列，没有头部设置判断全列
func isRowEmpty(head []string, row []string) bool {
	columnsNum := len(row)
	if len(head) > 0 && len(row) > len(head) {
		columnsNum = len(head)
	}
	return len(strings.Join(row[:columnsNum], "")) == 0
}

func checkResColumns(ctx context.Context, fileName string, sheet string, head []string,
	f *excelize.File) ([][]string, []byte, error) {
	styleID, err := f.NewStyle(&excelize.Style{Font: &excelize.Font{Color: "FF0000"}})
	if err != nil {
		log.ErrorContextf(ctx, "创建样式错误, docName: %s, err: %+v", fileName, err)
		return nil, nil, errs.ErrInvalidSampleSetExcel
	}
	col, _ := excelize.ColumnNumberToName(len(head) + 1)
	if err := f.SetColStyle(sheet, col, styleID); err != nil {
		log.ErrorContextf(ctx, "设置列样式错误, docName: %s, err: %+v", fileName, err)
		return nil, nil, errs.ErrInvalidSampleSetExcel
	}
	buf, err := f.WriteToBuffer()
	if err != nil {
		log.ErrorContextf(ctx, "回写 xlsx 文件失败, docName: %s, err: %+v", fileName, err)
		return nil, nil, errs.ErrInvalidSampleSetExcel
	}
	return nil, buf.Bytes(), errs.ErrExcelContent
}
