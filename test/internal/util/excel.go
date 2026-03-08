// Package util 工具
package util

import (
	"bytes"
	"context"
	"reflect"
	"strings"
	"sync"
	"time"

	"git.woa.com/adp/common/x/gox/stringx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/syncx/errgroupx"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	"git.woa.com/adp/kb/kb-config/pkg/i18nkey"
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
		logx.E(ctx, "读取 xlsx 文件失败, docName: %s, err: %+v", fileName, err)
		return nil, nil, errs.ErrSystem
	}
	if f.SheetCount == 0 {
		return nil, nil, errs.ErrExcelIsEmpty
	}
	sheet := f.GetSheetName(0)
	rows, err := f.Rows(sheet)
	if err != nil {
		logx.E(ctx, "读取 xlsx 文件失败, docName: %s, err: %+v", fileName, err)
		return nil, nil, errs.ErrSystem
	}
	i, max, rowList, errors, uniqueKeys := -1, -1, make([][]string, 0), map[int]string{}, map[string]int{}
	for rows.Next() {
		i++
		row, err := rows.Columns()
		if err != nil {
			logx.E(ctx, "读取 xlsx 文件失败, docName: %s, err: %+v", fileName, err)
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
			logx.E(ctx, "设置单元格值错误, docName: %s, err: %+v", fileName, err)
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
		logx.E(ctx, "读取 xlsx 文件失败, docName: %s, err: %+v", fileName, err)
		return nil, nil, errs.ErrSystem
	}
	if f.SheetCount == 0 {
		return nil, nil, errs.ErrExcelIsEmpty
	}
	sheet := f.GetSheetName(0)
	rows, err := f.Rows(sheet)
	if err != nil {
		logx.E(ctx, "读取 xlsx 文件失败, docName: %s, err: %+v", fileName, err)
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
	wg := errgroupx.New()
	wg.SetLimit(10)
	// 启动结果收集协程
	processWg := errgroupx.New()
	processWg.SetLimit(1)
	processWg.Go(func() error {
		for res := range ch {
			if res.err != nil {
				close(done)
				return res.err
			}
			if res.msg != "" {
				errors[res.index] = res.msg
				logx.I(ctx, "CheckXlsxContent ch写入errors i:%d msg:%s", res.index, res.msg)
			} else {
				logx.I(ctx, "CheckXlsxContent ch收到空msg i:%d", res.index)
			}
		}
		return nil
	})
	// 处理行数据
	for rows.Next() {
		i++
		row, err := rows.Columns()
		if err != nil {
			logx.E(ctx, "读取 xlsx 文件失败, docName: %s, err: %+v", fileName, err)
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
			if len(row) < docEntity.ExcelTplQaEnableScopeIndex+1 {
				close(done)
				return nil, nil, errs.ErrExcelHead
			}
			if !reflect.DeepEqual(row[:docEntity.ExcelTplQaEnableScopeIndex+1], head) {
				logx.W(ctx, "表头错误, row: %v, head: %v", row, head)
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
		logx.E(ctx, "group wait() error:%+v", err)
		return nil, nil, errs.ErrSystem
	}
	close(ch)
	if err := processWg.Wait(); err != nil {
		logx.E(ctx, "processWg group wait() error:%+v", err)
		return nil, nil, errs.ErrSystem
	}
	if len(rowList) == 0 {
		return nil, nil, errs.ErrWrapf(errs.ErrCodeExcelNumTooFew, i18n.Translate(ctx, i18nkey.KeyEmptyExcelFile))
	}
	if minRow > 0 && max < minRow {
		return nil, nil, errs.ErrWrapf(errs.ErrCodeExcelNumTooFew, i18n.Translate(ctx, i18nkey.KeyTableRowCountInsufficient), minRow)
	}
	logx.I(ctx, "CheckXlsxContent 主流程开始处理errors，当前errors:%v ", errors)
	// 收集所有错误信息
	var hasError bool
	for i, msg := range errors {
		if i > max {
			continue
		}
		hasError = true
		cell, _ := excelize.CoordinatesToCellName(len(head)+1, i+1, false)
		if err := f.SetCellValue(sheet, cell, msg); err != nil {
			logx.E(ctx, "设置单元格值错误, docName: %s, err: %+v", fileName, err)
			return nil, nil, errs.ErrInvalidDocQaExcel
		}
	}
	if !hasError {
		return rowList[1:], nil, nil
	}
	return checkResColumns(ctx, fileName, sheet, head, f)
}

// GetTimeFromString 字符串转时间
func GetTimeFromString(ctx context.Context, layout string, row []string) (time.Time, error) {
	if docEntity.ExcelTplExpireTimeIndex+1 > len(row) {
		return time.Unix(0, 0), nil
	}
	s := stringx.ToUTF8(strings.TrimSpace(row[docEntity.ExcelTplExpireTimeIndex]))
	if len(s) == 0 || s == docEntity.ExcelNoExpireTime || s == i18n.Translate(
		ctx, docEntity.ExcelNoExpireTime) {
		return time.Unix(0, 0), nil
	}
	t, err := time.ParseInLocation(layout, s, time.Local)
	if err != nil {
		logx.W(ctx, "解析文件时间字段失败 err:%+v", err)
		return time.Unix(0, 0), err
	}
	return t, nil
}

// GetStringFromTime 时间转字符串
func GetStringFromTime(ctx context.Context, layout string, t time.Time) string {
	if uint64(t.Unix()) == 0 {
		return i18n.Translate(ctx, docEntity.ExcelNoExpireTime)
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
		logx.E(ctx, "创建样式错误, docName: %s, err: %+v", fileName, err)
		return nil, nil, errs.ErrInvalidSampleSetExcel
	}
	col, _ := excelize.ColumnNumberToName(len(head) + 1)
	if err := f.SetColStyle(sheet, col, styleID); err != nil {
		logx.E(ctx, "设置列样式错误, docName: %s, err: %+v", fileName, err)
		return nil, nil, errs.ErrInvalidSampleSetExcel
	}
	buf, err := f.WriteToBuffer()
	if err != nil {
		logx.E(ctx, "回写 xlsx 文件失败, docName: %s, err: %+v", fileName, err)
		return nil, nil, errs.ErrInvalidSampleSetExcel
	}
	return nil, buf.Bytes(), errs.ErrExcelContent
}
