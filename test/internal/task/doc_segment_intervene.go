// Package task 解析切分干预任务处理
package task

import (
	"context"
	"errors"
	"fmt"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/i18nkey"
	"io/ioutil"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/app"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	logicDoc "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/doc"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"git.woa.com/dialogue-platform/bot-config/task_scheduler"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
	"go.opentelemetry.io/otel/trace"
)

const (
	taskKvKeyDocOrgData          = "docOrgData"
	taskKvKeyDocInterveneOrgData = "docInterveneOrgData"
	taskKvKeyDocInterveneSheet   = "docInterveneSheet"
	orgDataStartTag              = "\n{-----切分开始-----}\n\n"
	orgDataEndTag                = "\n\n{-----切分结束-----}\n"
	paginationTag                = "\n================================================================分页符\n"
	deleteDataMaxLimit           = 10
)

// TableIndex 表格行数
type TableIndex struct {
	// StartIndex 开始行
	StartIndex int
	// EndIndex 结束行
	EndIndex int
}

// DocSegInterveneScheduler 文档比较任务
type DocSegInterveneScheduler struct {
	dao      dao.Dao
	task     task_scheduler.Task
	instance app.Base
	params   model.DocSegInterveneParams
}

func initDocSegInterveneScheduler() {
	task_scheduler.Register(
		model.DocSegInterveneTask,
		func(t task_scheduler.Task, params model.DocSegInterveneParams) task_scheduler.TaskHandler {
			return &DocSegInterveneScheduler{
				dao:    dao.New(),
				task:   t,
				params: params,
			}
		},
	)
}

// Prepare 数据准备 仅在该任务第一次执行时触发一次, 在整个任务的生命周期内, 执行且只执行一次
func (d *DocSegInterveneScheduler) Prepare(ctx context.Context) (task_scheduler.TaskKV, error) {
	i18n.SetUserLangString(ctx, d.params.Language)
	log.DebugContextf(ctx, "task(DocSegIntervene) Prepare, task: %+v, params: %+v", d.task, d.params)
	kv := make(task_scheduler.TaskKV)
	// 默认解析中，无需更新状态
	// 做一些校验
	doc, err := d.dao.GetDocByBizID(ctx, d.params.OriginDocBizID, d.params.AppID)
	if err != nil {
		return kv, err
	}
	if doc == nil {
		return kv, errs.ErrDocNotFound
	}
	if doc.HasDeleted() {
		return kv, nil
	}
	kv[fmt.Sprintf("%d", doc.BusinessID)] = fmt.Sprintf("%d", doc.BusinessID)
	return kv, nil
}

// Init 初始化
func (d *DocSegInterveneScheduler) Init(ctx context.Context, _ task_scheduler.TaskKV) error {
	i18n.SetUserLangString(ctx, d.params.Language)
	log.InfoContextf(ctx, "task(DocSegIntervene) Init start")
	return nil
}

// Process 任务处理
func (d *DocSegInterveneScheduler) Process(ctx context.Context, progress *task_scheduler.Progress) error {
	log.DebugContextf(ctx, "task(DocSegIntervene) Process, task: %+v, params: %+v", d.task, d.params)
	for k, v := range progress.TaskKV(ctx) {
		log.DebugContextf(ctx, "task(DocSegIntervene) Start k:%s, v:%s", k, v)
		fastFailure, err := d.ProcessDocSegment(ctx)
		if err != nil && !fastFailure {
			log.ErrorContextf(ctx, "task(DocSegIntervene) ProcessDocSegment k:%s err:%+v", k, err)
			return err
		}
		if err := progress.Finish(ctx, k); err != nil {
			log.ErrorContextf(ctx, "task(DocSegIntervene) Finish kv:%s err:%+v", k, err)
			return err
		}
		log.DebugContextf(ctx, "task(DocSegIntervene) Finish kv:%s", k)
	}
	return nil
}

// Fail 任务失败
func (d *DocSegInterveneScheduler) Fail(ctx context.Context) error {
	log.DebugContextf(ctx, "task(DocSegIntervene) fail, doc id: %v", d.params.OriginDocBizID)
	// 确认文档是否存在，更新文档的状态为解析失败
	_, err := CheckExistAndUpdateStatus(ctx, d.dao, d.params.OriginDocBizID,
		d.params.AppBizID, model.DocStatusParseFail, i18nkey.KeyInterventionContentParseFailed)
	if err != nil {
		return err
	}
	return nil
}

// Stop 任务停止
func (d *DocSegInterveneScheduler) Stop(_ context.Context) error {
	return nil
}

// Done 任务完成回调
func (d *DocSegInterveneScheduler) Done(ctx context.Context) error {
	log.DebugContextf(ctx, "task(DocSegIntervene) done, doc id: %v", d.params.OriginDocBizID)
	return nil
}

func (d *DocSegInterveneScheduler) ProcessDocSegment(ctx context.Context) (bool, error) {
	// 拼接的新文档的cos信息
	var cosPath string
	var cosHash string
	var interventionType uint32
	cacheDocPath, err := getCacheInterveneDocPath(ctx, d.params.TaskID, d.params.OriginDocBizID, model.FileTypeMD)
	if err != nil {
		log.ErrorContextf(ctx, "task(DocSegIntervene) getCacheInterveneDocPath|err:%v", err)
		return false, err
	}
	log.InfoContextf(ctx, "task(DocSegIntervene) cacheDocPath:%s", cacheDocPath)
	docCommon := &model.DocSegmentCommon{
		AppID:      d.params.AppID,
		AppBizID:   d.params.AppBizID,
		CorpID:     d.params.CorpID,
		CorpBizID:  d.params.CorpBizID,
		StaffID:    d.params.StaffID,
		StaffBizID: d.params.StaffBizID,
		DocBizID:   d.params.OriginDocBizID,
		DataSource: d.params.DataSource,
	}
	// 判断文档类型，获取待拼接数据
	if d.params.FileType == model.FileTypeXlsx || d.params.FileType == model.FileTypeXls ||
		d.params.FileType == model.FileTypeCsv {
		interventionType = model.InterventionTypeSheet
		if docCommon.DataSource == model.DataSourceCOS {
			// 默认切分规则，从cos读取数据
			err = d.getAllSegmentAndSplicingSheet(ctx, docCommon, cacheDocPath)
		} else {
			// 自定义切分规则，从segment_org_data读取数据
			err = d.getAllSegmentAndSplicingDoc(ctx, docCommon, cacheDocPath, interventionType)
		}
		if err != nil {
			log.ErrorContextf(ctx, "task(DocSegIntervene) splicingTableDocuments|err:%v", err)
			return false, err
		}
	} else {
		interventionType = model.InterventionTypeOrgData
		err := d.getAllSegmentAndSplicingDoc(ctx, docCommon, cacheDocPath, interventionType)
		if err != nil {
			log.ErrorContextf(ctx, "task(DocSegIntervene) getAllSegmentAndSplicingDoc|err:%v", err)
			return false, err
		}
	}
	log.InfoContextf(ctx, "task(DocSegIntervene) intervene doc is splicing success")
	// 读文档上传cos
	docBytes, err := readCacheInterveneDoc(ctx, cacheDocPath)
	if err != nil {
		log.ErrorContextf(ctx, "task(DocSegIntervene) readCacheInterveneDoc|err:%v", err)
		return false, err
	}
	defer func() {
		if err = cleanCacheInterveneDoc(ctx, cacheDocPath); err != nil {
			log.WarnContextf(ctx, "task(DocSegIntervene)|failed to delete cache file|err:%+v", err)
		}
	}()

	cosPath, _, cosHash, err = logicDoc.UploadToCos(ctx, d.dao, d.params.CorpID, docBytes)
	if err != nil {
		log.ErrorContextf(ctx, "task(DocSegIntervene) UploadToCos|err:%v", err)
		return false, err
	}
	return d.createDocParseFromOldDoc(ctx, cosPath, cosHash, interventionType)
}

func getCacheInterveneDocPath(ctx context.Context, taskID, docBizID uint64, fileType string) (string, error) {
	filePath := config.App().SplicingInterveneDocPath
	if len(filePath) == 0 {
		err := errors.New("splicing intervene doc path is empty")
		return "", err
	}
	// 创建路径，确保文件路径存在
	err := os.MkdirAll(filePath, 0755)
	if err != nil {
		log.ErrorContextf(ctx, "task(DocSegIntervene)|getCacheInterveneDocPath|MkdirAll|err:%v", err)
		return "", err
	}

	fileName := strconv.FormatUint(taskID, 10) + "_" + strconv.FormatUint(docBizID, 10) + "." + fileType
	cacheInterveneFileName := path.Join(filePath, fileName)
	return cacheInterveneFileName, nil
}

func checkFileIsExist(ctx context.Context, filename string) error {
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		s := fmt.Sprintf("splicing intervene doc not found, filepath=%s", filename)
		err := errors.New(s)
		return err
	}
	return nil
}

func writeCacheInterveneDoc(ctx context.Context, filename string, content []byte) error {
	var fileWrite *os.File
	err := checkFileIsExist(ctx, filename)
	if err == nil {
		fileWrite, err = os.OpenFile(filename, os.O_APPEND|os.O_WRONLY, 0666)
	} else {
		fileWrite, err = os.Create(filename)
	}
	defer fileWrite.Close()
	if err != nil {
		return err
	}
	if _, err = fileWrite.Write(content); err != nil {
		return err
	}
	return nil
}

func readCacheInterveneDoc(ctx context.Context, filename string) ([]byte, error) {
	err := checkFileIsExist(ctx, filename)
	if err != nil {
		return nil, err
	}
	docBytes, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	return docBytes, nil
}

func cleanCacheInterveneDoc(ctx context.Context, filename string) error {
	err := checkFileIsExist(ctx, filename)
	if err != nil {
		return err
	}
	err = os.Remove(filename)
	if err != nil {
		return err
	}
	return nil
}

func CheckExistAndUpdateStatus(ctx context.Context, d dao.Dao, docBizID, appBizID uint64, status uint32,
	message string) (*model.Doc, error) {
	doc, err := d.GetDocByBizID(ctx, docBizID, appBizID)
	if err != nil {
		if errors.Is(err, errs.ErrDocNotFound) {
			log.InfoContextf(ctx, "task(DocSegIntervene)|CheckExistAndUpdateStatus|doc is not exist|docBizID:%d", docBizID)
			return nil, nil
		}
		log.ErrorContextf(ctx, "task(DocSegIntervene)|CheckExistAndUpdateStatus|GetDocByBizID|err:%v", err)
		return nil, err
	}
	if doc != nil {
		log.InfoContextf(ctx, "task(DocSegIntervene)|CheckExistAndUpdateStatus|old doc is exist|docBizID:%d", docBizID)
		// 直接更新旧文档的状态
		updateDocFilter := &dao.DocFilter{
			IDs:     []uint64{doc.ID},
			CorpId:  doc.CorpID,
			RobotId: doc.RobotID,
		}
		update := &model.Doc{
			Message: message,
			Status:  status,
		}
		updateDocColumns := []string{
			dao.DocTblColMessage,
			dao.DocTblColStatus}
		_, err := dao.GetDocDao().UpdateDoc(ctx, updateDocColumns, updateDocFilter, update)
		if err != nil {
			log.ErrorContextf(ctx, "TableStructuredDataIntervention|UpdateDocStatus|doc_id:%d|err:%+v", doc.ID, err)
			return doc, err
		}
		return doc, nil
	}
	return doc, nil
}

// getAllSegmentAndSplicingSheet 获取所有sheet&拼接成md
func (d *DocSegInterveneScheduler) getAllSegmentAndSplicingSheet(ctx context.Context, docCommon *model.DocSegmentCommon,
	cacheDocPath string) error {
	pageNumber := 0
	pageSize := 100
	count := 0
	for {
		pageNumber++
		req := &pb.ListTableSheetReq{
			AppBizId:   strconv.FormatUint(d.params.AppBizID, 10),
			DocBizId:   strconv.FormatUint(d.params.OriginDocBizID, 10),
			PageNumber: uint32(pageNumber),
			PageSize:   uint32(pageSize),
		}
		list, err := logicDoc.GetSheetList(ctx, d.dao, req, docCommon)
		if err != nil {
			log.ErrorContextf(ctx, "task(DocSegIntervene) GetSheetList|err:%v", err)
			return err
		}
		if len(list) == 0 {
			break
		}
		count += len(list)
		// 从cos上下载数据，拼接成md，然后上传到cos
		for _, sheet := range list {
			// 下载COS
			storageTypeKey := d.dao.GetTypeKeyWithBucket(ctx, sheet.Bucket)
			body, err := d.dao.GetObjectWithTypeKey(ctx, storageTypeKey, sheet.CosURL)
			if err != nil {
				log.ErrorContextf(ctx, "task(DocSegIntervene) GetObjectWithTypeKey|err:%v", err)
				return err
			}
			log.DebugContextf(ctx, "task(DocSegIntervene)|GetObjectWithTypeKey|body:%s,title:%s", string(body), sheet.SheetName)
			title := []byte("\n" + "\n" + sheet.SheetName + "\n" + "\n")
			content := append(title, body...)
			err = writeCacheInterveneDoc(ctx, cacheDocPath, content)
			if err != nil {
				log.ErrorContextf(ctx, "task(DocSegIntervene) writeCacheInterveneDoc|err:%v", err)
				return err
			}
		}
	}
	if count == 0 {
		return errs.ErrDocSegmentNotFound
	}
	return nil
}

// getAllSegmentAndSplicingDoc 获取所有切片&拼接成md
func (d *DocSegInterveneScheduler) getAllSegmentAndSplicingDoc(ctx context.Context, docCommon *model.DocSegmentCommon,
	cacheDocPath string, interventionType uint32) error {
	shortURLSyncMap := &sync.Map{}
	pageNumber := 0
	pageSize := 100
	count := 0
	lastPage := uint64(1)
	skipTimes := 0
	var fileName string
	// 切片文件名前缀
	lastIndex := strings.LastIndex(d.params.FileName, ".")
	if lastIndex == -1 {
		fileName = d.params.FileName
	} else {
		fileName = d.params.FileName[:lastIndex]
	}
	// 统计文档的切片数
	deleteFlag := dao.IsNotDeleted
	filter := &dao.DocSegmentOrgDataFilter{
		CorpBizID:          d.params.CorpBizID,
		AppBizID:           d.params.AppBizID,
		DocBizID:           d.params.OriginDocBizID,
		IsDeleted:          &deleteFlag,
		IsTemporaryDeleted: &deleteFlag,
		RouterAppBizID:     d.params.AppBizID,
	}
	total, err := dao.GetDocSegmentOrgDataDao().GetDocOrgDataCount(ctx, filter)
	if err != nil {
		log.ErrorContextf(ctx, "task(DocSegIntervene)|GetDocOrgDataCount|err:%v", err)
		return err
	}
	log.InfoContextf(ctx, "task(DocSegIntervene)|GetDocOrgDataCount|total:%d", total)
	for {
		pageNumber++
		req := &pb.ListDocSegmentReq{
			AppBizId:   strconv.FormatUint(d.params.AppBizID, 10),
			DocBizId:   strconv.FormatUint(d.params.OriginDocBizID, 10),
			PageNumber: uint32(pageNumber),
			PageSize:   uint32(pageSize),
		}
		list, err := logicDoc.GetDocSegmentList(ctx, req, docCommon)
		if err != nil {
			log.ErrorContextf(ctx, "task(DocSegIntervene) GetDocSegmentList|err:%v", err)
			return err
		}
		// 特殊处理，在干预中数据库中数据被修改报错
		if len(list) == 0 {
			skipTimes++
			if skipTimes > deleteDataMaxLimit {
				break
			}
			err = fmt.Errorf("切片数据获取错误或连续删除切片数量超过%d", deleteDataMaxLimit*pageSize)
			log.ErrorContextf(ctx, "task(DocSegIntervene) GetDocSegmentList|err:%v", err)
			return err
		} else {
			skipTimes = 0
		}
		err = d.splicingDoc(ctx, list, cacheDocPath, &lastPage, shortURLSyncMap, fileName, interventionType)
		if err != nil {
			log.ErrorContextf(ctx, "task(DocSegIntervene) splicingDoc|err:%v", err)
			return err
		}
		count += len(list)
		if int64(pageNumber)*int64(pageSize) >= total {
			break
		}
	}
	if count == 0 {
		return errs.ErrDocSegmentNotFound
	}
	return nil
}

// splicingDoc 拼接切片，存储到本地
func (d *DocSegInterveneScheduler) splicingDoc(ctx context.Context, list []*pb.ListDocSegmentRsp_DocSegmentItem,
	cacheDocPath string, lastPage *uint64, shortURLSyncMap *sync.Map, fileName string, interventionType uint32) error {
	// 拼接md，上传cos
	for _, seg := range list {
		content := make([]byte, 0)
		if len(seg.PageInfos) > 0 {
			curPage := seg.PageInfos[0]
			for i := *lastPage; i < curPage; i++ {
				content = append(content, []byte(paginationTag)...)
			}
			*lastPage = curPage
		}
		// 图片短链接还原处理
		imageURLs, err := d.dao.ParseImgURL(ctx, seg.OrgData)
		if err != nil {
			log.ErrorContextf(ctx, "task(DocSegIntervene) ParseImgURL|err:%v", err)
			return err
		}
		log.DebugContextf(ctx, "task(DocSegIntervene)|short imageURL:%+v", imageURLs)
		for _, imageURL := range imageURLs {
			if strings.HasPrefix(imageURL, config.App().ShortURLRootURL) ||
				strings.HasPrefix(imageURL, config.App().PreviewURLRootURL) {
				URL, err := url.Parse(imageURL)
				if err != nil {
					log.WarnContextf(ctx, "task(DocSegIntervene) url.Parse|err:%v", err)
					continue
				}
				if _, ok := shortURLSyncMap.Load(imageURL); ok {
					continue
				}
				cosURL, err := d.dao.ShortURLCodeRecoverCosURL(ctx, imageURL, URL.Path)
				if err != nil {
					log.WarnContextf(ctx, "task(DocSegIntervene) ShortURLCodeRecoverCosURL|err:%v", err)
					return err
				}
				log.DebugContextf(ctx, "task(DocSegIntervene)|cosURL:%s", cosURL)
				shortURLSyncMap.Store(imageURL, cosURL)
			}
		}
		// 链接替换
		if shortURLSyncMap != nil {
			shortURLSyncMap.Range(func(key, value any) bool {
				oldPath := key.(string)
				newPath := value.(string)
				seg.OrgData = strings.ReplaceAll(seg.OrgData, oldPath, newPath)
				return true
			})
		}
		// 切片前缀去除
		seg.OrgData = strings.TrimPrefix(seg.OrgData, fileName+":")
		seg.OrgData = strings.TrimSpace(seg.OrgData)
		log.DebugContextf(ctx, "task(DocSegIntervene) orgData:%+v", seg.OrgData)
		if interventionType == model.InterventionTypeOrgData {
			seg.OrgData = addLineBreak(seg.OrgData)
		}
		content = append(content, []byte(orgDataStartTag+seg.OrgData+orgDataEndTag)...)
		err = writeCacheInterveneDoc(ctx, cacheDocPath, content)
		if err != nil {
			log.ErrorContextf(ctx, "task(DocSegIntervene) writeCacheInterveneDoc|err:%v", err)
			return err
		}
	}
	return nil
}

// createDocParseFromOldDoc 创建解析任务
func (d *DocSegInterveneScheduler) createDocParseFromOldDoc(ctx context.Context, cosPath, cosHash string,
	interventionType uint32) (bool, error) {
	// 获取老文档
	oldDoc, err := d.dao.GetDocByBizID(ctx, d.params.OriginDocBizID, d.params.AppID)
	if err != nil {
		log.ErrorContextf(ctx, "task(DocSegIntervene) GetDocByBizID|err:%v", err)
		return false, err
	}
	if oldDoc == nil {
		log.ErrorContextf(ctx, "task(DocSegIntervene)|CreateNewDocFromOldDoc|GetDocByBizID doc is null")
		return false, errs.ErrDocNotFound
	}
	// 新数据写入redis(解析回调时使用)
	err = d.dao.SetInterveneOldDocCosHashToNewDocRedisValueByDoc(ctx, d.params.AppBizID, cosPath, cosHash, oldDoc, interventionType)
	if err != nil {
		log.ErrorContextf(ctx, "task(DocSegIntervene) SetOldDocCosHashToNewDocRedisValueByDoc|err:%+v", err)
		return false, err
	}
	// 在提交解析统计字符数任务前，替换doc的cos地址
	oldDoc.CosURL = cosPath
	oldDoc.CosHash = cosHash
	// 提交解析统计字符数&新增t_doc_parse
	// （解析中终止兼容）在插入数据前先校验文档状态，为解析失败则不在创建解析任务
	curDoc, err := d.dao.GetDocByBizID(ctx, d.params.OriginDocBizID, d.params.AppID)
	if err != nil {
		log.ErrorContextf(ctx, "task(DocSegIntervene) GetDocByBizID|err:%v", err)
		return false, err
	}
	if curDoc.Status == model.DocStatusParseFail {
		err := fmt.Errorf("current doc status is ParseFail")
		log.WarnContextf(ctx, "task(DocSegIntervene) before SendDocParseWordCount|err:%v", err)
		return true, err
	}
	requestID := trace.SpanContextFromContext(ctx).TraceID().String()
	taskID, err := d.dao.SendDocParseWordCount(ctx, oldDoc, requestID, oldDoc.FileType)
	if err != nil {
		log.ErrorContextf(ctx, "task(DocSegIntervene) getCacheInterveneDocPath|err:%v", err)
		return false, err
	}

	// todo 将之前的解析任务删除，仅保留当前任务（获取任务时通过order by id排序）
	err = dao.GetDocParseDao().DeleteDocParseByDocID(ctx, oldDoc.CorpID, oldDoc.RobotID, oldDoc.ID)
	if err != nil {
		log.ErrorContextf(ctx, "task(DocSegIntervene) DeleteDocParseByDocID|err:%v", err)
		return false, err
	}
	docParse := model.DocParse{
		DocID:     oldDoc.ID,
		CorpID:    oldDoc.CorpID,
		RobotID:   oldDoc.RobotID,
		StaffID:   oldDoc.StaffID,
		RequestID: requestID,
		Type:      model.DocParseTaskTypeWordCount,
		OpType:    model.DocParseOpTypeWordCount,
		Status:    model.DocParseIng,
		TaskID:    taskID,
	}
	err = d.dao.CreateDocParseWithSourceEnvSet(ctx, nil, docParse, d.params.SourceEnvSet)
	if err != nil {
		log.ErrorContextf(ctx, "task(DocSegIntervene) getCacheInterveneDocPath|err:%v", err)
		return false, err
	}
	return false, nil
}

// addLineBreak 给orgData每行均添加换行符（表格/空行除外）
func addLineBreak(orgData string) string {
	lines := strings.Split(orgData, "\n")
	var result strings.Builder
	for _, line := range lines {
		trimmedLine := strings.TrimSpace(line)
		if len(trimmedLine) == 0 {
			result.WriteString(line + "\n")
			continue
		}
		if util.IsTableLine(trimmedLine) ||
			util.IsSeparatorLine(trimmedLine) {
			result.WriteString(line + "\n")
		} else {
			result.WriteString(line + "\n\n")
		}
	}

	return result.String()
}

// extractTableDataFromMarkdown 从 Markdown 内容中提取表格开始行与结束行
func extractTableFromMarkdown(mdContent string) []*TableIndex {
	var tableIndex []*TableIndex
	lines := strings.Split(mdContent, "\n")
	tableStartIndex := -1
	for i := range lines {
		if i == 0 {
			continue
		}
		tableLine := util.IsTableLine(lines[i-1])
		separatorLine := util.IsSeparatorLine(lines[i])
		if separatorLine && tableLine {
			if tableStartIndex != -1 {
				tableIndex = append(tableIndex, &TableIndex{
					StartIndex: tableStartIndex,
					EndIndex:   i - 2,
				})
			}
			tableStartIndex = i - 1
		} else if !util.IsTableLine(lines[i]) && tableStartIndex != -1 {
			tableIndex = append(tableIndex, &TableIndex{
				StartIndex: tableStartIndex,
				EndIndex:   i - 1,
			})
			tableStartIndex = -1
		}
	}

	// 处理文件末尾的表格
	if tableStartIndex != -1 {
		tableIndex = append(tableIndex, &TableIndex{
			StartIndex: tableStartIndex,
			EndIndex:   len(lines) - 1,
		})
	}
	return tableIndex
}
