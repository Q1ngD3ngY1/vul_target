// Package task 解析切分干预任务处理
package async

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"

	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/gox/ptrx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	"git.woa.com/adp/kb/kb-config/internal/entity/segment"
	segEntity "git.woa.com/adp/kb/kb-config/internal/entity/segment"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	"git.woa.com/adp/kb/kb-config/pkg/i18nkey"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
	"git.woa.com/dialogue-platform/bot-config/task_scheduler"
)

const (
	orgDataStartTag    = "\n{-----切分开始-----}\n\n"
	orgDataEndTag      = "\n\n{-----切分结束-----}\n"
	paginationTag      = "\n================================================================分页符\n"
	deleteDataMaxLimit = 10
)

// DocSegInterveneTaskHandler 文档比较任务
type DocSegInterveneTaskHandler struct {
	*taskCommon

	task   task_scheduler.Task
	params entity.DocSegInterveneParams
}

func registerDocSegInterveneTaskHandler(tc *taskCommon) {
	task_scheduler.Register(
		entity.DocSegInterveneTask,
		func(t task_scheduler.Task, params entity.DocSegInterveneParams) task_scheduler.TaskHandler {
			return &DocSegInterveneTaskHandler{
				taskCommon: tc,
				task:       t,
				params:     params,
			}
		},
	)
}

// Prepare 数据准备 仅在该任务第一次执行时触发一次, 在整个任务的生命周期内, 执行且只执行一次
func (d *DocSegInterveneTaskHandler) Prepare(ctx context.Context) (task_scheduler.TaskKV, error) {
	i18n.SetUserLangString(ctx, d.params.Language)
	logx.D(ctx, "task(DocSegIntervene)Prepare, task: %+v, params: %+v", d.task, d.params)
	kv := make(task_scheduler.TaskKV)
	// 默认解析中，无需更新状态
	// 做一些校验
	doc, err := d.docLogic.GetDocByBizID(ctx, d.params.OriginDocBizID, d.params.AppID)
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
func (d *DocSegInterveneTaskHandler) Init(ctx context.Context, _ task_scheduler.TaskKV) error {
	i18n.SetUserLangString(ctx, d.params.Language)
	logx.I(ctx, "task(DocSegIntervene)Init start")
	return nil
}

// Process 任务处理
func (d *DocSegInterveneTaskHandler) Process(ctx context.Context, progress *task_scheduler.Progress) error {
	logx.D(ctx, "task(DocSegIntervene)Process, task: %+v, params: %+v", d.task, d.params)
	for k, v := range progress.TaskKV(ctx) {
		logx.D(ctx, "task(DocSegIntervene) Start k:%s, v:%s", k, v)
		fastFailure, err := d.ProcessDocSegment(ctx)
		if err != nil && !fastFailure {
			logx.E(ctx, "task(DocSegIntervene) ProcessDocSegment k:%s err:%+v", k, err)
			return err
		}
		if err := progress.Finish(ctx, k); err != nil {
			logx.E(ctx, "task(DocSegIntervene) Finish kv:%s err:%+v", k, err)
			return err
		}
		logx.D(ctx, "task(DocSegIntervene) Finish kv:%s", k)
	}
	return nil
}

// Fail 任务失败
func (d *DocSegInterveneTaskHandler) Fail(ctx context.Context) error {
	logx.D(ctx, "task(DocSegIntervene) fail, doc id: %v", d.params.OriginDocBizID)
	// 确认文档是否存在，更新文档的状态为解析失败
	_, err := d.CheckExistAndUpdateStatus(ctx, d.params.OriginDocBizID,
		d.params.AppBizID, docEntity.DocStatusParseFail, i18nkey.KeyInterventionContentParseFailed)
	if err != nil {
		return err
	}
	return nil
}

// Stop 任务停止
func (d *DocSegInterveneTaskHandler) Stop(_ context.Context) error {
	return nil
}

// Done 任务完成回调
func (d *DocSegInterveneTaskHandler) Done(ctx context.Context) error {
	logx.D(ctx, "task(DocSegIntervene) done, doc id: %v", d.params.OriginDocBizID)
	return nil
}

func (d *DocSegInterveneTaskHandler) ProcessDocSegment(ctx context.Context) (bool, error) {
	// 拼接的新文档的cos信息
	var cosPath string
	var cosHash string
	var interventionType uint32
	cacheDocPath, err := getCacheInterveneDocPath(ctx, d.params.TaskID, d.params.OriginDocBizID, docEntity.FileTypeMD)
	if err != nil {
		logx.E(ctx, "task(DocSegIntervene) getCacheInterveneDocPath|err:%v", err)
		return false, err
	}
	logx.I(ctx, "task(DocSegIntervene) cacheDocPath:%s", cacheDocPath)
	docCommon := &segment.DocSegmentCommon{
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
	if d.params.FileType == docEntity.FileTypeXlsx || d.params.FileType == docEntity.FileTypeXls ||
		d.params.FileType == docEntity.FileTypeCsv || d.params.FileType == docEntity.FileTypeNumbers {
		interventionType = docEntity.InterventionTypeSheet
		err := d.getAllSegmentAndSplicingSheet(ctx, docCommon, cacheDocPath)
		if err != nil {
			logx.E(ctx, "task(DocSegIntervene) splicingTableDocuments|err:%v", err)
			return false, err
		}
	} else {
		interventionType = docEntity.InterventionTypeOrgData
		err := d.getAllSegmentAndSplicingDoc(ctx, docCommon, cacheDocPath)
		if err != nil {
			logx.E(ctx, "task(DocSegIntervene) getAllSegmentAndSplicingDoc|err:%v", err)
			return false, err
		}
	}
	logx.I(ctx, "task(DocSegIntervene) intervene doc is splicing success")
	// 读文档上传cos
	docBytes, err := readCacheInterveneDoc(ctx, cacheDocPath)
	if err != nil {
		logx.E(ctx, "task(DocSegIntervene) readCacheInterveneDoc|err:%v", err)
		return false, err
	}
	defer func() {
		if err = cleanCacheInterveneDoc(ctx, cacheDocPath); err != nil {
			logx.W(ctx, "task(DocSegIntervene)|failed to delete cache file|err:%+v", err)
		}
	}()

	cosPath, _, cosHash, err = d.docLogic.UploadToCos(ctx, d.params.CorpID, docBytes)
	if err != nil {
		logx.E(ctx, "task(DocSegIntervene) UploadToCos|err:%v", err)
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

	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		logx.I(ctx, "task(DocSegIntervene) getCacheSplicingInterveneDocPath not exists, "+
			"start to MkdirAll|filePath:%s", filePath)
		err = os.MkdirAll(filePath, 0755)
		if err != nil {
			logx.E(ctx, "task(DocSegIntervene) getCacheSplicingInterveneDocPath:%s|MkdirAll|err:%v",
				filePath, err)
			return "", err
		}
	}

	fileName := strconv.FormatUint(taskID, 10) + "_" + strconv.FormatUint(docBizID, 10) + "." + fileType
	cacheInterveneFileName := path.Join(filePath, fileName)
	return cacheInterveneFileName, nil
}

// TODO(ericjwang): 这里有问题，模糊了 error 和 exists，当有非 os.ErrNotExist error 时，会误以为文件存在
func checkFileIsExist(ctx context.Context, filename string) error {
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		logx.E(ctx, "task(DocSegIntervene) checkFileIsExist|err:%v", err)
		return fmt.Errorf("splicing intervene doc not found, filepath=%s", filename)
	}
	return nil
}

func writeCacheInterveneDoc(ctx context.Context, filename string, content []byte) error {
	var fileWrite *os.File
	err := checkFileIsExist(ctx, filename)

	if err == nil {
		fileWrite, err = os.OpenFile(filename, os.O_APPEND|os.O_WRONLY, 0666)
	} else {
		logx.E(ctx, "task(DocSegIntervene) checkFileIsExist|err:%v", err)
		fileWrite, err = os.Create(filename)
		if err != nil {
			logx.E(ctx, "task(DocSegIntervene) checkFileIsExist|Create|err:%v", err)
			// return err
		}
	}
	defer fileWrite.Close()
	if err != nil {
		logx.E(ctx, "task(DocSegIntervene) checkFileIsExist|fileWrite|err:%v", err)
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

func (d *DocSegInterveneTaskHandler) CheckExistAndUpdateStatus(ctx context.Context, docBizID, appBizID uint64, status uint32,
	message string) (*docEntity.Doc, error) {
	doc, err := d.docLogic.GetDocByBizID(ctx, docBizID, appBizID)
	if err != nil {
		if errors.Is(err, errs.ErrDocNotFound) {
			logx.I(ctx, "task(DocSegIntervene)|CheckExistAndUpdateStatus|doc is not exist|docBizID:%d", docBizID)
			return nil, nil
		}
		logx.E(ctx, "task(DocSegIntervene)|CheckExistAndUpdateStatus|GetDocByBizID|err:%v", err)
		return nil, err
	}
	if doc != nil {
		logx.I(ctx, "task(DocSegIntervene)|CheckExistAndUpdateStatus|old doc is exist|docBizID:%d", docBizID)
		// 直接更新旧文档的状态
		updateDocFilter := &docEntity.DocFilter{
			IDs:     []uint64{doc.ID},
			CorpId:  doc.CorpID,
			RobotId: doc.RobotID,
		}
		update := &docEntity.Doc{
			Message: message,
			Status:  status,
		}
		updateDocColumns := []string{
			docEntity.DocTblColMessage,
			docEntity.DocTblColStatus}
		_, err := d.docLogic.UpdateLogicByDao(ctx, updateDocColumns, updateDocFilter, update)
		if err != nil {
			logx.E(ctx, "TableStructuredDataIntervention|UpdateDocStatus|doc_id:%d|err:%+v", doc.ID, err)
			return doc, err
		}
		return doc, nil
	}
	return doc, nil
}

// getAllSegmentAndSplicingSheet 获取所有sheet&拼接成md
func (d *DocSegInterveneTaskHandler) getAllSegmentAndSplicingSheet(ctx context.Context, docCommon *segment.DocSegmentCommon,
	cacheDocPath string) error {
	if docCommon.DataSource == entity.DataSourceDB {
		err := d.writeDataToCos(ctx, docCommon)
		if err != nil {
			logx.E(ctx, "task(DocSegIntervene) writeDataToCos|err:%v", err)
			return err
		}
	}
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
		list, err := d.docLogic.GetSheetList(ctx, req, docCommon)
		if err != nil {
			logx.E(ctx, "task(DocSegIntervene) GetSheetList|err:%v", err)
			return err
		}
		if len(list) == 0 {
			break
		}
		count += len(list)
		// 从cos上下载数据，拼接成md，然后上传到cos
		for _, sheet := range list {
			// 下载COS
			storageTypeKey := d.s3.GetTypeKeyWithBucket(ctx, sheet.Bucket)
			body, err := d.s3.GetObjectWithTypeKey(ctx, storageTypeKey, sheet.CosURL)
			if err != nil {
				logx.E(ctx, "task(DocSegIntervene) GetObjectWithTypeKey|err:%v", err)
				return err
			}
			title := []byte("\n" + "\n" + sheet.SheetName + "\n" + "\n")
			content := append(title, body...)
			err = writeCacheInterveneDoc(ctx, cacheDocPath, content)
			if err != nil {
				logx.E(ctx, "task(DocSegIntervene) writeCacheInterveneDoc|err:%v", err)
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
func (d *DocSegInterveneTaskHandler) getAllSegmentAndSplicingDoc(ctx context.Context, docCommon *segment.DocSegmentCommon,
	cacheDocPath string) error {
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
	deleteFlag := ptrx.Bool(false)
	filter := &segEntity.DocSegmentOrgDataFilter{
		CorpBizID:          d.params.CorpBizID,
		AppBizID:           d.params.AppBizID,
		DocBizID:           d.params.OriginDocBizID,
		IsDeleted:          deleteFlag,
		IsTemporaryDeleted: deleteFlag,
		RouterAppBizID:     d.params.AppBizID,
	}
	total, err := d.segLogic.GetDocOrgDatumCount(ctx, filter)
	if err != nil {
		logx.E(ctx, "task(DocSegIntervene)|GetDocOrgDataCount|err:%v", err)
		return err
	}
	logx.I(ctx, "task(DocSegIntervene)|GetDocOrgDataCount|total:%d", total)
	for {
		pageNumber++
		req := &pb.ListDocSegmentReq{
			AppBizId:   strconv.FormatUint(d.params.AppBizID, 10),
			DocBizId:   strconv.FormatUint(d.params.OriginDocBizID, 10),
			PageNumber: uint32(pageNumber),
			PageSize:   uint32(pageSize),
		}
		list, err := d.segLogic.GetDocSegmentItemList(ctx, req, docCommon)
		if err != nil {
			logx.E(ctx, "task(DocSegIntervene) GetDocSegmentList|err:%v", err)
			return err
		}
		// 特殊处理，在干预中数据库中数据被修改报错
		if len(list) == 0 {
			skipTimes++
			if skipTimes > deleteDataMaxLimit {
				break
			}
			err = fmt.Errorf("切片数据获取错误或连续删除切片数量超过%d", deleteDataMaxLimit*pageSize)
			logx.E(ctx, "task(DocSegIntervene) GetDocSegmentList|err:%v", err)
			return err
		} else {
			skipTimes = 0
		}
		err = d.splicingDoc(ctx, list, cacheDocPath, &lastPage, shortURLSyncMap, fileName)
		if err != nil {
			logx.E(ctx, "task(DocSegIntervene) splicingDoc|err:%v", err)
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
func (d *DocSegInterveneTaskHandler) splicingDoc(ctx context.Context, list []*pb.ListDocSegmentRsp_DocSegmentItem,
	cacheDocPath string, lastPage *uint64, shortURLSyncMap *sync.Map, fileName string) error {
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
		imageURLs, err := d.docLogic.ParseImgURL(ctx, seg.OrgData)
		if err != nil {
			logx.E(ctx, "task(DocSegIntervene) ParseImgURL|err:%v", err)
			return err
		}
		logx.D(ctx, "task(DocSegIntervene)|short imageURL:%+v", imageURLs)
		for _, imageURL := range imageURLs {
			if strings.HasPrefix(imageURL, config.App().ShortURLRootURL) ||
				strings.HasPrefix(imageURL, config.App().PreviewURLRootURL) {
				URL, err := url.Parse(imageURL)
				if err != nil {
					logx.W(ctx, "task(DocSegIntervene) url.Parse|err:%v", err)
					continue
				}
				if _, ok := shortURLSyncMap.Load(imageURL); ok {
					continue
				}
				cosURL, err := d.docLogic.ShortURLCodeRecoverCosURL(ctx, imageURL, URL.Path)
				if err != nil {
					logx.W(ctx, "task(DocSegIntervene) ShortURLCodeRecoverCosURL|err:%v", err)
					return err
				}
				logx.D(ctx, "task(DocSegIntervene)|cosURL:%s", cosURL)
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
		content = append(content, []byte(orgDataStartTag+seg.OrgData+orgDataEndTag)...)
		err = writeCacheInterveneDoc(ctx, cacheDocPath, content)
		if err != nil {
			logx.E(ctx, "task(DocSegIntervene) writeCacheInterveneDoc|err:%v", err)
			return err
		}
	}
	return nil
}

// createDocParseFromOldDoc 创建解析任务
func (d *DocSegInterveneTaskHandler) createDocParseFromOldDoc(ctx context.Context, cosPath, cosHash string,
	interventionType uint32) (bool, error) {
	// 获取老文档
	oldDoc, err := d.docLogic.GetDocByBizID(ctx, d.params.OriginDocBizID, d.params.AppID)
	if err != nil {
		logx.E(ctx, "task(DocSegIntervene) GetDocByBizID|err:%v", err)
		return false, err
	}
	if oldDoc == nil {
		logx.E(ctx, "task(DocSegIntervene)|CreateNewDocFromOldDoc|GetDocByBizID doc is null")
		return false, errs.ErrDocNotFound
	}
	// 新数据写入redis(解析回调时使用)
	err = d.docLogic.SetInterveneOldDocCosHashToNewDocRedisValueByDoc(ctx, d.params.AppBizID, cosPath, cosHash, oldDoc, interventionType)
	if err != nil {
		logx.E(ctx, "task(DocSegIntervene) SetOldDocCosHashToNewDocRedisValueByDoc|err:%+v", err)
		return false, err
	}
	// 在提交解析统计字符数任务前，替换doc的cos地址
	oldDoc.CosURL = cosPath
	oldDoc.CosHash = cosHash
	// 提交解析统计字符数&新增t_doc_parse
	// （解析中终止兼容）在插入数据前先校验文档状态，为解析失败则不在创建解析任务
	curDoc, err := d.docLogic.GetDocByBizID(ctx, d.params.OriginDocBizID, d.params.AppID)
	if err != nil {
		logx.E(ctx, "task(DocSegIntervene) GetDocByBizID|err:%v", err)
		return false, err
	}
	if curDoc.Status == docEntity.DocStatusParseFail {
		err := fmt.Errorf("current doc status is ParseFail")
		logx.W(ctx, "task(DocSegIntervene) before SendDocParseWordCount|err:%v", err)
		return true, err
	}
	requestID := contextx.TraceID(ctx)
	taskID, err := d.docLogic.SendDocParseWordCount(ctx, oldDoc, requestID, oldDoc.FileType)
	if err != nil {
		logx.E(ctx, "task(DocSegIntervene) getCacheInterveneDocPath|err:%v", err)
		return false, err
	}

	// todo 将之前的解析任务删除，仅保留当前任务（获取任务时通过order by id排序）
	err = d.docLogic.DeleteDocParseByDocID(ctx, oldDoc.CorpID, oldDoc.RobotID, oldDoc.ID)
	if err != nil {
		logx.E(ctx, "task(DocSegIntervene) DeleteDocParseByDocID|err:%v", err)
		return false, err
	}
	docParse := &docEntity.DocParse{
		DocID:     oldDoc.ID,
		CorpID:    oldDoc.CorpID,
		RobotID:   oldDoc.RobotID,
		StaffID:   oldDoc.StaffID,
		RequestID: requestID,
		Type:      docEntity.DocParseTaskTypeWordCount,
		OpType:    docEntity.DocParseOpTypeWordCount,
		Status:    docEntity.DocParseIng,
		TaskID:    taskID,
	}
	docParse.SourceEnvSet = d.params.SourceEnvSet
	err = d.docLogic.CreateDocParseTask(ctx, docParse)
	if err != nil {
		logx.E(ctx, "task(DocSegIntervene) getCacheInterveneDocPath|err:%v", err)
		return false, err
	}
	return false, nil
}

func (d *DocSegInterveneTaskHandler) writeDataToCos(ctx context.Context, docCommon *segment.DocSegmentCommon) error {
	// 1. 获取所有segmentOrgData
	var segmentOrgDatas []*pb.ListDocSegmentRsp_DocSegmentItem
	pageNumber := 0
	pageSize := 100
	for {
		pageNumber++
		req := &pb.ListDocSegmentReq{
			AppBizId:   strconv.FormatUint(d.params.AppBizID, 10),
			DocBizId:   strconv.FormatUint(d.params.OriginDocBizID, 10),
			PageNumber: uint32(pageNumber),
			PageSize:   uint32(pageSize),
		}
		list, err := d.segLogic.GetDocSegmentItemList(ctx, req, docCommon)
		if err != nil {
			logx.E(ctx, "task(DocSegIntervene) GetDocSegmentList|err:%v", err)
			return err
		}
		if len(list) == 0 {
			break
		}
		segmentOrgDatas = append(segmentOrgDatas, list...)
	}

	// 2.拼接数据
	sheetDataMap := make(map[string]string)
	for _, segmentOrgData := range segmentOrgDatas {
		if data, ok := sheetDataMap[segmentOrgData.SheetName]; ok {
			sheetDataMap[segmentOrgData.SheetName] = data + "\n" + extractTableDataFromMarkdown(segmentOrgData.OrgData)
		} else {
			table, err := extractTableFromMarkdown(segmentOrgData.OrgData)
			if err != nil {
				logx.E(ctx, "task(DocSegIntervene) writeDataToCos extractTableFromMarkdown|err:%v", err)
				return err
			}
			sheetDataMap[segmentOrgData.SheetName] = table
		}
		logx.D(ctx, "task(DocSegIntervene) writeDataToCos|sheetName:%s, sheetData:%s",
			segmentOrgData.SheetName, segmentOrgData.OrgData)
	}
	logx.D(ctx, "task(DocSegIntervene) writeDataToCos|sheetDataMap:%+v", sheetDataMap)

	// 3. 写入COS
	pageNumber = 0
	pageSize = 100
	for {
		pageNumber++
		req := &pb.ListTableSheetReq{
			AppBizId:   strconv.FormatUint(d.params.AppBizID, 10),
			DocBizId:   strconv.FormatUint(d.params.OriginDocBizID, 10),
			PageNumber: uint32(pageNumber),
			PageSize:   uint32(pageSize),
		}
		list, err := d.docLogic.GetSheetList(ctx, req, docCommon)
		if err != nil {
			logx.E(ctx, "task(DocSegIntervene) writeDataToCos GetSheetList|err:%v", err)
			return err
		}
		if len(list) == 0 {
			break
		}
		for _, sheet := range list {
			if data, ok := sheetDataMap[sheet.SheetName]; ok {
				logx.D(ctx, "task(DocSegIntervene) writeDataToCos|sheetName:%s, cosUrl:%s",
					sheet.SheetName, sheet.CosURL)
				err := d.s3.PutObject(ctx, []byte(data), sheet.CosURL)
				if err != nil {
					logx.E(ctx, "task(DocSegIntervene) writeDataToCos PutObject|err:%v", err)
					return err
				}
				err = d.updateSheetCos(ctx, docCommon, sheet)
				if err != nil {
					logx.E(ctx, "task(DocSegIntervene) writeDataToCos updateSheetCos err:%+v", err)
					return err
				}
			}
		}
	}
	return nil
}

func (d *DocSegInterveneTaskHandler) updateSheetCos(ctx context.Context, docCommon *segment.DocSegmentCommon,
	sheet *segment.DocSegmentSheetTemporary) error {
	objectInfo, err := d.s3.StatObject(ctx, sheet.CosURL)
	if err != nil {
		logx.E(ctx, "task(DocSegIntervene) writeDataToCos StatObject err:%+v", err)
		return err
	}
	newSheet := *sheet
	newSheet.CosHash = objectInfo.Hash
	updateColumns := []string{
		segEntity.DocSegmentSheetTemporaryTblColCosHash,
	}
	updateFilter := &segEntity.DocSegmentSheetTemporaryFilter{
		CorpBizID:   docCommon.CorpBizID,
		AppBizID:    docCommon.AppBizID,
		DocBizID:    docCommon.DocBizID,
		BusinessIDs: []uint64{newSheet.BusinessID},
	}
	_, err = d.segLogic.UpdateDocSegmentSheet(ctx, updateColumns, updateFilter, &newSheet)
	if err != nil {
		logx.E(ctx, "task(DocSegIntervene) writeDataToCos UpdateDocSegmentSheet|err:%v", err)
		return err
	}
	return nil
}

// extractTableDataFromMarkdown 从 Markdown 内容中提取表格(含表头)
func extractTableFromMarkdown(mdContent string) (string, error) {
	lines := strings.Split(mdContent, "\n")
	if len(lines) > 3 {
		lines = lines[3:] // 去除前三行
	} else {
		return "", errs.ErrInterveneDataFail
	}
	var result []string
	for _, s := range lines {
		if s != "" {
			result = append(result, s)
		}
	}
	return strings.Join(result, "\n"), nil
}

// extractTableDataFromMarkdown 从 Markdown 内容中提取表格数据(不含表头)
func extractTableDataFromMarkdown(mdContent string) string {
	lines := strings.Split(mdContent, "\n")
	var tableData string
	foundSeparator := false

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		// 检查是否是分隔符行（包含 |---| 格式）
		if strings.HasPrefix(trimmed, "|") && strings.Contains(trimmed, "---") {
			foundSeparator = true
			continue
		}
		// 只处理分隔符后的表格行
		if foundSeparator {
			if tableData != "" {
				tableData = tableData + "\n" + line
			} else {
				tableData = line
			}
		}
	}
	return tableData
}
