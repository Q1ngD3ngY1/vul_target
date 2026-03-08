package dao

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/baicaoyuan/moss/types/slicex"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	utilConfig "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/util/config"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/knowledge"
	webParserServer "git.woa.com/dialogue-platform/proto/pb-stub/web-parser-server"
	jsoniter "github.com/json-iterator/go"
	"github.com/xuri/excelize/v2"
	"google.golang.org/protobuf/proto"
)

type fileConfig struct {
	offlineWorkerChan  chan struct{} // 离线文档解析工作队列
	realtimeWorkerChan chan struct{} // 实时文件解析工作队列
}

// parseResult 文件解析结果数据
type parseResult struct {
	index int32 // 文件顺序
	err   error // 异常错误

	docPageContents   []*pb.PageContent // 文档数据
	tablePageContents []*pb.PageContent // 表格数据
	text2SqlResults   *pb.Tables        // text2Sql数据
}

const (
	// defaultOfflineMaxWorker 默认离线文档并行处理最大数量
	defaultOfflineMaxWorker = 10
	// defaultRealtimeMaxWorker 默认实时文档并行处理最大数量
	defaultRealtimeMaxWorker = 10
)

// fileConfig 文件解析配置
var fileCfg *fileConfig

// InitFileConfig 初始化文件解析配置
func InitFileConfig() {
	offlineWorker := utilConfig.GetMainConfig().FileParseConfig.OfflineMaxWorker
	if offlineWorker <= 0 {
		offlineWorker = defaultOfflineMaxWorker
	}
	log.Infof("InitFileConfig|offlineWorker:%d", offlineWorker)
	offlineWorkerChan := make(chan struct{}, offlineWorker)
	for i := 0; i < offlineWorker; i++ {
		offlineWorkerChan <- struct{}{}
	}

	realtimeWorker := utilConfig.GetMainConfig().FileParseConfig.RealtimeMaxWorker
	if realtimeWorker <= 0 {
		realtimeWorker = defaultRealtimeMaxWorker
	}
	log.Infof("InitFileConfig|realtimeWorker:%d", realtimeWorker)
	realtimeWorkerChan := make(chan struct{}, realtimeWorker)
	for i := 0; i < realtimeWorker; i++ {
		realtimeWorkerChan <- struct{}{}
	}

	fileCfg = &fileConfig{
		offlineWorkerChan:  offlineWorkerChan,
		realtimeWorkerChan: realtimeWorkerChan,
	}
}

// pb "git.woa.com/dialogue-platform/proto/pb-stub/file_manager_callback"

// imgReg 图片链接替换的Regexp
// 底座解析图片的格式：前缀 "![xxx](", 后缀 ")"
// - ![xxx](http://xxx)
// - ![Figure 3: Visualizing unnormalized relative perplexity gains with r=0.1.](https://fileparser-1251316161.cos.ap-guangzhou.myqcloud.com/image_files/a6b6785371a33249bbbf2c6485952643-image.png?size=max|456.3*137.7|0.77)
var imgReg = regexp.MustCompile(`!\[.*?\]\((.*?)\)`)

// ParseDocFile TODO
// Deprecated: Use ParseOfflineDocTaskResult instead
// ParseDocFile 解析文档内容（从Cos）
func (d *dao) ParseDocFile(ctx context.Context, docParse model.DocParse,
) (docPageContents []*pb.PageContent, tablePageContents []*pb.PageContent, text2SqlResults *pb.Tables, err error) {
	log.InfoContextf(ctx, "ParseDocFile docParse:%+v", docParse)
	docPageContents, tablePageContents = make([]*pb.PageContent, 0), make([]*pb.PageContent, 0)
	text2SqlResults = &pb.Tables{}
	if docParse.Result == "" {
		return docPageContents, tablePageContents, text2SqlResults, errs.ErrDocParseCosURLNotFound
	}
	result := &pb.FileParserCallbackReq{}
	err = jsoniter.UnmarshalFromString(docParse.Result, result)
	if err != nil {
		return docPageContents, tablePageContents, text2SqlResults, err
	}
	if result.ResultCosUrl == "" {
		return docPageContents, tablePageContents, text2SqlResults, errs.ErrDocParseCosURLNotFound
	}

	body, err := d.GetObject(ctx, result.ResultCosUrl)
	if err != nil {
		return docPageContents, tablePageContents, text2SqlResults, err
	}
	unSerialPb := &pb.RichContents{}
	if err = proto.Unmarshal(body, unSerialPb); err != nil {
		log.ErrorContextf(ctx, "解析文件反序列化失败 docID:%d ResultCosUrl:%s unSerialPb:%+v", docParse.DocID,
			result.ResultCosUrl, unSerialPb)
		return docPageContents, tablePageContents, text2SqlResults, err
	}

	imageURLs := make(map[string]string)
	for _, image := range unSerialPb.GetImages() {
		log.DebugContextf(ctx, "ParseDocFile imageURL:%s", image)
		imageURL, err := d.parseImgURL(ctx, image)
		if err != nil {
			return docPageContents, tablePageContents, text2SqlResults, err
		}
		URL, err := url.Parse(imageURL)
		if err != nil || URL.Path == "" {
			continue
		}
		if _, ok := imageURLs[URL.Scheme+"://"+URL.Host+URL.Path]; ok {
			continue
		}
		shortURL, err := d.shortURLCode(ctx, model.OfflineStorageTypeKey, URL.Path)
		if err != nil {
			return docPageContents, tablePageContents, text2SqlResults, err
		}
		log.DebugContextf(ctx, "ParseDocFile shortURL:%s", shortURL)
		imageURLs[URL.Scheme+"://"+URL.Host+URL.Path] = shortURL
	}

	// 文档切片
	for _, richContent := range unSerialPb.GetRichContents() {
		for _, pageContent := range richContent.GetPageContents() {
			orgData := pageContent.GetPageContentOrgString()
			bigData := pageContent.GetPageContentBigString()
			for oldPath, newPath := range imageURLs {
				orgData = strings.ReplaceAll(orgData, oldPath, newPath)
				bigData = strings.ReplaceAll(bigData, oldPath, newPath)
			}
			pageContent.PageContentOrgString = orgData
			pageContent.PageContentBigString = bigData
		}
		docPageContents = append(docPageContents, richContent.GetPageContents()...)
	}
	// 表格切片
	for _, tableContent := range unSerialPb.GetTableSplitResults() {
		pageContent := tableContent.GetTablePageContents()
		orgData := pageContent.GetPageContentOrgString()
		bigData := pageContent.GetPageContentBigString()
		for oldPath, newPath := range imageURLs {
			orgData = strings.ReplaceAll(orgData, oldPath, newPath)
			bigData = strings.ReplaceAll(bigData, oldPath, newPath)
		}
		pageContent.PageContentOrgString = orgData
		pageContent.PageContentBigString = bigData
		tablePageContents = append(tablePageContents, tableContent.GetTablePageContents())
	}
	// text2sql 图片url替换
	text2SqlResults = replaceShotURLFromText2SQLCells(ctx, unSerialPb, imageURLs)
	return docPageContents, tablePageContents, text2SqlResults, err
}

func replaceShotURLFromText2SQLCells(ctx context.Context, unSerialPb *pb.RichContents,
	imageURLs map[string]string) *pb.Tables {
	dataType := unSerialPb.GetMetaData().GetDataType().String()

	log.InfoContextf(ctx, "replaceShotURLFromText2SQLCells|dataType:%s|tables.len:%d|meta:%+v", dataType,
		len(unSerialPb.GetText2SqlResults().GetTables()), unSerialPb.GetMetaData())

	for _, table := range unSerialPb.GetText2SqlResults().GetTables() {
		for _, row := range table.GetRows() {
			for _, cell := range row.GetCells() {
				for oldPath, newPath := range imageURLs {
					cell.Value = strings.ReplaceAll(cell.Value, oldPath, newPath)
				}
				// row.Cells[ci] = cell
			}
			// table.Row[ri] = row
		}
	}
	return unSerialPb.GetText2SqlResults()
}

// parseImgURL 解析底座图片链接
func (d *dao) parseImgURL(ctx context.Context, image string) (string, error) {
	match := imgReg.FindStringSubmatch(image)
	if len(match) > 1 {
		imageURL := match[1]
		// 处理URL中的转义字符，特别是反斜杠转义的竖线
		imageURL = strings.ReplaceAll(imageURL, "\\|", "|")
		log.DebugContextf(ctx, "parseImgURL|original:%s, processed:%s", match[1], imageURL)
		return imageURL, nil
	} else {
		log.ErrorContextf(ctx, "parseImgURL invalid image:%s", image)
		return "", errs.ErrDocParseCosURLNotFound
	}
}

// ParseImgURL 解析底座图片链接
func (d *dao) ParseImgURL(ctx context.Context, orgData string) ([]string, error) {
	imgURLs := make([]string, 0)
	matches := imgReg.FindAllStringSubmatch(orgData, -1)
	for _, match := range matches {
		if len(match) > 1 {
			imgURLs = append(imgURLs, match[1])
		}
	}
	log.InfoContextf(ctx, "parseImgURL FindAllStringSubmatch len(imgURLs):%d", len(imgURLs))
	return imgURLs, nil
}

// shortURLCode 生成底座图片短链
func (d *dao) shortURLCode(ctx context.Context, storageTypeKey, path string) (string, error) {
	var scheme string
	if storageTypeKey == model.RealtimeStorageTypeKey {
		scheme = config.App().RealtimeShortURLScheme
	} else {
		scheme = config.App().OfflineShortURLScheme
	}
	name := util.RandStr(10)
	code, err := d.AddShortURL(ctx, name, scheme+"://"+path)
	if err != nil {
		log.ErrorContextf(
			ctx, "AddShortURL 失败, name: %s, path: %s, err: %v",
			name, path, err,
		)
		return "", err
	}
	return config.App().ShortURLRootURL + code, nil
}

// ShortURLCodeRecoverCosURL 短链恢复正常cos链接
func (d *dao) ShortURLCodeRecoverCosURL(ctx context.Context, shortURL, path string) (string, error) {
	if path == "" {
		log.ErrorContextf(ctx, "ShortURLCodeRecoverCosURL|shortURL:%s|path:%s", shortURL, path)
		return "", fmt.Errorf("not short url format")
	}
	region := d.GetRegion(ctx)
	bucket := d.GetBucket(ctx)
	code := filepath.Base(path)
	cosPath, err := d.ShortURLToCosPath(ctx, code)
	if err != nil {
		log.ErrorContextf(
			ctx, "ShortURLToCosPath 失败, code: %s, err: %v", code, err)
		return "", err
	}
	// 拼接cos地址
	cosURL := fmt.Sprintf("https://%s.cos.%s.myqcloud.com%s", bucket, region, cosPath)
	return cosURL, nil
}

// getSplitDataFromCosURL 从COS上下载文件并处理拆分数据
func (d *dao) getSplitDataFromCosURL(ctx context.Context, shortURLSyncMap *sync.Map, cosBucket, cosURL string) (
	docPageContents []*pb.PageContent, tablePageContents []*pb.PageContent, text2SqlResults *pb.Tables, err error) {
	log.InfoContextf(ctx, "getSplitDataFromCosURL|cosBucket:%s, cosURL:%s", cosBucket, cosURL)
	docPageContents, tablePageContents, text2SqlResults =
		make([]*pb.PageContent, 0), make([]*pb.PageContent, 0), &pb.Tables{}
	if len(cosBucket) == 0 || len(cosURL) == 0 {
		return docPageContents, tablePageContents, text2SqlResults, errs.ErrDocParseCosURLNotFound
	}
	// 下载COS
	storageTypeKey := d.GetTypeKeyWithBucket(ctx, cosBucket)
	body, err := d.GetObjectWithTypeKey(ctx, storageTypeKey, cosURL)
	if err != nil {
		return docPageContents, tablePageContents, text2SqlResults, err
	}
	unSerialPb := &pb.RichContents{}
	if err = proto.Unmarshal(body, unSerialPb); err != nil {
		log.ErrorContextf(ctx, "getSplitDataFromCosURL|cosURL:%s|proto.Unmarshal failed, err:%+v", cosURL, err)
		return docPageContents, tablePageContents, text2SqlResults, err
	}

	// 短链接替换
	log.InfoContextf(ctx, "getSplitDataFromCosURL|len(Images):%d", len(unSerialPb.GetImages()))
	for _, image := range unSerialPb.GetImages() {
		log.InfoContextf(ctx, "getSplitDataFromCosURL|imageURL:%s", image)
		imageURL, err := d.parseImgURL(ctx, image)
		if err != nil {
			log.WarnContextf(ctx, "getSplitDataFromCosURL|parseImgURL failed for image:%s, err:%v", image, err)
			continue
		}
		log.InfoContextf(ctx, "getSplitDataFromCosURL|parsed imageURL:%s", imageURL)

		URL, err := url.Parse(imageURL)
		if err != nil {
			log.WarnContextf(ctx, "getSplitDataFromCosURL|url.Parse failed for imageURL:%s, err:%v", imageURL, err)
			continue
		}
		if URL.Path == "" {
			log.WarnContextf(ctx, "getSplitDataFromCosURL|URL.Path is empty for imageURL:%s", imageURL)
			continue
		}

		fullURL := URL.Scheme + "://" + URL.Host + URL.Path
		if existingShortURL, ok := shortURLSyncMap.Load(fullURL); ok {
			log.InfoContextf(ctx, "getSplitDataFromCosURL|shortURL already exists for:%s, existing shortURL:%v", fullURL,
				existingShortURL)
			continue
		}

		shortURL, err := d.shortURLCode(ctx, storageTypeKey, URL.Path)
		if err != nil {
			log.ErrorContextf(ctx, "getSplitDataFromCosURL|shortURLCode failed for path:%s, err:%v", URL.Path, err)
			continue
		}
		log.InfoContextf(ctx, "getSplitDataFromCosURL|shortURL:%s", shortURL)
		shortURLSyncMap.Store(fullURL, shortURL)
	}

	// 文档切片
	docPageContents = d.getDocPageContents(ctx, unSerialPb, shortURLSyncMap)

	// 表格切片
	tablePageContents = d.getTablePageContents(ctx, unSerialPb, shortURLSyncMap)

	// text2sql切片
	text2SqlResults = d.getText2SqlResults(ctx, unSerialPb, shortURLSyncMap)

	return docPageContents, tablePageContents, text2SqlResults, err
}

// getDocPageContents 解析文档数据
func (d *dao) getDocPageContents(ctx context.Context, unSerialPb *pb.RichContents,
	shortURLSyncMap *sync.Map) []*pb.PageContent {
	log.InfoContextf(ctx, "getDocPageContents|len(unSerialPb.GetRichContents()):%d",
		len(unSerialPb.GetRichContents()))
	docPageContents := make([]*pb.PageContent, 0)
	for _, richContent := range unSerialPb.GetRichContents() {
		if len(richContent.GetPageContents()) == 0 {
			log.ErrorContextf(ctx, "getDocPageContents|richContent.GetPageContents() is empty")
			continue
		} else {
			log.InfoContextf(ctx, "getDocPageContents|len(richContent.GetPageContents()):%d",
				len(richContent.GetPageContents()))
		}
		for _, pageContent := range richContent.GetPageContents() {
			var orgData, bigData string
			if int32(len(unSerialPb.GetOriginStr())) > pageContent.GetPageContentOrgStringIndex() &&
				int32(len(unSerialPb.GetOriginStr())) > pageContent.GetPageContentBigStringIndex() {
				orgData = unSerialPb.GetOriginStr()[pageContent.GetPageContentOrgStringIndex()]
				bigData = unSerialPb.GetOriginStr()[pageContent.GetPageContentBigStringIndex()]
			} else {
				log.ErrorContextf(ctx, "getDocPageContents|failed|len(OriginStr):%d, "+
					"PageContentOrgStringIndex:%d, PageContentBigStringIndex:%d", len(unSerialPb.GetOriginStr()),
					pageContent.GetPageContentOrgStringIndex(), pageContent.GetPageContentBigStringIndex())
			}
			// 兼容逻辑
			if len(orgData) == 0 {
				log.WarnContextf(ctx, "getDocPageContents|failed|orgData is empty|use OrgString")
				orgData = pageContent.GetPageContentOrgString()
			}
			if len(bigData) == 0 {
				log.WarnContextf(ctx, "getDocPageContents|failed|bigData is empty|use BigString")
				bigData = pageContent.GetPageContentBigString()
			}
			shortURLSyncMap.Range(func(key, value any) bool {
				oldPath := key.(string)
				newPath := value.(string)
				orgData = strings.ReplaceAll(orgData, oldPath, newPath)
				bigData = strings.ReplaceAll(bigData, oldPath, newPath)
				return true
			})
			pageContent.PageContentOrgString = orgData
			pageContent.PageContentBigString = bigData
		}
		docPageContents = append(docPageContents, richContent.GetPageContents()...)
	}
	return docPageContents
}

// getTablePageContents 解析表格数据
func (d *dao) getTablePageContents(ctx context.Context, unSerialPb *pb.RichContents,
	shortURLSyncMap *sync.Map) []*pb.PageContent {
	log.InfoContextf(ctx, "getTablePageContents|len(unSerialPb.GetTableSplitResults()):%d",
		len(unSerialPb.GetTableSplitResults()))
	tablePageContents := make([]*pb.PageContent, 0)
	for _, tableContent := range unSerialPb.GetTableSplitResults() {
		pageContent := tableContent.GetTablePageContents()
		if pageContent == nil {
			log.ErrorContextf(ctx, "getTablePageContents|tableContent.GetTablePageContents() is nil")
			continue
		}
		var orgData, bigData string
		if int32(len(unSerialPb.GetOriginStr())) > pageContent.GetPageContentOrgStringIndex() &&
			int32(len(unSerialPb.GetOriginStr())) > pageContent.GetPageContentBigStringIndex() {
			orgData = unSerialPb.GetOriginStr()[pageContent.GetPageContentOrgStringIndex()]
			bigData = unSerialPb.GetOriginStr()[pageContent.GetPageContentBigStringIndex()]
		} else {
			log.ErrorContextf(ctx, "getTablePageContents|failed|len(OriginStr):%d, "+
				"PageContentOrgStringIndex:%d, PageContentBigStringIndex:%d", len(unSerialPb.GetOriginStr()),
				pageContent.GetPageContentOrgStringIndex(), pageContent.GetPageContentBigStringIndex())
		}
		// 兼容逻辑
		if len(orgData) == 0 {
			log.WarnContextf(ctx, "getTablePageContents|failed|orgData is empty|use OrgString")
			orgData = pageContent.GetPageContentOrgString()
		}
		if len(bigData) == 0 {
			log.WarnContextf(ctx, "getTablePageContents|failed|bigData is empty|use BigString")
			bigData = pageContent.GetPageContentBigString()
		}
		shortURLSyncMap.Range(func(key, value any) bool {
			oldPath := key.(string)
			newPath := value.(string)
			orgData = strings.ReplaceAll(orgData, oldPath, newPath)
			bigData = strings.ReplaceAll(bigData, oldPath, newPath)
			return true
		})
		pageContent.PageContentOrgString = orgData
		pageContent.PageContentBigString = bigData
		tablePageContents = append(tablePageContents, tableContent.GetTablePageContents())
	}
	return tablePageContents
}

// getText2SqlResults 解析Text2Sql数据
func (d *dao) getText2SqlResults(ctx context.Context, unSerialPb *pb.RichContents,
	shortURLSyncMap *sync.Map) *pb.Tables {
	log.InfoContextf(ctx, "getText2SqlResults|len(unSerialPb.GetText2SqlResults().GetTables())):%d",
		len(unSerialPb.GetText2SqlResults().GetTables()))
	text2SqlResults := unSerialPb.GetText2SqlResults()
	for _, table := range text2SqlResults.GetTables() {
		for _, row := range table.GetRows() {
			for _, cell := range row.GetCells() {
				if cell == nil {
					log.ErrorContextf(ctx, "getText2SqlResults|cell is nil")
					continue
				}
				shortURLSyncMap.Range(func(key, value any) bool {
					oldPath := key.(string)
					newPath := value.(string)
					cell.Value = strings.ReplaceAll(cell.Value, oldPath, newPath)
					return true
				})
			}
		}
	}
	return text2SqlResults
}

// getParseDataFromCosURL 从COS上下载文件并处理解析数据
func (d *dao) getParseDataFromCosURL(ctx context.Context, shortURLSyncMap *sync.Map, cosBucket, cosURL string) (
	docFullText string, err error) {
	log.InfoContextf(ctx, "getParseDataFromCosURL|cosBucket:%s, cosURL:%s", cosBucket, cosURL)
	if len(cosBucket) == 0 || len(cosURL) == 0 {
		return docFullText, errs.ErrDocParseCosURLNotFound
	}
	// 下载COS
	storageTypeKey := d.GetTypeKeyWithBucket(ctx, cosBucket)
	body, err := d.GetObjectWithTypeKey(ctx, storageTypeKey, cosURL)
	if err != nil {
		return docFullText, err
	}
	unSerialPb := &pb.ParseResult{}
	if err = proto.Unmarshal(body, unSerialPb); err != nil {
		log.ErrorContextf(ctx, "getParseDataFromCosURL|proto.Unmarshal failed, err:%+v", err)
		return docFullText, err
	}
	// 短链接替换
	log.InfoContextf(ctx, "getParseDataFromCosURL|len(Images):%d", len(unSerialPb.GetImages()))
	for _, image := range unSerialPb.GetImages() {
		log.InfoContextf(ctx, "getParseDataFromCosURL|imageURL:%s", image)
		imageURL, err := d.parseImgURL(ctx, image)
		if err != nil {
			log.WarnContextf(ctx, "getParseDataFromCosURL|parseImgURL failed for image:%s, err:%v", image, err)
			continue
		}
		log.InfoContextf(ctx, "getParseDataFromCosURL|parsed imageURL:%s", imageURL)

		URL, err := url.Parse(imageURL)
		if err != nil {
			log.WarnContextf(ctx, "getParseDataFromCosURL|url.Parse failed for imageURL:%s, err:%v", imageURL, err)
			continue
		}
		if URL.Path == "" {
			log.WarnContextf(ctx, "getParseDataFromCosURL|URL.Path is empty for imageURL:%s", imageURL)
			continue
		}

		fullURL := URL.Scheme + "://" + URL.Host + URL.Path
		if existingShortURL, ok := shortURLSyncMap.Load(fullURL); ok {
			log.InfoContextf(ctx, "getParseDataFromCosURL|shortURL already exists for:%s, existing shortURL:%v", fullURL,
				existingShortURL)
			continue
		}

		shortURL, err := d.shortURLCode(ctx, storageTypeKey, URL.Path)
		if err != nil {
			log.ErrorContextf(ctx, "getParseDataFromCosURL|shortURLCode failed for path:%s, err:%v", URL.Path, err)
			continue
		}
		log.InfoContextf(ctx, "getParseDataFromCosURL|shortURL:%s", shortURL)
		shortURLSyncMap.Store(fullURL, shortURL)
	}

	// 文档全文
	docFullText = unSerialPb.GetResult()
	shortURLSyncMap.Range(func(key, value any) bool {
		oldPath := key.(string)
		newPath := value.(string)
		docFullText = strings.ReplaceAll(docFullText, oldPath, newPath)
		return true
	})
	log.InfoContextf(ctx, "getParseDataFromCosURL|docFullText:%s", docFullText)
	return docFullText, nil
}

// ParseExcelQA QA上传拆分
func (d *dao) ParseExcelQA(ctx context.Context, cosURL string, fileName string) ([]string, error) {
	var segments []string

	body, err := d.GetObject(ctx, cosURL)
	if err != nil {
		return segments, err
	}

	qas, err := xlsxQA(ctx, fileName, body)
	if err != nil {
		return segments, err
	}
	chunks := slicex.Chunk(qas, 200)
	for _, chunk := range chunks {
		segment, err := jsoniter.MarshalToString(chunk)
		if err != nil {
			return segments, err
		}
		segments = append(segments, segment)
	}
	return segments, nil
}

// xlsxQA TODO
// excelQA 获取模板中的问答 (只取第一个 sheet)
func xlsxQA(ctx context.Context, fileName string, body []byte) ([]*model.QA, error) {
	f, err := excelize.OpenReader(bytes.NewReader(body))
	if err != nil {
		log.ErrorContextf(ctx, "读取 xlsx 文件失败, doc: %+v, err: %+v", fileName, err)
		return nil, err
	}
	rows, err := f.Rows(f.GetSheetName(0))
	if err != nil {
		log.ErrorContextf(ctx, "解析 xlsx 文件失败, doc: %+v, err: %+v", fileName, err)
		return nil, err
	}
	i := -1
	qas := make([]*model.QA, 0, 1024)
	for rows.Next() {
		i++
		if i == 0 { // 跳过表头行
			continue
		}
		row, err := rows.Columns()
		if err != nil {
			log.ErrorContextf(ctx, "解析 xlsx 文件失败, doc: %+v, err: %+v", fileName, err)
			return nil, err
		}
		if len(row) == 0 {
			continue
		}
		question := pkg.ToUTF8([]byte(strings.TrimSpace(row[model.ExcelTplQuestionIndex])))
		answer := pkg.ToUTF8([]byte(strings.TrimSpace(row[model.ExcelTplAnswerIndex])))
		var similarQuestions []byte
		if len(row) >= model.ExcelTplSimilarQuestionIndex+1 {
			similarQuestions = pkg.ToUTF8([]byte(strings.TrimSpace(row[model.ExcelTplSimilarQuestionIndex])))
		}
		questionDesc := ""
		if model.ExcelTplQuestionDescIndex+1 <= len(row) {
			questionDesc = string(pkg.ToUTF8([]byte(strings.TrimSpace(row[model.ExcelTplQuestionDescIndex]))))
		}

		expireEnd, err := util.GetTimeFromString(ctx, model.ExcelTplTimeLayout, row)
		if err != nil {
			log.ErrorContextf(ctx, "解析 xlsx 文件失败, doc: %+v, err: %+v", fileName, err)
			return nil, err
		}
		if expireEnd.Unix()%model.HalfHourTime != 0 {
			log.ErrorContextf(ctx, "解析 xlsx 文件失败, doc: %+v, err: %+v", fileName,
				errs.ErrExcelParseFailNotHalfHour)
			return nil, errs.ErrExcelParseFailNotHalfHour
		}
		customParam := ""
		if model.ExcelTplCustomParamIndex+1 <= len(row) {
			customParam = string(pkg.ToUTF8([]byte(strings.TrimSpace(row[model.ExcelTplCustomParamIndex]))))
		}
		var attributeFlag uint64
		if model.ExcelTplQaStatusIndex+1 <= len(row) {
			qaStatus := string(pkg.ToUTF8([]byte(strings.TrimSpace(row[model.ExcelTplQaStatusIndex]))))
			if qaStatus != "" && !slices.Contains(
				[]string{
					model.ExcelTplQaStatusDisable,
					model.ExcelTplQaStatusEnable,
					i18n.Translate(ctx, model.ExcelTplQaStatusDisable),
					i18n.Translate(ctx, model.ExcelTplQaStatusEnable),
				}, qaStatus) {
				continue
			}

			if slices.Contains(
				[]string{
					model.ExcelTplQaStatusDisable,
					i18n.Translate(ctx, model.ExcelTplQaStatusDisable),
				}, qaStatus) {
				attributeFlag = model.QAAttributeFlagDisable
			}
		}
		_, path := model.GetCatePath(row)
		qas = append(qas, &model.QA{
			Question:         string(question),
			Answer:           string(answer),
			SimilarQuestions: pkg.SplitAndTrimString(string(similarQuestions), "\n"),
			CustomParam:      customParam,
			QuestionDesc:     questionDesc,
			Path:             path,
			ExpireStart:      time.Now(),
			ExpireEnd:        expireEnd,
			AttributeFlag:    attributeFlag,
		})
	}
	return qas, nil
}

// ParseDocXlsxCharSize 解析文档(xlsx)字符数
func (d *dao) ParseDocXlsxCharSize(ctx context.Context, fileName string, cosURL string, fileType string) (int, error) {
	var charSize int
	qas, err := d.ParseExcelQA(ctx, cosURL, fileName)
	if err != nil {
		log.ErrorContextf(ctx, "解析文件失败 docName:%s fileType:%s cos:%s err:%+v", fileName, fileType, cosURL, err)
		return 0, err
	}
	for _, qa := range qas {
		charSize += utf8.RuneCountInString(qa)
	}
	return charSize, nil
}

// FetURLContent 网页URL解析
func (d *dao) FetURLContent(ctx context.Context, requestID string, botBizID uint64, url string) (
	string, string, error) {
	// 调用底座解析服务
	fetchReq := &webParserServer.FetchURLContentReq{
		RequestId: requestID,
		Url:       url,
		BotBizId:  botBizID,
	}
	log.InfoContextf(ctx, "webParserCli FetchURLContent req:%+v", fetchReq)
	fetchRsp, err := d.webParserCli.FetchURLContent(ctx, fetchReq)
	if err != nil {
		log.ErrorContextf(ctx, "webParserCli FetchURLContent Failed, err:%v", err)
		return "", "", errs.ErrFetchURLFail
	}
	log.InfoContextf(ctx, "webParserCli FetchURLContent rsp:%+v", fetchRsp)
	return fetchRsp.Title, fetchRsp.Content, nil
}

// GetFileDataFromCosURL 从COS上下载文件并返回文件内容
func (d *dao) GetFileDataFromCosURL(ctx context.Context, cosURL string) (string, error) {
	cosBucket := d.storageCli.GetBucket(ctx)
	log.InfoContextf(ctx, "getFileDataFromCosURL|cosBucket:%s, cosURL:%s", cosBucket, cosURL)
	if len(cosBucket) == 0 || len(cosURL) == 0 {
		return "", errs.ErrDocParseCosURLNotFound
	}
	// 下载COS
	storageTypeKey := d.GetTypeKeyWithBucket(ctx, cosBucket)
	body, err := d.GetObjectWithTypeKey(ctx, storageTypeKey, cosURL)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

// GetDocUpdateFrequency 获取文档更新频率
func (d *dao) GetDocUpdateFrequency(ctx context.Context, requestID string, botBizID, docBizID string) (
	uint32, error) {
	// 调用底座解析服务
	fetchReq := &webParserServer.GetDocUpdateFrequencyReq{
		RequestId:          requestID,
		DocBizId:           docBizID,
		BotBizId:           botBizID,
		LoginUin:           pkg.LoginUin(ctx),
		LoginSubAccountUin: pkg.LoginSubAccountUin(ctx),
	}
	log.InfoContextf(ctx, "webParserCli GetDocUpdateFrequency req:%+v", fetchReq)
	fetchRsp, err := d.webParserCli.GetDocUpdateFrequency(ctx, fetchReq)
	if err != nil {
		log.ErrorContextf(ctx, "webParserCli GetDocUpdateFrequency Failed, err:%v", err)
		return 0, err
	}
	log.InfoContextf(ctx, "webParserCli GetDocUpdateFrequency rsp:%+v", fetchRsp)
	if fetchRsp.GetUpdatePeriodInfo() == nil || fetchRsp.GetUpdatePeriodInfo().GetUpdatePeriodH() == 0 {
		return 0, nil
	}
	return fetchRsp.GetUpdatePeriodInfo().GetUpdatePeriodH(), nil
}
