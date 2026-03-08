package document

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"git.woa.com/adp/kb/kb-config/pkg/i18nkey"

	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	"git.woa.com/adp/kb/kb-config/internal/entity/qa"

	"github.com/xuri/excelize/v2"
	"google.golang.org/protobuf/proto"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/adp/common/x/encodingx/jsonx"
	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/common/x/gox/stringx"
	"git.woa.com/adp/common/x/mathx/randx"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	"git.woa.com/adp/kb/kb-config/internal/entity/category"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
	webParserServer "git.woa.com/dialogue-platform/proto/pb-stub/web-parser-server"
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

// initFileConfig 初始化文件解析配置
func initFileConfig() {
	offlineWorker := config.GetMainConfig().FileParseConfig.OfflineMaxWorker
	if offlineWorker <= 0 {
		offlineWorker = defaultOfflineMaxWorker
	}
	log.Infof("initFileConfig|offlineWorker:%d", offlineWorker)
	offlineWorkerChan := make(chan struct{}, offlineWorker)
	for i := 0; i < offlineWorker; i++ {
		offlineWorkerChan <- struct{}{}
	}

	realtimeWorker := config.GetMainConfig().FileParseConfig.RealtimeMaxWorker
	if realtimeWorker <= 0 {
		realtimeWorker = defaultRealtimeMaxWorker
	}
	log.Infof("initFileConfig|realtimeWorker:%d", realtimeWorker)
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

// parseImgURL 解析底座图片链接
func (l *Logic) parseImgURL(ctx context.Context, image string) (string, error) {
	match := imgReg.FindStringSubmatch(image)
	if len(match) > 1 {
		imageURL := match[1]
		// 处理URL中的转义字符，特别是反斜杠转义的竖线
		imageURL = strings.ReplaceAll(imageURL, "\\|", "|")
		logx.D(ctx, "parseImgURL|original:%s, processed:%s", match[1], imageURL)
		return imageURL, nil
	} else {
		logx.E(ctx, "parseImgURL invalid image:%s", image)
		return "", errs.ErrDocParseCosURLNotFound
	}
}

// ParseImgURL 解析底座图片链接
func (l *Logic) ParseImgURL(ctx context.Context, orgData string) ([]string, error) {
	imgURLs := make([]string, 0)
	matches := imgReg.FindAllStringSubmatch(orgData, -1)
	for _, match := range matches {
		if len(match) > 1 {
			imgURLs = append(imgURLs, match[1])
		}
	}
	logx.I(ctx, "parseImgURL FindAllStringSubmatch len(imgURLs):%d", len(imgURLs))
	return imgURLs, nil
}

// shortURLCode 生成底座图片短链
func (l *Logic) shortURLCode(ctx context.Context, storageTypeKey, path string) (string, error) {
	var scheme string
	if storageTypeKey == entity.RealtimeStorageTypeKey {
		scheme = config.App().RealtimeShortURLScheme
	} else {
		scheme = config.App().OfflineShortURLScheme
	}
	name := randx.RandomString(10, randx.WithMode(randx.AlphabetMode))
	code, err := l.rpc.ShortURL.AddShortURL(ctx, name, scheme+"://"+path)
	if err != nil {
		logx.E(
			ctx, "AddShortURL 失败, name: %s, path: %s, err: %v",
			name, path, err,
		)
		return "", err
	}
	return config.App().ShortURLRootURL + code, nil
}

// ShortURLCodeRecoverCosURL 短链恢复正常cos链接
func (l *Logic) ShortURLCodeRecoverCosURL(ctx context.Context, shortURL, path string) (string, error) {
	if path == "" {
		logx.E(ctx, "ShortURLCodeRecoverCosURL|shortURL:%s|path:%s", shortURL, path)
		return "", fmt.Errorf("not short url format")
	}
	code := filepath.Base(path)
	cosPath, err := l.rpc.ShortURL.ShortURLToCosPath(ctx, code)
	if err != nil {
		logx.E(ctx, "ShortURLToCosPath 失败, code: %s, err: %v", code, err)
		return "", err
	}
	// 构建 COS 地址
	cosURL := fmt.Sprintf("%s%s", l.s3.GetBucketURL(ctx), cosPath)
	return cosURL, nil
}

func (l *Logic) downloadParseResult(ctx context.Context, cosBucket, cosURL string) (*pb.RichContents, error) {
	logx.I(ctx, "downloadParseResult|cosBucket:%s, cosURL:%s", cosBucket, cosURL)
	if len(cosBucket) == 0 || len(cosURL) == 0 {
		return nil, errs.ErrDocParseCosURLNotFound
	}
	// 下载COS
	storageTypeKey := l.s3.GetTypeKeyWithBucket(ctx, cosBucket)
	body, err := l.s3.GetObjectWithTypeKey(ctx, storageTypeKey, cosURL)
	if err != nil {
		logx.E(ctx, "downloadParseResult|cosURL:%s|GetObjectWithTypeKey failed, err:%+v", cosURL, err)
		return nil, err
	}
	unSerialPb := &pb.RichContents{}
	if err = proto.Unmarshal(body, unSerialPb); err != nil {
		logx.E(ctx, "downloadParseResult|cosURL:%s|proto.Unmarshal failed, err:%+v", cosURL, err)
		return nil, err
	}
	return unSerialPb, nil
}

func (l *Logic) getSplitDataFromParsedResult(ctx context.Context, shortURLSyncMap *sync.Map, cosBucket string, unSerialPb *pb.RichContents) (
	docPageContents []*pb.PageContent, tablePageContents []*pb.PageContent, text2SqlResults *pb.Tables, err error) {
	logx.I(ctx, "getSplitDataFromParsedResult|cosBucket:%s, cosURL:%s", cosBucket)
	storageTypeKey := l.s3.GetTypeKeyWithBucket(ctx, cosBucket)
	logx.I(ctx, "getSplitDataFromParsedResult|storageTypeKey:%s", storageTypeKey)

	docPageContents, tablePageContents, text2SqlResults =
		make([]*pb.PageContent, 0), make([]*pb.PageContent, 0), &pb.Tables{}

	// 短链接替换
	logx.I(ctx, "getSplitDataFromParsedResult|len(Images):%d from pbFile",
		len(unSerialPb.GetImages()))
	shortUrlCount := 0
	for _, image := range unSerialPb.GetImages() {
		logx.I(ctx, "getSplitDataFromParsedResult|imageURL:%s", image)
		imageURL, err := l.parseImgURL(ctx, image)
		if err != nil {
			logx.W(ctx, "getSplitDataFromParsedResult|parseImgURL failed for image:%s, err:%v", image, err)
			continue
		}
		logx.I(ctx, "getSplitDataFromParsedResult|parsed imageURL:%s", imageURL)

		URL, err := url.Parse(imageURL)
		if err != nil {
			logx.W(ctx, "getSplitDataFromParsedResult|url.Parse failed for imageURL:%s, err:%v", imageURL, err)
			continue
		}
		if URL.Path == "" {
			logx.W(ctx, "getSplitDataFromParsedResult|URL.Path is empty for imageURL:%s", imageURL)
			continue
		}

		fullURL := URL.Scheme + "://" + URL.Host + URL.Path
		if existingShortURL, ok := shortURLSyncMap.Load(fullURL); ok {
			logx.I(ctx, "getSplitDataFromParsedResult|shortURL already exists for:%s, existing shortURL:%v", fullURL, existingShortURL)
			continue
		}

		shortURL, err := l.shortURLCode(ctx, storageTypeKey, URL.Path)
		if err != nil {
			logx.E(ctx, "getSplitDataFromParsedResult|shortURLCode failed for path:%s, err:%v", URL.Path, err)
			continue
		}
		logx.I(ctx, "getSplitDataFromParsedResult|fullURL:%s, shortURL:%s", fullURL, shortURL)

		shortURLSyncMap.Store(fullURL, shortURL)
		shortUrlCount = shortUrlCount + 1
	}

	logx.I(ctx, "getSplitDataFromParsedResult|len(shortURLSyncMap Counter):%d, cosBucket:%s",
		shortUrlCount, cosBucket)

	// 文档切片
	docPageContents = l.getDocPageContents(ctx, unSerialPb, shortURLSyncMap)

	// 表格切片
	tablePageContents = l.getTablePageContents(ctx, unSerialPb, shortURLSyncMap)

	// text2sql切片
	text2SqlResults = l.getText2SqlResults(ctx, unSerialPb, shortURLSyncMap)

	return docPageContents, tablePageContents, text2SqlResults, err

}

// getSplitDataFromCosURL 从COS上下载文件并处理拆分数据
func (l *Logic) getSplitDataFromCosURL(ctx context.Context, shortURLSyncMap *sync.Map, cosBucket, cosURL string) (
	docPageContents []*pb.PageContent, tablePageContents []*pb.PageContent, text2SqlResults *pb.Tables, err error) {
	logx.I(ctx, "getSplitDataFromCosURL|cosBucket:%s, cosURL:%s", cosBucket, cosURL)
	docPageContents, tablePageContents, text2SqlResults =
		make([]*pb.PageContent, 0), make([]*pb.PageContent, 0), &pb.Tables{}
	if len(cosBucket) == 0 || len(cosURL) == 0 {
		return docPageContents, tablePageContents, text2SqlResults, errs.ErrDocParseCosURLNotFound
	}
	// 下载COS
	storageTypeKey := l.s3.GetTypeKeyWithBucket(ctx, cosBucket)
	body, err := l.s3.GetObjectWithTypeKey(ctx, storageTypeKey, cosURL)
	if err != nil {
		logx.E(ctx, "getSplitDataFromCosURL|cosURL:%s|GetObjectWithTypeKey failed, err:%+v", cosURL, err)
		return docPageContents, tablePageContents, text2SqlResults, err
	}
	unSerialPb := &pb.RichContents{}
	if err = proto.Unmarshal(body, unSerialPb); err != nil {
		logx.E(ctx, "getSplitDataFromCosURL|cosURL:%s|proto.Unmarshal failed, err:%+v", cosURL, err)
		return docPageContents, tablePageContents, text2SqlResults, err
	}

	// 短链接替换
	logx.I(ctx, "getSplitDataFromCosURL|len(Images):%d from pbFile(cosBucket:%s, cosURL:%s)",
		len(unSerialPb.GetImages()), cosBucket, cosURL)
	shortUrlCount := 0
	for _, image := range unSerialPb.GetImages() {
		logx.I(ctx, "getSplitDataFromCosURL|imageURL:%s", image)
		imageURL, err := l.parseImgURL(ctx, image)
		if err != nil {
			logx.W(ctx, "getSplitDataFromCosURL|parseImgURL failed for image:%s, err:%v", image, err)
			continue
		}
		logx.I(ctx, "getSplitDataFromCosURL|parsed imageURL:%s", imageURL)

		URL, err := url.Parse(imageURL)
		if err != nil {
			logx.W(ctx, "getSplitDataFromCosURL|url.Parse failed for imageURL:%s, err:%v", imageURL, err)
			continue
		}
		if URL.Path == "" {
			logx.W(ctx, "getSplitDataFromCosURL|URL.Path is empty for imageURL:%s", imageURL)
			continue
		}

		fullURL := URL.Scheme + "://" + URL.Host + URL.Path
		if existingShortURL, ok := shortURLSyncMap.Load(fullURL); ok {
			logx.I(ctx, "getSplitDataFromCosURL|shortURL already exists for:%s, existing shortURL:%v", fullURL, existingShortURL)
			continue
		}

		shortURL, err := l.shortURLCode(ctx, storageTypeKey, URL.Path)
		if err != nil {
			logx.E(ctx, "getSplitDataFromCosURL|shortURLCode failed for path:%s, err:%v", URL.Path, err)
			continue
		}
		logx.I(ctx, "getSplitDataFromCosURL|fullURL:%s, shortURL:%s", fullURL, shortURL)

		shortURLSyncMap.Store(fullURL, shortURL)
		shortUrlCount = shortUrlCount + 1
	}

	logx.I(ctx, "getSplitDataFromCosURL|len(shortURLSyncMap Counter):%d (fileParsePbFile:cosBucket:%s, cosURL:%s)",
		shortUrlCount, cosBucket, cosURL)

	// 文档切片
	docPageContents = l.getDocPageContents(ctx, unSerialPb, shortURLSyncMap)

	// 表格切片
	tablePageContents = l.getTablePageContents(ctx, unSerialPb, shortURLSyncMap)

	// text2sql切片
	text2SqlResults = l.getText2SqlResults(ctx, unSerialPb, shortURLSyncMap)

	return docPageContents, tablePageContents, text2SqlResults, err
}

// getDocPageContents 解析文档数据
func (l *Logic) getDocPageContents(ctx context.Context, unSerialPb *pb.RichContents,
	shortURLSyncMap *sync.Map) []*pb.PageContent {
	logx.I(ctx, "getDocPageContents|len(unSerialPb.GetRichContents()):%d",
		len(unSerialPb.GetRichContents()))
	docPageContents := make([]*pb.PageContent, 0)
	for _, richContent := range unSerialPb.GetRichContents() {
		if len(richContent.GetPageContents()) == 0 {
			logx.W(ctx, "getDocPageContents|richContent.GetPageContents() is empty")
			continue
		} else {
			logx.I(ctx, "getDocPageContents|len(richContent.GetPageContents()):%d",
				len(richContent.GetPageContents()))
		}
		for _, pageContent := range richContent.GetPageContents() {
			var orgData, bigData string
			if int32(len(unSerialPb.GetOriginStr())) > pageContent.GetPageContentOrgStringIndex() &&
				int32(len(unSerialPb.GetOriginStr())) > pageContent.GetPageContentBigStringIndex() {
				orgData = unSerialPb.GetOriginStr()[pageContent.GetPageContentOrgStringIndex()]
				bigData = unSerialPb.GetOriginStr()[pageContent.GetPageContentBigStringIndex()]
			} else {
				logx.W(ctx, "getDocPageContents|failed|len(OriginStr):%d, "+
					"PageContentOrgStringIndex:%d, PageContentBigStringIndex:%d", len(unSerialPb.GetOriginStr()),
					pageContent.GetPageContentOrgStringIndex(), pageContent.GetPageContentBigStringIndex())
			}
			// 兼容逻辑
			if len(orgData) == 0 {
				logx.W(ctx, "getDocPageContents|failed|orgData is empty|use OrgString")
				orgData = pageContent.GetPageContentOrgString()
			}
			if len(bigData) == 0 {
				logx.W(ctx, "getDocPageContents|failed|bigData is empty|use BigString")
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
	logx.I(ctx, "getDocPageContents|len(docPageContents):%d", len(docPageContents))
	return docPageContents
}

// getTablePageContents 解析表格数据
func (l *Logic) getTablePageContents(ctx context.Context, unSerialPb *pb.RichContents,
	shortURLSyncMap *sync.Map) []*pb.PageContent {
	logx.I(ctx, "getTablePageContents|len(unSerialPb.GetTableSplitResults()):%d",
		len(unSerialPb.GetTableSplitResults()))
	tablePageContents := make([]*pb.PageContent, 0)
	for _, tableContent := range unSerialPb.GetTableSplitResults() {
		pageContent := tableContent.GetTablePageContents()
		if pageContent == nil {
			logx.W(ctx, "getTablePageContents|tableContent.GetTablePageContents() is nil")
			continue
		}
		var orgData, bigData string
		if int32(len(unSerialPb.GetOriginStr())) > pageContent.GetPageContentOrgStringIndex() &&
			int32(len(unSerialPb.GetOriginStr())) > pageContent.GetPageContentBigStringIndex() {
			orgData = unSerialPb.GetOriginStr()[pageContent.GetPageContentOrgStringIndex()]
			bigData = unSerialPb.GetOriginStr()[pageContent.GetPageContentBigStringIndex()]
		} else {
			logx.E(ctx, "getTablePageContents|failed|len(OriginStr):%d, "+
				"PageContentOrgStringIndex:%d, PageContentBigStringIndex:%d", len(unSerialPb.GetOriginStr()),
				pageContent.GetPageContentOrgStringIndex(), pageContent.GetPageContentBigStringIndex())
		}
		// 兼容逻辑
		if len(orgData) == 0 {
			logx.W(ctx, "getTablePageContents|failed|orgData is empty|use OrgString")
			orgData = pageContent.GetPageContentOrgString()
		}
		if len(bigData) == 0 {
			logx.W(ctx, "getTablePageContents|failed|bigData is empty|use BigString")
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
func (l *Logic) getText2SqlResults(ctx context.Context, unSerialPb *pb.RichContents,
	shortURLSyncMap *sync.Map) *pb.Tables {
	logx.I(ctx, "getText2SqlResults|len(unSerialPb.GetText2SqlResults().GetTables())):%d",
		len(unSerialPb.GetText2SqlResults().GetTables()))
	text2SqlResults := unSerialPb.GetText2SqlResults()
	for _, table := range text2SqlResults.GetTables() {
		for _, row := range table.GetRows() {
			for _, cell := range row.GetCells() {
				if cell == nil {
					logx.W(ctx, "getText2SqlResults|cell is nil")
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
func (l *Logic) getParseDataFromCosURL(ctx context.Context, shortURLSyncMap *sync.Map, cosBucket, cosURL string) (
	docFullText string, err error) {
	logx.I(ctx, "getParseDataFromCosURL|cosBucket:%s, cosURL:%s", cosBucket, cosURL)
	if len(cosBucket) == 0 || len(cosURL) == 0 {
		return docFullText, errs.ErrDocParseCosURLNotFound
	}
	// 下载COS
	storageTypeKey := l.s3.GetTypeKeyWithBucket(ctx, cosBucket)
	body, err := l.s3.GetObjectWithTypeKey(ctx, storageTypeKey, cosURL)
	if err != nil {
		return docFullText, err
	}
	unSerialPb := &pb.ParseResult{}
	if err = proto.Unmarshal(body, unSerialPb); err != nil {
		logx.E(ctx, "getParseDataFromCosURL|proto.Unmarshal failed, err:%+v", err)
		return docFullText, err
	}

	// 短链接替换
	logx.I(ctx, "getParseDataFromCosURL|len(Images):%d", len(unSerialPb.GetImages()))
	for _, image := range unSerialPb.GetImages() {
		logx.I(ctx, "getParseDataFromCosURL|imageURL:%s", image)
		imageURL, err := l.parseImgURL(ctx, image)
		if err != nil {
			logx.W(ctx, "getParseDataFromCosURL|parseImgURL failed for image:%s, err:%v", image, err)
			continue
		}
		logx.I(ctx, "getParseDataFromCosURL|parsed imageURL:%s", imageURL)

		URL, err := url.Parse(imageURL)
		if err != nil {
			logx.W(ctx, "getParseDataFromCosURL|url.Parse failed for imageURL:%s, err:%v", imageURL, err)
			continue
		}
		if URL.Path == "" {
			logx.W(ctx, "getParseDataFromCosURL|URL.Path is empty for imageURL:%s", imageURL)
			continue
		}

		fullURL := URL.Scheme + "://" + URL.Host + URL.Path
		if existingShortURL, ok := shortURLSyncMap.Load(fullURL); ok {
			logx.I(ctx, "getParseDataFromCosURL|shortURL already exists for:%s, existing shortURL:%v", fullURL, existingShortURL)
			continue
		}

		shortURL, err := l.shortURLCode(ctx, storageTypeKey, URL.Path)
		if err != nil {
			logx.W(ctx, "getParseDataFromCosURL|shortURLCode failed for path:%s, err:%v", URL.Path, err)
			continue
		}
		logx.I(ctx, "getParseDataFromCosURL|shortURL:%s", shortURL)
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
	logx.I(ctx, "getParseDataFromCosURL|docFullText:%s", docFullText)
	return docFullText, nil
}

// ParseExcelQA QA上传拆分
func (l *Logic) ParseExcelQA(ctx context.Context, cosURL string, fileName string) ([]string, error) {
	logx.I(ctx, "ParseExcelQA|cosURL:%s, fileName:%s", cosURL, fileName)
	var segments []string

	body, err := l.s3.GetObject(ctx, cosURL)
	if err != nil {
		logx.E(ctx, "ParseExcelQA|GetObject failed, err:%+v", err)
		return segments, err
	}

	qas, err := xlsxQA(ctx, fileName, body)
	if err != nil {
		return segments, err
	}
	chunks := slicex.Chunk(qas, 200)
	for _, chunk := range chunks {
		segment, err := jsonx.MarshalToString(chunk)
		if err != nil {
			return segments, err
		}
		segments = append(segments, segment)
	}
	return segments, nil
}

// xlsxQA TODO
// excelQA 获取模板中的问答 (只取第一个 sheet)
func xlsxQA(ctx context.Context, fileName string, body []byte) ([]*qa.QA, error) {
	logx.I(ctx, "xlsxQA|fileName:%s, body:%s", fileName, string(body))
	f, err := excelize.OpenReader(bytes.NewReader(body))
	if err != nil {
		logx.E(ctx, "Failed to read xlsx doc type, doc: %+v, err: %+v", fileName, err)
		return nil, err
	}
	rows, err := f.Rows(f.GetSheetName(0))
	if err != nil {
		logx.E(ctx, "Failed to parse xlsx doc type, doc: %+v, err: %+v", fileName, err)
		return nil, err
	}
	i := -1
	qas := make([]*qa.QA, 0, 1024)
	for rows.Next() {
		i++
		if i == 0 { // 跳过表头行
			continue
		}
		row, err := rows.Columns()
		if err != nil {
			logx.E(ctx, "Failed to parse xlsx doc type, doc: %+v, err: %+v", fileName, err)
			return nil, err
		}
		if len(row) == 0 {
			continue
		}
		question := stringx.ToUTF8(strings.TrimSpace(row[docEntity.ExcelTplQuestionIndex]))
		answer := stringx.ToUTF8(strings.TrimSpace(row[docEntity.ExcelTplAnswerIndex]))
		var similarQuestions []byte
		if len(row) >= docEntity.ExcelTplSimilarQuestionIndex+1 {
			similarQuestions = []byte(stringx.ToUTF8(strings.TrimSpace(row[docEntity.ExcelTplSimilarQuestionIndex])))
		}
		questionDesc := ""
		if docEntity.ExcelTplQuestionDescIndex+1 <= len(row) {
			questionDesc = stringx.ToUTF8(strings.TrimSpace(row[docEntity.ExcelTplQuestionDescIndex]))
		}

		expireEnd, err := util.GetTimeFromString(ctx, docEntity.ExcelTplTimeLayout, row)
		if err != nil {
			logx.E(ctx, "Failed to parse xlsx doc type, doc: %+v, err: %+v", fileName, err)
			return nil, err
		}
		if expireEnd.Unix()%docEntity.HalfHourTime != 0 {
			logx.E(ctx, "Failed to parse xlsx doc type, doc: %+v, err: %+v", fileName,
				errs.ErrExcelParseFailNotHalfHour)
			return nil, errs.ErrExcelParseFailNotHalfHour
		}
		customParam := ""
		if docEntity.ExcelTplCustomParamIndex+1 <= len(row) {
			customParam = stringx.ToUTF8(strings.TrimSpace(row[docEntity.ExcelTplCustomParamIndex]))
		}
		var attributeFlag uint64

		enableScope := entity.EnableScopeDev
		if docEntity.ExcelTplQaEnableScopeIndex+1 <= len(row) {
			qaEnableScope := stringx.ToUTF8(strings.TrimSpace(row[docEntity.ExcelTplQaEnableScopeIndex]))
			if qaEnableScope != "" {
				validEnableScopeKey := slicex.Filter(docEntity.ExcelTplEnableScopeValidKey, func(s string) bool {
					return qaEnableScope == i18n.Translate(ctx, s)
				})
				if len(validEnableScopeKey) > 0 {
					switch validEnableScopeKey[0] {
					case i18nkey.KeyEnableScopeAll:
						enableScope = entity.EnableScopeAll
					case i18nkey.KeyEnableScopeDev:
						enableScope = entity.EnableScopeDev
					case i18nkey.KeyEnableScopePublish:
						enableScope = entity.EnableScopePublish
					case i18nkey.KeyEnableScopeDisable:
						enableScope = entity.EnableScopeDisable
					default:
						logx.W(ctx, "Invalid enable scope, qaEnableScope:%s, fileName:%s", qaEnableScope, fileName)
						continue
					}
				}
			}
		}

		_, path := category.GetCatePath(row)
		qas = append(qas, &qa.QA{
			Question:         string(question),
			Answer:           string(answer),
			SimilarQuestions: stringx.SplitAndRemoveEmpty(string(similarQuestions), "\n"),
			CustomParam:      customParam,
			QuestionDesc:     questionDesc,
			Path:             path,
			ExpireStart:      time.Now(),
			ExpireEnd:        expireEnd,
			AttributeFlag:    attributeFlag,
			EnableScope:      uint32(enableScope),
		})
	}
	return qas, nil
}

// ParseDocXlsxCharSize 解析文档(xlsx)字符数
func (l *Logic) ParseDocXlsxCharSize(ctx context.Context, fileName string, cosURL string, fileType string) (int, error) {
	var charSize int
	qas, err := l.ParseExcelQA(ctx, cosURL, fileName)
	if err != nil {
		logx.E(ctx, "解析文件失败 docName:%s fileType:%s cos:%s err:%+v", fileName, fileType, cosURL, err)
		return 0, err
	}
	for _, qa := range qas {
		charSize += utf8.RuneCountInString(qa)
	}
	return charSize, nil
}

// FetURLContent 网页URL解析
func (l *Logic) FetURLContent(ctx context.Context, requestID string, botBizID uint64, url string) (
	string, string, error) {
	// 调用底座解析服务
	fetchReq := &webParserServer.FetchURLContentReq{
		RequestId: requestID,
		Url:       url,
		BotBizId:  botBizID,
	}
	logx.I(ctx, "webParserCli FetchURLContent req:%+v", fetchReq)
	fetchRsp, err := l.rpc.WebParser.FetchURLContent(ctx, fetchReq)
	if err != nil {
		logx.E(ctx, "webParserCli FetchURLContent Failed, err:%v", err)
		return "", "", errs.ErrFetchURLFail
	}
	logx.I(ctx, "webParserCli FetchURLContent rsp:%+v", fetchRsp)
	return fetchRsp.Title, fetchRsp.Content, nil
}

// GetFileDataFromCosURL 从COS上下载文件并返回文件内容
func (l *Logic) GetFileDataFromCosURL(ctx context.Context, cosURL string) (string, error) {
	cosBucket := l.s3.GetBucket(ctx)
	logx.I(ctx, "getFileDataFromCosURL|cosBucket:%s, cosURL:%s", cosBucket, cosURL)
	if len(cosBucket) == 0 || len(cosURL) == 0 {
		return "", errs.ErrDocParseCosURLNotFound
	}
	// 下载COS
	storageTypeKey := l.s3.GetTypeKeyWithBucket(ctx, cosBucket)
	body, err := l.s3.GetObjectWithTypeKey(ctx, storageTypeKey, cosURL)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

// GetDocUpdateFrequency 获取文档更新频率
func (l *Logic) GetDocUpdateFrequency(ctx context.Context, requestID string, botBizID, docBizID string) (
	uint32, error) {
	// 调用底座解析服务
	fetchReq := &webParserServer.GetDocUpdateFrequencyReq{
		RequestId:          requestID,
		DocBizId:           docBizID,
		BotBizId:           botBizID,
		LoginUin:           contextx.Metadata(ctx).LoginUin(),
		LoginSubAccountUin: contextx.Metadata(ctx).LoginSubAccountUin(),
	}
	logx.I(ctx, "webParserCli GetDocUpdateFrequency req:%+v", fetchReq)
	fetchRsp, err := l.rpc.WebParser.GetDocUpdateFrequency(ctx, fetchReq)
	if err != nil {
		logx.E(ctx, "webParserCli GetDocUpdateFrequency Failed, err:%v", err)
		return 0, err
	}
	logx.I(ctx, "webParserCli GetDocUpdateFrequency rsp:%+v", fetchRsp)
	if fetchRsp.GetUpdatePeriodInfo() == nil || fetchRsp.GetUpdatePeriodInfo().GetUpdatePeriodH() == 0 {
		return 0, nil
	}
	return fetchRsp.GetUpdatePeriodInfo().GetUpdatePeriodH(), nil
}

// GetWebTaskMetaData 获取网页文档元数据
func (l *Logic) GetWebTaskMetaData(ctx context.Context, requestID string, botBizID, docBizID string) (
	*webParserServer.GetWebTaskMetaDataRsp, error) {
	// 调用底座解析服务
	webTaskMetaDataReq := &webParserServer.GetWebTaskMetaDataReq{
		RequestId:          requestID,
		WebDocId:           docBizID,
		BotBizId:           botBizID,
		LoginUin:           contextx.Metadata(ctx).LoginUin(),
		LoginSubAccountUin: contextx.Metadata(ctx).LoginSubAccountUin(),
	}
	logx.I(ctx, "webParserCli GetWebTaskMetaData req:%+v", webTaskMetaDataReq)
	fetchRsp, err := l.rpc.WebParser.GetWebTaskMetaData(ctx, webTaskMetaDataReq)
	if err != nil {
		logx.E(ctx, "webParserCli GetWebTaskMetaData Failed, err:%v", err)
		return nil, err
	}
	logx.I(ctx, "webParserCli GetWebTaskMetaData rsp:%+v", fetchRsp)
	return fetchRsp, nil
}

// GetWebDocIsMult 获取网页文档是否多层级
func (l *Logic) GetWebDocIsMult(ctx context.Context, requestID string, botBizID, docBizID string) (
	bool, error) {
	fetchRsp, err := l.GetWebTaskMetaData(ctx, requestID, botBizID, docBizID)
	if err != nil {
		logx.E(ctx, "GetWebDocIsMult Failed, err:%v", err)
		return false, err
	}
	if fetchRsp == nil {
		return false, nil
	}
	// 检查元数据中是否有标记为多层级的项
	metaDataList := fetchRsp.GetMetaData()
	if metaDataList == nil || len(metaDataList) == 0 {
		return false, nil
	}
	// 遍历元数据，查找是否有标记为多层级的项
	for _, metaData := range metaDataList {
		if metaData.GetIsMult() {
			logx.I(ctx, "GetWebDocIsMulti docBizID:%s Mult:%v", docBizID, metaData.GetIsMult())
			return true, nil
		}
	}
	return false, err
}

// ConvertImage2ShortURL 图片原始链接转换成短链
func (l *Logic) ConvertImage2ShortURL(ctx context.Context, cosBucket string, imageURL string) (string, error) {
	logx.I(ctx, "ConvertShortURL|oriURL:%s", imageURL)

	storageTypeKey := l.s3.GetTypeKeyWithBucket(ctx, cosBucket)
	URL, err := url.Parse(imageURL)
	if err != nil {
		logx.W(ctx, "ConvertShortURL|url.Parse failed for imageURL:%s, err:%v", imageURL, err)
		return "", err
	}
	if URL.Path == "" {
		logx.W(ctx, "ConvertShortURL|URL.Path is empty for imageURL:%s", imageURL)
		return "", errors.New("URL.Path is empty")
	}

	fullURL := URL.Scheme + "://" + URL.Host + URL.Path

	shortURL, err := l.shortURLCode(ctx, storageTypeKey, URL.Path)
	if err != nil {
		logx.E(ctx, "ConvertShortURL|shortURLCode failed for path:%s, err:%v", URL.Path, err)
		return "", err
	}
	logx.I(ctx, "ConvertShortURL|fullURL:%s, shortURL:%s", fullURL, shortURL)

	return shortURL, nil
}
