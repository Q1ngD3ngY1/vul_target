package util

import (
	"context"
	"math"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"git.woa.com/adp/common/x/logx"
	secapi "git.woa.com/sec-api/go/scurl"
	md "github.com/JohannesKaufmann/html-to-markdown"
	"github.com/PuerkitoBio/goquery"
	"github.com/gabriel-vasile/mimetype"
	"golang.org/x/net/html"

	"git.woa.com/adp/kb/kb-config/internal/config"
	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
)

// DetectFileExtensionByContent 根据文件内容检测文件类型
func DetectFileExtensionByContent(ctx context.Context, fileName string, content []byte) (string, error) {
	ext := mimetype.Detect(content).Extension()
	if ext == ".txt" {
		fileExt, err := DetectFileExtension(ctx, fileName)
		if err != nil {
			logx.E(ctx, "DetectFileExtension fail, fileName: %s, err: %v", fileName, err)
			return "", err
		}
		if fileExt == ".md" || fileExt == ".markdown" {
			return fileExt, nil
		}
	}
	return ext, nil
}

// DetectFileExtension 检测文件类型
func DetectFileExtension(ctx context.Context, fileName string) (string, error) {
	u, err := url.Parse(fileName)
	if err != nil {
		logx.E(ctx, "url.Parse(%s) err: %+v", fileName, err)
		return "", err
	}
	return strings.ToLower(filepath.Ext(u.Path)), nil
}

// GetFileExt 获取文件扩展名
func GetFileExt(fileName string) string {
	return strings.ToLower(strings.TrimPrefix(filepath.Ext(fileName), "."))
}

// FileNameNoSuffix 获取文件名不带扩展名
func FileNameNoSuffix(fileName string) string {
	ext := filepath.Ext(fileName)
	return strings.TrimSuffix(fileName, ext)
}

// CheckFileType 校验文件名扩展和文件类型是否匹配
func CheckFileType(ctx context.Context, fileName, fileType string) bool {
	fileExt := GetFileExt(fileName)
	if fileExt == "" || fileType == "" {
		logx.W(ctx, "CheckFileType fail, fileName: %s, fileType: %s", fileName, fileType)
		return false
	}
	if fileExt != strings.ToLower(fileType) {
		logx.W(ctx, "CheckFileType fail, fileExt: %s, fileType: %s", fileExt, fileType)
		return false
	}
	return true
}

// AuditQaVideoURLs 审核视频链接
func AuditQaVideoURLs(ctx context.Context, htmlStr string) ([]*qaEntity.DocQAFile, error) {
	var files []*qaEntity.DocQAFile
	urls, err := CheckVideoUrls(htmlStr)
	if err != nil {
		logx.E(ctx, "AuditQaVideoURLs fail, htmlStr: %s, err: %v", htmlStr, err)
		return nil, err
	}
	for _, videoUrl := range urls {
		file := &qaEntity.DocQAFile{}
		file.CosURL = videoUrl
		file.FileType = qaEntity.QaVideoFile
		files = append(files, file)
	}
	return files, nil
}

// ExtractVideoURLs 从html中提取视频链接
func ExtractVideoURLs(ctx context.Context, htmlStr string) ([]*qaEntity.DocQAFile, error) {
	doc, err := html.Parse(strings.NewReader(htmlStr))
	if err != nil {
		logx.E(ctx, "ExtractVideoURLs fail, htmlStr: %s, err: %v", htmlStr, err)
		return nil, err
	}
	var files []*qaEntity.DocQAFile
	var f func(*html.Node)
	f = func(n *html.Node) {
		if n.Type == html.ElementNode && n.Data == "video" {
			var srcEmpty bool
			for _, a := range n.Attr {
				if a.Key == "src" {
					if a.Val != "" {
						file := &qaEntity.DocQAFile{}
						file.CosURL = a.Val
						file.FileType = qaEntity.QaVideoFile
						files = append(files, file)
					} else {
						srcEmpty = true
					}
				}
			}
			if srcEmpty {
				for c := n.FirstChild; c != nil; c = c.NextSibling {
					if c.Type == html.ElementNode && c.Data == "source" {
						for _, a := range c.Attr {
							if a.Key == "src" {
								file := &qaEntity.DocQAFile{}
								file.CosURL = a.Val
								file.FileType = qaEntity.QaVideoFile
								files = append(files, file)
							}
						}
					}
				}
			}
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			f(c)
		}
	}
	f(doc)
	return files, nil
}

// ConvertBytesToChars 根据给定的字节数，计算出字符数
func ConvertBytesToChars(ctx context.Context, bytes int64) int {
	const bytesPerMB = 1048576 // 1MB = 1024 * 1024 字节
	const charsPerMB = 10000   // 字符数公式为: 1MB = 1w字符
	// 计算字节数对应的MB数
	mb := float64(bytes) / float64(bytesPerMB)
	// 计算对应的字符数
	chars := mb * charsPerMB
	logx.I(ctx, "ConvertBytesToChars|bytes:%d|mb:%v|chars|%v", bytes, mb, chars)
	// 返回四舍五入后的字符数，并转换为int类型
	return int(math.Round(chars))
}

// KeepVideoRule 保留所有 video 标签的原始 HTML
func KeepVideoRule() md.Rule {
	return md.Rule{
		Filter: []string{"video", "source"},
		Replacement: func(_ string, selection *goquery.Selection, _ *md.Options) *string {
			// 获取完整的 video 标签 HTML（包含属性和子元素）
			RepHtml, _ := goquery.OuterHtml(selection) // 处理布尔属性格式（移除空值的属性）
			RepHtml = strings.ReplaceAll(RepHtml, ` controls=""`, ` controls`)
			RepHtml = strings.ReplaceAll(RepHtml, "/></source>", "></source>")
			return &RepHtml
		},
	}
}

// ConvertDocQaHtmlToMD 将问答html转换为markdown格式
func ConvertDocQaHtmlToMD(ctx context.Context, htmlStr string) (string, error) {
	// 1. 替换 <video> 和 </video> 为不同的占位符
	placeholderVideoStart := "[KNOWLEDGE_VIDEO_START]"
	placeholderVideoEnd := "[KNOWLEDGE_VIDEO_END]"
	placeholderSource := "[KNOWLEDGE_SOURCE]"
	htmlStr = strings.ReplaceAll(htmlStr, "<video", placeholderVideoStart)
	htmlStr = strings.ReplaceAll(htmlStr, "</video>", placeholderVideoEnd)
	htmlStr = strings.ReplaceAll(htmlStr, "<source", placeholderSource)
	htmlStr = strings.ReplaceAll(htmlStr, "</source>", placeholderSource)

	// 初始化转换器，关闭自动转义
	converter := md.NewConverter("", true, &md.Options{
		EscapeMode: "disabled", // 防止 HTML 被转义
	})
	// 添加自定义规则（优先级高于默认规则）
	// converter.AddRules(KeepVideoRule())
	// 转换
	markdown, err := converter.ConvertString(htmlStr)
	if err != nil {
		logx.W(ctx, "ConvertDocQaHtmlToMD fail, htmlStr: %s, err: %v", htmlStr, err)
		return "", err
	}
	// 3. 还原占位符为原始标签
	markdown = strings.ReplaceAll(markdown, placeholderVideoStart, "<video")
	markdown = strings.ReplaceAll(markdown, placeholderVideoEnd, "</video>")
	markdown = strings.ReplaceAll(markdown, placeholderSource, "<source")
	markdown = strings.ReplaceAll(markdown, placeholderSource, "</source>")
	return markdown, nil
}

// CheckVideoUrls 检查html中是否有video标签
func CheckVideoUrls(htmlStr string) ([]string, error) {
	doc, err := html.Parse(strings.NewReader(htmlStr))
	if err != nil {
		return nil, err
	}
	var urls []string
	var f func(*html.Node)
	f = func(n *html.Node) {
		if n.Type == html.ElementNode && n.Data == "video" {
			srcEmpty := false
			for _, a := range n.Attr {
				if a.Key == "src" {
					if strings.TrimSpace(a.Val) != "" {
						urls = append(urls, a.Val)
						srcEmpty = true
						break
					}
				}
			}
			if !srcEmpty {
				var findSource func(*html.Node) bool
				findSource = func(node *html.Node) bool {
					// 先检查当前节点是否是source标签
					if node.Type == html.ElementNode && node.Data == "source" {
						for _, a := range node.Attr {
							if a.Key == "src" && strings.TrimSpace(a.Val) != "" {
								urls = append(urls, a.Val)
								return true // 找到有效src立即返回
							}
						}
						return true // 找到source标签但无有效src也停止
					}
					// 递归检查所有子节点
					for c := node.FirstChild; c != nil; c = c.NextSibling {
						if findSource(c) {
							return true
						}
					}
					return false
				}
				findSource(n)
			}
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			f(c)
		}
	}
	f(doc)
	return urls, nil
}

// GetHTTPClient 获取 安全的http client
func GetHTTPClient() *http.Client {
	if !config.App().SecAPI.Enable {
		return http.DefaultClient
	} else {
		return secapi.NewSafeClient(
			secapi.WithAllowOuter(config.App().SecAPI.AllowOuter),
			secapi.WithUnsafeDomain(config.App().SecAPI.UnsafeDomains),
			secapi.WithAllowPolaris(config.GetAllowPolaris()),
			secapi.WithConfTimeout(config.App().SecAPI.WithConfTimeout),
		)
	}
}

// IsSafeURL 是否是安全链接
func IsSafeURL(ctx context.Context, rawUrl string) (bool, error) {
	url := rawUrl
	if strings.HasPrefix(rawUrl, "//") {
		url = "https:" + rawUrl
		if !config.App().SecAPI.Enable {
			url = "http:" + rawUrl
		}
	}
	// 发送 HTTP HEAD 请求
	cli := GetHTTPClient()
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
	if err != nil {
		logx.W(ctx, "IsSafeURL NewRequestWithContext fail url: %s, err: %v", url, err)
		return false, errs.ErrFileUrlNotFound
	}
	start := time.Now()
	resp, err := cli.Do(req)
	elapsed := time.Since(start)
	logx.D(ctx, "HEAD请求耗时: %v, URL: %s", elapsed, url)
	if err != nil {
		logx.W(ctx, "IsSafeURL request head fail url: %s, err: %v", url, err)
		return false, errs.ErrFileUrlFail
	}
	defer resp.Body.Close()
	// 无法访问的地址,默认放过,不判断http状态码
	// https://tapd.woa.com/tapd_fe/70080800/story/detail/1070080800121509551?from_iteration_id=1070080800002050135
	// if resp.StatusCode != http.StatusOK {
	//	logx.W(ctx, "IsSafeURL resp fail, url: %s, statusCode: %d", url, resp.StatusCode)
	//	return false, pkg.ErrFileUrlNotFound
	// }
	logx.I(ctx, "IsSafeURL resp code, url: %s, statusCode: %d", url, resp.StatusCode)
	return true, nil
}

// CheckMarkdownImageURL 检查Markdown文本中的图片URL是否安全
func CheckMarkdownImageURL(ctx context.Context, mdContext string, uin string, appBizID uint64, uniqueImgHost *sync.Map) error {
	if config.IsInWhiteList(uin, appBizID, config.GetWhitelistConfig().QaURLWhiteList) {
		logx.I(ctx, "CheckMarkdownImageURL|CheckQaURLWhiteList|uin:%s|appBizID:%d", uin, appBizID)
		return nil
	}
	// 从Markdown文本中提取所有图片URL
	images := ExtractImagesFromMarkdown(mdContext)
	// 如果没有图片则直接返回
	if len(images) == 0 {
		return nil
	}
	// 遍历所有图片URL进行检查
	for _, image := range images {
		u, err := url.Parse(image)
		if err != nil {
			logx.W(ctx, "CheckMarkdownImageURL|解析URL失败|imageURL:%s|err:%v", image, err)
			return errs.ErrFileUrlFail
		}
		host := u.Hostname()
		logx.I(ctx, "CheckMarkdownImageURL|host:%s", host)
		if uniqueImgHost != nil {
			// 如果image已经检查过，则不做处理
			if _, ok := uniqueImgHost.Load(host); ok {
				logx.I(ctx, "CheckMarkdownImageURL|uniqueImgHost|image:%s|host:%v", image, host)
				continue
			}
		}
		startTime := time.Now() // 记录开始时间
		// 检查URL是否安全
		safe, err := IsSafeURL(ctx, image)
		if err != nil {
			logx.W(ctx, "CheckMarkdownImageURL|imageURL:%s|safe:%v|err:%v", image, safe, err)
			return err
		}
		// 如果URL不安全则返回错误
		if !safe {
			logx.W(ctx, "CheckMarkdownImageURL|imageURL:%s|unsafe", image)
			return errs.ErrFileUrlFail
		}
		if uniqueImgHost != nil {
			logx.I(ctx, "CheckMarkdownImageURL|写入host:%s|", host)
			// 已经检查过的host，存入缓存map
			uniqueImgHost.Store(host, struct{}{})
		}
		// 记录处理耗时
		elapsed := time.Since(startTime)
		logx.I(ctx, "处理IsSafeURL 第%s 行数据耗时: %v", image, elapsed)
	}
	return nil
}

// CheckQaImgURLSafeToMD 将内容转成Markdown文本,检查Markdown文本中的图片URL是否安全,返回Markdown文本
func CheckQaImgURLSafeToMD(ctx context.Context, context string, uin string, appBizID uint64, uniqueImgHost *sync.Map) (string, error) {

	startTime := time.Now() // 记录开始时间
	mdAnswer, err := ConvertDocQaHtmlToMD(ctx, context)
	if err != nil {
		logx.W(ctx, "CheckQaImgURLSafeToMD context ConvertDocQaHtmlToMD err:%v", err)
		return "", err
	}
	// 记录处理耗时
	elapsed := time.Since(startTime)
	logx.I(ctx, "处理md %s 数据耗时: %v", context, elapsed)

	if err = CheckMarkdownImageURL(ctx, mdAnswer, uin, appBizID, uniqueImgHost); err != nil {
		logx.W(ctx, "CheckQaImgURLSafeToMD context CheckMarkdownImageURL err:%v", err)
		return "", err
	}
	return mdAnswer, nil
}
