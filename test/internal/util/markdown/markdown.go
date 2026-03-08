// Package markdown md内容处理
package markdown

import (
	"bytes"
	"fmt"
	"regexp"
	"strings"

	"github.com/gomarkdown/markdown"
	"github.com/gomarkdown/markdown/ast"
	"github.com/gomarkdown/markdown/parser"
)

var markdownEscaper = strings.NewReplacer(
	"\\", "\\\\",
)

// Markdown .
type Markdown struct {
	imgPlaceholder string
	imgMap         map[string]string

	linkPlaceholder string
	linkMap         map[string]string
}

// Placeholder 占位符
type Placeholder struct {
	Key   string
	Value string
}

// Option Markdown 参数
type Option func(*Markdown)

// WithImgPlaceholder 图片占位符
func WithImgPlaceholder(p string) func(*Markdown) {
	return func(e *Markdown) {
		e.imgPlaceholder = p
	}
}

// WithLinkPlaceholder 链接占位符
func WithLinkPlaceholder(p string) func(*Markdown) {
	return func(e *Markdown) {
		e.linkPlaceholder = p
	}
}

// New .
func New(opts ...Option) *Markdown {
	m := &Markdown{
		imgPlaceholder:  "https://I%d",
		imgMap:          make(map[string]string),
		linkPlaceholder: "https://L%d",
		linkMap:         make(map[string]string),
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

// ExtractLinkWithPlaceholder 使用占位符提取链接
func (m *Markdown) ExtractLinkWithPlaceholder(content []byte) ([]byte, []Placeholder) {
	x := make([]byte, len(content))
	copy(x, content)
	doc := markdown.Parse(
		x,
		parser.NewWithExtensions(parser.CommonExtensions|parser.HardLineBreak),
	)
	// 无法直接从 ast 渲染, 因为 markdown 的渲染器未完全实现
	// issue:
	//   https://github.com/gomarkdown/markdown/blob/c89a3d3cd9b5aca0ce0499d3868d94b1cf56f87b/md/md_renderer.go#L286
	// code:
	//   https://github.com/gomarkdown/markdown/issues/285
	// 完全实现后可以通过下面的方式直接渲染
	// doc, placeholders := m.extractLinkWithPlaceholder(m.ast)
	// return markdown.Render(doc, md.NewRenderer()), placeholders

	// 目前通过文本替换的方式临时处理
	// 可能导致的问题有2
	// 1. 图片和链接地址完全一致时, 链接占位符被替换为图片占位符的情况
	// 2. 图片 / 链接 带 title 时, 无法准确判断中间的空格数量, 现在默认为1个空格
	_, placeholders := m.extractLinkWithPlaceholder(doc)
	var kv []string
	for _, v := range placeholders {
		kv = append(kv, v.Value, v.Key) // 问题1
	}
	r := strings.NewReplacer(kv...)
	return []byte(r.Replace(string(content))), placeholders
}

func (m *Markdown) extractLinkWithPlaceholder(doc ast.Node) (ast.Node, []Placeholder) {
	allPlaceholders := make([]Placeholder, 0)
	var placeholderKeyMap = make(map[string]struct{})
	imgUrls := make([]string, 0)
	imgTitles := make([]string, 0)
	imgInTables := make([]bool, 0)
	linkUrls := make([]string, 0)
	linkTitles := make([]string, 0)
	linkInTables := make([]bool, 0)
	ast.WalkFunc(doc, func(node ast.Node, entering bool) ast.WalkStatus {
		switch node.(type) {
		case *ast.Image:
			if img, ok := node.(*ast.Image); ok && entering && len(img.Destination) > 0 {
				if !isValidUrl(string(img.Destination)) {
					break
				}
				imgUrls = append(imgUrls, string(img.Destination))
				imgTitles = append(imgTitles, string(img.Title))
				imgInTables = append(imgInTables, isInTable(node))
			}
		case *ast.Link:
			if link, ok := node.(*ast.Link); ok && entering && len(link.Destination) > 0 {
				if !isValidUrl(string(link.Destination)) {
					break
				}
				linkUrls = append(linkUrls, string(link.Destination))
				linkTitles = append(linkTitles, string(link.Title))
				linkInTables = append(linkInTables, isInTable(node))
			}
		default:
			leaf := node.AsLeaf()
			if leaf != nil && entering {
				// 从 Leaf 的 Literal 字段提取原始内容中的图片链接和标题
				urls, titles := extractMarkdownFromRawText(string(leaf.Literal))
				imgUrls = append(imgUrls, urls...)
				imgTitles = append(imgTitles, titles...)
				for range urls {
					imgInTables = append(imgInTables, isInTable(node))
				}
			}
		}
		return ast.GoToNext
	})

	for i, url := range imgUrls {
		placeholders := getPlaceholders(m.imgMap, m.imgPlaceholder, placeholderKeyMap, imgTitles[i], url, imgInTables[i])
		allPlaceholders = append(allPlaceholders, placeholders...)
	}
	for i, url := range linkUrls {
		placeholders := getPlaceholders(m.linkMap, m.linkPlaceholder, placeholderKeyMap, linkTitles[i], url, linkInTables[i])
		allPlaceholders = append(allPlaceholders, placeholders...)
	}

	return doc, allPlaceholders
}

// escape replaces instances of backslash with escaped backslash in text.
func escape(text []byte) []byte {
	return bytes.Replace(text, []byte(`|`), []byte(`\|`), -1)
}

func isInTable(node ast.Node) bool {
	maxDepth := 10
	for i := 0; i < maxDepth; i++ {
		if node.GetParent() != nil {
			if _, ok := node.GetParent().(*ast.TableCell); ok {
				return true
			}
			node = node.GetParent()
		} else {
			break
		}
	}
	return false
}

func getPlaceholders(urlKeyMap map[string]string, placeholderPrefix string,
	placeholderKeyMap map[string]struct{}, title string, destination string, isInTable bool) []Placeholder {
	placeholders := make([]Placeholder, 0)
	if len(destination) <= 0 {
		return placeholders
	}
	// 处理markdown中的图片链接中的转义字符，避免因为转义字符导致原文替换失败
	destination = markdownEscaper.Replace(destination)
	var placeholder string
	var ok bool
	if placeholder, ok = urlKeyMap[destination]; !ok {
		placeholder = fmt.Sprintf(placeholderPrefix, len(urlKeyMap))
		urlKeyMap[destination] = placeholder
	}
	if _, ok = placeholderKeyMap[placeholder]; !ok {
		if isInTable {
			destination = string(escape([]byte(destination)))
		}
		if title != "" {
			title = fmt.Sprintf(` "%s"`, title) // 问题2
		}
		placeholders = append(placeholders, Placeholder{
			Key:   "(" + placeholder + ")",
			Value: "(" + string(destination) + title + ")",
		})
		placeholderKeyMap[placeholder] = struct{}{}
	}
	return placeholders
}

// extractMarkdownFromRawText 从纯文本中提取markdown中的图片链接和标题
func extractMarkdownFromRawText(raw string) ([]string, []string) {
	// 1. 标题部分变为可选匹配
	// 2. 使用非捕获组处理标题的可选结构
	// 3. 支持alt文本为空的情况
	re := regexp.MustCompile(`!\[(.*?)\]\((.*?)(?:\s+"(.*?)")?\)`)
	matches := re.FindAllStringSubmatch(raw, -1)
	var urls, titles []string
	for _, m := range matches {
		if len(m) >= 4 {
			// matches[1]为方括号内的alt文本
			if !isValidUrl(m[2]) {
				break
			}
			urls = append(urls, m[2])     // 链接
			titles = append(titles, m[3]) // 标题
		}
	}
	return urls, titles
}

func isValidUrl(url string) bool {
	if strings.HasPrefix(url, "https://") || strings.HasPrefix(url, "http://") {
		return true
	}
	return false
}

var tableLineRegex = regexp.MustCompile(`^\s*\|.*\|\s*$`)
var separatorLineRegex = regexp.MustCompile(`^\s*\|?[\s:-]+\|([\s|:-]*)$`)

// IsTableLine 判断一行是否为表格行
func IsTableLine(line string) bool {
	// 至少有两个 |，且不是全是空格
	return tableLineRegex.MatchString(line) && len(strings.TrimSpace(line)) > 2
}

// IsSeparatorLine 判断是否为分隔线
func IsSeparatorLine(line string) bool {
	// 形如 | --- | --- | 或 |:---|:---:|---:|
	return separatorLineRegex.MatchString(line)
}
