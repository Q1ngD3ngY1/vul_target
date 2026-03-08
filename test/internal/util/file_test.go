package util

import (
	"bytes"
	"context"
	"fmt"
	"image"
	"image/jpeg"
	"image/png"
	"strings"
	"testing"

	md "github.com/JohannesKaufmann/html-to-markdown"
	"github.com/stretchr/testify/assert"
)

func TestDetectFileExtensionByContent(t *testing.T) {
	img := image.NewAlpha(image.Rect(0, 0, 1, 1))
	t.Run(".jpg", func(t *testing.T) {
		w := bytes.NewBuffer([]byte{})
		err := jpeg.Encode(w, img, nil)
		assert.Nil(t, err)
		ext, err := DetectFileExtensionByContent(context.Background(), "", w.Bytes())
		assert.Nil(t, err)
		assert.Equal(t, ".jpg", ext)
	})
	t.Run(".png", func(t *testing.T) {
		w := bytes.NewBuffer([]byte{})
		err := png.Encode(w, img)
		assert.Nil(t, err)
		ext, err := DetectFileExtensionByContent(context.Background(), "", w.Bytes())
		assert.Nil(t, err)
		assert.Equal(t, ".png", ext)
	})
}

func TestGetFileExt(t *testing.T) {
	type args struct {
		fileName string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "docx扩展名",
			args: args{
				fileName: "26种病虫害防治方法.docx",
			},
			want: "docx",
		},
		{
			name: "docx大小写混用扩展名",
			args: args{
				fileName: "26种病虫害防治方法.dOcx",
			},
			want: "docx",
		},
		{
			name: "md扩展名",
			args: args{
				fileName: "腾讯会议首发轻量化混合式教学方案，助力教学数字化升级.md",
			},
			want: "md",
		},
		{
			name: "md大小写混用扩展名",
			args: args{
				fileName: "腾讯会议首发轻量化混合式教学方案，助力教学数字化升级.Md",
			},
			want: "md",
		},
		{
			name: "txt扩展名",
			args: args{
				fileName: "三四章.txt",
			},
			want: "txt",
		},
		{
			name: "txt大小写混用扩展名",
			args: args{
				fileName: "三四章.Txt",
			},
			want: "txt",
		},
		{
			name: "pdf扩展名",
			args: args{
				fileName: "23款 飞度 用户手册.pdf",
			},
			want: "pdf",
		},
		{
			name: "pdf大小写混用扩展名",
			args: args{
				fileName: "23款 飞度 用户手册.pDf",
			},
			want: "pdf",
		},
		{
			name: "xlsx扩展名",
			args: args{
				fileName: "批量导入问答模板v2.xlsx",
			},
			want: "xlsx",
		},
		{
			name: "xlsx大小写混用扩展名",
			args: args{
				fileName: "批量导入问答模板v2.xlSX",
			},
			want: "xlsx",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, GetFileExt(tt.args.fileName), "GetFileExt(%v)", tt.args.fileName)
		})
	}
}

func TestCheckFileType(t *testing.T) {
	tests := []struct {
		name     string
		fileName string
		fileType string
		expected bool
	}{
		// 基础匹配测试
		{
			name:     "匹配的小写扩展",
			fileName: "file.jpg",
			fileType: "jpg",
			expected: true,
		},
		{
			name:     "文件名大写扩展匹配",
			fileName: "FILE.JPG",
			fileType: "jpg",
			expected: true,
		},
		{
			name:     "类型参数大写匹配",
			fileName: "file.jpg",
			fileType: "JPG",
			expected: true,
		},

		// 不匹配情况测试
		{
			name:     "扩展名不匹配",
			fileName: "image.png",
			fileType: "jpg",
			expected: false,
		},
		{
			name:     "类型参数带点不匹配",
			fileName: "file.jpg",
			fileType: ".jpg",
			expected: false,
		},
		// 边缘案例测试
		{
			name:     "无扩展名且类型为空",
			fileName: "README",
			fileType: "",
			expected: false,
		},
		{
			name:     "无扩展名但类型非空",
			fileName: "data",
			fileType: "txt",
			expected: false,
		},
		{
			name:     "多重扩展名验证",
			fileName: "archive.tar.gz",
			fileType: "gz",
			expected: true,
		},
		{
			name:     "空文件名验证",
			fileName: "",
			fileType: "txt",
			expected: false,
		},
		{
			name:     "空类型验证",
			fileName: "file.txt",
			fileType: "",
			expected: false,
		},
		{
			name:     "带路径文件名验证",
			fileName: "/path/to/file.pdf",
			fileType: "pdf",
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CheckFileType(context.Background(), tt.fileName, tt.fileType)
			if result != tt.expected {
				t.Errorf("CheckFileType(%q, %q) = %v, want %v", tt.fileName, tt.fileType, result, tt.expected)
			}
		})
	}
}

//// 保留所有 video 标签的原始 HTML
//func KeepVideoRule() md.Rule {
//	return md.Rule{
//		Filter: []string{"video", "track", "source"},
//		Replacement: func(_ string, selec *goquery.Selection, _ *md.Options) *string {
//			// 获取完整的 video 标签 HTML（包含属性和子元素）
//			html, _ := goquery.OuterHtml(selec) // 处理布尔属性格式（移除空值的属性）
//			html = strings.ReplaceAll(html, ` controls=""`, ` controls`)
//			html = strings.ReplaceAll(html, "/></source>", "></source>")
//
//			// 添加换行确保 Markdown 格式整洁
//			//result := fmt.Sprintf("\n%s\n", html)
//			return &html
//		},
//	}
//}

// 导入问答模板时，转换md格式保留video标签
func TestHtmlToMd(t *testing.T) {
	input := `<img src="/static/img/wx-com.a85f11e3.svg" data-v-55482e1e="" style="width: 20px;"><p style="text-indent: 2em; line-height: 1.
5em;">您好，如果对某条纳税明细记录存在异议，可以在特色应用下申报收入查询模块对查询结果有异议的收入提出申诉，选择申诉类型并填写申诉理由后提交。</p>
{||}<p style="line-height: 1.5em;"><video class="edui-upload-video  vjs-default-skin video-js" 
controls="" preload="none" width="300" height="200" 
src="https://static.tpass.chinatax.gov.cn/znhd/znzsNsrd/gy/
v1/download/upload/material/148/20231116/9AF61FD3EF1A4FE3993AD3DA856EB0E4.mp4" data-setup="{}">
<source src="https://static.tpass.chinatax.gov.cn/znhd/znzsNsrd/gy/v1/download/upload
/material/148/20231116/9AF61FD3EF1A4FE3993AD3DA856EB0E4.mp4" type="video/mp4"/></video><br/></p>`
	// 初始化转换器，关闭自动转义
	converter := md.NewConverter("", true, &md.Options{
		EscapeMode: "disabled", //防止 HTML 被转义
	})
	// 添加自定义规则（优先级高于默认规则）
	converter.AddRules(KeepVideoRule())
	// 转换
	markdown, err := converter.ConvertString(input)
	if err != nil {
		panic(err)
	}
	fmt.Println("转换结果:\n" + markdown)
}

// TestSaveDocQaHtmlToMd 自定义规则模式
func TestSaveDocQaHtmlToMd(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		contains []string
		excludes []string
	}{
		{
			name:  "保留所有video属性",
			input: `<video data-custom="value" class="player" id="vid1"></video>`,
			contains: []string{
				`data-custom="value"`,
				`class="player"`,
				`id="vid1"`,
			},
		},
		{
			name:  "空video标签处理",
			input: `<video></video>`,
			contains: []string{
				"<video></video>",
			},
		},
		{
			name:     "非video标签转换验证",
			input:    `<div class="container"><p>text</p></div>`,
			contains: []string{"text"},
			excludes: []string{"<div", "<p>", "class=\"container\""},
		},
	}

	converter := md.NewConverter("", true, &md.Options{
		EscapeMode: "disabled",
	})
	converter.AddRules(KeepVideoRule())

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := converter.ConvertString(tt.input)
			assert.NoError(t, err)

			// 验证应包含的元素
			for _, item := range tt.contains {
				assert.Contains(t, result, item)
			}

			// 验证应排除的元素
			for _, item := range tt.excludes {
				assert.NotContains(t, result, item)
			}
		})
	}
}

// TestSaveDocQaHtmlToMdReplace 替换标签模式
func TestSaveDocQaHtmlToMdReplace(t *testing.T) {
	html := `<img src="https://qidian-qbot-test-1251316161.cos.ap-guangzhou.myqcloud.com/public/1746827241600319488/1868490220099076096/image/hAzznHAVsjqqCoQSHeK-1868490222116536320..png" class="app-avatar">kkkk`

	// 1. 替换 <video> 和 </video> 为不同的占位符
	placeholderVideoStart := "[KNOWLEDGE_VIDEO_START]"
	placeholderVideoEnd := "[KNOWLEDGE_VIDEO_END]"
	placeholderSource := "[KNOWLEDGE_SOURCE]"
	html = strings.ReplaceAll(html, "<video", placeholderVideoStart)
	html = strings.ReplaceAll(html, "</video>", placeholderVideoEnd)
	html = strings.ReplaceAll(html, "<source", placeholderSource)
	html = strings.ReplaceAll(html, "</source>", placeholderSource)

	// 初始化转换器，关闭自动转义
	converter := md.NewConverter("", true, &md.Options{
		EscapeMode: "disabled", //防止 HTML 被转义
	})
	// 添加自定义规则（优先级高于默认规则）
	//converter.AddRules(KeepVideoRule())
	// 转换
	markdown, err := converter.ConvertString(html)
	if err != nil {
		panic(err)
	}
	// 3. 还原占位符为原始标签
	markdown = strings.ReplaceAll(markdown, placeholderVideoStart, "<video")
	markdown = strings.ReplaceAll(markdown, placeholderVideoEnd, "</video>")
	markdown = strings.ReplaceAll(markdown, placeholderSource, "<source")
	markdown = strings.ReplaceAll(markdown, placeholderSource, "</source>")
	fmt.Println("转换结果:\n" + markdown)
}

// TestCheckVideoUrls 测试解析video标签
func TestCheckVideoUrls(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantUrls []string
		wantErr  bool
	}{
		{
			name:     "single video with src",
			input:    `<video src="video1.mp4"></video>`,
			wantUrls: []string{"video1.mp4"},
		},
		{
			name: "multiple video tags",
			input: `<div>
				<video src="video1.mp4"></video>
				<video src="video2.mp4"></video>
			</div>`,
			wantUrls: []string{"video1.mp4", "video2.mp4"},
		},
		{
			name: "video without src and with source child",
			input: `<video>
				<source src="fallback1.mp4">
				<source src="fallback2.mp4">
			</video>`,
			wantUrls: []string{"fallback1.mp4"},
		},
		{
			name: "video with empty src and source child",
			input: `<video src="">
				<source src="fallback.mp4">
			</video>`,
			wantUrls: []string{"fallback.mp4"},
		},
		{
			name: "nested video structure",
			input: `<div>
				<video src="outer.mp4">
					<video src="inner.mp4"></video>
				</video>
			</div>`,
			wantUrls: []string{"outer.mp4", "inner.mp4"},
		},
		{
			name: "video with both src and source",
			input: `<video src="primary.mp4">
				<source src="secondary.mp4">
			</video>`,
			wantUrls: []string{"primary.mp4"},
		},
		{
			name:     "invalid html structure",
			input:    `<video><invalid`,
			wantUrls: []string{}, // 实际能解析出空video标签
			wantErr:  false,      // html包会自动修复不完整标签
		},
		{
			name: "multiple source elements",
			input: `<video>
				<source data-extra="data" src="source1.mp4">
				<source src="source2.mp4">
				<source src="source3.mp4">
			</video>`,
			wantUrls: []string{"source1.mp4"}, // 只取第一个source的第一个src
		},
		{
			name:     "no video tags",
			input:    `<div>Hello World</div>`,
			wantUrls: []string{},
		},
		{
			name:     "video with empty src and no source",
			input:    `<video src=""></video>`,
			wantUrls: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := CheckVideoUrls(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("checkVideoUrls() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !compareStringSlices(got, tt.wantUrls) {
				t.Errorf("checkVideoUrls() = %v, want %v", got, tt.wantUrls)
			}
		})
	}
}

func compareStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
