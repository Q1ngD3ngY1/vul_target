package task

import (
	"fmt"
	"testing"
)

func TestMarkdownData(t *testing.T) {
	lineBreak := addLineBreak("1. 第一项\n2. 第二项\n   - 可以包含子列表\n   - 多行内容\n3. 第三项\n\n\n\n| 姓名  | 年龄  | 城市  |" +
		"\n| --- | --- | --- |\n| 张三  | 25  | 北京  |\n| 李四  | 30  | 上海  |\n\n\n\n这是一个块级 HTML div，\n 可以包含多行内容，\n " +
		"甚至嵌入 CSS 或 JavaScript。\n\n这是 pre 标签，\n通常用于显示\n  有格式的\n    原始文本（比如代码，但不用语法高亮）。\n\n\n\n" +
		"这是一个带有脚注的句子。[^1](%E8%BF%99%E6%98%AF%E8%84%9A%E6%B3%A8%E7%9A%84%E5%86%85%E5%AE%B9%EF%BC%8C)\n\n\n```json\n可以写多行，    " +
		"通常放在文档末尾。\n11111111\n\n222\n```\n\n\n\n> 这是一段引用文字。\n> \n> > 这是引用的第二行，可以换行。\n> > 还可以有更多行。\n" +
		"> > 每个 > 表示引用层级，也可以嵌套。\n\n")
	fmt.Println(lineBreak)
}

func TestMarkdownTable(t *testing.T) {
	table := extractTableFromMarkdown("|1|2|\n| --- | --- |\n|2|3|\n|1|2|\n| --- | --- |\n|2|3|")
	for i := range table {
		fmt.Println(table[i])
	}
	table = extractTableFromMarkdown("1. 第一项\n2. 第二项\n   - 可以包含子列表\n   - 多行内容\n3. 第三项\n\n\n\n" +
		"| 姓名  | 年龄  | 城市  |\n| --- | --- | --- |\n| 张三  | 25  | 北京  |\n| 李四  | 30  | 上海  |\n" +
		"这是一个块级 HTML div，\n 可以包含多行内容，\n 甚至嵌入 CSS 或 JavaScript。\n\n这是 pre 标签，\n通常用于显示\n  有格式的" +
		"    原始文本（比如代码，但不用语法高亮）。\n\n" +
		"这是一个带有脚注的句子。[^1](%E8%BF%99%E6%98%AF%E8%84%9A%E6%B3%A8%E7%9A%84%E5%86%85%E5%AE%B9%EF%BC%8C)\n\n\n```json\n可以写多行，    通常放在文档末尾。" +
		"\n```\n\n\n\n> 这是一段引用文字。\n> \n> > 这是引用的第二行，可以换行。\n> > 还可以有更多行。\n> > 每个 > 表示引用层级，也可以嵌套。\n\n")
	for i := range table {
		fmt.Println(table[i])
	}
}

func TestAddBreakLine(t *testing.T) {
	table := addLineBreak("|1|2|\n| --- | --- |\n|2|3|\n|1|2|\n| --- | --- |\n|2|3|")
	fmt.Println(table)
	fmt.Println("=========================================")
	table = addLineBreak("1. 第一项\n2. 第二项\n   - 可以包含子列表\n   - 多行内容\n3. 第三项\n\n\n\n" +
		"| 姓名  | 年龄  | 城市  |\n| --- | --- | --- |\n| 张三  | 25  | 北京  |\n| 李四  | 30  | 上海  |\n" +
		"这是一个块级 HTML div，\n 可以包含多行内容，\n 甚至嵌入 CSS 或 JavaScript。\n\n这是 pre 标签，\n通常用于显示\n  有格式的" +
		"    原始文本（比如代码，但不用语法高亮）。\n\n" +
		"这是一个带有脚注的句子。[^1](%E8%BF%99%E6%98%AF%E8%84%9A%E6%B3%A8%E7%9A%84%E5%86%85%E5%AE%B9%EF%BC%8C)\n\n\n```json\n可以写多行，    通常放在文档末尾。" +
		"\n```\n\n\n\n> 这是一段引用文字。\n> \n> > 这是引用的第二行，可以换行。\n> > 还可以有更多行。\n> > 每个 > 表示引用层级，也可以嵌套。\n\n")
	fmt.Println(table)
}
