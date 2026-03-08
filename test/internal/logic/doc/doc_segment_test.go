package doc

import (
	"fmt"
	"testing"
)

func TestTableStartLine(t *testing.T) {
	fmt.Println(GetSliceTable("物揽收及运输标准3.27: \n快递货物收取标准\n\n| 物品类型 | 物品名称 | 是否支持陆运运输 | 是否支持航空运输 | 客户需知 |\n"+
		"| --- | --- | --- | --- | --- |\n| 文体用品 | 亚克力板（有机玻璃） | 可收寄 | 有条件收寄 | 包装要求请参考《包装操作指引》 |\n\n", 0))
	fmt.Println("================================")
	fmt.Println(GetSliceTable("物揽收及运输标准3.27: \n快递货物收取标准\n\n| 物品类型 | 物品名称 | 是否支持陆运运输 | 是否支持航空运输 | 客户需知 |\n"+
		"| --- | --- | --- | --- | --- |\n| 文体用品 | 亚克力板（有机玻璃） | 可收寄 | 有条件收寄 | 包装要求请参考《包装操作指引》 |\n\n", 1))
	fmt.Println("================================")

	fmt.Println(GetSliceTable("成绩表: \n<table>\n<caption>一模成绩</caption>\n<tr>\n<td>考号</td>\n<td>学号</td>\n<td>姓名</td>\n<td>语文</td>\n<td>数学</td>\n"+
		"<td>英语</td>\n<td>物理</td>\n<td>化学</td>\n<td>道法</td>\n<td>历史</td>\n<td>地理</td>\n<td>生物</td>\n<td>实验</td>\n<td>体育</td>\n<td>总分</td>\n<td>班级</td>\n"+
		"</tr>\n<tr>\n<td>309000088</td>\n<td>202409088</td>\n<td>杨海龙</td>\n<td>75.5</td>\n<td>37</td>\n<td>44.5</td>\n<td>27</td>\n<td>18</td>\n<td>36.5</td>\n<td>53.5</td>\n"+
		"<td Colspan=\"3\">20</td>\n<td>47</td>\n<td>399</td>\n<td>3</td>\n</tr>\n</table>", 0))
	fmt.Println("================================")
	fmt.Println(GetSliceTable("成绩表: \n<table>\n<caption>一模成绩</caption>\n<tr>\n<td>考号</td>\n<td>学号</td>\n<td>姓名</td>\n<td>语文</td>\n<td>数学</td>\n"+
		"<td>英语</td>\n<td>物理</td>\n<td>化学</td>\n<td>道法</td>\n<td>历史</td>\n<td>地理</td>\n<td>生物</td>\n<td>实验</td>\n<td>体育</td>\n<td>总分</td>\n<td>班级</td>\n"+
		"</tr>\n<tr>\n<td>309000088</td>\n<td>202409088</td>\n<td>杨海龙</td>\n<td>75.5</td>\n<td>37</td>\n<td>44.5</td>\n<td>27</td>\n<td>18</td>\n<td>36.5</td>\n<td>53.5</td>\n"+
		"<td Colspan=\"3\">20</td>\n<td>47</td>\n<td>399</td>\n<td>3</td>\n</tr>\n</table>", 1))
	fmt.Println("================================")
}
