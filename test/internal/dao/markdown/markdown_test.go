package markdown

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMarkdown_ExtractLinkWithPlaceholder(t *testing.T) {
	t.Run("sample 1", func(t *testing.T) {
		m := New(
			WithImgPlaceholder("https://Img%d"),
			WithLinkPlaceholder("https://Link%d"),
		)
		c, p := m.ExtractLinkWithPlaceholder([]byte(`[link0](http://link0.com)
![img0](http://img0.com)
[link1](http://link1.com)
[](http://link0.com)
![img1](http://img1.com)

[]()
![]()

[![img1](http://img1.com)](http://link2.com)

| key | value               |
| ---- | ------------------ |
| 链接 | [![](http://img3.com?size=max\|154.8*37.0\|+Inf "test")](http://link3.com) |
| 图片 | ![](http://img3.com?size=max\|154.8*37.0\|+Inf "test") |
`))
		require.EqualValues(t, []Placeholder{
			{Key: "(https://Img0)", Value: "(http://img0.com)"},
			{Key: "(https://Img1)", Value: "(http://img1.com)"},
			{Key: "(https://Img2)", Value: `(http://img3.com?size=max\|154.8*37.0\|+Inf "test")`},
			{Key: "(https://Link0)", Value: "(http://link0.com)"},
			{Key: "(https://Link1)", Value: "(http://link1.com)"},
			{Key: "(https://Link2)", Value: "(http://link2.com)"},
			{Key: "(https://Link3)", Value: "(http://link3.com)"},
		}, p)
		require.EqualValues(t, `[link0](https://Link0)
![img0](https://Img0)
[link1](https://Link1)
[](https://Link0)
![img1](https://Img1)

[]()
![]()

[![img1](https://Img1)](https://Link2)

| key | value               |
| ---- | ------------------ |
| 链接 | [![](https://Img2)](https://Link3) |
| 图片 | ![](https://Img2) |
`, string(c))
	})

	t.Run("sample 2", func(t *testing.T) {
		m := New(
			WithImgPlaceholder("https://I%d"),
			WithLinkPlaceholder("https://L%d"),
		)
		c, p := m.ExtractLinkWithPlaceholder([]byte("角色介绍: \n\n角色介绍\n 播报\n\n\n|  |  " +
			"（おかべ りんたろう）  配音： 宫野真守  LabMem No." +
			"001  1991年12月14日出生，18岁。本作的主人公。东京机电大学1" +
			"年级生。  自称狂妄的疯狂科学家，抱着作为恶人般目中无人的态度。时常穿着白大褂。  日常生活中，" +
			"常唐突地听电话并说着“机关的阴谋”之类的话，他的设定几乎就是所谓的“中二病”。在秋叶原成立了“未来道具研究所”，" +
			"发明用途不明的道具。不识气氛，本质上是个好人。非常重视实验室的伙伴们。  第二个名字是“凤凰院凶真（ほうおういん きょうま）”。" +
			"但是，桥田跟真由里直呼本名“冈伦（オカリン）”。喜欢喝被称作“智慧饮料”的Dr Pepper。 " +
			" |\n| --- | --- |\n| ![](https://oaqbot.qidian.qq.com/s/CVJDjVs1?size=max\\|487.8*586.4\\|0.61) | " +
			"着巫女服。被冈部称呼为“琉华子（ルカ子）”，还被收为弟子。受冈伦鼓励下拿着名为“妖刀·五月雨”的素振刀不停练习。" +
			"是个相当害羞的人，经常被真由理拜托去COSPLAY，但总是拒绝。 |\n\n"))
		fmt.Println(string(c))
		fmt.Println(p)
		require.EqualValues(t, `角色介绍: 

角色介绍
 播报


|  |  （おかべ りんたろう）  配音： 宫野真守  LabMem No.001  1991年12月14日出生，18岁。本作的主人公。东京机电大学1年级生。  自称狂妄的疯狂科学家，抱着作为恶人般目中无人的态度。时常穿着白大褂。  日常生活中，常唐突地听电话并说着“机关的阴谋”之类的话，他的设定几乎就是所谓的“中二病”。在秋叶原成立了“未来道具研究所”，发明用途不明的道具。不识气氛，本质上是个好人。非常重视实验室的伙伴们。  第二个名字是“凤凰院凶真（ほうおういん きょうま）”。但是，桥田跟真由里直呼本名“冈伦（オカリン）”。喜欢喝被称作“智慧饮料”的Dr Pepper。  |
| --- | --- |
| ![](https://I0) | 着巫女服。被冈部称呼为“琉华子（ルカ子）”，还被收为弟子。受冈伦鼓励下拿着名为“妖刀·五月雨”的素振刀不停练习。是个相当害羞的人，经常被真由理拜托去COSPLAY，但总是拒绝。 |

`, string(c))
		require.EqualValues(t, []Placeholder{
			{Key: "(https://I0)", Value: "(https://oaqbot.qidian.qq.com/s/CVJDjVs1?size=max\\|487.8*586.4\\|0.61)"},
		}, p)
	})

	t.Run("sample 3", func(t *testing.T) {
		m := New(
			WithImgPlaceholder("https://I%d"),
			WithLinkPlaceholder("https://L%d"),
		)
		c, p := m.ExtractLinkWithPlaceholder([]byte(`可以通过插入图片、图表、插图等方式将文字和图像结合起来呈现。\n\n![tR2eTLU0veyHqBD0y7fV-2777594556.jpg](https://qidian-qbot-test-1251316161.cos.ap-guangzhou.myqcloud.com/public/tR2eTLU0veyHqBD0y7fV-2777594556.jpg)`))
		fmt.Println(string(c))
		fmt.Println(p)
		require.EqualValues(t, `可以通过插入图片、图表、插图等方式将文字和图像结合起来呈现。\n\n![tR2eTLU0veyHqBD0y7fV-2777594556.jpg](https://I0)`, string(c))
		require.EqualValues(t, []Placeholder{
			{Key: "(https://I0)", Value: "(https://qidian-qbot-test-1251316161.cos.ap-guangzhou.myqcloud.com/public/tR2eTLU0veyHqBD0y7fV-2777594556.jpg)"},
		}, p)
	})

	t.Run("sample 4", func(t *testing.T) {
		m := New(
			WithImgPlaceholder("https://Img%d"),
			WithLinkPlaceholder("https://Link%d"),
		)
		c, p := m.ExtractLinkWithPlaceholder([]byte("150万测试数据: \n尽管也曾有过分裂割据的状态，但时间较短，而国家统一的局面则一直居于主导地位。这样一种持续数千年不散的大一统中央集权的政治格局，有其深刻的原因：（1）农耕文化及在其基础上形成的儒家“大一统”观念，奠定了中央集权的文化底蕴；（2）宗法制度和官僚制度加固了中央集权的政治基础；（3）民族融合和对中华民族的认同，形成了中央集权国家的民族凝聚力；（4）较为封闭的地理环境和抵御江河泛滥的需要，为中央集权国家提供了自然条件和驱动力；（5）近代以来的外族入侵，危及了中华民族的生存，自保求存的民族生命本能，进一步加强了各族人民的团结。长期的历史传统，决定了我们必须建立单一制的国家结构形式。\r\n\n2． 民族原因。我国是一个多民族国家，各民族的历史状况和民族关系决定了在我国的具体条件下，不适宜采取联邦制，而应该采取单一制的国家结构形式。"))
		require.EqualValues(t, []Placeholder{}, p)
		fmt.Println(string(c))
		fmt.Println(p)
	})

	t.Run("sample 5", func(t *testing.T) {
		m := New(
			WithImgPlaceholder("https://I%d"),
			WithLinkPlaceholder("https://L%d"),
		)
		c, p := m.ExtractLinkWithPlaceholder([]byte(`文档名：万孚-1905460765076488192\n文档片段：![](https://lke.cloud.tencent.com/s/YrQ4ZW0R?size=mid|863.3*338.3|0.21):这幅图的Mermaid内容可以描述为：\ngraph TD\n    A[长边搭接舌√30] --> B[文字区]\n    B --> C[净重:]\n    C --> D[千克]\n    B --> E[毛重:]\n    E --> F[千克]\n    B --> G[规格:]\n    G --> H[尺寸填写]\n    H --> I[厘米]\n    B --> J[产品批号:]\n    J --> K[标准化]\n    B --> L[生产日期:]\n    L --> M[有效期:]\n    L --> N[装箱量:]\n    N --> O[个月]\n    L --> P[箱号:]\n\n此虚线是标签定位框，\n要印刷\n(印刷尺寸设置要比贴纸尺寸少1-2mm)\n例:标签实物长90*宽55mm,\n定位框尺寸:88*53mm\n备注:\n1.版本号放置在短边摇盖上\n2.常规纸箱是牛皮纸黄,特殊情况下根据需求确认外箱整体色调\n3.堆叠层数根据承重量/堆放空间设定\n4.标签大小按需求设定,张贴位置(产品批号上方)\n5.其他图标参考医疗器械标签、标记符号使用标准设置\n物料编码&物料名称:\n\n<table>\n<caption></caption>\n<tr>\n<td Rowspan=\"3\">Wondfo 万孚</td>\n<td Colspan=\"4\">物料编码&物料名称: □内/□外尺寸</td>\n</tr>\n<tr>\n<td>颜色: ![](https://lke.cloud.tencent.com/s/uHooJLV6?size=min) K:100底色为常规牛皮纸黄</td>\n<td>![](https://lke.cloud.tencent.com/s/MrodfekC?size=min) 坑型:</td>\n<td>![](https://lke.cloud.tencent.com/s/1QnEovRp?size=min) 成型方式:</td>\n<td>改稿前编码:</td>\n</tr>\n<tr>\n<td>申请人:</td>\n<td>修改内容:</td>\n<td>设计师:</td>\n<td>![](https://lke.cloud.tencent.com/s/J5alEHjQ?size=min)</td>\n</tr>\n</table>\n\n受控编号/签名:`))
		fmt.Println(string(c))
		fmt.Println(p)
		require.EqualValues(t, `文档名：万孚-1905460765076488192\n文档片段：![](https://I0):这幅图的Mermaid内容可以描述为：\ngraph TD\n    A[长边搭接舌√30] --> B[文字区]\n    B --> C[净重:]\n    C --> D[千克]\n    B --> E[毛重:]\n    E --> F[千克]\n    B --> G[规格:]\n    G --> H[尺寸填写]\n    H --> I[厘米]\n    B --> J[产品批号:]\n    J --> K[标准化]\n    B --> L[生产日期:]\n    L --> M[有效期:]\n    L --> N[装箱量:]\n    N --> O[个月]\n    L --> P[箱号:]\n\n此虚线是标签定位框，\n要印刷\n(印刷尺寸设置要比贴纸尺寸少1-2mm)\n例:标签实物长90*宽55mm,\n定位框尺寸:88*53mm\n备注:\n1.版本号放置在短边摇盖上\n2.常规纸箱是牛皮纸黄,特殊情况下根据需求确认外箱整体色调\n3.堆叠层数根据承重量/堆放空间设定\n4.标签大小按需求设定,张贴位置(产品批号上方)\n5.其他图标参考医疗器械标签、标记符号使用标准设置\n物料编码&物料名称:\n\n<table>\n<caption></caption>\n<tr>\n<td Rowspan=\"3\">Wondfo 万孚</td>\n<td Colspan=\"4\">物料编码&物料名称: □内/□外尺寸</td>\n</tr>\n<tr>\n<td>颜色: ![](https://I1) K:100底色为常规牛皮纸黄</td>\n<td>![](https://I2) 坑型:</td>\n<td>![](https://I3) 成型方式:</td>\n<td>改稿前编码:</td>\n</tr>\n<tr>\n<td>申请人:</td>\n<td>修改内容:</td>\n<td>设计师:</td>\n<td>![](https://I4)</td>\n</tr>\n</table>\n\n受控编号/签名:`,
			string(c))
		require.EqualValues(t, []Placeholder{
			{Key: "(https://I0)", Value: "(https://lke.cloud.tencent.com/s/YrQ4ZW0R?size=mid|863.3*338.3|0.21)"},
			{Key: "(https://I1)", Value: "(https://lke.cloud.tencent.com/s/uHooJLV6?size=min)"},
			{Key: "(https://I2)", Value: "(https://lke.cloud.tencent.com/s/MrodfekC?size=min)"},
			{Key: "(https://I3)", Value: "(https://lke.cloud.tencent.com/s/1QnEovRp?size=min)"},
			{Key: "(https://I4)", Value: "(https://lke.cloud.tencent.com/s/J5alEHjQ?size=min)"},
		}, p)
	})

	t.Run("sample 6", func(t *testing.T) {
		m := New(
			WithImgPlaceholder("https://I%d"),
			WithLinkPlaceholder("https://L%d"),
		)
		c, p := m.ExtractLinkWithPlaceholder([]byte(`文档名：五粮液活动\n文档片段：\n\nSheet1\n\n| 产品型号 | 产品定位 | 运营思路 | 产品图片 | 产品包装说明 |\n| --- | --- | --- | --- | --- |\n| 名门春 | 过渡性 | 主要是策应二代春，在局部市场上巩固营造五粮春的消费氛围，培育转化五根春新品的消费者。96春作为老品中价格标杆，其价位符合江苏区域主流价位段，也是对五粮春品牌在400元价位段进行产品试点运行。 | ![](https://lke.cloud.tencent.com/s/dys5gdmu?size=def\\|14.0*14.0\\|0.00) | 名门春系列，包装主色调为浅绿色。正面用红色字体标有五粮春，底部成分说明区域无底纹，用浅金色字体表明度数、毫升数，顶部有开启盒子的扣子。 |\n| 二代春 | 主力核心 | 是支撑品牌未来发展、贡献市场销售、引领同价位品牌的核心大单品 | ![](https://lke.cloud.tencent.com/s/MYJjYJQP?size=def\\|14.0*14.0\\|0.00) | 二代春系列，包装主色调为棕金色。正面用红色字体标有五粮春，底部成分说明区域无底纹，用红色字体表明度数、毫升数，顶部无装饰。 |\n| 和美 | 形象性 | 是提高品牌价值、改善产品结构、提升产品形象，让消费者在购买二代春时对品牌有价值认知，为未来发褫幡展做好补充增量 | ![](https://lke.cloud.tencent.com/s/6aKM8WaB?size=def\\|13.0*13.0\\|0.00) | 和美春系列，包装主色调为浅金色。正面用红色字体标有五粮春，下面有红底金色花纹区域，印着时间酿造的芬芳，底部用红色色字体表明度数、毫升数，顶部有开启盒子的扣子。和美春和二代春主色调类似，主要区别是顶部，二代春顶部非常简单，无任何装饰。和美春的盒子与名门春接近，有盖子。 |\n\n`))
		fmt.Println(string(c))
		fmt.Println(p)
		require.EqualValues(t, `文档名：五粮液活动\n文档片段：\n\nSheet1\n\n| 产品型号 | 产品定位 | 运营思路 | 产品图片 | 产品包装说明 |\n| --- | --- | --- | --- | --- |\n| 名门春 | 过渡性 | 主要是策应二代春，在局部市场上巩固营造五粮春的消费氛围，培育转化五根春新品的消费者。96春作为老品中价格标杆，其价位符合江苏区域主流价位段，也是对五粮春品牌在400元价位段进行产品试点运行。 | ![](https://I0) | 名门春系列，包装主色调为浅绿色。正面用红色字体标有五粮春，底部成分说明区域无底纹，用浅金色字体表明度数、毫升数，顶部有开启盒子的扣子。 |\n| 二代春 | 主力核心 | 是支撑品牌未来发展、贡献市场销售、引领同价位品牌的核心大单品 | ![](https://I1) | 二代春系列，包装主色调为棕金色。正面用红色字体标有五粮春，底部成分说明区域无底纹，用红色字体表明度数、毫升数，顶部无装饰。 |\n| 和美 | 形象性 | 是提高品牌价值、改善产品结构、提升产品形象，让消费者在购买二代春时对品牌有价值认知，为未来发褫幡展做好补充增量 | ![](https://I2) | 和美春系列，包装主色调为浅金色。正面用红色字体标有五粮春，下面有红底金色花纹区域，印着时间酿造的芬芳，底部用红色色字体表明度数、毫升数，顶部有开启盒子的扣子。和美春和二代春主色调类似，主要区别是顶部，二代春顶部非常简单，无任何装饰。和美春的盒子与名门春接近，有盖子。 |\n\n`,
			string(c))
		require.EqualValues(t, []Placeholder{
			{Key: "(https://I0)", Value: "(https://lke.cloud.tencent.com/s/dys5gdmu?size=def\\\\|14.0*14.0\\\\|0.00)"},
			{Key: "(https://I1)", Value: "(https://lke.cloud.tencent.com/s/MYJjYJQP?size=def\\\\|14.0*14.0\\\\|0.00)"},
			{Key: "(https://I2)", Value: "(https://lke.cloud.tencent.com/s/6aKM8WaB?size=def\\\\|13.0*13.0\\\\|0.00)"},
		}, p)
	})

	t.Run("sample 7", func(t *testing.T) {
		m := New(
			WithImgPlaceholder("https://I%d"),
			WithLinkPlaceholder("https://L%d"),
		)
		c, p := m.ExtractLinkWithPlaceholder([]byte(`文档名：岚图梦想家用户手册-全版2023.11.8\n文档片段：\n车道保持辅助系统包含车道偏离预警(LDW)和车道保持辅助(LKA)两大安全驾驶辅助功能。LDW和 LKA通过智能前视摄像头探测前方车道线，并计算车辆在车道中的实际位置(车辆行驶轨迹)。LDW功能开启，当车辆偏离自车车道时,车辆发出警示音或方向盘振动,组合仪表显示警示信息,提醒驾驶员及时控制车辆。 LKA功能开启，当车辆偏离自车车道时，系统可通过对转向系统的控制，使车辆保持在自车车道内行驶，减轻驾驶员的转向负担，提高驾驶舒适性和安全性。\n车道保持辅助系统有效范围:车速位于 60 km/h~ 180 km/h之间。\n$$\\text{车道保持辅助指示灯}$$\n-车道保持辅助指示灯![](https://lke.cloud.tencent.com/s/1YYw8BUZ?size=min)灰色点亮，表示功能开启，系统处于待激活状态。\n![](https://lke.cloud.tencent.com/s/yFdz46Gl?size=min)$$\\text{-车道保持辅助指示灯绿色点亮，表示车速满足且车道线已识别，功能可![](https://lke.cloud.tencent.com/s/yFdz46Gl?size=min)![](https://lke.cloud.tencent.com/s/yFdz46Gl?size=min)![](https://lke.cloud.tencent.com/s/yFdz46Gl?size=min)![](https://lke.cloud.tencent.com/s/yFdz46Gl?size=min)![](https://lke.cloud.tencent.com/s/yFdz46Gl?size=min)![](https://lke.cloud.tencent.com/s/yFdz46Gl?size=min)![](https://lke.cloud.tencent.com/s/yFdz46Gl?size=min)![](https://lke.cloud.tencent.com/s/yFdz46Gl?size=min)![](https://lke.cloud.tencent.com/s/yFdz46Gl?size=min)![](https://lke.cloud.tencent.com/s/yFdz46Gl?size=min)![](https://lke.cloud.tencent.com/s/yFdz46Gl?size=min)![](https://lke.cloud.tencent.com/s/yFdz46Gl?size=min)![](https://lke.cloud.tencent.com/s/yFdz46Gl?size=min)![](https://lke.cloud.tencent.com/s/yFdz46Gl?size=min)![](https://lke.cloud.tencent.com/s/yFdz46Gl?size=min)![](https://lke.cloud.tencent.com/s/yFdz46Gl?size=min)![](https://lke.cloud.tencent.com/s/yFdz46Gl?size=min)![](https://lke.cloud.tencent.com/s/yFdz46Gl?size=min)![](https://lke.cloud.tencent.com/s/yFdz46Gl?size=min)![](https://lke.cloud.tencent.com/s/yFdz46Gl?size=min)![](https://lke.cloud.tencent.com/s/yFdz46Gl?size=min)![](https://lke.cloud.tencent.com/s/yFdz46Gl?size=min)![](https://lke.cloud.tencent.com/s/yFdz46Gl?size=min)![](https://lke.cloud.tencent.com/s/yFdz46Gl?size=min)![](https://lke.cloud.tencent.com/s/yFdz46Gl?size=min)![](https://lke.cloud.tencent.com/s/yFdz46Gl?size=min)![](https://lke.cloud.tencent.com/s/yFdz46Gl?size=min)![](https://lke.cloud.tencent.com/s/yFdz46Gl?size=min)![](https://lke.cloud.tencent.com/s/yFdz46Gl?size=min)![](https://lke.cloud.tencent.com/s/yFdz46Gl?size=min)![](https://lke.cloud.tencent.com/s/yFdz46Gl?size=min)![](https://lke.cloud.tencent.com/s/yFdz46Gl?size=min)![](https://lke.cloud.tencent.com/s/yFdz46Gl?size=min)![](https://lke.cloud.tencent.com/s/yFdz46Gl?size=min)![](https://lke.cloud.tencent.com/s/yFdz46Gl?size=min)![](https://lke.cloud.tencent.com/s/yFdz46Gl?size=min)![](https://lke.cloud.tencent.com/s/yFdz46Gl?size=min)![](https://lke.cloud.tencent.com/s/yFdz46Gl?size=min)![](https://lke.cloud.tencent.com/s/yFdz46Gl?size=min)![](https://lke.cloud.tencent.com/s/yFdz46Gl?size=min)-车道保持辅助指示灯![](https://lke.cloud.tencent.com/s/yFdz46Gl?size=min)![](https://lke.cloud.tencent.com/s/KCz29ZUG?size=min)![](https://lke.cloud.tencent.com/s/KCz29ZUG?size=min)![](https://lke.cloud.tencent.com/s/KCz29ZUG?size=min)以被激活。![](https://lke.cloud.tencent.com/s/YDQwbQYY?size=min)![](https://lke.cloud.tencent.com/s/NZlc3xsS?size=min)\n-车道保持辅助指示灯![](https://lke.cloud.tencent.com/s/aSXRFwOq?size=min)$$\\text{车道保持辅助指示灯绿色点亮，且车道线为蓝色，表示 LKA正在控制$$方向盘。\n-车道保持辅助指示灯![](https://lke.cloud.tencent.com/s/KVCuSNsL?size=min)$$\\text{车道保持辅助指示灯绿色点亮，且车道线为红色伴有警示音，表示 LDW}$$预警。\n![](https://I0)$$\\text{红色点亮，表示车道保持辅助系统故障，请联系岚-车道保持辅助指示灯![](https://lke.cloud.tencent.com/s/LzXujufd?size=min)图汽车服务中心。`))
		fmt.Println(string(c))
		fmt.Println(p)
		require.EqualValues(t, `文档名：岚图梦想家用户手册-全版2023.11.8\n文档片段：\n车道保持辅助系统包含车道偏离预警(LDW)和车道保持辅助(LKA)两大安全驾驶辅助功能。LDW和 LKA通过智能前视摄像头探测前方车道线，并计算车辆在车道中的实际位置(车辆行驶轨迹)。LDW功能开启，当车辆偏离自车车道时,车辆发出警示音或方向盘振动,组合仪表显示警示信息,提醒驾驶员及时控制车辆。 LKA功能开启，当车辆偏离自车车道时，系统可通过对转向系统的控制，使车辆保持在自车车道内行驶，减轻驾驶员的转向负担，提高驾驶舒适性和安全性。\n车道保持辅助系统有效范围:车速位于 60 km/h~ 180 km/h之间。\n$$\\text{车道保持辅助指示灯}$$\n-车道保持辅助指示灯![](https://I0)灰色点亮，表示功能开启，系统处于待激活状态。\n![](https://I1)$$\\text{-车道保持辅助指示灯绿色点亮，表示车速满足且车道线已识别，功能可![](https://I1)![](https://I1)![](https://I1)![](https://I1)![](https://I1)![](https://I1)![](https://I1)![](https://I1)![](https://I1)![](https://I1)![](https://I1)![](https://I1)![](https://I1)![](https://I1)![](https://I1)![](https://I1)![](https://I1)![](https://I1)![](https://I1)![](https://I1)![](https://I1)![](https://I1)![](https://I1)![](https://I1)![](https://I1)![](https://I1)![](https://I1)![](https://I1)![](https://I1)![](https://I1)![](https://I1)![](https://I1)![](https://I1)![](https://I1)![](https://I1)![](https://I1)![](https://I1)![](https://I1)![](https://I1)![](https://I1)-车道保持辅助指示灯![](https://I1)![](https://I2)![](https://I2)![](https://I2)以被激活。![](https://I3)![](https://I4)\n-车道保持辅助指示灯![](https://I5)$$\\text{车道保持辅助指示灯绿色点亮，且车道线为蓝色，表示 LKA正在控制$$方向盘。\n-车道保持辅助指示灯![](https://I6)$$\\text{车道保持辅助指示灯绿色点亮，且车道线为红色伴有警示音，表示 LDW}$$预警。\n![](https://I7)$$\\text{红色点亮，表示车道保持辅助系统故障，请联系岚-车道保持辅助指示灯![](https://I8)图汽车服务中心。`,
			string(c))
		require.EqualValues(t, []Placeholder{
			{Key: "(https://I0)", Value: "(https://lke.cloud.tencent.com/s/1YYw8BUZ?size=min)"},
			{Key: "(https://I1)", Value: "(https://lke.cloud.tencent.com/s/yFdz46Gl?size=min)"},
			{Key: "(https://I2)", Value: "(https://lke.cloud.tencent.com/s/KCz29ZUG?size=min)"},
			{Key: "(https://I3)", Value: "(https://lke.cloud.tencent.com/s/YDQwbQYY?size=min)"},
			{Key: "(https://I4)", Value: "(https://lke.cloud.tencent.com/s/NZlc3xsS?size=min)"},
			{Key: "(https://I5)", Value: "(https://lke.cloud.tencent.com/s/aSXRFwOq?size=min)"},
			{Key: "(https://I6)", Value: "(https://lke.cloud.tencent.com/s/KVCuSNsL?size=min)"},
			{Key: "(https://I7)", Value: "(https://I0)"},
			{Key: "(https://I8)", Value: "(https://lke.cloud.tencent.com/s/LzXujufd?size=min)"},
		}, p)
	})

	t.Run("sample 8", func(t *testing.T) {
		m := New(
			WithImgPlaceholder("https://I%d"),
			WithLinkPlaceholder("https://L%d"),
		)
		c, p := m.ExtractLinkWithPlaceholder([]byte(`SSL 证书_SSL 证书安装相关: \nenSSL 官网没有提供 Windows 版本的安装包，可以选择其他开源平台提供的工具，[请单击此处](https://slproweb.com/products/Win32OpenSSL.html)。以该工具为例，安装步骤和使用方法请参见 [安装 OpenSSL](undefined400/5707)。服务器如何开启443端口？请对应您使用的服务器进行操作：- 腾讯云轻量应用服务器，则已默认开启443端口。如需了解更多信息，请参见 \n- [管理防火墙](https://cloud.tencent.com/document/product/1207/44577)\n- 。\n- 腾讯云云服务器，请参考文档 \n- [添加安全组规则](https://cloud.tencent.com/document/product/213/39740)\n-  开启443端口。\n- 其他云厂商云服务器，请参考云厂商提供的相关文档。\nSSL 证书重新申请后，是否需要重新安装部署？证书重新申请后需要重新部署安装。如果是腾讯云资源可以通过 SSL 证书控制台的更新功能直接更新；如果是非腾讯云资源，需要下载更新。`))
		fmt.Println(string(c))
		fmt.Println(p)
		require.EqualValues(t, `SSL 证书_SSL 证书安装相关: \nenSSL 官网没有提供 Windows 版本的安装包，可以选择其他开源平台提供的工具，[请单击此处](https://L0)。以该工具为例，安装步骤和使用方法请参见 [安装 OpenSSL](undefined400/5707)。服务器如何开启443端口？请对应您使用的服务器进行操作：- 腾讯云轻量应用服务器，则已默认开启443端口。如需了解更多信息，请参见 \n- [管理防火墙](https://L1)\n- 。\n- 腾讯云云服务器，请参考文档 \n- [添加安全组规则](https://L2)\n-  开启443端口。\n- 其他云厂商云服务器，请参考云厂商提供的相关文档。\nSSL 证书重新申请后，是否需要重新安装部署？证书重新申请后需要重新部署安装。如果是腾讯云资源可以通过 SSL 证书控制台的更新功能直接更新；如果是非腾讯云资源，需要下载更新。`, string(c))
		require.EqualValues(t, []Placeholder{
			{Key: "(https://L0)", Value: "(https://slproweb.com/products/Win32OpenSSL.html)"},
			{Key: "(https://L1)", Value: "(https://cloud.tencent.com/document/product/1207/44577)"},
			{Key: "(https://L2)", Value: "(https://cloud.tencent.com/document/product/213/39740)"},
		}, p)
	})
}
