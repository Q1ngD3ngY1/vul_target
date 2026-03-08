package api

import (
	"testing"

	"git.woa.com/adp/kb/kb-config/internal/entity"
)

func Test_patchConfidence(t *testing.T) {
	refers := []entity.Refer{
		{OrgData: "核心人员通话记录: \n            \n            \n            \n            \n            | 会话人员  |\n            | --- |\n            | 张三  |\n            \n            \n            \n            "},
		{OrgData: "核心人员通话记录: \n            \n            \n            \n            \n            |  | 会话记录  | 服务内容  |\n            | --- | --- | --- |\n            | 张三  | [\"服务态度差\",\"业务能力差\",\"回复超级慢\",\"问题未解决\"],[\"产品不满意\",\"政策不满意\",\"活动不满意\",\"方案不满意\"]]}2023-07-01 22:09:18 {SYSTEM} 满意度评价已发送用户 | 保客咨询，汽车信号弱，手机开不了车门，海豚  |\n            \n            \n            \n            "},
		{OrgData: "核心人员通话记录: \n            \n            \n            \n            \n            | 会话人员  |\n            | --- |\n            | 李四  |\n            \n            \n            \n            "},
		{OrgData: "核心人员通话记录: \n            \n            \n            "},
	}
	answer := "{张三}的通话记录如下：\n    \n    - 2023年7月1日22:09:18，张三与用户进行了保客咨询，用户反映了汽车信号弱，手机无法开锁的问题，涉及到汽车型号为海豚。\n    - 在这次通话中，张三的服务态度、业务能力以及回复速度均受到用户的不满，用户还表达了对产品、政策、活动、方案的不满意。\n    - 通话结束后，张三将满意度评价发送给了用户。"
	refers, score := patchConfidence(refers, answer)
	t.Logf("refers:%+v\n", refers)
	t.Logf("score:%+v\n", score)
}
