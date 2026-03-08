package model

// VerifyCodeTypes 验证码类型
type VerifyCodeTypes int32

// 验证码类型
const (
	VerifyCodeTypesNumber   VerifyCodeTypes = 1 // 数字
	VerifyCodeTypesUnknown  VerifyCodeTypes = 0 // 未知
	VerifyCodeTypesAlphabet VerifyCodeTypes = 2 // 字母
	VerifyCodeTypesMix      VerifyCodeTypes = 3 // 数字字母混合

	ZeroUin = 0
)

// SmsTemplate 短信模板
type SmsTemplate struct {
	Uin         uint64 `json:"uin"`
	SignatureID uint64 `json:"signature"`   // 签名ID
	Type        uint64 `json:"type"`        // 短信类型：1:协同 2:营销
	Message     string `json:"message"`     // 短信内容
	RecordFlag  uint32 `json:"record_flag"` // 是否记录短信流水, 0:yes 1:no
	IsVerify    uint32 `json:"is_verify"`   // 是否是验证码短信
}

// SMSSendLimitRule SMS发送限制
type SMSSendLimitRule struct {
	Times       uint32 // 次数
	ExpireModel string // 过期方式
	ExpireTTL   uint64 // 周期(秒)
	LimitType   uint32 // 类型 1 用户维度，2 手机号维度
}

// VerifyOpt 验证码短信相关信息
type VerifyOpt struct {
	// 验证码类型
	Type VerifyCodeTypes `json:"type"`
	// 验证码长度
	Length uint32 `json:"length"`
	// 验证码有效期(秒)
	Expire uint32 `json:"expire"`
}

// SendSmsReq 发送消息req
type SendSmsReq struct {
	Uin       uint64     `json:"user_id"`    // 用户ID
	BizID     uint32     `json:"biz_id"`     // 业务ID (1 个Q注册)
	Mobile    string     `json:"mobile"`     // 手机号码
	Params    []string   `json:"params"`     // 模版参数
	VerifyOpt *VerifyOpt `json:"verify_opt"` // 验证码参数
}

// CheckVerifyCodeReq 验证码校验入参
type CheckVerifyCodeReq struct {
	Uin    uint64 // 用户ID
	BizID  uint32 // 业务ID (1 个Q注册)
	Code   string // 验证码
	Mobile string // 手机号码
}
