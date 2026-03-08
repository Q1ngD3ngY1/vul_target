package dao

import (
	"context"
	"errors"
	"fmt"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/baicaoyuan/moss/metadata"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	cloudccpb "git.woa.com/ivy/protobuf/global-pb-server/com/tencent/epc/innerprocess/cloudcc"
	innerToTrpc "git.woa.com/ivy/protobuf/inner-to-trpc"
	"github.com/gomodule/redigo/redis"
)

const (
	verifyCodeBizID    uint32 = 1 // 短信业务代码
	smsLimitTypeUser          = 1
	smsLimitTypeMobile        = 2
	smsVerifyCodeBizID uint32 = 1 // 短信业务代码
)

var (
	// 验证码缓存Key
	_smsVerifyCacheKey = "sms_verify_code:%d:%s"
	// 验证码尝试次数
	_smsVerifyTryTimeKey = "sms_verify_try_times:%d:%s"
	// 验证码频率限制key
	_smsRateKey = "sms_rate:%d:%s:%s"
)

// SendSmsCodeV2 发送短信验证码v2
func (d *dao) SendSmsCodeV2(ctx context.Context, mobile string, uin uint64) error {
	req := &model.SendSmsReq{
		Uin:    uin,
		BizID:  verifyCodeBizID,
		Mobile: mobile,
		Params: make([]string, 0),
		VerifyOpt: &model.VerifyOpt{
			Type:   model.VerifyCodeTypesNumber,
			Length: 6,
			Expire: uint32(config.App().LoginDefault.VerifyCode.Expire.Seconds()),
		},
	}
	return d.sendSmsMessageCode(ctx, req)
}

// CheckSmsCodeV2 校验短信验证码
func (d *dao) CheckSmsCodeV2(ctx context.Context, mobile string, uin uint64, code string) error {
	req := &model.CheckVerifyCodeReq{
		Uin:    uin,
		BizID:  smsVerifyCodeBizID,
		Code:   code,
		Mobile: mobile,
	}
	return d.checkVerifyCode(ctx, req)
}

// sendSmsMessageCode 发送短信验证码
func (d *dao) sendSmsMessageCode(ctx context.Context, req *model.SendSmsReq) (err error) {
	template := getSmsTemplate()
	if err = d.checkCanSend(ctx, req.BizID, req.Mobile); err != nil {
		return err
	}
	code := d.genVerifyCode(req.VerifyOpt.Type, req.VerifyOpt.Length)
	params := append([]string{code}, req.Params...)
	keyName := getSMSVerifyCacheKey(req.BizID, req.Mobile)
	if _, err = d.redis.Do(ctx, "SET", keyName, code, "EX", int(req.VerifyOpt.Expire)); err != nil {
		log.ErrorContextf(ctx, "保存验证码失败 req:%+v err:%+v", req.Mobile, err)
		return err
	}
	if err = d.increaseSendTime(ctx, req); err != nil {
		log.ErrorContextf(ctx, "increaseSendTime fail mobile:%s, err:%+v", req.Mobile, err)
		return err
	}
	if err = d.sendSms(ctx, template, req.Mobile, params); err != nil {
		log.ErrorContextf(ctx, "sendSms fail mobile:%s, err:%+v", req.Mobile, err)
		return err
	}
	return nil
}

// checkCanSend 检查发送频率
func (d *dao) checkCanSend(ctx context.Context, bizID uint32, mobile string) error {
	for _, rule := range initRules() {
		key := genSMSRateKey(bizID, rule.LimitType, mobile)
		var value uint64
		value, err := redis.Uint64(d.redis.Do(ctx, "GET", key))
		if err != nil && !errors.Is(err, redis.ErrNil) {
			log.ErrorContextf(ctx, "checkCanSend fail key:%s err:%+v", key, err)
			return err
		}
		if value >= uint64(rule.Times) {
			if rule.LimitType == smsLimitTypeUser {
				return errs.ErrLoginTimesLimit
			}
			return errs.ErrSmsSendLimit
		}
	}
	return nil
}

func initRules() []model.SMSSendLimitRule {
	now := time.Now()
	return []model.SMSSendLimitRule{{
		Times:       1,
		ExpireModel: "EXPIRE",
		ExpireTTL:   uint64(time.Minute.Seconds()),
		LimitType:   smsLimitTypeMobile,
	}, {
		Times:       config.App().LoginDefault.VerifyCode.DaySendLimit,
		ExpireModel: "EXPIREAT",
		ExpireTTL:   uint64(time.Date(now.Year(), now.Month(), now.Day(), 23, 59, 59, 0, now.Location()).Unix()),
		LimitType:   smsLimitTypeUser,
	}}
}

// genVerifyCode 生成验证码
func (d *dao) genVerifyCode(typ model.VerifyCodeTypes, verifyLen uint32) string {
	code := make([]byte, 0)
	pool := ""
	if typ == model.VerifyCodeTypesNumber {
		pool = "0123456789"
	} else if typ == model.VerifyCodeTypesAlphabet {
		// 不包含容易混淆的字母 I,i,l,L,O,o
		pool = "abcdefghjkmnpqrstuvwxyzABCDEFGHJKMNPQRSTUVWXYZ"
	} else {
		// 不包含容易混淆的字母 0,1,I,i,l,L,O,o
		pool = "23456789abcdefghjkmnpqrstuvwxyzABCDEFGHJKMNPQRSTUVWXYZ"
	}
	length := len(pool)
	for i := uint32(0); i < verifyLen; i++ {
		code = append(code, pool[d.rand.Intn(length)])
	}
	return string(code)
}

// increaseSendTime 增加发送次数记录
func (d *dao) increaseSendTime(ctx context.Context, req *model.SendSmsReq) error {
	conn, err := d.redis.Pipeline(ctx)
	if err != nil {
		log.ErrorContextf(ctx, "Redis pipeline err:%+v", err)
		return err
	}
	defer conn.Close()
	for _, rule := range initRules() {
		key := genSMSRateKey(req.BizID, rule.LimitType, req.Mobile)
		var value uint64
		value, err = redis.Uint64(conn.Do("INCR", key))
		if err != nil {
			log.ErrorContextf(ctx, "increaseSendTime失败 key:%s err:%+v", key, err)
			return err
		}
		if value == 1 {
			if err = conn.Send(rule.ExpireModel, key, rule.ExpireTTL); err != nil {
				log.ErrorContextf(ctx, "increaseSendTime set rate EXPIRE err: %+v, keyName:%s, timeout:%d",
					err, key, rule.ExpireTTL)
				return err
			}
		}
		if err = conn.Flush(); err != nil {
			log.ErrorContextf(ctx, "increaseSendTime Flush redis pipeline fail, error:%+v", err)
			return err
		}
	}
	return nil
}

// sendSms 发送验证码消息
func (d *dao) sendSms(ctx context.Context, template *model.SmsTemplate, mobile string, params []string) (err error) {
	p := make([]any, 0)
	for _, v := range params {
		p = append(p, v)
	}
	msg := fmt.Sprintf(template.Message, p...)
	log.DebugContextf(ctx, "sendSms msg:%s, mobile:%s", msg, mobile)
	req := &cloudccpb.ManualSendSmsReq{
		ManualSmsMsg:      []*cloudccpb.ManualSmsMsg{{StrCphone: &mobile}},
		Uint64SmsType:     &template.Type,
		Uint64SignatureId: &template.SignatureID,
		StrContent:        &msg,
		Uint32RecordFlag:  &template.RecordFlag,
	}
	ctx = metadata.WithKfuin(ctx, config.App().LoginDefault.CQQKfUin)
	rsp, err := innerToTrpc.NewCloudccClientProxy().Cloudcc308970(ctx, req)
	if err != nil {
		log.ErrorContextf(ctx, "sendSms call Cloudcc308970: %v, mobile %s", err, mobile)
		return err
	}
	results := rsp.GetManualSmsMsgResult()
	if len(results) > 0 && results[0].GetUint32Result() != 300000 {
		log.ErrorContextf(ctx, "sendSms failed rsp:%+v code:%d errMsg:%s", rsp,
			results[0].GetUint32Result(), results[0].GetStrErrorMsg())
		return errs.ErrSmsSendFailed
	}
	return nil
}

// checkVerifyCode 短信验证码校验
func (d *dao) checkVerifyCode(ctx context.Context, req *model.CheckVerifyCodeReq) (err error) {
	key := getSMSVerifyTryTimeKey(req.BizID, req.Mobile)
	tryTime, err := redis.Uint64(d.redis.Do(ctx, "GET", key))
	if err != nil && err != redis.ErrNil {
		log.ErrorContextf(ctx, "Get verifyTime fail key:%s err:%+v", key, err)
		return err
	}
	if err == redis.ErrNil {
		tryTime = 0
	}
	if tryTime >= config.App().LoginDefault.VerifyCode.MaxTry {
		log.ErrorContextf(ctx, "验证码验证次数达到限制 req:%+v", req)
		return errs.ErrVerifyFailedTooManyTimes
	}
	keyName := getSMSVerifyCacheKey(req.BizID, req.Mobile)
	code, err := redis.String(d.redis.Do(ctx, "GET", keyName))
	if err == redis.ErrNil {
		log.WarnContextf(ctx, "该手机号未发送验证码 req:%+v", req)
		return errs.ErrVerifyCodeNotFound
	}
	if err != nil {
		log.ErrorContextf(ctx, "读取验证码失败 req:%+v, err:%+v", req, err)
		return errs.ErrVerifyCode
	}
	if code != req.Code {
		_ = d.increaseVerifyCodeTryTime(ctx, req)
		return errs.ErrVerifyCode
	}
	_ = d.cleanVerifyCodeTryTime(ctx, req)
	return nil
}

// increaseVerifyCodeTryTime 增加验证码尝试次数记录，1小时有效期
func (d *dao) increaseVerifyCodeTryTime(ctx context.Context, req *model.CheckVerifyCodeReq) error {
	key := getSMSVerifyTryTimeKey(req.BizID, req.Mobile)
	conn, err := d.redis.Pipeline(ctx)
	if err != nil {
		log.ErrorContextf(ctx, "increaseVerifyCodeTryTime Redis pipeline error: %+v", err)
		return err
	}
	defer conn.Close()
	i, err := redis.Uint64(conn.Do("INCR", key))
	if err != nil {
		log.ErrorContextf(ctx, "increaseVerifyCodeTryTime incr error: %+v, key: %s", err, key)
		return err
	}
	log.DebugContextf(ctx, "Incr success result(%+v) isEqual(%v)", i, i == 1)
	if i == 1 {
		expire := config.App().LoginDefault.VerifyCode.MaxTryExpire.Seconds()
		if err = conn.Send("EXPIRE", key, uint64(expire)); err != nil {
			log.ErrorContextf(ctx, "increaseVerifyCodeTryTime set EXPIRE error: %+v, keyName: %s, timeout: %d",
				err, key, time.Hour)
			return err
		}
	}
	if err = conn.Flush(); err != nil {
		log.ErrorContextf(ctx, "increaseVerifyCodeTryTime Flush redis pipeline fail, error: %+v", err)
		return err
	}
	return nil
}

func (d *dao) cleanVerifyCodeTryTime(ctx context.Context, req *model.CheckVerifyCodeReq) error {
	key := getSMSVerifyTryTimeKey(req.BizID, req.Mobile)
	_, err := d.redis.Do(ctx, "DEL", key)
	if err != nil {
		log.WarnContextf(ctx, "清除验证码尝试次数失败, key: %s", key)
		return err
	}
	return nil
}

// getSmsTemplate 获取模版
func getSmsTemplate() (tpl *model.SmsTemplate) {
	return &model.SmsTemplate{
		Uin:         uint64(2355017619), // 接口人 @ttaylorli
		SignatureID: uint64(28),         // 短信签名id
		Type:        uint64(1),          // 三级分类：区分是内部短信还是外部短信 协同短信为1， 否则为营销短信
		Message:     "手机验证码 %s，请尽快完成操作。验证码将于5分钟后失效，若非本人操作，请忽略。",
		RecordFlag:  uint32(1), // 0 需要记录短信流水 1 不需要记录短信流水
		IsVerify:    1,
	}
}

// genSMSRateKey 生成验证码发送频控key
func genSMSRateKey(bizID, typeID uint32, identify string) string {
	typeStr := "user"
	if typeID == smsLimitTypeMobile {
		typeStr = "mobile"
	}
	return fmt.Sprintf(_smsRateKey, bizID, typeStr, identify)
}

func getSMSVerifyCacheKey(bizID uint32, mobile string) string {
	return fmt.Sprintf(_smsVerifyCacheKey, bizID, mobile)
}

func getSMSVerifyTryTimeKey(bizID uint32, mobile string) string {
	return fmt.Sprintf(_smsVerifyTryTimeKey, bizID, mobile)
}
