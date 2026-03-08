package service

import (
	"context"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	utilConfig "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/util/config"
)

// CheckIsUsedCharSizeExceeded 检查已使用是否超出允许的字符了【企业维度和单应用维度】
func CheckIsUsedCharSizeExceeded(ctx context.Context, d dao.Dao, botBizID, corpID uint64) error {

	// 单应用维度字符超限
	botUsedCharSize, err := d.GetBotUsedCharSizeUsage(ctx, botBizID)
	if err != nil {
		return errs.ErrSystem
	}
	if botUsedCharSize > config.GetBotMaxCharSize(ctx, botBizID) {
		return errs.ErrBotOverCharacterSizeLimit
	}

	// 企业维度字符超限
	corp, err := d.GetCorpByID(ctx, corpID)
	if err != nil {
		return errs.ErrCorpNotFound
	}
	// 如果没有打开集成商字符数校验开关，就不需要对集成商做校验
	if !utilConfig.GetMainConfig().CheckSystemIntegratorCharacterUsage && d.IsSystemIntegrator(ctx, corp) {
		return nil
	}
	corp, err = d.GetCorpBillingInfo(ctx, corp)
	if err != nil {
		return errs.ErrCorpNotFound
	}
	usedCharSize, err := d.GetCorpUsedCharSizeUsage(ctx, corpID)
	if err != nil {
		return errs.ErrSystem
	}
	if corp.IsUsedCharSizeExceeded(int64(usedCharSize)) {
		return errs.ErrOverCharacterSizeLimit
	}
	return nil
}

// CheckIsCharSizeExceeded 检查字符是否超限
func CheckIsCharSizeExceeded(ctx context.Context, d dao.Dao, botBizID, corpID uint64, diff int64) error {
	// 应用维度字符限制
	botUsedCharSize, err := d.GetBotUsedCharSizeUsage(ctx, botBizID)
	if err != nil {
		return errs.ErrSystem
	}
	if diff > 0 && uint64(diff)+botUsedCharSize > config.GetBotMaxCharSize(ctx, botBizID) {
		return errs.ErrBotOverCharacterSizeLimit
	}

	// 企业维度字符限制
	corp, err := d.GetCorpByID(ctx, corpID)
	if err != nil {
		return errs.ErrCorpNotFound
	}
	// 如果没有打开集成商字符数校验开关，就不需要对集成商做校验
	if !utilConfig.GetMainConfig().CheckSystemIntegratorCharacterUsage && d.IsSystemIntegrator(ctx, corp) {
		return nil
	}
	corp, err = d.GetCorpBillingInfo(ctx, corp)
	if err != nil {
		return errs.ErrCorpNotFound
	}
	usedCharSize, err := d.GetCorpUsedCharSizeUsage(ctx, corpID)
	if err != nil {
		return errs.ErrSystem
	}
	if corp.IsCharSizeExceeded(int64(usedCharSize), diff) {
		return d.ConvertErrMsg(ctx, 0, corpID, errs.ErrOverCharacterSizeLimit)
	}
	return nil
}

// checkSession 校验cookie
func (s *Service) checkSession(ctx context.Context) (*model.Session, error) {
	token := pkg.Token(ctx)
	si, err := s.dao.GetSystemIntegrator(ctx, pkg.Uin(ctx), pkg.SubAccountUin(ctx))
	if err != nil {
		return nil, err
	}
	loginUin := pkg.LoginUin(ctx)
	loginSubAccountUin := pkg.LoginSubAccountUin(ctx)
	if !si.IsCloudSI() && (loginUin == "" || loginSubAccountUin == "") {
		return nil, errs.ErrSystemIntegratorUserParams
	}
	if si.IsCloudSI() {
		loginUin = pkg.Uin(ctx)
		loginSubAccountUin = pkg.SubAccountUin(ctx)
	}
	session, err := s.dao.GetStaffSession(ctx, si, token, loginUin, loginSubAccountUin)
	if err != nil {
		return nil, err
	}
	if session == nil {
		return nil, errs.ErrSessionNotFound
	}
	if config.IsDemoModeOpen() && !config.CheckUserIDDemoMode(session.ID) {
		log.WarnContextf(ctx, "用户ID：%d 未开启演示模式", session.ID)
		return nil, errs.ErrSessionNotFound
	}
	return session, nil
}

// checkLogin 校验登陆态
func (s *Service) checkLogin(ctx context.Context) error {
	loginUserType := pkg.LoginUserType(ctx)
	if loginUserType != model.LoginUserExpType {
		// 如果普通用户，则是腾讯云登录，本身在底座就做了登录校验
		return nil
	}
	if _, err := s.checkSession(ctx); err != nil {
		return err
	}
	return nil
}
