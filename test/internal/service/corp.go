package service

import (
	"context"

	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/errx"
	"git.woa.com/adp/common/x/gox/ptrx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	pm "git.woa.com/adp/pb-go/platform/platform_manager"
)

// checkSession 校验cookie
func (s *Service) checkSession(ctx context.Context) (*entity.Session, error) {
	md := contextx.Metadata(ctx)
	token := md.Token()
	loginUin := md.LoginUin()
	loginSubAccountUin := md.LoginSubAccountUin()

	si, err := s.rpc.PlatformAdmin.DescribeIntegrator(ctx, loginUin, loginSubAccountUin)
	// si, err := s.dao.GetSystemIntegrator(ctx, contextx.Metadata(ctx).Uin(), contextx.Metadata(ctx).SubAccountUin())
	if err != nil {
		return nil, err
	}
	if !si.IsCloudSI() && (loginUin == "" || loginSubAccountUin == "") {
		return nil, errs.ErrSystemIntegratorUserParams
	}
	if si.IsCloudSI() {
		loginUin = contextx.Metadata(ctx).Uin()
		loginSubAccountUin = contextx.Metadata(ctx).SubAccountUin()
	}
	var session *entity.Session
	if loginSubAccountUin != "" {
		session, err = s.getStaffCloudSession(ctx, si, loginUin, loginSubAccountUin)
	} else {
		session, err = s.dao.GetStaffSession(ctx, token)
	}
	if err != nil {
		return nil, err
	}
	if session == nil {
		return nil, errs.ErrSessionNotFound
	}
	if config.IsDemoModeOpen() && !config.CheckUserIDDemoMode(session.ID) {
		logx.W(ctx, "用户ID：%d 未开启演示模式", session.ID)
		return nil, errs.ErrSessionNotFound
	}
	return session, nil
}

// getStaffCloudSession 获取腾讯云session
func (s *Service) getStaffCloudSession(ctx context.Context, si *entity.SystemIntegrator, loginUin, loginSubAccountUin string) (*entity.Session, error) {
	user, err := s.userLogic.DescribeSIUser(ctx, si.ID, loginUin, loginSubAccountUin)
	if err != nil {
		return nil, err
	}
	if user == nil {
		logx.W(ctx, "获取用户信息为空 sid:%d loginUin:%s loginSubAccountUin:%s", si.ID, loginUin, loginSubAccountUin)
		return nil, nil
	}
	staffReq := pm.DescribeCorpStaffReq{
		Status: ptrx.Uint32(entity.StaffStatusValid),
		UserId: user.ID,
	}
	staff, err := s.rpc.PlatformAdmin.DescribeCorpStaff(ctx, &staffReq)
	// staff, err := d.GetStaffByUserID(ctx, user.ID)
	if err != nil {
		if errx.Is(err, errx.ErrNotFound) {
			logx.W(ctx, "获取员工信息为空 sid:%d loginUin:%s loginSubAccountUin:%s", si.ID, loginUin, loginSubAccountUin)
			return nil, nil
		}
		return nil, err
	}
	if staff == nil {
		logx.W(ctx, "获取员工信息为空 sid:%d loginUin:%s loginSubAccountUin:%s", si.ID, loginUin, loginSubAccountUin)
		return nil, nil
	}
	corp, err := s.rpc.PlatformAdmin.DescribeCorpByPrimaryId(ctx, staff.CorpID)
	// corp, err := d.GetCorpByID(ctx, staff.CorpPrimaryId)
	if err != nil {
		return nil, err
	}
	if corp == nil {
		logx.W(ctx, "获取企业信息为空 sid:%d loginUin:%s loginSubAccountUin:%s", si.ID, loginUin, loginSubAccountUin)
		return nil, nil
	}
	if corp.GetStatus() != entity.CorpStatusValid {
		return nil, errs.ErrCorpInValid
	}
	if corp.Uin != loginUin {
		return nil, errs.ErrUinNotMatch
	}
	if !staff.IsValid() || !user.IsValid() {
		return nil, errs.ErrStaffInValid
	}
	return &entity.Session{
		ID:            staff.ID,
		SID:           user.SID,
		UIN:           user.Uin,
		SubAccountUin: user.SubAccountUin,
		BizID:         staff.BusinessID,
		CorpID:        staff.CorpID,
		Cellphone:     staff.Cellphone,
		Status:        staff.Status,
		ExpireTime:    0,
	}, nil
}

// checkLogin 校验登陆态
func (s *Service) checkLogin(ctx context.Context) error {
	loginUserType := contextx.Metadata(ctx).LoginUserType()
	if loginUserType != entity.LoginUserExpType {
		// 如果普通用户，则是腾讯云登录，本身在底座就做了登录校验
		return nil
	}
	if _, err := s.checkSession(ctx); err != nil {
		return err
	}
	return nil
}
