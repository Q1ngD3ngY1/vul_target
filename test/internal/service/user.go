package service

import (
	"context"
	"regexp"
	"time"

	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/gox/convx"
	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/utilx/validx"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
	"github.com/spf13/cast"
)

// CreateCustUser 新增用户
func (s *Service) CreateCustUser(ctx context.Context, req *pb.CreateCustUserReq) (
	rsp *pb.CreateCustUserRsp, err error) {
	logx.D(ctx, "CreateCustUser req:%+v", req)
	// 1.校验参数
	_, err = util.BatchCheckReqParamsIsUint64(ctx, append(req.GetRoleBizIds(), req.AppBizId))
	if err != nil {
		return nil, err
	}
	app, err := s.rpc.AppAdmin.DescribeAppById(ctx, convx.MustStringToUint64(req.GetAppBizId()))
	// app, err := s.dao.GetAppByAppBizID(ctx, cast.ToUint64(req.GetAppBizId()))
	if err != nil || app == nil {
		logx.E(ctx, "GetAppByAppBizID err:%v", err)
		return nil, errs.ErrGetAppFail
	}
	if err := s.checkCreateUser(ctx, req); err != nil {
		logx.E(ctx, "CreateCustUser checkSaveUsre err:%v,req:%+v", err, req)
		return nil, err
	}
	rsp = &pb.CreateCustUserRsp{}
	// 2.创建用户
	rsp.UserBizId, err = s.userLogic.CreateCustUser(ctx, convx.SliceStringToUint64(req.GetRoleBizIds()), &entity.CustUser{
		CorpID:      contextx.Metadata(ctx).CorpBizID(),
		AppID:       cast.ToUint64(req.GetAppBizId()),
		Name:        req.GetName(),
		ThirdUserID: req.GetThirdUserId(),
		IsDeleted:   false,
		CreateTime:  time.Now(),
		UpdateTime:  time.Now(),
	})
	if err != nil {
		logx.E(ctx, "CreateCustUser CreateCustUser err:%v,req:%+v", err, req)
		return nil, err
	}
	return rsp, nil
}

func (s *Service) checkCreateUser(ctx context.Context, req *pb.CreateCustUserReq) (err error) {
	corpBizId, appBizId := contextx.Metadata(ctx).CorpBizID(), cast.ToUint64(req.GetAppBizId())
	roleBizIds := make([]uint64, 0, len(req.GetRoleBizIds()))
	for _, v := range req.GetRoleBizIds() {
		roleBizIds = append(roleBizIds, cast.ToUint64(v))
	}
	// 1.1 名字长度校验
	if len([]rune(req.GetName())) < config.GetMainConfig().Permissions.UserNameMinLimit ||
		len([]rune(req.GetName())) > config.GetMainConfig().Permissions.UserNameMaxLimit {
		return errs.ErrCustUserNameFail
	}
	// 2.1 third_user_id长度校验
	if len(req.GetThirdUserId()) == 0 ||
		len(req.GetThirdUserId()) > config.GetMainConfig().Permissions.ThirdUserIdMaxLimit {
		return errs.ErrThirdUserIdFail
	}
	// 2.2 third_user_id正则校验
	re := regexp.MustCompile(config.GetMainConfig().Permissions.ThirdUserIdReg)
	if !re.MatchString(req.GetThirdUserId()) {
		return errs.ErrThirdUserIdFail
	}
	// 2.3 third_user_id不能重复
	count, err := s.userLogic.DescribeUserCount(ctx, corpBizId, appBizId,
		&entity.CustUserFilter{
			ThirdUserID: req.GetThirdUserId(),
		})
	if err != nil {
		logx.E(ctx, "checkCreateUser GetUserCount err:%v,req:%+v", err, req)
		return errs.ErrCommonFail // 返回一个通用报错
	}
	if count > 0 {
		return errs.ErrLkeUserIdExist
	}
	// 3. 校验角色业务id，并且过滤不存在的角色业务id
	req.RoleBizIds, err = s.checkUserRole(ctx, corpBizId, appBizId, roleBizIds)
	if err != nil {
		return err
	}
	// 4. 校验用户最大数量
	count, err = s.userLogic.DescribeUserCount(ctx, corpBizId, appBizId, nil)
	if err != nil {
		logx.E(ctx, "checkCreateUser GetUserCount err:%v,req:%+v", err, req)
		return errs.ErrCommonFail // 返回一个通用报错
	}
	if count+1 > int64(config.GetMainConfig().Permissions.UserMaxLimit) {
		return errs.ErrUserMaxLimit
	}
	return nil
}

func (s *Service) checkUserRole(ctx context.Context, corpBizId, appBizId uint64,
	roleBizIds []uint64) (bizIds []string, err error) {
	// 1.角色数组不能为空
	if len(roleBizIds) == 0 {
		return nil, errs.ErrUserRoleEmpty
	}
	// 2.过滤不存在的角色
	_, roleList, err := s.userLogic.DescribeKnowledgeRoleList(ctx, corpBizId, appBizId,
		&entity.KnowledgeRoleFilter{
			BizIDs: roleBizIds,
			Limit:  len(roleBizIds),
		})
	if err != nil {
		logx.E(ctx, "checkUserRole GetRoleList err:%v,corpBizId:%v,appBizId:%v,roleBizIds:%v",
			corpBizId, appBizId, roleBizIds)
		return nil, errs.ErrCommonFail
	}
	// 如果角色过滤后为空，直接报错
	if len(roleList) == 0 {
		return nil, errs.ErrUserRoleEmpty
	}
	bizIds = make([]string, 0, len(roleList))
	for _, v := range roleList {
		bizIds = append(bizIds, cast.ToString(v.BusinessID))
	}
	return bizIds, nil
}

// ModifyCustUser 编辑用户
func (s *Service) ModifyCustUser(ctx context.Context, req *pb.ModifyCustUserReq) (
	rsp *pb.ModifyCustUserRsp, err error) {
	logx.D(ctx, "ModifyCustUser req:%+v", req)
	// 1.校验参数
	_, err = util.BatchCheckReqParamsIsUint64(ctx, append(req.GetRoleBizIds(), []string{
		req.GetAppBizId(), req.GetUserBizId()}...))
	if err != nil {
		return nil, err
	}
	app, err := s.rpc.AppAdmin.DescribeAppById(ctx, convx.MustStringToUint64(req.GetAppBizId()))
	// app, err := s.dao.GetAppByAppBizID(ctx, cast.ToUint64(req.GetAppBizId()))
	if err != nil || app == nil {
		logx.E(ctx, "GetAppByAppBizID err:%v", err)
		return nil, errs.ErrGetAppFail
	}
	if err := s.checkEditUser(ctx, req); err != nil {
		logx.E(ctx, "ModifyCustUser checkEditUser err:%v,req:%+v", err, req)
		return nil, err
	}
	logx.D(ctx, "ModifyCustUser req:%+v", req)
	// 2.创建用户
	err = s.userLogic.ModifyCustUser(ctx, convx.SliceStringToUint64(req.GetRoleBizIds()),
		&entity.CustUser{
			AppID:       cast.ToUint64(req.GetAppBizId()),
			BusinessID:  cast.ToUint64(req.GetUserBizId()),
			Name:        req.GetName(),
			ThirdUserID: req.GetThirdUserId(),
		})
	if err != nil {
		logx.E(ctx, "ModifyCustUser UpdateCustUser err:%v,req:%+v", err, req)
		return nil, err
	}
	rsp = &pb.ModifyCustUserRsp{}
	return rsp, nil
}

func (s *Service) checkEditUser(ctx context.Context, req *pb.ModifyCustUserReq) (err error) {
	// 1.1 biz_id不能为空
	if req.GetUserBizId() == "" {
		return errs.ErrUserNotExist
	}
	corpBizId, appBizId, userBizId := contextx.Metadata(ctx).CorpBizID(), cast.ToUint64(req.GetAppBizId()),
		cast.ToUint64(req.GetUserBizId())
	roleBizIds := make([]uint64, 0, len(req.GetRoleBizIds()))
	for _, v := range req.GetRoleBizIds() {
		roleBizIds = append(roleBizIds, cast.ToUint64(v))
	}
	// 1.2 用户是否存在
	userList, err := s.userLogic.DescribeUserList(ctx, corpBizId, appBizId,
		&entity.CustUserFilter{
			BizIDs: []uint64{userBizId},
			Limit:  1,
		})
	if err != nil {
		logx.E(ctx, "checkEditUser GetUserList err:%v,req:%+v", err, req)
		return errs.ErrCommonFail
	}
	if len(userList) == 0 { // 用户不存在
		return errs.ErrUserNotExist
	}
	// 2.1 名字长度校验
	if len([]rune(req.GetName())) < config.GetMainConfig().Permissions.UserNameMinLimit ||
		len([]rune(req.GetName())) > config.GetMainConfig().Permissions.UserNameMaxLimit {
		return errs.ErrCustUserNameFail
	}
	// 3.1 third_user_id长度校验
	if len(req.GetThirdUserId()) == 0 ||
		len(req.GetThirdUserId()) > config.GetMainConfig().Permissions.ThirdUserIdMaxLimit {
		return errs.ErrThirdUserIdFail
	}
	// 3.2 third_user_id正则校验
	re := regexp.MustCompile(config.GetMainConfig().Permissions.ThirdUserIdReg)
	if !re.MatchString(req.GetThirdUserId()) {
		return errs.ErrThirdUserIdFail
	}
	// 3.3 third_user_id不能重复
	if userList[0].ThirdUserID != req.GetThirdUserId() {
		count, err := s.userLogic.DescribeUserCount(ctx, corpBizId, appBizId,
			&entity.CustUserFilter{
				ThirdUserID: req.GetThirdUserId(),
			})
		if err != nil {
			logx.E(ctx, "checkEditUser GetUserCount err:%v,req:%+v", err, req)
			return errs.ErrCommonFail
		}
		if count > 0 {
			return errs.ErrLkeUserIdExist
		}
	}
	// 4. 校验角色业务id，并且过滤不存在的角色业务id
	req.RoleBizIds, err = s.checkUserRole(ctx, corpBizId, appBizId, roleBizIds)
	if err != nil {
		return err
	}
	return nil
}

// BatchModifyUser 批量编辑用户
func (s *Service) BatchModifyUser(ctx context.Context, req *pb.BatchModifyUserReq) (
	rsp *pb.BatchModifyUserRsp, err error) {
	logx.D(ctx, "BatchModifyUser req:%+v", req)
	rsp = &pb.BatchModifyUserRsp{}
	// 1.校验参数
	params := []string{req.AppBizId}
	params = append(params, req.GetUserBizIds()...)
	params = append(params, req.GetRoleBizIds()...)
	_, err = util.BatchCheckReqParamsIsUint64(ctx, params)
	if err != nil {
		return nil, err
	}
	app, err := s.rpc.AppAdmin.DescribeAppById(ctx, convx.MustStringToUint64(req.GetAppBizId()))
	// app, err := s.dao.GetAppByAppBizID(ctx, cast.ToUint64(req.GetAppBizId()))
	if err != nil || app == nil {
		logx.E(ctx, "GetAppByAppBizID err:%v", err)
		return nil, errs.ErrGetAppFail
	}
	userList, err := s.checkBatchEditUser(ctx, req)
	if err != nil {
		logx.E(ctx, "EditCustUser checkEditUser err:%v,req:%+v", err, req)
		return nil, err
	}
	logx.D(ctx, "BatchModifyUser req:%+v", req)
	// 2.更新角色绑定关系
	roleBizIds := convx.SliceStringToUint64(req.GetRoleBizIds())
	err = s.userLogic.ModifyUserRoleList(ctx, contextx.Metadata(ctx).CorpBizID(), cast.ToUint64(req.GetAppBizId()),
		roleBizIds, userList, nil)
	if err != nil {
		logx.E(ctx, "BatchModifyUser UpdateUserRole err:%v,req:%+v", err, req)
		return nil, err
	}
	return rsp, nil
}

func (s *Service) checkBatchEditUser(ctx context.Context, req *pb.BatchModifyUserReq) (
	userList []*entity.CustUser, err error) {
	// 1.1 biz_id数组不能为空
	if len(req.GetUserBizIds()) == 0 {
		return nil, errs.ErrBatchEditUserId
	}
	corpBizID, appBizID := contextx.Metadata(ctx).CorpBizID(), cast.ToUint64(req.GetAppBizId())
	userBizIDs := convx.SliceStringToUint64(req.GetUserBizIds())
	roleBizIDs := convx.SliceStringToUint64(req.GetRoleBizIds())
	// 1.2 用户是否存在
	userList, err = s.userLogic.DescribeUserList(ctx, corpBizID, appBizID,
		&entity.CustUserFilter{
			BizIDs: userBizIDs,
			Limit:  len(userBizIDs),
		})
	if err != nil {
		logx.E(ctx, "checkEditUser GetUserList err:%v,req:%+v", err, req)
		return nil, errs.ErrCommonFail
	}
	if len(userList) == 0 { // 用户不存在
		return nil, errs.ErrUserNotExist
	}
	newUserBizIDs := make([]string, 0, len(req.GetUserBizIds()))
	for _, v := range userList {
		newUserBizIDs = append(newUserBizIDs, cast.ToString(v.BusinessID))
	}
	req.UserBizIds = newUserBizIDs // 过滤不存在的用户业务id
	// 2. 校验角色业务id，并且过滤不存在的角色业务id
	req.RoleBizIds, err = s.checkUserRole(ctx, corpBizID, appBizID, roleBizIDs)
	if err != nil {
		return nil, err
	}
	return userList, nil
}

func custUserWithRoleDO2PB(user *entity.CustUserWithRoleInfo) *pb.CustUserInfo {
	userPB := &pb.CustUserInfo{
		AppBizId:    user.AppID,
		UserBizId:   user.BusinessID,
		Name:        user.Name,
		ThirdUserId: user.ThirdUserID,
		IsDeleted:   0,
		CreateTime:  user.CreateTime,
		UpdateTime:  user.UpdateTime,
		RoleList:    make([]*pb.RoleBaseInfo, 0, len(user.RoleList)),
	}
	for _, v := range user.RoleList {
		userPB.RoleList = append(userPB.RoleList, &pb.RoleBaseInfo{
			RoleBizId: v.BusinessID,
			RoleName:  v.Name,
		})
	}
	return userPB
}

func custUserWithRolesDO2PB(users []*entity.CustUserWithRoleInfo) []*pb.CustUserInfo {
	return slicex.Map(users, func(user *entity.CustUserWithRoleInfo) *pb.CustUserInfo {
		return custUserWithRoleDO2PB(user)
	})
}

// ListCustUser 获取用户列表
func (s *Service) ListCustUser(ctx context.Context, req *pb.ListCustUserReq) (
	rsp *pb.ListCustUserRsp, err error) {
	logx.D(ctx, "ListCustUser req:%+v", req)
	rsp = &pb.ListCustUserRsp{}
	// 1.校验参数
	params := []string{req.AppBizId}
	params = append(params, req.GetUserBizIds()...)
	_, err = util.BatchCheckReqParamsIsUint64(ctx, params)
	if err != nil {
		return nil, err
	}
	app, err := s.rpc.AppAdmin.DescribeAppById(ctx, convx.MustStringToUint64(req.GetAppBizId()))
	// app, err := s.dao.GetAppByAppBizID(ctx, cast.ToUint64(req.GetAppBizId()))
	if err != nil || app == nil {
		logx.E(ctx, "GetAppByAppBizID err:%v", err)
		return nil, errs.ErrGetAppFail
	}
	userBizIDs := convx.SliceStringToUint64(req.GetUserBizIds())
	offset := int(0)
	if req.GetPageNumber() > 0 {
		offset = int((req.GetPageNumber() - 1) * req.GetPageSize())
	}
	var userWithRoleList []*entity.CustUserWithRoleInfo
	userWithRoleList, rsp.Total, err = s.userLogic.DescribeUserWithRoleList(ctx,
		contextx.Metadata(ctx).CorpBizID(),
		cast.ToUint64(req.GetAppBizId()),
		&entity.CustUserFilter{
			Offset:                offset,
			Limit:                 int(req.GetPageSize()),
			OrderByModifyTimeDesc: true,
			BizIDs:                userBizIDs,
			ThirdUserIDLike:       req.GetThirdUserId(),
			Query:                 req.GetQuery(),
		})
	rsp.UserList = custUserWithRolesDO2PB(userWithRoleList)
	if err != nil {
		logx.E(ctx, "ListCustUser GetUserList err:%v,req:%+v", err, req)
		return nil, err
	}
	logx.D(ctx, "ListCustUser rsp:%+v", rsp)
	return rsp, nil
}

// DescribeCustUser 获取用户详情
func (s *Service) DescribeCustUser(ctx context.Context, req *pb.DescribeCustUserReq) (
	rsp *pb.DescribeCustUserRsp, err error) {
	logx.D(ctx, "DescribeCustUser req:%+v", req)
	rsp = &pb.DescribeCustUserRsp{}
	// 1.校验参数
	if req.GetUserBizId() == "" {
		return nil, errs.ErrUserNotExist
	}
	_, err = util.BatchCheckReqParamsIsUint64(ctx, []string{req.AppBizId, req.UserBizId})
	if err != nil {
		return nil, err
	}
	app, err := s.rpc.AppAdmin.DescribeAppById(ctx, convx.MustStringToUint64(req.GetAppBizId()))
	// app, err := s.dao.GetAppByAppBizID(ctx, cast.ToUint64(req.GetAppBizId()))
	if err != nil || app == nil {
		logx.E(ctx, "GetAppByAppBizID err:%v", err)
		return nil, errs.ErrGetAppFail
	}
	userList, count, err := s.userLogic.DescribeUserWithRoleList(ctx,
		contextx.Metadata(ctx).CorpBizID(),
		cast.ToUint64(req.GetAppBizId()),
		&entity.CustUserFilter{
			BizIDs: []uint64{cast.ToUint64(req.GetUserBizId())},
			Limit:  1,
			Offset: 0,
		})
	if err != nil {
		logx.E(ctx, "DetailCustUser GetUserList err:%v,req:%+v", err, req)
		return nil, errs.ErrDetailUserFail
	}
	if count == 0 {
		return nil, errs.ErrUserNotExist
	}
	rsp.UserInfo = custUserWithRoleDO2PB(userList[0])
	return rsp, err
}

// DeleteCustUser 删除用户
func (s *Service) DeleteCustUser(ctx context.Context, req *pb.DeleteCustUserReq) (
	rsp *pb.DeleteCustUserRsp, err error) {
	logx.D(ctx, "DeleteCustUser req:%+v", req)
	rsp = &pb.DeleteCustUserRsp{}
	// 1.校验参数
	params := []string{req.AppBizId}
	params = append(params, req.GetUserBizIds()...)
	_, err = util.BatchCheckReqParamsIsUint64(ctx, params)
	if err != nil {
		return nil, err
	}
	app, err := s.rpc.AppAdmin.DescribeAppById(ctx, convx.MustStringToUint64(req.GetAppBizId()))
	// app, err := s.dao.GetAppByAppBizID(ctx, cast.ToUint64(req.GetAppBizId()))
	if err != nil || app == nil {
		logx.E(ctx, "GetAppByAppBizID err:%v", err)
		return nil, errs.ErrGetAppFail
	}
	err = s.userLogic.DeleteCustUser(ctx, cast.ToUint64(req.GetAppBizId()), convx.SliceStringToUint64(req.GetUserBizIds()))
	if err != nil {
		logx.E(ctx, "DeleteCustUser err:%v,req:%+v", err, req)
		return nil, err
	}
	for _, v := range req.GetUserBizIds() {
		rsp.DeleteUserBizIds = append(rsp.DeleteUserBizIds, cast.ToUint64(v))
	}
	return rsp, nil
}

// SetCustUserConfig 设置特殊权限配置
func (s *Service) SetCustUserConfig(ctx context.Context, req *pb.SetCustUserConfigReq) (
	rsp *pb.SetCustUserConfigRsp, err error) {
	logx.D(ctx, "SetCustUserConfig req:%+v", req)
	rsp = &pb.SetCustUserConfigRsp{}
	// 1.校验参数
	_, err = util.BatchCheckReqParamsIsUint64(ctx, []string{req.GetAppBizId(),
		req.GetNotSetRoleBizId(), req.GetNotUseRoleBizId()})
	if err != nil {
		return nil, err
	}
	app, err := s.rpc.AppAdmin.DescribeAppById(ctx, convx.MustStringToUint64(req.GetAppBizId()))
	// app, err := s.dao.GetAppByAppBizID(ctx, cast.ToUint64(req.GetAppBizId()))
	if err != nil || app == nil {
		logx.E(ctx, "GetAppByAppBizID err:%v", err)
		return nil, errs.ErrGetAppFail
	}
	// 2.校验下角色业务id是否存在
	corpBizId, appBizId, notSetRoleBizId, notUseRoleBizId := contextx.Metadata(ctx).CorpBizID(), cast.ToUint64(req.GetAppBizId()),
		cast.ToUint64(req.GetNotSetRoleBizId()), cast.ToUint64(req.GetNotUseRoleBizId())
	roleList := make([]uint64, 0, 2)
	if notSetRoleBizId != entity.NotRoleBizId { // 是不检索,无需过滤角色业务id
		roleList = append(roleList, cast.ToUint64(notSetRoleBizId))
	}
	if notUseRoleBizId != entity.NotRoleBizId && notUseRoleBizId != notSetRoleBizId {
		roleList = append(roleList, cast.ToUint64(notUseRoleBizId))
	}
	if len(roleList) > 0 { // 没有选自定义角色无需过滤
		bizIds, err := s.checkUserRole(ctx, corpBizId, appBizId, roleList)
		if err != nil || len(roleList) != len(bizIds) {
			logx.E(ctx, "SetCustUserConfig checkUserRole err:%v,bizIds:%v,roleList:%v", err, bizIds, roleList)
			return nil, errs.ErrSetCustUserConfigRoleFail
		}
	}
	err = s.userLogic.ModifyCustUserConfig(ctx, corpBizId, appBizId, notSetRoleBizId, notUseRoleBizId)
	if err != nil {
		logx.E(ctx, "SetCustUserConfig err:%v,req:%v", err, req)
		return nil, err
	}
	return rsp, nil
}

func custUserConfigDO2PB(custConfig *entity.CustUserConfig) *pb.CustUserConfig {
	return &pb.CustUserConfig{
		NotUserIdRoleInfo: &pb.RoleBaseInfo{
			RoleBizId: custConfig.NotUserIdRoleInfo.BusinessID,
			RoleName:  custConfig.NotUserIdRoleInfo.Name,
		},
		NotUseRoleInfo: &pb.RoleBaseInfo{
			RoleBizId: custConfig.NotUseRoleInfo.BusinessID,
			RoleName:  custConfig.NotUseRoleInfo.Name,
		},
	}
}

// GetCustUserConfig 获取特殊权限配置
func (s *Service) GetCustUserConfig(ctx context.Context, req *pb.GetCustUserConfigReq) (
	rsp *pb.GetCustUserConfigRsp, err error) {
	rsp = new(pb.GetCustUserConfigRsp)
	logx.D(ctx, "GetCustUserConfig req:%+v", req)
	// 1.校验参数
	_, err = util.BatchCheckReqParamsIsUint64(ctx, []string{req.GetAppBizId()})
	if err != nil {
		return nil, err
	}
	appBizID := cast.ToUint64(req.GetAppBizId())
	app, err := s.rpc.AppAdmin.DescribeAppById(ctx, appBizID)
	// app, err := s.dao.GetAppByAppBizID(ctx, appBizID)
	if err != nil || app == nil {
		logx.E(ctx, "GetAppByAppBizID err:%v", err)
		return nil, errs.ErrGetAppFail
	}
	userConfig, err := s.userLogic.DescribeCustUserConfig(ctx, appBizID)
	if err != nil {
		logx.E(ctx, "GetCustUserConfig err:%v,req:%v", err, req)
		return nil, err
	}
	rsp.UserConfig = custUserConfigDO2PB(userConfig)
	return rsp, nil
}

// GetAclConfigStatus 获取应用端权限配置状态
func (s *Service) GetAclConfigStatus(ctx context.Context, req *pb.GetAclConfigStatusReq) (rsp *pb.GetAclConfigStatusRsp, err error) {
	logx.D(ctx, "GetAclConfigStatus req:%+v", req)
	rsp = &pb.GetAclConfigStatusRsp{}
	appid, err := validx.CheckAndParseUint64(req.GetAppBizId())
	if err != nil {
		return nil, err
	}
	rsp.Status, err = s.userLogic.DescribeAclConfigStatus(ctx, appid)
	if err != nil {
		logx.E(ctx, "GetAclConfigStatus err:%v,req:%v", err, req)
		return nil, err
	}
	return rsp, nil
}
