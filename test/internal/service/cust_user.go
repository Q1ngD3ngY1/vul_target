package service

import (
	"context"
	"regexp"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	utilConfig "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/util/config"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
	"github.com/spf13/cast"
)

// CreateCustUser 新增用户
func (s *Service) CreateCustUser(ctx context.Context, req *pb.CreateCustUserReq) (
	rsp *pb.CreateCustUserRsp, err error) {
	log.DebugContextf(ctx, "CreateCustUser req:%+v", req)
	//1.校验参数
	_, err = util.BatchCheckReqParamsIsUint64(ctx, append(req.GetRoleBizIds(), req.AppBizId))
	if err != nil {
		return nil, err
	}
	app, err := s.dao.GetAppByAppBizID(ctx, cast.ToUint64(req.GetAppBizId()))
	if err != nil || app == nil {
		log.ErrorContextf(ctx, "GetAppByAppBizID err:%v", err)
		return nil, errs.ErrGetAppFail
	}
	if err := s.checkCreateUser(ctx, req); err != nil {
		log.ErrorContextf(ctx, "CreateCustUser checkSaveUsre err:%v,req:%+v", err, req)
		return nil, err
	}
	log.DebugContextf(ctx, "CreateCustUser req:%+v", req)
	//2.创建用户
	rsp, err = s.permisLogic.CreateCustUser(ctx, req)
	if err != nil {
		log.ErrorContextf(ctx, "CreateCustUser CreateCustUser err:%v,req:%+v", err, req)
		return nil, err
	}
	return rsp, nil
}

func (s *Service) checkCreateUser(ctx context.Context, req *pb.CreateCustUserReq) (err error) {
	corpBizId, appBizId := pkg.CorpBizID(ctx), cast.ToUint64(req.GetAppBizId())
	roleBizIds := make([]uint64, 0, len(req.GetRoleBizIds()))
	for _, v := range req.GetRoleBizIds() {
		roleBizIds = append(roleBizIds, cast.ToUint64(v))
	}
	//1.1 名字长度校验
	if len([]rune(req.GetName())) < utilConfig.GetMainConfig().Permissions.UserNameMinLimit ||
		len([]rune(req.GetName())) > utilConfig.GetMainConfig().Permissions.UserNameMaxLimit {
		return errs.ErrCustUserNameFail
	}
	//2.1 third_user_id长度校验
	if len(req.GetThirdUserId()) == 0 ||
		len(req.GetThirdUserId()) > utilConfig.GetMainConfig().Permissions.ThirdUserIdMaxLimit {
		return errs.ErrThirdUserIdFail
	}
	//2.2 third_user_id正则校验
	re := regexp.MustCompile(utilConfig.GetMainConfig().Permissions.ThirdUserIdReg)
	if !re.MatchString(req.GetThirdUserId()) {
		return errs.ErrThirdUserIdFail
	}
	//2.3 third_user_id不能重复
	count, err := dao.GetCustUserDao(nil).GetUserCount(ctx, &dao.CustUserFilter{
		KnowledgeBase: dao.KnowledgeBase{
			CorpBizID: corpBizId,
			AppBizID:  appBizId,
			IsDeleted: pkg.GetIntPtr(0),
		},
		ThirdUserId: req.GetThirdUserId(),
	})
	if err != nil {
		log.ErrorContextf(ctx, "checkCreateUser GetUserCount err:%v,req:%+v", err, req)
		return errs.ErrCommonFail //返回一个通用报错
	}
	if count > 0 {
		return errs.ErrLkeUserIdExist
	}
	//3. 校验角色业务id，并且过滤不存在的角色业务id
	req.RoleBizIds, err = s.checkUserRole(ctx, corpBizId, appBizId, roleBizIds)
	if err != nil {
		return err
	}
	//4. 校验用户最大数量
	count, err = dao.GetCustUserDao(nil).GetUserCount(ctx, &dao.CustUserFilter{
		KnowledgeBase: dao.KnowledgeBase{
			CorpBizID: corpBizId,
			AppBizID:  appBizId,
			IsDeleted: pkg.GetIntPtr(0),
		},
	})
	if err != nil {
		log.ErrorContextf(ctx, "checkCreateUser GetUserCount err:%v,req:%+v", err, req)
		return errs.ErrCommonFail //返回一个通用报错
	}
	if count+1 > int64(utilConfig.GetMainConfig().Permissions.UserMaxLimit) {
		return errs.ErrUserMaxLimit
	}
	return nil
}

func (s *Service) checkUserRole(ctx context.Context, corpBizId, appBizId uint64,
	roleBizIds []uint64) (bizIds []string, err error) {
	//1.角色数组不能为空
	if len(roleBizIds) == 0 {
		return nil, errs.ErrUserRoleEmpty
	}
	//2.过滤不存在的角色
	roleList, err := dao.GetRoleDao(nil).GetRoleList(ctx, []string{model.ColumnBusinessID}, &dao.KnowledgeRoleReq{
		KnowledgeBase: dao.KnowledgeBase{
			CorpBizID: corpBizId,
			AppBizID:  appBizId,
			IsDeleted: pkg.GetIntPtr(0),
			Limit:     len(roleBizIds),
		},
		BizIDs: roleBizIds,
	})
	if err != nil {
		log.ErrorContextf(ctx, "checkUserRole GetRoleList err:%v,corpBizId:%v,appBizId:%v,roleBizIds:%v",
			corpBizId, appBizId, roleBizIds)
		return nil, errs.ErrCommonFail
	}
	//如果角色过滤后为空，直接报错
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
	log.DebugContextf(ctx, "ModifyCustUser req:%+v", req)
	//1.校验参数
	_, err = util.BatchCheckReqParamsIsUint64(ctx, append(req.GetRoleBizIds(), []string{
		req.GetAppBizId(), req.GetUserBizId()}...))
	if err != nil {
		return nil, err
	}
	app, err := s.dao.GetAppByAppBizID(ctx, cast.ToUint64(req.GetAppBizId()))
	if err != nil || app == nil {
		log.ErrorContextf(ctx, "GetAppByAppBizID err:%v", err)
		return nil, errs.ErrGetAppFail
	}
	if err := s.checkEditUser(ctx, req); err != nil {
		log.ErrorContextf(ctx, "ModifyCustUser checkEditUser err:%v,req:%+v", err, req)
		return nil, err
	}
	log.DebugContextf(ctx, "ModifyCustUser req:%+v", req)
	//2.创建用户
	rsp, err = s.permisLogic.UpdateCustUser(ctx, req)
	if err != nil {
		log.ErrorContextf(ctx, "ModifyCustUser UpdateCustUser err:%v,req:%+v", err, req)
		return nil, err
	}
	return rsp, nil
}

func (s *Service) checkEditUser(ctx context.Context, req *pb.ModifyCustUserReq) (err error) {
	//1.1 biz_id不能为空
	if req.GetUserBizId() == "" {
		return errs.ErrUserNotExist
	}
	corpBizId, appBizId, userBizId := pkg.CorpBizID(ctx), cast.ToUint64(req.GetAppBizId()),
		cast.ToUint64(req.GetUserBizId())
	roleBizIds := make([]uint64, 0, len(req.GetRoleBizIds()))
	for _, v := range req.GetRoleBizIds() {
		roleBizIds = append(roleBizIds, cast.ToUint64(v))
	}
	//1.2 用户是否存在
	userList, err := dao.GetCustUserDao(nil).GetUserList(ctx,
		[]string{model.UserTblColId, model.UserTblColName, model.UserTblColThirdUserId}, &dao.CustUserFilter{
			KnowledgeBase: dao.KnowledgeBase{
				CorpBizID: corpBizId,
				AppBizID:  appBizId,
				IsDeleted: pkg.GetIntPtr(0),
				Limit:     1,
			},
			BusinessIds: []uint64{userBizId},
		})
	if err != nil {
		log.ErrorContextf(ctx, "checkEditUser GetUserList err:%v,req:%+v", err, req)
		return errs.ErrCommonFail
	}
	if len(userList) == 0 { //用户不存在
		return errs.ErrUserNotExist
	}
	//2.1 名字长度校验
	if len([]rune(req.GetName())) < utilConfig.GetMainConfig().Permissions.UserNameMinLimit ||
		len([]rune(req.GetName())) > utilConfig.GetMainConfig().Permissions.UserNameMaxLimit {
		return errs.ErrCustUserNameFail
	}
	//3.1 third_user_id长度校验
	if len(req.GetThirdUserId()) == 0 ||
		len(req.GetThirdUserId()) > utilConfig.GetMainConfig().Permissions.ThirdUserIdMaxLimit {
		return errs.ErrThirdUserIdFail
	}
	//3.2 third_user_id正则校验
	re := regexp.MustCompile(utilConfig.GetMainConfig().Permissions.ThirdUserIdReg)
	if !re.MatchString(req.GetThirdUserId()) {
		return errs.ErrThirdUserIdFail
	}
	//3.3 third_user_id不能重复
	if userList[0].ThirdUserID != req.GetThirdUserId() {
		count, err := dao.GetCustUserDao(nil).GetUserCount(ctx, &dao.CustUserFilter{
			KnowledgeBase: dao.KnowledgeBase{
				CorpBizID: corpBizId,
				AppBizID:  appBizId,
				IsDeleted: pkg.GetIntPtr(0),
			},
			ThirdUserId: req.GetThirdUserId(),
		})
		if err != nil {
			log.ErrorContextf(ctx, "checkEditUser GetUserCount err:%v,req:%+v", err, req)
			return errs.ErrCommonFail
		}
		if count > 0 {
			return errs.ErrLkeUserIdExist
		}
	}
	//4. 校验角色业务id，并且过滤不存在的角色业务id
	req.RoleBizIds, err = s.checkUserRole(ctx, corpBizId, appBizId, roleBizIds)
	if err != nil {
		return err
	}
	return nil
}

// BatchModifyUser 批量编辑用户
func (s *Service) BatchModifyUser(ctx context.Context, req *pb.BatchModifyUserReq) (
	rsp *pb.BatchModifyUserRsp, err error) {
	log.DebugContextf(ctx, "BatchModifyUser req:%+v", req)
	rsp = &pb.BatchModifyUserRsp{}
	//1.校验参数
	params := []string{req.AppBizId}
	params = append(params, req.GetUserBizIds()...)
	params = append(params, req.GetRoleBizIds()...)
	_, err = util.BatchCheckReqParamsIsUint64(ctx, params)
	if err != nil {
		return nil, err
	}
	app, err := s.dao.GetAppByAppBizID(ctx, cast.ToUint64(req.GetAppBizId()))
	if err != nil || app == nil {
		log.ErrorContextf(ctx, "GetAppByAppBizID err:%v", err)
		return nil, errs.ErrGetAppFail
	}
	userList, err := s.checkBatchEditUser(ctx, req)
	if err != nil {
		log.ErrorContextf(ctx, "EditCustUser checkEditUser err:%v,req:%+v", err, req)
		return nil, err
	}
	log.DebugContextf(ctx, "BatchModifyUser req:%+v", req)
	//2.更新角色绑定关系
	roleBizIds := make([]uint64, 0, len(req.GetRoleBizIds()))
	for _, v := range req.GetRoleBizIds() {
		roleBizIds = append(roleBizIds, cast.ToUint64(v))
	}
	err = s.permisLogic.UpdateUserRole(ctx, pkg.CorpBizID(ctx), cast.ToUint64(req.GetAppBizId()),
		roleBizIds, userList, nil)
	if err != nil {
		log.ErrorContextf(ctx, "BatchModifyUser UpdateUserRole err:%v,req:%+v", err, req)
		return nil, err
	}
	return rsp, nil
}

func (s *Service) checkBatchEditUser(ctx context.Context, req *pb.BatchModifyUserReq) (
	userList []*model.CustUser, err error) {
	//1.1 biz_id数组不能为空
	if len(req.GetUserBizIds()) == 0 {
		return nil, errs.ErrBatchEditUserId
	}
	corpBizId, appBizId := pkg.CorpBizID(ctx), cast.ToUint64(req.GetAppBizId())
	userBizIds, roleBizIds := make([]uint64, 0, len(req.GetUserBizIds())), make([]uint64, 0, len(req.GetRoleBizIds()))
	for _, v := range req.GetUserBizIds() {
		userBizIds = append(userBizIds, cast.ToUint64(v))
	}
	for _, v := range req.GetRoleBizIds() {
		roleBizIds = append(roleBizIds, cast.ToUint64(v))
	}
	//1.2 用户是否存在
	userList, err = dao.GetCustUserDao(nil).GetUserList(ctx,
		[]string{model.UserTblColBusinessId, model.UserTblColThirdUserId}, &dao.CustUserFilter{
			KnowledgeBase: dao.KnowledgeBase{
				CorpBizID: corpBizId,
				AppBizID:  appBizId,
				IsDeleted: pkg.GetIntPtr(0),
				Limit:     len(userBizIds),
			},
			BusinessIds: userBizIds,
		})
	if err != nil {
		log.ErrorContextf(ctx, "checkEditUser GetUserList err:%v,req:%+v", err, req)
		return nil, errs.ErrCommonFail
	}
	if len(userList) == 0 { //用户不存在
		return nil, errs.ErrUserNotExist
	}
	user_biz_ids := make([]string, 0, len(req.GetUserBizIds()))
	for _, v := range userList {
		user_biz_ids = append(user_biz_ids, cast.ToString(v.BusinessID))
	}
	req.UserBizIds = user_biz_ids //过滤不存在的用户业务id
	//2. 校验角色业务id，并且过滤不存在的角色业务id
	req.RoleBizIds, err = s.checkUserRole(ctx, corpBizId, appBizId, roleBizIds)
	if err != nil {
		return nil, err
	}
	return userList, nil
}

// ListCustUser 获取用户列表
func (s *Service) ListCustUser(ctx context.Context, req *pb.ListCustUserReq) (
	rsp *pb.ListCustUserRsp, err error) {
	log.DebugContextf(ctx, "ListCustUser req:%+v", req)
	rsp = &pb.ListCustUserRsp{}
	//1.校验参数
	params := []string{req.AppBizId}
	params = append(params, req.GetUserBizIds()...)
	_, err = util.BatchCheckReqParamsIsUint64(ctx, params)
	if err != nil {
		return nil, err
	}
	app, err := s.dao.GetAppByAppBizID(ctx, cast.ToUint64(req.GetAppBizId()))
	if err != nil || app == nil {
		log.ErrorContextf(ctx, "GetAppByAppBizID err:%v", err)
		return nil, errs.ErrGetAppFail
	}
	userBizIds := make([]uint64, 0, len(req.GetUserBizIds()))
	for _, v := range req.GetUserBizIds() {
		userBizIds = append(userBizIds, cast.ToUint64(v))
	}
	offset := int(0)
	if req.GetPageNumber() > 0 {
		offset = int((req.GetPageNumber() - 1) * req.GetPageSize())
	}
	rsp.UserList, rsp.Total, err = s.permisLogic.GetUserList(ctx, &dao.CustUserFilter{
		KnowledgeBase: dao.KnowledgeBase{
			CorpBizID:      pkg.CorpBizID(ctx),
			AppBizID:       cast.ToUint64(req.GetAppBizId()),
			IsDeleted:      pkg.GetIntPtr(0),
			Offset:         offset,
			Limit:          int(req.GetPageSize()),
			OrderColumn:    []string{"update_time"},
			OrderDirection: 1,
		},
		BusinessIds:     userBizIds,
		Query:           req.Query,
		ThirdUserIdLike: req.GetThirdUserId(),
	})
	if err != nil {
		log.ErrorContextf(ctx, "ListCustUser GetUserList err:%v,req:%+v", err, req)
		return nil, err
	}
	log.DebugContextf(ctx, "ListCustUser rsp:%+v", rsp)
	return rsp, nil
}

// DescribeCustUser 获取用户详情
func (s *Service) DescribeCustUser(ctx context.Context, req *pb.DescribeCustUserReq) (
	rsp *pb.DescribeCustUserRsp, err error) {
	log.DebugContextf(ctx, "DescribeCustUser req:%+v", req)
	rsp = &pb.DescribeCustUserRsp{}
	//1.校验参数
	if req.GetUserBizId() == "" {
		return nil, errs.ErrUserNotExist
	}
	_, err = util.BatchCheckReqParamsIsUint64(ctx, []string{req.AppBizId, req.UserBizId})
	if err != nil {
		return nil, err
	}
	app, err := s.dao.GetAppByAppBizID(ctx, cast.ToUint64(req.GetAppBizId()))
	if err != nil || app == nil {
		log.ErrorContextf(ctx, "GetAppByAppBizID err:%v", err)
		return nil, errs.ErrGetAppFail
	}
	userList, count, err := s.permisLogic.GetUserList(ctx, &dao.CustUserFilter{
		KnowledgeBase: dao.KnowledgeBase{
			CorpBizID: pkg.CorpBizID(ctx),
			AppBizID:  cast.ToUint64(req.GetAppBizId()),
			IsDeleted: pkg.GetIntPtr(0),
			Offset:    0,
			Limit:     1,
		},
		BusinessId: cast.ToUint64(req.GetUserBizId()),
	})
	if err != nil {
		log.ErrorContextf(ctx, "DetailCustUser GetUserList err:%v,req:%+v", err, req)
		return nil, errs.ErrDetailUserFail
	}
	if count == 0 {
		return nil, errs.ErrUserNotExist
	}
	rsp.UserInfo = userList[0]
	return rsp, err
}

// DeleteCustUser 删除用户
func (s *Service) DeleteCustUser(ctx context.Context, req *pb.DeleteCustUserReq) (
	rsp *pb.DeleteCustUserRsp, err error) {
	log.DebugContextf(ctx, "DeleteCustUser req:%+v", req)
	rsp = &pb.DeleteCustUserRsp{}
	//1.校验参数
	params := []string{req.AppBizId}
	params = append(params, req.GetUserBizIds()...)
	_, err = util.BatchCheckReqParamsIsUint64(ctx, params)
	if err != nil {
		return nil, err
	}
	app, err := s.dao.GetAppByAppBizID(ctx, cast.ToUint64(req.GetAppBizId()))
	if err != nil || app == nil {
		log.ErrorContextf(ctx, "GetAppByAppBizID err:%v", err)
		return nil, errs.ErrGetAppFail
	}
	err = s.permisLogic.DeleteCustUser(ctx, req)
	if err != nil {
		log.ErrorContextf(ctx, "DeleteCustUser err:%v,req:%+v", err, req)
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
	log.DebugContextf(ctx, "SetCustUserConfig req:%+v", req)
	rsp = &pb.SetCustUserConfigRsp{}
	//1.校验参数
	_, err = util.BatchCheckReqParamsIsUint64(ctx, []string{req.GetAppBizId(),
		req.GetNotSetRoleBizId(), req.GetNotUseRoleBizId()})
	if err != nil {
		return nil, err
	}
	app, err := s.dao.GetAppByAppBizID(ctx, cast.ToUint64(req.GetAppBizId()))
	if err != nil || app == nil {
		log.ErrorContextf(ctx, "GetAppByAppBizID err:%v", err)
		return nil, errs.ErrGetAppFail
	}
	//2.校验下角色业务id是否存在
	corpBizId, appBizId, notSetRoleBizId, notUseRoleBizId := pkg.CorpBizID(ctx), cast.ToUint64(req.GetAppBizId()),
		cast.ToUint64(req.GetNotSetRoleBizId()), cast.ToUint64(req.GetNotUseRoleBizId())
	roleList := make([]uint64, 0, 2)
	if notSetRoleBizId != model.NotRoleBizId { //是不检索,无需过滤角色业务id
		roleList = append(roleList, cast.ToUint64(notSetRoleBizId))
	}
	if notUseRoleBizId != model.NotRoleBizId && notUseRoleBizId != notSetRoleBizId {
		roleList = append(roleList, cast.ToUint64(notUseRoleBizId))
	}
	if len(roleList) > 0 { //没有选自定义角色无需过滤
		bizIds, err := s.checkUserRole(ctx, corpBizId, appBizId, roleList)
		if err != nil || len(roleList) != len(bizIds) {
			log.ErrorContextf(ctx, "SetCustUserConfig checkUserRole err:%v,bizIds:%v,roleList:%v", err, bizIds, roleList)
			return nil, errs.ErrSetCustUserConfigRoleFail
		}
	}
	err = s.permisLogic.SetCustUserConfig(ctx, corpBizId, appBizId, notSetRoleBizId, notUseRoleBizId)
	if err != nil {
		log.ErrorContextf(ctx, "SetCustUserConfig err:%v,req:%v", err, req)
		return nil, err
	}
	return rsp, nil
}

// GetCustUserConfig 获取特殊权限配置
func (s *Service) GetCustUserConfig(ctx context.Context, req *pb.GetCustUserConfigReq) (
	rsp *pb.GetCustUserConfigRsp, err error) {
	log.DebugContextf(ctx, "GetCustUserConfig req:%+v", req)
	//1.校验参数
	_, err = util.BatchCheckReqParamsIsUint64(ctx, []string{req.GetAppBizId()})
	if err != nil {
		return nil, err
	}
	app, err := s.dao.GetAppByAppBizID(ctx, cast.ToUint64(req.GetAppBizId()))
	if err != nil || app == nil {
		log.ErrorContextf(ctx, "GetAppByAppBizID err:%v", err)
		return nil, errs.ErrGetAppFail
	}
	rsp, err = s.permisLogic.GetCustUserConfig(ctx, req)
	if err != nil {
		log.ErrorContextf(ctx, "GetCustUserConfig err:%v,req:%v", err, req)
		return nil, err
	}
	return rsp, nil
}

// SetThirdAclConfig 设置外部权限接口配置
/*
func (s *Service) SetThirdAclConfig(ctx context.Context, req *pb.SetThirdAclConfigReq) (
	rsp *pb.SetThirdAclConfigRsp, err error) {
	log.DebugContextf(ctx, "SetThirdAclConfig req:%+v", req)
	rsp = &pb.SetThirdAclConfigRsp{}
	//1.校验参数
	_, err = util.BatchCheckReqParamsIsUint64(ctx, []string{req.GetAppBizId(), req.GetKnowledgeBizId()})
	if err != nil {
		return nil, err
	}
	app, err := s.dao.GetAppByAppBizID(ctx, cast.ToUint64(req.GetAppBizId()))
	if err != nil || app == nil {
		log.ErrorContextf(ctx, "GetAppByAppBizID err:%v", err)
		return nil, errs.ErrGetAppFail
	}
	knowledge, err := s.dao.GetAppByAppBizID(ctx, cast.ToUint64(req.GetKnowledgeBizId()))
	if err != nil || knowledge == nil {
		log.ErrorContextf(ctx, "GetAppByAppBizID err:%v", err)
		return nil, errs.ErrGetKnowledgeFailed
	}
	if req.GetType() == 1 { //按token
		if req.GetThirdToken() == "" {
			return nil, errs.ErrSetThirdAclNotToken
		}
	} else {
		return nil, errs.ErrSetThirdAclTypeFail
	}
	if req.GetCheckPermissionsUrl() == "" {
		return nil, errs.ErrSetThirdAclAclUrlFail
	}
	err = s.permisLogic.SetThirdAclConfig(ctx, req)
	if err != nil {
		log.ErrorContextf(ctx, "SetThirdAclConfig SetThirdAclConfig err:%v,req:%v", err, req)
		return nil, err
	}
	return rsp, nil
}

// GetThirdAclConfig 获取外部权限接口配置
func (s *Service) GetThirdAclConfig(ctx context.Context, req *pb.GetThirdAclConfigReq) (
	rsp *pb.GetThirdAclConfigRsp, err error) {
	log.DebugContextf(ctx, "GetThirdAclConfig req:%+v", req)
	//1.校验参数
	_, err = util.BatchCheckReqParamsIsUint64(ctx, []string{req.GetAppBizId(), req.GetKnowledgeBizId()})
	if err != nil {
		return nil, err
	}
	app, err := s.dao.GetAppByAppBizID(ctx, cast.ToUint64(req.GetAppBizId()))
	if err != nil || app == nil {
		log.ErrorContextf(ctx, "GetAppByAppBizID err:%v", err)
		return nil, errs.ErrGetAppFail
	}
	rsp, err = s.permisLogic.GetThirdAclConfig(ctx, req)
	if err != nil {
		log.ErrorContextf(ctx, "GetThirdAclConfig GetThirdAclConfig err:%v,req:%v", err, req)
		return nil, err
	}
	return rsp, nil
}
*/

// GetAclConfigStatus 获取应用端权限配置状态
func (s *Service) GetAclConfigStatus(ctx context.Context, req *pb.GetAclConfigStatusReq) (
	rsp *pb.GetAclConfigStatusRsp, err error) {
	log.DebugContextf(ctx, "GetAclConfigStatus req:%+v", req)
	//1.校验参数
	_, err = util.BatchCheckReqParamsIsUint64(ctx, []string{req.GetAppBizId()})
	if err != nil {
		return nil, err
	}
	rsp, err = s.permisLogic.GetAclConfigStatus(ctx, req)
	if err != nil {
		log.ErrorContextf(ctx, "GetAclConfigStatus err:%v,req:%v", err, req)
		return nil, err
	}
	return rsp, nil
}
