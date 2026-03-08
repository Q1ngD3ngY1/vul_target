package permissions

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"git.code.oa.com/trpc-go/trpc-database/redis"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
	"git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_retrieval_server"
	retrieval "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_retrieval_server"
	"github.com/spf13/cast"
	"gorm.io/gorm"
)

const (
	SetCustUserConfig   = "knowledge_config:Lock:SetCustUserConfig:%d"
	RedisThirdUserID    = "{%d}:knowThirdUserID:%s" //根据appBizID来设置哈希标签,以thirdUserID为维度 作redis key
	RedisCustUserConfig = "knowCustUserAcl:%d"      //应用维度的特殊权限配置redis key
)

type PermisLogic struct {
	dao dao.Dao
	LogicRole
}

// NewPermisLogic 获取全局的数据操作对象
func NewPermisLogic(d dao.Dao) PermisLogic {
	return PermisLogic{
		dao: d,
		LogicRole: LogicRole{
			LogicRoler: d,
		},
	}
}

// CreateCustUser 创建用户
func (l *PermisLogic) CreateCustUser(ctx context.Context, req *pb.CreateCustUserReq) (
	rsp *pb.CreateCustUserRsp, err error) {
	rsp = &pb.CreateCustUserRsp{}
	//开启事务，同时创建用户，增加用户角色绑定关系
	tx := l.dao.GetTdsqlGormDB().Begin()
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
		} else if err != nil {
			tx.Rollback()
		} else {
			tx.Commit()
		}
	}()
	userDao := dao.GetCustUserDao(tx)
	//1.创建用户
	bizId := l.dao.GenerateSeqID()
	_, err = userDao.InserCustUser(ctx, &model.CustUser{
		CorpID:      pkg.CorpBizID(ctx),
		AppID:       cast.ToUint64(req.GetAppBizId()),
		BusinessID:  bizId,
		Name:        req.GetName(),
		ThirdUserID: req.GetThirdUserId(),
		IsDeleted:   0,
		CreateTime:  time.Now(),
		UpdateTime:  time.Now(),
	})
	if err != nil {
		log.ErrorContextf(ctx, "CreateCustUser err:%v,req:%+v", err, req)
		return nil, errs.ErrCreateUserFail
	}
	//2.增加绑定表
	userRoleList := make([]*model.UserRole, 0, len(req.GetRoleBizIds()))
	roleBizIDs := make([]uint64, 0, len(req.GetRoleBizIds()))
	for _, v := range req.GetRoleBizIds() {
		roleBizID := cast.ToUint64(v)

		roleBizIDs = append(roleBizIDs, roleBizID)
		userRoleList = append(userRoleList, &model.UserRole{
			CorpID:      pkg.CorpBizID(ctx),
			AppID:       cast.ToUint64(req.GetAppBizId()),
			UserBizID:   bizId,
			ThirdUserId: req.GetThirdUserId(),
			RoleBizID:   roleBizID,
			CreateTime:  time.Now(),
			UpdateTime:  time.Now(),
		})
	}
	err = userDao.InserUserRole(ctx, userRoleList)
	if err != nil {
		log.ErrorContextf(ctx, "CreateCustUser err:%v,req:%+v", err, req)
		return nil, errs.ErrCreateUserFail
	}
	rsp.UserBizId = bizId
	//3.将third_user_id关联的角色业务ids写入缓存
	if err = l.SetThirdUserIDRedis(ctx, pkg.CorpBizID(ctx), req.GetThirdUserId(), roleBizIDs); err != nil {
		log.ErrorContextf(ctx, "CreateCustUser set redis err:%v,appBizID:%v,thirdUserID:%v,roleBizIDs:%v",
			err, pkg.CorpBizID(ctx), req.GetThirdUserId(), roleBizIDs)
		return nil, errs.ErrCommonFail
	}
	return rsp, nil
}

// UpdateCustUser 编辑用户
func (l *PermisLogic) UpdateCustUser(ctx context.Context, req *pb.ModifyCustUserReq) (
	rsp *pb.ModifyCustUserRsp, err error) {
	rsp = &pb.ModifyCustUserRsp{}
	corpBizId, appBizId, userBizId := pkg.CorpBizID(ctx), cast.ToUint64(req.GetAppBizId()),
		cast.ToUint64(req.GetUserBizId())
	roleBizIds := make([]uint64, 0, len(req.GetRoleBizIds()))
	for _, v := range req.GetRoleBizIds() {
		roleBizIds = append(roleBizIds, cast.ToUint64(v))
	}
	//开启事务
	tx := l.dao.GetTdsqlGormDB().Begin()
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
		} else if err != nil {
			tx.Rollback()
		} else {
			tx.Commit()
		}
	}()
	userDao := dao.GetCustUserDao(tx)
	//1.更新用户信息
	_, err = userDao.UpdateUser(ctx, map[string]interface{}{
		model.UserTblColName: req.GetName(), model.UserTblColThirdUserId: req.GetThirdUserId(),
	}, &dao.CustUserFilter{
		KnowledgeBase: dao.KnowledgeBase{
			CorpBizID: corpBizId,
			AppBizID:  appBizId,
			IsDeleted: pkg.GetIntPtr(0),
			Limit:     1,
		},
		BusinessId: cast.ToUint64(req.GetUserBizId()),
	})
	if err != nil {
		log.ErrorContextf(ctx, "UpdateCustUser UpdateUser err:%v,req:%+v", err, req)
		return nil, errs.ErrUpdateUserFail
	}
	//2.更新角色绑定关系
	err = l.UpdateUserRole(ctx, corpBizId, appBizId, roleBizIds, []*model.CustUser{
		{BusinessID: userBizId, ThirdUserID: req.GetThirdUserId()}}, tx)
	if err != nil {
		return nil, err
	}
	//3.将third_user_id关联的角色业务ids写入缓存
	if err = l.SetThirdUserIDRedis(ctx, corpBizId, req.GetThirdUserId(), roleBizIds); err != nil { //柔性放过
		log.ErrorContextf(ctx, "UpdateCustUser set redis err:%v,appBizID:%v,thirdUserID:%v,roleBizIds:%v",
			err, pkg.CorpBizID(ctx), req.GetThirdUserId(), roleBizIds)
		return nil, errs.ErrCommonFail
	}
	return rsp, nil
}

// UpdateUserRole 更新用户角色绑定关系
func (l *PermisLogic) UpdateUserRole(ctx context.Context, corpBizId, appBizId uint64,
	roleBizIds []uint64, userList []*model.CustUser, tx *gorm.DB) (err error) {
	//开启事务
	if tx == nil { //如果是传入的由调用方控制
		tx = l.dao.GetTdsqlGormDB().Begin()
		defer func() {
			if r := recover(); r != nil {
				tx.Rollback()
			} else if err != nil {
				tx.Rollback()
			} else {
				tx.Commit()
			}
		}()
	}
	userDao := dao.GetCustUserDao(tx)
	userBizIds, thirdUserIds := make([]uint64, 0, len(userList)), make([]string, 0, len(userList))
	for _, user := range userList {
		userBizIds = append(userBizIds, user.BusinessID)
		thirdUserIds = append(thirdUserIds, user.ThirdUserID)
	}
	//1.删除用户角色绑定表
	pageSize, length := 200, len(userBizIds)
	for start := 0; start < length; start += pageSize {
		deleteMaxRow, end := int64(10000), min(start+pageSize, length)
		tmp := userBizIds[start:end]
		for deleteMaxRow == 10000 { //一次软删除10000条
			deleteMaxRow, err = userDao.UpdateUserRole(ctx, map[string]any{model.UserTblColIsDeleted: 1},
				&dao.UserRoleFilter{
					KnowledgeBase: dao.KnowledgeBase{
						CorpBizID: corpBizId,
						AppBizID:  appBizId,
						IsDeleted: pkg.GetIntPtr(0),
						Limit:     10000,
					},
					UserBizIds: tmp,
				})
			if err != nil {
				log.ErrorContextf(ctx, "UpdateUserRole UpdateUserRole err:%v,corp_biz_id:%v,app_biz_id:%v,role_biz_ids:%v,userBizIds:%v",
					corpBizId, appBizId, roleBizIds, userBizIds)
				return errs.ErrCommonFail
			}
		}
	}
	//2.增加用户角色绑定表
	userRoleList := make([]*model.UserRole, 0, len(roleBizIds))
	for _, user := range userList {
		for _, roleBizId := range roleBizIds {
			userRoleList = append(userRoleList, &model.UserRole{
				CorpID:      corpBizId,
				AppID:       appBizId,
				UserBizID:   user.BusinessID,
				ThirdUserId: user.ThirdUserID,
				RoleBizID:   roleBizId,
				CreateTime:  time.Now(),
				UpdateTime:  time.Now(),
			})
		}
	}
	err = userDao.InserUserRole(ctx, userRoleList)
	if err != nil {
		log.ErrorContextf(ctx, "UpdateUserRole InserUserRole err:%v,corpBizId:%v,appBizId:%v,roleBizIds:%v,userBizIds:%v",
			corpBizId, appBizId, roleBizIds, userBizIds)
		return errs.ErrCommonFail
	}
	//3.将third_user_id关联的角色业务ids写入缓存
	if err = l.BatchSetThirdUserIDRedis(ctx, appBizId, thirdUserIds, roleBizIds); err != nil {
		log.ErrorContextf(ctx, "UpdateUserRole set redis err:%v,appBizID:%v,thirdUserIds:%v,roleBizIds:%v",
			err, pkg.CorpBizID(ctx), thirdUserIds, roleBizIds)
		return errs.ErrCommonFail
	}
	return nil
}

// GetUserList 获取用户列表
func (l *PermisLogic) GetUserList(ctx context.Context, filter *dao.CustUserFilter) (
	rspUserList []*pb.CustUserInfo, total uint64, err error) {
	userDao := dao.GetCustUserDao(nil)
	//1.先根据条件获取用户列表
	userList, count, err := userDao.GetUserCountAndList(ctx, []string{}, filter)
	if err != nil {
		log.ErrorContextf(ctx, "GetUserList GetUserCountAndList err:%v,filter:%+v", err, filter)
		return nil, 0, errs.ErrGetUserListFail
	}
	total = uint64(count)
	if total == 0 { //用户列表为空，直接返回
		return nil, total, nil
	}
	//2.根据用户业务ids获取绑定的角色业务id
	userBizIds := make([]uint64, 0)
	for _, v := range userList {
		userBizIds = append(userBizIds, v.BusinessID)
		rspUserList = append(rspUserList, &pb.CustUserInfo{
			AppBizId: v.AppID, UserBizId: v.BusinessID, Name: v.Name, ThirdUserId: v.ThirdUserID,
			IsDeleted: 0, CreateTime: v.CreateTime.Unix(), UpdateTime: v.UpdateTime.Unix(), RoleList: []*pb.RoleBaseInfo{},
		})
	}
	isDeleted, maxRows := 0, 10000
	userRoleFilter := &dao.UserRoleFilter{
		KnowledgeBase: dao.KnowledgeBase{
			CorpBizID: filter.CorpBizID, AppBizID: filter.AppBizID, IsDeleted: &isDeleted, Limit: maxRows, //每次取10000条
		},
		UserBizIds: userBizIds, Type: pkg.UIntToPtr(model.UserRolesNormal),
	}
	userRoleList := make([]*model.UserRole, 0) //用户角色绑定列表
	for maxRows == 10000 {
		tmp, err := userDao.GetUserRoleList(ctx, []string{model.UserRoleTblColUserBizId, model.UserRoleTblColRoleUserId},
			userRoleFilter)
		if err != nil {
			log.ErrorContextf(ctx, "GetUserList GetUserRoleList err:%v,userRoleFilter:%+v", err, userRoleFilter)
			return nil, 0, errs.ErrGetUserRoleListFail
		}
		userRoleList = append(userRoleList, tmp...)
		maxRows = len(tmp)
	}
	if len(userRoleList) == 0 { //用户绑定的角色为空,直接返回,几乎不可能有这种case
		return rspUserList, total, nil
	}
	//3.根据角色业务ids获取角色信息
	roleByUser, roleByRole, roleBizIds := make(map[uint64][]*pb.RoleBaseInfo, 0), make(map[uint64]*model.KnowledgeRole), make([]uint64, 0)
	for _, v := range userRoleList {
		//用户业务id->角色数组 目前只有id 返回时需要回填
		roleByUser[v.UserBizID] = append(roleByUser[v.UserBizID], &pb.RoleBaseInfo{RoleBizId: v.RoleBizID})
		if _, ok := roleByRole[v.RoleBizID]; !ok { //过滤重复的角色业务id,根据角色业务id构造map，方便后续获取角色信息回填数据
			roleBizIds = append(roleBizIds, v.RoleBizID)
			roleByRole[v.RoleBizID] = &model.KnowledgeRole{}
		}
	}
	roleDao, roleList := dao.GetRoleDao(nil), make([]*model.KnowledgeRole, 0)
	roleFilter := &dao.KnowledgeRoleReq{
		KnowledgeBase: dao.KnowledgeBase{
			CorpBizID: filter.CorpBizID, AppBizID: filter.AppBizID, IsDeleted: &isDeleted, Limit: 10000, //每次取10000条
		},
		BizIDs: roleBizIds,
	}
	maxRows = 10000
	for maxRows == 10000 {
		tmp, err := roleDao.GetRoleList(ctx, []string{model.ColumnBusinessID, model.ColumnName, model.ColumnType}, roleFilter)
		if err != nil {
			log.ErrorContextf(ctx, "GetUserList GetRoleList err:%v,roleFilter:%+v", err, roleFilter)
			return nil, 0, errs.ErrGetRoleListFail
		}
		roleList = append(roleList, tmp...)
		maxRows = len(tmp)
	}
	for _, role := range roleList {
		roleByRole[uint64(role.BusinessID)] = &model.KnowledgeRole{BusinessID: role.BusinessID, Name: role.Name}
	}
	//4.构造返回结构
	for i, user := range rspUserList {
		//取该用户业务id关联的所有角色
		roleListInfo, ok := roleByUser[user.UserBizId]
		if !ok || len(roleListInfo) == 0 {
			continue
		}
		//回填角色名称
		for _, role := range roleListInfo {
			tmp, ok := roleByRole[role.RoleBizId]
			if !ok || tmp.BusinessID == 0 {
				continue
			}
			rspUserList[i].RoleList = append(rspUserList[i].RoleList, &pb.RoleBaseInfo{
				RoleBizId: tmp.BusinessID,
				RoleName:  tmp.Name,
			})
		}
	}
	return rspUserList, total, nil
}

// DeleteCustUser 删除用户
func (l *PermisLogic) DeleteCustUser(ctx context.Context, req *pb.DeleteCustUserReq) (err error) {
	if len(req.UserBizIds) == 0 {
		return nil
	}
	corpBizId, appBizId, isDeleted := pkg.CorpBizID(ctx), cast.ToUint64(req.GetAppBizId()), pkg.GetIntPtr(0)
	userBizIds := make([]uint64, 0, len(req.GetUserBizIds()))
	for _, v := range req.GetUserBizIds() {
		userBizIds = append(userBizIds, cast.ToUint64(v))
	}
	//开启事务
	tx := l.dao.GetTdsqlGormDB().Begin()
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
		} else if err != nil {
			tx.Rollback()
		} else {
			tx.Commit()
		}
	}()
	userDao := dao.GetCustUserDao(tx)
	//1.先取用户信息,过滤不存在的用户业务id
	userList, err := userDao.GetUserList(ctx, []string{model.UserTblColBusinessId, model.UserTblColThirdUserId}, &dao.CustUserFilter{
		KnowledgeBase: dao.KnowledgeBase{
			CorpBizID: corpBizId,
			AppBizID:  appBizId,
			Limit:     len(userBizIds),
			IsDeleted: isDeleted,
		},
		BusinessIds: userBizIds,
	})
	if err != nil {
		log.ErrorContextf(ctx, "DeleteCustUser GetUserList err:%v,req:%v", err, req)
		return errs.ErrGetRoleListFail
	}
	if len(userList) == 0 {
		return nil
	}
	userBizIds = make([]uint64, 0, len(userList))
	thirdUserIds := make([]string, 0, len(userList))
	for _, v := range userList {
		userBizIds = append(userBizIds, v.BusinessID)
		thirdUserIds = append(thirdUserIds, v.ThirdUserID)
	}
	//2.删除用户数据，用户没有被引用，可以直接删除
	pageSize, length := 200, len(userBizIds)
	for start := 0; start < length; start += pageSize {
		deleteMaxRow, end := int64(10000), min(start+pageSize, length)
		tmp := userBizIds[start:end]
		for deleteMaxRow == 10000 { //一次软删除10000条
			deleteMaxRow, err = userDao.UpdateUser(ctx, map[string]interface{}{model.UserTblColIsDeleted: 1},
				&dao.CustUserFilter{
					KnowledgeBase: dao.KnowledgeBase{
						CorpBizID: corpBizId,
						AppBizID:  appBizId,
						IsDeleted: isDeleted,
						Offset:    0,
						Limit:     10000,
					},
					BusinessIds: tmp,
				})
			if err != nil {
				log.ErrorContextf(ctx, "DeleteCustUser UpdateUser err:%v,req:%+v", err, req)
				return errs.ErrDeleteUserFail
			}
		}
		// 3.删除用户角色绑定数据
		deleteMaxRow = 10000
		for deleteMaxRow == 10000 { //一次软删除10000条
			deleteMaxRow, err = userDao.UpdateUserRole(ctx, map[string]interface{}{model.UserRoleTblColIsDeleted: 1},
				&dao.UserRoleFilter{
					KnowledgeBase: dao.KnowledgeBase{
						CorpBizID: corpBizId,
						AppBizID:  appBizId,
						IsDeleted: isDeleted,
						Limit:     10000,
					},
					UserBizIds: tmp,
				})
			if err != nil {
				log.ErrorContextf(ctx, "UpdateUserRole UpdateUserRole err:%v,req:%+v", err, req)
				return errs.ErrDeleteUserRoleFail
			}
		}
	}
	//4.删除third_user_id缓存
	if err = l.DelThirdUserIDRedis(ctx, appBizId, thirdUserIds); err != nil {
		log.ErrorContextf(ctx, "UpdateUserRole del redis err:%v,appBizID:%v,thirdUserIds:%v", err, appBizId, thirdUserIds)
		return errs.ErrCommonFail
	}
	return nil
}

// SetCustUserConfig 设置特殊权限配置
func (l *PermisLogic) SetCustUserConfig(ctx context.Context, corpBizId, appBizId,
	notSetRoleBizId, notUseRoleBizId uint64) error {
	key := fmt.Sprintf(SetCustUserConfig, appBizId)
	err := l.dao.Lock(ctx, key, 2*time.Second)
	if err != nil {
		if err == errs.ErrAlreadyLocked {
			return errs.ErrSetThirdAclConfigLock
		}
		log.ErrorContextf(ctx, "SetCustUserConfig Lock err:%v,key:%v", err, key)
		return errs.ErrCommonFail
	}
	defer l.dao.UnLock(ctx, key)
	err = dao.GetCustUserDao(nil).SetCustUserConfig(ctx, corpBizId, appBizId, notSetRoleBizId, notUseRoleBizId)
	if err != nil {
		log.ErrorContextf(ctx, "SetCustUserConfig err:%v,corpBizId:%v,appBizId:%v,notSetRoleBizId:%v,notUseRoleBizId:%v",
			err, corpBizId, appBizId, notSetRoleBizId, notUseRoleBizId)
		return errs.ErrSetCustUserConfigFail
	}
	//写入缓存,供运行时检索使用
	if err = l.SetUserConfigRedis(ctx, appBizId, notSetRoleBizId, notUseRoleBizId); err != nil { //柔性放过
		log.ErrorContextf(ctx, "SetCustUserConfig set redis err:%v,appBizId:%v,notSetRoleBizId:%v,notUseRoleBizId:%v",
			err, notSetRoleBizId, notUseRoleBizId)
	}
	return nil
}

// GetCustUserConfig 获取特殊权限配置
func (l *PermisLogic) GetCustUserConfig(ctx context.Context, req *pb.GetCustUserConfigReq) (
	rsp *pb.GetCustUserConfigRsp, err error) {
	userDao := dao.GetCustUserDao(nil)
	corpBizId, appBizId := pkg.CorpBizID(ctx), cast.ToUint64(req.GetAppBizId())
	//1.先获取本应用的特殊权限配置
	isDeleted := 0
	userRoleFilter := &dao.UserRoleFilter{
		KnowledgeBase: dao.KnowledgeBase{
			CorpBizID: corpBizId, AppBizID: appBizId, IsDeleted: &isDeleted, Limit: 2, //目前是一对一
		},
		Types: []uint32{model.NotSetThirdUserId, model.NotUseThirdUserId},
	}
	userRoleList, err := userDao.GetUserRoleList(ctx, []string{model.UserRoleTblColType, model.UserRoleTblColRoleUserId},
		userRoleFilter)
	if err != nil {
		log.ErrorContextf(ctx, "GetCustUserConfig GetUserRoleList err:%v,req:%+v", err, req)
		return nil, errs.ErrGetUserConfigFail
	}
	presetBizId, presetRoleName, needInit, roleBizIds := uint64(0), "", false, make([]uint64, 0, 2)
	if len(userRoleList) != 2 { //如果没有设置过,取默认角色填充
		needInit = true
		presetBizId, presetRoleName, err = l.LogicRole.CheckPresetRole(ctx, appBizId)
		if err != nil {
			log.ErrorContextf(ctx, "GetCustUserConfig CheckPresetRole err:%v,req:%+v", err, req)
			return nil, errs.ErrGetRoleListFail
		}
	}
	for _, v := range userRoleList {
		if v.RoleBizID != 0 {
			roleBizIds = append(roleBizIds, v.RoleBizID)
		}
	}
	//2.获取角色详情
	roleDao, roleByRole := dao.GetRoleDao(nil), make(map[uint64]*model.KnowledgeRole, 0)
	if len(roleBizIds) != 0 {
		tmp, err := roleDao.GetRoleList(ctx, []string{model.ColumnBusinessID, model.ColumnName}, &dao.KnowledgeRoleReq{
			KnowledgeBase: dao.KnowledgeBase{
				CorpBizID: corpBizId, AppBizID: appBizId, IsDeleted: &isDeleted, Limit: len(roleBizIds),
			},
			BizIDs: roleBizIds,
		})
		if err != nil {
			log.ErrorContextf(ctx, "GetCustUserConfig GetRoleList err:%v,req:%+v", err, req)
			return nil, errs.ErrGetRoleListFail
		}
		for _, role := range tmp {
			roleByRole[uint64(role.BusinessID)] = &model.KnowledgeRole{BusinessID: role.BusinessID, Name: role.Name}
		}
	}
	//3.构造返回结构
	rsp = &pb.GetCustUserConfigRsp{ //假如从未设置过,初始化结构
		UserConfig: &pb.CustUserConfig{
			NotUserIdRoleInfo: &pb.RoleBaseInfo{
				RoleBizId: presetBizId,
				RoleName:  presetRoleName,
			},
			NotUseRoleInfo: &pb.RoleBaseInfo{
				RoleBizId: presetBizId,
				RoleName:  presetRoleName,
			},
		},
	}
	for _, v := range userRoleList {
		if v.Type == model.NotSetThirdUserId {
			if v.RoleBizID != 0 {
				rsp.UserConfig.NotUserIdRoleInfo.RoleBizId = v.RoleBizID
				if _, ok := roleByRole[v.RoleBizID]; ok {
					rsp.UserConfig.NotUserIdRoleInfo.RoleName = roleByRole[v.RoleBizID].Name
				}
			} else { //为0代表不检索知识
				rsp.UserConfig.NotUserIdRoleInfo.RoleBizId = 0
				rsp.UserConfig.NotUserIdRoleInfo.RoleName = model.NotSearchKnowledge
			}
		} else if v.Type == model.NotUseThirdUserId {
			if v.RoleBizID != 0 {
				rsp.UserConfig.NotUseRoleInfo.RoleBizId = v.RoleBizID
				if _, ok := roleByRole[v.RoleBizID]; ok {
					rsp.UserConfig.NotUseRoleInfo.RoleName = roleByRole[v.RoleBizID].Name
				}
			} else { //为0代表不检索知识
				rsp.UserConfig.NotUseRoleInfo.RoleBizId = 0
				rsp.UserConfig.NotUseRoleInfo.RoleName = model.NotSearchKnowledge
			}
		}
	}
	if needInit { //第一次获取的时候初始化
		key := fmt.Sprintf(SetCustUserConfig, appBizId)
		err := l.dao.Lock(ctx, key, 2*time.Second)
		if err != nil {
			if err == errs.ErrAlreadyLocked {
				return nil, err
			}
			log.ErrorContextf(ctx, "SetCustUserConfig Lock err:%v,key:%v", err, key)
			return nil, errs.ErrCommonFail
		}
		defer l.dao.UnLock(ctx, key)
		err = dao.GetCustUserDao(nil).SetCustUserConfig(ctx, corpBizId, appBizId, rsp.UserConfig.NotUserIdRoleInfo.RoleBizId,
			rsp.UserConfig.NotUseRoleInfo.RoleBizId)
		if err != nil {
			log.ErrorContextf(ctx, "GetCustUserConfig SetCustUserConfig err:%v,corpBizId:%v,appBizId:%v,rsp:%+v",
				err, corpBizId, appBizId, rsp)
			return nil, errs.ErrGetUserConfigFail
		}
	}
	return rsp, nil
}

/*
// SetThirdAclConfig 设置外部权限接口配置
func (l *PermisLogic) SetThirdAclConfig(ctx context.Context, req *pb.SetThirdAclConfigReq) error {
	config, err := json.Marshal(model.ThirdAclConfig{
		Type:                req.GetType(),
		ThirdToken:          req.GetThirdToken(),
		CheckPermissionsUrl: req.GetCheckPermissionsUrl(),
	})
	if err != nil {
		log.ErrorContextf(ctx, "SetThirdAclConfig Marshal config err:%v,req:%+v", err, req)
		return errs.ErrCommonFail
	}
	err = dao.GetKnowledgeConfigDao(nil).SetKnowledgeConfig(ctx, pkg.CorpBizID(ctx),
		cast.ToUint64(req.GetKnowledgeBizId()), model.ThirdAcl, string(config))
	if err != nil {
		log.ErrorContextf(ctx, "SetThirdAclConfig err:%v,req:%+v", err, req)
		return errs.ErrSetThirdAclFail
	}
	return nil
}

// GetThirdAclConfig 获取外部权限接口配置
func (l *PermisLogic) GetThirdAclConfig(ctx context.Context, req *pb.GetThirdAclConfigReq) (
	rsp *pb.GetThirdAclConfigRsp, err error) {
	knowConfig, err := dao.GetKnowledgeConfigDao(nil).GetKnowledgeConfig(ctx, pkg.CorpBizID(ctx),
		cast.ToUint64(req.GetKnowledgeBizId()), model.ThirdAcl)
	if err != nil {
		log.ErrorContextf(ctx, "GetThirdAclConfig err:%v,req:%+v", err, req)
		return nil, errs.ErrGetThirdAclFail
	}
	thirdAclConfig := &model.ThirdAclConfig{}
	if knowConfig.Config != "" {
		err = json.Unmarshal([]byte(knowConfig.Config), &thirdAclConfig)
		if err != nil {
			log.ErrorContextf(ctx, "GetThirdAclConfig Unmarshal config err:%v,knowConfig:%+v", err, knowConfig)
			return nil, errs.ErrGetThirdAclFail
		}
	}
	rsp = &pb.GetThirdAclConfigRsp{
		ThirdAclConfig: &pb.ThirdAclConfig{
			AppBizId:            cast.ToUint64(req.GetAppBizId()),
			KnowledgeBizId:      knowConfig.KnowledgeBizId,
			Type:                thirdAclConfig.Type,
			ThirdToken:          thirdAclConfig.ThirdToken,
			CheckPermissionsUrl: thirdAclConfig.CheckPermissionsUrl,
		},
	}
	return rsp, nil
}
*/

// GetAclConfigStatus 获取应用端权限配置状态
func (l *PermisLogic) GetAclConfigStatus(ctx context.Context, req *pb.GetAclConfigStatusReq) (
	rsp *pb.GetAclConfigStatusRsp, err error) {
	rsp = &pb.GetAclConfigStatusRsp{}
	//1.判断知识库是否有内容
	appBizId := cast.ToUint64(req.GetAppBizId())
	app, err := client.GetAppInfo(ctx, appBizId, 1)
	if err != nil {
		log.ErrorContextf(ctx, "GetAclConfigStatus getApp err:%v,req:%+v", err, req)
		return nil, errs.ErrGetAppFailed
	}
	docCount, err := dao.GetDocDao().GetDocCount(ctx, []string{"id"}, &dao.DocFilter{
		CorpId: pkg.CorpID(ctx), RobotId: app.Id, IsDeleted: pkg.GetIntPtr(model.DocIsNotDeleted), Limit: 1,
	})
	if err != nil {
		log.ErrorContextf(ctx, "GetAclConfigStatus GetDocCount err:%v,req:%+v", err, req)
		return nil, errs.ErrGetKnowledgeFailed
	}
	qaList, err := dao.GetDocQaDao().GetDocQas(ctx, []string{"id"}, &dao.DocQaFilter{
		CorpId: pkg.CorpID(ctx), RobotId: app.Id, IsDeleted: pkg.GetIntPtr(model.QAIsNotDeleted), Limit: 1,
	})
	if err != nil {
		log.ErrorContextf(ctx, "GetAclConfigStatus GetDocQas err:%v,req:%+v", err, req)
		return nil, errs.ErrGetKnowledgeFailed
	}
	opt := &dao.ListDBSourcesOption{
		CorpBizID: pkg.CorpBizID(ctx),
		AppBizID:  appBizId,
		Page:      1,
		PageSize:  1,
	}
	_, tableCount, err := dao.GetDBSourceDao().ListByOption(ctx, opt)
	if err != nil {
		log.ErrorContextf(ctx, "GetAclConfigStatus ListByAppBizID err:%v,req:%+v", err, req)
		return nil, errs.ErrGetKnowledgeFailed
	}
	if docCount == 0 && len(qaList) == 0 && tableCount == 0 {
		rsp.Status = model.NotKnowledge
		return rsp, nil
	}
	//2.知识库有内容，判断是否创建了用户
	userDao := dao.GetCustUserDao(nil)
	count, err := userDao.GetUserCount(ctx, &dao.CustUserFilter{
		KnowledgeBase: dao.KnowledgeBase{CorpBizID: pkg.CorpBizID(ctx), AppBizID: appBizId, IsDeleted: pkg.GetIntPtr(0)},
	})
	if err != nil {
		log.ErrorContextf(ctx, "GetAclConfigStatus GetUserCount err:%v,req:%+v", err, req)
		return nil, errs.ErrGetUserListFail
	}
	if count == 0 {
		rsp.Status = model.NotAppUser
		return rsp, nil
	}
	//3.判断是否对接了外部权限
	configs, err := dao.GetKnowledgeConfigDao(nil).GetKnowledgeConfigs(ctx, pkg.CorpBizID(ctx),
		[]uint64{appBizId}, []uint32{uint32(pb.KnowledgeBaseConfigType_THIRD_ACL)})
	if err != nil {
		log.ErrorContextf(ctx, "GetAclConfigStatus GetAppConfig err:%v,req:%+v", err, req)
		return nil, errs.ErrGetThirdAclFail
	}
	if len(configs) != 0 {
		rsp.Status = model.HasThirdAcl
		return rsp, nil
	}
	rsp.Status = model.HasAppUser
	return rsp, nil
}

// getThirdUserIDKey 获取third_user_id缓存key
func getThirdUserIDKey(appBizID uint64, thirdUserID string) string {
	return fmt.Sprintf(RedisThirdUserID, appBizID, thirdUserID)
}

// DelThirdUserIDRedis 批量删除third_user_id redis key
func (l *PermisLogic) DelThirdUserIDRedis(ctx context.Context, appBizID uint64, thirdUserIDs []string) error {
	pipe, err := l.dao.RedisCli().Pipeline(ctx)
	defer pipe.Close()
	if err != nil {
		log.ErrorContextf(ctx, "DelThirdUserIDRedis get Pipeline err:%v", err)
		return err
	}
	for _, v := range thirdUserIDs {
		pipe.Send("DEL", getThirdUserIDKey(appBizID, v))
	}
	if err := pipe.Flush(); err != nil {
		log.ErrorContextf(ctx, "DelThirdUserIDRedis pipeline exec err:%v,appBizID:%v,thirdUserIDs:%v", err, appBizID, thirdUserIDs)
		return err
	}
	return nil
}

// BatchSetThirdUserIDRedis 批量设置third_user_id redis key
func (l *PermisLogic) BatchSetThirdUserIDRedis(ctx context.Context, appBizID uint64, thirdUserIDs []string, roleBizIDs []uint64) error {
	pipe, err := l.dao.RedisCli().Pipeline(ctx)
	defer pipe.Close()
	if err != nil {
		log.ErrorContextf(ctx, "BatchSetThirdUserIDRedis get Pipeline err:%v", err)
		return err
	}
	str := make([]string, 0, len(roleBizIDs))
	for _, v := range roleBizIDs {
		str = append(str, cast.ToString(v))
	}
	value := strings.Join(str, ",")
	for _, v := range thirdUserIDs {
		pipe.Send("SETEX", getThirdUserIDKey(appBizID, v), 3600*24*10, value)
	}
	if err := pipe.Flush(); err != nil {
		log.ErrorContextf(ctx, "BatchSetThirdUserIDRedis pipeline exec err:%v,appBizID:%v,thirdUserIDs:%v,roleBizIDs:%v",
			err, appBizID, thirdUserIDs, roleBizIDs)
		return err
	}
	return nil
}

// SetThirdUserIDRedis third_user_id关联的角色业务ids存到缓存中,供运行时使用
func (l *PermisLogic) SetThirdUserIDRedis(ctx context.Context, appBizID uint64, thirdUserID string, roleBizIDs []uint64) error {
	str := make([]string, 0, len(roleBizIDs))
	for _, v := range roleBizIDs {
		str = append(str, cast.ToString(v))
	}
	value := strings.Join(str, ",")
	//有效期10天
	thirdUserIDKey := getThirdUserIDKey(appBizID, thirdUserID)
	if _, err := l.dao.RedisCli().Do(ctx, "SETEX", thirdUserIDKey, 3600*24*10, value); err != nil {
		log.ErrorContextf(ctx, "SetThirdUserIDRedis set redis err:%v,key:%v,value:%v", err, thirdUserIDKey, value)
		return err
	}
	return nil
}

// GetThirdUserIDRedis 从redis获取third_user_id关联的角色业务ids,供运行时使用
func (l *PermisLogic) GetThirdUserIDRedis(ctx context.Context, appBizID uint64, thirdUserID string) (roleBizIDs []uint64, err error) {
	thirdUserIDKey := getThirdUserIDKey(appBizID, thirdUserID)
	value, err := redis.String(l.dao.RedisCli().Do(ctx, "GET", thirdUserIDKey))
	if err != nil || value == "" { //获取redis为空,取db数据
		userRoleFilter := &dao.UserRoleFilter{
			KnowledgeBase: dao.KnowledgeBase{
				CorpBizID: pkg.CorpBizID(ctx), AppBizID: appBizID, IsDeleted: pkg.GetIntPtr(0),
			},
			Type:        pkg.UIntToPtr(model.UserRolesNormal),
			ThirdUserId: thirdUserID,
		}
		userRoleList, err := dao.GetCustUserDao(nil).GetUserRoleList(ctx, []string{model.UserRoleTblColRoleUserId}, userRoleFilter)
		if err != nil {
			log.ErrorContextf(ctx, "GetThirdUserIDRedis GetUserRoleList err:%v,userRoleFilter:%+v", err, userRoleFilter)
			return nil, err
		}
		for _, v := range userRoleList {
			roleBizIDs = append(roleBizIDs, v.RoleBizID)
		}
		//把db数据写入redis
		if err = l.SetThirdUserIDRedis(ctx, appBizID, thirdUserID, roleBizIDs); err != nil { //柔性放过
			log.ErrorContextf(ctx, "GetThirdUserIDRedis set redis err:%v,appBizID:%v,thirdUserID:%v,roleBizIDs:%v",
				err, appBizID, thirdUserID, roleBizIDs)
		}
		return roleBizIDs, nil
	}
	str := strings.Split(value, ",")
	for _, v := range str {
		roleBizIDs = append(roleBizIDs, cast.ToUint64(v))
	}
	ttl, err := redis.Int(l.dao.RedisCli().Do(ctx, "TTL", thirdUserIDKey))
	if err != nil {
		log.ErrorContextf(ctx, "GetUserConfigRedis ttl key:%v err:%v", thirdUserIDKey, err)
	} else if ttl < 30 { //续期30天
		_, err = l.dao.RedisCli().Do(ctx, "EXPIRE", thirdUserIDKey, 3600*24*10)
		if err != nil {
			log.ErrorContextf(ctx, "GetUserConfigRedis EXPIRE key:%v err:%v", thirdUserIDKey, err)
		}
	}
	return roleBizIDs, nil
}

// getCustUserConfigKey 获取特殊权限配置的redis key
func getCustUserConfigKey(appBizID uint64) string {
	return fmt.Sprintf(RedisCustUserConfig, appBizID)
}

// SetUserConfigRedis 特殊权限配置的角色业务id存到缓存中,供运行时使用
func (l *PermisLogic) SetUserConfigRedis(ctx context.Context, appBizID, notSet, notUse uint64) error {
	tmp, err := json.Marshal(model.CustUserConfigRedis{
		NotSet: cast.ToString(notSet),
		NotUse: cast.ToString(notUse),
	})
	if err != nil {
		log.ErrorContextf(ctx, "SetUserConfigRedis marshal redis value err:%v,notSet:%v,notUse:%v",
			err, notSet, notUse)
		return err
	}
	//有效期30天
	aclKey := getCustUserConfigKey(appBizID)
	if _, err = l.dao.RedisCli().Do(ctx, "SETEX", aclKey, 3600*24*30, string(tmp)); err != nil {
		log.ErrorContextf(ctx, "SetUserConfigRedis set redis err:%v,key:%v,value:%v", err, aclKey, string(tmp))
		return err
	}
	return nil
}

// GetUserConfigRedis 从redis获取特殊权限配置的角色业务id,供运行时使用
func (l *PermisLogic) GetUserConfigRedis(ctx context.Context, appBizID uint64) (notSet, notUse uint64, err error) {
	//1.从缓存取特殊权限配置
	aclKey := getCustUserConfigKey(appBizID)
	value, err := redis.String(l.dao.RedisCli().Do(ctx, "GET", aclKey))
	if err != nil || value == "" { //2.没取到,走db写入一次
		aclConfig, err := l.GetCustUserConfig(ctx, &pb.GetCustUserConfigReq{
			AppBizId: cast.ToString(appBizID),
		})
		if err != nil {
			if err == errs.ErrAlreadyLocked {
				return 0, 0, err
			}
			log.ErrorContextf(ctx, "GetUserConfigRedis GetCustUserConfig err:%v,appBizID:%v", err, appBizID)
			return 0, 0, err
		}
		notSet, notUse = aclConfig.UserConfig.NotUserIdRoleInfo.RoleBizId, aclConfig.UserConfig.NotUseRoleInfo.RoleBizId
		//写入缓存
		if err = l.SetUserConfigRedis(ctx, appBizID, notSet, notUse); err != nil { //柔性放过
			log.ErrorContextf(ctx, "GetUserConfigRedis set redis err:%v,appBizId:%v,notSet:%v,notUse:%v", err, notSet, notUse)
		}
		return notSet, notUse, nil
	}
	//3.有缓存返回数据,判断是否续期
	ttl, err := redis.Int(l.dao.RedisCli().Do(ctx, "TTL", aclKey))
	if err != nil {
		log.ErrorContextf(ctx, "GetUserConfigRedis ttl key:%v err:%v", aclKey, err)
	} else if ttl < 30 { //续期30天
		_, err = l.dao.RedisCli().Do(ctx, "EXPIRE", aclKey, 3600*24*30)
		if err != nil {
			log.ErrorContextf(ctx, "GetUserConfigRedis EXPIRE key:%v err:%v", aclKey, err)
		}
	}
	//4.解析缓存数据返回
	var aclConfig model.CustUserConfigRedis
	err = json.Unmarshal([]byte(value), &aclConfig)
	if err != nil {
		log.ErrorContextf(ctx, "GetAclRoleBizID Unmarshal err:%v,value:%v", err, value)
		return 0, 0, err
	}
	return cast.ToUint64(aclConfig.NotSet), cast.ToUint64(aclConfig.NotUse), nil
}

// GetRoleSearchLabel 获取角色的检索label
// searchLabels为nil代表可以检索全部知识
// searchLabels每个key是代表可以检索的知识库业务id 没有的知识库代表不能去检索
// searchLabels[123] == nil || len(searchLabels[123].Expressions) == 0 代表可以检索这个知识库所有知识
func (l *PermisLogic) GetRoleSearchLabel(ctx context.Context, corpBizID, appID, appBizID uint64, lkeUserID string) (
	searchLabels map[uint64]*bot_retrieval_server.LabelExpression, notSearch bool, err error) {
	log.DebugContextf(ctx, "GetRoleSearchLabel corpBizID:%v,appBizID:%v,lkeUserID:%v", corpBizID, appBizID, lkeUserID)
	//1.先获取应用的特殊权限配置
	ctx = pkg.WithCorpBizID(ctx, corpBizID)
	ctx = pkg.WithAppID(ctx, appID)
	notSetUserRoleBizID, notUseUserRoleBizID, err := l.GetUserConfigRedis(ctx, appBizID)
	if err != nil {
		if err == errs.ErrAlreadyLocked {
			return nil, false, err
		}
		log.WarnContextf(ctx, "GetRoleSearchLabel get aclConfig err:%v,appBizID:%v,lkeUserID:%v", err, appBizID, lkeUserID)
		return nil, false, err
	}
	//2.如果lke_user_id为空使用未传入配置
	if lkeUserID == "" {
		if notSetUserRoleBizID == model.NotRoleBizId { //为0代表不检索
			return nil, true, nil
		}
		vectorLabel, needSkip, err := l.LogicRole.FormatFilter(ctx, &FormatFilterReq{
			CorpBizID: corpBizID, AppID: appID, AppBizID: appBizID,
			RoleBizID: notSetUserRoleBizID,
		})
		if err != nil {
			log.ErrorContextf(ctx, "GetRoleSearchLabel get acl role err:%v,roleBizID:%v", err, notSetUserRoleBizID)
			return nil, false, err
		}
		if !needSkip {
			return vectorLabel, false, nil
		}
		//如果未传入角色配置为空,走无可使用配置兜底
		log.WarnContextf(ctx, "GetRoleSearchLabel role isEmpty err:%v,notSetUserRoleBizID:%v", err, notSetUserRoleBizID)
		return l.getNotUseRoleLabel(ctx, corpBizID, appID, appBizID, notUseUserRoleBizID)
	}
	//3.不为空根据lke_user_id取角色业务ids
	userRoleBizIDs, err := l.GetThirdUserIDRedis(ctx, appBizID, lkeUserID)
	if err != nil {
		log.ErrorContextf(ctx, "GetRoleSearchLabel GetThirdUserIDRedis err:%v,appBizID:%v,lkeUserID:%v", err, appBizID, lkeUserID)
		return nil, false, err
	}
	//4.如果根据lke_user_id没有取到可用角色,根据无可使用配置处理
	if len(userRoleBizIDs) == 0 {
		return l.getNotUseRoleLabel(ctx, corpBizID, appID, appBizID, notUseUserRoleBizID)
	}
	//5.根据角色获取向量标签
	searchLabels = make(map[uint64]*bot_retrieval_server.LabelExpression, 0)
	for _, roleBizID := range userRoleBizIDs {
		vectorLabel, needSkip, err := l.LogicRole.FormatFilter(ctx, &FormatFilterReq{
			CorpBizID: corpBizID, AppID: appID, AppBizID: appBizID,
			RoleBizID: roleBizID,
		})
		if err != nil {
			log.ErrorContextf(ctx, "GetRoleSearchLabel get role label err:%v,roleBizID:%v", err, roleBizID)
			continue
		}
		if needSkip { //角色配置为空,跳过
			log.WarnContextf(ctx, "GetRoleSearchLabel role isEmpty err:%v,roleBizID:%v", err, roleBizID)
			continue
		}
		if len(vectorLabel) == 0 { //如果其中一个角色没有配置,跳过
			continue
		}
		for knowBizID, labelExpression := range vectorLabel {
			//代表可以查看这个知识库所有知识
			if labelExpression == nil || len(labelExpression.Expressions) == 0 {
				searchLabels[knowBizID] = &bot_retrieval_server.LabelExpression{}
				continue
			}
			knowLabel, ok := searchLabels[knowBizID]
			if !ok {
				searchLabels[knowBizID] = &bot_retrieval_server.LabelExpression{
					Operator:    retrieval.LabelExpression_OR, //多个角色之前取或关系
					Expressions: []*bot_retrieval_server.LabelExpression{labelExpression},
				}
				continue
			}
			if len(knowLabel.Expressions) == 0 { //已经为该知识库全部知识,不需要再拼接了
				continue
			}
			knowLabel.Expressions = append(knowLabel.Expressions, labelExpression)
		}
	}
	//所有角色配置都为空,走无可使用配置兜底
	if len(searchLabels) == 0 {
		return l.getNotUseRoleLabel(ctx, corpBizID, appID, appBizID, notUseUserRoleBizID)
	}
	return searchLabels, false, nil
}

// getNotUseRoleLabel 获取无可使用配置兜底
func (l *PermisLogic) getNotUseRoleLabel(ctx context.Context, corpBizID, appID, appBizID, notUseUserRoleBizID uint64) (
	map[uint64]*bot_retrieval_server.LabelExpression, bool, error) {
	if notUseUserRoleBizID == model.NotRoleBizId { //为0代表不检索
		return nil, true, nil
	}
	vectorLabel, needSkip, err := l.LogicRole.FormatFilter(ctx, &FormatFilterReq{
		CorpBizID: corpBizID,
		AppID:     appID,
		AppBizID:  appBizID,
		RoleBizID: notUseUserRoleBizID,
	})
	if err != nil {
		log.ErrorContextf(ctx, "GetRoleSearchLabel getNotUseRoleLabel get acl role err:%v,roleBizID:%v", err, notUseUserRoleBizID)
		return nil, false, err
	}
	if needSkip { //两个特殊权限配置都为空,不检索
		log.WarnContextf(ctx, "GetRoleSearchLabel getNotUseRoleLabel role isEmpty err:%v,roleBizID:%v", err, notUseUserRoleBizID)
		return nil, true, nil
	}
	return vectorLabel, false, nil
}
