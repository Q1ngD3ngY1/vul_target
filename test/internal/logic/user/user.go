package user

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cast"

	"git.code.oa.com/trpc-go/trpc-go/client"
	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/gox/ptrx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/syncx/distributedlockx"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/tdsqlquery"
	"git.woa.com/adp/kb/kb-config/internal/dao/category"
	dbdao "git.woa.com/adp/kb/kb-config/internal/dao/database"
	"git.woa.com/adp/kb/kb-config/internal/dao/kb"
	"git.woa.com/adp/kb/kb-config/internal/dao/label"
	"git.woa.com/adp/kb/kb-config/internal/dao/types"
	"git.woa.com/adp/kb/kb-config/internal/dao/user"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	dbEntity "git.woa.com/adp/kb/kb-config/internal/entity/database"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	kbEntity "git.woa.com/adp/kb/kb-config/internal/entity/kb"
	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
	docLogic "git.woa.com/adp/kb/kb-config/internal/logic/document"
	qaLogic "git.woa.com/adp/kb/kb-config/internal/logic/qa"
	"git.woa.com/adp/kb/kb-config/internal/rpc"
	knowClient "git.woa.com/adp/kb/kb-config/internal/rpc"
	"git.woa.com/adp/kb/kb-config/internal/util/idgen"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	appconfig "git.woa.com/adp/pb-go/app/app_config"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
	retrieval "git.woa.com/adp/pb-go/kb/kb_retrieval"
)

const (
	ModifyCustUserConfig = "knowledge_config:Lock:SetCustUserConfig:%d"
)

type Logic struct {
	rpc      *rpc.RPC
	dao      user.Dao
	dbDao    dbdao.Dao
	kbDao    kb.Dao
	labelDao label.Dao
	cateDao  category.Dao
	lockRdb  types.AdminRedis
	docLogic *docLogic.Logic
	qaLogic  *qaLogic.Logic
}

func NewLogic(rpc *rpc.RPC, dao user.Dao, dbDao dbdao.Dao, kbDao kb.Dao,
	labelDao label.Dao, cateDao category.Dao, rdb types.AdminRedis, docLogic *docLogic.Logic,
	qaLogic *qaLogic.Logic) *Logic {
	return &Logic{
		rpc:      rpc,
		dao:      dao,
		dbDao:    dbDao,
		kbDao:    kbDao,
		labelDao: labelDao,
		cateDao:  cateDao,
		lockRdb:  rdb,
		docLogic: docLogic,
		qaLogic:  qaLogic,
	}
}

func (l *Logic) DescribeUserCountAndList(ctx context.Context,
	corpBizID, appBizID uint64,
	filter *entity.CustUserFilter) ([]*entity.CustUser, int64, error) {
	return l.dao.DescribeUserCountAndList(ctx, corpBizID, appBizID, filter)
}

func (l *Logic) DescribeUserCount(ctx context.Context,
	corpBizID, AppBizID uint64,
	filter *entity.CustUserFilter) (int64, error) {
	return l.dao.DescribeUserCount(ctx, corpBizID, AppBizID, filter)
}

func (l *Logic) DescribeUserList(ctx context.Context,
	corpBizID, appBizID uint64,
	filter *entity.CustUserFilter) ([]*entity.CustUser, error) {
	return l.dao.DescribeUserList(ctx, corpBizID, appBizID, filter)
}

func (l *Logic) DescribeUserRoleList(ctx context.Context, corpBizID, appBizID uint64, filter *entity.UserRoleFilter) ([]*entity.UserRole, error) {
	return l.dao.DescribeUserRoleList(ctx, corpBizID, appBizID, filter)
}

func (l *Logic) ModifyUserConfig(ctx context.Context,
	corpBizId, appBizId, notSetRoleBizId, notUseRoleBizId uint64) error {
	return l.dao.ModifyUserConfig(ctx, corpBizId, appBizId, notSetRoleBizId, notUseRoleBizId)
}

func (l *Logic) DescribeKnowledgeRoleList(ctx context.Context,
	corpBizID, appBizID uint64,
	filter *entity.KnowledgeRoleFilter) (int64, []*entity.KnowledgeRole, error) {
	return l.dao.DescribeKnowledgeRoleList(ctx, corpBizID, appBizID, filter)
}

func (l *Logic) DeleteKnowledgeRoleDocList(ctx context.Context,
	corpBizID, appBizID uint64,
	filter *entity.KnowledgeRoleDocFilter) error {
	return l.dao.DeleteKnowledgeRoleDocList(ctx, corpBizID, appBizID, filter, nil)
}

func (l *Logic) DescribeRoleIDListByDocBiz(ctx context.Context,
	appBizId, docBizId uint64, batchSize int) ([]uint64, error) {
	return l.dao.DescribeRoleIDListByDocBiz(ctx, appBizId, docBizId, batchSize)
}

func (l *Logic) DeleteKnowledgeRoleAttributeLabelByAttrAndLabelBizIDs(ctx context.Context,
	knowBizID uint64, attrBizIDs, labelBizIDs []uint64, pageSize, batchSize int) error {
	return l.dao.DeleteKnowledgeRoleAttributeLabelByAttrAndLabelBizIDs(ctx, knowBizID, attrBizIDs, labelBizIDs,
		pageSize, batchSize)
}

func (l *Logic) DeleteRoleCateListByKnowAndCateBizID(ctx context.Context,
	knowBizID, cateBizID uint64, batchSize int) error {
	return l.dao.DeleteRoleCateListByKnowAndCateBizID(ctx, knowBizID, cateBizID, batchSize)
}

func (l *Logic) DeleteKnowledgeRoleDbTables(ctx context.Context,
	knowBizID uint64, dbTableBizIDs []uint64, pageSize, batchSize int) error {
	return l.dao.DeleteKnowledgeRoleDbTables(ctx, knowBizID, dbTableBizIDs, pageSize, batchSize)
}

func (l *Logic) DescribeRoleByDbBiz(ctx context.Context, knowBizID, dbBizId uint64, batchSize int) ([]uint64, error) {
	return l.dao.DescribeRoleByDbBiz(ctx, knowBizID, dbBizId, batchSize)
}

func (l *Logic) DeleteKnowledgeAssociation(ctx context.Context,
	corpBizID, appBizID uint64, knowledgeBizIds []uint64) error {
	return l.dao.DeleteKnowledgeAssociation(ctx, corpBizID, appBizID, knowledgeBizIds)
}

func (l *Logic) ModifyRoleKnowledgeByAttrChange(ctx context.Context,
	knowBizID uint64, attrBizIds []uint64, labelBizIds []uint64) func() error {
	return l.dao.ModifyRoleKnowledgeByAttrChange(ctx, knowBizID, attrBizIds, labelBizIds)
}

func (l *Logic) ModifyRoleKnowledgeByCate(ctx context.Context,
	corpBizID, knowBizID uint64, cateBizIDs []uint64) error {
	return l.dao.ModifyRoleKnowledgeByCate(ctx, corpBizID, knowBizID, cateBizIDs)
}

func (l *Logic) DescribeSIUser(ctx context.Context, sid uint64, loginUin, loginSubAccountUin string) (*entity.User,
	error) {
	return l.dao.DescribeSIUser(ctx, sid, loginUin, loginSubAccountUin)
}

func (l *Logic) DescribeExpUser(ctx context.Context, id uint64) (*entity.User, error) {
	return l.dao.DescribeExpUser(ctx, id)
}

func (l *Logic) CreateCustUser(ctx context.Context,
	roleBizIDs []uint64,
	custUser *entity.CustUser) (uint64, error) {
	bizID := idgen.GetId()
	if err := l.dao.Query().Transaction(func(tx *tdsqlquery.Query) error {
		// 1.创建用户
		custUser.BusinessID = bizID
		_, err := l.dao.CreateUser(ctx, custUser, tx)
		if err != nil {
			logx.E(ctx, "CreateUser err:%v,custUser:%+v", err, custUser)
			return errs.ErrCreateUserFail
		}
		// 2.增加绑定表
		userRoleList := make([]*entity.UserRole, 0, len(roleBizIDs))
		for _, roleBizID := range roleBizIDs {
			roleBizIDs = append(roleBizIDs, roleBizID)
			userRoleList = append(userRoleList, &entity.UserRole{
				CorpID:      contextx.Metadata(ctx).CorpBizID(),
				AppID:       cast.ToUint64(custUser.AppID),
				UserBizID:   bizID,
				ThirdUserId: custUser.ThirdUserID,
				RoleBizID:   roleBizID,
				CreateTime:  time.Now(),
				UpdateTime:  time.Now(),
			})
		}
		err = l.dao.CreateUserRoleList(ctx, userRoleList, tx)
		if err != nil {
			logx.E(ctx, "CreateUserRoleList err:%v,custUser:%+v", err, custUser)
			return errs.ErrCreateUserFail
		}
		// 3.将third_user_id关联的角色业务ids写入缓存
		if err = l.dao.ModifyThirdUserIDCache(ctx, contextx.Metadata(ctx).CorpBizID(), custUser.ThirdUserID,
			roleBizIDs); err != nil {
			logx.E(ctx, "CreateCustUser set redis err:%v,appBizID:%v,thirdUserID:%v,roleBizIDs:%v",
				err, contextx.Metadata(ctx).CorpBizID(), custUser.ThirdUserID, roleBizIDs)
			return errs.ErrCommonFail
		}
		return nil
	}); err != nil {
		logx.E(ctx, "CreateCustUser err:%+v", err)
		return 0, err
	}
	return bizID, nil
}

func (l *Logic) ModifyCustUser(ctx context.Context,
	roleBizIDs []uint64,
	custUser *entity.CustUser) error {
	corpBizId, appBizId, userBizId := contextx.Metadata(ctx).CorpBizID(), custUser.AppID,
		custUser.BusinessID
	if err := l.dao.Query().Transaction(func(tx *tdsqlquery.Query) error {
		// 1.更新用户信息
		_, err := l.dao.ModifyUser(ctx, corpBizId, appBizId,
			&entity.CustUserFilter{
				BizIDs: []uint64{userBizId},
				Limit:  1,
			},
			&entity.CustUser{
				Name:        custUser.Name,
				ThirdUserID: custUser.ThirdUserID,
			}, tx)
		if err != nil {
			logx.E(ctx, "UpdateCustUser ModifyUser err:%v,custUser:%+v", err, custUser)
			return errs.ErrUpdateUserFail
		}
		// 2.更新角色绑定关系
		err = l.ModifyUserRoleList(ctx, corpBizId, appBizId, roleBizIDs, []*entity.CustUser{
			{BusinessID: userBizId, ThirdUserID: custUser.ThirdUserID}}, tx)
		if err != nil {
			return err
		}
		// 3.将third_user_id关联的角色业务ids写入缓存
		if err = l.dao.ModifyThirdUserIDCache(ctx, corpBizId, custUser.ThirdUserID, roleBizIDs); err != nil { // 柔性放过
			logx.E(ctx, "UpdateCustUser set redis err:%v,appBizID:%v,thirdUserID:%v,roleBizIDs:%v",
				err, contextx.Metadata(ctx).CorpBizID(), custUser.ThirdUserID, roleBizIDs)
			return errs.ErrCommonFail
		}
		return nil
	}); err != nil {
		logx.E(ctx, "ModifyCustUser err:%+v", err)
		return err
	}
	return nil
}

func (l *Logic) ModifyUserRoleList(ctx context.Context,
	corpBizId, appBizId uint64,
	roleBizIds []uint64,
	userList []*entity.CustUser, q *tdsqlquery.Query) error {
	if q == nil {
		q = l.dao.Query()
	}
	if err := q.Transaction(func(tx *tdsqlquery.Query) error {
		var err error
		userBizIds, thirdUserIds := make([]uint64, 0, len(userList)), make([]string, 0, len(userList))
		for _, user := range userList {
			userBizIds = append(userBizIds, user.BusinessID)
			thirdUserIds = append(thirdUserIds, user.ThirdUserID)
		}
		// 1.删除用户角色绑定表
		pageSize, length := 200, len(userBizIds)
		for start := 0; start < length; start += pageSize {
			deleteMaxRow, end := int64(10000), min(start+pageSize, length)
			tmp := userBizIds[start:end]
			for deleteMaxRow == 10000 { // 一次软删除10000条
				deleteMaxRow, err = l.dao.ModifyUserRole(ctx, corpBizId, appBizId,
					&entity.UserRoleFilter{
						UserBizIDs: tmp,
						Limit:      10000,
					}, &entity.UserRole{
						IsDeleted: true,
					}, tx)
				if err != nil {
					logx.E(ctx,
						"UpdateUserRole UpdateUserRole err:%v,corp_biz_id:%v,app_biz_id:%v,role_biz_ids:%v,userBizIds:%v",
						corpBizId, appBizId, roleBizIds, userBizIds)
					return errs.ErrCommonFail
				}
			}
		}
		// 2.增加用户角色绑定表
		userRoleList := make([]*entity.UserRole, 0, len(roleBizIds))
		for _, user := range userList {
			for _, roleBizId := range roleBizIds {
				userRoleList = append(userRoleList, &entity.UserRole{
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
		err = l.dao.CreateUserRoleList(ctx, userRoleList, tx)
		if err != nil {
			logx.E(ctx, "CreateUserRoleList err:%v,corpBizId:%v,appBizId:%v,roleBizIds:%v,userBizIds:%v",
				corpBizId, appBizId, roleBizIds, userBizIds)
			return errs.ErrCommonFail
		}
		// 3.将third_user_id关联的角色业务ids写入缓存
		if err = l.dao.ModifyThirdUserIDListCache(ctx, appBizId, thirdUserIds, roleBizIds); err != nil {
			logx.E(ctx,
				"ModifyThirdUserIDListCache set redis err:%v,appBizID:%v,thirdUserIds:%v,roleBizIds:%v",
				err, contextx.Metadata(ctx).CorpBizID(), thirdUserIds, roleBizIds)
			return errs.ErrCommonFail
		}
		return nil
	}); err != nil {
		logx.E(ctx, "ModifyUserRoleList err:%+v", err)
		return err
	}
	return nil
}

func (l *Logic) DescribeUserWithRoleList(ctx context.Context,
	corpBizID, appBizID uint64,
	filter *entity.CustUserFilter) ([]*entity.CustUserWithRoleInfo, uint64, error) {
	// 1.先根据条件获取用户列表
	userList, count, err := l.DescribeUserCountAndList(ctx, corpBizID, appBizID, filter)
	if err != nil {
		logx.E(ctx, "DescribeUserCountAndList err:%v,corpBizID:%v,appBizID:%v,filter:%+v",
			err, corpBizID, appBizID, filter)
		return nil, 0, errs.ErrGetUserListFail
	}
	total := uint64(count)
	if total == 0 { // 用户列表为空，直接返回
		return nil, total, nil
	}
	// 2.根据用户业务ids获取绑定的角色业务id
	userBizIDs := make([]uint64, 0)
	custUserWithRoleInfo := make([]*entity.CustUserWithRoleInfo, 0)
	for _, v := range userList {
		userBizIDs = append(userBizIDs, v.BusinessID)
		custUserWithRoleInfo = append(custUserWithRoleInfo,
			&entity.CustUserWithRoleInfo{
				AppID:       v.AppID,
				BusinessID:  v.BusinessID,
				Name:        v.Name,
				ThirdUserID: v.ThirdUserID,
				CreateTime:  v.CreateTime.Unix(),
				UpdateTime:  v.UpdateTime.Unix(),
				RoleList:    []*entity.KnowledgeRole{},
			})
	}
	maxRows := 10000
	userRoleList := make([]*entity.UserRole, 0) // 用户角色绑定列表
	for maxRows == 10000 {
		userRoleFilter := &entity.UserRoleFilter{
			Limit:      maxRows,
			UserBizIDs: userBizIDs,
			Types:      []uint32{entity.UserRolesNormal},
		}
		tmp, err := l.DescribeUserRoleList(ctx, corpBizID, appBizID, userRoleFilter)
		if err != nil {
			logx.E(ctx, "GetUserList GetUserRoleList err:%v,userRoleFilter:%+v", err, userRoleFilter)
			return nil, 0, errs.ErrGetUserRoleListFail
		}
		userRoleList = append(userRoleList, tmp...)
		maxRows = len(tmp)
	}
	if len(userRoleList) == 0 { // 用户绑定的角色为空,直接返回,几乎不可能有这种case
		logx.W(ctx, "DescribeUserWithRoleList userRoleList is empty,corpBizID:%v,appBizID:%v,userBizIDs:%v",
			corpBizID, appBizID, userBizIDs)
		return custUserWithRoleInfo, total, nil
	}
	// 3.根据角色业务ids获取角色信息
	userBizID2RoleInfo, roleBizID2KnowledgeRole, roleBizIDs :=
		make(map[uint64][]*entity.KnowledgeRole, 0), make(map[uint64]*entity.KnowledgeRole), make([]uint64, 0)
	for _, v := range userRoleList {
		// 用户业务id->角色数组 目前只有id 返回时需要回填
		userBizID2RoleInfo[v.UserBizID] = append(userBizID2RoleInfo[v.UserBizID],
			&entity.KnowledgeRole{BusinessID: v.RoleBizID})
		if _, ok := roleBizID2KnowledgeRole[v.RoleBizID]; !ok { // 过滤重复的角色业务id,根据角色业务id构造map，方便后续获取角色信息回填数据
			roleBizIDs = append(roleBizIDs, v.RoleBizID)
			roleBizID2KnowledgeRole[v.RoleBizID] = &entity.KnowledgeRole{}
		}
	}
	roleList := make([]*entity.KnowledgeRole, 0)
	maxRows = 10000
	for maxRows == 10000 {
		_, tmp, err := l.DescribeKnowledgeRoleList(ctx, corpBizID, appBizID,
			&entity.KnowledgeRoleFilter{
				BizIDs: roleBizIDs,
				Limit:  10000,
			})
		if err != nil {
			logx.E(ctx, "GetUserList GetRoleList err:%v", err)
			return nil, 0, errs.ErrGetRoleListFail
		}
		roleList = append(roleList, tmp...)
		maxRows = len(tmp)
	}
	for _, role := range roleList {
		roleBizID2KnowledgeRole[uint64(role.BusinessID)] = &entity.KnowledgeRole{BusinessID: role.BusinessID,
			Name: role.Name}
	}
	// 4.构造返回结构
	for i, v := range custUserWithRoleInfo {
		// 取该用户业务id关联的所有角色
		roleListInfo, ok := userBizID2RoleInfo[v.BusinessID]
		if !ok || len(roleListInfo) == 0 {
			continue
		}
		// 回填角色名称
		for _, role := range roleListInfo {
			tmp, ok := roleBizID2KnowledgeRole[role.BusinessID]
			if !ok || tmp.BusinessID == 0 {
				continue
			}
			custUserWithRoleInfo[i].RoleList = append(custUserWithRoleInfo[i].RoleList, &entity.KnowledgeRole{
				BusinessID: tmp.BusinessID,
				Name:       tmp.Name,
			})
		}
	}
	return custUserWithRoleInfo, total, nil
}

func (l *Logic) DeleteCustUser(ctx context.Context, appBizID uint64, userBizIDs []uint64) (err error) {
	if len(userBizIDs) == 0 {
		return nil
	}
	corpBizId := contextx.Metadata(ctx).CorpBizID()
	// 1.先取用户信息,过滤不存在的用户业务id
	userList, err := l.DescribeUserList(ctx, corpBizId, appBizID,
		&entity.CustUserFilter{
			BizIDs: userBizIDs,
			Limit:  len(userBizIDs),
		})
	if err != nil {
		logx.E(ctx, "DeleteCustUser GetUserList err:%v, appBizID:%d, userBizIDs:%v", err, appBizID,
			userBizIDs)
		return errs.ErrGetUserListFail
	}
	if len(userList) == 0 {
		return nil
	}
	filteredUserBizIDs := make([]uint64, 0, len(userList))
	filteredThirdUserIDs := make([]string, 0, len(userList))
	for _, v := range userList {
		filteredUserBizIDs = append(filteredUserBizIDs, v.BusinessID)
		filteredThirdUserIDs = append(filteredThirdUserIDs, v.ThirdUserID)
	}
	if err := l.dao.Query().Transaction(func(tx *tdsqlquery.Query) error {
		// 2.删除用户数据，用户没有被引用，可以直接删除
		pageSize, length := 200, len(filteredUserBizIDs)
		for start := 0; start < length; start += pageSize {
			deleteMaxRow, end := int64(10000), min(start+pageSize, length)
			tmp := filteredUserBizIDs[start:end]
			for deleteMaxRow == 10000 { // 一次软删除10000条
				deleteMaxRow, err = l.dao.ModifyUser(ctx, corpBizId, appBizID,
					&entity.CustUserFilter{
						BizIDs: tmp,
						Limit:  10000,
					},
					&entity.CustUser{
						IsDeleted: true,
					}, tx)
				if err != nil {
					logx.E(ctx, "DeleteCustUser ModifyUser err:%v, req appBizID:%d, userBizIDs:%v", err,
						appBizID, tmp)
					return errs.ErrDeleteUserFail
				}
			}
			// 3.删除用户角色绑定数据
			deleteMaxRow = 10000
			for deleteMaxRow == 10000 { // 一次软删除10000条
				deleteMaxRow, err = l.dao.ModifyUserRole(ctx, corpBizId, appBizID,
					&entity.UserRoleFilter{
						UserBizIDs: tmp,
						Limit:      10000,
					}, &entity.UserRole{
						IsDeleted: true,
					}, tx)
				if err != nil {
					logx.E(ctx, "UpdateUserRole UpdateUserRole err:%v,req appBizID:%d, userBizIDs:%v", err,
						appBizID, tmp)
					return errs.ErrDeleteUserRoleFail
				}
			}
		}
		return nil
	}); err != nil {
		logx.E(ctx, "DeleteCustUser Transaction err:%v, appBizID:%d, userBizIDs:%v", err, appBizID,
			userBizIDs)
		return err
	}

	// 4.删除third_user_id缓存
	if err = l.dao.DeleteThirdUserIDCache(ctx, appBizID, filteredThirdUserIDs); err != nil {
		logx.E(ctx, "UpdateUserRole del redis err:%v,appBizID:%v,thirdUserIds:%v", err, appBizID,
			filteredThirdUserIDs)
		return errs.ErrCommonFail
	}
	return nil
}

// ModifyCustUserConfig 设置特殊权限配置
func (l *Logic) ModifyCustUserConfig(ctx context.Context, corpBizID, appBizID,
	notSetRoleBizID, notUseRoleBizID uint64) error {
	key := fmt.Sprintf(ModifyCustUserConfig, appBizID)
	lock := distributedlockx.NewRedisLock(l.lockRdb, key, distributedlockx.WithTTL(2*time.Second))
	success, err := lock.Lock(ctx)
	if err != nil {
		logx.E(ctx, "Lock err:%v", err)
		return errs.ErrCommonFail
	}
	if success { // 加锁成功
		defer func() {
			if err = lock.Unlock(ctx); err != nil {
				logx.E(ctx, "Unlock err:%v", err)
			}
		}()
	} else {
		logx.W(ctx, "ModifyRole lock failed, appBizID:%d", appBizID)
		return errs.ErrSetThirdAclConfigLock
	}
	err = l.ModifyUserConfig(ctx, corpBizID, appBizID, notSetRoleBizID, notUseRoleBizID)
	if err != nil {
		logx.E(ctx, "ModifyUserConfig err:%v,corpBizID:%v,appBizID:%v,notSetRoleBizID:%v,notUseRoleBizID:%v",
			err, corpBizID, appBizID, notSetRoleBizID, notUseRoleBizID)
		return errs.ErrSetCustUserConfigFail
	}
	// 写入缓存,供运行时检索使用
	if err = l.dao.ModifyUserConfigCache(ctx, appBizID, notSetRoleBizID, notUseRoleBizID); err != nil { // 柔性放过
		logx.E(ctx, "SetCustUserConfig set redis err:%v,appBizID:%v,notSetRoleBizID:%v,notUseRoleBizID:%v",
			err, notSetRoleBizID, notUseRoleBizID)
	}
	return nil
}

// DescribeCustUserConfig 获取特殊权限配置
func (l *Logic) DescribeCustUserConfig(ctx context.Context, appBizID uint64) (
	userConfig *entity.CustUserConfig, err error) {
	corpBizId := contextx.Metadata(ctx).CorpBizID()
	// 1.先获取本应用的特殊权限配置
	userRoleList, err := l.DescribeUserRoleList(ctx, corpBizId, appBizID,
		&entity.UserRoleFilter{
			Types: []uint32{entity.NotSetThirdUserId, entity.NotUseThirdUserId},
			Limit: 2,
		}) // 目前是一对一
	if err != nil {
		logx.E(ctx, "GetCustUserConfig GetUserRoleList err:%v,appBizID:%d", err, appBizID)
		return nil, errs.ErrGetUserConfigFail
	}
	presetBizId, presetRoleName, needInit, roleBizIds := uint64(0), "", false, make([]uint64, 0, 2)
	if len(userRoleList) != 2 { // 如果没有设置过,取默认角色填充
		needInit = true
		presetBizId, presetRoleName, err = l.VerifyPresetRole(ctx, appBizID)
		if err != nil {
			logx.E(ctx, "GetCustUserConfig CheckPresetRole err:%v", err)
			return nil, errs.ErrGetRoleListFail
		}
	}
	for _, v := range userRoleList {
		if v.RoleBizID != 0 {
			roleBizIds = append(roleBizIds, v.RoleBizID)
		}
	}
	// 2.获取角色详情
	roleByRole := make(map[uint64]*entity.KnowledgeRole, 0)
	if len(roleBizIds) != 0 {
		_, tmp, err := l.DescribeKnowledgeRoleList(ctx, corpBizId, appBizID,
			&entity.KnowledgeRoleFilter{
				BizIDs: roleBizIds,
				Limit:  len(roleBizIds),
			})
		if err != nil {
			logx.E(ctx, "GetCustUserConfig GetRoleList err:%v,appBizID:%+v", err, appBizID)
			return nil, errs.ErrGetRoleListFail
		}
		for _, role := range tmp {
			roleByRole[uint64(role.BusinessID)] = &entity.KnowledgeRole{BusinessID: role.BusinessID, Name: role.Name}
		}
	}
	// 3.构造返回结构
	userConfig = &entity.CustUserConfig{ // 假如从未设置过,初始化结构
		NotUserIdRoleInfo: &entity.KnowledgeRole{
			BusinessID: presetBizId,
			Name:       presetRoleName,
		},
		NotUseRoleInfo: &entity.KnowledgeRole{
			BusinessID: presetBizId,
			Name:       presetRoleName,
		},
	}
	for _, v := range userRoleList {
		if v.Type == entity.NotSetThirdUserId {
			if v.RoleBizID != 0 {
				userConfig.NotUserIdRoleInfo.BusinessID = v.RoleBizID
				if _, ok := roleByRole[v.RoleBizID]; ok {
					userConfig.NotUserIdRoleInfo.Name = roleByRole[v.RoleBizID].Name
				}
			} else { // 为0代表不检索知识
				userConfig.NotUserIdRoleInfo.BusinessID = 0
				userConfig.NotUserIdRoleInfo.Name = entity.NotSearchKnowledge
			}
		} else if v.Type == entity.NotUseThirdUserId {
			if v.RoleBizID != 0 {
				userConfig.NotUseRoleInfo.BusinessID = v.RoleBizID
				if _, ok := roleByRole[v.RoleBizID]; ok {
					userConfig.NotUseRoleInfo.Name = roleByRole[v.RoleBizID].Name
				}
			} else { // 为0代表不检索知识
				userConfig.NotUseRoleInfo.BusinessID = 0
				userConfig.NotUseRoleInfo.Name = entity.NotSearchKnowledge
			}
		}
	}
	if needInit { // 第一次获取的时候初始化
		key := fmt.Sprintf(ModifyCustUserConfig, appBizID)
		lock := distributedlockx.NewRedisLock(l.lockRdb, key, distributedlockx.WithTTL(2*time.Second))
		success, err := lock.Lock(ctx)
		if err != nil {
			logx.E(ctx, "Lock err:%v", err)
			return nil, errs.ErrCommonFail
		}
		if success { // 加锁成功
			defer func() {
				if err = lock.Unlock(ctx); err != nil {
					logx.E(ctx, "Unlock err:%v", err)
				}
			}()
		} else {
			logx.W(ctx, "ModifyRole lock failed, appBizID:%d", appBizID)
			return nil, errs.ErrAlreadyLocked
		}
		err = l.ModifyUserConfig(ctx, corpBizId, appBizID, userConfig.NotUserIdRoleInfo.BusinessID,
			userConfig.NotUseRoleInfo.BusinessID)
		if err != nil {
			logx.E(ctx,
				"DescribeCustUserConfig ModifyUserConfig err:%v,corpBizId:%v,appBizId:%v,userConfig:%+v",
				err, corpBizId, appBizID, userConfig)
			return nil, errs.ErrGetUserConfigFail
		}
	}
	return userConfig, nil
}

func (l *Logic) DescribeAclConfigStatus(ctx context.Context, appBizID uint64) (uint32, error) {
	md := contextx.Metadata(ctx)
	// 获取应用引用共享库列表
	shareKGList, err := l.kbDao.GetAppShareKGList(ctx, appBizID)
	if err != nil {
		logx.E(ctx, "GetAppShareKGList failed, err: %+v", err)
		return 0, errs.ErrGetAppShareKGListFailed
	}
	appBizIDs := []uint64{appBizID}
	for _, shareKG := range shareKGList {
		// 共享知识库底层通过应用实现，KnowledgeBizID即为AppBizID
		appBizIDs = append(appBizIDs, shareKG.KnowledgeBizID)
	}
	appBaseInfos, _, err := l.rpc.AppAdmin.ListAppBaseInfo(ctx, &appconfig.ListAppBaseInfoReq{AppBizIds: appBizIDs})
	if err != nil {
		logx.E(ctx, "rpc ListAppBaseInfo failed, err: %+v", err)
		return 0, err
	}
	hasKnowledge := false
	// 1.判断知识库是否有内容
	for _, appBaseInfo := range appBaseInfos {
		hasKnowledge, err = l.checkKBHasKnowledge(ctx, appBaseInfo)
		if err != nil {
			logx.E(ctx, "checkKBHasKnowledge failed, err: %+v", err)
			return 0, err
		}
		if hasKnowledge {
			// 只要默认知识库或者共享知识库任意一个有知识，就算这个应用有知识
			break
		}
	}
	if !hasKnowledge {
		return kbEntity.NotKnowledge, nil
	}
	// 2.知识库有内容，判断是否创建了用户
	count, err := l.DescribeUserCount(ctx, md.CorpBizID(), appBizID, nil)
	if err != nil {
		logx.E(ctx, "GetAclConfigStatus GetUserCount err:%v,appBizID:%d", err, appBizID)
		return 0, errs.ErrGetUserListFail
	}
	if count == 0 {
		return kbEntity.NotAppUser, nil
	}
	// 3.判断是否对接了外部权限
	configs, err := l.kbDao.GetShareKnowledgeConfigs(ctx, md.CorpBizID(),
		[]uint64{appBizID}, []uint32{uint32(pb.KnowledgeBaseConfigType_THIRD_ACL)})
	if err != nil {
		logx.E(ctx, "GetAclConfigStatus GetAppConfig err:%v,appBizID:%d", err, appBizID)
		return 0, errs.ErrGetThirdAclFail
	}
	if len(configs) != 0 {
		return kbEntity.HasThirdAcl, nil
	}
	return kbEntity.HasAppUser, nil
}

func (l *Logic) DescribeRoleSearchLabel(ctx context.Context, corpBizID, appID, appBizID uint64, lkeUserID string) (
	searchLabels map[uint64]*retrieval.LabelExpression, notSearch bool, err error) {
	logx.D(ctx, "DescribeRoleSearchLabel corpBizID:%v,appBizID:%v,lkeUserID:%v", corpBizID, appBizID, lkeUserID)
	// 1.先获取应用的特殊权限配置
	md := contextx.Metadata(ctx)
	md.WithCorpBizID(corpBizID)
	md.WithAppID(appID)
	notSetUserRoleBizID, notUseUserRoleBizID, err := l.describeUserConfigFromCache(ctx, appBizID)
	if err != nil {
		if err == errs.ErrAlreadyLocked {
			return nil, false, err
		}
		logx.W(ctx, "DescribeRoleSearchLabel get aclConfig err:%v,appBizID:%v,lkeUserID:%v", err, appBizID, lkeUserID)
		return nil, false, err
	}
	// 2.如果lke_user_id为空使用未传入配置
	if lkeUserID == "" {
		if notSetUserRoleBizID == entity.NotRoleBizId { // 为0代表不检索
			return nil, true, nil
		}
		vectorLabel, needSkip, err := l.formatFilter(ctx, &FormatFilterReq{
			CorpBizID: corpBizID, AppID: appID, AppBizID: appBizID,
			RoleBizID: notSetUserRoleBizID,
		})
		if err != nil {
			logx.E(ctx, "DescribeRoleSearchLabel get acl role err:%v,roleBizID:%v", err, notSetUserRoleBizID)
			return nil, false, err
		}
		if !needSkip {
			return vectorLabel, false, nil
		}
		// 如果未传入角色配置为空,走无可使用配置兜底
		logx.W(ctx, "DescribeRoleSearchLabel role isEmpty err:%v,notSetUserRoleBizID:%v", err, notSetUserRoleBizID)
		return l.describeNotUseRoleLabel(ctx, corpBizID, appID, appBizID, notUseUserRoleBizID)
	}
	// 3.不为空根据lke_user_id取角色业务ids
	userRoleBizIDs, err := l.describeThirdUserIDFromCache(ctx, appBizID, lkeUserID)
	if err != nil {
		logx.E(ctx, "DescribeRoleSearchLabel GetThirdUserIDRedis err:%v,appBizID:%v,lkeUserID:%v", err, appBizID, lkeUserID)
		return nil, false, err
	}
	// 4.如果根据lke_user_id没有取到可用角色,根据无可使用配置处理
	if len(userRoleBizIDs) == 0 {
		return l.describeNotUseRoleLabel(ctx, corpBizID, appID, appBizID, notUseUserRoleBizID)
	}
	// 5.根据角色获取向量标签
	searchLabels = make(map[uint64]*retrieval.LabelExpression, 0)
	for _, roleBizID := range userRoleBizIDs {
		vectorLabel, needSkip, err := l.formatFilter(ctx, &FormatFilterReq{
			CorpBizID: corpBizID, AppID: appID, AppBizID: appBizID,
			RoleBizID: roleBizID,
		})
		if err != nil {
			logx.E(ctx, "DescribeRoleSearchLabel get role label err:%v,roleBizID:%v", err, roleBizID)
			continue
		}
		if needSkip { // 角色配置为空,跳过
			logx.W(ctx, "DescribeRoleSearchLabel role isEmpty err:%v,roleBizID:%v", err, roleBizID)
			continue
		}
		if len(vectorLabel) == 0 { // 如果其中一个角色没有配置,跳过
			continue
		}
		for knowBizID, labelExpression := range vectorLabel {
			// 代表可以查看这个知识库所有知识
			if labelExpression == nil || len(labelExpression.Expressions) == 0 {
				searchLabels[knowBizID] = &retrieval.LabelExpression{}
				continue
			}
			knowLabel, ok := searchLabels[knowBizID]
			if !ok {
				searchLabels[knowBizID] = &retrieval.LabelExpression{
					Operator:    retrieval.LabelExpression_OR, // 多个角色之前取或关系
					Expressions: []*retrieval.LabelExpression{labelExpression},
				}
				continue
			}
			if len(knowLabel.Expressions) == 0 { // 已经为该知识库全部知识,不需要再拼接了
				continue
			}
			knowLabel.Expressions = append(knowLabel.Expressions, labelExpression)
		}
	}
	// 所有角色配置都为空,走无可使用配置兜底
	if len(searchLabels) == 0 {
		return l.describeNotUseRoleLabel(ctx, corpBizID, appID, appBizID, notUseUserRoleBizID)
	}
	return searchLabels, false, nil
}

func (l *Logic) describeUserConfigFromCache(ctx context.Context, appBizID uint64) (notSet, notUse uint64, err error) {
	// 1.从缓存取特殊权限配置
	notSet, notUse, err = l.dao.DescribeUserConfigCache(ctx, appBizID)
	if err != nil { // 2.没取到,走db写入一次
		aclConfig, err := l.DescribeCustUserConfig(ctx, appBizID)
		if err != nil {
			if err == errs.ErrAlreadyLocked {
				return 0, 0, err
			}
			logx.E(ctx, "DescribeCustUserConfig err:%v,appBizID:%d", err, appBizID)
			return 0, 0, err
		}
		notSet, notUse = aclConfig.NotUserIdRoleInfo.BusinessID, aclConfig.NotUseRoleInfo.BusinessID
		// 写入缓存
		if err = l.dao.ModifyUserConfigCache(ctx, appBizID, notSet, notUse); err != nil { // 柔性放过
			logx.E(ctx, "ModifyUserConfigCache set redis err:%v,appBizId:%d,notSet:%d,notUse:%d", err,
				appBizID, notSet, notUse)
		}
		return notSet, notUse, nil
	}
	return notSet, notUse, nil
}

// describeThirdUserIDFromCache 从缓存获取third_user_id关联的角色业务ids,供运行时使用
func (l *Logic) describeThirdUserIDFromCache(ctx context.Context, appBizID uint64,
	thirdUserID string) (roleBizIDs []uint64, err error) {
	roleBizIDs, err = l.dao.DescribeThirdUserIDCache(ctx, appBizID, thirdUserID)
	logx.D(ctx, "describeThirdUserIDFromCache roleBizIDs:%v,err:%v", roleBizIDs, err)
	if err != nil { // 获取redis为空,取db数据
		userRoleList, err := l.DescribeUserRoleList(ctx, contextx.Metadata(ctx).CorpBizID(), appBizID,
			&entity.UserRoleFilter{
				ThirdUserID: thirdUserID,
				Types: []uint32{
					entity.UserRolesNormal,
				},
			})
		if err != nil {
			logx.E(ctx, "GetThirdUserIDRedis GetUserRoleList err:%v", err)
			return nil, err
		}
		for _, v := range userRoleList {
			roleBizIDs = append(roleBizIDs, v.RoleBizID)
		}
		logx.D(ctx, "DescribeUserRoleList res: %+v", roleBizIDs)
		// 把db数据写入缓存
		if err = l.dao.ModifyThirdUserIDCache(ctx, appBizID, thirdUserID, roleBizIDs); err != nil { // 柔性放过
			logx.E(ctx, "GetThirdUserIDRedis set redis err:%v,appBizID:%v,thirdUserID:%v,roleBizIDs:%v",
				err, appBizID, thirdUserID, roleBizIDs)
		}
		return roleBizIDs, nil
	}
	return roleBizIDs, nil
}

// describeNotUseRoleLabel 获取无可使用配置兜底
func (l *Logic) describeNotUseRoleLabel(ctx context.Context, corpBizID, appID, appBizID, notUseUserRoleBizID uint64) (
	map[uint64]*retrieval.LabelExpression, bool, error) {
	if notUseUserRoleBizID == entity.NotRoleBizId { // 为0代表不检索
		return nil, true, nil
	}
	vectorLabel, needSkip, err := l.formatFilter(ctx, &FormatFilterReq{
		CorpBizID: corpBizID,
		AppID:     appID,
		AppBizID:  appBizID,
		RoleBizID: notUseUserRoleBizID,
	})
	if err != nil {
		logx.E(ctx, "GetRoleSearchLabel getNotUseRoleLabel get acl role err:%v,roleBizID:%v", err,
			notUseUserRoleBizID)
		return nil, false, err
	}
	if needSkip { // 两个特殊权限配置都为空,不检索
		logx.W(ctx, "GetRoleSearchLabel getNotUseRoleLabel role isEmpty err:%v,roleBizID:%v", err,
			notUseUserRoleBizID)
		return nil, true, nil
	}
	return vectorLabel, false, nil
}

// checkKBHasKnowledge 检查知识库是否有内容
func (l *Logic) checkKBHasKnowledge(ctx context.Context, appDB *entity.AppBaseInfo) (bool, error) {
	md := contextx.Metadata(ctx)
	// 检查文档
	docDB, err := knowClient.GormClient(ctx, l.docLogic.GetDao().Query().TDoc.TableName(), appDB.PrimaryId, appDB.BizId,
		[]client.Option{}...)
	if err != nil {
		logx.E(ctx, "get GormClient failed, err: %+v", err)
		return false, err
	}
	docCount, err := l.docLogic.GetDao().GetDocCountWithFilter(ctx, []string{"id"}, &docEntity.DocFilter{
		CorpId: md.CorpID(), RobotId: appDB.PrimaryId, IsDeleted: ptrx.Bool(false),
		Limit: 1,
	}, docDB)
	if err != nil {
		logx.E(ctx, "checkKBHasKnowledge GetDocCountWithFilter err:%v,appBizID:%d", err, appDB.BizId)
		return false, errs.ErrGetKnowledgeFailed
	}
	if docCount > 0 {
		return true, nil
	}
	// 检查问答
	qaList, err := l.qaLogic.GetAllDocQas(ctx, []string{"id"}, &qaEntity.DocQaFilter{
		CorpId: md.CorpID(), RobotId: appDB.PrimaryId, IsDeleted: ptrx.Uint32(qaEntity.QAIsNotDeleted),
		Limit: 1,
	})
	if err != nil {
		logx.E(ctx, "checkKBHasKnowledge GetDocQas err:%v,appBizID:%d", err, appDB.BizId)
		return false, errs.ErrGetKnowledgeFailed
	}
	if len(qaList) > 0 {
		return true, nil
	}
	// 检查数据库
	dbFilter := dbEntity.DatabaseFilter{
		CorpBizID:  md.CorpBizID(),
		AppBizID:   appDB.BizId,
		PageNumber: ptrx.Uint32(1),
		PageSize:   1,
	}
	dbCount, err := l.dbDao.CountDatabase(ctx, &dbFilter)
	if err != nil {
		logx.E(ctx, "checkKBHasKnowledge ListByAppBizID err:%v,appBizID:%d", err, appDB.BizId)
		return false, errs.ErrGetKnowledgeFailed
	}
	if dbCount > 0 {
		return true, nil
	}
	// 文档、问答、数据库全都为空
	return false, nil
}
