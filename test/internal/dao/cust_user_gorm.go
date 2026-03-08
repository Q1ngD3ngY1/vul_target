package dao

import (
	"context"
	"fmt"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"gorm.io/gorm"
)

type CustUserDao struct {
	DB *gorm.DB
}

// GetCustUserDao 获取全局的数据操作对象
func GetCustUserDao(db *gorm.DB) *CustUserDao {
	if db == nil {
		db = globalBaseDao.tdsqlGormDB
	}
	return &CustUserDao{DB: db}
}

// CustUserFilter 用户表过滤条件
type CustUserFilter struct {
	KnowledgeBase
	IDs             []uint64
	BusinessId      uint64
	BusinessIds     []uint64
	ThirdUserId     string //第三方用户id
	Name            string //用户名称
	ThirdUserIdLike string //第三方用户id，模糊搜索
	NameLike        string //用户名称，模糊搜索
	Query           string
}

// GetUserCountAndList 获取用户总数和分页列表
func (d *CustUserDao) GetUserCountAndList(ctx context.Context, selectColumns []string,
	filter *CustUserFilter) ([]*model.CustUser, int64, error) {
	count, err := d.GetUserCount(ctx, filter)
	if err != nil {
		log.ErrorContextf(ctx, "GetUserCountAndList GetUserCount err:%v,select:%v,filter:%+v",
			err, selectColumns, filter)
		return nil, 0, err
	}
	if count == 0 {
		return nil, count, nil
	}
	list, err := d.GetUserList(ctx, selectColumns, filter)
	if err != nil {
		log.ErrorContextf(ctx, "GetUserCountAndList GetUserList err:%v,select:%v,filter:%+v",
			err, selectColumns, filter)
		return nil, 0, err
	}
	return list, count, nil
}

// GetUserCount 获取用户总数
func (d *CustUserDao) GetUserCount(ctx context.Context, filter *CustUserFilter) (int64, error) {
	sql := d.DB.WithContext(ctx).Model(&model.CustUser{})
	d.makeUserCondition(sql, filter)
	count := int64(0)
	err := sql.Limit(1).Count(&count).Error
	if err != nil {
		log.ErrorContextf(ctx, "GetUserCount sql failed, err: %+v,filter:%+v", err, filter)
		return 0, err
	}
	return count, nil
}

// GetUserList 获取用户列表
func (d *CustUserDao) GetUserList(ctx context.Context, selectColumns []string,
	filter *CustUserFilter) ([]*model.CustUser, error) {
	sql := d.DB.WithContext(ctx).Model(&model.CustUser{})
	if len(selectColumns) != 0 {
		sql.Select(selectColumns)
	}
	d.makeUserCondition(sql, filter)
	if filter.Offset > 0 {
		sql.Offset(filter.Offset)
	}
	if filter.Limit > 0 {
		sql.Limit(filter.Limit)
	}
	dir := "asc"
	if filter.OrderDirection == 1 {
		dir = "desc"
	}
	for _, column := range filter.OrderColumn {
		sql.Order(column + " " + dir)
	}
	var userList []*model.CustUser
	err := sql.Find(&userList).Error
	if err != nil {
		log.ErrorContextf(ctx, "GetUserList err:%v,filter:%+v", err, filter)
		return nil, err
	}
	return userList, nil
}

// makeUserCondition 生成用户表查询条件
func (d *CustUserDao) makeUserCondition(sql *gorm.DB, filter *CustUserFilter) {
	if filter.CorpBizID != 0 {
		sql.Where(model.UserTblColCorpBizId+sqlEqual, filter.CorpBizID)
	}
	if filter.AppBizID != 0 {
		sql.Where(model.UserTblColAppBizId+sqlEqual, filter.AppBizID)
	}
	if filter.IsDeleted != nil {
		sql.Where(model.UserTblColIsDeleted+sqlEqual, *filter.IsDeleted)
	}
	if filter.BusinessId != 0 {
		sql.Where(model.UserTblColBusinessId+sqlEqual, filter.BusinessId)
	}
	if len(filter.BusinessIds) != 0 {
		sql.Where(model.UserTblColBusinessId+sqlIn, filter.BusinessIds)
	}
	if len(filter.ThirdUserId) != 0 {
		sql.Where(model.UserTblColThirdUserId+sqlEqual, filter.ThirdUserId)
	}
	if len(filter.Name) != 0 {
		sql.Where(model.UserTblColName+sqlEqual, filter.Name)
	}
	if len(filter.ThirdUserIdLike) != 0 {
		newStr := fmt.Sprintf("%%%s%%", fileNameReplacer.Replace(filter.ThirdUserIdLike))
		sql.Where(model.UserTblColThirdUserId+sqlLike, newStr)
	}
	if len(filter.NameLike) != 0 {
		newStr := fmt.Sprintf("%%%s%%", fileNameReplacer.Replace(filter.NameLike))
		sql.Where(model.UserTblColName+sqlLike, newStr)
	}
	if len(filter.Query) != 0 {
		query := fmt.Sprintf("%%%s%%", fileNameReplacer.Replace(filter.Query))
		sql.Where(model.UserTblColName+sqlLike+sqlOr+model.UserTblColThirdUserId+sqlLike, query, query)
	}
}

// InserCustUser 新增用户
func (d *CustUserDao) InserCustUser(ctx context.Context, custUserInfo *model.CustUser) (id uint64, err error) {
	err = d.DB.Create(custUserInfo).Error
	if err != nil {
		log.ErrorContextf(ctx, "CreateUser err:%v,user:%+v", err, &custUserInfo)
		return 0, err
	}
	return custUserInfo.ID, nil
}

// UpdateUser 更新用户数据
func (d *CustUserDao) UpdateUser(ctx context.Context, setData map[string]interface{},
	filter *CustUserFilter) (rows int64, err error) {
	sql := d.DB.WithContext(ctx).Model(&model.CustUser{})
	d.makeUserCondition(sql, filter)
	if filter.Limit != 0 {
		sql.Limit(filter.Limit)
	}
	setData[model.UserTblColUpdateTime] = time.Now()
	res := sql.Updates(setData)
	if res.Error != nil {
		log.ErrorContextf(ctx, "UpdateUserRole err:%v,setData:%+v,filter:%+v", err, setData, filter)
		return 0, res.Error
	}
	return res.RowsAffected, nil
}

type UserRoleFilter struct {
	KnowledgeBase
	IDs         []uint64
	UserBizId   uint64
	UserBizIds  []uint64
	ThirdUserId string  //第三方用户id
	Type        *uint32 //类型，0默认，1没传第三方用户id兜底配置，2传了匹配为空兜底设置
	Types       []uint32
	RoleBizIds  []uint64
}

// InserCustUser 新增用户角色关系
func (d *CustUserDao) InserUserRole(ctx context.Context, userRoleList []*model.UserRole) (err error) {
	if len(userRoleList) == 0 {
		return nil
	}
	err = d.DB.CreateInBatches(userRoleList, 200).Error
	if err != nil {
		log.ErrorContextf(ctx, "CreateUser err:%v", err)
		return err
	}
	return nil
}

// UpdateUserRole 更新用户角色关系
func (d *CustUserDao) UpdateUserRole(ctx context.Context, setData map[string]interface{},
	filter *UserRoleFilter) (rows int64, err error) {
	sql := d.DB.WithContext(ctx).Model(&model.UserRole{})
	d.makeUserRoleCondition(sql, filter)
	if filter.Limit != 0 {
		sql.Limit(filter.Limit)
	}
	setData[model.UserRoleTblColUpdateTime] = time.Now()
	res := sql.Updates(setData)
	if res.Error != nil {
		log.ErrorContextf(ctx, "UpdateUserRole err:%v,setData:%+v,filter:%+v", err, setData, filter)
		return 0, res.Error
	}
	return res.RowsAffected, nil
}

// makeUserRoleCondition 生成用户角色表查询条件
func (d *CustUserDao) makeUserRoleCondition(sql *gorm.DB, filter *UserRoleFilter) {
	if filter.CorpBizID != 0 {
		sql.Where(model.UserRoleTblColCorpBizId+sqlEqual, filter.CorpBizID)
	}
	if filter.AppBizID != 0 {
		sql.Where(model.UserRoleTblColAppBizId+sqlEqual, filter.AppBizID)
	}
	if filter.IsDeleted != nil {
		sql.Where(model.UserRoleTblColIsDeleted+sqlEqual, *filter.IsDeleted)
	}
	if filter.UserBizId != 0 {
		sql.Where(model.UserRoleTblColUserBizId+sqlEqual, filter.UserBizId)
	}
	if len(filter.UserBizIds) != 0 {
		sql.Where(model.UserRoleTblColUserBizId+sqlIn, filter.UserBizIds)
	}
	if len(filter.ThirdUserId) != 0 {
		sql.Where(model.UserRoleTblColThirdUserId+sqlEqual, filter.ThirdUserId)
	}
	if filter.Type != nil {
		sql.Where(model.UserRoleTblColType+sqlEqual, *filter.Type)
	}
	if len(filter.RoleBizIds) != 0 {
		sql.Where(model.UserRoleTblColRoleUserId+sqlIn, filter.RoleBizIds)
	}
}

// GetUserRoleList 获取用户角色绑定关系列表
func (d *CustUserDao) GetUserRoleList(ctx context.Context, selectColumns []string,
	filter *UserRoleFilter) ([]*model.UserRole, error) {
	sql := d.DB.WithContext(ctx).Model(&model.UserRole{})
	if len(selectColumns) != 0 {
		sql.Select(selectColumns)
	}
	d.makeUserRoleCondition(sql, filter)
	if filter.Offset > 0 {
		sql.Offset(filter.Offset)
	}
	if filter.Limit > 0 {
		sql.Limit(filter.Limit)
	}
	dir := "asc"
	if filter.OrderDirection == 1 {
		dir = "desc"
	}
	for _, column := range filter.OrderColumn {
		sql.Order(column + "" + dir)
	}
	var userRoleList []*model.UserRole
	err := sql.Find(&userRoleList).Error
	if err != nil {
		log.ErrorContextf(ctx, "GetUserRoleList err:%v,filter:%+v", err, filter)
		return nil, err
	}
	return userRoleList, nil
}

// SetCustUserConfig 设置特殊权限配置
func (d *CustUserDao) SetCustUserConfig(ctx context.Context, corpBizId, appBizId,
	notSetRoleBizId, notUseRoleBizId uint64) error {
	//1.先获取
	var userRoleList []*model.UserRole
	err := d.DB.WithContext(ctx).Model(&model.UserRole{}).Where(model.UserRoleTblColIsDeleted+sqlEqual, 0).
		Where(model.UserRoleTblColCorpBizId+sqlEqual, corpBizId).Where(model.UserRoleTblColAppBizId+sqlEqual, appBizId).
		Where(model.UserRoleTblColType+sqlIn, []uint32{model.NotSetThirdUserId, model.NotUseThirdUserId}).
		Limit(2).Find(&userRoleList).Error
	if err != nil {
		log.ErrorContextf(ctx, "SetCustUserConfig get err:%v,corpBizId:%v,appBizId:%v,"+
			"notSetRoleBizId:%v, notUseRoleBizId:%v", err, corpBizId, appBizId, notSetRoleBizId, notUseRoleBizId)
		return err
	}
	//2.已存在更新
	notSet, notUse := false, false
	for _, v := range userRoleList {
		if v.Type == model.NotSetThirdUserId {
			err = d.DB.WithContext(ctx).Model(&model.UserRole{}).Where(model.UserRoleTblColId+sqlEqual, v.ID).
				Limit(1).Updates(map[string]interface{}{
				model.UserRoleTblColRoleUserId: notSetRoleBizId, model.UserRoleTblColUpdateTime: time.Now(),
			}).Error
			if err != nil {
				log.ErrorContextf(ctx, "SetCustUserConfig update err:%v,corpBizId:%v,appBizId:%v,"+
					"notSetRoleBizId:%v, notUseRoleBizId:%v", err, corpBizId, appBizId, notSetRoleBizId, notUseRoleBizId)
				return err
			}
			notSet = true
		} else if v.Type == model.NotUseThirdUserId {
			err = d.DB.WithContext(ctx).Model(&model.UserRole{}).Where(model.UserRoleTblColId+sqlEqual, v.ID).
				Limit(1).Updates(map[string]interface{}{
				model.UserRoleTblColRoleUserId: notUseRoleBizId, model.UserRoleTblColUpdateTime: time.Now(),
			}).Error
			if err != nil {
				log.ErrorContextf(ctx, "SetCustUserConfig update err:%v,corpBizId:%v,appBizId:%v,"+
					"notSetRoleBizId:%v, notUseRoleBizId:%v", err, corpBizId, appBizId, notSetRoleBizId, notUseRoleBizId)
				return err
			}
			notUse = true
		}
	}
	//3.不存在创建
	if !notSet {
		err = d.DB.WithContext(ctx).Create(&model.UserRole{
			CorpID:     corpBizId,
			AppID:      appBizId,
			Type:       model.NotSetThirdUserId,
			RoleBizID:  notSetRoleBizId,
			IsDeleted:  0,
			CreateTime: time.Now(),
			UpdateTime: time.Now(),
		}).Error
		if err != nil {
			log.ErrorContextf(ctx, "SetCustUserConfig set err:%v,corpBizId:%v,appBizId:%v,"+
				"notSetRoleBizId:%v, notUseRoleBizId:%v", err, corpBizId, appBizId, notSetRoleBizId, notUseRoleBizId)
			return err
		}
	}
	if !notUse {
		err = d.DB.WithContext(ctx).Create(&model.UserRole{
			CorpID:     corpBizId,
			AppID:      appBizId,
			Type:       model.NotUseThirdUserId,
			RoleBizID:  notUseRoleBizId,
			IsDeleted:  0,
			CreateTime: time.Now(),
			UpdateTime: time.Now(),
		}).Error
		if err != nil {
			log.ErrorContextf(ctx, "SetCustUserConfig set err:%v,corpBizId:%v,appBizId:%v,"+
				"notSetRoleBizId:%v, notUseRoleBizId:%v", err, corpBizId, appBizId, notSetRoleBizId, notUseRoleBizId)
			return err
		}
	}
	return nil
}
