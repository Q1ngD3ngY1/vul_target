package dao

import (
	"context"
	"fmt"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"github.com/jmoiron/sqlx"
)

const (
	userFields = `
		id,sid,uin,sub_account_uin,nick_name,avatar,cellphone,account,password,status,create_time,update_time
	`
	getUserByTel = `
		SELECT 
			%s 
		FROM 
		    t_user 
		WHERE 
		    cellphone = ?
	`
	createUser = `
		INSERT INTO 
		    t_user (%s) 
		VALUES 
		    (null,:sid,:uin,:sub_account_uin,:nick_name,:avatar,:cellphone,:account,:password,:status,:create_time,
			:update_time)
	`
	getUserByID = `
		SELECT 
			%s 
		FROM 
		    t_user 
		WHERE 
		    id = ?
	`
	getUserBySidAndUin = `
		SELECT 
			%s 
		FROM 
		    t_user 
		WHERE 
		    sid = ? AND uin = ? AND sub_account_uin = ?
	`
	getUserByAccount = `
		SELECT 
			%s 
		FROM 
		    t_user 
		WHERE 
		    account = ?
	`
	getUserByIDs = `
		SELECT 
			%s 
		FROM 
		    t_user 
		WHERE 
		    id IN (%s)
	`
	updateUserPassword = `
		UPDATE
			t_user
		SET
			password = :password
		WHERE
			id = :id
	`
	getSIUser = `
		SELECT 
			%s 
		FROM 
		    t_user 
		WHERE 
		    sid = ? AND uin = ? AND sub_account_uin = ?
	`
	updateUserUin = `
		UPDATE 
			t_user 
		SET 
		    uin = :uin,
		    sub_account_uin = :sub_account_uin,
		    nick_name = :nick_name 
		WHERE 
		    id = :id
	`
	getSIUserList = `
		SELECT 
			%s 
		FROM 
		    t_user 
		WHERE 
		    sid = ? %s
	`
	updateUserCloudNickname = `
		UPDATE 
			t_user 
		SET 
		    nick_name = :nick_name 
		WHERE 
		    id = :id
	`
	expUserFields = `
        id,business_id,sid,nick_name,avatar,cellphone,status,create_time,update_time
    `
	getExpUserByID = `
		SELECT 
			%s 
		FROM 
		    t_exp_user 
		WHERE 
		    id = ?
    `
	getExpUserByIDs = `
        SELECT
            %s
        FROM
            t_exp_user
        WHERE
            id IN (?)
    `
)

// GetUserByTel 获取用户信息
func (d *dao) GetUserByTel(ctx context.Context, cellphone string) (*model.User, error) {
	args := make([]any, 0, 1)
	args = append(args, cellphone)
	querySQL := fmt.Sprintf(getUserByTel, userFields)
	users := make([]*model.User, 0)
	if err := d.db.QueryToStructs(ctx, &users, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取用户信息失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	if len(users) == 0 {
		return nil, nil
	}
	return users[0], nil
}

// CreateUser 创建用户
func (d *dao) CreateUser(ctx context.Context, user *model.User) error {
	now := time.Now()
	user.UpdateTime = now
	user.CreateTime = now
	querySQL := fmt.Sprintf(createUser, userFields)
	res, err := d.db.NamedExec(ctx, querySQL, user)
	if err != nil {
		log.ErrorContextf(ctx, "新增用户失败 sql:%s args:%+v err:%+v", querySQL, user, err)
		return err
	}
	userID, _ := res.LastInsertId()
	user.ID = uint64(userID)
	return nil
}

// GetUserByID 获取用户信息
func (d *dao) GetUserByID(ctx context.Context, id uint64) (*model.User, error) {
	args := make([]any, 0, 1)
	args = append(args, id)
	querySQL := fmt.Sprintf(getUserByID, userFields)
	users := make([]*model.User, 0)
	if err := d.db.QueryToStructs(ctx, &users, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取用户信息失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	if len(users) == 0 {
		return nil, nil
	}
	return users[0], nil
}

// GetUserBySidAndUin 获取云用户信息
func (d *dao) GetUserBySidAndUin(ctx context.Context, sid int, uin, subAccountUin string) (*model.User, error) {
	args := make([]any, 0, 3)
	args = append(args, sid, uin, subAccountUin)
	querySQL := fmt.Sprintf(getUserBySidAndUin, userFields)
	users := make([]*model.User, 0)
	if err := d.db.QueryToStructs(ctx, &users, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取云用户信息 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	if len(users) == 0 {
		return nil, nil
	}
	return users[0], nil
}

// GetUserByAccount 通过账户获取用户信息
func (d *dao) GetUserByAccount(ctx context.Context, account string) (*model.User, error) {
	args := make([]any, 0, 1)
	args = append(args, account)
	querySQL := fmt.Sprintf(getUserByAccount, userFields)
	users := make([]*model.User, 0)
	if err := d.db.QueryToStructs(ctx, &users, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取用户信息失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	if len(users) == 0 {
		return nil, nil
	}
	return users[0], nil
}

// GetUserByIDs 批量获取用户信息
func (d *dao) GetUserByIDs(ctx context.Context, ids []uint64) (map[uint64]*model.User, error) {
	mapUsers := make(map[uint64]*model.User, 0)
	if len(ids) == 0 {
		return mapUsers, nil
	}
	querySQL := fmt.Sprintf(getUserByIDs, userFields, placeholder(len(ids)))
	args := make([]any, 0, len(ids))
	for _, id := range ids {
		args = append(args, id)
	}
	users := make([]*model.User, 0)
	if err := d.db.QueryToStructs(ctx, &users, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "通过IDs获取用户失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	for _, user := range users {
		mapUsers[user.ID] = user
	}
	return mapUsers, nil
}

// UpdateUserPassword 更新用户密码
func (d *dao) UpdateUserPassword(ctx context.Context, user *model.User) error {
	execSQL := updateUserPassword
	if _, err := d.db.NamedExec(ctx, execSQL, user); err != nil {
		log.ErrorContextf(ctx, "更新用户密码失败 sql:%s user:%+v err:%+v", execSQL, user, err)
		return err
	}
	return nil
}

// GetSIUser 获取集成商用户信息
func (d *dao) GetSIUser(ctx context.Context, sid int, loginUin, loginSubAccountUin string) (*model.User, error) {
	args := make([]any, 0, 3)
	args = append(args, sid, loginUin, loginSubAccountUin)
	querySQL := fmt.Sprintf(getSIUser, userFields)
	users := make([]*model.User, 0)
	if err := d.db.QueryToStructs(ctx, &users, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取集成商用户信息 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	if len(users) == 0 {
		return nil, nil
	}
	return users[0], nil
}

// GetSIUserList 获取集成商用户信息
func (d *dao) GetSIUserList(ctx context.Context, sid int, uin string) ([]*model.User, error) {
	args := make([]any, 0, 3)
	args = append(args, sid)
	condition := ""
	if uin != "" {
		condition = " AND uin = ?"
		args = append(args, uin)
	}
	querySQL := fmt.Sprintf(getSIUserList, userFields, condition)
	users := make([]*model.User, 0)
	if err := d.db.QueryToStructs(ctx, &users, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取集成商用户信息 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	return users, nil
}

// UpdateUserCloudNickname 更新用户云端昵称失败
func (d *dao) UpdateUserCloudNickname(ctx context.Context, user *model.User) error {
	querySQL := updateUserCloudNickname
	if _, err := d.db.NamedExec(ctx, querySQL, user); err != nil {
		log.ErrorContextf(ctx, "更新用户云端昵称失败 sql:%s user:%+v err:%+v", querySQL, user, err)
		return err
	}
	return nil
}

// GetExpUserByID 获取体验用户详情
func (d *dao) GetExpUserByID(ctx context.Context, id uint64) (*model.User, error) {
	args := []any{id}
	querySQL := fmt.Sprintf(getExpUserByID, expUserFields)
	users := make([]*model.User, 0)
	if err := d.db.QueryToStructs(ctx, &users, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取体验用户信息失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	if len(users) == 0 {
		return nil, nil
	}
	return users[0], nil
}

// GetExpUserByIDs 获取用户详情
func (d *dao) GetExpUserByIDs(ctx context.Context, ids []uint64) ([]*model.User, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	users := make([]*model.User, 0)
	args := make([]any, 0)
	args = append(args, ids)
	querySQL := fmt.Sprintf(getExpUserByIDs, expUserFields)
	querySQL, args, err := sqlx.In(querySQL, args...)
	if err != nil {
		log.ErrorContextf(ctx, "获取体验用户参数失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	// 查询
	if err := d.db.QueryToStructs(ctx, &users, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取体验用户失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	return users, nil
}
