package dao

import (
	"context"
	"crypto/md5"
	"fmt"
	"time"

	"git.code.oa.com/trpc-go/trpc-database/redis"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"github.com/jmoiron/sqlx"
	jsoniter "github.com/json-iterator/go"
	"github.com/spf13/cast"
)

const (
	corpStaffFields = `
		id,business_id,corp_id,user_id,nick_name,avatar,cellphone,status,is_gen_qa,join_time,create_time,update_time
	`
	getCorpStaffByIDs = `
        SELECT
            %s
        FROM
            t_corp_staff
        WHERE
            id IN (?)
    `
	createStaff = `
		INSERT INTO
			t_corp_staff(%s)
		VALUES
		    (null,:business_id,:corp_id,:user_id,:nick_name,:avatar,:cellphone,:status,:is_gen_qa,:join_time,
		:create_time,:update_time)
	`
	getStaffByID = `
		SELECT
			%s
		FROM
		    t_corp_staff
		WHERE
		    id = ?
	`
	getStaffByIDs = `
		SELECT
			%s
		FROM
		    t_corp_staff
		WHERE
		    id IN (%s)
	`
	getStaffByBusinessID = `
		SELECT
			%s
		FROM
		    t_corp_staff
		WHERE
		    business_id = ?
	`
	getStaffByBusinessIDs = `
		SELECT
			%s
		FROM
		    t_corp_staff
		WHERE
		    business_id IN (%s)
	`
	getStaffByUserID = `
		SELECT
			%s
		FROM
		    t_corp_staff
		WHERE
		    user_id = ? AND status = ?
	`
	exitCorp = `
		UPDATE
			t_corp_staff
		SET
		    status = :status,
		    update_time = :update_time
		WHERE
		    id = :id
	`
	getStaffCountByCorpID = `
		SELECT
			COUNT(*)
		FROM
		    t_corp_staff
		WHERE
		    corp_id = ? AND status = ? %s
	`
	getStaffByCorpID = `
		SELECT
			%s
		FROM
		    t_corp_staff
		WHERE
		    corp_id = ? AND status = ? %s
		LIMIT
			?,?
	`
	getStaffCountMultiple = `
		SELECT
			COUNT(*)
		FROM
		    t_corp_staff
		WHERE
		    1 = 1 %s
	`
	getStaffMultiple = `
		SELECT
			%s
		FROM
		    t_corp_staff
		WHERE
		    1 = 1 %s
		ORDER BY
		    update_time DESC
		LIMIT
			?,?

	`
	updateCorpStaffGenQA = `
		UPDATE
			t_corp_staff
		SET
		    is_gen_qa = :is_gen_qa,
		    update_time = :update_time
		WHERE
		    id = :id
	`
	updateStaffNickname = `
		UPDATE 
			t_corp_staff 
		SET 
		    nick_name = :nick_name 
		WHERE 
		    id = :id
	`
)

// GetCorpStaffByIDs 通过员工ID获取员工信息
func (d *dao) GetCorpStaffByIDs(ctx context.Context, ids []uint64) ([]*model.CorpStaff, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	corpStaffList := make([]*model.CorpStaff, 0)
	args := make([]any, 0)
	args = append(args, ids)
	querySQL := fmt.Sprintf(getCorpStaffByIDs, corpStaffFields)
	querySQL, args, err := sqlx.In(querySQL, args...)
	if err != nil {
		log.ErrorContextf(ctx, "获取企业员工参数失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	// 查询
	if err := d.db.QueryToStructs(ctx, &corpStaffList, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取企业员工失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	return corpStaffList, nil
}

func sessionKey(token string) string {
	return fmt.Sprintf("qbot:admin:session:%s", token)
}

// JoinCorp 加入企业
func (d *dao) JoinCorp(ctx context.Context, staffName string, user *model.User, corp *model.Corp) (*model.CorpStaff,
	error) {
	var staff = &model.CorpStaff{}
	err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		r, err := d.createStaff(ctx, tx, staffName, user, corp)
		if err != nil {
			return err
		}
		staff = r
		return nil
	})
	return staff, err
}

// ExitCorp 退出企业
func (d *dao) ExitCorp(ctx context.Context, staff *model.CorpStaff) error {
	querySQL := exitCorp
	staff.UpdateTime = time.Now()
	staff.Status = model.StaffStatusInvalid
	if _, err := d.db.NamedExec(ctx, querySQL, staff); err != nil {
		log.ErrorContextf(ctx, "退出企业失败 sql:%s args:%+v err:%+v", querySQL, staff, err)
		return err
	}
	return nil
}

func (d *dao) createStaff(ctx context.Context, tx *sqlx.Tx, staffName string, user *model.User,
	corp *model.Corp) (*model.CorpStaff, error) {
	now := time.Now()
	staff := &model.CorpStaff{
		BusinessID: d.GenerateSeqID(),
		CorpID:     corp.ID,
		UserID:     user.ID,
		NickName:   staffName,
		Avatar:     user.Avatar,
		Cellphone:  user.Cellphone,
		Status:     model.StaffStatusValid,
		IsGenQA:    model.IsGenQANo,
		JoinTime:   now,
		UpdateTime: now,
		CreateTime: now,
	}
	querySQL := fmt.Sprintf(createStaff, corpStaffFields)
	res, err := tx.NamedExecContext(ctx, querySQL, staff)
	if err != nil {
		log.ErrorContextf(ctx, "创建员工失败 sql:%s args:%+v err:%+v", querySQL, staff, err)
		return nil, err
	}
	id, _ := res.LastInsertId()
	staff.ID = uint64(id)
	return staff, nil
}

// GetStaffByID 获取企业员工
func (d *dao) GetStaffByID(ctx context.Context, id uint64) (*model.CorpStaff, error) {
	querySQL := fmt.Sprintf(getStaffByID, corpStaffFields)
	args := make([]any, 0, 1)
	args = append(args, id)
	staff := make([]*model.CorpStaff, 0)
	if err := d.db.QueryToStructs(ctx, &staff, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取企业员工信息失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	if len(staff) == 0 {
		return nil, nil
	}
	return staff[0], nil
}

// GetStaffNickNameMapByIDs 获取昵称Map
func (d *dao) GetStaffNickNameMapByIDs(ctx context.Context, staffIDs []uint64) (nickNameMap map[uint64]string, err error) {
	if len(staffIDs) == 0 {
		return
	}

	nickNameMap = make(map[uint64]string, 0)
	staffs, err := d.GetStaffByIDs(ctx, staffIDs)
	if err != nil {
		return nickNameMap, err
	}

	for _, v := range staffs {
		nickNameMap[v.ID] = v.NickName
	}
	return nickNameMap, nil
}

// GetStaffByIDs 获取企业员工
func (d *dao) GetStaffByIDs(ctx context.Context, ids []uint64) ([]*model.CorpStaff, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	querySQL := fmt.Sprintf(getStaffByIDs, corpStaffFields, placeholder(len(ids)))
	args := make([]any, 0, len(ids))
	for _, bID := range ids {
		args = append(args, bID)
	}
	staff := make([]*model.CorpStaff, 0)
	if err := d.db.QueryToStructs(ctx, &staff, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取企业员工信息失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	return staff, nil
}

// GetStaffByBusinessID 获取企业员工
func (d *dao) GetStaffByBusinessID(ctx context.Context, bID uint64) (*model.CorpStaff, error) {
	querySQL := fmt.Sprintf(getStaffByBusinessID, corpStaffFields)
	args := make([]any, 0, 1)
	args = append(args, bID)
	staff := make([]*model.CorpStaff, 0)
	if err := d.db.QueryToStructs(ctx, &staff, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取企业员工信息失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	if len(staff) == 0 {
		return nil, nil
	}
	return staff[0], nil
}

// GetStaffByBusinessIDs 获取企业员工
func (d *dao) GetStaffByBusinessIDs(ctx context.Context, bIDs []uint64) ([]*model.CorpStaff, error) {
	if len(bIDs) == 0 {
		return nil, nil
	}
	querySQL := fmt.Sprintf(getStaffByBusinessIDs, corpStaffFields, placeholder(len(bIDs)))
	args := make([]any, 0, len(bIDs))
	for _, bID := range bIDs {
		args = append(args, bID)
	}
	staff := make([]*model.CorpStaff, 0)
	if err := d.db.QueryToStructs(ctx, &staff, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取企业员工信息失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	return staff, nil
}

// GetStaffByUserID 获取企业员工
func (d *dao) GetStaffByUserID(ctx context.Context, userID uint64) (*model.CorpStaff, error) {
	querySQL := fmt.Sprintf(getStaffByUserID, corpStaffFields)
	args := make([]any, 0, 2)
	args = append(args, userID, model.StaffStatusValid)
	staff := make([]*model.CorpStaff, 0)
	if err := d.db.QueryToStructs(ctx, &staff, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取企业员工信息失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	if len(staff) == 0 {
		return nil, nil
	}
	return staff[0], nil
}

// GetStaffCountByCorpID 通过企业ID获取员工数量
func (d *dao) GetStaffCountByCorpID(ctx context.Context, corpID uint64, query string) (uint64, error) {
	args := make([]any, 0, 3)
	args = append(args, corpID, model.StaffStatusValid)
	condition := ""
	if query != "" {
		condition = fmt.Sprintf("%s%s", condition, " AND nick_name LIKE ? ")
		args = append(args, fmt.Sprintf("%%%s%%", special.Replace(query)))
	}
	querySQL := fmt.Sprintf(getStaffCountByCorpID, condition)
	var total uint64
	if err := d.db.Get(ctx, &total, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "通过企业ID获取员工数量失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return total, err
	}
	return total, nil
}

// GetStaffByCorpID 通过企业ID获取员工列表
func (d *dao) GetStaffByCorpID(ctx context.Context, corpID uint64, query string, excludeStaffIDs []uint64, page,
	pageSize uint32) ([]*model.CorpStaff, error) {
	args := make([]any, 0, 5+len(excludeStaffIDs))
	args = append(args, corpID, model.StaffStatusValid)
	condition := ""
	if query != "" {
		condition = fmt.Sprintf("%s%s", condition, " AND nick_name LIKE ? ")
		args = append(args, fmt.Sprintf("%%%s%%", special.Replace(query)))
	}
	if len(excludeStaffIDs) != 0 {
		condition = fmt.Sprintf("%s AND id NOT IN (%s)", condition, placeholder(len(excludeStaffIDs)))
		for _, id := range excludeStaffIDs {
			args = append(args, id)
		}
	}
	querySQL := fmt.Sprintf(getStaffByCorpID, corpStaffFields, condition)
	offset := (page - 1) * pageSize
	args = append(args, offset, pageSize)
	staff := make([]*model.CorpStaff, 0)
	if err := d.db.QueryToStructs(ctx, &staff, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "查询企业员工列表失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	return staff, nil
}

// GetStaffSession 获取员工session
func (d *dao) GetStaffSession(ctx context.Context, si *model.SystemIntegrator, token, loginUin,
	loginSubAccountUin string) (*model.Session, error) {
	if loginSubAccountUin != "" {
		return d.getStaffCloudSession(ctx, si, loginUin, loginSubAccountUin)
	}
	key := sessionKey(token)
	replyVec, err := redis.String(d.redis.Do(ctx, "GET", key))
	if err != nil && err != redis.ErrNil {
		log.ErrorContextf(ctx, "获取员工session失败 key:%s replyVec:%s err:%+v", key, replyVec, err)
		return nil, err
	}
	if err == redis.ErrNil {
		log.WarnContextf(ctx, "获取员工session redis 为空 key:%s replyVec:%s", key, replyVec)
		return nil, nil
	}
	session := &model.Session{}
	if err = jsoniter.UnmarshalFromString(replyVec, session); err != nil {
		log.ErrorContextf(ctx, "员工session解析失败 key:%s replyVec:%s err:%+v", key, replyVec, err)
		return nil, err
	}
	if session.IsEmpty() {
		return nil, errs.ErrSessionNotFound
	}
	session.SID = model.CloudSID
	return session, nil
}

// getStaffCloudSession 获取腾讯云session
func (d *dao) getStaffCloudSession(ctx context.Context, si *model.SystemIntegrator, loginUin,
	loginSubAccountUin string) (*model.Session, error) {
	user, err := d.GetSIUser(ctx, si.ID, loginUin, loginSubAccountUin)
	if err != nil {
		return nil, err
	}
	if user == nil {
		log.WarnContextf(ctx, "获取用户信息为空 sid:%d loginUin:%s loginSubAccountUin:%s",
			si.ID, loginUin, loginSubAccountUin)
		return nil, nil
	}
	staff, err := d.GetStaffByUserID(ctx, user.ID)
	if err != nil {
		return nil, err
	}
	if staff == nil {
		log.WarnContextf(ctx, "获取员工信息为空 sid:%d loginUin:%s loginSubAccountUin:%s",
			si.ID, loginUin, loginSubAccountUin)
		return nil, nil
	}
	corp, err := d.GetCorpByID(ctx, staff.CorpID)
	if err != nil {
		return nil, err
	}
	if corp == nil {
		log.WarnContextf(ctx, "获取企业信息为空 sid:%d loginUin:%s loginSubAccountUin:%s",
			si.ID, loginUin, loginSubAccountUin)
		return nil, nil
	}
	if corp.Uin != loginUin {
		return nil, errs.ErrUinNotMatch
	}
	if !staff.IsValid() || !user.IsValid() {
		return nil, errs.ErrStaffInValid
	}
	if !corp.IsValid() {
		return nil, errs.ErrCorpInValid
	}
	return &model.Session{
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

// SetStaffSession 设置用户session
func (d *dao) SetStaffSession(ctx context.Context, staff *model.CorpStaff) (string, error) {
	expHour := config.App().LoginDefault.SessionExpr
	session := &model.Session{
		ID:         staff.ID,
		BizID:      staff.BusinessID,
		CorpID:     staff.CorpID,
		Cellphone:  staff.Cellphone,
		Status:     staff.Status,
		ExpireTime: time.Now().Add(expHour).Unix(),
	}
	sessionStr, err := jsoniter.MarshalToString(session)
	if err != nil {
		log.ErrorContextf(ctx, "序列化session失败 session:%+v err:%+v", session, err)
		return "", err
	}
	token := fmt.Sprintf("%x", md5.Sum([]byte(fmt.Sprintf("%d:%d:%d", staff.ID, staff.CorpID, time.Now().Unix()))))
	key := sessionKey(token)
	if _, err = d.redis.Do(ctx, "SET", key, sessionStr, "EX", int64(expHour.Seconds())); err != nil {
		log.ErrorContextf(ctx, "设置员工session session:%+v err:%+v", session, err)
		return "", err
	}
	return token, nil
}

// DeleteStaffSession 删除用户session
func (d *dao) DeleteStaffSession(ctx context.Context, token string) error {
	key := sessionKey(token)
	if _, err := d.redis.Do(ctx, "DEL", key); err != nil {
		log.ErrorContextf(ctx, "删除用户session失败 key:%s err:%+v", key, err)
		return err
	}
	return nil
}

// GetCorpStaffTotal 获取企业员工总数
func (d *dao) GetCorpStaffTotal(ctx context.Context, corpBizID uint64, staffBizIDs []uint64, query string,
	status []uint32) (uint64, error) {
	args := make([]any, 0, 10)
	condition := ""
	if corpBizID != 0 {
		corp, err := d.GetCorpByBusinessID(ctx, corpBizID)
		if err != nil {
			return 0, err
		}
		if corp != nil {
			condition = fmt.Sprintf("%s%s", condition, " AND corp_id = ?")
			args = append(args, corp.ID)
		}
	}
	if len(staffBizIDs) != 0 {
		condition = fmt.Sprintf("%s AND business_id IN (%s)", condition, placeholder(len(staffBizIDs)))
		for _, v := range staffBizIDs {
			args = append(args, v)
		}
	}
	if query != "" {
		condition = fmt.Sprintf("%s%s", condition, " AND nick_name LIKE ? OR cellphone LIKE ?")
		args = append(args,
			fmt.Sprintf("%%%s%%", special.Replace(query)),
			fmt.Sprintf("%%%s%%", special.Replace(query)),
		)
	}
	if len(status) != 0 {
		condition = fmt.Sprintf("%s AND status IN (%s)", condition, placeholder(len(status)))
		for _, v := range status {
			args = append(args, v)
		}
	}
	querySQL := fmt.Sprintf(getStaffCountMultiple, condition)
	var total uint64
	if err := d.db.Get(ctx, &total, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取企业员工总数失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return total, err
	}
	return total, nil
}

// GetCorpStaffList 获取企业员工详情
func (d *dao) GetCorpStaffList(ctx context.Context, corpBizID uint64, staffBizIDs []uint64, query string,
	status []uint32, page, pageSize uint32) ([]*model.CorpStaff, error) {
	staff := make([]*model.CorpStaff, 0)
	args := make([]any, 0, 10)
	condition := ""
	if corpBizID != 0 {
		corp, err := d.GetCorpByBusinessID(ctx, corpBizID)
		if err != nil {
			return staff, err
		}
		if corp != nil {
			condition = fmt.Sprintf("%s%s", condition, " AND corp_id = ?")
			args = append(args, corp.ID)
		}
	}
	if len(staffBizIDs) != 0 {
		condition = fmt.Sprintf("%s AND business_id IN (%s)", condition, placeholder(len(staffBizIDs)))
		for _, v := range staffBizIDs {
			args = append(args, v)
		}
	}
	if query != "" {
		condition = fmt.Sprintf("%s%s", condition, " AND nick_name LIKE ? OR cellphone LIKE ?")
		args = append(args,
			fmt.Sprintf("%%%s%%", special.Replace(query)),
			fmt.Sprintf("%%%s%%", special.Replace(query)),
		)
	}
	if len(status) != 0 {
		condition = fmt.Sprintf("%s AND status IN (%s)", condition, placeholder(len(status)))
		for _, v := range status {
			args = append(args, v)
		}
	}
	querySQL := fmt.Sprintf(getStaffMultiple, corpStaffFields, condition)
	offset := (page - 1) * pageSize
	args = append(args, offset, pageSize)
	if err := d.db.QueryToStructs(ctx, &staff, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取企业员工详情失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return staff, err
	}
	return staff, nil
}

// UpdateCorpStaffGenQA 更新员工生成QA标记
func (d *dao) UpdateCorpStaffGenQA(ctx context.Context, staff *model.CorpStaff) error {
	querySQL := updateCorpStaffGenQA
	staff.UpdateTime = time.Now()
	staff.IsGenQA = model.IsGenQAYes
	if _, err := d.db.NamedExec(ctx, querySQL, staff); err != nil {
		log.ErrorContextf(ctx, "更新员工生成QA标记 sql:%s args:%+v err:%+v", querySQL, staff, err)
		return err
	}
	return nil
}

// RecordUserAccessUnCheckQATime 记录访问未检验问答时间
func (d *dao) RecordUserAccessUnCheckQATime(ctx context.Context, robotID, staffID uint64) error {
	key := fmt.Sprintf("qbot:admin:qa:%s:%s", cast.ToString(robotID), cast.ToString(staffID))
	val := time.Now().UnixMilli()
	if _, err := d.redis.Do(ctx, "SET", key, val); err != nil {
		log.ErrorContextf(ctx, "设置用户访问问答时间错误: %+v, key: %s", err, key)
		return err
	}
	return nil
}

// UpdateStaffCloudNickname 更新员工云端昵称
func (d *dao) UpdateStaffCloudNickname(ctx context.Context, staff *model.CorpStaff) error {
	querySQL := updateStaffNickname
	staff.UpdateTime = time.Now()
	if _, err := d.db.NamedExec(ctx, querySQL, staff); err != nil {
		log.ErrorContextf(ctx, "更新员工云端昵称 sql:%s args:%+v err:%+v", querySQL, staff, err)
		return err
	}
	return nil
}
