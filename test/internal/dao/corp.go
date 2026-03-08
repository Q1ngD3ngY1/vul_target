package dao

import (
	"context"
	"database/sql"
	"fmt"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"github.com/jmoiron/sqlx"
)

const (
	corpFields = `
		id,sid,business_id,uin,full_name,create_user_id,cellphone,robot_quota,contact_name,email,status,
		max_char_size,max_token_usage,is_trial,trial_start_time,trial_end_time,create_time,update_time
	`
	getCorpList = `
		SELECT 
			%s 
		FROM
			t_corp
		WHERE 
			 status = ? %s
	`
	getAuditingCorp = `
		SELECT 
			%s 
		FROM
			t_corp
		WHERE 
			 status = ? AND cellphone = ?
	`
	createCorp = `
		INSERT INTO
		    t_corp (%s)
		VALUES 
		    (null,:sid,:business_id,:uin,:full_name,:create_user_id,:cellphone,:robot_quota,:contact_name,:email,
			:status,:max_char_size,:max_token_usage,:is_trial,:trial_start_time,:trial_end_time,:create_time,
			:update_time)
	`
	getCorpByBusinessID = `
		SELECT 
			%s 
		FROM
			t_corp
		WHERE 
			business_id = ?
	`
	getCorpByID = `
		SELECT 
			%s 
		FROM
			t_corp
		WHERE 
			id = ?
	`
	getCorpByIDs = `
		SELECT 
			%s 
		FROM
			t_corp
		WHERE 
			id IN (%s)
	`
	getCorpByName = `
		SELECT 
			%s 
		FROM
			t_corp
		WHERE 
			full_name = ?
	`
	auditCorp = `
		UPDATE 
			t_corp 
		SET 
		    status = :status,
		    update_time = :update_time 
		WHERE 
		    id = :id
	`
	updateCorpRobotQuota = `
		UPDATE 
			t_corp 
		SET 
		    robot_quota = :robot_quota,
			update_time = :update_time 
		WHERE 
		    id = :id
	`
	getCorpCountForOp = `
		SELECT 
			COUNT(*) 
		FROM 
		    t_corp 
		WHERE 
		    1 = 1 %s 
	`
	getCorpForOp = `
		SELECT 
			%s 
		FROM 
		    t_corp 
		WHERE 
		    1 = 1 %s 
		ORDER BY 
		    update_time DESC 
		LIMIT 
			?,? 
	`
	enableCorp = `
		UPDATE 
			t_corp 
		SET 
		    status = :status,
		    update_time = :update_time 
		WHERE 
		    id = :id
	`
	disableCorp = `
		UPDATE 
			t_corp 
		SET 
		    status = :status,
		    update_time = :update_time 
		WHERE 
		    id = :id
	`
	getValidCorpBySidAndUin = `
		SELECT 
			%s 
		FROM 
		    t_corp 
		WHERE 
		    sid = ? AND uin = ? AND status = ?
    `
	getCorpBySidAndUin = `
		SELECT 
			%s 
		FROM 
		    t_corp 
		WHERE 
		    uin = ? 
    `
	getSidByID = `
		SELECT 
			sid 
		FROM 
		    t_corp 
		WHERE 
		    id = ? 
    `
	updateCorpCreateUser = `
		UPDATE 
			t_corp 
		SET 
		    create_user_id = :create_user_id,
		    update_time = :update_time 
		WHERE 
		    id = :id
	`
	getCorpByCreateUserID = `
		SELECT 
			%s 
		FROM 
		    t_corp 
		WHERE 
		    create_user_id = ?
    `
	updateCorpUin = `
		UPDATE 
			t_corp 
		SET 
		    uin = :uin
		WHERE 
		    id = :id
	`
	getValidTrailCorpList = `
		SELECT 
			%s 
		FROM 
		    t_corp 
		WHERE 
		    is_trial = ? AND status = ? AND trial_end_time >= ?
	`
)

// CreateCorp 创建企业
func (d *dao) CreateCorp(ctx context.Context, corp *model.Corp, user *model.User, staff *model.CorpStaff) error {
	if err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		now := time.Now()
		userID := user.ID
		if userID == 0 {
			user.CreateTime = now
			user.UpdateTime = now
			querySQL := fmt.Sprintf(createUser, userFields)
			res, err := tx.NamedExecContext(ctx, querySQL, user)
			if err != nil {
				log.ErrorContextf(ctx, "创建企业用户失败 sql:%s args:%+v err:%+v", querySQL, corp, err)
				return err
			}
			insertID, _ := res.LastInsertId()
			userID = uint64(insertID)
		}
		corpID := corp.ID
		if corpID == 0 {
			corp.CreateUserID = userID
			corp.CreateTime = now
			corp.UpdateTime = now
			querySQL := fmt.Sprintf(createCorp, corpFields)
			res, err := tx.NamedExecContext(ctx, querySQL, corp)
			if err != nil {
				log.ErrorContextf(ctx, "创建企业失败 sql:%s args:%+v err:%+v", querySQL, corp, err)
				return err
			}
			insertID, _ := res.LastInsertId()
			corpID = uint64(insertID)
		}
		staff.JoinTime = now
		staff.CreateTime = now
		staff.UpdateTime = now
		staff.UserID = userID
		staff.CorpID = corpID
		querySQL := fmt.Sprintf(createStaff, corpStaffFields)
		if _, err := tx.NamedExecContext(ctx, querySQL, staff); err != nil {
			log.ErrorContextf(ctx, "创建企业员工失败 sql:%s args:%+v err:%+v", querySQL, staff, err)
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

// CreateTrialCorp 创建试用企业
func (d *dao) CreateTrialCorp(ctx context.Context, corp *model.Corp) error {
	now := time.Now()
	corp.CreateTime = now
	corp.UpdateTime = now
	querySQL := fmt.Sprintf(createCorp, corpFields)
	if _, err := d.db.NamedExec(ctx, querySQL, corp); err != nil {
		log.ErrorContextf(ctx, "创建试用企业失败 sql:%s args:%+v err:%+v", querySQL, corp, err)
		return err
	}
	return nil
}

// UpdateTrialCorpCreateUser 更新试用企业创建人
func (d *dao) UpdateTrialCorpCreateUser(ctx context.Context, corp *model.Corp) error {
	querySQL := updateCorpCreateUser
	if _, err := d.db.NamedExec(ctx, querySQL, corp); err != nil {
		log.ErrorContextf(ctx, "更新企业创建人失败 sql:%s args:%+v err:%+v", querySQL, corp, err)
		return err
	}
	return nil
}

// RegisterCorp 注册企业
func (d *dao) RegisterCorp(ctx context.Context, corp *model.Corp) error {
	now := time.Now()
	corp.UpdateTime = now
	corp.CreateTime = now
	querySQL := fmt.Sprintf(createCorp, corpFields)
	if _, err := d.db.NamedExec(ctx, querySQL, corp); err != nil {
		log.ErrorContextf(ctx, "注册企业失败 sql:%s args:%+v err:%+v", querySQL, corp, err)
		return err
	}
	return nil
}

// GetCorpByBusinessID 获取企业信息
func (d *dao) GetCorpByBusinessID(ctx context.Context, bID uint64) (*model.Corp, error) {
	args := make([]any, 0, 1)
	args = append(args, bID)
	querySQL := fmt.Sprintf(getCorpByBusinessID, corpFields)
	corps := make([]*model.Corp, 0)
	if err := d.db.QueryToStructs(ctx, &corps, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取企业信息失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	log.DebugContextf(ctx, "获取企业信息 sql:%s args:%+v", querySQL, args)
	if len(corps) == 0 {
		return nil, nil
	}
	return corps[0], nil
}

// GetCorpByID 获取企业
func (d *dao) GetCorpByID(ctx context.Context, id uint64) (*model.Corp, error) {
	args := make([]any, 0, 1)
	args = append(args, id)
	querySQL := fmt.Sprintf(getCorpByID, corpFields)
	corps := make([]*model.Corp, 0)
	if err := d.db.QueryToStructs(ctx, &corps, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "通过ID获取获取企业信息失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	if len(corps) == 0 {
		log.ErrorContextf(ctx, "通过ID获取企业信息不存在 sql:%s args:%+v", querySQL, args)
		return nil, errs.ErrCorpNotFound
	}
	return corps[0], nil
}

// GetCorpByIDs 获取企业
func (d *dao) GetCorpByIDs(ctx context.Context, ids []uint64) (map[uint64]*model.Corp, error) {
	corps := make(map[uint64]*model.Corp, 0)
	if len(ids) == 0 {
		return corps, nil
	}
	args := make([]any, 0, len(ids))
	for _, id := range ids {
		args = append(args, id)
	}
	list := make([]*model.Corp, 0)
	querySQL := fmt.Sprintf(getCorpByIDs, corpFields, placeholder(len(ids)))
	if err := d.db.QueryToStructs(ctx, &list, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取企业信息失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return corps, err
	}
	for _, item := range list {
		corps[item.ID] = item
	}
	return corps, nil
}

// GetCorpByName 通过企业全称获取企业
func (d *dao) GetCorpByName(ctx context.Context, name string) (*model.Corp, error) {
	args := make([]any, 0, 1)
	args = append(args, name)
	querySQL := fmt.Sprintf(getCorpByName, corpFields)
	corps := make([]*model.Corp, 0)
	if err := d.db.QueryToStructs(ctx, &corps, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "通过企业全称获取企业失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	if len(corps) == 0 {
		return nil, nil
	}
	return corps[0], nil
}

// AuditCorp 企业审核
func (d *dao) AuditCorp(ctx context.Context, corp *model.Corp, pass bool) error {
	if err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		now := time.Now()
		corp.UpdateTime = now
		corp.Status = model.CorpStatusValid
		querySQL := auditCorp
		if !pass {
			corp.Status = model.CorpStatusAuditFail
		}
		if _, err := tx.NamedExecContext(ctx, querySQL, corp); err != nil {
			log.ErrorContextf(ctx, "企业审核失败 sql:%s args:%+v err:%+v", querySQL, corp, err)
			return err
		}
		if !pass {
			return nil
		}
		user, err := d.GetUserByID(ctx, corp.CreateUserID)
		if err != nil {
			return err
		}
		if user == nil {
			return errs.ErrUserNotFound
		}
		if _, err = d.createStaff(ctx, tx, corp.ContactName, user, corp); err != nil {
			return err
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "企业审核失败 err:%+v", err)
		return err
	}
	return nil
}

// ModifyCorpRobotQuota 修改企业机器人配额
func (d *dao) ModifyCorpRobotQuota(ctx context.Context, corp *model.Corp) error {
	corp.UpdateTime = time.Now()
	querySQL := updateCorpRobotQuota
	if _, err := d.db.NamedExec(ctx, querySQL, corp); err != nil {
		log.ErrorContextf(ctx, "修改企业机器人配额失败 sql:%s args:%+v err:%+v", querySQL, corp, err)
		return err
	}
	return nil
}

// GetCorpList 获取企业信息列表
func (d *dao) GetCorpList(ctx context.Context, corpStatus uint32, cellphone string) ([]*model.Corp, error) {
	var corps []*model.Corp
	var args []any
	args = append(args, corpStatus)
	var condition string
	if len(cellphone) > 0 {
		condition += " AND cellphone LIKE ?"
		args = append(args, "%"+cellphone+"%")
	}
	querySQL := fmt.Sprintf(getCorpList, corpFields, condition)
	// 查询
	if err := d.db.QueryToStructs(ctx, &corps, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取所有机器人列表失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	return corps, nil
}

// GetAuditingCorp 获取审核中的企业
func (d *dao) GetAuditingCorp(ctx context.Context, cellphone string) ([]*model.Corp, error) {
	var corps []*model.Corp
	var args []any
	args = append(args, model.CorpStatusInAudit, cellphone)
	querySQL := fmt.Sprintf(getAuditingCorp, corpFields)
	// 查询
	if err := d.db.QueryToStructs(ctx, &corps, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取审核中的企业失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	return corps, nil
}

// GetCorpTotal 获取企业总数
func (d *dao) GetCorpTotal(ctx context.Context, corpBizID []uint64, query string, status []uint32) (uint64, error) {
	args := make([]any, 0, 10)
	condition := ""
	if len(corpBizID) != 0 {
		condition = fmt.Sprintf("%s AND business_id IN (%s)", condition, placeholder(len(corpBizID)))
		for _, v := range corpBizID {
			args = append(args, v)
		}
	}
	if query != "" {
		condition = fmt.Sprintf("%s%s", condition, " AND full_name LIKE ? OR cellphone LIKE ?")
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
	querySQL := fmt.Sprintf(getCorpCountForOp, condition)
	var total uint64
	if err := d.db.Get(ctx, &total, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取企业总数失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return total, err
	}
	return total, nil
}

// GetCorpDetails 获取企业详情
func (d *dao) GetCorpDetails(ctx context.Context, corpBizID []uint64, query string, status []uint32, page,
	pageSize uint32) ([]*model.Corp, error) {
	args := make([]any, 0, 10)
	condition := ""
	if len(corpBizID) != 0 {
		condition = fmt.Sprintf("%s AND business_id IN (%s)", condition, placeholder(len(corpBizID)))
		for _, v := range corpBizID {
			args = append(args, v)
		}
	}
	if query != "" {
		condition = fmt.Sprintf("%s%s", condition, " AND full_name LIKE ? OR cellphone LIKE ?")
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
	querySQL := fmt.Sprintf(getCorpForOp, corpFields, condition)
	offset := (page - 1) * pageSize
	args = append(args, offset, pageSize)
	corp := make([]*model.Corp, 0)
	if err := d.db.QueryToStructs(ctx, &corp, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取企业详情失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return corp, err
	}
	return corp, nil
}

// EnableCorp 启用企业
func (d *dao) EnableCorp(ctx context.Context, corps []*model.Corp) error {
	if len(corps) == 0 {
		return nil
	}
	if err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		for _, corp := range corps {
			corp.UpdateTime = time.Now()
			corp.Status = model.CorpStatusValid
			execSQL := enableCorp
			if _, err := tx.NamedExecContext(ctx, execSQL, corp); err != nil {
				log.ErrorContextf(ctx, "企业启用失败 sql:%s args:%+v err:%+v", execSQL, corp, err)
				return err
			}
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "企业启用失败 err:%+v", err)
		return err
	}
	return nil
}

// DisableCorp 禁用企业
func (d *dao) DisableCorp(ctx context.Context, corps []*model.Corp) error {
	if len(corps) == 0 {
		return nil
	}
	if err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		for _, corp := range corps {
			corp.UpdateTime = time.Now()
			corp.Status = model.CorpStatusInValid
			execSQL := disableCorp
			if _, err := tx.NamedExecContext(ctx, execSQL, corp); err != nil {
				log.ErrorContextf(ctx, "企业禁用失败 sql:%s args:%+v err:%+v", execSQL, corp, err)
				return err
			}
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "企业禁用失败 err:%+v", err)
		return err
	}
	return nil
}

// GetValidCorpBySidAndUin 获取有效的集成商企业信息
func (d *dao) GetValidCorpBySidAndUin(ctx context.Context, sid int, uin string) (*model.Corp, error) {
	args := make([]any, 0, 3)
	args = append(args, sid, uin, model.CorpStatusValid)
	querySQL := fmt.Sprintf(getValidCorpBySidAndUin, corpFields)
	corps := make([]*model.Corp, 0)
	if err := d.db.QueryToStructs(ctx, &corps, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取集成商企业信息失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	log.DebugContextf(ctx, "获取集成商企业信息 sql:%s args:%+v", querySQL, args)
	if len(corps) == 0 {
		return nil, nil
	}
	return corps[0], nil
}

// GetCorpBySidAndUin 获取集成商企业信息
func (d *dao) GetCorpBySidAndUin(ctx context.Context, uin string) (*model.Corp, error) {
	args := make([]any, 0, 2)
	args = append(args, uin)
	querySQL := fmt.Sprintf(getCorpBySidAndUin, corpFields)
	corps := make([]*model.Corp, 0)
	if err := d.db.QueryToStructs(ctx, &corps, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取集成商企业信息失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	log.DebugContextf(ctx, "获取集成商企业信息 sql:%s args:%+v", querySQL, args)
	if len(corps) == 0 {
		return nil, errs.ErrCorpNotFound
	}
	return corps[0], nil
}

// GetSidByCorpID 获取集成商id
func (d *dao) GetSidByCorpID(ctx context.Context, id uint64) (int, error) {
	args := make([]any, 0, 1)
	args = append(args, id)
	sids := make([]sql.NullInt32, 0, 1)
	if err := d.db.Select(ctx, &sids, getSidByID, args...); err != nil {
		log.ErrorContextf(ctx, "获取集成商id失败 sql:%s args:%+v err:%+v", getSidByID, args, err)
		return 0, err
	}
	if len(sids) == 0 {
		return 0, errs.ErrCorpNotFound
	}
	if sids[0].Valid {
		return int(sids[0].Int32), nil
	}
	return 0, errs.ErrCorpNotFound
}

// MigrateCloud 企业迁移
func (d *dao) MigrateCloud(ctx context.Context, corp *model.Corp, user *model.User, corpStaff *model.CorpStaff) error {
	if err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		if _, err := tx.NamedExecContext(ctx, updateCorpUin, corp); err != nil {
			log.ErrorContextf(ctx, "企业迁移失败 sql:%s args:%+v err:%+v", updateCorpUin, corp, err)
			return err
		}
		if _, err := tx.NamedExecContext(ctx, updateUserUin, user); err != nil {
			log.ErrorContextf(ctx, "企业迁移失败 sql:%s args:%+v err:%+v", updateUserUin, user, err)
			return err
		}
		if _, err := tx.NamedExecContext(ctx, updateStaffNickname, corpStaff); err != nil {
			log.ErrorContextf(ctx, "企业迁移失败 sql:%s args:%+v err:%+v", updateStaffNickname, corpStaff, err)
			return err
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "企业迁移失败 err:%+v", err)
		return err
	}
	return nil
}

// GetCorpByCreateUserID 通过企业创建人获取企业
func (d *dao) GetCorpByCreateUserID(ctx context.Context, createUserID uint64) (*model.Corp, error) {
	args := make([]any, 0, 1)
	args = append(args, createUserID)
	querySQL := fmt.Sprintf(getCorpByCreateUserID, corpFields)
	corps := make([]*model.Corp, 0)
	if err := d.db.QueryToStructs(ctx, &corps, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "通过企业创建人获取企业 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	if len(corps) == 0 {
		return nil, nil
	}
	return corps[0], nil
}

// GetValidTrailCorpList 获取有效的试用企业
func (d *dao) GetValidTrailCorpList(ctx context.Context) ([]*model.Corp, error) {
	args := make([]any, 0, 3)
	args = append(args, model.IsTrialYes, model.CorpStatusValid, time.Now())
	querySQL := fmt.Sprintf(getValidTrailCorpList, corpFields)
	corps := make([]*model.Corp, 0)
	if err := d.db.QueryToStructs(ctx, &corps, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取有效的试用企业 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	return corps, nil
}

// GetTrailCorpTokenUsage 获取试用企业token用量
func (d *dao) GetTrailCorpTokenUsage(ctx context.Context, corpID uint64) (uint64, error) {
	corp, err := d.GetCorpByID(ctx, corpID)
	if err != nil {
		return 0, err
	}
	if !corp.IsValid() {
		return 0, nil
	}
	if !corp.IsCorpTrial() {
		return 0, nil
	}
	querySQL := getCorpTokenUsage
	args := []any{corp.ID}
	var tokenUsage uint64
	if err := d.db.Get(ctx, &tokenUsage, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取试用企业token用量失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return 0, err
	}
	return tokenUsage, nil
}

// GetCorpTokenUsage 获取企业token用量
func (d *dao) GetCorpTokenUsage(ctx context.Context, corpID uint64) (uint64, error) {
	querySQL := getCorpTokenUsage
	args := []any{corpID}
	var tokenUsage uint64
	if err := d.db.Get(ctx, &tokenUsage, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取企业token用量失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return 0, err
	}
	return tokenUsage, nil
}

// GetCorpUsedCharSizeUsage 获取试用企业字符用量
func (d *dao) GetCorpUsedCharSizeUsage(ctx context.Context, corpID uint64) (uint64, error) {
	querySQL := getCorpUsedCharSizeUsage
	args := []any{corpID, model.AppIsNotDeleted}
	var usedCharSizeUsage int64
	if err := d.db.Get(ctx, &usedCharSizeUsage, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取试用企业字符用量失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return 0, err
	}
	if usedCharSizeUsage < 0 {
		log.ErrorContextf(ctx, "获取试用企业字符用量错误 sql:%s args:%+v usedCharSizeUsage:%+v",
			querySQL, args, usedCharSizeUsage)
		return 0, nil
	}
	return uint64(usedCharSizeUsage), nil
}

// GetBotUsedCharSizeUsage 获取应用下企业字符用量
func (d *dao) GetBotUsedCharSizeUsage(ctx context.Context, botBizID uint64) (uint64, error) {
	querySQL := getBotUsedCharSizeUsage
	args := []any{botBizID, model.AppIsNotDeleted}
	var usedCharSizeUsage int64
	if err := d.db.Get(ctx, &usedCharSizeUsage, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取试用应用字符用量失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return 0, err
	}
	if usedCharSizeUsage < 0 {
		log.ErrorContextf(ctx, "获取试用应用字符用量错误 sql:%s args:%+v usedCharSizeUsage:%+v",
			querySQL, args, usedCharSizeUsage)
		return 0, nil
	}
	return uint64(usedCharSizeUsage), nil
}
