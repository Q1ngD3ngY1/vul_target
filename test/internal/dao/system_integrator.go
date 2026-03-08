package dao

import (
	"context"
	"fmt"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
)

const (
	systemIntegratorFields = `
		id,name,status,uin,sub_account_uin,is_self_permission,allow_action,deny_action,corp_app_quota,
        update_time,create_time
	`
	getSystemIntegrator = `
		SELECT 
			%s 
		FROM 
		    t_system_integrator 
		WHERE 
		    uin = ? AND sub_account_uin = ? AND status = ?
	`
	getSystemIntegratorByID = `
		SELECT 
			%s 
		FROM 
		    t_system_integrator 
		WHERE 
		    id = ?
	`
)

// GetSystemIntegrator 获取集成商信息 TODO: 缓存到内存里
func (d *dao) GetSystemIntegrator(ctx context.Context, uin, subAccountUin string) (
	*model.SystemIntegrator, error) {
	args := make([]any, 0, 3)
	args = append(args, uin, subAccountUin, model.SystemIntegratorValid)
	querySQL := fmt.Sprintf(getSystemIntegrator, systemIntegratorFields)
	records := make([]*model.SystemIntegrator, 0)
	if err := d.db.QueryToStructs(ctx, &records, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取集成商信息 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, errs.ErrSystem
	}
	if len(records) == 0 {
		return d.GetSystemIntegratorByID(ctx, model.CloudSID)
	}
	si := records[0]
	return si, nil
}

// GetSystemIntegratorByID 通过ID获取集成商信息
func (d *dao) GetSystemIntegratorByID(ctx context.Context, id int) (*model.SystemIntegrator, error) {
	args := []any{id}
	querySQL := fmt.Sprintf(getSystemIntegratorByID, systemIntegratorFields)
	records := make([]*model.SystemIntegrator, 0)
	if err := d.db.QueryToStructs(ctx, &records, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "通过ID获取集成商信息 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, errs.ErrSystem
	}
	if len(records) == 0 {
		log.ErrorContextf(ctx, "通过ID获取集成商信息不存在 sql:%s args:%+v", querySQL, args)
		return nil, errs.ErrSystemIntegratorNotFound
	}
	si := records[0]
	return si, nil
}

// IsSystemIntegrator 是否是集成商
func (d *dao) IsSystemIntegrator(ctx context.Context, corp *model.Corp) bool {
	log.InfoContextf(ctx, "IsSystemIntegrator|corp:%+v", corp)
	si, err := d.GetSystemIntegratorByID(ctx, corp.SID)
	if err != nil {
		log.ErrorContextf(ctx, "IsSystemIntegrator|GetSystemIntegratorByID failed, err:%+v", err)
		return false
	}
	log.InfoContextf(ctx, "IsSystemIntegrator|si:%+v|IsCloudSI:%+v", si, si.IsCloudSI())
	if !si.IsCloudSI() {
		return true
	}
	return false
}
