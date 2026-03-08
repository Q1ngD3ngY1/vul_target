package dao

import (
	"context"
	"errors"
	"fmt"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"gorm.io/gorm"
)

var globalAuditDao *auditDao

const (
	auditTableName = "t_audit"

	AuditTblColId             = "id"
	AuditTblColBusinessId     = "business_id"
	AuditTblColCorpId         = "corp_id"
	AuditTblColRobotId        = "robot_id"
	AuditTblColCreateStaffId  = "create_staff_id"
	AuditTblColParentId       = "parent_id"
	AuditTblColType           = "type"
	AuditTblColParams         = "params"
	AuditTblColRelateId       = "relate_id"
	AuditTblColStatus         = "status"
	AuditTblColRetryTimes     = "retry_times"
	AuditTblColETag           = "e_tag"
	AuditTblColMessage        = "message"
	AuditTblColCreateTime     = "create_time"
	AuditTblColUpdateTime     = "update_time"
	AuditTblColParentRelateId = "parent_relate_id"

	AuditTableMaxPageSize = 1000
)

var AuditTblColList = []string{AuditTblColId, AuditTblColBusinessId, AuditTblColCorpId, AuditTblColRobotId,
	AuditTblColCreateStaffId, AuditTblColParentId, AuditTblColType, AuditTblColParams, AuditTblColRelateId,
	AuditTblColStatus, AuditTblColRetryTimes, AuditTblColETag, AuditTblColMessage, AuditTblColCreateTime,
	AuditTblColUpdateTime, AuditTblColParentRelateId}

type auditDao struct {
	BaseDao
}

// GetAuditDao 获取全局的数据操作对象
func GetAuditDao() *auditDao {
	if globalAuditDao == nil {
		globalAuditDao = &auditDao{*globalBaseDao}
	}
	return globalAuditDao
}

type AuditFilter struct {
	IDs       []uint64
	CorpID    uint64
	RobotID   uint64
	ParentID  uint64
	IsDeleted *int
	Offset    uint32
	Limit     uint32
}

// 生成查询条件，必须按照索引的顺序排列
func (d *auditDao) generateCondition(ctx context.Context, session *gorm.DB, filter *AuditFilter) {
	if len(filter.IDs) != 0 {
		session = session.Where(AuditTblColId+sqlIn, filter.IDs)
	}
	if filter.CorpID != 0 {
		session = session.Where(AuditTblColCorpId+sqlEqual, filter.CorpID)
	}
	if filter.RobotID != 0 {
		session = session.Where(AuditTblColRobotId+sqlEqual, filter.RobotID)
	}
	if filter.ParentID != 0 {
		session = session.Where(AuditTblColParentId+sqlEqual, filter.ParentID)
	}
}

// getAuditList 获取审核列表
func (d *auditDao) getAuditList(ctx context.Context, selectColumns []string,
	filter *AuditFilter) ([]*model.Audit, error) {
	auditList := make([]*model.Audit, 0)
	if filter.Limit == 0 {
		// 为0正常返回空结果即可
		return auditList, nil
	}
	if filter.Limit > AuditTableMaxPageSize {
		// 限制单次查询最大条数
		err := errors.New(fmt.Sprintf("invalid limit: %d", filter.Limit))
		log.ErrorContextf(ctx, "getAuditList err: %+v", err)
		return auditList, err
	}
	session := d.gormDB.WithContext(ctx).Table(auditTableName).Select(selectColumns)
	d.generateCondition(ctx, session, filter)
	session = session.Offset(int(filter.Offset)).Limit(int(filter.Limit))
	res := session.Find(&auditList)
	if res.Error != nil {
		log.ErrorContextf(ctx, "execute sql failed, err: %+v", res.Error)
		return auditList, res.Error
	}
	return auditList, nil
}

// GetAuditList 获取审核列表
func (d *auditDao) GetAuditList(ctx context.Context, selectColumns []string,
	filter *AuditFilter) ([]*model.Audit, error) {
	allAuditList := make([]*model.Audit, 0)
	if filter.Limit == 0 {
		log.WarnContextf(ctx, "GetAuditList limit is 0")
		filter.Limit = AuditTableMaxPageSize
	}
	beginTime := time.Now()
	offset := filter.Offset
	alreadyGetCount := uint32(0)
	wantedCount := filter.Limit
	for {
		filter.Offset = offset
		filter.Limit = CalculateLimit(wantedCount, alreadyGetCount)
		if filter.Limit == 0 {
			break
		}
		auditList, err := d.getAuditList(ctx, selectColumns, filter)
		if err != nil {
			log.ErrorContextf(ctx, "GetAuditList failed, err: %+v", err)
			return nil, err
		}
		allAuditList = append(allAuditList, auditList...)
		if uint32(len(auditList)) < filter.Limit {
			// 已分页遍历完所有数据
			break
		}
		offset += filter.Limit
		alreadyGetCount += filter.Limit
	}
	log.DebugContextf(ctx, "GetAuditList count:%d cost:%dms",
		len(allAuditList), time.Since(beginTime).Milliseconds())
	return allAuditList, nil
}

// UpdateAudit 更新文档
func (d *auditDao) UpdateAudit(ctx context.Context, tx *gorm.DB, updateColumns []string, filter *AuditFilter,
	audit *model.Audit) (int64, error) {
	if tx == nil {
		tx = d.gormDB
	}
	session := tx.WithContext(ctx).Table(auditTableName).Select(updateColumns)
	d.generateCondition(ctx, session, filter)
	res := session.Updates(audit)
	if res.Error != nil {
		log.ErrorContextf(ctx, "update audit failed, audit:%+v err:%+v", audit, res.Error)
		return 0, res.Error
	}
	log.DebugContextf(ctx, "update audit record:%v", res.RowsAffected)
	return res.RowsAffected, nil
}
