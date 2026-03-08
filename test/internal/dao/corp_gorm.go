package dao

import (
	"context"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"gorm.io/gorm"
)

var globalCorpDao *CorpDao

const (
	corpTableName = "t_corp"

	CorpTblColID             = "id"
	CorpTblColSID            = "sid"
	CorpTblColBusinessID     = "business_id"
	CorpTblColUin            = "uin"
	CorpTblColFullName       = "full_name"
	CorpTblColCreateUserID   = "create_user_id"
	CorpTblColCellphone      = "cellphone"
	CorpTblColRobotQuota     = "robot_quota"
	CorpTblColContactName    = "contact_name"
	CorpTblColEmail          = "email"
	CorpTblColStatus         = "status"
	CorpTblColIsTrial        = "is_trial"
	CorpTblColMaxCharSize    = "max_char_size"
	CorpTblColMaxTokenUsage  = "max_token_usage"
	CorpTblColTrialStartTime = "trial_start_time"
	CorpTblColTrialEndTime   = "trial_end_time"
	CorpTblColCreateTime     = "create_time"
	CorpTblColUpdateTime     = "update_time"
)

type CorpDao struct {
	BaseDao
}

// GetCorpDao 获取全局的数据操作对象
func GetCorpDao() *CorpDao {
	if globalCorpDao == nil {
		globalCorpDao = &CorpDao{*globalBaseDao}
	}
	return globalCorpDao
}

type CorpFilter struct {
	IDs         []uint64 // 应用自增ID
	BusinessIds []uint64 // 应用业务ID
	Offset      uint32
	Limit       uint32
}

// 生成查询条件，必须按照索引的顺序排列
func generateCorpCondition(ctx context.Context, session *gorm.DB, filter *CorpFilter) {
	if len(filter.IDs) > 0 {
		session.Where(CorpTblColID+sqlIn, filter.IDs)
	}
	if len(filter.BusinessIds) > 0 {
		session.Where(CorpTblColBusinessID+sqlIn, filter.BusinessIds)
	}
}

// GetCorpList 获取应用信息
func (d *CorpDao) GetCorpList(ctx context.Context, selectColumns []string, filter *CorpFilter) ([]*model.Corp, error) {
	corps := make([]*model.Corp, 0)
	session := d.gormDB.Table(corpTableName).Select(selectColumns)
	generateCorpCondition(ctx, session, filter)
	if filter.Limit == 0 || filter.Limit > DefaultMaxPageSize {
		filter.Limit = DefaultMaxPageSize
	}
	session = session.Offset(int(filter.Offset)).Limit(int(filter.Limit))
	res := session.Find(&corps)
	if res.Error != nil {
		log.ErrorContextf(ctx, "execute sql failed, err: %+v", res.Error)
		return corps, res.Error
	}
	return corps, nil
}
