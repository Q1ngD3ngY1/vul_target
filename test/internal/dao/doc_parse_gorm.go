package dao

import (
	"context"
	"errors"
	"fmt"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/client"
	"git.code.oa.com/trpc-go/trpc-go/log"
	knowClient "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"gorm.io/gorm"
)

var globalDocParseDao *DocParseDao

const (
	DocParseTblColID           = "id"
	DocParseTblColRobotID      = "robot_id"
	DocParseTblColCorpID       = "corp_id"
	DocParseTblColRequestID    = "request_id"
	DocParseTblColDocID        = "doc_id"
	DocParseTblColSourceEnvSet = "source_env_set"
	DocParseTblColTaskID       = "task_id"
	DocParseTblColType         = "type"
	DocParseTblColOpType       = "op_type"
	DocParseTblColResult       = "result"
	DocParseTblColStatus       = "status"
	DocParseTblColUpdateTime   = "update_time"
	DocParseTblColCreateTime   = "create_time"

	DocParseMaxPageSize = 1000
)

var DocParseTblColList = []string{
	DocParseTblColID,
	DocParseTblColRobotID,
	DocParseTblColCorpID,
	DocParseTblColRequestID,
	DocParseTblColDocID,
	DocParseTblColSourceEnvSet,
	DocParseTblColTaskID,
	DocParseTblColType,
	DocParseTblColOpType,
	DocParseTblColResult,
	DocParseTblColStatus,
	DocParseTblColUpdateTime,
	DocParseTblColCreateTime,
}

type DocParseDao struct {
	BaseDao
	tableName string
}

// GetDocParseDao 获取全局的数据操作对象
func GetDocParseDao() *DocParseDao {
	if globalDocParseDao == nil {
		globalDocParseDao = &DocParseDao{*globalBaseDao, docParseTableName}
	}
	return globalDocParseDao
}

type DocParseFilter struct {
	RouterAppBizID uint64
	CorpID         uint64
	RobotID        uint64
	DocID          uint64
	Status         []int32
	Offset         uint32
	Limit          uint32
	OrderColumn    []string
	OrderDirection []string
}

// 生成查询条件，必须按照索引的顺序排列
func (d *DocParseDao) generateCondition(ctx context.Context, session *gorm.DB, filter *DocParseFilter) {
	if filter.CorpID != 0 {
		session.Where(DocParseTblColCorpID+sqlEqual, filter.CorpID)
	}
	if filter.RobotID != 0 {
		session.Where(DocParseTblColRobotID+sqlEqual, filter.RobotID)
	}
	if filter.DocID != 0 {
		session = session.Where(DocParseTblColDocID+sqlEqual, filter.DocID)
	}
	if len(filter.Status) > 0 {
		session = session.Where(DocParseTblColStatus+sqlIn, filter.Status)
	}
}

// DeleteDocParseByDocID 物理删除某一文档的解析任务数据
func (d *DocParseDao) DeleteDocParseByDocID(ctx context.Context, corpID,
	robotID, docID uint64) error {
	db, err := knowClient.GormClient(ctx, d.tableName, robotID, 0, []client.Option{}...)
	if err != nil {
		log.ErrorContextf(ctx, "get GormClient failed, err: %+v", err)
		return err
	}
	session := db.WithContext(ctx).Table(d.tableName).
		Where(DocParseTblColCorpID+sqlEqual, corpID).
		Where(DocParseTblColRobotID+sqlEqual, robotID).
		Where(DocParseTblColDocID+sqlEqual, docID).Limit(DocParseMaxPageSize)
	res := session.Delete(&model.DocParse{})
	if res.Error != nil {
		log.ErrorContextf(ctx, "DeleteDocParseByDocID|docID:%d|err:%+v",
			docID, res.Error)
		return res.Error
	}
	return nil
}

func (d *DocParseDao) getDocParseList(ctx context.Context, selectColumns []string, filter *DocParseFilter) ([]*model.DocParse, error) {
	docParses := make([]*model.DocParse, 0)
	if filter.Limit == 0 {
		// 为0正常返回空结果即可
		return docParses, nil
	}
	if filter.Limit > DefaultMaxPageSize {
		// 限制单次查询最大条数
		err := errors.New(fmt.Sprintf("invalid limit: %d", filter.Limit))
		log.ErrorContextf(ctx, "GetDocParseList err: %+v", err)
		return docParses, err
	}
	db, err := knowClient.GormClient(ctx, d.tableName, filter.RobotID, filter.RouterAppBizID, []client.Option{}...)
	if err != nil {
		log.ErrorContextf(ctx, "get GormClient failed, err: %+v", err)
		return docParses, err
	}
	session := db.WithContext(ctx).Table(d.tableName).Select(selectColumns)
	d.generateCondition(ctx, session, filter)
	session = session.Offset(int(filter.Offset)).Limit(int(filter.Limit))
	for i, orderColumn := range filter.OrderColumn {
		if filter.OrderDirection[i] != SqlOrderByAsc && filter.OrderDirection[i] != SqlOrderByDesc {
			log.ErrorContextf(ctx, "invalid order direction: %s", filter.OrderDirection[i])
			continue
		}
		session.Order(orderColumn + " " + filter.OrderDirection[i])
	}
	res := session.Find(&docParses)
	if res.Error != nil {
		log.ErrorContextf(ctx, "execute sql failed, err: %+v", res.Error)
		return docParses, res.Error
	}
	return docParses, nil
}

// GetDocParseList 获取文档解析任务列表
func (d *DocParseDao) GetDocParseList(ctx context.Context, selectColumns []string,
	filter *DocParseFilter) ([]*model.DocParse, error) {
	log.DebugContextf(ctx, "GetDocParseList filter:%+v", filter)
	allDocParseList := make([]*model.DocParse, 0)
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
		docParseList, err := d.getDocParseList(ctx, selectColumns, filter)
		if err != nil {
			log.ErrorContextf(ctx, "GetDocParseList failed, err: %+v", err)
			return nil, err
		}
		allDocParseList = append(allDocParseList, docParseList...)
		if uint32(len(docParseList)) < filter.Limit {
			// 已分页遍历完所有数据
			break
		}
		offset += filter.Limit
		alreadyGetCount += filter.Limit
	}
	log.DebugContextf(ctx, "GetDocParseList count:%d cost:%dms",
		len(allDocParseList), time.Since(beginTime).Milliseconds())
	return allDocParseList, nil
}
