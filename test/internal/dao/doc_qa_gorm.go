package dao

import (
	"context"
	"errors"
	"fmt"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/client"
	knowClient "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"gorm.io/gorm"
)

var globalDocQaDao *DocQaDao

const (
	docQaTableName = "t_doc_qa"

	DocQaTblColId              = "id"
	DocQaTblColBusinessId      = "business_id"
	DocQaTblColRobotId         = "robot_id"
	DocQaTblColCorpId          = "corp_id"
	DocQaTblColStaffId         = "staff_id"
	DocQaTblColDocId           = "doc_id"
	DocQaTblColOriginDocId     = "origin_doc_id"
	DocQaTblColSegmentId       = "segment_id"
	DocQaTblColCategoryId      = "category_id"
	DocQaTblColSource          = "source"
	DocQaTblColQuestion        = "question"
	DocQaTblColAnswer          = "answer"
	DocQaTblColCustomParam     = "custom_param"
	DocQaTblColQuestionDesc    = "question_desc"
	DocQaTblColReleaseStatus   = "release_status"
	DocQaTblColIsAuditFree     = "is_audit_free"
	DocQaTblColIsDeleted       = "is_deleted"
	DocQaTblColMessage         = "message"
	DocQaTblColAcceptStatus    = "accept_status"
	DocQaTblColSimilarStatus   = "similar_status"
	DocQaTblColNextAction      = "next_action"
	DocQaTblColCharSize        = "char_size"
	DocQaTblColAttrRange       = "attr_range"
	DocQaTblColCreateTime      = "create_time"
	DocQaTblColUpdateTime      = "update_time"
	DocQaTblColExpireStart     = "expire_start"
	DocQaTblColExpireEnd       = "expire_end"
	DocQaTblColSimilarQuestion = "similar_question"
	DocQaTblColAttributeFlag   = "attribute_flag"

	docQaTableMaxPageSize = 1000
)

var DocQaTblColList = []string{DocQaTblColId, DocQaTblColBusinessId, DocQaTblColRobotId, DocQaTblColCorpId,
	DocQaTblColStaffId, DocQaTblColDocId, DocQaTblColOriginDocId, DocQaTblColSegmentId, DocQaTblColCategoryId,
	DocQaTblColSource, DocQaTblColQuestion, DocQaTblColAnswer, DocQaTblColCustomParam, DocQaTblColQuestionDesc,
	DocQaTblColReleaseStatus, DocQaTblColIsAuditFree, DocQaTblColIsDeleted, DocQaTblColMessage, DocQaTblColAcceptStatus,
	DocQaTblColSimilarStatus, DocQaTblColNextAction, DocQaTblColCharSize, DocQaTblColAttrRange, DocQaTblColCreateTime,
	DocQaTblColUpdateTime, DocQaTblColExpireStart, DocQaTblColExpireEnd}

type DocQaDao struct {
	BaseDao
	tableName string
}

// GetDocQaDao 获取全局的数据操作对象
func GetDocQaDao() *DocQaDao {
	if globalDocQaDao == nil {
		globalDocQaDao = &DocQaDao{*globalBaseDao, docQaTableName}
	}
	return globalDocQaDao
}

type DocQaFilter struct {
	BusinessId     uint64 // 业务 ID
	CorpId         uint64 // 企业 ID
	RobotId        uint64
	DocID          uint64
	IsDeleted      *int
	BusinessIds    []uint64
	MaxUpdateTime  time.Time
	Offset         uint32
	Limit          uint32
	OrderColumn    []string
	OrderDirection []string
	CategoryIds    []uint64
	ReleaseStatus  []uint32
	AcceptStatus   uint32
	ReleaseCount   bool

	SegmentIDs []uint64
	RobotIDs   []uint64
}

// 生成查询条件，必须按照索引的顺序排列
func (d *DocQaDao) generateCondition(ctx context.Context, session *gorm.DB, filter *DocQaFilter) {
	if filter.CorpId != 0 {
		session = session.Where(DocQaTblColCorpId+sqlEqual, filter.CorpId)
	}
	if filter.RobotId != 0 {
		session = session.Where(DocQaTblColRobotId+sqlEqual, filter.RobotId)
	}
	if filter.BusinessId != 0 {
		// 业务Id为该表唯一索引
		session = session.Where(DocQaTblColBusinessId+sqlEqual, filter.BusinessId)
	}
	if len(filter.BusinessIds) != 0 {
		// 业务Id为该表唯一索引
		session = session.Where(DocQaTblColBusinessId+sqlIn, filter.BusinessIds)
	}

	if len(filter.CategoryIds) != 0 {
		// 分类
		session = session.Where(DocQaTblColCategoryId+sqlIn, filter.CategoryIds)
	}

	if filter.DocID != 0 {
		session = session.Where(DocQaTblColDocId+sqlEqual, filter.DocID)
	}
	if len(filter.SegmentIDs) != 0 {
		session = session.Where(DocQaTblColSegmentId+sqlIn, filter.SegmentIDs)
	}
	if len(filter.RobotIDs) != 0 {
		session = session.Where(DocQaTblColRobotId+sqlIn, filter.RobotIDs)
	}

	if len(filter.ReleaseStatus) != 0 {
		// 业务Id为该表唯一索引
		session = session.Where(DocQaTblColReleaseStatus+sqlIn, filter.ReleaseStatus)
	}
	if filter.AcceptStatus != 0 {
		session = session.Where(DocQaTblColAcceptStatus+sqlEqual, filter.AcceptStatus)
	}

	// 查询当前未发布(不包括未发布删除的)和已发布修改过(包括已发布后进行删除、修改)的数量
	if filter.ReleaseCount {
		//session = session.Where(DocQaTblColNextAction+sqlNotEqual, filter.NotNextAction)
		// 添加复杂条件: (next_action != 1 AND is_deleted = 2) or (is_deleted = 1)
		session = session.Where(
			"("+DocQaTblColNextAction+sqlNotEqual+" AND "+DocQaTblColIsDeleted+sqlEqual+") OR ("+
				DocQaTblColIsDeleted+sqlEqual+")",
			model.ReleaseActionAdd, model.QAIsDeleted, model.QAIsNotDeleted,
		)
	}

	// is_deleted必须加入查询条件
	if filter.IsDeleted != nil {
		session = session.Where(DocQaTblColIsDeleted+sqlEqual, *filter.IsDeleted)
	}

	if !filter.MaxUpdateTime.IsZero() {
		session = session.Where(DocQaTblColUpdateTime+sqlLess, filter.MaxUpdateTime)
	}
}

// GetDocQas 获取问答对
func (d *DocQaDao) GetDocQas(ctx context.Context, selectColumns []string, filter *DocQaFilter) ([]*model.DocQA, error) {
	docQas := make([]*model.DocQA, 0)
	session := d.gormDB.WithContext(ctx).Table(docQaTableName).Select(selectColumns)
	d.generateCondition(ctx, session, filter)
	if filter.Limit == 0 || filter.Limit > docQaTableMaxPageSize {
		filter.Limit = docQaTableMaxPageSize
	}
	session = session.Offset(int(filter.Offset)).Limit(int(filter.Limit))
	for i, orderColumn := range filter.OrderColumn {
		if filter.OrderDirection[i] != SqlOrderByAsc && filter.OrderDirection[i] != SqlOrderByDesc {
			log.ErrorContextf(ctx, "invalid order direction: %s", filter.OrderDirection[i])
			continue
		}
		session.Order(orderColumn + " " + filter.OrderDirection[i])
	}
	res := session.Find(&docQas)
	if res.Error != nil {
		log.ErrorContextf(ctx, "execute sql failed, err: %+v", res.Error)
		return docQas, res.Error
	}
	return docQas, nil
}

// GetDocQaCount 获取问答总数
func (d *DocQaDao) GetDocQaCount(ctx context.Context, selectColumns []string,
	filter *DocQaFilter) (int64, error) {
	db, err := knowClient.GormClient(ctx, docQaTableName, filter.RobotId, 0, []client.Option{}...)
	if err != nil {
		log.ErrorContextf(ctx, "get GormClient failed, err: %+v", err)
		return 0, err
	}
	session := db.WithContext(ctx).Table(docQaTableName).Select(selectColumns)
	d.generateCondition(ctx, session, filter)
	count := int64(0)
	res := session.Count(&count)
	if res.Error != nil {
		log.ErrorContextf(ctx, "execute sql failed, err: %+v", res.Error)
		return 0, res.Error
	}
	return count, nil
}

// getDocQaList 获取问答对
func (d *DocQaDao) getDocQaList(ctx context.Context, selectColumns []string, filter *DocQaFilter) ([]*model.DocQA, error) {
	docQas := make([]*model.DocQA, 0)
	if filter.Limit == 0 {
		// 为0正常返回空结果即可
		return docQas, nil
	}
	if filter.Limit > docQaTableMaxPageSize {
		// 限制单次查询最大条数
		err := errors.New(fmt.Sprintf("invalid limit: %d", filter.Limit))
		log.ErrorContextf(ctx, "getDocQaList err: %+v", err)
		return docQas, err
	}
	session := d.gormDB.WithContext(ctx).Table(d.tableName).Select(selectColumns)
	d.generateCondition(ctx, session, filter)
	session = session.Offset(int(filter.Offset)).Limit(int(filter.Limit))
	for i, orderColumn := range filter.OrderColumn {
		if filter.OrderDirection[i] != SqlOrderByAsc && filter.OrderDirection[i] != SqlOrderByDesc {
			log.ErrorContextf(ctx, "invalid order direction: %s", filter.OrderDirection[i])
			continue
		}
		session.Order(orderColumn + " " + filter.OrderDirection[i])
	}
	res := session.Find(&docQas)
	if res.Error != nil {
		log.ErrorContextf(ctx, "execute sql failed, err: %+v", res.Error)
		return docQas, res.Error
	}
	return docQas, nil
}

func (d *DocQaDao) GetDocQaList(ctx context.Context, selectColumns []string, filter *DocQaFilter) ([]*model.DocQA, error) {
	log.DebugContextf(ctx, "GetDocQaList filter:%+v", filter)
	allDocQaList := make([]*model.DocQA, 0)
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
		docQaList, err := d.getDocQaList(ctx, selectColumns, filter)
		if err != nil {
			log.ErrorContextf(ctx, "GetDocQaList failed, err: %+v", err)
			return nil, err
		}
		allDocQaList = append(allDocQaList, docQaList...)
		if uint32(len(docQaList)) < filter.Limit {
			// 已分页遍历完所有数据
			break
		}
		offset += filter.Limit
		alreadyGetCount += filter.Limit
	}
	log.DebugContextf(ctx, "GetDocQaList count:%d cost:%dms",
		len(allDocQaList), time.Since(beginTime).Milliseconds())
	return allDocQaList, nil
}

// GetAllDocQas 获取所有问答对
func (d *DocQaDao) GetAllDocQas(ctx context.Context, selectColumns []string, filter *DocQaFilter) ([]*model.DocQA, error) {
	beginTime := time.Now()
	offset := 0
	limit := docQaTableMaxPageSize
	allDocQas := make([]*model.DocQA, 0)
	for {
		filter.Offset = uint32(offset)
		filter.Limit = uint32(limit)

		docQas, err := d.GetDocQas(ctx, selectColumns, filter)
		if err != nil {
			log.ErrorContextf(ctx, "GetAllDocQas failed, err: %+v", err)
			return nil, err
		}
		allDocQas = append(allDocQas, docQas...)
		if len(docQas) < limit {
			// 已分页遍历完所有数据
			break
		}
		offset += limit
	}
	log.DebugContextf(ctx, "GetAllDocQas count:%d cost:%dms",
		len(allDocQas), time.Since(beginTime).Milliseconds())
	return allDocQas, nil
}

// GetQasBySegmentIDs 通过segment id获取qa
func (d *DocQaDao) GetQasBySegmentIDs(ctx context.Context, corpID, docID uint64, segmentIDs []uint64) ([]*model.DocQA, error) {
	filter := &DocQaFilter{
		CorpId:     corpID,
		DocID:      docID,
		SegmentIDs: segmentIDs,
	}
	return d.GetAllDocQas(ctx, DocQaTblColList, filter)
}

// UpdateDocQas 更新doc qa
func (d *DocQaDao) UpdateDocQas(ctx context.Context, updateColumns []string, filter *DocQaFilter, docQa *model.DocQA) error {
	session := d.gormDB.WithContext(ctx).Table(docQaTableName).Select(updateColumns)
	d.generateCondition(ctx, session, filter)
	res := session.Updates(docQa)
	if res.Error != nil {
		log.ErrorContextf(ctx, "UpdateDocQas failed, col: %v, param: %+v, qa: %+v, err: %+v",
			updateColumns, filter, docQa, res.Error)
		return res.Error
	}
	return nil

}

// UpdateQAWaitRelease 更新问答状态到待发布
func (d *DocQaDao) UpdateQAWaitRelease(ctx context.Context, appID, qaID uint64, simBizIDs []uint64) (err error) {
	//开启事务
	tx := d.gormDB.Begin()
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
		} else if err != nil {
			tx.Rollback()
		} else {
			tx.Commit()
		}
	}()
	//1.更新问答标准问的状态
	err = tx.WithContext(ctx).Table(docQaTableName).Where(DocQaTblColRobotId+sqlEqual, appID).
		Where(DocQaTblColId+sqlEqual, qaID).Limit(1).Updates(map[string]any{
		DocQaTblColReleaseStatus: model.QAReleaseStatusInit,
		DocQaTblColUpdateTime:    time.Now(),
	}).Error
	if err != nil {
		log.ErrorContextf(ctx, "UpdateQAWaitRelease failed qa:%v, err: %+v", qaID, err)
		return err
	}
	//2.更新问答相似问的状态
	if len(simBizIDs) > 0 {
		err = tx.WithContext(ctx).Table(docQaSimTableName).Where(DocQaSimTblColRobotId+sqlEqual, appID).
			Where(DocQaSimTblColSimilarId+sqlIn, simBizIDs).Where(DocQaSimTblColIsDelted+sqlEqual, model.QAIsNotDeleted).
			Limit(len(simBizIDs)).Updates(map[string]any{
			DocQaSimTblColReleaseStatus: model.QAReleaseStatusInit,
			DocQaSimTblColUpdateTime:    time.Now(),
		}).Error
		if err != nil {
			log.ErrorContextf(ctx, "UpdateQAWaitRelease failed simBizIDs:%v,qaID:%v,err: %+v", simBizIDs, qaID, err)
			return err
		}
	}
	return nil
}

// GetDocQACount 获取问答总数
func (d *DocQaDao) GetDocQACount(ctx context.Context,
	filter *DocQaFilter) (int64, error) {
	session := d.gormDB.WithContext(ctx).Table(docQaTableName)
	d.generateCondition(ctx, session, filter)
	count := int64(0)
	res := session.Count(&count)
	if res.Error != nil {
		log.ErrorContextf(ctx, "execute sql failed, err: %+v", res.Error)
		return 0, res.Error
	}
	return count, nil
}
