package dao

import (
	"context"

	"git.code.oa.com/trpc-go/trpc-go/client"
	"git.code.oa.com/trpc-go/trpc-go/log"
	knowClient "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"gorm.io/gorm"
)

var globalRetrievalNodeDao *RetrievalNodeDao

const (
	retrievalNodeInfoTable = "t_retrieval_node_info"

	NodeTblColId           = "id"            // ID
	NodeTblColRobotId      = "robot_id"      // 机器人ID
	NodeTblColDocType      = "doc_type"      // 类型: 1 QA, 2 DocSegment
	NodeTblColRelatedId    = "related_id"    // 关联id
	NodeTblColDocId        = "doc_id"        // 文档ID
	NodeTblColParentId     = "parent_id"     // 上一层的ID
	NodeTblColSegmentType  = "segment_type"  // 文档切片类型
	NodeTblColPageContent  = "page_content"  // 内容
	NodeTblColOrgData      = "org_data"      // 原始内容
	NodeTblColBigDataId    = "big_data_id"   // big_data标识id
	NodeTblColQuestion     = "question"      // 问题
	NodeTblColAnswer       = "answer"        // 答案
	NodeTblColCustomParam  = "custom_param"  // 自定义参数
	NodeTblColQuestionDesc = "question_desc" // 问题描述
	NodeTblColLabels       = "labels"        // 标签
	NodeTblColReserve1     = "reserve1"      // 预留字段1
	NodeTblColReserve2     = "reserve2"      // 预留字段2
	NodeTblColReserve3     = "reserve3"      // 预留字段3
	NodeTblColIsDeleted    = "is_deleted"    // 删除标记
	NodeTblColCreateTime   = "create_time"   // 创建时间
	NodeTblColUpdateTime   = "update_time"   // 更新时间
	NodeTblColExpireTime   = "expire_time"   // 过期时间
)

type RetrievalNodeDao struct {
	BaseDao
	tableName string
}

// GetRetrievalNodeDao 获取全局的数据操作对象
func GetRetrievalNodeDao() *RetrievalNodeDao {
	if globalRetrievalNodeDao == nil {
		globalRetrievalNodeDao = &RetrievalNodeDao{*globalBaseDao, retrievalNodeInfoTable}
	}
	return globalRetrievalNodeDao
}

type RetrievalNodeFilter struct {
	APPID     uint64
	DocType   uint32
	RelatedID uint64
	DocID     uint64
	ParentID  uint64 //问答标准问主键id
}

// GetNodeIdsList 获取节点id列表
func (r *RetrievalNodeDao) GetNodeIdsList(ctx context.Context, appID uint64, selectColumns []string,
	filter *RetrievalNodeFilter) (nodeList []*model.RetrievalNodeInfo, err error) {
	db, err := knowClient.GormClient(ctx, retrievalNodeInfoTable, appID, 0, []client.Option{}...)
	if err != nil {
		log.ErrorContextf(ctx, "GetNodeIdsList get GormClient failed, err:%+v,appID:%v", err, appID)
		return nil, err
	}
	startID, limit := uint64(0), 300
	for {
		var tmp []*model.RetrievalNodeInfo
		sql := db.WithContext(ctx).Model(&model.RetrievalNodeInfo{}).Where(NodeTblColIsDeleted+sqlEqual, 0).
			Where(NodeTblColId+sqlMore, startID)
		r.makeUserCondition(sql, filter)
		if len(selectColumns) != 0 {
			sql.Select(selectColumns)
		}
		err := sql.Limit(limit).Find(&tmp).Error
		if err != nil {
			log.ErrorContextf(ctx, "GetNodeIdsList get failed, err:%+v,appID:%v,filter:%+v,startID:%v", err, appID, filter, startID)
			return nil, err
		}
		if len(tmp) != 0 {
			nodeList = append(nodeList, tmp...)
		}
		if len(tmp) < int(limit) {
			break
		}
		startID = tmp[len(tmp)-1].ID
	}
	return nodeList, nil
}

func (r *RetrievalNodeDao) makeUserCondition(sql *gorm.DB, filter *RetrievalNodeFilter) {
	if filter.APPID != 0 {
		sql.Where(NodeTblColRobotId+sqlEqual, filter.APPID)
	}
	if filter.DocType != 0 {
		sql.Where(NodeTblColDocType+sqlEqual, filter.DocType)
	}
	if filter.DocID != 0 {
		sql.Where(NodeTblColDocId+sqlEqual, filter.DocID)
	}
	if filter.ParentID != 0 {
		sql.Where(NodeTblColParentId+sqlEqual, filter.ParentID)
	}
}

// GetDocNodeList 获取文档节点列表
func (r *RetrievalNodeDao) GetDocNodeList(ctx context.Context, appID uint64) (nodeList []*model.RetrievalNodeInfo, err error) {
	db, err := knowClient.GormClient(ctx, retrievalNodeInfoTable, appID, 0, []client.Option{}...)
	if err != nil {
		log.ErrorContextf(ctx, "GetDocNodeList get GormClient failed, err:%+v,appID:%v", err, appID)
		return nil, err
	}
	lastDocID, limit := uint64(0), 300
	for {
		var tmp []*model.RetrievalNodeInfo
		err := db.WithContext(ctx).Model(&model.RetrievalNodeInfo{}).
			Where(NodeTblColRobotId+sqlEqual, appID).
			Where(NodeTblColDocId+sqlMore, lastDocID).
			Where(NodeTblColIsDeleted+sqlEqual, 0).
			Select([]string{NodeTblColDocId}).
			Group(NodeTblColDocId).
			Order(NodeTblColDocId + " " + SqlOrderByAsc).Limit(limit).Find(&tmp).Error
		if err != nil {
			log.ErrorContextf(ctx, "GetDocNodeList get failed, err:%+v,appID:%v,lastDocID:%v", err, appID, lastDocID)
			return nil, err
		}
		if len(tmp) != 0 {
			nodeList = append(nodeList, tmp...)
		}
		if len(tmp) < int(limit) {
			break
		}
		lastDocID = tmp[len(tmp)-1].DocID
	}
	return nodeList, nil
}

// GetSegmentNodeByDocID 批量获取单文档切片节点列表
func (r *RetrievalNodeDao) GetSegmentNodeByDocID(ctx context.Context, appID, docID, startID, count uint64, selectColumns []string) (
	[]*model.RetrievalNodeInfo, uint64, error) {
	db, err := knowClient.GormClient(ctx, retrievalNodeInfoTable, appID, 0, []client.Option{}...)
	if err != nil {
		log.ErrorContextf(ctx, "GetSegmentNodeByDocID get GormClient failed, err:%+v,appID:%v", err, appID)
		return nil, 0, err
	}
	nodes, limit, lastID := make([]*model.RetrievalNodeInfo, 0, count), 500, startID
	for {
		var tmp []*model.RetrievalNodeInfo
		err := db.WithContext(ctx).Model(&model.RetrievalNodeInfo{}).Select(selectColumns).
			Where(NodeTblColIsDeleted+sqlEqual, 0).Where(NodeTblColDocType+sqlEqual, model.DocTypeSegment).
			Where(NodeTblColRobotId+sqlEqual, appID).Where(NodeTblColDocId+sqlEqual, docID).
			Where(NodeTblColId+sqlMore, lastID). //避免深分页
			Limit(limit).Find(&tmp).Error
		if err != nil {
			log.ErrorContextf(ctx, "GetSegmentNodeByDocID get err:%v,appID:%v,docID:%v", err, appID, docID)
			return nil, 0, err
		}
		if len(tmp) != 0 {
			nodes = append(nodes, tmp...)
			lastID = tmp[len(tmp)-1].ID
		}
		if len(tmp) < int(limit) || len(nodes) >= int(count) {
			break
		}
	}
	return nodes, lastID, nil
}
