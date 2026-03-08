// COMMENT(ericjwang): 文档检索节点信息表数据操作，本身这个表是 retrieval 领域的，这个文件不好归属，又是用 gorm 写的，不重构了
// COMMENT (wemysschen): 这个表目前还是放在了vector下面，后面和retrieval协商看看能否通过rpc来调用。
package vector

import (
	"context"

	"git.code.oa.com/trpc-go/trpc-go/client"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	knowClient "git.woa.com/adp/kb/kb-config/internal/rpc"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"golang.org/x/exp/slices"
	"gorm.io/gorm"
)

// GetNodeIdsList 获取节点id列表
func (d *daoImpl) GetNodeIdsList(ctx context.Context, appID uint64, selectColumns []string,
	filter *entity.RetrievalNodeFilter) (nodeList []*entity.RetrievalNodeInfo, err error) {
	db, err := knowClient.GormClient(ctx, retrievalNodeInfoTable, appID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "GetNodeIdsList get GormClient failed, err:%+v,appID:%v", err, appID)
		return nil, err
	}

	// 单次查询的最大限制，避免性能问题
	const maxBatchSize = 500
	// 默认批次大小
	batchSize := 300

	// 如果filter中指定了Limit，需要考虑总数限制
	var totalLimit int
	if filter != nil && filter.Limit > 0 {
		totalLimit = filter.Limit
		// 如果总限制小于批次大小，调整批次大小
		if totalLimit < batchSize {
			batchSize = totalLimit
		}
	}

	startID := uint64(0)
	for {
		// 检查是否已达到总数限制
		if totalLimit > 0 && len(nodeList) >= totalLimit {
			break
		}

		// 计算本次查询的数量
		currentBatchSize := batchSize
		if totalLimit > 0 {
			remaining := totalLimit - len(nodeList)
			if remaining < currentBatchSize {
				currentBatchSize = remaining
			}
		}

		// 确保单次查询不超过最大限制
		if currentBatchSize > maxBatchSize {
			currentBatchSize = maxBatchSize
		}

		var tmp []*entity.RetrievalNodeInfo
		sql := db.WithContext(ctx).Model(&entity.RetrievalNodeInfo{}).Where(NodeTblColIsDeleted+util.SqlEqual, 0).
			Where(NodeTblColId+util.SqlMore, startID)
		d.makeUserCondition(sql, filter)
		if len(selectColumns) != 0 {
			// 确保ID字段始终被查询，避免死循环
			if !slices.Contains(selectColumns, NodeTblColId) {
				selectColumns = append(selectColumns, NodeTblColId)
			}
			sql.Select(selectColumns)
		}
		// 添加排序确保结果稳定
		sql.Order(NodeTblColId + " " + util.SqlOrderByAsc)
		err := sql.Limit(currentBatchSize).Find(&tmp).Error
		if err != nil {
			logx.E(ctx, "GetNodeIdsList get failed, err:%+v,appID:%v,filter:%+v,startID:%v", err, appID, filter, startID)
			return nil, err
		}
		if len(tmp) != 0 {
			nodeList = append(nodeList, tmp...)
		}
		// 如果查询结果少于批次大小，说明已经没有更多数据
		if len(tmp) < currentBatchSize {
			break
		}
		// 安全检查：确保startID有更新，避免死循环
		newStartID := tmp[len(tmp)-1].ID
		if newStartID == 0 || newStartID <= startID {
			logx.E(ctx, "GetNodeIdsList startID not updated, potential infinite loop, startID:%v, newStartID:%v", startID, newStartID)
			break
		}
		startID = newStartID
	}

	// 如果设置了总数限制，确保返回的数据不超过限制
	if totalLimit > 0 && len(nodeList) > totalLimit {
		nodeList = nodeList[:totalLimit]
	}

	return nodeList, nil
}

func (d *daoImpl) makeUserCondition(sql *gorm.DB, filter *entity.RetrievalNodeFilter) {
	if filter.APPID != 0 {
		sql.Where(NodeTblColRobotId+util.SqlEqual, filter.APPID)
	}
	if filter.DocType != 0 {
		sql.Where(NodeTblColDocType+util.SqlEqual, filter.DocType)
	}
	if filter.DocID != 0 {
		sql.Where(NodeTblColDocId+util.SqlEqual, filter.DocID)
	}
	if filter.ParentID != 0 {
		sql.Where(NodeTblColParentId+util.SqlEqual, filter.ParentID)
	}
	if filter.RelatedID != 0 {
		sql.Where(NodeTblColRelatedId+util.SqlEqual, filter.RelatedID)
	}
}

// GetDocNodeList 获取文档节点列表
func (d *daoImpl) GetDocNodeList(ctx context.Context, appID uint64) (nodeList []*entity.RetrievalNodeInfo, err error) {
	db, err := knowClient.GormClient(ctx, retrievalNodeInfoTable, appID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "GetDocNodeList get GormClient failed, err:%+v,appID:%v", err, appID)
		return nil, err
	}
	lastDocID, limit := uint64(0), 300
	for {
		var tmp []*entity.RetrievalNodeInfo
		err := db.WithContext(ctx).Model(&entity.RetrievalNodeInfo{}).
			Where(NodeTblColRobotId+util.SqlEqual, appID).
			Where(NodeTblColDocId+util.SqlMore, lastDocID).
			Where(NodeTblColIsDeleted+util.SqlEqual, 0).
			Select([]string{NodeTblColDocId}).
			Group(NodeTblColDocId).
			Order(NodeTblColDocId + " " + util.SqlOrderByAsc).Limit(limit).Find(&tmp).Error
		if err != nil {
			logx.E(ctx, "GetDocNodeList get failed, err:%+v,appID:%v,lastDocID:%v", err, appID, lastDocID)
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
func (d *daoImpl) GetSegmentNodeByDocID(ctx context.Context, appID, docID, startID, count uint64, selectColumns []string) (
	[]*entity.RetrievalNodeInfo, uint64, error) {
	db, err := knowClient.GormClient(ctx, retrievalNodeInfoTable, appID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "GetSegmentNodeByDocID get GormClient failed, err:%+v,appID:%v", err, appID)
		return nil, 0, err
	}
	nodes, limit, lastID := make([]*entity.RetrievalNodeInfo, 0, count), 500, startID
	for {
		var tmp []*entity.RetrievalNodeInfo
		err := db.WithContext(ctx).Model(&entity.RetrievalNodeInfo{}).Select(selectColumns).
			Where(NodeTblColIsDeleted+util.SqlEqual, 0).Where(NodeTblColDocType+util.SqlEqual, entity.DocTypeSegment).
			Where(NodeTblColRobotId+util.SqlEqual, appID).Where(NodeTblColDocId+util.SqlEqual, docID).
			Where(NodeTblColId+util.SqlMore, lastID). // 避免深分页
			Limit(limit).Find(&tmp).Error
		if err != nil {
			logx.E(ctx, "GetSegmentNodeByDocID get err:%v,appID:%v,docID:%v", err, appID, docID)
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
