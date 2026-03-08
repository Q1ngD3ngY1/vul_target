package segment

import (
	"context"
	"fmt"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/client"
	"git.woa.com/adp/common/x/logx"
	segEntity "git.woa.com/adp/kb/kb-config/internal/entity/segment"
	knowClient "git.woa.com/adp/kb/kb-config/internal/rpc"
	"git.woa.com/adp/kb/kb-config/internal/util/idgen"
	"gorm.io/gorm"
)

// getDocSegmentPageInfo 获取文档切片页码信息
func (l *Logic) getDocSegmentPageInfo(ctx context.Context, segment *segEntity.DocSegmentExtend) (
	*segEntity.DocSegmentPageInfo, error) {
	if !segment.IsSegmentForIndex() {
		logx.I(ctx, "getDocSegmentPageInfo|segment:%+v|Type is ignore", segment)
		return nil, nil
	}
	return &segEntity.DocSegmentPageInfo{
		PageInfoID:     idgen.GetId(),
		DocID:          segment.DocID,
		RobotID:        segment.RobotID,
		CorpID:         segment.CorpID,
		StaffID:        segment.StaffID,
		OrgPageNumbers: segment.OrgPageNumbers,
		BigPageNumbers: segment.BigPageNumbers,
		SheetData:      segment.SheetData,
		IsDeleted:      segment.IsDeleted,
		CreateTime:     time.Now(),
		UpdateTime:     time.Now(),
		SegmentBizID:   segment.BusinessID,
	}, nil
}

// createDocSegmentImages 文档切片Images入库
func (l *Logic) createDocSegmentPageInfos(ctx context.Context, tx *gorm.DB,
	segmentBizIDMap map[uint64]*segEntity.DocSegmentExtend, segmentPageInfos []*segEntity.DocSegmentPageInfo) error {
	if len(segmentPageInfos) == 0 {
		logx.I(ctx, "createDocSegmentPageInfos|len(segmentPageInfos):%d|segmentPageInfos is empty",
			len(segmentPageInfos))
		return nil
	}
	for _, segmentPageInfo := range segmentPageInfos {
		seg, ok := segmentBizIDMap[segmentPageInfo.SegmentBizID]
		if !ok {
			logx.E(ctx, "createDocSegmentPageInfos|segBizID is not found|"+
				"segmentPageInfo:%+v", segmentPageInfo)
			return fmt.Errorf("segBizID is not found")
		}
		segmentPageInfo.SegmentID = seg.ID
	}
	if err := l.segDao.CreateDocSegmentPageInfos(ctx, segmentPageInfos, tx); err != nil {
		logx.E(ctx, "createDocSegmentPageInfos|CreateDocSegmentPageInfos|err:%+v", err)
		return err
	}
	logx.I(ctx, "createDocSegmentPageInfos|CreateDocSegmentPageInfos|success|len(segmentPageInfos):%d", len(segmentPageInfos))
	return nil
}

// GetSegmentPageInfosBySegIDs 通过SegIDs获取切片的页码信息
func (l *Logic) GetSegmentPageInfosBySegIDs(ctx context.Context, robotID uint64, segIDs []uint64) (
	map[uint64]*segEntity.DocSegmentPageInfo, error) {
	/*
			`
			SELECT
				%s
			FROM
			    t_doc_segment_page_info
			WHERE
			    robot_id = ? AND segment_id IN (%s)
		`
	*/

	segmentPageInfoMap := make(map[uint64]*segEntity.DocSegmentPageInfo)
	if len(segIDs) == 0 {
		logx.I(ctx, "GetSegmentPageInfosBySegIDs|len(segIDs):%d|ignore", len(segIDs))
		return segmentPageInfoMap, nil
	}

	docSegmentTableName := l.segDao.Query().TDocSegmentPageInfo.TableName()
	db, err := knowClient.GormClient(ctx, docSegmentTableName, robotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "GetSegmentByDocID get GormClient err:%v,appID:%v", err, robotID)
		return nil, err
	}

	deleteFlad := segEntity.SegmentIsNotDeleted

	filter := &segEntity.DocSegmentPageInfoFilter{
		SegmentIDs: segIDs,
		IsDeleted:  &deleteFlad,
	}

	selectColumns := segEntity.DocSegmentPageInfoTblCols

	list, err := l.segDao.GetDocSegmentPageInfoListWithTx(ctx, selectColumns, filter, db)
	if err != nil {
		logx.E(ctx, "GetSegmentPageInfosBySegIDs fail, filter: %+v err: %v", filter, err)
		return nil, err
	}

	for _, segPageInfo := range list {
		segmentPageInfoMap[segPageInfo.SegmentID] = segPageInfo
	}
	return segmentPageInfoMap, nil
}
