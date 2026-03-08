package document

import (
	"context"

	"git.woa.com/adp/common/x/logx"
)

const (

	// 先简单实现，后续再优化
	getOrgDataByDocID = `
		SELECT 
		    org_data 
		FROM 
		    t_realtime_doc_segment 
		WHERE 
		    doc_id = ? AND is_deleted = 1 
		LIMIT 
		     ?,?
	`

	getCountByDocID = `
		SELECT 
		    count(*) 
		FROM 
		    t_realtime_doc_segment 
		WHERE 
		    doc_id = ? AND is_deleted = 1 
	`
)

// GetCountByDocID 根据DocID 获取 t_realtime_doc_segment 中的文档片段数量
func (d *daoImpl) GetOrgDataCountByDocID(ctx context.Context, docID uint64) (count int64, err error) {
	/*
		`
			SELECT
			    count(*)
			FROM
			    t_realtime_doc_segment
			WHERE
			    doc_id = ? AND is_deleted = 1
		`
	*/
	tbl := d.Query().TRealtimeDocSegment
	count, err = tbl.WithContext(ctx).Where(tbl.DocID.Eq(docID), tbl.IsDeleted.Eq(1)).Count()
	if err != nil {
		logx.E(ctx, "GetCountByDocID sql:%s err:%+v", getCountByDocID, err)
		return count, err
	}
	logx.I(ctx, "GetCountByDocID sql:%s res:%s", getCountByDocID, count)
	return count, nil
}

// GetOrgDataListByDocID 根据DocID 获取 t_realtime_doc_segment 中的文档片段
func (d *daoImpl) GetOrgDataListByDocID(ctx context.Context,
	docID, offset, limit uint64) ([]string, error) {
	/*
			`
			SELECT
			    org_data
			FROM
			    t_realtime_doc_segment
			WHERE
			    doc_id = ? AND is_deleted = 1
			LIMIT
			     ?,?
		`
	*/
	tbl := d.Query().TRealtimeDocSegment
	segs, err := tbl.WithContext(ctx).Where(tbl.DocID.Eq(docID), tbl.IsDeleted.Eq(1)).
		Limit(int(limit)).Offset(int(offset)).
		Select(tbl.OrgData).Find()

	if err != nil {
		logx.E(ctx, "GetOrgDataListByDocID sql:%s err:%+v", getOrgDataByDocID, err)
		return nil, err
	}
	res := make([]string, 0, len(segs))
	for _, seg := range segs {
		res = append(res, seg.OrgData)
	}
	return res, nil
}
