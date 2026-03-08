package document

import (
	"context"

	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/model"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/mysqlquery"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	"gorm.io/gen/field"
)

// CreateRefer 创建refer
func (d *daoImpl) CreateRefer(ctx context.Context, refers []entity.Refer) error {
	if len(refers) == 0 {
		return nil
	}
	/*
		`
			INSERT INTO
				t_refer (%s)
			VALUES
			    (null,:business_id,:robot_id,:msg_id,:raw_question,:doc_id,:doc_type,:relate_id,:confidence,:question,
			:answer,:org_data,:page_infos,:sheet_infos,:mark,:session_type,:update_time,:create_time,:rouge_score)
		`
	*/

	tbl := d.mysql.TRefer

	tRefs := []*model.TRefer{}
	for _, refer := range refers {
		tRefs = append(tRefs, ConvertReferDO2PO(&refer))
	}

	if err := tbl.WithContext(ctx).Create(tRefs...); err != nil {
		logx.E(ctx, "Create refer error. err:%+v", err)
		return err
	}
	return nil
}

func (d *daoImpl) generateReferConditionFromFilter(filters *entity.ReferFilter, query mysqlquery.ITReferDo) mysqlquery.ITReferDo {
	tbl := d.mysql.TRefer
	if filters.BusinessIDs != nil {
		query = query.Where(tbl.BusinessID.In(filters.BusinessIDs...))
	}

	if filters.BusinessID != 0 {
		query = query.Where(tbl.BusinessID.Eq(filters.BusinessID))
	}

	if filters.RobotID != 0 {
		query = query.Where(tbl.RobotID.Eq(filters.RobotID))
	}

	if filters.ID != 0 {
		query = query.Where(tbl.ID.Eq(filters.ID))
	}

	return query

}

func (d *daoImpl) GetReferListByFilter(ctx context.Context, selectColumns []string, filters *entity.ReferFilter) ([]*entity.Refer, error) {
	tbl := d.mysql.TRefer
	query := tbl.WithContext(ctx)
	queryColumns := []field.Expr{}
	for _, column := range selectColumns {
		if col, ok := tbl.GetFieldByName(column); ok {
			queryColumns = append(queryColumns, col)
		}

	}
	if len(queryColumns) > 0 {
		query = query.Select(queryColumns...)
	} else {
		query = query.Select(tbl.ALL)
	}

	query = d.generateReferConditionFromFilter(filters, query)

	if refers, err := query.Find(); err != nil {
		return nil, err
	} else {
		return BatchConvertReferPO2DO(refers), nil
	}

}

// // MarkRefer .
// func (d *dao) MarkRefer(ctx context.Context, robotID, businessID uint64, mark uint32) error {
// 	querySQL := markRefer
// `
// 		UPDATE
// 			t_refer
// 		SET
// 		    mark = ?,
// 		    update_time = ?
// 		WHERE
// 		    business_id = ? AND mark = ? AND robot_id = ?
// 	`
// 	now := time.Now()
// 	args := make([]any, 0, 5)
// 	args = append(args, mark, now, businessID, entity.MarkInit, robotID)
// 	if _, err := d.db.Exec(ctx, querySQL, args...); err != nil {
// 		logx.E(ctx, "MarkRefer失败 sql:%s args:%+v err:%+v", querySQL, args, err)
// 		return err
// 	}
// 	return nil
// }

func ConvertReferDO2PO(refer *entity.Refer) *model.TRefer {
	return &model.TRefer{
		ID:          refer.ID,
		BusinessID:  refer.BusinessID,
		RobotID:     refer.RobotID,
		MsgID:       refer.MsgID,
		RawQuestion: refer.RawQuestion,
		DocID:       refer.DocID,
		DocType:     refer.DocType,
		RelateID:    refer.RelateID,
		Confidence:  refer.Confidence,
		Question:    refer.Question,
		Answer:      refer.Answer,
		OrgData:     refer.OrgData,
		PageInfos:   refer.PageInfos,
		SheetInfos:  refer.SheetInfos,
		Mark:        refer.Mark,
		SessionType: refer.SessionType,
		UpdateTime:  refer.UpdateTime,
		CreateTime:  refer.CreateTime,
		RougeScore:  refer.RougeScore,
	}
}

func BatchConvertReferDO2PO(refers []*entity.Refer) []*model.TRefer {
	if len(refers) == 0 {
		return nil
	}
	ret := make([]*model.TRefer, 0, len(refers))
	for _, refer := range refers {
		ret = append(ret, ConvertReferDO2PO(refer))
	}
	return ret

}

func ConvertReferPO2DO(refer *model.TRefer) *entity.Refer {
	return &entity.Refer{
		ID:          refer.ID,
		BusinessID:  refer.BusinessID,
		RobotID:     refer.RobotID,
		MsgID:       refer.MsgID,
		RawQuestion: refer.RawQuestion,
		DocID:       refer.DocID,
		DocType:     refer.DocType,
		RelateID:    refer.RelateID,
		Confidence:  refer.Confidence,
		Question:    refer.Question,
		Answer:      refer.Answer,
		OrgData:     refer.OrgData,
		PageInfos:   refer.PageInfos,
		SheetInfos:  refer.SheetInfos,
		Mark:        refer.Mark,
		SessionType: refer.SessionType,
		UpdateTime:  refer.UpdateTime,
		CreateTime:  refer.CreateTime,
		RougeScore:  refer.RougeScore,
	}
}

func BatchConvertReferPO2DO(refers []*model.TRefer) []*entity.Refer {
	if len(refers) == 0 {
		return nil
	}
	ret := make([]*entity.Refer, 0, len(refers))
	for _, refer := range refers {
		ret = append(ret, ConvertReferPO2DO(refer))
	}
	return ret
}
