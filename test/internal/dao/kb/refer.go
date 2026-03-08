package kb

import (
	"context"
	"fmt"
	"time"

	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/model"
	kbe "git.woa.com/adp/kb/kb-config/internal/entity/kb"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
)

// CreateRefer 创建refer
func (d *daoImpl) CreateRefer(ctx context.Context, refers []*kbe.Refer) error {
	if len(refers) == 0 {
		return nil
	}
	// 转换为PO模型列表
	poList := make([]*model.TRefer, 0, len(refers))
	now := time.Now()
	for _, refer := range refers {
		if refer != nil {
			poList = append(poList, &model.TRefer{
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
				RougeScore:  refer.RougeScore,
				CreateTime:  now,
				UpdateTime:  now,
			})
		}
	}
	// 使用Gen的批量插入功能
	err := d.mysql.TRefer.WithContext(ctx).CreateInBatches(poList, 100) // 每批100条
	if err != nil {
		logx.E(ctx, "创建refer失败: count=%d, err=%v", len(poList), err)
		return fmt.Errorf("create refer failed: %v", err)
	}
	logx.I(ctx, "创建refer成功: count=%d", len(poList))
	return nil
}

// GetRefersByBusinessIDs 通过business_id获取refer
func (d *daoImpl) GetRefersByBusinessIDs(ctx context.Context, robotID uint64,
	businessIDs []uint64) ([]*kbe.Refer, error) {
	if len(businessIDs) == 0 {
		return nil, nil
	}

	tbl := d.mysql.TRefer
	// 使用Gen的查询构建器
	refers, err := tbl.WithContext(ctx).
		Where(tbl.RobotID.Eq(robotID)).
		Where(tbl.BusinessID.In(businessIDs...)).
		Find()
	if err != nil {
		logx.E(ctx, "通过business_id获取refer失败: robotID=%d, businessIDs=%v, err=%v",
			robotID, businessIDs, err)
		return nil, fmt.Errorf("get refers by business ids failed: %v", err)
	}
	return refersPO2DO(refers), nil
}

// GetRefersByBusinessID 通过business_id获取refer
func (d *daoImpl) GetRefersByBusinessID(ctx context.Context, businessID uint64) (*kbe.Refer, error) {
	// 使用Gen的查询构建器
	tbl := d.mysql.TRefer
	refers, err := tbl.WithContext(ctx).
		Where(tbl.BusinessID.Eq(businessID)).
		Find()
	if err != nil {
		logx.E(ctx, "通过business_id获取refer失败: businessID=%d, err=%v", businessID, err)
		return nil, fmt.Errorf("get refer by business id failed: %v", err)
	}
	if len(refers) == 0 {
		logx.E(ctx, "通过business_id获取refer失败: 记录不存在 businessID=%d", businessID)
		return nil, errs.ErrGetReferFail
	}
	// 取第一条记录并转换为entity模型
	rsp := refersPO2DO(refers)
	return rsp[0], nil
}

func refersPO2DO(pos []*model.TRefer) []*kbe.Refer {
	return slicex.Map(pos, func(po *model.TRefer) *kbe.Refer {
		return referPO2DO(po)
	})
}

func referPO2DO(refer *model.TRefer) *kbe.Refer {
	if refer == nil {
		return nil
	}
	return &kbe.Refer{
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
		RougeScore:  refer.RougeScore,
		CreateTime:  refer.CreateTime,
		UpdateTime:  refer.UpdateTime,
	}
}
