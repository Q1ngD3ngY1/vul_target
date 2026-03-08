package qa

import (
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/model"
	entity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
)

func ConvertDocQAPOToDO(do *model.TDocQa) *entity.DocQA {
	if do == nil {
		return nil
	}
	// QaAuditFail, PicAuditFail,VideoAuditFail 这三个字段在DB中没有
	return &entity.DocQA{
		ID:            uint64(do.ID),
		BusinessID:    do.BusinessID,
		RobotID:       do.RobotID,
		CorpID:        do.CorpID,
		StaffID:       do.StaffID,
		DocID:         do.DocID,
		OriginDocID:   do.OriginDocID,
		SegmentID:     do.SegmentID,
		CategoryID:    uint64(do.CategoryID),
		Source:        do.Source,
		Question:      do.Question,
		Answer:        do.Answer,
		CustomParam:   do.CustomParam,
		QuestionDesc:  do.QuestionDesc,
		ReleaseStatus: do.ReleaseStatus,
		IsAuditFree:   do.IsAuditFree,
		IsDeleted:     do.IsDeleted,
		Message:       do.Message,
		AcceptStatus:  do.AcceptStatus,
		SimilarStatus: do.SimilarStatus,
		NextAction:    do.NextAction,
		CharSize:      uint64(do.CharSize),
		AttrRange:     do.AttrRange,
		CreateTime:    do.CreateTime,
		UpdateTime:    do.UpdateTime,
		ExpireStart:   do.ExpireStart,
		ExpireEnd:     do.ExpireEnd,
		AttributeFlag: uint64(do.AttributeFlag),
		EnableScope:   do.EnableScope,
		QaSize:        do.QaSize,
	}
}

func BatchConvertDocQAPOToDO(dos []*model.TDocQa) []*entity.DocQA {
	if len(dos) == 0 {
		return nil
	}
	docs := make([]*entity.DocQA, 0)
	for _, do := range dos {
		docs = append(docs, ConvertDocQAPOToDO(do))
	}
	return docs
}

func ConvertDocQADOToPO(po *entity.DocQA) *model.TDocQa {
	if po == nil {
		return nil
	}
	do := &model.TDocQa{
		ID:            po.ID,
		BusinessID:    po.BusinessID,
		RobotID:       po.RobotID,
		CorpID:        po.CorpID,
		StaffID:       po.StaffID,
		DocID:         po.DocID,
		OriginDocID:   po.OriginDocID,
		SegmentID:     po.SegmentID,
		CategoryID:    uint32(po.CategoryID),
		Source:        po.Source,
		Question:      po.Question,
		Answer:        po.Answer,
		CustomParam:   po.CustomParam,
		QuestionDesc:  po.QuestionDesc,
		ReleaseStatus: po.ReleaseStatus,
		IsDeleted:     po.IsDeleted,
		Message:       po.Message,
		AcceptStatus:  po.AcceptStatus,
		SimilarStatus: po.SimilarStatus,
		NextAction:    po.NextAction,
		CharSize:      int64(po.CharSize),
		AttrRange:     po.AttrRange,
		CreateTime:    po.CreateTime,
		UpdateTime:    po.UpdateTime,
		ExpireStart:   po.ExpireStart,
		ExpireEnd:     po.ExpireEnd,
		AttributeFlag: int64(po.AttributeFlag),
		IsAuditFree:   po.IsAuditFree,
		EnableScope:   po.EnableScope,
		QaSize:        po.QaSize,
	}
	return do
}

func BatchConvertDocQADOToPO(dos []*entity.DocQA) []*model.TDocQa {
	if len(dos) == 0 {
		return nil
	}
	docs := make([]*model.TDocQa, 0)
	for _, do := range dos {
		docs = append(docs, ConvertDocQADOToPO(do))
	}
	return docs
}

func ConvertTDocQATaskPO2DO(p *model.TDocQaTask) *entity.DocQATask {
	if p == nil {
		return nil
	}
	return &entity.DocQATask{
		ID:                p.ID,
		CorpID:            p.CorpID,
		RobotID:           p.RobotID,
		BusinessID:        p.BusinessID,
		DocID:             p.DocID,
		TaskID:            p.TaskID,
		SourceID:          p.SourceID,
		DocName:           p.DocName,
		DocType:           p.DocType,
		QACount:           p.QaCount,
		SegmentCountDone:  p.SegmentCountDone,
		SegmentCount:      p.SegmentCount,
		StopNextSegmentID: p.StopNextSegmentID,
		InputToken:        p.InputToken,
		OutputToken:       p.OutputToken,
		Status:            int(p.Status),
		IsDeleted:         p.IsDeleted,
		Message:           p.Message,
		CreateTime:        p.CreateTime,
		UpdateTime:        p.UpdateTime,
	}
}

func BatchConvertTDocQATaskPO2DO(pList []*model.TDocQaTask) []*entity.DocQATask {
	if len(pList) == 0 {
		return nil
	}
	result := make([]*entity.DocQATask, 0, len(pList))
	for _, p := range pList {
		result = append(result, ConvertTDocQATaskPO2DO(p))
	}
	return result
}

func ConvertTDocQATaskDO2PO(d *entity.DocQATask) *model.TDocQaTask {
	if d == nil {
		return nil
	}
	return &model.TDocQaTask{
		ID:                d.ID,
		CorpID:            d.CorpID,
		RobotID:           d.RobotID,
		BusinessID:        d.BusinessID,
		DocID:             d.DocID,
		TaskID:            d.TaskID,
		SourceID:          d.SourceID,
		DocName:           d.DocName,
		DocType:           d.DocType,
		QaCount:           d.QACount,
		SegmentCountDone:  d.SegmentCountDone,
		SegmentCount:      d.SegmentCount,
		StopNextSegmentID: d.StopNextSegmentID,
		InputToken:        d.InputToken,
		OutputToken:       d.OutputToken,
		Status:            int32(d.Status),
		IsDeleted:         d.IsDeleted,
		Message:           d.Message,
		CreateTime:        d.CreateTime,
		UpdateTime:        d.UpdateTime,
	}
}

func BatchConvertTDocQATaskDO2PO(dList []*entity.DocQATask) []*model.TDocQaTask {
	if len(dList) == 0 {
		return nil
	}
	result := make([]*model.TDocQaTask, 0, len(dList))
	for _, d := range dList {
		result = append(result, ConvertTDocQATaskDO2PO(d))
	}
	return result
}

func ConvertRejectedQuestionsPO2DO(v *model.TRejectedQuestion) *entity.RejectedQuestion {
	return &entity.RejectedQuestion{
		ID:               v.ID,
		BusinessID:       v.BusinessID,
		CorpID:           v.CorpID,
		RobotID:          v.RobotID,
		CreateStaffID:    v.CreateStaffID,
		BusinessSourceID: v.BusinessSourceID,
		BusinessSource:   v.BusinessSource,
		Question:         v.Question,
		ReleaseStatus:    v.ReleaseStatus,
		IsDeleted:        v.IsDeleted,
		Action:           v.Action,
		UpdateTime:       v.UpdateTime,
		CreateTime:       v.CreateTime,
	}

}

func BatchConvertRejectedQuestionsPO2DO(list []*model.TRejectedQuestion) []*entity.RejectedQuestion {
	ret := make([]*entity.RejectedQuestion, 0, len(list))
	for _, v := range list {
		ret = append(ret, ConvertRejectedQuestionsPO2DO(v))
	}
	return ret
}

func ConvertRejectQuestionDO2PO(v *entity.RejectedQuestion) *model.TRejectedQuestion {
	return &model.TRejectedQuestion{
		ID:               v.ID,
		BusinessID:       v.BusinessID,
		CorpID:           v.CorpID,
		RobotID:          v.RobotID,
		CreateStaffID:    v.CreateStaffID,
		BusinessSourceID: v.BusinessSourceID,
		BusinessSource:   uint32(v.BusinessSource),
		Question:         v.Question,
		ReleaseStatus:    v.ReleaseStatus,
		IsDeleted:        v.IsDeleted,
		Action:           uint32(v.Action),
		UpdateTime:       v.UpdateTime,
		CreateTime:       v.CreateTime,
	}
}

func BatchConvertRejectQuestionDO2PO(list []*entity.RejectedQuestion) []*model.TRejectedQuestion {
	ret := make([]*model.TRejectedQuestion, 0, len(list))
	for _, v := range list {
		ret = append(ret, ConvertRejectQuestionDO2PO(v))
	}
	return ret
}

func ConvertSimilarQuestionsPO2DO(q *model.TQaSimilarQuestion) *entity.SimilarQuestion {
	if q == nil {
		return nil
	}
	d := &entity.SimilarQuestion{
		ID:            q.ID,
		SimilarID:     q.SimilarID,
		RelatedQAID:   q.RelatedQaID,
		CorpID:        q.CorpID,
		RobotID:       q.RobotID,
		StaffID:       q.StaffID,
		CreateUserID:  q.CreateUserID,
		Source:        q.Source,
		Question:      q.Question,
		Message:       q.Message,
		CreateTime:    q.CreateTime,
		UpdateTime:    q.UpdateTime,
		IsDeleted:     q.IsDeleted,
		ReleaseStatus: q.ReleaseStatus,
		NextAction:    q.NextAction,
		CharSize:      uint64(q.CharSize),
		IsAuditFree:   q.IsAuditFree == 1,
	}
	return d
}

func BatchConvertSimilarQuestionsPO2DO(qs []*model.TQaSimilarQuestion) []*entity.SimilarQuestion {
	ds := make([]*entity.SimilarQuestion, len(qs))
	for i, q := range qs {
		ds[i] = ConvertSimilarQuestionsPO2DO(q)
	}
	return ds
}

func ConvertSimilarQuestionsDO2PO(po *entity.SimilarQuestion) *model.TQaSimilarQuestion {
	if po == nil {
		return nil
	}
	q := &model.TQaSimilarQuestion{
		ID:            po.ID,
		SimilarID:     po.SimilarID,
		RelatedQaID:   po.RelatedQAID,
		CorpID:        po.CorpID,
		RobotID:       po.RobotID,
		StaffID:       po.StaffID,
		CreateUserID:  po.CreateUserID,
		Source:        po.Source,
		Question:      po.Question,
		Message:       po.Message,
		CreateTime:    po.CreateTime,
		UpdateTime:    po.UpdateTime,
		IsDeleted:     po.IsDeleted,
		ReleaseStatus: po.ReleaseStatus,
		NextAction:    po.NextAction,
		CharSize:      int64(po.CharSize),
		QaSize:        po.QaSize,
	}
	if po.IsAuditFree {
		q.IsAuditFree = 1
	} else {
		q.IsAuditFree = 0
	}
	return q
}

func BatchConvertSimilarQuestionsDO2PO(pos []*entity.SimilarQuestion) []*model.TQaSimilarQuestion {
	qs := make([]*model.TQaSimilarQuestion, len(pos))
	for i, po := range pos {
		qs[i] = ConvertSimilarQuestionsDO2PO(po)
	}
	return qs
}

func ConvertDocQaSimilarPO2DO(p *model.TDocQaSimilar) *entity.DocQASimilar {
	if p == nil {
		return nil
	}
	return &entity.DocQASimilar{
		ID:         p.ID,
		BusinessID: p.BusinessID,
		SimilarID:  p.SimilarID,
		CorpID:     p.CorpID,
		RobotID:    p.RobotID,
		StaffID:    p.StaffID,
		DocID:      p.DocID,
		QaID:       p.QaID,
		IsValid:    p.IsValid,
		Status:     uint64(p.Status),
		CreateTime: p.CreateTime,
		UpdateTime: p.UpdateTime,
	}
}

func BatchConvertDocQaSimilarPO2DO(ps []*model.TDocQaSimilar) []*entity.DocQASimilar {
	if len(ps) == 0 {
		return nil
	}
	res := make([]*entity.DocQASimilar, 0, len(ps))
	for _, p := range ps {
		res = append(res, ConvertDocQaSimilarPO2DO(p))
	}
	return res
}

func ConvertDocQaSimilarDO2PO(d *entity.DocQASimilar) *model.TDocQaSimilar {
	if d == nil {
		return nil
	}
	return &model.TDocQaSimilar{
		ID:         d.ID,
		BusinessID: d.BusinessID,
		SimilarID:  d.SimilarID,
		CorpID:     d.CorpID,
		RobotID:    d.RobotID,
		StaffID:    d.StaffID,
		DocID:      d.DocID,
		QaID:       d.QaID,
		IsValid:    d.IsValid,
		Status:     uint32(d.Status),
		CreateTime: d.CreateTime,
		UpdateTime: d.UpdateTime,
	}
}

func BatchConvertDocQaSimilarDO2PO(ds []*entity.DocQASimilar) []*model.TDocQaSimilar {
	if len(ds) == 0 {
		return nil
	}
	res := make([]*model.TDocQaSimilar, 0, len(ds))
	for _, d := range ds {
		res = append(res, ConvertDocQaSimilarDO2PO(d))
	}
	return res
}
