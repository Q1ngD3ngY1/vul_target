package release

import (
	"strconv"

	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/model"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
)

func ConvertReleaseDoToPO(do *model.TRelease) *releaseEntity.Release {
	successCount, err := strconv.Atoi(do.SuccessCount)
	if err != nil {
		successCount = 0
	}
	return &releaseEntity.Release{
		ID:             do.ID,
		BusinessID:     do.BusinessID,
		CorpID:         do.CorpID,
		RobotID:        do.RobotID,
		StaffID:        do.StaffID,
		Description:    do.Description,
		Status:         do.Status,
		Message:        do.Message,
		TotalCount:     uint64(do.TotalCount),
		SuccessCount:   uint64(successCount),
		PauseMsg:       do.PauseMsg,
		CallbackStatus: uint32(do.CallbackStatus),
		CreateTime:     do.CreateTime,
		UpdateTime:     do.UpdateTime,
	}
}

func ConvertReleaseDocDoToPo(releaseDocDo *model.TReleaseDoc) *releaseEntity.ReleaseDoc {
	if releaseDocDo == nil {
		return nil
	}

	doc := &releaseEntity.ReleaseDoc{
		ID:              uint64(releaseDocDo.ID),
		VersionID:       releaseDocDo.VersionID,
		DocID:           releaseDocDo.DocID,
		BusinessID:      releaseDocDo.BusinessID,
		RobotID:         releaseDocDo.RobotID,
		CorpID:          releaseDocDo.CorpID,
		StaffID:         releaseDocDo.StaffID,
		FileName:        releaseDocDo.FileName,
		FileType:        releaseDocDo.FileType,
		FileSize:        uint64(releaseDocDo.FileSize),
		Bucket:          releaseDocDo.Bucket,
		CosURL:          releaseDocDo.CosURL,
		CosHash:         releaseDocDo.CosHash,
		Message:         releaseDocDo.Message,
		Status:          releaseDocDo.Status,
		IsDeleted:       releaseDocDo.IsDeleted,
		IsRefer:         releaseDocDo.IsRefer,
		Source:          releaseDocDo.Source,
		WebURL:          releaseDocDo.WebURL,
		BatchID:         int(releaseDocDo.BatchID),
		AuditFlag:       releaseDocDo.AuditFlag,
		IsCreatingQA:    releaseDocDo.IsCreatingQa,
		IsCreatingIndex: releaseDocDo.IsCreatingIndex,
		Action:          releaseDocDo.Action,
		AttrRange:       releaseDocDo.AttrRange,
		CreateTime:      releaseDocDo.CreateTime,
		UpdateTime:      releaseDocDo.UpdateTime,
		ExpireTime:      releaseDocDo.ExpireTime,
	}
	return doc
}

func BatchConvertRelaseDocDoToPo(releaseDocDo []*model.TReleaseDoc) []*releaseEntity.ReleaseDoc {
	releaseDocPo := make([]*releaseEntity.ReleaseDoc, 0, len(releaseDocDo))
	for _, v := range releaseDocDo {
		releaseDocPo = append(releaseDocPo, ConvertReleaseDocDoToPo(v))
	}
	return releaseDocPo
}

func ConverReleaseDocPOToDO(releaseDocPo *releaseEntity.ReleaseDoc) *model.TReleaseDoc {
	if releaseDocPo == nil {
		return nil
	}

	releaseDocDo := &model.TReleaseDoc{
		ID:         int64(releaseDocPo.ID),
		VersionID:  releaseDocPo.VersionID,
		DocID:      releaseDocPo.DocID,
		BusinessID: releaseDocPo.BusinessID,
		RobotID:    releaseDocPo.RobotID,
		CorpID:     releaseDocPo.CorpID,
		StaffID:    releaseDocPo.StaffID,
		FileName:   releaseDocPo.FileName,
		FileType:   releaseDocPo.FileType,
		FileSize:   releaseDocPo.FileSize,
		Bucket:     releaseDocPo.Bucket,
		CosURL:     releaseDocPo.CosURL,
		CosHash:    releaseDocPo.CosHash,
		Message:    releaseDocPo.Message,
		Status:     releaseDocPo.Status,
		IsDeleted:  releaseDocPo.IsDeleted,
		// IsRefer:         releaseDocPo.IsRefer,
		Source:          releaseDocPo.Source,
		WebURL:          releaseDocPo.WebURL,
		BatchID:         int32(releaseDocPo.BatchID),
		AuditFlag:       releaseDocPo.AuditFlag,
		Action:          releaseDocPo.Action,
		AttrRange:       releaseDocPo.AttrRange,
		CreateTime:      releaseDocPo.CreateTime,
		UpdateTime:      releaseDocPo.UpdateTime,
		ExpireTime:      releaseDocPo.ExpireTime,
		IsCreatingQa:    releaseDocPo.IsCreatingQA,
		IsCreatingIndex: releaseDocPo.IsCreatingIndex,
		IsRefer:         releaseDocPo.IsRefer,
	}
	return releaseDocDo
}

func BatchConvertReleaseDocPOToDO(releaseDocPo []*releaseEntity.ReleaseDoc) []*model.TReleaseDoc {
	releaseDocDo := make([]*model.TReleaseDoc, 0, len(releaseDocPo))
	for _, v := range releaseDocPo {
		releaseDocDo = append(releaseDocDo, ConverReleaseDocPOToDO(v))
	}
	return releaseDocDo
}

func ConvertReleaseQADOToPO(qaDo *model.TReleaseQa) *releaseEntity.ReleaseQA {
	if qaDo == nil {
		return nil
	}
	return &releaseEntity.ReleaseQA{
		ID:             uint64(qaDo.ID),
		RobotID:        qaDo.RobotID,
		CorpID:         qaDo.CorpID,
		StaffID:        qaDo.StaffID,
		VersionID:      qaDo.VersionID,
		QAID:           qaDo.QaID,
		DocID:          qaDo.DocID,
		OriginDocID:    qaDo.OriginDocID,
		SegmentID:      qaDo.SegmentID,
		CategoryID:     uint64(qaDo.CategoryID),
		Source:         qaDo.Source,
		Question:       qaDo.Question,
		Answer:         qaDo.Answer,
		CustomParam:    qaDo.CustomParam,
		QuestionDesc:   qaDo.QuestionDesc,
		ReleaseStatus:  qaDo.ReleaseStatus,
		IsDeleted:      qaDo.IsDeleted,
		Message:        qaDo.Message,
		AcceptStatus:   qaDo.AcceptStatus,
		SimilarStatus:  qaDo.SimilarStatus,
		Action:         qaDo.Action,
		CreateTime:     qaDo.CreateTime,
		UpdateTime:     qaDo.UpdateTime,
		IsAllowRelease: qaDo.IsAllowRelease,
		AuditStatus:    qaDo.AuditStatus,
		AuditResult:    qaDo.AuditResult,
		AttrLabels:     qaDo.AttrLabels,
		ExpireTime:     qaDo.ExpireTime,
	}
}

func BatchConvertReleaseQaDoToPo(do []*model.TReleaseQa) []*releaseEntity.ReleaseQA {
	ret := make([]*releaseEntity.ReleaseQA, 0, len(do))
	for _, v := range do {
		ret = append(ret, ConvertReleaseQADOToPO(v))
	}
	return ret
}

func ConvertReleaseQaPOToDO(qa *releaseEntity.ReleaseQA) *model.TReleaseQa {
	if qa == nil {
		return nil
	}
	return &model.TReleaseQa{
		ID:             int64(qa.ID),
		RobotID:        qa.RobotID,
		CorpID:         qa.CorpID,
		StaffID:        qa.StaffID,
		VersionID:      qa.VersionID,
		QaID:           qa.QAID,
		DocID:          qa.DocID,
		OriginDocID:    qa.OriginDocID,
		SegmentID:      qa.SegmentID,
		CategoryID:     uint32(qa.CategoryID),
		Source:         qa.Source,
		Question:       qa.Question,
		Answer:         qa.Answer,
		CustomParam:    qa.CustomParam,
		QuestionDesc:   qa.QuestionDesc,
		ReleaseStatus:  qa.ReleaseStatus,
		IsDeleted:      qa.IsDeleted,
		Message:        qa.Message,
		AcceptStatus:   qa.AcceptStatus,
		SimilarStatus:  qa.SimilarStatus,
		Action:         qa.Action,
		CreateTime:     qa.CreateTime,
		UpdateTime:     qa.UpdateTime,
		IsAllowRelease: qa.IsAllowRelease,
		AuditStatus:    qa.AuditStatus,
		AuditResult:    qa.AuditResult,
		AttrLabels:     qa.AttrLabels,
		ExpireTime:     qa.ExpireTime,
	}
}

func BatchConvertReleaseQaPoToDO(po []*releaseEntity.ReleaseQA) []*model.TReleaseQa {
	ret := make([]*model.TReleaseQa, 0, len(po))
	for _, v := range po {
		ret = append(ret, ConvertReleaseQaPOToDO(v))
	}
	return ret
}

func ConvertReleaseRejectedQuestionDOToPO(do *model.TReleaseRejectedQuestion) *releaseEntity.ReleaseRejectedQuestion {
	if do == nil {
		return nil
	}
	return &releaseEntity.ReleaseRejectedQuestion{
		ID:                 do.ID,
		CorpID:             do.CorpID,
		RobotID:            do.RobotID,
		CreateStaffID:      do.CreateStaffID,
		VersionID:          do.VersionID,
		RejectedQuestionID: do.RejectedQuestionID,
		Question:           do.Question,
		ReleaseStatus:      do.ReleaseStatus,
		Message:            do.Message,
		IsDeleted:          uint32(do.IsDeleted),
		Action:             uint32(do.Action),
		IsAllowRelease:     uint32(do.IsAllowRelease),
		UpdateTime:         do.UpdateTime,
		CreateTime:         do.CreateTime,
	}
}

func BatchConvertReleaseRejectedQuestionDOToPO(dos []*model.TReleaseRejectedQuestion) []*releaseEntity.ReleaseRejectedQuestion {
	res := make([]*releaseEntity.ReleaseRejectedQuestion, 0, len(dos))
	for _, do := range dos {
		res = append(res, ConvertReleaseRejectedQuestionDOToPO(do))
	}
	return res
}

func ConvertReleaseRejectedQuestionPOToDO(po *releaseEntity.ReleaseRejectedQuestion) *model.TReleaseRejectedQuestion {
	if po == nil {
		return nil
	}
	return &model.TReleaseRejectedQuestion{
		ID:                 po.ID,
		CorpID:             po.CorpID,
		RobotID:            po.RobotID,
		CreateStaffID:      po.CreateStaffID,
		VersionID:          po.VersionID,
		RejectedQuestionID: po.RejectedQuestionID,
		Question:           po.Question,
		ReleaseStatus:      po.ReleaseStatus,
		Message:            po.Message,
		IsDeleted:          po.IsDeleted,
		Action:             po.Action,
		IsAllowRelease:     po.IsAllowRelease,
		UpdateTime:         po.UpdateTime,
		CreateTime:         po.CreateTime,
	}
}

func BatchConvertReleaseRejectedQuestionPOToDO(pos []*releaseEntity.ReleaseRejectedQuestion) []*model.TReleaseRejectedQuestion {
	res := make([]*model.TReleaseRejectedQuestion, 0, len(pos))
	for _, po := range pos {
		res = append(res, ConvertReleaseRejectedQuestionPOToDO(po))
	}
	return res
}

func ConvertReleaseSegmentDoToPO(releaseSegmentDo *model.TReleaseSegment) *releaseEntity.ReleaseSegment {
	if releaseSegmentDo == nil {
		return nil
	}
	return &releaseEntity.ReleaseSegment{
		ID:             uint64(releaseSegmentDo.ID),
		RobotID:        releaseSegmentDo.RobotID,
		CorpID:         releaseSegmentDo.CorpID,
		StaffID:        releaseSegmentDo.StaffID,
		DocID:          releaseSegmentDo.DocID,
		SegmentID:      releaseSegmentDo.SegmentID,
		VersionID:      releaseSegmentDo.VersionID,
		FileType:       releaseSegmentDo.FileType,
		Title:          releaseSegmentDo.Title,
		PageContent:    releaseSegmentDo.PageContent,
		SegmentType:    releaseSegmentDo.SegmentType,
		OrgData:        releaseSegmentDo.OrgData,
		SplitModel:     releaseSegmentDo.SplitModel,
		Status:         uint32(releaseSegmentDo.Status),
		ReleaseStatus:  releaseSegmentDo.ReleaseStatus,
		Message:        releaseSegmentDo.Message,
		IsDeleted:      releaseSegmentDo.IsDeleted,
		Action:         uint32(releaseSegmentDo.Action),
		BatchID:        releaseSegmentDo.BatchID,
		RichTextIndex:  releaseSegmentDo.RichTextIndex,
		StartIndex:     releaseSegmentDo.StartIndex,
		EndIndex:       releaseSegmentDo.EndIndex,
		BigStartIndex:  releaseSegmentDo.BigStartIndex,
		BigEndIndex:    releaseSegmentDo.BigEndIndex,
		BigDataID:      releaseSegmentDo.BigDataID,
		UpdateTime:     releaseSegmentDo.UpdateTime,
		CreateTime:     releaseSegmentDo.CreateTime,
		IsAllowRelease: uint32(releaseSegmentDo.IsAllowRelease),
		AttrLabels:     releaseSegmentDo.AttrLabels,
		ExpireTime:     releaseSegmentDo.ExpireTime,
	}
}

func BatchConvertReleaseSegmentDoToPO(releaseSegmentDo []*model.TReleaseSegment) []*releaseEntity.ReleaseSegment {
	releaseSegmentPO := make([]*releaseEntity.ReleaseSegment, 0, len(releaseSegmentDo))
	for _, v := range releaseSegmentDo {
		releaseSegmentPO = append(releaseSegmentPO, ConvertReleaseSegmentDoToPO(v))
	}
	return releaseSegmentPO
}

func ConvertReleaseSegmentPOToDO(releaseSegmentPO *releaseEntity.ReleaseSegment) *model.TReleaseSegment {
	if releaseSegmentPO == nil {
		return nil
	}
	return &model.TReleaseSegment{
		ID:             int64(releaseSegmentPO.ID),
		RobotID:        releaseSegmentPO.RobotID,
		CorpID:         releaseSegmentPO.CorpID,
		StaffID:        releaseSegmentPO.StaffID,
		DocID:          releaseSegmentPO.DocID,
		SegmentID:      releaseSegmentPO.SegmentID,
		VersionID:      releaseSegmentPO.VersionID,
		FileType:       releaseSegmentPO.FileType,
		Title:          releaseSegmentPO.Title,
		PageContent:    releaseSegmentPO.PageContent,
		SegmentType:    releaseSegmentPO.SegmentType,
		OrgData:        releaseSegmentPO.OrgData,
		SplitModel:     releaseSegmentPO.SplitModel,
		Status:         releaseSegmentPO.Status,
		ReleaseStatus:  releaseSegmentPO.ReleaseStatus,
		Message:        releaseSegmentPO.Message,
		IsDeleted:      uint32(releaseSegmentPO.IsDeleted),
		Action:         uint32(releaseSegmentPO.Action),
		BatchID:        int32(releaseSegmentPO.BatchID),
		RichTextIndex:  int32(releaseSegmentPO.RichTextIndex),
		StartIndex:     int32(releaseSegmentPO.StartIndex),
		EndIndex:       int32(releaseSegmentPO.EndIndex),
		BigStartIndex:  int32(releaseSegmentPO.BigStartIndex),
		BigEndIndex:    int32(releaseSegmentPO.BigEndIndex),
		BigDataID:      releaseSegmentPO.BigDataID,
		UpdateTime:     releaseSegmentPO.UpdateTime,
		CreateTime:     releaseSegmentPO.CreateTime,
		IsAllowRelease: uint32(releaseSegmentPO.IsAllowRelease),
		AttrLabels:     releaseSegmentPO.AttrLabels,
		ExpireTime:     releaseSegmentPO.ExpireTime,
	}
}

func BatchConvertReleaseSegmentPOToDO(releaseSegmentPO []*releaseEntity.ReleaseSegment) []*model.TReleaseSegment {
	releaseSegmentDo := make([]*model.TReleaseSegment, 0, len(releaseSegmentPO))
	for _, v := range releaseSegmentPO {
		releaseSegmentDo = append(releaseSegmentDo, ConvertReleaseSegmentPOToDO(v))
	}
	return releaseSegmentDo
}

// ConvertReleaseQASimilarQuestionDOToPO 单个 DO 转 PO
func ConvertReleaseQASimilarQuestionDOToPO(do *releaseEntity.ReleaseQaSimilarQuestion) *model.TReleaseQaSimilarQuestion {
	if do == nil {
		return nil
	}

	return &model.TReleaseQaSimilarQuestion{
		ID:             do.ID,
		CorpID:         uint64(do.CorpID),
		StaffID:        uint64(do.StaffID),
		RobotID:        uint64(do.RobotID),
		CreateUserID:   uint64(do.CreateUserID),
		VersionID:      uint64(do.VersionID),
		SimilarID:      uint64(do.SimilarID),
		RelatedQaID:    uint64(do.RelatedQaID),
		Source:         uint32(do.Source),
		Question:       do.Question,
		ReleaseStatus:  uint32(do.ReleaseStatus),
		Message:        do.Message,
		Action:         do.Action,
		AttrLabels:     do.AttrLabels,
		AuditStatus:    do.AuditStatus,
		AuditResult:    do.AuditResult,
		IsAllowRelease: do.IsAllowRelease,
		ExpireTime:     do.ExpireTime,
		IsDeleted:      do.IsDeleted,
		CreateTime:     do.CreateTime,
		UpdateTime:     do.UpdateTime,
	}
}

// BatchConvertReleaseQASimilarQuestionDOToPO 批量 DO 转 PO
func BatchConvertReleaseQASimilarQuestionDOToPO(dos []*releaseEntity.ReleaseQaSimilarQuestion) []*model.TReleaseQaSimilarQuestion {
	if len(dos) == 0 {
		return nil
	}

	pos := make([]*model.TReleaseQaSimilarQuestion, 0, len(dos))
	for _, do := range dos {
		pos = append(pos, ConvertReleaseQASimilarQuestionDOToPO(do))
	}
	return pos
}

// ConvertReleaseQASimilarQuestionPOToDO PO 转 DO (反向转换)
func ConvertReleaseQASimilarQuestionPOToDO(po *model.TReleaseQaSimilarQuestion) *releaseEntity.ReleaseQaSimilarQuestion {
	if po == nil {
		return nil
	}

	return &releaseEntity.ReleaseQaSimilarQuestion{
		ID:             po.ID,
		CorpID:         int64(po.CorpID),
		StaffID:        int64(po.StaffID),
		RobotID:        int64(po.RobotID),
		CreateUserID:   int64(po.CreateUserID),
		VersionID:      int64(po.VersionID),
		SimilarID:      int64(po.SimilarID),
		RelatedQaID:    int64(po.RelatedQaID),
		Source:         int(po.Source),
		Question:       po.Question,
		ReleaseStatus:  int(po.ReleaseStatus),
		Message:        po.Message,
		Action:         po.Action,
		AttrLabels:     po.AttrLabels,
		AuditStatus:    po.AuditStatus,
		AuditResult:    po.AuditResult,
		IsAllowRelease: po.IsAllowRelease,
		ExpireTime:     po.ExpireTime,
		IsDeleted:      po.IsDeleted,
		CreateTime:     po.CreateTime,
		UpdateTime:     po.UpdateTime,
	}
}

// BatchConvertReleaseQASimilarQuestionPOsToDOs 批量 PO 转 DO (反向转换)
func BatchConvertReleaseQASimilarQuestionPOToDO(pos []*model.TReleaseQaSimilarQuestion) []*releaseEntity.ReleaseQaSimilarQuestion {
	if len(pos) == 0 {
		return nil
	}

	dos := make([]*releaseEntity.ReleaseQaSimilarQuestion, 0, len(pos))
	for _, po := range pos {
		dos = append(dos, ConvertReleaseQASimilarQuestionPOToDO(po))
	}
	return dos
}

func ConvertReleaseAttributePO2DO(po *model.TReleaseAttribute) *releaseEntity.ReleaseAttribute {
	if po == nil {
		return nil
	}
	return &releaseEntity.ReleaseAttribute{
		ID:            uint64(po.ID),
		BusinessID:    po.BusinessID,
		RobotID:       po.RobotID,
		VersionID:     po.VersionID,
		AttrID:        po.AttrID,
		AttrKey:       po.AttrKey,
		Name:          po.Name,
		ReleaseStatus: po.ReleaseStatus,
		Message:       po.Message,
		Action:        po.Action,
		IsDeleted:     po.IsDeleted,
		CreateTime:    po.CreateTime,
		UpdateTime:    po.UpdateTime,
	}
}

func BatchConvertReleaseAttributePO2DO(pos []*model.TReleaseAttribute) []*releaseEntity.ReleaseAttribute {
	if len(pos) == 0 {
		return nil
	}
	dos := make([]*releaseEntity.ReleaseAttribute, 0, len(pos))
	for _, po := range pos {
		dos = append(dos, ConvertReleaseAttributePO2DO(po))
	}
	return dos
}

func ConvertReleaseAttributeDO2PO(do *releaseEntity.ReleaseAttribute) *model.TReleaseAttribute {
	if do == nil {
		return nil
	}
	return &model.TReleaseAttribute{
		ID:            int64(do.ID),
		BusinessID:    do.BusinessID,
		RobotID:       do.RobotID,
		VersionID:     do.VersionID,
		AttrID:        do.AttrID,
		AttrKey:       do.AttrKey,
		Name:          do.Name,
		ReleaseStatus: do.ReleaseStatus,
		Message:       do.Message,
		Action:        do.Action,
		IsDeleted:     do.IsDeleted,
		DeletedTime:   do.DeletedTime,
		CreateTime:    do.CreateTime,
		UpdateTime:    do.UpdateTime,
	}
}

func BatchConvertReleaseAttributeDO2PO(dos []*releaseEntity.ReleaseAttribute) []*model.TReleaseAttribute {
	if len(dos) == 0 {
		return nil
	}
	pos := make([]*model.TReleaseAttribute, 0, len(dos))
	for _, do := range dos {
		pos = append(pos, ConvertReleaseAttributeDO2PO(do))
	}
	return pos
}

func ConvertReleaseAttributeLabelPO2DO(po *model.TReleaseAttributeLabel) *releaseEntity.ReleaseAttributeLabel {
	if po == nil {
		return nil
	}
	return &releaseEntity.ReleaseAttributeLabel{
		ID:            uint64(po.ID),
		BusinessID:    po.BusinessID,
		RobotID:       po.RobotID,
		VersionID:     po.VersionID,
		AttrID:        po.AttrID,
		LabelID:       po.LabelID,
		Name:          po.Name,
		SimilarLabel:  po.SimilarLabel,
		ReleaseStatus: po.ReleaseStatus,
		Message:       po.Message,
		Action:        po.Action,
		IsDeleted:     po.IsDeleted,
		CreateTime:    po.CreateTime,
		UpdateTime:    po.UpdateTime,
	}
}

func BatchConvertReleaseAttributeLabelPO2DO(pos []*model.TReleaseAttributeLabel) []*releaseEntity.ReleaseAttributeLabel {
	if len(pos) == 0 {
		return nil
	}
	dos := make([]*releaseEntity.ReleaseAttributeLabel, 0, len(pos))
	for _, po := range pos {
		dos = append(dos, ConvertReleaseAttributeLabelPO2DO(po))
	}
	return dos
}

func ConvertReleaseAttributeLabelDO2PO(do *releaseEntity.ReleaseAttributeLabel) *model.TReleaseAttributeLabel {
	if do == nil {
		return nil
	}
	return &model.TReleaseAttributeLabel{
		ID:            int64(do.ID),
		BusinessID:    do.BusinessID,
		RobotID:       do.RobotID,
		VersionID:     do.VersionID,
		AttrID:        do.AttrID,
		LabelID:       do.LabelID,
		Name:          do.Name,
		SimilarLabel:  do.SimilarLabel,
		ReleaseStatus: do.ReleaseStatus,
		Message:       do.Message,
		Action:        do.Action,
		IsDeleted:     do.IsDeleted,
		CreateTime:    do.CreateTime,
		UpdateTime:    do.UpdateTime,
	}
}

func BatchConvertReleaseAttributeLabelDO2PO(dos []*releaseEntity.ReleaseAttributeLabel) []*model.TReleaseAttributeLabel {
	if len(dos) == 0 {
		return nil
	}
	pos := make([]*model.TReleaseAttributeLabel, 0, len(dos))
	for _, do := range dos {
		pos = append(pos, ConvertReleaseAttributeLabelDO2PO(do))
	}
	return pos
}
