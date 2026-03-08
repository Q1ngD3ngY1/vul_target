package segment

import (
	"strconv"

	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/model"
	entity "git.woa.com/adp/kb/kb-config/internal/entity/segment"
	segEntity "git.woa.com/adp/kb/kb-config/internal/entity/segment"
)

func ConvertDocSegementPO2DO(po *model.TDocSegment) *segEntity.DocSegment {
	if po == nil {
		return nil
	}
	/*
		BigString,Images,OrgPageNumbers,BigPageNumbers,SheetData
		这几个字段不存入数据库，需要单独处理
	*/
	return &segEntity.DocSegment{
		ID:              uint64(po.ID),
		BusinessID:      po.BusinessID,
		RobotID:         po.RobotID,
		CorpID:          po.CorpID,
		StaffID:         po.StaffID,
		DocID:           po.DocID,
		FileType:        po.FileType,
		SegmentType:     po.SegmentType,
		Title:           po.Title,
		PageContent:     po.PageContent,
		OrgData:         po.OrgData,
		OrgDataBizID:    uint64(po.OrgDataBizID),
		Outputs:         po.Outputs,
		CostTime:        po.CostTime,
		SplitModel:      po.SplitModel,
		Status:          uint32(po.Status),
		ReleaseStatus:   uint32(po.ReleaseStatus),
		Message:         po.Message,
		IsDeleted:       uint32(po.IsDeleted),
		Type:            int(po.Type),
		NextAction:      uint32(po.NextAction),
		BatchID:         int(po.BatchID),
		RichTextIndex:   int(po.RichTextIndex),
		StartChunkIndex: int(po.StartChunkIndex),
		EndChunkIndex:   int(po.EndChunkIndex),
		LinkerKeep:      po.LinkerKeep != 0,
		UpdateTime:      po.UpdateTime,
		CreateTime:      po.CreateTime,
		BigDataID:       po.BigDataID,
		BigStart:        po.BigStartIndex,
		BigEnd:          po.BigEndIndex,
	}

}

func BatchConvertDocSegementPO2DO(pos []*model.TDocSegment) []*segEntity.DocSegment {
	if len(pos) == 0 {
		return nil
	}
	segments := make([]*segEntity.DocSegment, 0, len(pos))
	for _, po := range pos {
		segments = append(segments, ConvertDocSegementPO2DO(po))
	}
	return segments
}

func ConvertDocSegementDO2PO(po *segEntity.DocSegment) *model.TDocSegment {
	if po == nil {
		return nil
	}
	do := &model.TDocSegment{
		ID:              int64(po.ID),
		BusinessID:      po.BusinessID,
		RobotID:         po.RobotID,
		CorpID:          po.CorpID,
		StaffID:         po.StaffID,
		DocID:           po.DocID,
		FileType:        po.FileType,
		SegmentType:     po.SegmentType,
		Title:           po.Title,
		PageContent:     po.PageContent,
		OrgData:         po.OrgData,
		OrgDataBizID:    po.OrgDataBizID,
		Outputs:         po.Outputs,
		CostTime:        po.CostTime,
		SplitModel:      po.SplitModel,
		Status:          po.Status,
		ReleaseStatus:   int32(po.ReleaseStatus),
		Message:         po.Message,
		IsDeleted:       po.IsDeleted,
		Type:            int32(po.Type),
		NextAction:      po.NextAction,
		BatchID:         int32(po.BatchID),
		RichTextIndex:   int32(po.RichTextIndex),
		StartChunkIndex: int32(po.StartChunkIndex),
		EndChunkIndex:   int32(po.EndChunkIndex),
		UpdateTime:      po.UpdateTime,
		CreateTime:      po.CreateTime,
		BigDataID:       po.BigDataID,
		BigStartIndex:   po.BigStart,
		BigEndIndex:     po.BigEnd,
	}
	if po.LinkerKeep {
		do.LinkerKeep = 1
	} else {
		do.LinkerKeep = 0
	}
	return do
}

func BatchConvertDocSegementDO2PO(pos []*segEntity.DocSegment) []*model.TDocSegment {
	if len(pos) == 0 {
		return nil
	}
	dos := make([]*model.TDocSegment, 0, len(pos))
	for _, po := range pos {
		dos = append(dos, ConvertDocSegementDO2PO(po))
	}
	return dos
}

func ConvertDocSegmentPageInfoPO2DO(p *model.TDocSegmentPageInfo) *entity.DocSegmentPageInfo {
	if p == nil {
		return nil
	}

	return &entity.DocSegmentPageInfo{
		ID:             p.ID,
		PageInfoID:     p.PageInfoID,
		CorpID:         p.CorpID,
		RobotID:        p.RobotID,
		DocID:          uint64(p.DocID),
		SegmentID:      p.SegmentID,
		StaffID:        p.StaffID,
		OrgPageNumbers: p.OrgPageNumbers,
		BigPageNumbers: p.BigPageNumbers,
		SheetData:      p.SheetData,
		IsDeleted:      p.IsDeleted,
		CreateTime:     p.CreateTime,
		UpdateTime:     p.UpdateTime,
	}
}

func BatchConvertDocSegmentPageInfoPO2DO(pList []*model.TDocSegmentPageInfo) []*entity.DocSegmentPageInfo {
	if pList == nil {
		return nil
	}

	dList := make([]*entity.DocSegmentPageInfo, 0, len(pList))
	for _, p := range pList {
		dList = append(dList, ConvertDocSegmentPageInfoPO2DO(p))
	}
	return dList
}

func ConvertDocSegmentPageInfoDO2PO(d *entity.DocSegmentPageInfo) *model.TDocSegmentPageInfo {
	if d == nil {
		return nil
	}

	return &model.TDocSegmentPageInfo{
		ID:             d.ID,
		PageInfoID:     d.PageInfoID,
		CorpID:         d.CorpID,
		RobotID:        d.RobotID,
		DocID:          int64(d.DocID),
		SegmentID:      d.SegmentID,
		StaffID:        d.StaffID,
		OrgPageNumbers: d.OrgPageNumbers,
		BigPageNumbers: d.BigPageNumbers,
		SheetData:      d.SheetData,
		IsDeleted:      d.IsDeleted,
		CreateTime:     d.CreateTime,
		UpdateTime:     d.UpdateTime,
	}
}

func BatchConvertDocSegmentPageInfoDO2PO(dList []*entity.DocSegmentPageInfo) []*model.TDocSegmentPageInfo {
	if dList == nil {
		return nil
	}

	pList := make([]*model.TDocSegmentPageInfo, 0, len(dList))
	for _, d := range dList {
		pList = append(pList, ConvertDocSegmentPageInfoDO2PO(d))
	}
	return pList
}

func ConvertDocSegmentOrgDataDO2PO(d *segEntity.DocSegmentOrgData) *model.TDocSegmentOrgDatum {
	if d == nil {
		return nil
	}

	return &model.TDocSegmentOrgDatum{
		BusinessID:         d.BusinessID,
		DocBizID:           d.DocBizID,
		AppBizID:           d.AppBizID,
		CorpBizID:          d.CorpBizID,
		StaffBizID:         d.StaffBizID,
		OrgData:            d.OrgData,
		AddMethod:          d.AddMethod,
		OrgPageNumbers:     d.OrgPageNumbers,
		SegmentType:        d.SegmentType,
		SheetData:          d.SheetData,
		IsTemporaryDeleted: d.IsTemporaryDeleted,
		IsDeleted:          d.IsDeleted,
		IsDisabled:         d.IsDisabled,
		CreateTime:         d.CreateTime,
		UpdateTime:         d.UpdateTime,
		SheetName:          d.SheetName,
	}
}

func ConvertDocSegmentOrgDataPO2DO(t *model.TDocSegmentOrgDatum) *segEntity.DocSegmentOrgData {
	if t == nil {
		return nil
	}
	return &segEntity.DocSegmentOrgData{
		BusinessID:         t.BusinessID,
		AppBizID:           t.AppBizID,
		DocBizID:           t.DocBizID,
		CorpBizID:          t.CorpBizID,
		StaffBizID:         t.StaffBizID,
		OrgData:            t.OrgData,
		AddMethod:          t.AddMethod,
		OrgPageNumbers:     t.OrgPageNumbers,
		SegmentType:        t.SegmentType,
		SheetData:          t.SheetData,
		IsTemporaryDeleted: t.IsTemporaryDeleted,
		IsDeleted:          t.IsDeleted,
		IsDisabled:         t.IsDisabled,
		CreateTime:         t.CreateTime,
		UpdateTime:         t.UpdateTime,
		SheetName:          t.SheetName,
	}
}

func BatchConvertDocSegmentOrgDataDO2PO(src []*segEntity.DocSegmentOrgData) []*model.TDocSegmentOrgDatum {
	if len(src) == 0 {
		return nil
	}
	dst := make([]*model.TDocSegmentOrgDatum, len(src))
	for i := range src {
		dst[i] = ConvertDocSegmentOrgDataDO2PO(src[i])
	}
	return dst
}

func BatchConvertDocSegmentOrgDataPO2DO(src []*model.TDocSegmentOrgDatum) []*segEntity.DocSegmentOrgData {
	if len(src) == 0 {
		return nil
	}
	dst := make([]*segEntity.DocSegmentOrgData, len(src))
	for i := range src {
		dst[i] = ConvertDocSegmentOrgDataPO2DO(src[i])
	}
	return dst
}

// BatchConvertDocSegmentOrgDataTemporaryPo2DO []DocSegmentOrgDataTemporary -> []TDocSegmentOrgDataTemporary
func BatchConvertDocSegmentOrgDataTemporaryDO2PO(src []*segEntity.DocSegmentOrgDataTemporary) []*model.TDocSegmentOrgDataTemporary {
	if len(src) == 0 {
		return nil
	}
	dst := make([]*model.TDocSegmentOrgDataTemporary, len(src))
	for i := range src {
		dst[i] = ConvertDocSegmentOrgDataTemporaryDO2PO(src[i])
	}
	return dst
}

// TDocSegmentOrgDataTemporarySliceToDocSegmentOrgDataTemporarySlice []TDocSegmentOrgDataTemporary -> []DocSegmentOrgDataTemporary
func BatchConvertDocSegmentOrgDataTemporaryPO2DO(src []*model.TDocSegmentOrgDataTemporary) []*segEntity.DocSegmentOrgDataTemporary {
	if len(src) == 0 {
		return nil
	}
	dst := make([]*segEntity.DocSegmentOrgDataTemporary, len(src))
	for i := range src {
		dst[i] = ConvertDocSegmentOrgDataTemporaryPO2DO(src[i])
	}
	return dst
}

// ==================== 简单 string <-> uint64 辅助 ====================

func stringToUint64(s string) uint64 {
	if s == "" {
		return 0
	}
	v, _ := strconv.ParseUint(s, 10, 64)
	return v
}

func uint64ToString(v uint64) string {
	return strconv.FormatUint(v, 10)
}

// DocSegmentOrgDataTemporaryToTDocSegmentOrgDataTemporary DocSegmentOrgDataTemporary -> TDocSegmentOrgDataTemporary
func ConvertDocSegmentOrgDataTemporaryDO2PO(d *segEntity.DocSegmentOrgDataTemporary) *model.TDocSegmentOrgDataTemporary {
	if d == nil {
		return nil
	}

	return &model.TDocSegmentOrgDataTemporary{
		BusinessID:          d.BusinessID,
		DocBizID:            d.DocBizID,
		AppBizID:            d.AppBizID,
		CorpBizID:           d.CorpBizID,
		StaffBizID:          d.StaffBizID,
		OrgData:             d.OrgData,
		AddMethod:           d.AddMethod,
		Action:              d.Action,
		OrgPageNumbers:      d.OrgPageNumbers,
		SegmentType:         d.SegmentType,
		OriginOrgDataID:     d.OriginOrgDataID,
		LastOrgDataID:       d.LastOrgDataID,
		AfterOrgDataID:      d.AfterOrgDataID,
		LastOriginOrgDataID: d.LastOriginOrgDataID,
		IsDeleted:           d.IsDeleted,
		IsDisabled:          d.IsDisabled,
		CreateTime:          d.CreateTime,
		UpdateTime:          d.UpdateTime,
		AuditStatus:         d.AuditStatus,
		SheetName:           d.SheetName,
	}
}

// ConvertDocSegmentOrgDataTemporaryPO2DO TDocSegmentOrgDataTemporary -> DocSegmentOrgDataTemporary
func ConvertDocSegmentOrgDataTemporaryPO2DO(t *model.TDocSegmentOrgDataTemporary) *segEntity.DocSegmentOrgDataTemporary {
	if t == nil {
		return nil
	}
	return &segEntity.DocSegmentOrgDataTemporary{
		BusinessID:          t.BusinessID,
		AppBizID:            t.AppBizID,
		DocBizID:            t.DocBizID,
		CorpBizID:           t.CorpBizID,
		StaffBizID:          t.StaffBizID,
		OrgData:             t.OrgData,
		AddMethod:           t.AddMethod,
		Action:              t.Action,
		OrgPageNumbers:      t.OrgPageNumbers,
		SegmentType:         t.SegmentType,
		OriginOrgDataID:     t.OriginOrgDataID,
		LastOrgDataID:       t.LastOrgDataID,
		AfterOrgDataID:      t.AfterOrgDataID,
		LastOriginOrgDataID: t.LastOriginOrgDataID,
		IsDeleted:           t.IsDeleted,
		IsDisabled:          t.IsDisabled,
		CreateTime:          t.CreateTime,
		UpdateTime:          t.UpdateTime,
		AuditStatus:         t.AuditStatus,
		SheetName:           t.SheetName,
	}
}

func BatchConvertDocSegmentImageDO2PO(docSegImages []*segEntity.DocSegmentImage) []*model.TDocSegmentImage {
	if len(docSegImages) == 0 {
		return nil
	}

	ret := make([]*model.TDocSegmentImage, 0, len(docSegImages))
	for _, v := range docSegImages {
		ret = append(ret, ConvertDocSegmentImageDO2PO(v))
	}
	return ret
}

func ConvertDocSegmentImagePO2DO(docSegImage *model.TDocSegmentImage) *segEntity.DocSegmentImage {
	if docSegImage == nil {
		return nil
	}
	return &segEntity.DocSegmentImage{
		ID:          docSegImage.ID,
		ImageID:     docSegImage.ImageID,
		SegmentID:   docSegImage.SegmentID,
		DocID:       docSegImage.DocID,
		RobotID:     docSegImage.RobotID,
		CorpID:      docSegImage.CorpID,
		StaffID:     docSegImage.StaffID,
		OriginalUrl: docSegImage.OriginalURL,
		ExternalUrl: docSegImage.ExternalURL,
		IsDeleted:   docSegImage.IsDeleted,
		CreateTime:  docSegImage.CreateTime,
		UpdateTime:  docSegImage.UpdateTime,
	}
}

func BatchConvertDocSegmentImagePO2DO(docSegImages []*model.TDocSegmentImage) []*segEntity.DocSegmentImage {
	if len(docSegImages) == 0 {
		return nil
	}

	ret := make([]*segEntity.DocSegmentImage, 0, len(docSegImages))
	for _, v := range docSegImages {
		ret = append(ret, ConvertDocSegmentImagePO2DO(v))
	}
	return ret
}

func ConvertDocSegmentImageDO2PO(docSegImage *segEntity.DocSegmentImage) *model.TDocSegmentImage {
	if docSegImage == nil {
		return nil
	}
	return &model.TDocSegmentImage{
		ID:          docSegImage.ID,
		ImageID:     docSegImage.ImageID,
		SegmentID:   docSegImage.SegmentID,
		DocID:       docSegImage.DocID,
		RobotID:     docSegImage.RobotID,
		CorpID:      docSegImage.CorpID,
		StaffID:     docSegImage.StaffID,
		OriginalURL: docSegImage.OriginalUrl,
		ExternalURL: docSegImage.ExternalUrl,
		IsDeleted:   docSegImage.IsDeleted,
		CreateTime:  docSegImage.CreateTime,
		UpdateTime:  docSegImage.UpdateTime,
	}
}
