package document

import (
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/model"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
)

func ConvertDocPOToDO(do *model.TDoc) *docEntity.Doc {
	if do == nil {
		return nil
	}

	return &docEntity.Doc{
		ID:                  uint64(do.ID),
		BusinessID:          do.BusinessID,
		RobotID:             do.RobotID,
		CorpID:              do.CorpID,
		StaffID:             do.StaffID,
		FileName:            do.FileName,
		FileNameInAudit:     do.FileNameInAudit,
		FileType:            do.FileType,
		FileSize:            uint64(do.FileSize),
		Bucket:              do.Bucket,
		CosURL:              do.CosURL,
		CosHash:             do.CosHash,
		Message:             do.Message,
		Status:              do.Status,
		IsDeleted:           do.IsDeleted,
		IsRefer:             do.IsRefer,
		Source:              do.Source,
		WebURL:              do.WebURL,
		BatchID:             int(do.BatchID),
		AuditFlag:           do.AuditFlag,
		CharSize:            uint64(do.CharSize),
		IsCreatingQA:        do.IsCreatingQa,
		IsCreatedQA:         do.IsCreatedQa,
		IsCreatingIndex:     do.IsCreatingIndex,
		NextAction:          do.NextAction,
		AttrRange:           do.AttrRange,
		ReferURLType:        do.ReferURLType,
		CreateTime:          do.CreateTime,
		UpdateTime:          do.UpdateTime,
		ExpireStart:         do.ExpireStart,
		ExpireEnd:           do.ExpireEnd,
		Opt:                 do.Opt,
		CategoryID:          do.CategoryID,
		OriginalURL:         do.OriginalURL,
		ProcessingFlag:      uint64(do.ProcessingFlag),
		CustomerKnowledgeId: do.CustomerKnowledgeID,
		AttributeFlag:       uint64(do.AttributeFlag),
		IsDownloadable:      do.IsDownloadable,
		UpdatePeriodH:       do.UpdatePeriodH,
		NextUpdateTime:      do.NextUpdateTime,
		SplitRule:           do.SplitRule,
		EnableScope:         do.EnableScope,
	}
}

func BatchConvertDocPOToDO(docs []*model.TDoc) []*docEntity.Doc {
	ret := make([]*docEntity.Doc, 0, len(docs))
	if len(docs) == 0 {
		return ret
	}

	for _, do := range docs {
		ret = append(ret, ConvertDocPOToDO(do))
	}
	return ret
}

func ConvertDocDoToPO(po *docEntity.Doc) *model.TDoc {
	if po == nil {
		return nil
	}

	docDo := &model.TDoc{
		ID:                  po.ID,
		BusinessID:          po.BusinessID,
		RobotID:             po.RobotID,
		CorpID:              po.CorpID,
		StaffID:             po.StaffID,
		FileName:            po.FileName,
		FileNameInAudit:     po.FileNameInAudit,
		FileType:            po.FileType,
		FileSize:            int64(po.FileSize),
		Bucket:              po.Bucket,
		CosURL:              po.CosURL,
		CosHash:             po.CosHash,
		Message:             po.Message,
		Status:              po.Status,
		IsDeleted:           po.IsDeleted,
		Source:              po.Source,
		WebURL:              po.WebURL,
		BatchID:             int32(po.BatchID),
		AuditFlag:           po.AuditFlag,
		CharSize:            int64(po.CharSize),
		NextAction:          po.NextAction,
		AttrRange:           po.AttrRange,
		ReferURLType:        po.ReferURLType,
		CreateTime:          po.CreateTime,
		UpdateTime:          po.UpdateTime,
		ExpireStart:         po.ExpireStart,
		ExpireEnd:           po.ExpireEnd,
		Opt:                 po.Opt,
		CategoryID:          po.CategoryID,
		OriginalURL:         po.OriginalURL,
		ProcessingFlag:      int64(po.ProcessingFlag),
		CustomerKnowledgeID: po.CustomerKnowledgeId,
		AttributeFlag:       int64(po.AttributeFlag),
		UpdatePeriodH:       po.UpdatePeriodH,
		NextUpdateTime:      po.NextUpdateTime,
		SplitRule:           po.SplitRule,
		IsCreatingIndex:     po.IsCreatingIndex,
		IsCreatingQa:        po.IsCreatingQA,
		IsCreatedQa:         po.IsCreatedQA,
		IsRefer:             po.IsRefer,
		IsDownloadable:      po.IsDownloadable,
		EnableScope:         po.EnableScope,
	}
	return docDo
}

func BatchConvertDocDoToPO(docs []*docEntity.Doc) []*model.TDoc {
	if len(docs) == 0 {
		return nil
	}

	ret := make([]*model.TDoc, 0, len(docs))
	for _, po := range docs {
		ret = append(ret, ConvertDocDoToPO(po))
	}
	return ret
}

func ConvertDocSchemaPO2DO(po *model.TDocSchema) *docEntity.DocSchema {
	if po == nil {
		return nil
	}
	return &docEntity.DocSchema{
		ID:         po.ID,
		CorpBizID:  po.CorpBizID,
		AppBizID:   po.AppBizID,
		DocBizID:   po.DocBizID,
		FileName:   po.FileName,
		Summary:    po.Summary,
		Vector:     po.Vector,
		IsDeleted:  po.IsDeleted,
		CreateTime: po.CreateTime,
		UpdateTime: po.UpdateTime,
	}
}

func BatchConvertDocSchemaPO2DO(pos []*model.TDocSchema) []*docEntity.DocSchema {
	if len(pos) == 0 {
		return nil
	}
	dos := make([]*docEntity.DocSchema, 0, len(pos))
	for _, po := range pos {
		dos = append(dos, ConvertDocSchemaPO2DO(po))
	}
	return dos
}

func ConvertDocSchemaDO2PO(do *docEntity.DocSchema) *model.TDocSchema {
	if do == nil {
		return nil
	}
	return &model.TDocSchema{
		ID:         do.ID,
		CorpBizID:  do.CorpBizID,
		AppBizID:   do.AppBizID,
		DocBizID:   do.DocBizID,
		FileName:   do.FileName,
		Summary:    do.Summary,
		Vector:     do.Vector,
		IsDeleted:  do.IsDeleted,
		CreateTime: do.CreateTime,
		UpdateTime: do.UpdateTime,
	}
}

func BatchConvertDocSchemaDO2PO(dos []*docEntity.DocSchema) []*model.TDocSchema {
	if len(dos) == 0 {
		return nil
	}
	pos := make([]*model.TDocSchema, 0, len(dos))
	for _, do := range dos {
		pos = append(pos, ConvertDocSchemaDO2PO(do))
	}
	return pos
}

// ConvertDocParsePO2DO TDocParse -> DocParse
func ConvertDocParsePO2DO(t *model.TDocParse) *docEntity.DocParse {
	if t == nil {
		return nil
	}
	return &docEntity.DocParse{
		ID:           t.ID,
		CorpID:       t.CorpID,
		RobotID:      t.RobotID,
		StaffID:      0, // 源结构体无此字段，保持零值
		DocID:        t.DocID,
		SourceEnvSet: t.SourceEnvSet,
		RequestID:    t.RequestID,
		TaskID:       t.TaskID,
		Type:         uint32(t.Type),
		OpType:       uint32(t.OpType),
		Result:       t.Result,
		Status:       uint32(t.Status),
		CreateTime:   t.CreateTime,
		UpdateTime:   t.UpdateTime,
	}
}

// ConvertDocParseDO2PO DocParse -> TDocParse
func ConvertDocParseDO2PO(d *docEntity.DocParse) *model.TDocParse {
	return &model.TDocParse{
		ID:           d.ID,
		RobotID:      d.RobotID,
		CorpID:       d.CorpID,
		DocID:        d.DocID,
		SourceEnvSet: d.SourceEnvSet,
		RequestID:    d.RequestID,
		TaskID:       d.TaskID,
		OpType:       int32(d.OpType),
		Type:         int32(d.Type),
		Result:       d.Result,
		Status:       int32(d.Status),
		CreateTime:   d.CreateTime,
		UpdateTime:   d.UpdateTime,
	}
}

// BatchConvertDocParsePO2DO []TDocParse -> []DocParse
func BatchConvertDocParsePO2DO(src []*model.TDocParse) []*docEntity.DocParse {
	if len(src) == 0 {
		return nil
	}
	dst := make([]*docEntity.DocParse, len(src))
	for i := range src {
		dst[i] = ConvertDocParsePO2DO(src[i])
	}
	return dst
}

// BatchConvertDocParseDO2PO []DocParse -> []TDocParse
func BatchConvertDocParseDO2PO(src []*docEntity.DocParse) []*model.TDocParse {
	if len(src) == 0 {
		return nil
	}
	dst := make([]*model.TDocParse, len(src))
	for i := range src {
		dst[i] = ConvertDocParseDO2PO(src[i])
	}
	return dst
}

func ConvertText2sqlMetaMappingPO2DO(po *model.TText2sqlMetaMappingPreview) *docEntity.Text2sqlMetaMappingPreview {
	if po == nil {
		return nil
	}
	return &docEntity.Text2sqlMetaMappingPreview{
		ID:               po.ID,
		BusinessID:       po.BusinessID,
		CorpID:           po.CorpID,
		RobotID:          po.RobotID,
		DocID:            po.DocID,
		TableID:          po.TableID,
		FileName:         po.FileName,
		Mapping:          po.Mapping,
		ReleaseStatus:    int8(po.ReleaseStatus),
		IndexName:        po.IndexName,
		DbName:           po.DbName,
		MappingTableName: po.TableName_,
		SubType:          po.SubType,
		IsDeleted:        int(po.IsDeleted),
		CreateTime:       po.CreateTime,
		UpdateTime:       po.UpdateTime,
	}
}

func BatchConvertText2sqlMetaMappingPO2DO(pos []*model.TText2sqlMetaMappingPreview) []*docEntity.Text2sqlMetaMappingPreview {
	if len(pos) == 0 {
		return nil
	}
	var dos []*docEntity.Text2sqlMetaMappingPreview
	for _, po := range pos {
		dos = append(dos, ConvertText2sqlMetaMappingPO2DO(po))
	}
	return dos
}

func ConvertText2sqlMetaMappingDO2PO(do *docEntity.Text2sqlMetaMappingPreview) *model.TText2sqlMetaMappingPreview {
	if do == nil {
		return nil
	}
	return &model.TText2sqlMetaMappingPreview{
		ID:            do.ID,
		BusinessID:    do.BusinessID,
		CorpID:        do.CorpID,
		RobotID:       do.RobotID,
		DocID:         do.DocID,
		TableID:       do.TableID,
		FileName:      do.FileName,
		Mapping:       do.Mapping,
		ReleaseStatus: uint32(do.ReleaseStatus),
		IndexName:     do.IndexName,
		DbName:        do.DbName,
		TableName_:    do.MappingTableName,
		SubType:       do.SubType,
		IsDeleted:     int32(do.IsDeleted),
		CreateTime:    do.CreateTime,
		UpdateTime:    do.UpdateTime,
	}
}

func BatchConvertText2sqlMetaMappingDO2PO(dos []*docEntity.Text2sqlMetaMappingPreview) []*model.TText2sqlMetaMappingPreview {
	if len(dos) == 0 {
		return nil
	}
	var pos []*model.TText2sqlMetaMappingPreview
	for _, do := range dos {
		pos = append(pos, ConvertText2sqlMetaMappingDO2PO(do))
	}
	return pos
}

func ConvertDocDiffTaskPO2DO(po *model.TDocDiffTask) *docEntity.DocDiff {
	if po == nil {
		return nil
	}
	return &docEntity.DocDiff{
		BusinessID:            po.BusinessID,
		CorpBizID:             po.CorpBizID,
		RobotBizID:            po.RobotBizID,
		StaffBizID:            po.StaffBizID,
		NewDocBizID:           po.NewDocBizID,
		OldDocBizID:           po.OldDocBizID,
		TaskID:                po.TaskID,
		DocQATaskID:           po.DocQaTaskID,
		NewDocRename:          po.NewDocRename,
		OldDocRename:          po.OldDocRename,
		ComparisonReason:      uint32(po.ComparisonReason),
		DiffType:              uint32(po.DiffType),
		DocOperation:          uint32(po.DocOperation),
		DocOperationStatus:    uint32(po.DocOperationStatus),
		QaOperation:           uint32(po.QaOperation),
		QaOperationStatus:     uint32(po.QaOperationStatus),
		QaOperationResult:     po.QaOperationResult,
		Status:                uint32(po.Status),
		DiffDataProcessStatus: uint32(po.DiffDataProcessStatus),
		IsDeleted:             po.IsDeleted,
		CreateTime:            po.CreateTime,
		UpdateTime:            po.UpdateTime,
	}
}

func BatchConvertDocDiffTaskPO2DO(po []*model.TDocDiffTask) []*docEntity.DocDiff {
	if len(po) == 0 {
		return nil
	}
	var res []*docEntity.DocDiff
	for _, p := range po {
		res = append(res, ConvertDocDiffTaskPO2DO(p))
	}
	return res
}

func ConvertDocDiffTaskDO2PO(do *docEntity.DocDiff) *model.TDocDiffTask {
	if do == nil {
		return nil
	}
	return &model.TDocDiffTask{
		BusinessID:            do.BusinessID,
		CorpBizID:             do.CorpBizID,
		RobotBizID:            do.RobotBizID,
		StaffBizID:            do.StaffBizID,
		NewDocBizID:           do.NewDocBizID,
		OldDocBizID:           do.OldDocBizID,
		TaskID:                do.TaskID,
		DocQaTaskID:           do.DocQATaskID,
		NewDocRename:          do.NewDocRename,
		OldDocRename:          do.OldDocRename,
		ComparisonReason:      int32(do.ComparisonReason),
		DiffType:              int32(do.DiffType),
		DocOperation:          int32(do.DocOperation),
		DocOperationStatus:    int32(do.DocOperationStatus),
		QaOperation:           int32(do.QaOperation),
		QaOperationStatus:     int32(do.QaOperationStatus),
		QaOperationResult:     do.QaOperationResult,
		Status:                int32(do.Status),
		DiffDataProcessStatus: int32(do.DiffDataProcessStatus),
		IsDeleted:             do.IsDeleted,
		CreateTime:            do.CreateTime,
		UpdateTime:            do.UpdateTime,
	}
}

func BatchConvertDocDiffTaskDO2PO(do []*docEntity.DocDiff) []*model.TDocDiffTask {
	if len(do) == 0 {
		return nil
	}
	var res []*model.TDocDiffTask
	for _, d := range do {
		res = append(res, ConvertDocDiffTaskDO2PO(d))
	}
	return res
}

func ConvertDocDiffDataPO2DO(po *model.TDocDiffDatum) *docEntity.DocDiffData {
	if po == nil {
		return nil
	}
	return &docEntity.DocDiffData{
		CorpBizID:  po.CorpBizID,
		RobotBizID: po.RobotBizID,
		DiffBizID:  po.DiffBizID,
		DiffData:   po.DiffData,
		DiffIndex:  int(po.DiffIndex),
		IsDeleted:  po.IsDeleted,
		CreateTime: po.CreateTime,
		UpdateTime: po.UpdateTime,
	}
}

func BatchConvertDocDiffDataPO2DO(po []*model.TDocDiffDatum) []*docEntity.DocDiffData {
	if len(po) == 0 {
		return []*docEntity.DocDiffData{}
	}
	res := make([]*docEntity.DocDiffData, 0)
	for _, p := range po {
		res = append(res, ConvertDocDiffDataPO2DO(p))
	}
	return res
}

func ConvertDocDiffDataDO2PO(do *docEntity.DocDiffData) *model.TDocDiffDatum {
	if do == nil {
		return nil
	}
	return &model.TDocDiffDatum{
		CorpBizID:  do.CorpBizID,
		RobotBizID: do.RobotBizID,
		DiffBizID:  do.DiffBizID,
		DiffData:   do.DiffData,
		DiffIndex:  int32(do.DiffIndex),
		IsDeleted:  do.IsDeleted,
		CreateTime: do.CreateTime,
		UpdateTime: do.UpdateTime,
	}
}

func BatchConvertDocDiffDataDO2PO(do []*docEntity.DocDiffData) []*model.TDocDiffDatum {
	if len(do) == 0 {
		return []*model.TDocDiffDatum{}
	}
	res := make([]*model.TDocDiffDatum, 0)
	for _, d := range do {
		res = append(res, ConvertDocDiffDataDO2PO(d))
	}
	return res
}

func ConvertDocClusterSchemaPO2DO(p *model.TDocClusterSchema) *docEntity.DocClusterSchema {
	if p == nil {
		return nil
	}
	return &docEntity.DocClusterSchema{
		ID:          p.ID,
		CorpBizID:   p.CorpBizID,
		AppBizID:    p.AppBizID,
		BusinessID:  p.BusinessID,
		Version:     uint64(p.Version),
		ClusterName: p.ClusterName,
		Summary:     p.Summary,
		DocIDs:      p.DocIds,
		IsDeleted:   p.IsDeleted,
	}
}

func BatchConvertDocClusterSchemaPO2DO(ps []*model.TDocClusterSchema) []*docEntity.DocClusterSchema {
	if len(ps) == 0 {
		return nil
	}
	ds := make([]*docEntity.DocClusterSchema, 0, len(ps))
	for _, p := range ps {
		ds = append(ds, ConvertDocClusterSchemaPO2DO(p))
	}
	return ds
}

func ConvertDocClusterSchemaDO2PO(p *docEntity.DocClusterSchema) *model.TDocClusterSchema {
	if p == nil {
		return nil
	}
	return &model.TDocClusterSchema{
		ID:          p.ID,
		CorpBizID:   p.CorpBizID,
		AppBizID:    p.AppBizID,
		BusinessID:  p.BusinessID,
		Version:     int64(p.Version),
		ClusterName: p.ClusterName,
		Summary:     p.Summary,
		DocIds:      p.DocIDs,
		IsDeleted:   p.IsDeleted,
	}
}

func ConvertCorpCosDocDO2PO(do *docEntity.CorpCOSDoc) *model.TCorpCosDoc {
	if do == nil {
		return nil
	}
	return &model.TCorpCosDoc{
		ID:              do.ID,
		BusinessID:      do.BusinessID,
		CorpID:          do.CorpID,
		RobotID:         do.RobotID,
		StaffID:         do.StaffID,
		CosBucket:       do.CosBucket,
		CosPath:         do.CosPath,
		CosHash:         do.CosHash,
		CosTag:          do.CosTag,
		IsDeleted:       do.IsDeleted,
		Status:          int32(do.Status),
		FailReason:      do.FailReason,
		SyncTime:        do.SyncTime,
		BusinessCosURL:  do.BusinessCosURL,
		BusinessCosHash: do.BusinessCosHash,
		BusinessCosTag:  do.BusinessCosTag,
		CreateTime:      do.CreateTime,
		UpdateTime:      do.UpdateTime,
	}
}

func BatchConvertCorpCosDocDO2PO(dos []*docEntity.CorpCOSDoc) []*model.TCorpCosDoc {
	if len(dos) == 0 {
		return nil
	}
	pos := make([]*model.TCorpCosDoc, 0, len(dos))
	for _, do := range dos {
		pos = append(pos, ConvertCorpCosDocDO2PO(do))
	}
	return pos
}

func ConvertCorpCosDocPO2DO(po *model.TCorpCosDoc) *docEntity.CorpCOSDoc {
	if po == nil {
		return nil
	}
	return &docEntity.CorpCOSDoc{
		ID:              po.ID,
		BusinessID:      po.BusinessID,
		CorpID:          po.CorpID,
		RobotID:         po.RobotID,
		StaffID:         po.StaffID,
		CosBucket:       po.CosBucket,
		CosPath:         po.CosPath,
		CosHash:         po.CosHash,
		CosTag:          po.CosTag,
		IsDeleted:       po.IsDeleted,
		Status:          uint32(po.Status),
		FailReason:      po.FailReason,
		SyncTime:        po.SyncTime,
		BusinessCosURL:  po.BusinessCosURL,
		BusinessCosHash: po.BusinessCosHash,
		BusinessCosTag:  po.BusinessCosTag,
		CreateTime:      po.CreateTime,
		UpdateTime:      po.UpdateTime,
	}
}

func BatchConvertCorpCosDocPO2DO(pos []*model.TCorpCosDoc) []*docEntity.CorpCOSDoc {
	if len(pos) == 0 {
		return nil
	}
	dos := make([]*docEntity.CorpCOSDoc, 0, len(pos))
	for _, po := range pos {
		dos = append(dos, ConvertCorpCosDocPO2DO(po))
	}
	return dos
}
