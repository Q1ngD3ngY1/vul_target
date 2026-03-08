package document

import (
	"context"

	"git.woa.com/adp/common/x/logx"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
)

// DescribeFileUploadInfo 获取上传文件列表
func (l *Logic) DescribeFileUploadInfo(ctx context.Context, appID, corpID uint64, fieBizIDs []uint64) (
	map[uint64]*docEntity.CorpCOSDoc, error) {
	docsMap := make(map[uint64]*docEntity.CorpCOSDoc)
	selectColumns := []string{
		docEntity.CorpCOSDocTblColBusinessID,
		docEntity.CorpCOSDocTblColBusinessCosURL,
		docEntity.CorpCOSDocTblColBusinessCosHash,
		docEntity.CorpCOSDocTblColBusinessCosTag,
		docEntity.CorpCOSDocTblColIsDeleted,
		docEntity.CorpCOSDocTblColStatus,
		docEntity.CorpCOSDocTblColFailReason,
	}
	filter := &docEntity.CorpCOSDocFilter{
		BusinessIDs: fieBizIDs,
		RobotID:     appID,
		CorpID:      corpID,
	}
	docs, err := l.docDao.DescribeCorpCosDocList(ctx, selectColumns, filter)
	if err != nil {
		logx.E(ctx, "DescribeFileUploadInfo GetCorpCosDocByBizIDs err:%+v", err)
		return nil, err
	}
	for _, doc := range docs {
		docsMap[doc.BusinessID] = doc
	}
	return docsMap, nil
}
