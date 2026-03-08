package dao

import (
	"context"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
)

var globalReleaseQaDao *ReleaseQaDao

const (
	releaseQaTableName = "t_release_qa"

	ReleaseQaTblColCorpId    = "corp_id"    // 企业ID
	ReleaseQaTblColRobotId   = "robot_id"   // 机器人ID
	ReleaseQaTblColVersionId = "version_id" // 版本ID
	ReleaseQaTblColQaId      = "qa_id"      // 问答ID
	ReleaseQaTblColDocId     = "doc_id"     // 文档ID
)

type ReleaseQaDao struct {
	BaseDao
}

// GetReleaseQaDao 获取全局的数据操作对象
func GetReleaseQaDao() *ReleaseQaDao {
	if globalReleaseQaDao == nil {
		globalReleaseQaDao = &ReleaseQaDao{*globalBaseDao}
	}
	return globalReleaseQaDao
}

func (d *ReleaseQaDao) GetReleaseQaIdMap(ctx context.Context, corpId, robotId, versionId uint64,
	qaIds []uint64) (map[uint64]struct{}, error) {
	releaseQaIdMap := make(map[uint64]struct{}, 0)
	if len(qaIds) == 0 {
		return releaseQaIdMap, nil
	}
	releaseQas := make([]*model.ReleaseQA, 0)
	res := d.gormDB.WithContext(ctx).Table(releaseQaTableName).Select(ReleaseQaTblColQaId).
		Where(ReleaseQaTblColCorpId+sqlEqual, corpId).
		Where(ReleaseQaTblColRobotId+sqlEqual, robotId).
		Where(ReleaseQaTblColVersionId+sqlEqual, versionId).
		Where(ReleaseQaTblColQaId+sqlIn, qaIds).
		Find(&releaseQas)
	if res.Error != nil {
		log.ErrorContextf(ctx, "execute sql failed, err: %+v", res.Error)
		return nil, res.Error
	}
	for _, releaseQa := range releaseQas {
		releaseQaIdMap[releaseQa.QAID] = struct{}{}
	}
	return releaseQaIdMap, nil
}

func (d *ReleaseQaDao) GetReleaseQaDocIdMap(ctx context.Context, corpId, robotId, versionId uint64,
	docIds []uint64) (map[uint64]struct{}, error) {
	releaseQaDocIdMap := make(map[uint64]struct{}, 0)
	if len(docIds) == 0 {
		return releaseQaDocIdMap, nil
	}
	releaseQas := make([]*model.ReleaseQA, 0)
	res := d.gormDB.WithContext(ctx).Table(releaseQaTableName).Select(ReleaseQaTblColDocId).
		Where(ReleaseQaTblColCorpId+sqlEqual, corpId).
		Where(ReleaseQaTblColRobotId+sqlEqual, robotId).
		Where(ReleaseQaTblColVersionId+sqlEqual, versionId).
		Where(ReleaseQaTblColDocId+sqlIn, docIds).Distinct(ReleaseQaTblColDocId).
		Find(&releaseQas)
	if res.Error != nil {
		log.ErrorContextf(ctx, "execute sql failed, err: %+v", res.Error)
		return nil, res.Error
	}
	for _, releaseQa := range releaseQas {
		releaseQaDocIdMap[releaseQa.DocID] = struct{}{}
	}
	return releaseQaDocIdMap, nil
}
