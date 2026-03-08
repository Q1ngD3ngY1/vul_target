package dao

import (
	"context"

	"git.code.oa.com/trpc-go/trpc-go/client"
	"git.code.oa.com/trpc-go/trpc-go/log"
	knowClient "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
)

var globalReleaseDocDao *ReleaseDocDao

const (
	releaseDocTableName = "t_release_doc"

	ReleaseDocTblColCorpId    = "corp_id"    // 企业ID
	ReleaseDocTblColRobotId   = "robot_id"   // 机器人ID
	ReleaseDocTblColVersionId = "version_id" // 版本ID
	ReleaseDocTblColDocId     = "doc_id"     // 文档ID
)

type ReleaseDocDao struct {
	BaseDao
	tableName string
}

// GetReleaseDocDao 获取全局的数据操作对象
func GetReleaseDocDao() *ReleaseDocDao {
	if globalReleaseDocDao == nil {
		globalReleaseDocDao = &ReleaseDocDao{*globalBaseDao, releaseDocTableName}
	}
	return globalReleaseDocDao
}

func (d *ReleaseDocDao) GetReleaseDocIdMap(ctx context.Context, corpId, robotId, versionId uint64,
	docIds []uint64) (map[uint64]struct{}, error) {
	releaseDocIdMap := make(map[uint64]struct{}, 0)
	if len(docIds) == 0 {
		return releaseDocIdMap, nil
	}
	releaseDocs := make([]*model.ReleaseDoc, 0)
	db, err := knowClient.GormClient(ctx, d.tableName, robotId, 0, []client.Option{}...)
	if err != nil {
		log.ErrorContextf(ctx, "get GormClient failed, err: %+v", err)
		return nil, err
	}
	res := db.WithContext(ctx).Table(d.tableName).Select(ReleaseDocTblColDocId).
		Where(ReleaseDocTblColCorpId+sqlEqual, corpId).
		Where(ReleaseDocTblColRobotId+sqlEqual, robotId).
		Where(ReleaseDocTblColVersionId+sqlEqual, versionId).
		Where(ReleaseDocTblColDocId+sqlIn, docIds).
		Find(&releaseDocs)
	if res.Error != nil {
		log.ErrorContextf(ctx, "execute sql failed, err: %+v", res.Error)
		return nil, res.Error
	}
	for _, releaseDoc := range releaseDocs {
		releaseDocIdMap[releaseDoc.DocID] = struct{}{}
	}
	return releaseDocIdMap, nil
}
