package vector

import (
	"context"

	"git.code.oa.com/trpc-go/trpc-go/client"
	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/common/x/logx"
	knowClient "git.woa.com/adp/kb/kb-config/internal/rpc"
)

// UpdateImageVectorDeleteStatus 根据图片ID更新image_vector的删除状态
func (d *daoImpl) UpdateImageVectorDeleteStatus(ctx context.Context, robotID uint64, imageIDs []uint64, isDeleted int) error {
	if len(imageIDs) == 0 {
		return nil
	}
	db, err := knowClient.GormClient(ctx, imageVectorTable, robotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "UpdateImageVectorDeleteStatus get client error, %v", err)
		return err
	}
	for _, imageIDChunks := range slicex.Chunk(imageIDs, 200) {
		err = db.WithContext(ctx).Model(&ImageVector{}).
			Where("robot_id = ? AND image_id in (?)", robotID, imageIDChunks).
			UpdateColumns(map[string]any{
				"is_deleted": isDeleted,
			}).Error
		if err != nil {
			logx.E(ctx, "UpdateImageVectorDeleteStatus update failed, err:%+v, imageIDs:%+v", err, imageIDs)
			return err
		}
	}
	return nil
}
