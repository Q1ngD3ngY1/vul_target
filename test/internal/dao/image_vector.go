package dao

import (
	"context"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/client"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/baicaoyuan/moss/types/slicex"
	knowClient "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
)

const (
	imageVectorTable = "t_image_vector"
)

// ImageVector 切片图片向量存储
type ImageVector struct {
	ID                 uint64    `gorm:"column:id"` // 自增ID
	ImageID            uint64    `gorm:"column:image_id"`
	RobotID            uint64    `gorm:"column:robot_id"`
	Content            string    `gorm:"column:content"`                                     // 内容,图片链接
	EmbeddingVersionID uint64    `gorm:"column:embedding_version_id"`                        // embedding 版本
	VectorRaw          []byte    `gorm:"column:vector"`                                      // 向量
	IsDeleted          uint32    `gorm:"column:is_deleted"`                                  // 是否删除(0未删除 1已删除）
	UpdateTime         time.Time `gorm:"column:update_time;type:datetime(0);autoUpdateTime"` // 更新时间
	CreateTime         time.Time `gorm:"column:create_time;type:datetime(0);autoCreateTime"` // 响应时间
}

// TableName 切片图片向量存储
func (ImageVector) TableName() string {
	return "t_image_vector"
}

// UpdateImageVectorDeleteStatus 根据图片ID更新image_vector的删除状态
func UpdateImageVectorDeleteStatus(ctx context.Context, robotID uint64, imageIDs []uint64, isDeleted int) error {
	if len(imageIDs) == 0 {
		return nil
	}
	db, err := knowClient.GormClient(ctx, imageVectorTable, robotID, 0, []client.Option{}...)
	if err != nil {
		log.ErrorContextf(ctx, "UpdateImageVectorDeleteStatus get client error, %v", err)
		return err
	}
	for _, imageIDChunks := range slicex.Chunk(imageIDs, 200) {
		err = db.WithContext(ctx).Model(&ImageVector{}).
			Where("robot_id = ? AND image_id in (?)", robotID, imageIDChunks).
			UpdateColumns(map[string]interface{}{
				"is_deleted": isDeleted,
			}).Error
		if err != nil {
			log.ErrorContextf(ctx, "UpdateImageVectorDeleteStatus update failed, err:%+v, imageIDs:%+v", err, imageIDs)
			return err
		}
	}
	return nil
}
