package segment

import (
	"context"
	"fmt"
	"math"
	"net/url"
	"strings"
	"sync"
	"time"

	"git.woa.com/adp/common/x/logx"
	segEntity "git.woa.com/adp/kb/kb-config/internal/entity/segment"
	"git.woa.com/adp/kb/kb-config/internal/util/idgen"
	"gorm.io/gorm"
)

// createDocSegmentImages 文档切片Images入库
func (l *Logic) createDocSegmentImages(ctx context.Context, tx *gorm.DB,
	segmentBizIDMap map[uint64]*segEntity.DocSegmentExtend, segmentImages []*segEntity.DocSegmentImage) error {
	if len(segmentImages) == 0 {
		logx.I(ctx, "createDocSegmentImages|len(segmentImages):%d|segmentImages is empty",
			len(segmentImages))
		return nil
	}
	pageSize := 500
	total := len(segmentImages)
	pages := int(math.Ceil(float64(total) / float64(pageSize)))
	for i := 0; i < pages; i++ {
		start := pageSize * i
		end := pageSize * (i + 1)
		if end > total {
			end = total
		}
		logx.I(ctx, "createDocSegmentImages|segmentImages.len:%d|start:%d|end:%d",
			len(segmentImages), start, end)
		tmpSegmentImages := segmentImages[start:end]
		for _, tmpSegmentImage := range tmpSegmentImages {
			seg, ok := segmentBizIDMap[tmpSegmentImage.SegmentBizID]
			if !ok {
				logx.E(ctx, "createDocSegmentImages|segBizID is not found|"+
					"tmpSegmentImage:%+v", tmpSegmentImage)
				return fmt.Errorf("segBizID is not found")
			}
			tmpSegmentImage.SegmentID = seg.ID
		}

		if err := l.segDao.CreateDocSegmentImages(ctx, tmpSegmentImages, tx); err != nil {
			logx.E(ctx, "createDocSegmentImages|CreateDocSegmentImages|err:%+v", err)
			return err
		}
		logx.I(ctx, "createDocSegmentImages|CreateDocSegmentImages|success|len(tmpSegmentImages):%d", len(tmpSegmentImages))
	}
	return nil
}

// getDocSegmentImages 获取文档切片Images
func (l *Logic) getDocSegmentImages(ctx context.Context, shortURLSyncMap, imageDataSyncMap *sync.Map,
	segment *segEntity.DocSegmentExtend) ([]*segEntity.DocSegmentImage, error) {
	// 只有是文档切片，才需要把 切片图片存储
	if !segment.IsSegmentForIndex() {
		logx.I(ctx, "getDocSegmentBigData|segment:%+v|Type is ignore", segment)
		return nil, nil
	}

	if len(segment.Images) == 0 {
		logx.I(ctx, "getDocSegmentImages|segment:%+v|Images is empty", segment)
		return nil, nil
	}

	segmentImages := make([]*segEntity.DocSegmentImage, 0)
	for imageIndex, originalUrl := range segment.Images {
		// imageID := uint64(0)
		// if id, ok := imageDataSyncMap.Load(originalUrl); ok {
		//	imageID = id.(uint64)
		// } else {
		//	imageID = idgen.GetId()
		//	imageDataSyncMap.Store(originalUrl, imageID)
		// }
		// 2.4.0 @harryhlli @jouislu 结论：相同图片也用不同图片ID
		if len(originalUrl) == 0 {
			logx.E(ctx, "getDocSegmentImages|segment:(%d)|originalUrl (No. %d)is empty",
				segment.ID, imageIndex)
			continue
		}
		imageID := idgen.GetId()
		externalUrl := ""
		URL, err := url.Parse(originalUrl)
		if err != nil || URL.Path == "" {
			logx.E(ctx, "getDocSegmentImages|segment:%+v|originalUrl is illegal", segment)
			return nil, fmt.Errorf("originalUrl is illegal")
		}
		oldURL := URL.Scheme + "://" + URL.Host + URL.Path
		if value, ok := shortURLSyncMap.Load(oldURL); ok {
			newURL := value.(string)
			externalUrl = strings.ReplaceAll(originalUrl, oldURL, newURL)
		} else {
			logx.E(ctx, "getDocSegmentImages|segment:%+v|oldURL：%s｜externalUrl is empty",
				segment, oldURL)
			return nil, fmt.Errorf("externalUrl is empty")
		}
		segmentImages = append(segmentImages, &segEntity.DocSegmentImage{
			ImageID:      imageID,
			DocID:        segment.DocID,
			RobotID:      segment.RobotID,
			CorpID:       segment.CorpID,
			StaffID:      segment.StaffID,
			OriginalUrl:  originalUrl,
			ExternalUrl:  externalUrl,
			IsDeleted:    segment.IsDeleted,
			CreateTime:   time.Now(),
			UpdateTime:   time.Now(),
			SegmentBizID: segment.BusinessID, // SegmentID 此时还不能确定，需要等segment写入后再通过SegmentBizID查询
		})
	}
	logx.I(ctx, "Finallt, %d images were found in segment(ID:%d)", len(segmentImages), segment.ID)
	return segmentImages, nil
}
