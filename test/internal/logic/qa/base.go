package qa

import (
	"context"
	"net/url"
	"strings"

	"git.woa.com/adp/common/x/clientx/s3x"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/dao"
	"git.woa.com/adp/kb/kb-config/internal/dao/category"
	docDao "git.woa.com/adp/kb/kb-config/internal/dao/document"
	labelDao "git.woa.com/adp/kb/kb-config/internal/dao/label"
	qadao "git.woa.com/adp/kb/kb-config/internal/dao/qa"
	releaseDao "git.woa.com/adp/kb/kb-config/internal/dao/release"
	segDao "git.woa.com/adp/kb/kb-config/internal/dao/segment"
	labelEntity "git.woa.com/adp/kb/kb-config/internal/entity/label"
	"git.woa.com/adp/kb/kb-config/internal/logic/finance"
	"git.woa.com/adp/kb/kb-config/internal/logic/llm"
	segLogic "git.woa.com/adp/kb/kb-config/internal/logic/segment"
	"git.woa.com/adp/kb/kb-config/internal/logic/vector"
	"git.woa.com/adp/kb/kb-config/internal/rpc"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
)

func NewLogic(labelDao labelDao.Dao, cateDao category.Dao,
	docDao docDao.Dao, qaDao qadao.Dao, releaseDao releaseDao.Dao, segDao segDao.Dao,
	segLogic *segLogic.Logic, financeLogic *finance.Logic, llmLogic *llm.Logic,
	vectorSyncLogic *vector.VectorSyncLogic, s3 dao.S3, rpc *rpc.RPC) *Logic {
	return &Logic{
		labelDao:        labelDao,
		cateDao:         cateDao,
		docDao:          docDao,
		qaDao:           qaDao,
		releaseDao:      releaseDao,
		segDao:          segDao,
		segLogic:        segLogic,
		vectorSyncLogic: vectorSyncLogic,
		financeLogic:    financeLogic,
		llmLogic:        llmLogic,
		s3:              s3,
		rpc:             rpc,
	}
}

type Logic struct {
	cateDao         category.Dao
	docDao          docDao.Dao
	qaDao           qadao.Dao
	labelDao        labelDao.Dao
	releaseDao      releaseDao.Dao
	segDao          segDao.Dao
	segLogic        *segLogic.Logic
	vectorSyncLogic *vector.VectorSyncLogic
	s3              dao.S3
	rpc             *rpc.RPC
	financeLogic    *finance.Logic
	llmLogic        *llm.Logic
}

func (l *Logic) GetDao() qadao.Dao {
	return l.qaDao
}

func (l *Logic) GetVectorSyncLogic() *vector.VectorSyncLogic {
	return l.vectorSyncLogic
}

// updateQAAttributeLabel 更新QA的属性标签
func (l *Logic) UpdateQAAttributeLabel(ctx context.Context, robotID, qaID uint64,
	attributeLabelReq *labelEntity.UpdateQAAttributeLabelReq) error {
	if attributeLabelReq == nil {
		logx.W(ctx, "UpdateQAAttributeLabel|attributeLabelReq is nil")
		return nil
	}
	if !attributeLabelReq.IsNeedChange {
		return nil
	}
	for _, v := range attributeLabelReq.AttributeLabels {
		v.RobotID, v.QAID = robotID, qaID
	}
	if err := l.labelDao.DeleteQAAttributeLabel(ctx, robotID, qaID); err != nil {
		return err
	}
	if err := l.labelDao.CreateQAAttributeLabel(ctx, attributeLabelReq.AttributeLabels); err != nil {
		return err
	}
	return nil
}

// GetVideoURLsCharSize 从html中提取视频链接返回字符数
func (l *Logic) GetVideoURLsCharSize(ctx context.Context, htmlStr string) (int, int64, error) {
	if htmlStr == "" {
		return 0, 0, nil
	}
	var fileSizeCount int64
	var fileCharSize int
	videos, err := util.AuditQaVideoURLs(ctx, htmlStr)
	if err != nil {
		return 0, 0, err
	}
	if len(videos) == 0 {
		return 0, 0, nil
	}
	for _, videoUrl := range videos {
		objectInfo, err := l.GetCosFileInfoByUrl(ctx, videoUrl.CosURL)
		if err != nil {
			return 0, 0, err
		}
		if objectInfo != nil {
			fileSizeCount += objectInfo.Size
		}
	}
	logx.I(ctx, "getVideoURLsCharSize|len(files)|%d|fileSizeCount|%d", len(videos), fileSizeCount)
	if len(videos) > 0 && fileSizeCount > 0 {
		fileCharSize = util.ConvertBytesToChars(ctx, fileSizeCount)
		logx.I(ctx, "getVideoURLsCharSize|ConvertBytesToChars|%d", fileCharSize)
		return fileCharSize, fileSizeCount, nil
	}
	return 0, 0, nil
}

// GetCosFileInfoByUrl 根据cos_url获取cos文件信息
func (l *Logic) GetCosFileInfoByUrl(ctx context.Context, cosUrl string) (*s3x.ObjectInfo, error) {
	u, err := url.Parse(cosUrl)
	if err != nil {
		return nil, err
	}
	if u.Host != config.App().Storage.VideoDomain {
		logx.W(ctx, "GetCosFileInfoByUrl|Path:%s != VideoDomain:%s",
			u.Host, config.App().Storage.VideoDomain)
		return nil, errs.ErrVideoURLFail
	}
	// 去掉前面的斜线
	path := strings.TrimPrefix(u.Path, "/")
	logx.I(ctx, "GetCosFileInfoByUrl|Path:%s", path)
	objectInfo, err := l.s3.StatObject(ctx, path)
	if err != nil || objectInfo == nil {
		logx.E(ctx, "GetCosFileInfoByUrl|StatObject:%+v err:%v", objectInfo, err)
		return nil, err
	}
	logx.I(ctx, "GetCosFileInfoByUrl|StatObject:%+v", objectInfo)
	return objectInfo, nil
}

func (l *Logic) RecordUserAccessUnCheckQATime(ctx context.Context, robotID, staffID uint64) error {
	return l.qaDao.RecordUserAccessUnCheckQATime(ctx, robotID, staffID)
}
