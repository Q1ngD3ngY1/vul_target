package release

import (
	"context"
	"math"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/client"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/model"
	labelEntity "git.woa.com/adp/kb/kb-config/internal/entity/label"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
	knowClient "git.woa.com/adp/kb/kb-config/internal/rpc"
	"gorm.io/gorm"
)

func (l *Logic) GetReleaseAttributeCount(ctx context.Context, robotID, versionID uint64, name string,
	actions []uint32) (uint64, error) {
	return l.releaseDao.GetReleaseAttributeCount(ctx, robotID, versionID, name, actions, nil)
}

func (l *Logic) GetReleaseAttributeList(ctx context.Context, robotID, versionID uint64, name string,
	actions []uint32, page, pageSize uint32) ([]*releaseEntity.ReleaseAttribute, error) {
	return l.releaseDao.GetReleaseAttributeList(ctx, robotID, versionID, name, actions, page, pageSize, nil)
}

func (l *Logic) ReleaseAttributeLabelUnit(ctx context.Context,
	releaseAttributeLabels []*releaseEntity.ReleaseAttributeLabel) error {
	logx.I(ctx, "ReleaseAttributeLabelUnit:%+v", releaseAttributeLabels)
	if len(releaseAttributeLabels) == 0 {
		return nil
	}
	now := time.Now()
	for _, v := range releaseAttributeLabels {
		if v.ReleaseStatus == releaseEntity.LabelReleaseStatusSuccess {
			continue
		}
		db, err := knowClient.GormClient(ctx, model.TableNameTReleaseAttributeLabel, v.RobotID, 0, []client.Option{}...)
		if err != nil {
			logx.E(ctx, "ReleaseAttributeLabelUnit get GormClient err:%v", err)
			return err
		}
		if err := db.Transaction(func(tx *gorm.DB) error {
			logx.I(ctx, "ReleaseAttributeLabelUnit.ReleaseAttributeLabelProd:%+v", v)
			if err := l.labelDao.ReleaseAttributeLabelProd(ctx, v, tx); err != nil {
				return err
			}
			attributeLabel := &labelEntity.AttributeLabel{
				RobotID:       v.RobotID,
				AttrID:        v.AttrID,
				ID:            v.LabelID,
				ReleaseStatus: releaseEntity.LabelReleaseStatusSuccess,
				NextAction:    releaseEntity.LabelNextActionPublish,
				UpdateTime:    now,
			}
			logx.I(ctx, "ReleaseAttributeLabelUnit.UpdateAttributeLabelStatus:%+v", attributeLabel)
			err = l.labelDao.UpdateAttributeLabelStatus(ctx, attributeLabel, tx)
			if err != nil {
				logx.E(ctx, "UpdateAttributeStatus failed, err:%+v", err)
				return err
			}
			v.ReleaseStatus = releaseEntity.LabelReleaseStatusSuccess
			logx.I(ctx, "ReleaseAttributeLabelUnit.UpdateReleaseAttributeLabelStatus:%+v", v)
			err = l.releaseDao.UpdateReleaseAttributeLabelStatus(ctx, v, tx)
			if err != nil {
				logx.E(ctx, "UpdateReleaseAttributeLabelStatus failed , err:%+v", err)
				return err
			}
			return nil
		}); err != nil {
			logx.E(ctx, "Failed to ReleaseSuccess err:%+v", err)
			return err
		}
	}
	return nil
}

func (l *Logic) ReleaseAttributeUnit(ctx context.Context,
	releaseAttributes []*releaseEntity.ReleaseAttribute) error {
	if len(releaseAttributes) == 0 {
		return nil
	}
	now := time.Now()
	for _, v := range releaseAttributes {
		if v.ReleaseStatus == releaseEntity.LabelReleaseStatusSuccess {
			continue
		}
		db, err := knowClient.GormClient(ctx, model.TableNameTReleaseAttribute, v.RobotID, 0)
		if err != nil {
			logx.E(ctx, "ReleaseAttributeUnit get GormClient err:%v", err)
			return err
		}
		if err := db.Transaction(func(tx *gorm.DB) error {
			logx.I(ctx, "ReleaseAttributeUnit.ReleaseAttributeProd:%+v", v)
			if err := l.labelDao.ReleaseAttributeProd(ctx, v, tx); err != nil {
				return err
			}
			attribute := &labelEntity.Attribute{
				RobotID:       v.RobotID,
				ID:            v.AttrID,
				ReleaseStatus: releaseEntity.LabelReleaseStatusSuccess,
				NextAction:    releaseEntity.LabelNextActionPublish,
				UpdateTime:    now,
			}
			logx.I(ctx, "ReleaseAttributeUnit.UpdateAttributeStatus:%+v", attribute)
			err = l.labelDao.UpdateAttributeStatus(ctx, attribute, tx)
			if err != nil {
				logx.E(ctx, "UpdateAttributeStatus failed, err:%+v", err)
				return err
			}
			v.ReleaseStatus = releaseEntity.LabelReleaseStatusSuccess
			logx.I(ctx, "ReleaseAttributeUnit.UpdateReleaseAttributeStatus:%+v", v)
			err = l.releaseDao.UpdateReleaseAttributeStatus(ctx, v, tx)
			if err != nil {
				logx.E(ctx, "UpdateReleaseAttributeLabelStatus failed , err:%+v", err)
				return err
			}
			return nil
		}); err != nil {
			logx.E(ctx, "Failed to ReleaseSuccess err:%+v", err)
			return err
		}
	}
	return nil
}
func (l *Logic) ReleaseLabelDetailSuccess(ctx context.Context, release *releaseEntity.Release,
	releaseLabelDetails []*releaseEntity.ReleaseLabelDetail) error {
	if len(releaseLabelDetails) == 0 {
		return nil
	}
	labels := make([]*releaseEntity.ReleaseAttribute, 0)
	labelValues := make([]*releaseEntity.ReleaseAttributeLabel, 0)
	for _, v := range releaseLabelDetails {
		labels = append(labels, v.Label)
		labelValues = append(labelValues, v.LabelValues...)
	}
	if err := l.ReleaseAttribute(ctx, labels); err != nil {
		return err
	}
	if err := l.ReleaseAttributeLabel(ctx, labelValues); err != nil {
		return err
	}
	return nil
}

func (l *Logic) ReleaseAttribute(ctx context.Context, releaseAtrributes []*releaseEntity.ReleaseAttribute) error {
	if err := batchReleaseProcess(ctx, releaseAtrributes, l.ReleaseAttributeUnit); err != nil {
		logx.W(ctx, "releaseAttribute err :%v", err)
		return err
	}
	return nil
}

func (l *Logic) ReleaseAttributeLabel(ctx context.Context, releaseAttributeLabels []*releaseEntity.ReleaseAttributeLabel) error {
	if err := batchReleaseProcess(ctx, releaseAttributeLabels, l.ReleaseAttributeLabelUnit); err != nil {
		logx.W(ctx, "releaseAttributeLabel err :%v", err)
		return err
	}
	return nil
}

func (l *Logic) UpdateAttrLabelsCacheProd(ctx context.Context, release *releaseEntity.Release,
	releaseLabelDetails []*releaseEntity.ReleaseLabelDetail) error {
	if len(releaseLabelDetails) == 0 {
		return nil
	}
	attrKeys := make([]string, 0)
	for _, v := range releaseLabelDetails {
		attrKeys = append(attrKeys, v.Label.AttrKey)
	}
	if err := l.labelLogic.UpdateAttrLabelsCacheProd(ctx, release.RobotID, attrKeys); err != nil {
		logx.E(ctx, "发布标签通知knowledge更新缓存失败,versionID:%d,err:%+v", release.ID, err)
		return err
	}
	return nil
}

func (l *Logic) getReleaseLabel(ctx context.Context, appID, versionID uint64) (
	[]*releaseEntity.ReleaseLabelDetail, error) {
	robotID := appID
	zeroTime := time.Time{}
	total, err := l.labelDao.GetWaitReleaseAttributeCount(ctx, robotID, "", nil, zeroTime, zeroTime)
	if err != nil {
		return nil, err
	}
	releaseLabelDetails := make([]*releaseEntity.ReleaseLabelDetail, 0)
	pageSize := 500
	pages := int(math.Ceil(float64(total) / float64(pageSize)))
	for page := 1; page <= pages; page++ {
		list, err := l.labelDao.GetWaitReleaseAttributeList(ctx, robotID, "", nil, uint32(page), uint32(pageSize),
			zeroTime, zeroTime)
		if err != nil {
			return nil, err
		}
		attrIDs := make([]uint64, 0)
		for _, v := range list {
			attrIDs = append(attrIDs, v.ID)
		}
		attrLabelValues, err := l.labelDao.GetWaitReleaseAttributeLables(ctx, robotID, attrIDs)
		if err != nil {
			return nil, err
		}

		mapAttrID2LabelVaues := make(map[uint64][]*labelEntity.AttributeLabel)
		for _, v := range attrLabelValues {
			mapAttrID2LabelVaues[v.AttrID] = append(mapAttrID2LabelVaues[v.AttrID], v)
		}

		for _, v := range list {
			releaseAttribute := &releaseEntity.ReleaseAttribute{
				AttrID:        v.ID,
				BusinessID:    v.BusinessID,
				RobotID:       v.RobotID,
				VersionID:     versionID,
				AttrKey:       v.AttrKey,
				Name:          v.Name,
				ReleaseStatus: v.ReleaseStatus,
				Action:        v.NextAction,
				IsDeleted:     v.IsDeleted,
				DeletedTime:   v.DeletedTime,
				CreateTime:    time.Now(),
				UpdateTime:    time.Now(),
			}
			labelValues := mapAttrID2LabelVaues[v.ID]
			releaseLabelValues := make([]*releaseEntity.ReleaseAttributeLabel, 0)
			for _, lv := range labelValues {
				releaseLabelValues = append(releaseLabelValues, &releaseEntity.ReleaseAttributeLabel{
					BusinessID:    lv.BusinessID,
					VersionID:     versionID,
					RobotID:       v.RobotID,
					AttrID:        v.ID,
					LabelID:       lv.ID,
					Name:          lv.Name,
					SimilarLabel:  lv.SimilarLabel,
					ReleaseStatus: lv.ReleaseStatus,
					Action:        lv.NextAction,
					IsDeleted:     lv.IsDeleted,
					CreateTime:    time.Now(),
					UpdateTime:    time.Now(),
				})
			}
			releaseLabelDetails = append(releaseLabelDetails, &releaseEntity.ReleaseLabelDetail{
				Label:       releaseAttribute,
				LabelValues: releaseLabelValues,
			})
		}
	}
	return releaseLabelDetails, nil
}

func (l *Logic) BatchCreateReleaseLabelDetail(ctx context.Context,
	releaseLabelDetails []*releaseEntity.ReleaseLabelDetail) error {
	releaseLabels := make([]*releaseEntity.ReleaseAttribute, 0)
	releaseLabelValuess := make([]*releaseEntity.ReleaseAttributeLabel, 0)
	for _, v := range releaseLabelDetails {
		releaseLabels = append(releaseLabels, v.Label)
		releaseLabelValuess = append(releaseLabelValuess, v.LabelValues...)
	}
	// 标签值和标签具有强依赖关系，为了能达到失败重试幂等处理，需要先执行完发布标签值添加，再执行发布标签添加
	if err := l.batchCreateReleaseLabelValue(ctx, releaseLabelValuess); err != nil {
		return err
	}
	if err := l.batchCreateReleaseLabel(ctx, releaseLabels); err != nil {
		return err
	}
	return nil
}

func (l *Logic) batchCreateReleaseLabelValue(ctx context.Context,
	releaseLabelValuess []*releaseEntity.ReleaseAttributeLabel) error {
	if err := batchReleaseProcess(ctx, releaseLabelValuess, l.batchCreateReleaseLabelValueUnit); err != nil {
		logx.W(ctx, "batchCreateReleaseLabelValue err :%v", err)
		return err
	}
	return nil
}

func (l *Logic) batchCreateReleaseLabel(ctx context.Context, releaseLabels []*releaseEntity.ReleaseAttribute) error {
	if err := batchReleaseProcess(ctx, releaseLabels, l.batchCreateReleaseLabelUnit); err != nil {
		logx.W(ctx, "batchCreateReleaseLabel err :%v", err)
		return err
	}
	return nil
}

func (l *Logic) isExistReleaseLabel(ctx context.Context, relaseLabel *releaseEntity.ReleaseAttribute) (bool, error) {

	if relaseLabel == nil {
		return false, nil
	}
	filter := &releaseEntity.ReleaseArrtibuteFilter{
		RobotID:   relaseLabel.RobotID,
		VersionID: relaseLabel.VersionID,
		AttrID:    relaseLabel.AttrID,
	}
	db, err := knowClient.GormClient(ctx, model.TableNameTReleaseAttribute, relaseLabel.RobotID, 0)
	if err != nil {
		logx.E(ctx, "isExistReleaseLabel get GormClient err:%v", err)
		return false, err
	}

	return l.releaseDao.IsExistReleaseAttribute(ctx, filter, db)
}

func (l *Logic) isExistReleaseLabelValue(ctx context.Context, releaseLabelValue *releaseEntity.ReleaseAttributeLabel) (
	bool, error) {
	if releaseLabelValue == nil {
		return false, nil
	}
	filter := &releaseEntity.ReleaseArrtibuteLabelFilter{
		RobotID:   releaseLabelValue.RobotID,
		VersionID: releaseLabelValue.VersionID,
		AttrID:    releaseLabelValue.AttrID,
	}
	db, err := knowClient.GormClient(ctx, model.TableNameTReleaseAttributeLabel, releaseLabelValue.RobotID, 0)
	if err != nil {
		logx.E(ctx, "isExistReleaseLabelValue get GormClient err:%v", err)
		return false, err
	}

	return l.releaseDao.IsExistReleaseAttributeLabel(ctx, filter, db)
}

func (l *Logic) batchCreateReleaseLabelUnit(ctx context.Context, releaseLabels []*releaseEntity.ReleaseAttribute) error {
	if len(releaseLabels) == 0 {
		return nil
	}
	for _, v := range releaseLabels {
		isExist, err := l.isExistReleaseLabel(ctx, v)
		if err != nil {
			return err
		}
		if isExist {
			continue
		}

		db, err := knowClient.GormClient(ctx, model.TableNameTReleaseAttribute, v.RobotID, 0)
		if err != nil {
			logx.E(ctx, "batchCreateReleaseLabelUnit get GormClient err:%v", err)
			return err
		}
		if err := db.Transaction(func(tx *gorm.DB) error {
			if err := l.releaseDao.CreateReleaseAttribute(ctx, v, tx); err != nil {
				logx.E(ctx, "batchCreateReleaseLabelUnit | CreateReleaseAttribute err:%v", err)
				return err
			}

			/*
							 `
				        UPDATE
				            t_attribute
				        SET
				            release_status = ?,
				            update_time = ?
				        WHERE
				            robot_id = ? AND id IN (%s)
				    `
			*/

			req := &labelEntity.AttributeFilter{
				RobotId: v.RobotID,
				Ids:     []uint64{v.AttrID},
			}

			updateColumns := map[string]any{
				labelEntity.AttributeTblColReleaseStatus: releaseEntity.LabelReleaseStatusIng,
				labelEntity.AttributeTblColUpdateTime:    time.Now(),
			}

			if err := l.labelDao.BatchUpdateAttributes(ctx, req, updateColumns, tx); err != nil {
				logx.E(ctx, "batchCreateReleaseLabelUnit | BatchUpdateAttributes err:%v", err)
				return err
			}
			return nil
		}); err != nil {
			logx.E(ctx, "Failed to batchCreateReleaseLabelUnit. err:%+v", err)
			return err
		}

	}
	return nil
}

func (l *Logic) batchCreateReleaseLabelValueUnit(ctx context.Context,
	releaseLabelValues []*releaseEntity.ReleaseAttributeLabel) error {
	if len(releaseLabelValues) == 0 {
		return nil
	}
	for _, v := range releaseLabelValues {
		isExist, err := l.isExistReleaseLabelValue(ctx, v)
		if err != nil {
			return err
		}
		if isExist {
			continue
		}
		db, err := knowClient.GormClient(ctx, model.TableNameTReleaseAttribute, v.RobotID, 0)
		if err != nil {
			logx.E(ctx, "batchCreateReleaseLabelValueUnit get GormClient err:%v", err)
			return err
		}
		if err := db.Transaction(func(tx *gorm.DB) error {
			if err := l.releaseDao.CreateReleaseAttributeLabel(ctx, v, tx); err != nil {
				logx.E(ctx, "batchCreateReleaseLabelUnit | CreateReleaseAttributeLabel err:%v", err)
				return err
			}

			/*
							`
				        UPDATE
				            t_attribute_label
				        SET
				            release_status = ?,
				            update_time = ?
				        WHERE
				            robot_id = ? AND attr_id = ? AND id IN (%s)
				    `
			*/

			req := &labelEntity.AttributeLabelFilter{
				RobotId: v.RobotID,
				AttrId:  v.AttrID,
				Ids:     []uint64{v.LabelID},
			}

			updateColumns := map[string]any{
				labelEntity.AttributeTblColReleaseStatus: releaseEntity.LabelReleaseStatusIng,
				labelEntity.AttributeTblColUpdateTime:    time.Now(),
			}

			if err := l.labelDao.BatchUpdateAttributeLabels(ctx, req, updateColumns, tx); err != nil {
				logx.E(ctx, "batchCreateReleaseLabelUnit | BatchUpdatBatchUpdateAttributeLabelseAttributes err:%v", err)
				return err
			}
			return nil
		}); err != nil {
			logx.E(ctx, "Failed to batchCreateReleaseLabelValueUnit. err:%+v", err)
			return err
		}
	}
	return nil
}

func (l *Logic) GetReleaseLabelByVersionID(ctx context.Context, robotID, versionID uint64) (
	[]*releaseEntity.ReleaseLabelDetail, error) {
	db, err := knowClient.GormClient(ctx, model.TableNameTReleaseAttributeLabel, robotID, 0)
	if err != nil {
		return nil, err
	}
	total, err := l.releaseDao.GetReleaseAttributeCount(ctx, robotID, versionID, "", nil, db)
	if err != nil {
		return nil, err
	}
	releaseLabelDetails := make([]*releaseEntity.ReleaseLabelDetail, 0)
	pageSize := 500
	pages := int(math.Ceil(float64(total) / float64(pageSize)))
	for page := 1; page <= pages; page++ {
		list, err := l.releaseDao.GetReleaseAttributeList(ctx, robotID, versionID, "", nil, uint32(page),
			uint32(pageSize), db)
		if err != nil {
			return nil, err
		}
		attrIDs := make([]uint64, 0)
		for _, v := range list {
			attrIDs = append(attrIDs, v.AttrID)
		}
		mapAttrID2LabelVaues, err := l.releaseDao.GetReleaseAttributeLabels(ctx, robotID, versionID, attrIDs, db)
		if err != nil {
			return nil, err
		}
		for _, v := range list {
			releaseLabelDetails = append(releaseLabelDetails, &releaseEntity.ReleaseLabelDetail{
				Label:       v,
				LabelValues: mapAttrID2LabelVaues[v.AttrID],
			})
		}
	}
	return releaseLabelDetails, nil
}
