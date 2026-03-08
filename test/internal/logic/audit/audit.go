package audit

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"git.woa.com/adp/common/x/gox/ptrx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/entity/finance"
	"git.woa.com/adp/kb/kb-config/internal/logic/common"
	"git.woa.com/adp/kb/kb-config/pkg/i18nkey"
	"gorm.io/gorm"

	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/encodingx/jsonx"
	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/mysqlquery"
	async "git.woa.com/adp/kb/kb-config/internal/async/scheduler"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
	segEntity "git.woa.com/adp/kb/kb-config/internal/entity/segment"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"git.woa.com/adp/kb/kb-config/internal/util/idgen"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
)

// DescribeQaAuditFailStatus 获取问答审核详情
func (l *Logic) DescribeQaAuditFailStatus(ctx context.Context, qa *qaEntity.DocQA, sims []*qaEntity.SimilarQuestion,
	auditFailList []*releaseEntity.AuditFailList, uin string, appBizID uint64) error {
	stAndItems, err := releaseEntity.GetAuditStatusAndItemsByFailList(ctx, auditFailList)
	if err != nil {
		logx.E(ctx, "getAuditStatusAndItems1 err: %+v", err)
		return err
	}
	if len(stAndItems) == 0 {
		logx.E(ctx, "未找到失败的子审核数据,related qaID: %d", qa.ID)
		return fmt.Errorf("未找到失败的子审核数据,related qaID: %d", qa.ID)
	}
	var imageRedictedURLs = make([]string, 0)
	for _, image := range util.ExtractImagesFromMarkdown(qa.Answer) {
		if !config.IsInWhiteList(uin, appBizID, config.GetWhitelistConfig().QaURLWhiteList) {
			safe, err := util.IsSafeURL(ctx, image)
			if err != nil || !safe {
				logx.W(ctx, "getQaAuditParams|imageUrl|safe:%v|err:%v", safe, err)
				return errs.ErrFileUrlFail
			}
		}
		url := getRedirectedURL(image)
		imageRedictedURLs = append(imageRedictedURLs, url)
	}
	var videos []*qaEntity.DocQAFile
	if config.VideoAuditSwitch() {
		videos, err = util.AuditQaVideoURLs(ctx, qa.Answer)
		if err != nil {
			logx.E(ctx, "ExtractVideoURLs err: %+v", err)
			return err
		}
	}
	qa = getQaAuditFailDetails(qa, stAndItems, imageRedictedURLs, videos)
	for i, v := range sims {
		sims[i].ReleaseStatus = qaEntity.QAReleaseStatusAuditing
		for _, v1 := range stAndItems {
			if v1.AuditItem.Typ == releaseEntity.AuditTypePlainText && v1.AuditItem.Content == v.Question {
				// 相似问审核失败
				sims[i].ReleaseStatus = qaEntity.QAReleaseStatusAuditNotPass
				break
			}
		}
	}
	return nil
}

func (l *Logic) GetBizAuditStatusByRelateIDs(ctx context.Context, robotID, corpID uint64,
	relateIDs []uint64) (map[uint64]releaseEntity.AuditStatus, error) {
	if len(relateIDs) == 0 {
		return nil, errs.ErrAuditNotFound
	}

	auditRelateIDs, err := l.releaseDao.GetBizAuditStatusByRelateIDs(ctx, corpID, robotID, relateIDs)
	if err != nil {
		logx.E(ctx, "Get Latest status of audit based on RelateIDs error. err:%+v", err)
		return nil, err
	}
	audits := make(map[uint64]releaseEntity.AuditStatus)
	for _, v := range auditRelateIDs {
		audits[v.RelateID] = releaseEntity.AuditStatus{Status: v.Status}
	}
	return audits, nil

}

// GetParentAuditsByParentRelateID 通过父关联ID获取已存在的父审核数据
func (l *Logic) GetParentAuditsByParentRelateID(ctx context.Context, p entity.AuditSendParams, idStart uint64,
	limit int) ([]*releaseEntity.Audit, error) {
	/*
		`
		SELECT
			%s
		FROM
		    t_audit
		WHERE
			corp_id = ?
			AND robot_id = ?
			AND parent_relate_id = ?
		    AND type = ?
			AND parent_id = 0
			AND id > ?
			ORDER BY id ASC
			LIMIT ?
	*/

	filter := &releaseEntity.AuditFilter{
		CorpID:          p.CorpID,
		RobotID:         p.RobotID,
		ParentRelatedID: p.ParentRelateID,
		ParentID:        ptrx.Uint64(0),
		Type:            p.Type,
		IDMore:          idStart,
		Limit:           limit,

		OrderByField: "id",
		OrderByType:  "ASC",
	}

	audits, err := l.releaseDao.GetParentAuditsByParentRelateID(ctx, filter)
	if err != nil {
		logx.E(ctx, "GetParentAuditsByParentRelateID failed. err:%+v", err)
		return nil, err
	}
	return audits, nil
}

// GetParentAuditIDsByParentRelateID 通过父关联ID获取已存在的父审核ID列表
func (l *Logic) GetParentAuditIDsByParentRelateID(ctx context.Context, p entity.AuditCheckParams, idStart uint64,
	limit int) ([]uint64, error) {
	filter := &releaseEntity.AuditFilter{
		CorpID:          p.CorpID,
		RobotID:         p.RobotID,
		ParentRelatedID: p.ParentRelateID,
		ParentID:        ptrx.Uint64(0),
		Type:            p.Type,
		IDMore:          idStart,
		Limit:           limit,

		OrderByField: "id",
		OrderByType:  "ASC",
	}

	audits, err := l.releaseDao.GetAuditList(ctx, []string{}, filter)
	if err != nil {
		logx.E(ctx, "GetParentAuditsByParentRelateID failed. err:%+v", err)
		return nil, err
	}
	if len(audits) == 0 {
		return nil, nil
	}

	ids := make([]uint64, 0, len(audits))
	for _, audit := range audits {
		ids = append(ids, audit.ID)
	}
	return ids, nil
}

// GetBizAuditStatusStat 按status统计子审核数据
func (l *Logic) GetBizAuditStatusStat(ctx context.Context, id, corpID, robotID uint64) (map[uint32]*releaseEntity.AuditStatusStat,
	error) {
	/*
		`
		SELECT
			status,count(*) total
		FROM
		    t_audit
		WHERE
			corp_id = ?
			AND robot_id = ?
		    AND parent_id = ?
		GROUP BY
		    status
	*/
	statList, err := l.releaseDao.GetBizAuditStatusStat(ctx, id, corpID, robotID)
	if err != nil {
		logx.E(ctx, "GetBizAuditStatusStat failed. err:%+v", err)
		return nil, err
	}
	stat := make(map[uint32]*releaseEntity.AuditStatusStat, 0)
	for _, item := range statList {
		stat[item.Status] = item
	}
	return stat, nil
}

// GetLatestParentAuditFailByRelateID 根据 relateID 获取最后一次父审核数据
func (l *Logic) GetLatestParentAuditFailByRelateID(ctx context.Context, corpID, robotID, releateID uint64,
	auditType uint32) (*releaseEntity.AuditParent, error) {
	/*
		`
		SELECT
			id,status
		FROM
			t_audit
		WHERE
			corp_id = ?
			AND robot_id = ?
			AND relate_id = ?
			AND type = ?
			AND parent_id = 0
		ORDER BY
			id desc
		LIMIT 1
	*/
	filter := &releaseEntity.AuditFilter{
		CorpID:    corpID,
		RobotID:   robotID,
		RelatedID: int64(releateID),
		ParentID:  ptrx.Uint64(0),
		Type:      auditType,

		Limit: 1,

		OrderByField: "id",
		OrderByType:  "DESC",
	}

	audits, err := l.releaseDao.GetAuditByFilter(ctx, []string{releaseEntity.AuditTblColId, releaseEntity.AuditTblColStatus}, filter)
	if err != nil {
		logx.E(ctx, "GetLatestParentAuditFailByRelateID failed. err:%+v", err)
		return nil, err
	}
	return &releaseEntity.AuditParent{
		ID:     audits.ID,
		Status: audits.Status,
	}, nil
}

// GetLatestAuditFailListByRelateID 根据 relateID 获取最后一次子审核数据
func (l *Logic) GetLatestAuditFailListByRelateID(ctx context.Context, corpID, robotID, releateID uint64,
	auditType uint32, isAppeal bool) ([]*releaseEntity.AuditFailList, error) {
	auditParent, err := l.GetLatestParentAuditFailByRelateID(ctx, corpID, robotID, releateID, auditType)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, errs.ErrAppealNotFound
	}
	if err != nil {
		return nil, err
	}

	/*
			`
				SELECT
					id, type, params, parent_id, relate_id
				FROM
				    t_audit
				WHERE
					corp_id = ?
					AND robot_id = ?
		            AND parent_id = ?
				    AND type = ?
					AND status IN ( ?, ? )
			`
	*/
	filter := &releaseEntity.AuditFilter{
		CorpID:     corpID,
		RobotID:    robotID,
		ParentID:   ptrx.Uint64(auditParent.ID),
		Type:       auditType,
		StatusList: []uint32{releaseEntity.AuditStatusFail, releaseEntity.AuditStatusAppealFail},
	}

	if isAppeal {
		filter.StatusList = []uint32{releaseEntity.AuditStatusFail, releaseEntity.AuditStatusTimeoutFail}
	}

	selectColumns := []string{releaseEntity.AuditTblColId, releaseEntity.AuditTblColType, releaseEntity.AuditTblColParams,
		releaseEntity.AuditTblColParentId, releaseEntity.AuditTblColRelateId}

	audits, err := l.releaseDao.GetAuditList(ctx, selectColumns, filter)

	if err != nil {
		logx.E(ctx, "GetLatestAuditFailListByRelateID failed. err:%+v", err)
		return nil, err
	}

	if len(audits) == 0 {
		return nil, nil
	}

	auditFailList := make([]*releaseEntity.AuditFailList, 0)

	for _, audit := range audits {
		auditFailList = append(auditFailList, &releaseEntity.AuditFailList{
			ID:       audit.ID,
			Type:     audit.Type,
			Params:   audit.Params,
			ParentID: audit.ParentID,
			RelateID: audit.RelateID,
		})
	}

	return auditFailList, nil
}

// getAuditStatus 获取审核状态
func getAuditStatus(pass, isAppeal bool) uint32 {
	if pass {
		if isAppeal {
			return releaseEntity.AuditStatusAppealSuccess
		}
		return releaseEntity.AuditStatusPass
	} else {
		if isAppeal {
			return releaseEntity.AuditStatusAppealFail
		}
		return releaseEntity.AuditStatusFail
	}
}

// CreateParentAuditCheck 创建审核回调check任务
func (l *Logic) CreateParentAuditCheck(ctx context.Context, parent *releaseEntity.Audit) error {
	return async.NewAuditCheckTask(ctx, parent.RobotID, entity.AuditCheckParams{
		AuditID:        parent.ID,
		ParentRelateID: 0,
	})
}

// CreateParentAuditCheckWithOriginDocBizID 创建审核回调check任务(干预使用)
func (l *Logic) CreateParentAuditCheckWithOriginDocBizID(ctx context.Context, parent *releaseEntity.Audit, originDocBizID uint64) error {
	originDoc, err := l.docLogic.GetDocByBizID(ctx, originDocBizID, parent.RobotID)
	if err != nil {
		logx.E(ctx, "CreateParentAuditCheckWithOriginDocBizID|GetDocByBizID|err:%+v", err)
		return errs.ErrDocNotFound
	}
	return async.NewAuditCheckTask(ctx, parent.RobotID, entity.AuditCheckParams{
		AuditID:        parent.ID,
		ParentRelateID: 0,
		OriginDocID:    originDoc.ID,
	})
}

// CreateParentAuditCheckForExcel2Qa 批量导入问答场景下，创建审核回调check任务
func (l *Logic) CreateParentAuditCheckForExcel2Qa(ctx context.Context, p entity.AuditSendParams) error {
	return async.NewAuditCheckTask(ctx, p.RobotID, entity.AuditCheckParams{
		AuditID:        0,
		CorpID:         p.CorpID,
		StaffID:        p.StaffID,
		RobotID:        p.RobotID,
		Type:           p.Type,
		ParentRelateID: p.ParentRelateID,
	})
}

// BatchCreateReleaseAudit 批量创建发布审核数据
func (l *Logic) BatchCreateReleaseAudit(ctx context.Context, parent *releaseEntity.Audit, audits []*releaseEntity.Audit,
	p entity.AuditSendParams) ([]*releaseEntity.Audit, error) {
	isNeedNotice, noticeType, pageID, subject := p.GetAuditNotice()

	db := l.releaseDao.MysqlQuery().TAudit.WithContext(ctx).UnderlyingDB()

	err := db.Transaction(func(tx *gorm.DB) error {
		if err := l.sendNotice(ctx, isNeedNotice, parent, noticeType, pageID, subject); err != nil {
			return err
		}
		length := len(audits)
		pageSize := 200
		pages := int(math.Ceil(float64(length) / float64(pageSize)))
		for i := 0; i < pages; i++ {
			start := pageSize * i
			end := pageSize * (i + 1)
			if end > length {
				end = length
			}
			tmpAudits := audits[start:end]
			if err := l.releaseDao.BatchCreateAudit(ctx, tmpAudits, tx); err != nil {
				logx.E(ctx, "Failed to batch create audit data. err:%+v", err)
				return err
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}
	return audits, nil
}

// BatchCreateAudit 批量创建审核数据
func (l *Logic) BatchCreateAudit(ctx context.Context, parent *releaseEntity.Audit, appDB *entity.App,
	p entity.AuditSendParams) ([]*releaseEntity.Audit, error) {
	if parent == nil {
		logx.E(ctx, "Failed to batch create audit because parent is nil")
		return nil, errs.ErrAuditNotFound
	}
	logx.I(ctx, "BatchCreateAudit with parent:%+v, appDB:%+v, p:%+v", parent, appDB, p)
	isNeedNotice, noticeType, pageID, subject := p.GetAuditNotice()
	audits, err := l.getAudits(ctx, parent, appDB, p)
	if err != nil {
		return nil, err
	}
	err = l.releaseDao.MysqlQuery().TAudit.WithContext(ctx).UnderlyingDB().Transaction(func(tx *gorm.DB) error {
		if err := l.sendNotice(ctx, isNeedNotice, parent, noticeType, pageID, subject); err != nil {
			return err
		}

		length := len(audits)
		pageSize := 100
		pages := int(math.Ceil(float64(length) / float64(pageSize)))
		for i := 0; i < pages; i++ {
			start := pageSize * i
			end := pageSize * (i + 1)
			if end > length {
				end = length
			}
			tmpAudits := audits[start:end]
			if err := l.releaseDao.BatchCreateAudit(ctx, tmpAudits, tx); err != nil {
				logx.E(ctx, "Failed to batch create audit data. err:%+v", err)
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return audits, nil
}

// TestUpdateAuditStatusByParentID 根据父审核id更新子审核状态，仅用于测试
func (l *Logic) TestUpdateAuditStatusByParentID(ctx context.Context, parentAudit *releaseEntity.Audit) error {
	/*
		`
			UPDATE
				t_audit
			SET
			    update_time = :update_time,
			    status = :status
			WHERE
			    corp_id = :corp_id
				AND robot_id = :robot_id
				AND parent_id = :id
				AND type = :type
		`
	*/
	auditFilter := &releaseEntity.AuditFilter{
		CorpID:   parentAudit.CorpID,
		RobotID:  parentAudit.RobotID,
		ParentID: ptrx.Uint64(parentAudit.ID),
		Type:     parentAudit.Type,
	}

	selectColumns := []string{
		releaseEntity.AuditTblColUpdateTime,
		releaseEntity.AuditTblColStatus,
	}

	if err := l.releaseDao.MysqlQuery().TAudit.WithContext(ctx).UnderlyingDB().Transaction(func(tx *gorm.DB) error {
		if _, err := l.releaseDao.UpdateAudit(ctx, selectColumns, auditFilter, parentAudit, tx); err != nil {

			return err
		}
		return nil

	}); err != nil {
		logx.E(ctx, "Failed to TestUpdateAuditStatusByParentID. err:%+v", err)
		return err
	}

	return nil
}

// UpdateAuditStatusByParentID 根据父审核id更新子审核状态
func (l *Logic) UpdateAuditStatusByParentID(ctx context.Context, parentAudit *releaseEntity.Audit, limit int) error {
	if parentAudit == nil {
		return nil
	}
	/*
		`
			UPDATE
				t_audit
			SET
			    update_time = :update_time,
			    status = :status,
				message = :message
			WHERE
			    corp_id = :corp_id
				AND robot_id = :robot_id
				AND parent_id = :id
				AND type = :type
				AND status = 3
			%s
		`
	*/

	auditFilter := &releaseEntity.AuditFilter{
		CorpID:   parentAudit.CorpID,
		RobotID:  parentAudit.RobotID,
		ParentID: ptrx.Uint64(parentAudit.ID),
		Type:     parentAudit.Type,
		Status:   releaseEntity.AuditStatusSendSuccess, // 3
	}

	selectColumns := []string{
		releaseEntity.AuditTblColUpdateTime,
		releaseEntity.AuditTblColStatus,
		releaseEntity.AuditTblColMessage,
	}

	for {

		if limit > 0 {
			auditFilter.Limit = limit
		}

		if rowsAffected, err := l.releaseDao.UpdateAudit(ctx, selectColumns, auditFilter, parentAudit, nil); err != nil {
			logx.E(ctx, "Failed to UpdateAuditStatusByParentID. err:%+v", err)
			return err
		} else if limit > 0 {
			if rowsAffected < int64(limit) {
				logx.I(ctx, "UpdateAuditStatusByParentID rowsAffected:%d < limit:%d", rowsAffected, limit)
				break
			}
		}
	}

	return nil
}

// CreateQaAuditForExcel2Qa 批量导入问答时，创建问答送审任务
func (l *Logic) CreateQaAuditForExcel2Qa(ctx context.Context, doc *docEntity.Doc) error {
	logx.I(ctx, "CreateQaAuditForExcel2Qa with doc:%+v", doc)
	p := entity.AuditSendParams{
		CorpID: doc.CorpID, StaffID: doc.StaffID, RobotID: doc.RobotID, Type: releaseEntity.AuditBizTypeQa,
		RelateID: 0, EnvSet: contextx.Metadata(ctx).EnvSet(), ParentRelateID: doc.ID,
	}
	qaids, err := l.qaLogic.GetQAIDsByOriginDocID(ctx, doc.RobotID, doc.ID) // 通过来源文档id获取问答id列表
	if err != nil {
		logx.E(ctx, "Failed to  create audit for excel2qa with params:%+v, err:%+v", p, err)
		return err
	}
	if len(qaids) == 0 {
		logx.I(ctx, "Create audit for excel2qa with params:%+v, len(qaids)=0", p)
		return nil
	}
	if err := l.createAuditForExcel2Qa(ctx, p, qaids); err != nil {
		logx.E(ctx, "Failed to create qa audit for excel2qa when import qa,r:%+v", err)
		return err
	}
	return nil
}

// createAuditForExcel2Qa 创建批量送审
func (l *Logic) createAuditForExcel2Qa(ctx context.Context, p entity.AuditSendParams, qaIDs []uint64) error {
	logx.D(ctx, "Create audit for excel2qa with params:%+v", p)
	if !config.AuditSwitch() {
		return nil
	}

	now := time.Now()
	var auditList []*releaseEntity.Audit
	for _, qaid := range qaIDs {
		audit := releaseEntity.NewParentAudit(p.CorpID, p.RobotID, p.StaffID, qaid, p.ParentRelateID, p.Type)
		audit.BusinessID = idgen.GetId()
		audit.UpdateTime = now
		audit.CreateTime = now
		auditList = append(auditList, audit)
	}
	err := l.releaseDao.MysqlQuery().TAudit.WithContext(ctx).UnderlyingDB().Transaction(func(tx *gorm.DB) error {
		length := len(auditList)
		pageSize := 200
		pages := int(math.Ceil(float64(length) / float64(pageSize)))
		for i := 0; i < pages; i++ {
			start := pageSize * i
			end := pageSize * (i + 1)
			if end > length {
				end = length
			}
			tmpAudits := auditList[start:end]
			if err := l.releaseDao.BatchCreateAudit(ctx, tmpAudits, tx); err != nil {
				logx.E(ctx, "Failed to batch create audit data. err:%+v", err)
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	p.ParentAuditBizID = 0 // 批量导入问答时，params里面的父审核id无意义，使用的是ParentRelateID
	return async.NewAuditSendTask(ctx, p.RobotID, p)
}

func (l *Logic) getAudits(ctx context.Context, parent *releaseEntity.Audit, appDB *entity.App,
	p entity.AuditSendParams) ([]*releaseEntity.Audit, error) {
	logx.I(ctx, "getAudits with parent:%+v, appDB:%+v, p:%+v (type:%d)", parent, appDB, p, p.Type)
	auditItems := make([]*releaseEntity.AuditItem, 0)
	switch p.Type {
	case releaseEntity.AuditBizTypeBareAnswer:
		auditItems = append(
			auditItems,
			releaseEntity.NewPlainTextAuditItem(appDB.PrimaryId, releaseEntity.AuditSourceBareAnswer, appDB.BareAnswerInAudit, p.EnvSet),
		)
		for _, image := range util.ExtractImagesFromMarkdown(appDB.BareAnswerInAudit) {
			auditItems = append(
				auditItems,
				releaseEntity.NewPictureAuditItem(appDB.PrimaryId, releaseEntity.AuditSourceBareAnswer, image, p.EnvSet,

					l.s3.GetObjectETag(ctx, image)),
			)
		}
	case releaseEntity.AuditBizTypeDoc:
		tmpParams, err := l.getFileAuditParams(ctx, p)
		if err != nil {
			return nil, err
		}
		auditItems = append(auditItems, tmpParams...)
	case releaseEntity.AuditBizTypeRelease:
		tmpParams, err := l.getReleaseAuditParams(ctx, p)
		if err != nil {
			return nil, err
		}
		auditItems = append(auditItems, tmpParams...)
	case releaseEntity.AuditBizTypeRobotProfile:
		tmpParams, err := l.getRobotProfileAuditParams(ctx, p)
		if err != nil {
			return nil, err
		}
		auditItems = append(auditItems, tmpParams...)
	case releaseEntity.AuditBizTypeQa:
		// TOOD: refact by calling admin rpc interface
		corp, err := l.rpc.PlatformAdmin.DescribeCorpByPrimaryId(ctx, appDB.CorpPrimaryId)
		if err != nil {
			return nil, err
		}
		tmpParams, err := l.getQaAuditParams(ctx, p.EnvSet, parent, corp.Uin, appDB.BizId)
		if err != nil {
			return nil, err
		}
		auditItems = append(auditItems, tmpParams...)
	case releaseEntity.AuditBizTypeDocName:
		tmpParams, err := l.getDocNameAuditParams(ctx, p.EnvSet, parent)
		if err != nil {
			return nil, err
		}
		auditItems = append(auditItems, tmpParams...)
	case releaseEntity.AuditBizTypeDocSegment:
		tmpParams, err := l.getDocSegmentAuditParams(ctx, p.EnvSet, parent, p.OriginDocBizID)
		if err != nil {
			return nil, err
		}
		auditItems = append(auditItems, tmpParams...)
	case releaseEntity.AuditBizTypeDocTableSheet:
		tmpParams, err := l.getDocTableSheetAuditParams(ctx, p.EnvSet, parent, p.OriginDocBizID)
		if err != nil {
			return nil, err
		}
		auditItems = append(auditItems, tmpParams...)
	default:
		return nil, fmt.Errorf("unknown audit biz type %d", p.Type)
	}
	audits := releaseEntity.NewAudits(ctx, parent, auditItems)
	now := time.Now()
	for _, audit := range audits {
		audit.BusinessID = idgen.GetId()
		audit.UpdateTime = now
		audit.CreateTime = now
	}
	return audits, nil
}

func (l *Logic) UpdateDocAuditResult(ctx context.Context, doc *docEntity.Doc, audit *releaseEntity.Audit,
	auditsMap map[uint32][]*releaseEntity.AuditStatusSourceList, pass, isAppeal bool) (*docEntity.Doc, *releaseEntity.Audit, error) {
	now := time.Now()
	audit.Status = l.getAuditStatus(pass, isAppeal)
	audit.UpdateTime, doc.UpdateTime = now, now
	doc.Status = docEntity.DocStatusCreatingIndex
	doc.AuditFlag = docEntity.AuditFlagDone
	if !pass {

		childAudits, err := l.GetChildAuditsByParentID(ctx, audit.CorpID, audit.RobotID, audit.ID)
		if err != nil {
			return nil, nil, err
		}
		contentFailed := false
		nameFailed := false
		segmentFailed := false
		segmentPictureFailed := false
		for _, ca := range childAudits {
			// 解析params
			p := releaseEntity.AuditItem{}
			if err := jsonx.UnmarshalFromString(ca.Params, &p); err != nil {
				continue
			}
			if p.Source == releaseEntity.AuditSourceDocName && (ca.Status == releaseEntity.AuditStatusFail || ca.Status == releaseEntity.AuditStatusAppealFail) {
				nameFailed = true
			}
			if p.Source == releaseEntity.AuditSourceDoc && (ca.Status == releaseEntity.AuditStatusFail || ca.Status == releaseEntity.AuditStatusAppealFail) {
				contentFailed = true
			}
			// todo 缺少sheet类型
			if p.Source == releaseEntity.AuditSourceDocSegment && (ca.Status == releaseEntity.AuditStatusFail || ca.Status == releaseEntity.AuditStatusAppealFail) {
				segmentFailed = true
			}
			if p.Source == releaseEntity.AuditSourceDocSegmentPic && (ca.Status == releaseEntity.AuditStatusFail || ca.Status == releaseEntity.AuditStatusAppealFail) {
				segmentPictureFailed = true
			}
		}
		if isAppeal {
			// doc.Message = "审核失败，请修改后重新导入"
			// doc.Status = model.DocStatusAppealFailed
			if audit.Type == releaseEntity.AuditBizTypeDocSegment || audit.Type == releaseEntity.AuditBizTypeDocTableSheet {
				if (segmentFailed && segmentPictureFailed) || (contentFailed && segmentPictureFailed) {
					doc.Message = i18nkey.KeyDocumentInterventionTextImageReviewFailed
					doc.Status = docEntity.DocStatusAppealFailed
				} else if segmentPictureFailed {
					doc.Message = i18nkey.KeyDocumentInterventionImageReviewFailed
					doc.Status = docEntity.DocStatusAppealFailed
				} else if segmentFailed || contentFailed {
					doc.Message = i18nkey.KeyDocumentInterventionTextReviewFailed
					doc.Status = docEntity.DocStatusAppealFailed
				}
			} else {
				if contentFailed && nameFailed {
					doc.Message = i18nkey.KeyFileNameAndContentReviewFailed
					doc.Status = docEntity.DocStatusAppealFailed
				} else if nameFailed {
					doc.Message = i18nkey.KeyFileNameReviewFailed
					doc.Status = docEntity.DocStatusDocNameAppealFail
				} else {
					doc.Message = i18nkey.KeyFileContentReviewFailed
					doc.Status = docEntity.DocStatusAppealFailed
				}
			}
		} else if _, ok := auditsMap[releaseEntity.AuditStatusTimeoutFail]; ok {
			audit.Status = releaseEntity.AuditStatusTimeoutFail
			doc.Status = docEntity.DocStatusAuditFail
			if audit.Type == releaseEntity.AuditBizTypeDocSegment || audit.Type == releaseEntity.AuditBizTypeDocTableSheet {
				// todo 重试/申诉 功能待开发
				doc.Message = i18nkey.KeyDocumentReviewTimeout
			} else {
				doc.Message = i18nkey.KeyFileReviewTimeout
			}
		} else {
			// doc.Message = "审核失败，请修改后重新导入，或点击 人工申诉"
			if audit.Type == releaseEntity.AuditBizTypeDocSegment || audit.Type == releaseEntity.AuditBizTypeDocTableSheet {
				if (segmentFailed && segmentPictureFailed) || (contentFailed && segmentPictureFailed) {
					doc.Message = i18nkey.KeyDocumentInterventionTextImageReviewFailed
					doc.Status = docEntity.DocStatusAuditFail
				} else if segmentPictureFailed {
					doc.Message = i18nkey.KeyDocumentInterventionImageReviewFailed
					doc.Status = docEntity.DocStatusAuditFail
				} else if segmentFailed || contentFailed {
					doc.Message = i18nkey.KeyDocumentInterventionTextReviewFailed
					doc.Status = docEntity.DocStatusAuditFail
				}
			} else {
				if contentFailed && nameFailed {
					doc.Message = i18nkey.KeyFileNameAndContentReviewFailedWithOption
					doc.Status = docEntity.DocStatusDocNameAndContentAuditFail
				} else if nameFailed {
					doc.Message = i18nkey.KeyFileNameReviewFailedWithOption
					doc.Status = docEntity.DocStatusImportDocNameAuditFail
				} else {
					doc.Message = i18nkey.KeyFileContentReviewFailedWithOption
					doc.Status = docEntity.DocStatusAuditFail
				}
			}
		}
	}

	if err := l.docLogic.UpdateDocAuditResult(ctx, doc); err != nil {
		logx.E(ctx, "Failed to UpdateDocAuditResult. err:%+v ", err)
		return nil, nil, err
	}
	return doc, audit, nil
}

// UpdateAuditStatus 更新审核状态
func (l *Logic) UpdateAuditStatus(ctx context.Context, audit *releaseEntity.Audit) error {
	/*
		`
			UPDATE
				t_audit
			SET
			    update_time = :update_time,
			    retry_times = :retry_times,
			    status = :status,
			    message = :message
			WHERE
			    id = :id
		`
	*/
	auditFilter := &releaseEntity.AuditFilter{
		ID: audit.ID,
	}

	selectColumns := []string{
		releaseEntity.AuditTblColUpdateTime,
		releaseEntity.AuditTblColRetryTimes,
		releaseEntity.AuditTblColStatus,
		releaseEntity.AuditTblColMessage,
	}

	if err := l.releaseDao.MysqlQuery().TAudit.WithContext(ctx).UnderlyingDB().Transaction(func(tx *gorm.DB) error {
		if _, err := l.releaseDao.UpdateAudit(ctx, selectColumns, auditFilter, audit, tx); err != nil {
			logx.E(ctx, "Failed to Update Audit status err:%+v", err)
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

// UpdateAuditStatus 更新审核状态
func (l *Logic) UpdateAuditStatusOnly(ctx context.Context, audit *releaseEntity.Audit) error {
	/*
		`
			UPDATE
				t_audit
			SET
			    update_time = :update_time,
			    status = :status,
			WHERE
			    id = :id
		`
	*/
	auditFilter := &releaseEntity.AuditFilter{
		IDs: []uint64{audit.ID},
	}

	selectColumns := []string{
		releaseEntity.AuditTblColUpdateTime,
		releaseEntity.AuditTblColStatus,
	}

	if _, err := l.releaseDao.UpdateAudit(ctx, selectColumns, auditFilter, audit, nil); err != nil {
		logx.E(ctx, "Failed to Update Audit status only err:%+v", err)
		return err
	}
	return nil
}

// AuditDoc 文档审核或者申诉回调处理函数，audit是父审核任务
func (l *Logic) AuditDoc(ctx context.Context, audit *releaseEntity.Audit, pass, isAppeal bool, rejectReason string) error {
	docID := audit.RelateID
	intervene := false
	if audit.Type == releaseEntity.AuditBizTypeDocSegment || audit.Type == releaseEntity.AuditBizTypeDocTableSheet {
		intervene = true
	}
	doc, err := l.docLogic.GetDocByID(ctx, docID, audit.RobotID)
	if err != nil {
		return err
	}
	if doc == nil {
		return errs.ErrDocNotFound
	}
	if doc.HasDeleted() {
		audit.UpdateTime = time.Now()
		audit.Status = l.getAuditStatus(true, isAppeal) // 直接把审核状态改成成功
		_ = l.UpdateAuditStatus(ctx, audit)
		logx.I(ctx, "文档已经被删除，不再走审核逻辑，doc:%+v", doc)
		return nil
	}
	if !isAppeal && !doc.NeedAudit() {
		return nil
	}
	auditsMap, err := l.bizAuditStatusMap(ctx, audit)
	if err != nil || len(auditsMap) == 0 {
		return errs.ErrAuditNotFound
	}
	isNeedCharSizeNotice := false
	app, err := l.rpc.AppAdmin.DescribeAppByPrimaryId(ctx, doc.RobotID)
	if err != nil {
		return err
	}
	if err = l.releaseDao.MysqlQuery().Transaction(func(tx *mysqlquery.Query) error {
		doc, audit, err = l.UpdateDocAuditResult(ctx, doc, audit, auditsMap, pass, isAppeal)
		if err != nil {
			return err
		}
		if !pass {
			return l.sendNoticeAndUpdateAuditStatusIfDocAuditFail(ctx, doc, audit, auditsMap, isAppeal,
				rejectReason)
		}
		// 下面都是审核通过的处理流程
		if err = l.UpdateAuditStatus(ctx, audit); err != nil {
			logx.E(ctx, "Failed to update audit status err:%+v", err)
			return err
		}

		if err = l.financeLogic.CheckKnowledgeBaseQuota(ctx, finance.CheckQuotaReq{App: app}); err == nil { // 没超过上限,进入学习
			if isAppeal {
				_ = l.SendNoticeIfDocAppealPass(ctx, doc, audit) // 人工申诉成功，但是发通知失败，不报错
			}
			if err = l.docLogic.UpdateDocAuditResult(ctx, doc); err != nil {
				logx.E(ctx, "Failed to update doc aucit result. err:%+v", err)
				return err
			}
			if err = l.docLogic.DocParseSegment(ctx, nil, doc, intervene); err != nil {
				return err
			}
		} else { // 超过上限不进入学习，返回错误
			isNeedCharSizeNotice = true
			doc.Status = docEntity.DocStatusParseImportFail
			doc.Message = errs.ConvertErr2i18nKeyMsg(common.ConvertErrMsg(ctx, l.rpc, 0, doc.CorpID, errs.ErrOverCharacterSizeLimit))
			if err = l.docLogic.UpdateDocAuditResult(ctx, doc); err != nil {
				logx.E(ctx, "Failed to update doc aucit result. err:%+v", err)
				return err
			}
		}
		return nil
	}); err != nil {
		logx.E(ctx, "审核文档失败 err:%+v", err)
		return err
	}
	if isNeedCharSizeNotice {
		var docParses *docEntity.DocParse
		if docParses, err = l.docLogic.GetDocParseByDocID(ctx, doc.ID, doc.RobotID); err != nil {
			logx.E(ctx, "查询 文档解析任务失败 args:%+v err:%+v", doc, err)
			return err
		}
		docParses.Status = docEntity.DocParseCallBackCharSizeExceeded
		docParses.UpdateTime = time.Now()
		err = l.docLogic.UpdateDocParseTask(ctx, []string{docEntity.DocParseTblColStatus, docEntity.DocParseTblColUpdateTime}, docParses)
		if err != nil {
			return errs.ErrUpdateDocParseTaskStatusFail
		}
		if err = l.FailCharSizeNotice(ctx, doc); err != nil {
			return err
		}
	}
	return nil
}

func (l *Logic) updateDocNameAuditResult(ctx context.Context, doc *docEntity.Doc, audit *releaseEntity.Audit,
	auditsMap map[uint32][]*releaseEntity.AuditStatusSourceList, pass, isAppeal bool) (*docEntity.Doc, *releaseEntity.Audit, error) {
	now := time.Now()
	audit.Status = l.getAuditStatus(pass, isAppeal)
	audit.UpdateTime, doc.UpdateTime = now, now
	doc.Status = docEntity.DocStatusCreatingIndex
	doc.AuditFlag = docEntity.AuditFlagDone
	if !pass {
		if isAppeal {
			doc.Message = i18nkey.KeyFileNameReviewFailed
			doc.Status = docEntity.DocStatusDocNameAppealFail
		} else if _, ok := auditsMap[releaseEntity.AuditStatusTimeoutFail]; ok {
			audit.Status = releaseEntity.AuditStatusTimeoutFail
			doc.Message = i18nkey.KeyDocumentNameReviewTimeout
			doc.Status = docEntity.DocStatusDocNameAuditFail
		} else {
			doc.Message = i18nkey.KeyDocumentNameReviewFailed
			doc.Status = docEntity.DocStatusDocNameAuditFail
		}
	}
	if err := l.docLogic.UpdateDocAuditResult(ctx, doc); err != nil {
		logx.E(ctx, "Failed to UpdateDocAuditResult. err:%+v", err)
		return nil, nil, err
	}
	return doc, audit, nil
}

// AuditDocName 文档名称审核或者申诉回调处理函数，audit是父审核任务
func (l *Logic) AuditDocName(ctx context.Context, audit *releaseEntity.Audit, pass, isAppeal bool, rejectReason string) error {
	logx.I(ctx, "AuditDocName %+v, pass: %+v, isAppeal: %+v, rejectReason: %+v",
		audit, pass, isAppeal, rejectReason)
	intervene := false
	if audit.Type == releaseEntity.AuditBizTypeDocSegment || audit.Type == releaseEntity.AuditBizTypeDocTableSheet {
		intervene = true
	}

	doc, err := l.docLogic.GetDocByID(ctx, audit.RelateID, audit.RobotID)
	if err != nil {
		return err
	}
	if doc == nil {
		return errs.ErrDocNotFound
	}
	if doc.HasDeleted() {
		audit.UpdateTime = time.Now()
		audit.Status = l.getAuditStatus(true, isAppeal) // 直接把审核状态改成成功
		_ = l.UpdateAuditStatus(ctx, audit)
		logx.I(ctx, "文档已经被删除，不再走审核逻辑，doc:%+v", doc)
		return nil
	}
	auditsMap, err := l.bizAuditStatusMap(ctx, audit)
	if err != nil {
		return err
	}
	// 查询切片表, 看看文档是否有生成过切片,若没有生成过切片,则大概率这次审核是文档导入名称审核失败的送审
	segs, err := l.segLogic.GetSegmentList(ctx, doc.CorpID, doc.ID, 1, 1, audit.RobotID)
	if err != nil {
		return err
	}
	isNeedCharSizeNotice := false
	app, err := l.rpc.AppAdmin.DescribeAppByPrimaryId(ctx, doc.RobotID)
	if err != nil {
		return err
	}
	if err = l.releaseDao.MysqlQuery().Transaction(func(tx *mysqlquery.Query) error {
		// 重命名的处理逻辑
		doc, audit, err = l.updateDocNameAuditResult(ctx, doc, audit, auditsMap, pass, isAppeal)
		if err != nil {
			return err
		}
		if !pass {
			return l.sendNoticeAndUpdateAuditStatusIfDocNameAuditFail(ctx, doc, audit, auditsMap, isAppeal,
				rejectReason)
		}
		if len(segs) == 0 {
			// 若是导入过程中文档名称审核失败,则需要走切片入库学习流程
			if err = l.financeLogic.CheckKnowledgeBaseQuota(ctx, finance.CheckQuotaReq{App: app}); err == nil { // 没超过上限,进入学习
				if isAppeal {
					_ = l.SendNoticeIfDocAppealPass(ctx, doc, audit) // 人工申诉成功，但是发通知失败，不报错
				}
				if err = l.docLogic.UpdateDocAuditResult(ctx, doc); err != nil {
					logx.E(ctx, "Failed to update doc aucit result. err:%+v", err)
					return err
				}
				if err = l.docLogic.DocParseSegment(ctx, nil, doc, intervene); err != nil {
					return err
				}
			} else { // 超过上限不进入学习，返回错误
				isNeedCharSizeNotice = true
				doc.Status = docEntity.DocStatusParseImportFail
				doc.Message = errs.ConvertErr2i18nKeyMsg(common.ConvertErrMsg(ctx, l.rpc, 0, doc.CorpID, errs.ErrOverCharacterSizeLimit))
				if err = l.docLogic.UpdateDocAuditResult(ctx, doc); err != nil {
					logx.E(ctx, "Failed to update doc aucit result. err:%+v", err)
					return err
				}
			}
			return nil
		}
		// 审核通过, 执行重命名入库
		if isAppeal {
			_ = l.sendNoticeIfDocNameAppealPass(ctx, doc, audit) // 人工申诉成功，但是发通知失败，不报错
		}
		if err := l.docLogic.CreateDocRenameToIndexTask(ctx, doc); err != nil {
			logx.E(ctx, "审核文档名 新增向量重新入库任务失败 err:%+v", err)
			return err
		}
		return nil
	}); err != nil {
		logx.E(ctx, "审核文档名 状态修改失败 err:%+v", err)
		return err
	}
	if isNeedCharSizeNotice {
		var docParses *docEntity.DocParse
		if docParses, err = l.docLogic.GetDocParseByDocID(ctx, doc.ID, doc.RobotID); err != nil {
			logx.E(ctx, "查询 文档解析任务失败 args:%+v err:%+v", doc, err)
			return err
		}
		docParses.Status = docEntity.DocParseCallBackCharSizeExceeded
		docParses.UpdateTime = time.Now()
		// 更新解析字符状态,重试的时候不会重新解析
		err = l.docLogic.UpdateDocParseTask(ctx, []string{docEntity.DocParseTblColStatus, docEntity.DocParseTblColUpdateTime}, docParses)
		if err != nil {
			return errs.ErrUpdateDocParseTaskStatusFail
		}
		if err = l.FailCharSizeNotice(ctx, doc); err != nil {
			return err
		}
	}
	return nil
}

// AuditQa QA审核或者申诉回调处理函数，audit是父审核任务
func (l *Logic) AuditQa(ctx context.Context, audit *releaseEntity.Audit, pass, isAppeal bool,
	rejectReason string) error {
	qa, err := l.qaLogic.GetQAByID(ctx, audit.RelateID)
	if err != nil {
		return err
	}
	if qa == nil || qa.CorpID != audit.CorpID || qa.RobotID != audit.RobotID {
		return errs.ErrQANotFound
	}
	if qa.IsDelete() {
		audit.UpdateTime = time.Now()
		audit.Status = l.getAuditStatus(true, isAppeal) // 直接把审核状态改成成功
		_ = l.UpdateAuditStatus(ctx, audit)
		logx.I(ctx, "qa已经被删除，不再走审核或者申诉逻辑，qa:%+v", qa)
		return nil
	}
	// 这里只获取了未删除的相似问，已删除的相似问同步向量库是在UpdateQA()函数中处理的
	simQuestions, err := l.qaLogic.GetSimilarQuestionsByQA(ctx, qa)
	if err != nil {
		return err
	}
	var isExceeded bool // 是否超过字符数限制
	var syncID uint64
	var syncSimilarQuestionsIDs = make([]uint64, 0)
	var auditStatusList []*releaseEntity.AuditStatusList
	appDB, err := l.rpc.AppAdmin.DescribeAppByPrimaryIdWithoutNotFoundError(ctx, qa.RobotID)
	if err != nil {
		return err
	}
	err = l.releaseDao.MysqlQuery().Transaction(func(tx *mysqlquery.Query) error {
		if pass { // 审核通过
			syncID, syncSimilarQuestionsIDs, isExceeded, err = l.dealQaAuditPass(ctx, qa, simQuestions, appDB)
			if err != nil {
				return err
			}
		} else { // 审核不通过
			auditStatusList, err = l.getBizAuditList(ctx, audit.CorpID, audit.RobotID, audit.ID)
			if err != nil {
				return err
			}
			if len(auditStatusList) == 0 {
				return errs.ErrAuditNotFound
			}
			corp, err := l.rpc.PlatformAdmin.DescribeCorpByPrimaryId(ctx, qa.CorpID)
			if err != nil {
				return err
			}
			isExceeded, err = l.dealQaAuditFail(ctx, qa, simQuestions, auditStatusList, isAppeal, corp.Uin, appDB)
			if err != nil {
				logx.E(ctx, "审核不通过，更新qa和相似问状态失败 err:%+v", err)
				return err
			}
		}
		// 发送通知失败不报错
		_ = l.sendNoticeForQaAudit(ctx, qa, simQuestions, audit, pass, isAppeal, isExceeded, rejectReason)
		audit.UpdateTime = time.Now()
		audit.Status = l.getAuditStatus(pass, isAppeal)
		if err = l.UpdateAuditStatus(ctx, audit); err != nil {
			logx.E(ctx, "Failed to update audit status err:%+v", err)
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	if pass && !isExceeded { // 审核通过，且未超量，则同步
		l.qaLogic.GetVectorSyncLogic().Push(ctx, syncID)
		l.qaLogic.GetVectorSyncLogic().BatchPush(ctx, syncSimilarQuestionsIDs)
	}
	return nil
}

// dealQaAuditPass qa审核通过处理
func (l *Logic) dealQaAuditPass(ctx context.Context, qa *qaEntity.DocQA,
	sims []*qaEntity.SimilarQuestion, app *entity.App) (uint64, []uint64, bool, error) {

	now := time.Now()
	isExceeded := false // 是否超量
	qa.ReleaseStatus = qaEntity.QAReleaseStatusLearning
	qa.IsAuditFree = qaEntity.QAIsAuditFree
	qa.UpdateTime = now
	err := l.auditQaCheckCharExceeded(ctx, qa, app)
	if errors.Is(err, errs.ErrOverCharacterSizeLimit) {
		isExceeded = true
	}
	if isExceeded { // 审核/申诉通过，但是超量，则设置QA状态为超量
		qa.ReleaseStatus = qaEntity.QAReleaseStatusCharExceeded
	}
	err = l.qaLogic.UpdateQAAuditStatusAndUpdateTimeTx(ctx, qa)
	if err != nil {
		logx.E(ctx, "审核通过，更新qa状态失败 err:%+v", err)
		return 0, nil, isExceeded, err
	}
	var syncID uint64
	if !isExceeded { // 非超量状态才需要同步
		// 添加同步任务
		syncID, err = l.qaLogic.GetVectorSyncLogic().AddQASync(ctx, qa)
		if err != nil {
			logx.E(ctx, "审核通过，添加qa同步任务失败 err:%+v", err)
			return 0, nil, isExceeded, err
		}
	}

	// 处理相似问
	var syncSimilarQuestionsIDs []uint64
	if len(sims) > 0 {
		for i := range sims {
			sims[i].ReleaseStatus = qaEntity.QAReleaseStatusLearning
			if isExceeded {
				sims[i].ReleaseStatus = qaEntity.QAReleaseStatusCharExceeded
			}
			sims[i].UpdateTime = now
			sims[i].IsAuditFree = qaEntity.QAIsAuditFree
		}
		err = l.qaLogic.UpdateSimilarQuestionsReleaseStatus(ctx, sims)
		if err != nil {
			logx.E(ctx, "审核通过，更新相似问状态失败 err:%+v", err)
			return 0, nil, isExceeded, err
		}
		if !isExceeded { // 非超量状态才需要同步
			// 添加同步任务
			if syncSimilarQuestionsIDs, err = l.qaLogic.GetVectorSyncLogic().AddSimilarQuestionSyncBatch(ctx, sims); err != nil {
				logx.E(ctx, "审核通过，添加相似问同步任务失败 err:%+v", err)
				return 0, nil, isExceeded, err
			}
		}
	}
	return syncID, syncSimilarQuestionsIDs, isExceeded, nil
}

func (l *Logic) dealQaAuditFail(ctx context.Context, qa *qaEntity.DocQA,
	sims []*qaEntity.SimilarQuestion, auditStatusList []*releaseEntity.AuditStatusList, isAppeal bool,
	uin string, app *entity.App) (bool, error) {
	isExceeded := false
	err := l.auditQaCheckCharExceeded(ctx, qa, app)
	if errors.Is(err, errs.ErrOverCharacterSizeLimit) {
		isExceeded = true
	}
	if isExceeded {
		if isAppeal {
			qa.ReleaseStatus = qaEntity.QAReleaseStatusAppealFailCharExceeded
		} else {
			qa.ReleaseStatus = qaEntity.QAReleaseStatusAuditNotPassCharExceeded
		}
		qa.UpdateTime = time.Now()
		err = l.qaLogic.UpdateQAAuditStatusAndUpdateTimeTx(ctx, qa)
		if err != nil {
			logx.E(ctx, "更新qa状态失败 err:%+v", err)
			return isExceeded, err
		}
		if len(sims) > 0 {
			for i := range sims {
				sims[i].ReleaseStatus = qa.ReleaseStatus
				sims[i].UpdateTime = qa.UpdateTime
			}
			err = l.qaLogic.UpdateSimilarQuestionsReleaseStatus(ctx, sims)
			if err != nil {
				logx.E(ctx, "更新相似问状态失败 err:%+v", err)
				return isExceeded, err
			}
		}
		return isExceeded, nil
	}
	// 下面是未超量的逻辑
	stAndItems, err := releaseEntity.GetAuditStatusAndItems(ctx, auditStatusList)
	if err != nil {
		return isExceeded, err
	}
	if len(stAndItems) == 0 {
		return isExceeded, fmt.Errorf("未找到失败的子审核数据,related qaID: %d", qa.ID)
	}
	var imageRedictedURLs = make([]string, 0)
	for _, image := range util.ExtractImagesFromMarkdown(qa.Answer) {
		if !config.IsInWhiteList(uin, app.BizId, config.GetWhitelistConfig().QaURLWhiteList) {
			safe, err := util.IsSafeURL(ctx, image)
			if err != nil || !safe {
				logx.W(ctx, "getQaAuditParams|imageUrl|safe:%v|err:%v", safe, err)
				return isExceeded, errs.ErrFileUrlFail
			}
		}
		url := getRedirectedURL(image)
		imageRedictedURLs = append(imageRedictedURLs, url)
	}
	var videos []*qaEntity.DocQAFile
	if config.VideoAuditSwitch() {
		videos, err = util.AuditQaVideoURLs(ctx, qa.Answer)
		if err != nil {
			return isExceeded, err
		}
	}
	qa, sims = SetQaAndSimilarAuditStatusIfAuditFail(ctx, qa, sims, stAndItems, imageRedictedURLs, videos, isAppeal)
	err = l.qaLogic.UpdateQAAuditStatusAndUpdateTimeTx(ctx, qa)
	if err != nil {
		logx.E(ctx, "更新qa状态失败 err:%+v", err)
		return isExceeded, err
	}
	if len(sims) > 0 {
		err = l.qaLogic.UpdateSimilarQuestionsReleaseStatus(ctx, sims)
		if err != nil {
			logx.E(ctx, "更新相似问状态失败 err:%+v", err)
			return isExceeded, err
		}
	}
	return isExceeded, nil
}

// auditQaCheckCharExceeded 判断应用使用的字符数是否超量，或者qa对应的问答是否处于超量。文档超量，它对应的qa也当作超量处理
func (l *Logic) auditQaCheckCharExceeded(ctx context.Context, qa *qaEntity.DocQA, app *entity.App) error {
	var doc *docEntity.Doc
	var err error
	if qa.DocID > 0 {
		doc, err = l.docLogic.GetDocByID(ctx, qa.DocID, qa.RobotID)
		if err != nil {
			logx.I(ctx, "获取qa关联的doc失败，qa:%+v, err", qa, err)
			doc = nil
		}
	}
	err = l.financeLogic.CheckKnowledgeBaseQuota(ctx, finance.CheckQuotaReq{App: app})
	if err != nil && !errors.Is(err, errs.ErrOverCharacterSizeLimit) {
		logx.E(ctx, "Failed to fer doc char size，qa:%+v, err", qa, err)
		return err
	}
	if errors.Is(err, errs.ErrOverCharacterSizeLimit) || (doc != nil && doc.IsCharSizeExceeded()) {
		return errs.ErrOverCharacterSizeLimit
	}
	return nil
}

// getChildAudits 获取子审核任务列表
func (l *Logic) getChildAudits(ctx context.Context, audit *releaseEntity.Audit) ([]*releaseEntity.AuditStatusSourceList, error) {
	sourceList := make([]*releaseEntity.AuditStatusSourceList, 0)
	filter := &releaseEntity.AuditFilter{
		CorpID:   audit.CorpID,
		RobotID:  audit.RobotID,
		ParentID: ptrx.Uint64(audit.ID),
	}
	selectColumns := []string{releaseEntity.AuditTblColStatus, releaseEntity.AuditTblColType, releaseEntity.AuditTblColParams}
	childAudits, err := l.releaseDao.GetAuditList(ctx, selectColumns, filter)
	if err != nil {
		return nil, err
	}
	for _, v := range childAudits {
		auditItem := releaseEntity.AuditItem{}
		if err := jsonx.UnmarshalFromString(v.Params, &auditItem); err != nil {
			logx.E(ctx, "任务参数解析失败 v.Params:%s,err:%+v",
				v.Params, err)
			return nil, err
		}
		sourceList = append(sourceList, &releaseEntity.AuditStatusSourceList{
			Status:   v.Status,
			Source:   auditItem.Source,
			Avatar:   auditItem.HeadURL,
			Name:     auditItem.Nick,
			Greeting: auditItem.Greeting,
			Content:  auditItem.Content,
		})
	}
	return sourceList, nil
}

// getChildAuditStatusMap 获取子审核任务状态信息
func (l *Logic) getChildAuditStatusMap(ctx context.Context, audit *releaseEntity.Audit) (map[uint32][]*releaseEntity.AuditStatusSourceList,
	error) {
	auditStatusSourceMap := make(map[uint32][]*releaseEntity.AuditStatusSourceList)
	lists, err := l.getChildAudits(ctx, audit)
	if err != nil {
		return auditStatusSourceMap, err
	}
	for _, v := range lists {
		if _, ok := auditStatusSourceMap[v.Status]; !ok {
			auditStatusSourceMap[v.Status] = make([]*releaseEntity.AuditStatusSourceList, 0)
		}
		auditStatusSourceMap[v.Status] = append(auditStatusSourceMap[v.Status], v)
	}
	return auditStatusSourceMap, nil
}

func (l *Logic) updateDocAuditResult(ctx context.Context, doc *docEntity.Doc, audit *releaseEntity.Audit,
	auditsMap map[uint32][]*releaseEntity.AuditStatusSourceList, pass, isAppeal bool, event string) (
	*docEntity.Doc, *releaseEntity.Audit, error) {
	logx.I(ctx, "updateDocAuditResult|start|DocID:%d|event:%s|Audit.RelateID:%d|Audit.Type:%d|pass:%t|isAppeal:%t",
		doc.ID, event, audit.RelateID, audit.Type, pass, isAppeal)
	audit.Status = getAuditStatus(pass, isAppeal)
	docSegmentAuditStatusMap := &sync.Map{} // DocSegmentAuditStatus:["SegmentBizID"]
	docSegmentAuditStatusFailedMap := make(map[string]segEntity.DocSegmentAuditStatus)
	if !pass {
		filter := &releaseEntity.AuditFilter{
			CorpID:   audit.CorpID,
			RobotID:  audit.RobotID,
			ParentID: ptrx.Uint64(audit.ID),
		}
		childAudits, err := l.releaseDao.GetAuditList(ctx, releaseEntity.AuditTblColList, filter)
		if err != nil {
			return nil, nil, err
		}
		contentFailed := false
		nameFailed := false
		segmentFailed := false
		segmentPictureFailed := false
		for _, ca := range childAudits {
			key := segEntity.DocSegmentAuditStatusPass
			// 解析params
			p := releaseEntity.AuditItem{}
			if err := jsonx.UnmarshalFromString(ca.Params, &p); err != nil {
				continue
			}
			if p.Source == releaseEntity.AuditSourceDocName && (ca.Status == releaseEntity.AuditStatusFail || ca.Status == releaseEntity.AuditStatusAppealFail) {
				nameFailed = true
			}
			if p.Source == releaseEntity.AuditSourceDoc && (ca.Status == releaseEntity.AuditStatusFail || ca.Status == releaseEntity.AuditStatusAppealFail) {
				contentFailed = true
				key = segEntity.DocSegmentAuditStatusContentFailed
			}
			if p.Source == releaseEntity.AuditSourceDocSegment && (ca.Status == releaseEntity.AuditStatusFail || ca.Status == releaseEntity.AuditStatusAppealFail) {
				segmentFailed = true
				if _, ok := docSegmentAuditStatusFailedMap[p.SegmentBizID]; ok {
					key = segEntity.DocSegmentAuditStatusContentAndPictureFailed
				} else {
					key = segEntity.DocSegmentAuditStatusContentFailed
				}
				docSegmentAuditStatusFailedMap[p.SegmentBizID] = key
			}
			if p.Source == releaseEntity.AuditSourceDocSegmentPic && (ca.Status == releaseEntity.AuditStatusFail || ca.Status == releaseEntity.AuditStatusAppealFail) {
				segmentPictureFailed = true
				if _, ok := docSegmentAuditStatusFailedMap[p.SegmentBizID]; ok {
					key = segEntity.DocSegmentAuditStatusContentAndPictureFailed
				} else {
					key = segEntity.DocSegmentAuditStatusPictureFailed
				}
				docSegmentAuditStatusFailedMap[p.SegmentBizID] = key
			}
			if p.SegmentBizID != "" && key != segEntity.DocSegmentAuditStatusPass &&
				(ca.Status == releaseEntity.AuditStatusFail || ca.Status == releaseEntity.AuditStatusAppealFail) {
				if ids, ok := docSegmentAuditStatusMap.Load(key); ok {
					idList, ok1 := ids.([]string)
					if !ok1 {
						logx.E(ctx, "updateDocAuditResult|SegmentBizID:%s|type assertion failed for ids", p.SegmentBizID)
						continue
					}
					docSegmentAuditStatusMap.Store(key, append(idList, p.SegmentBizID))
				} else {
					docSegmentAuditStatusMap.Store(key, []string{p.SegmentBizID})
				}
			}
		}
		if isAppeal {
			event = docEntity.EventAppealFailed
			if audit.Type == releaseEntity.AuditBizTypeDocSegment || audit.Type == releaseEntity.AuditBizTypeDocTableSheet {
				if segmentFailed || segmentPictureFailed || contentFailed {
					doc.Message = i18nkey.KeyParseSegmentationInterventionReviewFailed
				}
			} else {
				if contentFailed && nameFailed {
					doc.Message = i18nkey.KeyFileNameAndContentReviewFailed
				} else if nameFailed {
					doc.Message = i18nkey.KeyFileNameReviewFailed
				} else {
					doc.Message = i18nkey.KeyFileContentReviewFailed
				}
			}
		} else {
			event = docEntity.EventProcessFailed
			if _, ok := auditsMap[releaseEntity.AuditStatusTimeoutFail]; ok {
				audit.Status = releaseEntity.AuditStatusTimeoutFail
				if audit.Type == releaseEntity.AuditBizTypeDocSegment || audit.Type == releaseEntity.AuditBizTypeDocTableSheet {
					// todo 重试/申诉 功能待开发
					doc.Message = i18nkey.KeyDocumentReviewTimeout
				} else {
					doc.Message = i18nkey.KeyFileReviewTimeout
				}
			} else {
				if audit.Type == releaseEntity.AuditBizTypeDocSegment || audit.Type == releaseEntity.AuditBizTypeDocTableSheet {
					if segmentFailed || segmentPictureFailed || contentFailed {
						doc.Message = i18nkey.KeyParseSegmentationInterventionReviewFailed
					}
				} else {
					if contentFailed && nameFailed {
						doc.Message = i18nkey.KeyFileNameAndContentReviewFailedWithOption
					} else if nameFailed {
						doc.Message = i18nkey.KeyFileNameReviewFailedWithOption
					} else {
						doc.Message = i18nkey.KeyFileContentReviewFailedWithOption
					}
				}
			}
		}
	}
	if event == docEntity.EventUsedCharSizeExceeded && (audit.Type == releaseEntity.AuditBizTypeDocSegment ||
		audit.Type == releaseEntity.AuditBizTypeDocTableSheet) {
		doc.Message = i18nkey.KeyDocumentInterventionCharacterExceeded
	}
	// 更新干预切片审核失败的状态
	if audit.Type == releaseEntity.AuditBizTypeDocSegment || audit.Type == releaseEntity.AuditBizTypeDocTableSheet {
		err := l.updateDocSegmentAuditResult(ctx, doc, audit.Type, docSegmentAuditStatusMap)
		if err != nil {
			logx.E(ctx, "updateDocSegmentAuditResult|err:%+v", err)
			return nil, nil, err
		}
	}
	docFilter := &docEntity.DocFilter{
		IDs:     []uint64{doc.ID},
		RobotId: doc.RobotID,
	}
	doc.AuditFlag = docEntity.AuditFlagDone
	updateCols := []string{docEntity.DocTblColStatus, docEntity.DocTblColMessage, docEntity.DocTblColAuditFlag}
	err := l.docLogic.UpdateDocStatusMachineByEvent(ctx, updateCols, docFilter, doc, event)
	if err != nil {
		return nil, nil, err
	}
	return doc, audit, nil
}

// updateDocSegmentAuditResult 更新切片审核状态
func (l *Logic) updateDocSegmentAuditResult(ctx context.Context, doc *docEntity.Doc, auditType uint32,
	docSegmentAuditStatusMap *sync.Map) error {
	logx.I(ctx, "updateDocSegmentAuditResult|start|DocID:%d|AuditType:%d", doc.ID, auditType)
	batchSize := 200
	corpBizID, appBizID, _, _, err := l.segLogic.SegmentCommonIDsToBizIDs(ctx, doc.CorpID,
		doc.RobotID, 0, 0)
	if err != nil {
		logx.E(ctx, "updateDocSegmentAuditResult|SegmentCommonIDsToBizIDs|err:%+v", err)
		return err
	}
	auditFailedStatus := []segEntity.DocSegmentAuditStatus{
		segEntity.DocSegmentAuditStatusContentFailed,
		segEntity.DocSegmentAuditStatusPictureFailed,
		segEntity.DocSegmentAuditStatusContentAndPictureFailed}
	for _, auditStatus := range auditFailedStatus {
		if ids, ok := docSegmentAuditStatusMap.Load(auditStatus); ok {
			idList, ok1 := ids.([]string)
			if !ok1 {
				logx.E(ctx, "updateDocSegmentAuditResult|auditStatus:%d|type assertion failed for ids", auditStatus)
				continue
			}
			// 分批更新审核状态
			logx.I(ctx, "updateDocSegmentAuditResult|auditStatus:%d|len(ids):%d", auditStatus, len(idList))
			for _, idChunks := range slicex.Chunk(idList, batchSize) {
				if auditType == releaseEntity.AuditBizTypeDocSegment {
					err = l.segDao.UpdateDocSegmentAuditStatus(ctx, corpBizID, appBizID, doc.BusinessID, idChunks, uint32(auditStatus))
					if err != nil {
						logx.E(ctx, "updateDocSegmentAuditResult|UpdateDocSegmentAuditStatus|err:%+v", err)
						return err
					}
				} else if auditType == releaseEntity.AuditBizTypeDocTableSheet {
					var sheetIDs []uint64
					sheetIDs, err = util.BatchCheckReqParamsIsUint64(ctx, idChunks)
					if err != nil {
						logx.E(ctx, "updateDocSegmentAuditResult|BatchCheckReqParamsIsUint64|err:%+v", err)
						return err
					}
					err = l.segDao.UpdateDocSegmentSheetAuditStatus(ctx, corpBizID,
						appBizID, doc.BusinessID, sheetIDs, uint32(auditStatus))
					if err != nil {
						logx.E(ctx, "updateDocSegmentAuditResult|UpdateDocSegmentAuditStatus|err:%+v", err)
						return err
					}
				}
			}
		}
	}
	return nil
}

// ProcessDocAuditParentTask 文档审核或者申诉回调处理函数，audit是父审核任务
func (l *Logic) ProcessDocAuditParentTask(ctx context.Context, audit *releaseEntity.Audit, pass, isAppeal bool,
	rejectReason string, params entity.AuditCheckParams) error {
	logx.D(ctx, "ProcessDocAuditParentTask audit:%+v pass:%v isAppeal:%v rejectReason:%s",
		audit, pass, isAppeal, rejectReason)
	intervene := false
	if audit.Type == releaseEntity.AuditBizTypeDocSegment || audit.Type == releaseEntity.AuditBizTypeDocTableSheet {
		intervene = true
	}
	doc, err := l.docLogic.GetDocByID(ctx, audit.RelateID, audit.RobotID)
	if err != nil {
		return err
	}
	if doc == nil {
		return errs.ErrDocNotFound
	}
	if doc.HasDeleted() {
		audit.UpdateTime = time.Now()
		audit.Status = getAuditStatus(true, isAppeal) // 直接把审核状态改成成功
		_ = l.UpdateAuditStatus(ctx, audit)
		logx.I(ctx, "文档已经被删除，不再走审核逻辑，doc:%+v", doc)
		return nil
	}
	if !isAppeal && !doc.NeedAudit() {
		return nil
	}
	logx.D(ctx, "ProcessDocAuditParentTask|current DocID:%d", doc.ID)
	auditsMap, err := l.getChildAuditStatusMap(ctx, audit)
	if err != nil || len(auditsMap) == 0 {
		return errs.ErrAuditNotFound
	}
	isNeedCharSizeNotice := false
	app, err := l.rpc.AppAdmin.DescribeAppByPrimaryId(ctx, doc.RobotID)
	if err != nil {
		return err
	}
	if err = l.releaseDao.MysqlQuery().TAudit.WithContext(ctx).UnderlyingDB().Transaction(func(tx *gorm.DB) error {
		// 更新审核任务状态
		filter := &releaseEntity.AuditFilter{
			ID: audit.ID,
		}
		audit.Status = getAuditStatus(pass, isAppeal) // 直接把审核状态改成成功
		updateCols := []string{releaseEntity.AuditTblColRetryTimes, releaseEntity.AuditTblColStatus, releaseEntity.AuditTblColMessage}
		_, err = l.releaseDao.UpdateAudit(ctx, updateCols, filter, audit, tx)
		if err != nil {
			return err
		}
		event := docEntity.EventProcessSuccess
		if pass {
			// 审核通过需要校验字符数是否超限
			if err = l.financeLogic.CheckKnowledgeBaseQuota(ctx, finance.CheckQuotaReq{App: app}); err != nil {
				isNeedCharSizeNotice = true
				event = docEntity.EventUsedCharSizeExceeded
				doc.Message = errs.ConvertErr2i18nKeyMsg(common.ConvertErrMsg(ctx, l.rpc, 0, doc.CorpID, errs.ErrOverCharacterSizeLimit))
			} else {
				if isAppeal {
					// 人工申诉成功发送通知，如果发通知失败，不报错
					_ = l.SendNoticeIfDocAppealPass(ctx, doc, audit)
				}
				if err = l.docLogic.DocParseSegment(ctx, tx, doc, intervene); err != nil {
					return err
				}
			}
		} else {
			event = docEntity.EventProcessFailed
			// 审核不通过发送通知，如果发通知失败，不报错
			_ = sendAuditNotPassNotice(ctx, l.rpc, doc, audit, auditsMap, isAppeal, rejectReason)
		}
		// 更新文档状态
		doc, audit, err = l.updateDocAuditResult(ctx, doc, audit, auditsMap, pass, isAppeal, event)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		logx.E(ctx, "审核文档失败 err:%+v", err)
		return err
	}
	if isNeedCharSizeNotice {
		var docParses *docEntity.DocParse
		if docParses, err = l.docLogic.GetDocParseByDocID(ctx, doc.ID, doc.RobotID); err != nil {
			logx.E(ctx, "查询 文档解析任务失败 args:%+v err:%+v", doc, err)
			return err
		}
		docParses.Status = docEntity.DocParseCallBackCharSizeExceeded
		docParses.UpdateTime = time.Now()
		err = l.docLogic.UpdateDocParseTask(ctx, []string{docEntity.DocParseTblColStatus, docEntity.DocParseTblColUpdateTime}, docParses) // 更新解析字符状态,重试的时候不会重新解析
		if err != nil {
			return errs.ErrUpdateDocParseTaskStatusFail
		}
		if err = l.FailCharSizeNotice(ctx, doc); err != nil {
			return err
		}
	}
	return nil
}

// SendAudit 发送审核
func (l *Logic) SendAudit(ctx context.Context, audit *releaseEntity.Audit, appInfosecBizType string) error {
	if err := l.releaseDao.MysqlQuery().TAudit.WithContext(ctx).UnderlyingDB().Transaction(func(tx *gorm.DB) error {
		robot, err := l.rpc.AppAdmin.DescribeAppByPrimaryIdWithoutNotFoundError(ctx, audit.RobotID)
		if err != nil || robot == nil {
			return errs.ErrRobotNotFound
		}
		corpRsp, err := l.rpc.PlatformAdmin.DescribeCorpByPrimaryId(ctx, robot.CorpPrimaryId)
		if err != nil || corpRsp == nil {
			return errs.ErrCorpNotFound
		}
		audit.UpdateTime = time.Now()
		audit.RetryTimes = audit.RetryTimes + 1
		params := &releaseEntity.AuditItem{}
		if err = jsonx.UnmarshalFromString(audit.Params, params); err != nil {
			logx.E(ctx, "Failed to parse audit pa audit:%+v err:%+v", audit, err)
			return err
		}

		if err = l.rpc.InfoSec.CheckAuditSendItem(ctx, robot.BizId, audit.BusinessID, appInfosecBizType, corpRsp.InfosecBizType, params); err != nil {
			logx.E(ctx, "Failed to CheckAuditSendItem err:%+v", err)
			return err
		}

		audit.Status = releaseEntity.AuditStatusSendSuccess

		auditFilter := &releaseEntity.AuditFilter{
			ID:         audit.ID,
			StatusList: []uint32{releaseEntity.AuditStatusDoing},
		}

		selectColumns := []string{
			releaseEntity.AuditTblColUpdateTime,
			releaseEntity.AuditTblColRetryTimes,
			releaseEntity.AuditTblColStatus,
			releaseEntity.AuditTblColMessage,
		}

		if _, err := l.releaseDao.UpdateAudit(ctx, selectColumns, auditFilter, audit, tx); err != nil {
			return err
		}
		return nil
	}); err != nil {
		logx.E(ctx, "SendAudit|db.Transactionx failed|err:%+v", err)
		if audit.IsMaxSendAuditRetryTimes() {
			audit.Status = releaseEntity.AuditStatusFail
		}

		if err := l.UpdateAuditStatusOnly(ctx, audit); err != nil {
			return err
		}
		return nil
	}
	return nil
}

// SendAuditFail 更新发送审核失败
func (l *Logic) SendAuditFail(ctx context.Context, audit *releaseEntity.Audit) error {
	audit.Status = releaseEntity.AuditStatusSendFail
	audit.UpdateTime = time.Now()

	if err := l.UpdateAuditStatusOnly(ctx, audit); err != nil {
		return err
	}
	return nil
}

// GetAuditByEtag 通过tag获取文件是否已经审核通过
func (l *Logic) GetAuditByEtag(ctx context.Context, robotID, corpID, relateID uint64, eTag string) ([]*releaseEntity.Audit,
	error) {
	/*
			`
			SELECT
				%s
			FROM
			    t_audit
			WHERE
				corp_id = ?
				AND robot_id = ?
				AND relate_id = ?
				AND e_tag = ?  and  status in (?,?)
		`
	*/

	filter := &releaseEntity.AuditFilter{
		CorpID:     corpID,
		RobotID:    robotID,
		RelatedID:  int64(relateID),
		Etag:       eTag,
		StatusList: []uint32{releaseEntity.AuditStatusPass, releaseEntity.AuditStatusTimeoutFail},
	}

	audits, err := l.releaseDao.GetAuditList(ctx, []string{}, filter)
	if err != nil {
		logx.E(ctx, "GetAuditByEtag failed. err:%+v", err)
		return nil, err
	}
	return audits, nil
}

// BeforeAudit 处理发布审核前状态
func (l *Logic) BeforeAudit(ctx context.Context, audit *releaseEntity.Audit) error {
	if err := l.updateDocQAAuditing(ctx, audit); err != nil {
		return err
	}
	return nil
}

// updateDocQAAuditing 送审前将QA文档更新成审核中
func (l *Logic) updateDocQAAuditing(ctx context.Context, audit *releaseEntity.Audit) error {
	qa, err := l.releaseDao.GetReleaseQAByID(ctx, audit.RelateID)
	if err != nil {
		return err
	}
	if qa == nil {
		logx.D(ctx, "get releaseQA but not found,id:%+v", audit.RelateID)
		return nil
	}
	docQA, err := l.qaDao.GetQAByID(ctx, qa.QAID)
	if err != nil {
		return err
	}
	if docQA == nil {
		return errs.ErrDocNotFound
	}
	docQA.ReleaseStatus = qaEntity.QAReleaseStatusAuditing
	if err = l.qaLogic.UpdateAuditQA(ctx, docQA); err != nil {
		return err
	}
	return nil
}
