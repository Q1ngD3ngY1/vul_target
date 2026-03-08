package audit

import (
	"context"
	"fmt"
	"slices"
	"time"

	"git.woa.com/adp/common/x/gox/ptrx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/dao"
	qaDao "git.woa.com/adp/kb/kb-config/internal/dao/qa"
	releaseDao "git.woa.com/adp/kb/kb-config/internal/dao/release"
	segDao "git.woa.com/adp/kb/kb-config/internal/dao/segment"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
	docLogic "git.woa.com/adp/kb/kb-config/internal/logic/document"
	financeLogic "git.woa.com/adp/kb/kb-config/internal/logic/finance"
	qaLogic "git.woa.com/adp/kb/kb-config/internal/logic/qa"
	releaseLogic "git.woa.com/adp/kb/kb-config/internal/logic/release"
	segLogic "git.woa.com/adp/kb/kb-config/internal/logic/segment"
	"git.woa.com/adp/kb/kb-config/internal/rpc"
)

type Logic struct {
	rpc          *rpc.RPC
	rawSqlDao    dao.Dao
	qaDao        qaDao.Dao
	segDao       segDao.Dao
	releaseDao   releaseDao.Dao
	qaLogic      *qaLogic.Logic
	segLogic     *segLogic.Logic
	docLogic     *docLogic.Logic
	releaseLogic *releaseLogic.Logic
	s3           dao.S3
	financeLogic *financeLogic.Logic
}

func NewLogic(
	rpc *rpc.RPC,
	rawSqlDao dao.Dao,
	qaDao qaDao.Dao,
	segDao segDao.Dao,
	releaseDao releaseDao.Dao,
	qaLogic *qaLogic.Logic,
	segLogic *segLogic.Logic,
	docLogic *docLogic.Logic,
	releaseLogic *releaseLogic.Logic,
	s3 dao.S3,
	financeLogic *financeLogic.Logic,
) *Logic {
	return &Logic{
		rpc:          rpc,
		rawSqlDao:    rawSqlDao,
		qaDao:        qaDao,
		segDao:       segDao,
		qaLogic:      qaLogic,
		segLogic:     segLogic,
		releaseLogic: releaseLogic,
		releaseDao:   releaseDao,
		docLogic:     docLogic,
		s3:           s3,
		financeLogic: financeLogic,
	}
}

// GetAuditByID 通过id获取审核数据
func (l *Logic) GetAuditByID(ctx context.Context, id uint64) (*releaseEntity.Audit, error) {
	/*
		`
				SELECT
					%s
				FROM
				    t_audit
				WHERE
				    id = ?
			`
	*/
	filter := &releaseEntity.AuditFilter{
		ID: id,
	}

	audit, err := l.releaseDao.GetAuditByFilter(ctx, []string{}, filter)
	if err != nil {
		logx.E(ctx, "Get audit by id failed. err:%+v", err)
		return nil, err
	}
	return audit, nil
}

// GetAuditByBizID 通过BizID获取审核数据
func (l *Logic) GetAuditByBizID(ctx context.Context, bizID uint64) (*releaseEntity.Audit, error) {
	/*
		`
			SELECT
				%s
			FROM
			    t_audit
			WHERE
			    business_id =
	*/
	filter := &releaseEntity.AuditFilter{
		BusinessID: bizID,
	}

	audit, err := l.releaseDao.GetAuditByFilter(ctx, []string{}, filter)
	if err != nil {
		logx.E(ctx, "Get audit by bussinessID failed. err:%+v", err)
		return nil, err
	}
	return audit, nil
}

// GetAuditByParentID 通过ID获取已存在的审核数据
func (l *Logic) GetAuditByParentID(ctx context.Context, parentID uint64, p entity.AuditSendParams) ([]*releaseEntity.Audit,
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
			AND parent_id = ?
		    AND type = ?
	*/

	filter := &releaseEntity.AuditFilter{
		CorpID:   p.CorpID,
		RobotID:  p.RobotID,
		ParentID: ptrx.Uint64(parentID),
		Type:     p.Type,
	}

	audits, err := l.releaseDao.GetAuditList(ctx, []string{}, filter)
	if err != nil {
		logx.E(ctx, "Get audit by ParentID failed. err:%+v", err)
		return nil, err
	}
	return audits, nil
}

// GetChildAuditsByParentID 通过parentID获取子审核信息
func (l *Logic) GetChildAuditsByParentID(ctx context.Context, corpID, robotID, parentID uint64) ([]*releaseEntity.Audit,
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
				AND parent_id = ?
		`
	*/
	selectColumns := releaseEntity.AuditTblColList

	filter := &releaseEntity.AuditFilter{
		CorpID:   corpID,
		RobotID:  robotID,
		ParentID: ptrx.Uint64(parentID),
	}

	audits, err := l.releaseDao.GetAuditList(ctx, selectColumns, filter)
	if err != nil {
		logx.E(ctx, "通过ID获取子审核信息失败 err:%+v", err)
		return nil, err
	}
	if len(audits) == 0 {
		return nil, nil
	}
	return audits, nil
}

func SetQaAndSimilarAuditStatusIfAuditFail(ctx context.Context, qa *qaEntity.DocQA, sims []*qaEntity.SimilarQuestion,
	stAndItems []*releaseEntity.AuditStatusAndItem, imageRedictedURLs []string, videos []*qaEntity.DocQAFile, isAppeal bool) (
	*qaEntity.DocQA, []*qaEntity.SimilarQuestion) {
	qa = getQaAuditFailDetails(qa, stAndItems, imageRedictedURLs, videos)
	now := time.Now()
	qa.ReleaseStatus = qaEntity.QAReleaseStatusAuditNotPass // 只要问答审核失败(问答文本/图片/视频/相似问)，就置为审核失败
	if isAppeal {
		qa.ReleaseStatus = qaEntity.QAReleaseStatusAppealFail
	}
	qa.IsAuditFree = qaEntity.QAIsAuditNotFree
	qa.UpdateTime = now

	for i, v := range sims {
		sims[i].ReleaseStatus = qaEntity.QAReleaseStatusAuditing
		sims[i].UpdateTime = now
		for _, v1 := range stAndItems {
			if v1.AuditItem.Typ == releaseEntity.AuditTypePlainText && v1.AuditItem.Content == v.Question {
				// 相似问审核失败
				sims[i].ReleaseStatus = qaEntity.QAReleaseStatusAuditNotPass
				if isAppeal {
					sims[i].ReleaseStatus = qaEntity.QAReleaseStatusAppealFail
				}
				break
			}
		}
	}
	logx.I(ctx, "setQaAndSimilarAuditStatusIfAuditFail qa:%+v, sims:%+v", qa, sims)
	return qa, sims
}

func getQaAuditFailDetails(qa *qaEntity.DocQA, stAndItems []*releaseEntity.AuditStatusAndItem,
	imageRedictedURLs []string, videos []*qaEntity.DocQAFile) *qaEntity.DocQA {
	qaAuditFail := false
	imageAuditFail := false
	videoAuditFail := false
	content := fmt.Sprintf("%s\n%s\n%s", qa.Question, qa.Answer, qa.QuestionDesc)
	for _, v := range stAndItems {
		if !qaAuditFail && v.AuditItem.Typ == releaseEntity.AuditTypePlainText && v.AuditItem.Content == content {
			// 问答审核失败
			qaAuditFail = true
		} else if !imageAuditFail && v.AuditItem.Typ == releaseEntity.AuditTypePicture {
			// 图片审核失败
			imageAuditFail = slices.Contains(imageRedictedURLs, v.AuditItem.URL)
		} else if config.VideoAuditSwitch() && !videoAuditFail && v.AuditItem.Typ == releaseEntity.AuditTypeVideo {
			// 视频审核失败
			videoAuditFail = slices.ContainsFunc(videos, func(u *qaEntity.DocQAFile) bool {
				return u.CosURL == v.AuditItem.URL
			})
		}
	}
	qa.QaAuditFail = qaAuditFail
	qa.PicAuditFail = imageAuditFail
	qa.VideoAuditFail = videoAuditFail
	return qa
}

// TODO: 以下的四个函数 需要确认 业务逻辑有没有在使用

// // auditReleaseQA 审核发布QA
// func (d *dao) auditReleaseQA(ctx context.Context, tx *sqlx.Tx, audit *releaseEntity.Audit, pass bool) error {
// 	qa, err := d.GetReleaseQAByID(ctx, audit.RelateID)
// 	if err != nil {
// 		return err
// 	}
// 	if qa == nil {
// 		logx.D(ctx, "try to get releaseQA from audit but not found,id:%+v", audit.RelateID)
// 		return nil
// 	}
// 	// 更新t_release_qa
// 	now := time.Now()
// 	args := make([]any, 0, 10)
// 	querySQL := releaseQAAuditPass
// 	// 审核通过不通过t_doc_qa发布状态为审核不通过，审核通过继续修改为发布中
// 	var docQA *qaEntity.DocQA
// 	docQA, err = d.GetQAByID(ctx, qa.QAID)
// 	if err != nil {
// 		return err
// 	}
// 	if pass {
// 		args = append(args, qa.ReleaseStatus, releaseEntity.ReleaseQAAuditStatusSuccess, "审核通过", "-", entity.AllowRelease,
// 			now, qa.ID, releaseEntity.ReleaseQAAuditStatusDoing, releaseEntity.ReleaseQAAuditStatusSuccess,
// 		)
// 		docQA.ReleaseStatus = qaEntity.QAReleaseStatusIng
// 	} else {
// 		querySQL = releaseQAAuditNotPass
// 		args = append(args, qaEntity.QAReleaseStatusFail, releaseEntity.ReleaseQAAuditStatusFail, "审核未通过",
// 			gox.IfElse(audit.Status == releaseEntity.AuditStatusTimeoutFail, "审核超时", "包含敏感词"),
// 			entity.ForbidRelease, now, qa.ID,
// 		)
// 		docQA.ReleaseStatus = qaEntity.QAReleaseStatusAuditNotPass
// 	}
// 	// 更新ReleaseQA
// 	if _, err = tx.ExecContext(ctx, querySQL, args...); err != nil {
// 		logx.E(ctx, "更新发布QA审核结果失败 sql:%s args:%+v err:%+v", querySQL, args, err)
// 		return err
// 	}
// 	// 更新DocQA
// 	docQA.UpdateTime = time.Now()
// 	if _, err = tx.NamedExecContext(ctx, updateAfterAuditQA, docQA); err != nil {
// 		logx.E(ctx, "更新审核结果到QA文档中失败 sql:%s args:%+v err:%+v", updateAfterAuditQA, args, err)
// 		return err
// 	}
// 	return nil
// }

// // auditReleaseConfig 审核发布QA
// func (d *dao) auditReleaseConfig(ctx context.Context, tx *sqlx.Tx, audit *releaseEntity.Audit, pass bool) error {
// 	cfg, err := d.GetReleaseConfigItemByID(ctx, audit.RelateID)
// 	if err != nil {
// 		return err
// 	}
// 	if cfg == nil {
// 		logx.D(ctx, "try to get release config item but not found,id:%+v", audit.RelateID)
// 		return nil
// 	}
// 	now := time.Now()
// 	args := make([]any, 0, 10)
// 	querySQL := releaseConfigAuditPass
// 	if pass {
// 		args = append(args, cfg.ReleaseStatus, entity.ReleaseConfigAuditStatusSuccess, "审核通过", "-",
// 			now, cfg.ID, entity.ConfigReleaseStatusAuditing, releaseEntity.ReleaseQAAuditStatusSuccess,
// 		)
// 	} else {
// 		querySQL = releaseConfigAuditNotPass
// 		args = append(args, releaseEntity.ConfigReleaseStatusFail, entity.ConfigReleaseStatusAuditNotPass, "审核未通过",
// 			gox.IfElse(audit.Status == releaseEntity.AuditStatusTimeoutFail, "审核超时", "包含敏感词"),
// 			now, cfg.ID,
// 		)
// 	}
// 	// 更新t_release_config
// 	if _, err = tx.ExecContext(ctx, querySQL, args...); err != nil {
// 		logx.E(ctx, "更新发布QA审核结果失败 sql:%s args:%+v err:%+v", querySQL, args, err)
// 		return err
// 	}
// 	return nil
// }

// // auditRelease 审核发布
// func (d *dao) auditRelease(ctx context.Context, tx *sqlx.Tx, audit *releaseEntity.Audit, pass bool) error {
// 	appDB, err := d.DescribeAppByPrimaryIdWithoutNotFoundError(ctx, audit.AppPrimaryId)
// 	if err != nil {
// 		return err
// 	}
// 	if appDB == nil {
// 		return errs.ErrRobotNotFound
// 	}
// 	switch appDB.AppType {
// 	case entity.KnowledgeQaAppType:
// 		err = d.auditReleaseQA(ctx, tx, audit, pass)
// 		err = d.auditReleaseConfig(ctx, tx, audit, pass)
// 	case entity.ClassifyAppType:
// 		err = d.auditReleaseConfig(ctx, tx, audit, pass)
// 	case entity.SummaryAppType:
// 		err = d.auditReleaseConfig(ctx, tx, audit, pass)
// 	default:
// 		return errs.ErrGetAppFail
// 	}
// 	if err != nil {
// 		return err
// 	}
// 	return nil
// }
