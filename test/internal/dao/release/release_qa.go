package release

import (
	"context"
	"fmt"

	"gorm.io/gorm/clause"

	"git.woa.com/adp/common/x/errx"
	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/utilx"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/model"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/mysqlquery"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
	"gorm.io/gen"
	"gorm.io/gen/field"
	"gorm.io/gorm"
)

func (d *daoImpl) getReleaseQAGormDB(ctx context.Context, tx *gorm.DB) *gorm.DB {
	if tx != nil {
		return mysqlquery.Use(tx).TReleaseQa.WithContext(ctx).UnderlyingDB()
	}
	return d.mysql.TReleaseQa.WithContext(ctx).Debug().UnderlyingDB()
}

func (d *daoImpl) IsExistReleaseQA(ctx context.Context, filter *releaseEntity.ReleaseQAFilter) (bool, error) {
	/*
		 `
			SELECT
				COUNT(1)
			FROM
				t_release_qa
			WHERE
				 robot_id = ? AND version_id = ? AND qa_id = ?
		`
	*/
	tbl := d.mysql.TReleaseQa
	db := tbl.WithContext(ctx).Debug()
	cond := []gen.Condition{
		tbl.RobotID.Eq(filter.RobotID),
		tbl.VersionID.Eq(filter.VersionID),
		tbl.QaID.Eq(filter.QAID),
	}

	count, err := db.Where(cond...).Count()
	if err != nil {
		logx.E(ctx, "IsExistReleaseQA data req:(robotId:%v, versionId:%v, qaId:%v), error:%v",
			filter.RobotID, filter.VersionID, filter.QAID, err)
		return false, err
	}
	return count > 0, nil
}

func (d *daoImpl) CreateReleaseQARecords(ctx context.Context, releaseQas []*releaseEntity.ReleaseQA, tx *gorm.DB) error {

	if len(releaseQas) == 0 {
		logx.I(ctx, "no release qa record to create")
		return nil
	}
	tbl := d.mysql.TReleaseQa
	db := d.getReleaseQAGormDB(ctx, tx)
	toCreateQas := BatchConvertReleaseQaPoToDO(releaseQas)
	logx.I(ctx, "CreateReleaseQARecords data %d releaseQas", len(toCreateQas))
	if err := db.Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: tbl.VersionID.ColumnName().String()},
			{Name: tbl.QaID.ColumnName().String()},
		},
		DoUpdates: clause.AssignmentColumns([]string{
			tbl.Question.ColumnName().String(),
			tbl.Answer.ColumnName().String(),
			tbl.SimilarStatus.ColumnName().String(),
			tbl.AcceptStatus.ColumnName().String(),
			tbl.Action.ColumnName().String(),
			tbl.AttrLabels.ColumnName().String(),
			tbl.ReleaseStatus.ColumnName().String(),
			tbl.UpdateTime.ColumnName().String(),
			tbl.IsDeleted.ColumnName().String(),
		}), // 冲突时更新 question,answer,similar_status,accept_status,action,attr_labels,release_status,update_time,is_deleted 字段
	}).CreateInBatches(toCreateQas, 100).Error; err != nil {
		logx.E(ctx, "CreateReleaseQARecords data %d releaseQas, error:%v",
			len(toCreateQas), err)
		return err
	}
	return nil

}

func (d *daoImpl) BatchUpdateReleaseQARecords(ctx context.Context, updateColumns map[string]any,
	filter *releaseEntity.ReleaseQAFilter, tx *gorm.DB) (uint64, error) {
	if len(updateColumns) == 0 {
		logx.I(ctx, "no release qa record to update")
		return 0, nil
	}
	db := d.getReleaseQAGormDB(ctx, tx)

	session := mysqlquery.Use(db).TReleaseQa.WithContext(ctx).Debug()

	if filter != nil {
		if filter.Id != 0 {
			session = session.Where(d.mysql.TReleaseQa.ID.Eq(int64(filter.Id)))
		}
		if filter.RobotID != 0 {
			session = session.Where(d.mysql.TReleaseQa.RobotID.Eq(filter.RobotID))
		}
		if filter.VersionID != 0 {
			session = session.Where(d.mysql.TReleaseQa.VersionID.Eq(filter.VersionID))
		}
		if filter.QAID != 0 {
			session = session.Where(d.mysql.TReleaseQa.QaID.Eq(filter.QAID))
		}
		if filter.ReleaseStatusNot != 0 {
			session = session.Where(d.mysql.TReleaseQa.ReleaseStatus.Neq(filter.ReleaseStatusNot))
		}
	}

	if info, err := session.Updates(updateColumns); err != nil {
		return 0, err
	} else {
		return uint64(info.RowsAffected), nil
	}

}

func (d *daoImpl) GetReleaseQaIdMap(ctx context.Context, corpId, robotId, versionId uint64,
	qaIds []uint64) (map[uint64]struct{}, error) {
	releaseQaIdMap := make(map[uint64]struct{}, 0)
	if len(qaIds) == 0 {
		return releaseQaIdMap, nil
	}

	db := d.mysql.TReleaseQa.WithContext(ctx).Debug()

	cond := []gen.Condition{
		d.mysql.TReleaseQa.CorpID.Eq(corpId),
		d.mysql.TReleaseQa.RobotID.Eq(robotId),
		d.mysql.TReleaseQa.VersionID.Eq(versionId),
		d.mysql.TReleaseQa.QaID.In(qaIds...),
	}

	releaseQas := make([]*model.TReleaseQa, 0)

	releaseQas, err := db.Where(cond...).Find()
	if err != nil {
		logx.E(ctx, "GetReleaseQaIdMap data req:(corpId:%v, robotId:%v, versionId:%v, qaIds:%v), error:%v",
			corpId, robotId, versionId, qaIds, err)
		return nil, err
	}

	for _, releaseQa := range releaseQas {
		releaseQaIdMap[uint64(releaseQa.QaID)] = struct{}{}
	}
	return releaseQaIdMap, nil
}

func (d *daoImpl) GetReleaseQaDocIdMap(ctx context.Context, corpId, robotId, versionId uint64,
	docIds []uint64) (map[uint64]struct{}, error) {
	releaseQaDocIdMap := make(map[uint64]struct{}, 0)
	if len(docIds) == 0 {
		return releaseQaDocIdMap, nil
	}

	db := d.mysql.TReleaseQa.WithContext(ctx).Debug()

	cond := []gen.Condition{
		d.mysql.TReleaseQa.CorpID.Eq(corpId),
		d.mysql.TReleaseQa.RobotID.Eq(robotId),
		d.mysql.TReleaseQa.VersionID.Eq(versionId),
		d.mysql.TReleaseQa.QaID.In(docIds...),
	}

	releaseDocs := make([]*model.TReleaseQa, 0)

	releaseDocs, err := db.Where(cond...).Find()
	if err != nil {
		logx.E(ctx, "GetReleaseQaDocIdMap data req:(corpId:%v, robotId:%v, versionId:%v, qaIds:%v), error:%v",
			corpId, robotId, versionId, docIds, err)
		return nil, err
	}

	for _, releaseQa := range releaseDocs {
		releaseQaDocIdMap[uint64(releaseQa.DocID)] = struct{}{}
	}
	return releaseQaDocIdMap, nil

}

func (d *daoImpl) GetDocIDInReleaseDocQAs(ctx context.Context, release *releaseEntity.Release) (
	[]uint64, error) {
	/***
		`
		SELECT
			DISTINCT(doc_id)
		FROM
		    t_release_qa
		WHERE
		    robot_id = ? AND version_id = ? AND corp_id = ? AND doc_id != 0
	`
	***/
	db := d.mysql.TReleaseQa.WithContext(ctx).Debug()

	queryCond := []gen.Condition{
		d.mysql.TReleaseQa.RobotID.Eq(release.RobotID),
		d.mysql.TReleaseQa.VersionID.Eq(release.ID),
		d.mysql.TReleaseQa.CorpID.Eq(release.CorpID),
		d.mysql.TReleaseQa.DocID.Neq(0),
	}

	res, err := db.Select(d.mysql.TReleaseQa.DocID.Distinct()).Where(queryCond...).Find()
	if err != nil {
		logx.E(ctx, "GetReleaseDocQAs data req:(robotId:%v, versionId:%v, corpId:%v), error:%v",
			release.RobotID, release.ID, release.CorpID, err)
		return nil, err
	}
	docIDs := slicex.Map(res, func(item *model.TReleaseQa) uint64 {
		return item.DocID
	})
	// docIDMap := make(map[uint64]struct{}, 0)
	// for _, item := range res {
	// 	docID[item.DocID] = struct{}{}
	// }

	return docIDs, nil
}

// GetModifyQACount 获取版本改动QA数量
func (d *daoImpl) GetModifyQACount(ctx context.Context, robotID, versionID uint64,
	question string, actions []uint32, releaseStatuses []uint32) (uint64, error) {

	/***
	`
			SELECT
				count(*)
			FROM
			    t_release_qa
			WHERE
				 robot_id = ? AND version_id = ? %s
		`
	***/
	db := d.mysql.TReleaseQa.WithContext(ctx).Debug()

	queryCond := []gen.Condition{
		d.mysql.TReleaseQa.RobotID.Eq(robotID),
		d.mysql.TReleaseQa.VersionID.Eq(versionID),
	}

	if question != "" {
		queryCond = append(queryCond, d.mysql.TReleaseQa.Question.Like("%"+special.Replace(question)+"%"))
	}

	if len(actions) > 0 {

		queryCond = append(queryCond, d.mysql.TReleaseQa.Action.In(actions...))
	}

	if len(releaseStatuses) > 0 {
		queryCond = append(queryCond, d.mysql.TReleaseQa.ReleaseStatus.In(releaseStatuses...))
	}

	count, err := db.Where(queryCond...).Count()
	if err != nil {
		logx.E(ctx, "GetModifyQACount data req:(robotID:%v, versionID:%v, question:%v, actions:%v, releaseStatuses:%v), error:%v",
			robotID, versionID, question, actions, releaseStatuses, err)
		return 0, err
	}
	return uint64(count), nil

}

// GetModifyQAList 获取版本改动QA范围
func (d *daoImpl) GetModifyQAList(ctx context.Context, req *releaseEntity.ListReleaseQAReq) (
	[]*releaseEntity.ReleaseQA, error) {
	/***
			`
			SELECT
				%s
			FROM
			    t_release_qa
			WHERE
			    robot_id = ? AND version_id = ? %s
			LIMIT ?,?
		`

	***/

	db := d.mysql.TReleaseQa.WithContext(ctx).Debug()

	queryCond := []gen.Condition{
		d.mysql.TReleaseQa.RobotID.Eq(req.RobotID),
		d.mysql.TReleaseQa.VersionID.Eq(req.VersionID),
	}

	if req.Question != "" {
		queryCond = append(queryCond, d.mysql.TReleaseQa.Question.Like("%"+special.Replace(req.Question)+"%"))
	}

	if req.IsDeleted != nil {
		queryCond = append(queryCond, d.mysql.TReleaseQa.IsDeleted.Eq(*req.IsDeleted))
	}

	if req.IsDeletedNot != nil {
		queryCond = append(queryCond, d.mysql.TReleaseQa.IsDeleted.Neq(*req.IsDeletedNot))
	}

	if req.IsAllowRelease != nil {
		queryCond = append(queryCond, d.mysql.TReleaseQa.IsAllowRelease.Eq(*req.IsAllowRelease))
	}

	if req.MinQAID != 0 {
		queryCond = append(queryCond, d.mysql.TReleaseQa.QaID.Gt(req.MinQAID))
	}

	if req.MaxQAID != 0 {
		queryCond = append(queryCond, d.mysql.TReleaseQa.QaID.Lt(req.MaxQAID))
	}

	if len(req.Actions) > 0 {
		actionParams := make([]uint32, 0, len(req.Actions))
		for _, action := range req.Actions {
			actionParams = append(actionParams, uint32(action))
		}

		queryCond = append(queryCond, d.mysql.TReleaseQa.Action.In(actionParams...))
	}

	if len(req.ReleaseStatus) > 0 {
		queryCond = append(queryCond, d.mysql.TReleaseQa.ReleaseStatus.In(req.ReleaseStatus...))
	}

	offset, limit := utilx.Page(req.Page, req.PageSize)

	orderFields := []field.Expr{}

	if len(req.OrderBy) > 0 {
		if field, ok := d.mysql.TReleaseQa.GetFieldByName(req.OrderBy); ok {
			orderFields = append(orderFields, field.Asc())
		} else {
			orderFields = append(orderFields, d.mysql.TReleaseQa.QaID.Asc())
		}
	}

	res, err := db.Where(queryCond...).Order(orderFields...).Limit(limit).Offset(offset).Find()
	if err != nil {
		logx.E(ctx, "GetModifyQAList data req:(robotID:%v, versionID:%v, question:%v, actions:%v, releaseStatuses:%v), error:%v",
			req.RobotID, req.VersionID, req.Question, req.Actions, req.ReleaseStatus, err)
		return nil, err
	}

	releaseQas := []*releaseEntity.ReleaseQA{}
	for _, item := range res {
		releaseQas = append(releaseQas, ConvertReleaseQADOToPO(item))
	}
	return releaseQas, nil
}

// GetAuditQAByVersion 获取要审核的QA内容
func (d *daoImpl) GetAuditQAByVersion(ctx context.Context, versionID uint64) (
	[]*releaseEntity.AuditReleaseQA, error) {

	/***

	`
			SELECT
				id,question,answer
			FROM
			    t_release_qa
			WHERE
			    version_id = ? AND audit_status = ?
	`
	***/

	db := d.mysql.TReleaseQa.WithContext(ctx).Debug()

	queryComd := []gen.Condition{
		d.mysql.TReleaseQa.VersionID.Eq(versionID),
		d.mysql.TReleaseQa.AuditStatus.Eq(releaseEntity.ReleaseQAAuditStatusDoing),
	}

	res, err := db.Where(queryComd...).Find()
	if err != nil {
		logx.E(ctx, "GetAuditQAByVersion data req:(versionID:%v), error:%v", versionID, err)
		return nil, err
	}

	releaseQas := []*releaseEntity.AuditReleaseQA{}
	for _, item := range res {
		auditReleaseQA := &releaseEntity.AuditReleaseQA{
			ID:       item.QaID,
			Question: item.Question,
			Answer:   item.Answer,
		}
		releaseQas = append(releaseQas, auditReleaseQA)
	}
	return releaseQas, nil
}

// GetReleaseModifyQA 获取版本改动的QA
func (d *daoImpl) GetReleaseModifyQA(ctx context.Context, release *releaseEntity.Release,
	qas []*qaEntity.DocQA) (map[uint64]*releaseEntity.ReleaseQA, error) {

	/***
		`
		SELECT
			%s
		FROM
		    t_release_qa
		WHERE
		    corp_id = ? AND robot_id = ? AND version_id = ? %s
	`
	***/

	db := d.mysql.TReleaseQa.WithContext(ctx).Debug()

	// 1=1?
	queryCond := []gen.Condition{
		d.mysql.TReleaseQa.CorpID.Eq(release.CorpID),
		d.mysql.TReleaseQa.RobotID.Eq(release.RobotID),
		d.mysql.TReleaseQa.VersionID.Eq(release.ID),
	}

	if len(qas) > 0 {

		qaIds := []uint64{}
		for _, qa := range qas {
			qaIds = append(qaIds, qa.ID)
		}
		queryCond = append(queryCond, d.mysql.TReleaseQa.QaID.In(qaIds...))
	}

	releaseQas, err := db.Where(queryCond...).Find()

	if err != nil {
		logx.E(ctx, "GetReleaseQaIdMap data req:(corpId:%v, robotId:%v, versionId:%v), error:%v",
			release.CorpID, release.RobotID, release.ID, err)
		return nil, err
	}

	modifyQA := make(map[uint64]*releaseEntity.ReleaseQA, len(releaseQas))
	for _, item := range releaseQas {
		releaseQa := ConvertReleaseQADOToPO(item)
		modifyQA[item.QaID] = releaseQa
	}
	return modifyQA, nil
}

// GetReleaseQAByID 获取发布的QA
func (d *daoImpl) GetReleaseQAByID(ctx context.Context, id uint64) (*releaseEntity.ReleaseQA, error) {

	/***

	`
		SELECT
			%s
		FROM
		    t_release_qa
		WHERE
			 id = ?
	`
	***/

	db := d.mysql.TReleaseQa.WithContext(ctx).Debug()
	queryCond := []gen.Condition{
		d.mysql.TReleaseQa.ID.Eq(int64(id)),
	}

	res, err := db.Where(queryCond...).Find()
	if err != nil {
		logx.E(ctx, "GetReleaseQAByID data req:(id:%v), error:%v",
			id, err)
		return nil, err
	}
	if len(res) == 0 {
		logx.I(ctx, "[warning] GetReleaseQAByID data req:(id:%v) not found", id)
		return nil, nil
	}
	return ConvertReleaseQADOToPO(res[0]), nil
}

// GetReleaseQAAuditStat 统计QA审核
func (d *daoImpl) GetReleaseQAAuditStat(ctx context.Context, versionID uint64) (
	map[uint32]*releaseEntity.AuditResultStat, error) {
	/***
		`
		SELECT
			audit_status,count(*) total
		FROM
		    t_release_qa
		WHERE
		    version_id = ?
		GROUP BY
		    audit_status
	`
	***/

	db := d.mysql.TReleaseQa.WithContext(ctx).Debug().UnderlyingDB().Table(model.TableNameTReleaseQa)

	res := make([]*AuditResultStat, 0)

	queryArgs := make([]any, 0, 1)
	queryArgs = append(queryArgs, versionID)
	queryStr := "version_id=?"

	if err := db.
		Where(queryStr, queryArgs...).
		Group("audit_status").
		Find(&res).
		Error; err != nil {
		logx.E(ctx, "GetReleaseQAAuditStat data req:(versionID:%v), error:%v",
			versionID, err)
		return nil, err
	}

	stat := make(map[uint32]*releaseEntity.AuditResultStat, 0)
	for _, item := range res {
		auditStat := &releaseEntity.AuditResultStat{
			AuditStatus: item.AuditStatus,
			Total:       item.Total,
		}
		stat[item.AuditStatus] = auditStat
	}
	return stat, nil
}

// GetForbidReleaseQA 获取禁止发布+非审核失败+发布失败的问答ID
// 非审核失败的问答可以再次置为待发布
func (d *daoImpl) GetForbidReleaseQA(ctx context.Context, versionID uint64) (
	[]*releaseEntity.ReleaseQA, error) {

	/***
	`
		SELECT
			qa_id
		FROM
		    t_release_qa
		WHERE
		    version_id = ? AND is_allow_release = ? AND release_status = ? AND audit_status != ?
	`
	***/

	db := d.mysql.TReleaseQa.WithContext(ctx).Debug()

	queryCond := []gen.Condition{
		d.mysql.TReleaseQa.VersionID.Eq(versionID),
		d.mysql.TReleaseQa.IsAllowRelease.Eq(entity.ForbidRelease),
		d.mysql.TReleaseQa.ReleaseStatus.Eq(qaEntity.QAReleaseStatusFail),
		d.mysql.TReleaseQa.AuditStatus.Neq(releaseEntity.ReleaseQAAuditStatusFail),
	}

	releaseQas, err := db.Where(queryCond...).Find()
	if err != nil {
		logx.E(ctx, "GetForbidReleaseQA data req:(versionID:%v), error:%v",
			versionID, err)
		return nil, err
	}

	if len(releaseQas) == 0 {
		logx.I(ctx, "[warning] GetForbidReleaseQA data req:(versionID:%v) not found", versionID)
		return nil, nil
	}

	// qaIds := make([]uint64, 0)
	// for _, item := range releaseQas {
	// 	qaIds = append(qaIds, uint64(item.QaID))
	// }

	return BatchConvertReleaseQaDoToPo(releaseQas), nil
}

// GetAuditFailReleaseQA 获取审核失败的问答ID
func (d *daoImpl) GetAuditFailReleaseQA(ctx context.Context, versionID uint64, message string) (uint64, error) {
	/***
	`
		SELECT
			count(*)
		FROM
		    t_release_qa
		WHERE
		    version_id = ? AND release_status = ? AND audit_status = ? %s

	***/

	db := d.mysql.TReleaseQa.WithContext(ctx).Debug()

	queryCond := []gen.Condition{
		d.mysql.TReleaseQa.VersionID.Eq(versionID),
		d.mysql.TReleaseQa.ReleaseStatus.Eq(qaEntity.QAReleaseStatusFail),
		d.mysql.TReleaseQa.AuditStatus.Eq(releaseEntity.ReleaseQAAuditStatusFail),
	}

	if len(message) > 0 {
		queryCond = append(queryCond, d.mysql.TReleaseQa.Message.Eq(message))
	}

	total, err := db.Where(queryCond...).Count()
	if err != nil {
		logx.E(ctx, "GetAuditFailReleaseQA data req:(versionID:%v), error:%v",
			versionID, err)
		return 0, err
	}
	return uint64(total), nil
}

// GetAuditQAFailByQaID 根据 QaID 获取审核不通过的QA内容（Gen风格）
func (d *daoImpl) GetAuditQAFailByQaID(ctx context.Context, corpID, qaID uint64) ([]uint64, error) {
	/*
		SELECT
		  id,qa_id,question,answer,audit_status
		FROM
			t_release_qa
		WHERE
			corp_id = ? AND qa_id = ?
		ORDER BY
		  id DESC
	*/
	// 1. 初始化Gen生成的Query
	db := d.mysql.TReleaseQa.WithContext(ctx).Debug()
	// 2. 构建类型安全查询
	releaseQas, err := db.Select(
		d.mysql.TReleaseQa.ID,
	).Where(
		d.mysql.TReleaseQa.CorpID.Eq(corpID),
		d.mysql.TReleaseQa.QaID.Eq(qaID),
	).
		Order(d.mysql.TReleaseQa.ID.Desc()).
		Find()
	// 3. 错误处理
	if err != nil {
		return nil, fmt.Errorf("GetAuditQAFailByQaID failed : corpID=%d, qaID=%d, err=%v", corpID, qaID, err)
	}
	if len(releaseQas) == 0 {
		return nil, errx.ErrNotFound
	}
	rsp := make([]uint64, len(releaseQas))
	for _, v := range releaseQas {
		rsp = append(rsp, uint64(v.ID))
	}
	return rsp, nil
}

// GetAuditQAFailByVersion 获取审核失败的QA内容（Gen风格）
func (d *daoImpl) GetAuditQAFailByVersion(ctx context.Context,
	corpID, versionID uint64) ([]*releaseEntity.AuditReleaseQA, error) {
	// 1. 初始化Gen生成的Query
	db := d.mysql.TReleaseQa.WithContext(ctx).Debug()
	releaseQas, err := db.Select(
		d.mysql.TReleaseQa.ID,
		d.mysql.TReleaseQa.QaID,
		d.mysql.TReleaseQa.Question,
		d.mysql.TReleaseQa.Answer,
	).Where(
		d.mysql.TReleaseQa.CorpID.Eq(corpID),
		d.mysql.TReleaseQa.VersionID.Eq(versionID),
		d.mysql.TReleaseQa.AuditStatus.Eq(releaseEntity.ReleaseQAAuditStatusFail),
	).Find()
	// 3. 错误处理
	if err != nil {
		return nil, fmt.Errorf("GetAuditQAFailByVersion failed : corpID=%d, versionID=%d, err=%v",
			corpID, versionID, err)
	}
	// 4. 空结果处理（保持与原逻辑一致）
	if len(releaseQas) == 0 {
		return nil, nil
	}
	rsp := make([]*releaseEntity.AuditReleaseQA, len(releaseQas))
	for _, item := range releaseQas {
		auditReleaseQA := &releaseEntity.AuditReleaseQA{
			ID:       item.QaID,
			Question: item.Question,
			Answer:   item.Answer,
		}
		rsp = append(rsp, auditReleaseQA)
	}
	return rsp, nil
}
