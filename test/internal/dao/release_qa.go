package dao

import (
	"context"
	"fmt"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/baicaoyuan/moss/utils"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
)

const (
	releaseQAFields = `
		id,robot_id,corp_id,staff_id,version_id,qa_id,doc_id,origin_doc_id,segment_id,category_id,source,question,
		answer,custom_param,question_desc,release_status,is_deleted,message,accept_status,similar_status,action,create_time,update_time,
		audit_status,audit_result,is_allow_release,attr_labels,expire_time
	`
	createReleaseQA = `
		INSERT INTO 
			t_release_qa(%s) 
		VALUES 
			(null,:robot_id,:corp_id,:staff_id,:version_id,:qa_id,:doc_id,:origin_doc_id,:segment_id,
			 :category_id,:source,:question,:answer,:custom_param,:question_desc,:release_status,:is_deleted,:message,
			 :accept_status,:similar_status,:action,:create_time,:update_time,:audit_status,:audit_result,
			 :is_allow_release,:attr_labels,:expire_time)	
	`
	getReleaseQAByVersion = `
		SELECT 
			%s 
		FROM 
		    t_release_qa 
		WHERE 
		    robot_id = ? AND version_id = ? %s 
		LIMIT ?,?
	`
	getAuditQAByVersion = `
		SELECT 
			id,question,answer 
		FROM 
		    t_release_qa 
		WHERE 
		    version_id = ? AND audit_status = ?
	`
	getAuditQAFailByVersion = `
		SELECT 
			id,qa_id,question,answer 
		FROM 
		    t_release_qa 
		WHERE 
		    corp_id = ? AND version_id = ? AND audit_status = ?
	`
	getAuditQAFailByQaID = `
		SELECT 
		  id,qa_id,question,answer,audit_status 
		FROM 
			t_release_qa 
		WHERE 
			corp_id = ? AND qa_id = ? 
		ORDER BY 
		  id DESC 
		LIMIT 1
  	`
	getReleaseQACountByVersion = `
		SELECT 
			count(*) 
		FROM 
		    t_release_qa 
		WHERE 
			 robot_id = ? AND version_id = ? %s
	`
	getReleaseModifyQA = `
		SELECT 
			%s 
		FROM 
		    t_release_qa 
		WHERE 
		    corp_id = ? AND robot_id = ? AND version_id = ? %s  
	`
	getReleaseDoc = `
		SELECT 
			DISTINCT(doc_id) 
		FROM 
		    t_release_qa 
		WHERE 
		    robot_id = ? AND version_id = ? AND corp_id = ? AND doc_id != 0  
	`
	getReleaseQAByID = `
		SELECT 
			%s 
		FROM 
		    t_release_qa 
		WHERE 
			 id = ?
	`
	releaseQAAuditPass = `
		UPDATE 
		    t_release_qa 
		SET 
		    release_status = ?,
		    audit_status = ?,
		    audit_result = ?,
		    message = ?,
		    is_allow_release = ?,
		    update_time = ? 
		WHERE 
		    id = ? AND audit_status IN (?, ?)
	`
	releaseQAAuditNotPass = `
		UPDATE 
		    t_release_qa 
		SET 
		    release_status = ?,
		    audit_status = ?,
		    audit_result = ?,
		    message = ?,
		    is_allow_release = ?,
		    update_time = ? 
		WHERE 
		    id = ? 
	`
	getReleaseQAAuditStat = `
		SELECT 
			audit_status,count(*) total 
		FROM 
		    t_release_qa 
		WHERE 
		    version_id = ? 
		GROUP BY 
		    audit_status
	`
	publishReleaseQA = `
		UPDATE 
			t_release_qa 
		SET 
		    update_time = :update_time, 
		    release_status = :release_status,
		    message = :message 
		WHERE 
		    id = :id
	`
	getForbidReleaseQA = `
		SELECT 
			qa_id 
		FROM 
		    t_release_qa 
		WHERE 
		    version_id = ? AND is_allow_release = ? AND release_status = ? AND audit_status != ?
	`
	getAuditFailReleaseQA = `
		SELECT 
			count(*) 
		FROM 
		    t_release_qa 
		WHERE 
		    version_id = ? AND release_status = ? AND audit_status = ? %s
	`
)

// GetModifyQACount 获取版本改动QA数量
func (d *dao) GetModifyQACount(ctx context.Context, robotID, versionID uint64, question string, actions []uint32,
	releaseStatuses []uint32) (
	uint64, error) {
	args := make([]any, 0, 3+len(actions)+len(releaseStatuses))
	args = append(args, robotID, versionID)
	condition := ""
	if question != "" {
		condition = fmt.Sprintf("%s%s", condition, " AND question LIKE ? ")
		args = append(args, fmt.Sprintf("%%%s%%", special.Replace(question)))
	}
	if len(actions) > 0 {
		condition = fmt.Sprintf("%s AND action IN (%s)", condition, placeholder(len(actions)))
		for _, action := range actions {
			args = append(args, action)
		}
	}
	if len(releaseStatuses) > 0 {
		condition = fmt.Sprintf("%s AND release_status IN (%s)", condition, placeholder(len(releaseStatuses)))
		for _, releaseStatus := range releaseStatuses {
			args = append(args, releaseStatus)
		}
	}
	var total uint64
	querySQL := fmt.Sprintf(getReleaseQACountByVersion, condition)
	if err := d.db.Get(ctx, &total, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取版本改动QA数量失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return 0, err
	}
	return total, nil
}

// GetModifyQAList 获取版本改动QA范围
func (d *dao) GetModifyQAList(ctx context.Context, robotID, versionID uint64, question string, actions []uint32,
	page, pageSize uint32, orderBy string, releaseStatuses []uint32) ([]*model.ReleaseQA, error) {
	var args []any
	args = append(args, robotID, versionID)
	condition := ""
	if len(releaseStatuses) > 0 {
		condition = fmt.Sprintf("%s AND release_status IN (%s)", condition, placeholder(len(releaseStatuses)))
		for _, releaseStatus := range releaseStatuses {
			args = append(args, releaseStatus)
		}
	}
	if question != "" {
		condition = fmt.Sprintf("%s%s", condition, " AND question LIKE ? ")
		args = append(args, fmt.Sprintf("%%%s%%", special.Replace(question)))
	}
	if len(actions) > 0 {
		condition = fmt.Sprintf("%s AND action IN (%s)", condition, placeholder(len(actions)))
		for _, action := range actions {
			args = append(args, action)
		}
	}
	if orderBy != "" {
		condition = fmt.Sprintf("%s%s", condition, orderBy)
	}
	offset := (page - 1) * pageSize
	args = append(args, offset, pageSize)
	querySQL := fmt.Sprintf(getReleaseQAByVersion, releaseQAFields, condition)
	modifyQas := make([]*model.ReleaseQA, 0)
	if err := d.db.QueryToStructs(ctx, &modifyQas, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取版本改动QA范围失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	return modifyQas, nil
}

// GetAuditQAByVersion 获取要审核的QA内容
func (d *dao) GetAuditQAByVersion(ctx context.Context, versionID uint64) ([]*model.AuditReleaseQA, error) {
	args := make([]any, 0, 2)
	args = append(args, versionID, model.ReleaseQAAuditStatusDoing)
	querySQL := getAuditQAByVersion
	modifyQas := make([]*model.AuditReleaseQA, 0)
	if err := d.db.QueryToStructs(ctx, &modifyQas, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取要审核的QA内容失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	return modifyQas, nil
}

// GetAuditQAFailByVersion 获取机器审核审核不通过的QA内容
func (d *dao) GetAuditQAFailByVersion(ctx context.Context, corpID, robotID, versionID uint64) ([]*model.AuditReleaseQA,
	error) {
	args := make([]any, 0, 2)
	args = append(args, corpID, versionID, model.ReleaseQAAuditStatusFail)
	querySQL := getAuditQAFailByVersion
	modifyQas := make([]*model.AuditReleaseQA, 0)
	if err := d.db.QueryToStructs(ctx, &modifyQas, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取要审核的QA内容失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	if len(modifyQas) == 0 {
		return nil, nil
	}
	return modifyQas, nil
}

// GetAuditQAFailByQaID 根据 QaID 获取机器审核审核不通过的QA内容
func (d *dao) GetAuditQAFailByQaID(ctx context.Context, corpID, robotID, qaID uint64) ([]*model.AuditReleaseQA, error) {
	args := make([]any, 0, 2)
	args = append(args, corpID, qaID)
	querySQL := getAuditQAFailByQaID
	modifyQas := make([]*model.AuditReleaseQA, 0)
	if err := d.db.QueryToStructs(ctx, &modifyQas, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取要审核的QA内容失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	return modifyQas, nil
}

// GetReleaseModifyQA 获取版本改动的QA
func (d *dao) GetReleaseModifyQA(ctx context.Context, release *model.Release, qas []*model.DocQA) (
	map[uint64]*model.ReleaseQA, error) {
	args := make([]any, 0, 3+len(qas))
	args = append(args, release.CorpID, release.RobotID, release.ID)
	condition := "AND 1=1"
	if len(qas) > 0 {
		condition = fmt.Sprintf("AND qa_id IN (%s)", placeholder(len(qas)))
		for _, qa := range qas {
			args = append(args, qa.ID)
		}
	}
	querySQL := fmt.Sprintf(getReleaseModifyQA, releaseQAFields, condition)
	list := make([]*model.ReleaseQA, 0)
	if err := d.db.QueryToStructs(ctx, &list, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取版本改动的QA失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	modifyQA := make(map[uint64]*model.ReleaseQA, 0)
	for _, item := range list {
		modifyQA[item.QAID] = item
	}
	return modifyQA, nil
}

// GetReleaseDoc 获取版本改动的文档ID
func (d *dao) GetReleaseDoc(ctx context.Context, release *model.Release) (map[uint64]struct{}, error) {
	querySQL := getReleaseDoc
	args := make([]any, 0, 2)
	args = append(args, release.RobotID, release.ID, release.CorpID)
	list := make([]*model.ReleaseDocID, 0)
	if err := d.db.QueryToStructs(ctx, &list, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取版本改动的文档ID失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	docID := make(map[uint64]struct{}, 0)
	for _, item := range list {
		docID[item.DocID] = struct{}{}
	}
	list = make([]*model.ReleaseDocID, 0)
	querySQL = getReleaseSegment
	if err := d.db.QueryToStructs(ctx, &list, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取版本改动的文档ID失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	for _, item := range list {
		docID[item.DocID] = struct{}{}
	}
	return docID, nil
}

// GetReleaseQAByID 获取发布的QA
func (d *dao) GetReleaseQAByID(ctx context.Context, id uint64) (*model.ReleaseQA, error) {
	args := make([]any, 0, 1)
	args = append(args, id)
	querySQL := fmt.Sprintf(getReleaseQAByID, releaseQAFields)
	list := make([]*model.ReleaseQA, 0)
	if err := d.db.QueryToStructs(ctx, &list, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取发布的QA失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	if len(list) == 0 {
		return nil, nil
	}
	return list[0], nil
}

// GetReleaseQAAuditStat 统计QA审核
func (d *dao) GetReleaseQAAuditStat(ctx context.Context, versionID uint64) (map[uint32]*model.AuditResultStat, error) {
	args := make([]any, 0, 1)
	args = append(args, versionID)
	querySQL := getReleaseQAAuditStat
	list := make([]*model.AuditResultStat, 0)
	if err := d.db.QueryToStructs(ctx, &list, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "统计QA审核失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	stat := make(map[uint32]*model.AuditResultStat, 0)
	for _, item := range list {
		stat[item.AuditStatus] = item
	}
	return stat, nil
}

// GetForbidReleaseQA 获取禁止发布+非审核失败+发布失败的问答ID
// 非审核失败的问答可以再次置为待发布
func (d *dao) GetForbidReleaseQA(ctx context.Context, versionID uint64) ([]uint64, error) {
	querySQL := getForbidReleaseQA
	args := make([]any, 0, 3)
	args = append(args, versionID, model.ForbidRelease, model.QAReleaseStatusFail, model.ReleaseQAAuditStatusFail)
	list := make([]*model.ForbidReleaseQA, 0)
	if err := d.db.QueryToStructs(ctx, &list, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取禁止发布的问答ID失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	ids := make([]uint64, 0)
	for _, item := range list {
		ids = append(ids, item.QAID)
	}
	return ids, nil
}

// GetAuditFailReleaseQA 获取审核失败的问答ID
func (d *dao) GetAuditFailReleaseQA(ctx context.Context, versionID uint64, message string) (uint64, error) {
	args := make([]any, 0, 4)
	args = append(args, versionID, model.QAReleaseStatusFail, model.ReleaseQAAuditStatusFail)
	if message != "" {
		args = append(args, message)
	}
	querySQL := fmt.Sprintf(getAuditFailReleaseQA, utils.When(message != "", " AND message = ?", ""))
	var total uint64
	if err := d.db.Get(ctx, &total, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取版本改动QA数量失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return 0, err
	}
	return total, nil
}
