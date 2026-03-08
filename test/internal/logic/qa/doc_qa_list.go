package qa

import (
	"context"
	"fmt"
	"strings"
	"time"

	"git.woa.com/adp/common/x/logx"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
	"git.woa.com/adp/kb/kb-config/internal/util"
)

const (
	qaFields = `
		id,business_id,robot_id,corp_id,staff_id,doc_id,origin_doc_id,segment_id,category_id,source,question,answer,
		custom_param,question_desc,release_status,is_audit_free,is_deleted,message,accept_status,next_action,
		similar_status,char_size,attr_range,create_time,update_time,expire_start,expire_end,attribute_flag,enable_scope,qa_size
	`
	qaSimilarTipsFields = ` 
		 IFNULL((select question from t_qa_similar_question where  t_qa_similar_question.robot_id = ? 
		 AND t_doc_qa.id = t_qa_similar_question.related_qa_id 
	     AND t_qa_similar_question.is_deleted = 1 
  		 ORDER BY t_qa_similar_question.create_time DESC,t_qa_similar_question.id DESC LIMIT 1 ), 
         '') as similar_question `

	getQACount = `
		SELECT
			t_doc_qa.accept_status,t_doc_qa.release_status,count(distinct t_doc_qa.id) as total
		FROM
		    t_doc_qa  %s
		WHERE
		     t_doc_qa.corp_id = ? AND t_doc_qa.robot_id = ? %s
		GROUP BY t_doc_qa.accept_status,t_doc_qa.release_status;
	`

	getQAList = `
		SELECT DISTINCT
			%s
		FROM
		    t_doc_qa %s
		WHERE
		     t_doc_qa.corp_id = ? AND t_doc_qa.robot_id = ? %s
		ORDER BY
		    t_doc_qa.update_time DESC,t_doc_qa.id DESC
		LIMIT ?,?
	` // Left JOIN t_qa_similar_question on t_doc_qa.id = t_qa_similar_question.related_qa_id

	getDocQAJoinSql = `LEFT JOIN t_qa_attribute_label ON t_doc_qa.id = t_qa_attribute_label.qa_id AND t_doc_qa.robot_id = t_qa_attribute_label.robot_id
  LEFT JOIN t_attribute_label ON t_qa_attribute_label.label_id = t_attribute_label.id
  LEFT JOIN t_attribute ON t_qa_attribute_label.attr_id = t_attribute.id`

	getDocQAUntaggedJoinSql = `LEFT JOIN t_qa_attribute_label as qa_attribute on t_doc_qa.id = qa_attribute.qa_id and  qa_attribute.robot_id = t_doc_qa.robot_id`
)

// GetQAList 获取问答对列表
func (l *Logic) GetQAList(ctx context.Context, req *qaEntity.QAListReq) ([]*qaEntity.DocQA, error) {
	condition := ""
	var args []any
	// if req.Query != "" { // 相似问tips子查询
	//	queryArg := fmt.Sprintf("%%%s%%", special.Replace(req.Query))
	//	args = append(args, queryArg)
	// }
	args = append(args, req.RobotID, req.CorpID, req.RobotID)
	if req.IsDeleted != 0 {
		condition = fmt.Sprintf("%s%s", condition, " AND t_doc_qa.is_deleted = ?")
		args = append(args, req.IsDeleted)
	}
	if req.Source != 0 {
		condition = fmt.Sprintf("%s%s", condition, " AND t_doc_qa.source = ?")
		args = append(args, req.Source)
	}
	joinSql := ""
	if req.Query != "" {
		queryArg := fmt.Sprintf("%%%s%%", util.Special.Replace(req.Query))
		if req.QueryType == docEntity.DocQueryTypeFileName && req.Query != docEntity.DocQuerySystemTypeUntagged {
			// condition = fmt.Sprintf("%s%s", condition,
			//	" AND (t_doc_qa.question LIKE ? OR t_qa_similar_question.question like ?)")
			condition = fmt.Sprintf("%s%s", condition,
				" AND (t_doc_qa.question LIKE ? )")
			args = append(args, queryArg)
		}
		if req.QueryType == docEntity.DocQueryTypeAttribute && req.Query != docEntity.DocQuerySystemTypeUntagged {
			joinSql = getDocQAJoinSql
			condition = fmt.Sprintf("%s%s", condition, " AND (t_attribute_label.name LIKE ? OR t_attribute_label.similar_label LIKE ? OR t_attribute.name LIKE ?)")
			args = append(args, queryArg, queryArg, queryArg)
		}
		if req.Query == docEntity.DocQuerySystemTypeUntagged {
			joinSql = getDocQAUntaggedJoinSql
			condition = fmt.Sprintf("%s%s", condition, " AND qa_attribute.id IS NULL")
		}
	}
	if req.QueryAnswer != "" {
		condition = fmt.Sprintf("%s%s", condition, " AND t_doc_qa.answer LIKE ?")
		args = append(args, fmt.Sprintf("%%%s%%", util.Special.Replace(req.QueryAnswer)))
	}
	if len(req.CateIDs) != 0 {
		condition = fmt.Sprintf("%s AND t_doc_qa.category_id IN (%s)", condition, util.Placeholder(len(req.CateIDs)))
		for _, cID := range req.CateIDs {
			args = append(args, cID)
		}
	}
	if len(req.QABizIDs) != 0 {
		condition = fmt.Sprintf("%s AND t_doc_qa.business_id IN (%s)", condition, util.Placeholder(len(req.QABizIDs)))
		for _, qaBizID := range req.QABizIDs {
			args = append(args, qaBizID)
		}
	}
	if len(req.AcceptStatus) != 0 {
		condition = fmt.Sprintf("%s AND t_doc_qa.accept_status IN (%s)", condition, util.Placeholder(len(req.AcceptStatus)))
		for _, acceptStatus := range req.AcceptStatus {
			args = append(args, acceptStatus)
		}
	}
	if req.ValidityStatus != 0 || len(req.ReleaseStatus) != 0 {
		c, a := l.getQaStatusConditionAndArgs(req.ReleaseStatus, req.ValidityStatus)
		condition = fmt.Sprintf("%s%s", condition, c)
		args = append(args, a...)
	}
	if len(req.DocID) != 0 {
		condition = fmt.Sprintf("%s AND t_doc_qa.doc_id IN (%s)", condition, util.Placeholder(len(req.DocID)))
		for _, id := range req.DocID {
			args = append(args, id)
		}
	}
	if len(req.ExcludeDocID) != 0 {
		condition = fmt.Sprintf("%s AND t_doc_qa.doc_id NOT IN (%s)", condition, util.Placeholder(len(req.ExcludeDocID)))
		for _, eid := range req.ExcludeDocID {
			args = append(args, eid)
		}
	}

	if req.EnableScope != nil {
		condition = fmt.Sprintf("%s AND t_doc_qa.enable_scope = ?", condition)
		args = append(args, *req.EnableScope)
	}
	pageSize := uint32(15)
	page := uint32(1)
	if req.PageSize != 0 {
		pageSize = req.PageSize
	}
	if req.Page != 0 {
		page = req.Page
	}
	offset := (page - 1) * pageSize
	args = append(args, offset, pageSize)
	// 查询字段指定表名
	qaFieldsArr := strings.Split(qaFields, ",")
	for i := range qaFieldsArr {
		qaFieldsArr[i] = "t_doc_qa." + strings.Trim(qaFieldsArr[i], " ")
	}
	qaSimilarTipsFieldsArr := qaSimilarTipsFields
	// if req.Query != "" {
	// qaSimilarTipsFieldsArr = qaSimilarTipsQueryFields
	// }
	qaFieldsArr = append(qaFieldsArr, qaSimilarTipsFieldsArr)

	querySQL := fmt.Sprintf(getQAList, strings.Join(qaFieldsArr, ","), joinSql, condition)

	logx.I(ctx, "qaSimilarTipsFieldsArr:%s", querySQL)

	filter := &qaEntity.DocQaFilter{
		RawQuery:     querySQL,
		RawQueryArgs: args,
	}

	qas, err := l.qaDao.GetDocQasByPagenation(ctx, nil, filter, true)

	if err != nil {
		return nil, err
	}

	return qas, nil
}

func (l *Logic) getQaStatusConditionAndArgs(releaseStatus []uint32, validityStatus uint32) (string, []any) {
	var c string
	var args []any
	// 勾选其他状态，未勾选已过期
	if len(releaseStatus) != 0 && validityStatus != qaEntity.QaExpiredStatus {
		c = fmt.Sprintf(` AND t_doc_qa.release_status IN (%s) AND (t_doc_qa.expire_end = ? OR t_doc_qa.expire_end >= ?) `,
			util.Placeholder(len(releaseStatus)))
		for i := range releaseStatus {
			args = append(args, releaseStatus[i])
		}
		args = append(args, time.Unix(0, 0).Format("2006-01-02 15:04:05.000"),
			time.Now().Format("2006-01-02 15:04:05.000"))
		return c, args
	}
	// 只勾选已过期
	if len(releaseStatus) == 0 && validityStatus == qaEntity.QaExpiredStatus {
		c = ` AND (t_doc_qa.expire_end > ? && t_doc_qa.expire_end < ?) `
		args = append(args, time.Unix(0, 0).Format("2006-01-02 15:04:05.000"),
			time.Now().Format("2006-01-02 15:04:05.000"))
		return c, args
	}
	// 勾选其他状态+已过期
	if len(releaseStatus) != 0 && validityStatus == qaEntity.QaExpiredStatus {
		c = fmt.Sprintf(` AND (%s OR (%s AND t_doc_qa.release_status IN (%s))) `,
			` (t_doc_qa.expire_end > ? && t_doc_qa.expire_end < ?) `,
			` (t_doc_qa.expire_end = ? OR t_doc_qa.expire_end >= ?) `, util.Placeholder(len(releaseStatus)))
		args = append(args, time.Unix(0, 0).Format("2006-01-02 15:04:05.000"),
			time.Now().Format("2006-01-02 15:04:05.000"),
			time.Unix(0, 0).Format("2006-01-02 15:04:05.000"),
			time.Now().Format("2006-01-02 15:04:05.000"))
		for i := range releaseStatus {
			args = append(args, releaseStatus[i])
		}
	}
	return c, args
}

// GetQAListCount 获取问答对列表数量
func (l *Logic) GetQAListCount(ctx context.Context, req *qaEntity.QAListReq) (uint32, uint32, uint32, uint32, error) {
	condition := ""
	var args []any
	args = append(args, req.CorpID, req.RobotID)
	if req.IsDeleted != 0 {
		condition = fmt.Sprintf("%s%s", condition, " AND t_doc_qa.is_deleted = ?")
		args = append(args, req.IsDeleted)
	}
	if req.Source != 0 {
		condition = fmt.Sprintf("%s%s", condition, " AND t_doc_qa.source = ?")
		args = append(args, req.Source)
	}
	joinSql := ""
	if req.Query != "" {
		queryArg := fmt.Sprintf("%%%s%%", util.Special.Replace(req.Query))
		if req.QueryType == docEntity.DocQueryTypeFileName && req.Query != docEntity.DocQuerySystemTypeUntagged {
			// condition = fmt.Sprintf("%s%s", condition,
			//	" AND (t_doc_qa.question LIKE ? OR t_qa_similar_question.question like ?)")
			// args = append(args, queryArg, queryArg)
			condition = fmt.Sprintf("%s%s", condition,
				" AND (t_doc_qa.question LIKE ?)")
			args = append(args, queryArg)
		}
		if req.QueryType == docEntity.DocQueryTypeAttribute && req.Query != docEntity.DocQuerySystemTypeUntagged {
			joinSql = getDocQAJoinSql
			condition = fmt.Sprintf("%s%s", condition, " AND (t_attribute_label.name LIKE ? OR t_attribute_label.similar_label LIKE ? OR t_attribute.name LIKE ?)")
			args = append(args, queryArg, queryArg, queryArg)
		}
		if req.Query == docEntity.DocQuerySystemTypeUntagged {
			joinSql = getDocQAUntaggedJoinSql
			condition = fmt.Sprintf("%s%s", condition, " AND qa_attribute.id IS NULL")
		}
	}
	if req.QueryAnswer != "" {
		condition = fmt.Sprintf("%s%s", condition, " AND t_doc_qa.answer LIKE ?")
		args = append(args, fmt.Sprintf("%%%s%%", util.Special.Replace(req.QueryAnswer)))
	}
	if len(req.CateIDs) != 0 {
		condition = fmt.Sprintf("%s AND t_doc_qa.category_id IN (%s)", condition, util.Placeholder(len(req.CateIDs)))
		for _, cID := range req.CateIDs {
			args = append(args, cID)
		}
	}
	if len(req.QABizIDs) != 0 {
		condition = fmt.Sprintf("%s AND t_doc_qa.business_id IN (%s)", condition, util.Placeholder(len(req.QABizIDs)))
		for _, qaBizID := range req.QABizIDs {
			args = append(args, qaBizID)
		}
	}
	if len(req.DocID) != 0 {
		condition = fmt.Sprintf("%s AND t_doc_qa.doc_id IN (%s)", condition, util.Placeholder(len(req.DocID)))
		for _, id := range req.DocID {
			args = append(args, id)
		}
	}
	if len(req.ExcludeDocID) != 0 {
		condition = fmt.Sprintf("%s AND t_doc_qa.doc_id NOT IN (%s)", condition, util.Placeholder(len(req.ExcludeDocID)))
		for _, eid := range req.ExcludeDocID {
			args = append(args, eid)
		}
	}
	if !req.UpdateTime.IsZero() && !req.UpdateTimeEqual {
		condition = fmt.Sprintf("%s%s", condition, " AND t_doc_qa.update_time > ?")
		args = append(args, req.UpdateTime)
	}
	if req.UpdateTimeEqual && !req.UpdateTime.IsZero() && req.QAID != 0 {
		condition = fmt.Sprintf("%s%s", condition, " AND t_doc_qa.update_time = ? AND t_doc_qa.id >= ?")
		args = append(args, req.UpdateTime, req.QAID)
	}

	if req.EnableScope != nil {
		condition = fmt.Sprintf("%s%s", condition, " AND t_doc_qa.enable_scope = ?")
		args = append(args, *req.EnableScope)
	}
	waitVerify, noAccepted, accepted, total, err := l.getQaTotalContainExpire(ctx, req, joinSql, condition, args)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	return waitVerify, noAccepted, accepted, total, nil
}

// getQaTotalContainExpire 获取问答对总数（包含过期）
func (l *Logic) getQaTotalContainExpire(ctx context.Context, req *qaEntity.QAListReq, joinSql, condition string,
	args []any) (waitVerify, noAccepted, accepted, total uint32, err error) {
	// 查询个数，没有增加筛选条件，一次查询结果，避免查询两次
	if req.ValidityStatus == 0 && len(req.ReleaseStatus) == 0 {
		tbl := l.qaDao.Query().TDocQa
		tableName := tbl.TableName()

		session := tbl.WithContext(ctx).UnderlyingDB()

		stat := make([]*qaEntity.QAStat, 0)
		querySQL := fmt.Sprintf(getQACount, joinSql, condition)

		dbRes := session.Table(tableName).Raw(querySQL, args...).Scan(&stat)
		if err = dbRes.Error; err != nil {
			logx.E(ctx, "获取问答对列表数量失败 sql:%s args:%+v err:%+v", querySQL, args, err)
			return 0, 0, 0, 0, err
		}
		waitVerify, noAccepted, accepted, total = l.getQaTotal(stat, req)
		return waitVerify, noAccepted, accepted, total, nil
	}
	// 计算总数有两部分来源计算
	// 1.未过期，sql语句会先用过期判别，其他流程按照之前老逻辑计算即可
	// 2.已过期，sql语句会先用过期判别，获取总数的逻辑，状态按照非发布状态来计算，但是需要排序如果未勾选过期状态，则计算时，只需要计算过期带校验
	// 原则，total是按照过期 状态筛选，其他状态都不需要过期时间筛选，因为sql语句无法group时间，所以只能分别获取两种条件，然后在进一步计算
	//  先算未过期的数量
	waitVerify, noAccepted, accepted, total, err = l.getNoExpireTotal(ctx, req, joinSql, condition, args)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	// 再算过期的
	eWaitVerify, eNoAccepted, eAccepted, eTotal, err := l.getExpireTotal(ctx, req, joinSql, condition, args)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	waitVerify += eWaitVerify
	noAccepted += eNoAccepted
	accepted += eAccepted
	total += eTotal
	return waitVerify, noAccepted, accepted, total, nil
}

func (l *Logic) getQaTotal(stat []*qaEntity.QAStat, req *qaEntity.QAListReq) (waitVerify, noAccepted, accepted, total uint32) {
	statMap := make(map[uint32]map[uint32]uint32, len(stat))
	for _, v := range stat {
		if _, ok := statMap[v.AcceptStatus]; !ok {
			statMap[v.AcceptStatus] = make(map[uint32]uint32, 0)
		}
		statMap[v.AcceptStatus][v.ReleaseStatus] = v.Total
		if v.AcceptStatus == qaEntity.AcceptInit {
			waitVerify += v.Total
		} else if v.AcceptStatus == qaEntity.AcceptNo {
			noAccepted += v.Total
		} else if v.AcceptStatus == qaEntity.AcceptYes && v.ReleaseStatus == qaEntity.QAReleaseStatusInit {
			accepted += v.Total
		}
		if len(req.AcceptStatus) != 0 {
			continue
		}
		if len(req.ReleaseStatus) == 0 {
			total += v.Total
			continue
		}
		for _, releaseStatus := range req.ReleaseStatus {
			if v.ReleaseStatus == releaseStatus {
				total += v.Total
			}
		}
	}
	for _, acceptStatus := range req.AcceptStatus {
		if _, ok := statMap[acceptStatus]; !ok {
			continue
		}
		if len(req.ReleaseStatus) == 0 {
			for _, t := range statMap[acceptStatus] {
				total += t
			}
		} else {
			for _, releaseStatus := range req.ReleaseStatus {
				total += statMap[acceptStatus][releaseStatus]
			}
		}
	}
	return waitVerify, noAccepted, accepted, total
}

func (l *Logic) getNoExpireTotal(ctx context.Context, req *qaEntity.QAListReq, joinSql, condition string,
	args []any) (waitVerify, noAccepted, accepted, total uint32, err error) {

	tbl := l.qaDao.Query().TDocQa
	tableName := tbl.TableName()

	session := tbl.WithContext(ctx).UnderlyingDB()

	stat := make([]*qaEntity.QAStat, 0)

	condition = fmt.Sprintf("%s%s", condition, ` AND (t_doc_qa.expire_end = ? OR t_doc_qa.expire_end >= ?) `)
	args = append(args, time.Unix(0, 0).Format("2006-01-02 15:04:05.000"),
		time.Now().Format("2006-01-02 15:04:05.000"))
	querySQL := fmt.Sprintf(getQACount, joinSql, condition)

	dbRes := session.Table(tableName).Raw(querySQL, args...).Scan(&stat)

	if err = dbRes.Error; err != nil {
		logx.E(ctx, "获取问答对列表数量失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return 0, 0, 0, 0, err
	}

	waitVerify, noAccepted, accepted, total = l.getQaNotExpireTotal(stat, req)
	return waitVerify, noAccepted, accepted, total, nil
}

func (l *Logic) getQaNotExpireTotal(stat []*qaEntity.QAStat,
	req *qaEntity.QAListReq) (waitVerify, noAccepted, accepted, total uint32) {
	statMap := make(map[uint32]map[uint32]uint32, len(stat))
	for _, v := range stat {
		if _, ok := statMap[v.AcceptStatus]; !ok {
			statMap[v.AcceptStatus] = make(map[uint32]uint32, 0)
		}
		statMap[v.AcceptStatus][v.ReleaseStatus] = v.Total
		if v.AcceptStatus == qaEntity.AcceptInit {
			waitVerify += v.Total
		} else if v.AcceptStatus == qaEntity.AcceptNo {
			noAccepted += v.Total
		} else if v.AcceptStatus == qaEntity.AcceptYes && v.ReleaseStatus == qaEntity.QAReleaseStatusInit {
			accepted += v.Total
		}
		if len(req.AcceptStatus) != 0 {
			continue
		}
		// 如果是只筛选过期问答对，则不能加总数
		if len(req.ReleaseStatus) == 0 && req.ValidityStatus != qaEntity.QaExpiredStatus {
			total += v.Total
			continue
		}
		for _, releaseStatus := range req.ReleaseStatus {
			if v.ReleaseStatus == releaseStatus {
				total += v.Total
			}
		}
	}
	for _, acceptStatus := range req.AcceptStatus {
		if _, ok := statMap[acceptStatus]; !ok {
			continue
		}
		if len(req.ReleaseStatus) == 0 && req.ValidityStatus != qaEntity.QaExpiredStatus {
			for _, t := range statMap[acceptStatus] {
				total += t
			}
		} else {
			for _, releaseStatus := range req.ReleaseStatus {
				total += statMap[acceptStatus][releaseStatus]
			}
		}
	}
	return waitVerify, noAccepted, accepted, total
}

func (l *Logic) getExpireTotal(ctx context.Context, req *qaEntity.QAListReq, joinSql, condition string,
	args []any) (waitVerify, noAccepted, accepted, total uint32, err error) {
	tbl := l.qaDao.Query().TDocQa
	tableName := tbl.TableName()

	session := tbl.WithContext(ctx).UnderlyingDB()

	stat := make([]*qaEntity.QAStat, 0)

	condition = fmt.Sprintf("%s%s", condition, ` AND (t_doc_qa.expire_end > ? && t_doc_qa.expire_end < ?) `)
	args = append(args, time.Unix(0, 0).Format("2006-01-02 15:04:05.000"),
		time.Now().Format("2006-01-02 15:04:05.000"))
	querySQL := fmt.Sprintf(getQACount, joinSql, condition)

	dbRes := session.Table(tableName).Raw(querySQL, args...).Scan(&stat)
	if err = dbRes.Error; err != nil {
		logx.E(ctx, "获取问答对列表数量失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return 0, 0, 0, 0, err
	}
	waitVerify, noAccepted, accepted, total = l.getQaExpireTotal(stat, req)
	return waitVerify, noAccepted, accepted, total, nil
}

func (l *Logic) getQaExpireTotal(stat []*qaEntity.QAStat,
	req *qaEntity.QAListReq) (waitVerify, noAccepted, accepted, total uint32) {
	statMap := make(map[uint32]map[uint32]uint32, len(stat))
	for _, v := range stat {
		if _, ok := statMap[v.AcceptStatus]; !ok {
			statMap[v.AcceptStatus] = make(map[uint32]uint32, 0)
		}
		statMap[v.AcceptStatus][v.ReleaseStatus] = v.Total
		if v.AcceptStatus == qaEntity.AcceptInit {
			waitVerify += v.Total
		} else if v.AcceptStatus == qaEntity.AcceptNo {
			noAccepted += v.Total
		} else if v.AcceptStatus == qaEntity.AcceptYes && v.ReleaseStatus == qaEntity.QAReleaseStatusInit {
			accepted += v.Total
		}
		if len(req.AcceptStatus) != 0 {
			continue
		}
		if req.ValidityStatus != qaEntity.QaUnExpiredStatus {
			total += v.Total
			continue
		}
	}
	// 如果是已过期的，同时未勾选已过期，那就不需要计算total了，默认就是0
	if req.ValidityStatus == qaEntity.QaUnExpiredStatus {
		return waitVerify, noAccepted, accepted, 0
	}
	for _, acceptStatus := range req.AcceptStatus {
		if _, ok := statMap[acceptStatus]; !ok {
			continue
		}
		if req.ValidityStatus != qaEntity.QaUnExpiredStatus {
			for _, t := range statMap[acceptStatus] {
				total += t
			}
		}
	}
	return waitVerify, noAccepted, accepted, total
}
