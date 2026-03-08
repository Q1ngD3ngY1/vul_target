package dao

import (
	"context"
	"fmt"

	"git.code.oa.com/trpc-go/trpc-database/mysql"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
)

const (
	getLikeData = `
		select sum(total_count) as total_count, sum(like_count) as like_count, sum(dislike_count) as dislike_count 
			from t_msg_record_like_count_hour 
			where day BETWEEN ? AND ?
	`

	getAnswerTypeData = `
		select sum(total_count) as total_count, sum(model_reply_count) as model_reply_count, 
		sum(knowledge_count) as knowledge_count, sum(task_flow_count) as task_flow_count, 
		sum(search_engine_count) as search_engine_count, sum(image_understanding_count) as image_understanding_count, 
		sum(reject_count) as reject_count, sum(sensitive_count) as sensitive_count, 
		sum(concurrent_limit_count) as concurrent_limit_count, sum(unknown_issues_count) as unknown_issues_count from 
		t_msg_record_reply_count_hour where day BETWEEN ? AND ?
	`
	likeDataCount = `
		SELECT CURDATE() as day, HOUR(DATE_SUB(NOW(), INTERVAL 1 HOUR)) AS hour, bot_biz_id, to_type as type,
			COUNT(*) AS total_count,
			SUM(CASE WHEN score = 1 THEN 1 ELSE 0 END) AS like_count,
			SUM(CASE WHEN score = 2 THEN 1 ELSE 0 END) AS dislike_count 
		FROM 
			t_msg_record
		WHERE 
			from_type = 65 and type = 1 and to_type in (2,5) and reply_method NOT IN (6,7)
			and create_time >= (CURDATE() + INTERVAL HOUR(NOW()) - 1 HOUR) 
			AND create_time < CURDATE() + INTERVAL HOUR(NOW()) HOUR
		group by 
			bot_biz_id,to_type
	`

	answerTypeDataCount = `
		SELECT CURDATE() as day, HOUR(DATE_SUB(NOW(), INTERVAL 1 HOUR)) AS hour, bot_biz_id, type, to_type,
			COUNT(*) AS total_count,
			SUM(CASE WHEN reply_method = 1 THEN 1 ELSE 0 END) AS model_reply_count,
			SUM(CASE WHEN reply_method in (5,12) THEN 1 ELSE 0 END) AS knowledge_count,
			SUM(CASE WHEN reply_method in (9,10) THEN 1 ELSE 0 END) AS task_flow_count,
			SUM(CASE WHEN reply_method = 11 THEN 1 ELSE 0 END) AS search_engine_count,
			SUM(CASE WHEN reply_method = 13 THEN 1 ELSE 0 END) AS image_understanding_count,
			SUM(CASE WHEN reply_method = 3 THEN 1 ELSE 0 END) AS reject_count,
			SUM(CASE WHEN reply_method = 4 THEN 1 ELSE 0 END) AS sensitive_count,
			SUM(CASE WHEN reply_method = 7 THEN 1 ELSE 0 END) AS concurrent_limit_count,
			SUM(CASE WHEN reply_method = 2 THEN 1 ELSE 0 END) AS unknown_issues_count
		FROM 
			t_msg_record
		WHERE 
			from_type = 65 and type in (1,3) and to_type in (2,5) and reply_method in (1,2,3,4,5,7,9,10,11,12,13)
			and create_time >= (CURDATE() + INTERVAL HOUR(NOW()) - 1 HOUR) 
			AND create_time < (CURDATE() + INTERVAL HOUR(NOW()) HOUR)
		group by 
			bot_biz_id,to_type,type
	`

	updateLikeDataCount = `
	   INSERT INTO
	       t_msg_record_like_count_hour (%s)
	   VALUES
	       (null,:bot_biz_id,:total_count,:like_count,:dislike_count,:type,:format_day,:hour)
		ON DUPLICATE KEY UPDATE total_count=VALUES(total_count),like_count=VALUES(like_count),
			dislike_count=VALUES(dislike_count)
	`

	// updateLikeDataCount = `
	//    INSERT INTO
	//        t_msg_record_like_count_hour (%s)
	//    VALUES
	//        (null,:bot_biz_id,:total_count,:like_count,:dislike_count,:type,:format_day,:hour)
	// `

	updateLikeDataCountFields = `
        id,bot_biz_id,total_count,like_count,dislike_count,type,day,hour
    `

	updateAnswerTypeDataCount = `
        INSERT INTO
            t_msg_record_reply_count_hour (%s)
        VALUES 
            (null, :bot_biz_id, :total_count, :model_reply_count, :knowledge_count, :task_flow_count, 
			:search_engine_count, :image_understanding_count, :reject_count, :sensitive_count, :concurrent_limit_count,
			:unknown_issues_count, :type, :to_type, :format_day, :hour)   
		ON DUPLICATE KEY UPDATE total_count=VALUES(total_count), model_reply_count=VALUES(model_reply_count), 
			knowledge_count=VALUES(knowledge_count),task_flow_count=VALUES(task_flow_count),
		search_engine_count=VALUES(search_engine_count),image_understanding_count=VALUES(image_understanding_count),
		reject_count=VALUES(reject_count),sensitive_count=VALUES(sensitive_count),
		concurrent_limit_count=VALUES(concurrent_limit_count),unknown_issues_count=VALUES(unknown_issues_count)
    `

	updateAnswerTypeDataCountFields = `
        id, bot_biz_id, total_count, model_reply_count, knowledge_count, task_flow_count, search_engine_count,
		image_understanding_count, reject_count, sensitive_count, concurrent_limit_count, unknown_issues_count,
		type , to_type, day, hour
    `
)

// GetLikeData 点赞点踩查询
func (d *dao) GetLikeData(ctx context.Context, req *model.MsgDataCountReq) (*model.LikeDataCount, error) {
	args := make([]any, 0)
	args = append(args, req.StartTime, req.EndTime)
	condition := ""
	if req.Type == 1 {
		condition = fmt.Sprintf("%s%s", condition, " AND type = ? ")
		args = append(args, 2)
	} else if req.Type == 2 {
		condition = fmt.Sprintf("%s%s", condition, " AND type = ? ")
		args = append(args, 5)
	}
	if len(req.AppBizIds) != 0 {
		condition = fmt.Sprintf("%s AND bot_biz_id IN (%s)", condition, placeholder(len(req.AppBizIds)))
		for i := range req.AppBizIds {
			args = append(args, req.AppBizIds[i])
		}
	}
	querySQL := getLikeData + condition
	data := &model.LikeDataCount{}
	log.DebugContextf(ctx, "GetLikeData sql:%s args:%+v", querySQL, args)
	if err := d.db.QueryToStruct(ctx, data, querySQL, args...); err != nil && !mysql.IsNoRowsError(err) {
		log.ErrorContextf(ctx, "GetLikeData sql err:%+v", err)
		return nil, err
	}
	return data, nil
}

// GetAnswerTypeData 回答类型数据查询
func (d *dao) GetAnswerTypeData(ctx context.Context, req *model.MsgDataCountReq) (
	*model.AnswerTypeDataCount, error) {
	args := make([]any, 0)
	args = append(args, req.StartTime, req.EndTime)
	condition := ""
	if req.Type == 1 {
		condition = fmt.Sprintf("%s%s", condition, " AND type = ?  AND to_type = ? ")
		args = append(args, 1, 2)
	} else if req.Type == 2 {
		condition = fmt.Sprintf("%s%s", condition, " AND type = ?  AND to_type = ? ")
		args = append(args, 1, 5)
	} else if req.Type == 3 {
		condition = fmt.Sprintf("%s%s", condition, " AND type = ?  AND to_type = ? ")
		args = append(args, 3, 2)
	} else if req.Type == 4 {
		condition = fmt.Sprintf("%s%s", condition, " AND type = ?  AND to_type = ? ")
		args = append(args, 3, 5)
	}
	if len(req.AppBizIds) != 0 {
		condition = fmt.Sprintf("%s AND bot_biz_id IN (%s)", condition, placeholder(len(req.AppBizIds)))
		for i := range req.AppBizIds {
			args = append(args, req.AppBizIds[i])
		}
	}
	querySQL := getAnswerTypeData + condition
	data := &model.AnswerTypeDataCount{}
	log.DebugContextf(ctx, "GetAnswerTypeData sql:%s args:%+v", querySQL, args)
	if err := d.db.QueryToStruct(ctx, data, querySQL, args...); err != nil && !mysql.IsNoRowsError(err) {
		log.ErrorContextf(ctx, "GetAnswerTypeData sql err:%+v", err)
		return nil, err
	}
	return data, nil
}

// LikeDataCount 点赞点踩统计
func (d *dao) LikeDataCount(ctx context.Context) ([]*model.LikeDataCount, error) {
	args := make([]any, 0)
	querySQL := likeDataCount
	dataList := make([]*model.LikeDataCount, 0)
	log.DebugContextf(ctx, "LikeDataCount sql:%s args:%+v", querySQL, args)
	if err := d.tdsqlRead.QueryToStructs(ctx, &dataList, querySQL, args...); err != nil && !mysql.IsNoRowsError(err) {
		log.ErrorContextf(ctx, "LikeDataCount sql err:%+v", err)
		return nil, err
	}
	log.DebugContextf(ctx, "LikeDataCount dataList:%+v", dataList)
	return dataList, nil
}

// AnswerTypeDataCount 回答类型数据统计
func (d *dao) AnswerTypeDataCount(ctx context.Context) ([]*model.AnswerTypeDataCount, error) {
	args := make([]any, 0)
	querySQL := answerTypeDataCount
	dataList := make([]*model.AnswerTypeDataCount, 0)
	log.DebugContextf(ctx, "AnswerTypeDataCount sql:%s args:%+v", querySQL, args)
	if err := d.tdsqlRead.QueryToStructs(ctx, &dataList, querySQL, args...); err != nil && !mysql.IsNoRowsError(err) {
		log.ErrorContextf(ctx, "AnswerTypeDataCount sql err:%+v", err)
		return nil, err
	}
	log.DebugContextf(ctx, "AnswerTypeDataCount dataList:%+v", dataList)
	return dataList, nil
}

// UpdateLikeDataCount 点赞点踩统计入库
func (d *dao) UpdateLikeDataCount(ctx context.Context, req []*model.LikeDataCount) error {
	insertSQL := fmt.Sprintf(updateLikeDataCount, updateLikeDataCountFields)
	log.DebugContextf(ctx, "UpdateLikeDataCount insertSQL:%s args:%+v", insertSQL, req)
	_, err := d.db.NamedExec(ctx, insertSQL, req)
	if err != nil {
		log.ErrorContextf(ctx, "UpdateLikeDataCount insertSQL err:%+v", err)
		return err
	}
	return nil
}

// UpdateAnswerTypeDataCount 回答类型数据统计入库
func (d *dao) UpdateAnswerTypeDataCount(ctx context.Context, req []*model.AnswerTypeDataCount) error {
	insertSQL := fmt.Sprintf(updateAnswerTypeDataCount, updateAnswerTypeDataCountFields)
	log.DebugContextf(ctx, "UpdateAnswerTypeDataCount insertSQL:%s args:%+v", insertSQL, req)
	_, err := d.db.NamedExec(ctx, insertSQL, req)
	if err != nil {
		log.ErrorContextf(ctx, "UpdateAnswerTypeDataCount insertSQL err:%+v", err)
		return err
	}
	return nil
}
