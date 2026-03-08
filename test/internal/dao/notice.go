package dao

import (
	"context"
	"fmt"
	"git.code.oa.com/trpc-go/trpc-go/client"
	knowClient "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"github.com/jmoiron/sqlx"
)

const (
	noticeField = `
		id,typ,relate_id,corp_id,robot_id,staff_id,is_global,page_id,level,subject,content,operation,is_read,
		is_closed,is_allow_close,update_time,create_time
	`
	createNotice = `
		INSERT INTO 
			t_notice (%s) 
		VALUES 
		    (null,:typ,:relate_id,:corp_id,:robot_id,:staff_id,:is_global,:page_id,:level,:subject,:content,
			:operation,:is_read,:is_closed,:is_allow_close,:update_time,:create_time)
	`
	closeNotice = `
		UPDATE
			t_notice 
		SET
			is_closed = ?,
			update_time = ? 
		WHERE 
			typ = ? 
			AND relate_id = ? 
		  	AND corp_id = ? 
		    AND robot_id = ? 
		  	AND staff_id = ? 
		  	AND is_allow_close = ? 
			AND is_closed = ? 
	`
	getAllowClosedNoticeIDs = `
		SELECT 
			id
		FROM 
		    t_notice 
		WHERE 
		    typ = ? 
			AND relate_id = ? 
		  	AND corp_id = ? 
		    AND robot_id = ? 
		  	AND staff_id = ? 
		  	AND is_allow_close = ? 
			AND is_closed = ?
		
	`
	getNoticeByIDs = `
		SELECT 
			%s	
		FROM 
		    t_notice 
		WHERE 
		    id IN (%s) 
    `
	closeNoticeByID = `
		UPDATE 
			t_notice 
		SET 
			is_closed = ?,
			update_time = ? 
		WHERE 
		    is_allow_close = ? AND id IN (%s)
	`
	readNoticeByID = `
		UPDATE 
			t_notice 
		SET 
			is_read = ?,
			update_time = ? 
		WHERE 
		    id IN (%s)
	`
	getPageNotice = `
		SELECT 
			%s	
		FROM 
		    t_notice 
		WHERE 
		    corp_id = ? AND staff_id = ? AND robot_id = ? AND page_id = ? AND is_closed = ? 
		ORDER BY 
		    id DESC 
		LIMIT 5
	`
	getLastReadNotice = `
		SELECT 
			%s	
		FROM 
		    t_notice 
		WHERE 
		    corp_id = ? AND staff_id = ? AND robot_id = ? AND is_read = ? 
		ORDER BY 
		    id DESC 
		LIMIT 1
	`
	getCursorNotice = `
		SELECT 
			%s	
		FROM 
		    t_notice 
		WHERE 
		    corp_id = ? AND staff_id = ? AND robot_id = ? AND is_global = ? AND is_read = ? AND id > ? 
		ORDER BY 
		    id DESC
    `
	getCenterNotice = `
		SELECT 
			%s	
		FROM 
		    t_notice 
		WHERE 
		    corp_id = ? AND staff_id = ? AND robot_id = ? AND is_global = ? AND id > ? 
		ORDER BY 
		    id DESC
    `
	getHistoryNoticeCount = `
		SELECT 
			COUNT(*) 
		FROM 
		    t_notice 
		WHERE 
		    corp_id = ? AND staff_id = ? AND robot_id = ? AND is_global = ? 
    `
	getHistoryNotice = `
		SELECT 
			%s	
		FROM 
		    t_notice 
		WHERE 
		    corp_id = ? AND staff_id = ? AND robot_id = ? AND is_global = ? %s
		ORDER BY 
		    id DESC 
		LIMIT 
			?
	`
	getUnreadTotal = `
		SELECT
			COUNT(*) 
		FROM 
		    t_notice 
		WHERE 
		    corp_id = ? AND staff_id = ? AND robot_id = ? AND is_global = ? AND is_read = ?
    `
)

const (
	noticeTableName = "t_notice"
)

// CreateNotice 创建通知
func (d *dao) CreateNotice(ctx context.Context, notice *model.Notice) error {
	if err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		now := time.Now()
		notice.UpdateTime = now
		notice.CreateTime = now
		// 关闭之前的通知
		querySQL := getAllowClosedNoticeIDs
		args := make([]any, 0, 9)
		args = append(args, notice.Type, notice.RelateID, notice.CorpID, notice.RobotID,
			notice.StaffID, model.NoticeIsForbidClose, model.NoticeOpen)
		allowClosedNotices := make([]*model.Notice, 0)
		if err := tx.SelectContext(ctx, &allowClosedNotices, querySQL, args...); err != nil {
			log.ErrorContextf(ctx, "查询通知失败 sql:%s args:%+v err:%+v", querySQL, args, err)
			return err
		}
		if len(allowClosedNotices) > 0 {
			updateSQL := fmt.Sprintf(closeNoticeByID, placeholder(len(allowClosedNotices)))
			updateArgs := make([]any, 0, 3+len(allowClosedNotices))
			updateArgs = append(updateArgs, model.NoticeClosed, now, model.NoticeIsForbidClose)
			for _, v := range allowClosedNotices {
				updateArgs = append(updateArgs, v.ID)
			}
			if _, err := tx.ExecContext(ctx, updateSQL, updateArgs...); err != nil {
				log.ErrorContextf(ctx, "关闭通知失败 sql:%s args:%+v err:%+v", updateSQL, updateArgs, err)
				return err
			}
		}
		querySQL = fmt.Sprintf(createNotice, noticeField)
		if _, err := tx.NamedExecContext(ctx, querySQL, notice); err != nil {
			log.ErrorContextf(ctx, "创建通知失败 sql:%s args:%+v err:%+v", querySQL, notice, err)
			return err
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "创建通知失败 err:%+v", err)
		return err
	}
	return nil
}

// CreateNoticex 创建通知
func (d *dao) CreateNoticex(ctx context.Context, tx *sqlx.Tx, notice *model.Notice) error {
	now := time.Now()
	notice.UpdateTime = now
	notice.CreateTime = now
	// 关闭之前的通知
	querySQL := getAllowClosedNoticeIDs
	args := make([]any, 0, 9)
	args = append(args, notice.Type, notice.RelateID, notice.CorpID, notice.RobotID,
		notice.StaffID, model.NoticeIsForbidClose, model.NoticeOpen)
	allowClosedNotices := make([]*model.Notice, 0)
	db := knowClient.DBClient(ctx, noticeTableName, notice.RobotID, []client.Option{}...)
	if err := db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		if err := tx.SelectContext(ctx, &allowClosedNotices, querySQL, args...); err != nil {
			log.ErrorContextf(ctx, "查询通知失败 sql:%s args:%+v err:%+v", querySQL, args, err)
			return err
		}
		if len(allowClosedNotices) > 0 {
			updateSQL := fmt.Sprintf(closeNoticeByID, placeholder(len(allowClosedNotices)))
			updateArgs := make([]any, 0, 3+len(allowClosedNotices))
			updateArgs = append(updateArgs, model.NoticeClosed, now, model.NoticeIsForbidClose)
			for _, v := range allowClosedNotices {
				updateArgs = append(updateArgs, v.ID)
			}
			if _, err := tx.ExecContext(ctx, updateSQL, updateArgs...); err != nil {
				log.ErrorContextf(ctx, "关闭通知失败 sql:%s args:%+v err:%+v", updateSQL, updateArgs, err)
				return err
			}
		}
		querySQL = fmt.Sprintf(createNotice, noticeField)
		if _, err := tx.NamedExecContext(ctx, querySQL, notice); err != nil {
			log.ErrorContextf(ctx, "创建通知失败 sql:%s args:%+v err:%+v", querySQL, notice, err)
			return err
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "创建通知失败 err:%+v", err)
		return err
	}
	return nil
}

func (d *dao) createNotice(ctx context.Context, tx *sqlx.Tx, notice *model.Notice) error {
	log.DebugContextf(ctx, "createNotice:%+v", notice)
	now := time.Now()
	// 关闭之前的通知
	querySQL := getAllowClosedNoticeIDs
	args := make([]any, 0, 9)
	args = append(args, notice.Type, notice.RelateID, notice.CorpID, notice.RobotID,
		notice.StaffID, model.NoticeIsForbidClose, model.NoticeOpen)
	allowClosedNotices := make([]*model.Notice, 0)
	db := knowClient.DBClient(ctx, noticeTableName, notice.RobotID, []client.Option{}...)
	if err := db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		if err := tx.SelectContext(ctx, &allowClosedNotices, querySQL, args...); err != nil {
			log.ErrorContextf(ctx, "查询通知失败 sql:%s args:%+v err:%+v", querySQL, args, err)
			return err
		}
		if len(allowClosedNotices) > 0 {
			updateSQL := fmt.Sprintf(closeNoticeByID, placeholder(len(allowClosedNotices)))
			updateArgs := make([]any, 0, 3+len(allowClosedNotices))
			updateArgs = append(updateArgs, model.NoticeClosed, now, model.NoticeIsForbidClose)
			for _, v := range allowClosedNotices {
				updateArgs = append(updateArgs, v.ID)
			}
			if _, err := tx.ExecContext(ctx, updateSQL, updateArgs...); err != nil {
				log.ErrorContextf(ctx, "关闭通知失败 sql:%s args:%+v err:%+v", updateSQL, updateArgs, err)
				return err
			}
		}
		// 创建新的通知
		querySQL = fmt.Sprintf(createNotice, noticeField)
		if _, err := tx.NamedExecContext(ctx, querySQL, notice); err != nil {
			log.ErrorContextf(ctx, "创建通知失败 sql:%s args:%+v err:%+v", querySQL, notice, err)
			return err
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "创建通知失败 err:%+v", err)
		return err
	}
	return nil
}

// GetNoticeByIDs 通过ID获取通知
func (d *dao) GetNoticeByIDs(ctx context.Context, ids []uint64) ([]*model.Notice, error) {
	list := make([]*model.Notice, 0)
	if len(ids) == 0 {
		return list, nil
	}
	querySQL := fmt.Sprintf(getNoticeByIDs, noticeField, placeholder(len(ids)))
	args := make([]any, 0, len(ids))
	for _, id := range ids {
		args = append(args, id)
	}
	if err := d.db.QueryToStructs(ctx, &list, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "通过ID获取通知失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	return list, nil
}

// CloseNoticeByID 关闭通知
func (d *dao) CloseNoticeByID(ctx context.Context, ids []uint64) error {
	if len(ids) == 0 {
		return nil
	}
	now := time.Now()
	querySQL := fmt.Sprintf(closeNoticeByID, placeholder(len(ids)))
	args := make([]any, 0, 3+len(ids))
	args = append(args, model.NoticeClosed, now, model.NoticeIsAllowClose)
	for _, id := range ids {
		args = append(args, id)
	}
	if _, err := d.db.Exec(ctx, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "关闭通知失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return err
	}
	return nil
}

// ReadNoticeByID 已读通知
func (d *dao) ReadNoticeByID(ctx context.Context, ids []uint64) error {
	if len(ids) == 0 {
		return nil
	}
	now := time.Now()
	querySQL := fmt.Sprintf(readNoticeByID, placeholder(len(ids)))
	args := make([]any, 0, 2+len(ids))
	args = append(args, model.NoticeRead, now)
	for _, id := range ids {
		args = append(args, id)
	}
	if _, err := d.db.Exec(ctx, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "已读通知失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return err
	}
	return nil
}

// GetPageNotice 获取页面通知列表
func (d *dao) GetPageNotice(ctx context.Context, corpID, staffID, robotID uint64, pageID uint32) (
	[]*model.Notice, error) {
	list := make([]*model.Notice, 0)
	if pageID == 0 {
		return list, nil
	}
	querySQL := fmt.Sprintf(getPageNotice, noticeField)
	args := make([]any, 0, 5)
	args = append(args, corpID, staffID, robotID, pageID, model.NoticeOpen)
	if err := d.db.QueryToStructs(ctx, &list, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取页面通知列表失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	return list, nil
}

// GetLastReadNotice 最新一条已读信息
func (d *dao) GetLastReadNotice(ctx context.Context, corpID, staffID, robotID uint64) (*model.Notice, error) {
	querySQL := fmt.Sprintf(getLastReadNotice, noticeField)
	args := make([]any, 0, 4)
	args = append(args, corpID, staffID, robotID, model.NoticeRead)
	list := make([]*model.Notice, 0)
	if err := d.db.QueryToStructs(ctx, &list, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "最新一条已读信息失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	if len(list) == 0 {
		return nil, nil
	}
	return list[0], nil
}

// GetCursorNotice 获取游标通知列表
func (d *dao) GetCursorNotice(ctx context.Context, corpID, staffID, robotID, cursor uint64) ([]*model.Notice, error) {
	querySQL := fmt.Sprintf(getCursorNotice, noticeField)
	args := make([]any, 0, 6)
	args = append(args, corpID, staffID, robotID, model.NoticeIsGlobal, model.NoticeUnread, cursor)
	list := make([]*model.Notice, 0)
	if err := d.db.QueryToStructs(ctx, &list, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取游标通知列表失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	return list, nil
}

// GetCenterNotice 获取通知中心最新消息
func (d *dao) GetCenterNotice(ctx context.Context, corpID, staffID, robotID, cursor uint64) ([]*model.Notice, error) {
	querySQL := fmt.Sprintf(getCenterNotice, noticeField)
	args := make([]any, 0, 5)
	args = append(args, corpID, staffID, robotID, model.NoticeIsGlobal, cursor)
	list := make([]*model.Notice, 0)
	if err := d.db.QueryToStructs(ctx, &list, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取通知中心最新消息失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	return list, nil
}

// GetHistoryNoticeCount 获取通知中心列表数量
func (d *dao) GetHistoryNoticeCount(ctx context.Context, corpID, staffID, robotID uint64) (uint64, error) {
	querySQL := getHistoryNoticeCount
	args := make([]any, 0, 4)
	args = append(args, corpID, staffID, robotID, model.NoticeIsGlobal)
	var total uint64
	if err := d.db.Get(ctx, &total, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取通知中心列表数量失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return 0, err
	}
	return total, nil
}

// GetHistoryNotice 获取通知中心列表
func (d *dao) GetHistoryNotice(ctx context.Context, corpID, staffID, robotID, id uint64, limit uint32) (
	[]*model.Notice, error) {
	args := make([]any, 0, 6)
	args = append(args, corpID, staffID, robotID, model.NoticeIsGlobal)
	condition := ""
	if id != 0 {
		condition = fmt.Sprintf("%s%s", condition, " AND id < ?")
		args = append(args, id)
	}
	args = append(args, limit)
	querySQL := fmt.Sprintf(getHistoryNotice, noticeField, condition)
	list := make([]*model.Notice, 0)
	if err := d.db.QueryToStructs(ctx, &list, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取通知中心列表失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	return list, nil
}

// GetUnreadTotal 未读数量
func (d *dao) GetUnreadTotal(ctx context.Context, corpID, staffID, robotID uint64) (uint64, error) {
	querySQL := getUnreadTotal
	args := make([]any, 0, 5)
	args = append(args, corpID, staffID, robotID, model.NoticeIsGlobal, model.NoticeUnread)
	var total uint64
	if err := d.db.Get(ctx, &total, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "GetUnreadTotal失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return 0, err
	}
	return total, nil
}

// SendRetryPausedReleaseNotice 重试发布发送发布中消息
func (d *dao) SendRetryPausedReleaseNotice(ctx context.Context, release *model.Release) error {
	// 发布状态设置为发布中，并且页面通知发布中
	if err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		querySQL := updateReleaseStatus
		release.Status = model.ReleaseStatusPending
		release.Message = "发布暂停重试中"
		release.UpdateTime = time.Now()
		if _, err := tx.NamedExecContext(ctx, querySQL, release); err != nil {
			log.ErrorContextf(ctx, "重试暂停的发布失败 sql:%s args:%+v err:%+v", querySQL, release, err)
			return err
		}
		if err := d.sendNotifyReleasing(ctx, tx, release); err != nil {
			return err
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "重试暂停的发布失败 err:%+v", err)
		return err
	}
	return nil
}
