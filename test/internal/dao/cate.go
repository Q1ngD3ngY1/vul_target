package dao

import (
	"context"
	"errors"
	"fmt"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"strings"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/client"
	knowClient "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"

	"github.com/jmoiron/sqlx"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
)

const (
	cateFields = `
		id,business_id,robot_id,corp_id,name,order_num,is_deleted,parent_id,create_time,update_time
	`
	createCate = `
		INSERT INTO
		    {{CATEGORY_TABLE}} (%s)
		VALUES 
		    (null,:business_id,:robot_id,:corp_id,:name,:order_num,:is_deleted,:parent_id,:create_time,:update_time)
	`
	updateCate = `
		UPDATE 
		    {{CATEGORY_TABLE}} 
		SET 
		    name = ?, update_time = NOW()
		WHERE 
		    id = ?
	`
	deleteCate = `
		UPDATE 
		    {{CATEGORY_TABLE}} 
		SET 
		    is_deleted = ?, update_time = NOW()
		WHERE 
		    id IN (%s)
	`
	getCateList = `
		SELECT 
		    %s 
		FROM 
		    {{CATEGORY_TABLE}} 
		WHERE 
		    corp_id = ? AND robot_id = ? AND is_deleted = ?
		ORDER BY 
		    order_num DESC 	
	`
	getCateByID = `
		SELECT 
		    %s 
		FROM 
		    {{CATEGORY_TABLE}} 
		WHERE 
		    corp_id = ? AND robot_id = ? AND id = ?
	`
	getCateByIDs = `
		SELECT 
		    %s 
		FROM 
		    {{CATEGORY_TABLE}} 
		WHERE 
		    id IN (%s)
	`
	getCateByBusinessID = `
		SELECT 
		    %s 
		FROM 
		    {{CATEGORY_TABLE}} 
		WHERE 
		    corp_id = ? AND robot_id = ? AND business_id = ?
	`
	getRobotUncategorizedCateID = `
		SELECT 
		    %s 
		FROM 
		    {{CATEGORY_TABLE}} 
		WHERE 
		    corp_id = ? AND robot_id = ? AND parent_id = 0 AND name = ?
	`
	getCateListByBusinessIDs = `
		SELECT 
		    %s 
		FROM 
		    {{CATEGORY_TABLE}} 
		WHERE 
		    corp_id = ? AND robot_id = ? AND business_id IN (%s)
	`

	batchUpdateQACate = `
		UPDATE
			t_doc_qa
		SET
		    category_id = ?, update_time = NOW()
		WHERE
		    robot_id = ? AND category_id IN (%s)
	`

	batchUpdateDocCate = `
		UPDATE
			t_doc
		SET
		    category_id = ?, update_time = NOW()
		WHERE
		    robot_id = ? AND category_id IN (%s)
	`

	batchUpdateSynonymsCate = `
		UPDATE
			t_synonyms
		SET
		    category_id = ?, update_time = NOW()
		WHERE
		    robot_id = ? AND category_id IN (%s)
	`

	getQACateStat = `
		SELECT
		    category_id,count(*) as total
		FROM
		    t_doc_qa
		WHERE
		    corp_id = ? AND robot_id = ? AND is_deleted = ? AND accept_status = ?
		GROUP BY
		    category_id
	`

	getDocCateStat = `
		SELECT
		    category_id, count(*) as total
		FROM
		    t_doc
		WHERE
		    corp_id = ? AND robot_id = ? AND is_deleted = ? AND opt = ?
		GROUP BY
		    category_id
	`

	getSynonymsCateStat = `
		SELECT
		    category_id, COUNT(DISTINCT CASE WHEN parent_id = 0 THEN synonyms_id ELSE parent_id END) AS total
		FROM
		    t_synonyms
		WHERE
		    corp_id = ? AND robot_id = ? AND is_deleted = ?
		GROUP BY
		    category_id
	`

	groupQA = `
		UPDATE
			t_doc_qa
		SET
		    category_id = ?, update_time = ?
		WHERE
		    id IN (%s)
	`

	groupDoc = `
		UPDATE
			t_doc
		SET
		    category_id = ?, update_time = ?
		WHERE
		    id IN (%s)
	`

	groupSynonyms = ` 
		UPDATE
			t_synonyms
		SET
		    category_id = ?, update_time = ?
		WHERE
		    synonyms_id IN (%s)
	`

	groupSynonymsNotStandard = ` 
		UPDATE
			t_synonyms
		SET
		    category_id = ?, update_time = ?
		WHERE
		   parent_id IN (%s)
	`
)

const (
	cateTablePlaceholder  = "{{CATEGORY_TABLE}}"
	cateTableNameQA       = "t_doc_qa_category"
	cateTableNameDoc      = "t_doc_category"
	cateTableNameSynonyms = "t_synonyms_category"
)

func getCateSql(sql string, t model.CateObjectType) string {
	var tbl string
	if t == model.QACate {
		tbl = cateTableNameQA
	} else if t == model.DocCate {
		tbl = cateTableNameDoc
	} else if t == model.SynonymsCate {
		tbl = cateTableNameSynonyms
	} else {
		// 未知分类,默认使用doc_qa的分类表
		tbl = cateTableNameQA
	}
	return strings.Replace(sql, cateTablePlaceholder, tbl, -1)
}

// CreateCate 新增Cate
func (d *dao) CreateCate(ctx context.Context, t model.CateObjectType, cate *model.CateInfo) (uint64,
	error) {
	cate.UpdateTime = time.Now()
	cate.CreateTime = time.Now()
	sql := fmt.Sprintf(getCateSql(createCate, t), cateFields)
	r, err := d.db.NamedExec(ctx, sql, cate)
	if err != nil {
		log.ErrorContextf(ctx, "新增分类失败, sql: %s, args: %+v, err: %+v", sql, cate, err)
		return 0, err
	}
	id, _ := r.LastInsertId()
	return uint64(id), nil
}

// UpdateCate 更新分类
func (d *dao) UpdateCate(ctx context.Context, t model.CateObjectType, id uint64, name string) error {
	sql := getCateSql(updateCate, t)
	args := []any{name, id}
	if _, err := d.db.Exec(ctx, sql, args...); err != nil {
		log.ErrorContextf(ctx, "更新分类失败, sql: %s, args: %+v, err: %+v", sql, args, err)
		return err
	}
	return nil
}

// DeleteCate 删除分类
func (d *dao) DeleteCate(ctx context.Context, t model.CateObjectType, cateIDs []uint64,
	uncategorizedCateID uint64, robotID uint64) error {
	if len(cateIDs) == 0 {
		return nil
	}
	if err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		args := make([]any, 0, len(cateIDs)+1)
		args = append(args, model.CateIsDeleted)
		for _, id := range cateIDs {
			args = append(args, id)
		}
		sql := fmt.Sprintf(getCateSql(deleteCate, t), placeholder(len(cateIDs)))
		if _, err := tx.ExecContext(ctx, sql, args...); err != nil {
			log.ErrorContextf(ctx, "删除分类失败, sql: %s, args: %+v, err: %+v", sql, args, err)
			return err
		}
		// 删除分类时,该分类下的Object移动到"未分类"目录下
		batchUpdateSql := batchUpdateQACate
		if t == model.QACate {
			batchUpdateSql = batchUpdateQACate
		} else if t == model.DocCate {
			batchUpdateSql = batchUpdateDocCate
		} else if t == model.SynonymsCate {
			batchUpdateSql = batchUpdateSynonymsCate
		}
		args = make([]any, 0, len(cateIDs)+2)
		args = append(args, uncategorizedCateID, robotID)
		for _, id := range cateIDs {
			args = append(args, id)
		}
		sql = fmt.Sprintf(batchUpdateSql, placeholder(len(cateIDs)))
		log.DebugContextf(ctx, "batchUpdate cate object sql: %s", sql)
		if _, err := tx.ExecContext(ctx, sql, args...); err != nil {
			log.ErrorContextf(ctx, "更新分类失败, sql: %s, args: %+v, err: %+v", sql, args, err)
			return err
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "删除分类失败 err:%+v", err)
		return err
	}
	return nil
}

// GetCateList 获取分类列表
func (d *dao) GetCateList(ctx context.Context, t model.CateObjectType, corpID, robotID uint64) ([]*model.CateInfo,
	error) {
	args := []any{corpID, robotID, model.CateIsNotDeleted}
	sql := fmt.Sprintf(getCateSql(getCateList, t), cateFields)
	var cates []*model.CateInfo
	if err := d.db.Select(ctx, &cates, sql, args...); err != nil {
		log.ErrorContextf(ctx, "获取问答分类列表失败, sql: %s, args: %+v, err: %+v", sql, args, err)
		return nil, err
	}
	return cates, nil
}

// GetCateByID 获取Cate详情
func (d *dao) GetCateByID(ctx context.Context, t model.CateObjectType, id, corpID, robotID uint64) (*model.CateInfo,
	error) {
	querySQL := fmt.Sprintf(getCateSql(getCateByID, t), cateFields)
	args := []any{corpID, robotID, id}
	var cate []*model.CateInfo
	if err := d.db.QueryToStructs(ctx, &cate, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取Cate详情失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	if len(cate) == 0 {
		// 特殊情况(脏数据)可能为0,不能直接返回nil
		return &model.CateInfo{}, nil
	}
	return cate[0], nil
}

// GetCateByIDs 获取Cate详情
func (d *dao) GetCateByIDs(ctx context.Context, t model.CateObjectType, ids []uint64) (map[uint64]*model.CateInfo,
	error) {
	list := make(map[uint64]*model.CateInfo)
	if len(ids) == 0 {
		return list, nil
	}
	querySQL := fmt.Sprintf(getCateSql(getCateByIDs, t), cateFields, placeholder(len(ids)))
	var args []any
	for _, id := range ids {
		args = append(args, id)
	}
	var cateInfo []*model.CateInfo
	if err := d.db.QueryToStructs(ctx, &cateInfo, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "通过业务ID获取Cate列表 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	for _, cate := range cateInfo {
		list[cate.ID] = cate
	}
	return list, nil
}

// GetCateByBusinessID 通过业务ID获取Cate详情
func (d *dao) GetCateByBusinessID(ctx context.Context, t model.CateObjectType,
	cateBizID, corpID, robotID uint64) (*model.CateInfo, error) {
	querySQL := fmt.Sprintf(getCateSql(getCateByBusinessID, t), cateFields)
	args := []any{corpID, robotID, cateBizID}
	var cateInfo []*model.CateInfo
	if err := d.db.QueryToStructs(ctx, &cateInfo, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取Cate详情失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	if len(cateInfo) == 0 {
		return nil, errs.ErrCateNotFound
	}
	return cateInfo[0], nil
}

// GetRobotUncategorizedCateID 获取机器人未分类的ID
func (d *dao) GetRobotUncategorizedCateID(ctx context.Context, t model.CateObjectType,
	corpID, robotID uint64) (uint64, error) {
	querySQL := fmt.Sprintf(getCateSql(getRobotUncategorizedCateID, t), cateFields)
	args := []any{corpID, robotID, model.UncategorizedCateName}
	var cateInfo []*model.CateInfo
	if err := d.db.QueryToStructs(ctx, &cateInfo, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取Cate详情失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return 0, err
	}
	if len(cateInfo) == 0 {
		return 0, errs.ErrCateNotFound
	}
	return cateInfo[0].ID, nil
}

// GetCateListByBusinessIDs 通过业务ID获取Cate列表
func (d *dao) GetCateListByBusinessIDs(ctx context.Context, t model.CateObjectType, corpID, robotID uint64,
	cateBizIDs []uint64) (
	map[uint64]*model.CateInfo, error) {
	if len(cateBizIDs) == 0 {
		return nil, nil
	}
	querySQL := fmt.Sprintf(getCateSql(getCateListByBusinessIDs, t), cateFields, placeholder(len(cateBizIDs)))
	args := []any{corpID, robotID}
	for _, bizID := range cateBizIDs {
		args = append(args, bizID)
	}
	var cateInfo []*model.CateInfo
	if err := d.db.QueryToStructs(ctx, &cateInfo, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "通过业务ID获取Cate列表 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	list := make(map[uint64]*model.CateInfo, len(cateInfo))
	for _, cate := range cateInfo {
		list[cate.BusinessID] = cate
	}
	return list, nil
}

// CheckCateBiz 检查分类BusinessID
func (d *dao) CheckCateBiz(ctx context.Context, t model.CateObjectType, corpID, cateBizID, robotID uint64) (uint64,
	error) {
	cate, err := d.GetCateByBusinessID(ctx, t, cateBizID, corpID, robotID)
	if err != nil {
		if errors.Is(err, errs.ErrCateNotFound) {
			return 0, err
		}
		log.ErrorContextf(ctx, "GetCateByBusinessID error, cateBizID: %d err:%+v", cateBizID, err)
		return 0, errs.ErrSystem
	}
	if cate == nil {
		log.ErrorContextf(ctx, "GetCateByBusinessID(cate is nil) cateBizID: %d corpID:%d, robotID: %d", cateBizID,
			corpID, robotID)
		return 0, errs.ErrCateNotFound
	}
	return cate.ID, nil
}

// CheckCate 检查分类ID
func (d *dao) CheckCate(ctx context.Context, t model.CateObjectType, corpID, cateID, robotID uint64) error {
	cate, err := d.GetCateByID(ctx, t, cateID, corpID, robotID)
	if err != nil {
		return errs.ErrSystem
	}
	if cate == nil {
		return errs.ErrCateNotFound
	}
	return nil
}

// GetCateChildrenIDs 获取分类下的子分类ID列表
func (d *dao) GetCateChildrenIDs(ctx context.Context, t model.CateObjectType, corpID, cateID, robotID uint64) (
	[]uint64, error) {
	cateInfos, err := d.GetCateList(ctx, t, corpID, robotID)
	if err != nil {
		return nil, errs.ErrSystem
	}
	node := model.BuildCateTree(cateInfos).FindNode(cateID)
	if node == nil {
		return nil, errs.ErrCateNotFound
	}
	return node.ChildrenIDs(), nil
}

// GetCateStat 按分类统计
func (d *dao) GetCateStat(ctx context.Context, t model.CateObjectType, corpID, robotID uint64) (map[uint64]uint32,
	error) {
	sql := getQACateStat
	args := []any{corpID, robotID}
	// QA,Doc等的isDeleted枚举值可能不同,分开判断
	// 默认按文档t_doc表路由，避免空db实例
	db := knowClient.DBClient(ctx, docTableName, robotID, []client.Option{}...)
	if t == model.QACate {
		sql = getQACateStat
		// QA里要限制accept_status状态
		args = append(args, model.QAIsNotDeleted)
		args = append(args, model.AcceptYes)
		db = knowClient.DBClient(ctx, docQaTableName, robotID, []client.Option{}...)
	} else if t == model.DocCate {
		sql = getDocCateStat
		args = append(args, model.DocIsNotDeleted)
		// 文档分类的统计需要指定opt类别
		args = append(args, model.DocOptDocImport)
		db = knowClient.DBClient(ctx, docTableName, robotID, []client.Option{}...)
	} else if t == model.SynonymsCate {
		args = append(args, model.SynonymIsNotDeleted)
		sql = getSynonymsCateStat
		db = knowClient.DBClient(ctx, synonymsTableName, robotID, []client.Option{}...)
	}
	var stat []*model.CateStat
	log.DebugContextf(ctx, "GetCateStat(sql:%s), args:%+v", sql, args)
	if err := db.QueryToStructs(ctx, &stat, sql, args...); err != nil {
		log.ErrorContextf(ctx, "统计分类(%+v)下问题数量失败, sql: %s, args: %+v, err:%+v", t, sql, args, err)
		return nil, err
	}
	m := make(map[uint64]uint32, len(stat))
	for _, v := range stat {
		m[v.CategoryID] = v.Total
	}
	return m, nil
}

// GroupCateObject 分组分类(QA/文档/同义词等)
func (d *dao) GroupCateObject(ctx context.Context, t model.CateObjectType, ids []uint64, cateID uint64) error {
	now := time.Now()
	if len(ids) == 0 {
		return nil
	}
	sql := groupQA
	onceMaxNum := 500
	if t == model.QACate {
		sql = groupQA
	} else if t == model.DocCate {
		sql = groupDoc
	} else if t == model.SynonymsCate {
		sql = groupSynonyms
		// 同义词场景下，标准词会对应多个相似词，这里减少一次处理条数
		onceMaxNum = 200
	} else {
		log.ErrorContextf(ctx, "未知的分类类别: %+v", t)
		return errs.ErrSystem
	}

	if len(ids) <= onceMaxNum {
		args := []any{cateID, now}
		for _, id := range ids {
			args = append(args, id)
		}
		if err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
			querySQL := fmt.Sprintf(sql, placeholder(len(ids)))
			if _, err := tx.ExecContext(ctx, querySQL, args...); err != nil {
				log.ErrorContextf(ctx, "分组(%+v)失败 sql:%s ids:%+v cateID:%+v err:%+v", t, querySQL, ids, cateID,
					err)
				return err
			}
			if t == model.SynonymsCate {
				querySQL = fmt.Sprintf(groupSynonymsNotStandard, placeholder(len(ids)))
				if _, err := tx.ExecContext(ctx, querySQL, args...); err != nil {
					log.ErrorContextf(ctx, "同义词分组(%+v)失败 sql:%s ids:%+v cateID:%+v err:%+v", t, querySQL, ids,
						cateID, err)
					return err
				}
			}
			return nil
		}); err != nil {
			log.ErrorContextf(ctx, "分组(%+v)失败 err:%+v", t, err)
			return err
		}
		return nil
	}

	batch := onceMaxNum
	if err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		for i := 0; i < len(ids); {
			next := i + batch
			if next >= len(ids) {
				next = len(ids)
			}
			args := []any{cateID, now}
			for _, v := range ids[i:next] {
				args = append(args, v)
			}
			tmpSQL := fmt.Sprintf(sql, placeholder(len(ids[i:next])))
			if _, err := tx.ExecContext(ctx, tmpSQL, args...); err != nil {
				log.ErrorContextf(ctx, "分组(%+v)失败 sql:%s qaIDs:%+v cateID:%+v err:%+v", t, tmpSQL, ids, cateID,
					err)
				return err
			}
			if t == model.SynonymsCate {
				tmpSQL = fmt.Sprintf(groupSynonymsNotStandard, placeholder(len(ids[i:next])))
				if _, err := tx.ExecContext(ctx, tmpSQL, args...); err != nil {
					log.ErrorContextf(ctx, "同义词分组(%+v)失败 sql:%s ids:%+v cateID:%+v err:%+v", t, tmpSQL,
						ids, cateID, err)
					return err
				}
			}
			i = next
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "分组(%+v)失败 err:%+v", t, err)
		return err
	}
	return nil
}

// initDefaultCategory 初始化默认分类(QA, Doc, Synonyms)
func (d *dao) initDefaultCategory(ctx context.Context, appDB *model.AppDB, tx *sqlx.Tx) error {
	typeList := []model.CateObjectType{model.QACate, model.DocCate, model.SynonymsCate}
	for _, t := range typeList {
		cate := &model.CateInfo{
			BusinessID: d.GenerateSeqID(),
			RobotID:    appDB.ID,
			CorpID:     appDB.CorpID,
			Name:       model.UncategorizedCateName,
			ParentID:   0,
			IsDeleted:  model.CateIsNotDeleted,
			UpdateTime: appDB.UpdateTime,
			CreateTime: appDB.CreateTime,
		}
		log.InfoContextf(ctx, "初始化%+v默认分类: %s", t, cate.Name)
		querySQL := fmt.Sprintf(getCateSql(createCate, t), cateFields)
		if _, err := tx.NamedExecContext(ctx, querySQL, cate); err != nil {
			log.ErrorContextf(ctx, "初始化应用默认分类失败 sql:%s args:%+v err:%+v", querySQL, cate, err)
			return err
		}
	}
	return nil
}

// createCates 新增分类
func (d *dao) createCates(ctx context.Context, tx *sqlx.Tx, t model.CateObjectType, corpID, robotID uint64,
	tree *model.CateNode) error {
	if tree == nil {
		return nil
	}
	for _, child := range tree.Children {
		if child.ID == 0 {
			cate := &model.CateInfo{
				BusinessID: d.GenerateSeqID(),
				CorpID:     corpID,
				RobotID:    robotID,
				Name:       child.Name,
				IsDeleted:  model.CateIsNotDeleted,
				ParentID:   tree.ID,
				CreateTime: time.Now(),
				UpdateTime: time.Now(),
			}
			sql := fmt.Sprintf(getCateSql(createCate, t), cateFields)
			r, err := tx.NamedExecContext(ctx, sql, cate)
			if err != nil {
				log.ErrorContextf(ctx, "新增分类失败, sql: %s, args: %+v, err: %+v", sql, cate, err)
				return err
			}
			id, _ := r.LastInsertId()
			child.ID = uint64(id)
		}
		if err := d.createCates(ctx, tx, t, corpID, robotID, child); err != nil {
			return err
		}
	}
	return nil
}
