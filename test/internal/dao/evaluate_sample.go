package dao

import (
	"context"
	"fmt"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/baicaoyuan/moss/types/slicex"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"github.com/jmoiron/sqlx"
)

// DB SQL
const (
	batchLimit      = 1000
	sampleSetFields = `
		id,business_id,corp_id,robot_id,name,cos_url,cos_bucket,cos_hash,num,is_deleted,create_staff_id,
		create_time,update_time`
	sampleRecordFields = `id,set_id,content,role_description,custom_variables,create_time,update_time`
	createSampleSet    = `
		INSERT INTO 
		    t_evaluate_sample_set (business_id,corp_id,robot_id,name,cos_url,cos_bucket,cos_hash,num,is_deleted,
		    create_staff_id,create_time)
		VALUES 
		    (:business_id,:corp_id,:robot_id,:name,:cos_url,:cos_bucket,:cos_hash,:num,:is_deleted,
		     :create_staff_id,NOW())`
	createSampleRecord = `
		INSERT INTO 
		    t_evaluate_sample_set_record (set_id,content,create_time,role_description,custom_variables)
		VALUES 
		    (:set_id,:content,NOW(),:role_description,:custom_variables)`
	getSampleSetByCosHash = `
		SELECT 
			%s 
		FROM 
		    t_evaluate_sample_set 
		WHERE 
		    corp_id = ? AND robot_id = ? AND cos_hash = ? AND is_deleted = 0
	`

	getSampleSetCount = `
		SELECT 
    		COUNT(*) 
		FROM 
		    t_evaluate_sample_set 
		WHERE 
		    corp_id = ? AND robot_id =? AND is_deleted = 0 %s 
	`
	getSampleSets = `
		SELECT 
    		%s 
		FROM 
		    t_evaluate_sample_set 
		WHERE 
		    corp_id = ? AND robot_id = ? AND is_deleted = 0 %s 
		ORDER BY 
		    id DESC 
		LIMIT ?,?
		`
	deleteSampleSetByIDs = `
		UPDATE 
		    t_evaluate_sample_set 
		SET 
		    is_deleted = 1
		WHERE 
		    corp_id = ? AND robot_id = ? AND id IN (?)
	`
	getSampleRecords = `
		SELECT 
    		%s 
		FROM 
		    t_evaluate_sample_set_record 
		WHERE 
		    set_id IN (?) AND id > ?
		ORDER BY 
		    id,set_id
		LIMIT ?
		`
	getSampleSetByBizIDs = `
		SELECT 
    		%s 
		FROM 
		    t_evaluate_sample_set 
		WHERE 
		    corp_id = ? AND robot_id = ? AND is_deleted = 0 AND business_id IN(%s) 
	
		`
	getDeleteSampleSets = `
		SELECT 
    		%s 
		FROM 
		    t_evaluate_sample_set 
		WHERE 
		    corp_id = ? AND robot_id = ? 
		`
	deleteSampleSetRecordsBySetID = `
		DELETE FROM 
		           t_evaluate_sample_set_record 
		       WHERE set_id = ?  `
	deleteSampleSet = `
		DELETE FROM 
		           t_evaluate_sample_set 
		       WHERE corp_id = ? and robot_id = ?   `
	deleteSampleSetBySetID = `
		DELETE FROM 
		   		t_evaluate_sample_set 
	    WHERE  id = ? and corp_id = ? and robot_id = ?  `
)

// GetSampleSetByCosHash 通过cos_hash获取评测文档
func (d *dao) GetSampleSetByCosHash(ctx context.Context, corpID, robotID uint64, cosHash string) (*model.SampleSet,
	error) {
	querySQL := fmt.Sprintf(getSampleSetByCosHash, sampleSetFields)
	args := []any{corpID, robotID, cosHash}
	var list []*model.SampleSet
	if err := d.db.QueryToStructs(ctx, &list, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "通过cos_hash获取样本集失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	if len(list) == 0 {
		return nil, nil
	}
	return list[0], nil
}

// CreateSampleSet 创建样本集
func (d *dao) CreateSampleSet(ctx context.Context, set *model.SampleSet, sampleRecord []model.SampleRecord) error {
	return d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		res, err := tx.NamedExecContext(ctx, createSampleSet, set)
		if err != nil {
			log.ErrorContextf(ctx, "创建样本集失败, sql:%s, args:%+v, err:%+v", createSampleSet, set, err)
			return err
		}
		id, _ := res.LastInsertId()
		samples := model.NewSamples(ctx, uint64(id), sampleRecord)
		set.ID = uint64(id)
		preSamplesList := slicex.Chunk(samples, batchLimit)
		for _, preSamples := range preSamplesList {
			if _, err := tx.NamedExecContext(ctx, createSampleRecord, preSamples); err != nil {
				log.ErrorContextf(ctx, "批量插入样本列表失败, sql:%s, err:%+v", createSampleRecord, err)
				return err
			}
		}
		return nil
	})
}

// GetSampleSets 分页获取样本集列表
func (d *dao) GetSampleSets(ctx context.Context, corpID, robotID uint64, setName string, page,
	pageSize uint32) (uint64, []*model.SampleSet, error) {
	args := []any{corpID, robotID}
	condition := ""
	if setName != "" {
		condition = " AND name LIKE ?"
		args = append(args, fmt.Sprintf("%%%s%%", special.Replace(setName)))
	}
	querySQL := fmt.Sprintf(getSampleSetCount, condition)
	var total uint64
	if err := d.db.Get(ctx, &total, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取样本集总数失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return 0, nil, err
	}
	querySQL = fmt.Sprintf(getSampleSets, sampleSetFields, condition)
	offset := (page - 1) * pageSize
	args = append(args, offset, pageSize)
	var sets []*model.SampleSet
	log.DebugContextf(ctx, "获取样本集列表 sql:%s args:%+v", querySQL, args)
	if err := d.db.QueryToStructs(ctx, &sets, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取样本集列表失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return 0, nil, err
	}
	return total, sets, nil
}

// DeleteSampleSets 删除样本集
func (d *dao) DeleteSampleSets(ctx context.Context, corpID, robotID uint64, ids []uint64) error {
	sql, args, err := sqlx.In(deleteSampleSetByIDs, corpID, robotID, ids)
	if err != nil {
		log.ErrorContextf(ctx, "删除样本集参数异常, sql:%s, ids:%+v, err:%+v", deleteSampleSetByIDs, args, err)
		return err
	}

	if _, err = d.db.Exec(ctx, sql, args...); err != nil {
		log.ErrorContextf(ctx, "删除样本集失败, sql:%s, args:%+v, err:%+v", deleteSampleSetByIDs, args, err)
		return err
	}
	return nil
}

// GetSampleRecordsBySetIDs 根据集合ID获取样本列表
func (d *dao) GetSampleRecordsBySetIDs(ctx context.Context, setIDs []uint64) ([]*model.Sample, error) {
	querySQL := fmt.Sprintf(getSampleRecords, sampleRecordFields)
	var samples []*model.Sample
	lastID := uint64(0)
	for {
		var subList []*model.Sample
		sql, args, err := sqlx.In(querySQL, setIDs, lastID, batchLimit)
		if err != nil {
			log.ErrorContextf(ctx, "样本列表列表参数异常, sql:%s, ids:%+v, err:%+v", querySQL, setIDs, err)
			return nil, err
		}
		if err = d.db.QueryToStructs(ctx, &subList, sql, args...); err != nil {
			log.ErrorContextf(ctx, "获取样本列表失败 sql:%s args:%+v err:%+v", sql, args, err)
			return nil, err
		}
		if len(subList) > 0 {
			lastID = subList[len(subList)-1].ID
		}
		samples = append(samples, subList...)
		if len(subList) < batchLimit {
			break
		}
	}
	return samples, nil
}

// GetSampleSetsByBizIDs 分页获取样本集列表
func (d *dao) GetSampleSetsByBizIDs(ctx context.Context, corpID, robotID uint64, ids []uint64) (
	map[uint64]*model.SampleSet, error) {
	var sets []*model.SampleSet
	args := []any{corpID, robotID}
	for _, ID := range ids {
		args = append(args, ID)
	}
	querySQL := fmt.Sprintf(getSampleSetByBizIDs, sampleSetFields, placeholder(len(ids)))
	if err := d.db.QueryToStructs(ctx, &sets, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取样本集列表失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	setMap := make(map[uint64]*model.SampleSet)
	for _, set := range sets {
		setMap[set.BusinessID] = set
	}
	return setMap, nil
}

// GetDeleteSampleSets 获取需要删除的样本集
func (d *dao) GetDeleteSampleSets(ctx context.Context, corpID, robotID uint64) ([]*model.SampleSet, error) {
	args := []any{corpID, robotID}
	querySQL := fmt.Sprintf(getDeleteSampleSets, sampleSetFields)
	var sets []*model.SampleSet
	log.DebugContextf(ctx, "获取需要删除的样本集 sql:%s args:%+v", querySQL, args)
	if err := d.db.QueryToStructs(ctx, &sets, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取需要删除的样本集失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	return sets, nil
}

// DeleteSampleSetRecords 删除样本集下所有样本
func (d *dao) DeleteSampleSetRecords(ctx context.Context, setID uint64) error {
	deleteSQL := deleteSampleSetRecordsBySetID
	result, err := d.db.Exec(ctx, deleteSQL, setID)
	if err != nil {
		log.ErrorContextf(ctx, "删除样本集下所有样本失败, sql:%s, setID:%+v, err:%+v",
			deleteSQL, setID, err)
		return err
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		log.ErrorContextf(ctx, "DeleteSampleSetRecords|GetRowsAffected Failed result:%+v,err:%+v", result, err)
		return err
	}
	log.DebugContextf(ctx, "删除样本集下所有样本 setID:%+d, rowsAffected:%d",
		setID, rowsAffected)
	return nil
}

// DeleteSampleSet 删除应用所有样本集
func (d *dao) DeleteSampleSet(ctx context.Context, corpID, robotID uint64) error {
	deleteSQL := deleteSampleSet
	result, err := d.db.Exec(ctx, deleteSQL, corpID, robotID)
	if err != nil {
		log.ErrorContextf(ctx, "删除应用所有样本集失败, sql:%s, corpID:%+v,robotID:%+v, err:%+v",
			deleteSQL, corpID, robotID, err)
		return err
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		log.ErrorContextf(ctx, "DeleteSampleSet|GetRowsAffected Failed result:%+v,err:%+v", result, err)
		return err
	}
	log.DebugContextf(ctx, "删除应用所有样本集 corpID:%+v,robotID:%+v, rowsAffected:%d",
		corpID, robotID, rowsAffected)
	return nil
}

// DeleteSampleSetBySetID 删除应用样本集BySetID
func (d *dao) DeleteSampleSetBySetID(ctx context.Context, corpID, robotID, setID uint64) error {
	deleteSQL := deleteSampleSetBySetID
	result, err := d.db.Exec(ctx, deleteSQL, setID, corpID, robotID)
	if err != nil {
		log.ErrorContextf(ctx, "删除应用样本集失败, sql:%s, corpID:%+v,robotID:%+v, setID:%v,err:%+v",
			deleteSQL, corpID, robotID, setID, err)
		return err
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		log.ErrorContextf(ctx, "DeleteSampleSetBySetID|GetRowsAffected Failed result:%+v,err:%+v", result, err)
		return err
	}
	log.DebugContextf(ctx, "删除应用样本集 corpID:%+v,robotID:%+v, setID:%v, rowsAffected:%d",
		corpID, robotID, setID, rowsAffected)
	return nil
}
