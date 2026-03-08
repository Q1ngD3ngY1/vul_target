package dao

import (
	"context"
	"encoding/json"
	"fmt"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/client"
	"git.code.oa.com/trpc-go/trpc-go/log"
	knowClient "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao/redis"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"gorm.io/gorm"
)

const (
	// cate table name is separated by business
	cateTableName         = "t_doc_category"
	qaCateTableName       = "t_doc_qa_category"
	synonymsCateTableName = "t_synonyms_category"

	cateCacheKey = "know_cate:%s:%d"
)

const (
	CateTblColId         = "id"
	CateTblColBusinessId = "business_id"
	CateTblColRobotId    = "robot_id"
	CateTblColCorpId     = "corp_id"
	CateTblColName       = "name"
	CateTblColOrderNum   = "order_num"
	CateTblColIsDeleted  = "is_deleted" // 是否删除
	CateTblColParentId   = "parent_id"
	CateTblColCreateTime = "create_time" // 创建时间
	CateTblColUpdateTime = "update_time" // 更新时间
	CateTableMaxPageSize = 1000
)

type CateDao struct {
	BaseDao
	tableName string
}

// GetCateDao 获取全局的数据操作对象
func GetCateDao(t model.CateObjectType) *CateDao {
	return &CateDao{
		BaseDao:   *globalBaseDao,
		tableName: getCateTableName(t),
	}
}

func (d *CateDao) GetGormDB() *gorm.DB {
	return d.gormDB
}

func getCateTableName(t model.CateObjectType) string {
	switch t {
	case model.QACate:
		return qaCateTableName
	case model.DocCate:
		return cateTableNameDoc
	case model.SynonymsCate:
		return synonymsCateTableName
	}
	return qaCateTableName
}

// getCateUpdateTableName 根据分类type获取需要根据分类的表名
func getCateUpdateTableName(t model.CateObjectType) string {
	switch t {
	case model.QACate:
		return docQaTableName
	case model.DocCate:
		return docTableName
	case model.SynonymsCate:
		return synonymsTableName
	}
	return docQaTableName
}

func (d *CateDao) CreateCate(ctx context.Context, tx *gorm.DB, cate *model.CateInfo) (id uint64, err error) {
	if tx == nil {
		tx = d.gormDB
	}
	res := tx.WithContext(ctx).Table(d.tableName).Create(cate)
	if res.Error != nil {
		log.ErrorContextf(ctx, "execute sql failed, err: %+v", res.Error)
		err = res.Error
		return
	}
	id = cate.ID
	return
}

func (d *CateDao) UpdateCates(ctx context.Context, tx *gorm.DB, cate model.CateInfo, updateColumns []string, cateIds []uint64) (err error) {
	if tx == nil {
		tx = d.gormDB
	}
	session := tx.WithContext(ctx).
		Table(d.tableName)
	if updateColumns != nil {
		session = session.Select(updateColumns)
	}
	session = session.
		Where(CateTblColId+sqlIn, cateIds)
	res := session.Updates(cate)
	if res.Error != nil {
		log.ErrorContextf(ctx, "execute sql failed, err: %+v", res.Error)
		err = res.Error
		return
	}
	return
}

func (d *CateDao) DeleteCates(ctx context.Context, tx *gorm.DB, cateIds []uint64) (err error) {
	if tx == nil {
		tx = d.gormDB
	}
	var (
		updateColumns = []string{CateTblColIsDeleted, CateTblColUpdateTime}
		cateUpdate    = model.CateInfo{
			IsDeleted:  IsDeleted,
			UpdateTime: time.Now(), // 更新时间
		}
	)
	if err := d.UpdateCates(ctx, tx, cateUpdate, updateColumns, cateIds); err != nil {
		log.ErrorContextf(ctx, "DeleteCates failed for cateIds: %+v, err: %+v", cateIds, err)
		return err
	}
	return nil
}

func (d *CateDao) GetCateByID(ctx context.Context, id, corpID, robotID uint64) (cate *model.CateInfo, err error) {
	cate = new(model.CateInfo)
	cond := d.gormDB.WithContext(ctx).
		Table(d.tableName).
		Where(CateTblColId+sqlEqual, id).
		Where(CateTblColCorpId+sqlEqual, corpID).
		Where(CateTblColRobotId+sqlEqual, robotID)
	res := cond.First(cate)
	if res.Error != nil {
		if res.Error == gorm.ErrRecordNotFound {
			return cate, nil // 未找到数据返回空的 cate 和 nil 错误
		}
		log.ErrorContextf(ctx, "execute sql failed, err: %+v", res.Error)
		err = res.Error
	}
	return
}

type CateFilter struct {
	Ids         []uint64
	CorpId      uint64 // 企业 ID
	RobotId     uint64
	BusinessId  uint64
	BusinessIds []uint64
	ParentID    uint64
	Name        string

	IsDeleted *int
	Offset    uint32
	Limit     uint32
}

func (f *CateFilter) Check() error {
	// 限制单次查询最大条数
	if f.Limit < 1 || f.Limit > CateTableMaxPageSize {
		return fmt.Errorf("invalid limit: %d", f.Limit)
	}
	return nil
}

// 生成查询条件，必须按照索引的顺序排列
func (f *CateFilter) Generate(ctx context.Context, session *gorm.DB) (sessionGet *gorm.DB) {
	sessionGet = session
	if len(f.Ids) != 0 {
		sessionGet = sessionGet.Where(CateTblColId+sqlIn, f.Ids)
	}
	if f.CorpId != 0 {
		sessionGet = sessionGet.Where(CateTblColCorpId+sqlEqual, f.CorpId)
	}
	if f.RobotId != 0 {
		sessionGet = sessionGet.Where(CateTblColRobotId+sqlEqual, f.RobotId)
	}
	if f.BusinessId != 0 {
		sessionGet = sessionGet.Where(CateTblColBusinessId+sqlEqual, f.BusinessId)
	}
	if len(f.BusinessIds) != 0 {
		sessionGet = sessionGet.Where(CateTblColBusinessId+sqlIn, f.BusinessIds)
	}
	if f.ParentID != 0 {
		sessionGet = sessionGet.Where(CateTblColParentId+sqlEqual, f.ParentID)
	}
	if f.Name != "" {
		sessionGet = sessionGet.Where(CateTblColName+sqlEqual, f.Name)
	}
	if f.IsDeleted != nil {
		sessionGet = sessionGet.Where(DocQaTblColIsDeleted+sqlEqual, *f.IsDeleted)
	}
	if f.Offset != 0 {
		sessionGet = sessionGet.Offset(int(f.Offset))
	}
	if f.Limit != 0 {
		sessionGet = sessionGet.Limit(int(f.Limit))
	}
	return sessionGet
}

// 获取分类列表
func (d *CateDao) GetCateList(ctx context.Context, selectColumns []string, filter *CateFilter) (cates []*model.CateInfo, err error) {
	if err = filter.Check(); err != nil {
		log.ErrorContextf(ctx, "invalid filter: %+v", filter)
		return
	}
	session := d.gormDB.WithContext(ctx).Table(d.tableName)
	if selectColumns != nil {
		session = session.Select(selectColumns)
	}
	cates = make([]*model.CateInfo, 0)
	session = filter.Generate(ctx, session)
	if res := session.Find(&cates); res.Error != nil {
		log.ErrorContextf(ctx, "execute sql failed, err: %+v", res.Error)
		err = res.Error
	}
	return
}

// DeleteCate 删除分类
func (d *CateDao) DeleteCate(ctx context.Context, baseDao Dao, t model.CateObjectType, cateIDs []uint64,
	uncategorizedCateID uint64, app *model.App) (err error) {
	if len(cateIDs) == 0 {
		return nil
	}
	robotID, appBizID := app.ID, app.BusinessID
	// feature_permission 获取需要变更的文档/问答id 触发向量更新
	qaIds, docIds := make([]uint64, 0), make([]uint64, 0)
	if t == model.QACate {
		qaList, err := GetDocQaDao().GetAllDocQas(ctx, []string{DocQaTblColId}, &DocQaFilter{
			RobotId:     robotID,
			IsDeleted:   pkg.GetIntPtr(model.QAIsNotDeleted), //t_doc_qa 1是正常 2是已删除
			CategoryIds: cateIDs,
		})
		if err != nil {
			log.ErrorContextf(ctx, "feature_permission DeleteCate get qaList err:%v,cateIDs:%v,appId:%v", err, cateIDs, robotID)
			return err
		}
		for _, v := range qaList {
			qaIds = append(qaIds, v.ID)
		}
	} else if t == model.DocCate {
		docList, err := GetDocDao().GetAllDocs(ctx, []string{DocTblColId, DocTblColExpireStart, DocTblColExpireEnd}, &DocFilter{
			RobotId:     robotID,
			IsDeleted:   pkg.GetIntPtr(IsNotDeleted),
			CategoryIds: cateIDs,
		})
		if err != nil {
			log.ErrorContextf(ctx, "feature_permission DeleteCate get docList err:%v,cateIDs:%v,appId:%v", err, cateIDs, robotID)
			return err
		}
		for _, v := range docList {
			docIds = append(docIds, v.ID)
		}
	}
	objectDB := d.gormDB
	if t == model.DocCate { //如果是文档需要根据应用id区分数据库实例
		objectDB, err = knowClient.GormClient(ctx, d.tableName, robotID, 0, []client.Option{}...)
		if err != nil {
			log.ErrorContextf(ctx, "DeleteCate get doc GormClient failed, err: %+v", err)
			return err
		}
	}
	//1.删除分类
	err = d.gormDB.WithContext(ctx).Table(getCateTableName(t)).Where(CateTblColId+sqlIn, cateIDs).
		Updates(map[string]any{
			CateTblColIsDeleted:  model.CateIsDeleted,
			CateTblColUpdateTime: time.Now(),
		}).Error
	if err != nil {
		log.ErrorContextf(ctx, "删除分类失败, cateIDs:%v,err:%+v", cateIDs, err)
		return err
	}
	//2.删除分类时,该分类下的Object移动到"未分类"目录下
	err = objectDB.WithContext(ctx).Table(getCateUpdateTableName(t)).
		Where(DocQaTblColRobotId+sqlEqual, robotID).Where(DocQaTblColCategoryId+sqlIn, cateIDs).
		Updates(map[string]any{
			DocQaTblColCategoryId: uncategorizedCateID,
			DocQaTblColUpdateTime: time.Now(),
		}).Error
	if err != nil {
		log.ErrorContextf(ctx, "更新分类失败, cateIDs:%v,uncategorizedCateID:%v,args:%+v, err: %+v", cateIDs, uncategorizedCateID, err)
		return err
	}
	//feature_permission 文档和分类更改分类需要更新向量标签
	if len(docIds) == 0 && len(qaIds) == 0 {
		return nil
	}
	updateData := make(map[uint64]model.KnowData)
	updateData[appBizID] = model.KnowData{
		DocIDs: docIds,
		QaIDs:  qaIds,
	}
	err = NewBatchUpdateVectorTask(ctx, appBizID, model.BatchUpdateVector{
		Type:      model.UpdateVectorByCate,
		CorpBizID: pkg.CorpBizID(ctx),
		AppBizID:  app.BusinessID,
		KnowIDs:   updateData,
	})
	if err != nil {
		log.ErrorContextf(ctx, "feature_permission DeleteCate BatchUpdateVector err:%v", err)
		return err
	}
	return nil
}

// GroupCateObject 分组分类(QA/文档/同义词等)
func (d *CateDao) GroupCateObject(ctx context.Context, baseDao Dao, t model.CateObjectType, ids []uint64, cateID uint64,
	app *model.App) (err error) {
	if len(ids) == 0 {
		return nil
	}
	robotID, appBizID := app.ID, app.BusinessID
	whereName := DocQaTblColId
	onceMaxNum := 500
	objectDB := d.gormDB
	if t == model.QACate {
		whereName = DocQaTblColId
	} else if t == model.DocCate {
		whereName = DocTblColId
		objectDB, err = knowClient.GormClient(ctx, d.tableName, robotID, 0, []client.Option{}...)
		if err != nil {
			log.ErrorContextf(ctx, "GroupCateObject get doc GormClient failed, err: %+v", err)
			return err
		}
	} else if t == model.SynonymsCate {
		whereName = synonymsTblColId
		// 同义词场景下，标准词会对应多个相似词，这里减少一次处理条数
		onceMaxNum = 200
	} else {
		log.ErrorContextf(ctx, "未知的分类类别: %+v", t)
		return errs.ErrSystem
	}
	tx := objectDB.Begin()
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
		} else if err != nil {
			tx.Rollback()
		} else {
			tx.Commit()
		}
	}()
	end, length := 0, len(ids)
	for start := 0; start < len(ids); start += onceMaxNum {
		end := min(end+onceMaxNum, length)
		tmp := ids[start:end]
		err := tx.WithContext(ctx).Table(getCateUpdateTableName(t)).Where(whereName+sqlIn, tmp).
			Updates(map[string]any{
				DocQaTblColCategoryId: cateID,
				DocQaTblColUpdateTime: time.Now(),
			}).Error
		if err != nil {
			log.ErrorContextf(ctx, "分组(%+v)失败 ids:%+v cateID:%+v err:%+v", t, ids, cateID, err)
			return err
		}
		if t == model.SynonymsCate {
			err := tx.WithContext(ctx).Table(synonymsTableName).Where(synonymsTblColParentId+sqlIn, tmp).
				Updates(map[string]any{
					synonymsTblColCateId:     cateID,
					synonymsTblColUpdateTime: time.Now(),
				}).Error
			if err != nil {
				log.ErrorContextf(ctx, "同义词分组(%+v)失败 ids:%+v cateID:%+v err:%+v", t, ids, cateID, err)
				return err
			}
		}
	}
	//feature_permission 文档和问答更改分类需要更新向量标签
	if t == model.QACate {
		updateData := make(map[uint64]model.KnowData)
		updateData[appBizID] = model.KnowData{
			QaIDs: ids,
		}
		err = NewBatchUpdateVectorTask(ctx, appBizID, model.BatchUpdateVector{
			Type:      model.UpdateVectorByCate,
			CorpBizID: pkg.CorpBizID(ctx),
			AppBizID:  app.BusinessID,
			KnowIDs:   updateData,
		})
	} else if t == model.DocCate {
		updateData := make(map[uint64]model.KnowData)
		updateData[appBizID] = model.KnowData{
			DocIDs: ids,
		}
		err = NewBatchUpdateVectorTask(ctx, appBizID, model.BatchUpdateVector{
			Type:      model.UpdateVectorByCate,
			CorpBizID: pkg.CorpBizID(ctx),
			AppBizID:  app.BusinessID,
			KnowIDs:   updateData,
		})
	}
	if err != nil {
		log.ErrorContextf(ctx, "feature_permission cateGroup BatchUpdateVector err:%v", err)
		return err
	}
	return nil
}

func getCateCacheKey(appID uint64, table string) string {
	return fmt.Sprintf(cateCacheKey, table, appID)
}

// SetCateCache 设置分类缓存
func (d *CateDao) SetCateCache(ctx context.Context, corpID, appID uint64) (map[int][]int, error) {
	if d.tableName != qaCateTableName && d.tableName != cateTableNameDoc {
		return nil, errs.ErrCateTypeFail
	}
	redisCli, err := redis.GetGoRedisClient(ctx)
	if err != nil {
		log.ErrorContextf(ctx, "SetCateCache GetGoRedisClient err:%v", err)
		return nil, err
	}
	var cate []*model.CateInfo
	err = d.gormDB.WithContext(ctx).Table(d.tableName).Where(CateTblColCorpId+sqlEqual, corpID).
		Where(CateTblColRobotId+sqlEqual, appID).Where(DocQaTblColIsDeleted+sqlEqual, model.CateIsNotDeleted).
		Select([]string{CateTblColId, CateTblColBusinessId, CateTblColParentId}).Find(&cate).Error
	if err != nil {
		log.ErrorContextf(ctx, "SetCateCache getCateList err:%v,corpID:%v,appID:%v", err, corpID, appID)
		return nil, err
	}
	cateCache := model.BuildCateCache(cate) //构建以分类业务id为key，value为所有递归子分类的map
	tmp, err := json.Marshal(cateCache)
	if err != nil {
		log.ErrorContextf(ctx, "SetCateCache json Marshal err:%v,corpID:%v,appID:%v,cateCache:%v", err, corpID, appID, cateCache)
		return nil, err
	}
	_, err = redisCli.Set(ctx, getCateCacheKey(appID, d.tableName), string(tmp), 7*24*time.Hour).Result()
	if err != nil {
		log.ErrorContextf(ctx, "GetCateCache set redis err:%v,appID:%v,cateCache:%v", err, appID, string(tmp))
	}
	return cateCache, nil
}

// GetCateCache 获取分类缓存
// key为分类业务id，values为递归所有子分类业务id
func (d *CateDao) GetCateCache(ctx context.Context, corpID, appID uint64) (map[int][]int, error) {
	if d.tableName != qaCateTableName && d.tableName != cateTableNameDoc {
		return nil, errs.ErrCateTypeFail
	}
	redisCli, err := redis.GetGoRedisClient(ctx)
	if err != nil {
		log.ErrorContextf(ctx, "GetCateCache GetGoRedisClient err:%v,appID:%v", err, appID)
		return nil, err
	}
	key := getCateCacheKey(appID, d.tableName)
	res, err := redisCli.Get(ctx, key).Result()
	if err != nil || res == "" { //缓存为空，重新构建
		log.WarnContextf(ctx, "GetCateCache get cache err:%v,appID:%v", err, appID)
		return d.SetCateCache(ctx, corpID, appID)
	}
	var result map[int][]int
	err = json.Unmarshal([]byte(res), &result)
	if err != nil {
		log.ErrorContextf(ctx, "GetCateCache Unmarshal redis err:%v,appID:%v,cateCache:%v", err, appID, res)
		return nil, err
	}
	return result, nil
}
