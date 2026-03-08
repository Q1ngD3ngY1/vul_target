package category

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/client"
	"git.woa.com/adp/common/x/errx"
	"git.woa.com/adp/common/x/gox/convx"
	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/model"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/mysqlquery"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	cateEntity "git.woa.com/adp/kb/kb-config/internal/entity/category"
	"git.woa.com/adp/kb/kb-config/internal/entity/document"
	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
	knowClient "git.woa.com/adp/kb/kb-config/internal/rpc"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"git.woa.com/adp/kb/kb-config/internal/util/idgen"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	"gorm.io/gen"
	"gorm.io/gorm"
)

func CategoriesPO2DO(pos []*model.TDocCategory) []*cateEntity.CateInfo {
	return slicex.Map(pos, func(po *model.TDocCategory) *cateEntity.CateInfo {
		return CategoryPO2DO(po)
	})
}

func CategoryPO2DO(po *model.TDocCategory) *cateEntity.CateInfo {
	if po == nil {
		return nil
	}
	return &cateEntity.CateInfo{
		ID:         po.ID,
		BusinessID: po.BusinessID,
		RobotID:    po.RobotID,
		CorpID:     po.CorpID,
		Name:       po.Name,
		OrderNum:   po.OrderNum,
		IsDeleted:  po.IsDeleted,
		ParentID:   po.ParentID,
		CreateTime: po.CreateTime,
		UpdateTime: po.UpdateTime,
	}
}

func CategoryDO2PO(do *cateEntity.CateInfo) *model.TDocCategory {
	if do == nil {
		return nil
	}
	return &model.TDocCategory{
		ID:         do.ID,
		BusinessID: do.BusinessID,
		RobotID:    do.RobotID,
		CorpID:     do.CorpID,
		Name:       do.Name,
		OrderNum:   do.OrderNum,
		IsDeleted:  do.IsDeleted,
		ParentID:   do.ParentID,
		CreateTime: do.CreateTime,
		UpdateTime: do.UpdateTime,
	}
}

// QACategoriesPO2DO QA分类PO转DO
func QACategoriesPO2DO(pos []*model.TDocQaCategory) []*cateEntity.CateInfo {
	return slicex.Map(pos, func(po *model.TDocQaCategory) *cateEntity.CateInfo {
		return qaCategoryPO2DO(po)
	})
}

func qaCategoryPO2DO(po *model.TDocQaCategory) *cateEntity.CateInfo {
	if po == nil {
		return nil
	}
	return &cateEntity.CateInfo{
		ID:         po.ID,
		BusinessID: po.BusinessID,
		RobotID:    po.RobotID,
		CorpID:     po.CorpID,
		Name:       po.Name,
		OrderNum:   int32(po.OrderNum),
		IsDeleted:  po.IsDeleted,
		ParentID:   po.ParentID,
		CreateTime: po.CreateTime,
		UpdateTime: po.UpdateTime,
	}
}

// DeleteCateById 根据主键ID删除分类
func (d *daoImpl) DeleteCateById(ctx context.Context, t cateEntity.CateObjectType, kbPrimaryId uint64) error {
	if kbPrimaryId == 0 {
		return nil
	}
	tbl := d.getCateTblName(t)
	dbTbl := d.mysql.TDocCategory
	db := dbTbl.Table(tbl).As(dbTbl.TableName()).WithContext(ctx)
	result, err := db.Where(d.mysql.TDocCategory.RobotID.Eq(kbPrimaryId)).Delete()
	if err != nil {
		return fmt.Errorf("failed to delete cate by id, tbl(%s), id(%d), err:%v", tbl, kbPrimaryId, err)
	}
	logx.I(ctx, "DeleteCateById success, tbl(%s), id(%d), rows affected: %d", tbl, kbPrimaryId, result.RowsAffected)
	return nil
}

func (d *daoImpl) DescribeCateStat(ctx context.Context, t cateEntity.CateObjectType, corpID, robotID uint64) (map[uint64]uint32, error) {
	var results []struct {
		CategoryID uint64
		Total      uint32
	}
	var err error

	switch t {
	case cateEntity.QACate:
		q := d.mysql.TDocQa
		conds := []gen.Condition{
			q.CorpID.Eq(corpID),
			q.RobotID.Eq(robotID),
			q.IsDeleted.Eq(qaEntity.QAIsNotDeleted),
			q.AcceptStatus.Eq(qaEntity.AcceptYes),
		}
		err = q.WithContext(ctx).Select(q.CategoryID, q.ID.Count().As("total")).
			Where(conds...).
			Group(q.CategoryID).
			Scan(&results)
	case cateEntity.DocCate:
		db, err := knowClient.GormClient(ctx, d.mysql.TDoc.TableName(), robotID, 0, []client.Option{}...)
		if err != nil {
			logx.E(ctx, "DescribeCateStat get db err:%v", err)
			return nil, err
		}
		q := mysqlquery.Use(db).TDoc
		conds := []gen.Condition{
			q.CorpID.Eq(corpID),
			q.RobotID.Eq(robotID),
			q.IsDeleted.Is(false),
			q.Opt.Eq(document.DocOptDocImport),
		}
		err = q.WithContext(ctx).Select(q.CategoryID, q.ID.Count().As("total")).
			Where(conds...).
			Group(q.CategoryID).
			Scan(&results)
	case cateEntity.SynonymsCate:
		cateID2Total, err := d.getSynonymsCateStat(ctx, corpID, robotID)
		return cateID2Total, err
	default:
		return nil, fmt.Errorf("unsupported cate object type: %v", t)
	}

	cateID2Total := make(map[uint64]uint32)
	for _, r := range results {
		cateID2Total[r.CategoryID] = r.Total
	}
	return cateID2Total, err
}

func (d *daoImpl) getSynonymsCateStat(ctx context.Context, corpID, robotID uint64) (map[uint64]uint32, error) {
	/*
		此原始SQL如何转为gorm，待优化，目前拆成了两个SQL，一个查询parent_id=0的记录，一个查询parent_id≠0的记录，然后内存计算
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
	*/
	// 查询parent_id=0的记录
	var primaryRecords []struct {
		CategoryID uint64
		SynonymsID uint64
	}
	tbl := d.mysql.TSynonym
	err := tbl.WithContext(ctx).Select(tbl.CategoryID, tbl.SynonymsID).
		Where(
			tbl.CorpID.Eq(corpID),
			tbl.RobotID.Eq(robotID),
			tbl.IsDeleted.Eq(0),
			tbl.ParentID.Eq(0),
		).
		Scan(&primaryRecords)
	if err != nil {
		return nil, err
	}

	// 查询parent_id≠0的记录
	var childRecords []struct {
		CategoryID uint64
		ParentID   uint64
	}
	err = tbl.WithContext(ctx).Select(tbl.CategoryID, tbl.ParentID).
		Where(
			tbl.CorpID.Eq(corpID),
			tbl.RobotID.Eq(robotID),
			tbl.IsDeleted.Eq(0),
			tbl.ParentID.Neq(0),
		).
		Scan(&childRecords)
	if err != nil {
		return nil, err
	}

	// 内存计算
	statMap := make(map[uint64]map[uint64]struct{})
	for _, r := range primaryRecords {
		if _, ok := statMap[r.CategoryID]; !ok {
			statMap[r.CategoryID] = make(map[uint64]struct{})
		}
		statMap[r.CategoryID][r.SynonymsID] = struct{}{}
	}
	for _, r := range childRecords {
		if _, ok := statMap[r.CategoryID]; !ok {
			statMap[r.CategoryID] = make(map[uint64]struct{})
		}
		statMap[r.CategoryID][r.ParentID] = struct{}{}
	}

	cateID2Total := make(map[uint64]uint32)
	for cateID, idSet := range statMap {
		cateID2Total[cateID] = uint32(len(idSet))
	}
	return cateID2Total, nil
}

func (d *daoImpl) getCateTblName(t cateEntity.CateObjectType) string {
	var tbl string
	switch t {
	case cateEntity.DocCate:
		tbl = d.mysql.TDocCategory.TableName()
	case cateEntity.SynonymsCate:
		tbl = d.mysql.TSynonymsCategory.TableName()
	default:
		tbl = d.mysql.TDocQaCategory.TableName()
	}
	return tbl
}

func (d *daoImpl) getObjTblName(t cateEntity.CateObjectType) string {
	var tbl string
	switch t {
	case cateEntity.DocCate:
		tbl = d.mysql.TDoc.TableName()
	case cateEntity.SynonymsCate:
		tbl = d.mysql.TSynonym.TableName()
	default:
		tbl = d.mysql.TDocQa.TableName()
	}
	return tbl
}

func (d *daoImpl) CreateCate(ctx context.Context, t cateEntity.CateObjectType, cate *cateEntity.CateInfo) (uint64, error) {
	now := time.Now()
	cate.UpdateTime = now
	cate.CreateTime = now

	docCate := CategoryDO2PO(cate)
	tbl := d.getCateTblName(t)
	db := d.mysql.TDocCategory.Table(tbl).WithContext(ctx)
	err := db.Create(docCate)
	if err != nil {
		return 0, fmt.Errorf("dao:CreateCate cate(%+v) tbl(%s), err:%v", t, tbl, err)
	}
	logx.I(ctx, "dao:CreateCate cate(%+v) tbl(%s) success", cate, tbl)
	return docCate.ID, nil
}

func (d *daoImpl) ModifyCate(ctx context.Context, t cateEntity.CateObjectType, id uint64, name string) error {
	updateFields := map[string]any{
		d.mysql.TDocCategory.Name.ColumnName().String():       name,
		d.mysql.TDocCategory.UpdateTime.ColumnName().String(): time.Now(),
	}
	tbl := d.getCateTblName(t)
	dbTbl := d.mysql.TDocCategory
	db := dbTbl.Table(tbl).As(dbTbl.TableName()).WithContext(ctx)
	_, err := db.Where(d.mysql.TDocCategory.ID.Eq(id)).Updates(updateFields)
	if err != nil {
		return fmt.Errorf("failed to update cate(%+v) tbl(%s), param(%+v), err:%v", t, tbl, id, err)
	}
	return nil
}

// 使用GORM Gen的事务和批量更新
func (d *daoImpl) DeleteCate(
	ctx context.Context,
	t cateEntity.CateObjectType,
	cateIDs []uint64,
	uncategorizedCateID uint64,
	robotID uint64,
) error {
	if len(cateIDs) == 0 {
		return nil
	}
	var err error
	// 第一步：标记分类为已删除
	if err = d.softDeleteCategories(ctx, t, cateIDs); err != nil {
		return err
	}

	// 第二步：将关联对象移到未分类目录
	if err = d.moveToUncategorized(ctx, t, cateIDs, uncategorizedCateID, robotID); err != nil {
		return err
	}

	return nil
}

// 标记分类为已删除
func (d *daoImpl) softDeleteCategories(
	ctx context.Context,
	t cateEntity.CateObjectType,
	cateIDs []uint64,
) error {
	updateFields := map[string]any{
		d.mysql.TDocCategory.IsDeleted.ColumnName().String():  true,
		d.mysql.TDocCategory.UpdateTime.ColumnName().String(): time.Now(),
	}
	tbl := d.getCateTblName(t)
	dbTbl := d.mysql.TDocCategory
	db := dbTbl.Table(tbl).As(dbTbl.TableName()).WithContext(ctx)
	_, err := db.Where(d.mysql.TDocCategory.ID.In(cateIDs...)).Updates(updateFields)
	if err != nil {
		return fmt.Errorf("failed to soft delete tbl(%s), param(%+v), err:%v", tbl, cateIDs, err)
	}
	return nil
}

// 将对象移动到未分类目录
func (d *daoImpl) moveToUncategorized(
	ctx context.Context,
	t cateEntity.CateObjectType,
	cateIDs []uint64,
	uncategorizedCateID uint64,
	robotID uint64) error {
	var err error
	tbl := d.getObjTblName(t)
	objectDB := d.mysql.TDocCategory.WithContext(ctx).UnderlyingDB()
	if t == cateEntity.DocCate { // 如果是文档需要根据应用id区分数据库实例
		objectDB, err = knowClient.GormClient(ctx, tbl, robotID, 0, []client.Option{}...)
		if err != nil {
			logx.E(ctx, "DeleteCate get doc GormClient failed, err:%+v", err)
			return err
		}
	}
	uint32CateIDs := make([]uint32, len(cateIDs))
	for i, v := range cateIDs {
		uint32CateIDs[i] = uint32(v)
	}
	var res *gorm.DB
	switch t {
	case cateEntity.DocCate:
		res = objectDB.Table(tbl).WithContext(ctx).Where(d.mysql.TDoc.CategoryID.In(uint32CateIDs...)).
			Where(d.mysql.TDoc.RobotID.Eq(robotID)).
			Updates(map[string]any{
				d.mysql.TDoc.CategoryID.ColumnName().String(): uncategorizedCateID,
				d.mysql.TDoc.UpdateTime.ColumnName().String(): time.Now(),
			})
	case cateEntity.QACate:
		res = objectDB.Table(tbl).WithContext(ctx).Where(d.mysql.TDocQa.CategoryID.In(uint32CateIDs...)).
			Where(d.mysql.TDocQa.RobotID.Eq(robotID)).
			Updates(map[string]any{
				d.mysql.TDocQa.CategoryID.ColumnName().String(): uncategorizedCateID,
				d.mysql.TDocQa.UpdateTime.ColumnName().String(): time.Now(),
			})
	case cateEntity.SynonymsCate:
		res = objectDB.Table(tbl).WithContext(ctx).Where(d.mysql.TSynonym.CategoryID.In(uint32CateIDs...)).
			Where(d.mysql.TSynonym.RobotID.Eq(robotID)).
			Updates(map[string]any{
				d.mysql.TSynonym.CategoryID.ColumnName().String(): uncategorizedCateID,
				d.mysql.TSynonym.UpdateTime.ColumnName().String(): time.Now(),
			})
	}
	if res != nil && res.Error != nil {
		return fmt.Errorf("failed to move to uncategorized tbl(%s), param(%+v), err:%v", tbl, cateIDs, res.Error)
	}
	return nil
}

// DescribeCateList 获取分类列表
func (d *daoImpl) DescribeCateList(
	ctx context.Context,
	t cateEntity.CateObjectType,
	corpID uint64,
	robotID uint64,
) ([]*cateEntity.CateInfo, error) {
	tbl := d.getCateTblName(t)
	dbTbl := d.mysql.TDocCategory
	db := dbTbl.Table(tbl).As(dbTbl.TableName()).WithContext(ctx)
	qs, err := db.Where(d.mysql.TDocCategory.CorpID.Eq(corpID),
		d.mysql.TDocCategory.RobotID.Eq(robotID),
		d.mysql.TDocCategory.IsDeleted.Is(false)).
		Find()
	if err != nil {
		return nil, err
	}
	return CategoriesPO2DO(qs), nil
}

// DescribeCateByID 获取分类详情
func (d *daoImpl) DescribeCateByID(
	ctx context.Context,
	t cateEntity.CateObjectType,
	id, corpID, robotID uint64,
) (*cateEntity.CateInfo, error) {
	tbl := d.getCateTblName(t)
	dbTbl := d.mysql.TDocCategory
	db := dbTbl.Table(tbl).As(dbTbl.TableName()).WithContext(ctx)
	qs, err := db.Where(d.mysql.TDocCategory.CorpID.Eq(corpID),
		d.mysql.TDocCategory.RobotID.Eq(robotID),
		d.mysql.TDocCategory.ID.Eq(id)).Find()
	if err != nil {
		return nil, fmt.Errorf("failed to get cate by id, param(%+v), err:%v", id, err)
	}
	if len(qs) == 0 {
		return nil, errx.ErrNotFound
	}
	return CategoryPO2DO(qs[0]), nil
}

func (d *daoImpl) DescribeQACateBusinessIDByID(
	ctx context.Context,
	id, corpID, robotID uint64,
) (uint64, error) {
	dbTbl := d.mysql.TDocQaCategory
	db := dbTbl.WithContext(ctx)
	qaCate, err := db.
		Where(d.mysql.TDocQaCategory.CorpID.Eq(corpID),
			d.mysql.TDocQaCategory.RobotID.Eq(robotID),
			d.mysql.TDocQaCategory.ID.Eq(id)).First()
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return 0, nil
		}
		return 0, fmt.Errorf("failed to get cate by id, param(%+v), err:%v", id, err)
	}
	if qaCate == nil {
		return 0, nil
	}
	return qaCate.BusinessID, nil
}

// DescribeCateByIDs 获取Cate详情
func (d *daoImpl) DescribeCateByIDs(ctx context.Context, t cateEntity.CateObjectType, ids []uint64) (
	map[uint64]*cateEntity.CateInfo, error) {
	list := make(map[uint64]*cateEntity.CateInfo)
	tbl := d.getCateTblName(t)
	dbTbl := d.mysql.TDocCategory
	db := dbTbl.Table(tbl).As(dbTbl.TableName()).WithContext(ctx)
	qs, err := db.Where(d.mysql.TDocCategory.ID.In(ids...)).Find()
	if err != nil {
		return nil, fmt.Errorf("failed to get cate by ids, param(%+v), err:%v", ids, err)
	}
	for _, v := range qs {
		list[v.ID] = CategoryPO2DO(v)
	}
	return list, nil
}

func (d *daoImpl) DescribeCateByBusinessID(ctx context.Context,
	t cateEntity.CateObjectType,
	cateBizID, corpID, robotID uint64) (*cateEntity.CateInfo, error) {
	tbl := d.getCateTblName(t)
	dbTbl := d.mysql.TDocCategory
	db := dbTbl.Table(tbl).As(dbTbl.TableName()).WithContext(ctx)
	qs, err := db.Where(d.mysql.TDocCategory.CorpID.Eq(corpID),
		d.mysql.TDocCategory.RobotID.Eq(robotID),
		d.mysql.TDocCategory.BusinessID.Eq(cateBizID),
		d.mysql.TDocCategory.IsDeleted.Is(false)).Find()
	if err != nil {
		return nil, fmt.Errorf("failed to get cate by business ids, cateBizID(%+v), err:%v", cateBizID, err)
	}
	if len(qs) == 0 {
		return nil, errs.ErrCateNotFound
	}
	return CategoryPO2DO(qs[0]), nil
}

// DescribeCateListByBusinessIDs 通过业务ID获取Cate列表
func (d *daoImpl) DescribeCateListByBusinessIDs(ctx context.Context,
	t cateEntity.CateObjectType,
	corpID, robotID uint64,
	cateBizIDs []uint64) (map[uint64]*cateEntity.CateInfo, error) {
	list := make(map[uint64]*cateEntity.CateInfo)
	tbl := d.getCateTblName(t)
	dbTbl := d.mysql.TDocCategory
	db := dbTbl.Table(tbl).As(dbTbl.TableName()).WithContext(ctx)
	qs, err := db.Where(
		d.mysql.TDocCategory.CorpID.Eq(corpID),
		d.mysql.TDocCategory.RobotID.Eq(robotID),
		d.mysql.TDocCategory.BusinessID.In(cateBizIDs...)).Find()
	if err != nil {
		return nil, fmt.Errorf("failed to get cate by business ids, param(%+v), err:%v", cateBizIDs, err)
	}
	for _, v := range qs {
		list[v.BusinessID] = CategoryPO2DO(v)
	}
	return list, nil
}

func (d *daoImpl) DescribeRobotUncategorizedCateID(ctx context.Context,
	t cateEntity.CateObjectType,
	corpID, robotID uint64) (uint64, error) {
	tbl := d.getCateTblName(t)
	dbTbl := d.mysql.TDocCategory
	db := dbTbl.Table(tbl).As(dbTbl.TableName()).WithContext(ctx)
	qs, err := db.Where(d.mysql.TDocCategory.CorpID.Eq(corpID),
		d.mysql.TDocCategory.RobotID.Eq(robotID),
		d.mysql.TDocCategory.ParentID.Eq(0),
		d.mysql.TDocCategory.Name.Eq(cateEntity.UncategorizedCateName)).Find()
	if err != nil {
		return 0, fmt.Errorf("failed to get robot uncategorized cate id, robotID(%d), err:%v", robotID, err)
	}
	if len(qs) == 0 {
		return 0, errx.ErrNotFound
	}
	return qs[0].ID, nil
}

// DescribeCateListByFaCateBizId 通过父分类ID获取Cate列表
func (d *daoImpl) DescribeCateListByParent(ctx context.Context,
	t cateEntity.CateObjectType,
	corpID, robotID, parentCatePrimaryId uint64,
	pageNumber, pageSize int) ([]*cateEntity.CateInfo, error) {
	tbl := d.getCateTblName(t)
	dbTbl := d.mysql.TDocCategory
	db := dbTbl.Table(tbl).As(dbTbl.TableName()).WithContext(ctx)
	offset := (pageNumber - 1) * pageSize
	qs, err := db.Where(d.mysql.TDocCategory.CorpID.Eq(corpID)).
		Where(d.mysql.TDocCategory.RobotID.Eq(robotID)).
		Where(d.mysql.TDocCategory.ParentID.Eq(parentCatePrimaryId)).
		Where(d.mysql.TDocCategory.IsDeleted.Is(false)).
		Limit(pageSize).Offset(offset).Order(d.mysql.TDocCategory.ID.Desc()).Find()
	if err != nil {
		return nil, fmt.Errorf("failed to get cate by parent cate business id, param(%+v), err:%v", parentCatePrimaryId, err)
	}
	return CategoriesPO2DO(qs), nil
}

func (d *daoImpl) VerifyCateBiz(ctx context.Context,
	t cateEntity.CateObjectType,
	corpID, cateBizID, robotID uint64) (uint64, error) {
	cate, err := d.DescribeCateByBusinessID(ctx, t, cateBizID, corpID, robotID)
	if err != nil {
		if errors.Is(err, errs.ErrCateNotFound) {
			return 0, err
		}
		logx.E(ctx, "DescribeCateByBusinessID error, cateBizID: %d err:%+v", cateBizID, err)
		return 0, errs.ErrSystem
	}
	if cate == nil {
		logx.E(ctx, "DescribeCateByBusinessID(cate is nil) cateBizID: %d corpID:%d, robotID: %d", cateBizID,
			corpID, robotID)
		return 0, errs.ErrCateNotFound
	}
	return cate.ID, nil
}

func (d *daoImpl) VerifyCate(ctx context.Context,
	t cateEntity.CateObjectType,
	corpID, cateID, robotID uint64) error {
	_, err := d.DescribeCateByID(ctx, t, cateID, corpID, robotID)
	return err
}

func (d *daoImpl) DescribeCateChildrenIDs(ctx context.Context,
	t cateEntity.CateObjectType,
	corpID, cateID, robotID uint64) ([]uint64, error) {
	cateInfos, err := d.DescribeCateList(ctx, t, corpID, robotID)
	if err != nil {
		return nil, errs.ErrSystem
	}
	node := cateEntity.BuildCateTree(cateInfos).FindNode(cateID)
	if node == nil {
		return nil, errs.ErrCateNotFound
	}
	return node.ChildrenIDs(), nil
}

func (d *daoImpl) CreateCateTree(ctx context.Context, t cateEntity.CateObjectType, corpID, robotID uint64, tree *cateEntity.CateNode) error {
	if tree == nil {
		return nil
	}
	for _, child := range tree.Children {
		cate := &cateEntity.CateInfo{
			BusinessID: idgen.GetId(),
			CorpID:     corpID,
			RobotID:    robotID,
			Name:       child.Name,
			IsDeleted:  false,
			ParentID:   tree.ID,
			CreateTime: time.Now(),
			UpdateTime: time.Now(),
		}
		id, err := d.CreateCate(ctx, t, cate)
		if err != nil {
			return fmt.Errorf("CreateCateChild error, err: %v", err)
		}
		cate.ID = id
		if err := d.CreateCateTree(ctx, t, corpID, robotID, child); err != nil {
			return err
		}
	}
	return nil
}

func getCateCacheKey(appID uint64, table string) string {
	return fmt.Sprintf("know_cate:%s:%d", table, appID)
}

func (d *daoImpl) ModifyCateCache(ctx context.Context, t cateEntity.CateObjectType, corpID, appID uint64) (map[int][]int, error) {
	if t != cateEntity.QACate && t != cateEntity.DocCate {
		return nil, errs.ErrCateTypeFail
	}
	tbl := d.getCateTblName(t)
	dbTbl := d.mysql.TDocCategory
	db := dbTbl.Table(tbl).As(dbTbl.TableName()).WithContext(ctx)
	qs, err := db.Where(d.mysql.TDocCategory.CorpID.Eq(corpID),
		d.mysql.TDocCategory.RobotID.Eq(appID),
		d.mysql.TDocCategory.IsDeleted.Is(false)).
		Select(d.mysql.TDocCategory.ID, d.mysql.TDocCategory.BusinessID, d.mysql.TDocCategory.ParentID).
		Find()
	if err != nil {
		logx.E(ctx, "SetCateCache getCateList err:%v,corpID:%v,appID:%v", err, corpID, appID)
		return nil, err
	}
	cate := CategoriesPO2DO(qs)
	cateCache := cateEntity.BuildCateCache(cate) // 构建以分类业务id为key，value为所有递归子分类的map
	tmp, err := json.Marshal(cateCache)
	if err != nil {
		logx.E(ctx, "SetCateCache json Marshal err:%v,corpID:%v,appID:%v,cateCache:%v", err, corpID, appID, cateCache)
		return nil, err
	}
	_, err = d.rdb.Set(ctx, getCateCacheKey(appID, tbl), string(tmp), 7*24*time.Hour).Result()
	if err != nil {
		logx.E(ctx, "GetCateCache set redis err:%v,appID:%v,cateCache:%v", err, appID, string(tmp))
	}
	return cateCache, nil
}

func (d *daoImpl) DescribeCateCache(ctx context.Context, t cateEntity.CateObjectType, corpID, appID uint64) (map[int][]int, error) {
	if t != cateEntity.QACate && t != cateEntity.DocCate {
		return nil, errs.ErrCateTypeFail
	}
	tbl := d.getCateTblName(t)
	key := getCateCacheKey(appID, tbl)
	res, err := d.rdb.Get(ctx, key).Result()
	if err != nil || res == "" { // 缓存为空，重新构建
		logx.W(ctx, "GetCateCache get cache err:%v,appID:%v", err, appID)
		return d.ModifyCateCache(ctx, t, corpID, appID)
	}
	var result map[int][]int
	err = json.Unmarshal([]byte(res), &result)
	if err != nil {
		logx.E(ctx, "GetCateCache Unmarshal redis err:%v,appID:%v,cateCache:%v", err, appID, res)
		return nil, err
	}
	return result, nil
}

func (d *daoImpl) GroupCateObject(ctx context.Context, t cateEntity.CateObjectType, ids []uint64, cateID uint64, app *entity.App) (err error) {
	tbl := d.getObjTblName(t)
	onceMaxNum := 500
	objectDB := d.mysql.TDocCategory.WithContext(ctx).UnderlyingDB()
	if t == cateEntity.DocCate {
		objectDB, err = knowClient.GormClient(ctx, tbl, app.PrimaryId, 0, []client.Option{}...)
		if err != nil {
			logx.E(ctx, "GroupCateObject get doc GormClient failed, err:%+v", err)
			return err
		}
	} else if t == cateEntity.SynonymsCate {
		// 同义词场景下，标准词会对应多个相似词，这里减少一次处理条数
		onceMaxNum = 200
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
		switch t {
		case cateEntity.QACate:
			err = tx.WithContext(ctx).Table(tbl).
				Where(d.mysql.TDocQa.ID.In(tmp...)).
				Updates(map[string]any{
					d.mysql.TDocQa.CategoryID.ColumnName().String(): cateID,
					d.mysql.TDocQa.UpdateTime.ColumnName().String(): time.Now(),
				}).Error
		case cateEntity.DocCate:
			err = tx.WithContext(ctx).Table(tbl).
				Where(d.mysql.TDoc.ID.In(tmp...)).
				Updates(map[string]any{
					d.mysql.TDoc.CategoryID.ColumnName().String(): cateID,
					d.mysql.TDoc.UpdateTime.ColumnName().String(): time.Now(),
				}).Error
		case cateEntity.SynonymsCate:
			err = tx.WithContext(ctx).Table(tbl).
				Where(d.mysql.TSynonym.ID.In(tmp...)).
				Updates(map[string]any{
					d.mysql.TSynonym.CategoryID.ColumnName().String(): cateID,
					d.mysql.TSynonym.UpdateTime.ColumnName().String(): time.Now(),
				}).Error
		}
		if err != nil {
			logx.E(ctx, "分组(%+v)失败 ids:%+v cateID:%+v err:%+v", t, ids, cateID, err)
			return err
		}
		if t == cateEntity.SynonymsCate {
			err := tx.WithContext(ctx).Table(d.mysql.TSynonym.TableName()).
				Where(d.mysql.TSynonym.ParentID.In(convx.SliceUint64ToInt64(tmp)...)).
				Updates(map[string]any{
					d.mysql.TSynonym.CategoryID.ColumnName().String(): cateID,
					d.mysql.TSynonym.UpdateTime.ColumnName().String(): time.Now(),
				}).Error
			if err != nil {
				logx.E(ctx, "同义词分组(%+v)失败 ids:%+v cateID:%+v err:%+v", t, ids, cateID, err)
				return err
			}
		}
	}
	return nil
}

func (d *daoImpl) InitDefaultCategory(ctx context.Context, t cateEntity.CateObjectType, corpID, robotID uint64) error {
	if t != cateEntity.QACate && t != cateEntity.DocCate {
		return errs.ErrCateTypeFail
	}
	now := time.Now()
	cate := &cateEntity.CateInfo{
		BusinessID: idgen.GetId(),
		RobotID:    robotID,
		CorpID:     corpID,
		Name:       cateEntity.UncategorizedCateName,
		ParentID:   0,
		IsDeleted:  false,
		UpdateTime: now,
		CreateTime: now,
	}
	docCate := CategoryDO2PO(cate)
	tbl := d.getCateTblName(t)
	db := d.mysql.TDocCategory.Table(tbl).WithContext(ctx)
	err := db.Create(docCate)
	if err != nil {
		return fmt.Errorf("failed to create cate(%+v) tbl(%s), err:%v", t, tbl, err)

	}
	return nil
}

func (d *daoImpl) GetDocCategoryByCursor(ctx context.Context, corpID, robotID uint64, lastID uint64, limit int, db *gorm.DB) ([]*cateEntity.CateInfo, error) {
	tbl := d.mysql.TDocCategory
	tableName := tbl.TableName()
	categoryList := make([]*model.TDocCategory, 0)

	session := db
	if session == nil {
		session = tbl.WithContext(ctx).UnderlyingDB()
	}

	session = session.WithContext(ctx).Table(tableName).
		Where(tbl.CorpID.ColumnName().String()+util.SqlEqual, corpID).
		Where(tbl.RobotID.ColumnName().String()+util.SqlEqual, robotID).
		Where(tbl.IsDeleted.ColumnName().String()+util.SqlEqual, false)

	// 添加游标分页条件
	if lastID > 0 {
		session = session.Where(tbl.BusinessID.ColumnName().String()+util.SqlMore, lastID)
	}

	session = session.Order(tbl.BusinessID.ColumnName().String() + " " + util.SqlOrderByAsc).
		Limit(limit)

	res := session.Find(&categoryList)
	if res.Error != nil {
		logx.E(ctx, "GetDocCategoryByCursor execute sql failed, err: %+v", res.Error)
		return nil, res.Error
	}
	return CategoriesPO2DO(categoryList), nil
}

func (d *daoImpl) GetQCategoryByCursor(ctx context.Context, corpID, robotID uint64, lastID uint64, limit int, db *gorm.DB) ([]*cateEntity.CateInfo, error) {
	tbl := d.mysql.TDocQaCategory
	tableName := tbl.TableName()
	categoryList := make([]*model.TDocQaCategory, 0)

	session := db
	if session == nil {
		session = tbl.WithContext(ctx).UnderlyingDB()
	}

	session = session.WithContext(ctx).Table(tableName).
		Where(tbl.CorpID.ColumnName().String()+util.SqlEqual, corpID).
		Where(tbl.RobotID.ColumnName().String()+util.SqlEqual, robotID).
		Where(tbl.IsDeleted.ColumnName().String()+util.SqlEqual, false)

	// 添加游标分页条件
	if lastID > 0 {
		session = session.Where(tbl.BusinessID.ColumnName().String()+util.SqlMore, lastID)
	}

	session = session.Order(tbl.BusinessID.ColumnName().String() + " " + util.SqlOrderByAsc).
		Limit(limit)

	res := session.Find(&categoryList)
	if res.Error != nil {
		logx.E(ctx, "GetQCategoryByCursor execute sql failed, err: %+v", res.Error)
		return nil, res.Error
	}
	return QACategoriesPO2DO(categoryList), nil
}
