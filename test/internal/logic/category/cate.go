package category

import (
	"context"

	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/gox/convx"
	"git.woa.com/adp/common/x/gox/ptrx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/async/scheduler"
	"git.woa.com/adp/kb/kb-config/internal/dao/category"
	docDao "git.woa.com/adp/kb/kb-config/internal/dao/document"
	qaDao "git.woa.com/adp/kb/kb-config/internal/dao/qa"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	cateEntity "git.woa.com/adp/kb/kb-config/internal/entity/category"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
)

type Logic struct {
	dao    category.Dao
	qaDao  qaDao.Dao
	docDao docDao.Dao
}

func NewLogic(dao category.Dao, qaDao qaDao.Dao, docDao docDao.Dao) *Logic {
	return &Logic{
		dao:    dao,
		qaDao:  qaDao,
		docDao: docDao,
	}
}

func (l *Logic) CreateCate(ctx context.Context, t cateEntity.CateObjectType, cate *cateEntity.CateInfo) (uint64, error) {
	return l.dao.CreateCate(ctx, t, cate)
}

func (l *Logic) DescribeCateStat(ctx context.Context, t cateEntity.CateObjectType, corpID, robotID uint64) (map[uint64]uint32, error) {
	return l.dao.DescribeCateStat(ctx, t, corpID, robotID)
}

func (l *Logic) ModifyCate(ctx context.Context, t cateEntity.CateObjectType, id uint64, name string) error {
	return l.dao.ModifyCate(ctx, t, id, name)
}

func (l *Logic) DeleteCate(ctx context.Context, t cateEntity.CateObjectType, cateIDs []uint64, uncategorizedCateID uint64, app *entity.App) error {
	if len(cateIDs) == 0 {
		return nil
	}
	robotID, appBizID := app.PrimaryId, app.BizId
	// feature_permission 获取需要变更的文档/问答id 触发向量更新
	qaIds, docIds := make([]uint64, 0), make([]uint64, 0)
	if t == cateEntity.QACate {
		qaList, err := l.qaDao.GetAllDocQas(ctx, []string{qaEntity.DocQaTblColId}, &qaEntity.DocQaFilter{
			RobotId:     robotID,
			IsDeleted:   ptrx.Uint32(qaEntity.QAIsNotDeleted), // t_doc_qa 1是正常 2是已删除
			CategoryIds: cateIDs,
		})
		if err != nil {
			logx.E(ctx, "feature_permission DeleteCate get qaList err:%v,cateIDs:%v,appId:%v", err, cateIDs, robotID)
			return err
		}
		for _, v := range qaList {
			qaIds = append(qaIds, v.ID)
		}
	} else if t == cateEntity.DocCate {
		docList, err := l.docDao.GetAllDocs(ctx, []string{docEntity.DocTblColId, docEntity.DocTblColExpireStart, docEntity.DocTblColExpireEnd},
			&docEntity.DocFilter{
				RobotId:     robotID,
				IsDeleted:   ptrx.Bool(false),
				CategoryIds: convx.SliceUint64ToUint32(cateIDs),
			})
		if err != nil {
			logx.E(ctx, "feature_permission DeleteCate get docList err:%v,cateIDs:%v,appId:%v", err, cateIDs, robotID)
			return err
		}
		for _, v := range docList {
			docIds = append(docIds, v.ID)
		}
	}
	err := l.dao.DeleteCate(ctx, t, cateIDs, uncategorizedCateID, robotID)
	if err != nil {
		logx.E(ctx, "删除分类失败, cateIDs:%v,err:%+v", cateIDs, err)
		return err
	}
	// feature_permission 文档和分类更改分类需要更新向量标签
	if len(docIds) == 0 && len(qaIds) == 0 {
		return nil
	}
	updateData := make(map[uint64]entity.KnowData)
	updateData[appBizID] = entity.KnowData{
		DocIDs: docIds,
		QaIDs:  qaIds,
	}
	err = scheduler.NewBatchUpdateVectorTask(ctx, appBizID, entity.BatchUpdateVector{
		Type:      entity.UpdateVectorByCate,
		CorpBizID: contextx.Metadata(ctx).CorpBizID(),
		AppBizID:  app.BizId,
		KnowIDs:   updateData,
	})
	if err != nil {
		logx.E(ctx, "feature_permission DeleteCate BatchUpdateVector err:%v", err)
		return err
	}
	return nil
}

func (l *Logic) DescribeCateList(ctx context.Context, t cateEntity.CateObjectType, corpID, robotID uint64) ([]*cateEntity.CateInfo, error) {
	return l.dao.DescribeCateList(ctx, t, corpID, robotID)
}

func (l *Logic) DescribeCateByID(ctx context.Context, t cateEntity.CateObjectType, id, corpID, robotID uint64) (*cateEntity.CateInfo, error) {
	return l.dao.DescribeCateByID(ctx, t, id, corpID, robotID)
}

func (l *Logic) DescribeCateByIDs(ctx context.Context, t cateEntity.CateObjectType, ids []uint64) (map[uint64]*cateEntity.CateInfo, error) {
	return l.dao.DescribeCateByIDs(ctx, t, ids)
}

func (l *Logic) DescribeCateByBusinessID(ctx context.Context, t cateEntity.CateObjectType, cateBizID, corpID, robotID uint64) (*cateEntity.CateInfo, error) {
	return l.dao.DescribeCateByBusinessID(ctx, t, cateBizID, corpID, robotID)
}

func (l *Logic) DescribeRobotUncategorizedCateID(ctx context.Context, t cateEntity.CateObjectType, corpID, robotID uint64) (uint64, error) {
	return l.dao.DescribeRobotUncategorizedCateID(ctx, t, corpID, robotID)
}

func (l *Logic) DescribeCateListByBusinessIDs(ctx context.Context, t cateEntity.CateObjectType, corpID, robotID uint64, cateBizIDs []uint64) (map[uint64]*cateEntity.CateInfo, error) {
	return l.dao.DescribeCateListByBusinessIDs(ctx, t, corpID, robotID, cateBizIDs)
}

func (l *Logic) DescribeCateListByParent(ctx context.Context, t cateEntity.CateObjectType, corpID, robotID, parentCateBizId uint64,
	pageNumber, pageSize int) ([]*cateEntity.CateInfo, error) {
	var parentCateInfo *cateEntity.CateInfo
	var err error
	if parentCateBizId == 0 { // 全部分类
		parentCateInfo = &cateEntity.CateInfo{
			ID:   0,
			Name: cateEntity.UncategorizedCateName,
		}
	} else {
		parentCateInfo, err = l.dao.DescribeCateByBusinessID(ctx, t, parentCateBizId, corpID, robotID)
		if err != nil {
			return nil, err
		}
	}
	if pageNumber < 1 {
		pageNumber = 1
	}
	if pageSize <= 0 {
		pageSize = 10
	}
	children, err := l.dao.DescribeCateListByParent(ctx, t, corpID, robotID, parentCateInfo.ID, pageNumber, pageSize)
	if err != nil {
		return nil, err
	}
	return children, nil
}

func (l *Logic) VerifyCateBiz(ctx context.Context, t cateEntity.CateObjectType, corpID, cateBizID, robotID uint64) (uint64, error) {
	return l.dao.VerifyCateBiz(ctx, t, corpID, cateBizID, robotID)
}

func (l *Logic) VerifyCate(ctx context.Context, t cateEntity.CateObjectType, corpID, cateID, robotID uint64) error {
	return l.dao.VerifyCate(ctx, t, corpID, cateID, robotID)
}

func (l *Logic) DescribeCateChildrenIDs(ctx context.Context, t cateEntity.CateObjectType, corpID, cateID, robotID uint64) ([]uint64, error) {
	return l.dao.DescribeCateChildrenIDs(ctx, t, corpID, cateID, robotID)
}

func (l *Logic) ModifyCateCache(ctx context.Context, t cateEntity.CateObjectType, corpID, appID uint64) (map[int][]int, error) {
	return l.dao.ModifyCateCache(ctx, t, corpID, appID)
}

func (l *Logic) DescribeCateCache(ctx context.Context, t cateEntity.CateObjectType, corpID, appID uint64) (map[int][]int, error) {
	return l.dao.DescribeCateCache(ctx, t, corpID, appID)
}

func (l *Logic) GroupCateObject(ctx context.Context, t cateEntity.CateObjectType, ids []uint64, cateID uint64, app *entity.App) error {
	err := l.dao.GroupCateObject(ctx, t, ids, cateID, app)
	if err != nil {
		return err
	}
	if t == cateEntity.QACate {
		updateData := make(map[uint64]entity.KnowData)
		updateData[app.BizId] = entity.KnowData{
			QaIDs: ids,
		}
		err = scheduler.NewBatchUpdateVectorTask(ctx, app.BizId, entity.BatchUpdateVector{
			Type:      entity.UpdateVectorByCate,
			CorpBizID: contextx.Metadata(ctx).CorpBizID(),
			AppBizID:  app.BizId,
			KnowIDs:   updateData,
		})
	} else if t == cateEntity.DocCate {
		updateData := make(map[uint64]entity.KnowData)
		updateData[app.BizId] = entity.KnowData{
			DocIDs: ids,
		}
		err = scheduler.NewBatchUpdateVectorTask(ctx, app.BizId, entity.BatchUpdateVector{
			Type:      entity.UpdateVectorByCate,
			CorpBizID: contextx.Metadata(ctx).CorpBizID(),
			AppBizID:  app.BizId,
			KnowIDs:   updateData,
		})
	}
	if err != nil {
		logx.E(ctx, "feature_permission cateGroup BatchUpdateVector err:%v", err)
		return err
	}
	return nil
}

func (l *Logic) InitDefaultCategory(ctx context.Context, corpID, robotID uint64) error {
	err := l.dao.InitDefaultCategory(ctx, cateEntity.QACate, corpID, robotID)
	if err != nil {
		logx.E(ctx, "InitDefaultCategory err:%v", err)
		return err
	}
	err = l.dao.InitDefaultCategory(ctx, cateEntity.DocCate, corpID, robotID)
	if err != nil {
		logx.E(ctx, "InitDefaultCategory err:%v", err)
		return err
	}
	return nil
}
