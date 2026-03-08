package kb

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"gorm.io/gen"
	"gorm.io/gen/field"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"git.woa.com/adp/common/x/errx"
	"git.woa.com/adp/common/x/gox/boolx"
	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/model"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/tdsqlquery"
	kbe "git.woa.com/adp/kb/kb-config/internal/entity/kb"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
)

var (
	KnowledgeBaseTblColCorpBizId      = "corp_biz_id"
	KnowledgeBaseTblColKnowBizId      = "knowledge_biz_id"
	KnowledgeBaseTblColProcessingFlag = "processing_flag"
	KnowledgeBaseTblColIsDeleted      = "is_deleted"  // 是否删除
	KnowledgeBaseTblColCreateTime     = "create_time" // 创建时间
	KnowledgeBaseTblColUpdateTime     = "update_time" // 更新时间

	sqlEqual = " = ?"
	sqlIn    = " IN ?"

	SqlOrderByAsc  = "ASC"
	SqlOrderByDesc = "DESC"

	knowledgeBaseTableName = "t_knowledge_base"
)

func (d *daoImpl) DescribeKnowledgeBase(ctx context.Context, corpBizID uint64, ids []uint64) ([]*kbe.KnowledgeBase, error) {
	tbl := d.tdsql.TKnowledgeBase
	conds := []gen.Condition{
		tbl.CorpBizID.Eq(corpBizID),
		tbl.ID.In(ids...),
		tbl.IsDeleted.Is(false),
	}
	qs, err := tbl.WithContext(ctx).Where(conds...).Find()
	if err != nil {
		return nil, err
	}
	return knowledgeBasesPO2DO(qs), nil
}

func knowledgeBasesPO2DO(pos []*model.TKnowledgeBase) []*kbe.KnowledgeBase {
	return slicex.Map(pos, func(po *model.TKnowledgeBase) *kbe.KnowledgeBase {
		return knowledgeBasePO2DO(po)
	})
}

func knowledgeBasePO2DO(po *model.TKnowledgeBase) *kbe.KnowledgeBase {
	if po == nil {
		return nil
	}
	return &kbe.KnowledgeBase{
		ID:             po.ID,
		CorpBizID:      po.CorpBizID,
		KnowledgeBizId: po.KnowledgeBizID,
		ProcessingFlag: po.ProcessingFlag,
		IsDeleted:      po.IsDeleted,
		CreateTime:     po.CreateTime,
		UpdateTime:     po.UpdateTime,
	}
}

func (d *daoImpl) SetKnowledgeBase(ctx context.Context, corpBizId, knowledgeBizId, processingFlag uint64) error {
	knowledgeBase := &model.TKnowledgeBase{
		CorpBizID:      corpBizId,
		KnowledgeBizID: knowledgeBizId,
		ProcessingFlag: processingFlag,
		IsDeleted:      false,
		CreateTime:     time.Now(),
		UpdateTime:     time.Now(),
	}
	q := d.tdsql.TKnowledgeBase.WithContext(ctx)
	err := q.UnderlyingDB().Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: KnowledgeBaseTblColCorpBizId},
			{Name: KnowledgeBaseTblColKnowBizId},
			{Name: KnowledgeBaseTblColProcessingFlag},
		},
		DoUpdates: clause.AssignmentColumns([]string{
			KnowledgeBaseTblColProcessingFlag,
			KnowledgeBaseTblColUpdateTime,
			KnowledgeBaseTblColIsDeleted,
		}), // 冲突时更新 flag,update_time,is_deleted 字段
	}).Create(knowledgeBase).Error
	if err != nil {
		logx.E(ctx, "SetKnowledgeBase set corpBizId:%d knowledgeBizId:%d processingFlag:%d err:%+v",
			corpBizId, knowledgeBizId, processingFlag, err)
		return err
	}
	return nil
}

// GetKnowledgeBases 获取知识库信息
func (d *daoImpl) GetKnowledgeBases(ctx context.Context,
	corpBizId uint64, knowledgeBizIds []uint64) ([]*kbe.KnowledgeBase, error) {
	var knowledgeBases []*kbe.KnowledgeBase
	q := d.tdsql.TKnowledgeBase.WithContext(ctx)
	err := q.UnderlyingDB().Model(&kbe.KnowledgeBase{}).
		Where(KnowledgeBaseTblColCorpBizId+sqlEqual, corpBizId).
		Where(KnowledgeBaseTblColKnowBizId+sqlIn, knowledgeBizIds).
		Where(KnowledgeBaseTblColIsDeleted+sqlEqual, kbe.IsNotDeleted).
		Find(&knowledgeBases).Error
	if err != nil {
		logx.E(ctx, "GetKnowledgeBase corpBizId:%d knowledgeBizIds:%d err:%+v",
			corpBizId, knowledgeBizIds, err)
		return nil, err
	}
	return knowledgeBases, err
}

// DeleteKnowledgeBases 删除知识库信息
func (d *daoImpl) DeleteKnowledgeBases(ctx context.Context, corpBizId uint64, knowledgeBizIds []uint64) error {
	q := d.tdsql.TKnowledgeBase.WithContext(ctx)
	knowledgeBase := &kbe.KnowledgeBase{
		IsDeleted: true,
	}
	updateColumns := []string{KnowledgeBaseTblColIsDeleted}
	res := q.UnderlyingDB().Table(knowledgeBaseTableName).Select(updateColumns).
		Where(KnowledgeBaseTblColCorpBizId+sqlEqual, corpBizId).
		Where(KnowledgeBaseTblColKnowBizId+sqlIn, knowledgeBizIds).
		Updates(knowledgeBase)
	if res.Error != nil {
		logx.E(ctx, "DeleteKnowledgeBases corpBizId:%+v knowledgeBizIds:%+v err:%+v",
			corpBizId, knowledgeBizIds, res.Error)
		return res.Error
	}
	return nil
}

// GetAppShareKGList 获取应用引用共享库的列表
func (d *daoImpl) GetAppShareKGList(ctx context.Context, appBizID uint64) ([]*kbe.AppShareKnowledge, error) {
	tbl := d.tdsql.TAppShareKnowledge
	conds := []gen.Condition{
		tbl.AppBizID.Eq(appBizID),
	}
	qs, err := tbl.WithContext(ctx).Where(conds...).Find()
	if err != nil {
		return nil, err
	}
	return shareKnowledgesPO2DO(qs), nil
}

func shareKnowledgesPO2DO(pos []*model.TAppShareKnowledge) []*kbe.AppShareKnowledge {
	//	  相当于
	//    for _, po := range pos {
	//        result = append(result, shareKnowledgePO2DO(po))
	//    }
	return slicex.Map(pos, func(po *model.TAppShareKnowledge) *kbe.AppShareKnowledge {
		return shareKnowledgePO2DO(po)
	})
}

func shareKnowledgePO2DO(po *model.TAppShareKnowledge) *kbe.AppShareKnowledge {
	if po == nil {
		return nil
	}
	return &kbe.AppShareKnowledge{
		ID:             po.ID,
		CorpBizID:      uint64(po.CorpBizID),
		KnowledgeBizID: po.KnowledgeBizID,
		AppBizID:       po.AppBizID,
	}
}

// GetAppShareKGListProd 获取应用引用共享库的列表
func (d *daoImpl) GetAppShareKGListProd(ctx context.Context, appBizID uint64) ([]*kbe.AppShareKnowledge, error) {
	db := d.tdsql.TAppShareKnowledgeProd.WithContext(ctx)
	conds := []gen.Condition{
		d.tdsql.TAppShareKnowledgeProd.AppBizID.Eq(appBizID),
	}
	qs, err := db.Where(conds...).Find()
	if err != nil {
		return nil, err
	}
	return getAppShareKGListsPO2DO(qs), nil
}

func getAppShareKGListsPO2DO(pos []*model.TAppShareKnowledgeProd) []*kbe.AppShareKnowledge {
	return slicex.Map(pos, func(po *model.TAppShareKnowledgeProd) *kbe.AppShareKnowledge {
		return getAppShareKGListPO2DO(po)
	})
}

func getAppShareKGListPO2DO(po *model.TAppShareKnowledgeProd) *kbe.AppShareKnowledge {
	if po == nil {
		return nil
	}
	return &kbe.AppShareKnowledge{
		ID:             po.ID,
		CorpBizID:      po.CorpBizID,
		KnowledgeBizID: po.KnowledgeBizID,
		AppBizID:       po.AppBizID,
	}
}

// GetShareKGAppBizIDList 获取共享库的引用应用列表
func (d *daoImpl) GetShareKGAppBizIDList(ctx context.Context, knowledgeBizId []uint64) ([]*kbe.AppShareKnowledge, error) {
	db := d.tdsql.TAppShareKnowledge.WithContext(ctx)
	conds := []gen.Condition{
		d.tdsql.TAppShareKnowledge.KnowledgeBizID.In(knowledgeBizId...),
	}
	qs, err := db.Where(conds...).Find()
	if err != nil {
		return nil, err
	}
	return getShareKGAppBizIDListsPO2DO(qs), nil
}

func getShareKGAppBizIDListsPO2DO(pos []*model.TAppShareKnowledge) []*kbe.AppShareKnowledge {
	return slicex.Map(pos, func(po *model.TAppShareKnowledge) *kbe.AppShareKnowledge {
		return getShareKGAppBizIDListPO2DO(po)
	})
}

func getShareKGAppBizIDListPO2DO(po *model.TAppShareKnowledge) *kbe.AppShareKnowledge {
	if po == nil {
		return nil
	}
	return &kbe.AppShareKnowledge{
		ID:             po.ID,
		CorpBizID:      uint64(po.CorpBizID),
		KnowledgeBizID: po.KnowledgeBizID,
		AppBizID:       po.AppBizID,
	}
}

// CreateAppShareKG 创建应用引用共享库
func (d *daoImpl) CreateAppShareKG(ctx context.Context, appShareKGs []*kbe.AppShareKnowledge) error {
	db := d.tdsql.TAppShareKnowledge.WithContext(ctx)
	ret := createAppShareKGsDO2PO(appShareKGs)
	err := db.Create(ret...)
	if err != nil {
		return err
	}
	return nil
}

func createAppShareKGsDO2PO(pos []*kbe.AppShareKnowledge) []*model.TAppShareKnowledge {
	return slicex.Map(pos, func(po *kbe.AppShareKnowledge) *model.TAppShareKnowledge {
		return createAppShareKGDO2PO(po)
	})
}

func createAppShareKGDO2PO(po *kbe.AppShareKnowledge) *model.TAppShareKnowledge {
	if po == nil {
		return nil
	}
	return &model.TAppShareKnowledge{
		ID:             po.ID,
		CorpBizID:      int64(po.CorpBizID),
		KnowledgeBizID: po.KnowledgeBizID,
		AppBizID:       po.AppBizID,
		CreateTime:     po.CreateTime,
		UpdateTime:     po.UpdateTime,
	}
}

// DeleteAppShareKG 删除应用引用共享库（硬删除）
func (d *daoImpl) DeleteAppShareKG(ctx context.Context, appBizID uint64, knowledgeBizIDs []uint64) error {
	db := d.tdsql.TAppShareKnowledge.WithContext(ctx)
	conds := []gen.Condition{
		d.tdsql.TAppShareKnowledge.AppBizID.Eq(appBizID),
		d.tdsql.TAppShareKnowledge.KnowledgeBizID.In(knowledgeBizIDs...),
	}
	qs, err := db.Where(conds...).Delete()
	if err != nil {
		return err
	}
	if qs.RowsAffected == 0 {
		return errx.ErrNotFound
	}
	return nil
}

// ExistShareKG 应用是否引用了共享库
func (d *daoImpl) ExistShareKG(ctx context.Context, appBizID uint64) (*kbe.AppShareKnowledge, error) {
	db := d.tdsql.TAppShareKnowledge.WithContext(ctx)
	conds := []gen.Condition{
		d.tdsql.TAppShareKnowledge.AppBizID.Eq(appBizID),
	}
	qs, err := db.Where(conds...).Limit(1).First()
	if err != nil {
		return nil, err
	}
	return ExistShareKGsPO2DO(qs), nil
}

func ExistShareKGsPO2DO(po *model.TAppShareKnowledge) *kbe.AppShareKnowledge {
	if po == nil {
		return nil
	}
	return &kbe.AppShareKnowledge{
		ID:             po.ID,
		AppBizID:       po.AppBizID,
		CorpBizID:      uint64(po.CorpBizID),
		KnowledgeBizID: po.KnowledgeBizID,
		UpdateTime:     po.UpdateTime,
		CreateTime:     po.CreateTime,
	}
}

// CreateSharedKnowledge 创建共享知识库（无事务模式）
func (d *daoImpl) CreateSharedKnowledge(ctx context.Context, params *kbe.CreateSharedKnowledgeParams) (uint64, error) {
	logx.D(ctx, "CreateSharedKnowledge: params:%+v", params)
	q := d.tdsql.TShareKnowledge.WithContext(ctx)
	info := &model.TShareKnowledge{
		CorpBizID:      params.CorpBizID,
		BusinessID:     params.KnowledgeBizID,
		Name:           params.Name,
		Description:    params.Description,
		UserBizID:      params.UserBizID,
		UserName:       params.UserName,
		EmbeddingModel: params.EmbeddingModel,
		IsDeleted:      false,
		UpdateTime:     time.Now(),
		CreateTime:     time.Now(),
		SpaceID:        params.SpaceID,
		OwnerStaffID:   params.OwnerStaffID,
	}
	if err := q.Create(info); err != nil {
		return 0, fmt.Errorf("CreateSharedKnowledge faield err : %v", err)
	}
	return info.ID, nil
}

// UpdateSharedKnowledge 更新共享知识库
func (d *daoImpl) UpdateSharedKnowledge(ctx context.Context, corpBizID, knowledgeBizID uint64,
	userInfo *pb.UserBaseInfo, updateInfo *pb.KnowledgeUpdateInfo) (int64, error) {
	q := d.tdsql.TShareKnowledge.WithContext(ctx)
	//  构建更新字段（显式指定非零值字段）
	updateData := map[string]any{
		d.tdsql.TShareKnowledge.EmbeddingModel.ColumnName().String(): updateInfo.GetEmbeddingModel(),
		d.tdsql.TShareKnowledge.UpdateTime.ColumnName().String():     time.Now(),
	}

	if updateInfo.GetKnowledgeName() != "" {
		updateData[d.tdsql.TShareKnowledge.Name.ColumnName().String()] = updateInfo.GetKnowledgeName()
	}

	if updateInfo.GetKnowledgeDescription() != "" {
		updateData[d.tdsql.TShareKnowledge.Description.ColumnName().String()] = updateInfo.GetKnowledgeDescription()
	}

	if userInfo != nil {
		updateData[d.tdsql.TShareKnowledge.UserBizID.ColumnName().String()] = userInfo.GetUserBizId()
		updateData[d.tdsql.TShareKnowledge.UserName.ColumnName().String()] = userInfo.GetUserName()
	}

	result, err := q.Where(
		d.tdsql.TShareKnowledge.CorpBizID.Eq(corpBizID),
		d.tdsql.TShareKnowledge.BusinessID.Eq(knowledgeBizID),
		d.tdsql.TShareKnowledge.IsDeleted.Is(false),
	).Updates(updateData)

	if err != nil {
		return 0, fmt.Errorf("UpdateSharedKnowledge faield err : %v", err)
	}
	return result.RowsAffected, nil
}

func genShareKnowledgeConditions(q *tdsqlquery.Query, filter *kbe.ShareKnowledgeFilter) []gen.Condition {
	if q == nil || filter == nil {
		return nil
	}
	conditions := []gen.Condition{
		q.TShareKnowledge.CorpBizID.Eq(filter.CorpBizID),
		q.TShareKnowledge.BusinessID.In(filter.BizIds...),
	}
	if filter.WithDeleted == nil {
		conditions = append(conditions, q.TShareKnowledge.IsDeleted.Is(false))
	}
	return conditions
}

// RetrieveBaseSharedKnowledge 查询共享知识库基础信息（Gen风格）
func (d *daoImpl) RetrieveBaseSharedKnowledge(ctx context.Context, filter *kbe.ShareKnowledgeFilter) ([]*kbe.SharedKnowledgeInfo, error) {
	// 1. 空列表快速返回
	if len(filter.BizIds) == 0 {
		return nil, nil
	}
	// 2. 初始化Gen生成的Query
	q := d.tdsql.TShareKnowledge.WithContext(ctx).Debug()
	conds := genShareKnowledgeConditions(d.tdsql, filter)
	result, err := q.Where(conds...).Find()
	if err != nil {
		logx.E(ctx, "RetrieveBaseSharedKnowledge find failed ,err: %v", err)
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errx.ErrNotFound
		}
		return nil, fmt.Errorf("RetrieveBaseSharedKnowledge find failed ,err: %v", err)
	}
	// 5. 处理空结果
	if len(result) == 0 {
		return nil, errx.ErrNotFound
	}
	return RetrieveBaseSharedKnowledgesPO2DO(result), nil
}

func RetrieveBaseSharedKnowledgesPO2DO(pos []*model.TShareKnowledge) []*kbe.SharedKnowledgeInfo {
	return slicex.Map(pos, func(po *model.TShareKnowledge) *kbe.SharedKnowledgeInfo {
		return retrieveBaseSharedKnowledgePO2DO(po)
	})
}

func retrieveBaseSharedKnowledgePO2DO(po *model.TShareKnowledge) *kbe.SharedKnowledgeInfo {
	if po == nil {
		return nil
	}
	return &kbe.SharedKnowledgeInfo{
		ID:             po.ID,
		CorpBizID:      po.CorpBizID,
		BusinessID:     po.BusinessID,
		Name:           po.Name,
		Description:    po.Description,
		UserBizID:      po.UserBizID,
		UserName:       po.UserName,
		EmbeddingModel: po.EmbeddingModel,
		QaExtractModel: po.QaExtractModel,
		IsDeleted:      po.IsDeleted,
		CreateTime:     po.CreateTime,
		UpdateTime:     po.UpdateTime,
		SpaceId:        po.SpaceID,
		OwnerStaffID:   po.OwnerStaffID,
	}
}

// ListBaseSharedKnowledge 分页查询共享知识库（Gen风格）
func (d *daoImpl) ListBaseSharedKnowledge(ctx context.Context, corpBizID uint64, knowledgeBizIDList []uint64, pageNumber, pageSize uint32,
	keyword string, spaceID string) ([]*kbe.SharedKnowledgeInfo, error) {
	// 1. 参数校验
	if pageNumber == 0 || pageSize == 0 {
		return nil, fmt.Errorf("分页参数无效: pageNumber=%d, pageSize=%d", pageNumber, pageSize)
	}
	// 2. 初始化Gen生成的Query
	q := d.tdsql.TShareKnowledge.WithContext(ctx)
	// 3. 构建类型安全查询条件
	q = q.Where(
		d.tdsql.TShareKnowledge.CorpBizID.Eq(corpBizID),
		d.tdsql.TShareKnowledge.IsDeleted.Is(false),
	)
	// 4. 动态添加条件
	if len(knowledgeBizIDList) > 0 {
		q = q.Where(d.tdsql.TShareKnowledge.BusinessID.In(knowledgeBizIDList...))
	}
	if keyword != "" {
		filter := fmt.Sprintf("%%%s%%", strings.ReplaceAll(keyword, "%", "\\%"))
		q = q.Where(field.Or(d.tdsql.TShareKnowledge.Name.Like(filter), d.tdsql.TShareKnowledge.UserName.Like(filter)))
	}
	if spaceID != "" {
		q = q.Where(d.tdsql.TShareKnowledge.SpaceID.Eq(spaceID))
	}
	// 5. 执行分页查询
	offset := int(pageSize * (pageNumber - 1))
	result, err := q.Order(d.tdsql.TShareKnowledge.UpdateTime.Desc()).Offset(offset).Limit(int(pageSize)).Find()
	if err != nil {
		return nil, fmt.Errorf("ListBaseSharedKnowledge Find failed: %v", err)
	}
	return RetrieveBaseSharedKnowledgesPO2DO(result), nil
}

// RetrieveSharedKnowledgeCount 获取共享知识库数量（Gen风格）
func (d *daoImpl) RetrieveSharedKnowledgeCount(ctx context.Context, corpBizID uint64, knowledgeBizIDList []uint64,
	keyword string, spaceID string) (int64, error) {
	// 1. 初始化Gen生成的Query
	tbl := d.tdsql.TShareKnowledge
	q := tbl.WithContext(ctx)
	// 2. 构建类型安全查询条件
	q = q.Where(
		tbl.CorpBizID.Eq(corpBizID),
		tbl.IsDeleted.Is(false),
	)
	// 3. 动态添加条件
	if len(knowledgeBizIDList) > 0 {
		q = q.Where(tbl.BusinessID.In(knowledgeBizIDList...))
	}
	if keyword != "" {
		filter := fmt.Sprintf("%%%s%%", strings.ReplaceAll(keyword, "%", "\\%"))
		q = q.Where(field.Or(tbl.Name.Like(filter), tbl.UserName.Like(filter)))
	}
	if spaceID != "" {
		q = q.Where(tbl.SpaceID.Eq(spaceID))
	}
	// 4. 执行计数查询
	count, err := q.Count()
	if err != nil {
		return 0, fmt.Errorf("RetrieveSharedKnowledgeCount Count failed : %v", err)
	}
	return count, nil
}

// DeleteSharedKnowledge 删除共享知识库（Gen风格）
func (d *daoImpl) DeleteSharedKnowledge(ctx context.Context, corpBizID uint64,
	knowledgeBizIDList []uint64) (int64, error) {
	// 1. 参数校验
	if len(knowledgeBizIDList) == 0 {
		return 0, fmt.Errorf("knowledgeBizIDList empty")
	}
	// 2. 初始化Gen生成的Query
	q := d.tdsql.TShareKnowledge.WithContext(ctx)
	// 3. 构建更新字段（使用Map避免零值问题）
	updateData := map[string]any{
		d.tdsql.TShareKnowledge.IsDeleted.ColumnName().String(): boolx.TrueNumber,
	}
	// 4. 执行更新操作（类型安全条件）
	result, err := q.Where(
		d.tdsql.TShareKnowledge.CorpBizID.Eq(corpBizID),
		d.tdsql.TShareKnowledge.BusinessID.In(knowledgeBizIDList...),
		d.tdsql.TShareKnowledge.IsDeleted.Is(false), // 只更新未删除的记录
	).Updates(updateData)
	if err != nil {
		logx.W(ctx, "DeleteSharedKnowledge Updates failed: %v", err)
		return 0, err
	}
	// 5. 记录删除结果
	if result.RowsAffected > 0 {
		logx.I(ctx, "DeleteSharedKnowledge success, corpBizID: %d, "+
			"deletedCount: %d", corpBizID, result.RowsAffected)
	}
	return result.RowsAffected, nil
}

func (d *daoImpl) RetrieveSharedKnowledgeByName(ctx context.Context, corpBizID uint64, knowledgeNameList []string, spaceId string) (
	[]*kbe.SharedKnowledgeInfo, error) {
	// 1. 参数校验
	if len(knowledgeNameList) == 0 {
		return nil, fmt.Errorf("knowledgeNameList empty")
	}
	// 2. 初始化Gen生成的Query
	q := d.tdsql.TShareKnowledge.WithContext(ctx)
	// 3. 构建类型安全查询条件
	q = q.Where(
		d.tdsql.TShareKnowledge.CorpBizID.Eq(corpBizID),
		d.tdsql.TShareKnowledge.IsDeleted.Is(false),
		d.tdsql.TShareKnowledge.Name.In(knowledgeNameList...),
	)
	if spaceId != "" {
		q = q.Where(d.tdsql.TShareKnowledge.SpaceID.Eq(spaceId))
	}
	// 4. 执行查询
	result, err := q.Find()
	if err != nil {
		return nil, fmt.Errorf("RetrieveSharedKnowledgeByName Find failed: %v", err)
	}
	return RetrieveBaseSharedKnowledgesPO2DO(result), nil
}

func (d *daoImpl) ClearSpaceSharedKnowledge(ctx context.Context, corpBizID uint64, spaceID string) (int64, error) {
	// 1. 参数校验
	if corpBizID == 0 || spaceID == "" {
		return 0, fmt.Errorf("corpBizID or spaceID empty")
	}
	// 2. 初始化Gen生成的Query
	q := d.tdsql.TShareKnowledge.WithContext(ctx)

	// 3. 构建更新字段（使用Map避免零值问题）
	updateData := map[string]any{
		d.tdsql.TShareKnowledge.IsDeleted.ColumnName().String(): boolx.TrueNumber,
	}

	// 4. 执行更新操作（类型安全条件）
	result, err := q.Where(
		d.tdsql.TShareKnowledge.CorpBizID.Eq(corpBizID),
		d.tdsql.TShareKnowledge.SpaceID.Eq(spaceID),
		d.tdsql.TShareKnowledge.IsDeleted.Is(false), // 只更新未删除的记录
	).Updates(updateData)

	if err != nil {
		return 0, fmt.Errorf("ClearSpaceSharedKnowledge Updates failed: %v", err)
	}
	return result.RowsAffected, nil
}

func (d *daoImpl) ListSpaceShareKnowledgeExSelf(ctx context.Context, corpBizID, exStaffID uint64, spaceID,
	keyword string, pageNumber, pageSize uint32) (int64, []*kbe.SharedKnowledgeInfo, error) {
	// 1. 参数校验
	if corpBizID == 0 {
		return 0, nil, fmt.Errorf("corpBizID empty")
	}
	if spaceID == "" {
		return 0, nil, fmt.Errorf("spaceID empty")
	}
	if pageNumber == 0 || pageSize == 0 {
		return 0, nil, fmt.Errorf("pageNumber/pageSize invalid")
	}
	// 2. 初始化Gen生成的Query
	q := d.tdsql.TShareKnowledge.WithContext(ctx)
	// 3. 构建基础查询条件
	q = q.Where(
		d.tdsql.TShareKnowledge.CorpBizID.Eq(corpBizID),
		d.tdsql.TShareKnowledge.SpaceID.Eq(spaceID),
		d.tdsql.TShareKnowledge.IsDeleted.Is(false),
	)
	// 4. 排除特定用户的条件
	if exStaffID > 0 {
		q = q.Where(d.tdsql.TShareKnowledge.OwnerStaffID.Neq(exStaffID))
	}
	// 5. 关键词搜索条件
	if keyword != "" {
		filter := fmt.Sprintf("%%%s%%", strings.ReplaceAll(keyword, "%", "\\%"))
		q = q.Where(field.Or(d.tdsql.TShareKnowledge.Name.Like(filter), d.tdsql.TShareKnowledge.UserName.Like(filter)))
	}
	// 6. 获取总数
	total, err := q.Count()
	if err != nil {
		return 0, nil, fmt.Errorf("ListSpaceShareKnowledgeExSelf Count failed: %v", err)
	}
	// 7. 执行分页查询
	offset := int(pageSize * (pageNumber - 1))
	result, err := q.Order(d.tdsql.TShareKnowledge.UpdateTime.Desc()).Offset(offset).Limit(int(pageSize)).Find()
	if err != nil {
		return 0, nil, fmt.Errorf("ListSpaceShareKnowledgeExSelf Find failed: %v", err)
	}
	return total, RetrieveBaseSharedKnowledgesPO2DO(result), nil
}
