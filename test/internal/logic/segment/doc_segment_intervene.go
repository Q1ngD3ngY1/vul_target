package segment

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/client"
	"git.woa.com/adp/common/x/gox/convx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/model"
	knowClient "git.woa.com/adp/kb/kb-config/internal/rpc"

	"git.woa.com/adp/common/x/encodingx/jsonx"
	"git.woa.com/adp/common/x/gox/ptrx"
	"git.woa.com/adp/common/x/utilx"
	"git.woa.com/adp/kb/kb-config/internal/entity/segment"
	segEntity "git.woa.com/adp/kb/kb-config/internal/entity/segment"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"git.woa.com/adp/kb/kb-config/internal/util/idgen"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	kb_pb "git.woa.com/adp/pb-go/kb/kb_config"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
	"gorm.io/gorm"
)

func (l *Logic) GetDocTemporaryOrgDataByDocBizID(ctx context.Context, selectColumns []string,
	filter *segEntity.DocSegmentOrgDataTemporaryFilter) ([]*segEntity.DocSegmentOrgDataTemporary, error) {
	return l.segDao.GetDocTemporaryOrgDataByDocBizID(ctx, selectColumns, filter)

}

func (l *Logic) GetDocTemporaryOrgDataCount(ctx context.Context, filter *segEntity.DocSegmentOrgDataTemporaryFilter) (int64, error) {
	return l.segDao.GetDocTemporaryOrgDataCount(ctx, filter)

}
func (l *Logic) BatchDeleteTemporaryDocOrgDataByDocBizID(ctx context.Context, corpBizID, appBizID, docBizID uint64, batchSize int) error {
	return l.segDao.BatchDeleteTemporaryDocOrgDataByDocBizID(ctx, corpBizID, appBizID, docBizID, batchSize)

}

// BatchUpdateDocOrgData 批量更新
func (l *Logic) BatchUpdateDocOrgData(ctx context.Context,
	updateColumns []string, filter *segEntity.DocSegmentOrgDataFilter, orgData *segEntity.DocSegmentOrgData, batchSize int) error {
	logx.D(ctx, "BatchUpdateDocOrgData|batchSize:%d", batchSize)
	if batchSize <= 0 {
		batchSize = 10000
	}
	if filter == nil {
		logx.E(ctx, "BatchUpdateDocOrgData|filter is null")
		return errs.ErrSystem
	}
	if filter.RouterAppBizID == 0 {
		logx.E(ctx, "BatchUpdateDocOrgData|filter RouterAppBizID is 0")
		return errs.ErrSystem
	}
	if orgData == nil {
		logx.E(ctx, "BatchUpdateDocOrgData|orgData is null")
		return errs.ErrSystem
	}

	for deleteRows := batchSize; deleteRows == batchSize; {
		filter.Limit = batchSize
		rowsAffected, err := l.UpdateDocSegmentOrgData(ctx, updateColumns, filter, orgData)
		if err != nil {
			logx.E(ctx, "BatchUpdateDocOrgData failed|err: %+v", err)
			return err
		}
		deleteRows = int(rowsAffected)
	}
	return nil
}

// BatchDeleteDocOrgDataByDocBizID 批量删除OrgData数据(逻辑删除)
func (l *Logic) BatchDeleteDocOrgDataByDocBizID(ctx context.Context, corpBizID,
	appBizID, docBizID uint64, batchSize int) error {
	logx.D(ctx, "BatchDeleteDocSegmentOrgDataDaoByDocBizID corpBizId:%d,appBizId:%d,docBizId:%d,batchSize:%d",
		corpBizID, appBizID, docBizID, batchSize)
	if batchSize <= 0 {
		batchSize = 10000
	}
	updateColumns := []string{segEntity.DocSegmentOrgDataTblColIsDeleted,
		segEntity.DocSegmentOrgDataTblColUpdateTime}
	orgData := &segEntity.DocSegmentOrgData{
		IsDeleted:  true,
		UpdateTime: time.Now(),
	}
	filter := &segEntity.DocSegmentOrgDataFilter{
		CorpBizID:      corpBizID,
		AppBizID:       appBizID,
		DocBizID:       docBizID,
		RouterAppBizID: appBizID,
	}
	return l.BatchUpdateDocOrgData(ctx, updateColumns, filter, orgData, batchSize)
}

// BatchRecoverDocOrgDataByDocBizID 批量恢复OrgData数据
func (l *Logic) BatchRecoverDocOrgDataByDocBizID(ctx context.Context, corpBizID,
	appBizID, docBizID uint64, batchSize int) error {
	logx.D(ctx, "BatchRecoverDocOrgDataByDocBizID corpBizId:%d,appBizId:%d,docBizId:%d,batchSize:%d",
		corpBizID, appBizID, docBizID, batchSize)
	if batchSize <= 0 {
		batchSize = 10000
	}
	updateColumns := []string{segEntity.DocSegmentOrgDataTblColIsDeleted,
		segEntity.DocSegmentOrgDataTblColUpdateTime}
	orgData := &segEntity.DocSegmentOrgData{
		IsDeleted:  false,
		UpdateTime: time.Now(),
	}
	filter := &segEntity.DocSegmentOrgDataFilter{
		CorpBizID:      corpBizID,
		AppBizID:       appBizID,
		DocBizID:       docBizID,
		IsDeleted:      ptrx.Bool(true),
		RouterAppBizID: appBizID,
	}
	return l.BatchUpdateDocOrgData(ctx, updateColumns, filter, orgData, batchSize)
}

func (l *Logic) GetDocOrgData(ctx context.Context, selectColumns []string,
	filter *segEntity.DocSegmentOrgDataFilter) (*segEntity.DocSegmentOrgData, error) {
	if filter == nil {
		logx.E(ctx, "GetDocOrgData|filter is null")
		return nil, errs.ErrSystem
	}
	if filter.RouterAppBizID == 0 {
		logx.E(ctx, "GetDocOrgData|filter RouterAppBizID is 0")
		return nil, errs.ErrSystem
	}
	// if filter.Limit == 0 {
	//	// 为0正常返回空结果即可
	//	return nil, nil
	// }
	if filter.Limit > segEntity.DocSegmentInterveneMaxPageSize {
		// 限制单次查询最大条数
		logx.E(ctx, "GetDocOrgData|is limit too large:%d", filter.Limit)
		return nil, errs.ErrGetDocSegmentTooLarge
	}
	db, err := l.GetGormDB(ctx, filter.RouterAppBizID, model.TableNameTDocSegmentOrgDatum)

	if err != nil {
		logx.E(ctx, "GetDocOrgData|get gorm failed|err: %+v", err)
		return nil, err
	}
	return l.segDao.GetDocOrgData(ctx, selectColumns, filter, db)
}

func (l *Logic) GetDocOrgDatumCount(ctx context.Context, filter *segEntity.DocSegmentOrgDataFilter) (int64, error) {
	if filter == nil {
		logx.E(ctx, "GetDocOrgDataCount|filter is null")
		return 0, errs.ErrSystem
	}
	if filter.RouterAppBizID == 0 {
		logx.E(ctx, "GetDocOrgDataCount|filter RouterAppBizID is 0")
		return 0, errs.ErrSystem
	}
	db, err := l.GetGormDB(ctx, filter.RouterAppBizID, model.TableNameTDocSegmentOrgDatum)
	if err != nil {
		logx.E(ctx, "GetDocOrgData|get gorm failed|err: %+v", err)
		return 0, err
	}
	return l.segDao.GetDocOrgDataCount(ctx, filter, db)
}

func (l *Logic) GetDocOrgDataByKeyWords(ctx context.Context, selectColumns []string,
	filter *segEntity.DocSegmentOrgDataFilter) ([]*segEntity.DocSegmentOrgData, error) {
	if filter == nil {
		logx.E(ctx, "GetDocOrgDataListByKeyWords|filter is null")
		return nil, errs.ErrSystem
	}
	if filter.RouterAppBizID == 0 {
		logx.E(ctx, "GetDocOrgDataListByKeyWords|filter RouterAppBizID is 0")
		return nil, errs.ErrSystem
	}
	if filter.Limit == 0 {
		// 为0正常返回空结果即可
		return nil, nil
	}
	if filter.Limit > segEntity.DocSegmentInterveneMaxPageSize {
		// 限制单次查询最大条数
		logx.E(ctx, "GetDocOrgDataListByKeyWords|is limit too large:%d", filter.Limit)
		return nil, errs.ErrGetDocSegmentTooLarge
	}
	db, err := l.GetGormDB(ctx, filter.RouterAppBizID, model.TableNameTDocSegmentOrgDatum)
	if err != nil {
		logx.E(ctx, "GetDocOrgDataListByKeyWords|get gorm failed|err: %+v", err)
		return nil, err
	}

	return l.segDao.GetDocOrgDataListByKeyWords(ctx, selectColumns, filter, db)
}

func (l *Logic) GetDocOrgDataList(ctx context.Context, selectColumns []string,
	filter *segEntity.DocSegmentOrgDataFilter) ([]*segEntity.DocSegmentOrgData, error) {
	if filter == nil {
		logx.E(ctx, "GetDocOrgDataList|filter is null")
		return nil, errs.ErrSystem
	}
	if filter.RouterAppBizID == 0 {
		logx.E(ctx, "GetDocOrgDataList|filter RouterAppBizID is 0")
		return nil, errs.ErrSystem
	}
	if filter.Limit == 0 {
		// 为0正常返回空结果即可
		return nil, nil
	}
	if filter.Limit > segEntity.DocSegmentInterveneMaxPageSize {
		// 限制单次查询最大条数
		logx.E(ctx, "GetDocOrgDataList|is limit too large:%d", filter.Limit)
		return nil, errs.ErrGetDocSegmentTooLarge
	}

	db, err := l.GetGormDB(ctx, filter.RouterAppBizID, model.TableNameTDocSegmentOrgDatum)
	if err != nil {
		logx.E(ctx, "GetDocOrgDataList|get gorm failed|err: %+v", err)
		return nil, err
	}

	return l.segDao.GetDocOrgDataList(ctx, selectColumns, filter, db)
}

func (l *Logic) GetDocOrgDataByBizID(ctx context.Context, selectColumns []string, corpBizID,
	appBizID, docBizID, businessID uint64) (*segEntity.DocSegmentOrgData, error) {
	logx.I(ctx, "GetDocOrgDataByBizID|corpBizID: %d, appBizID: %d, docBizID: %d, businessID: %d",
		corpBizID, appBizID, docBizID, businessID)
	deleteFlag := ptrx.Bool(false)
	filter := &segEntity.DocSegmentOrgDataFilter{
		CorpBizID:          corpBizID,
		AppBizID:           appBizID,
		DocBizID:           docBizID,
		BusinessIDs:        []uint64{businessID},
		IsDeleted:          deleteFlag,
		IsTemporaryDeleted: deleteFlag,
		RouterAppBizID:     appBizID,
	}
	return l.GetDocOrgData(ctx, selectColumns, filter)
}

func (l *Logic) GetLastOrgDataByCurrentOrgDataBizID(ctx context.Context, selectColumns []string, corpBizID, appBizID, docBizID, curBusinessID uint64) (
	*segEntity.DocSegmentOrgData, error) {
	tbl := l.segDao.TdsqlQuery().TDocSegmentOrgDatum
	docSegmentOrgDataTableName := model.TableNameTDocSegmentOrgDatum
	filter := &segEntity.DocSegmentOrgDataFilter{
		CorpBizID:      corpBizID,
		AppBizID:       appBizID,
		DocBizID:       docBizID,
		IsDeleted:      ptrx.Bool(false),
		MaxBusinessID:  curBusinessID,
		Limit:          1,
		OrderColumn:    []string{tbl.BusinessID.ColumnName().String()},
		OrderDirection: []string{"DESC"},
	}
	db, err := l.GetGormDB(ctx, appBizID, docSegmentOrgDataTableName)
	if err != nil {
		logx.E(ctx, "GetSegmentByDocID|get gorm failed|err: %+v", err)
		return nil, err
	}
	return l.segDao.GetDocOrgData(ctx, selectColumns, filter, db)
}

func (l *Logic) GetAfterOrgDataByCurrentOrgDataBizID(ctx context.Context, selectColumns []string, corpBizID,
	appBizID, docBizID, curBusinessID uint64) (*segEntity.DocSegmentOrgData, error) {
	tbl := l.segDao.TdsqlQuery().TDocSegmentOrgDatum
	docSegmentOrgDataTableName := model.TableNameTDocSegmentOrgDatum

	filter := &segEntity.DocSegmentOrgDataFilter{
		CorpBizID:      corpBizID,
		AppBizID:       appBizID,
		DocBizID:       docBizID,
		IsDeleted:      ptrx.Bool(false),
		MinBusinessID:  curBusinessID,
		Limit:          1,
		OrderColumn:    []string{tbl.BusinessID.ColumnName().String()},
		OrderDirection: []string{"DESC"},
	}

	db, err := l.GetGormDB(ctx, appBizID, docSegmentOrgDataTableName)
	if err != nil {
		logx.E(ctx, "GetSegmentByDocID|get gorm failed|err: %+v", err)
		return nil, err
	}
	return l.segDao.GetDocOrgData(ctx, selectColumns, filter, db)
}

func (l *Logic) CreateDocSegmentOrgData(ctx context.Context, orgData *segEntity.DocSegmentOrgData) error {
	db, err := l.GetGormDB(ctx, orgData.AppBizID, model.TableNameTDocSegmentOrgDatum)
	if err != nil {
		logx.E(ctx, "CreateDocSegmentOrgData|get gorm failed|err: %+v", err)
		return err
	}
	return l.segDao.CreateDocSegmentOrgData(ctx, orgData, db)
}

func (l *Logic) UpdateDocSegmentOrgData(ctx context.Context, updateColumns []string, filter *segEntity.DocSegmentOrgDataFilter,
	orgData *segEntity.DocSegmentOrgData) (int64, error) {
	docSegmentOrgDataTableName := model.TableNameTDocSegmentOrgDatum
	if filter == nil {
		logx.E(ctx, "UpdateDocSegmentOrgData|filter is null")
		return 0, errs.ErrSystem
	}
	if filter.RouterAppBizID == 0 {
		logx.E(ctx, "UpdateDocSegmentOrgData|filter RouterAppBizID is 0")
		return 0, errs.ErrSystem
	}
	if orgData == nil {
		logx.E(ctx, "UpdateDocSegmentOrgData|orgData is null")
		return 0, errs.ErrSystem
	}
	db, err := l.GetGormDB(ctx, filter.RouterAppBizID, docSegmentOrgDataTableName)
	if err != nil {
		logx.E(ctx, "UpdateDocSegmentOrgData|get gorm failed|err: %+v", err)
		return 0, err
	}
	return l.segDao.UpdateDocSegmentOrgData(ctx, updateColumns, filter, orgData, db)

}

// DeleteDocSegmentOrgData 删除org_data
func (l *Logic) DeleteDocSegmentOrgData(ctx context.Context, corpBizID uint64,
	appBizID uint64, docBizID uint64, businessIDs []uint64) error {
	updateColumns := []string{segEntity.DocSegmentOrgDataTblColIsDeleted, segEntity.DocSegmentOrgDataTblColUpdateTime}
	orgData := &segEntity.DocSegmentOrgData{
		IsDeleted:  true,       // 是否删除
		UpdateTime: time.Now(), // 更新时间
	}
	filter := &segEntity.DocSegmentOrgDataFilter{
		CorpBizID:      corpBizID,
		AppBizID:       appBizID,
		DocBizID:       docBizID,
		BusinessIDs:    businessIDs,
		RouterAppBizID: appBizID,
	}
	_, err := l.UpdateDocSegmentOrgData(ctx, updateColumns, filter, orgData)

	if err != nil {
		logx.E(ctx, "DeleteDocSegmentOrgData|update org data failed|err: %+v", err)
		return err
	}
	return nil
}

// TemporaryDeleteDocSegmentOrgData 临时删除org_data
func (l *Logic) TemporaryDeleteDocSegmentOrgData(ctx context.Context, corpBizID uint64,
	appBizID uint64, docBizID uint64, businessIDs []uint64) error {
	updateColumns := []string{segEntity.DocSegmentOrgDataTblColIsTemporaryDeleted, segEntity.DocSegmentOrgDataTblColUpdateTime}
	orgData := &segEntity.DocSegmentOrgData{
		IsTemporaryDeleted: true,       // 是否删除
		UpdateTime:         time.Now(), // 更新时间
	}
	filter := &segEntity.DocSegmentOrgDataFilter{
		CorpBizID:      corpBizID,
		AppBizID:       appBizID,
		DocBizID:       docBizID,
		BusinessIDs:    businessIDs,
		RouterAppBizID: appBizID,
	}
	_, err := l.UpdateDocSegmentOrgData(ctx, updateColumns, filter, orgData)

	if err != nil {
		logx.E(ctx, "TemporaryDeleteDocSegmentOrgData|update org data failed|err: %+v", err)
		return err
	}
	return nil
}

// DisabledDocSegmentOrgData 停用切片
func (l *Logic) DisabledDocSegmentOrgData(ctx context.Context, corpBizID uint64,
	appBizID uint64, docBizID uint64, businessIDs []uint64) error {
	updateColumns := []string{segEntity.DocSegmentOrgDataTblColIsDisabled, segEntity.DocSegmentOrgDataTblColUpdateTime}
	orgData := &segEntity.DocSegmentOrgData{
		IsDisabled: true,       // 停用切片
		UpdateTime: time.Now(), // 更新时间
	}
	filter := &segEntity.DocSegmentOrgDataFilter{
		CorpBizID:      corpBizID,
		AppBizID:       appBizID,
		DocBizID:       docBizID,
		BusinessIDs:    businessIDs,
		RouterAppBizID: appBizID,
	}
	_, err := l.UpdateDocSegmentOrgData(ctx, updateColumns, filter, orgData)

	if err != nil {
		logx.E(ctx, "DisabledDocSegmentOrgData|update org data failed|err: %+v", err)
		return err
	}
	return nil
}

// EnableDocSegmentOrgData 启用切片
func (l *Logic) EnableDocSegmentOrgData(ctx context.Context, corpBizID uint64,
	appBizID uint64, docBizID uint64, businessIDs []uint64) error {
	updateColumns := []string{segEntity.DocSegmentOrgDataTblColIsDisabled, segEntity.DocSegmentOrgDataTblColUpdateTime}
	orgData := &segEntity.DocSegmentOrgData{
		IsDisabled: false,      // 启用切片
		UpdateTime: time.Now(), // 更新时间
	}
	filter := &segEntity.DocSegmentOrgDataFilter{
		CorpBizID:      corpBizID,
		AppBizID:       appBizID,
		DocBizID:       docBizID,
		BusinessIDs:    businessIDs,
		RouterAppBizID: appBizID,
	}
	_, err := l.UpdateDocSegmentOrgData(ctx, updateColumns, filter, orgData)

	if err != nil {
		logx.E(ctx, "EnableDocSegmentOrgData|update org data failed|err: %+v", err)
		return err
	}
	return nil
}

// RealityDeleteDocSegmentOrgDataByOrgDataBizID 物理删除某个切片
func (l *Logic) RealityDeleteDocSegmentOrgDataByOrgDataBizID(ctx context.Context, corpBizID,
	appBizID, docBizID, businessID uint64) error {

	docSegmentOrgDataTableName := model.TableNameTDocSegmentOrgDatum

	filter := &segEntity.DocSegmentOrgDataFilter{
		CorpBizID:      corpBizID,
		AppBizID:       appBizID,
		DocBizID:       docBizID,
		BusinessIDs:    []uint64{businessID},
		Limit:          1,
		RouterAppBizID: appBizID,
	}
	db, err := l.GetGormDB(ctx, filter.RouterAppBizID, docSegmentOrgDataTableName)
	if err != nil {
		logx.E(ctx, "RealityDeleteDocSegmentOrgDataByOrgDataBizID|get gorm failed|err: %+v", err)
		return err
	}
	_, err = l.segDao.RealityDeleteDocSegment(ctx, filter, db)
	if err != nil {
		logx.E(ctx, "RealityDeleteDocSegmentOrgDataByOrgDataBizID failed for BusinessID: %d, err: %+v", businessID, err)
		return err
	}
	return nil
}

// RealityBatchDeleteDocOrgData 批量删除OrgData数据(物理删除)
func (l *Logic) RealityBatchDeleteDocOrgData(ctx context.Context,
	filter *segEntity.DocSegmentOrgDataFilter, batchSize int) error {
	logx.D(ctx, "RealityBatchDeleteDocOrgData|batchSize:%d", batchSize)
	if filter == nil {
		logx.E(ctx, "RealityBatchDeleteDocOrgData|filter is null")
		return errs.ErrSystem
	}
	if filter.RouterAppBizID == 0 {
		logx.E(ctx, "RealityBatchDeleteDocOrgData|filter RouterAppBizID is 0")
		return errs.ErrSystem
	}

	docSegmentOrgDataTableName := model.TableNameTDocSegmentOrgDatum
	if batchSize <= 0 {
		batchSize = 10000
	}

	db, err := l.GetGormDB(ctx, filter.RouterAppBizID, docSegmentOrgDataTableName)
	if err != nil {
		logx.E(ctx, "RealityBatchDeleteDocOrgData|get gorm failed|err: %+v", err)
		return err
	}

	for deleteRows := batchSize; deleteRows == batchSize; {
		filter.Limit = batchSize
		rowsAffected, err := l.segDao.RealityDeleteDocSegment(ctx, filter, db)
		if err != nil {
			logx.E(ctx, "RealityBatchDeleteDocOrgData failed|err: %+v", err)
			return err
		}
		deleteRows = int(rowsAffected)
	}
	return nil
}

func (l *Logic) GetSegmentSheetTemporaryList(ctx context.Context, selectColumns []string, filter *segEntity.DocSegmentSheetTemporaryFilter) (
	[]*segEntity.DocSegmentSheetTemporary, error) {
	return l.segDao.GetSheetList(ctx, selectColumns, filter)
}

func (l *Logic) GetDocSegmentSheetTemporaryCount(ctx context.Context, filter *segEntity.DocSegmentSheetTemporaryFilter) (int64, error) {
	return l.segDao.GetDocSheetCount(ctx, filter)

}
func (l *Logic) UpdateDocSegmentSheet(ctx context.Context, updateColumns []string,
	filter *segEntity.DocSegmentSheetTemporaryFilter, sheet *segEntity.DocSegmentSheetTemporary) (int64, error) {
	return l.segDao.UpdateDocSegmentSheet(ctx, updateColumns, filter, sheet)

}
func (l *Logic) BatchDeleteDocSegmentSheetByDocBizID(ctx context.Context, corpBizID, appBizID, docBizID uint64, batchSize int) error {
	return l.segDao.BatchDeleteDocSegmentSheetByDocBizID(ctx, corpBizID, appBizID, docBizID, batchSize)

}

func (l *Logic) RecoverSegmentsForIndex(ctx context.Context, corpBizID, appBizID, docBizID uint64) error {
	deleteFilter := &segEntity.DocSegmentOrgDataFilter{
		CorpBizID:      corpBizID,
		AppBizID:       appBizID,
		DocBizID:       docBizID,
		IsDeleted:      ptrx.Bool(false),
		RouterAppBizID: appBizID,
	}
	err := l.RealityBatchDeleteDocOrgData(ctx, deleteFilter, 10000)
	if err != nil {
		logx.E(ctx, "RecoverSegmentsForIndex|BatchDeleteDocOrgData failed, err:%+v", err)
		return err
	}
	err = l.BatchRecoverDocOrgDataByDocBizID(ctx, corpBizID, appBizID, docBizID, 10000)
	if err != nil {
		logx.E(ctx, "RecoverSegmentsForIndex|BatchDeleteTempDocOrgData failed, err:%+v", err)
		return err
	}
	return nil
}

// ListDocSegmentByReferBizIDOrSegmentBizID 通过ReferBizId或者SegmentBizId获取文档切片列表
func (l *Logic) ListDocSegmentByReferBizIDOrSegmentBizID(ctx context.Context, req *pb.ListDocSegmentReq,
	docCommon *segEntity.DocSegmentCommon, rsp *pb.ListDocSegmentRsp) (*pb.ListDocSegmentRsp, error) {
	docSegmentList := make([]*pb.ListDocSegmentRsp_DocSegmentItem, 0)
	logx.I(ctx, "ListDocSegmentByReferBizIDOrSegmentBizID|ReferBizId:%s, SegmentBizId:%s", req.ReferBizId, req.SegmentBizId)
	var err error
	segment := &segEntity.DocSegment{}
	if req.GetSegmentBizId() != "" {
		segment, err = l.GetSegmentBySegmentBizID(ctx, req.GetSegmentBizId(), docCommon.AppID)
	} else {
		segment, err = l.GetSegmentByReferID(ctx, docCommon.AppID, req.ReferBizId)
	}
	if err != nil {
		return rsp, err
	}
	// 先查临时表，看是否有编辑过的数据
	// 兼容共享知识库
	app, err := l.rpc.AppAdmin.DescribeAppByPrimaryIdWithoutNotFoundError(ctx, segment.RobotID)
	if err != nil {
		return rsp, err
	}
	docCommon.AppID = segment.RobotID
	docCommon.AppBizID = app.BizId
	editOriginList, err := l.GetEditOrgData(ctx, []string{strconv.FormatUint(segment.OrgDataBizID, 10)}, docCommon)
	if err != nil {
		logx.E(ctx, "GetEditOrgData failed, err:%+v", err)
		return rsp, errs.ErrSystem
	}
	if len(editOriginList) == 1 {
		docSegmentList, err = TempOriginListToDocSegment(ctx, editOriginList)
		if err != nil {
			logx.E(ctx, "TempOriginListToDocSegment failed, err:%+v", err)
			return rsp, errs.ErrSystem
		}
		rsp.SegmentList = docSegmentList
		rsp.Total = uint64(len(docSegmentList))
		return rsp, nil
	}
	// 临时表没有，返回原数据
	orgData, err := l.GetDocOrgDataByBizID(ctx, segEntity.DocSegmentOrgDataTblColList, docCommon.CorpBizID, docCommon.AppBizID, docCommon.DocBizID,
		segment.OrgDataBizID)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		// 返回空数组（兼容干预后切片ID变化的情况）
		rsp.SegmentList = make([]*pb.ListDocSegmentRsp_DocSegmentItem, 0)
		rsp.Total = uint64(len(docSegmentList))
		return rsp, nil
	} else if err != nil {
		return rsp, errs.ErrDocSegmentNotFound
	}
	if orgData == nil {
		// 返回空数组（兼容干预后切片ID变化的情况）
		rsp.SegmentList = make([]*pb.ListDocSegmentRsp_DocSegmentItem, 0)
		rsp.Total = uint64(len(docSegmentList))
		return rsp, nil
	}
	docSegmentList, err = OriginListToDocSegment(ctx, []*segEntity.DocSegmentOrgData{orgData})
	if err != nil {
		logx.E(ctx, "OriginListToDocSegment failed for orgData.BusinessID:%s, err:%+v", orgData.BusinessID, err)
		return nil, err
	}
	rsp.SegmentList = docSegmentList
	rsp.Total = uint64(len(docSegmentList))
	return rsp, nil
}

func (l *Logic) GetDocOrgDataCountByDocBizID(ctx context.Context, docCommon *segEntity.DocSegmentCommon) (int64, int64, error) {
	deleteFlag := ptrx.Bool(false)
	filter := &segEntity.DocSegmentOrgDataFilter{
		CorpBizID:          docCommon.CorpBizID,
		AppBizID:           docCommon.AppBizID,
		DocBizID:           docCommon.DocBizID,
		IsDeleted:          deleteFlag,
		IsTemporaryDeleted: deleteFlag,
		RouterAppBizID:     docCommon.AppBizID,
		SheetName:          docCommon.SheetName,
	}
	num, err := l.GetDocOrgDatumCount(ctx, filter)
	if err != nil {
		logx.E(ctx, "GetDocOrgDataCountByDocBizID|GetDocOrgDataCount|err:%v", err)
		return 0, 0, err
	}
	actionFlag := segEntity.InsertAction
	tempFilter := &segEntity.DocSegmentOrgDataTemporaryFilter{
		CorpBizID: docCommon.CorpBizID,
		AppBizID:  docCommon.AppBizID,
		DocBizID:  docCommon.DocBizID,
		IsDeleted: ptrx.Bool(false),
		Action:    &actionFlag,
		SheetName: docCommon.SheetName,
	}
	tempNum, err := l.segDao.GetDocTemporaryOrgDataCount(ctx, tempFilter)
	if err != nil {
		logx.E(ctx, "GetDocOrgDataCountByDocBizID|GetDocSegmentOrgDataTemporaryDao|err:%v", err)
		return 0, 0, err
	}
	return num, tempNum, nil
}

// GetDocSegmentOrgData 获取OrgData
func (l *Logic) GetDocSegmentOrgData(ctx context.Context, req *pb.ListDocSegmentReq,
	docCommon *segEntity.DocSegmentCommon, offset, limit int) ([]*pb.ListDocSegmentRsp_DocSegmentItem, []string, error) {
	logx.I(ctx, "GetDocSegmentOrgData|start|offset:%d|limit:%d", offset, limit)
	orgDataList := make([]*pb.ListDocSegmentRsp_DocSegmentItem, 0)
	orgDateBizIDs := make([]string, 0)
	deletedFlag := ptrx.Bool(false)
	list := make([]*segEntity.DocSegmentOrgData, 0)
	var err error
	filter := &segEntity.DocSegmentOrgDataFilter{
		CorpBizID:          docCommon.CorpBizID,
		AppBizID:           docCommon.AppBizID,
		DocBizID:           docCommon.DocBizID,
		Keywords:           req.Keywords,
		IsDeleted:          deletedFlag,
		IsTemporaryDeleted: deletedFlag,
		OrderColumn:        []string{segEntity.DocSegmentOrgDataTblColBusinessID},
		OrderDirection:     []string{util.SqlOrderByAsc},
		Offset:             offset,
		Limit:              limit,
		RouterAppBizID:     docCommon.AppBizID,
		SheetName:          docCommon.SheetName,
	}
	if req.Keywords != "" {
		list, err = l.GetDocOrgDataByKeyWords(ctx,
			segEntity.DocSegmentOrgDataTblColList, filter)
		if err != nil {
			return orgDataList, orgDateBizIDs, err
		}
	} else {
		list, err = l.GetDocOrgDataList(ctx,
			segEntity.DocSegmentOrgDataTblColList, filter)
		if err != nil {
			return orgDataList, orgDateBizIDs, err
		}
	}
	for _, orgDate := range list {
		pageInfos, pageData := make([]uint64, 0), make([]int64, 0)
		if orgDate.OrgPageNumbers != "" {
			if err = jsonx.UnmarshalFromString(orgDate.OrgPageNumbers, &pageData); err != nil {
				logx.W(ctx, "GetDocSegmentOrgData|PageInfos|UnmarshalFromString|err:%+v", err)
			}
			for _, page := range pageData {
				pageInfos = append(pageInfos, uint64(page))
			}
		}
		orgDateBizIDs = append(orgDateBizIDs, strconv.FormatUint(orgDate.BusinessID, 10))
		docSegmentItem := &pb.ListDocSegmentRsp_DocSegmentItem{
			SegBizId:    strconv.FormatUint(orgDate.BusinessID, 10),
			OrgData:     orgDate.OrgData,
			PageInfos:   pageInfos,
			IsOrigin:    orgDate.AddMethod == segEntity.AddMethodDefault,
			IsAdd:       orgDate.AddMethod == segEntity.AddMethodArtificial,
			SegmentType: orgDate.SegmentType,
			IsDisabled:  orgDate.IsDisabled,
			SheetName:   orgDate.SheetName,
		}
		orgDataList = append(orgDataList, docSegmentItem)
	}
	logx.I(ctx, "GetDocSegmentOrgData|len(OrgData):%d", len(orgDataList))
	return orgDataList, orgDateBizIDs, nil
}

// GetEditOrgData 获取编辑的切片
func (l *Logic) GetEditOrgData(ctx context.Context, orgDateBizIDs []string,
	docCommon *segEntity.DocSegmentCommon) ([]*segEntity.DocSegmentOrgDataTemporary, error) {
	logx.I(ctx, "GetEditOrgData|start")
	if len(orgDateBizIDs) == 0 {
		// 为0正常返回空结果即可
		logx.W(ctx, "Failed to GetEditOrgData cause by empty OriginOrgDataIDs. |len(OriginOrgDataIDs):%d", len(orgDateBizIDs))
		return nil, nil
	}

	// TODO: 优化成 GetListByChunks @wemysshchen
	if len(orgDateBizIDs) > segEntity.DocSegmentInterveneMaxPageSize {
		// 限制单次查询最大条数
		err := errors.New(fmt.Sprintf("origin org data ids too many, limit: %d", segEntity.DocSegmentInterveneMaxPageSize))
		logx.E(ctx, "GetEditOrgData err: %+v", err)
		return nil, err
	}

	filter := &segEntity.DocSegmentOrgDataTemporaryFilter{
		CorpBizID:        docCommon.CorpBizID,
		AppBizID:         docCommon.AppBizID,
		DocBizID:         docCommon.DocBizID,
		SheetName:        docCommon.SheetName,
		IsDeleted:        ptrx.Bool(false),
		Action:           ptrx.Int(segEntity.EditAction),
		OriginOrgDataIDs: orgDateBizIDs,
		OrderColumn:      []string{segEntity.DocSegmentOrgDataTemporaryTblColBusinessID},
		OrderDirection:   []string{util.SqlOrderByAsc},
	}

	// originList, err := l.segDao.GetEditTemporaryOrgData(ctx, segEntity.DocSegmentOrgDataTemporaryTblColList, filter)
	originList, err := l.segDao.GetDocTemporaryOrgDataList(ctx, segEntity.DocSegmentOrgDataTemporaryTblColList, filter)
	if err != nil {
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, err
		}
	}
	logx.I(ctx, "GetEditOrgData|len(OrgData):%d", len(originList))
	return originList, nil
}

// GetInsertOrgData 获取插入的切片
func (l *Logic) GetInsertOrgData(ctx context.Context, orgDateBizIDs []string,
	docCommon *segEntity.DocSegmentCommon) ([]*segEntity.DocSegmentOrgDataTemporary, error) {
	actionFlag := segEntity.InsertAction
	filter := &segEntity.DocSegmentOrgDataTemporaryFilter{
		CorpBizID:            docCommon.CorpBizID,
		AppBizID:             docCommon.AppBizID,
		DocBizID:             docCommon.DocBizID,
		IsDeleted:            ptrx.Bool(false),
		Action:               &actionFlag,
		LastOriginOrgDataIDs: orgDateBizIDs,
		OrderColumn:          []string{segEntity.DocSegmentOrgDataTemporaryTblColBusinessID},
		OrderDirection:       []string{util.SqlOrderByAsc},
		SheetName:            docCommon.SheetName,
	}
	logx.I(ctx, "GetInsertOrgData|start filter:%+v", filter)
	originList, err := l.segDao.GetInsertTemporaryOrgData(ctx,
		segEntity.DocSegmentOrgDataTemporaryTblColList, filter)
	if err != nil {
		logx.E(ctx, "GetInsertOrgData|err:%v", err)
		return nil, err
	}
	logx.I(ctx, "GetInsertOrgData|len(OrgData):%d", len(originList))
	return originList, nil
}

// GetOrgDataByKeywords 根据关键词获取全部临时切片
func (l *Logic) GetOrgDataByKeywords(ctx context.Context, keywords string, docCommon *segEntity.DocSegmentCommon, tempNum int64) (
	[]*segEntity.DocSegmentOrgDataTemporary, error) {
	logx.I(ctx, "GetOrgDataByKeywords|start")
	originList := make([]*segEntity.DocSegmentOrgDataTemporary, 0)
	pageNumber := uint32(1)
	pageSize := uint32(100)
	for {
		offset, limit := utilx.Page(pageNumber, pageSize)
		filter := &segEntity.DocSegmentOrgDataTemporaryFilter{
			CorpBizID: docCommon.CorpBizID,
			AppBizID:  docCommon.AppBizID,
			DocBizID:  docCommon.DocBizID,
			IsDeleted: ptrx.Bool(false),
			Keywords:  keywords,
			Offset:    offset,
			Limit:     limit,
			SheetName: docCommon.SheetName,
		}
		list, err := l.segDao.GetDocTemporaryOrgDataListByKeyWords(ctx,
			segEntity.DocSegmentOrgDataTemporaryTblColList, filter)
		if err != nil {
			if !errors.Is(err, gorm.ErrRecordNotFound) {
				return nil, err
			}
		}
		originList = append(originList, list...)
		if pageNumber*pageSize > uint32(tempNum) {
			// 已分页遍历完所有数据
			break
		}
		pageSize++
	}
	logx.I(ctx, "GetOrgDataByKeywords|len(OrgData):%d", len(originList))
	return originList, nil
}

// CheckDocIntervene 检查文档是否有改动
func (l *Logic) CheckDocIntervene(ctx context.Context, docCommon *segEntity.DocSegmentCommon) (bool, error) {
	// 检测临时表是否有数据
	deletedFlag := ptrx.Bool(false)
	tempFilter := &segEntity.DocSegmentOrgDataTemporaryFilter{
		CorpBizID: docCommon.CorpBizID,
		AppBizID:  docCommon.AppBizID,
		DocBizID:  docCommon.DocBizID,
		IsDeleted: deletedFlag,
		Offset:    0,
		Limit:     1,
	}
	tempList, err := l.segDao.GetDocTemporaryOrgDataByDocBizID(ctx,
		segEntity.DocSegmentOrgDataTemporaryTblColList, tempFilter)
	if err != nil {
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return false, err
		}
	}
	if len(tempList) > 0 {
		logx.I(ctx, "CheckDocIntervene|update")
		return true, nil
	}
	// 检测是否有切片被删除
	filter := &segEntity.DocSegmentOrgDataFilter{
		CorpBizID:          docCommon.CorpBizID,
		AppBizID:           docCommon.AppBizID,
		DocBizID:           docCommon.DocBizID,
		IsDeleted:          deletedFlag,
		IsTemporaryDeleted: ptrx.Bool(true),
		Offset:             0,
		Limit:              1,
		RouterAppBizID:     docCommon.AppBizID,
	}
	list, err := l.GetDocOrgDataList(ctx, segEntity.DocSegmentOrgDataTblColList, filter)
	if err != nil {
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return false, err
		}
	}
	if len(list) > 0 {
		logx.I(ctx, "CheckDocIntervene|tempDeleted")
		return true, nil
	}
	return false, nil
}

// InsertIntoOrgDataList 将新增切片放入对应位置
func (l *Logic) InsertIntoOrgDataList(ctx context.Context, insertOriginList []*segEntity.DocSegmentOrgDataTemporary,
	originList []*pb.ListDocSegmentRsp_DocSegmentItem) ([]*pb.ListDocSegmentRsp_DocSegmentItem, error) {
	logx.I(ctx, "InsertIntoOrgDataList|start")
	// 切片内容插入
	// 构建非新增数据映射（用于快速查找）
	originMap := make(map[string]struct{})
	for _, originSeg := range originList {
		originMap[originSeg.SegBizId] = struct{}{}
	}
	originMap[segEntity.InsertAtFirst] = struct{}{}

	// 构建新增数据映射（key: last_org_data_id, value: 切片）
	insertMap := make(map[string]*segEntity.DocSegmentOrgDataTemporary)
	for _, insertSeg := range insertOriginList {
		insertMap[insertSeg.LastOrgDataID] = insertSeg
	}

	// 收集指向非新增数据中的起点节点
	startSegs := make([]*segEntity.DocSegmentOrgDataTemporary, 0)
	for _, insertSeg := range insertOriginList {
		if _, exists := originMap[insertSeg.LastOrgDataID]; exists {
			startSegs = append(startSegs, insertSeg)
		}
	}

	// 按非新增数据分组存储插入数据
	segChains := make(map[string][]*segEntity.DocSegmentOrgDataTemporary)
	for _, startSeg := range startSegs {
		originSegID := startSeg.LastOrgDataID
		chain := []*segEntity.DocSegmentOrgDataTemporary{startSeg}

		// 沿着链向后收集所有节点
		current := startSeg
		for {
			nextSeg, exists := insertMap[current.BusinessID]
			if !exists {
				break
			}
			chain = append(chain, nextSeg)
			current = nextSeg
		}
		segChains[originSegID] = append(segChains[originSegID], chain...)
	}

	// 构建最终节点列表
	finalSegs := make([]*pb.ListDocSegmentRsp_DocSegmentItem, 0)
	// 先增加LastOrgDataID为first的链
	if segChain, exists := segChains[segEntity.InsertAtFirst]; exists {
		for _, orgDate := range segChain {
			docSegmentItem := &pb.ListDocSegmentRsp_DocSegmentItem{
				SegBizId:    orgDate.BusinessID,
				OrgData:     orgDate.OrgData,
				PageInfos:   []uint64{},
				IsOrigin:    false,
				IsAdd:       true,
				SegmentType: "",
				IsDisabled:  orgDate.IsDisabled,
				AuditStatus: uint64(orgDate.AuditStatus),
				SheetName:   orgDate.SheetName,
			}
			finalSegs = append(finalSegs, docSegmentItem)
		}
	}
	// 增加非新增数据关联的链
	for _, originSeg := range originList {
		// 添加非新增数据
		finalSegs = append(finalSegs, originSeg)

		// 添加该节点对应的插入链
		if segChain, exists := segChains[originSeg.SegBizId]; exists {
			for _, orgDate := range segChain {
				docSegmentItem := &pb.ListDocSegmentRsp_DocSegmentItem{
					SegBizId:    orgDate.BusinessID,
					OrgData:     orgDate.OrgData,
					PageInfos:   []uint64{},
					IsOrigin:    false,
					IsAdd:       true,
					SegmentType: "",
					IsDisabled:  orgDate.IsDisabled,
					AuditStatus: uint64(orgDate.AuditStatus),
					SheetName:   orgDate.SheetName,
				}
				finalSegs = append(finalSegs, docSegmentItem)
			}
		}
	}
	logx.I(ctx, "InsertIntoOrgDataList|len(finalSegs):%d", len(finalSegs))
	return finalSegs, nil
}

// GetLastOriginOrgDataIDByLastOrgDataID 获取新增切片对应的原始切片确保可以搜索到
func (l *Logic) GetLastOriginOrgDataIDByLastOrgDataID(ctx context.Context, corpBizID uint64,
	appBizID, docBizID uint64, lastID, afterID string) (string, error) {
	logx.I(ctx, "GetLastOriginOrgDataIDByLastOrgDataID|start|lastID:%s|afterID:%s", lastID, afterID)
	relateID := lastID
	if lastID == segEntity.InsertAtFirst {
		relateID = segEntity.InsertAtFirst
	}
	if !strings.HasPrefix(relateID, segEntity.EditPrefix) && !strings.HasPrefix(relateID, segEntity.InsertPrefix) {
		// 如果lastID不为临时数据，LastOriginOrgDataID用LastOrgDataID进行标识
		return relateID, nil
	} else if strings.HasPrefix(relateID, segEntity.EditPrefix) {
		// 查找该编辑数据对应的原始数据
		orgData, err := l.segDao.GetDocTemporaryOrgDataByBizID(ctx,
			segEntity.DocSegmentOrgDataTemporaryTblColList, corpBizID, appBizID, docBizID, relateID)
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				logx.E(ctx, "GetLastOriginOrgDataIDByLastOrgDataID|orgData is null|lastID:%s", relateID)
				return "", errs.ErrDocSegmentNotFound
			}
			return "", err
		}
		if orgData == nil {
			logx.E(ctx, "GetLastOriginOrgDataIDByLastOrgDataID|orgData is null|lastID:%s", relateID)
			return "", errs.ErrDocSegmentNotFound
		}
		return orgData.OriginOrgDataID, nil
	} else if strings.HasPrefix(relateID, segEntity.InsertPrefix) {
		// 查找该插入数据对应的原始数据&&更新该插入数据
		orgData, err := l.segDao.GetDocTemporaryOrgDataByBizID(ctx,
			segEntity.DocSegmentOrgDataTemporaryTblColList, corpBizID, appBizID, docBizID, relateID)
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				logx.E(ctx, "GetLastOriginOrgDataIDByLastOrgDataID|orgData is null|relateID:%s", relateID)
				return "", errs.ErrDocSegmentNotFound
			}
			return "", err
		}
		if orgData == nil {
			logx.E(ctx, "GetLastOriginOrgDataIDByLastOrgDataID|orgData is null|relateID:%s", relateID)
			return "", errs.ErrDocSegmentNotFound
		}
		return orgData.LastOriginOrgDataID, nil
	}
	return "", nil
}

func (l *Logic) CleanSegmentsForIndex(ctx context.Context, corpBizID, appBizID, docBizID uint64) error {
	// 删除OrgData
	deleteFilter := &segEntity.DocSegmentOrgDataFilter{
		CorpBizID:      corpBizID,
		AppBizID:       appBizID,
		DocBizID:       docBizID,
		IsDeleted:      ptrx.Bool(true),
		RouterAppBizID: appBizID,
	}
	err := l.RealityBatchDeleteDocOrgData(ctx, deleteFilter, 10000)
	if err != nil {
		logx.E(ctx, "createSegment|RealityBatchDeleteDocOrgData failed, err:%+v", err)
		return err
	}
	// 删除临时OrgData
	deleteTempFilter := &segment.DocSegmentOrgDataTemporaryFilter{
		CorpBizID: corpBizID,
		AppBizID:  appBizID,
		DocBizID:  docBizID,
	}
	err = l.segDao.RealityBatchDeleteTemporaryDocOrgData(ctx, deleteTempFilter, 10000)
	if err != nil {
		logx.E(ctx, "createSegment|RealityBatchDeleteDocOrgData failed, err:%+v", err)
		return err
	}
	return nil
}

// getDocSegmentOrgData 获取文档切片OrgData
func (l *Logic) getDocSegmentOrgData(ctx context.Context, orgDataSyncMap *sync.Map,
	segment *segEntity.DocSegmentExtend, docCommon *segEntity.DocSegmentCommon) (*segEntity.DocSegmentOrgData, error) {
	logx.I(ctx, "getDocSegmentOrgData|start|segment:%+v, docCommon:%+v", segment, docCommon)
	// 参数校验
	if orgDataSyncMap == nil {
		logx.E(ctx, "getDocSegmentOrgData|orgDataSyncMap is null")
		return nil, fmt.Errorf("orgDataSyncMap is null")
	}
	if segment == nil {
		logx.E(ctx, "getDocSegmentOrgData|segment is null")
		return nil, fmt.Errorf("segment is null")
	}
	if docCommon == nil {
		logx.E(ctx, "getDocSegmentOrgData|docCommon is null")
		return nil, fmt.Errorf("docCommon is null")
	}
	if len(segment.OrgData) == 0 {
		logx.W(ctx, "getDocSegmentOrgData|OrgData Empty|segment:%+v", segment)
		return nil, nil
	}
	hash := sha256.New()
	_, err := io.WriteString(hash, segment.OrgData)
	if err != nil {
		logx.E(ctx, "getDocSegmentOrgData|WriteString|err:%+v", err)
		return nil, err
	}
	hashValue := hash.Sum(nil)
	uniqueKey := string(hashValue)

	if id, ok := orgDataSyncMap.Load(uniqueKey); ok {
		if segment.OrgDataBizID, ok = id.(uint64); ok {
			segment.OrgData = ""
			return nil, nil
		}
	}
	segment.OrgDataBizID = idgen.GetId()
	// 存入org_data数据库
	orgDataSyncMap.Store(uniqueKey, segment.OrgDataBizID)

	// 解析SheetData，获取sheet名，如有多个只取第一个（按行拆分时使用）
	var sheetDatas []segEntity.SheetData
	err = jsonx.Unmarshal([]byte(segment.SheetData), &sheetDatas)
	if err != nil && segment.SheetData != "" {
		logx.W(ctx, "getDocSegmentOrgData|Unmarshal|err:%+v, SheetData: %+v", err, segment.SheetData)
	}
	sheetName := ""
	if sheetDatas != nil && len(sheetDatas) > 0 {
		sheetName = sheetDatas[0].SheetName
	}

	data := &segEntity.DocSegmentOrgData{
		BusinessID:         segment.OrgDataBizID,
		AppBizID:           docCommon.AppBizID,
		DocBizID:           docCommon.DocBizID,
		CorpBizID:          docCommon.CorpBizID,
		StaffBizID:         docCommon.StaffBizID,
		OrgData:            segment.OrgData,
		OrgPageNumbers:     segment.OrgPageNumbers,
		SheetData:          segment.SheetData,
		SegmentType:        segment.SegmentType,
		AddMethod:          segEntity.AddMethodDefault,
		IsTemporaryDeleted: false,
		IsDeleted:          false,
		IsDisabled:         false,
		CreateTime:         time.Now(),
		UpdateTime:         time.Now(),
		SheetName:          sheetName,
	}
	segment.OrgData = ""
	return data, nil
}

func (l *Logic) RelateOrgDataProcess(ctx context.Context, docCommon *segEntity.DocSegmentCommon, originOrgDataID uint64) (
	string, string, error) {
	lastChainStartOrgData := ""
	relateLastOriginOrgDataID := ""
	relateLastOrgDataID := ""
	// 有查询到关联切片，需要更新关联切片数据
	// 查找原始数据的上一个切片
	lastOrgData, err := l.GetLastOrgDataByCurrentOrgDataBizID(ctx,
		segEntity.DocSegmentOrgDataTblColList, docCommon.CorpBizID,
		docCommon.AppBizID, docCommon.DocBizID, originOrgDataID)
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return "", "", err
	}
	if lastOrgData == nil {
		// 上一个切片不存在，关联切片的前原始切片为first，前一个链的开头为first
		lastChainStartOrgData = segEntity.InsertAtFirst
		relateLastOriginOrgDataID = segEntity.InsertAtFirst
	} else {
		// 上一个切片存在，关联切片的前原始切片为前一切片
		lastTempOrgData, err := l.segDao.GetDocTemporaryOrgDataByOriginOrgDataID(ctx,
			segEntity.DocSegmentOrgDataTemporaryTblColList, docCommon.CorpBizID, docCommon.AppBizID,
			docCommon.DocBizID, strconv.FormatUint(lastOrgData.BusinessID, 10))
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return "", "", err
		}
		if lastTempOrgData != nil {
			// 上一个切片存在编辑数据
			lastChainStartOrgData = lastTempOrgData.BusinessID
		} else {
			lastChainStartOrgData = strconv.FormatUint(lastOrgData.BusinessID, 10)
		}
		relateLastOriginOrgDataID = strconv.FormatUint(lastOrgData.BusinessID, 10)
	}
	// 如果前一个链的开始切片存在，则需要关联到该链的最后
	if lastChainStartOrgData != "" {
		firstOrgData, err := l.segDao.GetDocTemporaryOrgDataByLastOrgDataID(ctx,
			segEntity.DocSegmentOrgDataTemporaryTblColList, docCommon.CorpBizID,
			docCommon.AppBizID, docCommon.DocBizID, lastChainStartOrgData)
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return "", "", err
		}
		if firstOrgData == nil {
			// 前一个链的起始位置之前未被使用，直接使用
			relateLastOrgDataID = lastChainStartOrgData
		} else {
			// 找到上一个链的末尾位置插入
			lastChainOrgData, err := l.GetInsertOrgData(ctx, []string{relateLastOriginOrgDataID}, docCommon)
			if err != nil {
				return "", "", err
			}
			// 构建新增数据映射（key: last_org_data_id, value: 切片）
			insertMap := make(map[string]*segEntity.DocSegmentOrgDataTemporary)
			for _, insertSeg := range lastChainOrgData {
				insertMap[insertSeg.LastOrgDataID] = insertSeg
			}
			// 按非新增数据分组存储插入数据
			chain := []*segEntity.DocSegmentOrgDataTemporary{firstOrgData}

			// 沿着链向后收集所有节点
			current := firstOrgData
			for {
				nextSeg, exists := insertMap[current.BusinessID]
				if !exists {
					break
				}
				chain = append(chain, nextSeg)
				current = nextSeg
			}
			if len(chain) > 0 {
				// 上一个节点为上一个链的末尾位置
				relateLastOrgDataID = chain[len(chain)-1].BusinessID
			}
		}
	}
	return relateLastOrgDataID, relateLastOriginOrgDataID, nil
}

func OriginListToDocSegment(ctx context.Context, list []*segEntity.DocSegmentOrgData) (
	[]*kb_pb.ListDocSegmentRsp_DocSegmentItem, error) {
	docSegmentList := make([]*kb_pb.ListDocSegmentRsp_DocSegmentItem, 0)
	for _, orgDate := range list {
		pageInfos, pageData := make([]uint64, 0), make([]int64, 0)
		if orgDate.OrgPageNumbers != "" {
			if err := jsonx.UnmarshalFromString(orgDate.OrgPageNumbers, &pageData); err != nil {
				logx.W(ctx, "OriginListToDocSegment|PageInfos|UnmarshalFromString|err:%+v", err)
			}
			for _, page := range pageData {
				pageInfos = append(pageInfos, uint64(page))
			}
		}
		docSegmentItem := &kb_pb.ListDocSegmentRsp_DocSegmentItem{
			SegBizId:    strconv.FormatUint(orgDate.BusinessID, 10),
			OrgData:     orgDate.OrgData,
			PageInfos:   pageInfos,
			IsOrigin:    true,
			IsAdd:       false,
			SegmentType: orgDate.SegmentType,
			IsDisabled:  orgDate.IsDisabled,
		}
		docSegmentList = append(docSegmentList, docSegmentItem)
	}
	return docSegmentList, nil
}

// RealityBatchDeleteTemporaryDocOrgData 物理删除临时切片表中的org_data数据
func (l *Logic) RealityBatchDeleteTemporaryDocOrgData(ctx context.Context,
	filter *segEntity.DocSegmentOrgDataTemporaryFilter, batchSize int) error {
	return l.segDao.RealityBatchDeleteTemporaryDocOrgData(ctx, filter, batchSize)
}

// RealityBatchDeleteDocSheet 物理删除临时切片表中的sheet数据
func (l *Logic) RealityBatchDeleteDocSheet(ctx context.Context, filter *segEntity.DocSegmentSheetTemporaryFilter, batchSize int) error {
	return l.segDao.RealityBatchDeleteDocSheet(ctx, filter, batchSize)
}

// GetSegmentBySegmentBizID 根据切片业务ID获取切片信息
func (l *Logic) GetSegmentBySegmentBizID(ctx context.Context, segmentBizID string, appID uint64) (*segEntity.DocSegment, error) {
	if segmentBizID == "" {
		logx.E(ctx, "GetSegmentBySegmentBizID|segmentBizID is empty")
		return nil, errs.ErrParamsNotExpected
	}
	segBizID, err := convx.StringToUint64(segmentBizID)
	if err != nil {
		logx.E(ctx, "GetSegmentBySegmentBizID|StringToUint64 error: %v", err)
		return nil, err
	}
	docSegmentFilter := &segEntity.DocSegmentFilter{
		BusinessIDs: []uint64{segBizID},
	}
	docSegmentFields := segEntity.DocSegmentTblColList
	docSegmentTableName := l.segDao.Query().TDocSegment.TableName()
	db, err := knowClient.GormClient(ctx, docSegmentTableName, appID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "GetSegmentBySegmentBizID|GormClient error: %v", err)
		return nil, err
	}
	segment, err := l.segDao.GetDocSegmentByFilter(ctx, docSegmentFields, docSegmentFilter, db)
	if err != nil {
		logx.E(ctx, "GetSegmentBySegmentBizID|GetDocSegmentByFilter error: %v", err)
		return nil, err
	}
	if segment == nil {
		logx.E(ctx, "GetSegmentBySegmentBizID|segment is nil")
		return nil, errs.ErrDocSegmentNotFound
	}
	return segment, nil
}
