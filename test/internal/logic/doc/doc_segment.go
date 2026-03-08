package doc

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/baicaoyuan/moss/metadata"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/common"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
	jsoniter "github.com/json-iterator/go"
	"gorm.io/gorm"
)

const (
	ModifyDocSegmentDeleteOperate   = 0
	ModifyDocSegmentDisabledOperate = 1
	ModifyDocSegmentEnableOperate   = 2
)

// ListDocSegment 获取切片列表
func ListDocSegment(ctx context.Context, req *pb.ListDocSegmentReq, d dao.Dao,
	docCommon *model.DocSegmentCommon, doc *model.Doc) (*pb.ListDocSegmentRsp, error) {
	log.InfoContextf(ctx, "ListDocSegment|start")
	rsp := new(pb.ListDocSegmentRsp)
	docSegmentList := make([]*pb.ListDocSegmentRsp_DocSegmentItem, 0)
	if req.PageNumber < 1 || req.PageSize < 1 {
		log.ErrorContextf(ctx, "ListDocSegment|params err|req:%+v", req)
		return rsp, errs.ErrParams
	}
	// 获取文档所有的切片数量
	num, tempNum, err := GetDocOrgDataCountByDocBizID(ctx, docCommon)
	if err != nil {
		log.ErrorContextf(ctx, "GetDocOrgDataCountByDocBizID|err:%+v", err)
		return rsp, errs.ErrSystem
	}
	// 当文档状态为审核失败状态时获取审核失败切片的数量
	if doc.IsAuditFailed() {
		log.InfoContextf(ctx, "ListDocSegment|count AuditFailNumber")
		deletedFlag := dao.IsNotDeleted
		filter := &dao.DocSegmentOrgDataTemporaryFilter{
			CorpBizID: docCommon.CorpBizID,
			AppBizID:  docCommon.AppBizID,
			DocBizID:  doc.BusinessID,
			IsDeleted: &deletedFlag,
			AuditStatus: []uint32{
				uint32(model.DocSegmentAuditStatusContentFailed),
				uint32(model.DocSegmentAuditStatusPictureFailed),
				uint32(model.DocSegmentAuditStatusContentAndPictureFailed)},
		}
		auditFailNum, err := dao.GetDocSegmentOrgDataTemporaryDao().GetDocOrgDataCount(ctx, filter)
		if err != nil {
			log.ErrorContextf(ctx, "getDocNotDeleteTemporaryOrgData|GetDocOrgDataCount|err:%v", err)
			return nil, err
		}
		rsp.AuditFailedNumber = uint64(auditFailNum)
	}
	rsp.FileSize = strconv.FormatUint(doc.FileSize, 10)
	rsp.SegmentNumber = strconv.FormatInt(num+tempNum, 10)
	// 确认改文档是否被编辑过（临时表是否有数据，orgData是否有临时删除）
	intervene, err := CheckDocIntervene(ctx, docCommon)
	if err != nil {
		return rsp, err
	}
	rsp.IsModify = intervene
	// 如果参考ID存在（对话处使用输入后会检索t_refer表）
	if req.ReferBizId != "" {
		return ListDocSegmentByReferBizID(ctx, req, d, docCommon, rsp)
	}
	// 分页查询，是否含有关键词
	if req.GetKeywords() != "" {
		return ListDocSegmentByKeywords(ctx, req, d, docCommon, num, tempNum, rsp)
	} else {
		log.InfoContextf(ctx, "ListDocSegment|NotKeywords")
		// 不含关键词，直接查询数据库
		docSegmentList, err = GetDocSegmentList(ctx, req, docCommon)
		if err != nil {
			return nil, err
		}
		rsp.SegmentList = docSegmentList
		rsp.Total = uint64(len(docSegmentList))
		return rsp, nil
	}
}

// ModifyDocSegment 保存切片修改(单个)
func ModifyDocSegment(ctx context.Context, req *pb.ModifyDocSegmentReq, d dao.Dao,
	docCommon *model.DocSegmentCommon, doc *model.Doc) (*pb.ModifyDocSegmentRsp, error) {
	log.InfoContextf(ctx, "ModifyDocSegment|start")
	rsp := new(pb.ModifyDocSegmentRsp)
	// todo 优化为状态机形式
	// 新增/编辑切片
	err := d.GetTdsqlGormDB().Transaction(func(tx *gorm.DB) error {
		if len(req.ModifySegments) > 0 {
			log.InfoContextf(ctx, "ModifyDocSegment|UpdateOrInsert|len(ModifySegments):%d", len(req.ModifySegments))
			for _, segment := range req.GetModifySegments() {
				if segment.SegBizId != "" {
					// 编辑
					log.InfoContextf(ctx, "ModifyDocSegment|Update|SegBizId:%s", segment.SegBizId)
					// 检查id格式，id以edit/insert开头，为临时表数据，去临时表查询/更新
					if strings.HasPrefix(segment.SegBizId, model.EditPrefix) || strings.HasPrefix(segment.SegBizId, model.InsertPrefix) {
						orgData, err := dao.GetDocSegmentOrgDataTemporaryDao().GetDocOrgDataByBizID(ctx,
							dao.DocSegmentOrgDataTemporaryTblColList, docCommon.CorpBizID, docCommon.AppBizID,
							docCommon.DocBizID, segment.SegBizId)
						if err != nil {
							if errors.Is(err, gorm.ErrRecordNotFound) {
								log.ErrorContextf(ctx, "ModifyDocSegment|orgData is null")
								return errs.ErrDocSegmentNotFound
							}
							return err
						}
						if orgData == nil {
							log.ErrorContextf(ctx, "ModifyDocSegment|orgData is null")
							return errs.ErrDocSegmentNotFound
						}
						err = dao.GetDocSegmentOrgDataTemporaryDao().UpdateDocSegmentOrgDataContent(ctx, tx,
							docCommon.CorpBizID, docCommon.AppBizID, docCommon.DocBizID,
							[]string{segment.SegBizId}, segment.OrgData)
						if err != nil {
							return err
						}
					} else {
						// 从原始表查询，在临时表新增
						// 如果切片被编辑过，则阻止操作
						tempOrgData, err := dao.GetDocSegmentOrgDataTemporaryDao().GetDocOrgDataByOriginOrgDataID(ctx,
							dao.DocSegmentOrgDataTemporaryTblColList, docCommon.CorpBizID, docCommon.AppBizID,
							docCommon.DocBizID, segment.SegBizId)
						if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
							return err
						}
						if tempOrgData != nil {
							log.ErrorContextf(ctx, "ModifyDocSegment|orgData is edit, operation not allowed")
							return errs.ErrDocSegmentOperationNotAllowedFailed
						}
						// 如果原始数据ID有被新增关联顺序，需替换
						originOrgDataID, err := util.CheckReqParamsIsUint64(ctx, segment.SegBizId)
						if err != nil {
							log.ErrorContextf(ctx, "ModifyDocSegmentByOperate|DocBizIDToUint64|err:%+v", err)
							return err
						}
						orgData, err := dao.GetDocSegmentOrgDataDao().GetDocOrgDataByBizID(ctx,
							dao.DocSegmentOrgDataTblColList, docCommon.CorpBizID, docCommon.AppBizID,
							docCommon.DocBizID, originOrgDataID)
						if err != nil {
							if errors.Is(err, gorm.ErrRecordNotFound) {
								log.ErrorContextf(ctx, "ModifyDocSegment|orgData is null")
								return errs.ErrDocSegmentNotFound
							}
							return err
						}
						if orgData == nil {
							log.ErrorContextf(ctx, "ModifyDocSegment|orgData is null")
							return errs.ErrDocSegmentNotFound
						}
						orgDataTemp := &model.DocSegmentOrgDataTemporary{}
						orgDataTemp.BusinessID = model.EditPrefix + strconv.FormatUint(d.GenerateSeqID(), 10)
						orgDataTemp.CorpBizID = orgData.CorpBizID
						orgDataTemp.AppBizID = orgData.AppBizID
						orgDataTemp.DocBizID = orgData.DocBizID
						orgDataTemp.AddMethod = model.AddMethodEdit
						orgDataTemp.Action = dao.EditAction
						orgDataTemp.OrgPageNumbers = orgData.OrgPageNumbers
						orgDataTemp.SegmentType = orgData.SegmentType
						orgDataTemp.OriginOrgDataID = strconv.FormatUint(orgData.BusinessID, 10)
						orgDataTemp.IsDeleted = dao.IsNotDeleted
						orgDataTemp.IsDisabled = model.SegmentIsEnable
						orgDataTemp.SheetName = orgData.SheetName
						orgDataTemp.CreateTime = time.Now()
						orgDataTemp.UpdateTime = time.Now()
						// excel文件需要拼接sheetName
						if IsExcel(doc.FileType) {
							orgDataTemp.OrgData = GetSliceTable(orgData.OrgData, 0) + "\n" + segment.OrgData
						} else {
							orgDataTemp.OrgData = segment.OrgData
						}
						err = dao.GetDocSegmentOrgDataTemporaryDao().CreateDocSegmentOrgData(ctx, tx, orgDataTemp)
						if err != nil {
							return err
						}
						// 查找原始id是否被插入使用
						oldOrgData, err := dao.GetDocSegmentOrgDataTemporaryDao().GetDocOrgDataByLastOrgDataID(ctx,
							dao.DocSegmentOrgDataTemporaryTblColList, docCommon.CorpBizID, docCommon.AppBizID,
							docCommon.DocBizID, segment.SegBizId)
						if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
							return err
						}
						if oldOrgData != nil {
							// 原始id有被插入使用
							updateColumns := []string{
								dao.DocSegmentOrgDataTemporaryTblColLastOrgDataID,
								dao.DocSegmentOrgDataTemporaryTblColUpdateTime,
							}
							update := &model.DocSegmentOrgDataTemporary{
								LastOrgDataID: orgDataTemp.BusinessID,
								UpdateTime:    time.Now(),
							}
							filter := &dao.DocSegmentOrgDataTemporaryFilter{
								CorpBizID:   docCommon.CorpBizID,
								AppBizID:    docCommon.AppBizID,
								DocBizID:    docCommon.DocBizID,
								BusinessIDs: []string{oldOrgData.BusinessID},
							}
							_, err := dao.GetDocSegmentOrgDataTemporaryDao().UpdateDocSegmentOrgData(ctx, tx,
								updateColumns, filter, update)
							if err != nil {
								return err
							}
						}
					}
				} else {
					// 新增
					log.InfoContextf(ctx, "ModifyDocSegment|Insert|LastSegBizId:%s", segment.LastSegBizId)
					// 参数校验
					if segment.OrgData == "" || segment.LastSegBizId == "" {
						return errs.ErrParams
					} else if segment.LastSegBizId == model.InsertAtFirst && segment.AfterSegBizId == "" {
						return errs.ErrParams
					}
					// 获取改数据插入位置关联的原始切片数据
					lastOriginOrgDataID, err := GetLastOriginOrgDataIDByLastOrgDataID(ctx, docCommon.CorpBizID,
						docCommon.AppBizID, docCommon.DocBizID, segment.LastSegBizId, segment.AfterSegBizId)
					if err != nil {
						return err
					}
					if lastOriginOrgDataID == "" {
						log.ErrorContextf(ctx, "ModifyDocSegment|Insert|lastOriginOrgDataID is empty")
						return errs.ErrDocSegmentNotFound
					}
					orgDataTemp := &model.DocSegmentOrgDataTemporary{}
					orgDataTemp.BusinessID = model.InsertPrefix + strconv.FormatUint(d.GenerateSeqID(), 10)
					orgDataTemp.CorpBizID = docCommon.CorpBizID
					orgDataTemp.AppBizID = docCommon.AppBizID
					orgDataTemp.DocBizID = docCommon.DocBizID
					orgDataTemp.AddMethod = model.AddMethodArtificial
					orgDataTemp.Action = dao.InsertAction
					orgDataTemp.LastOriginOrgDataID = lastOriginOrgDataID
					orgDataTemp.LastOrgDataID = segment.LastSegBizId
					orgDataTemp.AfterOrgDataID = segment.AfterSegBizId
					orgDataTemp.IsDeleted = dao.IsNotDeleted
					orgDataTemp.CreateTime = time.Now()
					orgDataTemp.UpdateTime = time.Now()
					orgDataTemp.SheetName = req.GetSheetName()

					// 查询旧插入数据
					oldOrgData, err := dao.GetDocSegmentOrgDataTemporaryDao().GetDocOrgDataByLastOrgDataID(ctx,
						dao.DocSegmentOrgDataTemporaryTblColList, docCommon.CorpBizID, docCommon.AppBizID,
						docCommon.DocBizID, segment.LastSegBizId)
					if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
						return err
					}
					// excel文件需要拼接sheetName
					if oldOrgData != nil && IsExcel(doc.FileType) {
						orgDataTemp.OrgData = GetSliceTable(oldOrgData.OrgData, 0) + "\n" + segment.OrgData
					} else {
						orgDataTemp.OrgData = segment.OrgData
					}
					err = dao.GetDocSegmentOrgDataTemporaryDao().CreateDocSegmentOrgData(ctx, tx, orgDataTemp)
					if err != nil {
						return err
					}
					// 更新旧插入数据
					if oldOrgData != nil {
						updateColumns := []string{
							dao.DocSegmentOrgDataTemporaryTblColLastOrgDataID,
							dao.DocSegmentOrgDataTemporaryTblColUpdateTime,
						}
						update := &model.DocSegmentOrgDataTemporary{
							LastOrgDataID: orgDataTemp.BusinessID,
							UpdateTime:    time.Now(),
						}
						filter := &dao.DocSegmentOrgDataTemporaryFilter{
							CorpBizID:   docCommon.CorpBizID,
							AppBizID:    docCommon.AppBizID,
							DocBizID:    docCommon.DocBizID,
							BusinessIDs: []string{oldOrgData.BusinessID},
						}
						_, err := dao.GetDocSegmentOrgDataTemporaryDao().UpdateDocSegmentOrgData(ctx, tx,
							updateColumns, filter, update)
						if err != nil {
							return err
						}
					}
				}
			}
		}
		// 删除切片
		if len(req.DeleteSegBizIds) > 0 {
			err := ModifyDocSegmentByOperate(ctx, docCommon, req.DeleteSegBizIds, ModifyDocSegmentDeleteOperate, tx)
			if err != nil {
				return err
			}
		}
		// 停用切片
		if len(req.DisabledSegBizIds) > 0 {
			err := ModifyDocSegmentByOperate(ctx, docCommon, req.DisabledSegBizIds, ModifyDocSegmentDisabledOperate, tx)
			if err != nil {
				return err
			}
		}
		// 启用切片
		if len(req.EnableSegBizIds) > 0 {
			err := ModifyDocSegmentByOperate(ctx, docCommon, req.EnableSegBizIds, ModifyDocSegmentEnableOperate, tx)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		log.ErrorContextf(ctx, "ModifyDocSegment|err:%+v", err)
		return rsp, errs.ErrSystem
	}
	return rsp, nil
}

// CreateDocParsingIntervention 创建解析干预任务
func CreateDocParsingIntervention(ctx context.Context, d dao.Dao,
	docCommon *model.DocSegmentCommon, auditFlag uint32, doc *model.Doc) (*pb.CreateDocParsingInterventionRsp, error) {
	rsp := new(pb.CreateDocParsingInterventionRsp)
	log.InfoContextf(ctx, "CreateDocParsingIntervention|start")
	// 老文档状态等字段更新
	if err := UpdateOldDocStatus(ctx, auditFlag, doc); err != nil {
		log.ErrorContextf(ctx, "CreateDocParsingIntervention|UpdateDocStatus|err:%+v", err)
		return rsp, err
	}
	// 减少已使用字符数
	if err := d.UpdateAppUsedCharSizeTx(ctx, -int64(doc.CharSize), doc.RobotID); err != nil {
		log.ErrorContextf(ctx, "CreateDocParsingIntervention|UpdateAppUsedCharSizeTx|err:%+v", err)
		return rsp, err
	}
	// 触发异步任务
	taskID := d.GenerateSeqID()
	if err := dao.NewDocSegInterveneTask(ctx, docCommon.AppID, model.DocSegInterveneParams{
		CorpID:         docCommon.CorpID,
		CorpBizID:      docCommon.CorpBizID,
		StaffID:        docCommon.StaffID,
		StaffBizID:     docCommon.StaffBizID,
		AppBizID:       docCommon.AppBizID,
		AppID:          docCommon.AppID,
		OriginDocBizID: docCommon.DocBizID,
		TaskID:         taskID,
		FileType:       doc.FileType,
		FileName:       doc.FileName,
		SourceEnvSet:   metadata.Metadata(ctx).EnvSet(),
		DataSource:     docCommon.DataSource,
	}); err != nil {
		log.ErrorContextf(ctx, "CreateDocParsingIntervention|NewDocSegInterveneTask|err:%+v", err)
		return rsp, err
	}
	log.InfoContextf(ctx, "CreateDocParsingIntervention|scheduler task running|taskID:%d", taskID)
	return rsp, nil
}

func ListDocSegmentByKeywords(ctx context.Context, req *pb.ListDocSegmentReq, d dao.Dao,
	docCommon *model.DocSegmentCommon, num, tempNum int64, rsp *pb.ListDocSegmentRsp) (*pb.ListDocSegmentRsp, error) {
	docSegmentList := make([]*pb.ListDocSegmentRsp_DocSegmentItem, 0)
	log.InfoContextf(ctx, "ListDocSegment|Keywords:%s", req.Keywords)
	docSegmentFilter, err := CheckAndConvertFilters(ctx, req.GetFilters())
	if err != nil {
		log.ErrorContextf(ctx, "ListDocSegmentByKeywords|CheckAndConvertFilters|err:%+v", err)
		return rsp, err
	}
	if len(docSegmentFilter.AuditStatusFilter) != 0 {
		log.InfoContextf(ctx, "GetDocSegmentList|AuditStatusFilter")
		// 如果存在审核状态的查询则仅查找临时表数据
		deletedFlag := dao.IsNotDeleted
		filter := &dao.DocSegmentOrgDataTemporaryFilter{
			CorpBizID:   docCommon.CorpBizID,
			AppBizID:    docCommon.AppBizID,
			DocBizID:    docCommon.DocBizID,
			IsDeleted:   &deletedFlag,
			Keywords:    req.Keywords,
			AuditStatus: docSegmentFilter.AuditStatusFilter,
			Offset:      common.GetOffsetByPage(req.PageNumber, req.PageSize),
			Limit:       req.PageSize,
			SheetName:   docCommon.SheetName,
		}
		list, err := dao.GetDocSegmentOrgDataTemporaryDao().GetDocOrgDataListByKeyWords(ctx,
			dao.DocSegmentOrgDataTemporaryTblColList, filter)
		if err != nil {
			log.ErrorContextf(ctx, "ListDocSegmentByKeywords|GetDocOrgDataListByKeyWords|err:%+v", err)
			return rsp, err
		}
		tempList, err := TempOriginListToDocSegment(ctx, list)
		if err != nil {
			log.ErrorContextf(ctx, "ListDocSegmentByKeywords|TempOriginListToDocSegment|err:%+v", err)
			return nil, err
		}
		docSegmentList = append(docSegmentList, tempList...)
		rsp.Total = uint64(len(docSegmentList))
		rsp.SegmentList = docSegmentList
		rsp.IsModify = true
		return rsp, nil
	}
	// 使用字符串匹配，先查找临时表数据(将临时表数据都查出来)，再查找主表
	tempOriginList, err := GetOrgDataByKeywords(ctx, req.Keywords, docCommon, tempNum)
	if err != nil {
		return nil, err
	}
	lack := int(req.PageSize*req.PageNumber) - len(tempOriginList)
	log.InfoContextf(ctx, "ListDocSegment|lack:%d|len(tempOriginList):%d", lack, len(tempOriginList))
	if lack <= 0 {
		startIndex := req.PageSize * (req.PageNumber - 1)
		endIndex := req.PageSize * req.PageNumber
		tempList, err := TempOriginListToDocSegment(ctx, tempOriginList[startIndex:endIndex])
		if err != nil {
			return nil, err
		}
		docSegmentList = append(docSegmentList, tempList...)
	} else if lack <= int(req.PageSize) {
		startIndex := req.PageSize * (req.PageNumber - 1)
		tempList, err := TempOriginListToDocSegment(ctx, tempOriginList[startIndex:])
		if err != nil {
			return nil, err
		}
		docSegmentList = append(docSegmentList, tempList...)
		originList, _, err := GetDocSegmentOrgData(ctx, req, docCommon, 0, uint32(lack))
		if err != nil {
			log.ErrorContextf(ctx, "ListDocSegment|GetDocSegmentOrgData|err:%+v", err)
			return nil, errs.ErrSystem
		}
		docSegmentList = append(docSegmentList, originList...)
	} else {
		originList, _, err := GetDocSegmentOrgData(ctx, req, docCommon, uint32(lack-int(req.PageSize)-1), req.PageSize)
		if err != nil {
			log.ErrorContextf(ctx, "ListDocSegment|GetDocSegmentOrgData|err:%+v", err)
			return nil, errs.ErrSystem
		}
		docSegmentList = append(docSegmentList, originList...)
	}
	rsp.Total = uint64(len(docSegmentList))
	rsp.SegmentList = docSegmentList
	rsp.IsModify = true
	return rsp, nil
}

func ListDocSegmentByReferBizID(ctx context.Context, req *pb.ListDocSegmentReq, d dao.Dao,
	docCommon *model.DocSegmentCommon, rsp *pb.ListDocSegmentRsp) (*pb.ListDocSegmentRsp, error) {
	docSegmentList := make([]*pb.ListDocSegmentRsp_DocSegmentItem, 0)
	log.InfoContextf(ctx, "ListDocSegmentByReferBizID|ReferBizId:%s", req.ReferBizId)
	segment, err := GetSegmentByReferID(ctx, d, docCommon.AppID, req.ReferBizId)
	if err != nil {
		return rsp, err
	}
	// 先查临时表，看是否有编辑过的数据
	// 兼容共享知识库
	app, err := d.GetAppByID(ctx, segment.RobotID)
	if err != nil {
		return rsp, err
	}
	docCommon.AppID = segment.RobotID
	docCommon.AppBizID = app.BusinessID
	editOriginList, err := GetEditOrgData(ctx, []string{strconv.FormatUint(segment.OrgDataBizID, 10)}, docCommon)
	if err != nil {
		log.ErrorContextf(ctx, "GetEditOrgData failed, err:%+v", err)
		return rsp, errs.ErrSystem
	}
	if len(editOriginList) == 1 {
		docSegmentList, err = TempOriginListToDocSegment(ctx, editOriginList)
		if err != nil {
			log.ErrorContextf(ctx, "TempOriginListToDocSegment failed, err:%+v", err)
			return rsp, errs.ErrSystem
		}
		rsp.SegmentList = docSegmentList
		rsp.Total = uint64(len(docSegmentList))
		return rsp, nil
	}
	// 临时表没有，返回原数据
	orgData, err := dao.GetDocSegmentOrgDataDao().GetDocOrgDataByBizID(ctx,
		dao.DocSegmentOrgDataTblColList, docCommon.CorpBizID, docCommon.AppBizID, docCommon.DocBizID,
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
	docSegmentList, err = OriginListToDocSegment(ctx, []*model.DocSegmentOrgData{orgData})
	if err != nil {
		log.ErrorContextf(ctx, "OriginListToDocSegment failed for orgData.BusinessID:%s, err:%+v", orgData.BusinessID, err)
		return nil, err
	}
	rsp.SegmentList = docSegmentList
	rsp.Total = uint64(len(docSegmentList))
	return rsp, nil
}

func GetDocOrgDataCountByDocBizID(ctx context.Context, docCommon *model.DocSegmentCommon) (int64, int64, error) {
	deleteFlag := dao.IsNotDeleted
	filter := &dao.DocSegmentOrgDataFilter{
		CorpBizID:          docCommon.CorpBizID,
		AppBizID:           docCommon.AppBizID,
		DocBizID:           docCommon.DocBizID,
		IsDeleted:          &deleteFlag,
		IsTemporaryDeleted: &deleteFlag,
		RouterAppBizID:     docCommon.AppBizID,
		SheetName:          docCommon.SheetName,
	}
	num, err := dao.GetDocSegmentOrgDataDao().GetDocOrgDataCount(ctx, filter)
	if err != nil {
		log.ErrorContextf(ctx, "GetDocOrgDataCountByDocBizID|GetDocOrgDataCount|err:%v", err)
		return 0, 0, err
	}
	deletedFlag := dao.IsNotDeleted
	actionFlag := dao.InsertAction
	tempFilter := &dao.DocSegmentOrgDataTemporaryFilter{
		CorpBizID: docCommon.CorpBizID,
		AppBizID:  docCommon.AppBizID,
		DocBizID:  docCommon.DocBizID,
		IsDeleted: &deletedFlag,
		Action:    &actionFlag,
		SheetName: docCommon.SheetName,
	}
	tempNum, err := dao.GetDocSegmentOrgDataTemporaryDao().GetDocOrgDataCount(ctx, tempFilter)
	if err != nil {
		log.ErrorContextf(ctx, "GetDocOrgDataCountByDocBizID|GetDocSegmentOrgDataTemporaryDao|err:%v", err)
		return 0, 0, err
	}
	return num, tempNum, nil
}

// GetDocSegmentOrgData 获取OrgData
func GetDocSegmentOrgData(ctx context.Context, req *pb.ListDocSegmentReq,
	docCommon *model.DocSegmentCommon, offset, limit uint32) ([]*pb.ListDocSegmentRsp_DocSegmentItem, []string, error) {
	log.InfoContextf(ctx, "GetDocSegmentOrgData|start|offset:%d|limit:%d", offset, limit)
	orgDataList := make([]*pb.ListDocSegmentRsp_DocSegmentItem, 0)
	orgDateBizIDs := make([]string, 0)
	deletedFlag := dao.IsNotDeleted
	list := make([]*model.DocSegmentOrgData, 0)
	var err error
	filter := &dao.DocSegmentOrgDataFilter{
		CorpBizID:          docCommon.CorpBizID,
		AppBizID:           docCommon.AppBizID,
		DocBizID:           docCommon.DocBizID,
		Keywords:           req.Keywords,
		IsDeleted:          &deletedFlag,
		IsTemporaryDeleted: &deletedFlag,
		OrderColumn:        []string{dao.DocSegmentOrgDataTblColBusinessID},
		OrderDirection:     []string{dao.SqlOrderByAsc},
		Offset:             offset,
		Limit:              limit,
		RouterAppBizID:     docCommon.AppBizID,
		SheetName:          docCommon.SheetName,
	}
	if req.Keywords != "" {
		list, err = dao.GetDocSegmentOrgDataDao().GetDocOrgDataListByKeyWords(ctx,
			dao.DocSegmentOrgDataTblColList, filter)
		if err != nil {
			return orgDataList, orgDateBizIDs, err
		}
	} else {
		list, err = dao.GetDocSegmentOrgDataDao().GetDocOrgDataList(ctx,
			dao.DocSegmentOrgDataTblColList, filter)
		if err != nil {
			return orgDataList, orgDateBizIDs, err
		}
	}
	for _, orgDate := range list {
		pageInfos, pageData := make([]uint64, 0), make([]int64, 0)
		if orgDate.OrgPageNumbers != "" {
			if err = jsoniter.UnmarshalFromString(orgDate.OrgPageNumbers, &pageData); err != nil {
				log.WarnContextf(ctx, "GetDocSegmentOrgData|PageInfos|UnmarshalFromString|err:%+v", err)
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
			IsOrigin:    orgDate.AddMethod == model.AddMethodDefault,
			IsAdd:       orgDate.AddMethod == model.AddMethodArtificial,
			SegmentType: orgDate.SegmentType,
			IsDisabled:  orgDate.IsDisabled == model.SegmentIsDisabled,
			SheetName:   orgDate.SheetName,
		}
		orgDataList = append(orgDataList, docSegmentItem)
	}
	log.InfoContextf(ctx, "GetDocSegmentOrgData|len(OrgData):%d", len(orgDataList))
	return orgDataList, orgDateBizIDs, nil
}

// GetEditOrgData 获取编辑的切片
func GetEditOrgData(ctx context.Context, orgDateBizIDs []string,
	docCommon *model.DocSegmentCommon) ([]*model.DocSegmentOrgDataTemporary, error) {
	log.InfoContextf(ctx, "GetEditOrgData|start")
	actionFlag := dao.EditAction
	deletedFlag := dao.IsNotDeleted
	filter := &dao.DocSegmentOrgDataTemporaryFilter{
		CorpBizID:        docCommon.CorpBizID,
		AppBizID:         docCommon.AppBizID,
		DocBizID:         docCommon.DocBizID,
		SheetName:        docCommon.SheetName,
		IsDeleted:        &deletedFlag,
		Action:           &actionFlag,
		OriginOrgDataIDs: orgDateBizIDs,
		OrderColumn:      []string{dao.DocSegmentOrgDataTemporaryTblColBusinessID},
		OrderDirection:   []string{dao.SqlOrderByAsc},
	}
	originList, err := dao.GetDocSegmentOrgDataTemporaryDao().GetEditOrgData(ctx,
		dao.DocSegmentOrgDataTemporaryTblColList, filter)
	if err != nil {
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, err
		}
	}
	log.InfoContextf(ctx, "GetEditOrgData|len(OrgData):%d", len(originList))
	return originList, nil
}

// GetInsertOrgData 获取插入的切片
func GetInsertOrgData(ctx context.Context, orgDateBizIDs []string,
	docCommon *model.DocSegmentCommon) ([]*model.DocSegmentOrgDataTemporary, error) {
	log.InfoContextf(ctx, "GetInsertOrgData|start")
	actionFlag := dao.InsertAction
	deletedFlag := dao.IsNotDeleted
	filter := &dao.DocSegmentOrgDataTemporaryFilter{
		CorpBizID:            docCommon.CorpBizID,
		AppBizID:             docCommon.AppBizID,
		DocBizID:             docCommon.DocBizID,
		IsDeleted:            &deletedFlag,
		Action:               &actionFlag,
		LastOriginOrgDataIDs: orgDateBizIDs,
		OrderColumn:          []string{dao.DocSegmentOrgDataTemporaryTblColBusinessID},
		OrderDirection:       []string{dao.SqlOrderByAsc},
		SheetName:            docCommon.SheetName,
	}
	originList, err := dao.GetDocSegmentOrgDataTemporaryDao().GetInsertOrgData(ctx,
		dao.DocSegmentOrgDataTemporaryTblColList, filter)
	if err != nil {
		log.ErrorContextf(ctx, "GetInsertOrgData|err:%v", err)
		return nil, err
	}
	log.InfoContextf(ctx, "GetInsertOrgData|len(OrgData):%d", len(originList))
	return originList, nil
}

// GetOrgDataByKeywords 根据关键词获取全部临时切片
func GetOrgDataByKeywords(ctx context.Context, keywords string, docCommon *model.DocSegmentCommon, tempNum int64) (
	[]*model.DocSegmentOrgDataTemporary, error) {
	log.InfoContextf(ctx, "GetOrgDataByKeywords|start")
	deletedFlag := dao.IsNotDeleted
	originList := make([]*model.DocSegmentOrgDataTemporary, 0)
	pageNumber := uint32(1)
	pageSize := uint32(100)
	for {
		filter := &dao.DocSegmentOrgDataTemporaryFilter{
			CorpBizID: docCommon.CorpBizID,
			AppBizID:  docCommon.AppBizID,
			DocBizID:  docCommon.DocBizID,
			IsDeleted: &deletedFlag,
			Keywords:  keywords,
			Offset:    common.GetOffsetByPage(pageNumber, pageSize),
			Limit:     pageSize,
			SheetName: docCommon.SheetName,
		}
		list, err := dao.GetDocSegmentOrgDataTemporaryDao().GetDocOrgDataListByKeyWords(ctx,
			dao.DocSegmentOrgDataTemporaryTblColList, filter)
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
	log.InfoContextf(ctx, "GetOrgDataByKeywords|len(OrgData):%d", len(originList))
	return originList, nil
}

// CheckDocIntervene 检查文档是否有改动
func CheckDocIntervene(ctx context.Context, docCommon *model.DocSegmentCommon) (bool, error) {
	// 检测临时表是否有数据
	deletedFlag := dao.IsNotDeleted
	tempFilter := &dao.DocSegmentOrgDataTemporaryFilter{
		CorpBizID: docCommon.CorpBizID,
		AppBizID:  docCommon.AppBizID,
		DocBizID:  docCommon.DocBizID,
		IsDeleted: &deletedFlag,
		Offset:    0,
		Limit:     1,
	}
	tempList, err := dao.GetDocSegmentOrgDataTemporaryDao().GetDocOrgDataByDocBizID(ctx,
		dao.DocSegmentOrgDataTemporaryTblColList, tempFilter)
	if err != nil {
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return false, err
		}
	}
	if len(tempList) > 0 {
		log.InfoContextf(ctx, "CheckDocIntervene|update")
		return true, nil
	}
	// 检测是否有切片被删除
	tempDeletedFlag := dao.IsDeleted
	filter := &dao.DocSegmentOrgDataFilter{
		CorpBizID:          docCommon.CorpBizID,
		AppBizID:           docCommon.AppBizID,
		DocBizID:           docCommon.DocBizID,
		IsDeleted:          &deletedFlag,
		IsTemporaryDeleted: &tempDeletedFlag,
		Offset:             0,
		Limit:              1,
		RouterAppBizID:     docCommon.AppBizID,
	}
	list, err := dao.GetDocSegmentOrgDataDao().GetDocOrgDataList(ctx,
		dao.DocSegmentOrgDataTblColList, filter)
	if err != nil {
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return false, err
		}
	}
	if len(list) > 0 {
		log.InfoContextf(ctx, "CheckDocIntervene|tempDeleted")
		return true, nil
	}
	return false, nil
}

// InsertIntoOrgDataList 将新增切片放入对应位置
func InsertIntoOrgDataList(ctx context.Context, insertOriginList []*model.DocSegmentOrgDataTemporary,
	originList []*pb.ListDocSegmentRsp_DocSegmentItem) ([]*pb.ListDocSegmentRsp_DocSegmentItem, error) {
	log.InfoContextf(ctx, "InsertIntoOrgDataList|start")
	// 切片内容插入
	// 构建非新增数据映射（用于快速查找）
	originMap := make(map[string]struct{})
	for _, originSeg := range originList {
		originMap[originSeg.SegBizId] = struct{}{}
	}
	originMap[model.InsertAtFirst] = struct{}{}

	// 构建新增数据映射（key: last_org_data_id, value: 切片）
	insertMap := make(map[string]*model.DocSegmentOrgDataTemporary)
	for _, insertSeg := range insertOriginList {
		insertMap[insertSeg.LastOrgDataID] = insertSeg
	}

	// 收集指向非新增数据中的起点节点
	startSegs := make([]*model.DocSegmentOrgDataTemporary, 0)
	for _, insertSeg := range insertOriginList {
		if _, exists := originMap[insertSeg.LastOrgDataID]; exists {
			startSegs = append(startSegs, insertSeg)
		}
	}

	// 按非新增数据分组存储插入数据
	segChains := make(map[string][]*model.DocSegmentOrgDataTemporary)
	for _, startSeg := range startSegs {
		originSegID := startSeg.LastOrgDataID
		chain := []*model.DocSegmentOrgDataTemporary{startSeg}

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
	if segChain, exists := segChains[model.InsertAtFirst]; exists {
		for _, orgDate := range segChain {
			docSegmentItem := &pb.ListDocSegmentRsp_DocSegmentItem{
				SegBizId:    orgDate.BusinessID,
				OrgData:     orgDate.OrgData,
				PageInfos:   []uint64{},
				IsOrigin:    false,
				IsAdd:       true,
				SegmentType: "",
				IsDisabled:  orgDate.IsDisabled == model.SegmentIsDisabled,
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
					IsDisabled:  orgDate.IsDisabled == model.SegmentIsDisabled,
					AuditStatus: uint64(orgDate.AuditStatus),
					SheetName:   orgDate.SheetName,
				}
				finalSegs = append(finalSegs, docSegmentItem)
			}
		}
	}
	log.InfoContextf(ctx, "InsertIntoOrgDataList|len(finalSegs):%d", len(finalSegs))
	return finalSegs, nil
}

func isModifyOrgData(orgDataBizID string) (isOrigin, isAdd bool) {
	if strings.HasPrefix(orgDataBizID, model.EditPrefix) {
		isOrigin = false
		isAdd = false
	} else if strings.HasPrefix(orgDataBizID, model.InsertPrefix) {
		isOrigin = false
		isAdd = true
	}
	return isOrigin, isAdd
}

// GetLastOriginOrgDataIDByLastOrgDataID 获取新增切片对应的原始切片确保可以搜索到
func GetLastOriginOrgDataIDByLastOrgDataID(ctx context.Context, corpBizID uint64,
	appBizID, docBizID uint64, lastID, afterID string) (string, error) {
	log.InfoContextf(ctx, "GetLastOriginOrgDataIDByLastOrgDataID|start|lastID:%s|afterID:%s", lastID, afterID)
	relateID := lastID
	if lastID == model.InsertAtFirst {
		relateID = model.InsertAtFirst
	}
	if !strings.HasPrefix(relateID, model.EditPrefix) && !strings.HasPrefix(relateID, model.InsertPrefix) {
		// 如果lastID不为临时数据，LastOriginOrgDataID用LastOrgDataID进行标识
		return relateID, nil
	} else if strings.HasPrefix(relateID, model.EditPrefix) {
		// 查找该编辑数据对应的原始数据
		orgData, err := dao.GetDocSegmentOrgDataTemporaryDao().GetDocOrgDataByBizID(ctx,
			dao.DocSegmentOrgDataTemporaryTblColList, corpBizID, appBizID, docBizID, relateID)
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				log.ErrorContextf(ctx, "GetLastOriginOrgDataIDByLastOrgDataID|orgData is null|lastID:%s", relateID)
				return "", errs.ErrDocSegmentNotFound
			}
			return "", err
		}
		if orgData == nil {
			log.ErrorContextf(ctx, "GetLastOriginOrgDataIDByLastOrgDataID|orgData is null|lastID:%s", relateID)
			return "", errs.ErrDocSegmentNotFound
		}
		return orgData.OriginOrgDataID, nil
	} else if strings.HasPrefix(relateID, model.InsertPrefix) {
		// 查找该插入数据对应的原始数据&&更新该插入数据
		orgData, err := dao.GetDocSegmentOrgDataTemporaryDao().GetDocOrgDataByBizID(ctx,
			dao.DocSegmentOrgDataTemporaryTblColList, corpBizID, appBizID, docBizID, relateID)
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				log.ErrorContextf(ctx, "GetLastOriginOrgDataIDByLastOrgDataID|orgData is null|relateID:%s", relateID)
				return "", errs.ErrDocSegmentNotFound
			}
			return "", err
		}
		if orgData == nil {
			log.ErrorContextf(ctx, "GetLastOriginOrgDataIDByLastOrgDataID|orgData is null|relateID:%s", relateID)
			return "", errs.ErrDocSegmentNotFound
		}
		return orgData.LastOriginOrgDataID, nil
	}
	return "", nil
}

func CreateNewDocFromOldDoc(ctx context.Context, d dao.Dao, doc *model.Doc,
	auditFlag uint32, newDocBizID uint64) (*model.Doc, error) {
	newDoc := &model.Doc{
		BusinessID:          newDocBizID,
		RobotID:             doc.RobotID,
		CorpID:              doc.CorpID,
		StaffID:             doc.StaffID,
		FileName:            doc.FileName,
		FileType:            doc.FileType,
		FileSize:            doc.FileSize,
		CosURL:              doc.CosURL,
		Bucket:              doc.Bucket,
		CosHash:             doc.CosHash,
		Status:              model.DocStatusParseIng,
		IsDeleted:           model.DocIsNotDeleted,
		Source:              doc.Source,
		WebURL:              doc.WebURL,
		AuditFlag:           auditFlag,
		CharSize:            doc.CharSize,
		NextAction:          model.DocNextActionAdd,
		IsRefer:             doc.IsRefer,
		AttrRange:           doc.AttrRange,
		ReferURLType:        doc.ReferURLType,
		ExpireStart:         doc.ExpireStart,
		ExpireEnd:           doc.ExpireEnd,
		Opt:                 doc.Opt,
		CategoryID:          doc.CategoryID,
		OriginalURL:         doc.OriginalURL,
		CustomerKnowledgeId: doc.CustomerKnowledgeId,
		AttributeFlag:       doc.AttributeFlag,
	}
	return newDoc, nil
}

func GetDocSegmentList(ctx context.Context, req *pb.ListDocSegmentReq,
	docCommon *model.DocSegmentCommon) ([]*pb.ListDocSegmentRsp_DocSegmentItem, error) {
	// 不含关键词，直接查询数据库
	docSegmentFilter, err := CheckAndConvertFilters(ctx, req.GetFilters())
	if err != nil {
		log.ErrorContextf(ctx, "GetDocSegmentList|CheckAndConvertFilters|err:%+v", err)
		return nil, err
	}
	if len(docSegmentFilter.AuditStatusFilter) != 0 {
		log.InfoContextf(ctx, "GetDocSegmentList|AuditStatusFilter")
		// 如果存在审核状态的查询则仅查找临时表数据
		deletedFlag := dao.IsNotDeleted
		filter := &dao.DocSegmentOrgDataTemporaryFilter{
			CorpBizID:   docCommon.CorpBizID,
			AppBizID:    docCommon.AppBizID,
			DocBizID:    docCommon.DocBizID,
			IsDeleted:   &deletedFlag,
			Keywords:    req.Keywords,
			AuditStatus: docSegmentFilter.AuditStatusFilter,
			Offset:      common.GetOffsetByPage(req.PageNumber, req.PageSize),
			Limit:       req.PageSize,
			SheetName:   docCommon.SheetName,
		}
		list, err := dao.GetDocSegmentOrgDataTemporaryDao().GetDocOrgDataByDocBizID(ctx,
			dao.DocSegmentOrgDataTemporaryTblColList, filter)
		if err != nil {
			log.ErrorContextf(ctx, "GetDocSegmentList|GetDocOrgDataByDocBizID|err:%+v", err)
			return nil, err
		}
		tempList, err := TempOriginListToDocSegment(ctx, list)
		if err != nil {
			log.ErrorContextf(ctx, "GetDocSegmentList|TempOriginListToDocSegment|err:%+v", err)
			return nil, err
		}
		return tempList, nil
	}
	// 1.获取原始切片
	offset := common.GetOffsetByPage(req.PageNumber, req.PageSize)
	limit := req.PageSize
	originList, orgDateBizIDs, err := GetDocSegmentOrgData(ctx, req, docCommon, offset, limit)
	if err != nil {
		log.ErrorContextf(ctx, "GetDocSegmentOrgData failed, err:%+v", err)
		return nil, errs.ErrSystem
	}
	// 2.编辑切片内容替换
	editOriginList, err := GetEditOrgData(ctx, orgDateBizIDs, docCommon)
	if err != nil {
		log.ErrorContextf(ctx, "GetEditOrgData failed, err:%+v", err)
		return nil, errs.ErrSystem
	}
	for _, edit := range editOriginList {
		for _, item := range originList {
			if item.SegBizId == edit.OriginOrgDataID {
				item.OrgData = edit.OrgData
				item.SegBizId = edit.BusinessID
				item.IsOrigin = false
				item.AuditStatus = uint64(edit.AuditStatus)
			}
		}
	}
	// 3.新增切片添加
	// 兼容原始切片都删除，只留新增切片的场景
	if req.PageNumber == 1 {
		orgDateBizIDs = append(orgDateBizIDs, model.InsertAtFirst)
	}
	insertOriginList, err := GetInsertOrgData(ctx, orgDateBizIDs, docCommon)
	if err != nil {
		log.ErrorContextf(ctx, "GetInsertOrgData failed, err:%+v", err)
		return nil, errs.ErrSystem
	}

	originList, err = InsertIntoOrgDataList(ctx, insertOriginList, originList)
	if err != nil {
		log.ErrorContextf(ctx, "InsertIntoOrgDataList failed, err:%+v", err)
		return nil, errs.ErrSystem
	}
	log.InfoContextf(ctx, "GetDocSegmentList|len(originList):%d", len(originList))
	return originList, nil
}

func TempOriginListToDocSegment(ctx context.Context, list []*model.DocSegmentOrgDataTemporary) (
	[]*pb.ListDocSegmentRsp_DocSegmentItem, error) {
	docSegmentList := make([]*pb.ListDocSegmentRsp_DocSegmentItem, 0)
	for _, orgDate := range list {
		pageInfos, pageData := make([]uint64, 0), make([]int64, 0)
		if orgDate.OrgPageNumbers != "" {
			if err := jsoniter.UnmarshalFromString(orgDate.OrgPageNumbers, &pageData); err != nil {
				log.WarnContextf(ctx, "TempOriginListToDocSegment|PageInfos|UnmarshalFromString|err:%+v", err)
			}
			for _, page := range pageData {
				pageInfos = append(pageInfos, uint64(page))
			}
		}
		docSegmentItem := &pb.ListDocSegmentRsp_DocSegmentItem{
			SegBizId:    orgDate.BusinessID,
			OrgData:     orgDate.OrgData,
			PageInfos:   pageInfos,
			IsOrigin:    false,
			IsAdd:       orgDate.Action == dao.InsertAction,
			SegmentType: orgDate.SegmentType,
			IsDisabled:  orgDate.IsDisabled == model.SegmentIsDisabled,
			AuditStatus: uint64(orgDate.AuditStatus),
			SheetName:   orgDate.SheetName,
		}
		docSegmentList = append(docSegmentList, docSegmentItem)
	}
	return docSegmentList, nil
}

func OriginListToDocSegment(ctx context.Context, list []*model.DocSegmentOrgData) (
	[]*pb.ListDocSegmentRsp_DocSegmentItem, error) {
	docSegmentList := make([]*pb.ListDocSegmentRsp_DocSegmentItem, 0)
	for _, orgDate := range list {
		pageInfos, pageData := make([]uint64, 0), make([]int64, 0)
		if orgDate.OrgPageNumbers != "" {
			if err := jsoniter.UnmarshalFromString(orgDate.OrgPageNumbers, &pageData); err != nil {
				log.WarnContextf(ctx, "OriginListToDocSegment|PageInfos|UnmarshalFromString|err:%+v", err)
			}
			for _, page := range pageData {
				pageInfos = append(pageInfos, uint64(page))
			}
		}
		docSegmentItem := &pb.ListDocSegmentRsp_DocSegmentItem{
			SegBizId:    strconv.FormatUint(orgDate.BusinessID, 10),
			OrgData:     orgDate.OrgData,
			PageInfos:   pageInfos,
			IsOrigin:    true,
			IsAdd:       false,
			SegmentType: orgDate.SegmentType,
			IsDisabled:  orgDate.IsDisabled == model.SegmentIsDisabled,
		}
		docSegmentList = append(docSegmentList, docSegmentItem)
	}
	return docSegmentList, nil
}

func ModifyDocSegmentByOperate(ctx context.Context, docCommon *model.DocSegmentCommon,
	bizIDs []string, operate int, tx *gorm.DB) error {
	log.InfoContextf(ctx, "ModifyDocSegmentByOperate|Operate:%d", operate)
	for _, segBizID := range bizIDs {
		if strings.HasPrefix(segBizID, model.EditPrefix) {
			// 校验切片是否存在
			orgData, err := dao.GetDocSegmentOrgDataTemporaryDao().GetDocOrgDataByBizID(ctx,
				dao.DocSegmentOrgDataTemporaryTblColList, docCommon.CorpBizID, docCommon.AppBizID,
				docCommon.DocBizID, segBizID)
			if err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					log.ErrorContextf(ctx, "ModifyDocSegmentByOperate|orgData is null")
					return errs.ErrDocSegmentNotFound
				}
				return err
			}
			if orgData == nil {
				log.ErrorContextf(ctx, "ModifyDocSegmentByOperate|orgData is null")
				return errs.ErrDocSegmentNotFound
			}
			// 获取关联的原始数据
			originOrgDataID, err := util.CheckReqParamsIsUint64(ctx, orgData.OriginOrgDataID)
			if err != nil {
				log.ErrorContextf(ctx, "ModifyDocSegmentByOperate|DocBizIDToUint64|err:%+v", err)
				return err
			}
			switch operate {
			case ModifyDocSegmentDeleteOperate:
				// 查询是否有关联这个切片的
				relateOrgData, err := dao.GetDocSegmentOrgDataTemporaryDao().GetDocOrgDataByLastOrgDataID(ctx,
					dao.DocSegmentOrgDataTemporaryTblColList, docCommon.CorpBizID, docCommon.AppBizID,
					docCommon.DocBizID, segBizID)
				if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
					return err
				}
				relateLastOriginOrgDataID := ""
				if relateOrgData != nil {
					var relateLastOrgDataID string
					relateLastOrgDataID, relateLastOriginOrgDataID, err = RelateOrgDataProcess(
						ctx, docCommon, originOrgDataID)
					if err != nil {
						return err
					}
					// 更新关联切片使用的last_org_data_id、last_origin_org_data_id
					updateColumns := []string{
						dao.DocSegmentOrgDataTemporaryTblColLastOrgDataID,
						dao.DocSegmentOrgDataTemporaryTblColLastOriginOrgDataID,
						dao.DocSegmentOrgDataTemporaryTblColUpdateTime,
					}
					update := &model.DocSegmentOrgDataTemporary{
						LastOriginOrgDataID: relateLastOriginOrgDataID,
						LastOrgDataID:       relateLastOrgDataID,
						UpdateTime:          time.Now(),
					}
					filter := &dao.DocSegmentOrgDataTemporaryFilter{
						CorpBizID:   docCommon.CorpBizID,
						AppBizID:    docCommon.AppBizID,
						DocBizID:    docCommon.DocBizID,
						BusinessIDs: []string{relateOrgData.BusinessID},
					}
					_, err := dao.GetDocSegmentOrgDataTemporaryDao().UpdateDocSegmentOrgData(ctx, tx,
						updateColumns, filter, update)
					if err != nil {
						return err
					}
				}
				// 查询是否有关联这个切片原始切片的
				relateOriginOrgDataList, err := GetInsertOrgData(ctx, []string{orgData.OriginOrgDataID}, docCommon)
				if err != nil {
					log.ErrorContextf(ctx, "GetInsertOrgData failed, err:%+v", err)
					return errs.ErrSystem
				}
				for _, relateOriginOrgData := range relateOriginOrgDataList {
					updateColumns := []string{
						dao.DocSegmentOrgDataTemporaryTblColLastOriginOrgDataID,
						dao.DocSegmentOrgDataTemporaryTblColUpdateTime,
					}
					update := &model.DocSegmentOrgDataTemporary{
						LastOriginOrgDataID: relateLastOriginOrgDataID,
						UpdateTime:          time.Now(),
					}
					filter := &dao.DocSegmentOrgDataTemporaryFilter{
						CorpBizID:   docCommon.CorpBizID,
						AppBizID:    docCommon.AppBizID,
						DocBizID:    docCommon.DocBizID,
						BusinessIDs: []string{relateOriginOrgData.BusinessID},
					}
					_, err := dao.GetDocSegmentOrgDataTemporaryDao().UpdateDocSegmentOrgData(ctx, tx,
						updateColumns, filter, update)
					if err != nil {
						return err
					}
				}
				err = dao.GetDocSegmentOrgDataTemporaryDao().DeleteDocSegmentOrgData(ctx, tx, docCommon.CorpBizID,
					docCommon.AppBizID, docCommon.DocBizID, []string{segBizID})
				if err != nil {
					return err
				}
				err = dao.GetDocSegmentOrgDataDao().TemporaryDeleteDocSegmentOrgData(ctx, tx, docCommon.CorpBizID,
					docCommon.AppBizID, docCommon.DocBizID, []uint64{originOrgDataID})
				if err != nil {
					return err
				}
			case ModifyDocSegmentDisabledOperate:
				err = dao.GetDocSegmentOrgDataTemporaryDao().DisabledDocSegmentOrgData(ctx, tx, docCommon.CorpBizID,
					docCommon.AppBizID, docCommon.DocBizID, []string{segBizID})
				if err != nil {
					return err
				}
				err = dao.GetDocSegmentOrgDataDao().DisabledDocSegmentOrgData(ctx, tx, docCommon.CorpBizID,
					docCommon.AppBizID, docCommon.DocBizID, []uint64{originOrgDataID})
				if err != nil {
					return err
				}
			case ModifyDocSegmentEnableOperate:
				err = dao.GetDocSegmentOrgDataTemporaryDao().EnableDocSegmentOrgData(ctx, tx, docCommon.CorpBizID,
					docCommon.AppBizID, docCommon.DocBizID, []string{segBizID})
				if err != nil {
					return err
				}
				err = dao.GetDocSegmentOrgDataDao().EnableDocSegmentOrgData(ctx, tx, docCommon.CorpBizID,
					docCommon.AppBizID, docCommon.DocBizID, []uint64{originOrgDataID})
				if err != nil {
					return err
				}
			default:
				log.ErrorContextf(ctx, "ModifyDocSegmentByOperate|no such operate|Operate:%d", operate)
				return errs.ErrSystem
			}
		} else if strings.HasPrefix(segBizID, model.InsertPrefix) {
			// 校验切片是否存在
			orgData, err := dao.GetDocSegmentOrgDataTemporaryDao().GetDocOrgDataByBizID(ctx,
				dao.DocSegmentOrgDataTemporaryTblColList, docCommon.CorpBizID, docCommon.AppBizID,
				docCommon.DocBizID, segBizID)
			if err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					log.ErrorContextf(ctx, "ModifyDocSegmentByOperate|orgData is null")
					return errs.ErrDocSegmentNotFound
				}
				return err
			}
			if orgData == nil {
				log.ErrorContextf(ctx, "ModifyDocSegmentByOperate|orgData is null")
				return errs.ErrDocSegmentNotFound
			}
			switch operate {
			case ModifyDocSegmentDeleteOperate:
				// 查询是否有关联这个切片的
				relateOrgData, err := dao.GetDocSegmentOrgDataTemporaryDao().GetDocOrgDataByLastOrgDataID(ctx,
					dao.DocSegmentOrgDataTemporaryTblColList, docCommon.CorpBizID, docCommon.AppBizID,
					docCommon.DocBizID, segBizID)
				if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
					return err
				}
				if relateOrgData != nil {
					updateColumns := []string{
						dao.DocSegmentOrgDataTemporaryTblColLastOrgDataID,
						dao.DocSegmentOrgDataTemporaryTblColLastOriginOrgDataID,
						dao.DocSegmentOrgDataTemporaryTblColUpdateTime,
					}
					update := &model.DocSegmentOrgDataTemporary{
						LastOriginOrgDataID: orgData.LastOriginOrgDataID,
						LastOrgDataID:       orgData.LastOrgDataID,
						UpdateTime:          time.Now(),
					}
					filter := &dao.DocSegmentOrgDataTemporaryFilter{
						CorpBizID:   docCommon.CorpBizID,
						AppBizID:    docCommon.AppBizID,
						DocBizID:    docCommon.DocBizID,
						BusinessIDs: []string{relateOrgData.BusinessID},
					}
					_, err := dao.GetDocSegmentOrgDataTemporaryDao().UpdateDocSegmentOrgData(ctx, tx,
						updateColumns, filter, update)
					if err != nil {
						return err
					}
				}
				// 删除期望删除的切片
				err = dao.GetDocSegmentOrgDataTemporaryDao().DeleteDocSegmentOrgData(ctx, tx, docCommon.CorpBizID,
					docCommon.AppBizID, docCommon.DocBizID, []string{segBizID})
				if err != nil {
					return err
				}
			case ModifyDocSegmentDisabledOperate:
				err := dao.GetDocSegmentOrgDataTemporaryDao().DisabledDocSegmentOrgData(ctx, tx, docCommon.CorpBizID,
					docCommon.AppBizID, docCommon.DocBizID, []string{segBizID})
				if err != nil {
					return err
				}
			case ModifyDocSegmentEnableOperate:
				err := dao.GetDocSegmentOrgDataTemporaryDao().EnableDocSegmentOrgData(ctx, tx, docCommon.CorpBizID,
					docCommon.AppBizID, docCommon.DocBizID, []string{segBizID})
				if err != nil {
					return err
				}
			default:
				log.ErrorContextf(ctx, "ModifyDocSegmentByOperate|no such operate|Operate:%d", operate)
				return errs.ErrSystem
			}
		} else {
			originOrgDataID, err := util.CheckReqParamsIsUint64(ctx, segBizID)
			if err != nil {
				log.ErrorContextf(ctx, "ModifyDocSegmentByOperate|DocBizIDToUint64|err:%+v", err)
				return err
			}
			// 校验切片是否存在
			orgData, err := dao.GetDocSegmentOrgDataDao().GetDocOrgDataByBizID(ctx,
				dao.DocSegmentOrgDataTblColList, docCommon.CorpBizID, docCommon.AppBizID,
				docCommon.DocBizID, originOrgDataID)
			if err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					log.ErrorContextf(ctx, "ModifyDocSegmentByOperate|orgData is null")
					return errs.ErrDocSegmentNotFound
				}
				return err
			}
			if orgData == nil {
				log.ErrorContextf(ctx, "ModifyDocSegmentByOperate|orgData is null")
				return errs.ErrDocSegmentNotFound
			}
			// 如果切片被编辑过，则阻止操作
			tempOrgData, err := dao.GetDocSegmentOrgDataTemporaryDao().GetDocOrgDataByOriginOrgDataID(ctx,
				dao.DocSegmentOrgDataTemporaryTblColList, docCommon.CorpBizID, docCommon.AppBizID,
				docCommon.DocBizID, segBizID)
			if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
				return err
			}
			if tempOrgData != nil {
				log.ErrorContextf(ctx, "ModifyDocSegmentByOperate|orgData is edit, operation not allowed")
				return errs.ErrDocSegmentOperationNotAllowedFailed
			}
			switch operate {
			case ModifyDocSegmentDeleteOperate:
				// 查询是否有关联这个切片的
				relateOrgData, err := dao.GetDocSegmentOrgDataTemporaryDao().GetDocOrgDataByLastOrgDataID(ctx,
					dao.DocSegmentOrgDataTemporaryTblColList, docCommon.CorpBizID, docCommon.AppBizID,
					docCommon.DocBizID, segBizID)
				if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
					return err
				}
				relateLastOriginOrgDataID := ""
				if relateOrgData != nil {
					var relateLastOrgDataID string
					relateLastOrgDataID, relateLastOriginOrgDataID, err = RelateOrgDataProcess(
						ctx, docCommon, originOrgDataID)
					if err != nil {
						return err
					}
					updateColumns := []string{
						dao.DocSegmentOrgDataTemporaryTblColLastOrgDataID,
						dao.DocSegmentOrgDataTemporaryTblColLastOriginOrgDataID,
						dao.DocSegmentOrgDataTemporaryTblColUpdateTime,
					}
					update := &model.DocSegmentOrgDataTemporary{
						LastOriginOrgDataID: relateLastOriginOrgDataID,
						LastOrgDataID:       relateLastOrgDataID,
						UpdateTime:          time.Now(),
					}
					filter := &dao.DocSegmentOrgDataTemporaryFilter{
						CorpBizID:   docCommon.CorpBizID,
						AppBizID:    docCommon.AppBizID,
						DocBizID:    docCommon.DocBizID,
						BusinessIDs: []string{relateOrgData.BusinessID},
					}
					_, err := dao.GetDocSegmentOrgDataTemporaryDao().UpdateDocSegmentOrgData(ctx, tx,
						updateColumns, filter, update)
					if err != nil {
						return err
					}
				}
				// 查询是否有关联原始新增切片的，更新关联的原始切片
				relateOriginOrgDataList, err := GetInsertOrgData(ctx, []string{segBizID}, docCommon)
				if err != nil {
					log.ErrorContextf(ctx, "GetInsertOrgData failed, err:%+v", err)
					return errs.ErrSystem
				}
				for _, relateOriginOrgData := range relateOriginOrgDataList {
					updateColumns := []string{
						dao.DocSegmentOrgDataTemporaryTblColLastOriginOrgDataID,
						dao.DocSegmentOrgDataTemporaryTblColUpdateTime,
					}
					update := &model.DocSegmentOrgDataTemporary{
						LastOriginOrgDataID: relateLastOriginOrgDataID,
						UpdateTime:          time.Now(),
					}
					filter := &dao.DocSegmentOrgDataTemporaryFilter{
						CorpBizID:   docCommon.CorpBizID,
						AppBizID:    docCommon.AppBizID,
						DocBizID:    docCommon.DocBizID,
						BusinessIDs: []string{relateOriginOrgData.BusinessID},
					}
					_, err := dao.GetDocSegmentOrgDataTemporaryDao().UpdateDocSegmentOrgData(ctx, tx,
						updateColumns, filter, update)
					if err != nil {
						return err
					}
				}
				err = dao.GetDocSegmentOrgDataDao().TemporaryDeleteDocSegmentOrgData(ctx, tx, docCommon.CorpBizID,
					docCommon.AppBizID, docCommon.DocBizID, []uint64{originOrgDataID})
				if err != nil {
					return err
				}
			case ModifyDocSegmentDisabledOperate:
				err = dao.GetDocSegmentOrgDataDao().DisabledDocSegmentOrgData(ctx, tx, docCommon.CorpBizID,
					docCommon.AppBizID, docCommon.DocBizID, []uint64{originOrgDataID})
				if err != nil {
					return err
				}
			case ModifyDocSegmentEnableOperate:
				err = dao.GetDocSegmentOrgDataDao().EnableDocSegmentOrgData(ctx, tx, docCommon.CorpBizID,
					docCommon.AppBizID, docCommon.DocBizID, []uint64{originOrgDataID})
				if err != nil {
					return err
				}
			default:
				log.ErrorContextf(ctx, "ModifyDocSegmentByOperate|no such operate|Operate:%d", operate)
				return errs.ErrSystem
			}
		}
	}
	return nil
}

func GetSegmentByReferID(ctx context.Context, d dao.Dao, appID uint64, referBizID string) (*model.DocSegmentExtend, error) {
	referBizIDInt, err := util.CheckReqParamsIsUint64(ctx, referBizID)
	if err != nil {
		return nil, err
	}
	refer, err := d.GetRefersByBusinessID(ctx, referBizIDInt)
	if err != nil {
		log.ErrorContextf(ctx, "ListDocSegment|GetRefersByBusinessIDs err|err:%+v", err)
		return nil, errs.ErrGetReferFail
	}
	if refer == nil {
		return nil, errs.ErrGetReferFail
	}
	segment, err := d.GetSegmentByID(ctx, refer.RelateID, appID)
	if err != nil {
		log.ErrorContextf(ctx, "ListDocSegment|GetSegmentByID err|err:%+v", err)
		return nil, errs.ErrDocSegmentNotFound
	}
	if segment == nil {
		return nil, errs.ErrDocSegmentNotFound
	}
	return segment, nil
}

func RelateOrgDataProcess(ctx context.Context, docCommon *model.DocSegmentCommon, originOrgDataID uint64) (
	string, string, error) {
	lastChainStartOrgData := ""
	relateLastOriginOrgDataID := ""
	relateLastOrgDataID := ""
	// 有查询到关联切片，需要更新关联切片数据
	// 查找原始数据的上一个切片
	lastOrgData, err := dao.GetDocSegmentOrgDataDao().GetLastOrgDataByCurrentOrgDataBizID(ctx,
		dao.DocSegmentOrgDataTblColList, docCommon.CorpBizID,
		docCommon.AppBizID, docCommon.DocBizID, originOrgDataID)
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return "", "", err
	}
	if lastOrgData == nil {
		// 上一个切片不存在，关联切片的前原始切片为first，前一个链的开头为first
		lastChainStartOrgData = model.InsertAtFirst
		relateLastOriginOrgDataID = model.InsertAtFirst
	} else {
		// 上一个切片存在，关联切片的前原始切片为前一切片
		lastTempOrgData, err := dao.GetDocSegmentOrgDataTemporaryDao().GetDocOrgDataByOriginOrgDataID(ctx,
			dao.DocSegmentOrgDataTemporaryTblColList, docCommon.CorpBizID, docCommon.AppBizID,
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
		firstOrgData, err := dao.GetDocSegmentOrgDataTemporaryDao().GetDocOrgDataByLastOrgDataID(ctx,
			dao.DocSegmentOrgDataTemporaryTblColList, docCommon.CorpBizID,
			docCommon.AppBizID, docCommon.DocBizID, lastChainStartOrgData)
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return "", "", err
		}
		if firstOrgData == nil {
			// 前一个链的起始位置之前未被使用，直接使用
			relateLastOrgDataID = lastChainStartOrgData
		} else {
			// 找到上一个链的末尾位置插入
			lastChainOrgData, err := GetInsertOrgData(ctx, []string{relateLastOriginOrgDataID}, docCommon)
			if err != nil {
				return "", "", err
			}
			// 构建新增数据映射（key: last_org_data_id, value: 切片）
			insertMap := make(map[string]*model.DocSegmentOrgDataTemporary)
			for _, insertSeg := range lastChainOrgData {
				insertMap[insertSeg.LastOrgDataID] = insertSeg
			}
			// 按非新增数据分组存储插入数据
			chain := []*model.DocSegmentOrgDataTemporary{firstOrgData}

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

func UpdateOldDocStatus(ctx context.Context, auditFlag uint32, doc *model.Doc) error {
	// 重置文档的状态
	updateDocFilter := &dao.DocFilter{
		IDs:     []uint64{doc.ID},
		CorpId:  doc.CorpID,
		RobotId: doc.RobotID,
	}
	update := &model.Doc{
		StaffID:    pkg.StaffID(ctx),
		Status:     model.DocStatusParseIng,
		Message:    "",
		AuditFlag:  auditFlag,
		BatchID:    0,
		NextAction: model.DocNextActionAdd,
		CharSize:   0,
		UpdateTime: time.Now(),
	}
	update.AddProcessingFlag([]uint64{model.DocProcessingFlagSegmentIntervene})
	updateDocColumns := []string{
		dao.DocTblColStaffId,
		dao.DocTblColStatus,
		dao.DocTblColMessage,
		dao.DocTblColAuditFlag,
		dao.DocTblColBatchId,
		dao.DocTblColNextAction,
		dao.DocTblColCharSize,
		dao.DocTblColUpdateTime,
		dao.DocTblColProcessingFlag}
	_, err := dao.GetDocDao().UpdateDoc(ctx, updateDocColumns, updateDocFilter, update)
	if err != nil {
		log.ErrorContextf(ctx, "CreateDocParsingIntervention|UpdateDoc|err:%+v", err)
		return err
	}
	return nil
}

func CheckAndConvertFilters(ctx context.Context, filters []*pb.FilterItem) (*model.DocSegmentFilter, error) {
	docSegmentFilter := new(model.DocSegmentFilter)
	// 校验筛选条件内容
	if len(filters) > 0 {
		for _, filter := range filters {
			if key, ok := model.DocSegmentFilterKeyMap[filter.FilterKey]; ok {
				switch key {
				case model.DocSegmentFilterKeyAuditStatus:
					for _, value := range filter.FilterValue {
						if v, ok := model.DocSegmentFilterAuditStatusMap[value]; ok {
							docSegmentFilter.AuditStatusFilter = append(docSegmentFilter.AuditStatusFilter, uint32(v))
						} else {
							// 过滤值未找到
							log.ErrorContextf(ctx, "ListDocSegment|FilterValue not found|FilterKey:%s|FilterValue:%s",
								filter.FilterKey, value)
							return docSegmentFilter, errs.ErrDocSegmentFilterInvalid
						}
					}
				}
			} else {
				// 过滤条件未找到
				log.ErrorContextf(ctx, "ListDocSegment|FilterKey not found|FilterKey:%s", filter.FilterKey)
				return docSegmentFilter, errs.ErrDocSegmentFilterInvalid
			}
		}
	}
	return docSegmentFilter, nil
}

func RecoverSegmentsForIndex(ctx context.Context, corpBizID, appBizID, docBizID uint64) error {
	deleteFlag := dao.IsNotDeleted
	deleteFilter := &dao.DocSegmentOrgDataFilter{
		CorpBizID:      corpBizID,
		AppBizID:       appBizID,
		DocBizID:       docBizID,
		IsDeleted:      &deleteFlag,
		RouterAppBizID: appBizID,
	}
	err := dao.GetDocSegmentOrgDataDao().RealityBatchDeleteDocOrgData(ctx, nil, deleteFilter, 10000)
	if err != nil {
		log.ErrorContextf(ctx, "RecoverSegmentsForIndex|BatchDeleteDocOrgData failed, err:%+v", err)
		return err
	}
	err = dao.GetDocSegmentOrgDataDao().BatchRecoverDocOrgDataByDocBizID(ctx, nil, corpBizID,
		appBizID, docBizID, 10000)
	if err != nil {
		log.ErrorContextf(ctx, "RecoverSegmentsForIndex|BatchDeleteTempDocOrgData failed, err:%+v", err)
		return err
	}
	return nil
}

func CleanSegmentsForIndex(ctx context.Context, corpBizID, appBizID, docBizID uint64) error {
	// 删除OrgData
	deleteFlag := dao.IsDeleted
	deleteFilter := &dao.DocSegmentOrgDataFilter{
		CorpBizID:      corpBizID,
		AppBizID:       appBizID,
		DocBizID:       docBizID,
		IsDeleted:      &deleteFlag,
		RouterAppBizID: appBizID,
	}
	err := dao.GetDocSegmentOrgDataDao().RealityBatchDeleteDocOrgData(ctx, nil, deleteFilter, 10000)
	if err != nil {
		log.ErrorContextf(ctx, "createSegment|RealityBatchDeleteDocOrgData failed, err:%+v", err)
		return err
	}
	// 删除临时OrgData
	deleteTempFilter := &dao.DocSegmentOrgDataTemporaryFilter{
		CorpBizID: corpBizID,
		AppBizID:  appBizID,
		DocBizID:  docBizID,
	}
	err = dao.GetDocSegmentOrgDataTemporaryDao().RealityBatchDeleteDocOrgData(ctx,
		nil, deleteTempFilter, appBizID, 10000)
	if err != nil {
		log.ErrorContextf(ctx, "createSegment|RealityBatchDeleteDocOrgData failed, err:%+v", err)
		return err
	}
	return nil
}

// RemoveTableHeader 从切片中移除表格的表头
func RemoveTableHeader(ctx context.Context,
	docSegments []*pb.ListDocSegmentRsp_DocSegmentItem) []*pb.ListDocSegmentRsp_DocSegmentItem {
	if docSegments == nil || len(docSegments) == 0 {
		return docSegments
	}
	for i := range docSegments {
		docSegments[i].OrgData = GetSliceTable(docSegments[i].OrgData, 1)
		log.DebugContextf(ctx, "RemoveTableHeader|OrgData:%s", docSegments[i].OrgData)
	}
	return docSegments
}

// GetSliceTable 获取切片表格数据
func GetSliceTable(orgData string, tag int) string {
	startLine := 0
	lines := strings.Split(orgData, "\n")
	for i := range lines {
		// 判断是否为markdown
		if i > 0 {
			tableLine := util.IsTableLine(lines[i-1])
			separatorLine := util.IsSeparatorLine(lines[i])
			if tableLine && separatorLine {
				startLine = i - 1
				break
			}
		}
		// 判断是否为html
		if strings.Contains(lines[i], TableMarkerHTML) {
			startLine = i
			break
		}
	}
	if tag == 1 {
		lines = lines[startLine:]
	} else {
		lines = lines[:startLine]
	}
	return strings.Join(lines, "\n")
}
