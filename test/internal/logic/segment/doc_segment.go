package segment

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/model"
	"git.woa.com/adp/kb/kb-config/internal/util/markdown"

	"git.code.oa.com/trpc-go/trpc-go/client"
	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/contextx/clues"
	"git.woa.com/adp/common/x/encodingx/jsonx"
	"git.woa.com/adp/common/x/errx"
	"git.woa.com/adp/common/x/gox/ptrx"
	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/common/x/utilx"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/mysqlquery"
	dbdao "git.woa.com/adp/kb/kb-config/internal/dao/database"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	dbentity "git.woa.com/adp/kb/kb-config/internal/entity/database"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
	segEntity "git.woa.com/adp/kb/kb-config/internal/entity/segment"
	knowClient "git.woa.com/adp/kb/kb-config/internal/rpc"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"git.woa.com/adp/kb/kb-config/internal/util/idgen"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	kb_pb "git.woa.com/adp/pb-go/kb/kb_config"
	retrieval_pb "git.woa.com/adp/pb-go/kb/kb_retrieval"
	"gorm.io/gorm"
)

const (
	ModifyDocSegmentDeleteOperate   = 0
	ModifyDocSegmentDisabledOperate = 1
	ModifyDocSegmentEnableOperate   = 2
)

const (
	TableMarkerMarkdown = "| --- |"
	TableMarkerHTML     = "<table>"
)

// CreateSegment 创建文档分段
func (l *Logic) CreateSegment(ctx context.Context, segments []*segEntity.DocSegmentExtend, robotID uint64) error {
	pageSize := 500
	// 创建哈希表存储唯一字符串和BigData的ID
	uniqueBigData := make(map[string]string)
	docSegTableName := l.segDao.Query().TDocSegment.TableName()
	db, err := knowClient.GormClient(ctx, docSegTableName, robotID, 0, []client.Option{}...)
	if err != nil {
		return err
	}
	if err = db.Transaction(func(tx *gorm.DB) error {
		for index, tmpSegments := range slicex.Chunk(segments, pageSize) {
			logx.I(ctx, "CreateSegment|tmpSegments.len:%d|index:%d", len(tmpSegments), index)

			var bigData []*retrieval_pb.BigData

			for _, tmpSegment := range tmpSegments {
				// 生成业务ID
				tmpSegment.BusinessID = idgen.GetId()

				// 如果是文档切片，那需要把 BigData存入ES
				if !tmpSegment.IsSegmentForIndex() {
					continue
				}

				if len(tmpSegment.BigString) == 0 {
					continue
				}

				hash := sha256.New()
				_, _ = io.WriteString(hash, tmpSegment.BigString)
				hashValue := hash.Sum(nil)
				// 2024-04-14:
				// 	为了确保 模型解出来的结果有不同的拆分策略（规则拆分、模型拆分），所以加上rich_text_index
				//	uniqueKey := strconv.Itoa(tmpSegment.RichTextIndex) + string(hashValue)
				// 2024-04-15: mobisysfeng, tangyuanlin, harryhlli 沟通后决定，只用 BigString 即可；
				uniqueKey := string(hashValue)
				// 长文本通过hash，只把不重复的存入ES
				if id, ok := uniqueBigData[uniqueKey]; ok {
					tmpSegment.BigDataID = id
				} else {
					tmpSegment.BigDataID = strconv.FormatUint(idgen.GetId(), 10) // 生成ES的ID
					uniqueBigData[uniqueKey] = tmpSegment.BigDataID
					bigData = append(bigData, &retrieval_pb.BigData{
						RobotId:   tmpSegment.RobotID,
						DocId:     tmpSegment.DocID,
						BigDataId: tmpSegment.BigDataID,
						BigStart:  tmpSegment.BigStart,
						BigEnd:    tmpSegment.BigEnd,
						BigString: tmpSegment.BigString,
					})
				}

			}

			tmpSehmentInDBs := slicex.Map(tmpSegments, func(seg *segEntity.DocSegmentExtend) *segEntity.DocSegment {
				return &seg.DocSegment
			})
			logx.I(ctx, "CreateSegment|tmpSehmentInDBs.len:%d", len(tmpSehmentInDBs))
			if err := l.segDao.CreateDocSegments(ctx, tmpSehmentInDBs, tx); err != nil {
				logx.E(ctx, "CreateSegment error: %v", err)
				return err
			}

			req := retrieval_pb.AddBigDataElasticReq{Data: bigData, Type: retrieval_pb.KnowledgeType_KNOWLEDGE}
			logx.I(ctx, "CreateSegment|AddBigDataElastic|req:%+v", req)
			if err := l.rpc.RetrievalDirectIndex.AddBigDataElastic(ctx, &req); err != nil {
				logx.E(ctx, "CreateSegment|AddBigDataElastic|seg:%+v|err:%+v", tmpSegments, err)
				return err
			}
		}
		// total := len(segments)
		// pages := int(math.Ceil(float64(total) / float64(pageSize)))
		// slicex.Chunk(segments, pageSize)
		// // querySQL := fmt.Sprintf(createSegment, docSegmentFields)
		// for i := 0; i < pages; i++ {
		//	start := pageSize * i
		//	end := pageSize * (i + 1)
		//	if end > total {
		//		end = total
		//	}
		//	tmpSegments := segments[start:end]
		//	logx.I(ctx, "CreateSegment|tmpSegments.len:%d|start:%d|end:%d", len(tmpSegments), start, end)
		//	tmpSehmentInDBs := make([]*segEntity.DocSegment, 0, len(tmpSegments))
		//
		//	var bigData []*retrieval_pb.BigData
		//
		//	for _, tmpSegment := range tmpSegments {
		//		// 生成业务ID
		//		tmpSegment.BusinessID = idgen.GetId()
		//
		//		// 如果是文档切片，那需要把 BigData存入ES
		//		if !tmpSegment.IsSegmentForIndex() {
		//			continue
		//		}
		//
		//		if len(tmpSegment.BigString) == 0 {
		//			continue
		//		}
		//
		//		hash := sha256.New()
		//		_, _ = io.WriteString(hash, tmpSegment.BigString)
		//		hashValue := hash.Sum(nil)
		//		// 2024-04-14:
		//		// 	为了确保 模型解出来的结果有不同的拆分策略（规则拆分、模型拆分），所以加上rich_text_index
		//		//	uniqueKey := strconv.Itoa(tmpSegment.RichTextIndex) + string(hashValue)
		//		// 2024-04-15: mobisysfeng, tangyuanlin, harryhlli 沟通后决定，只用 BigString 即可；
		//		uniqueKey := string(hashValue)
		//		// 长文本通过hash，只把不重复的存入ES
		//		if id, ok := uniqueBigData[uniqueKey]; ok {
		//			tmpSegment.BigDataID = id
		//		} else {
		//			tmpSegment.BigDataID = strconv.FormatUint(idgen.GetId(), 10) // 生成ES的ID
		//			uniqueBigData[uniqueKey] = tmpSegment.BigDataID
		//			bigData = append(bigData, &retrieval_pb.BigData{
		//				RobotId:   tmpSegment.AppPrimaryId,
		//				DocId:     tmpSegment.DocID,
		//				BigDataId: tmpSegment.BigDataID,
		//				BigStart:  tmpSegment.BigStart,
		//				BigEnd:    tmpSegment.BigEnd,
		//				BigString: tmpSegment.BigString,
		//			})
		//		}
		//		tmpSehmentInDBs = append(tmpSehmentInDBs, &tmpSegment.DocSegment)
		//	}
		//	if err := l.segDao.CreateDocSegments(ctx, tmpSehmentInDBs, tx); err != nil {
		//		logx.E(ctx, "CreateSegment error: %v", err)
		//		return err
		//	}
		//
		//	req := retrieval_pb.AddBigDataElasticReq{Data: bigData, Type: retrieval_pb.KnowledgeType_KNOWLEDGE}
		//	logx.I(ctx, "CreateSegment|AddBigDataElastic|req:%+v", req)
		//	if err := l.rpc.RetrievalDirectIndex.AddBigDataElastic(ctx, &req); err != nil {
		//		logx.E(ctx, "CreateSegment|AddBigDataElastic|seg:%+v|err:%+v", tmpSegments, err)
		//		return err
		//	}
		// }
		return nil
	}); err != nil {
		logx.E(ctx, "CreateSegment|Transaction|err:%+v", err)
		return err
	}
	return nil
}

// createSegmentWithBigData 创建文档分段
func (l *Logic) CreateSegmentWithBigData(ctx context.Context,
	shortURLSyncMap, orgDataSyncMap, bigDataSyncMap, imageDataSyncMap *sync.Map, segments []*segEntity.DocSegmentExtend,
	robotID uint64, currentOrgDataOrder *int, OldOrgDataInfos []*segEntity.OldOrgDataInfo) error {
	eSAddSize := 50
	pageSize := 1000
	// 创建哈希表存储唯一字符串和BigData的ID
	total := len(segments)
	pages := int(math.Ceil(float64(total) / float64(pageSize)))
	for i := 0; i < pages; i++ {
		start := pageSize * i
		end := pageSize * (i + 1)
		if end > total {
			end = total
		}
		logx.I(ctx, "createSegmentWithBigData|segments.len:%d|start:%d|end:%d",
			len(segments), start, end)
		tmpSegments := segments[start:end]
		var orgData []*segEntity.DocSegmentOrgData
		var bigData []*retrieval_pb.BigData
		var images []*segEntity.DocSegmentImage
		var pageInfos []*segEntity.DocSegmentPageInfo
		docCommon := &segEntity.DocSegmentCommon{}
		for index, tmpSegment := range tmpSegments {
			if index == 0 {
				corpBizID, appBizID, staffBizID, docBizID, err := l.SegmentCommonIDsToBizIDs(ctx,
					tmpSegment.CorpID, tmpSegment.RobotID, tmpSegment.StaffID, tmpSegment.DocID)

				if err != nil {
					logx.E(ctx, "createSegmentWithBigData|SegmentCommonIDsToBizIDs|seg:%+v|err:%+v",
						tmpSegment, err)
					return err
				}
				logx.I(ctx, "createSegmentWithBigData|SegmentCommonIDsToBizIDs|seg:%+v|corpBizID:%d|appBizID:%d|staffBizID:%d|docBizID:%d",
					tmpSegment, corpBizID, appBizID, staffBizID, docBizID)
				docCommon.CorpID = tmpSegment.CorpID
				docCommon.AppID = tmpSegment.RobotID
				docCommon.StaffID = tmpSegment.StaffID
				docCommon.DocID = tmpSegment.DocID
				docCommon.CorpBizID = corpBizID
				docCommon.AppBizID = appBizID
				docCommon.StaffBizID = staffBizID
				docCommon.DocBizID = docBizID
			}
			// 生成业务ID
			tmpSegment.BusinessID = idgen.GetId()
			// orgData数据
			tmpSegmentOrgData, err := l.getDocSegmentOrgData(ctx, orgDataSyncMap, tmpSegment, docCommon)
			if err != nil {
				return err
			}
			if tmpSegmentOrgData != nil {
				orgData = append(orgData, tmpSegmentOrgData)
			}
			// bigData数据
			tmpSegmentBigData, err := l.getDocSegmentBigData(ctx, bigDataSyncMap, tmpSegment)
			if err != nil {
				return err
			}
			if tmpSegmentBigData != nil {
				bigData = append(bigData, tmpSegmentBigData)
			}
			// image数据
			tmpSegmentImages, err := l.getDocSegmentImages(ctx, shortURLSyncMap, imageDataSyncMap, tmpSegment)
			if err != nil {
				return err
			}
			if len(tmpSegmentImages) > 0 {
				images = append(images, tmpSegmentImages...)
			}
			// page页码数据
			tmpPageInfo, err := l.getDocSegmentPageInfo(ctx, tmpSegment)
			if err != nil {
				return err
			}
			if tmpPageInfo != nil {
				pageInfos = append(pageInfos, tmpPageInfo)
			}
		}

		docSegTableName := l.segDao.Query().TDocSegment.TableName()
		db, err := knowClient.GormClient(ctx, docSegTableName, robotID, 0, []client.Option{}...)
		if err != nil {
			return err
		}
		err = db.Transaction(func(tx *gorm.DB) error {
			// 写DB
			if err := l.createSegmentInfoToDB(ctx, robotID, tx, tmpSegments, pageInfos, images, orgData,
				currentOrgDataOrder, OldOrgDataInfos); err != nil {
				logx.E(ctx, "createSegmentWithBigData|createSegmentInfoToDB|seg:%+v|err:%+v",
					tmpSegments, err)
				return err
			}

			// 写ES
			// 分批写入bigData
			logx.I(ctx, "createSegmentWithBigData|len(bigData):%d", len(bigData))
			if len(bigData) > 0 {
				for _, bigDataChunks := range slicex.Chunk(bigData, eSAddSize) {
					req := retrieval_pb.AddBigDataElasticReq{Data: bigDataChunks, Type: retrieval_pb.KnowledgeType_KNOWLEDGE}
					if err := l.rpc.RetrievalDirectIndex.AddBigDataElastic(ctx, &req); err != nil {
						logx.E(ctx, "createSegmentWithBigData|AddBigDataElastic|seg:%+v|err:%+v",
							tmpSegments, err)
						return err
					}
				}
			}
			return nil
		})
		if err != nil {
			logx.E(ctx, "createSegmentWithBigData|Transactionx|seg:%+v|err:%+v",
				tmpSegments, err)
			return err
		}
	}
	return nil
}

// getDocSegmentBigData 获取文档切片BigData
func (l *Logic) getDocSegmentBigData(ctx context.Context, bigDataSyncMap *sync.Map,
	segment *segEntity.DocSegmentExtend) (*retrieval_pb.BigData, error) {
	// 只有是文档切片，才需要把 BigData存入ES
	if !segment.IsSegmentForIndex() {
		logx.W(ctx, "getDocSegmentBigData|segment:%+v", segment)
		return nil, nil
	}

	if len(segment.BigString) == 0 {
		logx.W(ctx, "getDocSegmentBigData|BigString|segment:%+v", segment)
		return nil, nil
	}

	hash := sha256.New()
	_, _ = io.WriteString(hash, segment.BigString)
	hashValue := hash.Sum(nil)
	// 2024-04-14:
	// 	为了确保 模型解出来的结果有不同的拆分策略（规则拆分、模型拆分），所以加上rich_text_index
	//	uniqueKey := strconv.Itoa(tmpSegment.RichTextIndex) + string(hashValue)
	// 2024-04-15: mobisysfeng, tangyuanlin, harryhlli 沟通后决定，只用 BigString 即可；
	uniqueKey := string(hashValue)
	// 长文本通过hash，只把不重复的存入ES
	if id, ok := bigDataSyncMap.Load(uniqueKey); ok {
		segment.BigDataID = id.(string)
		return nil, nil
	} else {
		segment.BigDataID = strconv.FormatUint(idgen.GetId(), 10) // 生成ES的ID
		bigDataSyncMap.Store(uniqueKey, segment.BigDataID)
		return &retrieval_pb.BigData{
			RobotId:   segment.RobotID,
			DocId:     segment.DocID,
			BigDataId: segment.BigDataID,
			BigStart:  segment.BigStart,
			BigEnd:    segment.BigEnd,
			BigString: segment.BigString,
		}, nil
	}
}

// createSegmentInfoToDB 将切片相关信息入库
func (l *Logic) createSegmentInfoToDB(ctx context.Context, robotID uint64, tx *gorm.DB,
	segments []*segEntity.DocSegmentExtend,
	pageInfos []*segEntity.DocSegmentPageInfo, images []*segEntity.DocSegmentImage, orgData []*segEntity.DocSegmentOrgData,
	currentOrgDataOrder *int, oldOrgDataInfos []*segEntity.OldOrgDataInfo) (err error) {
	segmentBizIDMap := make(map[uint64]*segEntity.DocSegmentExtend)
	// orgData存储
	logx.I(ctx, "createSegmentInfoToDB|len(orgData):%d|currentOrgDataOrder:%d|len(oldOrgDataInfos):%d",
		len(orgData), *currentOrgDataOrder, len(oldOrgDataInfos))

	if len(orgData) > 0 {
		// currentOrgDataOrder 先保留，清理代码后暂时未用到
		*currentOrgDataOrder += len(orgData)
		for _, data := range orgData {
			// logx.I(ctx, "createSegmentInfoToDB|intervene:%v|IsDisabled:%+v|AddMethod:%v",
			//	intervene, data.IsDisabled, data.AddMethod)
			err = l.CreateDocSegmentOrgData(ctx, data)
			if err != nil {
				logx.E(ctx, "createSegmentInfoToDB|CreateDocSegmentTemporaryOrgData err:%+v", err)
				return err
			}
		}
	}

	logx.I(ctx, "createSegmentInfoToDB|len(segments):%d", len(segments))
	if len(segments) > 0 {
		tSegments := make([]*segEntity.DocSegment, 0)
		for _, seg := range segments {
			tSegments = append(tSegments, &seg.DocSegment)
		}

		if err := l.segDao.CreateDocSegments(ctx, tSegments, tx); err != nil {
			logx.E(ctx, "createSegmentInfoToDB|CreateDocSegments err:%+v", err)
			return err
		}

		segmentBizIDMap, err = l.getSegmentByBizIDsWithTx(ctx, tx, segments)
		if err != nil {
			return err
		}
	}

	logx.I(ctx, "createSegmentInfoToDB|len(pageInfos):%d", len(pageInfos))
	if len(pageInfos) > 0 {
		if err = l.createDocSegmentPageInfos(ctx, tx, segmentBizIDMap, pageInfos); err != nil {
			logx.E(ctx, "createSegmentInfoToDB|createDocSegmentPageInfos|err:%+v", err)
			return err
		}
	}

	logx.I(ctx, "createSegmentInfoToDB|len(images):%d", len(images))
	if len(images) > 0 {
		if err = l.createDocSegmentImages(ctx, tx, segmentBizIDMap, images); err != nil {
			logx.E(ctx, "createSegmentInfoToDB|createDocSegmentImages|err:%+v", err)
			return err
		}
	}

	return nil
}

// getSegmentByBizIDsWithTx 事物获取切片信息
func (l *Logic) getSegmentByBizIDsWithTx(ctx context.Context, tx *gorm.DB, segments []*segEntity.DocSegmentExtend) (
	map[uint64]*segEntity.DocSegmentExtend, error) {
	/*
			`
			SELECT
				%s
			FROM
			    t_doc_segment
			WHERE
			    business_id IN (%s)
		`
	*/
	if len(segments) == 0 {
		logx.I(ctx, "getSegmentByBizIDsWithTx|len(segments):%d|ignore", len(segments))
		return nil, nil
	}

	args := make([]uint64, 0, len(segments))
	for _, seg := range segments {
		args = append(args, seg.BusinessID)
	}

	logx.I(ctx, "getSegmentByBizIDsWithTx|len(segments):%d|businessIDs:%+v", len(segments), args)

	docSegmentFilter := &segEntity.DocSegmentFilter{
		BusinessIDs: args,
	}

	segList, err := l.segDao.GetDocSegmentListWithTx(ctx, segEntity.DocSegmentTblColList, docSegmentFilter, tx)
	if err != nil {
		logx.E(ctx, "getSegmentByBizIDsWithTx|GetDocSegmentList|err:%+v", err)
		return nil, err
	}
	segmentMap := make(map[uint64]*segEntity.DocSegmentExtend)
	for _, seg := range segList {
		segExd := &segEntity.DocSegmentExtend{
			DocSegment: *seg,
		}
		segmentMap[seg.BusinessID] = segExd
	}
	return segmentMap, nil
}

// DescribeSegments 获取文档分段信息
func (l *Logic) DescribeSegments(ctx context.Context, appBizID uint64, segBizIDs []uint64) (*kb_pb.DescribeSegmentsRsp, error) {
	rsp := &kb_pb.DescribeSegmentsRsp{}
	segments, err := l.GetDocSegmentList(ctx, appBizID, segBizIDs)
	if err != nil {
		return nil, err
	}
	if len(segments) == 0 {
		return rsp, nil
	}
	// 获取切片的文档信息
	var docIDs []uint64
	for i := range segments {
		docIDs = append(docIDs, segments[i].DocID)
	}
	docIDMap := make(map[uint64]*docEntity.Doc)
	if len(docIDs) != 0 {
		docFilter := &docEntity.DocFilter{
			RouterAppBizID: appBizID,
			CorpId:         contextx.Metadata(ctx).CorpID(),
			IDs:            docIDs,
		}
		selectColumns := []string{docEntity.DocTblColId, docEntity.DocTblColBusinessId, docEntity.DocTblColCosURL, docEntity.DocTblColWebURL}
		docs, err := l.docDao.GetDocList(ctx, selectColumns, docFilter)
		if err != nil {
			return nil, err
		}
		for _, doc := range docs {
			docIDMap[doc.ID] = doc
		}
	}

	// 获取切片的文档页码信息
	docSegmentIDs := make([]uint64, 0, len(segments))
	for _, segment := range segments {
		docSegmentIDs = append(docSegmentIDs, segment.ID)
	}
	segmentPageInfoMap := make(map[uint64]*segEntity.DocSegmentPageInfo)
	logx.D(ctx, "DescribeSegments docSegmentIDs:%+v", docSegmentIDs)
	if len(docSegmentIDs) > 0 {
		filter := &segEntity.DocSegmentPageInfoFilter{
			RouterAppBizId: appBizID,
			SegmentIDs:     docSegmentIDs,
			Limit:          len(docSegmentIDs),
		}
		selectColumns := []string{segEntity.DocSegmentPageInfoTblSegmentID, segEntity.DocSegmentPageInfoTblOrgPageNumbers}
		db, err := l.GetGormDB(ctx, appBizID, model.TableNameTDocSegment)
		if err != nil {
			logx.E(ctx, "GetGormDB error: %v", err)
			return rsp, err
		}
		segmentPageInfos, err := l.segDao.BatchGetDocSegmentPageInfoList(ctx, selectColumns, filter, db)
		if err != nil {
			logx.E(ctx, "GetDocSegmentPageInfoList error: %v", err)
			return rsp, err
		}
		logx.D(ctx, "DescribeSegments GetDocSegmentPageInfoList len(segmentPageInfos): %+v",
			len(segmentPageInfos))
		for _, segmentPageInfo := range segmentPageInfos {
			segmentPageInfoMap[segmentPageInfo.SegmentID] = segmentPageInfo
		}
	}
	pbSegments := DocSegmentDb2Pb(ctx, segments, docIDMap, segmentPageInfoMap)
	rsp.List = pbSegments

	clues.AddT(ctx, "DescribeSegmentsRsp", rsp)
	return rsp, nil
}

// GetSegmentByID 通过ID获取段落内容
func (l *Logic) GetSegmentByID(ctx context.Context, id uint64, robotID uint64) (*segEntity.DocSegmentExtend, error) {
	/*
		`
				SELECT
					%s
				FROM
				    t_doc_segment
				WHERE
				    id = ?
			`
	*/
	if id == 0 {
		logx.E(ctx, "GetSegmentByID id is empty")
		return nil, errors.New("GetSegmentByID id is empty")
	}
	docSegmentFilter := &segEntity.DocSegmentFilter{
		ID: id,
	}
	docSegmentFields := segEntity.DocSegmentTblColList
	docSegmentTableName := l.segDao.Query().TDocSegment.TableName()

	db, err := knowClient.GormClient(ctx, docSegmentTableName, robotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "GormClient error: %v", err)
		return nil, err
	}

	docSeg, err := l.segDao.GetDocSegmentByFilter(ctx, docSegmentFields, docSegmentFilter, db)
	if err != nil {
		if errors.Is(err, errx.ErrNotFound) {
			logx.W(ctx, "GetSegmentByID record not found: %v", err)
			return nil, nil
		}
		logx.E(ctx, "GetSegmentByID error: %v", err)
		return nil, err
	}
	if docSeg == nil {
		return nil, nil
	}

	segment := &segEntity.DocSegmentExtend{
		DocSegment: *docSeg,
	}
	return segment, nil
}

// GetSegmentByIDs 通过ID获取段落内容
func (l *Logic) GetSegmentByIDs(ctx context.Context, ids []uint64, robotID uint64) (
	[]*segEntity.DocSegmentExtend, error) {
	// `SELECT %s FROM t_doc_segment WHERE id IN(?)`
	if len(ids) == 0 {
		return []*segEntity.DocSegmentExtend{}, nil
	}
	docSegmentFilter := &segEntity.DocSegmentFilter{
		IDs: ids,
	}
	docSegmentFields := segEntity.DocSegmentTblColList
	docSegmentTableName := l.segDao.Query().TDocSegment.TableName()

	db, err := knowClient.GormClient(ctx, docSegmentTableName, robotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "GormClient error: %v", err)
		return nil, err
	}

	segList, err := l.segDao.GetDocSegmentListWithTx(ctx, docSegmentFields, docSegmentFilter, db)
	if err != nil {
		logx.E(ctx, "GetDocSegmentList error: %v", err)
		return nil, err
	}

	segments := []*segEntity.DocSegmentExtend{}
	for _, seg := range segList {
		segments = append(segments, &segEntity.DocSegmentExtend{
			DocSegment: *seg,
		})
	}
	return segments, nil
}

func (l *Logic) GetSegmentByDocID(ctx context.Context, robotID, docID, startID, count uint64, selectColumns []string) (
	[]*segEntity.DocSegmentExtend, uint64, error) {
	docSegmentTableName := l.segDao.Query().TDocSegment.TableName()
	db, err := knowClient.GormClient(ctx, docSegmentTableName, robotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "GetSegmentByDocID get GormClient err:%v,robotID:%v", err, robotID)
		return nil, 0, err
	}
	return l.segDao.GetSegmentByDocID(ctx, robotID, docID, startID, count, selectColumns, db)
}

// GetPagedSegmentIDsByDocID 通过文档ID分页获取分片ID列表
func (l *Logic) GetPagedSegmentIDsByDocID(ctx context.Context, docID uint64, page uint32, pageSize uint32,
	robotID uint64) ([]uint64, error) {
	/*
		`
		SELECT
			id
		FROM
		    t_doc_segment
		WHERE
		    doc_id = ? AND is_deleted = ? ORDER BY id ASC LIMIT ?, ?
	*/

	docSegmentTableName := l.segDao.Query().TDocSegment.TableName()
	db, err := knowClient.GormClient(ctx, docSegmentTableName, robotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "GetSegmentByDocID get GormClient err:%v,robotID:%v", err, robotID)
		return nil, err
	}

	deleteFlag := segEntity.SegmentIsNotDeleted

	docSegmentFilter := &segEntity.DocSegmentFilter{
		DocID:          docID,
		IsDeleted:      &deleteFlag,
		OrderColumn:    []string{segEntity.DocSegmentTblColID},
		OrderDirection: []string{"ASC"},
	}
	docSegmentFields := []string{segEntity.DocSegmentTblColID}

	offset, limit := utilx.Page(page, pageSize)
	docSegmentFilter.Offset = offset
	docSegmentFilter.Limit = limit

	segments, err := l.segDao.GetDocSegmentListWithTx(ctx, docSegmentFields, docSegmentFilter, db)
	if err != nil {
		logx.E(ctx, "GetPagedSegmentIDsByDocID error: %v", err)
		return nil, err
	}

	ids := make([]uint64, 0)
	for _, seg := range segments {
		ids = append(ids, seg.ID)
	}
	return ids, nil
}

// GetSegmentIDByDocIDAndBatchID 通过文档ID和批次ID获取段落内容
func (l *Logic) GetSegmentIDByDocIDAndBatchID(ctx context.Context, docID uint64, batchID int, robotID uint64) (
	[]uint64, error) {
	/*
		`
		SELECT
			id
		FROM
		    t_doc_segment
		WHERE
		    doc_id = ? AND batch_id = ? ND is_deleted = ? order by id desc
	*/

	docSegmentTableName := l.segDao.Query().TDocSegment.TableName()
	db, err := knowClient.GormClient(ctx, docSegmentTableName, robotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "GetSegmentByDocID get GormClient err:%v,robotID:%v", err, robotID)
		return nil, err
	}

	deleteFlag := segEntity.SegmentIsNotDeleted

	docSegmentFilter := &segEntity.DocSegmentFilter{
		DocID:          docID,
		BatchID:        batchID,
		IsDeleted:      &deleteFlag,
		OrderColumn:    []string{segEntity.DocSegmentTblColID},
		OrderDirection: []string{"DESC"},
	}
	docSegmentFields := []string{segEntity.DocSegmentTblColID}

	segments, err := l.segDao.GetDocSegmentListWithTx(ctx, docSegmentFields, docSegmentFilter, db)
	if err != nil {
		logx.E(ctx, "GetPagedSegmentIDsByDocID error: %v", err)
		return nil, err
	}

	ids := make([]uint64, 0)
	for _, seg := range segments {
		ids = append(ids, seg.ID)
	}
	return ids, nil
}

// GetQASegmentIDByDocIDAndBatchID 通过文档ID和批次ID获取需要生成QA的段落内容
func (l *Logic) GetQASegmentIDByDocIDAndBatchID(ctx context.Context, docID, stopNextSegmentID, segmentCount uint64,
	batchID int, robotID uint64) ([]uint64, error) {
	/*
			`
			SELECT
				id
			FROM
			    t_doc_segment
			WHERE
			    doc_id = ? AND batch_id = ? AND is_deleted = ? AND status != ?
		`
	*/

	docSegmentTableName := l.segDao.Query().TDocSegment.TableName()
	db, err := knowClient.GormClient(ctx, docSegmentTableName, robotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "GetSegmentByDocID get GormClient err:%v,robotID:%v", err, robotID)
		return nil, err
	}

	deleteFlag := segEntity.SegmentIsNotDeleted

	docSegmentFilter := &segEntity.DocSegmentFilter{
		DocID:     docID,
		BatchID:   batchID,
		IsDeleted: &deleteFlag,
		StatusNot: segEntity.SegmentStatusCreatedQa,
	}
	docSegmentFields := []string{segEntity.DocSegmentTblColID}

	segments, err := l.segDao.GetDocSegmentListWithTx(ctx, docSegmentFields, docSegmentFilter, db)
	if err != nil {
		logx.E(ctx, "GetPagedSegmentIDsByDocID error: %v", err)
		return nil, err
	}

	ids := make([]uint64, 0)
	for _, seg := range segments {
		ids = append(ids, seg.ID)
	}
	return ids, nil
}

// GetSegmentListCount 获取segment列表数量
func (l *Logic) GetSegmentListCount(ctx context.Context, corpID, docID, robotID uint64) (uint64, error) {
	/*
		`
			SELECT
				count(*)
			FROM
			    t_doc_segment
			WHERE
			     corp_id = ? AND doc_id = ? AND is_deleted = ?
		`
	*/

	docSegmentTableName := l.segDao.Query().TDocSegment.TableName()
	db, err := knowClient.GormClient(ctx, docSegmentTableName, robotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "GetSegmentByDocID get GormClient err:%v,robotID:%v", err, robotID)
		return 0, err
	}

	deleteFlag := segEntity.SegmentIsNotDeleted

	filter := &segEntity.DocSegmentFilter{
		CorpID:    corpID,
		DocID:     docID,
		RobotId:   robotID,
		IsDeleted: &deleteFlag,
	}

	count, err := l.segDao.GetDocSegmentCountWithTx(ctx, []string{}, filter, db)
	if err != nil {
		logx.E(ctx, "Failed to GetSegmentListCount. err:%v,robotID:%v", err, robotID)
		return 0, err
	}
	return uint64(count), nil
}

// GetSegmentList 获取segment列表
func (l *Logic) GetSegmentList(ctx context.Context, corpID, docID uint64, page, pageSize uint32, robotID uint64) (
	[]*segEntity.DocSegmentExtend, error) {
	/*
			`
			SELECT
				%s
			FROM
			    t_doc_segment
			WHERE
			     corp_id = ? AND doc_id = ? AND is_deleted = ?
			LIMIT
			     ?,?
		`
	*/
	docSegmentTableName := l.segDao.Query().TDocSegment.TableName()
	db, err := knowClient.GormClient(ctx, docSegmentTableName, robotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "GetSegmentByDocID get GormClient err:%v,robotID:%v", err, robotID)
		return nil, err
	}

	deleteFlag := segEntity.SegmentIsNotDeleted

	docSegmentFields := segEntity.DocSegmentTblColList

	filter := &segEntity.DocSegmentFilter{
		CorpID:    corpID,
		DocID:     docID,
		IsDeleted: &deleteFlag,
	}

	offset, limit := utilx.Page(page, pageSize)

	filter.Offset = offset
	filter.Limit = limit

	segments, err := l.segDao.GetDocSegmentListWithTx(ctx, docSegmentFields, filter, db)

	list := make([]*segEntity.DocSegmentExtend, 0)
	for _, seg := range segments {
		list = append(list, &segEntity.DocSegmentExtend{
			DocSegment: *seg,
		})
	}
	return list, nil
}

// GetSegmentDeletedList 获取删除的segment列表
func (l *Logic) GetSegmentDeletedList(ctx context.Context, corpID, docID uint64, page, pageSize uint32, robotID uint64) (
	[]*segEntity.DocSegmentExtend, error) {
	/*
		`
		SELECT
			%s
		FROM
		    t_doc_segment
		WHERE
		     corp_id = ? AND doc_id = ? AND is_deleted = ?
		LIMIT
		     ?,?
	*/
	docSegmentTableName := l.segDao.Query().TDocSegment.TableName()
	db, err := knowClient.GormClient(ctx, docSegmentTableName, robotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "GetSegmentByDocID get GormClient err:%v,robotID:%v", err, robotID)
		return nil, err
	}

	deleteFlag := segEntity.SegmentIsDeleted

	docSegmentFields := segEntity.DocSegmentTblColList

	filter := &segEntity.DocSegmentFilter{
		CorpID:    corpID,
		DocID:     docID,
		RobotId:   robotID,
		IsDeleted: &deleteFlag,
	}

	offset, limit := utilx.Page(page, pageSize)

	filter.Offset = offset
	filter.Limit = limit

	segments, err := l.segDao.GetDocSegmentListWithTx(ctx, docSegmentFields, filter, db)

	list := make([]*segEntity.DocSegmentExtend, 0)
	for _, seg := range segments {
		list = append(list, &segEntity.DocSegmentExtend{
			DocSegment: *seg,
		})
	}
	return list, nil
}

// GetDocSegmentList 获取文档分段信息，兼容2.9.0之前旧文档的org data同时存在于t_doc_segment和t_doc_segment_org_data表中
func (l *Logic) GetDocSegmentList(ctx context.Context, appBizID uint64, segBizIDs []uint64) ([]*segEntity.DocSegment, error) {
	if len(segBizIDs) == 0 {
		return []*segEntity.DocSegment{}, nil
	}
	filter := &segEntity.DocSegmentFilter{
		RouterAppBizId: appBizID,
	}
	if segBizIDs[0] < util.MinBizID {
		// 对外接口，需要兼容之前接收自增id的场景
		filter.IDs = segBizIDs
	} else {
		filter.BusinessIDs = segBizIDs
	}
	selectColumns := []string{segEntity.DocSegmentTblColID, segEntity.DocSegmentTblColCorpID, segEntity.DocSegmentTblColBusinessID,
		segEntity.DocSegmentTblColDocId, segEntity.DocSegmentTblColFileType, segEntity.DocSegmentTblColSegmentType,
		segEntity.DocSegmentTblColTitle, segEntity.DocSegmentTblColPageContent, segEntity.DocSegmentTblColOrgData,
		segEntity.DocSegmentTblColOrgDataBizID}

	db, err := l.GetGormDB(ctx, appBizID, model.TableNameTDocSegment)
	if err != nil {
		logx.E(ctx, "GetSegmentByDocID get GormClient err:%v,robotID:%v", err, filter.RouterAppBizId)
		return nil, err
	}
	segments, err := l.segDao.BatchGetDocSegmentList(ctx, selectColumns, filter, db)
	clues.AddTrackE(ctx, "dao.GetSegmentByIDs", segments, err)
	if err != nil {
		return segments, err
	}

	for _, seg := range segments {
		// 防越权校验，为了兼容入参中的segmentID为共享知识库的情况
		if seg.CorpID != contextx.Metadata(ctx).CorpID() {
			return segments, errs.ErrSegmentNotFound
		}
	}

	// 获取分段信息
	orgDataBizIDs := make([]uint64, 0)
	for _, seg := range segments {
		if seg == nil {
			logx.W(ctx, "GetDocSegmentList seg is nil")
			continue
		}
		if seg.OrgData == "" {
			// 2.9.0之前旧文档的org data同时存在于t_doc_segment和t_doc_segment_org_data表中
			// 2.9.0之后的新文档org data只存在t_doc_segment_org_data表中，需要再查一次t_doc_segment_org_data
			orgDataBizIDs = append(orgDataBizIDs, seg.OrgDataBizID)
		}
	}
	if len(orgDataBizIDs) != 0 {
		orgDataBizIDs = slicex.Unique(orgDataBizIDs)
		logx.D(ctx, "DescribeSegments orgDataBizIDs:%+v", orgDataBizIDs)
		selectColumns := []string{segEntity.DocSegmentOrgDataTblColBusinessID, segEntity.DocSegmentOrgDataTblColOrgData}
		filter := &segEntity.DocSegmentOrgDataFilter{
			CorpBizID: contextx.Metadata(ctx).CorpBizID(),
			// AppBizID:    appBizID, // 不能打开，需要兼容入参中的segmentID为共享知识库的情况
			BusinessIDs:    orgDataBizIDs,
			Offset:         0,
			Limit:          len(orgDataBizIDs),
			RouterAppBizID: appBizID,
		}
		docSegmentOrgData, err := l.GetDocOrgDataList(ctx, selectColumns, filter)
		if err != nil {
			return segments, err
		}
		// logx.D(ctx, "DescribeSegments docSegmentOrgData:%+v", docSegmentOrgData)
		orgDataBizIDMap := make(map[uint64]string)
		for _, orgData := range docSegmentOrgData {
			orgDataBizIDMap[orgData.BusinessID] = orgData.OrgData
		}
		for i := range segments {
			if segments[i].OrgData == "" {
				if orgData, ok := orgDataBizIDMap[segments[i].OrgDataBizID]; ok {
					segments[i].OrgData = orgData
				}
			}
		}
	}
	return segments, nil
}

// DocSegmentDb2Pb 将数据库中的DocSegment转换为pb格式
func DocSegmentDb2Pb(ctx context.Context, segments []*segEntity.DocSegment, docIDMap map[uint64]*docEntity.Doc,
	segmentPageInfoMap map[uint64]*segEntity.DocSegmentPageInfo) []*kb_pb.DescribeSegmentsRsp_Segment {
	pbSegments := make([]*kb_pb.DescribeSegmentsRsp_Segment, 0, len(segments))
	for _, segment := range segments {
		pbSegment := &kb_pb.DescribeSegmentsRsp_Segment{
			Id:          segment.ID,
			BusinessId:  segment.BusinessID,
			DocId:       segment.DocID,
			FileType:    segment.FileType,
			SegmentType: segment.SegmentType,
			Title:       segment.Title,
			PageContent: segment.PageContent,
			OrgData:     segment.OrgData,
		}
		if doc, ok := docIDMap[pbSegment.DocId]; ok {
			pbSegment.DocBizId = doc.BusinessID
			pbSegment.DocUrl = doc.CosURL
			pbSegment.WebUrl = doc.WebURL
		}
		if segmentPageInfo, ok := segmentPageInfoMap[segment.ID]; ok {
			if len(segmentPageInfo.OrgPageNumbers) != 0 {
				pageInfos, pageData := make([]uint32, 0), make([]int32, 0)
				if err := jsonx.UnmarshalFromString(segmentPageInfo.OrgPageNumbers, &pageData); err != nil {
					logx.W(ctx, "DocSegmentDb2Pb UnmarshalFromString err:%+v", err)
				}
				for _, page := range pageData {
					pageInfos = append(pageInfos, uint32(page))
				}
				pbSegment.PageInfos = pageInfos
			}
		}

		pbSegments = append(pbSegments, pbSegment)
	}
	return pbSegments
}

// ListDocSegment 获取切片列表
func (l *Logic) ListDocSegment(ctx context.Context, req *kb_pb.ListDocSegmentReq,
	docCommon *segEntity.DocSegmentCommon, doc *docEntity.Doc) (*kb_pb.ListDocSegmentRsp, error) {
	logx.I(ctx, "ListDocSegment|start")
	rsp := new(kb_pb.ListDocSegmentRsp)
	docSegmentList := make([]*kb_pb.ListDocSegmentRsp_DocSegmentItem, 0)
	if req.PageNumber < 1 || req.PageSize < 1 {
		logx.E(ctx, "ListDocSegment|params err|req:%+v", req)
		return rsp, errs.ErrParams
	}
	// 获取文档所有的切片数量
	num, tempNum, err := l.GetDocOrgDataCountByDocBizID(ctx, docCommon)
	if err != nil {
		logx.E(ctx, "GetDocOrgDataCountByDocBizID|err:%+v", err)
		return rsp, errs.ErrSystem
	}
	// 当文档状态为审核失败状态时获取审核失败切片的数量
	if doc.IsAuditFailed() {
		logx.I(ctx, "ListDocSegment|count AuditFailNumber")
		filter := &segEntity.DocSegmentOrgDataTemporaryFilter{
			CorpBizID: docCommon.CorpBizID,
			AppBizID:  docCommon.AppBizID,
			DocBizID:  doc.BusinessID,
			IsDeleted: ptrx.Bool(false),
			AuditStatus: []uint32{
				uint32(segEntity.DocSegmentAuditStatusContentFailed),
				uint32(segEntity.DocSegmentAuditStatusPictureFailed),
				uint32(segEntity.DocSegmentAuditStatusContentAndPictureFailed)},
		}
		auditFailNum, err := l.segDao.GetDocTemporaryOrgDataCount(ctx, filter)
		if err != nil {
			logx.E(ctx, "getDocNotDeleteTemporaryOrgData|GetDocOrgDataCount|err:%v", err)
			return nil, err
		}
		rsp.AuditFailedNumber = uint64(auditFailNum)
	}
	rsp.FileSize = strconv.FormatUint(doc.FileSize, 10)
	rsp.SegmentNumber = strconv.FormatInt(num+tempNum, 10)
	// 确认改文档是否被编辑过（临时表是否有数据，orgData是否有临时删除）
	intervene, err := l.CheckDocIntervene(ctx, docCommon)
	if err != nil {
		return rsp, err
	}
	rsp.IsModify = intervene
	// 如果参考ID存在 or 切片id存在 （运行时使用）
	if req.GetReferBizId() != "" || req.GetSegmentBizId() != "" {
		return l.ListDocSegmentByReferBizIDOrSegmentBizID(ctx, req, docCommon, rsp)
	}
	// 分页查询，是否含有关键词
	if req.GetKeywords() != "" {
		return l.ListDocSegmentByKeywords(ctx, req, docCommon, num, tempNum, rsp)
	} else {
		logx.I(ctx, "GetDocSegmentItemList|NotKeywords")
		// 不含关键词，直接查询数据库
		docSegmentList, err = l.GetDocSegmentItemList(ctx, req, docCommon)
		if err != nil {
			return nil, err
		}
		rsp.SegmentList = docSegmentList
		rsp.Total = uint64(len(docSegmentList))
		return rsp, nil
	}
}

// ResumeSegments 恢复文档分片,删除的逆操作
func (l *Logic) ResumeSegments(ctx context.Context, segments []*segEntity.DocSegmentExtend, robotID uint64) error {
	docSegmentTableName := l.segDao.Query().TDocSegment.TableName()
	db, err := knowClient.GormClient(ctx, docSegmentTableName, robotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "GetSegmentByDocID get GormClient err:%v,robotID:%v", err, robotID)
		return err
	}

	var allImagesIDs []uint64
	if err := db.Transaction(func(tx *gorm.DB) error {
		for _, segment := range segments {
			/*
						`
					UPDATE
						t_doc_segment
					SET
					    is_deleted = :is_deleted,
					    update_time = :update_time,
					    release_status = :release_status,
					    next_action = :next_action
					WHERE
					    id = :id
				`
			*/

			segment.IsDeleted = segEntity.SegmentIsNotDeleted
			segment.UpdateTime = time.Now()
			segment.NextAction = qaEntity.NextActionUpdate
			segment.ReleaseStatus = segEntity.SegmentReleaseStatusInit
			if segment.IsSegmentForQA() {
				// 用于生成QA的切片不需要发布
				segment.NextAction = qaEntity.NextActionAdd
				segment.ReleaseStatus = segEntity.SegmentReleaseStatusNotRequired
			}

			updateColumnList := []string{
				segEntity.DocSegmentTblColIsDeleted, segEntity.DocSegmentTblColUpdateTime,
				segEntity.DocSegmentTblColReleaseStatus, segEntity.DocSegmentTblColNextAction,
			}

			filter := &segEntity.DocSegmentFilter{
				ID: segment.ID,
			}

			if err := l.segDao.UpdateDocSegmentWithTx(ctx, updateColumnList, filter, &segment.DocSegment, tx); err != nil {
				logx.E(ctx, "恢复文档分片失败  err:%+v", err)
				return err

			}

			// 切片页码
			/*
						`
					UPDATE
						t_doc_segment_page_info
					SET
					    is_deleted = ?,
					    update_time = ?
					WHERE
					    robot_id = ? AND segment_id = ?
				`
			*/

			pageFilter := &segEntity.DocSegmentPageInfoFilter{
				AppID:     robotID,
				SegmentID: segment.ID,
			}

			updateColumns := map[string]any{
				segEntity.DocSegmentPageInfoTblColIsDeleted:  segment.IsDeleted,
				segEntity.DocSegmentPageInfoTblColUpdateTime: time.Now(),
			}

			if err := l.segDao.BatchUpdateDocSegmentPageInfos(ctx, pageFilter, updateColumns,
				tx); err != nil {
				logx.E(ctx, "恢复文档分片页码失败 err:%+v", err)
				return err
			}

			// 图片ID
			/*
					`
				SELECT
					DISTINCT image_id
				FROM
				    t_doc_segment_image
				WHERE
				    robot_id = ? AND segment_id = ? AND is_deleted = ?

			*/
			deleteFlag := segEntity.SegmentIsDeleted
			imageFilter := &segEntity.DocSegmentImageFilter{
				AppID:          segment.RobotID,
				SegmentID:      segment.ID,
				IsDeleted:      &deleteFlag,
				DistinctColumn: []string{segEntity.DocSegmentImageTblColImageId},
			}
			imageIds := make([]uint64, 0)
			list, err := l.segDao.GetDocSegmentImageListWithTx(ctx, []string{}, imageFilter,
				tx)
			if err != nil {
				logx.E(ctx, "获取文档分片图片失败 err:%+v", err)
				return err
			}
			for _, image := range list {
				imageIds = append(imageIds, image.ImageID)
			}

			// 切片图片
			/*
					`
				UPDATE
					t_doc_segment_image
				SET
				    is_deleted = ?,
				    update_time = ?
				WHERE
				    robot_id = ? AND segment_id =
			*/
			imageFilter = &segEntity.DocSegmentImageFilter{
				AppID:     segment.RobotID,
				SegmentID: segment.ID,
			}

			updateColumns = map[string]any{
				segEntity.DocSegmentImageTblColIsDeleted:  segment.IsDeleted,
				segEntity.DocSegmentImageTblColUpdateTime: time.Now(),
			}

			if err := l.segDao.BatchUpdateDocSegmentImages(ctx, imageFilter, updateColumns,
				tx); err != nil {
				logx.E(ctx, "恢复文档分片图片失败 err:%+v", err)
				return err

			}
			allImagesIDs = append(allImagesIDs, imageIds...)
		}
		return nil
	}); err != nil {
		logx.E(ctx, "恢复文档分片失败 err:%+v", err)
		return err
	}
	// 恢复图片向量
	err = l.vectorSyncLogic.GetDao().UpdateImageVectorDeleteStatus(
		ctx, robotID, allImagesIDs, segEntity.SegmentImageVectorIsNotDeleted)
	if err != nil {
		return err
	}

	return nil
}

// BatchUpdateSegmentContent 批量更新文档分片
func (l *Logic) BatchUpdateSegmentContent(ctx context.Context,
	segments []*segEntity.DocSegmentExtend, robotID uint64) error {
	/*
		 `
				UPDATE
					t_doc_segment
				SET
				    title = :title,
				    page_content = :page_content,
				    org_data = :org_data,
				    org_data_biz_id = :org_data_biz_id,
				    update_time = :update_time,
				    release_status = :release_status,
				    next_action = :next_action
				WHERE
				    id = :id
			`
	*/

	docSegmentTableName := model.TableNameTDocSegment
	db, err := knowClient.GormClient(ctx, docSegmentTableName, robotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "GetSegmentByDocID get GormClient err:%v,robotID:%v", err, robotID)
		return err
	}

	updateColums := []string{
		segEntity.DocSegmentTblColTitle,
		segEntity.DocSegmentTblColPageContent,
		segEntity.DocSegmentTblColOrgData,
		segEntity.DocSegmentTblColOrgDataBizID,
		segEntity.DocSegmentTblColUpdateTime,
		segEntity.DocSegmentTblColReleaseStatus,
		segEntity.DocSegmentTblColNextAction,
	}

	updateSegemnts := make([]*segEntity.DocSegment, 0)

	for _, segment := range segments {
		updateSeg := &segment.DocSegment
		updateSeg.UpdateTime = time.Now()
		if !segment.IsNextActionAdd() {
			updateSeg.NextAction = qaEntity.NextActionUpdate
			updateSeg.ReleaseStatus = segEntity.SegmentReleaseStatusInit
		}
		updateSegemnts = append(updateSegemnts, updateSeg)
	}

	if err := l.segDao.BatchUpdateDocSegmentsWithTx(ctx, updateColums, updateSegemnts, db); err != nil {
		logx.E(ctx, "BatchUpdateSegmentContent err:%v", err)

	}
	return nil
}

// BatchUpdateSegmentOrgDataContent 批量更新文档分片原始数据内容
func (l *Logic) BatchUpdateSegmentOrgDataContent(ctx context.Context, orgData []*segEntity.DocSegmentOrgData) error {
	if len(orgData) == 0 {
		logx.W(ctx, "BatchUpdateSegmentOrgDataContent|orgData is empty")
		return nil
	}

	// 更新字段列表
	updateColumns := []string{
		segEntity.DocSegmentOrgDataTblColOrgData,
	}

	// 循环更新每个orgData
	for _, data := range orgData {
		if data == nil {
			logx.W(ctx, "BatchUpdateSegmentOrgDataContent|data is nil, skip")
			continue
		}

		// 构造filter，使用BusinessIDs
		filter := &segEntity.DocSegmentOrgDataFilter{
			BusinessIDs:    []uint64{data.BusinessID},
			RouterAppBizID: data.AppBizID,
		}

		// 获取数据库连接
		docSegmentOrgDataTableName := model.TableNameTDocSegmentOrgDatum
		db, err := l.GetGormDB(ctx, data.AppBizID, docSegmentOrgDataTableName)
		if err != nil {
			logx.E(ctx, "BatchUpdateSegmentOrgDataContent|get gorm failed|businessID:%d|err:%+v", data.BusinessID, err)
			return err
		}

		// 调用dao层更新
		rowsAffected, err := l.segDao.UpdateDocSegmentOrgData(ctx, updateColumns, filter, data, db)
		if err != nil {
			logx.E(ctx, "BatchUpdateSegmentOrgDataContent|UpdateDocSegmentOrgData failed|businessID:%d|err:%+v",
				data.BusinessID, err)
			return err
		}

		logx.D(ctx, "BatchUpdateSegmentOrgDataContent|updated|businessID:%d|rowsAffected:%d",
			data.BusinessID, rowsAffected)
	}

	return nil
}

// BatchUpdateSegment 批量更新文档分片
func (l *Logic) BatchUpdateSegment(ctx context.Context, segments []*segEntity.DocSegmentExtend, robotID uint64) error {
	/*
		`
			UPDATE
				t_doc_segment
			SET
			    update_time = :update_time,
			    release_status = :release_status,
			    next_action = :next_action
			WHERE
			    id = :id
	*/

	docSegmentTableName := model.TableNameTDocSegment
	db, err := knowClient.GormClient(ctx, docSegmentTableName, robotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "GetSegmentByDocID get GormClient err:%v,robotID:%v", err, robotID)
		return err
	}

	updateColums := []string{
		segEntity.DocSegmentTblColUpdateTime,
		segEntity.DocSegmentTblColReleaseStatus,
		segEntity.DocSegmentTblColNextAction,
	}

	updateSegemnts := make([]*segEntity.DocSegment, 0)

	for _, segment := range segments {
		updateSeg := &segment.DocSegment
		updateSeg.UpdateTime = time.Now()
		if !segment.IsNextActionAdd() {
			updateSeg.NextAction = qaEntity.NextActionUpdate
			updateSeg.ReleaseStatus = segEntity.SegmentReleaseStatusInit
		}
		updateSegemnts = append(updateSegemnts, updateSeg)
	}

	if err := l.segDao.BatchUpdateDocSegmentsWithTx(ctx, updateColums, updateSegemnts, db); err != nil {
		logx.E(ctx, "BatchUpdateSegment err:%v", err)

	}
	return nil
}

// UpdateSegmentReleaseStatus  更新文档分片
func (l *Logic) UpdateSegmentReleaseStatus(ctx context.Context,
	segment *segEntity.DocSegmentExtend, robotID uint64) error {
	/*
		`
				UPDATE
					t_doc_segment
				SET
				    update_time = :update_time,
				    release_status = :release_status
				WHERE
				    id = :id
	*/
	docSegmentTableName := model.TableNameTDocSegment
	db, err := knowClient.GormClient(ctx, docSegmentTableName, robotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "GetSegmentByDocID get GormClient err:%v,robotID:%v", err, robotID)
		return err
	}

	updateColums := []string{
		segEntity.DocSegmentTblColUpdateTime,
		segEntity.DocSegmentTblColReleaseStatus,
	}

	updateSegemnt := &segment.DocSegment
	updateSegemnt.UpdateTime = time.Now()

	if err := l.segDao.BatchUpdateDocSegmentsWithTx(ctx, updateColums, []*segEntity.DocSegment{updateSegemnt}, db); err != nil {
		logx.E(ctx, "UpdateSegmentReleaseStatus err:%v", err)

	}
	return nil
}

// updateSegmentOutputs 更新文档段落算法响应结果
func (l *Logic) UpdateSegmentOutputs(ctx context.Context, segment *segEntity.DocSegmentExtend, robotID uint64) error {
	/*
		`
			UPDATE
			    t_doc_segment
			SET
			    outputs = :outputs,
			    cost_time = :cost_time,
			    update_time = :update_time
			WHERE
			    id=:id
		`
	*/
	docSegmentTableName := l.segDao.Query().TDocSegment.TableName()
	db, err := knowClient.GormClient(ctx, docSegmentTableName, robotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "GetSegmentByDocID get GormClient err:%v,robotID:%v", err, robotID)
		return err
	}

	updateColums := []string{
		segEntity.DocSegmentTblColUpdateTime,
		segEntity.DocSegmentTblColCostTime,
		segEntity.DocSegmentTblColOutputs,
	}

	filter := &segEntity.DocSegmentFilter{
		ID: segment.ID,
	}

	updateSeg := &segment.DocSegment
	updateSeg.UpdateTime = time.Now()
	updateSeg.CostTime = segment.CostTime
	updateSeg.Outputs = segment.Outputs

	if err := l.segDao.UpdateDocSegmentWithTx(ctx, updateColums, filter, updateSeg, db); err != nil {
		logx.E(ctx, "UpdateSegmentOutputs err:%v", err)

	}
	return nil
}

// BatchUpdateSegmentReleaseStatus 批量更新文档分片
func (l *Logic) BatchUpdateSegmentReleaseStatus(ctx context.Context, segs []*segEntity.DocSegmentExtend,
	status uint32, robotID uint64) error {
	/*
				batchUpdateSegmentReleaseStatus = `
			UPDATE
				t_doc_segment
			SET
			    update_time = ?,
			    release_status = ?
			WHERE
			    id IN (%s)
		`
	*/

	docSegmentTableName := l.segDao.Query().TDocSegment.TableName()
	db, err := knowClient.GormClient(ctx, docSegmentTableName, robotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "GetSegmentByDocID get GormClient err:%v,robotID:%v", err, robotID)
		return err
	}

	updateColums := []string{
		segEntity.DocSegmentTblColUpdateTime,
		segEntity.DocSegmentTblColReleaseStatus,
	}

	if err := db.Transaction(func(tx *gorm.DB) error {
		now := time.Now()
		total := len(segs)
		pageSize := 100
		pages := int(math.Ceil(float64(total) / float64(pageSize)))
		for i := 0; i < pages; i++ {
			start := pageSize * i
			end := pageSize * (i + 1)
			if end > total {
				end = total
			}
			batch := segs[start:end]

			for _, seg := range batch {
				s := &seg.DocSegment
				docSegmentFilter := &segEntity.DocSegmentFilter{
					ID: s.ID,
				}
				s.UpdateTime = now
				s.ReleaseStatus = status
				if err := l.segDao.UpdateDocSegmentWithTx(ctx, updateColums, docSegmentFilter, s, tx); err != nil {
					logx.E(ctx, "UpdateSegmentError error. err:%v", err)
					return err
				}
			}

		}
		return nil
	}); err != nil {
		logx.E(ctx, "Failed to BatchUpdateSegmentReleaseStatus segs:%+v err:%+v", segs, err)
		return err
	}

	return nil
}

// // GetSheetByNameWithCache 获取sheet
func (l *Logic) GetSheetByNameWithCache(ctx context.Context, corpBizID, appBizID, docBizID uint64,
	sheetName string, sheetSyncMap *sync.Map) (*segEntity.DocSegmentSheetTemporary, error) {
	logx.I(ctx, "GetSheetByNameWithCache|start|SheetName:%s", sheetName)
	if sheetSyncMap == nil {
		logx.E(ctx, "GetSheetByNameWithCache|sheetSyncMap is null")
		return nil, errs.ErrSystem
	}
	hash := sha256.New()
	_, err := io.WriteString(hash, sheetName)
	if err != nil {
		logx.E(ctx, "GetSheetByNameWithCache|WriteString|err:%+v", err)
		return nil, err
	}
	hashValue := hash.Sum(nil)
	uniqueKey := string(hashValue)
	if value, ok := sheetSyncMap.Load(uniqueKey); ok {
		if sheet, ok := value.(*segEntity.DocSegmentSheetTemporary); ok {
			return sheet, nil
		}
	}
	sheets, err := l.GetSheetByName(ctx, corpBizID, appBizID, docBizID, sheetName)
	if err != nil {
		logx.E(ctx, "GetSheetByName|err:%+v", err)
		return nil, err
	}
	if len(sheets) > 0 {
		// 存入sheet数据
		sheetSyncMap.Store(uniqueKey, sheets[0])
		return sheets[0], nil
	}
	logx.E(ctx, "GetSheetByNameWithCache|SheetName not found|sheetName:%s", sheetName)
	return nil, errs.ErrDocSegmentSheetNotFound
}

// GetSheetByName 获取sheet
func (l *Logic) GetSheetByName(ctx context.Context, corpBizID, appBizID, docBizID uint64,
	sheetName string) ([]*segEntity.DocSegmentSheetTemporary, error) {
	logx.I(ctx, "GetSheetByName|start|SheetName:%s", sheetName)
	filter := &segEntity.DocSegmentSheetTemporaryFilter{
		CorpBizID:  corpBizID,
		AppBizID:   appBizID,
		DocBizID:   docBizID,
		IsDeleted:  ptrx.Bool(false),
		SheetNames: []string{sheetName},
		Offset:     0,
		Limit:      1,
	}
	list, err := l.segDao.GetSheetList(ctx, segEntity.DocSegmentSheetTemporaryTblColList, filter)
	if err != nil {
		logx.E(ctx, "GetSheetByName|err:%+v", err)
		return nil, err
	}
	return list, nil
}

// GetSegmentChunk 分段获取文段
func (l *Logic) GetSegmentChunk(ctx context.Context, corpID, appID, offset, limit uint64) (
	[]*segEntity.DocSegment, error) {
	/*
			 `
			SELECT ` + docSegmentFields + ` FROM t_doc_segment
			WHERE corp_id = ? AND robot_id = ? AND is_deleted = ? AND (type = ? OR type = ?) AND segment_type NOT IN (?,?,?) AND id > ?
			ORDER BY id ASC LIMIT ?
		`
	*/

	docSegmentTableName := l.segDao.Query().TDocSegment.TableName()
	db, err := knowClient.GormClient(ctx, docSegmentTableName, appID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "GetSegmentByDocID get GormClient err:%v,appID:%v", err, appID)
		return nil, err
	}

	deleteFlad := segEntity.SegmentIsNotDeleted

	filter := &segEntity.DocSegmentFilter{
		CorpID:         corpID,
		RobotId:        appID,
		IsDeleted:      &deleteFlad,
		ExtraCondition: "(type = ? OR type = ?) AND segment_type NOT IN (?,?,?) AND id > ?",
		ExtraParams: []any{segEntity.SegmentTypeIndex, segEntity.SegmentTypeQAAndIndex,
			segEntity.SegmentTypeTable, segEntity.SegmentTypeText2SQLMeta, segEntity.SegmentTypeText2SQLContent, offset},
		Limit:          int(limit),
		Offset:         int(offset),
		OrderColumn:    []string{segEntity.DocSegmentTblColID},
		OrderDirection: []string{"ASC"},
	}

	selectColumns := segEntity.DocSegmentTblColList

	list, err := l.segDao.GetDocSegmentListWithTx(ctx, selectColumns, filter, db)
	if err != nil {
		logx.E(ctx, "GetSegmentChunk fail, filter: %+v err: %v", filter, err)
		return nil, err
	}

	return list, nil
}

// GetSegmentChunkCount 获取文段总数
func (l *Logic) GetSegmentChunkCount(ctx context.Context, corpID, appID uint64) (int, error) {
	/*
		`
			SELECT COUNT(*) FROM t_doc_segment
			WHERE corp_id = ? AND robot_id = ? AND is_deleted = ? AND (type = ? OR type = ?)
		`
	*/
	docSegmentTableName := l.segDao.Query().TDocSegment.TableName()
	db, err := knowClient.GormClient(ctx, docSegmentTableName, appID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "GetSegmentByDocID get GormClient err:%v,appID:%v", err, appID)
		return 0, err
	}

	deleteFlad := segEntity.SegmentIsNotDeleted

	filter := &segEntity.DocSegmentFilter{
		CorpID:         corpID,
		RobotId:        appID,
		IsDeleted:      &deleteFlad,
		ExtraCondition: "(type = ? OR type = ?)",
		ExtraParams:    []any{segEntity.SegmentTypeIndex, segEntity.SegmentTypeQAAndIndex},
	}

	count, err := l.segDao.GetDocSegmentCountWithTx(ctx, []string{}, filter, db)

	if err != nil {
		logx.E(ctx, "GetSegmentChunkCount fail, err: %v", err)
		return 0, err
	}

	return int(count), nil
}

// GetSegmentSyncChunk 分段获取同步文段
func (l *Logic) GetSegmentSyncChunk(ctx context.Context, corpID, appID, offset, limit uint64) (
	[]*segEntity.DocSegment, error) {
	/*
			`
			SELECT ` + docSegmentFields + ` FROM t_doc_segment
			WHERE corp_id = ? AND robot_id = ? AND is_deleted = ? AND (type = ? OR type = ?) AND segment_type NOT IN (?,?) AND org_data_biz_id = 0 AND id > ?
			ORDER BY id ASC LIMIT ?
		`
	*/
	docSegmentTableName := l.segDao.Query().TDocSegment.TableName()
	db, err := knowClient.GormClient(ctx, docSegmentTableName, appID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "GetSegmentByDocID get GormClient err:%v,appID:%v", err, appID)
		return nil, err
	}

	deleteFlad := segEntity.SegmentIsNotDeleted

	filter := &segEntity.DocSegmentFilter{
		CorpID:         corpID,
		RobotId:        appID,
		IsDeleted:      &deleteFlad,
		ExtraCondition: "(type = ? OR type = ?) AND segment_type NOT IN (?,?,?) AND org_data_biz_id = 0 AND id > ?",
		ExtraParams: []any{segEntity.SegmentTypeIndex, segEntity.SegmentTypeQAAndIndex,
			segEntity.SegmentTypeText2SQLMeta, segEntity.SegmentTypeText2SQLContent, offset},
		Limit:          int(limit),
		Offset:         int(offset),
		OrderColumn:    []string{segEntity.DocSegmentTblColID},
		OrderDirection: []string{"ASC"},
	}

	selectColumns := segEntity.DocSegmentTblColList

	list, err := l.segDao.GetDocSegmentListWithTx(ctx, selectColumns, filter, db)
	if err != nil {
		logx.E(ctx, "GetSegmentSyncChunk fail, filter: %+v err: %v", filter, err)
		return nil, err
	}

	return list, nil
}

// GetSegmentSyncChunkCount 获取同步文段总数
func (l *Logic) GetSegmentSyncChunkCount(ctx context.Context, corpID, appID uint64) (int, error) {
	/*
		`
			SELECT COUNT(*) FROM t_doc_segment
			WHERE corp_id = ? AND robot_id = ? AND is_deleted = ? AND (type = ? OR type = ?) AND org_data_biz_id = 0
		`
	*/
	docSegmentTableName := l.segDao.Query().TDocSegment.TableName()
	db, err := knowClient.GormClient(ctx, docSegmentTableName, appID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "GetSegmentByDocID get GormClient err:%v,appID:%v", err, appID)
		return 0, err
	}

	deleteFlad := segEntity.SegmentIsNotDeleted

	filter := &segEntity.DocSegmentFilter{
		CorpID:         corpID,
		RobotId:        appID,
		IsDeleted:      &deleteFlad,
		ExtraCondition: "(type = ? OR type = ?) AND org_data_biz_id = 0",
		ExtraParams:    []any{segEntity.SegmentTypeIndex, segEntity.SegmentTypeQAAndIndex},
	}

	count, err := l.segDao.GetDocSegmentCountWithTx(ctx, []string{}, filter, db)

	if err != nil {
		logx.E(ctx, "GetSegmentSyncChunkCount fail, err: %v", err)
		return 0, err
	}

	return int(count), nil
}

// GetText2SqlSegmentMeta 通过DocID获取Text2Sql的meta数据；
func (l *Logic) GetText2SqlSegmentMeta(ctx context.Context, docID uint64, robotID uint64) (
	[]*segEntity.DocSegmentExtend, error) {
	/*
			`
			SELECT
				id, page_content
			FROM
			    t_doc_segment
			WHERE
			    doc_id = ? AND segment_type = ? AND is_deleted = ?
		`
	*/

	docSegmentTableName := l.segDao.Query().TDocSegment.TableName()
	db, err := knowClient.GormClient(ctx, docSegmentTableName, robotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "GetSegmentByDocID get GormClient err:%v,appID:%v", err, robotID)
		return nil, err
	}

	deleteFlad := segEntity.SegmentIsNotDeleted

	filter := &segEntity.DocSegmentFilter{
		RobotId:     robotID,
		DocID:       docID,
		SegmentType: segEntity.SegmentTypeText2SQLMeta,
		IsDeleted:   &deleteFlad,
	}

	selectColumns := []string{segEntity.DocSegmentTblColID, segEntity.DocSegmentTblColPageContent}

	list, err := l.segDao.GetDocSegmentListWithTx(ctx, selectColumns, filter, db)
	if err != nil {
		logx.E(ctx, "GetText2SqlSegmentMeta fail, filter: %+v err: %v", filter, err)
		return nil, err
	}

	res := make([]*segEntity.DocSegmentExtend, 0, len(list))
	for _, seg := range list {
		res = append(res, &segEntity.DocSegmentExtend{
			DocSegment: *seg,
		})
	}

	return res, nil
}

// ModifyDocSegment 保存切片修改(单个)
func (l *Logic) ModifyDocSegment(ctx context.Context, req *kb_pb.ModifyDocSegmentReq,
	docCommon *segEntity.DocSegmentCommon, doc *docEntity.Doc) (*kb_pb.ModifyDocSegmentRsp, error) {
	logx.I(ctx, "ModifyDocSegment|start")
	rsp := new(kb_pb.ModifyDocSegmentRsp)
	// todo 优化为状态机形式
	// 新增/编辑切片
	err := l.segDao.Query().TDocSegment.WithContext(ctx).UnderlyingDB().Transaction(func(tx *gorm.DB) error {
		if len(req.ModifySegments) > 0 {
			logx.I(ctx, "ModifyDocSegment|UpdateOrInsert|len(ModifySegments):%d", len(req.ModifySegments))
			for _, segment := range req.GetModifySegments() {
				if segment.SegBizId != "" {
					// 编辑
					logx.I(ctx, "ModifyDocSegment|Update|SegBizId:%s", segment.SegBizId)
					// 检查id格式，id以edit/insert开头，为临时表数据，去临时表查询/更新
					if strings.HasPrefix(segment.SegBizId, segEntity.EditPrefix) || strings.HasPrefix(segment.SegBizId, segEntity.InsertPrefix) {

						orgData, err := l.segDao.GetDocTemporaryOrgDataByBizID(ctx,
							segEntity.DocSegmentOrgDataTemporaryTblColList, docCommon.CorpBizID, docCommon.AppBizID,
							docCommon.DocBizID, segment.SegBizId)
						if err != nil {
							if errors.Is(err, gorm.ErrRecordNotFound) {
								logx.E(ctx, "ModifyDocSegment|orgData is null")
								return errs.ErrDocSegmentNotFound
							}
							return err
						}
						if orgData == nil {
							logx.E(ctx, "ModifyDocSegment|orgData is null")
							return errs.ErrDocSegmentNotFound
						}
						err = l.segDao.UpdateDocSegmentTemporaryOrgDataContent(ctx,
							docCommon.CorpBizID, docCommon.AppBizID, docCommon.DocBizID,
							[]string{segment.SegBizId}, segment.OrgData)
						if err != nil {
							return err
						}
					} else {
						// 从原始表查询，在临时表新增
						// 如果切片被编辑过，则阻止操作
						tempOrgData, err := l.segDao.GetDocTemporaryOrgDataByOriginOrgDataID(ctx,
							segEntity.DocSegmentOrgDataTemporaryTblColList, docCommon.CorpBizID, docCommon.AppBizID,
							docCommon.DocBizID, segment.SegBizId)
						if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
							return err
						}
						if tempOrgData != nil {
							logx.E(ctx, "ModifyDocSegment|orgData is edit, operation not allowed")
							return errs.ErrDocSegmentOperationNotAllowedFailed
						}
						// 如果原始数据ID有被新增关联顺序，需替换
						originOrgDataID, err := util.CheckReqParamsIsUint64(ctx, segment.SegBizId)
						if err != nil {
							logx.E(ctx, "ModifyDocSegmentByOperate|DocBizIDToUint64|err:%+v", err)
							return err
						}
						orgData, err := l.GetDocOrgDataByBizID(ctx,
							segEntity.DocSegmentOrgDataTblColList, docCommon.CorpBizID, docCommon.AppBizID,
							docCommon.DocBizID, originOrgDataID)
						if err != nil {
							if errors.Is(err, gorm.ErrRecordNotFound) {
								logx.E(ctx, "ModifyDocSegment|orgData is null")
								return errs.ErrDocSegmentNotFound
							}
							return err
						}
						if orgData == nil {
							logx.E(ctx, "ModifyDocSegment|orgData is null")
							return errs.ErrDocSegmentNotFound
						}
						orgDataTemp := &segEntity.DocSegmentOrgDataTemporary{}
						orgDataTemp.BusinessID = segEntity.EditPrefix + strconv.FormatUint(idgen.GetId(), 10)
						orgDataTemp.CorpBizID = orgData.CorpBizID
						orgDataTemp.AppBizID = orgData.AppBizID
						orgDataTemp.DocBizID = orgData.DocBizID
						orgDataTemp.OrgData = segment.OrgData
						orgDataTemp.AddMethod = segEntity.AddMethodEdit
						orgDataTemp.Action = segEntity.EditAction
						orgDataTemp.OrgPageNumbers = orgData.OrgPageNumbers
						orgDataTemp.SegmentType = orgData.SegmentType
						orgDataTemp.OriginOrgDataID = strconv.FormatUint(orgData.BusinessID, 10)
						orgDataTemp.IsDeleted = false
						orgDataTemp.IsDisabled = false
						orgDataTemp.SheetName = orgData.SheetName
						orgDataTemp.CreateTime = time.Now()
						orgDataTemp.UpdateTime = time.Now()
						if doc.IsExcelx() {
							orgDataTemp.OrgData = l.GetSliceTable(orgData.OrgData, 0) + "\n" + segment.OrgData
						} else {
							orgDataTemp.OrgData = segment.OrgData
						}
						err = l.segDao.CreateDocSegmentOrgDataTemporary(ctx, orgDataTemp)
						if err != nil {
							return err
						}
						// 查找原始id是否被插入使用
						oldOrgData, err := l.segDao.GetDocTemporaryOrgDataByLastOrgDataID(ctx,
							segEntity.DocSegmentOrgDataTemporaryTblColList, docCommon.CorpBizID, docCommon.AppBizID,
							docCommon.DocBizID, segment.SegBizId)
						if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
							return err
						}
						if oldOrgData != nil {
							// 原始id有被插入使用
							updateColumns := []string{
								segEntity.DocSegmentOrgDataTemporaryTblColLastOrgDataID,
								segEntity.DocSegmentOrgDataTemporaryTblColUpdateTime,
							}
							update := &segEntity.DocSegmentOrgDataTemporary{
								LastOrgDataID: orgDataTemp.BusinessID,
								UpdateTime:    time.Now(),
							}
							filter := &segEntity.DocSegmentOrgDataTemporaryFilter{
								CorpBizID:   docCommon.CorpBizID,
								AppBizID:    docCommon.AppBizID,
								DocBizID:    docCommon.DocBizID,
								BusinessIDs: []string{oldOrgData.BusinessID},
							}
							_, err := l.segDao.UpdateDocSegmentTemporaryOrgData(ctx, updateColumns, filter, update)
							if err != nil {
								return err
							}
						}
					}
				} else {
					// 新增
					logx.I(ctx, "ModifyDocSegment|Insert|LastSegBizId:%s", segment.LastSegBizId)
					// 参数校验
					if segment.OrgData == "" || segment.LastSegBizId == "" {
						return errs.ErrParams
					} else if segment.LastSegBizId == segEntity.InsertAtFirst && segment.AfterSegBizId == "" {
						return errs.ErrParams
					}
					// 获取改数据插入位置关联的原始切片数据
					lastOriginOrgDataID, err := l.GetLastOriginOrgDataIDByLastOrgDataID(ctx, docCommon.CorpBizID,
						docCommon.AppBizID, docCommon.DocBizID, segment.LastSegBizId, segment.AfterSegBizId)
					if err != nil {
						return err
					}
					if lastOriginOrgDataID == "" {
						logx.E(ctx, "ModifyDocSegment|Insert|lastOriginOrgDataID is empty")
						return errs.ErrDocSegmentNotFound
					}
					orgDataTemp := &segEntity.DocSegmentOrgDataTemporary{}
					orgDataTemp.BusinessID = segEntity.InsertPrefix + strconv.FormatUint(idgen.GetId(), 10)
					orgDataTemp.CorpBizID = docCommon.CorpBizID
					orgDataTemp.AppBizID = docCommon.AppBizID
					orgDataTemp.DocBizID = docCommon.DocBizID
					orgDataTemp.OrgData = segment.OrgData
					orgDataTemp.AddMethod = segEntity.AddMethodArtificial
					orgDataTemp.Action = segEntity.InsertAction
					orgDataTemp.LastOriginOrgDataID = lastOriginOrgDataID
					orgDataTemp.LastOrgDataID = segment.LastSegBizId
					orgDataTemp.AfterOrgDataID = segment.AfterSegBizId
					orgDataTemp.IsDeleted = false
					orgDataTemp.CreateTime = time.Now()
					orgDataTemp.UpdateTime = time.Now()
					orgDataTemp.SheetName = req.GetSheetName()

					// 查询旧插入数据
					oldOrgData, err := l.segDao.GetDocTemporaryOrgDataByLastOrgDataID(ctx,
						segEntity.DocSegmentOrgDataTemporaryTblColList, docCommon.CorpBizID, docCommon.AppBizID,
						docCommon.DocBizID, segment.LastSegBizId)
					if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
						return err
					}

					// excel文件需要拼接sheetName
					if oldOrgData != nil && doc.IsExcelx() {
						orgDataTemp.OrgData = l.GetSliceTable(oldOrgData.OrgData, 0) + "\n" + segment.OrgData
					} else {
						orgDataTemp.OrgData = segment.OrgData
					}
					err = l.segDao.CreateDocSegmentOrgDataTemporary(ctx, orgDataTemp)
					if err != nil {
						return err
					}
					// 更新旧插入数据
					if oldOrgData != nil {
						updateColumns := []string{
							segEntity.DocSegmentOrgDataTemporaryTblColLastOrgDataID,
							segEntity.DocSegmentOrgDataTemporaryTblColUpdateTime,
						}
						update := &segEntity.DocSegmentOrgDataTemporary{
							LastOrgDataID: orgDataTemp.BusinessID,
							UpdateTime:    time.Now(),
						}
						filter := &segEntity.DocSegmentOrgDataTemporaryFilter{
							CorpBizID:   docCommon.CorpBizID,
							AppBizID:    docCommon.AppBizID,
							DocBizID:    docCommon.DocBizID,
							BusinessIDs: []string{oldOrgData.BusinessID},
						}
						_, err := l.segDao.UpdateDocSegmentTemporaryOrgData(ctx, updateColumns, filter, update)
						if err != nil {
							return err
						}
					}
				}
			}
		}
		// 删除切片
		if len(req.DeleteSegBizIds) > 0 {
			err := l.ModifyDocSegmentByOperate(ctx, docCommon, req.DeleteSegBizIds, ModifyDocSegmentDeleteOperate, tx)
			if err != nil {
				return err
			}
		}
		// 停用切片
		if len(req.DisabledSegBizIds) > 0 {
			err := l.ModifyDocSegmentByOperate(ctx, docCommon, req.DisabledSegBizIds, ModifyDocSegmentDisabledOperate, tx)
			if err != nil {
				return err
			}
		}
		// 启用切片
		if len(req.EnableSegBizIds) > 0 {
			err := l.ModifyDocSegmentByOperate(ctx, docCommon, req.EnableSegBizIds, ModifyDocSegmentEnableOperate, tx)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		logx.E(ctx, "ModifyDocSegment|err:%+v", err)
		return rsp, errs.ErrSystem
	}
	return rsp, nil
}

func (l *Logic) ListDocSegmentByKeywords(ctx context.Context, req *kb_pb.ListDocSegmentReq,
	docCommon *segEntity.DocSegmentCommon, num, tempNum int64, rsp *kb_pb.ListDocSegmentRsp) (*kb_pb.ListDocSegmentRsp, error) {
	docSegmentList := make([]*kb_pb.ListDocSegmentRsp_DocSegmentItem, 0)
	logx.I(ctx, "ListDocSegment|Keywords:%s", req.Keywords)
	docSegmentFilter, err := CheckAndConvertFilters(ctx, req.GetFilters())
	if err != nil {
		logx.E(ctx, "ListDocSegmentByKeywords|CheckAndConvertFilters|err:%+v", err)
		return rsp, err
	}
	if len(docSegmentFilter.AuditStatusFilter) != 0 {
		logx.I(ctx, "GetDocSegmentList|AuditStatusFilter %v", docSegmentFilter.AuditStatusFilter)
		offset, limit := utilx.Page(req.PageNumber, req.PageSize)
		// 如果存在审核状态的查询则仅查找临时表数据
		filter := &segEntity.DocSegmentOrgDataTemporaryFilter{
			CorpBizID:   docCommon.CorpBizID,
			AppBizID:    docCommon.AppBizID,
			DocBizID:    docCommon.DocBizID,
			IsDeleted:   ptrx.Bool(false),
			Keywords:    req.Keywords,
			AuditStatus: docSegmentFilter.AuditStatusFilter,
			Offset:      offset,
			Limit:       limit,
			SheetName:   docCommon.SheetName,
		}
		list, err := l.segDao.GetDocTemporaryOrgDataListByKeyWords(ctx, segEntity.DocSegmentOrgDataTemporaryTblColList, filter)
		if err != nil {
			logx.E(ctx, "ListDocSegmentByKeywords|GetDocOrgDataListByKeyWords|err:%+v", err)
			return rsp, err
		}
		tempList, err := TempOriginListToDocSegment(ctx, list)
		if err != nil {
			logx.E(ctx, "ListDocSegmentByKeywords|TempOriginListToDocSegment|err:%+v", err)
			return nil, err
		}
		docSegmentList = append(docSegmentList, tempList...)
		rsp.Total = uint64(len(docSegmentList))
		rsp.SegmentList = docSegmentList
		rsp.IsModify = true
		return rsp, nil
	}
	// 使用字符串匹配，先查找临时表数据(将临时表数据都查出来)，再查找主表
	tempOriginList, err := l.GetOrgDataByKeywords(ctx, req.Keywords, docCommon, tempNum)
	if err != nil {
		return nil, err
	}
	lack := int(req.PageSize*req.PageNumber) - len(tempOriginList)
	logx.I(ctx, "ListDocSegment|lack:%d|len(tempOriginList):%d", lack, len(tempOriginList))
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
		originList, _, err := l.GetDocSegmentOrgData(ctx, req, docCommon, 0, lack)
		if err != nil {
			logx.E(ctx, "ListDocSegment|GetDocSegmentOrgData|err:%+v", err)
			return nil, errs.ErrSystem
		}
		docSegmentList = append(docSegmentList, originList...)
	} else {
		originList, _, err := l.GetDocSegmentOrgData(ctx, req, docCommon, lack-int(req.PageSize)-1, int(req.PageSize))
		if err != nil {
			logx.E(ctx, "ListDocSegment|GetDocSegmentOrgData|err:%+v", err)
			return nil, errs.ErrSystem
		}
		docSegmentList = append(docSegmentList, originList...)
	}
	rsp.Total = uint64(len(docSegmentList))
	rsp.SegmentList = docSegmentList
	rsp.IsModify = true
	return rsp, nil
}

func (l *Logic) GetDocSegmentItemList(ctx context.Context, req *kb_pb.ListDocSegmentReq,
	docCommon *segEntity.DocSegmentCommon) ([]*kb_pb.ListDocSegmentRsp_DocSegmentItem, error) {
	// 不含关键词，直接查询数据库
	docSegmentFilter, err := CheckAndConvertFilters(ctx, req.GetFilters())
	if err != nil {
		logx.E(ctx, "GetDocSegmentItemList|CheckAndConvertFilters|err:%+v", err)
		return nil, err
	}
	offset, limit := utilx.Page(req.PageNumber, req.PageSize)
	if len(docSegmentFilter.AuditStatusFilter) != 0 {
		logx.I(ctx, "GetDocSegmentItemList|AuditStatusFilter")
		// 如果存在审核状态的查询则仅查找临时表数据
		filter := &segEntity.DocSegmentOrgDataTemporaryFilter{
			CorpBizID:   docCommon.CorpBizID,
			AppBizID:    docCommon.AppBizID,
			DocBizID:    docCommon.DocBizID,
			IsDeleted:   ptrx.Bool(false),
			Keywords:    req.Keywords,
			AuditStatus: docSegmentFilter.AuditStatusFilter,
			Offset:      offset,
			Limit:       limit,
			SheetName:   docCommon.SheetName,
		}
		list, err := l.segDao.GetDocTemporaryOrgDataByDocBizID(ctx,
			segEntity.DocSegmentOrgDataTemporaryTblColList, filter)
		if err != nil {
			logx.E(ctx, "GetDocSegmentItemList|GetDocTemporaryOrgDataByDocBizID|err:%+v", err)
			return nil, err
		}
		tempList, err := TempOriginListToDocSegment(ctx, list)
		if err != nil {
			logx.E(ctx, "GetDocSegmentItemList|TempOriginListToDocSegment|err:%+v", err)
			return nil, err
		}
		return tempList, nil
	}
	// 1.获取原始切片
	originList, orgDateBizIDs, err := l.GetDocSegmentOrgData(ctx, req, docCommon, offset, limit)
	if err != nil {
		logx.E(ctx, "GetDocSegmentOrgData failed, err:%+v", err)
		return nil, errs.ErrSystem
	}
	// 2.编辑切片内容替换
	editOriginList, err := l.GetEditOrgData(ctx, orgDateBizIDs, docCommon)
	if err != nil {
		logx.E(ctx, "GetEditOrgData failed, err:%+v", err)
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
		orgDateBizIDs = append(orgDateBizIDs, segEntity.InsertAtFirst)
	}
	insertOriginList, err := l.GetInsertOrgData(ctx, orgDateBizIDs, docCommon)
	if err != nil {
		logx.E(ctx, "GetInsertOrgData failed, err:%+v", err)
		return nil, errs.ErrSystem
	}

	originList, err = l.InsertIntoOrgDataList(ctx, insertOriginList, originList)
	if err != nil {
		logx.E(ctx, "InsertIntoOrgDataList failed, err:%+v", err)
		return nil, errs.ErrSystem
	}
	logx.I(ctx, "GetDocSegmentList|len(originList):%d", len(originList))
	return originList, nil
}

func TempOriginListToDocSegment(ctx context.Context, list []*segEntity.DocSegmentOrgDataTemporary) (
	[]*kb_pb.ListDocSegmentRsp_DocSegmentItem, error) {
	docSegmentList := make([]*kb_pb.ListDocSegmentRsp_DocSegmentItem, 0)
	for _, orgDate := range list {
		pageInfos, pageData := make([]uint64, 0), make([]int64, 0)
		if orgDate.OrgPageNumbers != "" {
			if err := jsonx.UnmarshalFromString(orgDate.OrgPageNumbers, &pageData); err != nil {
				logx.W(ctx, "TempOriginListToDocSegment|PageInfos|UnmarshalFromString|err:%+v", err)
			}
			for _, page := range pageData {
				pageInfos = append(pageInfos, uint64(page))
			}
		}
		docSegmentItem := &kb_pb.ListDocSegmentRsp_DocSegmentItem{
			SegBizId:    orgDate.BusinessID,
			OrgData:     orgDate.OrgData,
			PageInfos:   pageInfos,
			IsOrigin:    false,
			IsAdd:       orgDate.Action == segEntity.InsertAction,
			SegmentType: orgDate.SegmentType,
			IsDisabled:  orgDate.IsDisabled,
			AuditStatus: uint64(orgDate.AuditStatus),
			SheetName:   orgDate.SheetName,
		}
		docSegmentList = append(docSegmentList, docSegmentItem)
	}
	return docSegmentList, nil
}

func (l *Logic) ModifyDocSegmentByOperate(ctx context.Context, docCommon *segEntity.DocSegmentCommon,
	bizIDs []string, operate int, tx *gorm.DB) error {
	logx.I(ctx, "ModifyDocSegmentByOperate|Operate:%d", operate)
	for _, segBizID := range bizIDs {
		if strings.HasPrefix(segBizID, segEntity.EditPrefix) {
			// 校验切片是否存在
			orgData, err := l.segDao.GetDocTemporaryOrgDataByBizID(ctx,
				segEntity.DocSegmentOrgDataTemporaryTblColList, docCommon.CorpBizID, docCommon.AppBizID,
				docCommon.DocBizID, segBizID)
			if err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					logx.E(ctx, "ModifyDocSegmentByOperate|orgData is null")
					return errs.ErrDocSegmentNotFound
				}
				return err
			}
			if orgData == nil {
				logx.E(ctx, "ModifyDocSegmentByOperate|orgData is null")
				return errs.ErrDocSegmentNotFound
			}
			// 获取关联的原始数据
			originOrgDataID, err := util.CheckReqParamsIsUint64(ctx, orgData.OriginOrgDataID)
			if err != nil {
				logx.E(ctx, "ModifyDocSegmentByOperate|DocBizIDToUint64|err:%+v", err)
				return err
			}
			switch operate {
			case ModifyDocSegmentDeleteOperate:
				// 查询是否有关联这个切片的
				relateOrgData, err := l.segDao.GetDocTemporaryOrgDataByLastOrgDataID(ctx,
					segEntity.DocSegmentOrgDataTemporaryTblColList, docCommon.CorpBizID, docCommon.AppBizID,
					docCommon.DocBizID, segBizID)
				if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
					return err
				}
				relateLastOriginOrgDataID := ""
				if relateOrgData != nil {
					var relateLastOrgDataID string
					relateLastOrgDataID, relateLastOriginOrgDataID, err = l.RelateOrgDataProcess(
						ctx, docCommon, originOrgDataID)
					if err != nil {
						return err
					}
					// 更新关联切片使用的last_org_data_id、last_origin_org_data_id
					updateColumns := []string{
						segEntity.DocSegmentOrgDataTemporaryTblColLastOrgDataID,
						segEntity.DocSegmentOrgDataTemporaryTblColLastOriginOrgDataID,
						segEntity.DocSegmentOrgDataTemporaryTblColUpdateTime,
					}
					update := &segEntity.DocSegmentOrgDataTemporary{
						LastOriginOrgDataID: relateLastOriginOrgDataID,
						LastOrgDataID:       relateLastOrgDataID,
						UpdateTime:          time.Now(),
					}
					filter := &segEntity.DocSegmentOrgDataTemporaryFilter{
						CorpBizID:   docCommon.CorpBizID,
						AppBizID:    docCommon.AppBizID,
						DocBizID:    docCommon.DocBizID,
						BusinessIDs: []string{relateOrgData.BusinessID},
					}
					_, err := l.segDao.UpdateDocSegmentTemporaryOrgData(ctx, updateColumns, filter, update)
					if err != nil {
						return err
					}
				}
				// 查询是否有关联这个切片原始切片的
				relateOriginOrgDataList, err := l.GetInsertOrgData(ctx, []string{orgData.OriginOrgDataID}, docCommon)
				if err != nil {
					logx.E(ctx, "GetInsertOrgData failed, err:%+v", err)
					return errs.ErrSystem
				}
				for _, relateOriginOrgData := range relateOriginOrgDataList {
					updateColumns := []string{
						segEntity.DocSegmentOrgDataTemporaryTblColLastOriginOrgDataID,
						segEntity.DocSegmentOrgDataTemporaryTblColUpdateTime,
					}
					update := &segEntity.DocSegmentOrgDataTemporary{
						LastOriginOrgDataID: relateLastOriginOrgDataID,
						UpdateTime:          time.Now(),
					}
					filter := &segEntity.DocSegmentOrgDataTemporaryFilter{
						CorpBizID:   docCommon.CorpBizID,
						AppBizID:    docCommon.AppBizID,
						DocBizID:    docCommon.DocBizID,
						BusinessIDs: []string{relateOriginOrgData.BusinessID},
					}
					_, err := l.segDao.UpdateDocSegmentTemporaryOrgData(ctx, updateColumns, filter, update)
					if err != nil {
						return err
					}
				}
				err = l.segDao.DeleteDocSegmentTemporaryOrgData(ctx, docCommon.CorpBizID,
					docCommon.AppBizID, docCommon.DocBizID, []string{segBizID})
				if err != nil {
					return err
				}
				err = l.TemporaryDeleteDocSegmentOrgData(ctx, docCommon.CorpBizID,
					docCommon.AppBizID, docCommon.DocBizID, []uint64{originOrgDataID})
				if err != nil {
					return err
				}
			case ModifyDocSegmentDisabledOperate:
				err = l.segDao.DisabledDocSegmentTemporaryOrgData(ctx, docCommon.CorpBizID,
					docCommon.AppBizID, docCommon.DocBizID, []string{segBizID})
				if err != nil {
					return err
				}
				err = l.DisabledDocSegmentOrgData(ctx, docCommon.CorpBizID,
					docCommon.AppBizID, docCommon.DocBizID, []uint64{originOrgDataID})
				if err != nil {
					return err
				}
			case ModifyDocSegmentEnableOperate:
				err = l.segDao.EnableDocSegmentTemporaryOrgData(ctx, docCommon.CorpBizID,
					docCommon.AppBizID, docCommon.DocBizID, []string{segBizID})
				if err != nil {
					return err
				}
				err = l.EnableDocSegmentOrgData(ctx, docCommon.CorpBizID,
					docCommon.AppBizID, docCommon.DocBizID, []uint64{originOrgDataID})
				if err != nil {
					return err
				}
			default:
				logx.E(ctx, "ModifyDocSegmentByOperate|no such operate|Operate:%d", operate)
				return errs.ErrSystem
			}
		} else if strings.HasPrefix(segBizID, segEntity.InsertPrefix) {
			// 校验切片是否存在
			orgData, err := l.segDao.GetDocTemporaryOrgDataByBizID(ctx,
				segEntity.DocSegmentOrgDataTemporaryTblColList, docCommon.CorpBizID, docCommon.AppBizID,
				docCommon.DocBizID, segBizID)
			if err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					logx.E(ctx, "ModifyDocSegmentByOperate|orgData is null")
					return errs.ErrDocSegmentNotFound
				}
				return err
			}
			if orgData == nil {
				logx.E(ctx, "ModifyDocSegmentByOperate|orgData is null")
				return errs.ErrDocSegmentNotFound
			}
			switch operate {
			case ModifyDocSegmentDeleteOperate:
				// 查询是否有关联这个切片的
				relateOrgData, err := l.segDao.GetDocTemporaryOrgDataByLastOrgDataID(ctx,
					segEntity.DocSegmentOrgDataTemporaryTblColList, docCommon.CorpBizID, docCommon.AppBizID,
					docCommon.DocBizID, segBizID)
				if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
					return err
				}
				if relateOrgData != nil {
					updateColumns := []string{
						segEntity.DocSegmentOrgDataTemporaryTblColLastOrgDataID,
						segEntity.DocSegmentOrgDataTemporaryTblColLastOriginOrgDataID,
						segEntity.DocSegmentOrgDataTemporaryTblColUpdateTime,
					}
					update := &segEntity.DocSegmentOrgDataTemporary{
						LastOriginOrgDataID: orgData.LastOriginOrgDataID,
						LastOrgDataID:       orgData.LastOrgDataID,
						UpdateTime:          time.Now(),
					}
					filter := &segEntity.DocSegmentOrgDataTemporaryFilter{
						CorpBizID:   docCommon.CorpBizID,
						AppBizID:    docCommon.AppBizID,
						DocBizID:    docCommon.DocBizID,
						BusinessIDs: []string{relateOrgData.BusinessID},
					}
					_, err := l.segDao.UpdateDocSegmentTemporaryOrgData(ctx,
						updateColumns, filter, update)
					if err != nil {
						return err
					}
				}
				// 删除期望删除的切片
				err = l.segDao.DeleteDocSegmentTemporaryOrgData(ctx, docCommon.CorpBizID,
					docCommon.AppBizID, docCommon.DocBizID, []string{segBizID})
				if err != nil {
					return err
				}
			case ModifyDocSegmentDisabledOperate:
				err := l.segDao.DisabledDocSegmentTemporaryOrgData(ctx, docCommon.CorpBizID,
					docCommon.AppBizID, docCommon.DocBizID, []string{segBizID})
				if err != nil {
					return err
				}
			case ModifyDocSegmentEnableOperate:
				err := l.segDao.EnableDocSegmentTemporaryOrgData(ctx, docCommon.CorpBizID,
					docCommon.AppBizID, docCommon.DocBizID, []string{segBizID})
				if err != nil {
					return err
				}
			default:
				logx.E(ctx, "ModifyDocSegmentByOperate|no such operate|Operate:%d", operate)
				return errs.ErrSystem
			}
		} else {
			originOrgDataID, err := util.CheckReqParamsIsUint64(ctx, segBizID)
			if err != nil {
				logx.E(ctx, "ModifyDocSegmentByOperate|DocBizIDToUint64|err:%+v", err)
				return err
			}
			// 校验切片是否存在
			orgData, err := l.GetDocOrgDataByBizID(ctx,
				segEntity.DocSegmentOrgDataTblColList, docCommon.CorpBizID, docCommon.AppBizID,
				docCommon.DocBizID, originOrgDataID)
			if err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					logx.E(ctx, "ModifyDocSegmentByOperate|orgData is null")
					return errs.ErrDocSegmentNotFound
				}
				return err
			}
			if orgData == nil {
				logx.E(ctx, "ModifyDocSegmentByOperate|orgData is null")
				return errs.ErrDocSegmentNotFound
			}
			// 如果切片被编辑过，则阻止操作
			tempOrgData, err := l.segDao.GetDocTemporaryOrgDataByOriginOrgDataID(ctx,
				segEntity.DocSegmentOrgDataTemporaryTblColList, docCommon.CorpBizID, docCommon.AppBizID,
				docCommon.DocBizID, segBizID)
			if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
				return err
			}
			if tempOrgData != nil {
				logx.E(ctx, "ModifyDocSegmentByOperate|orgData is edit, operation not allowed")
				return errs.ErrDocSegmentOperationNotAllowedFailed
			}
			switch operate {
			case ModifyDocSegmentDeleteOperate:
				// 查询是否有关联这个切片的
				relateOrgData, err := l.segDao.GetDocTemporaryOrgDataByLastOrgDataID(ctx,
					segEntity.DocSegmentOrgDataTemporaryTblColList, docCommon.CorpBizID, docCommon.AppBizID,
					docCommon.DocBizID, segBizID)
				if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
					return err
				}
				relateLastOriginOrgDataID := ""
				if relateOrgData != nil {
					var relateLastOrgDataID string
					relateLastOrgDataID, relateLastOriginOrgDataID, err = l.RelateOrgDataProcess(
						ctx, docCommon, originOrgDataID)
					if err != nil {
						return err
					}
					updateColumns := []string{
						segEntity.DocSegmentOrgDataTemporaryTblColLastOrgDataID,
						segEntity.DocSegmentOrgDataTemporaryTblColLastOriginOrgDataID,
						segEntity.DocSegmentOrgDataTemporaryTblColUpdateTime,
					}
					update := &segEntity.DocSegmentOrgDataTemporary{
						LastOriginOrgDataID: relateLastOriginOrgDataID,
						LastOrgDataID:       relateLastOrgDataID,
						UpdateTime:          time.Now(),
					}
					filter := &segEntity.DocSegmentOrgDataTemporaryFilter{
						CorpBizID:   docCommon.CorpBizID,
						AppBizID:    docCommon.AppBizID,
						DocBizID:    docCommon.DocBizID,
						BusinessIDs: []string{relateOrgData.BusinessID},
					}
					_, err := l.segDao.UpdateDocSegmentTemporaryOrgData(ctx,
						updateColumns, filter, update)
					if err != nil {
						return err
					}
				}
				// 查询是否有关联原始新增切片的，更新关联的原始切片
				relateOriginOrgDataList, err := l.GetInsertOrgData(ctx, []string{segBizID}, docCommon)
				if err != nil {
					logx.E(ctx, "GetInsertOrgData failed, err:%+v", err)
					return errs.ErrSystem
				}
				for _, relateOriginOrgData := range relateOriginOrgDataList {
					updateColumns := []string{
						segEntity.DocSegmentOrgDataTemporaryTblColLastOriginOrgDataID,
						segEntity.DocSegmentOrgDataTemporaryTblColUpdateTime,
					}
					update := &segEntity.DocSegmentOrgDataTemporary{
						LastOriginOrgDataID: relateLastOriginOrgDataID,
						UpdateTime:          time.Now(),
					}
					filter := &segEntity.DocSegmentOrgDataTemporaryFilter{
						CorpBizID:   docCommon.CorpBizID,
						AppBizID:    docCommon.AppBizID,
						DocBizID:    docCommon.DocBizID,
						BusinessIDs: []string{relateOriginOrgData.BusinessID},
					}
					_, err := l.segDao.UpdateDocSegmentTemporaryOrgData(ctx, updateColumns, filter, update)
					if err != nil {
						return err
					}
				}
				err = l.TemporaryDeleteDocSegmentOrgData(ctx, docCommon.CorpBizID,
					docCommon.AppBizID, docCommon.DocBizID, []uint64{originOrgDataID})
				if err != nil {
					return err
				}
			case ModifyDocSegmentDisabledOperate:
				err = l.DisabledDocSegmentOrgData(ctx, docCommon.CorpBizID,
					docCommon.AppBizID, docCommon.DocBizID, []uint64{originOrgDataID})
				if err != nil {
					return err
				}
			case ModifyDocSegmentEnableOperate:
				err = l.EnableDocSegmentOrgData(ctx, docCommon.CorpBizID,
					docCommon.AppBizID, docCommon.DocBizID, []uint64{originOrgDataID})
				if err != nil {
					return err
				}
			default:
				logx.E(ctx, "ModifyDocSegmentByOperate|no such operate|Operate:%d", operate)
				return errs.ErrSystem
			}
		}
	}
	return nil
}

func (l *Logic) GetRefersByBusinessID(ctx context.Context, businessID uint64) (*entity.Refer, error) {
	/*
		`
			SELECT
				%s
			FROM
			    t_refer
			WHERE
			    business_id = ?
		`
	*/
	filter := &entity.ReferFilter{
		BusinessID: businessID,
	}
	refers, err := l.docDao.GetReferListByFilter(ctx, []string{}, filter)
	if err != nil {
		return nil, err
	}
	if len(refers) == 0 {
		return nil, errs.ErrGetReferFail
	}
	return refers[0], nil
}

func (l *Logic) GetSegmentByReferID(ctx context.Context, appID uint64, referBizID string) (*segEntity.DocSegment, error) {
	referBizIDInt, err := util.CheckReqParamsIsUint64(ctx, referBizID)
	if err != nil {
		return nil, err
	}
	refer, err := l.GetRefersByBusinessID(ctx, referBizIDInt)
	if err != nil {
		logx.E(ctx, "ListDocSegment|GetRefersByBusinessIDs err|err:%+v", err)
		return nil, errs.ErrGetReferFail
	}

	segmentID := refer.RelateID

	if refer.DocType == entity.DocTypeQA {
		logx.I(ctx, "GetSegmentByReferID|qa type refer|find qa related segment for qaID:%d", refer.RelateID)
		qa, err := l.qaDao.GetQAByID(ctx, refer.RelateID)
		if err != nil {
			logx.E(ctx, "GetSegmentByReferID|GetReleatedDocQAByFilter err|err:%+v", err)
			return nil, err
		}
		if qa == nil {
			logx.E(ctx, "GetSegmentByReferID|GetReleatedDocQAByFilter err|qa is nil")
			return nil, errs.ErrDocSegmentNotFound
		}
		segmentID = qa.SegmentID
	}
	logx.I(ctx, "GetSegmentByReferID|segmentID:%d", segmentID)
	segment, err := l.GetSegmentByID(ctx, segmentID, appID)
	if err != nil {
		logx.E(ctx, "GetSegmentByReferID|GetSegmentByID err|err:%+v", err)
		return nil, errs.ErrDocSegmentNotFound
	}
	if segment == nil {
		logx.E(ctx, "GetSegmentByReferID|GetSegmentByID err|segment is nil")
		return nil, errs.ErrDocSegmentNotFound
	}
	return &segment.DocSegment, nil
}

func CheckAndConvertFilters(ctx context.Context, filters []*kb_pb.FilterItem) (*segEntity.DocSegmentFilter, error) {
	docSegmentFilter := new(segEntity.DocSegmentFilter)
	// 校验筛选条件内容
	if len(filters) > 0 {
		for _, filter := range filters {
			if key, ok := segEntity.DocSegmentFilterKeyMap[filter.FilterKey]; ok {
				switch key {
				case segEntity.DocSegmentFilterKeyAuditStatus:
					for _, value := range filter.FilterValue {
						if v, ok := segEntity.DocSegmentFilterAuditStatusMap[value]; ok {
							docSegmentFilter.AuditStatusFilter = append(docSegmentFilter.AuditStatusFilter, uint32(v))
						} else {
							// 过滤值未找到
							logx.E(ctx, "ListDocSegment|FilterValue not found|FilterKey:%s|FilterValue:%s",
								filter.FilterKey, value)
							return docSegmentFilter, errs.ErrDocSegmentFilterInvalid
						}
					}
				}
			} else {
				// 过滤条件未找到
				logx.E(ctx, "ListDocSegment|FilterKey not found|FilterKey:%s", filter.FilterKey)
				return docSegmentFilter, errs.ErrDocSegmentFilterInvalid
			}
		}
	}
	return docSegmentFilter, nil
}

// SegmentCommonIDsToBizIDs 基础信息获取
func (l *Logic) SegmentCommonIDsToBizIDs(ctx context.Context, corpID, appID, staffID, docID uint64) (
	uint64, uint64, uint64, uint64, error) {
	corpBizID, appBizID, staffBizID, docBizID := uint64(0), uint64(0), uint64(0), uint64(0)
	corp, err := l.rpc.PlatformAdmin.DescribeCorpByPrimaryId(ctx, corpID)
	if err != nil || corp == nil {
		return 0, 0, 0, 0, errs.ErrCorpNotFound
	}
	corpBizID = corp.GetCorpId()
	app, err := l.rpc.AppAdmin.DescribeAppByPrimaryIdWithoutNotFoundError(ctx, appID)
	if err != nil || app == nil {
		return 0, 0, 0, 0, errs.ErrAppNotFound
	}
	appBizID = app.BizId
	if staffID != 0 {

		staff, err := l.rpc.PlatformAdmin.GetStaffByID(ctx, staffID)
		if err != nil || staff == nil {
			return 0, 0, 0, 0, errs.ErrStaffNotFound
		}
		staffBizID = staff.BusinessID
	}
	if docID != 0 {

		filter := &docEntity.DocFilter{
			ID:      docID,
			CorpId:  corpID,
			RobotId: appID,
		}

		tbl := l.docDao.Query().TDoc
		tableName := tbl.TableName()

		db, err := knowClient.GormClient(ctx, tableName, filter.RobotId, filter.RouterAppBizID, []client.Option{}...)
		if err != nil {
			logx.E(ctx, "get GormClient failed, err: %+v", err)
			return 0, 0, 0, 0, err
		}

		doc, err := l.docDao.GetDocByDocFilter(ctx, docEntity.DocTblColList, filter, db)

		if err != nil || doc == nil {
			return 0, 0, 0, 0, errs.ErrDocNotFound
		}
		docBizID = doc.BusinessID
	}
	return corpBizID, appBizID, staffBizID, docBizID, nil
}

// BatchDeleteSegments 批量删除文档分片, 超量隔离专用
func (l *Logic) BatchDeleteSegments(ctx context.Context, segments []*segEntity.DocSegmentExtend, robotID uint64) error {
	var allImagesIDs []uint64
	docSegmentTableName := l.segDao.Query().TDocSegment.TableName()
	db, err := knowClient.GormClient(ctx, docSegmentTableName, robotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "GetSegmentByDocID get GormClient err:%v,robotID:%v", err, robotID)
		return err
	}
	if err := db.Transaction(func(tx *gorm.DB) error {
		for _, segment := range segments {
			/*
					`
				UPDATE
					t_doc_segment
				SET
				    is_deleted = :is_deleted,
				    update_time = :update_time,
				    release_status = :release_status,
				    next_action = :next_action
				WHERE
				    id = :id
			*/
			segment.IsDeleted = segEntity.SegmentIsDeleted
			segment.UpdateTime = time.Now()
			if !segment.IsNextActionAdd() {
				segment.NextAction = qaEntity.NextActionDelete
				segment.ReleaseStatus = segEntity.SegmentReleaseStatusInit
			}

			updateColumnList := []string{
				segEntity.DocSegmentTblColIsDeleted, segEntity.DocSegmentTblColUpdateTime,
				segEntity.DocSegmentTblColReleaseStatus, segEntity.DocSegmentTblColNextAction,
			}

			filter := &segEntity.DocSegmentFilter{
				ID: segment.ID,
			}

			if err := l.segDao.UpdateDocSegmentWithTx(ctx, updateColumnList, filter, &segment.DocSegment, tx); err != nil {
				logx.E(ctx, "删除文档分片失败 err:%+v", err)
				return err

			}

			// 切片页码
			/*
						`
					UPDATE
						t_doc_segment_page_info
					SET
					    is_deleted = ?,
					    update_time = ?
					WHERE
					    robot_id = ? AND segment_id = ?
				`
			*/

			pageFilter := &segEntity.DocSegmentPageInfoFilter{
				AppID:     robotID,
				SegmentID: segment.ID,
			}

			updateColumns := map[string]any{
				segEntity.DocSegmentPageInfoTblColIsDeleted:  segment.IsDeleted,
				segEntity.DocSegmentPageInfoTblColUpdateTime: time.Now(),
			}

			if err := l.segDao.BatchUpdateDocSegmentPageInfos(ctx, pageFilter, updateColumns,
				tx); err != nil {
				logx.E(ctx, "删除文档分片页码失败 err:%+v", err)
				return err
			}

			// 图片ID
			/*
						`
					SELECT
						DISTINCT image_id
					FROM
					    t_doc_segment_image
					WHERE
					    robot_id = ? AND segment_id = ? AND is_deleted = ?
				`
			*/
			deleteFlag := segEntity.SegmentIsNotDeleted
			imageFilter := &segEntity.DocSegmentImageFilter{
				AppID:          segment.RobotID,
				SegmentID:      segment.ID,
				IsDeleted:      &deleteFlag,
				DistinctColumn: []string{segEntity.DocSegmentImageTblColImageId},
			}
			imageIds := make([]uint64, 0)
			list, err := l.segDao.GetDocSegmentImageListWithTx(ctx, []string{}, imageFilter,
				tx)
			if err != nil {
				logx.E(ctx, "获取文档分片图片失败 err:%+v", err)
				return err
			}
			for _, image := range list {
				imageIds = append(imageIds, image.ImageID)
			}
			// 切片图片
			/*
						`
					UPDATE
						t_doc_segment_image

					SET
					    is_deleted = ?,
					    update_time = ?
					WHERE
					    robot_id = ? AND segment_id = ?
				`
			*/
			imageFilter = &segEntity.DocSegmentImageFilter{
				AppID:     segment.RobotID,
				SegmentID: segment.ID,
			}

			updateColumns = map[string]any{
				segEntity.DocSegmentImageTblColIsDeleted:  segment.IsDeleted,
				segEntity.DocSegmentImageTblColUpdateTime: time.Now(),
			}

			if err := l.segDao.BatchUpdateDocSegmentImages(ctx, imageFilter, updateColumns,
				tx); err != nil {
				logx.E(ctx, "删除文档分片图片失败 err:%+v", err)
				return err

			}
			allImagesIDs = append(allImagesIDs, imageIds...)
		}
		return nil
	}); err != nil {
		return err
	}
	// 删除图片向量
	err = l.vectorSyncLogic.GetDao().UpdateImageVectorDeleteStatus(
		ctx, robotID, allImagesIDs, segEntity.SegmentImageVectorIsDeleted)
	if err != nil {
		return err
	}

	return nil
}

// DeleteSegmentsForQA 删除用于生成QA的分片
func (l *Logic) DeleteSegmentsForQA(ctx context.Context, doc *docEntity.Doc) error {
	/*
		`
			UPDATE
				t_doc_segment
			SET
			    is_deleted = ?,
			    update_time = ?
			WHERE
			    doc_id = ? AND is_deleted = ? AND type = ?
		`
	*/

	docSegmentTableName := l.segDao.Query().TDocSegment.TableName()
	db, err := knowClient.GormClient(ctx, docSegmentTableName, doc.RobotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "GetSegmentByDocID get GormClient err:%v,robotID:%v", err, doc.RobotID)
		return err
	}

	updateColumns := map[string]any{
		segEntity.DocSegmentTblColIsDeleted:  segEntity.SegmentIsDeleted,
		segEntity.DocSegmentTblColUpdateTime: time.Now(),
	}

	deleteFlag := segEntity.SegmentIsNotDeleted

	filter := &segEntity.DocSegmentFilter{
		DocID:     doc.ID,
		Type:      segEntity.SegmentTypeQA,
		IsDeleted: &deleteFlag,
	}

	if err := l.segDao.BatchUpdateDocSegmentByFilter(ctx, filter, updateColumns, db); err != nil {
		logx.E(ctx, "Failed to DeleteSegmentsForQA. err:%+v", err)
		return err

	}
	return nil
}

// DeleteSegmentsForIndex 删除用于写向量的分片
func (l *Logic) DeleteSegmentsForIndex(ctx context.Context, doc *docEntity.Doc,
	embeddingModel string, embeddingVersion uint64) error {
	docSegmentTableName := l.segDao.Query().TDocSegment.TableName()
	_, err := knowClient.GormClient(ctx, docSegmentTableName, doc.RobotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "GetSegmentByDocID get GormClient err:%v,robotID:%v", err, doc.RobotID)
		return err
	}
	if err := l.segDao.Query().Transaction(func(tx *mysqlquery.Query) error {
		/*now := time.Now()
		querySQL := deleteSegmentForType
		args := []any{model.SegmentIsDeleted, now, doc.ID, model.SegmentIsNotDeleted, model.SegmentTypeIndex}
		if _, err := tx.ExecContext(ctx, querySQL, args...); err != nil {
			logx.E(ctx, "删除用于写向量的分片 sql:%s args:%+v err:%+v", querySQL, args, err)
			return err
		}
		querySQL = fmt.Sprintf(deleteSegmentPageInfoByDocID, placeholder(1))
		args = []any{model.SegmentIsDeleted, now, doc.AppPrimaryId, doc.ID, model.SegmentIsNotDeleted}
		if _, err := tx.ExecContext(ctx, querySQL, args...); err != nil {
			logx.E(ctx, "删除文档分片页码失败 sql:%s args:%+v err:%+v", querySQL, args, err)
			return err
		}
		querySQL = fmt.Sprintf(deleteSegmentImageByDocID, placeholder(1))
		args = []any{model.SegmentIsDeleted, now, doc.AppPrimaryId, doc.ID, model.SegmentIsNotDeleted}
		if _, err := tx.ExecContext(ctx, querySQL, args...); err != nil {
			logx.E(ctx, "删除文档分片图片失败 sql:%s args:%+v err:%+v", querySQL, args, err)
			return err
		}*/
		// 逻辑删除文档对应的org_data

		corpBizID, appBizID, _, docBizID, err := l.SegmentCommonIDsToBizIDs(ctx, doc.CorpID,
			doc.RobotID, 0, doc.ID)
		if err != nil {
			logx.E(ctx, "SegmentCommonIDsToBizIDs|doc:%+v|err:%+v", doc, err)
			return err
		}
		if err := l.BatchDeleteSegmentsAndKnowledge(ctx, doc, embeddingModel, embeddingVersion); err != nil {
			logx.E(ctx, "BatchDeleteSegmentsAndKnowledge|doc:%+v|err:%+v", doc, err)
			return err
		}
		req := retrieval_pb.DeleteBigDataElasticReq{
			RobotId:    doc.RobotID,
			DocId:      doc.ID,
			Type:       retrieval_pb.KnowledgeType_KNOWLEDGE,
			HardDelete: true,
		}
		if err = l.rpc.RetrievalDirectIndex.DeleteBigDataElastic(ctx, &req); err != nil {
			logx.E(ctx, "DeleteBigDataElastic|doc:%+v|err:%+v", doc, err)
			return err
		}
		if docEntity.IsTableTypeDocument(doc.FileType) {
			err = l.DeleteSheetDbTableAndColumns(ctx, l.dbDao, corpBizID, appBizID, doc.BusinessID, doc.RobotID)
			if err != nil {
				logx.E(ctx, "deleteSheetDbTableAndColumns %v, %v", doc.BusinessID, err)
				return err
			}
		}
		req2 := retrieval_pb.DeleteText2SQLReq{
			RobotId:     doc.RobotID,
			DocId:       doc.ID,
			SegmentType: segEntity.SegmentTypeText2SQLContent,
		}
		if err = l.rpc.RetrievalDirectIndex.DeleteText2SQL(ctx, &req2); err != nil {
			logx.E(ctx, "DeleteText2SQL %v, %v", doc.BusinessID, err)
			return err
		}
		err = l.BatchDeleteDocOrgDataByDocBizID(ctx, corpBizID,
			appBizID, docBizID, 10000)
		if err != nil {
			logx.E(ctx, "BatchDeleteDocOrgDataByDocBizID|doc:%+v|err:%+v", doc, err)
			return err
		}
		return nil
	}); err != nil {
		logx.E(ctx, "删除用于写向量的分片和chunk失败 err:%+v", err)
		return err
	}
	return nil
}

func (l *Logic) BatchDeleteSegmentsAndKnowledge(ctx context.Context, doc *docEntity.Doc,
	embeddingModel string, embeddingVersion uint64) error {
	appDB, err := l.rpc.AppAdmin.DescribeAppByPrimaryIdWithoutNotFoundError(ctx, doc.RobotID)
	if err != nil {
		return err
	}
	newCtx := util.SetMultipleMetaData(ctx, appDB.SpaceId, appDB.Uin)
	total, err := l.GetSegmentListCount(ctx, doc.CorpID, doc.ID, doc.RobotID)
	if err != nil {
		return err
	}
	pageSize := 500
	pages := int(math.Ceil(float64(total) / float64(pageSize)))
	for i := 1; i <= pages; i++ {
		page := uint32(i)
		docSegments, err := l.GetSegmentList(ctx, doc.CorpID, doc.ID, page, uint32(pageSize), doc.RobotID)
		if err != nil {
			return err
		}
		deleteKnowledgeSegments := make([]*segEntity.DocSegmentExtend, 0)
		for _, seg := range docSegments {
			if !seg.IsSegmentForQA() && !seg.IsText2sqlSegmentType() {
				deleteKnowledgeSegments = append(deleteKnowledgeSegments, seg)
			}
		}
		if err = l.BatchDeleteSegments(ctx, docSegments, doc.RobotID); err != nil {
			return err
		}
		if len(deleteKnowledgeSegments) > 0 {
			if err = l.vectorSyncLogic.BatchDirectDeleteSegmentKnowledge(newCtx, appDB.PrimaryId,
				deleteKnowledgeSegments, embeddingVersion, embeddingModel); err != nil {
				return err
			}
		}
	}
	return nil
}

func (l *Logic) DeleteSheetDbTableAndColumns(ctx context.Context, dbDao dbdao.Dao, corpBizID, appBizID, docBizID, robotID uint64) error {
	metaMappings := make([]*docEntity.Text2sqlMetaMappingPreview, 0)

	metaMappings, err := l.docDao.GetDocMetaDataByDocId(ctx, docBizID, robotID)
	if err != nil {
		logx.E(ctx, "GetDocMetaDataByDocId err: %v", err)
		return err
	}

	for _, mapping := range metaMappings {
		// isExist, err := dao.GetDBTableDao().Text2sqlExistsByDbSourceBizID(ctx, corpBizID, appBizID, mapping.BusinessID)
		// if err != nil {
		// 	return err
		// }
		// if !isExist {
		// 	continue
		// }

		tableFilter := dbentity.TableFilter{
			CorpBizID:     corpBizID,
			AppBizID:      appBizID,
			DBSourceBizID: mapping.BusinessID,
		}
		dbTable, err := dbDao.DescribeTable(ctx, &tableFilter)
		// dbTable, err := dao.GetDBTableDao().Text2sqlGetByDbSourceBizID(ctx, corpBizID, appBizID, mapping.BusinessID)
		if err != nil {
			if errors.Is(err, errx.ErrNotFound) {
				continue // 不存在就 continue
			}
			return err
		}
		logx.I(ctx, "delete db table %v %v for doc", dbTable.DBTableBizID, dbTable.AliasName)

		tableFilter = dbentity.TableFilter{
			CorpBizID:    corpBizID,
			AppBizID:     appBizID,
			DBTableBizID: dbTable.DBTableBizID,
		}
		err = dbDao.DeleteTable(ctx, &tableFilter)
		// err = dao.GetDBTableDao().SoftDeleteByBizID(ctx, corpBizID, appBizID, dbTable.DBTableBizID)
		if err != nil {
			return err
		}

		columnFilter := dbentity.ColumnFilter{
			CorpBizID:    corpBizID,
			AppBizID:     appBizID,
			DBTableBizID: dbTable.DBTableBizID,
		}
		err = dbDao.SoftDeleteByTableBizID(ctx, &columnFilter)
		if err != nil {
			return err
		}
	}
	return nil
}

// UpdateSegmentSyncOrgDataBizID 更新同步文段 org_data_biz_id
func (l *Logic) UpdateSegmentSyncOrgDataBizID(ctx context.Context, robotID, docID, corpID, staffID uint64,
	ids []uint64, orgDataBizID uint64) error {
	/*
				`
				UPDATE
					t_doc_segment
				SET
				    org_data_biz_id = ?
				WHERE
				    corp_id = ? AND robot_id = ? AND doc_id = ? AND staff_id = ? AND id IN (%s)
		    `
	*/

	docSegmentTableName := l.segDao.Query().TDocSegment.TableName()
	db, err := knowClient.GormClient(ctx, docSegmentTableName, robotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "GetSegmentByDocID get GormClient err:%v,robotID:%v", err, robotID)
		return err
	}

	filter := &segEntity.DocSegmentFilter{
		AppID:   robotID,
		CorpID:  corpID,
		StaffID: staffID,
		DocID:   docID,
	}

	updateColumns := map[string]any{
		segEntity.DocSegmentTblColOrgDataBizID:       orgDataBizID,
		segEntity.DocSegmentPageInfoTblColUpdateTime: time.Now(),
	}

	if err := db.Transaction(func(tx *gorm.DB) error {
		total := len(ids)
		pageSize := 500
		pages := int(math.Ceil(float64(total) / float64(pageSize)))
		for i := 0; i < pages; i++ {
			start := i * pageSize
			end := (i + 1) * pageSize
			if end > total {
				end = total
			}
			tmpIDs := ids[start:end]
			filter.IDs = tmpIDs
			if err := l.segDao.BatchUpdateDocSegmentByFilter(ctx, filter, updateColumns,
				db); err != nil {
				logx.E(ctx, "UpdateSegmentSyncOrgDataBizID error. err:%+v", err)
				return err
			}
		}
		return nil
	}); err != nil {
		logx.E(ctx, "Failed to UpdateSegmentSyncOrgDataBizID. err:%+v", err)
		return err
	}

	return nil

}

// UpdateQaSegmentStatus 更新分片生成QA状态
func (l *Logic) UpdateQaSegmentStatus(ctx context.Context, segment *segEntity.DocSegmentExtend, robotID uint64) error {
	/*
		`
			UPDATE
			    t_doc_segment
			SET
			    status = :status,
			    update_time = :update_time
			WHERE
			    id=:id
		`
	*/

	segment.UpdateTime = time.Now()

	docSegmentTableName := l.segDao.Query().TDocSegment.TableName()
	db, err := knowClient.GormClient(ctx, docSegmentTableName, robotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "UpdateQaSegmentStatus get GormClient err:%v,robotID:%v", err, robotID)
		return err
	}

	updateColumnList := []string{
		segEntity.DocSegmentTblColStatus, segEntity.DocSegmentTblColUpdateTime,
	}

	filter := &segEntity.DocSegmentFilter{
		ID: segment.ID,
	}

	if err := l.segDao.UpdateDocSegmentWithTx(ctx, updateColumnList, filter, &segment.DocSegment, db); err != nil {
		logx.E(ctx, "Failed to UpdateQaSegmentStatus. err:%+v", err)
		return err

	}
	return nil
}

// UpdateQaSegmentToDocStatus 还原切片状态
func (l *Logic) UpdateQaSegmentToDocStatus(ctx context.Context, docID uint64, batchID int, robotID uint64) error {
	/*
		`
			UPDATE
			    t_doc_segment
			SET
			    status = ?,
			    update_time = NOW()
			WHERE
			    doc_id= ? AND batch_id = ?
	*/

	docSegmentTableName := l.segDao.Query().TDocSegment.TableName()
	db, err := knowClient.GormClient(ctx, docSegmentTableName, robotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "UpdateQaSegmentStatus get GormClient err:%v,robotID:%v", err, robotID)
		return err
	}

	updateColumnList := map[string]any{
		segEntity.DocSegmentTblColStatus:     segEntity.SegmentStatusDone,
		segEntity.DocSegmentTblColUpdateTime: time.Now(),
	}

	filter := &segEntity.DocSegmentFilter{
		DocID:   docID,
		BatchID: batchID,
	}

	if err := l.segDao.BatchUpdateDocSegmentByFilter(ctx, filter, updateColumnList, db); err != nil {
		logx.E(ctx, "Failed to UpdateQaSegmentToDocStatus. err:%+v", err)
		return err

	}
	return nil
}

// GetSliceTable 获取切片表格数据
func (l *Logic) GetSliceTable(orgData string, tag int) string {
	startLine := 0
	lines := strings.Split(orgData, "\n")
	for i := range lines {
		// 判断是否为markdown
		if i > 0 {
			tableLine := markdown.IsTableLine(lines[i-1])
			separatorLine := markdown.IsSeparatorLine(lines[i])
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
