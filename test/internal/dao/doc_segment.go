package dao

import (
	"context"
	"crypto/sha256"
	"fmt"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"io"
	"math"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"git.woa.com/dialogue-platform/lke_proto/pb-protocol/knowledge"
	jsoniter "github.com/json-iterator/go"

	"git.code.oa.com/trpc-go/trpc-go/client"
	"git.woa.com/baicaoyuan/moss/types/slicex"
	knowClient "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
	"github.com/jmoiron/sqlx"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_retrieval_server"
)

const (
	docSegmentFields = `
		id,business_id,robot_id,corp_id,staff_id,doc_id,file_type,title,page_content,org_data,org_data_biz_id,outputs,cost_time,
		split_model,status,release_status,message,is_deleted,type,next_action,batch_id,rich_text_index,start_index,
		end_index,linker_keep,update_time,create_time,big_data_id, big_start_index, big_end_index, segment_type`
	createSegment = `
		INSERT INTO
		    t_doc_segment (%s)
		VALUES
		    (null,:business_id,:robot_id,:corp_id,:staff_id,:doc_id,:file_type,:title,:page_content,:org_data,
		     :org_data_biz_id,:outputs,:cost_time,:split_model,:status,:release_status,:message,:is_deleted,
		     :type,:next_action,:batch_id,:rich_text_index,:start_index,:end_index,:linker_keep,:update_time,
		     :create_time,:big_data_id, :big_start_index, :big_end_index, :segment_type)`
	getSegmentByID = `
		SELECT
			%s
		FROM
		    t_doc_segment
		WHERE
		    id = ?
	`
	getSegmentByIDs           = `SELECT %s FROM t_doc_segment WHERE id IN(?)`
	getPagedSegmentIDsByDocID = `
		SELECT
			id
		FROM
		    t_doc_segment
		WHERE
		    doc_id = ? AND is_deleted = ? ORDER BY id ASC LIMIT ?, ?
	`
	getSegmentIDByDocIDAndBatchID = `
		SELECT
			id
		FROM
		    t_doc_segment
		WHERE
		    doc_id = ? AND batch_id = ? AND is_deleted = ? order by id desc
	`
	getQASegmentIDByDocIDAndBatchID = `
		SELECT
			id
		FROM
		    t_doc_segment
		WHERE
		    doc_id = ? AND batch_id = ? AND is_deleted = ? AND status != ?
	`
	updateOutputs = `
		UPDATE
		    t_doc_segment
		SET
		    outputs = :outputs,
		    cost_time = :cost_time,
		    update_time = :update_time
		WHERE
		    id=:id
	`
	updatesQaStatus = `
		UPDATE
		    t_doc_segment
		SET
		    status = :status,
		    update_time = :update_time
		WHERE
		    id=:id
	`

	updateQaSegmentToDocStatus = `
		UPDATE
		    t_doc_segment
		SET
		    status = ?,
		    update_time = NOW()
		WHERE
		    doc_id= ? AND batch_id = ? AND is_deleted = ?
	`
	updateSegmentDone = `
		UPDATE
		    t_doc_segment
		SET
		    status = :status,
		    update_time = :update_time
		WHERE
		    id = :id
	`
	getInitSegmentNum = `
		SELECT
			COUNT(*)
		FROM
		    t_doc_segment
		WHERE
		    doc_id = ? AND status = ?
	`
	getReleaseSegmentCount = `
		SELECT
			count(*)
		FROM
		    t_doc_segment
		WHERE
			doc_id = ? AND release_status = ?
	`
	getReleaseSegmentList = `
		SELECT
			%s
		FROM
		    t_doc_segment
		WHERE
			doc_id = ? AND release_status = ?
		LIMIT ?,?
	`
	publishSegment = `
		UPDATE
			t_doc_segment
		SET
		    update_time = :update_time,
		    release_status = :release_status,
		    message = :message,
		    next_action = :next_action
		WHERE
		    id = :id
	`
	deleteSegment = `
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
	deleteSegmentForType = `
		UPDATE
			t_doc_segment
		SET
		    is_deleted = ?,
		    update_time = ?
		WHERE
		    doc_id = ? AND is_deleted = ? AND type = ?
	`
	getSegmentCount = `
		SELECT
			count(*)
		FROM
		    t_doc_segment
		WHERE
		     corp_id = ? AND doc_id = ? AND is_deleted = ?
	`
	getSegmentList = `
		SELECT
			%s
		FROM
		    t_doc_segment
		WHERE
		     corp_id = ? AND doc_id = ? AND is_deleted = ?
		LIMIT
		     ?,?
	`
	updateSegmentLastAction = `
		UPDATE
			t_doc_segment
		SET
		    next_action = ?, update_time = ?
		WHERE
		    id IN (%s)
	`
	batchUpdateSegmentReleaseStatus = `
		UPDATE
			t_doc_segment
		SET
		    update_time = ?,
		    release_status = ?
		WHERE
		    id IN (%s)
	`
	updateSegment = `
		UPDATE
			t_doc_segment
		SET
		    update_time = :update_time,
		    release_status = :release_status,
		    next_action = :next_action
		WHERE
		    id = :id
	`
	updateSegmentContent = `
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
	updateSegmentReleaseStatus = `
		UPDATE
			t_doc_segment
		SET
		    update_time = :update_time,
		    release_status = :release_status
		WHERE
		    id = :id
	`
	getSegmentChunk = `
		SELECT ` + docSegmentFields + ` FROM t_doc_segment
		WHERE corp_id = ? AND robot_id = ? AND is_deleted = ? AND (type = ? OR type = ?) AND segment_type NOT IN (?,?,?) AND id > ?
		ORDER BY id ASC LIMIT ?
	`
	getSegmentChunkCount = `
		SELECT COUNT(*) FROM t_doc_segment
		WHERE corp_id = ? AND robot_id = ? AND is_deleted = ? AND (type = ? OR type = ?)
	`

	getSegmentSyncChunk = `
		SELECT ` + docSegmentFields + ` FROM t_doc_segment
		WHERE corp_id = ? AND robot_id = ? AND is_deleted = ? AND (type = ? OR type = ?) AND segment_type NOT IN (?,?) AND org_data_biz_id = 0 AND id > ?
		ORDER BY id ASC LIMIT ?
	`
	getSegmentSyncChunkCount = `
		SELECT COUNT(*) FROM t_doc_segment
		WHERE corp_id = ? AND robot_id = ? AND is_deleted = ? AND (type = ? OR type = ?) AND org_data_biz_id = 0
	`

	updateSegmentOrdDataBizIDByID = `
		UPDATE
			t_doc_segment
		SET
		    org_data_biz_id = ?
		WHERE
		    corp_id = ? AND robot_id = ? AND doc_id = ? AND staff_id = ? AND id IN (%s) 
    `

	getText2SqlSegmentMetaByDocID = `
		SELECT
			id, page_content
		FROM
		    t_doc_segment
		WHERE
		    doc_id = ? AND segment_type = ? AND is_deleted = ?
	`

	docSegmentImageFields = `
		id,image_id,segment_id,doc_id,robot_id,corp_id,staff_id,original_url,external_url,
		is_deleted,create_time,update_time
	`

	createSegmentImage = `
		INSERT INTO
		    t_doc_segment_image (%s)
		VALUES
		    (null,:image_id,:segment_id,:doc_id,:robot_id,:corp_id,:staff_id,:original_url,:external_url,
		     :is_deleted,:create_time,:update_time)
	`

	getSegmentImageIdsBySegID = `
		SELECT
			DISTINCT image_id
		FROM
		    t_doc_segment_image
		WHERE
		    robot_id = ? AND segment_id = ? AND is_deleted = ?
	`

	deleteSegmentImageBySegID = `
		UPDATE
			t_doc_segment_image
		SET
		    is_deleted = ?,
		    update_time = ?
		WHERE
		    robot_id = ? AND segment_id = ?
	`

	deleteSegmentImageByDocID = `
		UPDATE
			t_doc_segment_image
		SET
		    is_deleted = ?,
		    update_time = ?
		WHERE
		    robot_id = ? AND doc_id IN (%s) AND is_deleted = ?
	`

	getSegmentByBizIDs = `
		SELECT
			%s
		FROM
		    t_doc_segment
		WHERE
		    business_id IN (%s)
	`

	docSegmentPageInfoFields = `
		id,page_info_id,segment_id,doc_id,robot_id,corp_id,staff_id,org_page_numbers,big_page_numbers,
		sheet_data,is_deleted,create_time,update_time
	`

	createSegmentPageInfo = `
		INSERT INTO
		    t_doc_segment_page_info (%s)
		VALUES
		    (null,:page_info_id,:segment_id,:doc_id,:robot_id,:corp_id,:staff_id,:org_page_numbers,:big_page_numbers,
		     :sheet_data,:is_deleted,:create_time,:update_time)
	`

	deleteSegmentPageInfoBySegID = `
		UPDATE
			t_doc_segment_page_info
		SET
		    is_deleted = ?,
		    update_time = ?
		WHERE
		    robot_id = ? AND segment_id = ?
	`

	deleteSegmentPageInfoByDocID = `
		UPDATE
			t_doc_segment_page_info
		SET
		    is_deleted = ?,
		    update_time = ?
		WHERE
		    robot_id = ? AND doc_id IN (%s) AND is_deleted = ?
	`

	getSegmentPageInfoBySegID = `
		SELECT
			%s
		FROM
		    t_doc_segment_page_info
		WHERE
		    robot_id = ? AND segment_id IN (%s)
	`
)

const (
	docSegmentTableName         = "t_doc_segment"
	docSegmentImageTableName    = "t_doc_segment_image"
	docSegmentPageInfoTableName = "t_doc_segment_page_info"
)

// CreateSegment 创建文档分段
func (d *dao) CreateSegment(ctx context.Context, segments []*model.DocSegmentExtend, robotID uint64) error {
	pageSize := 500
	// 创建哈希表存储唯一字符串和BigData的ID
	uniqueBigData := make(map[string]string)
	db := knowClient.DBClient(ctx, docSegmentTableName, robotID, []client.Option{}...)
	err := db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		total := len(segments)
		pages := int(math.Ceil(float64(total) / float64(pageSize)))
		querySQL := fmt.Sprintf(createSegment, docSegmentFields)
		for i := 0; i < pages; i++ {
			start := pageSize * i
			end := pageSize * (i + 1)
			if end > total {
				end = total
			}
			tmpSegments := segments[start:end]

			var bigData []*pb.BigData

			for _, tmpSegment := range tmpSegments {
				// 生成业务ID
				tmpSegment.BusinessID = d.GenerateSeqID()

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
					tmpSegment.BigDataID = strconv.FormatUint(d.GenerateSeqID(), 10) // 生成ES的ID
					uniqueBigData[uniqueKey] = tmpSegment.BigDataID
					bigData = append(bigData, &pb.BigData{
						RobotId:   tmpSegment.RobotID,
						DocId:     tmpSegment.DocID,
						BigDataId: tmpSegment.BigDataID,
						BigStart:  tmpSegment.BigStart,
						BigEnd:    tmpSegment.BigEnd,
						BigString: tmpSegment.BigString,
					})
				}
			}
			if _, err := tx.NamedExecContext(ctx, querySQL, tmpSegments); err != nil {
				log.ErrorContextf(ctx, "创建文档分段失败 sql:%s seg:%+v err:%+v", querySQL,
					tmpSegments, err)
				return err
			}

			if err := d.AddBigDataElastic(ctx, bigData, pb.KnowledgeType_KNOWLEDGE); err != nil {
				log.ErrorContextf(ctx, "CreateSegment|AddBigDataElastic|seg:%+v|err:%+v", tmpSegments, err)
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

// createSegmentWithBigData 创建文档分段
func (d *dao) createSegmentWithBigData(ctx context.Context,
	shortURLSyncMap, orgDataSyncMap, bigDataSyncMap, imageDataSyncMap *sync.Map, segments []*model.DocSegmentExtend,
	robotID uint64, currentOrgDataOrder *int, OldOrgDataInfos []*model.OldOrgDataInfo) error {
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
		log.InfoContextf(ctx, "createSegmentWithBigData|segments.len:%d|start:%d|end:%d",
			len(segments), start, end)
		tmpSegments := segments[start:end]
		var orgData []*model.DocSegmentOrgData
		var bigData []*pb.BigData
		var images []*model.DocSegmentImage
		var pageInfos []*model.DocSegmentPageInfo
		docCommon := &model.DocSegmentCommon{}
		for index, tmpSegment := range tmpSegments {
			if index == 0 {
				corpBizID, appBizID, staffBizID, docBizID, err := d.SegmentCommonIDsToBizIDs(ctx, tmpSegment.CorpID,
					tmpSegment.RobotID, tmpSegment.StaffID, tmpSegment.DocID)
				if err != nil {
					return err
				}
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
			tmpSegment.BusinessID = d.GenerateSeqID()
			// orgData数据
			tmpSegmentOrgData, err := d.getDocSegmentOrgData(ctx, orgDataSyncMap, tmpSegment, docCommon)
			if err != nil {
				return err
			}
			if tmpSegmentOrgData != nil {
				orgData = append(orgData, tmpSegmentOrgData)
			}
			// bigData数据
			tmpSegmentBigData, err := d.getDocSegmentBigData(ctx, bigDataSyncMap, tmpSegment)
			if err != nil {
				return err
			}
			if tmpSegmentBigData != nil {
				bigData = append(bigData, tmpSegmentBigData)
			}
			// image数据
			tmpSegmentImages, err := d.getDocSegmentImages(ctx, shortURLSyncMap, imageDataSyncMap, tmpSegment)
			if err != nil {
				return err
			}
			if len(tmpSegmentImages) > 0 {
				images = append(images, tmpSegmentImages...)
			}
			// page页码数据
			tmpPageInfo, err := d.getDocSegmentPageInfo(ctx, tmpSegment)
			if err != nil {
				return err
			}
			if tmpPageInfo != nil {
				pageInfos = append(pageInfos, tmpPageInfo)
			}
		}

		db := knowClient.DBClient(ctx, docSegmentTableName, robotID, []client.Option{}...)
		err := db.Transactionx(ctx, func(tx *sqlx.Tx) error {
			// 写DB
			if err := d.createSegmentInfoToDB(ctx, robotID, tx, tmpSegments, pageInfos, images, orgData,
				currentOrgDataOrder, OldOrgDataInfos); err != nil {
				log.ErrorContextf(ctx, "createSegmentWithBigData|createSegmentInfoToDB|seg:%+v|err:%+v",
					tmpSegments, err)
				return err
			}

			// 写ES
			// 分批写入bigData
			log.InfoContextf(ctx, "createSegmentWithBigData|len(bigData):%d", len(bigData))
			if len(bigData) > 0 {
				for _, bigDataChunks := range slicex.Chunk(bigData, eSAddSize) {
					if err := d.AddBigDataElastic(ctx, bigDataChunks, pb.KnowledgeType_KNOWLEDGE); err != nil {
						log.ErrorContextf(ctx, "createSegmentWithBigData|AddBigDataElastic|seg:%+v|err:%+v",
							tmpSegments, err)
						return err
					}
				}
			}
			return nil
		})
		if err != nil {
			log.ErrorContextf(ctx, "createSegmentWithBigData|Transactionx|seg:%+v|err:%+v",
				tmpSegments, err)
			return err
		}
	}
	return nil
}

// createSegmentInfoToDB 将切片相关信息入库
func (d *dao) createSegmentInfoToDB(ctx context.Context, robotID uint64, tx *sqlx.Tx, segments []*model.DocSegmentExtend,
	pageInfos []*model.DocSegmentPageInfo, images []*model.DocSegmentImage, orgData []*model.DocSegmentOrgData,
	currentOrgDataOrder *int, oldOrgDataInfos []*model.OldOrgDataInfo) (err error) {
	segmentBizIDMap := make(map[uint64]*model.DocSegmentExtend)
	// orgData存储
	log.InfoContextf(ctx, "createSegmentInfoToDB|len(orgData):%d|currentOrgDataOrder:%d|len(oldOrgDataInfos):%d",
		len(orgData), *currentOrgDataOrder, len(oldOrgDataInfos))
	if len(orgData) > 0 {
		// currentOrgDataOrder 先保留，清理代码后暂时未用到
		*currentOrgDataOrder += len(orgData)
		for _, data := range orgData {
			//log.InfoContextf(ctx, "createSegmentInfoToDB|intervene:%v|IsDisabled:%+v|AddMethod:%v",
			//	intervene, data.IsDisabled, data.AddMethod)
			err := GetDocSegmentOrgDataDao().CreateDocSegmentOrgData(ctx, data)
			if err != nil {
				log.ErrorContextf(ctx, "createSegmentInfoToDB|CreateDocSegmentOrgData err:%+v", err)
				return err
			}
		}
	}

	log.InfoContextf(ctx, "createSegmentInfoToDB|len(segments):%d", len(segments))
	if len(segments) > 0 {
		createSegmentSQL := fmt.Sprintf(createSegment, docSegmentFields)
		if _, err = tx.NamedExecContext(ctx, createSegmentSQL, segments); err != nil {
			log.ErrorContextf(ctx, "createSegmentInfoToDB|sql:%s seg:%+v err:%+v", createSegmentSQL,
				segments, err)
			return err
		}
		segmentBizIDMap, err = d.getSegmentByBizIDsWithTx(ctx, tx, segments)
		if err != nil {
			return err
		}
	}

	log.InfoContextf(ctx, "createSegmentInfoToDB|len(pageInfos):%d", len(pageInfos))
	if len(pageInfos) > 0 {
		if err = d.createDocSegmentPageInfos(ctx, tx, segmentBizIDMap, pageInfos); err != nil {
			log.ErrorContextf(ctx, "createSegmentInfoToDB|createDocSegmentPageInfos|err:%+v", err)
			return err
		}
	}

	log.InfoContextf(ctx, "createSegmentInfoToDB|len(images):%d", len(images))
	if len(images) > 0 {
		if err = d.createDocSegmentImages(ctx, tx, segmentBizIDMap, images); err != nil {
			log.ErrorContextf(ctx, "createSegmentInfoToDB|createDocSegmentImages|err:%+v", err)
			return err
		}
	}

	return nil
}

// getSegmentByBizIDsWithTx 事物获取切片信息
func (d *dao) getSegmentByBizIDsWithTx(ctx context.Context, tx *sqlx.Tx, segments []*model.DocSegmentExtend) (
	map[uint64]*model.DocSegmentExtend, error) {
	if len(segments) == 0 {
		log.InfoContextf(ctx, "getSegmentByBizIDsWithTx|len(segments):%d|ignore", len(segments))
		return nil, nil
	}
	args := make([]any, 0, len(segments))
	for _, seg := range segments {
		args = append(args, seg.BusinessID)
	}
	querySegmentSQL := fmt.Sprintf(getSegmentByBizIDs, docSegmentFields, placeholder(len(segments)))
	segmentList := make([]*model.DocSegmentExtend, 0)
	if err := tx.Select(&segmentList, querySegmentSQL, args...); err != nil {
		log.ErrorContextf(ctx, "getSegmentByBizIDsWithTx|getSegmentByBizIDs|"+
			"sql:%s, args:%+v|err:%+v", querySegmentSQL, args, err)
		return nil, err
	}
	segmentMap := make(map[uint64]*model.DocSegmentExtend)
	for _, seg := range segmentList {
		segmentMap[seg.BusinessID] = seg
	}
	return segmentMap, nil
}

// getDocSegmentBigData 获取文档切片BigData
func (d *dao) getDocSegmentBigData(ctx context.Context, bigDataSyncMap *sync.Map,
	segment *model.DocSegmentExtend) (*pb.BigData, error) {
	// 只有是文档切片，才需要把 BigData存入ES
	if !segment.IsSegmentForIndex() {
		log.WarnContextf(ctx, "getDocSegmentBigData|segment:%+v", segment)
		return nil, nil
	}

	if len(segment.BigString) == 0 {
		log.WarnContextf(ctx, "getDocSegmentBigData|BigString|segment:%+v", segment)
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
		segment.BigDataID = strconv.FormatUint(d.GenerateSeqID(), 10) // 生成ES的ID
		bigDataSyncMap.Store(uniqueKey, segment.BigDataID)
		return &pb.BigData{
			RobotId:   segment.RobotID,
			DocId:     segment.DocID,
			BigDataId: segment.BigDataID,
			BigStart:  segment.BigStart,
			BigEnd:    segment.BigEnd,
			BigString: segment.BigString,
		}, nil
	}
}

// getDocSegmentOrgData 获取文档切片OrgData
func (d *dao) getDocSegmentOrgData(ctx context.Context, orgDataSyncMap *sync.Map,
	segment *model.DocSegmentExtend, docCommon *model.DocSegmentCommon) (*model.DocSegmentOrgData, error) {
	// 参数校验
	if orgDataSyncMap == nil {
		log.ErrorContextf(ctx, "getDocSegmentOrgData|orgDataSyncMap is null")
		return nil, fmt.Errorf("orgDataSyncMap is null")
	}
	if segment == nil {
		log.ErrorContextf(ctx, "getDocSegmentOrgData|segment is null")
		return nil, fmt.Errorf("segment is null")
	}
	if docCommon == nil {
		log.ErrorContextf(ctx, "getDocSegmentOrgData|docCommon is null")
		return nil, fmt.Errorf("docCommon is null")
	}
	if len(segment.OrgData) == 0 {
		log.WarnContextf(ctx, "getDocSegmentOrgData|OrgData Empty|segment:%+v", segment)
		return nil, nil
	}
	hash := sha256.New()
	_, err := io.WriteString(hash, segment.OrgData)
	if err != nil {
		log.ErrorContextf(ctx, "getDocSegmentOrgData|WriteString|err:%+v", err)
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
	segment.OrgDataBizID = d.GenerateSeqID()
	// 存入org_data数据库
	orgDataSyncMap.Store(uniqueKey, segment.OrgDataBizID)

	// 解析SheetData，获取sheet名，如有多个只取第一个（按行拆分时使用）
	var sheetDatas []model.SheetData
	err = jsoniter.Unmarshal([]byte(segment.SheetData), &sheetDatas)
	if err != nil && segment.SheetData != "" {
		log.WarnContextf(ctx, "getDocSegmentOrgData|Unmarshal|err:%+v, SheetData: %+v", err, segment.SheetData)
	}
	sheetName := ""
	if sheetDatas != nil && len(sheetDatas) > 0 {
		sheetName = sheetDatas[0].SheetName
	}

	data := &model.DocSegmentOrgData{
		BusinessID:         segment.OrgDataBizID,
		AppBizID:           docCommon.AppBizID,
		DocBizID:           docCommon.DocBizID,
		CorpBizID:          docCommon.CorpBizID,
		StaffBizID:         docCommon.StaffBizID,
		OrgData:            segment.OrgData,
		OrgPageNumbers:     segment.OrgPageNumbers,
		SheetData:          segment.SheetData,
		SegmentType:        segment.SegmentType,
		AddMethod:          model.AddMethodDefault,
		IsTemporaryDeleted: IsNotDeleted,
		IsDeleted:          IsNotDeleted,
		IsDisabled:         model.SegmentIsEnable,
		CreateTime:         time.Now(),
		UpdateTime:         time.Now(),
		SheetName:          sheetName,
	}
	segment.OrgData = ""
	return data, nil
}

// getDocSegmentImages 获取文档切片Images
func (d *dao) getDocSegmentImages(ctx context.Context, shortURLSyncMap, imageDataSyncMap *sync.Map,
	segment *model.DocSegmentExtend) ([]*model.DocSegmentImage, error) {
	// 只有是文档切片，才需要把 切片图片存储
	if !segment.IsSegmentForIndex() {
		log.InfoContextf(ctx, "getDocSegmentBigData|segment:%+v|Type is ignore", segment)
		return nil, nil
	}

	if len(segment.Images) == 0 {
		log.InfoContextf(ctx, "getDocSegmentImages|segment:%+v|Images is empty", segment)
		return nil, nil
	}

	segmentImages := make([]*model.DocSegmentImage, 0)
	for _, originalUrl := range segment.Images {
		//imageID := uint64(0)
		//if id, ok := imageDataSyncMap.Load(originalUrl); ok {
		//	imageID = id.(uint64)
		//} else {
		//	imageID = d.GenerateSeqID()
		//	imageDataSyncMap.Store(originalUrl, imageID)
		//}
		// 2.4.0 @harryhlli @jouislu 结论：相同图片也用不同图片ID
		imageID := d.GenerateSeqID()
		externalUrl := ""
		URL, err := url.Parse(originalUrl)
		if err != nil || URL.Path == "" {
			log.ErrorContextf(ctx, "getDocSegmentImages|segment:%+v|originalUrl is illegal", segment)
			return nil, fmt.Errorf("originalUrl is illegal")
		}
		oldURL := URL.Scheme + "://" + URL.Host + URL.Path
		if value, ok := shortURLSyncMap.Load(oldURL); ok {
			newURL := value.(string)
			externalUrl = strings.ReplaceAll(originalUrl, oldURL, newURL)
		} else {
			log.ErrorContextf(ctx, "getDocSegmentImages|segment:%+v|oldURL：%s｜externalUrl is empty",
				segment, oldURL)
			return nil, fmt.Errorf("externalUrl is empty")
		}
		segmentImages = append(segmentImages, &model.DocSegmentImage{
			ImageID:      imageID,
			DocID:        segment.DocID,
			RobotID:      segment.RobotID,
			CorpID:       segment.CorpID,
			StaffID:      segment.StaffID,
			OriginalUrl:  originalUrl,
			ExternalUrl:  externalUrl,
			IsDeleted:    segment.IsDeleted,
			CreateTime:   time.Now(),
			UpdateTime:   time.Now(),
			SegmentBizID: segment.BusinessID, // SegmentID 此时还不能确定，需要等segment写入后再通过SegmentBizID查询
		})
	}
	return segmentImages, nil
}

// getDocSegmentPageInfo 获取文档切片页码信息
func (d *dao) getDocSegmentPageInfo(ctx context.Context, segment *model.DocSegmentExtend) (
	*model.DocSegmentPageInfo, error) {
	if !segment.IsSegmentForIndex() {
		log.InfoContextf(ctx, "getDocSegmentPageInfo|segment:%+v|Type is ignore", segment)
		return nil, nil
	}
	return &model.DocSegmentPageInfo{
		PageInfoID:     d.GenerateSeqID(),
		DocID:          segment.DocID,
		RobotID:        segment.RobotID,
		CorpID:         segment.CorpID,
		StaffID:        segment.StaffID,
		OrgPageNumbers: segment.OrgPageNumbers,
		BigPageNumbers: segment.BigPageNumbers,
		SheetData:      segment.SheetData,
		IsDeleted:      segment.IsDeleted,
		CreateTime:     time.Now(),
		UpdateTime:     time.Now(),
		SegmentBizID:   segment.BusinessID,
	}, nil
}

// createDocSegmentImages 文档切片Images入库
func (d *dao) createDocSegmentImages(ctx context.Context, tx *sqlx.Tx,
	segmentBizIDMap map[uint64]*model.DocSegmentExtend, segmentImages []*model.DocSegmentImage) error {
	if len(segmentImages) == 0 {
		log.InfoContextf(ctx, "createDocSegmentImages|len(segmentImages):%d|segmentImages is empty",
			len(segmentImages))
		return nil
	}
	pageSize := 500
	total := len(segmentImages)
	pages := int(math.Ceil(float64(total) / float64(pageSize)))
	for i := 0; i < pages; i++ {
		start := pageSize * i
		end := pageSize * (i + 1)
		if end > total {
			end = total
		}
		log.InfoContextf(ctx, "createDocSegmentImages|segmentImages.len:%d|start:%d|end:%d",
			len(segmentImages), start, end)
		tmpSegmentImages := segmentImages[start:end]
		for _, tmpSegmentImage := range tmpSegmentImages {
			seg, ok := segmentBizIDMap[tmpSegmentImage.SegmentBizID]
			if !ok {
				log.ErrorContextf(ctx, "createDocSegmentImages|segBizID is not found|"+
					"tmpSegmentImage:%+v", tmpSegmentImage)
				return fmt.Errorf("segBizID is not found")
			}
			tmpSegmentImage.SegmentID = seg.ID
		}

		createSegmentImageSQL := fmt.Sprintf(createSegmentImage, docSegmentImageFields)
		if _, err := tx.NamedExecContext(ctx, createSegmentImageSQL, tmpSegmentImages); err != nil {
			log.ErrorContextf(ctx, "createDocSegmentImages|sql:%s tmpSegmentImages:%+v err:%+v",
				createSegmentImageSQL, tmpSegmentImages, err)
			return err
		}
	}
	return nil
}

// createDocSegmentImages 文档切片Images入库
func (d *dao) createDocSegmentPageInfos(ctx context.Context, tx *sqlx.Tx,
	segmentBizIDMap map[uint64]*model.DocSegmentExtend, segmentPageInfos []*model.DocSegmentPageInfo) error {
	if len(segmentPageInfos) == 0 {
		log.InfoContextf(ctx, "createDocSegmentPageInfos|len(segmentPageInfos):%d|segmentPageInfos is empty",
			len(segmentPageInfos))
		return nil
	}
	for _, segmentPageInfo := range segmentPageInfos {
		seg, ok := segmentBizIDMap[segmentPageInfo.SegmentBizID]
		if !ok {
			log.ErrorContextf(ctx, "createDocSegmentPageInfos|segBizID is not found|"+
				"segmentPageInfo:%+v", segmentPageInfo)
			return fmt.Errorf("segBizID is not found")
		}
		segmentPageInfo.SegmentID = seg.ID
	}
	createSegmentPageInfoSQL := fmt.Sprintf(createSegmentPageInfo, docSegmentPageInfoFields)
	if _, err := tx.NamedExecContext(ctx, createSegmentPageInfoSQL, segmentPageInfos); err != nil {
		log.ErrorContextf(ctx, "createDocSegmentPageInfos|sql:%s segmentPageInfos:%+v err:%+v",
			createSegmentPageInfoSQL, segmentPageInfos, err)
		return err
	}
	return nil
}

// fillDocSegmentImagesSegID 填充文档切片Images的SegmentID
func (d *dao) fillDocSegmentImagesSegID(ctx context.Context, tx *sqlx.Tx,
	segmentImages []*model.DocSegmentImage) error {
	if len(segmentImages) == 0 {
		log.InfoContextf(ctx, "fillDocSegmentImagesSegID|len(segmentImages):%d|segmentImages is empty",
			len(segmentImages))
		return nil
	}
	pageSize := 500
	total := len(segmentImages)
	pages := int(math.Ceil(float64(total) / float64(pageSize)))
	for i := 0; i < pages; i++ {
		start := pageSize * i
		end := pageSize * (i + 1)
		if end > total {
			end = total
		}
		log.InfoContextf(ctx, "fillDocSegmentImagesSegID|segmentImages.len:%d|start:%d|end:%d",
			len(segmentImages), start, end)
		tmpSegmentImages := segmentImages[start:end]

		tmpSegmentBizIDMap := make(map[uint64]struct{})
		tmpUniqueSegmentBizIDs := make([]uint64, 0)

		for _, tmpSegmentImage := range tmpSegmentImages {
			if _, ok := tmpSegmentBizIDMap[tmpSegmentImage.SegmentBizID]; !ok {
				tmpUniqueSegmentBizIDs = append(tmpUniqueSegmentBizIDs, tmpSegmentImage.SegmentBizID)
				tmpSegmentBizIDMap[tmpSegmentImage.SegmentBizID] = struct{}{}
			}
		}

		args := make([]any, 0, len(tmpUniqueSegmentBizIDs))
		for _, id := range tmpUniqueSegmentBizIDs {
			args = append(args, id)
		}
		querySegmentSQL := fmt.Sprintf(getSegmentByBizIDs,
			docSegmentFields, placeholder(len(tmpUniqueSegmentBizIDs)))
		segmentList := make([]*model.DocSegmentExtend, 0)
		if err := tx.Select(&segmentList, querySegmentSQL, args...); err != nil {
			log.ErrorContextf(ctx, "fillDocSegmentImagesSegID|getSegmentByBizIDs|"+
				"sql:%s, args:%+v|err:%+v", querySegmentSQL, args, err)
			return err
		}
		segmentMap := make(map[uint64]*model.DocSegmentExtend)
		for _, seg := range segmentList {
			segmentMap[seg.BusinessID] = seg
		}

		for _, tmpSegmentImage := range tmpSegmentImages {
			seg, ok := segmentMap[tmpSegmentImage.SegmentBizID]
			if !ok {
				log.ErrorContextf(ctx, "fillDocSegmentImagesSegID|segBizID is not found|"+
					"tmpSegmentImage:%+v", tmpSegmentImage)
				return fmt.Errorf("segBizID is not found")
			}
			tmpSegmentImage.SegmentID = seg.ID
		}
	}
	return nil
}

// GetSegmentByID 通过ID获取段落内容
func (d *dao) GetSegmentByID(ctx context.Context, id uint64, robotID uint64) (*model.DocSegmentExtend, error) {
	querySQL := fmt.Sprintf(getSegmentByID, docSegmentFields)
	segment := make([]*model.DocSegmentExtend, 0)
	args := []any{id}
	db := knowClient.DBClient(ctx, docSegmentTableName, robotID, []client.Option{}...)
	if err := db.QueryToStructs(ctx, &segment, querySQL, args...); err != nil {
		return nil, fmt.Errorf("GetSegmentByID failed sql:%s args:%+v err:%w", querySQL, args, err)
	}
	if len(segment) == 0 {
		return nil, nil
	}
	return segment[0], nil
}

// GetSegmentByIDs 通过ID获取段落内容
func (d *dao) GetSegmentByIDs(ctx context.Context, ids []uint64, robotID uint64) ([]*model.DocSegmentExtend, error) {
	if len(ids) == 0 {
		return []*model.DocSegmentExtend{}, nil
	}
	querySQL := fmt.Sprintf(getSegmentByIDs, docSegmentFields)
	query, args, err := sqlx.In(querySQL, ids)
	if err != nil {
		log.ErrorContextf(ctx, "通过ID获取段落内容失败 sql:%s args:%+v err:%+v", querySQL, ids, err)
		return nil, err
	}
	var segments []*model.DocSegmentExtend
	db := knowClient.DBClient(ctx, docSegmentTableName, robotID, []client.Option{}...)
	if err = db.Select(ctx, &segments, query, args...); err != nil {
		log.ErrorContextf(ctx, "通过ID获取段落内容失败 sql:%s args:%+v err:%+v", query, args, err)
		return nil, err
	}
	return segments, nil
}

// GetPagedSegmentIDsByDocID 通过文档ID分页获取分片ID列表
func (d *dao) GetPagedSegmentIDsByDocID(ctx context.Context, docID uint64, page uint32, pageSize uint32,
	robotID uint64) ([]uint64, error) {
	querySQL := getPagedSegmentIDsByDocID
	segments := make([]*model.DocSegmentExtend, 0)
	offset := (page - 1) * pageSize
	args := make([]any, 0, 4)
	args = append(args, docID, model.SegmentIsNotDeleted, offset, pageSize)
	db := knowClient.DBClient(ctx, docSegmentTableName, robotID, []client.Option{}...)
	if err := db.QueryToStructs(ctx, &segments, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "通过文档ID和批次ID获取段落内容失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	ids := make([]uint64, 0)
	for _, seg := range segments {
		ids = append(ids, seg.ID)
	}
	return ids, nil
}

// GetSegmentIDByDocIDAndBatchID 通过文档ID和批次ID获取段落内容
func (d *dao) GetSegmentIDByDocIDAndBatchID(ctx context.Context, docID uint64, batchID int, robotID uint64) (
	[]uint64, error) {
	querySQL := getSegmentIDByDocIDAndBatchID
	segments := make([]*model.DocSegmentExtend, 0)
	args := make([]any, 0, 3)
	args = append(args, docID, batchID, model.SegmentIsNotDeleted)
	db := knowClient.DBClient(ctx, docSegmentTableName, robotID, []client.Option{}...)
	if err := db.QueryToStructs(ctx, &segments, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "通过文档ID和批次ID获取段落内容失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	ids := make([]uint64, 0)
	for _, seg := range segments {
		ids = append(ids, seg.ID)
	}
	return ids, nil
}

// GetQASegmentIDByDocIDAndBatchID 通过文档ID和批次ID获取需要生成QA的段落内容
func (d *dao) GetQASegmentIDByDocIDAndBatchID(ctx context.Context, docID, stopNextSegmentID, segmentCount uint64,
	batchID int, robotID uint64) ([]uint64, error) {
	querySQL := getQASegmentIDByDocIDAndBatchID
	segments := make([]*model.DocSegmentExtend, 0)
	args := make([]any, 0, 4)
	args = append(args, docID, batchID, model.SegmentIsNotDeleted, model.SegmentStatusCreatedQa)
	db := knowClient.DBClient(ctx, docSegmentTableName, robotID, []client.Option{}...)
	if err := db.QueryToStructs(ctx, &segments, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "通过文档ID和批次ID获取需要生成QA的段落内容 sql:%s args:%+v err:%+v",
			querySQL, args, err)
		return nil, err
	}
	ids := make([]uint64, 0)
	for _, seg := range segments {
		ids = append(ids, seg.ID)
	}
	return ids, nil
}

// updateSegmentOutputs 更新文档段落算法响应结果
func (d *dao) updateSegmentOutputs(ctx context.Context, segment *model.DocSegmentExtend, robotID uint64) error {
	querySQL := updateOutputs
	segment.UpdateTime = time.Now()
	db := knowClient.DBClient(ctx, docSegmentTableName, robotID, []client.Option{}...)
	if _, err := db.NamedExec(ctx, querySQL, segment); err != nil {
		log.ErrorContextf(ctx, "更新文档段落算法响应结果失败 sql:%s segment:%+v err:%+v", querySQL, segment, err)
		return err
	}
	return nil
}

// GetReleaseSegmentCount 获取发布文档分片总数
func (d *dao) GetReleaseSegmentCount(ctx context.Context, docID uint64, robotID uint64) (uint64, error) {
	var total uint64
	querySQL := getReleaseSegmentCount
	args := make([]any, 0, 2)
	args = append(args, docID, model.SegmentReleaseStatusInit)
	db := knowClient.DBClient(ctx, docSegmentTableName, robotID, []client.Option{}...)
	if err := db.Get(ctx, &total, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取发布文档分片总数失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return 0, err
	}
	return total, nil
}

// GetReleaseSegmentList 获取发布文档分片列表
func (d *dao) GetReleaseSegmentList(ctx context.Context, docID uint64, page, pageSize uint32, robotID uint64) (
	[]*model.DocSegmentExtend, error) {
	args := make([]any, 0, 4)
	args = append(args, docID, model.SegmentReleaseStatusInit)
	offset := (page - 1) * pageSize
	args = append(args, offset, pageSize)
	querySQL := fmt.Sprintf(getReleaseSegmentList, docSegmentFields)
	qas := make([]*model.DocSegmentExtend, 0)
	db := knowClient.DBClient(ctx, docSegmentTableName, robotID, []client.Option{}...)
	if err := db.QueryToStructs(ctx, &qas, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取发布文档分片列表失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	return qas, nil
}

// PublishSegment 发布文档片段
func (d *dao) PublishSegment(ctx context.Context, segment *model.DocSegmentExtend,
	releaseSeg *model.ReleaseSegment, robotID uint64) error {
	now := time.Now()
	db := knowClient.DBClient(ctx, docSegmentTableName, robotID, []client.Option{}...)
	if err := db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		segment.UpdateTime = now
		querySQL := publishSegment
		if _, err := tx.NamedExecContext(ctx, querySQL, segment); err != nil {
			log.ErrorContextf(ctx, "发布文档片段失败 sql:%s args:%+v err:%+v", querySQL, segment, err)
			return err
		}
		releaseSeg.UpdateTime = now
		querySQL = publishReleaseSeg
		if _, err := tx.NamedExecContext(ctx, querySQL, releaseSeg); err != nil {
			log.ErrorContextf(ctx, "发布文档片段失败 sql:%s args:%+v err:%+v", querySQL, segment, err)
			return err
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "发布文档片段失败 err:%+v", err)
		return err
	}
	return nil
}

// BatchDeleteSegments 批量删除文档分片, 超量隔离专用
func (d *dao) BatchDeleteSegments(ctx context.Context, segments []*model.DocSegmentExtend, robotID uint64) error {
	db := knowClient.DBClient(ctx, docSegmentTableName, robotID, []client.Option{}...)
	var allImagesIDs []uint64
	if err := db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		for _, segment := range segments {
			querySQL := deleteSegment
			segment.IsDeleted = model.SegmentIsDeleted
			segment.UpdateTime = time.Now()
			if !segment.IsNextActionAdd() {
				segment.NextAction = model.NextActionDelete
				segment.ReleaseStatus = model.SegmentReleaseStatusInit
			}
			if _, err := tx.NamedExecContext(ctx, querySQL, segment); err != nil {
				log.ErrorContextf(ctx, "删除文档分片失败 sql:%s segment:%+v err:%+v", querySQL, segment, err)
				return err
			}
			// 切片页码
			querySQL = deleteSegmentPageInfoBySegID
			args := make([]any, 0, 4)
			args = append(args, segment.IsDeleted, time.Now(), segment.RobotID, segment.ID)
			if _, err := tx.ExecContext(ctx, querySQL, args...); err != nil {
				log.ErrorContextf(ctx, "删除文档分片页码失败 sql:%s args:%+v err:%+v", querySQL, args, err)
				return err
			}
			// 图片ID
			querySQL = getSegmentImageIdsBySegID
			args = make([]any, 0, 3)
			args = append(args, segment.RobotID, segment.ID, model.SegmentIsNotDeleted)
			imageIds := make([]uint64, 0)
			if err := tx.Select(&imageIds, querySQL, args...); err != nil {
				log.ErrorContextf(ctx, "获取文档分片图片 sql:%s, args:%+v|err:%+v", querySQL, args, err)
				return err
			}
			// 切片图片
			querySQL = deleteSegmentImageBySegID
			args = make([]any, 0, 4)
			args = append(args, segment.IsDeleted, time.Now(), segment.RobotID, segment.ID)
			if _, err := tx.ExecContext(ctx, querySQL, args...); err != nil {
				log.ErrorContextf(ctx, "删除文档分片图片失败 sql:%s args:%+v err:%+v", querySQL, args, err)
				return err
			}
			allImagesIDs = append(allImagesIDs, imageIds...)
		}
		return nil
	}); err != nil {
		return err
	}
	// 删除图片向量
	err := UpdateImageVectorDeleteStatus(ctx, robotID, allImagesIDs, model.SegmentImageVectorIsDeleted)
	if err != nil {
		return err
	}

	return nil
}

// DeleteSegmentsForQA 删除用于生成QA的分片
func (d *dao) DeleteSegmentsForQA(ctx context.Context, doc *model.Doc) error {
	db := knowClient.DBClient(ctx, docSegmentTableName, doc.RobotID, []client.Option{}...)
	if err := db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		now := time.Now()
		querySQL := deleteSegmentForType
		args := []any{model.SegmentIsDeleted, now, doc.ID, model.SegmentIsNotDeleted, model.SegmentTypeQA}
		if _, err := tx.ExecContext(ctx, querySQL, args...); err != nil {
			log.ErrorContextf(ctx, "删除用于生成QA的分片失败 sql:%s args:%+v err:%+v", querySQL, args, err)
			return err
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "删除用于生成QA的分片和chunk失败 err:%+v", err)
		return err
	}
	return nil
}

// DeleteSegmentsForIndex 删除用于写向量的分片
func (d *dao) DeleteSegmentsForIndex(ctx context.Context, doc *model.Doc, embeddingModelName string) error {
	db := knowClient.DBClient(ctx, docSegmentTableName, doc.RobotID, []client.Option{}...)
	if err := db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		/*now := time.Now()
		querySQL := deleteSegmentForType
		args := []any{model.SegmentIsDeleted, now, doc.ID, model.SegmentIsNotDeleted, model.SegmentTypeIndex}
		if _, err := tx.ExecContext(ctx, querySQL, args...); err != nil {
			log.ErrorContextf(ctx, "删除用于写向量的分片 sql:%s args:%+v err:%+v", querySQL, args, err)
			return err
		}
		querySQL = fmt.Sprintf(deleteSegmentPageInfoByDocID, placeholder(1))
		args = []any{model.SegmentIsDeleted, now, doc.RobotID, doc.ID, model.SegmentIsNotDeleted}
		if _, err := tx.ExecContext(ctx, querySQL, args...); err != nil {
			log.ErrorContextf(ctx, "删除文档分片页码失败 sql:%s args:%+v err:%+v", querySQL, args, err)
			return err
		}
		querySQL = fmt.Sprintf(deleteSegmentImageByDocID, placeholder(1))
		args = []any{model.SegmentIsDeleted, now, doc.RobotID, doc.ID, model.SegmentIsNotDeleted}
		if _, err := tx.ExecContext(ctx, querySQL, args...); err != nil {
			log.ErrorContextf(ctx, "删除文档分片图片失败 sql:%s args:%+v err:%+v", querySQL, args, err)
			return err
		}*/
		// 逻辑删除文档对应的org_data
		corpBizID, appBizID, _, docBizID, err := d.SegmentCommonIDsToBizIDs(ctx, doc.CorpID,
			doc.RobotID, 0, doc.ID)
		if err != nil {
			log.ErrorContextf(ctx, "SegmentCommonIDsToBizIDs|doc:%+v|err:%+v", doc, err)
			return err
		}
		if err := d.BatchDeleteSegmentsAndKnowledge(ctx, doc, embeddingModelName); err != nil {
			log.ErrorContextf(ctx, "BatchDeleteSegmentsAndKnowledge|doc:%+v|err:%+v", doc, err)
			return err
		}
		if err := d.DeleteBigDataElastic(ctx, doc.RobotID, doc.ID,
			pb.KnowledgeType_KNOWLEDGE, true); err != nil {
			log.ErrorContextf(ctx, "DeleteBigDataElastic|doc:%+v|err:%+v", doc, err)
			return err
		}
		if model.IsTableTypeDocument(doc.FileType) {
			err = DeleteSheetDbTableAndColumns(ctx, corpBizID, appBizID, doc.BusinessID, doc.RobotID)
			if err != nil {
				log.ErrorContextf(ctx, "deleteSheetDbTableAndColumns %v, %v", doc.BusinessID, err)
				return err
			}
		}
		if err = d.DeleteText2SQL(ctx, doc.RobotID, doc.ID); err != nil {
			log.ErrorContextf(ctx, "DeleteText2SQL %v, %v", doc.BusinessID, err)
			return err
		}
		err = GetDocSegmentOrgDataDao().BatchDeleteDocOrgDataByDocBizID(ctx, nil, corpBizID,
			appBizID, docBizID, 10000)
		if err != nil {
			log.ErrorContextf(ctx, "BatchDeleteDocOrgDataByDocBizID|doc:%+v|err:%+v", doc, err)
			return err
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "删除用于写向量的分片和chunk失败 err:%+v", err)
		return err
	}
	return nil
}

// DeleteSegmentImages 删除文档分片的图片
func (d *dao) DeleteSegmentImages(ctx context.Context, robotID uint64, docIDs []uint64) error {
	log.InfoContextf(ctx, "DeleteSegmentImages|robotID:%d, docIDs:%+v", robotID, docIDs)
	db := knowClient.DBClient(ctx, docSegmentImageTableName, robotID, []client.Option{}...)
	if err := db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		now := time.Now()
		args := make([]any, 0, 4+len(docIDs))
		querySQL := fmt.Sprintf(deleteSegmentImageByDocID, placeholder(len(docIDs)))
		args = append(args, model.SegmentIsDeleted, now, robotID)
		for _, docID := range docIDs {
			args = append(args, docID)
		}
		args = append(args, model.SegmentIsNotDeleted)
		if _, err := tx.ExecContext(ctx, querySQL, args...); err != nil {
			log.ErrorContextf(ctx, "DeleteSegmentImages|sql:%s args:%+v err:%+v", querySQL, args, err)
			return err
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "DeleteSegmentImages|err:%+v", err)
		return err
	}
	return nil
}

// ResumeSegments 恢复文档分片,删除的逆操作
func (d *dao) ResumeSegments(ctx context.Context, segments []*model.DocSegmentExtend, robotID uint64) error {
	db := knowClient.DBClient(ctx, docSegmentTableName, robotID, []client.Option{}...)
	var allImagesIDs []uint64
	if err := db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		for _, segment := range segments {
			querySQL := deleteSegment
			segment.IsDeleted = model.SegmentIsNotDeleted
			segment.UpdateTime = time.Now()
			segment.NextAction = model.NextActionUpdate
			segment.ReleaseStatus = model.SegmentReleaseStatusInit
			if segment.IsSegmentForQA() {
				// 用于生成QA的切片不需要发布
				segment.NextAction = model.NextActionAdd
				segment.ReleaseStatus = model.SegmentReleaseStatusNotRequired
			}
			if _, err := tx.NamedExecContext(ctx, querySQL, segment); err != nil {
				log.ErrorContextf(ctx, "恢复文档分片失败 sql:%s segment:%+v err:%+v", querySQL, segment, err)
				return err
			}
			// 切片页码
			querySQL = deleteSegmentPageInfoBySegID
			args := make([]any, 0, 4)
			args = append(args, segment.IsDeleted, time.Now(), segment.RobotID, segment.ID)
			if _, err := tx.ExecContext(ctx, querySQL, args...); err != nil {
				log.ErrorContextf(ctx, "恢复文档分片页码失败 sql:%s args:%+v err:%+v", querySQL, args, err)
				return err
			}
			// 图片ID
			querySQL = getSegmentImageIdsBySegID
			args = make([]any, 0, 3)
			args = append(args, segment.RobotID, segment.ID, model.SegmentIsDeleted)
			imageIds := make([]uint64, 0)
			if err := tx.Select(&imageIds, querySQL, args...); err != nil {
				log.ErrorContextf(ctx, "获取文档分片图片 sql:%s, args:%+v|err:%+v", querySQL, args, err)
				return err
			}
			// 切片图片
			querySQL = deleteSegmentImageBySegID
			args = make([]any, 0, 4)
			args = append(args, segment.IsDeleted, time.Now(), segment.RobotID, segment.ID)
			if _, err := tx.ExecContext(ctx, querySQL, args...); err != nil {
				log.ErrorContextf(ctx, "恢复文档分片图片失败 sql:%s args:%+v err:%+v", querySQL, args, err)
				return err
			}
			allImagesIDs = append(allImagesIDs, imageIds...)
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "恢复文档分片失败 err:%+v", err)
		return err
	}
	// 恢复图片向量
	err := UpdateImageVectorDeleteStatus(ctx, robotID, allImagesIDs, model.SegmentImageVectorIsNotDeleted)
	if err != nil {
		return err
	}

	return nil
}

// GetSegmentListCount 获取segment列表数量
func (d *dao) GetSegmentListCount(ctx context.Context, corpID, docID, robotID uint64) (uint64, error) {
	args := make([]any, 0, 3)
	args = append(args, corpID, docID, model.SegmentIsNotDeleted)
	var total uint64
	querySQL := getSegmentCount
	db := knowClient.DBClient(ctx, docSegmentTableName, robotID, []client.Option{}...)
	if err := db.Get(ctx, &total, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取segment列表数量失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return 0, err
	}
	return total, nil
}

// GetSegmentList 获取segment列表
func (d *dao) GetSegmentList(ctx context.Context, corpID, docID uint64, page, pageSize uint32, robotID uint64) (
	[]*model.DocSegmentExtend, error) {
	args := make([]any, 0, 5)
	offset := (page - 1) * pageSize
	args = append(args, corpID, docID, model.SegmentIsNotDeleted, offset, pageSize)
	querySQL := fmt.Sprintf(getSegmentList, docSegmentFields)
	list := make([]*model.DocSegmentExtend, 0)
	db := knowClient.DBClient(ctx, docSegmentTableName, robotID, []client.Option{}...)
	if err := db.QueryToStructs(ctx, &list, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取segment列表失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	return list, nil
}

// GetSegmentDeletedList 获取删除的segment列表
func (d *dao) GetSegmentDeletedList(ctx context.Context, corpID, docID uint64, page, pageSize uint32, robotID uint64) (
	[]*model.DocSegmentExtend, error) {
	args := make([]any, 0, 5)
	offset := (page - 1) * pageSize
	args = append(args, corpID, docID, model.SegmentIsDeleted, offset, pageSize)
	querySQL := fmt.Sprintf(getSegmentList, docSegmentFields)
	list := make([]*model.DocSegmentExtend, 0)
	db := knowClient.DBClient(ctx, docSegmentTableName, robotID, []client.Option{}...)
	if err := db.QueryToStructs(ctx, &list, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取删除的segment列表失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	return list, nil
}

// BatchUpdateSegmentContent 批量更新文档分片
func (d *dao) BatchUpdateSegmentContent(ctx context.Context, segments []*model.DocSegmentExtend, robotID uint64) error {
	db := knowClient.DBClient(ctx, docSegmentTableName, robotID, []client.Option{}...)
	if err := db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		for _, segment := range segments {
			querySQL := updateSegmentContent
			segment.UpdateTime = time.Now()
			if !segment.IsNextActionAdd() {
				segment.NextAction = model.NextActionUpdate
				segment.ReleaseStatus = model.SegmentReleaseStatusInit
			}
			if _, err := tx.NamedExecContext(ctx, querySQL, segment); err != nil {
				log.ErrorContextf(ctx, "批量更新文档分片内容失败 sql:%s segment:%+v err:%+v", querySQL, segment, err)
				return err
			}
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "批量更新文档分片内容失败 err:%+v", err)
		return err
	}
	return nil
}

// BatchUpdateSegment 批量更新文档分片
func (d *dao) BatchUpdateSegment(ctx context.Context, segments []*model.DocSegmentExtend, robotID uint64) error {
	db := knowClient.DBClient(ctx, docSegmentTableName, robotID, []client.Option{}...)
	if err := db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		for _, segment := range segments {
			querySQL := updateSegment
			segment.UpdateTime = time.Now()
			if !segment.IsNextActionAdd() {
				segment.NextAction = model.NextActionUpdate
				segment.ReleaseStatus = model.SegmentReleaseStatusInit
			}
			if _, err := tx.NamedExecContext(ctx, querySQL, segment); err != nil {
				log.ErrorContextf(ctx, "批量更新文档分片失败 sql:%s segment:%+v err:%+v", querySQL, segment, err)
				return err
			}
			// 因为不会编辑文档不会更新文档的内容本身，所以不需要更新ES
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "批量更新文档分片失败 segments:%+v err:%+v", segments, err)
		return err
	}

	return nil
}

// UpdateSegmentReleaseStatus  更新文档分片
func (d *dao) UpdateSegmentReleaseStatus(ctx context.Context, segment *model.DocSegmentExtend, robotID uint64) error {
	db := knowClient.DBClient(ctx, docSegmentTableName, robotID, []client.Option{}...)
	if err := db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		querySQL := updateSegmentReleaseStatus
		segment.UpdateTime = time.Now()
		if _, err := tx.NamedExecContext(ctx, querySQL, segment); err != nil {
			log.ErrorContextf(ctx, "UpdateSegmentReleaseStatus sql:%s segment:%+v err:%+v", querySQL, segment, err)
			return err
		}
		// 因为不会编辑文档不会更新文档的内容本身，所以不需要更新ES
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "UpdateSegmentReleaseStatus segment:%+v err:%+v", segment, err)
		return err
	}

	return nil
}

// BatchUpdateSegmentReleaseStatus 批量更新文档分片
func (d *dao) BatchUpdateSegmentReleaseStatus(ctx context.Context, segs []*model.DocSegmentExtend,
	status uint32, robotID uint64) error {
	db := knowClient.DBClient(ctx, docSegmentTableName, robotID, []client.Option{}...)
	if err := db.Transactionx(ctx, func(tx *sqlx.Tx) error {
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

			args := make([]any, 0, 2+len(batch))
			args = append(args, now, status)
			sql := fmt.Sprintf(batchUpdateSegmentReleaseStatus, placeholder(len(batch)))
			for _, seg := range batch {
				args = append(args, seg.ID)
			}
			if _, err := tx.ExecContext(ctx, sql, args...); err != nil {
				log.ErrorContextf(ctx, "BatchUpdateSegmentReleaseStatus|sql:%s args:%+v err:%+v", sql, args, err)
				return err
			}
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "UpdateSegmentReleaseStatus segs:%+v err:%+v", segs, err)
		return err
	}

	return nil
}

// GetSegmentChunk 分段获取文段
func (d *dao) GetSegmentChunk(ctx context.Context, corpID, appID, offset, limit uint64) ([]*model.DocSegment, error) {
	query := getSegmentChunk
	// 表格解析的结果不需要写向量，向量升级需要排除表格数据
	args := []any{
		corpID, appID, model.SegmentIsNotDeleted, model.SegmentTypeIndex, model.SegmentTypeQAAndIndex,
		model.SegmentTypeTable, model.SegmentTypeText2SQLMeta, model.SegmentTypeText2SQLContent,
		offset, limit,
	}
	var segments []*model.DocSegment
	db := knowClient.DBClient(ctx, docSegmentTableName, appID, []client.Option{}...)
	if err := db.Select(ctx, &segments, query, args...); err != nil {
		log.ErrorContextf(ctx, "GetSegmentChunk fail, query: %s args: %+v err: %v", query, args, err)
		return nil, err
	}
	return segments, nil
}

// GetSegmentChunkCount 获取文段总数
func (d *dao) GetSegmentChunkCount(ctx context.Context, corpID, appID uint64) (int, error) {
	query := getSegmentChunkCount
	args := []any{corpID, appID, model.SegmentIsNotDeleted, model.SegmentTypeIndex, model.SegmentTypeQAAndIndex}
	var count int
	db := knowClient.DBClient(ctx, docSegmentTableName, appID, []client.Option{}...)
	if err := db.Get(ctx, &count, query, args...); err != nil {
		log.ErrorContextf(ctx, "GetSegmentCount fail, query: %s args: %+v err: %v", query, args, err)
		return 0, err
	}
	return count, nil
}

// GetSegmentSyncChunk 分段获取同步文段
func (d *dao) GetSegmentSyncChunk(ctx context.Context, corpID, appID, offset, limit uint64) (
	[]*model.DocSegment, error) {
	query := getSegmentSyncChunk
	// 表格解析的结果不需要写向量，向量升级需要排除表格数据
	args := []any{
		corpID, appID, model.SegmentIsNotDeleted, model.SegmentTypeIndex, model.SegmentTypeQAAndIndex,
		model.SegmentTypeText2SQLMeta, model.SegmentTypeText2SQLContent,
		offset, limit,
	}
	var segments []*model.DocSegment
	db := knowClient.DBClient(ctx, docSegmentTableName, appID, []client.Option{}...)
	if err := db.Select(ctx, &segments, query, args...); err != nil {
		log.ErrorContextf(ctx, "GetSegmentChunk fail, query: %s args: %+v err: %v", query, args, err)
		return nil, err
	}
	return segments, nil
}

// GetSegmentSyncChunkCount 获取同步文段总数
func (d *dao) GetSegmentSyncChunkCount(ctx context.Context, corpID, appID uint64) (int, error) {
	query := getSegmentSyncChunkCount
	args := []any{corpID, appID, model.SegmentIsNotDeleted, model.SegmentTypeIndex, model.SegmentTypeQAAndIndex}
	var count int
	db := knowClient.DBClient(ctx, docSegmentTableName, appID, []client.Option{}...)
	if err := db.Get(ctx, &count, query, args...); err != nil {
		log.ErrorContextf(ctx, "GetSegmentCount fail, query: %s args: %+v err: %v", query, args, err)
		return 0, err
	}
	return count, nil
}

// UpdateSegmentSyncOrgDataBizID 更新同步文段 org_data_biz_id
func (d *dao) UpdateSegmentSyncOrgDataBizID(ctx context.Context, robotID, docID, corpID, staffID uint64,
	ids []uint64, orgDataBizID uint64) error {
	db := knowClient.DBClient(ctx, docSegmentTableName, robotID, []client.Option{}...)
	if err := db.Transactionx(ctx, func(tx *sqlx.Tx) error {
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
			args := make([]any, 0, len(tmpIDs)+5)
			args = append(args, orgDataBizID, corpID, robotID, docID, staffID)
			for _, id := range tmpIDs {
				args = append(args, id)
			}
			querySQL := fmt.Sprintf(updateSegmentOrdDataBizIDByID, placeholder(len(tmpIDs)))
			if _, err := tx.ExecContext(ctx, querySQL, args...); err != nil {
				log.ErrorContextf(ctx, "UpdateSegmentSyncOrgDataBizID sql:%s args:%+v err:%+v", querySQL, args,
					err)
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

// GetText2SqlSegmentMeta 通过DocID获取Text2Sql的meta数据；
func (d *dao) GetText2SqlSegmentMeta(ctx context.Context, docID uint64, robotID uint64) ([]*model.DocSegmentExtend, error) {
	querySQL := getText2SqlSegmentMetaByDocID
	segments := make([]*model.DocSegmentExtend, 0)
	args := make([]any, 0, 3)
	args = append(args, docID, model.SegmentTypeText2SQLMeta, model.SegmentIsNotDeleted)
	db := knowClient.DBClient(ctx, docSegmentTableName, robotID, []client.Option{}...)
	if err := db.QueryToStructs(ctx, &segments, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "通过DocID获取Text2Sql的meta数据失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	return segments, nil
}

// GetSegmentPageInfosBySegIDs 通过SegIDs获取切片的页码信息
func (d *dao) GetSegmentPageInfosBySegIDs(ctx context.Context, robotID uint64, segIDs []uint64) (
	map[uint64]*model.DocSegmentPageInfo, error) {
	segmentPageInfoMap := make(map[uint64]*model.DocSegmentPageInfo)
	if len(segIDs) == 0 {
		log.InfoContextf(ctx, "GetSegmentPageInfosBySegIDs|len(segIDs):%d|ignore", len(segIDs))
		return segmentPageInfoMap, nil
	}
	querySQL := fmt.Sprintf(getSegmentPageInfoBySegID, docSegmentPageInfoFields, placeholder(len(segIDs)))
	segmentPageInfos := make([]*model.DocSegmentPageInfo, 0)
	args := make([]any, 0, 1+len(segIDs))
	args = append(args, robotID)
	for _, segID := range segIDs {
		args = append(args, segID)
	}
	db := knowClient.DBClient(ctx, docSegmentPageInfoTableName, robotID, []client.Option{}...)
	if err := db.QueryToStructs(ctx, &segmentPageInfos, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "GetSegmentPageInfosBySegIDs failed|sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	for _, segPageInfo := range segmentPageInfos {
		segmentPageInfoMap[segPageInfo.SegmentID] = segPageInfo
	}
	return segmentPageInfoMap, nil
}

// UpdateQaSegmentStatus 更新分片生成QA状态
func (d *dao) UpdateQaSegmentStatus(ctx context.Context, segment *model.DocSegmentExtend, robotID uint64) error {
	querySQL := updatesQaStatus
	segment.UpdateTime = time.Now()
	db := knowClient.DBClient(ctx, docSegmentTableName, robotID, []client.Option{}...)
	if _, err := db.NamedExec(ctx, querySQL, segment); err != nil {
		log.ErrorContextf(ctx, "更新分片生成QA状态失败 sql:%s segment:%+v err:%+v", querySQL, segment, err)
		return err
	}
	return nil
}

// UpdateQaSegmentToDocStatus 还原切片状态
func (d *dao) UpdateQaSegmentToDocStatus(ctx context.Context, docID uint64, batchID int, robotID uint64) error {
	querySQL := updateQaSegmentToDocStatus
	args := make([]any, 0, 4)
	args = append(args, model.SegmentStatusDone, docID, batchID, model.SegmentIsNotDeleted)
	db := knowClient.DBClient(ctx, docSegmentTableName, robotID, []client.Option{}...)
	if _, err := db.Exec(ctx, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "更新分片生成QA状态失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return err
	}
	return nil
}

// UpdateQaSegmentToDocStatusTx 还原切片状态
func (d *dao) UpdateQaSegmentToDocStatusTx(ctx context.Context, tx *sqlx.Tx, docID uint64, batchID int) error {
	querySQL := updateQaSegmentToDocStatus
	args := make([]any, 0, 4)
	args = append(args, model.SegmentStatusDone, docID, batchID, model.SegmentIsNotDeleted)
	if _, err := tx.ExecContext(ctx, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "更新分片生成QA状态失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return err
	}
	return nil
}

// SegmentCommonIDsToBizIDs 基础信息获取
func (d *dao) SegmentCommonIDsToBizIDs(ctx context.Context, corpID, appID, staffID, docID uint64) (corpBizID, appBizID, staffBizID, docBizID uint64, err error) {
	corp, err := d.GetCorpByID(ctx, corpID)
	if err != nil || corp == nil {
		return 0, 0, 0, 0, errs.ErrCorpNotFound
	}
	corpBizID = corp.BusinessID
	app, err := d.GetAppByID(ctx, appID)
	if err != nil || app == nil {
		return 0, 0, 0, 0, errs.ErrAppNotFound
	}
	appBizID = app.BusinessID
	if staffID != 0 {
		staff, err := d.GetStaffByID(ctx, staffID)
		if err != nil || staff == nil {
			return 0, 0, 0, 0, errs.ErrStaffNotFound
		}
		staffBizID = staff.BusinessID
	}
	if docID != 0 {
		doc, err := d.GetDocByID(ctx, docID, appID)
		if err != nil || doc == nil {
			return 0, 0, 0, 0, errs.ErrDocNotFound
		}
		docBizID = doc.BusinessID
	}
	return corpBizID, appBizID, staffBizID, docBizID, nil
}

// GetSheetByName 获取sheet
func GetSheetByName(ctx context.Context, corpBizID, appBizID, docBizID uint64,
	sheetName string) ([]*model.DocSegmentSheetTemporary, error) {
	log.InfoContextf(ctx, "GetSheetByName|start|SheetName:%s", sheetName)
	deletedFlag := IsNotDeleted
	filter := &DocSegmentSheetTemporaryFilter{
		CorpBizID:  corpBizID,
		AppBizID:   appBizID,
		DocBizID:   docBizID,
		IsDeleted:  &deletedFlag,
		SheetNames: []string{sheetName},
		Offset:     0,
		Limit:      1,
	}
	list, err := GetDocSegmentSheetTemporaryDao().GetSheetList(ctx, DocSegmentSheetTemporaryTblColList, filter)
	if err != nil {
		log.ErrorContextf(ctx, "GetSheetByName|err:%+v", err)
		return nil, err
	}
	return list, nil
}

// GetSheetByNameWithCache 获取sheet
func (d *dao) GetSheetByNameWithCache(ctx context.Context, corpBizID, appBizID, docBizID uint64,
	sheetName string, sheetSyncMap *sync.Map) (*model.DocSegmentSheetTemporary, error) {
	log.InfoContextf(ctx, "GetSheetByNameWithCache|start|SheetName:%s", sheetName)
	if sheetSyncMap == nil {
		log.ErrorContextf(ctx, "GetSheetByNameWithCache|sheetSyncMap is null")
		return nil, errs.ErrSystem
	}
	hash := sha256.New()
	_, err := io.WriteString(hash, sheetName)
	if err != nil {
		log.ErrorContextf(ctx, "GetSheetByNameWithCache|WriteString|err:%+v", err)
		return nil, err
	}
	hashValue := hash.Sum(nil)
	uniqueKey := string(hashValue)
	if value, ok := sheetSyncMap.Load(uniqueKey); ok {
		if sheet, ok := value.(*model.DocSegmentSheetTemporary); ok {
			return sheet, nil
		}
	}
	sheets, err := GetSheetByName(ctx, corpBizID, appBizID, docBizID, sheetName)
	if err != nil {
		log.ErrorContextf(ctx, "GetSheetByName|err:%+v", err)
		return nil, err
	}
	if len(sheets) > 0 {
		// 存入sheet数据
		sheetSyncMap.Store(uniqueKey, sheets[0])
		return sheets[0], nil
	}
	log.ErrorContextf(ctx, "GetSheetByNameWithCache|SheetName not found|sheetName:%s", sheetName)
	return nil, errs.ErrDocSegmentSheetNotFound
}

// GetSheetFromDocSegment 从切片中获取sheet信息
func (d *dao) GetSheetFromDocSegment(ctx context.Context, segment *model.DocSegmentExtend,
	corpBizID, appBizID, docBizID uint64, sheetSyncMap *sync.Map) (*model.DocSegmentSheetTemporary, error) {
	if segment == nil {
		log.ErrorContextf(ctx, "GetSheetFromDocSegment|segment is null")
		return nil, errs.ErrDocSegmentNotFound
	}
	segmentPageInfoMap, err := d.GetSegmentPageInfosBySegIDs(ctx, segment.RobotID, []uint64{segment.ID})
	if err != nil {
		return nil, err
	}
	sheetName := ""
	if pageInfo, ok := segmentPageInfoMap[segment.ID]; ok {
		if pageInfo != nil {
			// SheetData为空则SheetName为空
			if pageInfo.SheetData != "" {
				// 解析sheetName
				sheetInfos, sheetData := make([]string, 0), make([]*knowledge.PageContent_SheetData, 0)
				if err := jsoniter.UnmarshalFromString(pageInfo.SheetData, &sheetData); err != nil {
					log.WarnContextf(ctx, "GetSheetFromDocSegmentPageInfo|SheetInfos|UnmarshalFromString err:%+v", err)
				}
				for _, sheet := range sheetData {
					sheetInfos = append(sheetInfos, sheet.SheetName)
				}
				if len(sheetInfos) > 0 {
					// todo 目前一个segment只对应一个sheet
					sheetName = sheetInfos[0]
				}
			}
		}
	}
	log.InfoContextf(ctx, "GetSheetFromDocSegment|SheetName:%s", sheetName)
	return d.GetSheetByNameWithCache(ctx, corpBizID, appBizID, docBizID,
		sheetName, sheetSyncMap)
}

func (d *dao) BatchDeleteSegmentsAndKnowledge(ctx context.Context, doc *model.Doc, embeddingModelName string) error {
	appDB, err := d.GetAppByID(ctx, doc.RobotID)
	if err != nil {
		return err
	}
	ctx = pkg.WithSpaceID(ctx, appDB.SpaceID)
	total, err := d.GetSegmentListCount(ctx, doc.CorpID, doc.ID, doc.RobotID)
	if err != nil {
		return err
	}
	pageSize := 500
	pages := int(math.Ceil(float64(total) / float64(pageSize)))
	for i := 1; i <= pages; i++ {
		page := uint32(i)
		docSegments, err := d.GetSegmentList(ctx, doc.CorpID, doc.ID, page, uint32(pageSize), doc.RobotID)
		if err != nil {
			return err
		}
		deleteKnowledgeSegments := make([]*model.DocSegmentExtend, 0)
		for _, seg := range docSegments {
			if !seg.IsSegmentForQA() && !seg.IsText2sqlSegmentType() {
				deleteKnowledgeSegments = append(deleteKnowledgeSegments, seg)
			}
		}
		if err = d.BatchDeleteSegments(ctx, docSegments, doc.RobotID); err != nil {
			return err
		}
		if len(deleteKnowledgeSegments) > 0 {
			embeddingConf, _, err := appDB.GetEmbeddingConf()
			if err != nil {
				log.ErrorContextf(ctx, "task(DocDelete) GetEmbeddingConf() err:%+v", err)
				return err
			}
			embeddingVersion := embeddingConf.Version
			if err = d.BatchDirectDeleteSegmentKnowledge(ctx, appDB.ID,
				deleteKnowledgeSegments, embeddingVersion, embeddingModelName); err != nil {
				return err
			}
		}
	}
	return nil
}

func DeleteSheetDbTableAndColumns(ctx context.Context, corpBizID, appBizID, docBizID, robotID uint64) error {
	metaMappings, err := GetDocMetaDataDao().GetDocMetaDataByDocId(ctx, docBizID, robotID)
	if err != nil {
		log.ErrorContextf(ctx, "GetDocMetaDataByDocId err: %v", err)
		return err
	}

	for _, mapping := range metaMappings {
		isExist, err := GetDBTableDao().Text2sqlExistsByDbSourceBizID(ctx, corpBizID, appBizID, mapping.BusinessID)
		if err != nil {
			return err
		}
		if !isExist {
			continue
		}

		dbTable, err := GetDBTableDao().Text2sqlGetByDbSourceBizID(ctx, corpBizID, appBizID, mapping.BusinessID)
		if err != nil {
			return err
		}
		log.InfoContextf(ctx, "delete db table %v %v for doc", dbTable.DBTableBizID, dbTable.AliasName)
		err = GetDBTableDao().SoftDeleteByBizID(ctx, corpBizID, appBizID, dbTable.DBTableBizID)
		if err != nil {
			return err
		}
		err = GetDBTableColumnDao().SoftDeleteByTableBizID(ctx, corpBizID, appBizID, dbTable.DBTableBizID)
		if err != nil {
			return err
		}
	}
	return nil
}
