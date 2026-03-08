package doc

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/common"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/doc_intervene"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
	knowledge "git.woa.com/dialogue-platform/lke_proto/pb-protocol/knowledge"
	jsoniter "github.com/json-iterator/go"
	"google.golang.org/protobuf/proto"
	"gorm.io/gorm"
)

const (
	ModifyTableSheetDeleteOperate                   = 0
	ModifyTableSheetDisabledOperate                 = 1
	ModifyTableSheetEnableOperate                   = 2
	ModifyTableSheetDisabledRetrievalEnhanceOperate = 3
	ModifyTableSheetEnableRetrievalEnhanceOperate   = 4
	BlockTypeTitle                                  = 1
	BlockTypeTable                                  = 2
	TableMarkerMarkdown                             = "| --- |"
	TableMarkerHTML                                 = "<table>"
)

// ListTableSheet 获取表格文档sheet列表
func ListTableSheet(ctx context.Context, req *pb.ListTableSheetReq, d dao.Dao,
	docCommon *model.DocSegmentCommon, doc *model.Doc) (*pb.ListTableSheetRsp, error) {
	log.InfoContextf(ctx, "ListTableSheet|start")
	rsp := new(pb.ListTableSheetRsp)
	sheetList := make([]*pb.ListTableSheetRsp_TableSheetItem, 0)
	if req.PageNumber < 1 || req.PageSize < 1 {
		log.ErrorContextf(ctx, "ListTableSheet|params err|req:%+v", req)
		return rsp, errs.ErrParams
	}
	// 获取所有sheet的数量
	deletedFlag := dao.IsNotDeleted
	filter := &dao.DocSegmentSheetTemporaryFilter{
		CorpBizID: docCommon.CorpBizID,
		AppBizID:  docCommon.AppBizID,
		DocBizID:  docCommon.DocBizID,
		IsDeleted: &deletedFlag,
	}
	num, err := dao.GetDocSegmentSheetTemporaryDao().GetDocSheetCount(ctx, filter)
	if err != nil {
		log.ErrorContextf(ctx, "GetDocOrgDataCountByDocBizID|GetDocSegmentOrgDataDao|err:%v", err)
		return rsp, errs.ErrSystem
	}
	if doc.IsAuditFailed() {
		deletedFlag := dao.IsNotDeleted
		filter := &dao.DocSegmentSheetTemporaryFilter{
			CorpBizID: docCommon.CorpBizID,
			AppBizID:  docCommon.AppBizID,
			DocBizID:  doc.BusinessID,
			IsDeleted: &deletedFlag,
			AuditStatus: []uint32{
				uint32(model.DocSegmentAuditStatusContentFailed)},
		}
		auditFailNum, err := dao.GetDocSegmentSheetTemporaryDao().GetDocSheetCount(ctx, filter)
		if err != nil {
			log.ErrorContextf(ctx, "getDocNotDeleteTemporaryOrgData|GetDocOrgDataCount|err:%v", err)
			return nil, err
		}
		rsp.AuditFailedNumber = uint64(auditFailNum)
	}
	rsp.FileSize = strconv.FormatUint(doc.FileSize, 10)
	rsp.SegmentNumber = strconv.FormatInt(num, 10)
	// 确认改文档是否被编辑过（临时表的version是否都为0，text2sql是否有改动）
	intervene, err := CheckTableIntervene(ctx, docCommon)
	if err != nil {
		return nil, err
	}
	rsp.IsModify = intervene
	// 确认临时表中存在数据
	tempList, err := GetSheetListIncludeDeleted(ctx, req, docCommon)
	if err != nil {
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errs.ErrSystem
		}
	}
	if len(tempList) > 0 {
		// 从临时表获取sheet列表
		list, err := GetSheetList(ctx, d, req, docCommon)
		if err != nil {
			return nil, err
		}
		log.InfoContextf(ctx, "ListTableSheet|len(SheetList):%d", len(list))
		// 临时表有，直接返回
		sheetList = SheetTemporaryListToRspSheetList(ctx, list)
		rsp.Total = uint64(len(list))
		rsp.SheetList = sheetList
		return rsp, nil
	}
	err = StoreSheetByDocParse(ctx, d, docCommon)
	if err != nil {
		log.ErrorContextf(ctx, "ListTableSheet|StoreSheetByDocParse|err:%+v", err)
		return nil, errs.ErrSystem
	}
	// 4.存储后查询数据库
	// 获取所有sheet的数量
	deletedFlag = dao.IsNotDeleted
	filter = &dao.DocSegmentSheetTemporaryFilter{
		CorpBizID: docCommon.CorpBizID,
		AppBizID:  docCommon.AppBizID,
		DocBizID:  docCommon.DocBizID,
		IsDeleted: &deletedFlag,
	}
	num, err = dao.GetDocSegmentSheetTemporaryDao().GetDocSheetCount(ctx, filter)
	if err != nil {
		log.ErrorContextf(ctx, "GetDocOrgDataCountByDocBizID|GetDocSegmentOrgDataDao|err:%v", err)
		return rsp, errs.ErrSystem
	}
	rsp.SegmentNumber = strconv.FormatInt(num, 10)
	list, err := GetSheetList(ctx, d, req, docCommon)
	if err != nil || len(list) == 0 {
		return rsp, errs.ErrSystem
	}
	sheetList = SheetTemporaryListToRspSheetList(ctx, list)
	rsp.Total = uint64(len(list))
	rsp.SheetList = sheetList
	return rsp, nil
}

// ModifyTableSheet 修改表格文档sheet
func ModifyTableSheet(ctx context.Context, req *pb.ModifyTableSheetReq, d dao.Dao,
	docCommon *model.DocSegmentCommon) (*pb.ModifyTableSheetRsp, error) {
	log.InfoContextf(ctx, "ModifyTableSheet|start")
	rsp := &pb.ModifyTableSheetRsp{}
	newSheets := req.GetModifyTableSheets()
	// 查找ID对应的sheet并更新，事务
	err := d.GetTdsqlGormDB().Transaction(func(tx *gorm.DB) error {
		if len(req.ModifyTableSheets) > 0 {
			err := ModifyTableSheetContent(ctx, d, docCommon, newSheets, tx)
			if err != nil {
				return err
			}
		}
		if len(req.DeleteSheetBizIds) > 0 {
			err := ModifyTableSheetByOperateAndSheetBizID(ctx, docCommon, req.DeleteSheetBizIds, ModifyTableSheetDeleteOperate, tx)
			if err != nil {
				return err
			}
		}
		if len(req.DisabledSheetBizIds) > 0 {
			err := ModifyTableSheetByOperateAndSheetBizID(ctx, docCommon, req.DisabledSheetBizIds, ModifyTableSheetDisabledOperate, tx)
			if err != nil {
				return err
			}
		}
		if len(req.EnableSheetBizIds) > 0 {
			err := ModifyTableSheetByOperateAndSheetBizID(ctx, docCommon, req.EnableSheetBizIds, ModifyTableSheetEnableOperate, tx)
			if err != nil {
				return err
			}
		}
		if len(req.DisabledRetrievalEnhanceSheetNames) > 0 {
			err := ModifyTableSheetByOperateAndSheetName(ctx, docCommon, req.DisabledRetrievalEnhanceSheetNames,
				ModifyTableSheetDisabledRetrievalEnhanceOperate, tx)
			if err != nil {
				return err
			}
		}
		if len(req.EnableRetrievalEnhanceSheetNames) > 0 {
			err := ModifyTableSheetByOperateAndSheetName(ctx, docCommon, req.EnableRetrievalEnhanceSheetNames,
				ModifyTableSheetEnableRetrievalEnhanceOperate, tx)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		log.ErrorContextf(ctx, "ModifyTableSheet|err:%+v", err)
		return rsp, errs.ErrSystem
	}
	return rsp, nil
}

// GetSheetList 获取sheet
func GetSheetList(ctx context.Context, d dao.Dao, req *pb.ListTableSheetReq,
	docCommon *model.DocSegmentCommon) ([]*model.DocSegmentSheetTemporary, error) {
	log.InfoContextf(ctx, "GetSheetList|start")
	// 如果参考ID存在（对话处使用输入后会检索t_refer表）
	if req.ReferBizId != "" {
		segment, _ := GetSegmentByReferID(ctx, d, docCommon.AppID, req.ReferBizId)
		if segment != nil {
			// 通过sheetName定位sheet数据
			// 兼容共享知识库
			app, err := d.GetAppByID(ctx, segment.RobotID)
			if err != nil {
				return nil, err
			}
			docCommon.AppID = segment.RobotID
			docCommon.AppBizID = app.BusinessID
			sheet, err := GetSheetFromDocSegmentPageInfo(ctx, d, segment.ID, docCommon)
			if errors.Is(err, errs.ErrDocSegmentSheetNotFound) {
				// 返回空数组（兼容干预后切片ID变化的情况）
				return []*model.DocSegmentSheetTemporary{}, nil
			} else if err != nil {
				return nil, err
			}
			if sheet == nil {
				// 返回空数组（兼容干预后切片ID变化的情况）
				return []*model.DocSegmentSheetTemporary{}, nil
			} else {
				return []*model.DocSegmentSheetTemporary{sheet}, nil
			}
		} else {
			// text2sql没有参考来源的片段id，这里如果查到的segment为nil，无法跳转到指定片段，则走底下的获取文档片段兜底
		}
	}
	deletedFlag := dao.IsNotDeleted
	filter := &dao.DocSegmentSheetTemporaryFilter{
		CorpBizID:      docCommon.CorpBizID,
		AppBizID:       docCommon.AppBizID,
		DocBizID:       docCommon.DocBizID,
		IsDeleted:      &deletedFlag,
		OrderColumn:    []string{dao.DocSegmentSheetTemporaryTblColSheetOrder},
		OrderDirection: []string{dao.SqlOrderByAsc},
		Offset:         common.GetOffsetByPage(req.PageNumber, req.PageSize),
		Limit:          req.PageSize,
	}
	list, err := dao.GetDocSegmentSheetTemporaryDao().GetSheetList(ctx, dao.DocSegmentSheetTemporaryTblColList, filter)
	if err != nil {
		log.ErrorContextf(ctx, "GetSheetList|err:%+v", err)
		return nil, errs.ErrSystem
	}
	return list, nil
}

func GetSheetListIncludeDeleted(ctx context.Context, req *pb.ListTableSheetReq,
	docCommon *model.DocSegmentCommon) ([]*model.DocSegmentSheetTemporary, error) {
	log.InfoContextf(ctx, "GetSheetList|start")
	filter := &dao.DocSegmentSheetTemporaryFilter{
		CorpBizID:      docCommon.CorpBizID,
		AppBizID:       docCommon.AppBizID,
		DocBizID:       docCommon.DocBizID,
		OrderColumn:    []string{dao.DocSegmentSheetTemporaryTblColSheetOrder},
		OrderDirection: []string{dao.SqlOrderByAsc},
		Offset:         common.GetOffsetByPage(req.PageNumber, req.PageSize),
		Limit:          req.PageSize,
	}
	list, err := dao.GetDocSegmentSheetTemporaryDao().GetSheetList(ctx, dao.DocSegmentSheetTemporaryTblColList, filter)
	if err != nil {
		return nil, err
	}
	return list, nil
}

// SheetTemporaryListToRspSheetList 输出结果转换
func SheetTemporaryListToRspSheetList(ctx context.Context,
	tempList []*model.DocSegmentSheetTemporary) []*pb.ListTableSheetRsp_TableSheetItem {
	sheetList := make([]*pb.ListTableSheetRsp_TableSheetItem, 0)
	for _, sheet := range tempList {
		sheetList = append(sheetList, &pb.ListTableSheetRsp_TableSheetItem{
			SheetName:                  sheet.SheetName,
			SheetBizId:                 strconv.FormatUint(sheet.BusinessID, 10),
			Bucket:                     sheet.Bucket,
			Region:                     sheet.Region,
			CosUrl:                     sheet.CosURL,
			CosHash:                    sheet.CosHash,
			FileName:                   sheet.FileName,
			FileType:                   sheet.FileType,
			IsOrigin:                   isSheetOrigin(sheet.Version),
			IsDisabled:                 sheet.IsDisabled == model.SegmentIsDisabled,
			IsDisabledRetrievalEnhance: sheet.IsDisabledRetrievalEnhance == model.SheetDisabledRetrievalEnhance,
			AuditStatus:                uint64(sheet.AuditStatus),
		})
	}
	return sheetList
}

func isSheetOrigin(version int) bool {
	if version == 0 {
		return true
	}
	return false
}

// TableSection sheet数据
type TableSection struct {
	Title   string
	Content []byte // 包含所有表格行（包括表头、分隔符、数据）
}

type Block struct {
	Content string
	Type    int // "table" 或 "title"
}

// GetTableSectionFromDocParse 解析md获取sheet
func GetTableSectionFromDocParse(ctx context.Context, d dao.Dao, appID, docID uint64, bucket string) ([]*TableSection, error) {
	log.InfoContextf(ctx, "GetTableSectionFromDocParse|start")
	// todo 优化，控制拆分并发
	var sheets []*TableSection
	docParse, err := d.GetDocParseByDocIDAndTypeAndStatus(ctx, docID, model.DocParseTaskTypeSplitSegment,
		model.DocParseSuccess, appID)
	if err != nil {
		log.ErrorContextf(ctx, "GetDocParseByDocIDAndTypeAndStatus|failed, err:%+v", err)
		return sheets, err
	}
	result := &knowledge.FileParserCallbackReq{}
	err = jsoniter.UnmarshalFromString(docParse.Result, result)
	if err != nil {
		log.ErrorContextf(ctx, "getDocParseContent|jsoniter.UnmarshalFromString failed, err:%+v", err)
		return sheets, err
	}
	log.InfoContextf(ctx, "GetDocParseByDocIDAndTypeAndStatus|file parse result:%+v", result)
	resultDataMap := result.GetResults()
	docParseRes := resultDataMap[int32(knowledge.FileParserSubDataType_FILE_PARSER_SUB_DATA_TYPE_PARSE)]
	// FileParserSubDataType_FILE_PARSER_SUB_DATA_TYPE_PARSE 只会返回一个result
	for _, res := range docParseRes.GetResult() {
		storageTypeKey := d.GetTypeKeyWithBucket(ctx, bucket)
		body, err := d.GetObjectWithTypeKey(ctx, storageTypeKey, res.GetResult())
		if err != nil {
			log.ErrorContextf(ctx, "getDocParseContent|GetObjectWithTypeKey|err:%+v", err)
			return sheets, err
		}
		// 解析pb
		unSerialPb := &knowledge.ParseResult{}
		if err = proto.Unmarshal(body, unSerialPb); err != nil {
			log.ErrorContextf(ctx, "GetTableSectionFromDocParse|cosURL:%s|proto.Unmarshal failed, err:%+v", res.GetResult(), err)
			return sheets, err
		}
		sections := splitSheet(ctx, unSerialPb.GetResult())
		sheets = append(sheets, sections...)
	}
	log.InfoContextf(ctx, "GetDocParseByDocIDAndTypeAndStatus|sheets:%+v", sheets)
	return sheets, nil
}

func splitSheet(ctx context.Context, file string) []*TableSection {
	log.InfoContextf(ctx, "GetDocParseByDocIDAndTypeAndStatus|splitSheet|:%s", file)
	blocks := parseBlocks(file)
	sections := make([]*TableSection, 0)
	if len(blocks) == 1 {
		sections = append(sections, &TableSection{
			Title:   "",
			Content: []byte(blocks[0].Content),
		})
	}
	if len(blocks) == 0 || len(blocks)%2 != 0 {
		log.ErrorContextf(ctx, "GetDocParseByDocIDAndTypeAndStatus|blocks is empty or unable to cope, len:%d",
			len(blocks))
		return sections
	}
	tableType := BlockTypeTitle
	table := new(TableSection)
	for _, block := range blocks {
		if block.Type != tableType {
			log.ErrorContextf(ctx, "GetDocParseByDocIDAndTypeAndStatus|blocks wrong order")
			return []*TableSection{}
		}
		if tableType == BlockTypeTitle {
			table.Title = block.Content
			tableType = BlockTypeTable
		} else {
			table.Content = []byte(block.Content)
			sections = append(sections, table)
			table = new(TableSection)
			tableType = BlockTypeTitle
		}
	}
	return sections
}

func parseBlocks(file string) []*Block {
	var blocks []*Block
	var currentBlock strings.Builder
	lines := strings.Split(file, "\n")
	//processHeadInvalidChar := false
	for i := 0; i < len(lines); i++ {
		// todo 处理头部无效数据(来源：解析服务bug)
		//if !processHeadInvalidChar && lines[i] != "" {
		//	lines[i] = removeInvalidChar(lines[i])
		//	processHeadInvalidChar = true
		//}
		trimmed := strings.TrimSpace(lines[i])
		if strings.HasPrefix(trimmed, "![") {
			// 跳过表格外部的图片，不处理
			continue
		}

		if trimmed == "" {
			if currentBlock.Len() > 0 {
				// 完成当前块的收集
				content := strings.TrimSpace(currentBlock.String())
				blockType := determineBlockType(content)
				blocks = append(blocks, &Block{
					Content: content,
					Type:    blockType,
				})
				currentBlock.Reset()
			}
			continue
		}

		// 添加内容到当前块
		if currentBlock.Len() > 0 {
			currentBlock.WriteString("\n")
		}
		currentBlock.WriteString(lines[i])
	}

	// 处理最后一个块
	if currentBlock.Len() > 0 {
		content := strings.TrimSpace(currentBlock.String())
		blockType := determineBlockType(content)
		blocks = append(blocks, &Block{
			Content: content,
			Type:    blockType,
		})

	}
	return blocks
}

func determineBlockType(content string) int {
	if strings.Contains(content, TableMarkerMarkdown) ||
		strings.Contains(content, TableMarkerHTML) {
		return BlockTypeTable
	}
	return BlockTypeTitle
}

func removeInvalidChar(s string) string {
	return strings.Map(func(r rune) rune {
		// 过滤 NAK (0x15)、ACK (0x06)、STX (0x02)、ETX (0x03)
		if r == utf8.RuneError || (r >= 0x00 && r <= 0x1F) || r == 0x7F {
			return -1 // 删除无效字符
		}
		return r
	}, s)
}

// UploadToCos 上传到cos
func UploadToCos(ctx context.Context, d dao.Dao, corpID uint64, content []byte) (string, string, string, error) {
	// 包含换行符拼接，上传到cos，存入表
	// 上传到COS
	fileName := fmt.Sprintf("sheet-%d-%d-%d.md", corpID, d.GenerateSeqID(), time.Now().Unix())
	cosPath := d.GetCorpCOSFilePath(ctx, corpID, fileName)
	if err := d.PutObject(ctx, content, cosPath); err != nil {
		log.ErrorContextf(ctx, "UploadToCos|upload sheet md|corpID:%d|cosPath:%s|err:%+v", corpID, cosPath, err)
		return "", "", "", err
	}
	log.InfoContextf(ctx, "UploadToCos|upload sheet md|fileName:%s|cosPath:%s", fileName, cosPath)
	// 这里objectInfo.ETag的结果会带有转义字符 类似 "\"5784a190d6af4214020f54edc87429ab\""
	// 需要对转义字符特殊处理
	objectInfo, err := d.StatObject(ctx, cosPath)
	if err != nil {
		log.ErrorContextf(ctx, "UploadToCos|StatObject|err:%+v", err)
		return "", "", "", err
	}
	log.InfoContextf(ctx, "UploadToCos|StatObject|Hash:%s|Etag:%s|", objectInfo.Hash, objectInfo.ETag)
	cosHash := objectInfo.Hash
	return cosPath, fileName, cosHash, nil
}

// CheckTableIntervene 检查文档是否有改动
func CheckTableIntervene(ctx context.Context, docCommon *model.DocSegmentCommon) (bool, error) {
	// 检查临时表数据的版本
	intervene, err := CheckTableContentIntervene(ctx, docCommon)
	if err != nil {
		return false, err
	}
	if intervene == true {
		return true, nil
	} else {
		// 检查text2sql数据是否有改动
		isChanged, err := doc_intervene.CheckTextToSqlTableIsChanged(ctx, docCommon.AppBizID, docCommon.DocBizID)
		if err != nil {
			log.ErrorContextf(ctx, "CheckTableIntervene|CheckTextToSqlTableIsChanged|err:%v", err)
			return false, err
		}
		log.InfoContextf(ctx, "CheckTextToSqlTableIsChanged|CheckTextToSqlTableIsChanged|isChanged:%t", isChanged)
		return isChanged, nil
	}
}

func CheckTableContentIntervene(ctx context.Context, docCommon *model.DocSegmentCommon) (bool, error) {
	// 检查临时表数据的版本
	versionFlag := model.SheetDefaultVersion
	tempFilter := &dao.DocSegmentSheetTemporaryFilter{
		CorpBizID: docCommon.CorpBizID,
		AppBizID:  docCommon.AppBizID,
		DocBizID:  docCommon.DocBizID,
		Version:   &versionFlag,
		Offset:    0,
		Limit:     1,
	}
	tempList, err := dao.GetDocSegmentSheetTemporaryDao().GetSheetList(ctx, dao.DocSegmentSheetTemporaryTblColList, tempFilter)
	if err != nil {
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			log.ErrorContextf(ctx, "CheckTableContentIntervene|GetSheetList|err:%v", err)
			return false, err
		}
	}
	if len(tempList) > 0 {
		log.InfoContextf(ctx, "CheckTableContentIntervene|update")
		return true, nil
	}
	filter := &dao.DocSegmentOrgDataTemporaryFilter{
		CorpBizID: docCommon.CorpBizID,
		AppBizID:  docCommon.AppBizID,
		DocBizID:  docCommon.DocBizID,
		Offset:    0,
		Limit:     1,
	}
	list, err := dao.GetDocSegmentOrgDataTemporaryDao().GetDocOrgDataList(ctx, dao.DocSegmentOrgDataTemporaryTblColList, filter)
	if err != nil {
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			log.ErrorContextf(ctx, "CheckTableContentIntervene|GetSheetList|err:%v", err)
			return false, err
		}
	}
	if len(list) > 0 {
		log.InfoContextf(ctx, "CheckTableContentIntervene|update")
		return true, nil
	}
	return false, nil
}

// ModifyTableSheetContent 更新sheet内容
func ModifyTableSheetContent(ctx context.Context, d dao.Dao, docCommon *model.DocSegmentCommon,
	newSheets []*pb.ModifyTableSheetReq_ModifyTableSheetItem, tx *gorm.DB) error {
	for _, sheet := range newSheets {
		// 检查文档格式是否为md
		if sheet.GetFileType() != model.FileTypeMD {
			return errs.ErrUnknownDocType
		}
		sheetBizId, err := util.CheckReqParamsIsUint64(ctx, sheet.SheetBizId)
		if err != nil {
			log.ErrorContextf(ctx, "ModifyTableSheet|SheetBizIdToUint64|err:%+v", err)
			return err
		}
		deletedFlag := dao.IsNotDeleted
		filter := &dao.DocSegmentSheetTemporaryFilter{
			CorpBizID:   docCommon.CorpBizID,
			AppBizID:    docCommon.AppBizID,
			DocBizID:    docCommon.DocBizID,
			BusinessIDs: []uint64{sheetBizId},
			IsDeleted:   &deletedFlag,
		}
		oldSheet, err := dao.GetDocSegmentSheetTemporaryDao().GetSheet(ctx,
			dao.DocSegmentSheetTemporaryTblColList, filter)
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return errs.ErrDocSegmentNotFound
			}
			return errs.ErrSystem
		}
		if oldSheet == nil {
			return errs.ErrDocSegmentNotFound
		}
		// todo 检查文件，当前对前端文件不重新存储
		// 通过hash拿到文件的etag
		if err = d.CheckURLFileByHash(ctx, docCommon.CorpID, docCommon.CorpBizID, docCommon.AppBizID, oldSheet.CosURL, oldSheet.CosHash); err != nil {
			log.ErrorContextf(ctx, "ModifyTableSheet|CheckURLFileByHash|err:%+v", err)
			return errs.ErrInvalidURL
		}
		newSheet := *oldSheet
		newSheet.Bucket = sheet.Bucket
		newSheet.Region = sheet.Region
		newSheet.CosURL = sheet.CosUrl
		newSheet.CosHash = sheet.CosHash
		newSheet.FileName = sheet.FileName
		newSheet.FileType = sheet.FileType

		updateColumns := []string{
			dao.DocSegmentSheetTemporaryTblColBucket,
			dao.DocSegmentSheetTemporaryTblColRegion,
			dao.DocSegmentSheetTemporaryTblColCosURL,
			dao.DocSegmentSheetTemporaryTblColCosHash,
			dao.DocSegmentSheetTemporaryTblColFileName,
			dao.DocSegmentSheetTemporaryTblColFileType,
			dao.DocSegmentSheetTemporaryTblColVersion,
		}
		updateFilter := &dao.DocSegmentSheetTemporaryFilter{
			CorpBizID:   docCommon.CorpBizID,
			AppBizID:    docCommon.AppBizID,
			DocBizID:    docCommon.DocBizID,
			BusinessIDs: []uint64{newSheet.BusinessID},
		}
		_, err = dao.GetDocSegmentSheetTemporaryDao().UpdateDocSegmentSheet(ctx, tx, updateColumns,
			updateFilter, &newSheet)
		if err != nil {
			log.ErrorContextf(ctx, "ModifyTableSheet|UpdateDocSegmentSheetWithVersion|err:%+v", err)
			return errs.ErrSystem
		}
	}
	return nil
}

// ModifyTableSheetByOperateAndSheetBizID 根据SheetBizID更新数据
func ModifyTableSheetByOperateAndSheetBizID(ctx context.Context, docCommon *model.DocSegmentCommon,
	bizIDs []string, operate int, tx *gorm.DB) error {
	log.InfoContextf(ctx, "ModifyTableSheetByOperate|Operate:%d", operate)
	for _, segBizID := range bizIDs {
		sheetBizID, err := util.CheckReqParamsIsUint64(ctx, segBizID)
		if err != nil {
			log.ErrorContextf(ctx, "ModifyTableSheetByOperateAndSheetBizID|SheetBizIdToUint64|err:%+v", err)
			return err
		}
		switch operate {
		case ModifyTableSheetDeleteOperate:
			err := DeleteSheetBySheetBizID(ctx, tx, docCommon, sheetBizID)
			if err != nil {
				log.ErrorContextf(ctx, "ModifyTableSheetByOperateAndSheetBizID|DeleteSheetBySheetBizID|err:%v", err)
				return err
			}
		case ModifyTableSheetDisabledOperate:
			err := dao.GetDocSegmentSheetTemporaryDao().DisabledDocSegmentSheet(ctx, tx, docCommon.CorpBizID,
				docCommon.AppBizID, docCommon.DocBizID, []uint64{sheetBizID})
			if err != nil {
				log.ErrorContextf(ctx, "ModifyTableSheetByOperateAndSheetBizID|DisabledDocSegmentSheet|err:%v", err)
				return err
			}
		case ModifyTableSheetEnableOperate:
			err := dao.GetDocSegmentSheetTemporaryDao().EnableDocSegmentSheet(ctx, tx, docCommon.CorpBizID,
				docCommon.AppBizID, docCommon.DocBizID, []uint64{sheetBizID})
			if err != nil {
				log.ErrorContextf(ctx, "ModifyTableSheetByOperateAndSheetBizID|EnableDocSegmentSheet|err:%v", err)
				return err
			}
		default:
			log.ErrorContextf(ctx, "ModifyTableSheetByOperateAndSheetBizID|no such operate|Operate:%d", operate)
			return errs.ErrSystem
		}
	}
	return nil
}

// ModifyTableSheetByOperateAndSheetName 根据SheetName更新数据
func ModifyTableSheetByOperateAndSheetName(ctx context.Context, docCommon *model.DocSegmentCommon,
	sheetNames []string, operate int, tx *gorm.DB) error {
	log.InfoContextf(ctx, "ModifyTableSheetByOperateAndSheetName|Operate:%d", operate)
	for _, sheetName := range sheetNames {
		switch operate {
		case ModifyTableSheetDisabledRetrievalEnhanceOperate:
			err := dao.GetDocSegmentSheetTemporaryDao().DisabledRetrievalEnhanceSheet(ctx, tx, docCommon.CorpBizID,
				docCommon.AppBizID, docCommon.DocBizID, []string{sheetName})
			if err != nil {
				log.ErrorContextf(ctx, "ModifyTableSheetByOperateAndSheetName|DisabledRetrievalEnhanceSheet|err:%v", err)
				return err
			}
		case ModifyTableSheetEnableRetrievalEnhanceOperate:
			err := dao.GetDocSegmentSheetTemporaryDao().EnableRetrievalEnhanceSheet(ctx, tx, docCommon.CorpBizID,
				docCommon.AppBizID, docCommon.DocBizID, []string{sheetName})
			if err != nil {
				log.ErrorContextf(ctx, "ModifyTableSheetByOperateAndSheetName|EnableRetrievalEnhanceSheet|err:%v", err)
				return err
			}
		default:
			log.ErrorContextf(ctx, "ModifyTableSheetByOperateAndSheetName|no such operate|Operate:%d", operate)
			return errs.ErrSystem
		}
	}
	return nil
}

// DeleteSheetBySheetBizID 删除sheet 临时表删除&结构化数据删除
func DeleteSheetBySheetBizID(ctx context.Context, tx *gorm.DB, docCommon *model.DocSegmentCommon,
	sheetBizID uint64) error {
	// 通过id查询到sheetName
	deletedFlag := dao.IsNotDeleted
	filter := &dao.DocSegmentSheetTemporaryFilter{
		CorpBizID:   docCommon.CorpBizID,
		AppBizID:    docCommon.AppBizID,
		DocBizID:    docCommon.DocBizID,
		BusinessIDs: []uint64{sheetBizID},
		IsDeleted:   &deletedFlag,
	}
	sheet, err := dao.GetDocSegmentSheetTemporaryDao().GetSheet(ctx,
		dao.DocSegmentSheetTemporaryTblColList, filter)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			log.ErrorContextf(ctx, "DeleteSheetBySheetBizID|GetSheet|sheet is null")
			return errs.ErrDocSegmentSheetNotFound
		}
		log.ErrorContextf(ctx, "DeleteSheetBySheetBizID|GetSheet|err:%v", err)
		return err
	}
	if sheet == nil {
		log.ErrorContextf(ctx, "DeleteSheetBySheetBizID|GetSheet|sheet is null")
		return errs.ErrDocSegmentSheetNotFound
	}
	err = dao.GetDocSegmentSheetTemporaryDao().DeleteDocSegmentSheet(ctx, tx, docCommon.CorpBizID,
		docCommon.AppBizID, docCommon.DocBizID, []uint64{sheetBizID})
	if err != nil {
		log.ErrorContextf(ctx, "DeleteSheetBySheetBizID|DeleteDocSegmentSheet|err:%v", err)
		return err
	}
	// 删除结构化数据 需要依赖SheetName
	err = doc_intervene.DelText2SqlTablesByDocIdAndSheetName(ctx, docCommon.AppBizID, docCommon.DocBizID, sheet.SheetName)
	if err != nil {
		log.ErrorContextf(ctx, "DeleteSheetBySheetBizID|DelText2SqlTablesByDocIdAndSheetName|err:%v", err)
		return err
	}
	return nil
}

func StoreSheetByDocParse(ctx context.Context, d dao.Dao,
	docCommon *model.DocSegmentCommon) error {
	// 临时表没有对应数据
	region := d.GetRegion(ctx)
	bucket := d.GetBucket(ctx)
	// 解析一次pb，获取sheet列表
	sheets, err := GetTableSectionFromDocParse(ctx, d, docCommon.AppID, docCommon.DocID, bucket)
	if err != nil {
		log.ErrorContextf(ctx, "ListTableSheet|GetTableSectionFromDocParse|err:%+v", err)
		return errs.ErrSystem
	}
	err = d.GetTdsqlGormDB().Transaction(func(tx *gorm.DB) error {
		for i, sheet := range sheets {
			cosPath, fileName, cosHash, err := UploadToCos(ctx, d, docCommon.CorpID, sheet.Content)
			if err != nil {
				log.ErrorContextf(ctx, "ListTableSheet|UploadToCos|err:%+v", err)
				return errs.ErrSystem
			}
			// 存入数据库
			docSheet := model.DocSegmentSheetTemporary{}
			docSheet.BusinessID = d.GenerateSeqID()
			docSheet.AppBizID = docCommon.AppBizID
			docSheet.CorpBizID = docCommon.CorpBizID
			docSheet.StaffBizID = docCommon.StaffBizID
			docSheet.Bucket = bucket
			docSheet.Region = region
			docSheet.CosURL = cosPath
			docSheet.CosHash = cosHash
			docSheet.SheetName = sheet.Title
			docSheet.SheetTotalNum = len(sheets)
			docSheet.IsDeleted = dao.IsNotDeleted
			docSheet.CreateTime = time.Now()
			docSheet.UpdateTime = time.Now()
			docSheet.DocBizID = docCommon.DocBizID
			docSheet.SheetOrder = i
			docSheet.FileName = fileName
			docSheet.FileType = model.FileTypeMD
			docSheet.Version = model.SheetDefaultVersion
			docSheet.IsDisabled = model.SegmentIsEnable
			docSheet.IsDisabledRetrievalEnhance = model.SheetEnableRetrievalEnhance
			err = dao.GetDocSegmentSheetTemporaryDao().CreateDocSegmentSheet(ctx, tx, &docSheet)
			if err != nil {
				log.ErrorContextf(ctx, "ListTableSheet|CreateDocSegmentSheet|err:%+v", err)
				return errs.ErrSystem
			}
		}
		return nil
	})
	if err != nil {
		log.ErrorContextf(ctx, "StoreSheetByDocParse|err:%+v", err)
		return err
	}
	return nil
}

type OriginSheetStatus struct {
	IsDisabled                 uint32
	IsDisabledRetrievalEnhance uint32
}

func StoreSheetByDocParseAndCompareOriginDocuments(ctx context.Context, d dao.Dao,
	docCommon *model.DocSegmentCommon, originDocBizID uint64) error {
	log.InfoContextf(ctx, "StoreSheetByDocParseAndCompareOriginDocuments|originDocBizID:%d|newDocBizID:%d",
		originDocBizID, docCommon.DocBizID)
	originSheetMap := make(map[string]OriginSheetStatus)
	// 获取原始文档的停用启用数据
	pageNumber := 0
	pageSize := 100
	oldSheets := make([]*model.DocSegmentSheetTemporary, 0)
	oldDoc, err := d.GetDocByBizID(ctx, originDocBizID, docCommon.AppID)
	if err != nil {
		log.ErrorContextf(ctx, "GetDocByBizID|err:%+v", err)
		return errs.ErrDocNotFound
	}
	oldDocCommon := &model.DocSegmentCommon{
		AppBizID:   docCommon.AppBizID,
		AppID:      docCommon.AppID,
		CorpBizID:  docCommon.CorpBizID,
		CorpID:     docCommon.CorpID,
		StaffID:    docCommon.StaffID,
		StaffBizID: docCommon.StaffBizID,
		DocBizID:   originDocBizID,
		DocID:      oldDoc.ID,
	}
	for {
		pageNumber++
		req := &pb.ListTableSheetReq{
			AppBizId:   strconv.FormatUint(docCommon.AppBizID, 10),
			DocBizId:   strconv.FormatUint(originDocBizID, 10),
			PageNumber: uint32(pageNumber),
			PageSize:   uint32(pageSize),
		}
		list, err := GetSheetList(ctx, d, req, oldDocCommon)
		if err != nil {
			log.ErrorContextf(ctx, "GetSheetList|err:%v", err)
			return err
		}
		oldSheets = append(oldSheets, list...)
		if len(list) == 0 {
			break
		}
	}
	for _, sheet := range oldSheets {
		originSheetMap[sheet.SheetName] = OriginSheetStatus{
			IsDisabled:                 sheet.IsDisabled,
			IsDisabledRetrievalEnhance: sheet.IsDisabledRetrievalEnhance,
		}
	}
	log.InfoContextf(ctx, "StoreSheetByDocParseAndCompareOriginDocuments|len(oldSheets):%d", len(oldSheets))

	// 为自定义拆分规则且原表有数据，直接返回，无需写入数据
	source := GetDataSource(ctx, oldDoc.SplitRule)
	if source == model.DataSourceDB && len(oldSheets) != 0 {
		log.InfoContextf(ctx, "StoreSheetByDocParseAndCompareOriginDocuments is Customize, return")
		return nil
	}

	// 解析新md数据
	region := d.GetRegion(ctx)
	bucket := d.GetBucket(ctx)
	// 解析一次pb，获取sheet列表
	newSheets, err := GetTableSectionFromDocParse(ctx, d, docCommon.AppID, docCommon.DocID, bucket)
	if err != nil {
		log.ErrorContextf(ctx, "GetTableSectionFromDocParse|err:%+v", err)
		return errs.ErrSystem
	}
	log.InfoContextf(ctx, "StoreSheetByDocParseAndCompareOriginDocuments|len(newSheets):%d", len(newSheets))

	err = d.GetTdsqlGormDB().Transaction(func(tx *gorm.DB) error {
		// 物理删除旧sheet
		deleteTempFilter := &dao.DocSegmentSheetTemporaryFilter{
			CorpBizID: docCommon.CorpBizID,
			AppBizID:  docCommon.AppBizID,
			DocBizID:  originDocBizID,
		}
		err = dao.GetDocSegmentSheetTemporaryDao().RealityBatchDeleteDocSheet(ctx,
			nil, deleteTempFilter, 10000)
		if err != nil {
			log.ErrorContextf(ctx, "StoreSheetByDocParseAndCompareOriginDocuments|RealityBatchDeleteDocOrgData failed, err:%+v", err)
			return err
		}
		// 新sheet入库
		for i, sheet := range newSheets {
			isDisabled := model.SegmentIsEnable
			isDisabledRetrievalEnhance := model.SheetEnableRetrievalEnhance
			if status, ok := originSheetMap[sheet.Title]; ok {
				isDisabled = status.IsDisabled
				isDisabledRetrievalEnhance = status.IsDisabledRetrievalEnhance
			}
			cosPath, fileName, cosHash, err := UploadToCos(ctx, d, docCommon.CorpID, sheet.Content)
			if err != nil {
				log.ErrorContextf(ctx, "ListTableSheet|UploadToCos|err:%+v", err)
				return errs.ErrSystem
			}
			// 存入数据库
			docSheet := model.DocSegmentSheetTemporary{}
			docSheet.BusinessID = d.GenerateSeqID()
			docSheet.AppBizID = docCommon.AppBizID
			docSheet.CorpBizID = docCommon.CorpBizID
			docSheet.StaffBizID = docCommon.StaffBizID
			docSheet.Bucket = bucket
			docSheet.Region = region
			docSheet.CosURL = cosPath
			docSheet.CosHash = cosHash
			docSheet.SheetName = sheet.Title
			docSheet.SheetTotalNum = len(newSheets)
			docSheet.IsDeleted = dao.IsNotDeleted
			docSheet.CreateTime = time.Now()
			docSheet.UpdateTime = time.Now()
			docSheet.DocBizID = docCommon.DocBizID
			docSheet.SheetOrder = i
			docSheet.FileName = fileName
			docSheet.FileType = model.FileTypeMD
			docSheet.Version = model.SheetDefaultVersion
			docSheet.IsDisabled = isDisabled
			docSheet.IsDisabledRetrievalEnhance = isDisabledRetrievalEnhance
			err = dao.GetDocSegmentSheetTemporaryDao().CreateDocSegmentSheet(ctx, tx, &docSheet)
			if err != nil {
				log.ErrorContextf(ctx, "CreateDocSegmentSheet|err:%+v", err)
				return errs.ErrSystem
			}
		}
		return nil
	})
	if err != nil {
		log.ErrorContextf(ctx, "StoreSheetByDocParseAndCompareOriginDocuments|err:%+v", err)
		return err
	}
	return nil
}

// GetSheetFromDocSegmentPageInfo 从切片信息中获取sheet信息
func GetSheetFromDocSegmentPageInfo(ctx context.Context, d dao.Dao, segID uint64,
	docCommon *model.DocSegmentCommon) (*model.DocSegmentSheetTemporary, error) {
	segmentPageInfoMap, err := d.GetSegmentPageInfosBySegIDs(ctx, docCommon.AppID, []uint64{segID})
	if err != nil {
		return nil, err
	}
	sheetName := ""
	if pageInfo, ok := segmentPageInfoMap[segID]; ok {
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
	sheets, err := dao.GetSheetByName(ctx, docCommon.CorpBizID, docCommon.AppBizID, docCommon.DocBizID, sheetName)
	if err != nil {
		return nil, err
	}
	if len(sheets) > 0 {
		return sheets[0], nil
	}
	return nil, errs.ErrDocSegmentSheetNotFound
}

// IsExcel 判断文件类型是否是excel
func IsExcel(fileType string) bool {
	return fileType == model.FileTypeXlsx || fileType == model.FileTypeXls || fileType == model.FileTypeCsv
}
