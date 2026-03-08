package document

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/encodingx/jsonx"
	"git.woa.com/adp/common/x/gox/ptrx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/utilx"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/tdsqlquery"
	dbEntity "git.woa.com/adp/kb/kb-config/internal/entity/database"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	segEntity "git.woa.com/adp/kb/kb-config/internal/entity/segment"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"git.woa.com/adp/kb/kb-config/internal/util/idgen"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	knowledge "git.woa.com/adp/pb-go/kb/kb_config"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
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
func (l *Logic) ListTableSheet(ctx context.Context, req *pb.ListTableSheetReq,
	docCommon *segEntity.DocSegmentCommon, doc *docEntity.Doc) (*pb.ListTableSheetRsp, error) {
	logx.I(ctx, "ListTableSheet|start")
	rsp := new(pb.ListTableSheetRsp)
	sheetList := make([]*pb.ListTableSheetRsp_TableSheetItem, 0)
	if req.PageNumber < 1 || req.PageSize < 1 {
		logx.E(ctx, "ListTableSheet|params err|req:%+v", req)
		return rsp, errs.ErrParams
	}
	// 获取所有sheet的数量
	filter := &segEntity.DocSegmentSheetTemporaryFilter{
		CorpBizID: docCommon.CorpBizID,
		AppBizID:  docCommon.AppBizID,
		DocBizID:  docCommon.DocBizID,
		IsDeleted: ptrx.Bool(false),
	}
	num, err := l.segDao.GetDocSheetCount(ctx, filter)
	if err != nil {
		logx.E(ctx, "GetDocOrgDataCountByDocBizID|GetDocSegmentOrgDataDao|err:%v", err)
		return rsp, errs.ErrSystem
	}
	if doc.IsAuditFailed() {
		filter := &segEntity.DocSegmentSheetTemporaryFilter{
			CorpBizID: docCommon.CorpBizID,
			AppBizID:  docCommon.AppBizID,
			DocBizID:  doc.BusinessID,
			IsDeleted: ptrx.Bool(false),
			AuditStatus: []uint32{
				uint32(segEntity.DocSegmentAuditStatusContentFailed)},
		}
		auditFailNum, err := l.segDao.GetDocSheetCount(ctx, filter)
		if err != nil {
			logx.E(ctx, "getDocNotDeleteTemporaryOrgData|GetDocOrgDataCount|err:%v", err)
			return nil, err
		}
		rsp.AuditFailedNumber = uint64(auditFailNum)
	}
	rsp.FileSize = strconv.FormatUint(doc.FileSize, 10)
	rsp.SegmentNumber = strconv.FormatInt(num, 10)
	// 确认改文档是否被编辑过（临时表的version是否都为0，text2sql是否有改动）
	intervene, err := l.CheckTableIntervene(ctx, docCommon)
	if err != nil {
		return nil, err
	}
	rsp.IsModify = intervene
	// 确认临时表中存在数据
	tempList, err := l.GetSheetListIncludeDeleted(ctx, req, docCommon)
	if err != nil {
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errs.ErrSystem
		}
	}
	if len(tempList) > 0 {
		// 从临时表获取sheet列表
		list, err := l.GetSheetList(ctx, req, docCommon)
		if err != nil {
			return nil, err
		}
		logx.I(ctx, "ListTableSheet|len(SheetList):%d", len(list))
		// 临时表有，直接返回
		sheetList = SheetTemporaryListToRspSheetList(ctx, list)
		rsp.Total = uint64(len(list))
		rsp.SheetList = sheetList
		return rsp, nil
	}
	err = l.StoreSheetByDocParse(ctx, docCommon)
	if err != nil {
		logx.E(ctx, "ListTableSheet|StoreSheetByDocParse|err:%+v", err)
		return nil, errs.ErrSystem
	}
	// 4.存储后查询数据库
	// 获取所有sheet的数量
	filter = &segEntity.DocSegmentSheetTemporaryFilter{
		CorpBizID: docCommon.CorpBizID,
		AppBizID:  docCommon.AppBizID,
		DocBizID:  docCommon.DocBizID,
		IsDeleted: ptrx.Bool(false),
	}
	num, err = l.segDao.GetDocSheetCount(ctx, filter)
	if err != nil {
		logx.E(ctx, "GetDocOrgDataCountByDocBizID|GetDocSegmentOrgDataDao|err:%v", err)
		return rsp, errs.ErrSystem
	}
	rsp.SegmentNumber = strconv.FormatInt(num, 10)
	list, err := l.GetSheetList(ctx, req, docCommon)
	if err != nil || len(list) == 0 {
		return rsp, errs.ErrSystem
	}
	sheetList = SheetTemporaryListToRspSheetList(ctx, list)
	rsp.Total = uint64(len(list))
	rsp.SheetList = sheetList
	return rsp, nil
}

// ModifyTableSheet 修改表格文档sheet
func (l *Logic) ModifyTableSheet(ctx context.Context, req *pb.ModifyTableSheetReq,
	docCommon *segEntity.DocSegmentCommon) (*pb.ModifyTableSheetRsp, error) {
	logx.I(ctx, "ModifyTableSheet|start")
	rsp := &pb.ModifyTableSheetRsp{}
	newSheets := req.GetModifyTableSheets()
	// 查找ID对应的sheet并更新，事务
	err := l.docDao.Query().TDoc.WithContext(ctx).UnderlyingDB().Transaction(func(tx *gorm.DB) error {
		if len(req.ModifyTableSheets) > 0 {
			err := l.ModifyTableSheetContent(ctx, docCommon, newSheets, tx)
			if err != nil {
				return err
			}
		}
		if len(req.DeleteSheetBizIds) > 0 {
			err := l.ModifyTableSheetByOperateAndSheetBizID(ctx, docCommon, req.DeleteSheetBizIds,
				ModifyTableSheetDeleteOperate)
			if err != nil {
				return err
			}
		}
		if len(req.DisabledSheetBizIds) > 0 {
			err := l.ModifyTableSheetByOperateAndSheetBizID(ctx, docCommon, req.DisabledSheetBizIds,
				ModifyTableSheetDisabledOperate)
			if err != nil {
				return err
			}
		}
		if len(req.EnableSheetBizIds) > 0 {
			err := l.ModifyTableSheetByOperateAndSheetBizID(ctx, docCommon, req.EnableSheetBizIds,
				ModifyTableSheetEnableOperate)
			if err != nil {
				return err
			}
		}
		if len(req.DisabledRetrievalEnhanceSheetNames) > 0 {
			err := l.ModifyTableSheetByOperateAndSheetName(ctx, docCommon, req.DisabledRetrievalEnhanceSheetNames,
				ModifyTableSheetDisabledRetrievalEnhanceOperate, tx)
			if err != nil {
				return err
			}
		}
		if len(req.EnableRetrievalEnhanceSheetNames) > 0 {
			err := l.ModifyTableSheetByOperateAndSheetName(ctx, docCommon, req.EnableRetrievalEnhanceSheetNames,
				ModifyTableSheetEnableRetrievalEnhanceOperate, tx)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		logx.E(ctx, "ModifyTableSheet|err:%+v", err)
		return rsp, errs.ErrSystem
	}
	return rsp, nil
}

// GetSheetList 获取sheet
func (l *Logic) GetSheetList(ctx context.Context, req *pb.ListTableSheetReq,
	docCommon *segEntity.DocSegmentCommon) ([]*segEntity.DocSegmentSheetTemporary, error) {
	logx.I(ctx, "GetSheetList|start")
	// 如果参考ID存在（对话处使用输入后会检索t_refer表）
	if req.ReferBizId != "" {
		segment, _ := l.segLogic.GetSegmentByReferID(ctx, docCommon.AppID, req.ReferBizId)
		if segment != nil {
			// 通过sheetName定位sheet数据
			// 兼容共享知识库
			app, err := l.rpc.AppAdmin.DescribeAppByPrimaryIdWithoutNotFoundError(ctx, segment.RobotID)
			if err != nil {
				return nil, err
			}
			docCommon.AppID = segment.RobotID
			docCommon.AppBizID = app.BizId
			sheet, err := l.GetSheetFromDocSegmentPageInfo(ctx, segment.ID, docCommon)
			if errors.Is(err, errs.ErrDocSegmentSheetNotFound) {
				// 返回空数组（兼容干预后切片ID变化的情况）
				return []*segEntity.DocSegmentSheetTemporary{}, nil
			} else if err != nil {
				return nil, err
			}
			if sheet == nil {
				// 返回空数组（兼容干预后切片ID变化的情况）
				return []*segEntity.DocSegmentSheetTemporary{}, nil
			} else {
				return []*segEntity.DocSegmentSheetTemporary{sheet}, nil
			}
		} else {
			// text2sql没有参考来源的片段id，这里如果查到的segment为nil，无法跳转到指定片段，则走底下的获取文档片段兜底
		}
	}
	offset, limit := utilx.Page(req.PageNumber, req.PageSize)
	filter := &segEntity.DocSegmentSheetTemporaryFilter{
		CorpBizID:      docCommon.CorpBizID,
		AppBizID:       docCommon.AppBizID,
		DocBizID:       docCommon.DocBizID,
		IsDeleted:      ptrx.Bool(false),
		OrderColumn:    []string{segEntity.DocSegmentSheetTemporaryTblColSheetOrder},
		OrderDirection: []string{util.SqlOrderByAsc},
		Offset:         offset,
		Limit:          limit,
	}
	list, err := l.segDao.GetSheetList(ctx, segEntity.DocSegmentSheetTemporaryTblColList, filter)
	if err != nil {
		logx.E(ctx, "GetSheetList|err:%+v", err)
		return nil, errs.ErrSystem
	}
	return list, nil
}

func (l *Logic) GetSheetListIncludeDeleted(ctx context.Context, req *pb.ListTableSheetReq,
	docCommon *segEntity.DocSegmentCommon) ([]*segEntity.DocSegmentSheetTemporary, error) {
	logx.I(ctx, "GetSheetList|start")
	offset, limit := utilx.Page(req.PageNumber, req.PageSize)
	filter := &segEntity.DocSegmentSheetTemporaryFilter{
		CorpBizID:      docCommon.CorpBizID,
		AppBizID:       docCommon.AppBizID,
		DocBizID:       docCommon.DocBizID,
		OrderColumn:    []string{segEntity.DocSegmentSheetTemporaryTblColSheetOrder},
		OrderDirection: []string{util.SqlOrderByAsc},
		Offset:         offset,
		Limit:          limit,
	}
	list, err := l.segDao.GetSheetList(ctx, segEntity.DocSegmentSheetTemporaryTblColList, filter)
	if err != nil {
		return nil, err
	}
	return list, nil
}

// SheetTemporaryListToRspSheetList 输出结果转换
func SheetTemporaryListToRspSheetList(ctx context.Context,
	tempList []*segEntity.DocSegmentSheetTemporary) []*pb.ListTableSheetRsp_TableSheetItem {
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
			IsDisabled:                 sheet.IsDisabled,
			IsDisabledRetrievalEnhance: sheet.IsDisabledRetrievalEnhance,
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
func (l *Logic) GetTableSectionFromDocParse(ctx context.Context, appID, docID uint64, bucket string) ([]*TableSection,
	error) {
	logx.I(ctx, "GetTableSectionFromDocParse|start")
	// todo 优化，控制拆分并发
	var sheets []*TableSection
	docParse, err := l.GetDocParseByDocIDAndTypeAndStatus(ctx, docID, docEntity.DocParseTaskTypeSplitSegment,
		docEntity.DocParseSuccess, appID)
	if err != nil {
		logx.E(ctx, "GetDocParseByDocIDAndTypeAndStatus|failed, err:%+v", err)
		return sheets, err
	}
	result := &knowledge.FileParserCallbackReq{}
	err = jsonx.UnmarshalFromString(docParse.Result, result)
	if err != nil {
		logx.E(ctx, "getDocParseContent|jsonx.UnmarshalFromString failed, err:%+v", err)
		return sheets, err
	}
	logx.I(ctx, "GetDocParseByDocIDAndTypeAndStatus|file parse result:%+v", result)
	resultDataMap := result.GetResults()
	docParseRes := resultDataMap[int32(knowledge.FileParserSubDataType_FILE_PARSER_SUB_DATA_TYPE_PARSE)]
	// FileParserSubDataType_FILE_PARSER_SUB_DATA_TYPE_PARSE 只会返回一个result
	for _, res := range docParseRes.GetResult() {
		storageTypeKey := l.s3.GetTypeKeyWithBucket(ctx, bucket)
		body, err := l.s3.GetObjectWithTypeKey(ctx, storageTypeKey, res.GetResult())
		if err != nil {
			logx.E(ctx, "getDocParseContent|GetObjectWithTypeKey|err:%+v", err)
			return sheets, err
		}
		// 解析pb
		unSerialPb := &knowledge.ParseResult{}
		if err = proto.Unmarshal(body, unSerialPb); err != nil {
			logx.E(ctx, "GetTableSectionFromDocParse|cosURL:%s|proto.Unmarshal failed, err:%+v",
				res.GetResult(), err)
			return sheets, err
		}
		sections := splitSheet(ctx, unSerialPb.GetResult())
		sheets = append(sheets, sections...)
	}
	logx.I(ctx, "GetDocParseByDocIDAndTypeAndStatus|sheets:%+v", sheets)
	return sheets, nil
}

func splitSheet(ctx context.Context, file string) []*TableSection {
	logx.I(ctx, "GetDocParseByDocIDAndTypeAndStatus|splitSheet|:%s", file)
	blocks := parseBlocks(file)
	sections := make([]*TableSection, 0)
	if len(blocks) == 1 {
		sections = append(sections, &TableSection{
			Title:   "",
			Content: []byte(blocks[0].Content),
		})
	}
	if len(blocks) == 0 || len(blocks)%2 != 0 {
		logx.E(ctx, "GetDocParseByDocIDAndTypeAndStatus|blocks is empty or unable to cope")
		return sections
	}
	tableType := BlockTypeTitle
	table := new(TableSection)
	for _, block := range blocks {
		if block.Type != tableType {
			logx.E(ctx, "GetDocParseByDocIDAndTypeAndStatus|blocks wrong order")
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
	// processHeadInvalidChar := false
	for i := 0; i < len(lines); i++ {
		// todo 处理头部无效数据(来源：解析服务bug)
		// if !processHeadInvalidChar && lines[i] != "" {
		//	lines[i] = removeInvalidChar(lines[i])
		//	processHeadInvalidChar = true
		// }
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
func (l *Logic) UploadToCos(ctx context.Context, corpID uint64, content []byte) (string, string, string, error) {
	// 包含换行符拼接，上传到cos，存入表
	// 上传到COS
	fileName := fmt.Sprintf("sheet-%d-%d-%d.md", corpID, idgen.GetId(), time.Now().Unix())
	cosPath := l.s3.GetCorpCOSFilePath(ctx, corpID, fileName)
	if err := l.s3.PutObject(ctx, content, cosPath); err != nil {
		logx.E(ctx, "UploadToCos|upload sheet md|corpID:%d|cosPath:%s|err:%+v", corpID, cosPath, err)
		return "", "", "", err
	}
	logx.I(ctx, "UploadToCos|upload sheet md|fileName:%s|cosPath:%s", fileName, cosPath)
	// 这里objectInfo.ETag的结果会带有转义字符 类似 "\"5784a190d6af4214020f54edc87429ab\""
	// 需要对转义字符特殊处理
	objectInfo, err := l.s3.StatObject(ctx, cosPath)
	if err != nil {
		logx.E(ctx, "UploadToCos|StatObject|err:%+v", err)
		return "", "", "", err
	}
	logx.I(ctx, "UploadToCos|StatObject|Hash:%s|Etag:%s|", objectInfo.Hash, objectInfo.ETag)
	cosHash := objectInfo.Hash
	return cosPath, fileName, cosHash, nil
}

// ModifyTableSheetContent 更新sheet内容
func (l *Logic) ModifyTableSheetContent(ctx context.Context, docCommon *segEntity.DocSegmentCommon,
	newSheets []*pb.ModifyTableSheetReq_ModifyTableSheetItem, tx *gorm.DB) error {
	for _, sheet := range newSheets {
		// 检查文档格式是否为md
		if sheet.GetFileType() != docEntity.FileTypeMD {
			return errs.ErrUnknownDocType
		}
		sheetBizId, err := util.CheckReqParamsIsUint64(ctx, sheet.SheetBizId)
		if err != nil {
			logx.E(ctx, "ModifyTableSheet|SheetBizIdToUint64|err:%+v", err)
			return err
		}
		filter := &segEntity.DocSegmentSheetTemporaryFilter{
			CorpBizID:   docCommon.CorpBizID,
			AppBizID:    docCommon.AppBizID,
			DocBizID:    docCommon.DocBizID,
			BusinessIDs: []uint64{sheetBizId},
			IsDeleted:   ptrx.Bool(false),
		}
		oldSheet, err := l.segDao.GetSheet(ctx,
			segEntity.DocSegmentSheetTemporaryTblColList, filter)
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
		if err = l.s3.CheckURLFileByHash(ctx, docCommon.CorpID, docCommon.CorpBizID, docCommon.AppBizID,
			oldSheet.CosURL, oldSheet.CosHash); err != nil {
			logx.E(ctx, "ModifyTableSheet|CheckURLFileByHash|err:%+v", err)
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
			segEntity.DocSegmentSheetTemporaryTblColBucket,
			segEntity.DocSegmentSheetTemporaryTblColRegion,
			segEntity.DocSegmentSheetTemporaryTblColCosURL,
			segEntity.DocSegmentSheetTemporaryTblColCosHash,
			segEntity.DocSegmentSheetTemporaryTblColFileName,
			segEntity.DocSegmentSheetTemporaryTblColFileType,
			segEntity.DocSegmentSheetTemporaryTblColVersion,
		}
		updateFilter := &segEntity.DocSegmentSheetTemporaryFilter{
			CorpBizID:   docCommon.CorpBizID,
			AppBizID:    docCommon.AppBizID,
			DocBizID:    docCommon.DocBizID,
			BusinessIDs: []uint64{newSheet.BusinessID},
		}
		_, err = l.segDao.UpdateDocSegmentSheet(ctx, updateColumns,
			updateFilter, &newSheet)
		if err != nil {
			logx.E(ctx, "ModifyTableSheet|UpdateDocSegmentSheetWithVersion|err:%+v", err)
			return errs.ErrSystem
		}
	}
	return nil
}

// ModifyTableSheetByOperateAndSheetBizID 根据SheetBizID更新数据
func (l *Logic) ModifyTableSheetByOperateAndSheetBizID(ctx context.Context, docCommon *segEntity.DocSegmentCommon,
	bizIDs []string, operate int) error {
	logx.I(ctx, "ModifyTableSheetByOperate|Operate:%d", operate)
	for _, segBizID := range bizIDs {
		sheetBizID, err := util.CheckReqParamsIsUint64(ctx, segBizID)
		if err != nil {
			logx.E(ctx, "ModifyTableSheetByOperateAndSheetBizID|SheetBizIdToUint64|err:%+v", err)
			return err
		}
		switch operate {
		case ModifyTableSheetDeleteOperate:
			err := l.DeleteSheetBySheetBizID(ctx, docCommon, sheetBizID)
			if err != nil {
				logx.E(ctx, "ModifyTableSheetByOperateAndSheetBizID|DeleteSheetBySheetBizID|err:%v", err)
				return err
			}
		case ModifyTableSheetDisabledOperate:
			err := l.segDao.DisabledDocSegmentSheet(ctx, docCommon.CorpBizID,
				docCommon.AppBizID, docCommon.DocBizID, []uint64{sheetBizID})
			if err != nil {
				logx.E(ctx, "ModifyTableSheetByOperateAndSheetBizID|DisabledDocSegmentSheet|err:%v", err)
				return err
			}
		case ModifyTableSheetEnableOperate:
			err := l.segDao.EnableDocSegmentSheet(ctx, docCommon.CorpBizID,
				docCommon.AppBizID, docCommon.DocBizID, []uint64{sheetBizID})
			if err != nil {
				logx.E(ctx, "ModifyTableSheetByOperateAndSheetBizID|EnableDocSegmentSheet|err:%v", err)
				return err
			}
		default:
			logx.E(ctx, "ModifyTableSheetByOperateAndSheetBizID|no such operate|Operate:%d", operate)
			return errs.ErrSystem
		}
	}
	return nil
}

// ModifyTableSheetByOperateAndSheetName 根据SheetName更新数据
func (l *Logic) ModifyTableSheetByOperateAndSheetName(ctx context.Context, docCommon *segEntity.DocSegmentCommon,
	sheetNames []string, operate int, tx *gorm.DB) error {
	logx.I(ctx, "ModifyTableSheetByOperateAndSheetName|Operate:%d", operate)
	for _, sheetName := range sheetNames {
		switch operate {
		case ModifyTableSheetDisabledRetrievalEnhanceOperate:
			err := l.segDao.DisabledRetrievalEnhanceSheet(ctx, docCommon.CorpBizID,
				docCommon.AppBizID, docCommon.DocBizID, []string{sheetName})
			if err != nil {
				logx.E(ctx, "ModifyTableSheetByOperateAndSheetName|DisabledRetrievalEnhanceSheet|err:%v",
					err)
				return err
			}
		case ModifyTableSheetEnableRetrievalEnhanceOperate:
			err := l.segDao.EnableRetrievalEnhanceSheet(ctx, docCommon.CorpBizID,
				docCommon.AppBizID, docCommon.DocBizID, []string{sheetName})
			if err != nil {
				logx.E(ctx, "ModifyTableSheetByOperateAndSheetName|EnableRetrievalEnhanceSheet|err:%v", err)
				return err
			}
		default:
			logx.E(ctx, "ModifyTableSheetByOperateAndSheetName|no such operate|Operate:%d", operate)
			return errs.ErrSystem
		}
	}
	return nil
}

// DeleteSheetBySheetBizID 删除sheet 临时表删除&结构化数据删除
func (l *Logic) DeleteSheetBySheetBizID(ctx context.Context, docCommon *segEntity.DocSegmentCommon,
	sheetBizID uint64) error {

	// 通过id查询到sheetName
	filter := &segEntity.DocSegmentSheetTemporaryFilter{
		CorpBizID:   docCommon.CorpBizID,
		AppBizID:    docCommon.AppBizID,
		DocBizID:    docCommon.DocBizID,
		BusinessIDs: []uint64{sheetBizID},
		IsDeleted:   ptrx.Bool(false),
	}
	sheet, err := l.segDao.GetSheet(ctx, segEntity.DocSegmentSheetTemporaryTblColList, filter)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			logx.E(ctx, "DeleteSheetBySheetBizID|GetSheet|sheet is null")
			return errs.ErrDocSegmentSheetNotFound
		}
		logx.E(ctx, "DeleteSheetBySheetBizID|GetSheet|err:%v", err)
		return err
	}
	if sheet == nil {
		logx.E(ctx, "DeleteSheetBySheetBizID|GetSheet|sheet is null")
		return errs.ErrDocSegmentSheetNotFound
	}
	err = l.segDao.DeleteDocSegmentSheet(ctx, docCommon.CorpBizID,
		docCommon.AppBizID, docCommon.DocBizID, []uint64{sheetBizID})
	if err != nil {
		logx.E(ctx, "DeleteSheetBySheetBizID|DeleteDocSegmentSheet|err:%v", err)
		return err
	}
	// 删除结构化数据 需要依赖SheetName
	err = l.DelText2SqlTablesByDocIdAndSheetName(ctx, docCommon.AppBizID, docCommon.DocBizID, sheet.SheetName)
	if err != nil {
		logx.E(ctx, "DeleteSheetBySheetBizID|DelText2SqlTablesByDocIdAndSheetName|err:%v", err)
		return err
	}
	return nil
}

// DelText2SqlTablesByDocIdAndSheetName  解析干预 text2sql， 根据 docId， sheetName， 删除结构化数据用户注释
func (l *Logic) DelText2SqlTablesByDocIdAndSheetName(ctx context.Context, appBizId, docBizId uint64,
	sheetName string) error {

	appDB, err := l.rpc.AppAdmin.DescribeAppById(ctx, appBizId)
	if err != nil {
		logx.E(ctx, "GetRobotIdByAppBizId err: %v", err)
		return err
	}
	robotID := appDB.PrimaryId
	metaMappings, err := l.GetDocMetaDataByDocId(ctx, docBizId, robotID)
	if err != nil {
		logx.E(ctx, "GetDocMetaDataByDocId err: %v", err)
		return err
	}

	for _, metaMapping := range metaMappings {
		name, err := getSheetName(ctx, metaMapping)
		if err != nil {
			logx.E(ctx, "getSheetName err: %v", err)
			return err
		}
		if name == sheetName {
			err := l.dbDao.BatchSoftDeleteByDBSourceBizID(ctx, contextx.Metadata(ctx).CorpBizID(), appBizId,
				[]uint64{metaMapping.BusinessID})
			if err != nil {
				logx.E(ctx, "BatchSoftDeleteByDBSourceBizID err: %v", err)
				return err
			}
		}
	}
	return nil
}

func getSheetName(ctx context.Context, metaMapping *docEntity.Text2sqlMetaMappingPreview) (string, error) {
	var data map[string]any
	err := json.Unmarshal([]byte(metaMapping.Mapping), &data)
	if err != nil {
		logx.E(ctx, "GetSheetTableColumn json.Unmarshal err: %v", err)
		return "", err
	}

	tableName, ok := data["table_name"].(map[string]any)
	if !ok {
		return "", err
	}
	rawValue, ok := tableName["raw"].(string)
	if !ok {
		return "", err
	}
	return rawValue, nil
}

// CheckTableIntervene 检查文档是否有改动
func (l *Logic) CheckTableIntervene(ctx context.Context, docCommon *segEntity.DocSegmentCommon) (bool, error) {
	// 检查临时表数据的版本
	intervene, err := l.CheckTableContentIntervene(ctx, docCommon)
	if err != nil {
		return false, err
	}
	if intervene == true {
		return true, nil
	} else {
		// 检查text2sql数据是否有改动
		isChanged, err := l.CheckTextToSqlTableIsChanged(ctx, docCommon.AppBizID, docCommon.DocBizID)
		if err != nil {
			logx.E(ctx, "CheckTableIntervene|CheckTextToSqlTableIsChanged|err:%v", err)
			return false, err
		}
		logx.I(ctx, "CheckTextToSqlTableIsChanged|CheckTextToSqlTableIsChanged|isChanged:%t", isChanged)
		return isChanged, nil
	}
}

// CheckTextToSqlTableIsChanged 解析干预 text2sql， 判断用户是否修改结构化数据注释
func (l *Logic) CheckTextToSqlTableIsChanged(ctx context.Context, appBizId, docBizId uint64) (bool, error) {
	logx.I(ctx, "CheckTextToSqlTableIsChanged check table is changed,  doc biz id: %v", docBizId)
	appDB, err := l.rpc.AppAdmin.DescribeAppById(ctx, appBizId)
	// roBotId, err := db_source.GetRobotIdByAppBizId(ctx, appBizId)
	if err != nil {
		logx.E(ctx, "GetRobotIdByAppBizId err: %v", err)
		return false, err
	}
	robotID := appDB.PrimaryId

	metaMappings, err := l.GetDocMetaDataByDocId(ctx, docBizId, robotID)
	if err != nil {
		logx.E(ctx, "GetDocMetaDataByDocId err: %v", err)
		return false, err
	}

	corpBizID := contextx.Metadata(ctx).CorpBizID()
	for _, metaMapping := range metaMappings {
		tableFilter := dbEntity.TableFilter{
			CorpBizID:     corpBizID,
			AppBizID:      appBizId,
			DBSourceBizID: metaMapping.BusinessID,
		}
		dbTable, err := l.dbDao.DescribeTable(ctx, &tableFilter)
		// dbTable, err := l.dbDao.Text2sqlGetByDbSourceBizID(ctx, corpBizID, appBizId, metaMapping.BusinessID)
		if err != nil {
			return false, err
		}
		if dbTable.LastSyncTime != dbTable.UpdateTime {
			return true, nil
		}
	}
	return false, nil
}

func (l *Logic) CheckTableContentIntervene(ctx context.Context, docCommon *segEntity.DocSegmentCommon) (bool, error) {
	// 检查临时表数据的版本
	versionFlag := segEntity.SheetDefaultVersion
	tempFilter := &segEntity.DocSegmentSheetTemporaryFilter{
		CorpBizID: docCommon.CorpBizID,
		AppBizID:  docCommon.AppBizID,
		DocBizID:  docCommon.DocBizID,
		Version:   &versionFlag,
		Offset:    0,
		Limit:     1,
	}
	tempList, err := l.segDao.GetSheetList(ctx, segEntity.DocSegmentSheetTemporaryTblColList, tempFilter)
	if err != nil {
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			logx.E(ctx, "CheckTableContentIntervene|GetSheetList|err:%v", err)
			return false, err
		}
	}
	if len(tempList) > 0 {
		logx.I(ctx, "CheckTableContentIntervene|update")
		return true, nil
	}
	return false, nil
}

func (l *Logic) StoreSheetByDocParse(ctx context.Context,
	docCommon *segEntity.DocSegmentCommon) error {
	// 临时表没有对应数据
	region := l.s3.GetRegion(ctx)
	bucket := l.s3.GetBucket(ctx)
	// 解析一次pb，获取sheet列表
	sheets, err := l.GetTableSectionFromDocParse(ctx, docCommon.AppID, docCommon.DocID, bucket)
	if err != nil {
		logx.E(ctx, "ListTableSheet|GetTableSectionFromDocParse|err:%+v", err)
		return errs.ErrSystem
	}
	err = l.segDao.TdsqlQuery().Transaction(func(tx *tdsqlquery.Query) error {
		for i, sheet := range sheets {
			cosPath, fileName, cosHash, err := l.UploadToCos(ctx, docCommon.CorpID, sheet.Content)
			if err != nil {
				logx.E(ctx, "ListTableSheet|UploadToCos|err:%+v", err)
				return errs.ErrSystem
			}
			// 存入数据库
			docSheet := segEntity.DocSegmentSheetTemporary{}
			docSheet.BusinessID = idgen.GetId()
			docSheet.AppBizID = docCommon.AppBizID
			docSheet.CorpBizID = docCommon.CorpBizID
			docSheet.StaffBizID = docCommon.StaffBizID
			docSheet.Bucket = bucket
			docSheet.Region = region
			docSheet.CosURL = cosPath
			docSheet.CosHash = cosHash
			docSheet.SheetName = sheet.Title
			docSheet.SheetTotalNum = len(sheets)
			docSheet.IsDeleted = false
			docSheet.CreateTime = time.Now()
			docSheet.UpdateTime = time.Now()
			docSheet.DocBizID = docCommon.DocBizID
			docSheet.SheetOrder = i
			docSheet.FileName = fileName
			docSheet.FileType = docEntity.FileTypeMD
			docSheet.Version = segEntity.SheetDefaultVersion
			docSheet.IsDisabled = false
			docSheet.IsDisabledRetrievalEnhance = false
			err = l.segDao.CreateDocSegmentSheet(ctx, &docSheet)
			if err != nil {
				logx.E(ctx, "ListTableSheet|CreateDocSegmentSheet|err:%+v", err)
				return errs.ErrSystem
			}
		}
		return nil
	})
	if err != nil {
		logx.E(ctx, "StoreSheetByDocParse|err:%+v", err)
		return err
	}
	return nil
}

type OriginSheetStatus struct {
	IsDisabled                 bool
	IsDisabledRetrievalEnhance bool
}

func (l *Logic) StoreSheetByDocParseAndCompareOriginDocuments(ctx context.Context,
	docCommon *segEntity.DocSegmentCommon, originDocBizID uint64) error {
	logx.I(ctx, "StoreSheetByDocParseAndCompareOriginDocuments|originDocBizID:%d|newDocBizID:%d",
		originDocBizID, docCommon.DocBizID)
	// 临时表没有对应数据
	region := l.s3.GetRegion(ctx)
	bucket := l.s3.GetBucket(ctx)
	// 解析一次pb，获取sheet列表
	newSheets, err := l.GetTableSectionFromDocParse(ctx, docCommon.AppID, docCommon.DocID, bucket)
	if err != nil {
		logx.E(ctx, "GetTableSectionFromDocParse|err:%+v", err)
		return errs.ErrSystem
	}
	logx.I(ctx, "StoreSheetByDocParseAndCompareOriginDocuments|len(newSheets):%d", len(newSheets))
	originSheetMap := make(map[string]OriginSheetStatus)
	// 获取原始文档的停用启用数据
	pageNumber := 0
	pageSize := 100
	oldSheets := make([]*segEntity.DocSegmentSheetTemporary, 0)
	oldDoc, err := l.GetDocByBizID(ctx, originDocBizID, docCommon.AppID)
	if err != nil {
		logx.E(ctx, "GetDocByBizID|err:%+v", err)
		return errs.ErrDocNotFound
	}
	oldDocCommon := &segEntity.DocSegmentCommon{
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
		list, err := l.GetSheetList(ctx, req, oldDocCommon)
		if err != nil {
			logx.E(ctx, "GetSheetList|err:%v", err)
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
	logx.I(ctx, "StoreSheetByDocParseAndCompareOriginDocuments|len(oldSheets):%d", len(oldSheets))
	err = l.segDao.TdsqlQuery().Transaction(func(tx *tdsqlquery.Query) error {
		// 物理删除旧sheet
		deleteTempFilter := &segEntity.DocSegmentSheetTemporaryFilter{
			CorpBizID: docCommon.CorpBizID,
			AppBizID:  docCommon.AppBizID,
			DocBizID:  originDocBizID,
		}
		err = l.segDao.RealityBatchDeleteDocSheet(ctx, deleteTempFilter, 10000)
		if err != nil {
			logx.E(ctx,
				"StoreSheetByDocParseAndCompareOriginDocuments|RealityBatchDeleteDocOrgData failed, err:%+v", err)
			return err
		}
		// 新sheet入库
		for i, sheet := range newSheets {
			isDisabled := false
			isDisabledRetrievalEnhance := false
			if status, ok := originSheetMap[sheet.Title]; ok {
				isDisabled = status.IsDisabled
				isDisabledRetrievalEnhance = status.IsDisabledRetrievalEnhance
			}
			cosPath, fileName, cosHash, err := l.UploadToCos(ctx, docCommon.CorpID, sheet.Content)
			if err != nil {
				logx.E(ctx, "ListTableSheet|UploadToCos|err:%+v", err)
				return errs.ErrSystem
			}
			// 存入数据库
			docSheet := segEntity.DocSegmentSheetTemporary{}
			docSheet.BusinessID = idgen.GetId()
			docSheet.AppBizID = docCommon.AppBizID
			docSheet.CorpBizID = docCommon.CorpBizID
			docSheet.StaffBizID = docCommon.StaffBizID
			docSheet.Bucket = bucket
			docSheet.Region = region
			docSheet.CosURL = cosPath
			docSheet.CosHash = cosHash
			docSheet.SheetName = sheet.Title
			docSheet.SheetTotalNum = len(newSheets)
			docSheet.IsDeleted = false
			docSheet.CreateTime = time.Now()
			docSheet.UpdateTime = time.Now()
			docSheet.DocBizID = docCommon.DocBizID
			docSheet.SheetOrder = i
			docSheet.FileName = fileName
			docSheet.FileType = docEntity.FileTypeMD
			docSheet.Version = segEntity.SheetDefaultVersion
			docSheet.IsDisabled = isDisabled
			docSheet.IsDisabledRetrievalEnhance = isDisabledRetrievalEnhance
			err = l.segDao.CreateDocSegmentSheet(ctx, &docSheet)
			if err != nil {
				logx.E(ctx, "CreateDocSegmentSheet|err:%+v", err)
				return errs.ErrSystem
			}
		}
		return nil
	})
	if err != nil {
		logx.E(ctx, "StoreSheetByDocParseAndCompareOriginDocuments|err:%+v", err)
		return err
	}
	return nil
}

// GetSheetFromDocSegmentPageInfo 从切片信息中获取sheet信息
func (l *Logic) GetSheetFromDocSegmentPageInfo(ctx context.Context, segID uint64,
	docCommon *segEntity.DocSegmentCommon) (*segEntity.DocSegmentSheetTemporary, error) {
	segmentPageInfoMap, err := l.segLogic.GetSegmentPageInfosBySegIDs(ctx, docCommon.AppID, []uint64{segID})
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
				if err := jsonx.UnmarshalFromString(pageInfo.SheetData, &sheetData); err != nil {
					logx.W(ctx, "GetSheetFromDocSegmentPageInfo|SheetInfos|UnmarshalFromString err:%+v", err)
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
	sheets, err := l.segLogic.GetSheetByName(ctx, docCommon.CorpBizID, docCommon.AppBizID, docCommon.DocBizID,
		sheetName)
	if err != nil {
		return nil, err
	}
	if len(sheets) > 0 {
		return sheets[0], nil
	}
	return nil, errs.ErrDocSegmentSheetNotFound
}
