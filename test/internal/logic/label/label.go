package label

import (
	"context"
	"fmt"
	"net"
	"slices"
	"strings"
	"time"

	"github.com/bwmarrin/snowflake"
	"golang.org/x/exp/maps"
	"gorm.io/gorm"

	"git.code.oa.com/trpc-go/trpc-go"
	"git.code.oa.com/trpc-go/trpc-go/client"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/gox/ptrx"
	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/syncx/errgroupx"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	async "git.woa.com/adp/kb/kb-config/internal/async/scheduler"
	"git.woa.com/adp/kb/kb-config/internal/config"
	docDao "git.woa.com/adp/kb/kb-config/internal/dao/document"
	"git.woa.com/adp/kb/kb-config/internal/dao/label"
	"git.woa.com/adp/kb/kb-config/internal/entity/kb_package"
	entity "git.woa.com/adp/kb/kb-config/internal/entity/label"
	labelEntity "git.woa.com/adp/kb/kb-config/internal/entity/label"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
	logicApp "git.woa.com/adp/kb/kb-config/internal/logic/app"
	"git.woa.com/adp/kb/kb-config/internal/rpc"
	knowClient "git.woa.com/adp/kb/kb-config/internal/rpc"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	"git.woa.com/adp/kb/kb-config/pkg/i18nkey"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
)

var snowFlakeNode *snowflake.Node

type Logic struct {
	rpc       *rpc.RPC
	dao       label.Dao
	docDao    docDao.Dao
	appLogic  *logicApp.Logic
	uniIDNode *snowflake.Node
}

func NewLogic(rpc *rpc.RPC, dao label.Dao, docDao docDao.Dao, appLogic *logicApp.Logic) *Logic {
	return &Logic{
		rpc:       rpc,
		dao:       dao,
		docDao:    docDao,
		uniIDNode: snowFlakeNode,
		appLogic:  appLogic,
	}
}

func init() {
	// "github.com/bwmarrin/snowflake" total 22 bits to share between NodeBits/StepBits
	// 机器码：使用IP二进制的后16位
	snowflake.NodeBits = 16
	// 序列号：6位，每毫秒最多生成64个ID [0,64)
	snowflake.StepBits = 6

	nodeNum := int64(0)

	// 取IP二进制的后16位
	ip := net.ParseIP(getClientIP()).To4()
	if ip != nil && len(ip) == 4 {
		nodeNum = (int64(ip[2]) << 8) + int64(ip[3])
	}

	node, err := snowflake.NewNode(nodeNum)
	log.Infof("GenerateSeqID ip:%s nodeNum:%d NodeBits:%d StepBits:%d",
		ip, nodeNum, snowflake.NodeBits, snowflake.StepBits)
	if err != nil {
		log.Fatalf("GenerateSeqID ip:%s nodeNum:%d NodeBits:%d StepBits:%d err:%+v",
			ip, nodeNum, snowflake.NodeBits, snowflake.StepBits, err)
	}
	snowFlakeNode = node
}

// getClientIP 获取本机IP
func getClientIP() string {
	ip := trpc.GetIP("eth1")
	if len(ip) > 0 {
		return ip
	}
	if addresses, err := net.InterfaceAddrs(); err == nil {
		for _, addr := range addresses {
			if ipNet, ok := addr.(*net.IPNet); ok {
				if !ipNet.IP.IsLoopback() && ipNet.IP.To4() != nil {
					ip = ipNet.IP.To4().String()
					// only ignore 127.0.0.1, return ip
					return ip
				}
			}
		}
	}
	panic("getClientIP failed")
}

func (l *Logic) DeleteAttribute(ctx context.Context, robotID uint64, ids []uint64, attrKeys []string) error {
	err := l.dao.BatchDeleteAttributes(ctx, robotID, ids)
	if err != nil {
		return err
	}
	err = l.dao.BatchDeleteAttributeLabelByAttrIDs(ctx, robotID, ids)
	if err != nil {
		return err
	}
	db, err := knowClient.GormClient(ctx, "t_attribute", robotID, 0, []client.Option{}...)
	if err != nil {
		return err
	}
	err = db.Transaction(func(tx *gorm.DB) error {
		if err := l.dao.DeleteAttribute(ctx, robotID, ids, tx); err != nil {
			return err
		}
		if err := l.dao.DeleteAttributeLabelByAttrIDs(ctx, robotID, ids, tx); err != nil {
			return err
		}
		if err := l.dao.PipelineDelAttributeLabelRedis(ctx, robotID, attrKeys, entity.AttributeLabelsPreview); err != nil {
			return err
		}
		return nil
	}, nil)

	if err != nil {
		return fmt.Errorf("failed to DeleteAttribute robotID(%d), ids(%+v), attrKeys(%+v), err:%v", robotID, ids, attrKeys, err)
	}
	return nil
}

func (l *Logic) IsDocAttributeLabelChange(ctx context.Context, robotID, docID uint64, oldAttrRange,
	attrRange uint32, refers []*pb.AttrLabelRefer) (bool, error) {
	if oldAttrRange != attrRange {
		return true, nil
	}
	oldRefers, err := l.dao.GetDocAttributeLabel(ctx, robotID, []uint64{docID})
	if err != nil {
		return false, err
	}
	mapOldRefer := make(map[string]struct{})
	mapRefer := make(map[string]struct{})
	for _, v := range refers {
		for _, labelID := range v.GetLabelBizIds() {
			mapRefer[fmt.Sprintf("%d_%s_%s", v.GetSource(), v.GetAttributeBizId(), labelID)] = struct{}{}
		}
	}
	var attrIds []uint64
	var labelIds []uint64
	for _, v := range oldRefers {
		attrIds = append(attrIds, v.AttrID)
		labelIds = append(labelIds, v.LabelID)
	}
	attrs, err := l.dao.GetAttributeByIDs(ctx, robotID, attrIds)
	if err != nil {
		return false, err
	}
	labels, err := l.dao.GetAttributeLabelByIDs(ctx, labelIds, robotID)
	if err != nil {
		return false, err
	}
	if len(attrs) <= 0 {
		return false, nil
	}
	for _, v := range oldRefers {
		var labelBusinessID uint64
		if v.LabelID != 0 {
			labelBusinessID = labels[v.LabelID].BusinessID
		}
		mapOldRefer[fmt.Sprintf("%d_%d_%d", v.Source, attrs[v.AttrID].BusinessID,
			labelBusinessID)] = struct{}{}
	}
	if len(mapOldRefer) != len(mapRefer) {
		return true, nil
	}
	for key := range mapOldRefer {
		if _, ok := mapRefer[key]; !ok {
			return true, nil
		}
	}
	return false, nil
}

// GetDocIdsByAttr 查询已有标签的文档id
func (l *Logic) GetDocIdsByAttr(ctx context.Context, robotId uint64) ([]uint64, error) {
	logx.I(ctx, "GetDocIdsByAttrSubStr robotId:%d", robotId)
	// 查询文档属性标签表，获取所有满足条件的文档id
	docAttributeLabelFilter := &entity.DocAttributeLabelFilter{
		RobotId:   robotId,
		IsDeleted: ptrx.Bool(false),
	}
	selectColumns := []string{entity.DocAttributeLabelTblColDocId}
	docAttributeLabels, err := l.GetDocAttributeLabelList(ctx, selectColumns, docAttributeLabelFilter)
	if err != nil {
		logx.E(ctx, "GetDocIdsByAttrSubStr failed, err: %+v", err)
		return nil, err
	}
	docIds := make([]uint64, 0)
	docIdMap := make(map[uint64]struct{}, 0)
	for _, docAttributeLabel := range docAttributeLabels {
		if _, ok := docIdMap[docAttributeLabel.DocID]; ok {
			continue
		}
		docIds = append(docIds, docAttributeLabel.DocID)
		docIdMap[docAttributeLabel.DocID] = struct{}{}
	}
	return docIds, nil
}

// GetDocIdsByAttrSubStr 查询属性名称或者属性标签包含指定子串的文档id列表
func (l *Logic) GetDocIdsByAttrSubStr(ctx context.Context, robotId uint64, nameSubStr string) ([]uint64, error) {
	logx.I(ctx, "GetDocIdsByAttrSubStr robotId:%d nameSubStr:%s", robotId, nameSubStr)
	// 因为t_doc_attribute_label表缺少robot_id字段，需要先查询t_doc_attribute表，获取该应用，所有的属性id
	attributeFilter := &entity.AttributeFilter{
		RobotId:   robotId,
		IsDeleted: ptrx.Bool(false),
	}
	selectColumns := []string{entity.AttributeTblColId}
	attributes, err := l.GetAttributeList(ctx, selectColumns, attributeFilter)
	if err != nil {
		logx.E(ctx, "GetDocIdsByAttrSubStr failed, err: %+v", err)
		return nil, err
	}
	attributeIds := make([]uint64, 0)
	for _, attribute := range attributes {
		attributeIds = append(attributeIds, attribute.ID)
	}
	// 查询属性标签表t_doc_attribute_label，获取标签名称或者相似标签包含子串的属性标签id
	attributeLabelFilter := &entity.AttributeLabelFilter{
		RobotId:                  robotId,
		AttrIds:                  attributeIds,
		NameOrSimilarLabelSubStr: nameSubStr,
		IsDeleted:                ptrx.Bool(false),
	}
	selectColumns = []string{entity.AttributeLabelTblColAttrId, entity.AttributeLabelTblColId}
	attributeLabels, err := l.GetAttributeLabelList(ctx, selectColumns, attributeLabelFilter)
	if err != nil {
		logx.E(ctx, "GetDocIdsByAttrSubStr failed, err: %+v", err)
		return nil, err
	}
	// 再查属性表t_doc_attribute，获取属性名称包含子串的属性id
	attributeFilter = &entity.AttributeFilter{
		RobotId:    robotId,
		NameSubStr: nameSubStr,
		IsDeleted:  ptrx.Bool(false),
	}
	selectColumns = []string{entity.AttributeTblColId}
	attributes, err = l.GetAttributeList(ctx, selectColumns, attributeFilter)
	if err != nil {
		logx.E(ctx, "GetDocIdsByAttrSubStr failed, err: %+v", err)
		return nil, err
	}
	// 将属性表和属性标签表的属性id合并，去重
	attributeIDMap := make(map[uint64]struct{})
	labelIDs := make([]uint64, 0)
	for _, attribute := range attributes {
		attributeIDMap[attribute.ID] = struct{}{}
	}
	for _, attributeLabel := range attributeLabels {
		if _, ok := attributeIDMap[attributeLabel.AttrID]; ok {
			continue
		}
		labelIDs = append(labelIDs, attributeLabel.ID)
	}
	docIDMap := make(map[uint64]struct{})
	if len(attributeIDMap) != 0 {
		attributeIDs := maps.Keys(attributeIDMap)
		for _, attributeIDChunks := range slicex.Chunk(attributeIDs, util.MaxSqlInCount) {
			// 查询文档属性标签表，获取所有满足条件的文档id
			docAttributeLabelFilter := &entity.DocAttributeLabelFilter{
				RobotId:   robotId,
				Source:    entity.AttributeLabelSourceKg,
				AttrIDs:   attributeIDChunks,
				IsDeleted: ptrx.Bool(false),
			}
			selectColumns = []string{entity.DocAttributeLabelTblColDocId}
			docAttributeLabels, err := l.GetDocAttributeLabelList(ctx, selectColumns, docAttributeLabelFilter)
			if err != nil {
				logx.E(ctx, "GetDocIdsByAttrSubStr failed, err: %+v", err)
				return nil, err
			}
			for _, docAttributeLabel := range docAttributeLabels {
				docIDMap[docAttributeLabel.DocID] = struct{}{}
			}
		}
	}
	if len(labelIDs) != 0 {
		// 查询文档属性标签表，获取所有满足条件的文档id
		for _, labelIDChunks := range slicex.Chunk(labelIDs, util.MaxSqlInCount) {
			docAttributeLabelFilter := &entity.DocAttributeLabelFilter{
				RobotId:   robotId,
				Source:    entity.AttributeLabelSourceKg,
				LabelIDs:  labelIDChunks,
				IsDeleted: ptrx.Bool(false),
			}
			selectColumns = []string{entity.DocAttributeLabelTblColDocId}
			docAttributeLabels, err := l.GetDocAttributeLabelList(ctx, selectColumns, docAttributeLabelFilter)
			if err != nil {
				logx.E(ctx, "GetDocIdsByAttrSubStr failed, err: %+v", err)
				return nil, err
			}
			for _, docAttributeLabel := range docAttributeLabels {
				docIDMap[docAttributeLabel.DocID] = struct{}{}
			}
		}
	}
	return maps.Keys(docIDMap), nil
}

// GetAttributeList 查询知识库信息
func (l *Logic) GetAttributeList(ctx context.Context, selectColumns []string, filter *entity.AttributeFilter) ([]*entity.Attribute, error) {
	allAttributeList := make([]*entity.Attribute, 0)
	if filter.Limit == 0 {
		logx.W(ctx, "GetAttributeList limit is 0")
		filter.Limit = entity.AttributeTableMaxPageSize
	}
	beginTime := time.Now()
	offset := filter.Offset
	alreadyGetCount := 0
	wantedCount := filter.Limit
	for {
		filter.Offset = offset
		filter.Limit = util.CalculateLimit(wantedCount, alreadyGetCount)
		if filter.Limit == 0 {
			break
		}
		attributeList, err := l.dao.GetAttributeListInfo(ctx, selectColumns, filter)
		if err != nil {
			logx.E(ctx, "GetAttributeList failed, err: %+v", err)
			return nil, err
		}
		allAttributeList = append(allAttributeList, attributeList...)
		if len(attributeList) < filter.Limit {
			// 已分页遍历完所有数据
			break
		}
		offset += filter.Limit
		alreadyGetCount += filter.Limit
	}
	logx.D(ctx, "GetAttributeList count:%d cost:%dms",
		len(allAttributeList), time.Since(beginTime).Milliseconds())
	return allAttributeList, nil
}

// GetAttributeLabelList 查询属性标签列表
func (l *Logic) GetAttributeLabelList(ctx context.Context, selectColumns []string, filter *entity.AttributeLabelFilter) ([]*entity.AttributeLabel, error) {
	allAttributeLabels := make([]*entity.AttributeLabel, 0)
	if filter.Limit == 0 {
		logx.W(ctx, "GetAttributeLabelList limit is 0")
		filter.Limit = entity.AttributeLabelTableMaxPageSize
	}
	beginTime := time.Now()
	offset := filter.Offset
	alreadyGetCount := 0
	wantedCount := filter.Limit
	for {
		filter.Offset = offset
		filter.Limit = util.CalculateLimit(wantedCount, alreadyGetCount)
		if filter.Limit == 0 {
			break
		}
		attributeLabels, err := l.dao.GetAttributeLabelListInfo(ctx, selectColumns, filter)
		if err != nil {
			logx.E(ctx, "GetAttributeLabelList failed, err: %+v", err)
			return nil, err
		}
		allAttributeLabels = append(allAttributeLabels, attributeLabels...)
		if len(attributeLabels) < filter.Limit {
			// 已分页遍历完所有数据
			break
		}
		offset += filter.Limit
		alreadyGetCount += filter.Limit
	}
	logx.D(ctx, "GetAttributeLabelList count:%d cost:%dms",
		len(allAttributeLabels), time.Since(beginTime).Milliseconds())
	return allAttributeLabels, nil
}

func (l *Logic) GetDocAttributeLabelList(ctx context.Context, selectColumns []string, filter *entity.DocAttributeLabelFilter) ([]*entity.DocAttributeLabel, error) {
	allAttributeLabels := make([]*entity.DocAttributeLabel, 0)
	if filter.Limit == 0 {
		logx.W(ctx, "GetAttributeLabelList limit is 0")
		filter.Limit = entity.AttributeLabelTableMaxPageSize
	}
	beginTime := time.Now()
	offset := filter.Offset
	alreadyGetCount := 0
	wantedCount := filter.Limit
	for {
		filter.Offset = offset
		filter.Limit = util.CalculateLimit(wantedCount, alreadyGetCount)
		if filter.Limit == 0 {
			break
		}
		attributeLabels, err := l.dao.GetDocAttributeLabelListInfo(ctx, selectColumns, filter)
		if err != nil {
			logx.E(ctx, "GetAttributeLabelList failed, err: %+v", err)
			return nil, err
		}
		allAttributeLabels = append(allAttributeLabels, attributeLabels...)
		if len(attributeLabels) < filter.Limit {
			// 已分页遍历完所有数据
			break
		}
		offset += filter.Limit
		alreadyGetCount += filter.Limit
	}
	logx.D(ctx, "GetAttributeLabelList count:%d cost:%dms",
		len(allAttributeLabels), time.Since(beginTime).Milliseconds())
	return allAttributeLabels, nil
}

func (l *Logic) GetDocAttributeLabelDetail(ctx context.Context, robotID uint64, docIDs []uint64) (
	map[uint64][]*entity.AttrLabel, error) {
	mapDocID2AttrLabels := make(map[uint64][]*entity.AttrLabel)
	if len(docIDs) == 0 {
		return mapDocID2AttrLabels, nil
	}
	attributeLabels, err := l.dao.GetDocAttributeLabel(ctx, robotID, docIDs)
	if err != nil {
		return nil, err
	}
	if len(attributeLabels) == 0 {
		return mapDocID2AttrLabels, nil
	}
	var mapKgAttrLabels map[uint64][]*entity.AttrLabel
	var kgErr error
	// 查询不同来源的属性标签信息
	g := errgroupx.New()
	g.SetLimit(10)
	// 来源，属性标签
	g.Go(func() error {
		mapKgAttrLabels, kgErr = l.GetDocAttributeLabelOfKg(ctx, robotID, attributeLabels)
		return kgErr
	})
	if err := g.Wait(); err != nil {
		logx.W(ctx, "GetDocAttributeLabelDetail robotID:%d,docIDs:%+v err :%v", robotID, docIDs, err)
		return nil, err
	}
	for docID, attrLabels := range mapKgAttrLabels {
		mapDocID2AttrLabels[docID] = append(mapDocID2AttrLabels[docID], attrLabels...)
	}
	return mapDocID2AttrLabels, nil
}

// GetDocAttributeLabelOfKg 获取获取来源为知识标签的文档属性标签信息
func (l *Logic) GetDocAttributeLabelOfKg(ctx context.Context, robotID uint64,
	attributeLabels []*entity.DocAttributeLabel) (map[uint64][]*entity.AttrLabel, error) {
	mapDocID2AttrLabels := make(map[uint64][]*entity.AttrLabel)
	mapDocAttrID2Attr := make(map[string]*entity.AttrLabel)
	sourceAttributeLabels, attrIDs, labelIDs := getDocAttributeLabelOfSource(attributeLabels,
		entity.AttributeLabelSourceKg)
	if len(sourceAttributeLabels) == 0 {
		return nil, nil
	}
	mapAttrID2Info, err := l.dao.GetAttributeByIDs(ctx, robotID, attrIDs)
	if err != nil {
		return nil, err
	}
	mapLabelID2Info, err := l.dao.GetAttributeLabelByIDs(ctx, labelIDs, robotID)
	if err != nil {
		return nil, err
	}
	for _, v := range sourceAttributeLabels {
		attr, ok := mapAttrID2Info[v.AttrID]
		if !ok {
			return nil, errs.ErrAttributeLabelNotFound
		}
		label, ok := mapLabelID2Info[v.LabelID]
		if v.LabelID > 0 && !ok {
			return nil, errs.ErrAttributeLabelNotFound
		}
		labelName := label.GetName()
		if v.LabelID == 0 {
			config.App()
			labelName = config.App().AttributeLabel.FullLabelValue
		}
		labelInfo := &entity.Label{
			LabelID:    v.LabelID,
			BusinessID: label.GetBusinessID(),
			LabelName:  labelName,
		}
		docAttrID := fmt.Sprintf("%d_%d", v.DocID, v.AttrID)
		attrInfo, ok := mapDocAttrID2Attr[docAttrID]
		if !ok {
			attrInfo = &entity.AttrLabel{
				Source:     v.Source,
				AttrID:     v.AttrID,
				BusinessID: attr.BusinessID,
				AttrKey:    attr.AttrKey,
				AttrName:   attr.Name,
			}
			mapDocAttrID2Attr[docAttrID] = attrInfo
			mapDocID2AttrLabels[v.DocID] = append(mapDocID2AttrLabels[v.DocID], attrInfo)
		}
		attrInfo.Labels = append(attrInfo.Labels, labelInfo)
	}
	return mapDocID2AttrLabels, nil
}

// getDocAttributeLabelOfSource
func getDocAttributeLabelOfSource(attributeLabels []*entity.DocAttributeLabel, source uint32) (
	[]*entity.DocAttributeLabel, []uint64, []uint64) {
	var sourceAttributeLabels []*entity.DocAttributeLabel
	var attrIDs, labelIDs []uint64
	mapAttrID := make(map[uint64]struct{}, 0)
	mapLabelID := make(map[uint64]struct{}, 0)
	for _, v := range attributeLabels {
		if v.Source != source {
			continue
		}
		sourceAttributeLabels = append(sourceAttributeLabels, v)
		if _, ok := mapAttrID[v.AttrID]; !ok {
			mapAttrID[v.AttrID] = struct{}{}
			attrIDs = append(attrIDs, v.AttrID)
		}
		if v.LabelID == 0 {
			continue
		}
		if _, ok := mapLabelID[v.LabelID]; !ok {
			mapLabelID[v.LabelID] = struct{}{}
			labelIDs = append(labelIDs, v.LabelID)
		}
	}
	return sourceAttributeLabels, attrIDs, labelIDs
}

// GetQAAttributeLabelDetail 获取QA的属性标签详情
func (l *Logic) GetQAAttributeLabelDetail(ctx context.Context, robotID uint64, qaIDs []uint64) (
	map[uint64][]*entity.AttrLabel, error) {
	mapQAID2AttrLabels := make(map[uint64][]*entity.AttrLabel)
	if len(qaIDs) == 0 {
		return mapQAID2AttrLabels, nil
	}
	attributeLabels, err := l.dao.GetQAAttributeLabel(ctx, robotID, qaIDs)
	if err != nil {
		return nil, err
	}
	if len(attributeLabels) == 0 {
		return mapQAID2AttrLabels, nil
	}
	var mapKgAttrLabels map[uint64][]*entity.AttrLabel
	// 查询不同来源的属性标签信息
	mapKgAttrLabels, err = l.GetQAAttributeLabelOfKg(ctx, robotID, attributeLabels)
	if err != nil {
		logx.W(ctx, "GetQAAttributeLabelDetail robotID:%d,qaIDs:%+v err :%v", robotID, qaIDs, err)
		return nil, err
	}
	for qaID, attrLabels := range mapKgAttrLabels {
		mapQAID2AttrLabels[qaID] = append(mapQAID2AttrLabels[qaID], attrLabels...)
	}
	return mapQAID2AttrLabels, nil
}

func (l *Logic) UpdateAttributeSuccess(ctx context.Context, attr *entity.Attribute, corpID, staffID uint64) error {
	db, err := knowClient.GormClient(ctx, "t_attribute", attr.RobotID, 0, []client.Option{}...)
	if err != nil {
		return err
	}
	err = db.Transaction(func(tx *gorm.DB) error {
		err := l.dao.UpdateAttributeSuccess(ctx, attr, tx)
		if err != nil {
			logx.W(ctx, "UpdateAttributeSuccess robotID:%d,attr:%+v err :%v", attr.RobotID, attr, err)
			return err
		}
		err = l.sendAttributeLabelUpdateNotice(ctx, corpID, staffID, attr, entity.AttributeLabelUpdateSuccessNoticeContent, releaseEntity.LevelSuccess)
		if err != nil {
			logx.W(ctx, "sendAttributeLabelUpdateNotice robotID:%d,attr:%+v err :%v", attr.RobotID, attr, err)
			return err
		}
		return nil
	}, nil)
	if err != nil {
		return fmt.Errorf("UpdateAttributeSuccess failed  err:%v", err)
	}
	return nil
}

func (l *Logic) UpdateAttributeFail(ctx context.Context, attr *entity.Attribute, corpID, staffID uint64) error {
	db, err := knowClient.GormClient(ctx, "t_attribute", attr.RobotID, 0, []client.Option{}...)
	if err != nil {
		return err
	}
	err = db.Transaction(func(tx *gorm.DB) error {
		err := l.dao.UpdateAttributeFail(ctx, attr, tx)
		if err != nil {
			logx.W(ctx, "UpdateAttributeFail robotID:%d,attr:%+v err :%v", attr.RobotID, attr, err)
			return err
		}
		err = l.sendAttributeLabelUpdateNotice(ctx, corpID, staffID, attr, entity.AttributeLabelUpdateFailNoticeContent, releaseEntity.LevelError)
		if err != nil {
			logx.W(ctx, "UpdateAttributeFail robotID:%d,attr:%+v err :%v", attr.RobotID, attr, err)
			return err
		}
		return nil
	}, nil)
	if err != nil {
		return fmt.Errorf("UpdateAttributeFail failed  err:%v", err)
	}
	return nil
}

func (l *Logic) sendAttributeLabelUpdateNotice(ctx context.Context, corpID, staffID uint64,
	attr *entity.Attribute, content, level string) error {
	operations := make([]releaseEntity.Operation, 0)
	noticeOptions := []releaseEntity.NoticeOption{
		releaseEntity.WithPageID(releaseEntity.NoticeAttributeLabelPageID),
		releaseEntity.WithLevel(level),
		releaseEntity.WithContent(i18n.Translate(ctx, content, attr.Name)),
	}
	switch level {
	case releaseEntity.LevelSuccess:
		noticeOptions = append(noticeOptions, releaseEntity.WithSubject(i18n.Translate(ctx, i18nkey.KeyKnowledgeTagUpdateSuccess)))
		noticeOptions = append(noticeOptions, releaseEntity.WithGlobalFlag())
	case releaseEntity.LevelError:
		noticeOptions = append(noticeOptions, releaseEntity.WithSubject(i18n.Translate(ctx, i18nkey.KeyKnowledgeTagUpdateFailure)))
		noticeOptions = append(noticeOptions, releaseEntity.WithGlobalFlag())
	case releaseEntity.LevelInfo:
		noticeOptions = append(noticeOptions, releaseEntity.WithForbidCloseFlag())
	}
	notice := releaseEntity.NewNotice(releaseEntity.NoticeTypeAttributeLabelUpdate, attr.ID, corpID, attr.RobotID,
		staffID, noticeOptions...)
	if err := notice.SetOperation(operations); err != nil {
		logx.E(ctx, "序列化通知操作参数失败 operations:%+v err:%+v", operations, err)
		return err
	}
	if err := l.rpc.PlatformAdmin.CreateNotice(ctx, notice); err != nil {
		return err
	}
	return nil
}

// UpdateAttribute 更新属性
func (l *Logic) UpdateAttribute(ctx context.Context, req *entity.UpdateAttributeLabelReq, oldAttr *entity.Attribute,
	corpID, staffID uint64, needUpdateCacheFlag bool, newLabelRedisValue []entity.AttributeLabelRedisValue) (uint64, error) {
	taskID, err := l.dao.CreateUpdateAttributeTask(ctx, req, corpID, staffID, oldAttr.RobotID)
	if err != nil {
		logx.E(ctx, "更新标签,创建任务失败 err:%+v", err)
		return 0, err
	}
	robotID := oldAttr.RobotID
	db, err := knowClient.GormClient(ctx, "t_attribute", robotID, 0, []client.Option{}...)
	if err != nil {
		return 0, err
	}
	err = db.Transaction(func(tx *gorm.DB) error {
		if err = l.dao.UpdateAttribute(ctx, req.Attr, tx); err != nil {
			return err
		}
		if err = l.dao.DeleteAttributeLabel(ctx, oldAttr.RobotID, req.Attr.ID, req.DeleteLabelIDs, tx); err != nil {
			return err
		}
		if err = l.dao.CreateAttributeLabel(ctx, req.AddLabels, tx); err != nil {
			return err
		}
		if err = l.dao.UpdateAttributeLabels(ctx, req.UpdateLabels, tx); err != nil {
			return err
		}
		if needUpdateCacheFlag {
			if len(newLabelRedisValue) == 0 {
				if err = l.dao.PipelineDelAttributeLabelRedis(ctx, oldAttr.RobotID, []string{oldAttr.AttrKey},
					entity.AttributeLabelsPreview); err != nil {
					return err
				}
			} else {
				if err = l.dao.SetAttributeLabelsRedis(ctx, oldAttr.RobotID, oldAttr.AttrKey, newLabelRedisValue,
					entity.AttributeLabelsPreview); err != nil {
					// 可能出现删除redis旧数据成功，添加新数据失败，没法回滚，要通过定时任务刷新缓存
					return err
				}
			}
		}
		if !req.IsNeedPublish { // 不需要同步的情况下,PublishParams为空使用传入的参数
			taskInfo := entity.AttributeLabelTask{
				ID:            taskID,
				CorpID:        corpID,
				CreateStaffID: staffID,
				RobotID:       oldAttr.RobotID,
				Status:        entity.AttributeLabelTaskStatusSuccess,
			}
			err = l.UpdateAttributeTask(ctx, &taskInfo)
			return err
		}
		req.PublishParams.TaskID = taskID
		if err = l.sendAttributeLabelUpdateNotice(ctx, req.PublishParams.CorpID, req.PublishParams.StaffID,
			req.Attr, entity.AttributeLabelUpdatingNoticeContent, releaseEntity.LevelInfo); err != nil {
			return err
		}
		if err = async.NewAttributeLabelUpdateTask(ctx, req.Attr.RobotID, req.PublishParams); err != nil {
			return err
		}
		return nil
	}, nil)
	if err != nil {
		logx.W(ctx, "Failed to Update Attributes err:%+v", err)
		taskInfo := entity.AttributeLabelTask{
			ID:            taskID,
			CorpID:        corpID,
			CreateStaffID: staffID,
			RobotID:       oldAttr.RobotID,
			Status:        entity.AttributeLabelTaskStatusFailed,
		}
		err = l.UpdateAttributeTask(ctx, &taskInfo)
		return 0, err
	}
	err = l.dao.AddAndUpdateAttribute(ctx, robotID, req.Attr)
	if err != nil {
		return 0, err
	}
	addAndUpdateLabels, err := l.fillAttributeLabelIDsAfterInsert(ctx, req.Attr.RobotID, req.AddLabels)
	if err != nil {
		return 0, err
	}
	err = l.dao.BatchAddAndUpdateAttributeLabels(ctx, req.Attr.RobotID, req.Attr.ID, addAndUpdateLabels)
	if err != nil {
		return 0, err
	}
	err = l.dao.BatchDeleteAttributeLabelsByIDs(ctx, req.Attr.RobotID, req.DeleteLabelIDs)
	if err != nil {
		return 0, err
	}

	return taskID, nil
}

// IDMapping ID映射信息
type IDMapping struct {
	NewBizID     uint64 // 新的 BusinessID
	NewPrimaryID uint64 // 新的自增ID
}

// BatchCreateAttributeResult 批量创建属性的结果
type BatchCreateAttributeResult struct {
	// AttrOldBizIDMapping 属性 旧BusinessID -> 新ID映射
	AttrOldBizIDMapping map[uint64]*IDMapping
	// LabelOldBizIDMapping 标签值 旧BusinessID -> 新ID映射
	LabelOldBizIDMapping map[uint64]*IDMapping
}

// BatchCreateAttribute 批量创建属性，返回创建结果
// 注意：attrLabels 中的 Attr.BusinessID 和 Label.BusinessID 导入场景为非零，用于记录映射关系
// idMappingConfig: 导入场景的 ID 映射配置，如果存在映射则使用映射的 ID，否则生成新 ID；非导入场景传 nil
func (l *Logic) BatchCreateAttribute(ctx context.Context, attrLabels []*entity.AttributeLabelItem,
	idMappingConfig *kb_package.IDMappingConfig) (*BatchCreateAttributeResult, error) {
	result := &BatchCreateAttributeResult{
		AttrOldBizIDMapping:  make(map[uint64]*IDMapping),
		LabelOldBizIDMapping: make(map[uint64]*IDMapping),
	}
	if len(attrLabels) == 0 {
		return result, nil
	}
	robotID := attrLabels[0].Attr.RobotID
	var attributeList []*entity.Attribute
	var attributeLabelList []*entity.AttributeLabel
	// 记录标签值的 新BusinessID -> 旧BusinessID 映射（用于后续填充 ID 后构建最终映射）
	newBizIDToOldBizID := make(map[uint64]uint64)
	db, err := knowClient.GormClient(ctx, "t_attribute", attrLabels[0].Attr.RobotID, 0, []client.Option{}...)
	if err != nil {
		return nil, err
	}
	err = db.Transaction(func(tx *gorm.DB) error {
		for _, v := range attrLabels {
			// 保存旧的 BusinessID（导入场景会存在）
			oldAttrBizID := v.Attr.BusinessID
			v.Attr.BusinessID = idMappingConfig.GetOrGenerateBizID(kb_package.ModuleKbLabel, oldAttrBizID)

			attrID, err := l.dao.CreateAttribute(ctx, v.Attr, tx)
			v.Attr.ID = attrID
			if err != nil {
				return err
			}
			// 记录属性 旧BusinessID -> 新ID映射（导入场景）
			if oldAttrBizID != 0 {
				result.AttrOldBizIDMapping[oldAttrBizID] = &IDMapping{
					NewBizID:     v.Attr.BusinessID,
					NewPrimaryID: attrID,
				}
			}

			redisAttrItem := &entity.AttributeLabelItem{
				Attr: &entity.Attribute{
					RobotID: v.Attr.RobotID,
					AttrKey: v.Attr.AttrKey,
				},
				Labels: make([]*entity.AttributeLabel, 0),
			}
			for _, label := range v.Labels {
				// 保存旧ID
				oldLabelBizID := label.BusinessID
				// 获取或生成新ID
				newLabelBizID := idMappingConfig.GetOrGenerateBizID(kb_package.ModuleKbLabelValue, oldLabelBizID)
				label.BusinessID = newLabelBizID
				label.AttrID = attrID
				// 记录 新BizID -> 旧BizID 映射（导入场景，用于后续反查）
				if oldLabelBizID != 0 {
					newBizIDToOldBizID[newLabelBizID] = oldLabelBizID
				}
				if label.SimilarLabel != "" {
					redisAttrItem.Labels = append(redisAttrItem.Labels, label)
				}
			}
			if err := l.dao.CreateAttributeLabel(ctx, v.Labels, tx); err != nil {
				return err
			}
			if len(redisAttrItem.Labels) > 0 {
				// 包含相似标签的标签才需要同步到redis，以便在对话时做相似标签替换
				// TODO(ericjwang): 为什么要这样设计？cache miss 回源不行吗
				if err := l.dao.CreateAttributeLabelsRedis(ctx, redisAttrItem); err != nil {
					return err
				}
			}
			attributeList = append(attributeList, v.Attr)
			attributeLabelList = append(attributeLabelList, v.Labels...)
		}
		return nil
	}, nil)
	if err != nil {
		logx.E(ctx, "批量创建标签失败 err:%+v", err)
		return nil, err
	}
	// 往es写入所有attr bulk超过1000之后会有性能问题，导入属性目前上限为100
	err = l.dao.BatchAddAndUpdateAttributes(ctx, robotID, attributeList)
	if err != nil {
		return nil, err
	}

	logx.I(ctx, "批量创建标签数量,count:%+v", len(attributeLabelList))
	// 填充插入后的标签ID和attr id
	labelsWithID, err := l.fillAttributeLabelIDsAfterInsert(ctx, robotID, attributeLabelList)
	if err != nil {
		return nil, err
	}

	// 记录标签值 旧BusinessID -> 新ID映射（通过 新BizID 反查 旧BizID）
	for _, label := range labelsWithID {
		if oldBizID, ok := newBizIDToOldBizID[label.BusinessID]; ok {
			result.LabelOldBizIDMapping[oldBizID] = &IDMapping{
				NewBizID:     label.BusinessID,
				NewPrimaryID: label.ID,
			}
		}
	}

	logx.I(ctx, "批量创建标签标准值,count:%+v", len(labelsWithID))
	for _, labelChunk := range slicex.Chunk(labelsWithID, 100) {
		err = l.dao.BatchAddAndUpdateAttributeLabels(ctx, robotID, 0, labelChunk)
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}

// UpdateAttributeTask 更新标签异步任务
func (l *Logic) UpdateAttributeTask(ctx context.Context, attributeLabelTask *entity.AttributeLabelTask) error {
	logx.D(ctx, "UpdateAttributeTask,  params: %+v", attributeLabelTask)
	if err := l.dao.UpdateAttributeLabelTask(ctx, attributeLabelTask); err != nil {
		logx.E(ctx, "编辑更新标签异步任务失败 err:%+v", err)
		return err
	}
	return nil
}

func (l *Logic) fillAttributeLabelIDsAfterInsert(ctx context.Context, robotID uint64,
	addLabels []*entity.AttributeLabel) ([]*entity.AttributeLabel, error) {
	if len(addLabels) == 0 {
		return addLabels, nil
	}
	// 插入的label没有id，需要从数据库中反查出来
	addLabelBizIDs := make([]uint64, 0)
	for _, label := range addLabels {
		addLabelBizIDs = append(addLabelBizIDs, label.BusinessID)
	}
	addLabelMap, err := l.dao.GetAttributeLabelByBizIDs(ctx, addLabelBizIDs, robotID)
	if err != nil {
		return addLabels, err
	}

	var addLabelsWithID []*entity.AttributeLabel
	for _, label := range addLabelMap {
		addLabelsWithID = append(addLabelsWithID, label)
	}
	return addLabelsWithID, nil
}

// GetWorkflowListByAttribute 获取标签被引用的工作流列表
func (l *Logic) GetWorkflowListByAttribute(ctx context.Context, req *pb.CheckAttributeReferWorkFlowReq) ([]*pb.AttributeRefByWorkflow, error) {
	rsp, err := l.rpc.TaskFlow.GetWorkflowListByAttribute(ctx, req.BotBizId, req.GetAttributeBizIds())
	if err != nil {
		logx.E(ctx, "GetWorkflowListByAttribute failed, err: %+v", err)
		return nil, err
	}
	var ret []*pb.AttributeRefByWorkflow
	if rsp == nil || len(rsp.GetList()) == 0 {
		return ret, nil
	}
	for _, item := range rsp.GetList() {
		var list []*pb.WorkflowRef
		for _, wf := range item.GetWorkflowList() {
			list = append(list, &pb.WorkflowRef{
				WorkflowId:   wf.GetWorkflowId(),
				WorkflowName: wf.GetWorkflowName(),
				WorkflowDesc: wf.GetWorkflowDesc(),
				AppBizId:     wf.GetAppBizId(),
				UpdateTime:   wf.GetUpdateTime(),
			})
		}
		ret = append(ret, &pb.AttributeRefByWorkflow{AttributeBizId: item.GetAttributeBizId(), WorkflowList: list})
	}
	return ret, nil
}

// GetWorkflowListByAttributeLabel 获取标签值被引用的工作流列表
func (l *Logic) GetWorkflowListByAttributeLabel(ctx context.Context, req *pb.CheckAttributeLabelReferReq) ([]*pb.AttributeLabelRefByWorkflow, error) {
	rsp, err := l.rpc.TaskFlow.GetWorkflowListByAttributeLabel(ctx, req.BotBizId, []string{req.GetLabelBizId()})
	if err != nil {
		logx.E(ctx, "GetWorkflowListByAttributeLabel failed, err: %+v", err)
		return nil, err
	}
	var ret []*pb.AttributeLabelRefByWorkflow
	if rsp == nil || len(rsp.GetList()) == 0 {
		return ret, nil
	}
	for _, item := range rsp.GetList() {
		var list []*pb.WorkflowRef
		for _, wf := range item.GetWorkflowList() {
			list = append(list, &pb.WorkflowRef{
				WorkflowId:   wf.GetWorkflowId(),
				WorkflowName: wf.GetWorkflowName(),
				WorkflowDesc: wf.GetWorkflowDesc(),
				AppBizId:     wf.GetAppBizId(),
				UpdateTime:   wf.GetUpdateTime(),
			})
		}
		ret = append(ret, &pb.AttributeLabelRefByWorkflow{AttributeLabelBizId: item.GetAttributeLabelBizId(), WorkflowList: list})
	}
	return ret, nil
}

// UpdateAttrLabelsCacheProd 更新发布端标签缓存
func (l *Logic) UpdateAttrLabelsCacheProd(ctx context.Context, robotID uint64, attrKeys []string) error {
	delStatusAndIDs, err := l.dao.GetAttributeKeysDelStatusAndIDs(ctx, robotID, attrKeys)
	if err != nil {
		logx.E(ctx, "UpdateAttrLabelsCacheProd, GetAttributeKeysDelStatusAndIDs err, %v", err)
		return err
	}
	var updateAttrIDs []uint64 // 包括新增和修改
	var delAttrKeys []string
	for k, v := range delStatusAndIDs {
		if v.IsDeleted {
			delAttrKeys = append(delAttrKeys, k)
		} else {
			updateAttrIDs = append(updateAttrIDs, v.AttrID)
		}
	}
	logx.I(ctx, "UpdateAttrLabelsCacheProd delStatusAndIDs, %+v", delStatusAndIDs)
	if len(delAttrKeys) > 0 {
		err = l.dao.PipelineDelAttributeLabelRedis(ctx, robotID, delAttrKeys, entity.AttributeLabelsProd)
		if err != nil {
			logx.E(ctx, "UpdateAttrLabelsCacheProd, PiplineDelAttributeLabelRedis err, %v", err)
			return err
		}
	}
	mapAttr2Labels, err := l.dao.GetAttributeLabelByAttrIDsWithNotEmptySimilarLabelProd(ctx, updateAttrIDs, robotID)
	if err != nil {
		logx.E(ctx, "UpdateAttrLabelsCacheProd, GetAttributeLabelByAttrIDsProd err, %v", err)
		return err
	}
	logx.I(ctx, "UpdateAttrLabelsCacheProd mapAttr2Labels, %+v", mapAttr2Labels)
	attrKey2RedisValue := make(map[string][]entity.AttributeLabelRedisValue)
	for attrID, Labels := range mapAttr2Labels {
		var redisValue []entity.AttributeLabelRedisValue
		for _, l := range Labels {
			redisValue = append(redisValue, entity.AttributeLabelRedisValue{
				Name:          l.Name,
				BusinessID:    l.BusinessID,
				SimilarLabels: l.SimilarLabel,
			})
		}
		if len(redisValue) == 0 {
			continue
		}
		for k, v := range delStatusAndIDs {
			if v.AttrID == attrID {
				attrKey2RedisValue[k] = redisValue
				break
			}
		}
	}
	logx.I(ctx, "UpdateAttrLabelsCacheProd attrKey2RedisValue, %+v", attrKey2RedisValue)
	if len(attrKey2RedisValue) == 0 {
		return nil
	}
	err = l.dao.PipelineSetAttributeLabelRedis(ctx, robotID, attrKey2RedisValue, entity.AttributeLabelsProd)
	if err != nil {
		logx.E(ctx, "UpdateAttrLabelsCacheProd, Pipline SetAttributeLabelRedis err, %v", err)
		return err
	}
	return nil
}

// GetQAAttributeLabelOfKg 获取来源为知识标签的QA属性标签信息
func (l *Logic) GetQAAttributeLabelOfKg(ctx context.Context, robotID uint64,
	attributeLabels []*entity.QAAttributeLabel) (map[uint64][]*entity.AttrLabel, error) {
	mapQAID2AttrLabels := make(map[uint64][]*entity.AttrLabel)
	mapQAAttrID2Attr := make(map[string]*entity.AttrLabel)
	sourceAttributeLabels, attrIDs, labelIDs := getQAAttributeLabelOfSource(attributeLabels,
		entity.AttributeLabelSourceKg)
	if len(sourceAttributeLabels) == 0 {
		return nil, nil
	}
	mapAttrID2Info, err := l.dao.GetAttributeByIDs(ctx, robotID, attrIDs)
	if err != nil {
		return nil, err
	}
	mapLabelID2Info, err := l.dao.GetAttributeLabelByIDs(ctx, labelIDs, robotID)
	if err != nil {
		return nil, err
	}
	for _, v := range sourceAttributeLabels {
		attr, ok := mapAttrID2Info[v.AttrID]
		if !ok {
			return nil, errs.ErrAttributeLabelNotFound
		}
		label, ok := mapLabelID2Info[v.LabelID]
		if v.LabelID > 0 && !ok {
			return nil, errs.ErrAttributeLabelNotFound
		}
		labelName := label.GetName()
		if v.LabelID == 0 {
			labelName = config.App().AttributeLabel.FullLabelValue
		}
		labelInfo := &entity.Label{
			LabelID:    v.LabelID,
			BusinessID: label.GetBusinessID(),
			LabelName:  labelName,
		}
		qaAttrID := fmt.Sprintf("%d_%d", v.QAID, v.AttrID)
		attrInfo, ok := mapQAAttrID2Attr[qaAttrID]
		if !ok {
			attrInfo = &entity.AttrLabel{
				Source:     v.Source,
				AttrID:     v.AttrID,
				BusinessID: attr.BusinessID,
				AttrKey:    attr.AttrKey,
				AttrName:   attr.Name,
			}
			mapQAAttrID2Attr[qaAttrID] = attrInfo
			mapQAID2AttrLabels[v.QAID] = append(mapQAID2AttrLabels[v.QAID], attrInfo)
		}
		attrInfo.Labels = append(attrInfo.Labels, labelInfo)
	}
	return mapQAID2AttrLabels, nil
}

// getQAAttributeLabelOfSource TODO
func getQAAttributeLabelOfSource(attributeLabels []*entity.QAAttributeLabel, source uint32) (
	[]*entity.QAAttributeLabel, []uint64, []uint64) {
	var sourceAttributeLabels []*entity.QAAttributeLabel
	var attrIDs, labelIDs []uint64
	mapAttrID := make(map[uint64]struct{}, 0)
	mapLabelID := make(map[uint64]struct{}, 0)
	for _, v := range attributeLabels {
		if v.Source != source {
			continue
		}
		sourceAttributeLabels = append(sourceAttributeLabels, v)
		if _, ok := mapAttrID[v.AttrID]; !ok {
			mapAttrID[v.AttrID] = struct{}{}
			attrIDs = append(attrIDs, v.AttrID)
		}
		if v.LabelID == 0 {
			continue
		}
		if _, ok := mapLabelID[v.LabelID]; !ok {
			mapLabelID[v.LabelID] = struct{}{}
			labelIDs = append(labelIDs, v.LabelID)
		}
	}
	return sourceAttributeLabels, attrIDs, labelIDs
}

func (l *Logic) GetWaitReleaseAttributeCount(ctx context.Context, robotID uint64, name string,
	actions []uint32, startTime, endTime time.Time) (uint64, error) {
	return l.dao.GetWaitReleaseAttributeCount(ctx, robotID, name, actions, startTime, endTime)
}

func (l *Logic) GetWaitReleaseAttributeList(ctx context.Context, robotID uint64, name string,
	actions []uint32, page, pageSize uint32, startTime, endTime time.Time) ([]*entity.Attribute, error) {
	return l.dao.GetWaitReleaseAttributeList(ctx, robotID, name, actions, page, pageSize, startTime, endTime)
}

// ModifyAttributeLabel 编辑属性标签
func (l *Logic) ModifyAttributeLabel(ctx context.Context, req *pb.ModifyAttributeLabelReq) (
	*entity.ModifyAttributeLabelRsp, error) {
	logx.I(ctx, "ModifyAttributeLabel Req:%+v", req)
	var err error
	rsp := new(entity.ModifyAttributeLabelRsp)
	// 检查应用
	app, err := l.appLogic.CheckApp(ctx, req.GetBotBizId())
	if err != nil {
		logx.W(ctx, "ModifyAttributeLabel CheckApp err:%+v", err)
		return rsp, err
	}
	// 检查属性
	oldAttr, err := l.checkModifyAttribute(ctx, app.PrimaryId, req)
	if err != nil {
		logx.E(ctx, "ModifyAttributeLabel checkModifyAttribute err:%+v", err)
		return nil, err
	}
	// 收集所有变化的标签信息
	labelBizID2Info := make(map[uint64]*entity.AttributeLabel)
	// 检查待删除的标签值
	deleteLabelBizIDs, deleteLabelIDs, err := l.checkModifyAttributeDeleteLabels(ctx, req.GetDeleteLabelBizIds(), app.PrimaryId, oldAttr.ID, labelBizID2Info)
	if err != nil {
		logx.W(ctx, "ModifyAttributeLabel checkModifyAttributeDeleteLabels err:%+v", err)
		return nil, err
	}
	// 检查待更新的标签值
	needPublishLabelIDs, err := l.checkModifyAttributeUpdateLabels(ctx, req.GetLabels(), app.PrimaryId, oldAttr.ID, labelBizID2Info)
	if err != nil {
		logx.W(ctx, "ModifyAttributeLabel checkModifyAttributeUpdateLabels err:%+v", err)
		return nil, err
	}
	// 构造发布参数
	publishParams := &entity.AttributeLabelUpdateParams{
		CorpID:   app.CorpPrimaryId,
		StaffID:  app.StaffID,
		RobotID:  app.PrimaryId,
		AttrID:   oldAttr.ID,
		LabelIDs: needPublishLabelIDs,
	}

	if err := l.checkNeedPublishLabelDocAndQaStatus(ctx, app.PrimaryId, publishParams); err != nil {
		logx.W(ctx, "ModifyAttributeLabel checkNeedPublishLabelDocAndQaStatus err:%+v", err)
		return rsp, err
	}
	// 检查待新增的标签值
	addLabelCounts, err := l.checkModifyAttributeAddLabels(ctx, req.GetLabels(), labelBizID2Info)
	if err != nil {
		logx.W(ctx, "ModifyAttributeLabel checkModifyAttributeAddLabels err:%+v", err)
		return nil, err
	}
	// 检查标签值总数是否超限
	uin := contextx.Metadata(ctx).Uin()
	if !config.IsInWhiteList(uin, app.BizId, config.GetWhitelistConfig().InfinityAttributeLabel) {
		// 非白名单应用需要校验标签值总数是否超限
		filter := &entity.AttributeLabelFilter{
			RobotId: app.PrimaryId,
			AttrIds: []uint64{oldAttr.ID},
		}
		selectColumns := []string{entity.AttributeLabelTblColId}
		count, err := l.dao.GetAttributeLabelCountByFilter(ctx, selectColumns, filter)
		if err != nil {
			return nil, err
		}
		if uint64(count)+addLabelCounts-uint64(len(deleteLabelBizIDs)) >
			uint64(config.App().AttributeLabel.LabelLimit) {
			return nil, errs.ErrAttributeLabelLimit
		}
	}
	updateAttributeLabelReq, needUpdateCacheFlag, newLabelRedisValue, err := l.fillModifyAttributeLabelReq(ctx,
		app.PrimaryId, req, publishParams, oldAttr, deleteLabelIDs, deleteLabelBizIDs, labelBizID2Info)
	logx.D(ctx, "ModifyAttributeLabel updateAttributeLabelReq:%+v needUpdateCacheFlag:%+v "+
		"newLabelRedisValue:%+v ", updateAttributeLabelReq, needUpdateCacheFlag, newLabelRedisValue)
	taskID, err := l.UpdateAttribute(ctx, updateAttributeLabelReq, oldAttr, app.CorpPrimaryId, app.StaffID,
		needUpdateCacheFlag, newLabelRedisValue)
	if err != nil {
		return rsp, err
	}
	rsp.TaskId = taskID
	rsp.BusinessID = app.BizId
	rsp.DeleteLabelBizIDs = deleteLabelBizIDs
	rsp.AddLabels = updateAttributeLabelReq.AddLabels
	rsp.UpdateLabels = updateAttributeLabelReq.UpdateLabels
	return rsp, nil
}

// fillModifyAttributeLabelReq 构造修改属性标签请求结构体
func (l *Logic) fillModifyAttributeLabelReq(ctx context.Context, robotID uint64, req *pb.ModifyAttributeLabelReq,
	publishParams *entity.AttributeLabelUpdateParams, oldAttr *entity.Attribute, deleteLabelIDs []uint64, deleteLabelBizIDs []uint64,
	labelBizID2Info map[uint64]*entity.AttributeLabel) (*entity.UpdateAttributeLabelReq, bool, []entity.AttributeLabelRedisValue, error) {
	logx.D(ctx, "fillModifyAttributeLabelReq robotID:%d, req:%+v publishParams:%+v oldAttr:%+v "+
		"deleteLabelIDs:%+v deleteLabelBizIDs:%+v labelBizID2Info:%+v", robotID, req, publishParams, oldAttr,
		deleteLabelIDs, deleteLabelBizIDs, labelBizID2Info)
	for key, value := range labelBizID2Info {
		logx.D(ctx, "fillModifyAttributeLabelReq labelBizID2Info key:%d value:%+v", key, value)
	}
	attrNextAction := oldAttr.NextAction
	if oldAttr.NextAction != entity.AttributeNextActionAdd {
		attrNextAction = entity.AttributeNextActionUpdate
	}
	isNeedPublish := false
	if len(publishParams.LabelIDs) > 0 {
		isNeedPublish = true
	}
	// 构造属性标签
	attr := &entity.Attribute{
		ID:            oldAttr.ID,
		BusinessID:    oldAttr.BusinessID,
		RobotID:       robotID,
		AttrKey:       req.GetAttrKey(),
		Name:          req.GetAttrName(),
		ReleaseStatus: entity.AttributeStatusWaitRelease,
		NextAction:    attrNextAction,
		IsUpdating:    isNeedPublish,
		UpdateTime:    time.Now(),
	}
	// 构造标签值
	addLabels := make([]*entity.AttributeLabel, 0)
	updateLabels := make([]*entity.AttributeLabel, 0)
	needUpdateCacheFlag := false
	for _, labelBizId := range deleteLabelBizIDs {
		if labelBizID2Info[labelBizId].SimilarLabel != "" {
			// 如果删除的标签包含相似标签值，需要更新缓存
			needUpdateCacheFlag = true
		}
		delete(labelBizID2Info, labelBizId)
	}
	for _, label := range req.GetLabels() {
		logx.D(ctx, "fillModifyAttributeLabelReq label:%+v", label)
		similarLabel, err := parseSimilarLabels(label.GetSimilarLabels())
		if err != nil {
			logx.D(ctx, "parse similar labels err:%v", err)
			return nil, false, nil, err
		}
		if label.GetLabelBizId() == "" {
			newLabel := &entity.AttributeLabel{
				RobotID:       robotID,
				BusinessID:    uint64(l.uniIDNode.Generate()),
				AttrID:        oldAttr.ID,
				Name:          label.GetLabelName(),
				SimilarLabel:  similarLabel,
				ReleaseStatus: entity.AttributeStatusWaitRelease,
				NextAction:    entity.AttributeNextActionAdd,
			}
			addLabels = append(addLabels, newLabel)
			labelBizID2Info[newLabel.BusinessID] = newLabel
			if len(label.GetSimilarLabels()) > 0 {
				// 如果新增的标签包含相似标签值，需要更新缓存
				needUpdateCacheFlag = true
			}
			continue
		}
		labelBizID, err := util.CheckReqParamsIsUint64(ctx, label.GetLabelBizId())
		if err != nil {
			return nil, false, nil, err
		}
		labelInfo, ok := labelBizID2Info[labelBizID]
		if !ok || labelInfo == nil {
			logx.W(ctx, "labelBizID:%d not found", labelBizID)
			continue
		}
		if labelBizID2Info[labelBizID].SimilarLabel != "" || len(label.GetSimilarLabels()) > 0 {
			// 如果更新的标签变更前后包含相似标签值，需要更新缓存
			needUpdateCacheFlag = true
		}
		labelNextAction := labelBizID2Info[labelBizID].NextAction
		if labelBizID2Info[labelBizID].NextAction != entity.AttributeNextActionAdd {
			labelNextAction = entity.AttributeNextActionUpdate
		}
		updateLabel := &entity.AttributeLabel{
			ID:            labelBizID2Info[labelBizID].ID,
			RobotID:       robotID,
			BusinessID:    labelBizID,
			AttrID:        oldAttr.ID,
			Name:          label.GetLabelName(),
			SimilarLabel:  similarLabel,
			ReleaseStatus: entity.AttributeStatusWaitRelease,
			NextAction:    labelNextAction,
		}
		labelBizID2Info[labelBizID] = updateLabel
		updateLabels = append(updateLabels, updateLabel)
	}
	updateAttributeLabelReq := &entity.UpdateAttributeLabelReq{
		IsNeedPublish:     isNeedPublish,
		PublishParams:     *publishParams,
		Attr:              attr,
		DeleteLabelIDs:    deleteLabelIDs,
		DeleteLabelBizIDs: deleteLabelBizIDs,
		AddLabels:         addLabels,
		UpdateLabels:      updateLabels,
	}
	newLabelRedisValue := make([]entity.AttributeLabelRedisValue, 0)
	for _, label := range labelBizID2Info {
		if label.SimilarLabel == "" {
			// 只缓存包含相似标签值的标签
			continue
		}
		newLabelRedisValue = append(newLabelRedisValue, entity.AttributeLabelRedisValue{
			BusinessID:    label.BusinessID,
			Name:          label.Name,
			SimilarLabels: label.SimilarLabel,
		})
	}
	return updateAttributeLabelReq, needUpdateCacheFlag, newLabelRedisValue, nil
}

// GetAttrLabelsByAttrKeyAndLabelName 通过attrKey和labelName获取标签的详细信息
func (l *Logic) GetAttrLabelsByAttrKeyAndLabelName(ctx context.Context, robotID uint64, attrKeyAndLabelNameMap map[string][]string) (
	map[string]map[string]*entity.AttributeLabel, error) {
	// 先查询所有的属性ID
	attrKeys := maps.Keys(attrKeyAndLabelNameMap)
	attrMap, err := l.dao.GetAttributeByKeys(ctx, robotID, attrKeys)
	if err != nil {
		logx.E(ctx, "GetAttributeByKeys fail, robotID:%d, attrKeys:%+v, err:%v", robotID, attrKeys, err)
		return nil, err
	}
	attrLabelMap := make(map[string]map[string]*entity.AttributeLabel)
	for attrKey, labelNames := range attrKeyAndLabelNameMap {
		attr, ok := attrMap[attrKey]
		if !ok {
			logx.W(ctx, "attrKey:%s not found", attrKey)
			continue
		}
		selectColumns := []string{entity.AttributeLabelTblColId, entity.AttributeLabelTblColBusinessId,
			entity.AttributeLabelTblColAttrId, entity.AttributeLabelTblColName}
		filter := &entity.AttributeLabelFilter{
			AttrId: attr.ID,
			Names:  labelNames,
		}
		attrLabelList, err := l.GetAttributeLabelList(ctx, selectColumns, filter)
		if err != nil {
			logx.E(ctx, "GetAttributeLabelList fail, robotID:%d, attrKey:%s, labelNames:%+v, err:%v",
				robotID, attrKey, labelNames, err)
			return nil, err
		}
		for _, attrLabel := range attrLabelList {
			if _, ok := attrLabelMap[attrKey]; !ok {
				attrLabelMap[attrKey] = make(map[string]*entity.AttributeLabel)
			}
			attrLabelMap[attrKey][attrLabel.Name] = attrLabel
		}
	}
	return attrLabelMap, nil
}

// ParseLabelStrings 将字符串数组转换为ReleaseAttrLabel数组
// 每个字符串格式为 "name:value"，通过冒号分隔name和value
func (l *Logic) ParseLabelStrings(ctx context.Context, labelStrings []string) []*releaseEntity.ReleaseAttrLabel {
	result := make([]*releaseEntity.ReleaseAttrLabel, 0, len(labelStrings))
	for _, labelStr := range labelStrings {
		if labelStr == "" {
			continue
		}
		// 通过冒号分隔 name 和 value
		parts := strings.SplitN(labelStr, ":", 2)
		if len(parts) != 2 {
			logx.W(ctx, "ParseLabelStrings invalid label format, expected 'name:value', got:%s", labelStr)
			continue
		}
		name := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])
		if name == "" || value == "" {
			logx.W(ctx, "ParseLabelStrings skip empty label, name:%s, value:%s", name, value)
			continue
		}
		result = append(result, &releaseEntity.ReleaseAttrLabel{
			Name:  name,
			Value: value,
		})
	}
	return result
}

// TransRetrievalLabel2AttrIDAndLabelID 将ReleaseAttrLabel数组转换为AttributeLabel数组
// 通过属性名和标签名查询对应的属性ID和标签ID
func (l *Logic) TransRetrievalLabel2AttrIDAndLabelID(ctx context.Context, robotID uint64, releaseLabels []*releaseEntity.ReleaseAttrLabel) ([]*entity.AttributeLabel, error) {
	// 构建属性名到标签值的映射
	attrKeyAndLabelNameMap := make(map[string][]string)
	for _, label := range releaseLabels {
		if label == nil || label.Name == "" || label.Value == "" {
			continue
		}
		if slices.Contains(labelEntity.SystemLabelAttrKeys, label.Name) {
			// 系统标签没有对应的属性ID和标签ID，忽略
			continue
		}
		if _, ok := attrKeyAndLabelNameMap[label.Name]; !ok {
			attrKeyAndLabelNameMap[label.Name] = make([]string, 0)
		}
		attrKeyAndLabelNameMap[label.Name] = append(attrKeyAndLabelNameMap[label.Name], label.Value)
	}
	attrKeys := maps.Keys(attrKeyAndLabelNameMap)
	attrMap, err := l.dao.GetAttributeByKeys(ctx, robotID, attrKeys)
	if err != nil {
		logx.E(ctx, "GetAttributeByKeys fail, robotID:%d, attrKeys:%+v, err:%v", robotID, attrKeys, err)
		return nil, err
	}
	allAttrLabelList := make([]*entity.AttributeLabel, 0)
	for attrKey, labelNames := range attrKeyAndLabelNameMap {
		attr, ok := attrMap[attrKey]
		if !ok {
			logx.W(ctx, "attrKey:%s not found", attrKey)
			continue
		}
		selectColumns := []string{entity.AttributeLabelTblColId, entity.AttributeLabelTblColBusinessId,
			entity.AttributeLabelTblColAttrId, entity.AttributeLabelTblColName}
		filter := &entity.AttributeLabelFilter{
			AttrId: attr.ID,
			Names:  labelNames,
		}
		attrLabelList, err := l.GetAttributeLabelList(ctx, selectColumns, filter)
		if err != nil {
			logx.E(ctx, "GetAttributeLabelList fail, robotID:%d, attrKey:%s, labelNames:%+v, err:%v",
				robotID, attrKey, labelNames, err)
			return nil, err
		}
		allAttrLabelList = append(allAttrLabelList, attrLabelList...)
	}
	return allAttrLabelList, nil
}
