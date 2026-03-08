package search

import (
	"context"
	"encoding/json"
	"errors"

	"golang.org/x/exp/maps"
	"gorm.io/gorm"

	"git.woa.com/adp/common/x/errx"
	"git.woa.com/adp/common/x/logx"
	kbdao "git.woa.com/adp/kb/kb-config/internal/dao/kb"
	kbEntity "git.woa.com/adp/kb/kb-config/internal/entity/kb"

	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/kb/kb-config/internal/config"
	docDao "git.woa.com/adp/kb/kb-config/internal/dao/document"
	releaseDao "git.woa.com/adp/kb/kb-config/internal/dao/release"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	"git.woa.com/adp/kb/kb-config/internal/logic/localcache"
	"git.woa.com/adp/kb/kb-config/internal/rpc"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
	pb_knowledge "git.woa.com/adp/pb-go/kb/kb_config"
)

const (
	CustomVariableKeyLkeUserId   = "lke_userid"
	CustomVariableKeyLkeDocBizID = "ADP_DOC_BIZ_ID"

	HeaderTokenKey = "X-Token"
)

type Logic struct {
	docDao     docDao.Dao
	kbDao      kbdao.Dao
	releaseDao releaseDao.Dao
	cache      *localcache.Logic
}

func NewLogic(docDao docDao.Dao, kbDao kbdao.Dao, releaseDao releaseDao.Dao, cache *localcache.Logic) *Logic {
	return &Logic{
		docDao:     docDao,
		kbDao:      kbDao,
		releaseDao: releaseDao,
		cache:      cache,
	}
}

// CheckThirdPermission 校验是否有权限
func (l *Logic) CheckThirdPermission(ctx context.Context, app *entity.App,
	lkeUserID string, rspDocs []*pb.SearchKnowledgeRsp_SearchRsp_Doc) ([]*pb.SearchKnowledgeRsp_SearchRsp_Doc, error) {
	logx.D(ctx, "SearchKnowledge CheckThirdPermission app:%+v lkeUserID:%s rspDocs:%+v",
		app, lkeUserID, rspDocs)
	if lkeUserID == "" || len(rspDocs) == 0 {
		return rspDocs, nil
	}

	// 按照共享知识库的路由信息来查询文档
	// 先通过数据库配置来校验权限
	rspDocs, err := l.CheckThirdPermissionFromDb(ctx, app, lkeUserID, rspDocs)
	if err != nil {
		logx.E(ctx, "CheckThirdPermissionFromDb err:%+v", err)
		// iSearch迁移到数据库配置方式后再放开return err，不走后续的流程
		// return nil, err
	}
	// 再通过七彩石配置来校验权限，iSearch项目临时使用，后续通过数据库配置来实现，当前直接两次校验不做特殊处理
	rspDocs, err = l.CheckThirdPermissionFromConf(ctx, app, lkeUserID, rspDocs)
	if err != nil {
		return nil, err
	}
	return rspDocs, nil
}

// CheckThirdPermissionFromDb 通过数据中的第三方权限校验配置来校验权限
func (l *Logic) CheckThirdPermissionFromDb(ctx context.Context, appDB *entity.App, userId string,
	rspDocs []*pb.SearchKnowledgeRsp_SearchRsp_Doc) ([]*pb.SearchKnowledgeRsp_SearchRsp_Doc, error) {
	logx.D(ctx, "CheckThirdPermissionFromDb corpBizId:%d appBizId:%d userId:%s rspDocs:%+v",
		appDB.CorpBizId, appDB.BizId, userId, rspDocs)
	// 从数据库读取第三方权限校验配置
	knowledgeBizIds := []uint64{appDB.BizId}
	knowConfigs, err := l.kbDao.GetShareKnowledgeConfigs(ctx, appDB.CorpBizId,
		knowledgeBizIds, []uint32{uint32(pb_knowledge.KnowledgeBaseConfigType_THIRD_ACL)})
	if err != nil {
		logx.E(ctx, "CheckThirdPermissionFromDb GetThirdAclConfig err:%v,appBizId:%+v", err,
			appDB.BizId)
		return rspDocs, errs.ErrGetThirdAclFail
	}
	logx.D(ctx, "CheckThirdPermissionFromDb knowConfigs:%+v", knowConfigs)
	if len(knowConfigs) == 0 || knowConfigs[0].Config == "" {
		return rspDocs, nil
	}
	thirdAclConfig := &kbEntity.ThirdAclConfig{}
	err = json.Unmarshal([]byte(knowConfigs[0].Config), &thirdAclConfig)
	if err != nil {
		logx.E(ctx, "CheckThirdPermissionFromDb Unmarshal config err:%v,knowConfigs:%+v", err, knowConfigs)
		return rspDocs, errs.ErrGetThirdAclFail
	}

	header := make(map[string]string)
	thirdPermissionConfig, ok := config.GetMainConfig().ThirdPermissionCheck[appDB.BizId]
	if ok {
		header = maps.Clone(thirdPermissionConfig.Header)
	}
	logx.D(ctx, "CheckThirdPermissionFromDb thirdAclConfig:%+v thirdPermissionConfig:%+v",
		thirdAclConfig, thirdPermissionConfig)
	header[HeaderTokenKey] = thirdAclConfig.ThirdToken
	newThirdPermissionConfig := config.ThirdPermissionConfig{
		Enable: true,
		Url:    thirdAclConfig.CheckPermissionsUrl,
		Header: header,
	}
	newRspDoc, err := l.CheckRspDocs(ctx, rspDocs, userId, newThirdPermissionConfig)
	if err != nil {
		return rspDocs, err
	}
	return newRspDoc, nil
}

// CheckThirdPermissionFromConf 通过七彩石配置中的第三方权限校验配置来校验权限，isearch项目临时使用，后续通过数据库配置来实现
func (l *Logic) CheckThirdPermissionFromConf(ctx context.Context, appDB *entity.App, userId string,
	rspDocs []*pb.SearchKnowledgeRsp_SearchRsp_Doc) (
	[]*pb.SearchKnowledgeRsp_SearchRsp_Doc, error) {
	logx.D(ctx, "CheckThirdPermissionFromConf appBizId:%d userId:%s rspDocs:%+v",
		appDB.BizId, userId, rspDocs)
	thirdPermissionConfig, ok := config.GetMainConfig().ThirdPermissionCheck[appDB.BizId]
	if !ok || !thirdPermissionConfig.Enable || len(rspDocs) == 0 {
		logx.D(ctx, "CheckThirdPermissionFromConf ok:%+v thirdPermissionConfig.Enable:%v len(rspDocs):%d",
			ok, thirdPermissionConfig.Enable, len(rspDocs))
		return rspDocs, nil
	}
	if userId == "" {
		logx.W(ctx, "CheckThirdPermissionFromConf lke_userid in custom variables is empty")
		return rspDocs, errs.ErrUserIdInCustomVariablesIsEmpty
	}

	newRspDoc, err := l.CheckRspDocs(ctx, rspDocs, userId, thirdPermissionConfig)
	if err != nil {
		return nil, err
	}
	return newRspDoc, nil
}

// CheckRspDocs 检查返回的文档是否有权限
func (l *Logic) CheckRspDocs(ctx context.Context, rspDocs []*pb.SearchKnowledgeRsp_SearchRsp_Doc, userId string,
	thirdPermissionConfig config.ThirdPermissionConfig) ([]*pb.SearchKnowledgeRsp_SearchRsp_Doc, error) {
	logx.D(ctx, "CheckThirdPermission userId:%s rspDocs:%+v", userId, rspDocs)
	// 获取所有检索到的文档信息，需要先按文档id去重
	docIDMap := make(map[uint64]struct{})
	routerAppBizID := uint64(0)
	for _, doc := range rspDocs {
		if doc.GetDocType() == entity.DocTypeSegment {
			// 只对检索出的文档鉴权
			docIDMap[doc.DocId] = struct{}{}
			routerAppBizID = doc.KnowledgeBizId
		}
	}
	if len(docIDMap) == 0 {
		return rspDocs, nil
	}

	docFilter := &docEntity.DocFilter{
		RouterAppBizID: routerAppBizID,
		IDs:            maps.Keys(docIDMap),
	}
	selectColumns := []string{docEntity.DocTblColId, docEntity.DocTblColCustomerKnowledgeId,
		docEntity.DocTblColAttributeFlag}
	docs, err := l.docDao.GetDocList(ctx, selectColumns, docFilter)
	if err != nil {
		return nil, err
	}
	if len(docs) != len(docFilter.IDs) {
		logx.W(ctx, "CheckThirdPermission docs not found, len(docs):%d docIDs:%+v",
			len(docs), docFilter.IDs)
		return nil, errs.ErrDocNotFound
	}
	docInfos := make(map[uint64]*docEntity.Doc)
	customerKnowledgeIds := make([]string, 0)
	customerKnowledgeId2DocID := make(map[string]uint64)
	for _, doc := range docs {
		docInfos[doc.ID] = doc
		if doc.CustomerKnowledgeId == "" || doc.HasAttributeFlag(docEntity.DocAttributeFlagPublic) {
			// 公开文档，或者没有customerKnowledgeId的文档，不需要调用鉴权接口
			continue
		}
		customerKnowledgeIds = append(customerKnowledgeIds, doc.CustomerKnowledgeId)
		customerKnowledgeId2DocID[doc.CustomerKnowledgeId] = doc.ID
	}
	logx.D(ctx, "CheckThirdPermission docInfos: %+v", docInfos)
	if len(customerKnowledgeIds) == 0 {
		// 没有需要鉴权的文档，不需要调用鉴权接口
		return rspDocs, nil
	}
	// 只需要对非公开的文档调用鉴权
	// 需要进行第三方权限校验,返回的结果表示该用户有权限的文档结果
	thirdPermissionCheckReq := &rpc.CheckKnowledgePermissionReq{
		RequestId:            contextx.Metadata(ctx).RequestID(),
		CustomerKnowledgeIds: customerKnowledgeIds,
		CustomerUserId:       userId,
	}
	checkKnowledgePermissionRsp, err := rpc.CheckKnowledgePermission(ctx, thirdPermissionConfig,
		thirdPermissionCheckReq)
	if err != nil {
		return nil, err
	}
	hasPermissionDocIDs := make(map[uint64]struct{})
	for _, hasPermissionCustomerKnowledgeIds := range checkKnowledgePermissionRsp.CustomerKnowledgeIds {
		if docID, ok := customerKnowledgeId2DocID[hasPermissionCustomerKnowledgeIds]; ok {
			hasPermissionDocIDs[docID] = struct{}{}
		}
	}
	// 去掉该用户无权限的文档结果
	newRspDoc := make([]*pb.SearchKnowledgeRsp_SearchRsp_Doc, 0)
	for _, doc := range rspDocs {
		if doc.GetDocType() == entity.DocTypeSegment {
			// 只校验文档片段
			docInfo, ok := docInfos[doc.DocId]
			if !ok {
				// 如果数据库中没查到该文档，则去掉该文档
				logx.W(ctx, "CheckThirdPermission doc id: %d not found in db",
					doc.DocId)
				continue
			}
			if !docInfo.HasAttributeFlag(docEntity.DocAttributeFlagPublic) {
				// 如果该文档不包含公开属性，则需要进行第三方权限校验
				if _, ok := hasPermissionDocIDs[doc.DocId]; !ok {
					// 如果该文档不在t有权限的文档列表中，则去掉该文档
					continue
				}
			}
		}
		newRspDoc = append(newRspDoc, doc)
	}
	return newRspDoc, nil
}

// GetReferKnowledgeName 参考来源返回知识库名称
func (l *Logic) GetReferKnowledgeName(ctx context.Context, appBizID uint64, knowIDs []uint64) (
	knowInfoByID map[uint64]*kbEntity.SharedKnowledgeInfo, hasShare bool, err error) {
	// 1.判断本应用是否引入了共享知识库
	shareInfo, err := l.kbDao.ExistShareKG(ctx, appBizID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, false, nil
		}
		logx.E(ctx, "GetReferKnowledgeName ExistShareKG err:%v,appBizID:%v", err, appBizID)
		return nil, false, err
	}
	if shareInfo == nil {
		return nil, false, nil
	}
	// 有引用过共享知识库才返回知识库名称
	knowIDs = slicex.Unique(knowIDs)
	logx.D(ctx, "GetReferKnowledgeName knowIDS:%v", knowIDs)
	corpPrimaryId := contextx.Metadata(ctx).CorpID()
	appID2AppBizIDMap, err := l.cache.GetAppBizIdsByPrimaryIds(ctx, corpPrimaryId, knowIDs)
	if err != nil {
		logx.E(ctx, "GetReferKnowledgeName GetAppBizIDs err:%+v,knowIDs:%v", err, knowIDs)
		return nil, false, err
	}
	logx.D(ctx, "GetReferKnowledgeName appID2AppBizIDMap:%v", appID2AppBizIDMap)
	if len(appID2AppBizIDMap) == 0 {
		return nil, false, nil
	}
	knowBizIDs, appBizID2IDMap := make([]uint64, 0, len(appID2AppBizIDMap)), make(map[uint64]uint64,
		len(appID2AppBizIDMap))
	for appID, appBizID := range appID2AppBizIDMap {
		knowBizIDs = append(knowBizIDs, appBizID)
		appBizID2IDMap[appBizID] = appID
	}
	shareKnowledgeFilter := kbEntity.ShareKnowledgeFilter{
		CorpBizID: shareInfo.CorpBizID,
		BizIds:    knowBizIDs,
	}
	knowList, err := l.kbDao.RetrieveBaseSharedKnowledge(ctx, &shareKnowledgeFilter)
	if err != nil && !errx.IsNotFound(err) {
		logx.E(ctx, "GetReferKnowledgeName RetrieveBaseSharedKnowledge err:%v,appBizID:%v,knowIDs:%v,appID2AppBizIDMap:%v", err, appBizID, knowIDs, appID2AppBizIDMap)
		return nil, false, err
	}
	knowInfoByID = make(map[uint64]*kbEntity.SharedKnowledgeInfo, len(knowList))
	for _, v := range knowList {
		if appID, ok := appBizID2IDMap[v.BusinessID]; ok {
			knowInfoByID[appID] = v
		}
	}
	return knowInfoByID, true, nil
}
