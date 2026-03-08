package search

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/baicaoyuan/moss/types/slicex"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/common"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	utilConfig "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/util/config"
	admin "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_admin_config_server"
	pb_knowledge "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/knowledge"
	"golang.org/x/exp/maps"
)

const (
	CustomVariableKeyLkeUserId   = "lke_userid"
	CustomVariableKeyLkeDocBizID = "ADP_DOC_BIZ_ID"

	HeaderTokenKey = "X-Token"
)

// CheckThirdPermission 校验是否有权限
func CheckThirdPermission(ctx context.Context, reqBotBizId uint64,
	lkeUserID string, rspDocs []*pb.SearchKnowledgeRsp_SearchRsp_Doc) ([]*pb.SearchKnowledgeRsp_SearchRsp_Doc, error) {
	log.DebugContextf(ctx, "SearchKnowledge CheckThirdPermission reqBotBizId:%d lkeUserID:%s rspDocs:%+v",
		reqBotBizId, lkeUserID, rspDocs)
	if lkeUserID == "" || len(rspDocs) == 0 {
		return rspDocs, nil
	}

	// 按照共享知识库的路由信息来查询文档
	app, err := client.GetAppInfo(ctx, reqBotBizId, model.AppTestScenes)
	if err != nil {
		return nil, errs.ErrAppNotFound
	}
	replaceApp := app
	if newAppID, ok := utilConfig.GetMainConfig().SearchKnowledgeAppIdReplaceMap[app.GetAppBizId()]; ok {
		replaceApp, err = client.GetAppInfo(ctx, newAppID, model.AppTestScenes)
		if err != nil {
			return nil, errs.ErrAppNotFound
		}
		// iSearch项目的bot_biz_id替换，临时实现共享知识库功能
		log.DebugContextf(ctx, "SearchKnowledge CheckThirdPermission replace app:%+v replaceApp:%+v",
			app, replaceApp)
	}

	// 先通过数据库配置来校验权限
	rspDocs, err = CheckThirdPermissionFromDb(ctx, replaceApp, lkeUserID, rspDocs)
	if err != nil {
		log.ErrorContextf(ctx, "CheckThirdPermissionFromDb err:%+v", err)
		// iSearch迁移到数据库配置方式后再放开return err，不走后续的流程
		//return nil, err
	}
	// 再通过七彩石配置来校验权限，iSearch项目临时使用，后续通过数据库配置来实现，当前直接两次校验不做特殊处理
	rspDocs, err = CheckThirdPermissionFromConf(ctx, replaceApp, lkeUserID, rspDocs)
	if err != nil {
		return nil, err
	}
	return rspDocs, nil
}

// CheckThirdPermissionFromDb 通过数据中的第三方权限校验配置来校验权限
func CheckThirdPermissionFromDb(ctx context.Context, app *admin.GetAppInfoRsp, userId string,
	rspDocs []*pb.SearchKnowledgeRsp_SearchRsp_Doc) ([]*pb.SearchKnowledgeRsp_SearchRsp_Doc, error) {
	log.DebugContextf(ctx, "CheckThirdPermissionFromDb corpBizId:%d appBizId:%d userId:%s rspDocs:%+v corpId",
		app.GetCorpBizId(), app.GetAppBizId(), userId, rspDocs)
	// 从数据库读取第三方权限校验配置
	knowledgeBizIds := []uint64{app.GetAppBizId()}
	knowConfigs, err := dao.GetKnowledgeConfigDao(nil).GetKnowledgeConfigs(ctx, app.GetCorpBizId(),
		knowledgeBizIds, []uint32{uint32(pb_knowledge.KnowledgeBaseConfigType_THIRD_ACL)})
	if err != nil {
		log.ErrorContextf(ctx, "CheckThirdPermissionFromDb GetThirdAclConfig err:%v,appBizId:%+v", err, app.GetAppBizId())
		return rspDocs, errs.ErrGetThirdAclFail
	}
	log.DebugContextf(ctx, "CheckThirdPermissionFromDb knowConfigs:%+v", knowConfigs)
	if len(knowConfigs) == 0 || knowConfigs[0].Config == "" {
		return rspDocs, nil
	}
	thirdAclConfig := &model.ThirdAclConfig{}
	err = json.Unmarshal([]byte(knowConfigs[0].Config), &thirdAclConfig)
	if err != nil {
		log.ErrorContextf(ctx, "CheckThirdPermissionFromDb Unmarshal config err:%v,knowConfigs:%+v", err, knowConfigs)
		return rspDocs, errs.ErrGetThirdAclFail
	}

	header := make(map[string]string)
	thirdPermissionConfig, ok := utilConfig.GetMainConfig().ThirdPermissionCheck[app.GetAppBizId()]
	if ok {
		header = maps.Clone(thirdPermissionConfig.Header)
	}
	log.DebugContextf(ctx, "CheckThirdPermissionFromDb thirdAclConfig:%+v thirdPermissionConfig:%+v",
		thirdAclConfig, thirdPermissionConfig)
	header[HeaderTokenKey] = thirdAclConfig.ThirdToken
	newThirdPermissionConfig := utilConfig.ThirdPermissionConfig{
		Enable: true,
		Url:    thirdAclConfig.CheckPermissionsUrl,
		Header: header,
	}
	newRspDoc, err := CheckRspDocs(ctx, rspDocs, userId, newThirdPermissionConfig)
	if err != nil {
		return rspDocs, err
	}
	return newRspDoc, nil
}

// CheckThirdPermissionFromConf 通过七彩石配置中的第三方权限校验配置来校验权限，isearch项目临时使用，后续通过数据库配置来实现
func CheckThirdPermissionFromConf(ctx context.Context, app *admin.GetAppInfoRsp, userId string,
	rspDocs []*pb.SearchKnowledgeRsp_SearchRsp_Doc) (
	[]*pb.SearchKnowledgeRsp_SearchRsp_Doc, error) {
	log.DebugContextf(ctx, "CheckThirdPermissionFromConf userId:%s rspDocs:%+v", userId, rspDocs)
	thirdPermissionConfig, ok := utilConfig.GetMainConfig().ThirdPermissionCheck[app.GetAppBizId()]
	if !ok || !thirdPermissionConfig.Enable || len(rspDocs) == 0 {
		return rspDocs, nil
	}
	if userId == "" {
		log.WarnContextf(ctx, "CheckThirdPermissionFromConf lke_userid in custom variables is empty")
		return rspDocs, errs.ErrUserIdInCustomVariablesIsEmpty
	}

	newRspDoc, err := CheckRspDocs(ctx, rspDocs, userId, thirdPermissionConfig)
	if err != nil {
		return nil, err
	}
	return newRspDoc, nil
}

// CheckRspDocs 检查返回的文档是否有权限
func CheckRspDocs(ctx context.Context, rspDocs []*pb.SearchKnowledgeRsp_SearchRsp_Doc, userId string,
	thirdPermissionConfig utilConfig.ThirdPermissionConfig) ([]*pb.SearchKnowledgeRsp_SearchRsp_Doc, error) {
	log.DebugContextf(ctx, "CheckThirdPermission userId:%s rspDocs:%+v", userId, rspDocs)
	// 获取所有检索到的文档信息，需要先按文档id去重
	docIDMap := make(map[uint64]struct{})
	routerAppBizID := uint64(0)
	for _, doc := range rspDocs {
		if doc.GetDocType() == model.DocTypeSegment {
			// 只对检索出的文档鉴权
			docIDMap[doc.DocId] = struct{}{}
			routerAppBizID = doc.KnowledgeBizId
		}
	}
	if len(docIDMap) == 0 {
		return rspDocs, nil
	}

	docFilter := &dao.DocFilter{
		RouterAppBizID: routerAppBizID,
		IDs:            maps.Keys(docIDMap),
	}
	selectColumns := []string{dao.DocTblColId, dao.DocTblColCustomerKnowledgeId, dao.DocTblColAttributeFlag}
	docs, err := dao.GetDocDao().GetDocList(ctx, selectColumns, docFilter)
	if err != nil {
		return nil, err
	}
	if len(docs) != len(docFilter.IDs) {
		log.ErrorContextf(ctx, "CheckThirdPermission docs not found, len(docs):%d docIDs:%+v",
			len(docs), docFilter.IDs)
		return nil, errs.ErrDocNotFound
	}
	docInfos := make(map[uint64]*model.Doc)
	customerKnowledgeIds := make([]string, 0)
	customerKnowledgeId2DocID := make(map[string]uint64)
	for _, doc := range docs {
		docInfos[doc.ID] = doc
		if doc.CustomerKnowledgeId == "" || doc.HasAttributeFlag(model.DocAttributeFlagPublic) {
			// 公开文档，或者没有customerKnowledgeId的文档，不需要调用鉴权接口
			continue
		}
		customerKnowledgeIds = append(customerKnowledgeIds, doc.CustomerKnowledgeId)
		customerKnowledgeId2DocID[doc.CustomerKnowledgeId] = doc.ID
	}
	log.DebugContextf(ctx, "CheckThirdPermission docInfos: %+v", docInfos)
	if len(customerKnowledgeIds) == 0 {
		// 没有需要鉴权的文档，不需要调用鉴权接口
		return rspDocs, nil
	}
	// 只需要对非公开的文档调用鉴权
	// 需要进行第三方权限校验,返回的结果表示该用户有权限的文档结果
	thirdPermissionCheckReq := &client.CheckKnowledgePermissionReq{
		RequestId:            common.GetRequestID(ctx),
		CustomerKnowledgeIds: customerKnowledgeIds,
		CustomerUserId:       userId,
	}
	checkKnowledgePermissionRsp, err := client.CheckKnowledgePermission(ctx, thirdPermissionConfig, thirdPermissionCheckReq)
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
		if doc.GetDocType() == model.DocTypeSegment {
			// 只校验文档片段
			docInfo, ok := docInfos[doc.DocId]
			if !ok {
				// 如果数据库中没查到该文档，则去掉该文档
				log.WarnContextf(ctx, "CheckThirdPermission doc id: %d not found in db",
					doc.DocId)
				continue
			}
			if !docInfo.HasAttributeFlag(model.DocAttributeFlagPublic) {
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
func GetReferKnowledgeName(ctx context.Context, d dao.Dao, appBizID uint64, knowIDs []uint64) (
	knowInfoByID map[uint64]*model.SharedKnowledgeInfo, hasShare bool, err error) {
	//1.判断本应用是否引入了共享知识库
	shareInfo, err := dao.GetAppShareKGDao().ExistShareKG(ctx, appBizID)
	if err != nil {
		log.ErrorContextf(ctx, "GetReferKnowledgeName ExistShareKG err:%v,appBizID:%v", err, appBizID)
		return nil, false, err
	}
	if shareInfo == nil {
		return nil, false, nil
	}
	//有引用过共享知识库才返回知识库名称
	knowIDs = slicex.Unique(knowIDs)
	log.DebugContextf(ctx, "GetReferKnowledgeName knowIDS:%v", knowIDs)
	appID2AppBizIDMap, err := dao.GetAppBizIDsByAppIDs(ctx, knowIDs)
	if err != nil {
		log.ErrorContextf(ctx, "GetReferKnowledgeName GetAppBizIDs err:%+v,knowIDs:%v", err, knowIDs)
		return nil, false, err
	}
	log.DebugContextf(ctx, "GetReferKnowledgeName appID2AppBizIDMap:%v", appID2AppBizIDMap)
	if len(appID2AppBizIDMap) == 0 {
		return nil, false, nil
	}
	knowBizIDs, appBizID2IDMap := make([]uint64, 0, len(appID2AppBizIDMap)), make(map[uint64]uint64, len(appID2AppBizIDMap))
	for appID, appBizID := range appID2AppBizIDMap {
		knowBizIDs = append(knowBizIDs, appBizID)
		appBizID2IDMap[appBizID] = appID
	}
	knowList, err := d.RetrieveBaseSharedKnowledge(ctx, shareInfo.CorpBizID, knowBizIDs)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		log.ErrorContextf(ctx, "GetReferKnowledgeName RetrieveBaseSharedKnowledge err:%v,appBizID:%v,knowIDs:%v,appID2AppBizIDMap:%v",
			err, appBizID, knowIDs, appID2AppBizIDMap)
		return nil, false, err
	}
	knowInfoByID = make(map[uint64]*model.SharedKnowledgeInfo, len(knowList))
	for _, v := range knowList {
		if appID, ok := appBizID2IDMap[v.BusinessID]; ok {
			knowInfoByID[appID] = v
		}
	}
	return knowInfoByID, true, nil
}
