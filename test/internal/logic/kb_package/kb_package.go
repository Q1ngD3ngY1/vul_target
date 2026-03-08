package kb_package

import (
	"context"
	"fmt"
	"hash/crc64"
	"io"
	"os"
	"strconv"

	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/dao"
	"git.woa.com/adp/kb/kb-config/internal/dao/kb"
	"git.woa.com/adp/kb/kb-config/internal/dao/label"
	docLogic "git.woa.com/adp/kb/kb-config/internal/logic/document"
	kbLogic "git.woa.com/adp/kb/kb-config/internal/logic/kb"
	cacheLogic "git.woa.com/adp/kb/kb-config/internal/logic/localcache"
	"git.woa.com/adp/kb/kb-config/internal/rpc"
	"git.woa.com/adp/kb/kb-config/internal/util/idgen"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	"git.woa.com/adp/pb-go/common"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
	"github.com/spf13/cast"
)

type Logic struct {
	rpc        *rpc.RPC
	kbDao      kb.Dao
	labelDao   label.Dao
	docLogic   *docLogic.Logic
	s3         dao.S3
	cacheLogic *cacheLogic.Logic
}

func NewLogic(rpc *rpc.RPC, kbDao kb.Dao, labelDao label.Dao, docLogic *docLogic.Logic, s3 dao.S3, cacheLogic *cacheLogic.Logic) *Logic {
	return &Logic{
		rpc:        rpc,
		kbDao:      kbDao,
		labelDao:   labelDao,
		docLogic:   docLogic,
		s3:         s3,
		cacheLogic: cacheLogic,
	}
}

var ecmaTable = crc64.MakeTable(crc64.ECMA)

// ConvertBizIdLogic 转换业务ID
func (l *Logic) ConvertBizIdLogic(ctx context.Context, req *pb.ConvertKBIdReq) (*pb.ConvertKBIdRsp, error) {
	if len(req.GetIdMap()) == 0 {
		return nil, errs.ErrConvertBizIdParamsEmpty
	}
	rsp := &pb.ConvertKBIdRsp{
		IdMap: make([]*common.IdMap, 0, len(req.IdMap)),
	}

	// 第一步：先处理 IdType_IdTypeKb 类型，检查是否有同环境下的共享知识库
	// 记录共享知识库的ID映射关系，用于后续其他类型的处理
	sharedKbIds := make(map[string]uint64) // 记录哪些知识库ID是同环境下的共享知识库
	for _, idMap := range req.IdMap {
		if idMap.IdType == common.IdType_IdTypeKb {
			resultMap, sharedIds, err := l.processKnowledgeBaseType(ctx, idMap, req)
			if err != nil {
				logx.WarnContextf(ctx, "process IdType_IdTypeKb failed: err=%v", err)
				continue
			}
			// 记录共享知识库的ID
			for kbId, appPrimaryID := range sharedIds {
				sharedKbIds[kbId] = appPrimaryID
			}
			rsp.IdMap = append(rsp.IdMap, resultMap)
			logx.I(ctx, "processed IdType_IdTypeKb, sharedKbIds: %v", sharedKbIds)
			break // 只处理第一个 IdType_IdTypeKb
		}
	}

	// 第二步：处理其他类型的IdMap，根据共享知识库标识决定是否保持原ID
	for _, idMap := range req.IdMap {
		if idMap.IdType == common.IdType_IdTypeKb {
			// 已经处理过了，跳过
			continue
		}
		resultMap, err := l.processIdMap(ctx, idMap, req, sharedKbIds)
		if err != nil {
			logx.WarnContextf(ctx, "process idMap no need process : type=%d, err=%v", idMap.IdType, err)
			continue
		}
		rsp.IdMap = append(rsp.IdMap, resultMap)
	}

	return rsp, nil
}

// 处理单个IdMap（除了IdType_IdTypeKb类型）
func (l *Logic) processIdMap(ctx context.Context, idMap *common.IdMap,
	req *pb.ConvertKBIdReq, sharedKbIds map[string]uint64) (*common.IdMap, error) {
	resultMap := &common.IdMap{
		IdType: idMap.IdType,
		Ids:    make(map[string]string, len(idMap.Ids)),
	}
	// 处理IdMap中的每个键值对
	for oldId, _ := range idMap.Ids {
		switch idMap.IdType {
		case common.IdType_IdTypeKbQaCategory, common.IdType_IdTypeKbDocCategory, common.IdType_IdTypeKbSchemaCluster:
			// 这些不存在被外部依赖，直接生成新的ID
			resultMap.Ids[oldId] = strconv.FormatUint(idgen.GetId(), 10)
		case common.IdType_IdTypeKbLabel: // 存在外部依赖，需要看是否有同环境下的共享知识库，有的话，需要保持原id
			if len(sharedKbIds) > 0 {
				// 处理标签，遍历共享知识库，看该标签是否存在，如果存在需要保持原id
				newId, err := l.processLabelType(ctx, oldId, sharedKbIds)
				if err != nil {
					logx.WarnContextf(ctx, "processLabelType failed: oldId=%s, err=%v", oldId, err)
					// 处理失败时生成新ID
					resultMap.Ids[oldId] = strconv.FormatUint(idgen.GetId(), 10)
				} else {
					resultMap.Ids[oldId] = newId
				}
			} else {
				resultMap.Ids[oldId] = strconv.FormatUint(idgen.GetId(), 10)
			}
		case common.IdType_IdTypeKbLabelValue:
			if len(sharedKbIds) > 0 {
				// 处理标签值，遍历共享知识库，看该标签是否存在，如果存在需要保持原id
				newId, err := l.processLabelValueType(ctx, oldId, sharedKbIds)
				if err != nil {
					logx.WarnContextf(ctx, "processLabelValueType failed: oldId=%s, err=%v", oldId, err)
					// 处理失败时生成新ID
					resultMap.Ids[oldId] = strconv.FormatUint(idgen.GetId(), 10)
				} else {
					resultMap.Ids[oldId] = newId
				}
			} else {
				resultMap.Ids[oldId] = strconv.FormatUint(idgen.GetId(), 10)
			}
		case common.IdType_IdTypeKbDoc: // 存在外部依赖，需要看是否有同环境下的共享知识库，有的话，需要保持原id
			if len(sharedKbIds) > 0 {
				// 调用新函数处理文档
				newId, err := l.processDocType(ctx, oldId, sharedKbIds)
				if err != nil {
					logx.WarnContextf(ctx, "processDocType failed: oldId=%s, err=%v", oldId, err)
					// 处理失败时生成新ID
					resultMap.Ids[oldId] = strconv.FormatUint(idgen.GetId(), 10)
				} else {
					resultMap.Ids[oldId] = newId
				}
			} else {
				resultMap.Ids[oldId] = strconv.FormatUint(idgen.GetId(), 10)
			}
		default:
			logx.WarnContextf(ctx, "unknown id type: %d", idMap.IdType)
			return nil, errs.ErrConvertBizIdUnknownIdType
		}
	}
	return resultMap, nil
}

// processDocType 处理文档类型，检查文档是否在共享知识库中存在
// 如果文档在任一共享知识库中存在，则保持原ID；否则生成新ID
func (l *Logic) processDocType(ctx context.Context, docBizIdStr string, sharedKbIds map[string]uint64) (string, error) {
	docBizId := cast.ToUint64(docBizIdStr)
	if docBizId == 0 {
		return "", fmt.Errorf("invalid doc biz id: %s", docBizIdStr)
	}

	// 遍历所有共享知识库，检查该文档是否存在
	for kbIdStr, appPrimaryID := range sharedKbIds {
		kbId := cast.ToUint64(kbIdStr)
		if kbId == 0 {
			logx.WarnContextf(ctx, "invalid kb id: %s", kbIdStr)
			continue
		}

		// 调用docLogic.GetDocByBizID()判断文档是否存在于该知识库中
		doc, err := l.docLogic.GetDocByBizID(ctx, docBizId, appPrimaryID)
		if err != nil {
			logx.WarnContextf(ctx, "GetDocByBizID failed: appPrimaryID=%d, docBizId=%d, err=%v", appPrimaryID, docBizId, err)
			continue
		}

		// 如果在该共享知识库中找到了该文档，保持原ID
		if doc != nil && doc.RobotID == appPrimaryID {
			logx.I(ctx, "found doc in shared knowledge base: docBizId=%s, kbId=%d, keep original id", docBizIdStr, kbId)
			return docBizIdStr, nil
		}
	}

	// 文档在所有共享知识库中都不存在，生成新ID
	newId := strconv.FormatUint(idgen.GetId(), 10)
	logx.I(ctx, "doc not found in any shared knowledge base: oldDocId=%s, newDocId=%s", docBizIdStr, newId)
	return newId, nil
}

// processLabelValueType 处理标签值类型，检查标签值是否在共享知识库中存在
// 如果标签值在任一共享知识库中存在，则保持原ID；否则生成新ID
func (l *Logic) processLabelValueType(ctx context.Context, labelValueBizIdStr string, sharedKbIds map[string]uint64) (string, error) {
	labelValueBizId := cast.ToUint64(labelValueBizIdStr)
	if labelValueBizId == 0 {
		return "", fmt.Errorf("invalid label value biz id: %s", labelValueBizIdStr)
	}

	// 遍历所有共享知识库，检查该标签值是否存在
	for kbIdStr, appPrimaryID := range sharedKbIds {
		kbId := cast.ToUint64(kbIdStr)
		if kbId == 0 {
			logx.WarnContextf(ctx, "invalid kb id: %s", kbIdStr)
			continue
		}

		// 调用labelDao.GetAttributeLabelByBizIDs()判断标签值是否存在于该知识库中
		attrLabelMap, err := l.labelDao.GetAttributeLabelByBizIDs(ctx, []uint64{labelValueBizId}, appPrimaryID)
		if err != nil {
			logx.WarnContextf(ctx, "GetAttributeLabelByBizIDs failed: appPrimaryID=%d, labelValueBizId=%d, err=%v", appPrimaryID, labelValueBizId, err)
			continue
		}

		// 如果在该共享知识库中找到了该标签值，保持原ID
		if attrLabel, exists := attrLabelMap[labelValueBizId]; exists && attrLabel != nil {
			logx.I(ctx, "found label value in shared knowledge base: labelValueBizId=%s, kbId=%d, keep original id", labelValueBizId, kbId)
			return labelValueBizIdStr, nil
		}
	}

	// 标签值在所有共享知识库中都不存在，生成新ID
	newId := strconv.FormatUint(idgen.GetId(), 10)
	logx.I(ctx, "label value not found in any shared knowledge base: oldLabelValueId=%s, newLabelValueId=%s", labelValueBizId, newId)
	return newId, nil
}

// processLabelType 处理标签类型，检查标签是否在共享知识库中存在
// 如果标签在任一共享知识库中存在，则保持原ID；否则生成新ID
func (l *Logic) processLabelType(ctx context.Context, labelBizIdStr string, sharedKbIds map[string]uint64) (string, error) {
	labelBizId := cast.ToUint64(labelBizIdStr)
	if labelBizId == 0 {
		return "", fmt.Errorf("invalid label biz id: %s", labelBizIdStr)
	}

	// 遍历所有共享知识库，检查该标签是否存在
	for kbIdStr, appPrimaryID := range sharedKbIds {
		kbId := cast.ToUint64(kbIdStr)
		if kbId == 0 {
			logx.WarnContextf(ctx, "invalid kb id: %s", kbIdStr)
			continue
		}

		// 调用labelDao.GetAttributeByBizIDs()判断标签是否存在于该知识库中
		attrMap, err := l.labelDao.GetAttributeByBizIDs(ctx, appPrimaryID, []uint64{labelBizId})
		if err != nil {
			logx.WarnContextf(ctx, "GetAttributeByBizIDs failed: appPrimaryID=%d, labelBizId=%d, err=%v", appPrimaryID, labelBizId, err)
			continue
		}

		// 如果在该共享知识库中找到了该标签，保持原ID
		if attr, exists := attrMap[labelBizId]; exists && attr != nil {
			logx.I(ctx, "found label in shared knowledge base: labelBizId=%s, kbId=%d, keep original id", labelBizIdStr, kbId)
			return labelBizIdStr, nil
		}
	}

	// 标签在所有共享知识库中都不存在，生成新ID
	newId := strconv.FormatUint(idgen.GetId(), 10)
	logx.I(ctx, "label not found in any shared knowledge base: oldLabelId=%s, newLabelId=%s", labelBizIdStr, newId)
	return newId, nil
}

// 处理知识库类型（IdType_IdTypeKb），返回结果映射和共享知识库ID集合
func (l *Logic) processKnowledgeBaseType(ctx context.Context, idMap *common.IdMap,
	req *pb.ConvertKBIdReq) (*common.IdMap, map[string]uint64, error) {
	resultMap := &common.IdMap{
		IdType: idMap.IdType,
		Ids:    make(map[string]string, len(idMap.Ids)),
	}
	sharedKbIds := make(map[string]uint64) // 记录哪些是同环境下的共享知识库

	// 收集所有知识库ID，准备批量查询
	kbIds := make([]uint64, 0, len(idMap.Ids))
	//oldIdToKbId := make(map[string]uint64) // oldId -> kbId 的映射
	for oldId := range idMap.Ids {
		kbId := cast.ToUint64(oldId)
		kbIds = append(kbIds, kbId)
		//oldIdToKbId[oldId] = kbId
	}

	// 一次性批量查询所有知识库是否为同环境下的共享知识库
	shareKnowledgeInfos, err := l.kbDao.ListBaseSharedKnowledge(ctx, req.GetCorpBizId(),
		kbIds, 1, 100, "", req.GetSpaceId())
	if err != nil {
		logx.ErrorContextf(ctx, "ListBaseSharedKnowledge failed: corpBizId=%d, kbIds=%v, spaceId=%s, err=%v",
			req.GetCorpBizId(), kbIds, req.GetSpaceId(), err)
		return nil, nil, err
	}

	// 构建同环境下的共享知识库ID集合，方便快速查找
	sharedKbIdSet := make(map[string]uint64) // kbId -> appPrimaryID
	for _, info := range shareKnowledgeInfos {
		appPrimaryID, _ := l.cacheLogic.GetAppPrimaryIdByBizId(ctx, info.BusinessID)
		sharedKbIdSet[cast.ToString(info.BusinessID)] = appPrimaryID
	}
	logx.I(ctx, "found %d shared knowledge bases: %v", len(sharedKbIdSet), sharedKbIdSet)

	// 处理每个知识库ID
	for oldId, _ := range idMap.Ids {
		if appPrimaryID, ok := sharedKbIdSet[oldId]; ok {
			// 存在共享知识库，保持原ID，并记录到sharedKbIds中
			resultMap.Ids[oldId] = oldId
			sharedKbIds[oldId] = appPrimaryID
			logx.I(ctx, "found shared knowledge base: kbId=%s, keep original id", oldId)
		} else {
			// 不是共享知识库，需要生成新ID
			// 检查是否为默认知识库（在 IdType_IdTypeApp 中存在相同的 oldId）
			foundInAppMap := false
			for _, appIdMap := range req.IdMap {
				if appIdMap.IdType == common.IdType_IdTypeApp {
					if _, exists := appIdMap.Ids[oldId]; exists {
						foundInAppMap = true
						break
					}
				}
			}

			if foundInAppMap {
				// 为默认知识库，使用新传入的应用ID
				resultMap.Ids[oldId] = fmt.Sprintf("%d", req.GetNewAppBizId())
				logx.I(ctx, "default knowledge base: kbId=%s, use new app id=%d", oldId, req.GetNewAppBizId())
			} else {
				// 创建新的共享知识库
				uin := contextx.Metadata(ctx).Uin()
				newKbBizId, err := l.rpc.AppAdmin.CreateShareKnowledgeBaseApp(ctx, uin,
					oldId+"_need_replace_name", kbLogic.SharedKnowledgeAppAvatar, req.GetSpaceId())
				if err != nil {
					logx.ErrorContextf(ctx, "CreateShareKnowledgeBaseApp failed: uin=%s, err=%v", uin, err)
					return nil, nil, err
				}
				resultMap.Ids[oldId] = fmt.Sprintf("%d", newKbBizId.GetAppBizId())
				logx.I(ctx, "created new shared knowledge base: oldKbId=%s, newKbId=%d", oldId, newKbBizId.GetAppBizId())
			}
		}
	}

	return resultMap, sharedKbIds, nil
}

// CalculateFileCRC64 计算文件的CRC64哈希值（带密钥）
// 使用ECMA-182标准（ISO 3309），与腾讯云COS保持一致
// 通过添加密钥参与哈希计算，防止哈希值被随意破解或伪造
// 返回16位大写十六进制字符串，例如：A1B2C3D4E5F6A7B8
func (l *Logic) CalculateFileCRC64(ctx context.Context, filePath string, secret string) (string, error) {
	logx.I(ctx, "calculateFileCRC64 start, filePath: %s", filePath)

	// 打开文件
	file, err := os.Open(filePath)
	if err != nil {
		logx.E(ctx, "calculateFileCRC64 open file failed, filePath: %s, err: %v", filePath, err)
		return "", fmt.Errorf("open file failed: %w", err)
	}
	defer file.Close()

	// 创建CRC64哈希对象，使用ECMA多项式（最常用的CRC64标准）
	hash := crc64.New(ecmaTable)

	// 先写入秘钥，增强哈希安全性，防止被随意破解
	if secret != "" {
		hash.Write([]byte(secret))
	}

	// 读取文件内容并计算哈希
	if _, err := io.Copy(hash, file); err != nil {
		logx.E(ctx, "calculateFileCRC64 read file failed, filePath: %s, err: %v", filePath, err)
		return "", fmt.Errorf("read file failed: %w", err)
	}

	// 获取CRC64值（使用ECMA-182标准，与腾讯云COS一致）
	crc64Value := hash.Sum64()
	// 转换为16位十六进制字符串（大写，与腾讯云COS格式一致）
	crc64Hex := fmt.Sprintf("%016X", crc64Value)
	logx.I(ctx, "calculateFileCRC64 done, filePath: %s, crc64: %s (uint64: %d, standard: ECMA-182, with secret)", filePath, crc64Hex, crc64Value)
	return crc64Hex, nil
}
