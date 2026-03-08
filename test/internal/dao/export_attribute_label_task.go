package dao

import (
	"context"
	"strings"

	jsoniter "github.com/json-iterator/go"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
)

// ExportAttributeLabelTask 导出属性标签任务定义
type ExportAttributeLabelTask struct {
	Dao Dao
}

// GetExportTotal 获取导出数据总数
func (eklt *ExportAttributeLabelTask) GetExportTotal(ctx context.Context, corpID, robotID uint64, params string) (
	uint64, error) {
	req := &pb.ExportAttributeLabelReq{}
	if err := jsoniter.UnmarshalFromString(params, req); err != nil {
		log.ErrorContextf(ctx, "任务参数解析失败 corpID:%d,robotID:%d,params:%s,err:%+v", corpID, robotID,
			params, err)
		return 0, err
	}
	// API兼容逻辑，后续上云需删除if
	if len(req.GetAttributeBizIds()) > 0 {
		var ids []uint64
		attributeBizIDs, err := util.CheckReqSliceUint64(ctx, req.GetAttributeBizIds())
		if err != nil {
			return 0, err
		}
		attrs, err := eklt.Dao.GetAttributeByBizIDs(ctx, robotID, attributeBizIDs)
		if err != nil {
			return 0, err
		}
		for _, v := range attrs {
			ids = append(ids, v.ID)
		}
		total, err := eklt.Dao.GetAttributeTotal(ctx, robotID, req.GetFilters().GetQuery(), ids)
		if err != nil {
			log.ErrorContextf(ctx, "export get attribute label total corpID:%d,robotID:%d,params:%s,err:%+v",
				corpID, robotID, params, err)
			return 0, err
		}
		return total, nil
	}
	ids, err := util.CheckReqSliceUint64(ctx, req.GetIds())
	if err != nil {
		return 0, err
	}
	total, err := eklt.Dao.GetAttributeTotal(ctx, robotID, req.GetFilters().GetQuery(), ids)
	if err != nil {
		log.ErrorContextf(ctx, "export get attribute label total corpID:%d,robotID:%d,params:%s,err:%+v",
			corpID, robotID, params, err)
		return 0, err
	}
	return total, nil
}

// GetExportData 分页获取导出数据
func (eklt *ExportAttributeLabelTask) GetExportData(ctx context.Context, corpID, robotID uint64, params string,
	page, pageSize uint32) ([][]string, error) {
	req := &pb.ExportAttributeLabelReq{}
	if err := jsoniter.UnmarshalFromString(params, req); err != nil {
		log.ErrorContextf(ctx, "任务参数解析失败 corpID:%d,robotID:%d,params:%s,err:%+v", corpID, robotID,
			params, err)
		return nil, err
	}
	// API兼容逻辑，后续上云需删除if
	if len(req.GetAttributeBizIds()) > 0 {
		var ids []uint64
		attributeBizIDs, err := util.CheckReqSliceUint64(ctx, req.GetAttributeBizIds())
		if err != nil {
			return nil, err
		}
		attrs, err := eklt.Dao.GetAttributeByBizIDs(ctx, robotID, attributeBizIDs)
		if err != nil {
			return nil, err
		}
		for _, v := range attrs {
			ids = append(ids, v.ID)
		}
		list, err := eklt.Dao.GetAttributeList(ctx, robotID, req.GetFilters().GetQuery(), page, pageSize, ids)
		if err != nil {
			log.ErrorContextf(ctx, "export get attribute label list corpID:%d,robotID:%d,params:%s,err:%+v",
				corpID, robotID, params, err)
			return nil, err
		}
		attrIDs := make([]uint64, 0)
		for _, v := range list {
			attrIDs = append(attrIDs, v.ID)
		}
		mapAttrID2Labels, err := eklt.Dao.GetAttributeLabelByAttrIDs(ctx, attrIDs, robotID)
		if err != nil {
			return nil, err
		}
		rows := make([][]string, 0)
		for _, v := range list {
			labels, ok := mapAttrID2Labels[v.ID]
			if !ok {
				rows = append(rows, []string{v.Name, "", ""})
				continue
			}
			for _, label := range labels {
				similarLabel, err := getSimilarLabels(label.SimilarLabel)
				if err != nil {
					return nil, err
				}
				rows = append(rows, []string{v.Name, label.Name, similarLabel})
			}
		}
		return rows, nil
	}
	ids, err := util.CheckReqSliceUint64(ctx, req.GetIds())
	if err != nil {
		return nil, err
	}
	list, err := eklt.Dao.GetAttributeList(ctx, robotID, req.GetFilters().GetQuery(), page, pageSize, ids)
	if err != nil {
		log.ErrorContextf(ctx, "export get attribute label list corpID:%d,robotID:%d,params:%s,err:%+v",
			corpID, robotID, params, err)
		return nil, err
	}
	attrIDs := make([]uint64, 0)
	for _, v := range list {
		attrIDs = append(attrIDs, v.ID)
	}
	mapAttrID2Labels, err := eklt.Dao.GetAttributeLabelByAttrIDs(ctx, attrIDs, robotID)
	if err != nil {
		return nil, err
	}
	rows := make([][]string, 0)
	for _, v := range list {
		labels, ok := mapAttrID2Labels[v.ID]
		if !ok {
			rows = append(rows, []string{v.Name, "", ""})
			continue
		}
		for _, label := range labels {
			similarLabel, err := getSimilarLabels(label.SimilarLabel)
			if err != nil {
				return nil, err
			}
			rows = append(rows, []string{v.Name, label.Name, similarLabel})
		}
	}
	return rows, nil
}

// getSimilarLabels TODO
func getSimilarLabels(similarLabelStr string) (string, error) {
	if len(similarLabelStr) == 0 {
		return "", nil
	}
	var similarLabels []string
	if err := jsoniter.UnmarshalFromString(similarLabelStr, &similarLabels); err != nil {
		return "", err
	}
	return strings.Join(similarLabels, ","), nil
}

// GetExportHeader 获取导出数据表头
func (eklt *ExportAttributeLabelTask) GetExportHeader(ctx context.Context) []string {
	heads := make([]string, 0, len(config.App().AttributeLabel.ExeclHead))
	for _, v := range config.App().AttributeLabel.ExeclHead {
		heads = append(heads, i18n.Translate(ctx, v))
	}
	return heads
}
