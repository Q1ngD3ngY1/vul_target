package export

import (
	"context"
	"strings"

	"git.woa.com/adp/common/x/encodingx/jsonx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/dao/label"
	"git.woa.com/adp/kb/kb-config/internal/util"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
)

type AttributeLabelExportLogic struct {
	labelDao label.Dao
}

func NewAttributeLabelExportLogic(labelDao label.Dao) *AttributeLabelExportLogic {
	return &AttributeLabelExportLogic{
		labelDao: labelDao,
	}
}

func (e AttributeLabelExportLogic) GetExportData(ctx context.Context, corpID, robotID uint64, params string, page,
	pageSize uint32) ([][]string, error) {
	req := &pb.ExportAttributeLabelReq{}
	if err := jsonx.UnmarshalFromString(params, req); err != nil {
		logx.E(ctx, "任务参数解析失败 corpID:%d,robotID:%d,params:%s,err:%+v", corpID, robotID,
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
		attrs, err := e.labelDao.GetAttributeByBizIDs(ctx, robotID, attributeBizIDs)
		if err != nil {
			return nil, err
		}
		for _, v := range attrs {
			ids = append(ids, v.ID)
		}
		// todo ethangguo use logic
		list, err := e.labelDao.GetAttributeList(ctx, robotID, req.GetFilters().GetQuery(), page, pageSize, ids)
		if err != nil {
			logx.E(ctx, "export get attribute label list corpID:%d,robotID:%d,params:%s,err:%+v",
				corpID, robotID, params, err)
			return nil, err
		}
		attrIDs := make([]uint64, 0)
		for _, v := range list {
			attrIDs = append(attrIDs, v.ID)
		}
		mapAttrID2Labels, err := e.labelDao.GetAttributeLabelByAttrIDs(ctx, attrIDs, robotID)
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
	list, err := e.labelDao.GetAttributeList(ctx, robotID, req.GetFilters().GetQuery(), page, pageSize, ids)
	if err != nil {
		logx.E(ctx, "export get attribute label list corpID:%d,robotID:%d,params:%s,err:%+v",
			corpID, robotID, params, err)
		return nil, err
	}
	attrIDs := make([]uint64, 0)
	for _, v := range list {
		attrIDs = append(attrIDs, v.ID)
	}
	mapAttrID2Labels, err := e.labelDao.GetAttributeLabelByAttrIDs(ctx, attrIDs, robotID)
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

func getSimilarLabels(similarLabelStr string) (string, error) {
	if len(similarLabelStr) == 0 {
		return "", nil
	}
	var similarLabels []string
	if err := jsonx.UnmarshalFromString(similarLabelStr, &similarLabels); err != nil {
		return "", err
	}
	return strings.Join(similarLabels, ","), nil
}

// GetExportHeader 获取 QA 导出表头信息
func (e AttributeLabelExportLogic) GetExportHeader(ctx context.Context) []string {
	heads := make([]string, 0, len(config.App().AttributeLabel.ExeclHead))
	for _, v := range config.App().AttributeLabel.ExeclHead {
		heads = append(heads, i18n.Translate(ctx, v))
	}
	return heads
}
