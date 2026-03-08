package dao

import (
	"context"
	"strings"

	jsoniter "github.com/json-iterator/go"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
)

// ExportSynonymsTask 导出 同义词 任务定义
type ExportSynonymsTask struct {
	Dao Dao
}

// GetExportTotal 获取导出 同义词 总数
func (e ExportSynonymsTask) GetExportTotal(ctx context.Context, corpID, robotID uint64, params string) (uint64, error) {
	return 0, nil
}

// GetExportData 分页获取数据
func (e ExportSynonymsTask) GetExportData(ctx context.Context, corpID, robotID uint64, params string,
	page, pageSize uint32) (
	[][]string, error) {
	req := &pb.ExportSynonymsListReq{}
	if err := jsoniter.UnmarshalFromString(params, req); err != nil {
		log.ErrorContextf(ctx, "任务参数解析失败 req:%+v err:%+v", req, err)
		return nil, err
	}
	var err error
	synonymsList := make([]*model.SynonymsItem, 0)
	if len(req.GetSynonymsBizIds()) > 0 {
		synBizIDs, err := util.CheckReqSliceUint64(ctx, req.GetSynonymsBizIds())
		synonymsListReq := &model.SynonymsListReq{
			RobotID:     robotID,
			CorpID:      corpID,
			IsDeleted:   model.SynonymIsNotDeleted,
			Page:        page,
			PageSize:    pageSize,
			SynonymsIDs: synBizIDs,
		}
		log.DebugContextf(ctx, "根据BizIds导出同义词, synListReq: %+v", synonymsListReq)
		synonymsListRsp, err := e.Dao.GetSynonymsList(ctx, synonymsListReq)
		if err != nil {
			log.ErrorContextf(ctx, "根据BizIDs导出同义词失败, 获取同义词失败, err:%+v", err)
			return nil, err
		}
		synonymsList = append(synonymsList, synonymsListRsp.Synonyms...)
	} else {
		listReq := req.GetFilters()
		listReq.PageSize = pageSize
		listReq.PageNumber = page
		log.DebugContextf(ctx, "根据筛选器导出同义词, synListReq: %+v", listReq)
		synonymsListReq, err := e.Dao.GetSynonymsListReq(ctx, listReq, robotID, corpID)
		if err != nil {
			return nil, err
		}
		synonymsListRsp, err := e.Dao.GetSynonymsList(ctx, synonymsListReq)
		if err != nil {
			log.ErrorContextf(ctx, "根据筛选器导出同义词失败(GetSynonymsList), 获取同义词失败, err:%+v", err)
			return nil, err
		}
		synonymsList = append(synonymsList, synonymsListRsp.Synonyms...)
	}
	categories, err := e.Dao.GetCateList(ctx, model.SynonymsCate, corpID, robotID)
	if err != nil {
		return nil, err
	}
	tree := model.BuildCateTree(categories)
	rows := e.getSynonymsRows(ctx, synonymsList, tree)
	return rows, nil
}

// getSynonymsRows 获取同义词列表
func (e ExportSynonymsTask) getSynonymsRows(ctx context.Context, synonyms []*model.SynonymsItem,
	tree *model.CateNode) [][]string {
	var rows [][]string
	for _, word := range synonyms {
		cateTree := tree.Path(ctx, word.CateID)
		head := make([]string, 0, len(cateTree))
		head = append(head, cateTree...)
		headLen := len(cateTree)
		if headLen < model.ExcelTplCateLen {
			start := headLen + 1
			for i := start; i <= model.ExcelTplCateLen; i++ {
				head = append(head, "")
			}
		}
		rows = append(rows, append(head, word.StandardWord,
			strings.Join(word.Synonyms, "\n")))
	}
	return rows
}

// GetExportHeader 获取 同义词 导出表头信息
func (e ExportSynonymsTask) GetExportHeader(ctx context.Context) []string {
	var headers []string
	for _, v := range model.SynonymsExcelTplHead {
		headers = append(headers, i18n.Translate(ctx, v))
	}
	return headers
}
