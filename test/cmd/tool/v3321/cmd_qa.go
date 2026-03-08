package main

import (
	"context"
	"os"
	"reflect"
	"slices"

	"git.woa.com/adp/common/x/encodingx/jsonx"
	"git.woa.com/adp/common/x/gox/ptrx"
	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/spf13/cast"
	"github.com/spf13/cobra"

	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
)

var (
	cmdQA = &cobra.Command{
		Use:     "qa",
		Short:   "Operations on QA resources",
		PreRunE: func(cmd *cobra.Command, args []string) error { return initConfig() },
		RunE:    func(cmd *cobra.Command, args []string) error { return cmd.Usage() },
	}
	flagQABizIDs []string
)

var (
	cmdQAList = &cobra.Command{
		Use:     "list",
		Short:   "List QA resources with the given filters",
		PreRunE: func(cmd *cobra.Command, args []string) error { return initConfig() },
		RunE:    RunCmdQAList,
	}
	flagQAListFields []string
)

var (
	cmdQAUpdateQaSize = &cobra.Command{
		Use:     "update-qa-size",
		Short:   "Update qa_size for QA resources",
		PreRunE: func(cmd *cobra.Command, args []string) error { return initConfig() },
		RunE:    RunCmdQAUpdateQaSize,
	}
	flagQAUpdateQaSizeType      string
	flagQAUpdateQaSizeUin       string
	flagQAUpdateQaSizeAppBizIDs []string
	flagQAUpdateQaSizeSpaceID   string
	flagQAUpdateQaSizeAll       bool
)

func init() {
	flags := cmdQA.PersistentFlags()
	flags.StringSliceVar(&flagQABizIDs, "biz_ids", []string{}, "biz IDs of QA resources")

	flags = cmdQAList.PersistentFlags()
	flags.StringSliceVar(&flagQAListFields, "fields", []string{}, "db field names to list QA separated with comma")
	flags = cmdQAUpdateQaSize.PersistentFlags()
	flags.StringVarP(&flagQAUpdateQaSizeType, "type", "t", "field", "update qa size type: field or label")
	flags.StringVar(&flagQAUpdateQaSizeUin, "uin", "", "uin of the corp (required)")
	flags.StringSliceVar(&flagQAUpdateQaSizeAppBizIDs, "app_biz_ids", []string{}, "app biz IDs to process (optional, cannot be used with --space_id or --all)")
	flags.StringVar(&flagQAUpdateQaSizeSpaceID, "space_id", "", "space ID to process all apps under it (optional, cannot be used with --app_biz_ids or --all)")
	flags.BoolVar(&flagQAUpdateQaSizeAll, "all", false, "process all apps under the uin (optional, cannot be used with --app_biz_ids or --space_id)")

	cmdQA.AddCommand(cmdQAList)
	cmdQA.AddCommand(cmdQAUpdateQaSize)
}

func RunCmdQAList(cmd *cobra.Command, args []string) error {
	filter := &qaEntity.DocQaFilter{
		BusinessIds: slicex.Map(flagQABizIDs, func(s string) uint64 { return cast.ToUint64(s) }),
		Limit:       PageSize,
	}
	if len(CorpIDs) > 0 {
		filter.CorpId = cast.ToUint64(CorpIDs[0])
	}
	qaList, err := GetCmdService().QaLogic.GetDao().GetDocQaList(cmd.Context(), nil, filter)
	if err != nil {
		return err
	}

	tw := table.NewWriter()
	tw.SetOutputMirror(os.Stdout)
	var header table.Row
	if !slices.Contains(flagQAListFields, "id") {
		flagQAListFields = append([]string{"id"}, flagQAListFields...)
	}
	if !slices.Contains(flagQAListFields, "business_id") {
		flagQAListFields = append([]string{"business_id"}, flagQAListFields...)
	}
	if !slices.Contains(flagQAListFields, "corp_id") {
		flagQAListFields = append([]string{"corp_id"}, flagQAListFields...)
	}
	if !slices.Contains(flagQAListFields, "robot_id") {
		flagQAListFields = append([]string{"robot_id"}, flagQAListFields...)
	}
	if !slices.Contains(flagQAListFields, "question") {
		flagQAListFields = append([]string{"question"}, flagQAListFields...)
	}

	for _, field := range flagQAListFields {
		header = append(header, field)
	}
	tw.AppendHeader(header)
	for _, q := range qaList {
		var row table.Row
		for _, field := range flagQAListFields {
			qv := reflect.ValueOf(q).Elem()
			qt := qv.Type()

			var fieldIdx int
			for i := 0; i < qt.NumField(); i++ {
				if field == qt.Field(i).Tag.Get("db") {
					fieldIdx = i
					break
				}
			}
			row = append(row, qv.Field(fieldIdx).Interface())
		}
		tw.AppendRow(row)
	}
	tw.Render()
	return nil
}

func RunCmdQAUpdateQaSize(cmd *cobra.Command, args []string) error {
	return RunUpdateUsedCapacityCommand(cmd, UpdateUsedCapacityParams{
		Uin:       flagQAUpdateQaSizeUin,
		AppBizIDs: flagQAUpdateQaSizeAppBizIDs,
		SpaceID:   flagQAUpdateQaSizeSpaceID,
		All:       flagQAUpdateQaSizeAll,
		TypeName:  "Qa",
	}, ProcessAppQa)
}

func ProcessAppQa(ctx context.Context, app *entity.AppBaseInfo) error {
	logx.I(ctx, "processing app: %s", jsonx.MustMarshal(app))

	// 分批查询该应用所有的问答
	pageSize := getCurrentPageSize()
	offset := 0

	for {
		// 分批获取QA列表
		qaList, err := GetCmdService().QaLogic.GetDao().GetDocQaList(ctx, nil, &qaEntity.DocQaFilter{
			RobotId:   app.PrimaryId,
			IsDeleted: ptrx.Uint32(qaEntity.QAIsNotDeleted),
			Offset:    offset,
			Limit:     pageSize,
		})
		if err != nil {
			logx.E(ctx, "GetDocQaList err: %+v, app_id: %d, offset: %d", err, app.PrimaryId, offset)
			return err
		}

		if len(qaList) == 0 {
			break
		}

		logx.I(ctx, "GetDocQaList success, app_id: %d, offset: %d, qa_count: %d", app.PrimaryId, offset, len(qaList))

		// 遍历每个问答
		for _, qa := range qaList {
			// 计算该问答的问题和答案长度
			qaQuestionAnswerSize := uint64(len(qa.Question) + len(qa.Answer))
			totalQaSize := qaQuestionAnswerSize

			// 分批查询该问答的所有相似问
			simPage := 0
			simPageSize := getCurrentPageSize()

			for {
				similarQuestions, err := GetCmdService().QaLogic.GetDao().ListSimilarQuestions(ctx,
					[]string{qaEntity.DocQaSimTblColID, qaEntity.DocQaSimTblColSimilarID, qaEntity.DocQaSimTblColQuestion, qaEntity.DocQaSimTblColQASize},
					&qaEntity.SimilarityQuestionReq{
						RobotId:     app.PrimaryId,
						CorpId:      qa.CorpID,
						RelatedQAID: qa.ID,
						IsDeleted:   qaEntity.QAIsNotDeleted,
						Page:        uint32(simPage),
						PageSize:    uint32(simPageSize),
					})
				if err != nil {
					logx.E(ctx, "ListSimilarQuestions err: %+v, qa_id: %d, page: %d", err, qa.ID, simPage)
					return err
				}

				if len(similarQuestions) == 0 {
					break
				}

				logx.I(ctx, "ListSimilarQuestions success, qa_id: %d, page: %d, similar_count: %d", qa.ID, simPage, len(similarQuestions))

				// 遍历每个相似问，计算并更新qa_size
				for _, simQ := range similarQuestions {
					// 计算相似问的question字段长度
					simQaSize := uint64(len(simQ.Question))

					// 更新相似问的qa_size字段
					updateFilter := &qaEntity.SimilarityQuestionReq{
						ID: simQ.ID,
					}
					err := GetCmdService().QaLogic.GetDao().BatchUpdateSimilarQuestion(ctx, updateFilter,
						map[string]any{qaEntity.DocQaSimTblColQASize: simQaSize}, nil)
					if err != nil {
						logx.E(ctx, "BatchUpdateSimilarQuestion err: %+v, similar_id: %d", err, simQ.ID)
						return err
					}

					// 累加到总的qa_size
					totalQaSize += simQaSize
				}

				// 如果查询结果少于pageSize，说明已经查完了
				if len(similarQuestions) < simPageSize {
					break
				}

				simPage++
			}

			// 更新问答的qa_size字段
			qaFilter := &qaEntity.DocQaFilter{
				QAId: qa.ID,
			}
			rowsAffected, err := GetCmdService().QaLogic.GetDao().BatchUpdateDocQA(ctx, qaFilter,
				map[string]any{qaEntity.DocQaTblColQaSize: totalQaSize}, nil)
			if err != nil {
				logx.E(ctx, "BatchUpdateDocQA qa_id:%d err:%+v", qa.ID, err)
				return err
			}
			logx.I(ctx, "BatchUpdateDocQA qa_id:%d, total_qa_size:%d, rowsAffected:%d", qa.ID, totalQaSize, rowsAffected)
		}

		// 如果查询结果少于pageSize，说明已经查完了
		if len(qaList) < pageSize {
			break
		}

		offset += pageSize
	}

	return nil
}
