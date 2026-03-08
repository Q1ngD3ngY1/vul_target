package knowledge_schema

import (
	"context"
	"fmt"
	"strings"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
)

// GenerateDBTableKnowledgeSchema 构建应用下所有数据库的所有表的建表SQL
func GenerateDBTableKnowledgeSchema(ctx context.Context, corpBizID, appBizID uint64, envType string) ([]*model.KnowledgeSchema, error) {
	releaseStatus := []int{model.ReleaseStatusReleased}
	if envType == model.EnvTypeSandbox {
		releaseStatus = append(releaseStatus, model.ReleaseStatusUnreleased)
	}
	dbSourceBizIds, err := dao.GetDBSourceDao().GetDbSourceBizIdByAppBizIDWithReleaseStatus(ctx, corpBizID, appBizID, releaseStatus)
	if err != nil {
		log.ErrorContextf(ctx, "BuildAppCreateTableSQL GetDbSourceBizIdByAppBizIDWithReleaseStatus fail, err:%v", err)
		return nil, err
	}
	var result []*model.KnowledgeSchema
	for _, dbSourceBizId := range dbSourceBizIds {
		dbTables, err := dao.GetDBTableDao().ListAllByDBSourceBizID(ctx, corpBizID, appBizID, dbSourceBizId)
		if err != nil {
			log.ErrorContextf(ctx, "BuildAppCreateTableSQL ListAllByDBSourceBizID fail, dbSourceBizId:%v, err:%v", dbSourceBizId, err)
			return nil, err
		}
		if len(dbTables) == 0 {
			log.InfoContextf(ctx, "BuildAppCreateTableSQL dbTables is nil, dbSourceBizId:%v", dbSourceBizId)
			continue
		}

		for _, dbTable := range dbTables {
			if !dbTable.IsIndexed {
				continue
			}
			dbColumns, err := dao.GetDBTableColumnDao().GetByTableBizID(ctx, corpBizID, appBizID, dbTable.DBTableBizID)
			if err != nil {
				log.ErrorContextf(ctx, "BuildAppCreateTableSQL GetByTableBizID fail, dbTableBizIDs:%v, err:%v", dbTable.DBTableBizID, err)
				return nil, err
			}
			if len(dbColumns) == 0 {
				log.InfoContextf(ctx, "BuildAppCreateTableSQL dbColumns is nil, dbTableBizID:%v", dbTable.DBTableBizID)
				continue
			}
			topValuesMap, err := getTopValuesMap(ctx, corpBizID, appBizID, dbTable.DBTableBizID)
			if err != nil {
				log.ErrorContextf(ctx, "BuildAppCreateTableSQL getTopValueMap fail, err:%v", err)
				return nil, err
			}

			example := dbTable.Name + " (\n"
			for _, col := range dbColumns {
				dbColumnsTopValues := topValuesMap[col.DBTableColumnBizID]
				if len(dbColumnsTopValues) == 0 {
					example += fmt.Sprintf("  %s %s COMMENT '%s',\n", col.ColumnName, col.DataType, col.ColumnComment)
				} else {
					example += fmt.Sprintf("  %s %s COMMENT '%s', -- example: [%s]\n", col.ColumnName, col.DataType, col.ColumnComment, strings.Join(dbColumnsTopValues, ","))
				}

			}
			example += fmt.Sprintf(") COMMENT='%s", dbTable.TableComment)
			example += "'\n"

			result = append(result, &model.KnowledgeSchema{
				CorpBizId: corpBizID,
				AppBizId:  appBizID,
				ItemType:  model.KnowledgeSchemaItemTypeDBTable,
				ItemBizId: dbTable.DBTableBizID,
				Name:      dbTable.Name,
				Summary:   example,
			})
		}
	}

	return result, nil
}

func getTopValuesMap(ctx context.Context, corpBizID, appBizID, dbTableBizID uint64) (map[uint64][]string, error) {
	dbTableTopValues, err := dao.GetDBSourceDao().GetTopValuesByDbTableBizID(ctx, corpBizID, appBizID, dbTableBizID)
	if err != nil {
		log.ErrorContext(ctx, "getTopValuesMap GetTopValuesByDbTableBizID fail, err:%v", err)
		return nil, err
	}

	resultMap := make(map[uint64]map[string]bool, len(dbTableTopValues))
	for _, dbTableTopValue := range dbTableTopValues {
		dbTableColumnBizID := dbTableTopValue.DbTableColumnBizID
		if resultMap[dbTableColumnBizID] == nil {
			resultMap[dbTableColumnBizID] = map[string]bool{}
		}
		resultMap[dbTableColumnBizID][dbTableTopValue.ColumnValue] = true
	}

	result := make(map[uint64][]string, len(resultMap))
	for columnBizID, columnValues := range resultMap {
		if result[columnBizID] == nil {
			result[columnBizID] = make([]string, 0, len(columnValues))
		}
		for value := range columnValues {
			result[columnBizID] = append(result[columnBizID], value)
		}
	}
	return result, nil
}
