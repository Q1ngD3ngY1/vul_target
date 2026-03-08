package clean_database

import (
	"context"
	"git.code.oa.com/trpc-go/trpc-database/mysql"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/util/config"
)

// DeleteDataInTableBySql 删除表中所有满足条件的数据
func DeleteDataInTableBySql(ctx context.Context, db mysql.Client, deleteSql string, args []any) (int64, error) {
	if deleteSql == "" {
		log.ErrorContextf(ctx, "DeleteDataInTableByLimit deleteSql is empty")
		return 0, errs.ErrSystem
	}
	if len(args) == 0 {
		log.ErrorContextf(ctx, "DeleteDataInTableByLimit args is empty")
		return 0, errs.ErrSystem
	}
	deletedCount := int64(0)
	limit := config.GetMainConfig().DefaultDatabaseCleanConfig.DeleteBatchSize
	args = append(args, limit)
	for {
		affectedCount, err := DeleteDataInTableByLimitBySql(ctx, db, deleteSql, args)
		if err != nil {
			log.ErrorContextf(ctx, "DeleteDataInTable deleteSql:%s args:%+v error:%+v",
				deleteSql, args, err)
			return 0, err
		}
		deletedCount += affectedCount
		if affectedCount < int64(limit) {
			// 已分页清理完所有数据
			break
		}
	}
	log.DebugContextf(ctx, "DeleteDataInTable deleteSql:%s args:%+v deletedCount:%d",
		deleteSql, args, deletedCount)
	return deletedCount, nil
}

// DeleteDataInTableByLimitBySql 删除表中指定数量满足条件的数据
func DeleteDataInTableByLimitBySql(ctx context.Context, db mysql.Client, deleteSql string, args []any) (int64, error) {
	if deleteSql == "" {
		log.ErrorContextf(ctx, "DeleteDataInTableByLimit deleteSql is empty")
		return 0, errs.ErrSystem
	}
	if len(args) == 0 {
		log.ErrorContextf(ctx, "DeleteDataInTableByLimit args is empty")
		return 0, errs.ErrSystem
	}
	// 2、根据id删除数据
	deletedRows, err := db.Exec(ctx, deleteSql, args...)
	if err != nil {
		log.ErrorContextf(ctx, "cleanTablesWithDeletedFlag deleteSql:%s args:%+v error:%+v",
			deleteSql, args, err)
		return 0, err
	}
	affectedCount, err := deletedRows.RowsAffected()
	if err != nil {
		log.ErrorContextf(ctx, "cleanTablesWithDeletedFlag deleteSql:%s args:%+v error:%+v",
			deleteSql, args, err)
		return 0, err
	}
	log.DebugContextf(ctx, "cleanTablesWithDeletedFlag deleteSql:%s args:%+v affectedCount:%d",
		deleteSql, args, affectedCount)
	return affectedCount, nil
}
