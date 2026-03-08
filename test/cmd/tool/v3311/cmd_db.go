package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/model"
	"git.woa.com/adp/kb/kb-config/internal/config"

	"git.woa.com/adp/common/x/gox/ptrx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	"git.woa.com/adp/kb/kb-config/internal/entity/database"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
	"git.woa.com/adp/kb/kb-config/internal/util/idgen"
	retrieval "git.woa.com/adp/pb-go/kb/kb_retrieval"
	elasticv8 "github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/spf13/cast"
	"github.com/spf13/cobra"
	"golang.org/x/net/context"
)

var (
	cmdDB = &cobra.Command{
		Use:   "db",
		Short: "Operations on database resources",
		RunE:  func(cmd *cobra.Command, args []string) error { return cmd.Usage() },
	}
)

var (
	cmdDbEnableScope = &cobra.Command{
		Use:     "enable-scope",
		Short:   "Set enable_scope for DB resources",
		PreRunE: func(cmd *cobra.Command, args []string) error { return initConfig() },
		RunE:    RunCmdDbEnableScope,
	}
	flagDBEnableScopeUin       string
	flagDBEnableScopeAppBizIDs []string
	flagDBEnableScopeSpaceID   string
	flagDBEnableScopeAll       bool
)

var (
	cmdDbRevert = &cobra.Command{
		Use:     "revert",
		Short:   "revert db resources with the given filters",
		PreRunE: func(cmd *cobra.Command, args []string) error { return initConfig() },
		RunE:    RunCmdDbRevert,
	}
	flagDBRevertUin       string
	flagDBRevertAppBizIDs []string
	flagDBRevertSpaceID   string
	flagDBRevertAll       bool
)

func init() {
	flags := cmdDbEnableScope.PersistentFlags()
	flags.StringVar(&flagDBEnableScopeUin, "uin", "", "uin of the corp (required)")
	flags.StringSliceVar(&flagDBEnableScopeAppBizIDs, "app_biz_ids", []string{}, "app biz IDs to process (optional, cannot be used with --space_id or --all)")
	flags.StringVar(&flagDBEnableScopeSpaceID, "space_id", "", "space ID to process all apps under it (optional, cannot be used with --app_biz_ids or --all)")
	flags.BoolVar(&flagDBEnableScopeAll, "all", false, "process all apps under the uin (optional, cannot be used with --app_biz_ids or --space_id)")

	flags = cmdDbRevert.PersistentFlags()
	flags.StringVar(&flagDBRevertUin, "uin", "", "uin of the corp (required)")
	flags.StringSliceVar(&flagDBRevertAppBizIDs, "app_biz_ids", []string{}, "app biz IDs to process (optional, cannot be used with --space_id or --all)")
	flags.StringVar(&flagDBRevertSpaceID, "space_id", "", "space ID to process all apps under it (optional, cannot be used with --app_biz_ids or --all)")
	flags.BoolVar(&flagDBRevertAll, "all", false, "process all apps under the uin (optional, cannot be used with --app_biz_ids or --space_id)")

	cmdDB.AddCommand(cmdDbEnableScope)
	cmdDB.AddCommand(cmdDbRevert)
}

func RunCmdDbEnableScope(cmd *cobra.Command, args []string) error {
	return RunEnableScopeCommand(cmd, ProcessAppDb, EnableScopeParams{
		Uin:       flagDBEnableScopeUin,
		AppBizIDs: flagDBEnableScopeAppBizIDs,
		SpaceID:   flagDBEnableScopeSpaceID,
		All:       flagDBEnableScopeAll,
		TypeName:  "Db",
	}, &AppWorkerConfig{})
}

func ProcessAppDb(ctx context.Context, app *entity.App, config *AppWorkerConfig) error {
	logx.I(ctx, "-------------ProcessAppDb app: %+v", app)
	corpID := app.CorpPrimaryId
	robotId := app.PrimaryId
	embeddingVersion, embeddingName, err := GetCmdService().DbLogic.GetAppEmbeddingInfoById(ctx, app.BizId)
	if err != nil {
		logx.E(ctx, "GetAppEmbeddingVersionById failed, get app embedding version failed: %+v", err)
		return err
	}
	logx.I(ctx, "-------------embeddingVersion=%d, embeddingName=%s", embeddingVersion, embeddingName)

	dbFilter := &database.DatabaseFilter{
		AppBizID:  app.BizId,
		WithTable: true,
	}
	dbs, _, err := GetCmdService().DbLogic.DescribeDatabaseList(ctx, dbFilter)
	if err != nil {
		logx.E(ctx, "DescribeDatabaseList err:%+v", err)
		return err
	}
	logx.I(ctx, "-------------get dbs: %+v", dbs)

	for _, db := range dbs {
		if db.EnableScope != entity.EnableScopeInvalid {
			logx.I(ctx, "当前db已经刷过数据了 -------------db scope = %d", db.EnableScope)
			continue
		}
		previewTableBizIdMap := make(map[uint64]struct{})
		for _, table := range db.Tables {
			previewTableBizIdMap[table.DBTableBizID] = struct{}{}
		}
		// 查数据库发布table
		releaseTables, _, err := GetCmdService().DbLogic.DescribeTableProdList(ctx, &database.TableFilter{
			AppBizID:      app.BizId,
			DBSourceBizID: db.DBSourceBizID,
		})
		logx.I(ctx, "-------------get releaseTables: %+v", releaseTables)

		if err != nil {
			logx.E(ctx, "DescribeTableProdList err:%+v", err)
			return err
		}
		releaseTableBizIdMap := make(map[uint64]struct{})
		for _, tableProd := range releaseTables {
			releaseTableBizIdMap[tableProd.DBTableBizID] = struct{}{}
		}

		if app.IsShared {
			// 共享知识库，更新mysql的enable scope以及es和vdb的enable scope label
			err = modifyShareKBDbAndTables(ctx, robotId, embeddingVersion, embeddingName, db)
			if err != nil {
				logx.E(ctx, "GetAppEmbeddingVersionById failed, err:%+v", err)
			}
			continue
		}

		for _, previewTable := range db.Tables {
			if previewTable.IsIndexed {
				logx.I(ctx, "-------------当前表的状态是启用")
				if previewTable.ReleaseStatus == releaseEntity.ReleaseStatusSuccess {
					// 已发布，更新mysql的enable scope以及es和vdb的enable scope label
					logx.I(ctx, "-------------已发布 table has released: %+v", previewTable)
					err = modifyDefaultKBIndexedReleasedTable(ctx, robotId, embeddingVersion, embeddingName, previewTable)
					if err != nil {
						logx.E(ctx, "modifyDefaultKBIndexedReleasedTable failed, err:%+v", err)
						return err
					}
					continue
				}
				// 先查这个表是否发布过
				if _, ok := releaseTableBizIdMap[previewTable.DBTableBizID]; !ok {
					// 未发布过,更新mysql的enable scope以及es和vdb的enable scope label
					logx.I(ctx, "-------------未发布过 table has no release record: %+v", previewTable)
					err = modifyDefaultKBDevDomainTable(ctx, robotId, embeddingVersion, embeddingName, previewTable)
					if err != nil {
						logx.E(ctx, "modifyDefaultKBDevDomainTable failed, err:%+v", err)
						return err
					}
				} else {
					logx.I(ctx, "-------------发布过 更新作用域为仅发布: %+v", previewTable)
					// 发布过,更新开发域的mysql的enable scope以及es和vdb的enable scope label
					err = modifyDefaultKBDevDomainTable(ctx, robotId, embeddingVersion, embeddingName, previewTable)
					if err != nil {
						logx.E(ctx, "modifyDefaultKBDevDomainTable failed, err:%+v", err)
						return err
					}
					// 从发布域复制数据到公共域
					logx.I(ctx, "-------------发布过, 从发布域复制数据到公共域: %+v", previewTable)
					err = copyDefaultKBReleaseDomainTable2DevDomain(ctx, corpID, robotId, embeddingVersion, embeddingName, previewTable)
					if err != nil {
						logx.E(ctx, "copyDefaultKBReleaseDomainTable2DevDomain failed, err:%+v", err)
						return err
					}
				}
			} else {
				logx.I(ctx, "-------------当前表的状态是停用")
				if previewTable.ReleaseStatus == releaseEntity.ReleaseStatusSuccess {
					// 停用，已发布，作用域为停用，更新mysql的enable
					logx.I(ctx, "-------------已发布 table has released: %+v", previewTable)
					err = modifyDefaultKBUnindexedReleasedTable(ctx, previewTable)
					if err != nil {
						logx.E(ctx, "modifyDefaultKBIndexedReleasedTable failed, err:%+v", err)
						return err
					}
				} else {
					// 停用，未发布，先查这个表是否发布了
					if _, ok := releaseTableBizIdMap[previewTable.DBTableBizID]; !ok {
						// 停用，未发布，发布域没有这张表，作用域为停用
						logx.I(ctx, "-------------未发布过 table has no release record: %+v", previewTable)
						err = modifyDefaultKBUnindexedReleasedTable(ctx, previewTable)
						if err != nil {
							logx.E(ctx, "modifyDefaultKBIndexedReleasedTable failed, err:%+v", err)
							return err
						}
					} else {
						// 停用，未发布，但发布域有这张表, 先更新当前表的作用域为停用（因为用户可能更新过这张表再停用，所以是需要保留的）
						logx.I(ctx, "-------------发布过, 先更新当前表的作用域为停用: %+v", previewTable)
						err = modifyDefaultKBUnindexedReleasedTable(ctx, previewTable)
						if err != nil {
							logx.E(ctx, "modifyDefaultKBIndexedReleasedTable failed, err:%+v", err)
							return err
						}
						// 从发布域复制数据到公共域
						logx.I(ctx, "-------------发布过, 从发布域复制数据到公共域: %+v", previewTable)
						err = copyDefaultKBReleaseDomainTable2DevDomain(ctx, corpID, robotId, embeddingVersion, embeddingName, previewTable)
						if err != nil {
							logx.E(ctx, "recoverDefaultKBReleaseDomainTable2DevDomain failed, err:%+v", err)
							return err
						}
					}
				}
			}
		}

		for _, releaseTable := range releaseTables {
			// 查已发布的表是否已经在开发域已经不存在了
			if _, ok := previewTableBizIdMap[releaseTable.DBTableBizID]; !ok {
				logx.I(ctx, "-------------在开发域已经不存在了 table has released: %+v", releaseTable)
				err = recoverDefaultKBReleaseDomainTable2DevDomain(ctx, corpID, robotId, embeddingVersion, embeddingName, releaseTable)
				if err != nil {
					logx.E(ctx, "recoverDefaultKBReleaseDomainTable2DevDomain failed, err:%+v", err)
					return err
				}
			}
		}

		err = modifyDbEnableScope(ctx, db, false)
		if err != nil {
			return err
		}
	}
	// 处理db source在开发域已删除，但发布域未删除的情况
	deletedDbFilter := &database.DatabaseFilter{
		AppBizID:  app.BizId,
		IsDeleted: ptrx.Bool(true),
	}
	previewDeletedDbs, _, err := GetCmdService().DbLogic.DescribeDatabaseList(ctx, deletedDbFilter)
	if err != nil {
		logx.E(ctx, "DescribeDatabaseList(deleted) err:%+v", err)
		return err
	}
	for _, previewDeleteDb := range previewDeletedDbs {
		// 查开发域已删除的db source，是否在发布域有未删除的table
		releasedTables, _, err := GetCmdService().DbLogic.DescribeTableProdList(ctx, &database.TableFilter{
			AppBizID:      app.BizId,
			DBSourceBizID: previewDeleteDb.DBSourceBizID,
		})
		if err != nil {
			logx.E(ctx, "DescribeTableProdList(deleted db) err:%+v", err)
			return err
		}
		if len(releasedTables) == 0 {
			// 所有的表在发布域都已删除，则无需处理
			continue
		}
		// 恢复开发域已删除的db source及其发布域未删除的table
		err = recoverDefaultKBPreviewDeletedDb(ctx, corpID, robotId, embeddingVersion, embeddingName, previewDeleteDb, releasedTables)
		if err != nil {
			logx.E(ctx, "recoverDefaultKBPreviewDeletedDb err:%+v", err)
			return err
		}
	}
	return nil
}

// modifyShareKBDbAndTables 共享知识库，更新mysql的enable scope以及es和vdb的enable scope label
func modifyShareKBDbAndTables(ctx context.Context, robotId, embeddingVersion uint64, embeddingName string, db *database.Database) error {
	q := GetCmdService().DbDao.Query()
	for _, table := range db.Tables {
		// 更新mysql的enable scope
		tableEnableScope := entity.EnableScopeAll
		if !table.IsIndexed {
			tableEnableScope = entity.EnableScopeDisable
		}
		_, err := q.TDbTable.WithContext(ctx).Where(q.TDbTable.ID.Eq(table.ID)).Updates(map[string]any{
			"enable_scope": tableEnableScope,
		})
		if err != nil {
			logx.E(ctx, "Failed to update t_db_table, err:%+v", err)
			return err
		}
		if !table.IsIndexed {
			logx.I(ctx, "-------------共享知识库的表未启用: %+v", table)
			continue
		}
		logx.I(ctx, "-------------共享知识库的表已启用: %+v", table)
		// 更新es的enable scope label
		updateIndexResult, err := updateDocumentsByQuery(ctx, GetCmdService().EsClient, config.App().ESIndexNameConfig["text2sql_meta_preview"],
			EsCommonParams{
				RobotID:         robotId,
				DbBizID:         fmt.Sprintf("%d", table.DBSourceBizID),
				TableID:         fmt.Sprintf("%d", table.DBTableBizID),
				ScrollKeepAlive: 60 * time.Second,
				BatchSize:       100,
				EnableScope:     tableEnableScope,
			})
		logx.I(ctx, "updateDocumentsByQuery, result:%+v, err:%+v", updateIndexResult, err)
		if err != nil {
			logx.E(ctx, "Failed to updateDocumentsByQuery, err:%+v", err)
			return err
		}
		// 更新vdb的enable scope label
		err = modifyDbTable2Vdb(ctx, robotId, table.CorpBizID, table.AppBizID, table.DBTableBizID, embeddingVersion, embeddingName, tableEnableScope)
		if err != nil {
			logx.E(ctx, "Failed to modifyDbTable2Vdb, err:%+v", err)
			return err
		}
	}
	// 更新db的enable scope
	return modifyDbEnableScope(ctx, db, true)
}

// modifyDbEnableScope 更新db的enable scope
func modifyDbEnableScope(ctx context.Context, db *database.Database, isShared bool) error {
	q := GetCmdService().DbDao.Query()
	dbEnableScope := entity.EnableScopeAll
	if !db.IsIndexed {
		if isShared {
			dbEnableScope = entity.EnableScopeDisable
		} else {
			if db.ReleaseStatus == releaseEntity.ReleaseStatusInit {
				dbEnableScope = entity.EnableScopePublish
			} else {
				dbEnableScope = entity.EnableScopeDisable
			}
		}
	}
	_, err := q.TDbSource.WithContext(ctx).Where(q.TDbSource.ID.Eq(db.ID)).Updates(map[string]any{
		"enable_scope": dbEnableScope,
	})
	if err != nil {
		logx.E(ctx, "Failed to update t_db_source, err:%+v", err)
	}
	return err
}

// modifyDefaultKBIndexedReleasedTable 已启用已发布，更新mysql的enable scope以及es和vdb的enable scope label
func modifyDefaultKBIndexedReleasedTable(ctx context.Context, robotId, embeddingVersion uint64, embeddingName string, table *database.Table) error {
	logx.I(ctx, "-------------modifyDefaultKBIndexedReleasedTable: %+v", table)
	// 更新mysql的enable scope
	q := GetCmdService().DbDao.Query()
	_, err := q.TDbTable.WithContext(ctx).Where(q.TDbTable.ID.Eq(table.ID)).Updates(map[string]any{
		"enable_scope": entity.EnableScopeAll,
	})
	if err != nil {
		logx.E(ctx, "Failed to update t_db_table, err:%+v", err)
		return err
	}
	// 更新es的enable scope label
	updateIndexResult, err := updateDocumentsByQuery(ctx, GetCmdService().EsClient, config.App().ESIndexNameConfig["text2sql_meta_preview"],
		EsCommonParams{
			RobotID:         robotId,
			DbBizID:         fmt.Sprintf("%d", table.DBSourceBizID),
			TableID:         fmt.Sprintf("%d", table.DBTableBizID),
			ScrollKeepAlive: 60 * time.Second,
			BatchSize:       100,
			EnableScope:     entity.EnableScopeAll,
		})
	logx.I(ctx, "updateDocumentsByQuery, result:%+v, err:%+v", updateIndexResult, err)
	if err != nil {
		logx.E(ctx, "Failed to updateDocumentsByQuery, err:%+v", err)
		return err
	}
	// 更新vdb的enable scope label
	err = modifyDbTable2Vdb(ctx, robotId, table.CorpBizID, table.AppBizID, table.DBTableBizID, embeddingVersion, embeddingName, entity.EnableScopeAll)
	if err != nil {
		logx.E(ctx, "Failed to modifyDbTable2Vdb, err:%+v", err)
		return err
	}

	return nil
}

// modifyDefaultKBUnindexedReleasedTable 未启用已发布，更新mysql的enable scope以及es和vdb的enable scope label
func modifyDefaultKBUnindexedReleasedTable(ctx context.Context, table *database.Table) error {
	logx.I(ctx, "-------------modifyDefaultKBUnindexedReleasedTable: %+v", table)
	// 更新mysql的enable scope
	q := GetCmdService().DbDao.Query()
	_, err := q.TDbTable.WithContext(ctx).Where(q.TDbTable.ID.Eq(table.ID)).Updates(map[string]any{
		"enable_scope": entity.EnableScopeDisable,
	})
	if err != nil {
		logx.E(ctx, "Failed to update t_db_table, err:%+v", err)
		return err
	}
	return nil
}

// modifyDefaultKBDevDomainTable 未发布过,更新mysql的enable scope以及es和vdb的enable scope label
func modifyDefaultKBDevDomainTable(ctx context.Context, robotId, embeddingVersion uint64, embeddingName string, table *database.Table) error {
	logx.I(ctx, "-------------modifyDefaultKBDevDomainTable: %+v", table)
	// 更新mysql的enable scope
	q := GetCmdService().DbDao.Query()
	_, err := q.TDbTable.WithContext(ctx).Where(q.TDbTable.ID.Eq(table.ID)).Updates(map[string]any{
		"enable_scope": entity.EnableScopeDev,
	})
	if err != nil {
		logx.E(ctx, "Failed to update t_db_table, err:%+v", err)
		return err
	}
	// 更新es的enable scope label
	updateIndexResult, err := updateDocumentsByQuery(ctx, GetCmdService().EsClient, config.App().ESIndexNameConfig["text2sql_meta_preview"],
		EsCommonParams{
			RobotID:         robotId,
			DbBizID:         fmt.Sprintf("%d", table.DBSourceBizID),
			TableID:         fmt.Sprintf("%d", table.DBTableBizID),
			ScrollKeepAlive: 60 * time.Second,
			BatchSize:       100,
			EnableScope:     entity.EnableScopeDev,
		})
	logx.I(ctx, "updateDocumentsByQuery, result:%+v, err:%+v", updateIndexResult, err)
	if err != nil {
		logx.E(ctx, "Failed to updateDocumentsByQuery, err:%+v", err)
		return err
	}
	// 更新vdb的enable scope label
	err = modifyDbTable2Vdb(ctx, robotId, table.CorpBizID, table.AppBizID, table.DBTableBizID, embeddingVersion, embeddingName, entity.EnableScopeDev)
	if err != nil {
		logx.E(ctx, "Failed to modifyDbTable2Vdb, err:%+v", err)
		return err
	}

	return nil
}

// copyDefaultKBReleaseDomainTable2DevDomain 从发布域复制数据到公共域
func copyDefaultKBReleaseDomainTable2DevDomain(ctx context.Context, corpId, robotId, embeddingVersion uint64, embeddingName string, table *database.Table) error {
	logx.I(ctx, "-------------copyDefaultKBReleaseDomainTable2DevDomain: %+v", table)
	// 注意，这里有个大坑，即使表t_db_table_prod里有记录，但is_index字段一定是1，仅通过is_index是看不出来这个表在发布域是否是启用的。
	// 所以，这里不得不查一遍t_release_db_table这张表，看最近的一条发布记录，这个表是否是启用的，如果是启用，则恢复，如果是不启用，则不能恢复
	q := GetCmdService().DbDao.Query()
	releaseDbTableRecord, err := q.TReleaseDbTable.WithContext(ctx).Where(
		q.TReleaseDbTable.CorpBizID.Eq(table.CorpBizID),
		q.TReleaseDbTable.AppBizID.Eq(table.AppBizID),
		q.TReleaseDbTable.DbTableBizID.Eq(table.DBTableBizID)).
		Order(q.TReleaseDbTable.CreateTime.Desc()).Limit(1).Find()
	if err != nil {
		logx.E(ctx, "TReleaseDbTable.Find failed, err:%+v", err)
		return err
	}
	if len(releaseDbTableRecord) == 0 {
		logx.E(ctx, "TReleaseDbTable.Find failed, record not found")
		return fmt.Errorf("TReleaseDbTable.Find failed, record not found")
	}
	if !releaseDbTableRecord[0].IsIndexed {
		logx.I(ctx, "-------------此表虽然发布过，但最后一次的发布记录是不启用，因此不需要恢复!!!")
		return nil
	}

	// t_db_table
	newTable := *table
	newTable.ID = 0
	newTableBizId := idgen.GetId()
	newTable.DBTableBizID = newTableBizId
	newTable.AliasName = releaseDbTableRecord[0].AliasName
	newTable.Description = releaseDbTableRecord[0].Description
	newTable.EnableScope = entity.EnableScopePublish
	newTable.ReleaseStatus = releaseEntity.ReleaseStatusSuccess
	err = GetCmdService().DbDao.CreateTableList(ctx, []*database.Table{&newTable})
	if err != nil {
		logx.E(ctx, "CreateTableList failed, err:%+v", err)
		return err
	}
	logx.I(ctx, "-------------create new table done: %+v", newTable)

	// t_db_table_column
	columnFilter := &database.ColumnFilter{
		CorpBizID:    table.CorpBizID,
		AppBizID:     table.AppBizID,
		DBTableBizID: table.DBTableBizID,
	}
	columnList, _, err := GetCmdService().DbDao.DescribeColumnList(ctx, columnFilter)
	if err != nil {
		logx.E(ctx, "DescribeColumnList failed, err:%+v", err)
		return err
	}
	columnName2NewColumnBizId := make(map[string]uint64)
	for _, column := range columnList {
		column.ID = 0
		column.DBTableBizID = newTableBizId
		column.DBTableColumnBizID = idgen.GetId()
		columnName2NewColumnBizId[column.ColumnName] = column.DBTableColumnBizID
	}
	err = GetCmdService().DbDao.CreateColumnList(ctx, columnList)
	if err != nil {
		logx.E(ctx, "CreateColumnList failed, err:%+v", err)
		return err
	}
	logx.I(ctx, "-------------create new columnList")

	// t_db_top_value
	topValueList, err := GetCmdService().DbDao.GetTopValuesByDbTableBizID(ctx, table.CorpBizID, table.AppBizID, table.DBTableBizID)
	for _, topValue := range topValueList {
		topValue.ID = 0
		topValue.DbTableBizID = newTableBizId
		topValue.DbTableColumnBizID = columnName2NewColumnBizId[topValue.ColumnName]
		topValue.BusinessID = idgen.GetId()
	}
	err = GetCmdService().DbDao.CreateTopValue(ctx, topValueList)
	if err != nil {
		logx.E(ctx, "CreateTopValue failed, err:%+v", err)
	}
	logx.I(ctx, "-------------create new top value done")

	// copyDocumentsByQuery 根据查询条件复制文档，并修改labels字段
	copyResult, err := copyDocumentsByQuery(ctx, GetCmdService().EsClient,
		config.App().ESIndexNameConfig["text2sql_meta_prod"], config.App().ESIndexNameConfig["text2sql_meta_preview"],
		EsCommonParams{
			RobotID:         robotId,
			DbBizID:         fmt.Sprintf("%d", table.DBSourceBizID),
			TableID:         fmt.Sprintf("%d", table.DBTableBizID),
			ScrollKeepAlive: 60 * time.Second,
			BatchSize:       100,
			NewDbTableBizID: fmt.Sprintf("%d", newTableBizId),
			EnableScope:     entity.EnableScopePublish, // 3-仅发布
		})
	logx.I(ctx, "copyDocumentsByQuery, copyResult:%+v, err:%+v", copyResult, err)
	if err != nil {
		logx.E(ctx, "copyDocumentsByQuery failed, err:%+v", err)
		return err
	}

	// 更新vdb
	err = modifyDbTable2Vdb(ctx, robotId, newTable.CorpBizID, newTable.AppBizID, newTableBizId, embeddingVersion, embeddingName, entity.EnableScopePublish)
	if err != nil {
		logx.E(ctx, "modifyDbTable2Vdb failed, err:%+v", err)
	}

	// t_dev_release_relation_info
	qaQuery := GetCmdService().QaLogic.GetDao().Query()
	err = qaQuery.TDevReleaseRelationInfo.WithContext(ctx).Create(&model.TDevReleaseRelationInfo{
		CorpID:             corpId,
		RobotID:            robotId,
		Type:               releaseEntity.DevReleaseRelationTypeTable,
		DevBusinessID:      table.DBTableBizID,
		ReleaseBusinessID:  newTableBizId,
		DiffTaskBusinessID: 0,
	})
	if err != nil {
		logx.E(ctx, "create TDevReleaseRelationInfo failed, err:%+v", err)
	}
	return err
}

// recoverDefaultKBReleaseDomainTable2DevDomain 恢复开发域已停用/删除的表，从发布域恢复到开发域
func recoverDefaultKBReleaseDomainTable2DevDomain(ctx context.Context, corpId, robotId, embeddingVersion uint64, embeddingName string, releaseTable *database.TableProd) error {
	// 注意，这里有个大坑，即使表t_db_table_prod里有记录，但is_index字段一定是1，仅通过is_index是看不出来这个表在发布域是否是启用的。
	// 所以，这里不得不查一遍t_release_db_table这张表，看最近的一条发布记录，这个表是否是启用的，如果是启用，则恢复，如果是不启用，则不能恢复
	q := GetCmdService().DbDao.Query()
	releaseDbTableRecord, err := q.TReleaseDbTable.WithContext(ctx).Where(
		q.TReleaseDbTable.CorpBizID.Eq(releaseTable.CorpBizID),
		q.TReleaseDbTable.AppBizID.Eq(releaseTable.AppBizID),
		q.TReleaseDbTable.DbTableBizID.Eq(releaseTable.DBTableBizID)).
		Order(q.TReleaseDbTable.CreateTime.Desc()).Limit(1).Find()
	if err != nil {
		logx.E(ctx, "TReleaseDbTable.Find failed, err:%+v", err)
		return err
	}
	if len(releaseDbTableRecord) == 0 {
		logx.E(ctx, "TReleaseDbTable.Find failed, record not found")
		return fmt.Errorf("TReleaseDbTable.Find failed, record not found")
	}
	logx.I(ctx, "-------------releaseDbTableRecord: %+v", releaseDbTableRecord[0])
	enableScope := entity.EnableScopePublish
	if !releaseDbTableRecord[0].IsIndexed {
		enableScope = entity.EnableScopeDisable
	}
	logx.I(ctx, "-------------recoverDefaultKBReleaseDomainTable2DevDomain releaseTable: %+v", releaseTable)
	// 已发布的表在开发域不存在
	// 这时候在t_db_table中是有这条记录的，可以直接将is_deleted恢复为false，enable_scope更新为3-仅发布
	_, err = q.TDbTable.WithContext(ctx).Where(q.TDbTable.CorpBizID.Eq(releaseTable.CorpBizID),
		q.TDbTable.AppBizID.Eq(releaseTable.AppBizID),
		q.TDbTable.DbTableBizID.Eq(releaseTable.DBTableBizID)).Updates(map[string]any{
		"is_deleted":   false,
		"enable_scope": enableScope,
	})
	if err != nil {
		logx.E(ctx, "ModifyTable err:%+v", err)
		return err
	}

	// 如果发布域的表是删除的，在恢复这张表的时候需要记录，以便回滚。
	qaQuery := GetCmdService().QaLogic.GetDao().Query()
	err = qaQuery.TDevReleaseRelationInfo.WithContext(ctx).Create(&model.TDevReleaseRelationInfo{
		CorpID:             corpId,
		RobotID:            robotId,
		Type:               releaseEntity.DevReleaseRelationTypeTable,
		DevBusinessID:      releaseTable.DBTableBizID,
		ReleaseBusinessID:  releaseTable.DBTableBizID,
		DiffTaskBusinessID: 0,
	})

	// 这时候在t_db_table_column中也是有记录的，可以直接将is_deleted恢复为false
	err = recoverTDbTableColumn(ctx, releaseTable.CorpBizID, releaseTable.AppBizID, releaseTable.DBTableBizID)
	if err != nil {
		logx.E(ctx, "recoverTDbTableColumn err:%+v", err)
		return err
	}
	// 这时候在t_db_table_top_value中也是有记录的，可以直接将is_deleted恢复为false
	err = recoverTDbTableTopValue(ctx, releaseTable.CorpBizID, releaseTable.AppBizID, releaseTable.DBTableBizID)
	if err != nil {
		logx.E(ctx, "recoverTDbTableColumn err:%+v", err)
	}
	if enableScope == entity.EnableScopeDisable {
		logx.I(ctx, "-------------最新的发布记录中，这张发布域的表没有启用，所以不需要恢复ES/VDB！")
		return nil
	}
	// copyDocumentsByQuery 根据查询条件复制文档，并修改labels字段
	copyResult, err := copyDocumentsByQuery(ctx, GetCmdService().EsClient,
		config.App().ESIndexNameConfig["text2sql_meta_prod"], config.App().ESIndexNameConfig["text2sql_meta_preview"],
		EsCommonParams{
			RobotID:         robotId,
			DbBizID:         fmt.Sprintf("%d", releaseTable.DBSourceBizID),
			TableID:         fmt.Sprintf("%d", releaseTable.DBTableBizID),
			ScrollKeepAlive: 60 * time.Second,
			BatchSize:       100,
			NewDbTableBizID: fmt.Sprintf("%d", releaseTable.DBTableBizID),
			EnableScope:     entity.EnableScopePublish, // 3-仅发布
		})
	logx.I(ctx, "copyDocumentsByQuery, copyResult:%+v, err:%+v", copyResult, err)
	if err != nil {
		logx.E(ctx, "copyDocumentsByQuery failed, err:%+v", err)
		return err
	}
	// 更新外部数据库的值到 vdb 中
	// 更新vdb
	err = modifyDbTable2Vdb(ctx, robotId, releaseTable.CorpBizID, releaseTable.AppBizID, releaseTable.DBTableBizID, embeddingVersion, embeddingName, entity.EnableScopePublish)
	if err != nil {
		logx.E(ctx, "modifyDbTable2Vdb failed, err:%+v", err)
	}
	return err
}

func recoverTDbTableColumn(ctx context.Context, corpBizId, appBizId, dbTableBizId uint64) error {
	q := GetCmdService().DbDao.Query()
	_, err := q.TDbTableColumn.WithContext(ctx).Where(
		q.TDbTableColumn.CorpBizID.Eq(corpBizId),
		q.TDbTableColumn.AppBizID.Eq(appBizId),
		q.TDbTableColumn.DbTableBizID.Eq(dbTableBizId)).
		UpdateSimple(q.TDbTableColumn.IsDeleted.Value(false))
	return err
}

func recoverTDbTableTopValue(ctx context.Context, corpBizId, appBizId, dbTableBizId uint64) error {
	q := GetCmdService().DbDao.Query()
	_, err := q.TDbTableTopValue.WithContext(ctx).Where(
		q.TDbTableTopValue.CorpBizID.Eq(corpBizId),
		q.TDbTableTopValue.AppBizID.Eq(appBizId),
		q.TDbTableTopValue.DbTableBizID.Eq(dbTableBizId)).
		UpdateSimple(q.TDbTableTopValue.IsDeleted.Value(false))
	return err
}

// recoverDefaultKBPreviewDeletedDb 恢复开发域已删除的数据库，从发布域恢复到开发域
func recoverDefaultKBPreviewDeletedDb(ctx context.Context, corpId, robotId, embeddingVersion uint64, embeddingName string, previewDeletedDb *database.Database, releasedTables []*database.TableProd) error {
	for _, releasedTable := range releasedTables {
		err := recoverDefaultKBReleaseDomainTable2DevDomain(ctx, corpId, robotId, embeddingVersion, embeddingName, releasedTable)
		if err != nil {
			logx.E(ctx, "recoverDefaultKBReleaseDomainTable2DevDomain failed, releasedTable:%+v, err:%+v", releasedTable, err)
			return err
		}
	}
	q := GetCmdService().DbDao.Query()
	_, err := q.TDbSource.WithContext(ctx).Where(q.TDbSource.ID.Eq(previewDeletedDb.ID)).Updates(map[string]any{
		"enable_scope": entity.EnableScopePublish,
		"is_deleted":   false,
	})
	if err != nil {
		logx.E(ctx, "modify TDbSource failed, err:%+v", err)
		return err
	}
	qaQuery := GetCmdService().QaLogic.GetDao().Query()
	err = qaQuery.TDevReleaseRelationInfo.WithContext(ctx).Create(&model.TDevReleaseRelationInfo{
		CorpID:             corpId,
		RobotID:            robotId,
		Type:               releaseEntity.DevReleaseRelationTypeDatabase,
		DevBusinessID:      previewDeletedDb.DBSourceBizID,
		ReleaseBusinessID:  previewDeletedDb.DBSourceBizID,
		DiffTaskBusinessID: 0,
	})
	return nil
}

// EsCommonParams 复制文档的参数
type EsCommonParams struct {
	RobotID         uint64
	DbBizID         string
	TableID         string
	ScrollKeepAlive time.Duration
	BatchSize       int
	NewDbTableBizID string // 新的db_table_biz_id，用于替换labels
	EnableScope     int    // 检索范围，1-停用，2-仅开发，3-仅发布，4-all
}

// CopyIndexResult 复制结果
type CopyIndexResult struct {
	TotalCopied int64         `json:"total_copied"`
	TotalTime   time.Duration `json:"total_time"`
	Success     bool          `json:"success"`
	Error       string        `json:"error,omitempty"`
}

// UpdateIndexResult 更新结果
type UpdateIndexResult struct {
	TotalUpdated int64         `json:"total_updated"`
	TotalTime    time.Duration `json:"total_time"`
	Success      bool          `json:"success"`
	Error        string        `json:"error,omitempty"`
}

// SearchResponse 搜索响应结构
type SearchResponse struct {
	Took     int  `json:"took"`
	TimedOut bool `json:"timed_out"`
	Hits     struct {
		Total struct {
			Value int64 `json:"value"`
		} `json:"total"`
		Hits []struct {
			Index  string          `json:"_index"`
			ID     string          `json:"_id"`
			Score  float64         `json:"_score"`
			Source json.RawMessage `json:"_source"`
		} `json:"hits"`
	} `json:"hits"`
	ScrollID string `json:"_scroll_id,omitempty"`
}

// BulkResponse 批量操作响应
type BulkResponse struct {
	Took   int  `json:"took"`
	Errors bool `json:"errors"`
	Items  []map[string]struct {
		ID     string `json:"_id"`
		Result string `json:"result"`
		Status int    `json:"status"`
		Error  struct {
			Type   string `json:"type"`
			Reason string `json:"reason"`
		} `json:"error,omitempty"`
	} `json:"items,omitempty"`
}

// Document 文档结构体
type Document struct {
	RobotID     int      `json:"robot_id"`
	ID          int      `json:"id"`
	DocType     int      `json:"doc_type"`
	SegmentType string   `json:"segment_type"`
	DocID       int      `json:"doc_id"`
	DbBizID     string   `json:"db_biz_id"`
	TableID     string   `json:"table_id"`
	TableName   string   `json:"table_name"`
	FileName    string   `json:"file_name"`
	Data        string   `json:"data"`
	Labels      []string `json:"labels"`
	ExpireTime  int      `json:"expire_time"`
	UpdateTime  string   `json:"update_time"`
}

// copyDocumentsByQuery 根据查询条件复制文档，并修改labels字段
func copyDocumentsByQuery(
	ctx context.Context,
	es *elasticv8.TypedClient,
	sourceIndex, targetIndex string,
	params EsCommonParams,
) (*CopyIndexResult, error) {

	startTime := time.Now()
	result := &CopyIndexResult{}

	// 设置默认参数
	if params.ScrollKeepAlive == 0 {
		params.ScrollKeepAlive = 2 * time.Minute
	}
	if params.BatchSize == 0 {
		params.BatchSize = 100
	}

	// 构建查询条件
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"filter": []map[string]interface{}{
					{
						"term": map[string]interface{}{
							"robot_id": params.RobotID,
						},
					},
					{
						"term": map[string]interface{}{
							"db_biz_id": params.DbBizID,
						},
					},
					{
						"term": map[string]interface{}{
							"table_id": params.TableID,
						},
					},
				},
			},
		},
	}

	// 初始化scroll搜索
	documents, scrollID, totalHits, err := initScrollSearch(ctx, es, sourceIndex, query, params)
	if err != nil {
		return nil, fmt.Errorf("failed to init scroll search: %w", err)
	}
	if len(documents) == 0 {
		logx.I(ctx, "-------------copyDocumentsByQuery: no documents found")
		return nil, nil
	}

	fmt.Printf("开始复制文档，预计总数: %d\n", totalHits)

	var totalCopied int64
	// 修改文档内容（特别是labels字段）
	modifiedDocs, err := modifyCopiedDocuments(documents, params)
	if err != nil {
		return nil, fmt.Errorf("failed to modify documents: %w", err)
	}

	// 批量索引修改后的文档到目标索引
	successCount, err := bulkIndexDocuments(ctx, es, targetIndex, modifiedDocs)
	if err != nil {
		return nil, fmt.Errorf("failed to bulk index documents: %w", err)
	}

	totalCopied += successCount
	fmt.Printf("已复制: %d/%d 文档\n", totalCopied, totalHits)

	// 确保清理scroll上下文
	defer func() {
		if scrollID != "" {
			clearScroll(ctx, es, scrollID)
		}
	}()

	// 使用scroll遍历所有文档
	for {
		documents, nextScrollID, hasMore, err := scrollNextBatch(es, scrollID, params)
		if err != nil {
			return nil, fmt.Errorf("failed to scroll next batch: %w", err)
		}
		scrollID = nextScrollID

		if len(documents) == 0 {
			break
		}

		// 修改文档内容（特别是labels字段）
		modifiedDocs, err := modifyCopiedDocuments(documents, params)
		if err != nil {
			return nil, fmt.Errorf("failed to modify documents: %w", err)
		}

		// 批量索引修改后的文档到目标索引
		successCount, err := bulkIndexDocuments(ctx, es, targetIndex, modifiedDocs)
		if err != nil {
			return nil, fmt.Errorf("failed to bulk index documents: %w", err)
		}

		totalCopied += successCount
		fmt.Printf("已复制: %d/%d 文档\n", totalCopied, totalHits)

		if !hasMore {
			break
		}
	}

	result.TotalCopied = totalCopied
	result.TotalTime = time.Since(startTime)
	result.Success = true

	fmt.Printf("复制完成! 总共复制 %d 个文档, 耗时: %v\n", totalCopied, result.TotalTime)
	return result, nil
}

// modifyCopiedDocuments 修改文档内容
func modifyCopiedDocuments(documents []map[string]interface{}, params EsCommonParams) ([]map[string]interface{}, error) {
	modifiedDocs := make([]map[string]interface{}, 0, len(documents))

	for _, doc := range documents {
		// 解析原始文档
		var originalDoc Document
		sourceBytes, err := json.Marshal(doc["source"])
		if err != nil {
			return nil, fmt.Errorf("error marshaling source: %w", err)
		}

		if err := json.Unmarshal(sourceBytes, &originalDoc); err != nil {
			return nil, fmt.Errorf("error parsing document: %w", err)
		}

		// 修改labels字段
		modifiedLabels := modifyLabels(originalDoc.Labels, params.NewDbTableBizID, params.EnableScope)

		// 创建修改后的文档
		modifiedDoc := Document{
			RobotID:     originalDoc.RobotID,
			ID:          originalDoc.ID,
			DocType:     originalDoc.DocType,
			SegmentType: originalDoc.SegmentType,
			DocID:       originalDoc.DocID,
			DbBizID:     originalDoc.DbBizID,
			TableID:     params.NewDbTableBizID,
			TableName:   originalDoc.TableName,
			FileName:    originalDoc.FileName,
			Data:        originalDoc.Data,
			Labels:      modifiedLabels,
			ExpireTime:  originalDoc.ExpireTime,
			UpdateTime:  originalDoc.UpdateTime,
		}

		// 转换为map用于批量索引
		modifiedDocMap := make(map[string]interface{})
		modifiedDocBytes, _ := json.Marshal(modifiedDoc)
		json.Unmarshal(modifiedDocBytes, &modifiedDocMap)

		modifiedDocs = append(modifiedDocs, map[string]interface{}{
			"id":     fmt.Sprintf("%s_%d", params.NewDbTableBizID, originalDoc.ID),
			"source": modifiedDocMap,
		})
	}

	return modifiedDocs, nil
}

// modifyLabels 修改labels字段
func modifyLabels(labels []string, newDbTableBizID string, enableScope int) []string {
	if newDbTableBizID == "" {
		return labels // 如果没有提供新的db_table_biz_id，保持原样
	}

	modifiedLabels := make([]string, 0, len(labels))

	for _, label := range labels {
		// 检查是否是db_table_biz_id标签
		if strings.HasPrefix(label, "db_table_biz_id:") {
			// 替换为新的db_table_biz_id
			modifiedLabels = append(modifiedLabels, "db_table_biz_id:"+newDbTableBizID)
		} else {
			// 保持其他标签不变
			modifiedLabels = append(modifiedLabels, label)
		}
	}
	modifiedLabels = append(modifiedLabels, fmt.Sprintf("enable_scope:%s", entity.EnableScopeDb2Label[uint32(enableScope)]))
	return modifiedLabels
}

// initScrollSearch 初始化scroll搜索
func initScrollSearch(
	ctx context.Context,
	es *elasticv8.TypedClient,
	sourceIndex string,
	query map[string]interface{},
	params EsCommonParams,
) ([]map[string]interface{}, string, int64, error) {

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		return nil, "", 0, fmt.Errorf("error encoding query: %w", err)
	}
	logx.I(ctx, "-------------initScrollSearch: init query--%s", buf.String())
	// 执行初始搜索获取scroll ID
	req := esapi.SearchRequest{
		Index:  []string{sourceIndex},
		Body:   &buf,
		Size:   &params.BatchSize,
		Scroll: params.ScrollKeepAlive,
	}
	res, err := req.Do(ctx, es)
	if err != nil {
		return nil, "", 0, fmt.Errorf("error executing search: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, "", 0, fmt.Errorf("search error: %s", res.String())
	}

	var searchResp SearchResponse
	if err := json.NewDecoder(res.Body).Decode(&searchResp); err != nil {
		return nil, "", 0, fmt.Errorf("error parsing search response: %w", err)
	}

	// 解析文档
	documents := make([]map[string]interface{}, 0, len(searchResp.Hits.Hits))
	for _, hit := range searchResp.Hits.Hits {
		var doc map[string]interface{}
		if err := json.Unmarshal(hit.Source, &doc); err != nil {
			return nil, "", 0, fmt.Errorf("error parsing document: %w", err)
		}
		documents = append(documents, map[string]interface{}{
			"id":     hit.ID,
			"source": doc,
		})
	}
	logx.I(ctx, "-------------initScrollSearch: documents--%+v", documents)
	return documents, searchResp.ScrollID, searchResp.Hits.Total.Value, nil
}

// scrollNextBatch 获取下一批文档
func scrollNextBatch(
	es *elasticv8.TypedClient,
	scrollID string,
	params EsCommonParams,
) ([]map[string]interface{}, string, bool, error) {

	if scrollID == "" {
		return nil, "", false, nil
	}

	req := esapi.ScrollRequest{
		ScrollID: scrollID,
		Scroll:   params.ScrollKeepAlive,
	}
	res, err := req.Do(context.Background(), es)
	if err != nil {
		return nil, "", false, fmt.Errorf("error executing scroll: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, "", false, fmt.Errorf("scroll error: %s", res.String())
	}

	var searchResp SearchResponse
	if err := json.NewDecoder(res.Body).Decode(&searchResp); err != nil {
		return nil, "", false, fmt.Errorf("error parsing scroll response: %w", err)
	}

	// 解析文档
	documents := make([]map[string]interface{}, 0, len(searchResp.Hits.Hits))
	for _, hit := range searchResp.Hits.Hits {
		var doc map[string]interface{}
		if err := json.Unmarshal(hit.Source, &doc); err != nil {
			return nil, "", false, fmt.Errorf("error parsing document: %w", err)
		}
		documents = append(documents, map[string]interface{}{
			"id":     hit.ID,
			"source": doc,
		})
	}

	// 检查是否还有更多数据
	hasMore := len(searchResp.Hits.Hits) > 0

	return documents, searchResp.ScrollID, hasMore, nil
}

// bulkIndexDocuments 批量索引文档
func bulkIndexDocuments(
	ctx context.Context,
	es *elasticv8.TypedClient,
	targetIndex string,
	documents []map[string]interface{},
) (int64, error) {

	if len(documents) == 0 {
		return 0, nil
	}

	var bulkBody strings.Builder
	for _, doc := range documents {
		docID, ok := doc["id"].(string)
		if !ok {
			continue
		}

		source, ok := doc["source"].(map[string]interface{})
		if !ok {
			continue
		}

		// 添加批量操作头
		indexCmd := map[string]interface{}{
			"index": map[string]interface{}{
				"_index": targetIndex,
				"_id":    docID,
			},
		}
		logx.D(ctx, "-------------bulkIndexDocuments, index:%s, _id:%s", targetIndex, docID)
		indexCmdBytes, _ := json.Marshal(indexCmd)
		bulkBody.Write(indexCmdBytes)
		bulkBody.WriteString("\n")

		sourceBytes, _ := json.Marshal(source)
		bulkBody.Write(sourceBytes)
		bulkBody.WriteString("\n")
	}

	// 执行批量操作
	req := esapi.BulkRequest{
		Body: strings.NewReader(bulkBody.String()),
	}
	res, err := req.Do(ctx, es)
	if err != nil {
		return 0, fmt.Errorf("error executing bulk request: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return 0, fmt.Errorf("bulk error: %s", res.String())
	}

	// 解析响应
	var bulkResp BulkResponse
	if err := json.NewDecoder(res.Body).Decode(&bulkResp); err != nil {
		return 0, fmt.Errorf("error parsing bulk response: %w", err)
	}

	// 统计成功数量
	var successCount int64
	for _, item := range bulkResp.Items {
		for _, action := range item {
			if action.Status >= 200 && action.Status < 300 {
				successCount++
			} else {
				logx.E(ctx, "文档索引失败: ID=%s, 错误: %s", action.ID, action.Error.Reason)
			}
		}
	}

	return successCount, nil
}

// clearScroll 清理scroll上下文
func clearScroll(ctx context.Context, es *elasticv8.TypedClient, scrollID string) {
	req := esapi.ClearScrollRequest{
		ScrollID: []string{scrollID},
	}
	_, err := req.Do(ctx, es)
	if err != nil {
		logx.E(ctx, "Warning: Failed to clear scroll context: %s", err)
	}
}

// updateDocumentsByQuery 根据查询条件更新文档，在labels数组中添加"enable_scope:x"
func updateDocumentsByQuery(
	ctx context.Context,
	es *elasticv8.TypedClient,
	index string,
	params EsCommonParams,
) (*UpdateIndexResult, error) {

	startTime := time.Now()
	result := &UpdateIndexResult{}

	// 设置默认参数
	if params.ScrollKeepAlive == 0 {
		params.ScrollKeepAlive = 2 * time.Minute
	}
	if params.BatchSize == 0 {
		params.BatchSize = 100
	}

	// 构建查询条件
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"filter": []map[string]interface{}{
					{
						"term": map[string]interface{}{
							"robot_id": params.RobotID,
						},
					},
					{
						"term": map[string]interface{}{
							"db_biz_id": params.DbBizID,
						},
					},
					{
						"term": map[string]interface{}{
							"table_id": params.TableID,
						},
					},
				},
			},
		},
	}

	// 初始化scroll搜索
	documents, scrollID, totalHits, err := initScrollSearch(ctx, es, index, query, params)
	if err != nil {
		return nil, fmt.Errorf("failed to init scroll search: %w", err)
	}
	if len(documents) == 0 {
		logx.I(ctx, "-------------updateDocumentsByQuery no documents found")
		return nil, nil
	}
	fmt.Printf("开始更新文档，scrollID:%s, 预计总数: %d\n", scrollID, totalHits)
	var totalUpdated int64
	// 批量更新文档
	successCount, err := bulkUpdateDocuments(ctx, es, index, documents, params)
	if err != nil {
		return nil, fmt.Errorf("failed to bulk update documents: %w", err)
	}
	totalUpdated += successCount
	logx.I(ctx, "已更新: %d/%d 文档\n", totalUpdated, totalHits)

	// 确保清理scroll上下文
	defer func() {
		if scrollID != "" {
			clearScroll(ctx, es, scrollID)
		}
	}()

	// 使用scroll遍历所有文档
	for {
		documents, nextScrollID, hasMore, err := scrollNextBatch(es, scrollID, params)
		if err != nil {
			return nil, fmt.Errorf("failed to scroll next batch: %w", err)
		}
		scrollID = nextScrollID

		if len(documents) == 0 {
			break
		}

		// 批量更新文档
		successCount, err := bulkUpdateDocuments(ctx, es, index, documents, params)
		if err != nil {
			return nil, fmt.Errorf("failed to bulk update documents: %w", err)
		}

		totalUpdated += successCount
		logx.I(ctx, "已更新: %d/%d 文档\n", totalUpdated, totalHits)

		if !hasMore {
			break
		}
	}

	result.TotalUpdated = totalUpdated
	result.TotalTime = time.Since(startTime)
	result.Success = true

	logx.I(ctx, "更新完成! 总共更新 %d 个文档, 耗时: %v\n", totalUpdated, result.TotalTime)
	return result, nil
}

// bulkUpdateDocuments 批量更新文档
func bulkUpdateDocuments(
	ctx context.Context,
	es *elasticv8.TypedClient,
	index string,
	documents []map[string]interface{},
	params EsCommonParams,
) (int64, error) {

	if len(documents) == 0 {
		return 0, nil
	}

	var bulkBody strings.Builder
	for _, doc := range documents {
		docID, ok := doc["id"].(string)
		if !ok {
			continue
		}

		source, ok := doc["source"].(map[string]interface{})
		if !ok {
			continue
		}

		// 解析原始文档
		var originalDoc Document
		sourceBytes, err := json.Marshal(source)
		if err != nil {
			return 0, fmt.Errorf("error marshaling source: %w", err)
		}

		if err := json.Unmarshal(sourceBytes, &originalDoc); err != nil {
			return 0, fmt.Errorf("error parsing document: %w", err)
		}

		// 修改labels字段，添加enable_scope:x
		updatedLabels := addEnableScopeLabel(originalDoc.Labels, params.EnableScope)

		// 构建更新脚本
		updateScript := buildUpdateScript(updatedLabels)

		// 添加批量操作头
		updateCmd := map[string]interface{}{
			"update": map[string]interface{}{
				"_index": index,
				"_id":    docID,
			},
		}
		logx.D(ctx, "-------------bulkUpdateDocuments index:%s, _id:%s", index, docID)

		updateCmdBytes, _ := json.Marshal(updateCmd)
		bulkBody.Write(updateCmdBytes)
		bulkBody.WriteString("\n")

		// 添加更新内容
		bulkBody.WriteString(string(updateScript))
		bulkBody.WriteString("\n")
	}

	// 执行批量操作
	req := esapi.BulkRequest{
		Body: strings.NewReader(bulkBody.String()),
	}
	res, err := req.Do(ctx, es)
	if err != nil {
		return 0, fmt.Errorf("error executing bulk request: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return 0, fmt.Errorf("bulk error: %s", res.String())
	}

	// 解析响应
	var bulkResp BulkResponse
	if err := json.NewDecoder(res.Body).Decode(&bulkResp); err != nil {
		return 0, fmt.Errorf("error parsing bulk response: %w", err)
	}

	// 统计成功数量
	var successCount int64
	for _, item := range bulkResp.Items {
		for _, action := range item {
			if action.Status >= 200 && action.Status < 300 {
				successCount++
			} else {
				logx.I(ctx, "文档更新失败: ID=%s, 错误: %s", action.ID, action.Error.Reason)
			}
		}
	}

	return successCount, nil
}

// addEnableScopeLabel 在labels数组中添加enable_scope:3
func addEnableScopeLabel(labels []string, enableScope int) []string {
	enableScopeLabel := fmt.Sprintf("enable_scope:%s", entity.EnableScopeDb2Label[uint32(enableScope)])

	// 检查是否已经存在enable_scope标签
	for _, label := range labels {
		if strings.HasPrefix(label, "enable_scope:") {
			// 如果已存在，先移除旧的enable_scope标签
			newLabels := make([]string, 0, len(labels))
			for _, l := range labels {
				if !strings.HasPrefix(l, "enable_scope:") {
					newLabels = append(newLabels, l)
				}
			}
			newLabels = append(newLabels, enableScopeLabel)
			return newLabels
		}
	}

	// 如果不存在，直接添加
	return append(labels, enableScopeLabel)
}

// buildUpdateScript 构建更新脚本
func buildUpdateScript(updatedLabels []string) string {
	updateScript := map[string]interface{}{
		"doc": map[string]interface{}{
			"labels": updatedLabels,
		},
	}

	scriptBytes, _ := json.Marshal(updateScript)
	return string(scriptBytes)
}

func modifyDbTable2Vdb(ctx context.Context,
	robotID, corpBizID, appBizID, dbTableBizID, embeddingVersion uint64, embeddingName string, enableScope int) error {
	logx.I(ctx,
		"-------------modifyDbTable2Vdb| robotID: %v, corpBizID: %v, appBizID: %v, dbTableBizID: %v, embeddingVersion:%v", robotID,
		corpBizID, appBizID, dbTableBizID, embeddingVersion)

	maxID := uint64(0)
	for {
		topValues, err := GetCmdService().DbDao.GetTopValuesPageByDbTableBizID(ctx, corpBizID, appBizID, dbTableBizID, maxID, 50)
		if err != nil {
			return err
		}
		if len(topValues) == 0 {
			break
		}
		maxID = topValues[len(topValues)-1].ID
		logx.D(ctx, "-------------modifyDbTable2Vdb|dbTableBizID: %v, maxID: %v", dbTableBizID, maxID)

		// 先清空数据
		var topValueBizIDs []uint64
		for _, value := range topValues {
			topValueBizIDs = append(topValueBizIDs, value.BusinessID)
		}
		err = GetCmdService().DbDao.DeleteVector(ctx, robotID, appBizID, embeddingVersion, embeddingName, topValueBizIDs, retrieval.EnvType_Test)
		if err != nil {
			logx.E(ctx, "DeleteVector failed, err:%+v", err)
			return err
		}
		logx.D(ctx, "-------------modifyDbTable2Vdb|dbTableBizID: %v, Do delete topValueBizIDs:%v", dbTableBizID, topValueBizIDs)

		// 再添加
		err = addDbVector(ctx, robotID, appBizID, dbTableBizID, embeddingVersion, enableScope, topValues)
		if err != nil {
			logx.E(ctx,
				"modifyDbTable2Vdb| robotID: %v, corpBizID: %v, appBizID: %v, dbTableBizID: %v, embeddingVersion:%v, enableScope: %d, failed, %v",
				robotID, corpBizID, appBizID, dbTableBizID, embeddingVersion, enableScope, err)
			return err
		}
	}
	return nil
}

func addDbVector(ctx context.Context, robotId, appBizID, dbTableBizID, embeddingVersion uint64,
	enableScope int, topValues []*database.TableTopValue) error {
	for _, topValueChunk := range slicex.Chunk(topValues, 100) {
		knowledgeList := make([]*retrieval.KnowledgeData, 0, len(topValueChunk))
		for _, topValue := range topValueChunk {
			labels := make([]*retrieval.VectorLabel, 0)
			labels = append(labels, &retrieval.VectorLabel{ // 表的业务id统一写标签 todo role labels.
				Name:  database.LabelDBTableBizID,
				Value: cast.ToString(dbTableBizID),
			})
			labels = append(labels, &retrieval.VectorLabel{
				Name:  "enable_scope",
				Value: entity.EnableScopeDb2Label[uint32(enableScope)],
			})
			knowledgeList = append(knowledgeList, &retrieval.KnowledgeData{
				Id:          topValue.BusinessID,
				PageContent: fmt.Sprintf("%v;%v;%v", topValue.ColumnName, topValue.ColumnComment, topValue.ColumnValue),
				Labels:      labels,
			})
		}

		req := &retrieval.BatchAddKnowledgeReq{
			RobotId:          robotId,
			IndexId:          entity.DbSourceVersionID,
			DocType:          entity.DocTypeSegment,
			EmbeddingVersion: embeddingVersion,
			BotBizId:         appBizID,
			EnvType:          retrieval.EnvType_Test,
			Type:             retrieval.KnowledgeType_KNOWLEDGE,
			IsVector:         true,
			Knowledge:        knowledgeList,
		}
		logx.I(ctx, "addDbVector|req :%v", req)
		_, err := GetCmdService().RpcImpl.RetrievalDirectIndex.BatchAddKnowledge(ctx, req)
		if err != nil {
			logx.E(ctx, "addDbVector|req :%v input vdb error: %v", req, err)
			return err
		}
	}
	return nil
}

func RunCmdDbRevert(cmd *cobra.Command, args []string) error {
	return RunEnableScopeCommand(cmd, RevertAppDb, EnableScopeParams{
		Uin:                  flagDBRevertUin,
		AppBizIDs:            flagDBRevertAppBizIDs,
		SpaceID:              flagDBRevertSpaceID,
		All:                  flagDBRevertAll,
		TypeName:             "Db",
		SkipEmbeddingUpgrade: true, // Revert操作跳过embedding升级
	}, &AppWorkerConfig{})
}

func RevertAppDb(ctx context.Context, app *entity.App, config *AppWorkerConfig) error {
	logx.I(ctx, "-------------Do app: %+v", app)
	corpID := app.CorpPrimaryId
	robotId := app.PrimaryId
	embeddingVersion, embeddingName, err := GetCmdService().DbLogic.GetAppEmbeddingInfoById(ctx, app.BizId)
	if err != nil {
		logx.E(ctx, "GetAppEmbeddingVersionById failed, get app embedding version failed: %+v", err)
		return err
	}
	logx.I(ctx, "-------------embeddingVersion=%d, embeddingName=%s", embeddingVersion, embeddingName)

	dbFilter := &database.DatabaseFilter{
		AppBizID:  app.BizId,
		WithTable: true,
	}
	dbs, _, err := GetCmdService().DbLogic.DescribeDatabaseList(ctx, dbFilter)
	if err != nil {
		logx.E(ctx, "DescribeDatabaseList err:%+v", err)
		return err
	}
	logx.I(ctx, "-------------get dbs: %+v", dbs)

	// t_dev_release_relation_info
	qaQuery := GetCmdService().QaLogic.GetDao().Query()
	dbDevReleaseRelationInfoList, err := qaQuery.TDevReleaseRelationInfo.WithContext(ctx).
		Where(qaQuery.TDevReleaseRelationInfo.CorpID.Eq(cast.ToUint64(corpID))).
		Where(qaQuery.TDevReleaseRelationInfo.RobotID.Eq(robotId)).
		Where(qaQuery.TDevReleaseRelationInfo.Type.In(4, 5)).Find() // 4: 数据表 5: 数据库
	if err != nil {
		logx.E(ctx, "create TDevReleaseRelationInfo failed, err:%+v", err)
		return err
	}
	revertDbBizIds := make([]uint64, 0)
	revertTableBizIds := make([]uint64, 0)
	relationIds := make([]uint64, len(dbDevReleaseRelationInfoList))
	for _, dbDevReleaseRelationInfo := range dbDevReleaseRelationInfoList {
		if dbDevReleaseRelationInfo.Type == 4 { // 4: 数据表
			revertTableBizIds = append(revertTableBizIds, dbDevReleaseRelationInfo.ReleaseBusinessID)
		} else if dbDevReleaseRelationInfo.Type == 5 {
			revertDbBizIds = append(revertDbBizIds, dbDevReleaseRelationInfo.ReleaseBusinessID)
		}
		relationIds = append(relationIds, dbDevReleaseRelationInfo.ID)
	}

	q := GetCmdService().DbDao.Query()
	for _, db := range dbs {
		logx.I(ctx, "-------------回滚已复数据库的enable_scope: %+v", db)
		updates := map[string]any{
			"enable_scope": entity.EnableScopeInvalid,
		}
		if slices.Contains(revertDbBizIds, db.DBSourceBizID) {
			logx.I(ctx, "-------------数据库需要被删除: %+v", db)
			updates["is_deleted"] = true
		}
		_, err := q.TDbSource.WithContext(ctx).Where(q.TDbSource.ID.Eq(db.ID)).Updates(updates)
		if err != nil {
			logx.E(ctx, "Failed to update t_db_source, err:%+v", err)
			return err
		}
		for _, previewTable := range db.Tables {
			if slices.Contains(revertTableBizIds, previewTable.DBTableBizID) {
				logx.I(ctx, "-------------回滚已复制的数据表: %+v", previewTable)
				err = GetCmdService().DbLogic.DeleteTable(ctx, &database.TableFilter{
					CorpBizID:    previewTable.CorpBizID,
					AppBizID:     previewTable.AppBizID,
					DBTableBizID: previewTable.DBTableBizID,
					RobotID:      robotId})
				if err != nil {
					logx.E(ctx, "DeleteTable failed, err:%+v")
					return err
				}
			} else {
				logx.I(ctx, "-------------回滚已复数据表的enable_scope: %+v", previewTable)
				_, err := q.TDbTable.WithContext(ctx).Where(q.TDbTable.ID.Eq(previewTable.ID)).Updates(map[string]any{
					"enable_scope": entity.EnableScopeInvalid,
				})
				if err != nil {
					logx.E(ctx, "Failed to update t_db_table, err:%+v", err)
					return err
				}
			}
		}
	}

	_, err = qaQuery.TDevReleaseRelationInfo.WithContext(ctx).
		Where(qaQuery.TDevReleaseRelationInfo.ID.In(relationIds...)).Delete()
	if err != nil {
		logx.E(ctx, "delete TDevReleaseRelationInfo err:%+v, app_id:%d", err, app.PrimaryId)
		return err
	}
	logx.I(ctx, "delete TDevReleaseRelationInfo success, app_id:%d, deleted count:%d", app.PrimaryId, len(relationIds))
	return nil
}
