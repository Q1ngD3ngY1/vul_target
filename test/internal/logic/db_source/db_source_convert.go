package db_source

import (
	"errors"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
	"github.com/spf13/cast"
)

func DBSourceModelToViewProto(m *model.DBSource, tableNames []string, tableNum int32) (*pb.DbSourceView, error) {
	if m == nil {
		return nil, errors.New("model is nil")
	}
	password := ""
	return &pb.DbSourceView{
		DbSourceBizId: m.DBSourceBizID,
		DbName:        m.DBName,
		AliasName:     m.AliasName,
		Description:   m.Description,
		DbType:        m.DBType,
		Host:          m.Host,
		Port:          int32(m.Port),
		Alive:         m.Alive,
		Username:      m.Username,
		Password:      password,
		TableNames:    tableNames,
		LastSyncTime:  m.LastSyncTime.Unix(), // 转换为Unix时间戳(int64)
		CreateTime:    m.CreateTime.Unix(),   // 转换为Unix时间戳(int64)
		Status:        uint32(m.ReleaseStatus),
		IsEnabled:     m.IsIndexed,
		TableNum:      tableNum,
	}, nil
}

func DBSourceModelsToViews(models []*model.DBSource, staffByID map[uint64]string, tableNum map[uint64]int32) ([]*pb.DbSourceView, error) {
	result := make([]*pb.DbSourceView, 0, len(models))
	if tableNum == nil {
		tableNum = make(map[uint64]int32)
	}
	for _, m := range models {
		if v, err := DBSourceModelToViewProto(m, nil, tableNum[m.DBSourceBizID]); v != nil {
			if staffName, ok := staffByID[m.StaffID]; ok {
				v.StaffName = staffName
			} else {
				v.StaffName = cast.ToString(m.StaffID)
			}
			result = append(result, v)
			if err != nil {
				return nil, err
			}
		}
	}
	return result, nil
}

func DBTableModelToView(m *model.DBTable, isShared bool) (*pb.DbTableView, error) {
	if m == nil {
		return nil, errors.New("model is nil")
	}
	status := uint32(m.ReleaseStatus)
	if m.LearnStatus == model.LearnStatusLearning {
		status = uint32(model.FaceStatusLearning)
	}
	if m.LearnStatus == model.LearnStatusFailed {
		status = uint32(model.FaceStatusLearnFailed)
	}
	if isShared && m.LearnStatus == model.LearnStatusLearned {
		status = uint32(model.FaceStatusLearnSuccess)
	}
	return &pb.DbTableView{
		DbTableBizId:      m.DBTableBizID,
		TableName:         m.Name,
		TableSchema:       m.TableSchema,
		AliasName:         m.AliasName,
		Description:       m.Description,
		RowCount:          int32(m.RowCount),
		ColumnCount:       int32(m.ColumnCount),
		TableAddedTime:    m.TableAddedTime.Unix(),
		TableModifiedTime: m.UpdateTime.Unix(),
		// (1 待发布 2 发布中 3 已发布 4 发布失败 5 学习中 6 学习失败 7 导入完成)
		Status:    status,
		IsEnabled: m.IsIndexed,
		IsDeleted: !m.Alive,
	}, nil
}

func DBTableModelToViews(models []*model.DBTable, staffByID map[uint64]string, appBizID2Shared map[uint64]bool) ([]*pb.DbTableView, error) {
	result := make([]*pb.DbTableView, 0, len(models))
	if appBizID2Shared == nil {
		appBizID2Shared = make(map[uint64]bool)
	}
	for _, m := range models {
		if v, err := DBTableModelToView(m, appBizID2Shared[m.AppBizID]); v != nil {
			if staffName, ok := staffByID[m.StaffID]; ok {
				v.StaffName = staffName
			} else {
				v.StaffName = cast.ToString(m.StaffID)
			}
			result = append(result, v)
			if err != nil {
				return nil, err
			}
		}
	}
	return result, nil
}

func DBTableColumnsToView(m *model.DBTableColumn) (*pb.DbTableColumnView, error) {
	if m == nil {
		return nil, errors.New("model is nil")
	}
	return &pb.DbTableColumnView{
		DbTableColumnBizId: m.DBTableColumnBizID,
		ColumnName:         m.ColumnName,
		DataType:           m.DataType,
		AliasName:          m.AliasName,
		Description:        m.Description,
		Unit:               m.Unit,
		IsIndexed:          m.IsIndexed,
	}, nil
}

func DBTableColumnsToViews(models []*model.DBTableColumn) ([]*pb.DbTableColumnView, error) {
	result := make([]*pb.DbTableColumnView, 0, len(models))
	for _, m := range models {
		if v, err := DBTableColumnsToView(m); v != nil {
			result = append(result, v)
			if err != nil {
				return nil, err
			}
		}
	}
	return result, nil
}

func Text2sqlTableColumnsToView(m *model.DBTableColumn) (*pb.Text2SqlTableColumnView, error) {
	if m == nil {
		return nil, errors.New("model is nil")
	}
	return &pb.Text2SqlTableColumnView{
		DbTableColumnBizId: m.DBTableColumnBizID,
		ColumnName:         m.ColumnName,
		AliasName:          m.AliasName,
		Description:        m.Description,
		DataType:           m.DataType,
		Unit:               m.Unit,
	}, nil
}

func Text2sqlColumnsToViews(models []*model.DBTableColumn) ([]*pb.Text2SqlTableColumnView, error) {
	result := make([]*pb.Text2SqlTableColumnView, 0, len(models))
	for _, m := range models {
		if v, err := Text2sqlTableColumnsToView(m); v != nil {
			result = append(result, v)
			if err != nil {
				return nil, err
			}
		}
	}
	return result, nil
}

func DbSourceWithTablesToView(dbSource *model.DBSource, dbTables []*model.DBTable) *pb.DbSourceWithTables {
	if dbSource == nil {
		return nil
	}
	dbSourceView := &pb.DbSourceView{
		DbSourceBizId: dbSource.DBSourceBizID,
		DbName:        dbSource.DBName,
		Alive:         dbSource.Alive,
		AliasName:     dbSource.AliasName,
		DbType:        dbSource.DBType,
		Description:   dbSource.Description,
		LastSyncTime:  dbSource.LastSyncTime.Unix(),
	}
	dbTablesView := make([]*pb.DbTableView, 0, len(dbTables))
	for _, dbTable := range dbTables {
		if dbTable == nil {
			continue
		}
		dbTableView := &pb.DbTableView{
			DbTableBizId: dbTable.DBTableBizID,
			TableName:    dbTable.Name,
		}
		dbTablesView = append(dbTablesView, dbTableView)
	}
	return &pb.DbSourceWithTables{
		DbSource: dbSourceView,
		DbTables: dbTablesView,
	}
}

func DbTableWithColumnsToView(dbTable *model.DBTable, dbTableColumns []*model.DBTableColumn) *pb.DbTableWithColumns {
	dbTableView := &pb.DbTableView{
		DbTableBizId: dbTable.DBTableBizID,
		TableName:    dbTable.Name,
	}
	dbTableColumnsView := make([]*pb.DbTableColumnView, 0, len(dbTableColumns))
	for _, dbTableColumn := range dbTableColumns {
		dbTableColumnView := &pb.DbTableColumnView{
			DbTableColumnBizId: dbTableColumn.DBTableColumnBizID,
			ColumnName:         dbTableColumn.ColumnName,
		}
		dbTableColumnsView = append(dbTableColumnsView, dbTableColumnView)
	}
	return &pb.DbTableWithColumns{
		DbTable:   dbTableView,
		DbColumns: dbTableColumnsView,
	}
}

func DbSourceWithTableToView(dbSource *model.DBSource, dbTables []*model.DBTable) *pb.DbSourceBizIDItem {
	if dbSource == nil {
		return nil
	}
	view := &pb.DbSourceBizIDItem{
		DbSourceBizId: dbSource.DBSourceBizID,
	}
	for _, dbTable := range dbTables {
		view.DbSourceTableBizId = append(view.DbSourceTableBizId, dbTable.DBTableBizID)
	}
	return view
}
