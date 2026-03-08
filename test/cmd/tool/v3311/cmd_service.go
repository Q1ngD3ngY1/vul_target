package main

import (
	"git.woa.com/adp/kb/kb-config/internal/dao"
	dbdao "git.woa.com/adp/kb/kb-config/internal/dao/database"
	kbdao "git.woa.com/adp/kb/kb-config/internal/dao/kb"
	labeldao "git.woa.com/adp/kb/kb-config/internal/dao/label"
	"git.woa.com/adp/kb/kb-config/internal/dao/llm"
	"git.woa.com/adp/kb/kb-config/internal/dao/types"
	auditLogic "git.woa.com/adp/kb/kb-config/internal/logic/audit"
	"git.woa.com/adp/kb/kb-config/internal/logic/category"
	dbLogic "git.woa.com/adp/kb/kb-config/internal/logic/database"
	docLogic "git.woa.com/adp/kb/kb-config/internal/logic/document"
	"git.woa.com/adp/kb/kb-config/internal/logic/export"
	"git.woa.com/adp/kb/kb-config/internal/logic/finance"
	"git.woa.com/adp/kb/kb-config/internal/logic/kb"
	"git.woa.com/adp/kb/kb-config/internal/logic/label"
	"git.woa.com/adp/kb/kb-config/internal/logic/localcache"
	"git.woa.com/adp/kb/kb-config/internal/logic/qa"
	releaseLogic "git.woa.com/adp/kb/kb-config/internal/logic/release"
	segLogic "git.woa.com/adp/kb/kb-config/internal/logic/segment"
	"git.woa.com/adp/kb/kb-config/internal/logic/task"
	"git.woa.com/adp/kb/kb-config/internal/logic/third_document"
	"git.woa.com/adp/kb/kb-config/internal/logic/user"
	"git.woa.com/adp/kb/kb-config/internal/logic/vector"
	"git.woa.com/adp/kb/kb-config/internal/rpc"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
	elasticv8 "github.com/elastic/go-elasticsearch/v8"
)

// CmdService is service logic object
type CmdService struct {
	pb.UnimplementedAdmin

	RpcImpl       *rpc.RPC
	Dao           dao.Dao
	AuditLogic    *auditLogic.Logic
	CacheLogic    *localcache.Logic
	DocLogic      *docLogic.Logic
	QaLogic       *qa.Logic
	SegLogic      *segLogic.Logic
	ReleaseLogic  *releaseLogic.Logic
	KbLogic       *kb.Logic
	CateLogic     *category.Logic
	UserLogic     *user.Logic
	LabelLogic    *label.Logic
	DbLogic       *dbLogic.Logic
	ExportLogic   *export.Logic
	TaskLogic     *task.Logic
	VectorLogic   *vector.VectorSyncLogic
	DbDao         dbdao.Dao
	S3            dao.S3
	KbDao         kbdao.Dao
	LabelDao      labeldao.Dao
	FinanceLogic  *finance.Logic
	PromptDao     llm.Dao
	ThirdDocLogic *third_document.Logic

	AdminRdb types.AdminRedis
	EsClient *elasticv8.TypedClient
}

// New creates service instance

func New(rpc *rpc.RPC, d dao.Dao, auditLogic *auditLogic.Logic, cacheLogic *localcache.Logic, docLogic *docLogic.Logic, qaLogic *qa.Logic,
	segLogic *segLogic.Logic, releaseLogic *releaseLogic.Logic, kbLogic *kb.Logic, s3 dao.S3,
	cateLogic *category.Logic, userLogic *user.Logic, labelLogic *label.Logic, taskLogic *task.Logic,
	dbLogic *dbLogic.Logic, exportLogic *export.Logic, vectorLogic *vector.VectorSyncLogic, dbDao dbdao.Dao,
	kbDao kbdao.Dao, labelDao labeldao.Dao, financeLogic *finance.Logic, adminRdb types.AdminRedis, promptDao llm.Dao, thirdDocLogic *third_document.Logic,
	esClient *elasticv8.TypedClient) *CmdService {
	srv := CmdService{
		RpcImpl:       rpc,
		AuditLogic:    auditLogic,
		CacheLogic:    cacheLogic,
		DocLogic:      docLogic,
		QaLogic:       qaLogic,
		SegLogic:      segLogic,
		ReleaseLogic:  releaseLogic,
		Dao:           d,
		KbLogic:       kbLogic,
		CateLogic:     cateLogic,
		UserLogic:     userLogic,
		LabelLogic:    labelLogic,
		DbLogic:       dbLogic,
		ExportLogic:   exportLogic,
		TaskLogic:     taskLogic,
		VectorLogic:   vectorLogic,
		DbDao:         dbDao,
		S3:            s3,
		KbDao:         kbDao,
		LabelDao:      labelDao,
		FinanceLogic:  financeLogic,
		AdminRdb:      adminRdb,
		PromptDao:     promptDao,
		ThirdDocLogic: thirdDocLogic,
		EsClient:      esClient,
	}

	return &srv
}
