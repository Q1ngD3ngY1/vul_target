// Package service 业务逻辑层
package service

import (
	pb "git.woa.com/adp/pb-go/kb/kb_config"

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
	kbPKGLogic "git.woa.com/adp/kb/kb-config/internal/logic/kb_package"
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
)

// Service is service logic object
type Service struct {
	pb.UnimplementedAdmin

	rpc           *rpc.RPC
	dao           dao.Dao
	auditLogic    *auditLogic.Logic
	cacheLogic    *localcache.Logic
	docLogic      *docLogic.Logic
	qaLogic       *qa.Logic
	segLogic      *segLogic.Logic
	releaseLogic  *releaseLogic.Logic
	kbLogic       *kb.Logic
	cateLogic     *category.Logic
	userLogic     *user.Logic
	labelLogic    *label.Logic
	dbLogic       *dbLogic.Logic
	exportLogic   *export.Logic
	taskLogic     *task.Logic
	vectorLogic   *vector.VectorSyncLogic
	dbDao         dbdao.Dao
	s3            dao.S3
	kbDao         kbdao.Dao
	labelDao      labeldao.Dao
	financeLogic  *finance.Logic
	promptDao     llm.Dao
	thirdDocLogic *third_document.Logic
	kbPKGLogic    *kbPKGLogic.Logic
	AdminRdb      types.AdminRedis
}

// New creates service instance

func New(rpc *rpc.RPC, d dao.Dao, auditLogic *auditLogic.Logic, cacheLogic *localcache.Logic, docLogic *docLogic.Logic, qaLogic *qa.Logic,
	segLogic *segLogic.Logic, releaseLogic *releaseLogic.Logic, kbLogic *kb.Logic, s3 dao.S3,
	cateLogic *category.Logic, userLogic *user.Logic, labelLogic *label.Logic, taskLogic *task.Logic,
	dbLogic *dbLogic.Logic, exportLogic *export.Logic, vectorLogic *vector.VectorSyncLogic, dbDao dbdao.Dao,
	kbDao kbdao.Dao, labelDao labeldao.Dao, financeLogic *finance.Logic, adminRdb types.AdminRedis, promptDao llm.Dao, thirdDocLogic *third_document.Logic,
	kbPKGLogic *kbPKGLogic.Logic,
) *Service {
	srv := Service{
		rpc:           rpc,
		auditLogic:    auditLogic,
		cacheLogic:    cacheLogic,
		docLogic:      docLogic,
		qaLogic:       qaLogic,
		segLogic:      segLogic,
		releaseLogic:  releaseLogic,
		dao:           d,
		kbLogic:       kbLogic,
		cateLogic:     cateLogic,
		userLogic:     userLogic,
		labelLogic:    labelLogic,
		dbLogic:       dbLogic,
		exportLogic:   exportLogic,
		taskLogic:     taskLogic,
		vectorLogic:   vectorLogic,
		dbDao:         dbDao,
		s3:            s3,
		kbDao:         kbDao,
		labelDao:      labelDao,
		financeLogic:  financeLogic,
		AdminRdb:      adminRdb,
		promptDao:     promptDao,
		thirdDocLogic: thirdDocLogic,
		kbPKGLogic:    kbPKGLogic,
	}

	return &srv
}

// GetVectorLogic 返回VectorLogic 实例
func (s *Service) GetVectorLogic() *vector.VectorSyncLogic {
	return s.vectorLogic
}
