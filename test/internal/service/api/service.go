// Package api 业务逻辑层
package api

import (
	"context"

	"git.woa.com/adp/kb/kb-config/internal/dao"
	dbdao "git.woa.com/adp/kb/kb-config/internal/dao/database"
	kbdao "git.woa.com/adp/kb/kb-config/internal/dao/kb"
	"git.woa.com/adp/kb/kb-config/internal/dao/label"
	qadao "git.woa.com/adp/kb/kb-config/internal/dao/qa"
	releaseDao "git.woa.com/adp/kb/kb-config/internal/dao/release"
	auditLogic "git.woa.com/adp/kb/kb-config/internal/logic/audit"
	cateLogic "git.woa.com/adp/kb/kb-config/internal/logic/category"
	dbLogic "git.woa.com/adp/kb/kb-config/internal/logic/database"
	docLogic "git.woa.com/adp/kb/kb-config/internal/logic/document"
	"git.woa.com/adp/kb/kb-config/internal/logic/finance"
	kbLogic "git.woa.com/adp/kb/kb-config/internal/logic/kb"
	kbPKGLogic "git.woa.com/adp/kb/kb-config/internal/logic/kb_package"
	labelLogic "git.woa.com/adp/kb/kb-config/internal/logic/label"
	cacheLogic "git.woa.com/adp/kb/kb-config/internal/logic/localcache"
	qaLogic "git.woa.com/adp/kb/kb-config/internal/logic/qa"
	releaseLogic "git.woa.com/adp/kb/kb-config/internal/logic/release"
	searchLogic "git.woa.com/adp/kb/kb-config/internal/logic/search"
	segLogic "git.woa.com/adp/kb/kb-config/internal/logic/segment"
	"git.woa.com/adp/kb/kb-config/internal/logic/task"
	"git.woa.com/adp/kb/kb-config/internal/logic/user"
	"git.woa.com/adp/kb/kb-config/internal/rpc"
	"git.woa.com/adp/kb/kb-config/internal/service"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
)

// Service is service logic object
type Service struct {
	pb.UnimplementedApi

	rpc          *rpc.RPC
	svc          *service.Service
	dao          dao.Dao
	auditLogic   *auditLogic.Logic
	docLogic     *docLogic.Logic
	qaLogic      *qaLogic.Logic
	segLogic     *segLogic.Logic
	releaseLogic *releaseLogic.Logic
	userLogic    *user.Logic
	cateLogic    *cateLogic.Logic
	cacheLogic   *cacheLogic.Logic
	kbLogic      *kbLogic.Logic
	labelLogic   *labelLogic.Logic
	dbLogic      *dbLogic.Logic
	taskLogic    *task.Logic
	dbDao        dbdao.Dao
	kbDao        kbdao.Dao
	labelDao     label.Dao
	releaseDao   releaseDao.Dao
	qaDao        qadao.Dao
	searchLogic  *searchLogic.Logic
	s3           dao.S3
	financeLogic *finance.Logic
	kbPKGLogic   *kbPKGLogic.Logic
}

func (s *Service) SearchKnowledgeInfo(ctx context.Context, req *pb.SearchKnowledgeInfoReq) (*pb.SearchKnowledgeInfoRsp, error) {
	// TODO implement me
	panic("implement me")
}

// New creates service instance
func New(svc *service.Service, d dao.Dao, rpc *rpc.RPC, auditLogic *auditLogic.Logic,
	docLogic *docLogic.Logic, qaLogic *qaLogic.Logic, s3 dao.S3,
	cacheLogic *cacheLogic.Logic,
	segLogic *segLogic.Logic, releaseLogic *releaseLogic.Logic, searchLogic *searchLogic.Logic,
	userLogic *user.Logic, cateLogic *cateLogic.Logic, dbLogic *dbLogic.Logic, dbDao dbdao.Dao, labelDao label.Dao,
	kbLogic *kbLogic.Logic, labelLogic *labelLogic.Logic, taskLogic *task.Logic, kbDao kbdao.Dao, releaseDao releaseDao.Dao,
	qaDao qadao.Dao, financeLogic *finance.Logic, kbPKGLogic *kbPKGLogic.Logic,
) *Service {
	srv := Service{
		rpc:          rpc,
		svc:          svc,
		dao:          d,
		auditLogic:   auditLogic,
		docLogic:     docLogic,
		qaLogic:      qaLogic,
		segLogic:     segLogic,
		releaseLogic: releaseLogic,
		labelLogic:   labelLogic,
		userLogic:    userLogic,
		cateLogic:    cateLogic,
		cacheLogic:   cacheLogic,
		kbLogic:      kbLogic,
		dbLogic:      dbLogic,
		searchLogic:  searchLogic,
		taskLogic:    taskLogic,
		dbDao:        dbDao,
		labelDao:     labelDao,
		releaseDao:   releaseDao,
		qaDao:        qaDao,
		s3:           s3,
		kbDao:        kbDao,
		financeLogic: financeLogic,
		kbPKGLogic:   kbPKGLogic,
	}
	return &srv
}
