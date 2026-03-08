package document

import (
	"context"
	"fmt"

	"git.code.oa.com/trpc-go/trpc-database/localcache"
	"git.woa.com/adp/common/x/gox"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/mathx/randx"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/model"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/dao"
	cateDao "git.woa.com/adp/kb/kb-config/internal/dao/category"
	dbdao "git.woa.com/adp/kb/kb-config/internal/dao/database"
	docDao "git.woa.com/adp/kb/kb-config/internal/dao/document"
	kbDao "git.woa.com/adp/kb/kb-config/internal/dao/kb"
	labelDao "git.woa.com/adp/kb/kb-config/internal/dao/label"
	qaDao "git.woa.com/adp/kb/kb-config/internal/dao/qa"
	releaseDao "git.woa.com/adp/kb/kb-config/internal/dao/release"
	segDao "git.woa.com/adp/kb/kb-config/internal/dao/segment"
	userDao "git.woa.com/adp/kb/kb-config/internal/dao/user"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
	"git.woa.com/adp/kb/kb-config/internal/logic/finance"
	"git.woa.com/adp/kb/kb-config/internal/logic/label"
	"git.woa.com/adp/kb/kb-config/internal/logic/llm"
	"git.woa.com/adp/kb/kb-config/internal/logic/third_document"
	"git.woa.com/adp/kb/kb-config/internal/rpc"
	"git.woa.com/adp/kb/kb-config/internal/util/idgen"
	"git.woa.com/adp/kb/kb-config/pkg/i18nkey"

	qaLogic "git.woa.com/adp/kb/kb-config/internal/logic/qa"
	segLogic "git.woa.com/adp/kb/kb-config/internal/logic/segment"
)

type Logic struct {
	rawSqlDao     dao.Dao
	docDao        docDao.Dao
	labelDao      labelDao.Dao
	segDao        segDao.Dao
	releaseDao    releaseDao.Dao
	qaDao         qaDao.Dao
	dbDao         dbdao.Dao
	kbDao         kbDao.Dao
	cateDao       cateDao.Dao
	userDao       userDao.Dao
	segLogic      *segLogic.Logic
	qaLogic       *qaLogic.Logic
	labelLogic    *label.Logic
	rpc           *rpc.RPC
	s3            dao.S3
	llmLogic      *llm.Logic
	thirdDocLogic *third_document.Logic
	financeLogic  *finance.Logic
}

func NewLogic(
	rawSqlDao dao.Dao,
	docDao docDao.Dao,
	labelDao labelDao.Dao,
	segDao segDao.Dao,
	releaseDao releaseDao.Dao,
	qaDao qaDao.Dao,
	dbDao dbdao.Dao,
	segLogic *segLogic.Logic,
	qaLogic *qaLogic.Logic,
	labelLogic *label.Logic,
	rpc *rpc.RPC,
	s3 dao.S3,
	kbDao kbDao.Dao,
	cateDao cateDao.Dao,
	llmLogic *llm.Logic,
	thirdDocLogic *third_document.Logic,
	financeLogic *finance.Logic,
	userDao userDao.Dao,
) *Logic {

	initFileConfig()

	return &Logic{
		rawSqlDao:     rawSqlDao,
		docDao:        docDao,
		labelDao:      labelDao,
		segDao:        segDao,
		releaseDao:    releaseDao,
		qaDao:         qaDao,
		dbDao:         dbDao,
		kbDao:         kbDao,
		cateDao:       cateDao,
		userDao:       userDao,
		segLogic:      segLogic,
		qaLogic:       qaLogic,
		labelLogic:    labelLogic,
		rpc:           rpc,
		s3:            s3,
		llmLogic:      llmLogic,
		thirdDocLogic: thirdDocLogic,
		financeLogic:  financeLogic,
	}
}

const (
	expiration = 10 // 10 sec
	capacity   = 10000
)

var (
	modelType2DefaultConfigCache localcache.Cache
)

func init() {
	modelType2DefaultConfigCache = localcache.New(localcache.WithExpiration(expiration), localcache.WithCapacity(capacity))
}

// GenerateCOSFileName 生成 COS 文件名
// 格式: {random20}-{id}.{fileType}
func GenerateCOSFileName(fileType string) string {
	if len(fileType) == 0 {
		return ""
	}
	random := randx.RandomString(20, randx.WithMode(randx.AlphabetMode))
	return fmt.Sprintf("%s-%d.%s", random, idgen.GetId(), fileType)
}

func (l *Logic) GetDao() docDao.Dao {
	return l.docDao
}

func (l *Logic) getDocTableName() string {
	return model.TableNameTDoc
}

func (l *Logic) GetRpcCli() *rpc.RPC {
	return l.rpc
}

// getOfflineFileManagerVersion 离线文档解析服务版本号
func (l *Logic) getOfflineFileManagerVersion() int32 {
	// 实时文档解析版本号
	fileManagerVersion := config.GetMainConfig().FileParseConfig.OfflineFileManagerVersion
	if fileManagerVersion <= 0 {
		fileManagerVersion = defaultOfflineFileManagerVersion
	}
	return int32(fileManagerVersion)
}

func (l *Logic) sendExcelImportNotice(ctx context.Context, staffID uint64, doc *docEntity.Doc) error {
	if !doc.IsExcel() {
		return nil
	}
	operations := make([]releaseEntity.Operation, 0)
	noticeOptions := []releaseEntity.NoticeOption{
		releaseEntity.WithPageID(releaseEntity.NoticeQAPageID),
		releaseEntity.WithLevel(releaseEntity.LevelInfo),
		releaseEntity.WithContent(i18n.Translate(ctx, i18nkey.KeyQATemplateImportingWithParam, doc.FileName)),
		releaseEntity.WithForbidCloseFlag(),
	}
	notice := releaseEntity.NewNotice(releaseEntity.NoticeTypeDocToQA, doc.ID, doc.CorpID, doc.RobotID, staffID, noticeOptions...)
	if err := notice.SetOperation(operations); err != nil {
		logx.E(ctx, "序列化通知操作参数失败 operations:%+v err:%+v", operations, err)
		return err
	}

	if err := l.rpc.PlatformAdmin.CreateNotice(ctx, notice); err != nil {
		return err
	}
	return nil
}

func (l *Logic) sendDocModifyNotice(ctx context.Context, staffID uint64, doc *docEntity.Doc,
	content, level string) error {
	operations := make([]releaseEntity.Operation, 0)
	noticeOptions := []releaseEntity.NoticeOption{
		releaseEntity.WithPageID(releaseEntity.NoticeDocPageID),
		releaseEntity.WithLevel(level),
		releaseEntity.WithContent(i18n.Translate(ctx, content, doc.GetRealFileName())),
	}
	switch level {
	case releaseEntity.LevelSuccess:
		noticeOptions = append(noticeOptions, releaseEntity.WithSubject(i18n.Translate(ctx, i18nkey.KeyDocumentUpdateSuccess)))
		noticeOptions = append(noticeOptions, releaseEntity.WithGlobalFlag())
	case releaseEntity.LevelError:
		noticeOptions = append(noticeOptions, releaseEntity.WithSubject(i18n.Translate(ctx, i18nkey.KeyDocumentUpdateFailure)))
		noticeOptions = append(noticeOptions, releaseEntity.WithGlobalFlag())
	case releaseEntity.LevelInfo:
		noticeOptions = append(noticeOptions, releaseEntity.WithForbidCloseFlag())
	}
	notice := releaseEntity.NewNotice(releaseEntity.NoticeTypeDocModify, doc.ID, doc.CorpID, doc.RobotID, staffID, noticeOptions...)
	if err := notice.SetOperation(operations); err != nil {
		logx.E(ctx, "Failed to serialize operations:%+v err:%+v", operations, err)
		return err
	}
	gox.GoWithContext(ctx, func(ctx context.Context) {
		if err := l.rpc.PlatformAdmin.CreateNotice(ctx, notice); err != nil {
			logx.E(ctx, "create notice fail, notice: %+v, err: %+v", notice, err)
		}
	})
	return nil
}
