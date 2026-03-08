// Package dao 与外界系统做数据交换，如访问 http/cache/mq/database 等
package dao

import (
	"context"

	"net"
	"strings"
	"sync"
	"time"

	tgorm "git.code.oa.com/trpc-go/trpc-database/gorm"
	"git.code.oa.com/trpc-go/trpc-database/mysql"
	"git.code.oa.com/trpc-go/trpc-database/redis"
	"git.code.oa.com/trpc-go/trpc-go"
	"git.code.oa.com/trpc-go/trpc-go/client"
	"git.code.oa.com/trpc-go/trpc-go/http"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/adp/common/llm/prompt"
	"git.woa.com/baicaoyuan/moss/metadata"
	mredis "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao/redis"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao/storage"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/linker"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	cloudModel "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model/cloud"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model/realtime"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/vector"
	taskFlow "git.woa.com/dialogue-platform/lke_proto/pb-protocol/KEP"
	admin "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_admin_config_server"
	knowledgeConfig "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_retrieval_server"
	"git.woa.com/dialogue-platform/lke_proto/pb-protocol/knowledge"
	accessManage "git.woa.com/dialogue-platform/proto/pb-stub/access-manager-server"
	entityextractor "git.woa.com/dialogue-platform/proto/pb-stub/entity-extractor"
	fileManagerServer "git.woa.com/dialogue-platform/proto/pb-stub/file_manager_server"
	llmm "git.woa.com/dialogue-platform/proto/pb-stub/llm-manager-server"
	"git.woa.com/dialogue-platform/proto/pb-stub/nrt_file_manager_server"
	webParserServer "git.woa.com/dialogue-platform/proto/pb-stub/web-parser-server"
	"git.woa.com/ivy/protobuf/trpc-go/qbot/finance/finance"
	"git.woa.com/ivy/protobuf/trpc-go/qbot/qbot/chat"
	"git.woa.com/ivy/protobuf/trpc-go/qbot/qbot/infosec"
	shortURL "git.woa.com/ivy/protobuf/trpc-go/qbot/qbot/short_url"
	"github.com/bwmarrin/snowflake"
	redisV8 "github.com/go-redis/redis/v8"
	"github.com/jmoiron/sqlx"
	cloudsts "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/sts/v20180813"
	"github.com/tencentyun/cos-go-sdk-v5"
	sts "github.com/tencentyun/qcloud-cos-sts-sdk/go"
	"gorm.io/gorm"
)

// Dao is Dao interface
type Dao interface {
	// GetDB 返回DB实例
	GetDB() mysql.Client
	GetTdsqlGormDB() *gorm.DB
	GetGormDB() *gorm.DB
	GetText2sqlGormDB() *gorm.DB
	GetGormDBDelete() *gorm.DB
	GetAdminApiCli() admin.ApiClientProxy
	GetDirectIndexCli() pb.DirectIndexClientProxy
	GetRetrievalCli() pb.RetrievalClientProxy
	GetTaskFlowCli() taskFlow.TaskConfigClientProxy
	GetTDocLinkerCli() http.Client
	GetDocParseCli() fileManagerServer.ManagerObjClientProxy
	GetFinanceCli() finance.FinanceClientProxy
	GetLlmmCli() llmm.ChatClientProxy
	GetPromptCli() prompt.PromptProcessor
	// StorageWithTypeKeyInterface 指定类型存储相关接口
	StorageWithTypeKeyInterface
	// GetCredential 获取cos临时密钥
	GetCredential(ctx context.Context, pathList []string, storageAction string) (*sts.CredentialResult, error)
	// GetPresignedURL 获取Cos预签名URL
	GetPresignedURL(ctx context.Context, key string) (string, error)
	// GetObject 获取 COS 文件
	GetObject(ctx context.Context, key string) ([]byte, error)
	// PutObject 上传 COS 文件
	PutObject(ctx context.Context, bs []byte, key string) error
	// DelObject 删除 COS 文件
	DelObject(ctx context.Context, key string) error
	// StatObject 获取object的元数据信息
	StatObject(ctx context.Context, key string) (*model.ObjectInfo, error)
	// GetObjectETag 获取存储对象的ETag
	GetObjectETag(ctx context.Context, url string) string
	// GetCorpCOSPath 获取企业COS路径
	GetCorpCOSPath(ctx context.Context, corpID uint64) string
	// GetCorpImagePath 获取企业COS图片路径
	GetCorpImagePath(_ context.Context, corpID uint64) string
	// GetCorpRobotCOSPath 获取企业机器人COS路径
	GetCorpRobotCOSPath(ctx context.Context, corpBizID, botBizID uint64, fileName string) string
	// GetCorpAppImagePath 获取企业应用图片路径
	GetCorpAppImagePath(_ context.Context, corpBizID, botBizID uint64, fileName string) string
	// GetCorpCOSFilePath 获取企业COS文件路径
	GetCorpCOSFilePath(ctx context.Context, corpID uint64, filename string) string
	// CheckURLPrefix 校验文件URL前缀
	CheckURLPrefix(ctx context.Context, corpID, corpBizID, botBizID uint64, url string) error
	// CheckURLFile 校验文件URL有效
	CheckURLFile(ctx context.Context, corpID, corpBizID, botBizID uint64, url, eTag string) error
	// CheckURLFileByHash 校验文件URL有效(hash校验)
	CheckURLFileByHash(ctx context.Context, corpID, corpBizID, botBizID uint64, url, hash string) error
	// GetDomain 获取对象存储domain
	GetDomain(ctx context.Context) string
	// GetStorageType 获取对象存储类型
	GetStorageType(ctx context.Context) string
	// GetBucket 获取存储桶
	GetBucket(ctx context.Context) string
	// GetRegion 获取存储桶地域
	GetRegion(ctx context.Context) string
	// GetStaffByUserID 获取企业员工
	GetStaffByUserID(ctx context.Context, userID uint64) (*model.CorpStaff, error)
	// GetStaffByID 获取企业员工
	GetStaffByID(ctx context.Context, id uint64) (*model.CorpStaff, error)
	// GetStaffByIDs 获取企业员工
	GetStaffByIDs(ctx context.Context, ids []uint64) ([]*model.CorpStaff, error)
	// GetStaffNickNameMapByIDs 获取昵称Map
	GetStaffNickNameMapByIDs(ctx context.Context, staffIDs []uint64) (nickNameMap map[uint64]string, err error)
	// GetStaffByBusinessID 获取企业员工
	GetStaffByBusinessID(ctx context.Context, bID uint64) (*model.CorpStaff, error)
	// GetStaffByBusinessIDs 获取企业员工
	GetStaffByBusinessIDs(ctx context.Context, bIDs []uint64) ([]*model.CorpStaff, error)
	// GetStaffCountByCorpID 通过企业ID获取员工数量
	GetStaffCountByCorpID(ctx context.Context, corpID uint64, query string) (uint64, error)
	// GetStaffByCorpID 通过企业ID获取员工列表
	GetStaffByCorpID(ctx context.Context, corpID uint64, query string, excludeStaffIDs []uint64, page,
		pageSize uint32) ([]*model.CorpStaff, error)
	// GetCorpTotal 获取企业总数
	GetCorpTotal(ctx context.Context, corpBizID []uint64, query string, status []uint32) (uint64, error)
	// GetCorpDetails 获取企业详情
	GetCorpDetails(ctx context.Context, corpBizID []uint64, query string, status []uint32, page,
		pageSize uint32) ([]*model.Corp, error)
	// GetCorpStaffTotal 获取企业员工总数
	GetCorpStaffTotal(ctx context.Context, corpBizID uint64, staffBizIDs []uint64, query string,
		status []uint32) (uint64, error)
	// GetCorpStaffList 获取企业员工详情
	GetCorpStaffList(ctx context.Context, corpBizID uint64, staffBizIDs []uint64, query string,
		status []uint32, page, pageSize uint32) ([]*model.CorpStaff, error)
	// GetCorpByCreateUserID 通过企业创建人获取企业
	GetCorpByCreateUserID(ctx context.Context, createUserID uint64) (*model.Corp, error)
	// MigrateCloud 企业迁移
	MigrateCloud(ctx context.Context, corp *model.Corp, user *model.User, corpStaff *model.CorpStaff) error

	// CreateCorp 创建企业
	CreateCorp(ctx context.Context, corp *model.Corp, user *model.User, staff *model.CorpStaff) error
	// CreateTrialCorp 创建试用企业
	CreateTrialCorp(ctx context.Context, corp *model.Corp) error
	// UpdateTrialCorpCreateUser 更新试用企业创建人
	UpdateTrialCorpCreateUser(ctx context.Context, corp *model.Corp) error
	// UpdateCorpStaffGenQA 更新员工生成QA标记
	UpdateCorpStaffGenQA(ctx context.Context, staff *model.CorpStaff) error
	// RecordUserAccessUnCheckQATime 记录访问未检验问答时间
	RecordUserAccessUnCheckQATime(ctx context.Context, robotID, staffID uint64) error
	// RegisterCorp 注册企业
	RegisterCorp(ctx context.Context, corp *model.Corp) error
	// GetCorpByBusinessID 获取企业
	GetCorpByBusinessID(ctx context.Context, bID uint64) (*model.Corp, error)
	// GetCorpByID 获取企业
	GetCorpByID(ctx context.Context, id uint64) (*model.Corp, error)
	// GetCorpByIDs 获取企业
	GetCorpByIDs(ctx context.Context, ids []uint64) (map[uint64]*model.Corp, error)
	// GetCorpByName 通过企业全称获取企业
	GetCorpByName(ctx context.Context, name string) (*model.Corp, error)
	// GetAuditingCorp 获取审核中的企业
	GetAuditingCorp(ctx context.Context, cellphone string) ([]*model.Corp, error)
	// AuditCorp 审核企业
	AuditCorp(ctx context.Context, corp *model.Corp, pass bool) error
	// ModifyCorpRobotQuota 修改企业机器人配额
	ModifyCorpRobotQuota(ctx context.Context, corp *model.Corp) error
	// JoinCorp 加入企业
	JoinCorp(ctx context.Context, staffName string, user *model.User, corp *model.Corp) (*model.CorpStaff, error)
	// ExitCorp 退出企业
	ExitCorp(ctx context.Context, staff *model.CorpStaff) error
	// GetValidCorpBySidAndUin 获取有效的集成商企业信息
	GetValidCorpBySidAndUin(ctx context.Context, sid int, uin string) (*model.Corp, error)
	// GetCorpBySidAndUin 获取集成商企业信息
	GetCorpBySidAndUin(ctx context.Context, uin string) (*model.Corp, error)
	// GetSidByCorpID 获取集成商id
	GetSidByCorpID(ctx context.Context, id uint64) (int, error)

	// IndexRebuild 索引重建
	IndexRebuild(ctx context.Context, appID, versionID uint64) error
	// UpdateRobot 更新机器人属性
	UpdateRobot(ctx context.Context, typ uint32, appDB *model.AppDB, isNeedAudit bool) error
	// GetRobotTotal 获取机器人数量
	GetRobotTotal(ctx context.Context, corpID uint64, name string, botBizIDs []uint64, deleteFlag uint32) (
		uint32, error)
	// GetRobotList 获取机器人列表
	GetRobotList(
		ctx context.Context, corpID uint64, name string, bozBizIDs []uint64, deleteFlag uint32, page, pageSize uint32,
	) ([]*model.AppDB, error)
	// GetRobotInfo 获取机器人信息
	GetRobotInfo(ctx context.Context, corpID, appID uint64) (*model.AppDB, error)
	// UpdateRobotOfOp 运营工具更新robot
	UpdateRobotOfOp(ctx context.Context, robot *model.AppDB) error

	// CreateUser 创建用户
	CreateUser(ctx context.Context, user *model.User) error
	// GetUserByTel 获取用户
	GetUserByTel(ctx context.Context, cellphone string) (*model.User, error)
	// GetExpUserByID 获取体验用户
	GetExpUserByID(ctx context.Context, id uint64) (*model.User, error)
	// GetExpUserByIDs 获取体验用户
	GetExpUserByIDs(ctx context.Context, ids []uint64) ([]*model.User, error)
	// GetUserByID 获取用户信息
	GetUserByID(ctx context.Context, id uint64) (*model.User, error)
	// GetUserBySidAndUin 获取云用户信息
	GetUserBySidAndUin(ctx context.Context, sid int, uin, subAccountUin string) (*model.User, error)
	// GetStaffSession 获取员工session
	GetStaffSession(ctx context.Context, si *model.SystemIntegrator, token, loginUin, loginSubAccountUin string) (
		*model.Session, error)
	// SetStaffSession 设置员工session
	SetStaffSession(ctx context.Context, staff *model.CorpStaff) (string, error)
	// DeleteStaffSession 删除员工session
	DeleteStaffSession(ctx context.Context, token string) error
	// SendSmsCodeV2 发送短信验证码v2
	SendSmsCodeV2(ctx context.Context, mobile string, uin uint64) error
	// CheckSmsCodeV2 校验短信验证码
	CheckSmsCodeV2(ctx context.Context, mobile string, uin uint64, code string) error
	// GetUserPermission 获取用户权限
	GetUserPermission(ctx context.Context, uin, subAccountUin string, permissionIDs []string) (
		[]*model.PermissionInfo, error)
	// VerifyPermission 验证权限
	VerifyPermission(ctx context.Context, uin, subAccountUin, action string) (bool, error)
	// GetUserResource 获取用户资源
	GetUserResource(ctx context.Context, uin, subAccountUin, permissionID string) ([]string, error)
	// VerifyResource 验证资源
	VerifyResource(ctx context.Context, uin, subAccountUin string, botBizID uint64, isShared bool) (bool, error)

	// GetDocList 获取文档列表
	GetDocList(ctx context.Context, req *model.DocListReq) (uint64, []*model.Doc, error)
	// DeleteDocs 删除文档
	DeleteDocs(ctx context.Context, staffID, businessID uint64, docs []*model.Doc) error
	// DeleteDocs 删除应用下超量失效的文档
	DeleteDocsCharSizeExceeded(ctx context.Context, corpID uint64, robotID uint64, reserveTime time.Duration) error
	// DeleteQAs 删除应用下超量失效的问答
	DeleteQAsCharSizeExceeded(ctx context.Context, corpID uint64, robotID uint64, reserveTime time.Duration) error
	// GetDocByID 通过ID获取文档
	GetDocByID(ctx context.Context, id uint64, robotID uint64) (*model.Doc, error)
	// GetDocByIDs 通过ID获取文档
	GetDocByIDs(ctx context.Context, ids []uint64, robotID uint64) (map[uint64]*model.Doc, error)
	// GetDocByBizID 通过业务ID获取文档
	GetDocByBizID(ctx context.Context, bizID uint64, robotID uint64) (*model.Doc, error)
	// GetDocByBizIDs 通过业务ID获取文档
	GetDocByBizIDs(ctx context.Context, bizIDs []uint64, robotID uint64) (map[uint64]*model.Doc, error)
	// IsDocInEditState 判断文档是否正在生成QA或者正在删除
	IsDocInEditState(ctx context.Context, corpID, robotID uint64) (bool, error)
	// GetDocByIDAndFileName 通过ID和文档名称获取文档
	GetDocByIDAndFileName(ctx context.Context, ids []uint64, fileName string) ([]*model.Doc, error)
	// CmdGetQAList  命令行拉取qa
	CmdGetQAList(ctx context.Context, corpID uint64, acceptStatus []uint32, page, pageSize uint32) (
		[]*model.DocQA, error)
	// CmdGetQAListCount  命令行拉取qa
	CmdGetQAListCount(ctx context.Context, corpID uint64, acceptStatus []uint32) (uint64, error)
	// GetDocIDByBusinessID 通过BusinessID获取文档ID
	GetDocIDByBusinessID(ctx context.Context, businessID uint64, robotID uint64) (uint64, error)
	// GetDocByCosHash 通过cos_hash获取文档
	GetDocByCosHash(ctx context.Context, corpID, robotID uint64, cosHash string) (*model.Doc, error)
	// CreateDoc 创建doc（异步任务）
	CreateDoc(ctx context.Context, staffID uint64, doc *model.Doc,
		attributeLabelReq *model.UpdateDocAttributeLabelReq) error
	// CreateDocWithLabel 创建doc（不触发异步任务）
	CreateDocWithLabel(ctx context.Context, doc *model.Doc,
		attributeLabelReq *model.UpdateDocAttributeLabelReq) error
	// GetDocParseByTaskIDAndOpType 获取文档解析任务（指定 TaskID 和 opType）
	GetDocParseByTaskIDAndOpType(ctx context.Context, taskID string, opType int32) (model.DocParse, error)
	// GetDocParseByDocIDAndType 获取文档解析任务（指定 DocID 和 Type）
	GetDocParseByDocIDAndType(ctx context.Context, docID uint64, fType int32, robotID uint64) (model.DocParse, error)
	// GetDocParseByDocID 获取文档解析任务（指定 DocID）
	GetDocParseByDocID(ctx context.Context, docID uint64, robotID uint64) (model.DocParse, error)
	// GetDocParseByDocIDs 获取文档解析任务（指定 DocIDs）
	GetDocParseByDocIDs(ctx context.Context, docIDs []uint64, robotID uint64) ([]model.DocParse, error)
	// GetDocParseByDocIDAndTypeAndStatus 获取文档解析任务（指定 DocID、type、status）
	GetDocParseByDocIDAndTypeAndStatus(ctx context.Context, docID uint64, fType, status uint32, robotID uint64) (model.DocParse, error)
	// DocParseCanBeRetried 获取重试文档列表 获取文档解析任务（指定 DocID、type、status）
	DocParseCanBeRetried(ctx context.Context, docID uint64, fType uint32, status []uint32, robotID uint64) ([]model.DocParse, error)
	// GetDocParses 获取文档解析任务列表
	GetDocParses(ctx context.Context, corpID, robotID uint64) ([]model.DocParse, error)
	// CreateDocParse 新建文档解析任务
	CreateDocParse(ctx context.Context, tx *sqlx.Tx, docParse model.DocParse) error
	// CreateDocParseWithSourceEnvSet 新建文档解析任务
	CreateDocParseWithSourceEnvSet(ctx context.Context, tx *sqlx.Tx, docParse model.DocParse, sourceEnvSet string) error
	// CreateDocParseTask 新建文档解析任务
	CreateDocParseTask(ctx context.Context, docParse model.DocParse) error
	// StopDocParseTask 终止 文档解析任务
	StopDocParseTask(ctx context.Context, taskID string, requestID string, robotBizID uint64) error
	// UpdateDocParseTask 更新文档解析任务状态
	UpdateDocParseTask(ctx context.Context, docParse model.DocParse) error
	// SendDocParseWordCount 文档提交解析
	SendDocParseWordCount(ctx context.Context, doc *model.Doc, requestID string,
		originFileType string) (string, error)
	// UpdateDoc 更新doc
	UpdateDoc(ctx context.Context, staffID uint64, doc *model.Doc, isNeedPublish bool,
		attributeLabelReq *model.UpdateDocAttributeLabelReq) error
	// UpdateDocDisableState 更新文档停用启用状态
	UpdateDocDisableState(ctx context.Context, staffID uint64, doc *model.Doc, isDisable bool) error
	// BatchUpdateDoc 批量更新文档参考链接 过期时间
	BatchUpdateDoc(ctx context.Context, staffID uint64, docs []*model.Doc, isNeedPublish map[uint64]int) error
	// UpdateDocAttrRange 更新doc适用范围
	UpdateDocAttrRange(ctx context.Context, staffID uint64, docs []*model.Doc,
		attributeLabelReq *model.UpdateDocAttributeLabelReq) error
	// RenameDoc 文档重命名
	RenameDoc(ctx context.Context, staffID uint64, app *admin.GetAppInfoRsp, doc *model.Doc) error
	// CountDocWithTimeAndStatus 通过时间，获取指定状态的文档总数
	CountDocWithTimeAndStatus(ctx context.Context, corpID, robotID uint64, status []uint32, startTime time.Time) (uint64, error)
	// CreateDocToIndexTask 创建问答生成索引任务
	CreateDocToIndexTask(ctx context.Context, doc *model.Doc, originDocBizID uint64) error
	// CreateDocToQATask 创建问答生成索引任务
	CreateDocToQATask(ctx context.Context, doc *model.Doc, qaTask *model.DocQATask, appBizID uint64) (uint64, error)
	// UpdateDocCharSize 更新字符
	UpdateDocCharSize(ctx context.Context, doc model.Doc) error
	// UpdateDocStatus 更新文档状态信息
	UpdateDocStatus(ctx context.Context, doc *model.Doc) error
	// UpdateDocIsDelete 更新文档删除状态
	UpdateDocIsDelete(ctx context.Context, doc *model.Doc) error
	// RecoverDocStatusWithInterveneAfterAuditFail 审核失败后恢复文档审核中的状态
	RecoverDocStatusWithInterveneAfterAuditFail(ctx context.Context, doc *model.Doc) error
	// UpdateDocStatusAndUpdateTime 更新文档状态信息
	UpdateDocStatusAndUpdateTime(ctx context.Context, doc *model.Doc) error
	// UpdateDocStatusAndCharSize 更新文档状态信息
	UpdateDocStatusAndCharSize(ctx context.Context, doc *model.Doc) error
	// RetryDocParseTask 终止 文档解析任务
	RetryDocParseTask(ctx context.Context, taskID string, requestID string, robotBizID uint64) error
	// CreateDocAudit 创建文档送审任务
	CreateDocAudit(ctx context.Context, doc *model.Doc, envSet string) error
	// CreateInterveneDocAudit 创建文档干预切片送审任务
	CreateInterveneDocAudit(ctx context.Context, doc *model.Doc, interventionType uint32, envSet string) error
	// SetInterveneOldDocCosHashToNewDocRedisValueByDoc 解析切分干预历史文档信息存储
	SetInterveneOldDocCosHashToNewDocRedisValueByDoc(ctx context.Context, appBizID uint64, cosPath, cosHash string,
		oldDoc *model.Doc, interventionType uint32) error
	// GetInterveneOldDocCosHashToNewDocRedisValue 解析切分干预历史文档信息获取
	GetInterveneOldDocCosHashToNewDocRedisValue(ctx context.Context, corpID, botBizID, docBizID uint64,
		oldDocCosHash string) (*model.Doc, *model.DocParsingInterventionRedisValue, error)
	// DeleteInterveneOldDocCosHashToNewDocRedisValue 解析切分干预历史文档信息删除
	DeleteInterveneOldDocCosHashToNewDocRedisValue(ctx context.Context, corpID, botBizID, docBizID uint64,
		oldDocCosHash string) error
	// UpdateCosInfo 更新cos信息
	UpdateCosInfo(ctx context.Context, doc *model.Doc) error
	// GenerateQA 开始生成问答
	GenerateQA(ctx context.Context, staffID uint64, docs []*model.Doc, qaTask *model.DocQATask, appBizID uint64) error
	// UpdateDocQATaskStatusTx 更新问答任务状态
	UpdateDocQATaskStatusTx(ctx context.Context, tx *sqlx.Tx, status int, id uint64, msg string) error
	// UpdateDocQATaskStatus 更新问答任务状态
	UpdateDocQATaskStatus(ctx context.Context, status int, id uint64) error
	// UpdateDocQATaskToken 更新问答任务使用token
	UpdateDocQATaskToken(ctx context.Context, inputToken, outputToken, corpID, robotID, id uint64) error
	// UpdateDocQATaskSegmentDoneAndQaCount 更新问答任务已完成的切片数量和问答数
	UpdateDocQATaskSegmentDoneAndQaCount(ctx context.Context, qaCount, segmentCountDone, corpID, robotID,
		id uint64) error
	// ReferDoc 答案中引用
	ReferDoc(ctx context.Context, doc *model.Doc) error
	// CreateDocQADone 生成QA完成
	CreateDocQADone(ctx context.Context, staffID uint64, doc *model.Doc, qaCount int, success bool) error
	// GetDeletingDoc 获取删除中的文档
	GetDeletingDoc(ctx context.Context, corpID, robotID uint64) (map[uint64]*model.Doc, error)
	// GetCreatingIndexDoc 获取生成分片中的文档
	GetCreatingIndexDoc(ctx context.Context, corpID, robotID uint64) (map[uint64]*model.Doc, error)
	// DeleteDocSuccess 删除文档任务成功
	DeleteDocSuccess(ctx context.Context, doc *model.Doc) error
	// ModifyDocSuccess 更新文档任务成功
	ModifyDocSuccess(ctx context.Context, doc *model.Doc, staffID uint64) error
	// ModifyDocFail 更新文档任务失败
	ModifyDocFail(ctx context.Context, doc *model.Doc, staffID uint64) error
	// GetResumeDocCount 获取恢复中的文档数量
	GetResumeDocCount(ctx context.Context, corpID, robotID uint64) (uint64, error)

	// GetSegmentByID 通过ID获取段落内容
	GetSegmentByID(ctx context.Context, id uint64, robotID uint64) (*model.DocSegmentExtend, error)
	// GetSegmentByIDs 通过ID获取段落内容
	GetSegmentByIDs(ctx context.Context, ids []uint64, robotID uint64) ([]*model.DocSegmentExtend, error)
	// GetSegmentIDByDocIDAndBatchID 通过文档ID和批次ID获取段落内容
	GetSegmentIDByDocIDAndBatchID(ctx context.Context, docID uint64, batchID int, robotID uint64) ([]uint64, error)
	// GetSegmentPagedIDsByDocID 通过文档ID分页获取切片ID列表,用于批量操作
	GetPagedSegmentIDsByDocID(ctx context.Context, docID uint64, page, pageSize uint32, robotID uint64) ([]uint64, error)
	// GetQASegmentIDByDocIDAndBatchID 通过文档ID和批次ID获取需要生成QA的段落内容
	GetQASegmentIDByDocIDAndBatchID(ctx context.Context, docID, stopNextSegmentID, segmentCount uint64,
		batchID int, robotID uint64) ([]uint64, error)
	// CreateSegment 创建文档分段
	CreateSegment(ctx context.Context, segments []*model.DocSegmentExtend, robotID uint64) error
	// GetReleaseSegmentCount 获取发布文档分片总数
	GetReleaseSegmentCount(ctx context.Context, docID uint64, robotID uint64) (uint64, error)
	// GetReleaseSegmentList 获取发布文档分片列表
	GetReleaseSegmentList(ctx context.Context, docID uint64, page, pageSize uint32, robotID uint64) (
		[]*model.DocSegmentExtend, error)
	// SegmentCommonIDsToBizIDs 基础信息获取
	SegmentCommonIDsToBizIDs(ctx context.Context, corpID, appID, staffID, docID uint64) (
		corpBizID, appBizID, staffBizID, docBizID uint64, err error)
	// GetSheetFromDocSegment 从切片中获取sheet信息
	GetSheetFromDocSegment(ctx context.Context, segment *model.DocSegmentExtend,
		corpBizID, appBizID, docBizID uint64, sheetSyncMap *sync.Map) (*model.DocSegmentSheetTemporary, error)
	// GetSheetByNameWithCache 通过SheetName获取Sheet信息
	GetSheetByNameWithCache(ctx context.Context, corpBizID, appBizID, docBizID uint64,
		sheetName string, sheetSyncMap *sync.Map) (*model.DocSegmentSheetTemporary, error)
	// ParseDocFile TODO
	// Deprecated: Use ParseOfflineDocTaskResult instead
	// ParseDocFile 解析文档
	ParseDocFile(ctx context.Context, docParse model.DocParse) (
		[]*knowledge.PageContent, []*knowledge.PageContent, *knowledge.Tables, error)
	// ParseImgURL 解析底座图片链接
	ParseImgURL(ctx context.Context, image string) ([]string, error)
	// ShortURLCodeRecoverCosURL 短链恢复正常cos链接
	ShortURLCodeRecoverCosURL(ctx context.Context, shortURL, path string) (string, error)
	// ParseOfflineDocTaskResult 解析实时文档解析结果
	ParseOfflineDocTaskResult(ctx context.Context, doc *model.Doc, docParse model.DocParse, segmentType uint32,
		intervene bool) error
	// GetOfflineDocParseResult 获取解析结果
	GetOfflineDocParseResult(ctx context.Context, docParse model.DocParse) (result string, err error)
	// GetFileDataFromCosURL 从COS上下载文件并返回文件内容
	GetFileDataFromCosURL(ctx context.Context, cosURL string) (string, error)
	// ParseExcelQA 解析excel问答
	ParseExcelQA(ctx context.Context, cosURL, fileName string) ([]string, error)
	// LLMSegmentQA 获取段落的问答对
	LLMSegmentQA(ctx context.Context, doc *model.Doc, segment *model.DocSegmentExtend, app *model.App) (
		[]*model.QA, *llmm.StatisticInfo, error)
	// GetDocQANum 统计文档有效问答对
	GetDocQANum(ctx context.Context, corpID, robotID uint64, docIDs []uint64) (map[uint64]map[uint32]uint32, error)
	// GetCateStat 按分类统计
	GetCateStat(ctx context.Context, t model.CateObjectType, corpID, robotID uint64) (map[uint64]uint32, error)
	// GetCateList 获取Cate列表
	GetCateList(ctx context.Context, t model.CateObjectType, corpID, robotID uint64) ([]*model.CateInfo, error)
	// CreateCate 新增分类
	CreateCate(ctx context.Context, t model.CateObjectType, cate *model.CateInfo) (uint64, error)
	// GetCateByID 获取Cate详情
	GetCateByID(ctx context.Context, t model.CateObjectType, id, corpID, robotID uint64) (*model.CateInfo, error)
	// GetCateByIDs 获取多个Cate详情
	GetCateByIDs(ctx context.Context, t model.CateObjectType, ids []uint64) (map[uint64]*model.CateInfo, error)
	// GetCateListByBusinessIDs 通过业务ID获取Cate列表
	GetCateListByBusinessIDs(ctx context.Context, t model.CateObjectType, corpID, robotID uint64, cateBizIDs []uint64) (
		map[uint64]*model.CateInfo, error)
	// GetCateByBusinessID 通过业务ID获取Cate详情
	GetCateByBusinessID(ctx context.Context, t model.CateObjectType,
		cateBizID, corpID, robotID uint64) (*model.CateInfo, error)
	// UpdateCate 更新问答对分类
	UpdateCate(ctx context.Context, t model.CateObjectType, id uint64, name string) error
	// CheckCateBiz 检查分类Biz
	CheckCateBiz(ctx context.Context, t model.CateObjectType, corpID, cateBizID, robotID uint64) (id uint64, err error)
	// CheckCate 检查分类ID
	CheckCate(ctx context.Context, t model.CateObjectType, corpID, cateID, robotID uint64) error

	// GetCateChildrenIDs 获取分类下的子分类ID列表
	GetCateChildrenIDs(ctx context.Context, t model.CateObjectType, corpID, cateID, robotID uint64) ([]uint64, error)

	// GetRobotUncategorizedCateID 获取机器人未分类的ID
	GetRobotUncategorizedCateID(ctx context.Context, t model.CateObjectType, corpID, robotID uint64) (uint64, error)
	// GetQAList 获取问答对列表
	GetQAList(ctx context.Context, req *model.QAListReq) ([]*model.DocQA, error)
	// GetListQaTask 查询文档生成问答任务列表
	GetListQaTask(ctx context.Context, req *model.ListQaTaskReq) (uint64, []*model.DocQATaskList, error)
	// GetDocQATaskByBusinessID 根据对外查询生成问答任务
	GetDocQATaskByBusinessID(ctx context.Context, taskID, corpID, robotID uint64) (*model.DocQATask, error)
	// GetDocQATaskGenerating 查询文档是否有进行中任务
	GetDocQATaskGenerating(ctx context.Context, corpID, robotID, docID uint64) (bool, error)
	// DeleteQaTask 删除生成问答任务
	DeleteQaTask(ctx context.Context, corpID, robotID, taskID uint64) error
	// StopQaTask 暂停任务
	StopQaTask(ctx context.Context, corpID, robotID, taskID uint64, finance bool, modelName string) error
	// CancelQaTask 取消任务
	CancelQaTask(ctx context.Context, corpID, robotID, taskID uint64) error
	// ContinueQaTask 继续任务
	ContinueQaTask(ctx context.Context, corpID, robotID uint64, qaTask *model.DocQATask, appBizID uint64) error
	// GetDocQATaskByID 根据ID查询生成问答任务
	GetDocQATaskByID(ctx context.Context, id, corpID, robotID uint64) (*model.DocQATask, error)
	// GetQAListCount 获取问答对列表数量
	GetQAListCount(ctx context.Context, req *model.QAListReq) (uint32, uint32, uint32, uint32, error)
	// GetQaCountWithDocID 获取某个文档ID对应的问答个数
	GetQaCountWithDocID(ctx context.Context, req *model.QAListReq) (uint32, error)
	// GetQADetail 获取QA详情
	GetQADetail(ctx context.Context, corpID, robotID uint64, id uint64) (*model.DocQA, error)
	// GetQADetails 批量获取QA详情
	GetQADetails(ctx context.Context, corpID, robotID uint64, ids []uint64) (map[uint64]*model.DocQA, error)
	// GetQADetailsByBizIDs 批量获取QA详情
	GetQADetailsByBizIDs(ctx context.Context, corpID, robotID uint64, bizIDs []uint64) (map[uint64]*model.DocQA, error)
	// GetQADetailsByBizID 获取QA详情
	GetQADetailsByBizID(ctx context.Context, corpID, robotID uint64, bizID uint64) (*model.DocQA, error)
	// GetQADetailsByReleaseStatus 批量获取QA详情
	GetQADetailsByReleaseStatus(ctx context.Context, corpID, robotID uint64, ids []uint64,
		releaseStatus uint32) (map[uint64]*model.DocQA, error)
	// GetQAByID 通过ID获取QA详情
	GetQAByID(ctx context.Context, id uint64) (*model.DocQA, error)
	// GetQAByBizID 通过bizID获取QA详情
	GetQAByBizID(ctx context.Context, bizID uint64) (*model.DocQA, error)
	// CreateQA 创建QA(支持相似问)
	CreateQA(ctx context.Context, qa *model.DocQA, businessSource uint32, businessID uint64,
		attributeLabelReq *model.UpdateQAAttributeLabelReq, simQuestions []string) error
	// UpdateQA 更新问答对(支持相似问)
	UpdateQA(ctx context.Context, qa *model.DocQA, sqm *model.SimilarQuestionModifyInfo, isNeedPublish,
		isNeedAudit bool, diffCharSize int64, attributeLabelReq *model.UpdateQAAttributeLabelReq) error
	UpdateQADisableState(ctx context.Context, qa *model.DocQA, sqm *model.SimilarQuestionModifyInfo, isDisable bool) error
	// GetVideoURLsCharSize 从html中提取视频链接返回字符数
	GetVideoURLsCharSize(ctx context.Context, htmlStr string) (int, error)
	// UpdateQAStatusAndUpdateTime 更新问答对状态与更新时间
	UpdateQAStatusAndUpdateTime(ctx context.Context, qa *model.DocQA) error
	// UpdateQAAuditStatusAndUpdateTimeTx 事务的方式更新问答对审核状态和更新时间
	UpdateQAAuditStatusAndUpdateTimeTx(ctx context.Context, tx *sqlx.Tx, qa *model.DocQA) error
	// UpdateQASimilarsDocID 批量更新t_doc_qa_similar的DocID
	UpdateQASimilarsDocID(ctx context.Context, qaIDs []uint64, docID uint64) error
	// PublishQA 发布问答对
	PublishQA(ctx context.Context, qa *model.DocQA, releaseQA *model.ReleaseQA) error
	// DeleteQAs 删除QAs(支持相似问联动) 无sync操作,新建deleteTask
	DeleteQAs(ctx context.Context, corpID, robotID, staffID uint64, qas []*model.DocQA) error
	// DeleteQA 删除一条问答(支持相似问联动) 有sync操作
	DeleteQA(ctx context.Context, qa *model.DocQA) error
	// DeleteDocToQA 删除文档只取消文档下问答对文档的引用
	DeleteDocToQA(ctx context.Context, qa *model.DocQA) error
	// VerifyQA 校验QA
	VerifyQA(ctx context.Context, qas []*model.DocQA, robotID, charSize uint64) error
	// UpdateQAAttrRange 编辑QA适用范围
	UpdateQAAttrRange(ctx context.Context, qas []*model.DocQA, attributeLabelReq *model.UpdateQAAttributeLabelReq) error
	// UpdateQAsExpire 批量更新问答过期时间
	UpdateQAsExpire(ctx context.Context, qas []*model.DocQA) error
	// UpdateQAsDoc 更新问答关联文档
	UpdateQAsDoc(ctx context.Context, qas []*model.DocQA) error
	// BatchCreateQA 批量创建QA(支持相似问)
	BatchCreateQA(context.Context, *model.DocSegmentExtend, *model.Doc, []*model.QA, *model.CateNode, bool) error
	// PollQaToSimilar 获取发生写操作的QA
	PollQaToSimilar(ctx context.Context) ([]*model.DocQA, error)
	// LockOneQa 锁定一条问答对
	LockOneQa(ctx context.Context, task *model.DocQA) error
	// UnLockOneQa 解锁一条问答对
	UnLockOneQa(ctx context.Context, task *model.DocQA) error
	// UpdateAuditQA 更新QA审核状态
	UpdateAuditQA(ctx context.Context, qa *model.DocQA) error
	// GetReleaseQACount 获取发布QA总数
	GetReleaseQACount(ctx context.Context, corpID, robotID uint64, question string, startTime, endTime time.Time,
		actions []uint32) (uint64, error)
	// GetReleaseQAList 获取发布问答对列表
	GetReleaseQAList(ctx context.Context, corpID, robotID uint64, question string, startTime, endTime time.Time,
		actions []uint32, page, pageSize uint32) ([]*model.DocQA, error)
	// GetQABySegment 通过分段获取QA详情
	GetQABySegment(ctx context.Context, segment *model.DocSegmentExtend) ([]*model.DocQA, error)
	// UpdateCreatingQAFlag 更新问答生成中标记
	UpdateCreatingQAFlag(ctx context.Context, doc *model.Doc) error
	// UpdateCreatingQATaskFlag 更新问答任务生成中标记
	UpdateCreatingQATaskFlag(ctx context.Context, doc *model.Doc) error
	// GetQAAndRelateDocs 获取QA和QA关联的文档
	GetQAAndRelateDocs(ctx context.Context, ids []uint64, robotID uint64) (map[uint64]*model.DocQA, map[uint64]*model.Doc, error)
	// UpdateCreatingIndexFlag 更新索引生成中标记
	UpdateCreatingIndexFlag(ctx context.Context, doc *model.Doc) error
	// UpdateDocNameAndStatus 更新文档名称与状态,以及索引生成中标记
	UpdateDocNameAndStatus(ctx context.Context, doc *model.Doc) error
	// GetWaitReleaseDoc 获取待发布的文档
	GetWaitReleaseDoc(ctx context.Context, corpID, robotID uint64, fileName string, startTime,
		endTime time.Time, actions []uint32, page, pageSize uint32) ([]*model.Doc, error)
	// GetNewCharSize 包含相似问的 char size
	GetNewCharSize(ctx context.Context, oldQA *model.DocQA, similarModify *knowledgeConfig.ModifyQAReq) (uint64, error)
	// CalcQACharSize 计算doc的charSize(含相似问)
	CalcQACharSize(ctx context.Context, doc *model.QA) uint64
	// GetCosFileInfoByUrl 根据cos_url获取cos文件信息
	GetCosFileInfoByUrl(ctx context.Context, cosUrl string) (*model.ObjectInfo, error)

	// GetSimilarQuestionsCountByQAIDs 获取标准问对应的相似问个数
	GetSimilarQuestionsCountByQAIDs(ctx context.Context, corpID, robotID uint64, qaIDs []uint64) (map[uint64]uint32,
		error)
	// GetSimilarQuestionsSimpleByQAIDs 根据标准问获批量取所有相似问, 返回map[qa_id]:相似问列表
	GetSimilarQuestionsSimpleByQAIDs(ctx context.Context, corpID, robotID uint64,
		qaIDs []uint64) (map[uint64][]*model.SimilarQuestionSimple, error)
	// GetSimilarQuestionsByQA 根据标准问获取所有相似问
	GetSimilarQuestionsByQA(ctx context.Context, qa *model.DocQA) ([]*model.SimilarQuestion, error)
	// GetSimilarQuestionsByUpdateTime 根据更新时间获取相似问
	GetSimilarQuestionsByUpdateTime(ctx context.Context, start, end time.Time, limit, offset uint64,
		appidList []uint64) ([]*model.SimilarQuestion, error)
	// NewSimilarQuestionsFromModifyReq 从修改请求中生成相似问
	NewSimilarQuestionsFromModifyReq(ctx context.Context, qa *model.DocQA,
		similarModify *knowledgeConfig.SimilarQuestionModify) (*model.SimilarQuestionModifyInfo, error)
	// NewSimilarQuestionsFromDBAndReq 从 db 中和修改请求中生成去重后的相似问
	NewSimilarQuestionsFromDBAndReq(ctx context.Context, qa *model.DocQA,
		similarModify *knowledgeConfig.SimilarQuestionModify, isAll bool) (*model.SimilarQuestionModifyInfo, error)
	// GetSimilarQuestionsCount 获取标准问对应的相似问个数
	GetSimilarQuestionsCount(ctx context.Context, qa *model.DocQA) (int, error)
	// AddSimilarQuestionSyncBatch 新增相似问同步流水(批量)
	AddSimilarQuestionSyncBatch(ctx context.Context, sqs []*model.SimilarQuestion) error

	// UpdateSynonymsTaskErrorCosUrl 更新导入任务ErrorCosUrl信息
	UpdateSynonymsTaskErrorCosUrl(ctx context.Context, synonymsTask *model.SynonymsTask) error
	// UpdateSynonymsImportTaskStatus 更新同义词导入任务状态,并发送Notice
	UpdateSynonymsImportTaskStatus(ctx context.Context, t *model.SynonymsTask, status int) error
	// CreateSynonymsImportTask 创建同义词导入任务
	CreateSynonymsImportTask(ctx context.Context, req *knowledgeConfig.UploadSynonymsListReq,
		corpID, staffID, robotID uint64) (uint64, error)
	// ParseExcelAndImportSynonyms 解析excel导入同义词
	ParseExcelAndImportSynonyms(ctx context.Context, cosURL string, fileName string, robotID, corpID uint64) (string,
		error)
	// GetSynonymsTaskInfo 获取同义词任务信息
	GetSynonymsTaskInfo(ctx context.Context, taskID, corpID, robotID uint64) (*model.SynonymsTask, error)
	// GetSynonymsListCount 获取同义词列表数量
	GetSynonymsListCount(ctx context.Context, req *model.SynonymsListReq) (uint32, error)
	// GetSynonymsListReq 获取同义词列表请求参数
	GetSynonymsListReq(ctx context.Context, req *knowledgeConfig.ListSynonymsReq,
		appID, corpID uint64) (*model.SynonymsListReq, error)
	// GetSynonymsList 获取同义词列表
	GetSynonymsList(ctx context.Context, req *model.SynonymsListReq) (*model.SynonymsListRsp, error)
	// CreateSynonyms 创建同义词
	CreateSynonyms(ctx context.Context, req *model.SynonymsCreateReq) (*model.SynonymsCreateRsp, error)
	// GetSynonymDetailsByBizID 通过bizID获取同义词详情
	GetSynonymDetailsByBizID(ctx context.Context, corpId, appID, synonymsID uint64) (*model.Synonyms, error)
	// UpdateSynonyms 更新同义词
	UpdateSynonyms(ctx context.Context, oldSynonym *model.Synonyms,
		synonymsModifyReq *model.SynonymsModifyReq) (uint32, string, error)
	// GetSynonymsDetailsByBizIDs 批量获取同义词详情
	GetSynonymsDetailsByBizIDs(ctx context.Context, corpId, appID uint64,
		synonymsIDs []uint64) (map[uint64]*model.Synonyms, error)
	// DeleteSynonyms 删除同义词
	DeleteSynonyms(ctx context.Context, corpID, appID, staffID uint64, synonyms []*model.Synonyms) error
	// GetSynonymsNER 获取同义词NER
	GetSynonymsNER(ctx context.Context, nerReq *model.SynonymsNERReq) (*model.SynonymsNERRsp, error)

	// CreateRelease 创建发布记录
	CreateRelease(ctx context.Context, record *model.Release, previewJSON string) error
	// CreateReleaseDetail 创建发布记录
	CreateReleaseDetail(ctx context.Context, record *model.Release, releaseDoc []*model.ReleaseDoc,
		releaseQA []*model.ReleaseQA, releaseSegments []*model.ReleaseSegment,
		releaseRejectedQuestions []*model.ReleaseRejectedQuestion, releaseConfig []*model.ReleaseConfig) error
	// CreateReleaseConfigDetail 采集发布配置记录详情
	CreateReleaseConfigDetail(ctx context.Context, record *model.Release,
		releaseConfig []*model.ReleaseConfig) error
	// CreateReleaseAudit 创建发布送审
	CreateReleaseAudit(ctx context.Context, release *model.Release, envSet string) error
	// NotifyReleaseSuccess 通知发布成功
	NotifyReleaseSuccess(ctx context.Context, release *model.Release) error
	// CreateReleaseAppealNotice 创建问答发布申诉通知
	CreateReleaseAppealNotice(ctx context.Context, numSuccess, numFail, numTotal uint32, audit *model.Audit) error
	// ReleasePause 发布暂停
	ReleasePause(ctx context.Context, release *model.Release) error
	// RetryPausedRelease 重试暂停的发布
	RetryPausedRelease(ctx context.Context, release *model.Release, req *pb.ContinueTerminatedTaskReq) error
	// ReleaseSuccess 发布成功
	ReleaseSuccess(ctx context.Context, appDB *model.AppDB, release *model.Release,
		qaIDs, segmentIDs, rejectedQuestionIDs, forbidReleaseQAIDs []uint64, configAuditPass,
		configAuditFail []*model.ReleaseConfig, releaseDoc []*model.ReleaseDoc, docs []*model.Doc) error
	// GetReleaseRecord 根据向量库版本获取发布记录
	GetReleaseRecord(ctx context.Context, robotID, versionID uint64) (*model.Release, error)
	// GetReleaseRecords 获取发布记录
	GetReleaseRecords(ctx context.Context, corpID, robotID uint64, page, pageSize uint32) (
		uint64, []*model.Release, error)
	// GetLatestRelease 获取最近一次发布记录
	GetLatestRelease(ctx context.Context, corpID, robotID uint64) (*model.Release, error)
	// GetLatestSuccessRelease 获取最后一次状态为发布成功的记录
	GetLatestSuccessRelease(ctx context.Context, corpID, robotID uint64) (*model.Release, error)
	// CheckUnconfirmedQa 是否存在未确认问答
	CheckUnconfirmedQa(ctx context.Context, robotID uint64) (bool, error)
	// HasUnconfirmedQa 是否存在未确认问答
	HasUnconfirmedQa(ctx context.Context, corpID, staffID, robotID uint64) (bool, error)
	// GetUnconfirmedQaNum 未确认问答数量
	GetUnconfirmedQaNum(ctx context.Context, corpID, robotID uint64) (uint64, error)
	// GetWaitReleaseDocCount 获取待发布的文档数量
	GetWaitReleaseDocCount(ctx context.Context, corpID, robotID uint64, fileName string, startTime,
		endTime time.Time, actions []uint32) (uint64, error)
	// GetWaitRelease 获取待发布记录
	GetWaitRelease(ctx context.Context, pageSize uint32) ([]*model.Release, error)
	// ExecRelease 通知发布
	ExecRelease(ctx context.Context, isQaAllowExec, isCfgAllowExec, isOnlyReleaseQA bool, record *model.Release) error
	// GetReleaseModifyQA 获取版本改动的QA
	GetReleaseModifyQA(ctx context.Context, release *model.Release, qas []*model.DocQA) (
		map[uint64]*model.ReleaseQA, error)
	// GetReleaseDoc 获取版本改动的文档ID
	GetReleaseDoc(ctx context.Context, release *model.Release) (map[uint64]struct{}, error)
	// GetReleaseModifySegment 获取版本改动的segment
	GetReleaseModifySegment(ctx context.Context, release *model.Release, segments []*model.DocSegmentExtend) (
		map[uint64]*model.ReleaseSegment, error)
	// PublishSegment 发布文档片段
	PublishSegment(ctx context.Context, segment *model.DocSegmentExtend, modifySeg *model.ReleaseSegment, robotID uint64) error
	// BatchDeleteSegments 批量删除文档分片, 不执行DeleteBigDataElastic的步骤
	BatchDeleteSegments(ctx context.Context, segments []*model.DocSegmentExtend, robotID uint64) error
	// DeleteSegmentsForQA 删除用于生成QA的分片
	DeleteSegmentsForQA(ctx context.Context, doc *model.Doc) error
	// DeleteSegmentsForIndex 删除用于写向量的分片
	DeleteSegmentsForIndex(ctx context.Context, doc *model.Doc, embeddingModelName string) error
	// ResumeSegments 批量恢复文档分片,删除的逆操作
	ResumeSegments(ctx context.Context, segment []*model.DocSegmentExtend, robotID uint64) error
	// DeleteSegmentImages 删除文档分片的图片
	DeleteSegmentImages(ctx context.Context, robotID uint64, docIDs []uint64) error
	// GetSegmentListCount 获取segment列表数量
	GetSegmentListCount(ctx context.Context, corpID, docID, robotID uint64) (uint64, error)
	// GetSegmentList 获取segment列表
	GetSegmentList(ctx context.Context, corpID, docID uint64, page, pageSize uint32, robotID uint64) (
		[]*model.DocSegmentExtend, error)
	// GetSegmentDeletedList 获取删除的segment列表
	GetSegmentDeletedList(ctx context.Context, corpID, docID uint64, page, pageSize uint32, robotID uint64) (
		[]*model.DocSegmentExtend, error)
	// BatchUpdateSegment 批量更新文档分片内容
	BatchUpdateSegment(ctx context.Context, segments []*model.DocSegmentExtend, robotID uint64) error
	// BatchUpdateSegmentContent 批量更新文档分片
	BatchUpdateSegmentContent(ctx context.Context, segments []*model.DocSegmentExtend, robotID uint64) error
	// UpdateQaSegmentStatus 更新分片生成QA状态
	UpdateQaSegmentStatus(ctx context.Context, segment *model.DocSegmentExtend, robotID uint64) error
	// UpdateQaSegmentToDocStatus 还原切片状态
	UpdateQaSegmentToDocStatus(ctx context.Context, docID uint64, batchID int, robotID uint64) error
	// UpdateSegmentReleaseStatus 更新文档分片
	UpdateSegmentReleaseStatus(ctx context.Context, segment *model.DocSegmentExtend, robotID uint64) error
	// BatchUpdateSegmentReleaseStatus 批量更新文档分片
	BatchUpdateSegmentReleaseStatus(ctx context.Context, segs []*model.DocSegmentExtend, status uint32, robotID uint64) error
	// GetReleaseByID 通过版本ID获取发布记录
	GetReleaseByID(ctx context.Context, id uint64) (*model.Release, error)
	// GetReleaseByBizID 通过版本BizID获取发布记录
	GetReleaseByBizID(ctx context.Context, bizID uint64) (*model.Release, error)
	// GetReleaseQAByID 获取发布的QA
	GetReleaseQAByID(ctx context.Context, id uint64) (*model.ReleaseQA, error)
	// GetReleaseQAAuditStat 统计QA审核
	GetReleaseQAAuditStat(ctx context.Context, versionID uint64) (map[uint32]*model.AuditResultStat, error)
	// GetModifyDocCount 获取版本改动文档数量
	GetModifyDocCount(ctx context.Context, robotID, versionID uint64, fileName string, actions []uint32,
		status []uint32) (uint64, error)
	// GetModifyDocList 获取版本改动文档范围
	GetModifyDocList(ctx context.Context, robotID, versionID uint64, fileName string, actions []uint32, page,
		pageSize uint32) ([]*model.ReleaseDoc, error)
	// GetForbidReleaseQA 获取禁止发布的问答ID
	GetForbidReleaseQA(ctx context.Context, versionID uint64) ([]uint64, error)

	// SaveQaSimilar 保存相似问答对
	SaveQaSimilar(ctx context.Context, qaSimilar *model.DocQASimilar) error
	// GetModifyQACount 获取版本改动QA数量
	GetModifyQACount(ctx context.Context, robotID, versionID uint64, question string, actions []uint32,
		releaseStatus []uint32) (uint64, error)
	// GetModifyQAList 获取版本改动QA范围
	GetModifyQAList(ctx context.Context, robotID, versionID uint64, question string, actions []uint32, page,
		pageSize uint32, orderBy string, releaseStatus []uint32) ([]*model.ReleaseQA, error)
	// GetAuditQAByVersion 获取要审核的QA内容
	GetAuditQAByVersion(ctx context.Context, versionID uint64) ([]*model.AuditReleaseQA, error)
	// GetAuditConfigItemByVersion 获取要审核的配置内容
	GetAuditConfigItemByVersion(ctx context.Context, versionID uint64) ([]*model.AuditReleaseConfig, error)
	// GetAuditQAFailByVersion 获取机器审核审核不通过的QA内容
	GetAuditQAFailByVersion(ctx context.Context, corpID, robotID, versionID uint64) ([]*model.AuditReleaseQA, error)
	// GetAuditQAFailByQaID 根据 QaID 获取机器审核审核不通过的QA内容
	GetAuditQAFailByQaID(ctx context.Context, corpID, robotID, qaID uint64) ([]*model.AuditReleaseQA, error)
	// GetModifySegmentCount 获取版本改动segment数量
	GetModifySegmentCount(ctx context.Context, robotID, versionID uint64, action uint32) (uint64, error)
	// GetModifySegmentList 获取版本改动segment范围
	GetModifySegmentList(ctx context.Context, robotID, versionID uint64, action []uint32, page, pageSize uint32) (
		[]*model.ReleaseSegment, error)
	// GetQASimilarList 获取相似问答对列表
	GetQASimilarList(ctx context.Context, corpID, robotID uint64, page, pageSize uint32) (uint64, []uint64, error)
	// ListQASimilar 获取相似问答对列表
	ListQASimilar(ctx context.Context, corpID, robotID uint64, page, pageSize uint32) (
		uint64, []*model.DocQASimilar, error)
	// GetQASimilarID 获取相似问答对 ID
	GetQASimilarID(ctx context.Context, corpID, similarID uint64) (*model.DocQASimilar, error)
	// GetQASimilarBizID 获取相似问答对 ID
	GetQASimilarBizID(ctx context.Context, corpID, similarBizID uint64) (*model.DocQASimilar, error)
	// DoQASimilar 删除未保留的qa，标记相似问答已处理
	DoQASimilar(ctx context.Context, corpID, robotID, staffID uint64, qaSimilarID []uint64,
		delQas []*model.DocQA) error
	// DoQABizSimilar 删除未保留的qa，标记相似问答已处理
	DoQABizSimilar(ctx context.Context, corpID, robotID, staffID uint64, qaSimilarBizID []uint64,
		delQas []*model.DocQA) error
	// DeleteQASimilar 删除相似问答对
	DeleteQASimilar(ctx context.Context, qa *model.DocQA) error
	// IgnoreAllQASimilar 忽略当前所有相似问答对
	IgnoreAllQASimilar(ctx context.Context, corpID, robotID uint64) error

	// CreateNoticex 创建通知
	CreateNoticex(ctx context.Context, tx *sqlx.Tx, notice *model.Notice) error
	// CreateNotice 创建通知
	CreateNotice(ctx context.Context, notice *model.Notice) error
	// GetNoticeByIDs 通过ID获取通知
	GetNoticeByIDs(ctx context.Context, ids []uint64) ([]*model.Notice, error)
	// CloseNoticeByID 关闭通知
	CloseNoticeByID(ctx context.Context, ids []uint64) error
	// ReadNoticeByID 已读通知
	ReadNoticeByID(ctx context.Context, ids []uint64) error
	// GetPageNotice 获取页面通知列表
	GetPageNotice(ctx context.Context, corpID, staffID, robotID uint64, pageID uint32) ([]*model.Notice, error)
	// GetLastReadNotice 最新一条已读信息
	GetLastReadNotice(ctx context.Context, corpID, staffID, robotID uint64) (*model.Notice, error)
	// GetCursorNotice 获取游标通知列表
	GetCursorNotice(ctx context.Context, corpID, staffID, robotID, cursor uint64) ([]*model.Notice, error)
	// GetCenterNotice 获取通知中心最新消息
	GetCenterNotice(ctx context.Context, corpID, staffID, robotID, cursor uint64) ([]*model.Notice, error)
	// GetHistoryNoticeCount 获取通知中心列表数量
	GetHistoryNoticeCount(ctx context.Context, corpID, staffID, robotID uint64) (uint64, error)
	// GetHistoryNotice 获取通知中心列表
	GetHistoryNotice(ctx context.Context, corpID, staffID, robotID, id uint64, limit uint32) ([]*model.Notice, error)
	// GetUnreadTotal 未读数量
	GetUnreadTotal(ctx context.Context, corpID, staffID, robotID uint64) (uint64, error)

	// CreateParentAuditCheck 创建父审核任务
	CreateParentAuditCheck(ctx context.Context, parent *model.Audit) error
	// CreateParentAuditCheckWithOriginDocBizID 创建父审核任务(干预使用)
	CreateParentAuditCheckWithOriginDocBizID(ctx context.Context, parent *model.Audit, originDocBizID uint64) error
	// CreateParentAuditCheckForExcel2Qa 批量导入问答场景下，创建审核回调check任务
	CreateParentAuditCheckForExcel2Qa(ctx context.Context, p model.AuditSendParams) error
	// CreateAudit 创建单条送审
	CreateAudit(ctx context.Context, audit *model.Audit) error
	// BatchCreateAudit 批量创建审核数据
	BatchCreateAudit(ctx context.Context, parent *model.Audit, appDB *model.AppDB, p model.AuditSendParams) (
		[]*model.Audit, error)
	// CreateQaAuditForExcel2Qa 批量导入问答时，创建问答送审任务
	CreateQaAuditForExcel2Qa(ctx context.Context, doc *model.Doc) error
	// GetAuditByID 通过id获取审核数据
	GetAuditByID(ctx context.Context, id uint64) (*model.Audit, error)
	// GetAuditByParentID 通过BizID获取已存在的审核数据
	GetAuditByParentID(ctx context.Context, parentID uint64, p model.AuditSendParams) ([]*model.Audit, error)
	// GetAuditByBizID 通过BizID获取审核数据
	GetAuditByBizID(ctx context.Context, bizID uint64) (*model.Audit, error)
	// GetParentAuditsByParentRelateID 通过父关联ID获取已存在的父审核数据
	GetParentAuditsByParentRelateID(ctx context.Context, p model.AuditSendParams, idStart uint64,
		limit int) ([]*model.Audit, error)
	// GetParentAuditIDsByParentRelateID 通过父关联ID获取已存在的父审核ID列表
	GetParentAuditIDsByParentRelateID(ctx context.Context, p model.AuditCheckParams, idStart uint64,
		limit int) ([]uint64, error)
	// GetBizAuditStatusStat 按status统计子审核数据
	GetBizAuditStatusStat(ctx context.Context, id, corpID, robotID uint64) (map[uint32]*model.AuditStatusStat, error)
	// GetBizAuditFailList 根据 Type 获取最新一次子审核数据
	GetBizAuditFailList(ctx context.Context, corpID, robotID uint64, auditType uint32) ([]*model.AuditFailList, error)
	// GetBizAuditFailListByRelateIDs 根据 relate_id 查询审核失败列表，不包括人工申诉失败
	GetBizAuditFailListByRelateIDs(ctx context.Context, corpID, robotID uint64, auditType uint32,
		releateIDs []uint64) ([]*model.AuditFailList, error)
	// GetBizAuditFailListByRelateIDsIncludeAppealFail 根据 relate_id 查询审核失败列表，包括人工申诉失败
	GetBizAuditFailListByRelateIDsIncludeAppealFail(ctx context.Context, corpID, robotID uint64,
		auditType uint32, releateIDs []uint64) ([]*model.AuditFailList, error)
	// GetLatestParentAuditFailByRelateID 根据 relateID 获取最后一次父审核数据
	GetLatestParentAuditFailByRelateID(ctx context.Context, corpID, robotID, releateID uint64,
		auditType uint32) (*model.AuditParent, error)
	// GetLatestAuditFailListByRelateID 根据 relateID 获取最后一次子审核数据
	GetLatestAuditFailListByRelateID(ctx context.Context, corpID, robotID, releateID uint64,
		auditType uint32, isAppeal bool) ([]*model.AuditFailList, error)
	// DescribeQaAuditFailStatus 获取问答审核详情
	DescribeQaAuditFailStatus(ctx context.Context, qa *model.DocQA, sim []*model.SimilarQuestion,
		auditFailList []*model.AuditFailList, uin string, appBizID uint64) error
	// GetBizAuditStatusByRelateIDs 根据 RelateIDs 查询审核状态
	GetBizAuditStatusByRelateIDs(ctx context.Context, robotID, corpID uint64, relateIDs []uint64) (
		map[uint64]model.AuditStatus, error)
	// SendAudit 发送审核
	SendAudit(ctx context.Context, audit *model.Audit, appInfosecBizType string) error
	// GetBizAuditStatusByType 根据 Type 查询审核状态
	GetBizAuditStatusByType(ctx context.Context, robotID, corpID uint64, auditTypes uint32) (model.AuditStatus, error)
	// GetBizAuditStatusByTypes 根据 Type 查询审核状态
	GetBizAuditStatusByTypes(ctx context.Context, robotID, corpID uint64, auditTypes []uint32) (
		map[uint32]model.AuditStatus, error)
	// SendAuditFail 更新发送审核失败
	SendAuditFail(ctx context.Context, audit *model.Audit) error
	// AuditRobotProfile 审核机器人资料
	AuditRobotProfile(ctx context.Context, audit *model.Audit) error
	// AuditRelease 审核发布
	AuditRelease(ctx context.Context, audit *model.Audit, pass bool) error
	// AuditDoc 文档审核或者申诉回调处理函数，audit是父审核任务
	AuditDoc(ctx context.Context, audit *model.Audit, pass, isAppeal bool, rejectReason string) error
	// AuditDocName 文档名称审核或者申诉回调处理函数，audit是父审核任务
	AuditDocName(ctx context.Context, audit *model.Audit, pass, isAppeal bool, rejectReason string) error
	// AuditQa QA审核或者申诉回调处理函数，audit是父审核任务
	AuditQa(ctx context.Context, audit *model.Audit, pass, isAppeal bool, rejectReason string) error
	// AuditBareAnswer 审核BareAnswer
	AuditBareAnswer(ctx context.Context, audit *model.Audit) error
	// NoNeedAuditDoc 无需审核文档，直接发起解析片段任务
	NoNeedAuditDoc(ctx context.Context, doc *model.Doc) error

	// GenerateSeqID 生成唯一ID
	GenerateSeqID() uint64
	// Lock 加锁
	Lock(ctx context.Context, key string, duration time.Duration) error
	// UnLock 解锁
	UnLock(ctx context.Context, key string) error

	// AddOperationLog 添加写操作记录
	AddOperationLog(ctx context.Context, event string, corpID, robotID uint64, req, rsp, before, after any) error

	// GetQAsByIDs 根据ID获取问答
	GetQAsByIDs(
		ctx context.Context, corpID, robotID uint64, qaIDs []uint64, offset, limit uint64,
	) ([]*model.DocQA, error)
	// GetQAsByBizIDs 根据业务ID获取问答
	GetQAsByBizIDs(
		ctx context.Context, corpID, robotID uint64, qaBizIDs []uint64, offset, limit uint64,
	) ([]*model.DocQA, error)
	// GetCorpList 获取企业列表
	GetCorpList(ctx context.Context, corpStatus uint32, cellphone string) ([]*model.Corp, error)

	// DirectSearchVector 向量搜索
	DirectSearchVector(ctx context.Context, req *pb.DirectSearchVectorReq) (*pb.DirectSearchVectorRsp, error)
	// DirectAddVector 新增向量
	DirectAddVector(ctx context.Context, req *pb.DirectAddVectorReq) (*pb.DirectAddVectorRsp, error)
	// DirectUpdateVector 更新向量
	DirectUpdateVector(ctx context.Context, req *pb.DirectUpdateVectorReq) (*pb.DirectUpdateVectorRsp, error)
	// DirectDeleteVector 删除向量
	DirectDeleteVector(ctx context.Context, req *pb.DirectDeleteVectorReq) (*pb.DirectDeleteVectorRsp, error)
	// DirectCreateIndex 创建索引库
	DirectCreateIndex(ctx context.Context, req *pb.DirectCreateIndexReq) (*pb.DirectCreateIndexRsp, error)
	// DirectDeleteIndex 删除索引库
	DirectDeleteIndex(ctx context.Context, req *pb.DirectDeleteIndexReq) (*pb.DirectDeleteIndexRsp, error)
	// Search 向量搜索
	Search(ctx context.Context, req *pb.SearchReq) (*pb.SearchRsp, error)
	// AddQAVector 新增问答向量
	AddQAVector(ctx context.Context, qa *model.DocQA) error
	// DeleteQAVector 删除问答向量
	DeleteQAVector(ctx context.Context, qa *model.DocQA) error

	// CreateRefer 创建refer
	CreateRefer(ctx context.Context, refers []model.Refer) error
	// GetRefersByBusinessIDs 通过business_id获取refer
	GetRefersByBusinessIDs(ctx context.Context, robotID uint64, businessIDs []uint64) ([]*model.Refer, error)
	// GetRefersByBusinessID 通过business_id获取refer
	GetRefersByBusinessID(ctx context.Context, businessID uint64) (*model.Refer, error)
	// MarkRefer .
	MarkRefer(ctx context.Context, robotID, businessID uint64, mark uint32) error

	// ReleaseDocRebuild 发布文档重建
	ReleaseDocRebuild(ctx context.Context, versionID uint64) error
	// AddShortURL 新增短链接
	AddShortURL(ctx context.Context, name string, url string) (string, error)
	// ShortURLToCosPath 获取短链接对应的cos path
	ShortURLToCosPath(ctx context.Context, code string) (string, error)
	// GetRejectedQuestionList 获取拒答问题列表
	GetRejectedQuestionList(ctx context.Context,
		req model.GetRejectedQuestionListReq) (uint64, []*model.RejectedQuestion, error)
	// CreateRejectedQuestion 创建拒答问题
	CreateRejectedQuestion(ctx context.Context, rejectedQuestion *model.RejectedQuestion) error
	// UpdateRejectedQuestion 修改拒答问题
	UpdateRejectedQuestion(ctx context.Context, rejectedQuestion *model.RejectedQuestion, isNeedPublish bool) error
	// DeleteRejectedQuestion 删除拒答问题
	DeleteRejectedQuestion(ctx context.Context, corpID, robotID uint64,
		rejectedQuestions []*model.RejectedQuestion) error
	// GetModifyRejectedQuestionCount 获取待发布的拒答问题数量
	GetModifyRejectedQuestionCount(ctx context.Context, corpID, robotID, versionID uint64,
		question string, releaseStatuses []uint32) (uint64, error)
	// GetModifyRejectedQuestionList 获取拒答问题列表
	GetModifyRejectedQuestionList(ctx context.Context, corpID, robotID, versionID uint64, question string,
		page, pageSize uint32) ([]*model.ReleaseRejectedQuestion, error)
	// GetReleaseRejectedQuestionCount 获取拒答问题数量
	GetReleaseRejectedQuestionCount(ctx context.Context, corpID, robotID uint64, question string, startTime,
		endTime time.Time, status []uint32) (uint64, error)
	// GetReleaseRejectedQuestionList 获取待发布拒答问题列表
	GetReleaseRejectedQuestionList(ctx context.Context, corpID, robotID uint64, page, pageSize uint32,
		query string, startTime, endTime time.Time, status []uint32) ([]*model.RejectedQuestion, error)
	// GetRejectedQuestionByID Deprecated  按 ID 查询拒答问题
	GetRejectedQuestionByID(ctx context.Context, corpId, robotId, id uint64) (*model.RejectedQuestion, error)
	// GetRejectedQuestionByBizID 按bizID查询拒答问题
	GetRejectedQuestionByBizID(ctx context.Context, corpId, robotId, bizID uint64) (*model.RejectedQuestion, error)
	// GetRejectedQuestionByIDs Deprecated 按 IDs 批量查询拒答问题
	GetRejectedQuestionByIDs(ctx context.Context, corpID uint64, id []uint64) ([]*model.RejectedQuestion, error)
	// GetRejectedQuestionByBizIDs 按多个bizID查询拒答问题
	GetRejectedQuestionByBizIDs(ctx context.Context, corpID uint64, id []uint64) ([]*model.RejectedQuestion, error)
	// GetReleaseModifyRejectedQuestion 查询修改的拒答问题
	GetReleaseModifyRejectedQuestion(ctx context.Context, release *model.Release,
		rejectedQuestion []*model.RejectedQuestion) (map[uint64]*model.ReleaseRejectedQuestion, error)
	// PublishRejectedQuestion 拒答问题发布完成
	PublishRejectedQuestion(ctx context.Context, question *model.RejectedQuestion,
		modifyRejectedQuestion *model.ReleaseRejectedQuestion) error
	// GetReleaseRejectedQuestionByVersion 按 Version 版本获取拒答问题发布列表
	GetReleaseRejectedQuestionByVersion(ctx context.Context, corpID uint64, robotID uint64, versionID uint64) (
		[]*model.ReleaseRejectedQuestion, error)
	// CreateExportTask 新建导出任务
	CreateExportTask(ctx context.Context, corpID, staffID, robotID uint64, export model.Export,
		params model.ExportParams) (uint64, error)
	// CreateExportEvaluateTask 新建评测任务导出
	CreateExportEvaluateTask(ctx context.Context, corpID, bizID, robotID uint64) (string, error)
	// UpdateExport 更新导出任务
	UpdateExport(ctx context.Context, export model.Export) error
	// GetExportInfo 查询导出任务信息
	GetExportInfo(ctx context.Context, taskID uint64) (*model.Export, error)
	// GetExportTaskInfo 查询导出任务信息
	GetExportTaskInfo(ctx context.Context, taskID, robotID, corpID uint64) (*model.Export, error)
	// AddUnsatisfiedReply 创建不满意回复
	AddUnsatisfiedReply(ctx context.Context, unsatisfiedReply *model.UnsatisfiedReplyInfo) error
	// UpdateUnsatisfiedReplyStatus 更新不满意回复状态
	UpdateUnsatisfiedReplyStatus(ctx context.Context, corpID, robotID uint64, ids []uint64, oldStatus,
		status uint32) error
	// GetUnsatisfiedReplyTotal 获取不满意回复数量
	GetUnsatisfiedReplyTotal(ctx context.Context, req *model.UnsatisfiedReplyListReq) (uint64, error)
	// GetUnsatisfiedReplyList 获取不满意回复列表
	GetUnsatisfiedReplyList(ctx context.Context, req *model.UnsatisfiedReplyListReq) (
		[]*model.UnsatisfiedReplyInfo, error)
	// GetUnsatisfiedReplyByRecordID 通过记录ID获取不满意回复
	GetUnsatisfiedReplyByRecordID(ctx context.Context, corpID, robotID uint64, recordID string) (
		*model.UnsatisfiedReplyInfo, error)
	// UpdateUnsatisfiedReply 更新不满意回复
	UpdateUnsatisfiedReply(ctx context.Context, unsatisfiedReply *model.UnsatisfiedReplyInfo) error
	// GetUnsatisfiedReplyByIDs 通过不满意回复ID获取不满意记录
	GetUnsatisfiedReplyByIDs(ctx context.Context, corpID, robotID uint64, ids []uint64) (
		[]*model.UnsatisfiedReplyInfo, error)
	// GetCorpStaffByIDs 通过员工ID获取员工信息
	GetCorpStaffByIDs(ctx context.Context, ids []uint64) ([]*model.CorpStaff, error)
	// GetSampleSetByCosHash 通过cos_hash获取评测文档
	GetSampleSetByCosHash(ctx context.Context, corpID, robotID uint64, cosHash string) (*model.SampleSet, error)
	// CreateSampleSet 创建样本集
	CreateSampleSet(ctx context.Context, set *model.SampleSet, sampleRecord []model.SampleRecord) error
	// GetSampleSets 分页获取样本集列表
	GetSampleSets(ctx context.Context, corpID, robotID uint64, setName string, page,
		pageSize uint32) (uint64, []*model.SampleSet, error)
	// DeleteSampleSets 删除样本集
	DeleteSampleSets(ctx context.Context, corpID, robotID uint64, ids []uint64) error
	// GetSampleRecordsBySetIDs 根据集合ID获取样本列表
	GetSampleRecordsBySetIDs(ctx context.Context, setIDs []uint64) ([]*model.Sample, error)
	// CreateTest 创建评测任务
	CreateTest(ctx context.Context, test *model.RobotTest) error
	// GetRunningTests 查询使用该样本集的评测中的任务列表
	GetRunningTests(ctx context.Context, corpID, robotID uint64, setIDs []string) ([]*model.RobotTest, error)
	// GetTestByTestID 查询一个评测任务
	GetTestByTestID(ctx context.Context, testID uint64) (*model.RobotTest, error)
	// StopTest 事务批量停止评测任务
	StopTest(ctx context.Context, robotID uint64, corpID uint64, testIDs []uint64) error
	// DeleteTest 事务批量删除评测任务
	DeleteTest(ctx context.Context, robotID uint64, corpID uint64, testIDs []uint64) error
	// RetryTest 事务批量重试评测任务
	RetryTest(ctx context.Context, robotID uint64, corpID uint64, testIDs []uint64) error
	// GetTestByName 查询一个同名评测任务
	GetTestByName(ctx context.Context, corpID, robotID uint64, testName string) (*model.RobotTest, error)
	// GetTests 分页查询机器人评测任务
	GetTests(ctx context.Context, corpID, robotID uint64, testName string, page,
		pageSize uint64) (uint64, []*model.RobotTest, error)
	// GetRecordToJudge 获取一个待判断的记录
	GetRecordToJudge(ctx context.Context, testID uint64, appKey string) (*model.RobotTestRecord,
		*chat.MsgRecord,
		error)
	// GetTestRecordByID 查询标注的记录
	GetTestRecordByID(ctx context.Context, testID uint64, id uint64) (*model.RobotTestRecord, error)
	// UpdateTestStatus 更新任务状态
	UpdateTestStatus(ctx context.Context, test *model.RobotTest) error
	// UpdateTestDoneNum 更新任务完成数
	UpdateTestDoneNum(ctx context.Context, corpID, robotID, testID uint64) error
	// UpdateTestTaskID 更新机器人任务ID
	UpdateTestTaskID(ctx context.Context, taskID, testID uint64) error
	// JudgeTestRecord 标注记录
	JudgeTestRecord(ctx context.Context, testID, recordID, staffID, judge uint64) error
	// UpdateTestRecord 更新记录
	UpdateTestRecord(ctx context.Context, record *model.RobotTestRecord) error
	// UpdateTestRecordFromSelf 更新首条记录信息
	UpdateTestRecordFromSelf(ctx context.Context, record *model.RobotTestRecord) error
	// DeleteRobotTests 批量删除评测任务
	DeleteRobotTests(ctx context.Context, corpID, robotID uint64, ids []uint64) error
	// GetRecordByTestIDs 查询评测任务的全部待评测记录
	GetRecordByTestIDs(ctx context.Context, testID uint64) ([]*model.RobotTestRecord, error)

	// ParseDocXlsxCharSize 解析文档(xlsx)字符数
	ParseDocXlsxCharSize(ctx context.Context, fileName string, cosURL string, fileType string) (int, error)
	// GetRobotQACharSize TODO
	// ParseDocCharSize 解析文档的字符大小
	// ParseDocCharSize(ctx context.Context, fileName string, cosURL string, fileType string) (int, error)
	// GetRobotQACharSize 获取单个机器人问答字符总数
	GetRobotQACharSize(ctx context.Context, robotID uint64, corpID uint64) (uint64, error)
	// GetRobotDocCharSize 获取机器人总文档大小
	GetRobotDocCharSize(ctx context.Context, robotID uint64, corpID uint64) (uint64, error)

	// GetRobotQAExceedCharSize 获取机器人超量问答字符总数
	GetRobotQAExceedCharSize(ctx context.Context, corpID uint64, robotIDs []uint64) (
		map[uint64]uint64, error)
	// GetRobotDocExceedCharSize 获取机器人超量总文档大小
	GetRobotDocExceedCharSize(ctx context.Context, corpID uint64, robotIDs []uint64) (
		map[uint64]uint64, error)

	// GetListRole 获取机器人角色配置
	GetListRole(ctx context.Context) ([]*model.RobotRole, error)
	// GetTaskTotal 获取任务数量
	GetTaskTotal(ctx context.Context, tableName string, robotID uint64, taskTypes []uint32) (uint64, error)
	// GetTaskList 获取任务列表
	GetTaskList(ctx context.Context, tableName string, robotID uint64, taskTypes []uint32, page,
		pageSize uint32) ([]*model.Task, error)
	// GetTaskHistoryTotal 获取历史任务数量
	GetTaskHistoryTotal(ctx context.Context, tableName string, robotID uint64, taskTypes []uint32) (uint64, error)
	// GetTaskHistoryList 获取历史任务列表
	GetTaskHistoryList(ctx context.Context, tableName string, robotID uint64, taskTypes []uint32, page,
		pageSize uint32) ([]*model.TaskHistory, error)
	// GetUserByAccount 通过账户获取用户信息
	GetUserByAccount(ctx context.Context, account string) (*model.User, error)
	// GetUserByIDs 批量获取用户信息
	GetUserByIDs(ctx context.Context, ids []uint64) (map[uint64]*model.User, error)
	// UpdateUserPassword 更新用户密码
	UpdateUserPassword(ctx context.Context, user *model.User) error
	// SendAppealNotice 发送申诉通知
	SendAppealNotice(ctx context.Context, appealType uint32, audit *model.Audit) error
	// InfoSecCreateAppeal 调用 infoSec 批量创建父申诉申请
	InfoSecCreateAppeal(ctx context.Context, appealList []*model.Appeal, appealType uint32, robotID, corpID,
		staffID uint64, reason string) error
	// UpdateAppealQA 更新申诉单状态
	UpdateAppealQA(ctx context.Context, qaDetails map[uint64]*model.DocQA) error
	// UpdateAuditListStatus 更新审核单状态
	UpdateAuditListStatus(ctx context.Context, auditList []*model.Audit) error
	// UpdateAuditStatus 更新审核状态
	UpdateAuditStatus(ctx context.Context, audit *model.Audit) error
	// TestUpdateAuditStatusByParentID 根据父审核id更新子审核状态，仅用于测试
	TestUpdateAuditStatusByParentID(ctx context.Context, parentAudit *model.Audit) error
	// UpdateAuditStatusByParentID 根据父审核id更新子审核状态
	UpdateAuditStatusByParentID(ctx context.Context, parentAudit *model.Audit, limit int64) error

	// GetAttributeTotal 查询属性标签属性数量
	GetAttributeTotal(ctx context.Context, robotID uint64, query string, ids []uint64) (uint64, error)
	// GetAttributeList 查询属性标签属性列表
	GetAttributeList(ctx context.Context, robotID uint64, query string, page, pageSize uint32, ids []uint64) (
		[]*model.Attribute, error)
	// GetAttributeByIDs 通过属性IDs获取属性信息
	GetAttributeByIDs(ctx context.Context, robotID uint64, ids []uint64) (
		map[uint64]*model.Attribute, error)
	// GetAttributeListByIDs 通过属性IDs获取属性列表信息
	GetAttributeListByIDs(ctx context.Context, robotID uint64, ids []uint64) ([]*model.Attribute, error)
	// GetAttributeByBizIDs 通过属性BizIDs获取属性信息
	GetAttributeByBizIDs(ctx context.Context, robotID uint64, ids []uint64) (map[uint64]*model.Attribute, error)
	// GetAttributeByKeys 通过属性标识获取属性信息
	GetAttributeByKeys(ctx context.Context, robotID uint64, keys []string) (
		map[string]*model.Attribute, error)
	// GetAttributeByNames 通过属性名称获取属性信息
	GetAttributeByNames(ctx context.Context, robotID uint64, names []string) (
		map[string]*model.Attribute, error)
	// GetAttributeKeysDelStatusAndIDs 获取属性标签的删除状态和id
	GetAttributeKeysDelStatusAndIDs(ctx context.Context, robotID uint64, attrKeys []string) (
		map[string]*model.Attribute, error)
	// GetAttributeByRobotID 查询机器人下的属性标签属性信息
	GetAttributeByRobotID(ctx context.Context, robotID uint64) (map[string]struct{}, map[string]struct{}, error)
	// GetAttributeKeyAndIDsByRobotID 查询机器人下的属性key和id
	GetAttributeKeyAndIDsByRobotID(ctx context.Context, robotID uint64) ([]*model.AttributeKeyAndID, error)
	// GetAttributeKeyAndIDsByRobotIDProd 查询发布环境机器人下的属性key和id
	GetAttributeKeyAndIDsByRobotIDProd(ctx context.Context, robotID uint64) ([]*model.AttributeKeyAndID, error)
	// BatchCreateAttribute 批量创建属性标签属性
	BatchCreateAttribute(ctx context.Context, attrLabels []*model.AttributeLabelItem) error
	// UpdateAttribute 更新属性标签属性
	UpdateAttribute(ctx context.Context, req *model.UpdateAttributeLabelReq, oldAttr *model.Attribute,
		corpID, staffID uint64, needUpdateCacheFlag bool, newLabelRedisValue []model.AttributeLabelRedisValue) (
		uint64, error)
	// UpdateAttributeTask 更新属性标签任务
	UpdateAttributeTask(ctx context.Context, attributeLabelTask *model.AttributeLabelTask) error
	// GetUpdateAttributeTask 查询编辑标签任务状态
	GetUpdateAttributeTask(ctx context.Context, taskID, corpID, robotID uint64) (
		*model.AttributeLabelTask, error)
	// DeleteAttribute 删除属性标签属性
	DeleteAttribute(ctx context.Context, robotID uint64, ids []uint64, attrKeys []string) error
	// GetAttributeLabelCount 获取属性标签数量
	GetAttributeLabelCount(ctx context.Context, attrID uint64, query string, queryScope string, robotID uint64) (uint64, error)
	// GetAttributeLabelList 获取属性标签列表
	GetAttributeLabelList(ctx context.Context, attrID uint64, query string, queryScope string, lastLabelID uint64, limit uint32, robotID uint64) (
		[]*model.AttributeLabel, error)
	// GetAttributeLabelByIDOrder 根据输入的id的顺序查询AttributeLabel
	GetAttributeLabelByIDOrder(ctx context.Context, robotID uint64, ids []uint64) ([]*model.AttributeLabel, error)
	// GetAttributeLabelByAttrIDs 获取指定属性下的标签信息
	GetAttributeLabelByAttrIDs(ctx context.Context, attrIDs []uint64, robotID uint64) (map[uint64][]*model.AttributeLabel, error)
	// GetAttributeLabelByAttrIDsWithNotEmptySimilarLabelProd 获取发布环境指定属性下相似标签不为空的标签信息
	GetAttributeLabelByAttrIDsWithNotEmptySimilarLabelProd(ctx context.Context, attrIDs []uint64, robotID uint64) (map[uint64][]*model.AttributeLabel, error)
	// GetAttributeLabelByIDs 获取指定标签ID的信息
	GetAttributeLabelByIDs(ctx context.Context, ids []uint64, robotID uint64) (map[uint64]*model.AttributeLabel, error)
	// GetAttributeLabelByBizIDs 获取指定标签ID的信息
	GetAttributeLabelByBizIDs(ctx context.Context, ids []uint64, robotID uint64) (map[uint64]*model.AttributeLabel, error)
	// GetAttributeLabelByName 检索标签名或相似标签名
	GetAttributeLabelByName(ctx context.Context, attrID uint64, name string, robotID uint64) ([]*model.AttributeLabel, error)
	// GetDocAttributeLabel 获取文档的属性标签信息
	GetDocAttributeLabel(ctx context.Context, robotID uint64, docIDs []uint64) (
		[]*model.DocAttributeLabel, error)
	// GetDocAttributeLabelCountByAttrLabelIDs 通过属性和标签ID获取文档属性标签数量
	GetDocAttributeLabelCountByAttrLabelIDs(ctx context.Context, robotID uint64, source uint32, attrIDs,
		labelIDs []uint64) (uint64, error)
	// GetDocAttributeLabelByAttrLabelIDs 通过属性和标签ID获取文档属性标签
	GetDocAttributeLabelByAttrLabelIDs(ctx context.Context, robotID uint64, source uint32, attrIDs,
		labelIDs []uint64, page, pageSize uint32) ([]*model.DocAttributeLabel, error)
	// GetDocCountByAttributeLabel 通过关联的属性标签获取文档数量
	GetDocCountByAttributeLabel(ctx context.Context, robotID uint64, noStatusList []uint32, attrID uint64,
		labelIDs []uint64) (uint64, error)
	// GetDocAttributeLabelDetail 获取文档的属性标签详情
	GetDocAttributeLabelDetail(ctx context.Context, robotID uint64, docIDs []uint64) (
		map[uint64][]*model.AttrLabel, error)
	// GetQAAttributeLabel 获取QA的属性标签信息
	GetQAAttributeLabel(ctx context.Context, robotID uint64, qaIDs []uint64) ([]*model.QAAttributeLabel, error)
	// GetQAAttributeLabelCountByAttrLabelIDs 通过属性和标签ID获取QA属性标签数量
	GetQAAttributeLabelCountByAttrLabelIDs(ctx context.Context, robotID uint64, source uint32, attrIDs,
		labelIDs []uint64) (uint64, error)
	// GetQAAttributeLabelByAttrLabelIDs 通过属性和标签ID获取QA属性标签
	GetQAAttributeLabelByAttrLabelIDs(ctx context.Context, robotID uint64, source uint32, attrIDs,
		labelIDs []uint64, page, pageSize uint32) ([]*model.QAAttributeLabel, error)
	// GetQACountByAttributeLabel 通过关联的属性标签获取QA数量
	GetQACountByAttributeLabel(ctx context.Context, robotID uint64, noReleaseStatusList []uint32, attrID uint64,
		labelIDs []uint64) (uint64, error)
	// GetQAAttributeLabelDetail 获取QA的属性标签详情
	GetQAAttributeLabelDetail(ctx context.Context, robotID uint64, qaIDs []uint64) (
		map[uint64][]*model.AttrLabel, error)
	// GetAttributeLabelsRedis 获取属性标签redis
	GetAttributeLabelsRedis(ctx context.Context, robotID uint64, attrKey, envType string) (
		[]model.AttributeLabelRedisValue, error)
	// SetAttributeLabelsRedis 添加属性标签redis
	SetAttributeLabelsRedis(ctx context.Context, robotID uint64, attrKey string,
		redisValue []model.AttributeLabelRedisValue, envType string) error
	// PiplineDelAttributeLabelRedis 批量删除属性标签redis
	PiplineDelAttributeLabelRedis(ctx context.Context, robotID uint64, attrKeys []string, envType string) error
	// PiplineSetAttributeLabelRedis 批量删除属性标签redis
	PiplineSetAttributeLabelRedis(ctx context.Context, robotID uint64,
		attrKey2RedisValue map[string][]model.AttributeLabelRedisValue, envType string) error
	// UpdateAttributeSuccess 属性标签更新成功
	UpdateAttributeSuccess(ctx context.Context, attr *model.Attribute, corpID, staffID uint64) error
	// UpdateAttributeFail 属性标签更新失败
	UpdateAttributeFail(ctx context.Context, attr *model.Attribute, corpID, staffID uint64) error
	// GetLinkContentsFromSearchVectorResponse 从检索请求构造 linkContents
	GetLinkContentsFromSearchVectorResponse(
		ctx context.Context, robotID uint64, docs []*pb.SearchVectorRsp_Doc,
		qaFn func(doc *pb.SearchVectorRsp_Doc, qa *model.DocQA) any,
		segmentFn func(doc *pb.SearchVectorRsp_Doc, segment *model.DocSegmentExtend) any,
		searchEngineFn func(doc *pb.SearchVectorRsp_Doc) any,
	) ([]linker.Content, error)
	// GetLinkContentsFromSearchResponse 从检索请求构造 linkContents
	GetLinkContentsFromSearchResponse(
		ctx context.Context, robotID uint64, docs []*pb.SearchRsp_Doc,
		qaFn func(doc *pb.SearchRsp_Doc, qa *model.DocQA) any,
		segmentFn func(doc *pb.SearchRsp_Doc, segment *model.DocSegmentExtend) any,
		searchEngineFn func(doc *pb.SearchRsp_Doc) any,
	) ([]linker.Content, error)

	// GetGlobalKnowledge 通过id获取知识库
	GetGlobalKnowledge(ctx context.Context, id model.GlobalKnowledgeID) (model.GlobalKnowledge, error)
	// GetGlobalKnowledges 通过id获取知识库
	GetGlobalKnowledges(ctx context.Context, ids []model.GlobalKnowledgeID) ([]model.GlobalKnowledge, error)
	// InsertGlobalKnowledge 插入知识库
	InsertGlobalKnowledge(ctx context.Context, knowledge model.GlobalKnowledge) (model.GlobalKnowledgeID, error)
	// UpdateGlobalKnowledge 更新知识库
	UpdateGlobalKnowledge(ctx context.Context, knowledge model.GlobalKnowledge) error
	// DeleteGlobalKnowledge 删除知识库
	DeleteGlobalKnowledge(ctx context.Context, id model.GlobalKnowledgeID) error
	// ListGlobalKnowledge 知识库列表
	ListGlobalKnowledge(ctx context.Context, query string, pageNumber, pageSize uint32) (uint64,
		[]*model.GlobalKnowledge, error)

	// EnableCorp 启用企业
	EnableCorp(ctx context.Context, corps []*model.Corp) error
	// DisableCorp 禁用企业
	DisableCorp(ctx context.Context, corps []*model.Corp) error

	// GetSystemIntegrator 获取集成商信息
	GetSystemIntegrator(ctx context.Context, uin, subAccountUin string) (*model.SystemIntegrator, error)
	// GetSystemIntegratorByID 通过ID获取集成商信息
	GetSystemIntegratorByID(ctx context.Context, id int) (*model.SystemIntegrator, error)
	// IsSystemIntegrator 是否是集成商
	IsSystemIntegrator(ctx context.Context, corp *model.Corp) bool

	// GetSampleSetsByBizIDs 分页获取样本集列表
	GetSampleSetsByBizIDs(ctx context.Context, corpID, robotID uint64,
		ids []uint64) (map[uint64]*model.SampleSet, error)
	// GetTestByTestBizID 根据业务ID查询评测任务
	GetTestByTestBizID(ctx context.Context, testBizID uint64) (*model.RobotTest, error)
	// GetTestByTestBizIDs 根据业务ID查询评测任务
	GetTestByTestBizIDs(ctx context.Context, corpID, robotID uint64, testBizIDs []uint64) ([]*model.RobotTest, error)

	// GetDeleteSampleSets 获取需要删除的样本集
	GetDeleteSampleSets(ctx context.Context, corpID, robotID uint64) ([]*model.SampleSet, error)
	// DeleteTestRecords 删除样本集下所有评测结果
	DeleteTestRecords(ctx context.Context, setID uint64) error
	// DeleteEvaluateTests 删除样本集下所有评测任务
	DeleteEvaluateTests(ctx context.Context, corpID, robotID, setID uint64) error
	// DeleteSampleSetRecords 删除样本集下所有样本
	DeleteSampleSetRecords(ctx context.Context, setID uint64) error
	// DeleteSampleSet 删除应用所有样本集
	DeleteSampleSet(ctx context.Context, corpID, robotID uint64) error
	// DeleteSampleSetBySetID 删除应用样本集BySetID
	DeleteSampleSetBySetID(ctx context.Context, corpID, robotID, setID uint64) error

	// GetTestRecordByBizID 查询标注的记录
	GetTestRecordByBizID(ctx context.Context, id uint64) (*model.RobotTestRecord, error)
	// GetUnsatisfiedReplyByBizIDs 通过不满意回复BizID获取不满意记录
	GetUnsatisfiedReplyByBizIDs(ctx context.Context, corpID, robotID uint64, ids []uint64) (
		[]*model.UnsatisfiedReplyInfo, error)
	// UpdateReleaseCallbackStatus 更新发布通知回调情况
	UpdateReleaseCallbackStatus(ctx context.Context, release *model.Release, oldStatus uint32) error
	// SendDataSyncTask 任务型通知事件
	SendDataSyncTask(ctx context.Context, robotID, versionID, corpID, staffID uint64, event string) (
		*taskFlow.SendDataSyncTaskEventRsp, error)
	// GetDataSyncTaskDetail 任务型获取事件详情
	GetDataSyncTaskDetail(ctx context.Context, robotID, versionID, corpID, staffID uint64) (
		*taskFlow.GetDataSyncTaskRsp, error)
	// RetryTaskConfigRelease 重试任务型暂停的发布
	RetryTaskConfigRelease(ctx context.Context, release *model.Release) error
	// GetUnreleasedTaskQACount 获取任务型待发布数量
	GetUnreleasedTaskQACount(ctx context.Context, robotBizID, corpID, staffID uint64) (uint32, error)
	// CreateDocQATaskRecord 创建一条文档生成QA任务
	CreateDocQATaskRecord(ctx context.Context, qaTask *model.DocQATask, doc *model.Doc) (uint64, error)
	// SendRetryPausedReleaseNotice 重试发布发送发布中消息
	SendRetryPausedReleaseNotice(ctx context.Context, release *model.Release) error
	// GetVarList 获取自定义变量数据
	GetVarList(ctx context.Context, appBizId string, varIDs []string) (*admin.FetchVarListRsp, error)
	// DescribeUserPermissions 获取主账号权限数据
	DescribeUserPermissions(ctx context.Context, uin string) (*model.DescribeUserPermissionsRsp, error)
	// MustCreateBusinessAdministrator 确保创建主账号
	MustCreateBusinessAdministrator(ctx context.Context, uin, desc string, pp model.ProductPermission) error
	// DescribeProductPermissions 获取产品对应的权限数据
	DescribeProductPermissions(ctx context.Context) (*model.DescribeProductPermissionsRsp, error)
	// GetUserAppCategory 获取用户应用分类
	GetUserAppCategory(ctx context.Context, sid int, uin, subAccountUin string) ([]string, []model.AppCategory, error)
	// BatchCreateReleaseAudit 批量创建发布审核数据
	BatchCreateReleaseAudit(ctx context.Context, parent *model.Audit, audits []*model.Audit,
		p model.AuditSendParams) ([]*model.Audit, error)
	// GetAuditByEtag 通过tag获取文件是否已经审核通过
	GetAuditByEtag(ctx context.Context, robotID, corpID, relateID uint64, eTag string) ([]*model.Audit,
		error)
	// IsUsedCharSizeExceeded 校验字符使用量是否已经超过限
	IsUsedCharSizeExceeded(ctx context.Context, corpID, robotID uint64) error

	// GetAppsByCorpID 通过企业ID获取应用信息
	GetAppsByCorpID(ctx context.Context, corpID uint64) ([]*model.AppDB, error)
	// GetAppsByCorpIDAndBizIDList 通过企业ID和应用ID获取应用信息
	GetAppsByCorpIDAndBizIDList(ctx context.Context, corpID uint64, appBizIDList []uint64) ([]*model.AppDB, error)
	// GetAllValidAppIDs 获取所有应用ID
	GetAllValidAppIDs(ctx context.Context) ([]uint64, error)
	// GetAppByName 获取机器人名称
	GetAppByName(ctx context.Context, corpID uint64, name string) (*model.AppDB, error)
	// CreateAppVectorIndex 按应用维度创建相似库、评测库
	CreateAppVectorIndex(ctx context.Context, appDB *model.AppDB) error
	// GetAppByAppBizID 通过对外ID获取应用信息
	GetAppByAppBizID(ctx context.Context, bID uint64) (*model.AppDB, error)
	// GetAppByID 获取应用信息
	GetAppByID(ctx context.Context, id uint64) (*model.AppDB, error)
	// GetBotBizIDByID 获取应用business_id, 带缓存
	GetBotBizIDByID(ctx context.Context, id uint64) (uint64, error)
	// ModifyApp 更新应用配置信息
	ModifyApp(ctx context.Context, appDB *model.AppDB) error
	// DeleteApp 删除应用
	DeleteApp(ctx context.Context, appDB *model.AppDB) error
	// UpdateAppCharSize 更新应用使用字符数
	UpdateAppCharSize(ctx context.Context, id, size uint64) error
	// UpdateAppUsedCharSizeTx 更新应用使用字符数(支持事物)
	UpdateAppUsedCharSizeTx(ctx context.Context, charSize int64, appID uint64) error
	// UpdateAppUsedCharSizeNotTx 更新应用使用字符数
	UpdateAppUsedCharSizeNotTx(ctx context.Context, charSize int64, appID uint64) error
	// GetAppCount 获取应用数量
	GetAppCount(ctx context.Context, corpID uint64, staffIDs, appBizIDList []uint64,
		appTypeList []string, deleteFlags []uint32, keywords, appKey string) (uint64, error)
	// GetAppList 获取应用列表
	GetAppList(ctx context.Context, corpID uint64, staffIDs, appBizIDList []uint64, appTypeList []string,
		deleteFlags []uint32, keywords, appKey string, page, pageSize uint32) ([]*model.AppDB, error)
	// GetAppListOrderByUsedCharSize 获取应用列表按照字符数使用情况排序
	GetAppListOrderByUsedCharSize(ctx context.Context, corpID uint64, appBizIDList []uint64, appTypeList []string,
		deleteFlags []uint32, page, pageSize uint32) ([]*model.AppDB, error)
	// GetConfigHistoryByVersionID 通过发布单ID获取历史配置信息
	GetConfigHistoryByVersionID(ctx context.Context, robotID, versionID uint64) (
		*model.RobotConfigHistory, error)
	// GetReleaseConfigItemByID 通过ID获取发布的配置内容
	GetReleaseConfigItemByID(ctx context.Context, id uint64) (*model.ReleaseConfig, error)
	// UpdateAuditConfigItem 更新配置审核状态
	UpdateAuditConfigItem(ctx context.Context, cfg *model.ReleaseConfig) error
	// GetModifyReleaseConfigCount 发布配置项预览数量
	GetModifyReleaseConfigCount(ctx context.Context, versionID uint64,
		releaseStatuses []uint32, query string) (uint64, error)
	// GetConfigItemByVersionID 获取发布配置内容列表
	GetConfigItemByVersionID(ctx context.Context, versionID uint64) ([]*model.ReleaseConfig, error)
	// GetInAppealConfigItem 获取正在申诉的发布配置内容列表
	GetInAppealConfigItem(ctx context.Context, robotID uint64) ([]*model.ReleaseConfig, error)
	// GetAppByAppKey 通过应用key获取应用信息
	GetAppByAppKey(ctx context.Context, appKey string) (*model.AppDB, error)
	// ListConfigByVersionID 获取发布配置内容列表
	ListConfigByVersionID(ctx context.Context, versionID uint64, query string, page, pageSize uint32, status []uint32) (
		[]*model.ReleaseConfig, error)
	// GetAppByIDRange 通过ID范围获取机器人信息
	GetAppByIDRange(ctx context.Context, startID, endID uint64, limit uint32) ([]*model.AppDB, error)
	// SyncAppData 同步机器人数据（机器人转化为应用数据）
	SyncAppData(ctx context.Context, apps []*model.AppDB) error
	// FlushAppData 同步机器人数据（机器人转化为应用数据）
	FlushAppData(ctx context.Context, apps []*model.AppDB) error
	// DescribeNickname 获取昵称
	DescribeNickname(ctx context.Context, uin, subAccountUin string) (*cloudModel.NicknameInfo, error)
	// BatchCheckWhitelist 批量检查白名单
	BatchCheckWhitelist(ctx context.Context, key, uin string) (bool, error)
	// ModifyAppTokenUsage 更新应用token用量
	ModifyAppTokenUsage(ctx context.Context, app *model.AppDB) error
	// GetSIUserList 获取集成商用户信息
	GetSIUserList(ctx context.Context, sid int, uin string) ([]*model.User, error)
	// UpdateUserCloudNickname 更新用户云端昵称失败
	UpdateUserCloudNickname(ctx context.Context, user *model.User) error
	// UpdateStaffCloudNickname 更新员工云端昵称
	UpdateStaffCloudNickname(ctx context.Context, staff *model.CorpStaff) error
	// GetValidTrailCorpList 获取有效的试用企业
	GetValidTrailCorpList(ctx context.Context) ([]*model.Corp, error)
	// GetTrailCorpTokenUsage 获取试用企业token用量
	GetTrailCorpTokenUsage(ctx context.Context, corpID uint64) (uint64, error)
	// GetCorpTokenUsage 获取企业token用量
	GetCorpTokenUsage(ctx context.Context, corpID uint64) (uint64, error)
	// GetCorpUsedCharSizeUsage 获取试用企业字符用量
	GetCorpUsedCharSizeUsage(ctx context.Context, corpID uint64) (uint64, error)
	// GetBotUsedCharSizeUsage 获取某个应用字符用量
	GetBotUsedCharSizeUsage(ctx context.Context, botBizID uint64) (uint64, error)
	// UpdateTrialCorpAppStatus 更新试用企业机器人状态
	UpdateTrialCorpAppStatus(ctx context.Context, corpID uint64, appStatus uint32, appStatusReason string) error
	// ModifyAppJSON 更新应用配置信息
	ModifyAppJSON(ctx context.Context, appDB *model.AppDB) error
	// ModifyAppPreviewJSON 更新应用待发布配置信息
	ModifyAppPreviewJSON(ctx context.Context, appDB *model.AppDB) error
	// GetAuditFailReleaseQA 获取审核失败的问答ID
	GetAuditFailReleaseQA(ctx context.Context, versionID uint64, message string) (uint64, error)
	// GetWaitEmbeddingUpgradeApp 获取待升级 embedding 的应用
	GetWaitEmbeddingUpgradeApp(
		ctx context.Context, ids []uint64, fromEmbVer uint64, toEmbVer uint64,
	) ([]model.AppDB, error)
	// GetWaitOrgDataSyncApp 获取待同步 org_data 的应用
	GetWaitOrgDataSyncApp(ctx context.Context, ids []uint64) ([]model.AppDB, error)
	// GetAllGlobalKnowledge 获取所有全局知识(不包括已删除的记录)
	GetAllGlobalKnowledge(ctx context.Context) ([]model.GlobalKnowledge, error)
	// GetQAChunk 分段获取问答
	GetQAChunk(ctx context.Context, corpID, appID, offset, limit uint64) ([]*model.DocQA, error)
	// GetQAChunkCount 获取问答总数
	GetQAChunkCount(ctx context.Context, corpID, appID uint64) (int, error)
	// GetSimilarChunkCount 获取相似问总数
	GetSimilarChunkCount(ctx context.Context, corpID, appID uint64) (int, error)
	// GetQASimilarQuestionsCount 获取qa的相似问总数
	GetQASimilarQuestionsCount(ctx context.Context, corpID, appID uint64) (int, error)
	// GetRejectChunk 分段获取拒答
	GetRejectChunk(ctx context.Context, corpID, appID, offset, limit uint64) ([]*model.RejectedQuestion, error)
	// GetRejectChunkCount 获取拒答总数
	GetRejectChunkCount(ctx context.Context, corpID, appID uint64) (int, error)
	// GetSegmentChunk 分段获取文段
	GetSegmentChunk(ctx context.Context, corpID, appID, offset, limit uint64) ([]*model.DocSegment, error)
	// GetSegmentChunkCount 获取文段总数
	GetSegmentChunkCount(ctx context.Context, corpID, appID uint64) (int, error)
	// GetSegmentPageInfosBySegIDs 通过SegIDs获取切片的页码信息
	GetSegmentPageInfosBySegIDs(ctx context.Context, robotID uint64, segIDs []uint64) (
		map[uint64]*model.DocSegmentPageInfo, error)

	// GetSegmentSyncChunk 分段获取同步文段
	GetSegmentSyncChunk(ctx context.Context, corpID, appID, offset, limit uint64) ([]*model.DocSegment, error)
	// GetSegmentSyncChunkCount 获取同步文段总数
	GetSegmentSyncChunkCount(ctx context.Context, corpID, appID uint64) (int, error)
	// UpdateSegmentSyncOrgDataBizID 更新同步文段 org_data_biz_id
	UpdateSegmentSyncOrgDataBizID(ctx context.Context, robotID, docID, corpID, staffID uint64, ids []uint64, orgDataBizID uint64) error

	// AddVector 新增向量
	AddVector(ctx context.Context, req *pb.AddVectorReq) (*pb.AddVectorRsp, error)
	// DeleteIndex 删除检索库
	DeleteIndex(ctx context.Context, req *pb.DeleteIndexReq) (*pb.DeleteIndexRsp, error)
	// StartEmbeddingUpgradeApp 开始为应用升级 embedding
	StartEmbeddingUpgradeApp(ctx context.Context, id uint64, fromEmbVer uint64, toEmbVer uint64) error
	// FinishEmbeddingUpgradeApp 应用升级 embedding 结束
	FinishEmbeddingUpgradeApp(ctx context.Context, id uint64, fromEmbVer uint64, toEmbVer uint64) error
	// GetAttributes 获取标签
	// 返回数组索引对应入参 labelAbles 索引
	GetAttributes(ctx context.Context, appID uint64, labelAbles []model.LabelAble) ([]model.Attributes, error)
	// CreateAllVectorIndex 创建相似库、评测库
	CreateAllVectorIndex(ctx context.Context, robotID uint64, embeddingVersion uint64, embeddingModelName string) error
	// DeleteVectorIndex 删除指定的库
	DeleteVectorIndex(ctx context.Context, robotID uint64, botBizID uint64, embeddingVersion uint64,
		embeddingModelName string, indexIds []uint64) error
	// ReCreateVectorIndex 重建相似库、评测库
	ReCreateVectorIndex(
		ctx context.Context, appID uint64, indexType uint64, embeddingVersion uint64, botBizID uint64,
		wait time.Duration, embeddingModelName string,
	) error
	// RedisCli redis 客户端
	RedisCli() redis.Client
	// RedisCli redis 客户端
	GlobalRedisCli(ctx context.Context) (redisV8.UniversalClient, error)
	// GetIntent 获取意图
	GetIntent(ctx context.Context, policyID uint32, name string) (*model.Intent, error)
	// ListIntent 获取意图列表
	ListIntent(ctx context.Context, req *model.ListIntentReq) ([]*model.Intent, uint32, error)
	// CreateIntent 创建意图
	CreateIntent(ctx context.Context, intent *model.Intent) error
	// UpdateIntent 更新意图
	UpdateIntent(ctx context.Context, intent *model.Intent) error
	// BatchUpdateIntentPolicyID 更新意图
	BatchUpdateIntentPolicyID(ctx context.Context, policyID uint32, ids []uint32, operator string) error
	// BatchUpdateIntent 批量更新意图内容
	BatchUpdateIntent(ctx context.Context, policyID uint32, ids []uint32, operator string, category string) error
	// DeleteIntent 删除意图
	DeleteIntent(ctx context.Context, intent *model.Intent) error
	// GetIntentByID 通过ID获取意图
	GetIntentByID(ctx context.Context, intentID uint64) (*model.Intent, error)
	// GetIntentByPolicyID 获取策略下的意图列表
	GetIntentByPolicyID(ctx context.Context, policyID []uint32) ([]*model.Intent, error)
	// ListIntentPolicy 获取策略列表
	ListIntentPolicy(ctx context.Context, req *model.ListIntentPolicyReq) ([]*model.IntentPolicy, uint32, error)
	// CreateIntentPolicy 创建策略
	CreateIntentPolicy(ctx context.Context, intentPolicy *model.IntentPolicy) (uint64, error)
	// UpdateIntentPolicy 更新策略
	UpdateIntentPolicy(ctx context.Context, intentPolicy *model.IntentPolicy) error
	// DeleteIntentPolicy 删除策略
	DeleteIntentPolicy(ctx context.Context, intentPolicy *model.IntentPolicy) error
	// GetIntentPolicyByID 通过ID获取策略
	GetIntentPolicyByID(ctx context.Context, policyID uint32) (*model.IntentPolicy, error)
	// GetUnusedIntentList 获取未使用的意图列表
	GetUnusedIntentList(ctx context.Context) ([]*model.Intent, error)
	// GetIntentPolicyIDMap 获取未使用的意图列表
	GetIntentPolicyIDMap(ctx context.Context) ([]*model.IntentPolicy, error)
	// GetUsePolicyRobotCount 获取使用了对应id策略的机器人数量
	GetUsePolicyRobotCount(ctx context.Context, policyID uint32) (uint32, error)
	// ModifyAppOfOp OP更新应用配置信息
	ModifyAppOfOp(ctx context.Context, appDB *model.AppDB) error
	// GetCustomModelList 获取企业自定义模型信息列表
	GetCustomModelList(ctx context.Context, corpID uint64, appType string) ([]*model.CorpCustomModel, error)
	// CreateCustomModel 创建自定义模型
	CreateCustomModel(ctx context.Context, corpID uint64, customModel *model.CorpCustomModel) error
	// GetCustomModelByModelName 通过模型别名获取企业自定义模型
	GetCustomModelByModelName(ctx context.Context, corpID uint64, modelName string) (*model.CorpCustomModel, error)
	// GetLastConfigVersionID 获取最近一次的配置版本id
	GetLastConfigVersionID(ctx context.Context, appIds []uint64) (map[uint64]uint64, error)

	// BatchGetBigDataESByRobotBigDataID 获取big_data
	BatchGetBigDataESByRobotBigDataID(ctx context.Context, robotID uint64, bitDataIDs []string, knowledgeType pb.KnowledgeType) ([]*pb.BigData, error)
	// AddBigDataElastic 新建或更新BigData数据到ES
	AddBigDataElastic(ctx context.Context, bigData []*pb.BigData, knowledgeType pb.KnowledgeType) error
	// DeleteBigDataElastic 从ES里删除BigData
	DeleteBigDataElastic(ctx context.Context, robotID, docID uint64, knowledgeType pb.KnowledgeType, hardDelete bool) error

	// FetURLContent 网页URL解析
	FetURLContent(ctx context.Context, requestID string, botBizID uint64, url string) (string, string, error)

	// GetDocUpdateFrequency 获取文档更新频率
	GetDocUpdateFrequency(ctx context.Context, requestID string, botBizID, docBizID string) (uint32, error)

	// DirectAddSegmentKnowledge 新增分片知识
	DirectAddSegmentKnowledge(ctx context.Context, seg *model.DocSegmentExtend, embeddingVersion uint64,
		vectorLabels []*pb.VectorLabel, embeddingModelName string) error
	// BatchDirectAddSegmentKnowledge 批量新增分片知识
	BatchDirectAddSegmentKnowledge(ctx context.Context, robotID uint64, segments []*model.DocSegmentExtend,
		embeddingVersion uint64, vectorLabels []*pb.VectorLabel, embeddingModelName string) error

	// BatchDirectDeleteSegmentKnowledge 批量删除分片知识
	BatchDirectDeleteSegmentKnowledge(ctx context.Context, robotID uint64,
		segments []*model.DocSegmentExtend, embeddingVersion uint64, embeddingModelName string) error

	// GetRetrievalConfig 查询检索配置
	GetRetrievalConfig(ctx context.Context, robotID uint64) (model.RetrievalConfig, error)
	// SaveRetrievalConfig 保存检索配置
	SaveRetrievalConfig(ctx context.Context, robotID uint64, retrievalConfig model.RetrievalConfig,
		operator string) error
	// SyncRetrievalConfigFromDB 应用的检索配置从DB同步到redis robotID为空则表示同步所有已配置的应用
	SyncRetrievalConfigFromDB(ctx context.Context, robotIDs []uint64) error

	// ProdEmbeddingUpgrade 线上库embedding升级
	ProdEmbeddingUpgrade(ctx context.Context, robotID uint64, embeddingVersionID uint64) error

	// GetLikeData 点赞点踩数据查询
	GetLikeData(ctx context.Context, req *model.MsgDataCountReq) (*model.LikeDataCount, error)

	// GetAnswerTypeData 回答类型数据查询
	GetAnswerTypeData(ctx context.Context, req *model.MsgDataCountReq) (*model.AnswerTypeDataCount, error)

	// LikeDataCount 点赞点踩数据统计
	LikeDataCount(ctx context.Context) ([]*model.LikeDataCount, error)

	// AnswerTypeDataCount 回答类型数据统计
	AnswerTypeDataCount(ctx context.Context) ([]*model.AnswerTypeDataCount, error)

	// UpdateLikeDataCount 点赞点踩数据统计入库
	UpdateLikeDataCount(ctx context.Context, req []*model.LikeDataCount) error

	// StreamGetOneDocSummary 流式获取摘要
	StreamGetOneDocSummary(ctx context.Context, request *knowledge.GetDocSummaryReq, finalString string, docID uint64,
		finalName string, summaryCh chan *knowledge.GetDocSummaryRsp) (summary *knowledge.GetDocSummaryRsp, err error)

	// UpdateAnswerTypeDataCount 回答类型数据统计入库
	UpdateAnswerTypeDataCount(ctx context.Context, req []*model.AnswerTypeDataCount) error

	// CreateResourceExpireTask 创建资源包过期后处理任务
	CreateResourceExpireTask(ctx context.Context, params model.ResExpireParams) error

	// CreateDocResumeTask 创建文档恢复处理任务
	CreateDocResumeTask(ctx context.Context, corpID uint64, robotID uint64, stuffID uint64,
		docExceededTimes []model.DocExceededTime) error

	// CreateQAResumeTask 创建问答恢复处理任务
	CreateQAResumeTask(ctx context.Context, corpID uint64, robotID uint64, stuffID uint64,
		qaExceededTimes []model.QAExceededTime) error
	// BatchDeleteAllKnowledgeProd 批量删除发布库的所有知识（包括QA/文档/混合检索/text2sql等）
	BatchDeleteAllKnowledgeProd(ctx context.Context,
		req *pb.BatchDeleteAllKnowledgeProdReq) (*pb.BatchDeleteAllKnowledgeProdRsp, error)
	// RecoverBigDataElastic 从ES恢复离线知识库的BigData
	RecoverBigDataElastic(ctx context.Context, req *pb.RecoverBigDataElasticReq) (*pb.RecoverBigDataElasticRsp, error)
	// BatchDeleteKnowledge  批量删除知识 -- 替换DeleteKnowledge接口
	BatchDeleteKnowledge(ctx context.Context, req *pb.BatchDeleteKnowledgeReq) (*pb.BatchDeleteKnowledgeRsp, error)

	ConvertErrMsg(ctx context.Context, sID int, corpID uint64, oldErr error) error
	SendNoticeIfDocAppealPass(ctx context.Context, tx *sqlx.Tx, doc *model.Doc, audit *model.Audit) error
	DocParseSegment(ctx context.Context, tx *sqlx.Tx, doc *model.Doc, intervene bool) error
	FailCharSizeNotice(ctx context.Context, doc *model.Doc) error

	// RealtimeInterface 实时文档相关接口
	RealtimeInterface

	// Text2sqlInterface text2sql相关接口
	Text2sqlInterface

	// ILLM 大模型相关接口
	ILLM

	// Billing 计费相关接口
	Billing

	// KnowledgeDelete 知识删除相关接口
	KnowledgeDelete

	// KnowledgeShareInterface 共享知识库接口
	KnowledgeShareInterface

	// COSDocumentInterface COS文档接口
	COSDocumentInterface
}

// StorageWithTypeKeyInterface 指定类型存储相关接口
type StorageWithTypeKeyInterface interface {
	// GetCredentialWithTypeKey 获取cos临时密钥
	GetCredentialWithTypeKey(ctx context.Context, typeKey string, pathList []string, storageAction string) (
		*sts.CredentialResult, error)
	// GetPresignedURLWithTypeKey 获取Cos预签名URL
	GetPresignedURLWithTypeKey(ctx context.Context, typeKey string, key string) (string, error)
	// GetObjectWithTypeKey 获取 COS 文件
	GetObjectWithTypeKey(ctx context.Context, typeKey string, key string) ([]byte, error)
	// PutObjectWithTypeKey 上传 COS 文件
	PutObjectWithTypeKey(ctx context.Context, typeKey string, bs []byte, key string) error
	// DelObjectWithTypeKey 删除 COS 文件
	DelObjectWithTypeKey(ctx context.Context, typeKey string, key string) error
	// StatObjectWithTypeKey 获取object的元数据信息
	StatObjectWithTypeKey(ctx context.Context, typeKey string, key string) (*model.ObjectInfo, error)
	// GetObjectETagWithTypeKey 获取存储对象的ETag
	GetObjectETagWithTypeKey(ctx context.Context, typeKey string, url string) (string, error)
	// GetDomainWithTypeKey 获取对象存储domain
	GetDomainWithTypeKey(ctx context.Context, typeKey string) (string, error)
	// GetStorageTypeWithTypeKey 获取对象存储类型
	GetStorageTypeWithTypeKey(ctx context.Context, typeKey string) (string, error)
	// GetBucketWithTypeKey 获取存储桶
	GetBucketWithTypeKey(ctx context.Context, typeKey string) (string, error)
	// GetRegionWithTypeKey 获取存储桶地域
	GetRegionWithTypeKey(ctx context.Context, typeKey string) (string, error)
	// GetTypeKeyWithBucket 通过COS桶名称获取typeKey
	GetTypeKeyWithBucket(ctx context.Context, bucket string) string
}

// RealtimeInterface 实时文档
type RealtimeInterface interface {
	// CheckRealtimeStorageInfo 校验实时文档存储信息
	CheckRealtimeStorageInfo(ctx context.Context, bucket, url, eTag string, app *model.App) error

	// GetRealtimeDocByID 根据DocID查询实时文档
	GetRealtimeDocByID(ctx context.Context, docID uint64) (*realtime.TRealtimeDoc, error)

	// ParseRealtimeDoc 实时文档解析
	ParseRealtimeDoc(ctx context.Context,
		reqCh <-chan *realtime.ParseDocReqChan, rspCh chan<- *realtime.ParseDocRspChan) error

	// SearchRealtimeKnowledge 实时文档检索
	SearchRealtimeKnowledge(ctx context.Context, req *pb.RetrievalRealTimeReq) (*pb.RetrievalRealTimeRsp, error)

	// GetLinkContentsFromRealtimeSearchVectorResponse 从检索请求构造 linkContents
	GetLinkContentsFromRealtimeSearchVectorResponse(
		ctx context.Context, docs []*pb.RetrievalRealTimeRsp_Doc,
		segmentFn func(doc *pb.RetrievalRealTimeRsp_Doc, segment *realtime.TRealtimeDocSegment) any,
		searchEngineFn func(doc *pb.RetrievalRealTimeRsp_Doc) any,
	) ([]linker.Content, error)

	// CreateRealtimeDoc 插入或者找到第一条符合条件的实时文档
	CreateRealtimeDoc(ctx context.Context, realtimeDoc *realtime.TRealtimeDoc) (*realtime.TRealtimeDoc, error)

	// GetCountByDocID TODO
	GetCountByDocID(ctx context.Context, docID uint64) (int, error)
	// GetOrgDataListByDocID TODO
	GetOrgDataListByDocID(ctx context.Context, docID, offset, limit uint64) ([]string, error)

	// DeletedRealtimeDocInfo 删除实时文档相关信息【t_realtime_doc,t_realtime_doc_segment,vector】
	DeletedRealtimeDocInfo(ctx context.Context, botBizID uint64, sessionID string, docIds []uint64) error

	// GetRealTimeSegmentChunk 分片查询实时文档切片 用于embedding升级时拉取
	GetRealTimeSegmentChunk(ctx context.Context,
		corpID, robotID, offset, limit uint64) ([]*realtime.TRealtimeDocSegment,
		error)
}

// Text2sqlInterface text2sql相关接口
type Text2sqlInterface interface {

	// GetText2SqlSegmentMeta 通过DocID获取Text2Sql的meta数据；
	GetText2SqlSegmentMeta(ctx context.Context, docID uint64, robotID uint64) ([]*model.DocSegmentExtend, error)

	// AddText2SQL 增加或修改text2sql
	AddText2SQL(ctx context.Context, robotID, docID uint64, expireTime int64, meta *pb.Text2SQLMeta,
		rows []*pb.Text2SQLRowData,
		vectorLabels []*pb.VectorLabel, fileName string, corpID uint64, disableEs bool) error

	// DeleteText2SQL 删除text2sql
	DeleteText2SQL(ctx context.Context, robotID, docID uint64) error
}

// ILLM TODO
type ILLM interface {
	// SimpleChat 模型
	SimpleChat(ctx context.Context, serviceName string, req *llmm.Request) (*llmm.Response, error)
	// Chat 流式调用 LLM
	Chat(ctx context.Context, req *llmm.Request, ch chan *llmm.Response) error
	// GetModelPromptLimit 获取模型Prompt长度限制
	GetModelPromptLimit(ctx context.Context, corpID uint64, modelName string) int
	// GetModelInfo 获取模型信息
	GetModelInfo(ctx context.Context, modelName string) (*llmm.GetModelResponse, error)
}

// Billing 计费相关接口
type Billing interface {
	// GetCorpBillingInfo 获取企业的计费信息
	GetCorpBillingInfo(ctx context.Context, corp *model.Corp) (*model.Corp, error)
}

// KnowledgeDelete 知识删除
type KnowledgeDelete interface {
	// GetAppListByBizIDs 获取应用列表
	GetAppListByBizIDs(ctx context.Context, scenes uint32, robotIDs []uint64) (
		map[uint64]*admin.GetAppListRsp_AppInfo, error)

	// CreateKnowledgeDeleteTask 创建知识删除任务
	CreateKnowledgeDeleteTask(ctx context.Context, params model.KnowledgeDeleteParams) error

	// CountTableNeedDeletedData 统计表需要删除数据的数量
	CountTableNeedDeletedData(ctx context.Context, corpID, robotID uint64,
		tableName string) (int64, error)

	// CountTableNeedDeletedDataBizID 统计表需要删除数据的数量
	CountTableNeedDeletedDataBizID(ctx context.Context, corpBizID, robotBizID uint64,
		tableName string) (int64, error)

	// CountTableNeedDeletedDataByCorpAndAppBizID 统计表需要删除数据的数量
	CountTableNeedDeletedDataByCorpAndAppBizID(ctx context.Context, corpBizID, robotBizID uint64,
		tableName string) (int64, error)

	// DeleteTableNeedDeletedData 删除表需要删除的数据
	DeleteTableNeedDeletedData(ctx context.Context, corpID, robotID uint64,
		tableName string, totalCount int64) error

	// DeleteTableNeedDeletedDataBizID 删除表需要删除的数据
	DeleteTableNeedDeletedDataBizID(ctx context.Context, corpID, robotID uint64,
		tableName string, totalCount int64) error

	// DeleteTableNeedDeletedDataByCorpAndAppBizID 删除表需要删除的数据
	DeleteTableNeedDeletedDataByCorpAndAppBizID(ctx context.Context, corpID, robotID uint64,
		tableName string, totalCount int64) error

	// GetCustomFieldIDList 获取指定表自定义主键ID列表
	GetCustomFieldIDList(ctx context.Context, corpID, robotID uint64,
		tableName, customField string) ([]uint64, error)
	// DeleteByCustomFieldID 删除指定表自定义字段列表
	DeleteByCustomFieldID(ctx context.Context, tableName string, limit int64,
		customFields []string, customConditions []string, customFieldValues []interface{}) (int64, error)

	// KnowledgeDeleteResultCallback 知识删除任务结果回调
	KnowledgeDeleteResultCallback(ctx context.Context, taskID uint64, isSuccess bool, message string) error
}

type COSDocumentInterface interface {
	// AssumeServiceRole 扮演服务角色
	AssumeServiceRole(ctx context.Context, roleUIN, roleName string, duration uint64, policy *string) (
		*cloudsts.AssumeRoleResponseParams, knowledge.RoleStatusType, error)
	// ListBucketByCredential 通过凭证获取存储桶列表
	ListBucketByCredential(ctx context.Context, credential *cloudsts.Credentials) (
		*cos.ServiceGetResult, error)
	// AddBucketCORSRule 添加CORS规则
	AddBucketCORSRule(ctx context.Context, credential *cloudsts.Credentials, bucket, region, origin string) (
		*cos.BucketCORSRule, bool, error)
	// GetCOSObject 通过凭证获取存储桶COS对象
	GetCOSObject(ctx context.Context, credential *cloudsts.Credentials, bucket, region, fileKey string) (
		[]byte, error)
}

type KnowledgeShareInterface interface {
	// CreateSharedKnowledge 创建共享知识库
	CreateSharedKnowledge(ctx context.Context, corpBizID, knowledgeBizID uint64, userInfo *knowledge.UserBaseInfo,
		createInfo *knowledge.CreateSharedKnowledgeReq) (uint64, error)
	// UpdateSharedKnowledge 更新共享知识库
	UpdateSharedKnowledge(ctx context.Context, corpBizID, knowledgeBizID uint64, userInfo *knowledge.UserBaseInfo,
		updateInfo *knowledge.KnowledgeUpdateInfo) (int64, error)
	// RetrieveBaseSharedKnowledge 检索共享知识库基础信息
	RetrieveBaseSharedKnowledge(ctx context.Context, corpBizID uint64, knowledgeBizIDList []uint64) (
		[]*model.SharedKnowledgeInfo, error)
	// ListBaseSharedKnowledge 列举共享知识库清单
	ListBaseSharedKnowledge(ctx context.Context, corpBizID uint64, knowledgeBizIDList []uint64,
		pageNumber, pageSize uint32, keyword string, spaceID string) (
		[]*model.SharedKnowledgeInfo, error)
	// RetrieveSharedKnowledgeCount 统计共享知识库数量
	RetrieveSharedKnowledgeCount(ctx context.Context, corpBizID uint64, knowledgeBizIDList []uint64,
		keyword string, spaceID string) (int64, error)
	// DeleteSharedKnowledge 删除共享知识库
	DeleteSharedKnowledge(ctx context.Context, corpBizID uint64, knowledgeBizIDList []uint64) (int64, error)
	// RetrieveSharedKnowledgeByName 按名称检索知识库
	RetrieveSharedKnowledgeByName(ctx context.Context, corpBizID uint64, knowledgeNameList []string, spaceID string) (
		[]*model.SharedKnowledgeInfo, error)
	// ClearSpaceSharedKnowledge 清除空间下的所有共享知识库
	ClearSpaceSharedKnowledge(ctx context.Context, corpBizID uint64, spaceID string) (int64, error)
	// ListSpaceShareKnowledgeExSelf 获取空间下的共享知识库，排除owner为staffID的
	ListSpaceShareKnowledgeExSelf(ctx context.Context,
		corpBizID, staffID uint64,
		spaceID, keyword string,
		pageNumber, pageSize uint32) (int64, []*model.SharedKnowledgeInfo, error)
	// GetShareKnowledgeBaseByIDRange 获取共享知识库ID范围内的知识库
	GetShareKnowledgeBaseByIDRange(ctx context.Context, startID, endID uint64, limit int) ([]*model.SharedKnowledgeInfo, error)
	// UpdateShareKnowledgeBaseOwnerStaffID 更新共享知识库的所有者
	UpdateShareKnowledgeBaseOwnerStaffID(ctx context.Context, kb *model.SharedKnowledgeInfo) error
}

type dao struct {
	// 应用级数据库路由
	dbMap                map[uint64]mysql.Client
	gormMap              map[uint64]*gorm.DB
	db                   mysql.Client
	dbRead               mysql.Client
	tdsqlGorm            *gorm.DB
	tdsqlRead            mysql.Client
	tdsqlGormDelete      *gorm.DB
	gormDB               *gorm.DB
	gormDBDelete         *gorm.DB
	text2sqlGorm         *gorm.DB
	redis                redis.Client
	retrievalConfigRedis redis.Client
	rand                 *util.Rand
	uniIDNode            *snowflake.Node
	directIndexCli       pb.DirectIndexClientProxy
	infosecCli           infosec.InfosecClientProxy
	shortURLCli          shortURL.AdminClientProxy
	retrievalCli         pb.RetrievalClientProxy
	vector               *vector.SyncVector
	llmmCli              llmm.ChatClientProxy
	storageCli           storage.Storage
	accessCli            accessManage.AccessManagerClientProxy
	docParseCli          fileManagerServer.ManagerObjClientProxy
	taskFlowCli          taskFlow.TaskConfigClientProxy
	aiConfCli            http.Client
	chatCli              chat.ChatClientProxy
	webParserCli         webParserServer.ParserObjClientProxy
	adminApiCli          admin.ApiClientProxy
	nrtFileManagerCli    nrt_file_manager_server.ManagerObjClientProxy
	qBotFinanceClient    finance.FinanceClientProxy
	nerSandBoxCli        entityextractor.EntityExtractorObjClientProxy
	nerProdCli           entityextractor.EntityExtractorObjClientProxy
	tDocLinkerCli        http.Client
	promptCli            prompt.PromptProcessor
}

var snowFlakeNode *snowflake.Node

func init() {
	// "github.com/bwmarrin/snowflake" total 22 bits to share between NodeBits/StepBits
	// 机器码：使用IP二进制的后16位
	snowflake.NodeBits = 16
	// 序列号：6位，每毫秒最多生成64个ID [0,64)
	snowflake.StepBits = 6

	nodeNum := int64(0)

	// 取IP二进制的后16位
	ip := net.ParseIP(getClientIP()).To4()
	if ip != nil && len(ip) == 4 {
		nodeNum = (int64(ip[2]) << 8) + int64(ip[3])
	}

	node, err := snowflake.NewNode(nodeNum)
	log.Infof("GenerateSeqID ip:%s nodeNum:%d NodeBits:%d StepBits:%d",
		ip, nodeNum, snowflake.NodeBits, snowflake.StepBits)
	if err != nil {
		log.Fatalf("GenerateSeqID ip:%s nodeNum:%d NodeBits:%d StepBits:%d err:%+v",
			ip, nodeNum, snowflake.NodeBits, snowflake.StepBits, err)
	}
	snowFlakeNode = node
}

func (d *dao) GetDB() mysql.Client {
	return d.db
}

func (d *dao) GetTdsqlGormDB() *gorm.DB {
	return d.tdsqlGorm
}

func (d *dao) GetTdsqlGormDBDelete() *gorm.DB {
	return d.tdsqlGormDelete
}

func (d *dao) GetGormDB() *gorm.DB {
	return d.gormDB
}
func (d *dao) GetText2sqlGormDB() *gorm.DB {
	return d.text2sqlGorm
}

func (d *dao) GetGormDBDelete() *gorm.DB {
	return d.gormDBDelete
}

func (d *dao) GetAdminApiCli() admin.ApiClientProxy {
	return d.adminApiCli
}

func (d *dao) GetDirectIndexCli() pb.DirectIndexClientProxy {
	return d.directIndexCli
}

// GetRetrievalCli 获取检索句柄
func (d *dao) GetRetrievalCli() pb.RetrievalClientProxy {
	return d.retrievalCli
}

func (d *dao) GetTaskFlowCli() taskFlow.TaskConfigClientProxy {
	return d.taskFlowCli
}

func (d *dao) GetTDocLinkerCli() http.Client {
	return d.tDocLinkerCli
}

func (d *dao) GetDocParseCli() fileManagerServer.ManagerObjClientProxy {
	return d.docParseCli
}

func (d *dao) GetFinanceCli() finance.FinanceClientProxy {
	return d.qBotFinanceClient
}

func (d *dao) GetLlmmCli() llmm.ChatClientProxy {
	return d.llmmCli
}

func (d *dao) GetPromptCli() prompt.PromptProcessor {
	return d.promptCli
}

// New creates Dao instance
func New() Dao {
	db := mysql.NewClientProxy("mysql.qbot.admin")
	dbRead := mysql.NewClientProxy("mysql.qbot.admin.read")
	tdsqlGorm, err := tgorm.NewClientProxy("tdsql.qbot.qbot")
	if err != nil {
		panic(err)
	}
	tdsqlRead := mysql.NewClientProxy("tdsql.qbot.qbot.read")
	tdsqlGormDelete, err := tgorm.NewClientProxy("tdsql.qbot.qbot.delete")
	if err != nil {
		panic(err)
	}
	gormDB, err := tgorm.NewClientProxy("mysql.qbot.admin")
	if err != nil {
		panic(err)
	}
	gormDBDelete, err := tgorm.NewClientProxy("mysql.qbot.admin.delete")
	if err != nil {
		panic(err)
	}

	text2sqlGromDB, err := tgorm.NewClientProxy("mysql.db_text2sql")
	if err != nil {
		panic(err)
	}
	promptCli, err := prompt.NewBasicPromptProcessor(gormDB)
	if err != nil {
		log.Errorf("init promptCli err:%v", err)
		panic(err)
	}
	d := &dao{
		db:                   db,
		text2sqlGorm:         text2sqlGromDB,
		dbRead:               dbRead,
		tdsqlGorm:            tdsqlGorm.Debug(),
		tdsqlRead:            tdsqlRead,
		tdsqlGormDelete:      tdsqlGormDelete.Debug(),
		gormDB:               gormDB.Debug(),
		gormDBDelete:         gormDBDelete.Debug(),
		redis:                redis.NewClientProxy("redis.qbot.admin"),
		retrievalConfigRedis: redis.NewClientProxy("redis.qbot.retrieval.config"),
		rand:                 util.New(),
		uniIDNode:            snowFlakeNode,
		directIndexCli:       pb.NewDirectIndexClientProxy(),
		retrievalCli:         pb.NewRetrievalClientProxy(),
		infosecCli:           infosec.NewInfosecClientProxy(),
		shortURLCli:          shortURL.NewAdminClientProxy(),
		vector:               vector.NewVectorSync(db, tdsqlGorm),
		llmmCli:              llmm.NewChatClientProxy(),
		storageCli:           storage.New(),
		accessCli:            accessManage.NewAccessManagerClientProxy(),
		docParseCli:          fileManagerServer.NewManagerObjClientProxy(),
		taskFlowCli:          taskFlow.NewTaskConfigClientProxy(),
		aiConfCli:            http.NewClientProxy("trpc.SmartService.AIConfManagerServer.AIConfManagerHttp"),
		chatCli:              chat.NewChatClientProxy(),
		webParserCli:         webParserServer.NewParserObjClientProxy(),
		adminApiCli:          admin.NewApiClientProxy(),
		nrtFileManagerCli:    nrt_file_manager_server.NewManagerObjClientProxy(),
		qBotFinanceClient: finance.NewFinanceClientProxy(
			[]client.Option{
				client.WithServiceName("qbot.finance.finance.Finance"),
			}...),
		nerSandBoxCli: entityextractor.NewEntityExtractorObjClientProxy(
			[]client.Option{
				client.WithServiceName("trpc.SmartAssistant.EntityExtractorServerSandBox.EntityExtractorObj"),
			}...),
		nerProdCli: entityextractor.NewEntityExtractorObjClientProxy(
			[]client.Option{
				client.WithServiceName("trpc.SmartAssistant.EntityExtractorServer.EntityExtractorObj"),
			}...),
		tDocLinkerCli: http.NewClientProxy("lke-code-node-tdoclinker",
			[]client.Option{
				client.WithServiceName("lke-code-node-tdoclinker"),
			}...),
		promptCli: promptCli,
	}
	d.vector.SetGetBotBizIDByIDFunc(d.GetBotBizIDByID)
	return d
}

// getClientIP 获取本机IP
func getClientIP() string {
	ip := trpc.GetIP("eth1")
	if len(ip) > 0 {
		return ip
	}
	if addresses, err := net.InterfaceAddrs(); err == nil {
		for _, addr := range addresses {
			if ipNet, ok := addr.(*net.IPNet); ok {
				if !ipNet.IP.IsLoopback() && ipNet.IP.To4() != nil {
					ip = ipNet.IP.To4().String()
					// only ignore 127.0.0.1, return ip
					return ip
				}
			}
		}
	}
	panic("getClientIP failed")
}

// GenerateSeqID 生成唯一ID
func (d *dao) GenerateSeqID() uint64 {
	return uint64(d.uniIDNode.Generate())
}

func placeholder(c int) string {
	if c <= 0 {
		log.Errorf("invalid placeholder count: %d", c)
		return ""
	}
	return "?" + strings.Repeat(", ?", c-1)
}

func getEnvSet(ctx context.Context) string {
	return metadata.Metadata(ctx).EnvSet()
}

// RedisCli 获取 redis 客户端
func (d *dao) RedisCli() redis.Client {
	return d.redis
}

func (d *dao) GlobalRedisCli(ctx context.Context) (redisV8.UniversalClient, error) {
	return mredis.GetGoRedisClient(ctx)
}
