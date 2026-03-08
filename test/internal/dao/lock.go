package dao

import (
	"context"
	"errors"
	"fmt"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"math"
	"time"

	"git.code.oa.com/trpc-go/trpc-database/redis"
	"git.code.oa.com/trpc-go/trpc-go/log"
)

const (
	// LockForSaveDoc 保存文档锁
	LockForSaveDoc = "qbot:admin:lock:save_doc:%s"
	// LockForCreateCorp 创建企业锁
	LockForCreateCorp = "qbot:admin:lock:create_corp:%s"
	// LockForJoinCorp 加入企业锁
	LockForJoinCorp = "qbot:admin:lock:join_corp:%s"
	// LockForCreateRobot 创建机器人
	LockForCreateRobot = "qbot:admin:lock:create_robot:%d"
	// LockForAuditCheck 审核回调
	LockForAuditCheck = "qbot:admin:lock:audit_check:%d"
	// LockForAuditCorp 企业审核锁
	LockForAuditCorp = "qbot:admin:lock:audit_corp:%d"
	// LockForCreateRelease 发布锁
	LockForCreateRelease = "qbot:admin:lock:create_release:%d"
	// LockForAddUnsatisfiedReply 添加不满意问题锁
	LockForAddUnsatisfiedReply = "qbot:admin:lock:add_unsatisfied_reply:%s"
	// LockForUploadSampleFiles 保存样本文件锁
	LockForUploadSampleFiles = "qbot:admin:lock:upload_sample_files:%s"
	// LockForCreateTest 创建评测任务锁锁
	LockForCreateTest = "qbot:admin:lock:create_test:%s"
	// LockForOperateTest 操作评测任务锁锁,删除，停止，重试等修改操作共享
	LockForOperateTest = "qbot:admin:lock:operate_test:%d"
	// LockForUplodAttributeLabel 上传属性标签文件锁
	LockForUplodAttributeLabel = "qbot:admin:lock:upload_attribute_label_files:%s"
	// LockForUplodSynonymsList 上传同义词文件锁
	LockForUplodSynonymsList = "qbot:admin:lock:upload_synonyms_list_files:%s"
	// LockForCreateAppeal 创建申诉单锁定
	LockForCreateAppeal = "qbot:admin:lock:create_appeal:%s"
	// LockForAuditAppeal 申诉单审核锁定
	LockForAuditAppeal = "qbot:admin:lock:audit_appeal:%d"
	// LockForActiveProduct 产品开通
	LockForActiveProduct = "qbot:admin:lock:active_product:%s"
	// LockForCreateApp 创建应用
	LockForCreateApp = "qbot:admin:lock:create_app:%d"
	// LockForTrialProduct 产品试用开通
	LockForTrialProduct = "qbot:admin:lock:trial_product:%s"
	// LockTMsgDataCount TMsg数据统计
	LockTMsgDataCount = "knowledge_config:lock:msg_count:%s"
	// LockCleanVectorSyncHistory 清理t_vector_sync_history数据用
	LockCleanVectorSyncHistory = "knowledge_config:lock:clean_vector_sync_history:%s"
	// UpdateAttributeLabelsTaskPreview 评测环境更新属性标签任务
	UpdateAttributeLabelsTaskPreview = "knowledge_config:attr_label_task_preview"
	// UpdateAttributeLabelsTaskProd 发布环境更新属性标签任务
	UpdateAttributeLabelsTaskProd = "knowledge_config:attr_label_task_prod"
	// LockHandleDocDiffTask 处理对比任务锁
	LockHandleDocDiffTask = "knowledge_config:handle_doc_diff_task:%s"
	// LockForModifyOrDeleteDoc 更新或者删除文档的锁
	LockForModifyOrDeleteDoc = "knowledge_config:lock:modify_or_delete_doc:%d"
	// LockForModifyOrDeleteQa 更新或者删除文档问答的锁
	LockForModifyOrDeleteQa = "knowledge_config:lock:modify_or_delete_qa:%d"
	// LockForCreateDocParsingIntervention 提交切片干预任务锁
	LockForCreateDocParsingIntervention = "knowledge_config:lock:create_doc_parsing_intervention:%s"
	// LockKnowledgeSchemaCache 更新知识库schema缓存锁
	LockKnowledgeSchemaCache = "knowledge_config::lock:set_schema:%d_%s"

	// LockAutoDocRefresh 刷新文档任务锁
	LockAutoDocRefresh = "knowledge_config:lock:auto_doc_refresh:%s"

	// LockSaveCorpCOSDoc 存储企业cos文档
	LockSaveCorpCOSDoc = "knowledge_config:lock:save_corp_cos_doc:%d_%s"
)

// Lock 加锁
func (d *dao) Lock(ctx context.Context, key string, duration time.Duration) error {
	t := math.Ceil(duration.Seconds())
	expire := 1
	if t > 1.0 {
		expire = int(t)
	}
	value := time.Now().Format("2006-01-02 15:04:05")
	_, err := redis.String(d.redis.Do(
		ctx, "SET", key, value, "EX", expire, "NX",
	))

	if err != nil {
		// ErrNil 为 key 已存在导致, 为正常错误, 只有非 ErrNil 的错误才需要记录
		if errors.Is(err, redis.ErrNil) {
			return errs.ErrAlreadyLocked
		}

		err = fmt.Errorf("redis加锁失败, key: %s, err: %w", key, err)

		return err
	}

	return nil
}

// UnLock 解锁
func (d *dao) UnLock(ctx context.Context, key string) error {
	_, err := d.redis.Do(ctx, "DEL", key)

	if err != nil {
		err = fmt.Errorf("redis解锁失败, key: %s, err: %w", key, err)
		log.ErrorContext(ctx, err)

		return err
	}

	return nil
}
