package dao

import (
	"context"
	"errors"
	"fmt"
	"time"

	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	"github.com/redis/go-redis/v9"
)

const (
	// LockForSaveDoc 保存文档锁
	LockForSaveDoc = "qbot:admin:lock:save_doc:%s:%s"
	// LockForAuditCheck 审核回调
	LockForAuditCheck = "qbot:admin:lock:audit_check:%d"
	// LockForAddUnsatisfiedReply 添加不满意问题锁
	LockForAddUnsatisfiedReply = "qbot:admin:lock:add_unsatisfied_reply:%s"
	// LockForUplodAttributeLabel 上传属性标签文件锁
	LockForUplodAttributeLabel = "qbot:admin:lock:upload_attribute_label_files:%s"
	// LockForUplodSynonymsList 上传同义词文件锁
	LockForUplodSynonymsList = "qbot:admin:lock:upload_synonyms_list_files:%s"
	// LockForAuditAppeal 申诉单审核锁定
	LockForAuditAppeal = "qbot:admin:lock:audit_appeal:%d"
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
	value := time.Now().Format("2006-01-02 15:04:05")
	err := d.adminRdb.Set(ctx, key, value, duration).Err()
	if err != nil {
		// ErrNil 为 key 已存在导致, 为正常错误, 只有非 ErrNil 的错误才需要记录
		if errors.Is(err, redis.Nil) {
			return errs.ErrAlreadyLocked
		}

		err = fmt.Errorf("redis加锁失败, key: %s, err: %w", key, err)

		return err
	}

	return nil
}

// UnLock 解锁
func (d *dao) UnLock(ctx context.Context, key string) error {
	err := d.adminRdb.Del(ctx, key).Err()
	if err != nil {
		err = fmt.Errorf("redis解锁失败, key: %s, err: %w", key, err)
		logx.W(ctx, err.Error())
		return err
	}

	return nil
}
