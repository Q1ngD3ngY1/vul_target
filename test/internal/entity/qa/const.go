package qa

import (
	"git.woa.com/adp/kb/kb-config/pkg/i18nkey"
)

const (
	// AcceptInit 未校验
	AcceptInit = uint32(1)
	// AcceptYes 采纳
	AcceptYes = uint32(2)
	// AcceptNo 不采纳
	AcceptNo = uint32(3)

	// QAIsNotDeleted 未删除
	QAIsNotDeleted = uint32(1)
	// QAIsDeleted 已删除
	QAIsDeleted = uint32(2)

	// QA状态：优先展示校验状态，如果已校验再展示发布状态。t_doc_qa和t_release_qa共用的状态字段

	// QAReleaseStatusInit 未发布
	QAReleaseStatusInit = uint32(2)
	// QAReleaseStatusIng 发布中
	QAReleaseStatusIng = uint32(3)
	// QAReleaseStatusSuccess 已发布
	QAReleaseStatusSuccess = uint32(4)
	// QAReleaseStatusFail 发布失败
	QAReleaseStatusFail = uint32(5)
	// QAReleaseStatusAcceptNo 不采纳
	QAReleaseStatusAcceptNo = uint32(6)
	// QAReleaseStatusAuditing 审核中
	QAReleaseStatusAuditing = uint32(7)
	// QAReleaseStatusAuditNotPass 审核不通过【给页面展示审核失败】
	QAReleaseStatusAuditNotPass = uint32(8)
	// QAReleaseStatusAppealIng 人工审核中
	QAReleaseStatusAppealIng = uint32(9)
	// QAReleaseStatusAppealFail 人工审核不通过
	QAReleaseStatusAppealFail = uint32(11)
	// QAReleaseStatusExpired 已过期
	QAReleaseStatusExpired = uint32(12)
	// QAReleaseStatusCharExceeded 超量失效
	QAReleaseStatusCharExceeded = uint32(13)
	// QAReleaseStatusResuming 超量失效恢复
	QAReleaseStatusResuming = uint32(14)
	// QAReleaseStatusAppealFailCharExceeded 人工审核不通过-超量失效
	QAReleaseStatusAppealFailCharExceeded = uint32(15)
	// QAReleaseStatusAppealFailResuming 人工审核不通过-超量失效恢复
	QAReleaseStatusAppealFailResuming = uint32(16)
	// QAReleaseStatusAuditNotPassCharExceeded 审核失败-超量失效
	QAReleaseStatusAuditNotPassCharExceeded = uint32(17)
	// QAReleaseStatusAuditNotPassResuming 审核失败-超量失效恢复
	QAReleaseStatusAuditNotPassResuming = uint32(18)
	// QAReleaseStatusLearning 学习中
	QAReleaseStatusLearning = uint32(19)

	// QAReleaseStatusLearnFail 学习失败
	QAReleaseStatusLearnFail = uint32(20)
	// QAReleaseStatusLearnFailCharExceeded 学习失败-超量失效
	QAReleaseStatusLearnFailCharExceeded = uint32(21)
	// QAReleaseStatusLearnFailResuming 学习失败-超量失效恢复
	QAReleaseStatusLearnFailResuming = uint32(22)

	// QAIsAuditFree qa 问答免审
	QAIsAuditFree = true
	// QAIsAuditNotFree qa 问答不免审
	QAIsAuditNotFree = false

	// ExportQANoticeContent 导出QA通知内容
	ExportQANoticeContent = i18nkey.KeyQaLibraryBatchExportStatus
	// ExportQANoticeContentIng 导出QA通知中通知
	ExportQANoticeContentIng = i18nkey.KeyQaLibraryBatchExporting
	// QABusinessSourceDefault QA数据业务来源，默认问答模块
	QABusinessSourceDefault = uint32(0)
	// QABusinessSourceUnsatisfiedReply QA数据业务来源，来自不满意回复
	QABusinessSourceUnsatisfiedReply = uint32(1)

	// QaUnExpiredStatus 未过期
	QaUnExpiredStatus = uint32(2)
	// QaExpiredStatus 已过期
	QaExpiredStatus = uint32(3)

	// QaVideoFile 问答对视频文件类型
	QaVideoFile = 1

	// NextActionAdd 新增
	NextActionAdd = uint32(1)
	// NextActionUpdate 更新
	NextActionUpdate = uint32(2)
	// NextActionDelete 删除
	NextActionDelete = uint32(3)
	// NextActionPublish 发布
	NextActionPublish = uint32(4)
)

// QAStableStatus 问答稳定状态，即不会再自动流转到其他状态
var QAStableStatus = []uint32{
	QAReleaseStatusInit,
	QAReleaseStatusSuccess,
	QAReleaseStatusFail,
	QAReleaseStatusAcceptNo,
	QAReleaseStatusAuditNotPass,
	QAReleaseStatusAppealFail,
	QAReleaseStatusExpired,
	QAReleaseStatusCharExceeded,
	QAReleaseStatusAppealFailCharExceeded,
	QAReleaseStatusAuditNotPassCharExceeded,
	QAReleaseStatusLearnFail,
	QAReleaseStatusLearnFailCharExceeded,
}

var qaStatusMap = map[uint32]string{
	QAReleaseStatusInit:         i18nkey.KeyImportComplete,
	QAReleaseStatusIng:          i18nkey.KeyReleasing,
	QAReleaseStatusSuccess:      i18nkey.KeyImportComplete,
	QAReleaseStatusFail:         i18nkey.KeyImportFail,
	QAReleaseStatusAuditing:     i18nkey.KeyAuditIng,
	QAReleaseStatusAuditNotPass: i18nkey.KeyAuditFail,
	QAReleaseStatusAppealIng:    i18nkey.KeyUnderAppeal,
	QAReleaseStatusAppealFail:   i18nkey.KeyAppealFailed,
	QAReleaseStatusExpired:      i18nkey.KeyExpired,
	QAReleaseStatusCharExceeded: i18nkey.KeyCharExceeded,
	QAReleaseStatusResuming:     i18nkey.KeyResuming,
	QAReleaseStatusLearning:     i18nkey.KeyCreatingIndex,
	QAReleaseStatusLearnFail:    i18nkey.KeyCreateIndexFail,
}

var QANextActionDesc = map[uint32]string{
	NextActionAdd:     i18nkey.KeyAdd,
	NextActionUpdate:  i18nkey.KeyModify,
	NextActionDelete:  i18nkey.KeyDeleted,
	NextActionPublish: i18nkey.KeyPublish,
}
