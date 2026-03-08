package async

import (
	"archive/zip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"git.woa.com/adp/common/x/encodingx/jsonx"
	"git.woa.com/adp/common/x/gox"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	kbe "git.woa.com/adp/kb/kb-config/internal/entity/kb"
	"git.woa.com/adp/kb/kb-config/internal/entity/kb_package"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	"git.woa.com/adp/kb/kb-config/pkg/i18nkey"
	appconfig "git.woa.com/adp/pb-go/app/app_config"
	"git.woa.com/adp/pb-go/common"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
	"git.woa.com/dialogue-platform/bot-config/task_scheduler"
	"github.com/spf13/cast"
)

// ExportKBPackageTaskHandler 知识库导出任务处理器
type ExportKBPackageTaskHandler struct {
	*taskCommon

	task              task_scheduler.Task
	p                 entity.ExportKbPackageParams
	exportRootPath    string // 导出根路径
	exportZipFilePath string // 导出zip路径

	// 并发安全的导出信息收集
	mu            sync.Mutex
	kbExportInfos []kb_package.KbExportInfo
}

// 1. 收集文件信息
type fileInfo struct {
	path    string
	relPath string
	size    int64
	modTime time.Time
}

// registerExportKBPackageTaskHandler 注册知识库导出任务处理器
func registerExportKBPackageTaskHandler(tc *taskCommon) {
	task_scheduler.Register(
		entity.ExportKbPackageTask,
		func(t task_scheduler.Task, params entity.ExportKbPackageParams) task_scheduler.TaskHandler {
			return &ExportKBPackageTaskHandler{
				taskCommon: tc,
				task:       t,
				p:          params,
			}
		},
	)
}

// Init 初始化任务
func (d *ExportKBPackageTaskHandler) Init(ctx context.Context, _ task_scheduler.TaskKV) error {
	logx.I(ctx, "task(ExportKBPackage) Init, task: %+v", d.task)

	return nil
}

// Prepare 数据准备阶段
func (d *ExportKBPackageTaskHandler) Prepare(ctx context.Context) (task_scheduler.TaskKV, error) {
	logx.I(ctx, "task(ExportKBPackage) Prepare, task: %+v, params: %+v", d.task, d.p)

	// 创建导出根目录，路径格式：/日期/task_xxx/knowledge_bases
	dateStr := time.Now().Format("20060102") // 格式：YYYYMMDD
	exportRootPath := filepath.Join(kb_package.ExportRootPath, dateStr, fmt.Sprintf("task_%d/knowledge_bases", d.p.TaskID))
	if err := os.MkdirAll(exportRootPath, 0755); err != nil {
		logx.E(ctx, "task(ExportKBPackage) Process mkdir failed, path: %s, err: %+v", exportRootPath, err)
		return nil, fmt.Errorf("mkdir failed: %w", err)
	}
	d.exportRootPath = exportRootPath
	d.exportZipFilePath = fmt.Sprintf("%s.zip", d.exportRootPath)
	logx.I(ctx, "task(ExportKBPackage)  exportRootPath: %s", exportRootPath)

	kv := make(task_scheduler.TaskKV)

	// 1. 如果没有需要导出的知识库，直接返回
	if len(d.p.KbIDs) == 0 {
		logx.W(ctx, "task(ExportKBPackage) Prepare no knowledge base to export")
		return kv, nil
	}

	// 2. 验证应用信息
	appBaseInfo, err := d.rpc.AppAdmin.GetAppBaseInfoByPrimaryId(ctx, d.p.AppPrimaryID)
	if err != nil {
		logx.E(ctx, "task(ExportKBPackage) Prepare GetAppBaseInfoByPrimaryId failed, appPrimaryID: %d, err: %+v", d.p.AppPrimaryID, err)
		return nil, err
	}
	if appBaseInfo == nil {
		logx.E(ctx, "task(ExportKBPackage) Prepare app not found, appPrimaryID: %d", d.p.AppPrimaryID)
		return nil, errs.ErrAppNotFound
	}
	logx.I(ctx, "task(ExportKBPackage) Prepare app info: appBizID=%d, appName=%s", appBaseInfo.BizId, appBaseInfo.Name)

	// 3. 获取知识库信息
	appListReq := appconfig.ListAppBaseInfoReq{
		AppBizIds:  d.p.KbIDs,
		PageNumber: 1,
		PageSize:   uint32(len(d.p.KbIDs)),
	}
	knowledgeBases, _, err := d.rpc.AppAdmin.ListAppBaseInfo(ctx, &appListReq)
	if err != nil {
		logx.E(ctx, "task(ExportKBPackage) Prepare ListAppBaseInfo failed, corpBizID: %d, kbIDs: %+v, err: %+v",
			d.p.CorpBizID, d.p.KbIDs, err)
		return nil, err
	}

	// 验证所有知识库都存在
	if len(knowledgeBases) != len(d.p.KbIDs) {
		existKbIDs := make(map[uint64]bool)
		for _, kb := range knowledgeBases {
			existKbIDs[kb.BizId] = true
		}

		var missingKbIDs []uint64
		for _, kbID := range d.p.KbIDs {
			if !existKbIDs[kbID] {
				missingKbIDs = append(missingKbIDs, kbID)
			}
		}
		logx.E(ctx, "task(ExportKBPackage) Prepare some knowledge bases not found, missingKbIDs: %+v", missingKbIDs)
		return nil, fmt.Errorf("some knowledge bases not found: %+v", missingKbIDs)
	}

	// 4. 设置任务KV，将每个知识库ID作为key存储
	for _, kb := range knowledgeBases {
		kbIDStr := cast.ToString(kb.BizId)
		kbPrimaryIDStr := cast.ToString(kb.PrimaryId)
		kv[kbIDStr] = kbPrimaryIDStr
		logx.I(ctx, "task(ExportKBPackage) Prepare add knowledge base to kv, kbBizID: %d", kb.BizId)
	}

	// 5. 导出知识库配置到COS
	if d.p.Scene == kb_package.SceneAppPackage { // 应用包场景,需要导出知识库配置
		if err := d.exportKBConfig(ctx, appBaseInfo, d.p.KbIDs); err != nil {
			logx.E(ctx, "task(ExportKBPackage) Prepare exportKBConfig failed, err: %+v", err)
			return nil, fmt.Errorf("export kb config failed: %w", err)
		}
	}

	logx.I(ctx, "task(ExportKBPackage) Prepare completed, kv: %+v", kv)
	return kv, nil
}

// Process 任务处理阶段
func (d *ExportKBPackageTaskHandler) Process(ctx context.Context, progress *task_scheduler.Progress) error {
	start := time.Now()
	logx.I(ctx, "task(ExportKBPackage) Process, task: %+v, params: %+v", d.task, d.p)

	// 2. 遍历每个知识库进行导出
	taskKV := progress.TaskKV(ctx)

	for kbIDStr, kbPrimaryIDStr := range taskKV {
		t1 := time.Now()
		logx.I(ctx, "task(ExportKBPackage) Process start export kb, kbID:%s", kbIDStr)

		// 解析知识库ID
		kbID := cast.ToUint64(kbIDStr)
		if kbID == 0 {
			logx.E(ctx, "task(ExportKBPackage) Process invalid kbID: %s", kbIDStr)
			continue
		}
		kbPrimaryID := cast.ToUint64(kbPrimaryIDStr)
		if kbPrimaryID == 0 {
			logx.E(ctx, "task(ExportKBPackage) Process invalid kbPrimaryID: %s", kbPrimaryIDStr)
			continue
		}

		// 创建知识库导出目录
		kbExportPath := filepath.Join(d.exportRootPath, fmt.Sprintf("kb_%d", kbID))
		if err := os.MkdirAll(kbExportPath, 0755); err != nil {
			logx.E(ctx, "task(ExportKBPackage) Process mkdir kb export path failed, path: %s, err: %+v", kbExportPath, err)
			return fmt.Errorf("mkdir kb export path failed: %w", err)
		}

		// 构建导出配置
		exportConfig := &kb_package.ExportConfig{
			CorpPrimaryID: d.p.CorpPrimaryID,
			CorpBizID:     d.p.CorpBizID,
			AppBizID:      d.p.AppBizID,
			AppPrimaryID:  d.p.AppPrimaryID,
			KbID:          kbID,
			KbPrimaryID:   kbPrimaryID,
			LocalPath:     kbExportPath,
		}
		logx.I(ctx, "task(ExportKBPackage) Process exportConfig: %+v", jsonx.MustMarshalToString(exportConfig))

		// 为每个知识库创建一个 IDs 收集器
		idsCollector := kb_package.NewKbIdsCollector()

		// 导出分类数据
		exportConfig.LocalPath = filepath.Join(kbExportPath, kb_package.ExportDirCategory)
		if err := d.cateLogic.ExportCategory(ctx, exportConfig, idsCollector); err != nil {
			logx.E(ctx, "task(ExportKBPackage) Process ExportCategory failed, kbID: %d, err: %+v", kbID, err)
			return fmt.Errorf("export category failed: %w", err)
		}
		logx.I(ctx, "task(ExportKBPackage) Process ExportCategory success, kbID: %d", kbID)

		// 导出标签数据
		exportConfig.LocalPath = filepath.Join(kbExportPath, kb_package.ExportDirLabel)
		if err := d.labelLogic.ExportLabels(ctx, exportConfig); err != nil {
			logx.E(ctx, "task(ExportKBPackage) Process ExportLabel failed, kbID: %d, err: %+v", kbID, err)
			return fmt.Errorf("export label failed: %w", err)
		}
		logx.I(ctx, "task(ExportKBPackage) Process ExportLabel success, kbID: %d", kbID)

		// 导出问答数据
		exportConfig.LocalPath = filepath.Join(kbExportPath, kb_package.ExportDirQA)
		if err := d.qaLogic.ExportQAs(ctx, exportConfig, idsCollector); err != nil {
			logx.E(ctx, "task(ExportKBPackage) Process ExportQAs failed, kbID: %d, err: %+v", kbID, err)
			return fmt.Errorf("export qas failed: %w", err)
		}
		logx.I(ctx, "task(ExportKBPackage) Process ExportQAs success, kbID: %d", kbID)

		// 导出文档数据
		exportConfig.LocalPath = filepath.Join(kbExportPath, kb_package.ExportDirDocument)
		if err := d.docLogic.ExportDocuments(ctx, exportConfig, idsCollector); err != nil {
			logx.E(ctx, "task(ExportKBPackage) Process ExportDocuments failed, kbID: %d, err: %+v", kbID, err)
			return fmt.Errorf("export documents failed: %w", err)
		}
		logx.I(ctx, "task(ExportKBPackage) Process ExportDocuments success, kbID: %d", kbID)

		// 导出schema数据
		exportConfig.LocalPath = filepath.Join(kbExportPath, kb_package.ExportDirSchema)
		if err := d.kbLogic.ExportKnowledgeSchemas(ctx, exportConfig, idsCollector); err != nil {
			logx.E(ctx, "task(ExportKBPackage) Process ExportKnowledgeSchemas failed, kbID: %d, err: %+v", kbID, err)
			//return fmt.Errorf("export schemas failed: %w", err) todo cooper schema先不报错，后续再处理
		}
		logx.I(ctx, "task(ExportKBPackage) Process ExportKnowledgeSchemas success, kbID: %d", kbID)

		// 从收集器获取该知识库的 IDs 信息
		idsMetadata := idsCollector.GetResult()
		logx.I(ctx, "task(ExportKBPackage) Process collected IDs, kbID: %d, labels: %d, labelValues: %d, qaCategories: %d, docCategories: %d, docs: %d, segments: %d",
			kbID, len(idsMetadata.KbLabel), len(idsMetadata.KbLabelValue), len(idsMetadata.KbQaCategory),
			len(idsMetadata.KbDocCategory), len(idsMetadata.KbDoc), len(idsMetadata.KbSegment))

		// 获取知识库信息
		kbInfo, err := d.rpc.AppAdmin.GetAppBaseInfoByPrimaryId(ctx, kbPrimaryID)
		if err != nil {
			logx.E(ctx, "task(ExportKBPackage) Process GetAppBaseInfoByPrimaryId failed, kbPrimaryID: %d, err: %+v", kbPrimaryID, err)
			return fmt.Errorf("get kb info failed: %w", err)
		}

		// 在kb_xxx目录下写入metadata.json（不包含IDs信息）
		if err := d.writeKbMetadata(ctx, kbExportPath, kbID, kbInfo); err != nil {
			logx.E(ctx, "task(ExportKBPackage) Process writeKbMetadata failed, kbID: %d, err: %+v", kbID, err)
			return fmt.Errorf("write kb metadata failed: %w", err)
		}
		logx.I(ctx, "task(ExportKBPackage) Process writeKbMetadata success, kbID: %d", kbID)

		// 将kb_xxxx 目录压缩成zip包
		kbZipFileName := fmt.Sprintf("kb_%d.zip", kbID)
		kbZipFilePath := filepath.Join(d.exportRootPath, kbZipFileName)
		if err := d.zipDirectory(ctx, kbExportPath, kbZipFilePath); err != nil {
			logx.E(ctx, "task(ExportKBPackage) Process zip kb directory failed, kbID: %d, err: %+v", kbID, err)
			return fmt.Errorf("zip kb directory failed: %w", err)
		}
		logx.I(ctx, "task(ExportKBPackage) Process zip kb directory success, kbID: %d, zipFile: %s", kbID, kbZipFilePath)

		// 计算zip包的CRC64值
		kbZipCRC64, err := d.kbPKGLogic.CalculateFileCRC64(ctx, kbZipFilePath, config.App().KbPackageConfig.SecretKey)
		if err != nil {
			logx.E(ctx, "task(ExportKBPackage) Process calculate kb zip crc64 failed, kbID: %d, err: %+v", kbID, err)
			return fmt.Errorf("calculate kb zip crc64 failed: %w", err)
		}
		logx.I(ctx, "task(ExportKBPackage) Process calculate kb zip crc64 success, kbID: %d, crc64: %s", kbID, kbZipCRC64)

		// 压缩完成后删除原始目录以节省空间
		if err := os.RemoveAll(kbExportPath); err != nil {
			logx.W(ctx, "task(ExportKBPackage) Process remove kb export dir failed, kbID: %d, path: %s, err: %+v", kbID, kbExportPath, err)
		} else {
			logx.I(ctx, "task(ExportKBPackage) Process remove kb export dir success, kbID: %d", kbID)
		}

		// 将导出信息添加到数组中（线程安全），同时保存IDs信息
		kbExportInfo := kb_package.KbExportInfo{
			KnowledgeBaseId: cast.ToString(kbID),
			Name:            kbInfo.Name,
			Path:            fmt.Sprintf("kb_%d", kbID),
			Hash:            kbZipCRC64,
			Ids:             idsMetadata, // 保存该知识库的IDs信息
		}
		d.mu.Lock()
		d.kbExportInfos = append(d.kbExportInfos, kbExportInfo)
		d.mu.Unlock()
		logx.I(ctx, "task(ExportKBPackage) Process added kb export info, kbID: %d, total: %d", kbID, len(d.kbExportInfos))

		if err := progress.Finish(ctx, kbIDStr); err != nil {
			logx.E(ctx, "task(ExportKBPackage) Process export kb Finish key:%s,err:%+v", kbIDStr, err)
			return err
		}
		logx.I(ctx, "task(ExportKBPackage) Process export kb completed, kbID: %d, name: %s, cost:%dms", kbID, kbInfo.Name, time.Since(t1).Milliseconds())
	}

	logx.I(ctx, "task(ExportKBPackage) Process completed, cost:%dms", time.Since(start).Milliseconds())
	return nil
}

// Fail 任务失败处理
func (d *ExportKBPackageTaskHandler) Fail(ctx context.Context) error {
	logx.E(ctx, "task(ExportKBPackage) Failed, task: %+v", d.task)
	// 1. 清理临时文件
	if err := os.RemoveAll(d.exportRootPath); err != nil {
		logx.W(ctx, "task(ExportKBPackage) Failed remove temp dir failed, path: %s, err: %+v", d.exportRootPath, err)
	}
	knowledgeBasesZipPath := fmt.Sprintf("%s.zip", d.exportRootPath)
	if err := os.Remove(knowledgeBasesZipPath); err != nil {
		logx.W(ctx, "task(ExportKBPackage) Failed remove knowledge_bases.zip failed, path: %s, err: %+v", knowledgeBasesZipPath, err)
	}
	// 2. 更新任务状态为失败
	if err := d.callbackTaskStatus(ctx, false); err != nil {
		logx.E(ctx, "task(ExportKBPackage) Failed callback failed, err: %+v", err)
		return err
	}
	logx.I(ctx, "task(ExportKBPackage) Failed callback success")
	return nil
}

// Stop 任务停止处理
func (d *ExportKBPackageTaskHandler) Stop(ctx context.Context) error {
	logx.I(ctx, "task(ExportKBPackage) Stopped, task: %+v", d.task)

	// TODO cooper: 实现停止处理逻辑
	// 1. 中断导出过程
	// 2. 清理资源

	return nil
}

// Done 任务完成回调
func (d *ExportKBPackageTaskHandler) Done(ctx context.Context) error {
	start := time.Now()
	logx.I(ctx, "task(ExportKBPackage) Done, task: %+v", d.task)

	// 1. 获取所有知识库的导出信息（已在Process中收集）
	exportedKbInfos := d.kbExportInfos
	logx.I(ctx, "task(ExportKBPackage) Done collected %d kb infos", len(exportedKbInfos))

	// 汇总所有知识库的IDs信息
	allIdsMetadata := d.mergeAllKbIds(ctx, exportedKbInfos)

	// 2. 写入knowledge_bases目录的metadata.json（包含汇总的IDs）
	logx.I(ctx, "task(ExportKBPackage) Done ready to write kb package metadata, taskID: %d, exportedKbCount: %d", d.p.TaskID, len(exportedKbInfos))
	if err := d.writeKbPackageMetadata(ctx, d.exportRootPath, exportedKbInfos, allIdsMetadata); err != nil {
		logx.E(ctx, "task(ExportKBPackage) Done writeKbPackageMetadata failed, err: %+v", err)
		return fmt.Errorf("write kb package metadata failed: %w", err)
	}
	logx.I(ctx, "task(ExportKBPackage) Done writeKbPackageMetadata completed successfully")

	// 3. 压缩knowledge_bases目录为knowledge_bases.zip
	knowledgeBasesZipPath := fmt.Sprintf("%s.zip", d.exportRootPath)
	if err := d.zipDirectory(ctx, d.exportRootPath, knowledgeBasesZipPath); err != nil {
		logx.E(ctx, "task(ExportKBPackage) Done zip knowledge_bases directory failed, err: %+v", err)
		return fmt.Errorf("zip knowledge_bases directory failed: %w", err)
	}
	logx.I(ctx, "task(ExportKBPackage) Done zip knowledge_bases directory success, zipFilePath: %s", knowledgeBasesZipPath)

	// 4. 上传knowledge_bases.zip到COS
	cosPath := filepath.Join(d.p.ExportCosPath, filepath.Base(knowledgeBasesZipPath))
	if err := d.s3.PutFile(ctx, knowledgeBasesZipPath, cosPath); err != nil {
		logx.E(ctx, "task(ExportKBPackage) Done upload knowledge_bases.zip to cos failed, cosPath: %s, err: %+v", cosPath, err)
		return fmt.Errorf("upload knowledge_bases.zip to cos failed: %w", err)
	}
	logx.I(ctx, "task(ExportKBPackage) Done upload knowledge_bases.zip to cos success, cosPath: %s", cosPath)

	// 5. 清理临时文件
	if err := os.RemoveAll(d.exportRootPath); err != nil {
		logx.W(ctx, "task(ExportKBPackage) Done remove temp dir failed, path: %s, err: %+v", d.exportRootPath, err)
	}
	if err := os.Remove(knowledgeBasesZipPath); err != nil {
		logx.W(ctx, "task(ExportKBPackage) Done remove knowledge_bases.zip failed, path: %s, err: %+v", knowledgeBasesZipPath, err)
	}

	// 6. 更新任务状态为完成
	if err := d.callbackTaskStatus(ctx, true); err != nil {
		logx.E(ctx, "task(ExportKBPackage) Done callback failed, err: %+v", err)
		return err
	}

	logx.I(ctx, "task(ExportKBPackage) Done completed, exported %d knowledge bases, cost:%dms", len(exportedKbInfos), time.Since(start).Milliseconds())
	return nil
}

// writeJSONFile 通用的写入JSON文件函数
func (d *ExportKBPackageTaskHandler) writeJSONFile(ctx context.Context, filePath string, data interface{}) error {
	// 创建JSON文件
	file, err := os.Create(filePath)
	if err != nil {
		logx.E(ctx, "writeJSONFile create file failed, path: %s, err: %v", filePath, err)
		return fmt.Errorf("create file failed: %w", err)
	}
	defer file.Close()

	// 使用encoding/json包编码JSON
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ") // 设置缩进为2个空格
	if err := encoder.Encode(data); err != nil {
		logx.E(ctx, "writeJSONFile encode failed, path: %s, err: %v", filePath, err)
		return fmt.Errorf("encode json failed: %w", err)
	}

	logx.I(ctx, "writeJSONFile done, file: %s", filePath)
	return nil
}

// writeKbPackageMetadata 写入知识库包的metadata.json
func (d *ExportKBPackageTaskHandler) writeKbPackageMetadata(ctx context.Context, exportRootPath string, kbInfos []kb_package.KbExportInfo, allIds *kb_package.KbMetadataIds) error {
	logx.I(ctx, "writeKbPackageMetadata start, taskID: %d, exportRootPath: %s, kbCount: %d", d.p.TaskID, exportRootPath, len(kbInfos))

	// 构建metadata结构
	metadata := kb_package.KbPackageMetadata{
		KnowledgeBases: kbInfos,
		ReferenceIds:   allIds,
	}

	// 写入JSON文件
	metadataFilePath := filepath.Join(exportRootPath, "metadata.json")
	if err := d.writeJSONFile(ctx, metadataFilePath, metadata); err != nil {
		return fmt.Errorf("write kb package metadata failed: %w", err)
	}

	logx.I(ctx, "writeKbPackageMetadata done, file: %s, kb_count: %d", metadataFilePath, len(kbInfos))
	return nil
}

// writeKbMetadata 写入单个知识库的metadata.json
func (d *ExportKBPackageTaskHandler) writeKbMetadata(ctx context.Context, kbExportPath string, kbID uint64, kbInfo *entity.AppBaseInfo) error {
	logx.I(ctx, "writeKbMetadata start, kbExportPath: %s, kbID: %d, kbName: %s, isShared: %v", kbExportPath, kbID, kbInfo.Name, kbInfo.IsShared)

	// 构建metadata结构（不包含IDs，IDs信息会汇总到knowledge_bases/metadata.json）
	metadata := kb_package.KbMetadata{
		KnowledgeBaseId: cast.ToString(kbID),
		Name:            kbInfo.Name,
		IsShared:        kbInfo.IsShared,
	}

	// 写入JSON文件
	metadataFilePath := filepath.Join(kbExportPath, "metadata.json")
	if err := d.writeJSONFile(ctx, metadataFilePath, metadata); err != nil {
		return fmt.Errorf("write kb metadata failed: %w", err)
	}

	logx.I(ctx, "writeKbMetadata done, file: %s", metadataFilePath)
	return nil
}

// callbackTaskStatus 回调任务状态
func (d *ExportKBPackageTaskHandler) callbackTaskStatus(ctx context.Context, success bool) error {
	logx.I(ctx, "callbackTaskStatus start, taskID: %d, subTaskID: %d, success: %v, cosPath: %s",
		d.p.TaskID, d.p.SubTaskID, success)

	message := ""
	if !success {
		message = i18n.Translate(ctx, i18nkey.KeyExportKbPackageFailed)
	}
	// 构造回调请求
	req := &appconfig.ImportExportComponentCallbackReq{
		TaskId:     d.p.TaskID,
		SubTaskId:  d.p.SubTaskID,
		Status:     gox.IfElse(success, appconfig.TaskStatus_SUCCESS, appconfig.TaskStatus_FAILED),
		ErrMessage: message,
	}

	// 调用RPC接口
	rsp, err := d.rpc.AppAdmin.ImportExportComponentCallback(ctx, req)
	if err != nil {
		logx.E(ctx, "callbackTaskStatus failed, req: %+v, err: %+v", req, err)
		return fmt.Errorf("callback task status failed: %w", err)
	}

	logx.I(ctx, "callbackTaskStatus success, rsp: %+v", rsp)
	return nil
}

// zipDirectory 压缩目录（串行压缩，优化 I/O）
func (d *ExportKBPackageTaskHandler) zipDirectory(ctx context.Context, sourceDir, zipFilePath string) error {
	logx.I(ctx, "zipDirectory start, sourceDir: %s, zipFilePath: %s", sourceDir, zipFilePath)

	// 1. 收集文件信息
	var files []fileInfo
	var totalSize int64

	err := filepath.Walk(sourceDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}

		relPath, err := filepath.Rel(sourceDir, path)
		if err != nil {
			return err
		}

		files = append(files, fileInfo{
			path:    path,
			relPath: relPath,
			size:    info.Size(),
			modTime: info.ModTime(),
		})
		totalSize += info.Size()

		return nil
	})

	if err != nil {
		return fmt.Errorf("collect files failed: %w", err)
	}

	logx.I(ctx, "Found %d files, total size: %s", len(files), formatFileSize(totalSize))

	// 2. 创建临时文件，最后重命名
	tmpZipPath := zipFilePath + ".tmp"
	zipFile, err := os.Create(tmpZipPath)
	if err != nil {
		return fmt.Errorf("create temp zip file failed: %w", err)
	}

	// 确保清理临时文件
	defer func() {
		zipFile.Close()
		if err != nil {
			os.Remove(tmpZipPath) // 失败时清理临时文件
		}
	}()

	zipWriter := zip.NewWriter(zipFile)
	defer zipWriter.Close()

	// 3. 使用大缓冲区优化 I/O（8MB）
	buffer := make([]byte, config.DescribeZipBufferSize()*1024*1024)

	// 4. 串行处理每个文件（zip.Writer 不是线程安全的）
	for i, file := range files {
		if i%100 == 0 && i > 0 {
			logx.I(ctx, "zipDirectory progress: %d/%d files processed", i, len(files))
		}

		if err := d.addFileToZip(zipWriter, file, buffer); err != nil {
			logx.E(ctx, "addFileToZip failed, file: %s, err: %v", file.path, err)
			return fmt.Errorf("add file to zip failed [%s]: %w", file.relPath, err)
		}
	}

	// 5. 关闭 zipWriter 以确保所有数据写入
	if err := zipWriter.Close(); err != nil {
		return fmt.Errorf("close zip writer failed: %w", err)
	}

	// 6. 关闭文件句柄
	if err := zipFile.Close(); err != nil {
		return fmt.Errorf("close zip file failed: %w", err)
	}

	// 7. 重命名临时文件
	if err := os.Rename(tmpZipPath, zipFilePath); err != nil {
		return fmt.Errorf("rename temp file failed: %w", err)
	}

	logx.I(ctx, "zipDirectory done, zipFilePath: %s, total files: %d", zipFilePath, len(files))
	return nil
}

// addFileToZip 添加单个文件到ZIP
func (d *ExportKBPackageTaskHandler) addFileToZip(zipWriter *zip.Writer, file fileInfo, buffer []byte) error {
	// 创建文件头
	header := &zip.FileHeader{
		Name:     file.relPath,
		Modified: file.modTime,
		Method:   zip.Deflate,
	}
	writer, err := zipWriter.CreateHeader(header)
	if err != nil {
		return fmt.Errorf("create header failed: %w", err)
	}

	// 打开源文件
	srcFile, err := os.Open(file.path)
	if err != nil {
		return fmt.Errorf("open source file failed: %w", err)
	}

	// 流式复制（使用大缓冲区）
	_, copyErr := io.CopyBuffer(writer, srcFile, buffer)

	// 立即关闭文件
	closeErr := srcFile.Close()

	// 优先返回复制错误
	if copyErr != nil {
		return fmt.Errorf("copy file content failed: %w", copyErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close source file failed: %w", closeErr)
	}

	return nil
}

// formatFileSize 格式化文件大小
func formatFileSize(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div := int64(unit)
	exp := 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

// exportKBConfig 导出知识库配置到COS
func (d *ExportKBPackageTaskHandler) exportKBConfig(ctx context.Context, appInfo *entity.AppBaseInfo, kbIDs []uint64) error {
	logx.I(ctx, "exportKBConfig start, appBizId: %d, kbIDs: %v", appInfo.BizId, kbIDs)

	// 1. 先基于应用id获取默认知识库的配置（包含所有共享知识库的检索配置）
	logx.I(ctx, "get default kb config first, appBizId: %d", appInfo.BizId)
	defaultConfigs, err := d.kbDao.DescribeAppKnowledgeConfigList(ctx, d.p.CorpBizID, []uint64{appInfo.BizId})
	if err != nil {
		logx.E(ctx, "DescribeAppKnowledgeConfigList failed, corpBizId: %d, appBizId: %d, err: %v",
			d.p.CorpBizID, appInfo.BizId, err)
		return fmt.Errorf("get default kb config failed: %w", err)
	}
	logx.I(ctx, "get default kb config success, configs count: %d", len(defaultConfigs))

	// 2. 从默认配置中提取所有检索配置，构建 kbID -> 检索配置 的映射
	// 当应用引用了多个共享知识库时，会有多个检索配置，它们的AppBizID相同但KnowledgeBizID不同
	retrievalConfigMap := make(map[uint64]*kbe.KnowledgeConfig)
	for _, config := range defaultConfigs {
		if pb.KnowledgeBaseConfigType(config.Type) == pb.KnowledgeBaseConfigType_RETRIEVAL_SETTING {
			retrievalConfigMap[config.KnowledgeBizID] = config
			logx.I(ctx, "found retrieval config, kbID: %d", config.KnowledgeBizID)
		}
	}
	logx.I(ctx, "extracted retrieval configs, total count: %d", len(retrievalConfigMap))

	// 3. 构建最终的配置结构
	kbConfigExport := &kb_package.KBConfigExport{
		KnowledgeBases: make([]kb_package.KBConfigItem, 0, len(kbIDs)),
	}

	// 4. 遍历每个知识库ID，获取配置信息
	for _, kbID := range kbIDs {
		var configs []*kbe.KnowledgeConfig

		// 判断是否为默认知识库（kbID和appBizID一致）
		if kbID == appInfo.BizId {
			// 默认知识库：使用已获取的配置，但需要过滤检索配置，只保留自己的
			logx.I(ctx, "use default kb config, kbID: %d", kbID)
			configs = make([]*kbe.KnowledgeConfig, 0, len(defaultConfigs))
			for _, config := range defaultConfigs {
				// 检索配置需要过滤：只保留属于当前知识库的检索配置
				if pb.KnowledgeBaseConfigType(config.Type) == pb.KnowledgeBaseConfigType_RETRIEVAL_SETTING {
					if config.KnowledgeBizID == kbID {
						configs = append(configs, config)
						logx.I(ctx, "append retrieval config for default kb, kbID: %d", kbID)
					}
				} else {
					// 其他配置直接添加
					configs = append(configs, config)
				}
			}
		} else {
			// 共享知识库：获取共享配置（不包含检索配置）
			logx.I(ctx, "get share kb config, kbID: %d", kbID)
			configTypes := []uint32{
				uint32(pb.KnowledgeBaseConfigType_EMBEDDING_MODEL),
				uint32(pb.KnowledgeBaseConfigType_QA_EXTRACT_MODEL),
				uint32(pb.KnowledgeBaseConfigType_KNOWLEDGE_SCHEMA_MODEL),
				uint32(pb.KnowledgeBaseConfigType_FILE_PARSE_MODEL),
			}
			shareConfigs, err := d.kbDao.GetShareKnowledgeConfigs(ctx, d.p.CorpBizID, []uint64{kbID}, configTypes)
			if err != nil {
				logx.E(ctx, "GetShareKnowledgeConfigs failed, corpBizId: %d, kbID: %d, err: %v",
					d.p.CorpBizID, kbID, err)
				return fmt.Errorf("get share kb config failed: %w", err)
			}

			// 将共享配置和检索配置合并
			configs = shareConfigs
			// 从map中根据kbID获取对应的检索配置
			if retrievalConfig, exists := retrievalConfigMap[kbID]; exists {
				configs = append(configs, retrievalConfig)
				logx.I(ctx, "append retrieval config to share kb, kbID: %d", kbID)
			} else {
				logx.W(ctx, "retrieval config not found for share kb, kbID: %d", kbID)
			}
		}

		logx.I(ctx, "get kb config success, kbID: %d, configs: %s", kbID, jsonx.MustMarshalToString(configs))

		// 导出配置并组装成目标格式
		kbConfigItem, err := d.exportKBConfigItem(ctx, kbID, configs)
		if err != nil {
			logx.E(ctx, "exportKBConfigItem failed, kbID: %d, err: %v", kbID, err)
			return fmt.Errorf("export kb config item failed: %w", err)
		}
		logx.D(ctx, "exported kb config item, kbID: %d, kbConfigItem: %s", kbID, jsonx.MustMarshalToString(kbConfigItem))
		kbConfigExport.KnowledgeBases = append(kbConfigExport.KnowledgeBases, *kbConfigItem)
	}

	// 将配置信息序列化为JSON
	configJSON, err := json.MarshalIndent(kbConfigExport, "", "  ")
	if err != nil {
		logx.E(ctx, "marshal config to json failed, err: %v", err)
		return fmt.Errorf("marshal config to json failed: %w", err)
	}

	// 直接上传到COS
	kbConfigCosPath := filepath.Join(d.p.ExportCosPath, "app/kb_config/kb_refs.json")
	if err := d.s3.PutObject(ctx, configJSON, kbConfigCosPath); err != nil {
		logx.E(ctx, "upload config to cos failed, cosPath: %s, err: %v", kbConfigCosPath, err)
		return fmt.Errorf("upload config to cos failed: %w", err)
	}
	logx.I(ctx, "upload config to cos success, cosPath: %s", kbConfigCosPath)

	// 上传 metadata.json
	if err := d.uploadKBConfigMetadata(ctx, kbConfigExport); err != nil {
		logx.E(ctx, "upload kb config metadata failed, err: %v", err)
		return fmt.Errorf("upload kb config metadata failed: %w", err)
	}

	logx.I(ctx, "exportKBConfig completed, appBizId: %d, kbIDs: %v", appInfo.BizId, kbIDs)
	return nil
}

// exportKBConfigItem 导出知识库配置项
func (d *ExportKBPackageTaskHandler) exportKBConfigItem(ctx context.Context, kbID uint64, configs []*kbe.KnowledgeConfig) (*kb_package.KBConfigItem, error) {
	kbConfigItem := &kb_package.KBConfigItem{
		KnowledgeBaseId: cast.ToString(kbID),
		ModelConfig:     &kb_package.ModelConfig{},
		RetrievalConfig: &kb_package.RetrievalConfigData{},
	}

	for _, config := range configs {
		// 根据是否为默认知识库选择不同的配置字段
		configStr := config.Config
		if configStr == "" {
			continue
		}
		logx.I(ctx, "exportKBConfigItem, configType:%s, configStr: %s", pb.KnowledgeBaseConfigType(config.Type).String(), configStr)

		// 根据配置类型导出并填充到对应字段
		configType := pb.KnowledgeBaseConfigType(config.Type)
		switch configType {
		case pb.KnowledgeBaseConfigType_EMBEDDING_MODEL,
			pb.KnowledgeBaseConfigType_QA_EXTRACT_MODEL,
			pb.KnowledgeBaseConfigType_KNOWLEDGE_SCHEMA_MODEL,
			pb.KnowledgeBaseConfigType_FILE_PARSE_MODEL:
			// 统一处理模型配置
			if err := d.exportModelConfig(ctx, configStr, configType, kbConfigItem.ModelConfig); err != nil {
				logx.E(ctx, "exportModelConfig(%s) failed, kbID: %d, err: %v", configType.String(), kbID, err)
				return nil, fmt.Errorf("export %s config failed: %w", configType.String(), err)
			}

		case pb.KnowledgeBaseConfigType_RETRIEVAL_SETTING:
			// 导出检索配置，同时提取NaturalLanguageToSqlModel
			if err := d.exportRetrievalConfig(ctx, configStr, kbConfigItem.RetrievalConfig, kbConfigItem.ModelConfig); err != nil {
				logx.E(ctx, "exportRetrievalConfig failed, kbID: %d, err: %v", kbID, err)
				return nil, err
			}
		}
	}

	return kbConfigItem, nil
}

// exportModelConfig 导出模型配置（统一处理不同类型的模型）
// 流程：DB小驼峰JSON -> pb结构体 -> kb_package.ModelInfo(大驼峰)
func (d *ExportKBPackageTaskHandler) exportModelConfig(ctx context.Context, configStr string, configType pb.KnowledgeBaseConfigType, modelConfig *kb_package.ModelConfig) error {
	logx.I(ctx, "exportModelConfig start, configType: %s", configType.String())

	// 1. 根据配置类型反序列化到对应的pb结构体
	pbModel, err := d.unmarshalPBModel(ctx, configStr, configType)
	if err != nil {
		logx.E(ctx, "exportModelConfig unmarshal to pb failed, configType: %s, err: %v", configType.String(), err)
		return fmt.Errorf("unmarshal model config failed: %w", err)
	}

	// 2. 从pb结构体转换为对外的大驼峰结构体并填充到对应字段
	switch configType {
	case pb.KnowledgeBaseConfigType_EMBEDDING_MODEL,
		pb.KnowledgeBaseConfigType_QA_EXTRACT_MODEL,
		pb.KnowledgeBaseConfigType_KNOWLEDGE_SCHEMA_MODEL:
		// 转换为 ModelInfo
		modelInfo, err := d.convertPBToModelInfo(ctx, pbModel, configType)
		if err != nil {
			logx.E(ctx, "exportModelConfig convert pb to model info failed, configType: %s, err: %v", configType.String(), err)
			return fmt.Errorf("convert pb to model info failed: %w", err)
		}
		// 填充到对应字段
		switch configType {
		case pb.KnowledgeBaseConfigType_EMBEDDING_MODEL:
			modelConfig.EmbeddingModel = modelInfo
		case pb.KnowledgeBaseConfigType_QA_EXTRACT_MODEL:
			modelConfig.QaExtractModel = modelInfo
		case pb.KnowledgeBaseConfigType_KNOWLEDGE_SCHEMA_MODEL:
			modelConfig.KnowledgeSchemaModel = modelInfo
		}

	case pb.KnowledgeBaseConfigType_FILE_PARSE_MODEL:
		// FILE_PARSE_MODEL 使用不同的结构体
		pbFileParseModel, ok := pbModel.(*common.FileParseModel)
		if !ok {
			return fmt.Errorf("invalid pb model type for FILE_PARSE_MODEL")
		}
		modelConfig.FileParseModel = &kb_package.FileParseModelInfo{
			ModelName:                     pbFileParseModel.ModelName,
			ModelAliasName:                pbFileParseModel.AliasName,
			FormulaEnhancement:            pbFileParseModel.FormulaEnhancement != nil && *pbFileParseModel.FormulaEnhancement,
			LargeLanguageModelEnhancement: pbFileParseModel.LargeLanguageModelEnhancement != nil && *pbFileParseModel.LargeLanguageModelEnhancement,
			OutputHtmlTable:               pbFileParseModel.OutputHtmlTable != nil && *pbFileParseModel.OutputHtmlTable,
		}

	default:
		return fmt.Errorf("unsupported config type: %s", configType.String())
	}

	logx.I(ctx, "exportModelConfig success, configType: %s", configType.String())
	return nil
}

// unmarshalPBModel 根据配置类型反序列化为对应的pb结构体
func (d *ExportKBPackageTaskHandler) unmarshalPBModel(ctx context.Context, configStr string, configType pb.KnowledgeBaseConfigType) (interface{}, error) {
	switch configType {
	case pb.KnowledgeBaseConfigType_EMBEDDING_MODEL:
		pbModel := &pb.EmbeddingModel{}
		if err := jsonx.Unmarshal([]byte(configStr), pbModel); err != nil {
			return nil, err
		}
		return pbModel, nil

	case pb.KnowledgeBaseConfigType_QA_EXTRACT_MODEL:
		pbModel := &pb.QaExtractModel{}
		if err := jsonx.Unmarshal([]byte(configStr), pbModel); err != nil {
			return nil, err
		}
		return pbModel, nil

	case pb.KnowledgeBaseConfigType_KNOWLEDGE_SCHEMA_MODEL:
		pbModel := &pb.KnowledgeSchemaModel{}
		if err := jsonx.Unmarshal([]byte(configStr), pbModel); err != nil {
			return nil, err
		}
		return pbModel, nil

	case pb.KnowledgeBaseConfigType_FILE_PARSE_MODEL:
		pbModel := &common.FileParseModel{}
		if err := jsonx.Unmarshal([]byte(configStr), pbModel); err != nil {
			return nil, err
		}
		return pbModel, nil

	default:
		return nil, fmt.Errorf("unsupported config type: %s", configType.String())
	}
}

// convertPBToModelInfo 将pb结构体转换为kb_package.ModelInfo
func (d *ExportKBPackageTaskHandler) convertPBToModelInfo(ctx context.Context, pbModel interface{}, configType pb.KnowledgeBaseConfigType) (*kb_package.ModelInfo, error) {
	if pbModel == nil {
		return nil, fmt.Errorf("pbModel is nil")
	}

	switch configType {
	case pb.KnowledgeBaseConfigType_EMBEDDING_MODEL:
		// EmbeddingModel 只有 ModelName 和 AliasName
		model, ok := pbModel.(*pb.EmbeddingModel)
		if !ok {
			return nil, fmt.Errorf("invalid pb model type for EMBEDDING_MODEL")
		}
		return &kb_package.ModelInfo{
			ModelName:      model.ModelName,
			ModelAliasName: model.AliasName,
			// EmbeddingModel 没有 ModelParams 和 HistoryLimit
		}, nil

	case pb.KnowledgeBaseConfigType_QA_EXTRACT_MODEL:
		// QaExtractModel 包含 ModelParams 和 HistoryLimit
		model, ok := pbModel.(*pb.QaExtractModel)
		if !ok {
			return nil, fmt.Errorf("invalid pb model type for QA_EXTRACT_MODEL")
		}
		return &kb_package.ModelInfo{
			ModelName:      model.ModelName,
			ModelAliasName: model.AliasName,
			ModelParams:    kb_package.ConvertToLocalModelParams(model.ModelParams),
			HistoryLimit:   int(model.HistoryLimit),
		}, nil

	case pb.KnowledgeBaseConfigType_KNOWLEDGE_SCHEMA_MODEL:
		// KnowledgeSchemaModel 包含 ModelParams 和 HistoryLimit
		model, ok := pbModel.(*pb.KnowledgeSchemaModel)
		if !ok {
			return nil, fmt.Errorf("invalid pb model type for KNOWLEDGE_SCHEMA_MODEL")
		}
		return &kb_package.ModelInfo{
			ModelName:      model.ModelName,
			ModelAliasName: model.AliasName,
			ModelParams:    kb_package.ConvertToLocalModelParams(model.ModelParams),
			HistoryLimit:   int(model.HistoryLimit),
		}, nil

	default:
		return nil, fmt.Errorf("unsupported config type: %s", configType.String())
	}
}

// exportRetrievalConfig 导出检索配置
// 流程：DB小驼峰JSON -> pb.RetrievalConfig -> kb_package.RetrievalConfigData(大驼峰)
func (d *ExportKBPackageTaskHandler) exportRetrievalConfig(ctx context.Context, configStr string, retrievalConfig *kb_package.RetrievalConfigData, modelConfig *kb_package.ModelConfig) error {
	// 1. 从DB读取的小驼峰JSON反序列化到pb结构体
	pbConfig := &pb.RetrievalConfig{}
	if err := jsonx.Unmarshal([]byte(configStr), pbConfig); err != nil {
		logx.E(ctx, "exportRetrievalConfig unmarshal to pb failed, err: %v", err)
		return fmt.Errorf("unmarshal retrieval config failed: %w", err)
	}

	// 2. 从pb结构体转换为对外的大驼峰结构体

	// 2.1 转换Retrievals -> Filters
	if len(pbConfig.Retrievals) > 0 {
		retrievalConfig.Filters = make([]kb_package.FilterItem, 0, len(pbConfig.Retrievals))
		for _, r := range pbConfig.Retrievals {
			retrievalConfig.Filters = append(retrievalConfig.Filters, kb_package.FilterItem{
				RetrievalType: r.RetrievalType.String(),
				IndexId:       int(r.IndexId),
				Confidence:    float64(r.Confidence),
				TopN:          int(r.TopN),
				IsEnable:      r.IsEnable,
			})
		}
	}

	// 2.2 转换SearchStrategy
	if pbConfig.SearchStrategy != nil {
		retrievalConfig.SearchStrategy = &kb_package.KBSearchStrategy{
			RerankModelSwitch:      pbConfig.SearchStrategy.RerankModelSwitch,
			RerankModel:            pbConfig.SearchStrategy.RerankModel,
			StrategyType:           pbConfig.SearchStrategy.StrategyType.String(),
			EnableTableEnhancement: pbConfig.SearchStrategy.TableEnhancement,
		}

		// 提取NaturalLanguageToSqlModel配置到ModelConfig中
		if pbConfig.SearchStrategy.NatureLanguageToSqlModelConfig != nil &&
			pbConfig.SearchStrategy.NatureLanguageToSqlModelConfig.Model != nil {
			pbModel := pbConfig.SearchStrategy.NatureLanguageToSqlModelConfig.Model
			modelConfig.NaturalLanguageToSqlModel = &kb_package.ModelInfo{
				ModelName:      pbModel.ModelName,
				ModelAliasName: pbModel.AliasName,
				ModelParams:    kb_package.ConvertToLocalModelParams(pbModel.ModelParams),
				// NL2SQL模型没有HistoryLimit
			}
			logx.I(ctx, "exported NaturalLanguageToSqlModel from retrieval config: %s", pbModel.ModelName)
		}
	}

	// 2.3 转换RetrievalRange
	if pbConfig.RetrievalRange != nil && len(pbConfig.RetrievalRange.ApiVarAttrInfos) > 0 {
		retrievalConfig.RetrievalRange = &kb_package.RetrievalRange{
			Condition:       pbConfig.RetrievalRange.Condition,
			ApiVarAttrInfos: make([]kb_package.KBApiVarAttrInfo, 0, len(pbConfig.RetrievalRange.ApiVarAttrInfos)),
		}
		for _, info := range pbConfig.RetrievalRange.ApiVarAttrInfos {
			retrievalConfig.RetrievalRange.ApiVarAttrInfos = append(retrievalConfig.RetrievalRange.ApiVarAttrInfos, kb_package.KBApiVarAttrInfo{
				ApiVarId:             info.ApiVarId,
				KnowledgeItemLabelId: info.AttrBizId,
			})
		}
	}

	logx.I(ctx, "exportRetrievalConfig success, filters count: %d", len(retrievalConfig.Filters))
	return nil
}

// uploadKBConfigMetadata 上传知识库配置的 metadata.json
func (d *ExportKBPackageTaskHandler) uploadKBConfigMetadata(ctx context.Context, kbConfigExport *kb_package.KBConfigExport) error {
	logx.I(ctx, "uploadKBConfigMetadata start")

	// 收集所有的 KbLabel ID 和 AppVariable ID
	kbLabelIDSet := make(map[string]bool)
	appVariableIDSet := make(map[string]bool)
	kbIDSet := make(map[string]bool)

	for _, kbConfig := range kbConfigExport.KnowledgeBases {
		if kbConfig.RetrievalConfig != nil && kbConfig.RetrievalConfig.RetrievalRange != nil {
			for _, info := range kbConfig.RetrievalConfig.RetrievalRange.ApiVarAttrInfos {
				// 收集 AttrBizId 作为 KbLabel ID
				if info.KnowledgeItemLabelId > 0 {
					kbLabelIDSet[cast.ToString(info.KnowledgeItemLabelId)] = true
				}
				// 收集 ApiVarId 作为 AppVariable ID
				if info.ApiVarId != "" {
					appVariableIDSet[info.ApiVarId] = true
				}
			}
		}
		kbIDSet[kbConfig.KnowledgeBaseId] = true
	}

	// 构建 metadata 结构
	type IDsMetadata struct {
		ReferenceIds struct {
			AppVariable []string `json:"AppVariables"`
			KbID        []string `json:"Kbs"`
			KbLabel     []string `json:"KbLabels"`
		} `json:"ReferenceIds"`
	}

	metadata := IDsMetadata{}

	// 填充 KbLabel IDs
	metadata.ReferenceIds.KbLabel = make([]string, 0, len(kbLabelIDSet))
	for id := range kbLabelIDSet {
		metadata.ReferenceIds.KbLabel = append(metadata.ReferenceIds.KbLabel, id)
	}

	// 填充 AppVariable IDs
	metadata.ReferenceIds.AppVariable = make([]string, 0, len(appVariableIDSet))
	for id := range appVariableIDSet {
		metadata.ReferenceIds.AppVariable = append(metadata.ReferenceIds.AppVariable, id)
	}
	// 填充 Kb IDs
	metadata.ReferenceIds.KbID = make([]string, 0, len(kbIDSet))
	for id := range kbIDSet {
		metadata.ReferenceIds.KbID = append(metadata.ReferenceIds.KbID, id)
	}

	logx.I(ctx, "collected metadata, KbLabel count: %d, AppVariable count: %d, Kb count: %d",
		len(metadata.ReferenceIds.KbLabel), len(metadata.ReferenceIds.AppVariable), len(metadata.ReferenceIds.KbID))

	// 序列化为 JSON
	metadataJSON, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		logx.E(ctx, "marshal metadata to json failed, err: %v", err)
		return fmt.Errorf("marshal metadata to json failed: %w", err)
	}

	// 上传到 COS
	metadataCosPath := filepath.Join(d.p.ExportCosPath, "app/kb_config/metadata.json")
	if err := d.s3.PutObject(ctx, metadataJSON, metadataCosPath); err != nil {
		logx.E(ctx, "upload metadata to cos failed, cosPath: %s, err: %v", metadataCosPath, err)
		return fmt.Errorf("upload metadata to cos failed: %w", err)
	}

	logx.I(ctx, "upload metadata to cos success, cosPath: %s", metadataCosPath)
	return nil
}

// convertIDSetToSlice 将ID set转换为slice
func (d *ExportKBPackageTaskHandler) convertIDSetToSlice(idSet map[string]bool) []string {
	result := make([]string, 0, len(idSet))
	for id := range idSet {
		result = append(result, id)
	}
	return result
}

// mergeAllKbIds 汇总所有知识库的IDs信息
func (d *ExportKBPackageTaskHandler) mergeAllKbIds(ctx context.Context, kbInfos []kb_package.KbExportInfo) *kb_package.KbMetadataIds {
	logx.I(ctx, "mergeAllKbIds start, kb count: %d", len(kbInfos))

	// 使用map去重
	idSets := map[kb_package.ModuleType]map[string]bool{
		kb_package.ModuleKbLabel:            make(map[string]bool),
		kb_package.ModuleKbLabelValue:       make(map[string]bool),
		kb_package.ModuleKbQaCategory:       make(map[string]bool),
		kb_package.ModuleKbDocCategory:      make(map[string]bool),
		kb_package.ModuleKbDoc:              make(map[string]bool),
		kb_package.ModuleKbSegment:          make(map[string]bool),
		kb_package.ModuleKbDocClusterSchema: make(map[string]bool),
		kb_package.ModuleKbQa:               make(map[string]bool),
	}

	// 遍历所有知识库，合并IDs
	for _, kbInfo := range kbInfos {
		if kbInfo.Ids == nil {
			continue
		}
		// 定义映射：模块类型对应的字段切片
		mappings := []struct {
			slice []string
			set   map[string]bool
		}{
			{slice: kbInfo.Ids.KbLabel, set: idSets[kb_package.ModuleKbLabel]},
			{slice: kbInfo.Ids.KbLabelValue, set: idSets[kb_package.ModuleKbLabelValue]},
			{slice: kbInfo.Ids.KbQaCategory, set: idSets[kb_package.ModuleKbQaCategory]},
			{slice: kbInfo.Ids.KbDocCategory, set: idSets[kb_package.ModuleKbDocCategory]},
			{slice: kbInfo.Ids.KbDoc, set: idSets[kb_package.ModuleKbDoc]},
			{slice: kbInfo.Ids.KbSegment, set: idSets[kb_package.ModuleKbSegment]},
			{slice: kbInfo.Ids.KbSchemaCluster, set: idSets[kb_package.ModuleKbDocClusterSchema]},
			{slice: kbInfo.Ids.KbQa, set: idSets[kb_package.ModuleKbQa]},
		}

		for _, mapping := range mappings {
			for _, item := range mapping.slice {
				if item != "" {
					mapping.set[item] = true
				}
			}
		}
	}

	// 转换为最终结构
	result := &kb_package.KbMetadataIds{
		KbLabel:         d.convertIDSetToSlice(idSets[kb_package.ModuleKbLabel]),
		KbLabelValue:    d.convertIDSetToSlice(idSets[kb_package.ModuleKbLabelValue]),
		KbQaCategory:    d.convertIDSetToSlice(idSets[kb_package.ModuleKbQaCategory]),
		KbDocCategory:   d.convertIDSetToSlice(idSets[kb_package.ModuleKbDocCategory]),
		KbDoc:           d.convertIDSetToSlice(idSets[kb_package.ModuleKbDoc]),
		KbSegment:       d.convertIDSetToSlice(idSets[kb_package.ModuleKbSegment]),
		KbSchemaCluster: d.convertIDSetToSlice(idSets[kb_package.ModuleKbDocClusterSchema]),
		KbQa:            d.convertIDSetToSlice(idSets[kb_package.ModuleKbQa]),
	}

	logx.I(ctx, "mergeAllKbIds done, total - labels: %d, labelValues: %d, qaCategories: %d, docCategories: %d, docs: %d, segments: %d, schemaClusters: %d, qas: %d",
		len(result.KbLabel), len(result.KbLabelValue), len(result.KbQaCategory),
		len(result.KbDocCategory), len(result.KbDoc), len(result.KbSegment),
		len(result.KbSchemaCluster), len(result.KbQa))

	return result
}
