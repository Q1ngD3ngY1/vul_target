package async

import (
	"archive/zip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"git.woa.com/adp/common/x/encodingx/jsonx"
	"git.woa.com/adp/common/x/gox"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	kbe "git.woa.com/adp/kb/kb-config/internal/entity/kb"
	"git.woa.com/adp/kb/kb-config/internal/entity/kb_package"
	"git.woa.com/adp/kb/kb-config/internal/util/idgen"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	"git.woa.com/adp/kb/kb-config/pkg/i18nkey"
	appconfig "git.woa.com/adp/pb-go/app/app_config"
	"git.woa.com/adp/pb-go/common"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
	"git.woa.com/dialogue-platform/bot-config/task_scheduler"
	"github.com/spf13/cast"
)

// ImportKBPackageTaskHandler 知识库导入任务处理器
type ImportKBPackageTaskHandler struct {
	*taskCommon
	importRootPath     string                         // 导入根路径
	task               task_scheduler.Task            // 任务信息
	p                  entity.ImportKbPackageParams   // 导入参数
	idMap              kb_package.IDMappingConfig     // ID映射配置
	appPackageMetadata *kb_package.AppPackageMetadata // 应用包元数据（根目录metadata.json）
	appMetadata        *kb_package.AppMetadata        // 应用配置元数据（app/metadata.json）
}

// registerImportKBPackageTaskHandler 注册知识库导入任务处理器
func registerImportKBPackageTaskHandler(tc *taskCommon) {
	task_scheduler.Register(
		entity.ImportKbPackageTask,
		func(t task_scheduler.Task, params entity.ImportKbPackageParams) task_scheduler.TaskHandler {
			return &ImportKBPackageTaskHandler{
				taskCommon: tc,
				task:       t,
				p:          params,
			}
		},
	)
}

// Prepare 数据准备阶段
func (d *ImportKBPackageTaskHandler) Prepare(ctx context.Context) (task_scheduler.TaskKV, error) {
	logx.I(ctx, "task(ImportKBPackage) Prepare, task: %+v, params: %+v", jsonx.MustMarshalToString(d.task), jsonx.MustMarshalToString(d.p))
	var err error
	// 创建导入根目录，路径格式：/日期/task_xxx
	dateStr := time.Now().Format("20060102") // 格式：YYYYMMDD
	d.importRootPath = filepath.Join(kb_package.ImportRootPath, dateStr, fmt.Sprintf("task_%d", d.p.TaskID))
	logx.I(ctx, "task(ImportKBPackage) Init importRootPath: %s", d.importRootPath)
	if err := os.MkdirAll(d.importRootPath, 0755); err != nil {
		logx.E(ctx, "task(ImportKBPackage) Init mkdir failed, path: %s, err: %+v", d.importRootPath, err)
		return nil, err
	}

	kv := make(task_scheduler.TaskKV)
	if d.p.Scene == kb_package.SceneAppPackage { // 应用包导入
		kv, err = d.prepareAppPackage(ctx)
	} else if d.p.Scene == kb_package.SceneKBDataPackage { // 后续才支持导入知识库包
		logx.W(ctx, "task(ImportKBPackage) Prepare failed, params: %+v", d.p)
		return nil, nil
	}
	logx.I(ctx, "task(ImportKBPackage) Prepare completed, kv: %+v", kv)
	return kv, err
}

// Init 初始化任务
func (d *ImportKBPackageTaskHandler) Init(ctx context.Context, _ task_scheduler.TaskKV) error {
	logx.I(ctx, "task(ImportKBPackage) Init, task: %+v, params: %+v", d.task, d.p)
	// todo cooper 这里需要校验下，如果是重试场景，需要看是重新执行prepareAppPackage还是直接执行Process
	return nil
}

// Process 任务处理阶段
func (d *ImportKBPackageTaskHandler) Process(ctx context.Context, progress *task_scheduler.Progress) error {
	logx.I(ctx, "task(ImportKBPackage) Process, task: %+v, params: %+v", d.task, d.p)

	// 获取任务KV，包含需要导入的知识库列表
	taskKV := progress.TaskKV(ctx)
	if len(taskKV) == 0 {
		logx.W(ctx, "task(ImportKBPackage) Process no knowledge base to import")
		return nil
	}

	// 构建knowledge_bases目录路径
	knowledgeBasesPath := filepath.Join(d.importRootPath, "knowledge_bases")
	// 遍历每个需要导入的知识库
	successCount := 0
	for oldKbIDStr, newKbIDStr := range taskKV {
		logx.I(ctx, "task(ImportKBPackage) Process start import kb, oldKbIDStr: %s, newKbIDStr: %s", oldKbIDStr, newKbIDStr)

		// 1. 解压知识库包
		kbZipPath := filepath.Join(knowledgeBasesPath, fmt.Sprintf("kb_%s.zip", oldKbIDStr))
		kbExtractPath := filepath.Join(knowledgeBasesPath, fmt.Sprintf("kb_%s", oldKbIDStr))
		logx.I(ctx, "task(ImportKBPackage) Process kbZipPath: %s", kbZipPath)
		// 检查zip文件是否存在
		if _, err := os.Stat(kbZipPath); err != nil {
			logx.E(ctx, "task(ImportKBPackage) Process kb zip not found, oldKbIDStr: %s, newKbIDStr: %s, path: %s, err: %+v", oldKbIDStr, newKbIDStr, kbZipPath, err)
			return fmt.Errorf("kb zip not found, oldKbIDStr: %s, path: %s, err: %w", oldKbIDStr, kbZipPath, err)
		}

		// 解压知识库包
		if err := d.unzipFile(ctx, kbZipPath, kbExtractPath); err != nil {
			logx.E(ctx, "task(ImportKBPackage) Process unzip kb failed, oldKbIDStr: %s, err: %+v", oldKbIDStr, err)
			return fmt.Errorf("unzip kb failed, oldKbIDStr: %s, err: %w", oldKbIDStr, err)
		}
		logx.I(ctx, "task(ImportKBPackage) Process unzip kb success, oldKbIDStr: %s, path: %s", oldKbIDStr, kbExtractPath)

		// 2. 读取知识库的metadata.json，判断是否为共享知识库
		kbMetadataPath := filepath.Join(kbExtractPath, "metadata.json")
		kbMetadataData, err := os.ReadFile(kbMetadataPath)
		if err != nil {
			logx.E(ctx, "task(ImportKBPackage) Process read kb metadata failed, oldKbIDStr: %s, path: %s, err: %+v", oldKbIDStr, kbMetadataPath, err)
			return fmt.Errorf("read kb metadata failed, oldKbIDStr: %s, err: %w", oldKbIDStr, err)
		}

		var kbMetadata kb_package.KbMetadata
		if err := json.Unmarshal(kbMetadataData, &kbMetadata); err != nil {
			logx.E(ctx, "task(ImportKBPackage) Process unmarshal kb metadata failed, oldKbIDStr: %s, err: %+v", oldKbIDStr, err)
			return fmt.Errorf("unmarshal kb metadata failed, oldKbIDStr: %s, err: %w", oldKbIDStr, err)
		}
		logx.I(ctx, "task(ImportKBPackage) Process kb metadata, oldKbIDStr: %s, name: %s, isShared: %v", oldKbIDStr, kbMetadata.Name, kbMetadata.IsShared)

		// 将newKbIDStr转换为uint64
		newKbID, err := strconv.ParseUint(newKbIDStr, 10, 64)
		if err != nil {
			logx.E(ctx, "task(ImportKBPackage) Process newKbIDStr to uint64 failed, oldKbIDStr: %s, newKbIDStr: %s, err: %+v", oldKbIDStr, newKbIDStr, err)
			return fmt.Errorf("parse newKbIDStr failed, oldKbIDStr: %s, newKbIDStr: %s, err: %w", oldKbIDStr, newKbIDStr, err)
		}
		// 4. 获取目标知识库的详细信息
		kbInfo, err := d.rpc.AppAdmin.GetAppBaseInfo(ctx, newKbID)
		if err != nil {
			logx.E(ctx, "task(ImportKBPackage) Process GetAppBaseInfo failed, targetKbBizID: %d, err: %+v", newKbID, err)
			return fmt.Errorf("get app base info failed, targetKbBizID: %d, err: %w", newKbID, err)
		}
		if kbInfo == nil {
			logx.E(ctx, "task(ImportKBPackage) Process kb not found, targetKbBizID: %d", newKbID)
			return fmt.Errorf("kb not found, targetKbBizID: %d", newKbID)
		}

		// 3. 构建导入配置基础信息
		importConfig := &kb_package.ImportConfig{
			CorpPrimaryID:   d.p.CorpPrimaryID,
			CorpBizID:       d.p.CorpBizID,
			StaffPrimaryID:  d.p.StaffPrimaryID,
			AppBizID:        d.p.AppBizID,
			AppPrimaryID:    d.p.AppPrimaryID,
			KbID:            kbInfo.BizId,
			KbPrimaryID:     kbInfo.PrimaryId,
			IDMappingConfig: &d.idMap,
		}
		logx.I(ctx, "task(ImportKBPackage) Process importConfig base: %+v", jsonx.MustMarshalToString(importConfig))

		// 4. 依次导入各个模块的数据
		// 4.1 导入分类数据
		importConfig.LocalPath = filepath.Join(kbExtractPath, kb_package.ExportDirCategory)
		if err := d.cateLogic.ImportCategory(ctx, importConfig); err != nil {
			logx.E(ctx, "task(ImportKBPackage) Process ImportCategory failed, oldKbIDStr: %s, newKbIDStr: %d, err: %+v", oldKbIDStr, importConfig.KbID, err)
			return fmt.Errorf("import category failed, oldKbIDStr: %s, newKbIDStr: %d, err: %w", oldKbIDStr, importConfig.KbID, err)
		}
		logx.I(ctx, "task(ImportKBPackage) Process ImportCategory success, oldKbIDStr: %s, newKbIDStr: %d", oldKbIDStr, importConfig.KbID)

		// 4.2 导入标签数据
		importConfig.LocalPath = filepath.Join(kbExtractPath, kb_package.ExportDirLabel)
		if err := d.labelLogic.ImportLabels(ctx, importConfig); err != nil {
			logx.E(ctx, "task(ImportKBPackage) Process ImportLabels failed, oldKbIDStr: %s, newKbIDStr: %d, err: %+v", oldKbIDStr, importConfig.KbID, err)
			return fmt.Errorf("import labels failed, oldKbIDStr: %s, newKbIDStr: %d, err: %w", oldKbIDStr, importConfig.KbID, err)
		}
		logx.I(ctx, "task(ImportKBPackage) Process ImportLabels success, oldKbIDStr: %s, newKbIDStr: %d", oldKbIDStr, importConfig.KbID)

		// 4.3 导入文档数据
		importConfig.LocalPath = filepath.Join(kbExtractPath, kb_package.ExportDirDocument)
		if err := d.docLogic.ImportDocuments(ctx, importConfig); err != nil {
			logx.E(ctx, "task(ImportKBPackage) Process ImportDocuments failed, oldKbIDStr: %s, newKbIDStr: %d, err: %+v", oldKbIDStr, importConfig.KbID, err)
			return fmt.Errorf("import documents failed, oldKbIDStr: %s, newKbIDStr: %d, err: %w", oldKbIDStr, importConfig.KbID, err)
		}
		logx.I(ctx, "task(ImportKBPackage) Process ImportDocuments success, oldKbIDStr: %s, newKbIDStr: %d", oldKbIDStr, importConfig.KbID)

		// 4.4 导入问答数据
		importConfig.LocalPath = filepath.Join(kbExtractPath, kb_package.ExportDirQA)
		if err := d.qaLogic.ImportQAs(ctx, importConfig); err != nil {
			logx.E(ctx, "task(ImportKBPackage) Process ImportQAs failed, oldKbIDStr: %s, newKbIDStr: %d, err: %+v", oldKbIDStr, importConfig.KbID, err)
			return fmt.Errorf("import qas failed, oldKbIDStr: %s, newKbIDStr: %d, err: %w", oldKbIDStr, importConfig.KbID, err)
		}
		logx.I(ctx, "task(ImportKBPackage) Process ImportQAs success, oldKbIDStr: %s, newKbIDStr: %d", oldKbIDStr, importConfig.KbID)

		// 4.5 导入schema数据
		importConfig.LocalPath = filepath.Join(kbExtractPath, kb_package.ExportDirSchema)
		if err := d.kbLogic.ImportKnowledgeSchemas(ctx, importConfig); err != nil {
			logx.E(ctx, "task(ImportKBPackage) Process ImportKnowledgeSchemas failed, oldKbIDStr: %s, newKbIDStr: %d, err: %+v", oldKbIDStr, importConfig.KbID, err)
			// todo cooper schema先不报错，后续再处理
			//return fmt.Errorf("import schema failed, oldKbIDStr: %s, newKbIDStr: %d, err: %w", oldKbIDStr, importConfig.KbID, err)
		}
		logx.I(ctx, "task(ImportKBPackage) Process ImportKnowledgeSchemas success, oldKbIDStr: %s, newKbIDStr: %d", oldKbIDStr, importConfig.KbID)

		// 5. 导入成功，清理解压后的目录以节省空间
		if err := os.RemoveAll(kbExtractPath); err != nil {
			logx.W(ctx, "task(ImportKBPackage) Process remove kb extract dir failed, oldKbIDStr: %s, path: %s, err: %+v", oldKbIDStr, importConfig.KbID, kbExtractPath, err)
		} else {
			logx.I(ctx, "task(ImportKBPackage) Process remove kb extract dir success, oldKbIDStr: %s", oldKbIDStr)
		}

		if err := progress.Finish(ctx, oldKbIDStr); err != nil {
			logx.E(ctx, "task(ImportKBPackage) Process import kb Finish oldKbIDStr:%s, newKbIDStr:%s, err:%+v", oldKbIDStr, importConfig.KbID, err)
			return fmt.Errorf("finish progress failed, oldKbIDStr: %s, newKbIDStr: %s, err: %w", oldKbIDStr, newKbIDStr, err)
		}
		successCount++
		logx.I(ctx, "task(ImportKBPackage) Process import kb completed, oldKbIDStr: %s, newKbIDStr: %s", oldKbIDStr, importConfig.KbID)
	}

	logx.I(ctx, "task(ImportKBPackage) Process completed, total: %d, success: %d",
		len(taskKV), successCount)

	return nil
}

// Fail 任务失败处理
func (d *ImportKBPackageTaskHandler) Fail(ctx context.Context) error {
	logx.E(ctx, "task(ImportKBPackage) Failed, task: %+v", d.task)
	// 1. 清理临时文件
	if err := os.RemoveAll(d.importRootPath); err != nil {
		logx.W(ctx, "task(ImportKBPackage) Fail remove temp dir failed, path: %s, err: %+v", d.importRootPath, err)
	}
	// 2. 更新任务状态为失败
	if err := d.callbackTaskStatus(ctx, false); err != nil {
		logx.E(ctx, "task(ImportKBPackage) Fail callback failed, err: %+v", err)
		return err
	}
	return nil
}

// Stop 任务停止处理
func (d *ImportKBPackageTaskHandler) Stop(ctx context.Context) error {
	logx.I(ctx, "task(ImportKBPackage) Stopped, task: %+v", d.task)

	// TODO: 实现停止处理逻辑
	// 1. 中断导入过程
	// 2. 清理资源
	// 3. 保存当前进度

	return nil
}

// Done 任务完成回调
func (d *ImportKBPackageTaskHandler) Done(ctx context.Context) error {
	logx.I(ctx, "task(ImportKBPackage) Done, task: %+v", d.task)

	// 1. 清理临时文件
	if err := os.RemoveAll(d.importRootPath); err != nil {
		logx.W(ctx, "task(ImportKBPackage) Done remove temp dir failed, path: %s, err: %+v", d.importRootPath, err)
	}
	// 2. 更新任务状态为完成
	if err := d.callbackTaskStatus(ctx, true); err != nil {
		logx.E(ctx, "task(ImportKBPackage) Done callback failed, err: %+v", err)
		return err
	}
	return nil
}

// unzipFile 解压zip文件到指定目录
// 如果zip文件只有一个顶层目录，自动跳过该目录，将内容直接解压到destPath
func (d *ImportKBPackageTaskHandler) unzipFile(ctx context.Context, zipFilePath, destPath string) error {
	logx.I(ctx, "unzipFile start, zipFile: %s, destPath: %s", zipFilePath, destPath)

	// 打开zip文件
	zipReader, err := zip.OpenReader(zipFilePath)
	if err != nil {
		logx.E(ctx, "unzipFile open zip failed, err: %v", err)
		return fmt.Errorf("open zip failed: %w", err)
	}
	defer zipReader.Close()

	// 创建目标目录
	if err := os.MkdirAll(destPath, 0755); err != nil {
		logx.E(ctx, "unzipFile mkdir failed, path: %s, err: %v", destPath, err)
		return fmt.Errorf("mkdir failed: %w", err)
	}

	// 检测是否需要跳过顶层目录
	stripPrefix := d.detectTopLevelDirectory(ctx, zipReader.File)
	if stripPrefix != "" {
		logx.I(ctx, "unzipFile detected single top-level directory, will strip prefix: %s", stripPrefix)
	}

	// 遍历zip文件中的所有文件
	for _, file := range zipReader.File {
		// 获取文件路径，如果需要跳过顶层目录，则去掉前缀
		filePath := file.Name
		if stripPrefix != "" {
			// 跳过顶层目录本身
			if filePath == stripPrefix || filePath == stripPrefix+"/" {
				continue
			}
			// 去掉顶层目录前缀
			if len(filePath) > len(stripPrefix) && filePath[:len(stripPrefix)+1] == stripPrefix+"/" {
				filePath = filePath[len(stripPrefix)+1:]
			} else {
				// 不在顶层目录下的文件，保持原样
				logx.W(ctx, "unzipFile file not under top-level directory: %s", filePath)
			}
		}

		// 构建目标文件路径
		targetPath := filepath.Join(destPath, filePath)

		// 如果是目录，创建目录
		if file.FileInfo().IsDir() {
			if err := os.MkdirAll(targetPath, file.Mode()); err != nil {
				logx.E(ctx, "unzipFile mkdir failed, path: %s, err: %v", targetPath, err)
				return fmt.Errorf("mkdir failed: %w", err)
			}
			continue
		}

		// 确保父目录存在
		if err := os.MkdirAll(filepath.Dir(targetPath), 0755); err != nil {
			logx.E(ctx, "unzipFile mkdir parent failed, path: %s, err: %v", filepath.Dir(targetPath), err)
			return fmt.Errorf("mkdir parent failed: %w", err)
		}

		// 打开zip中的文件
		srcFile, err := file.Open()
		if err != nil {
			logx.E(ctx, "unzipFile open file in zip failed, file: %s, err: %v", file.Name, err)
			return fmt.Errorf("open file in zip failed: %w", err)
		}

		// 创建目标文件
		destFile, err := os.OpenFile(targetPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, file.Mode())
		if err != nil {
			srcFile.Close()
			logx.E(ctx, "unzipFile create dest file failed, path: %s, err: %v", targetPath, err)
			return fmt.Errorf("create dest file failed: %w", err)
		}

		// 复制文件内容
		_, err = io.Copy(destFile, srcFile)
		srcFile.Close()
		destFile.Close()

		if err != nil {
			logx.E(ctx, "unzipFile copy file failed, file: %s, err: %v", file.Name, err)
			return fmt.Errorf("copy file failed: %w", err)
		}
	}

	logx.I(ctx, "unzipFile done, extracted %d files", len(zipReader.File))
	return nil
}

func (d *ImportKBPackageTaskHandler) prepareAppPackage(ctx context.Context) (task_scheduler.TaskKV, error) {
	kv := make(task_scheduler.TaskKV)

	// 1. 下载并加载ID映射配置
	if err := d.loadIDMapping(ctx); err != nil {
		return nil, err
	}
	// 2. 下载最外层的应用包 app_name_v202512311530_package.zip（使用流式下载，避免大文件内存问题）
	logx.I(ctx, "prepareAppPackage download app package, url: %s", d.p.ImportAppPackageURL)
	appPackageZipPath := filepath.Join(d.importRootPath, "app_package.zip")

	// 使用流式下载，直接写入文件，避免占用大量内存
	if err := d.downloadObjectToFile(ctx, d.p.ImportAppPackageURL, appPackageZipPath); err != nil {
		logx.E(ctx, "prepareAppPackage download app package failed, url: %s, err: %+v", d.p.ImportAppPackageURL, err)
		return nil, fmt.Errorf("download app package failed: %w", err)
	}

	// 获取文件大小用于日志
	fileInfo, err := os.Stat(appPackageZipPath)
	if err != nil {
		logx.W(ctx, "prepareAppPackage stat app package failed, path: %s, err: %+v", appPackageZipPath, err)
	} else {
		logx.I(ctx, "prepareAppPackage save app package success, path: %s, size: %d bytes", appPackageZipPath, fileInfo.Size())
	}

	// 3. 解压最外层应用包，得到 app_name_v202512311530_package/ 目录
	appPackageExtractPath := filepath.Join(d.importRootPath, "app_package")
	if err := d.unzipFile(ctx, appPackageZipPath, appPackageExtractPath); err != nil {
		logx.E(ctx, "prepareAppPackage unzip app package failed, err: %+v", err)
		return nil, fmt.Errorf("unzip app package failed: %w", err)
	}
	logx.I(ctx, "prepareAppPackage unzip app package success, path: %s", appPackageExtractPath)

	// 4. 处理应用包根目录的 metadata.json（应用包元数据）
	if err := d.processAppPackageMetadata(ctx, appPackageExtractPath); err != nil {
		logx.E(ctx, "prepareAppPackage process app package metadata failed, err: %+v", err)
		return nil, fmt.Errorf("process app metadata failed: %w", err)
	}

	// 5. 处理 app.zip（应用配置包）
	if err := d.processAppZip(ctx, appPackageExtractPath); err != nil {
		logx.E(ctx, "prepareAppPackage process app.zip failed, err: %+v", err)
		return nil, fmt.Errorf("process app.zip failed: %w", err)
	}

	// 6. 判断是否有知识库组件需要导入
	if d.appPackageMetadata == nil ||
		d.appPackageMetadata.Components.KnowledgeBases == nil ||
		len(d.appPackageMetadata.Components.KnowledgeBases.Items) == 0 {
		logx.I(ctx, "prepareAppPackage no knowledge bases to import, skip knowledge_bases.zip processing")
		return kv, nil
	}

	// 7. 处理 knowledge_bases.zip（知识库包）并构建导入列表
	if err := d.processKnowledgeBasesZip(ctx, appPackageExtractPath, kv); err != nil {
		logx.E(ctx, "prepareAppPackage process knowledge_bases.zip failed, err: %+v", err)
		return nil, fmt.Errorf("process knowledge_bases.zip failed: %w", err)
	}

	// 7. 设置任务KV
	logx.I(ctx, "prepareAppPackage completed, kv: %+v", jsonx.MustMarshalToString(kv))
	return kv, nil
}

// loadIDMapping 下载并加载ID映射配置
func (d *ImportKBPackageTaskHandler) loadIDMapping(ctx context.Context) error {
	// 如果没有提供ID映射URL，跳过加载
	if d.p.IdMappingCosUrl == "" {
		logx.I(ctx, "loadIDMapping skip, no id mapping url provided")
		return nil
	}

	logx.I(ctx, "loadIDMapping start, url: %s", d.p.IdMappingCosUrl)

	// 1. 从COS下载ID映射文件
	idMappingData, err := d.s3.GetObject(ctx, d.p.IdMappingCosUrl)
	if err != nil {
		logx.E(ctx, "loadIDMapping download failed, url: %s, err: %+v", d.p.IdMappingCosUrl, err)
		return fmt.Errorf("download id mapping file failed: %w", err)
	}
	logx.I(ctx, "loadIDMapping download success, size: %d bytes", len(idMappingData))

	// 2. 解析ID映射文件，格式为: {"ModuleName": {"oldID": "newID"}}
	var rawMapping map[string]map[string]string
	if err := json.Unmarshal(idMappingData, &rawMapping); err != nil {
		logx.E(ctx, "loadIDMapping unmarshal failed, err: %+v", err)
		return fmt.Errorf("unmarshal id mapping failed: %w", err)
	}
	logx.I(ctx, "loadIDMapping unmarshal success, modules count: %d", len(rawMapping))

	// 3. 转换为IDMappingConfig结构
	if err := d.parseIDMapping(ctx, rawMapping); err != nil {
		logx.E(ctx, "loadIDMapping parse failed, err: %+v", err)
		return fmt.Errorf("parse id mapping failed: %w", err)
	}

	logx.I(ctx, "loadIDMapping completed, total modules: %d", len(d.idMap.Modules))
	return nil
}

// parseIDMapping 解析ID映射数据
// rawMapping 格式: {"ModuleName": {"oldID": "newID"}}
// 转换为 IDMappingConfig 结构
func (d *ImportKBPackageTaskHandler) parseIDMapping(ctx context.Context, rawMapping map[string]map[string]string) error {
	logx.I(ctx, "parseIDMapping start, modules count: %d", len(rawMapping))

	// 初始化IDMappingConfig
	d.idMap.Modules = make(map[kb_package.ModuleType]map[string]kb_package.MappedID)

	// 遍历每个模块
	for moduleName, idMapping := range rawMapping {
		moduleType := kb_package.ModuleType(moduleName)
		logx.I(ctx, "parseIDMapping process module: %s, id count: %d", moduleName, len(idMapping))

		// 初始化该模块的映射表
		d.idMap.Modules[moduleType] = make(map[string]kb_package.MappedID)

		// 遍历该模块下的ID映射
		for oldID, newID := range idMapping {
			// 直接将newID填充到BizID，PrimaryID保持为0（空值）
			mappedID := kb_package.MappedID{
				BizID: newID,
			}

			d.idMap.Modules[moduleType][oldID] = mappedID
			logx.D(ctx, "parseIDMapping add mapping, module: %s, oldID: %s, newBizID: %s",
				moduleName, oldID, newID)
		}
	}

	logx.I(ctx, "parseIDMapping completed, total modules: %d", len(d.idMap.Modules))
	return nil
}

// detectTopLevelDirectory 检测zip文件是否只有一个顶层目录
// 如果是，返回该目录名；否则返回空字符串
func (d *ImportKBPackageTaskHandler) detectTopLevelDirectory(ctx context.Context, files []*zip.File) string {
	if len(files) == 0 {
		return ""
	}

	topLevelEntries := make(map[string]bool)

	// 遍历所有文件，找出所有顶层条目
	for _, file := range files {
		// 将路径标准化为使用正斜杠
		normalizedPath := filepath.ToSlash(file.Name)

		// 跳过空路径
		if normalizedPath == "" {
			continue
		}

		// 获取第一个路径分隔符的位置
		slashIdx := -1
		for i, c := range normalizedPath {
			if c == '/' {
				slashIdx = i
				break
			}
		}

		var topLevelEntry string
		if slashIdx > 0 {
			// 有子路径，取第一部分作为顶层条目
			topLevelEntry = normalizedPath[:slashIdx]
		} else if slashIdx == -1 {
			// 没有斜杠，整个路径就是顶层条目
			topLevelEntry = normalizedPath
		} else {
			// 以斜杠开头，跳过
			continue
		}

		topLevelEntries[topLevelEntry] = true
	}

	// 如果只有一个顶层条目，返回该条目名
	if len(topLevelEntries) == 1 {
		for entry := range topLevelEntries {
			logx.I(ctx, "detectTopLevelDirectory found single top-level entry: %s", entry)
			return entry
		}
	}

	logx.I(ctx, "detectTopLevelDirectory found %d top-level entries, will not strip", len(topLevelEntries))
	return ""
}

// processAppPackageMetadata 处理应用包根目录的 metadata.json
func (d *ImportKBPackageTaskHandler) processAppPackageMetadata(ctx context.Context, appPackageExtractPath string) error {
	metadataPath := filepath.Join(appPackageExtractPath, "metadata.json")

	// 检查metadata.json是否存在
	if _, err := os.Stat(metadataPath); err != nil {
		logx.W(ctx, "processAppPackageMetadata metadata.json not found, path: %s, err: %+v", metadataPath, err)
		return nil // metadata.json是可选的，不存在不报错
	}

	logx.I(ctx, "processAppPackageMetadata found metadata.json, path: %s", metadataPath)

	// 读取metadata.json
	metadataData, err := os.ReadFile(metadataPath)
	if err != nil {
		logx.E(ctx, "processAppPackageMetadata read metadata.json failed, path: %s, err: %+v", metadataPath, err)
		return fmt.Errorf("read metadata.json failed: %w", err)
	}

	// 解析metadata.json
	var metadata kb_package.AppPackageMetadata
	if err := json.Unmarshal(metadataData, &metadata); err != nil {
		logx.E(ctx, "processAppPackageMetadata unmarshal metadata.json failed, err: %+v", err)
		return fmt.Errorf("unmarshal metadata.json failed: %w", err)
	}

	// 存储到结构体中
	d.appPackageMetadata = &metadata

	logx.I(ctx, "processAppPackageMetadata parse metadata.json success, packageId: %s, version: %s, packageType: %s, exportedAt: %s, exportedBy: %s",
		metadata.PackageId, metadata.Version, metadata.PackageType, metadata.ExtraInfo.ExportedAt, metadata.ExtraInfo.ExportedBy)
	// TODO: 根据需要使用metadata信息
	// 例如：校验版本兼容性、记录导入信息等

	return nil
}

// processAppZip 处理应用配置包 app.zip
func (d *ImportKBPackageTaskHandler) processAppZip(ctx context.Context, appPackageExtractPath string) error {
	appZipPath := filepath.Join(appPackageExtractPath, "app.zip")

	// 检查app.zip是否存在
	if _, err := os.Stat(appZipPath); err != nil {
		logx.W(ctx, "processAppZip app.zip not found, path: %s, err: %+v", appZipPath, err)
		return nil // app.zip是可选的，不存在不报错
	}

	logx.I(ctx, "processAppZip found app.zip, path: %s", appZipPath)

	// 1. 解压app.zip到临时目录
	appExtractPath := filepath.Join(d.importRootPath, "app")
	if err := d.unzipFile(ctx, appZipPath, appExtractPath); err != nil {
		logx.E(ctx, "processAppZip unzip app.zip failed, err: %+v", err)
		return fmt.Errorf("unzip app.zip failed: %w", err)
	}
	logx.I(ctx, "processAppZip unzip app.zip success, path: %s", appExtractPath)

	// 2. 处理 app/metadata.json（应用配置元数据）
	if err := d.processAppMetadata(ctx, appExtractPath); err != nil {
		logx.E(ctx, "processAppZip process app metadata failed, err: %+v", err)
		return fmt.Errorf("process app metadata failed: %w", err)
	}

	// 3. 处理 kb_config/kb_refs.json（知识库引用配置）
	if err := d.processKBRefs(ctx, appExtractPath); err != nil {
		logx.E(ctx, "processAppZip process kb_refs.json failed, err: %+v", err)
		return fmt.Errorf("process kb_refs.json failed: %w", err)
	}
	logx.I(ctx, "processAppZip completed")
	return nil
}

// processAppMetadata 处理应用配置元数据 app/metadata.json
func (d *ImportKBPackageTaskHandler) processAppMetadata(ctx context.Context, appExtractPath string) error {
	metadataPath := filepath.Join(appExtractPath, "metadata.json")

	// 检查metadata.json是否存在
	if _, err := os.Stat(metadataPath); err != nil {
		logx.W(ctx, "processAppMetadata metadata.json not found, path: %s, err: %+v", metadataPath, err)
		return nil // metadata.json是可选的，不存在不报错
	}

	logx.I(ctx, "processAppMetadata found metadata.json, path: %s", metadataPath)

	// 1. 读取metadata.json
	metadataData, err := os.ReadFile(metadataPath)
	if err != nil {
		logx.E(ctx, "processAppMetadata read metadata.json failed, path: %s, err: %+v", metadataPath, err)
		return fmt.Errorf("read metadata.json failed: %w", err)
	}

	// 2. 解析metadata.json
	var metadata kb_package.AppMetadata
	if err := json.Unmarshal(metadataData, &metadata); err != nil {
		logx.E(ctx, "processAppMetadata unmarshal metadata.json failed, err: %+v", err)
		return fmt.Errorf("unmarshal metadata.json failed: %w", err)
	}

	logx.I(ctx, "processAppMetadata parse metadata.json success, appName: %s, appVersion: %s",
		metadata.AppName, metadata.AppVersion)

	// 3. 构建知识库信息映射表
	metadata.KbInfoMap = make(map[string]kb_package.AppKnowledgeBaseItem)
	if metadata.Modules.KnowledgeBase != nil {
		logx.I(ctx, "processAppMetadata knowledge base module, count: %d", len(metadata.Modules.KnowledgeBase.Items))
		for _, item := range metadata.Modules.KnowledgeBase.Items {
			metadata.KbInfoMap[item.KnowledgeBaseId] = item
			logx.I(ctx, "processAppMetadata kb item added to map, id: %s, name: %s, isShared: %v",
				item.KnowledgeBaseId, item.Name, item.IsShared)
		}
		logx.I(ctx, "processAppMetadata kb info map built, total: %d", len(metadata.KbInfoMap))
	} else {
		logx.I(ctx, "processAppMetadata no knowledge base module found")
	}

	// 存储到结构体中
	d.appMetadata = &metadata

	// TODO: 根据需要使用metadata信息
	// 例如：校验模块完整性、记录导入信息等

	logx.I(ctx, "processAppMetadata completed")
	return nil
}

// processKBRefs 处理知识库引用配置 kb_config/kb_refs.json
func (d *ImportKBPackageTaskHandler) processKBRefs(ctx context.Context, appExtractPath string) error {
	kbRefsPath := filepath.Join(appExtractPath, "kb_config", "kb_refs.json")

	// 检查kb_refs.json是否存在
	if _, err := os.Stat(kbRefsPath); err != nil {
		logx.W(ctx, "processKBRefs kb_refs.json not found, path: %s, err: %+v", kbRefsPath, err)
		return nil // kb_refs.json是可选的，不存在不报错
	}

	logx.I(ctx, "processKBRefs found kb_refs.json, path: %s", kbRefsPath)

	// 1. 读取kb_refs.json
	kbRefsData, err := os.ReadFile(kbRefsPath)
	if err != nil {
		logx.E(ctx, "processKBRefs read kb_refs.json failed, path: %s, err: %+v", kbRefsPath, err)
		return fmt.Errorf("read kb_refs.json failed: %w", err)
	}

	// 2. 解析kb_refs.json
	var kbConfig kb_package.KBConfigExport
	if err := json.Unmarshal(kbRefsData, &kbConfig); err != nil {
		logx.E(ctx, "processKBRefs unmarshal kb_refs.json failed, err: %+v", err)
		return fmt.Errorf("unmarshal kb_refs.json failed: %w", err)
	}

	logx.I(ctx, "processKBRefs parse kb_refs.json success, kb count: %d", len(kbConfig.KnowledgeBases))

	// 3. 根据ID映射确认需要导入的知识库
	needImportKbs := make([]kb_package.KBConfigItem, 0, len(kbConfig.KnowledgeBases))
	for _, kbItem := range kbConfig.KnowledgeBases {
		oldKbID := kbItem.KnowledgeBaseId
		logx.I(ctx, "processKBRefs process kb ref, oldKbID: %s", oldKbID)

		// 检查是否有ID映射
		if !d.idMap.IsMappedIDExist(kb_package.ModuleKb, oldKbID) {
			logx.W(ctx, "processKBRefs kb not in id mapping, skip, oldKbID: %s", oldKbID)
			continue
		}

		// 获取新的知识库ID
		mappedID := d.idMap.GetMappedID(kb_package.ModuleKb, oldKbID)
		newKbID := mappedID.BizID

		// 更新知识库ID
		kbItem.KnowledgeBaseId = newKbID
		kbItem.OldKnowledgeBaseId = oldKbID
		needImportKbs = append(needImportKbs, kbItem)
		logx.I(ctx, "processKBRefs update kb ref, oldKbID: %s, newKbID: %s", oldKbID, newKbID)
	}

	// 4. 导入知识库配置
	if len(needImportKbs) > 0 {
		if err := d.importKBConfigs(ctx, needImportKbs); err != nil {
			logx.E(ctx, "processKBRefs import kb configs failed, err: %+v", err)
			return fmt.Errorf("import kb configs failed: %w", err)
		}
		logx.I(ctx, "processKBRefs import kb configs success, count: %d", len(needImportKbs))
	} else {
		logx.I(ctx, "processKBRefs no kb refs to import")
	}

	logx.I(ctx, "processKBRefs completed, total: %d, updated: %d", len(kbConfig.KnowledgeBases), len(needImportKbs))
	return nil
}

// processKnowledgeBasesZip 处理知识库包 knowledge_bases.zip
func (d *ImportKBPackageTaskHandler) processKnowledgeBasesZip(ctx context.Context, appPackageExtractPath string, kv task_scheduler.TaskKV) error {
	knowledgeBasesZipPath := filepath.Join(appPackageExtractPath, "knowledge_bases.zip")

	// 检查knowledge_bases.zip是否存在
	if _, err := os.Stat(knowledgeBasesZipPath); err != nil {
		logx.E(ctx, "processKnowledgeBasesZip knowledge_bases.zip not found, path: %s, err: %+v", knowledgeBasesZipPath, err)
		return fmt.Errorf("knowledge_bases.zip not found: %w", err)
	}
	logx.I(ctx, "processKnowledgeBasesZip found knowledge_bases.zip, path: %s", knowledgeBasesZipPath)

	// 1. 解压 knowledge_bases.zip，得到 knowledge_bases/ 目录
	knowledgeBasesExtractPath := filepath.Join(d.importRootPath, "knowledge_bases")
	if err := d.unzipFile(ctx, knowledgeBasesZipPath, knowledgeBasesExtractPath); err != nil {
		logx.E(ctx, "processKnowledgeBasesZip unzip knowledge_bases.zip failed, err: %+v", err)
		return fmt.Errorf("unzip knowledge_bases.zip failed: %w", err)
	}
	logx.I(ctx, "processKnowledgeBasesZip unzip knowledge_bases.zip success, path: %s", knowledgeBasesExtractPath)

	// 2. 解析knowledge_bases目录的metadata.json
	metadata, err := d.parseKnowledgeBasesMetadata(ctx, knowledgeBasesExtractPath)
	if err != nil {
		return err
	}

	// 3. 校验知识库包完整性并确定导入列表
	if err := d.buildImportList(ctx, knowledgeBasesExtractPath, metadata, kv); err != nil {
		return err
	}

	logx.I(ctx, "processKnowledgeBasesZip completed, import list count: %d", len(kv))
	return nil
}

// parseKnowledgeBasesMetadata 解析knowledge_bases目录的metadata.json
func (d *ImportKBPackageTaskHandler) parseKnowledgeBasesMetadata(ctx context.Context, knowledgeBasesExtractPath string) (*kb_package.KbPackageMetadata, error) {
	metadataPath := filepath.Join(knowledgeBasesExtractPath, "metadata.json")

	// 读取metadata.json
	metadataData, err := os.ReadFile(metadataPath)
	if err != nil {
		logx.E(ctx, "parseKnowledgeBasesMetadata read metadata failed, path: %s, err: %+v", metadataPath, err)
		return nil, fmt.Errorf("read metadata failed: %w", err)
	}

	// 解析metadata.json
	var metadata kb_package.KbPackageMetadata
	if err := json.Unmarshal(metadataData, &metadata); err != nil {
		logx.E(ctx, "parseKnowledgeBasesMetadata unmarshal metadata failed, err: %+v", err)
		return nil, fmt.Errorf("unmarshal metadata failed: %w", err)
	}

	// 将metadata.json中的ids补充到d.idMap中
	if metadata.ReferenceIds != nil {
		d.supplementIDsToMap(ctx, metadata.ReferenceIds)
	}
	logx.I(ctx, "parseKnowledgeBasesMetadata idMap: %s", jsonx.MustMarshalToString(d.idMap))
	logx.I(ctx, "parseKnowledgeBasesMetadata parse metadata success, kb count: %d", len(metadata.KnowledgeBases))
	return &metadata, nil
}

// buildImportList 校验知识库包完整性并构建导入列表
func (d *ImportKBPackageTaskHandler) buildImportList(ctx context.Context, knowledgeBasesExtractPath string, metadata *kb_package.KbPackageMetadata, kv task_scheduler.TaskKV) error {
	for _, kbInfo := range metadata.KnowledgeBases {
		logx.I(ctx, "buildImportList process kb, id: %s, name: %s", kbInfo.KnowledgeBaseId, kbInfo.Name)

		// 校验知识库包完整性（CRC64校验）
		if err := d.verifyKbPackageIntegrity(ctx, knowledgeBasesExtractPath, kbInfo); err != nil {
			logx.E(ctx, "buildImportList verify kb package integrity failed, kbID: %s, err: %+v", kbInfo.KnowledgeBaseId, err)
			return err
		}

		// 结合idmap确定是否需要导入该知识库
		if !d.idMap.IsMappedIDExist(kb_package.ModuleKb, kbInfo.KnowledgeBaseId) {
			logx.W(ctx, "buildImportList kb not in id mapping, skip import, kbID: %s", kbInfo.KnowledgeBaseId)
			continue
		}

		mappedID := d.idMap.GetMappedID(kb_package.ModuleKb, kbInfo.KnowledgeBaseId)
		oldKbID := kbInfo.KnowledgeBaseId
		newKbID := mappedID.BizID

		// 如果新老ID一致，说明不需要导入，跳过
		if oldKbID == newKbID {
			logx.I(ctx, "buildImportList kb id unchanged, skip import, kbID: %s", oldKbID)
			continue
		}

		// 新老ID不一致，需要导入，添加到任务KV
		kv[oldKbID] = newKbID
		logx.I(ctx, "buildImportList add kb to import list, oldKbID: %s, newKbID: %s", oldKbID, newKbID)
	}

	logx.I(ctx, "buildImportList completed, total kbs: %d, import count: %d", len(metadata.KnowledgeBases), len(kv))
	return nil
}

// importKBConfigs 导入知识库配置
func (d *ImportKBPackageTaskHandler) importKBConfigs(ctx context.Context, kbConfigItems []kb_package.KBConfigItem) error {
	logx.I(ctx, "importKBConfigs start, count: %d", len(kbConfigItems))

	// 收集所有需要创建引用关系的共享知识库ID
	sharedKnowledgeBizIDs := make([]uint64, 0)
	// 记录每个知识库是否为共享知识库
	kbSharedMap := make(map[string]bool)

	// ========== 第一阶段：判断并创建所有共享知识库 ==========
	logx.I(ctx, "importKBConfigs Phase 1: create shared knowledge bases")
	for _, kbItem := range kbConfigItems {
		kbID := kbItem.KnowledgeBaseId
		oldKbID := kbItem.OldKnowledgeBaseId

		// 1. 判断是否为共享知识库
		isShared := false
		if d.appMetadata != nil && d.appMetadata.KbInfoMap != nil {
			if kbInfo, exists := d.appMetadata.KbInfoMap[oldKbID]; exists {
				isShared = kbInfo.IsShared
				logx.I(ctx, "importKBConfigs kb info from metadata, kbID: %s, name: %s, isShared: %v",
					kbID, kbInfo.Name, isShared)
			}
		}

		// 记录共享状态
		kbSharedMap[kbID] = isShared

		// 2. 如果是共享知识库，创建共享知识库并收集ID
		if isShared {
			knowledgeBizID, err := d.createSharedKBIfNeeded(ctx, kbID)
			if err != nil {
				logx.E(ctx, "importKBConfigs create shared kb failed, kbID: %s, err: %+v", kbID, err)
				return fmt.Errorf("create shared kb failed for kbID %s: %w", kbID, err)
			}
			sharedKnowledgeBizIDs = append(sharedKnowledgeBizIDs, knowledgeBizID)
			logx.I(ctx, "importKBConfigs Phase 1: created shared kb, kbID: %s, knowledgeBizID: %d", kbID, knowledgeBizID)
		}
	}
	logx.I(ctx, "importKBConfigs Phase 1 completed, created %d shared kbs", len(sharedKnowledgeBizIDs))

	// ========== 第二阶段：统一创建应用与所有共享知识库的引用关系 ==========
	logx.I(ctx, "importKBConfigs Phase 2: create shared kb references")
	if len(sharedKnowledgeBizIDs) > 0 {
		if err := d.createSharedKBReferences(ctx, sharedKnowledgeBizIDs); err != nil {
			logx.E(ctx, "importKBConfigs create shared kb references failed, err: %+v", err)
			return fmt.Errorf("create shared kb references failed: %w", err)
		}
		logx.I(ctx, "importKBConfigs Phase 2 completed, created references for %d shared kbs", len(sharedKnowledgeBizIDs))
	} else {
		logx.I(ctx, "importKBConfigs Phase 2 skipped, no shared kbs to reference")
	}

	// ========== 第三阶段：导入每个知识库的配置 ==========
	logx.I(ctx, "importKBConfigs Phase 3: import kb configs")
	for _, kbItem := range kbConfigItems {
		kbID := kbItem.KnowledgeBaseId
		isShared := kbSharedMap[kbID]

		// 导入知识库配置
		if err := d.importKBConfigItem(ctx, kbItem, isShared); err != nil {
			logx.E(ctx, "importKBConfigs import kb config failed, kbID: %s, err: %+v", kbID, err)
			return fmt.Errorf("import kb config failed for kbID %s: %w", kbID, err)
		}

		logx.I(ctx, "importKBConfigs Phase 3: imported kb config, kbID: %s, isShared: %v", kbID, isShared)
	}
	logx.I(ctx, "importKBConfigs Phase 3 completed, imported %d kb configs", len(kbConfigItems))

	logx.I(ctx, "importKBConfigs completed successfully, total kbs: %d, shared kbs: %d", len(kbConfigItems), len(sharedKnowledgeBizIDs))
	return nil
}

// importKBConfigItem 导入单个知识库的配置项
func (d *ImportKBPackageTaskHandler) importKBConfigItem(ctx context.Context, kbItem kb_package.KBConfigItem, isShared bool) error {
	kbID, err := strconv.ParseUint(kbItem.KnowledgeBaseId, 10, 64)
	if err != nil {
		logx.E(ctx, "importKBConfigItem parse kbID failed, kbID: %s, err: %+v", kbItem.KnowledgeBaseId, err)
		return fmt.Errorf("parse kbID failed: %w", err)
	}

	logx.I(ctx, "importKBConfigItem start, kbID: %d, isShared: %v", kbID, isShared)

	// 导入模型配置 - 使用配置映射来简化代码
	if kbItem.ModelConfig != nil {
		// 定义模型配置映射：配置类型 -> (模型信息, 错误描述)
		// 注意：modelInfo 使用 interface{} 类型以支持不同的结构体
		modelConfigs := []struct {
			configType pb.KnowledgeBaseConfigType
			modelInfo  interface{}
			errDesc    string
		}{
			{pb.KnowledgeBaseConfigType_EMBEDDING_MODEL, kbItem.ModelConfig.EmbeddingModel, "embedding model"},
			{pb.KnowledgeBaseConfigType_QA_EXTRACT_MODEL, kbItem.ModelConfig.QaExtractModel, "qa extract model"},
			{pb.KnowledgeBaseConfigType_KNOWLEDGE_SCHEMA_MODEL, kbItem.ModelConfig.KnowledgeSchemaModel, "knowledge schema model"},
			{pb.KnowledgeBaseConfigType_FILE_PARSE_MODEL, kbItem.ModelConfig.FileParseModel, "file parse model"},
		}

		// 遍历配置映射，导入非空的模型配置
		for _, cfg := range modelConfigs {
			if cfg.modelInfo != nil {
				if err := d.importModelConfig(ctx, kbID, cfg.configType, cfg.modelInfo, isShared); err != nil {
					logx.E(ctx, "import %s config failed, kbID: %d, err: %+v", cfg.errDesc, kbID, err)
					continue
				}
			}
		}
	}

	// 导入检索配置
	if kbItem.RetrievalConfig != nil {
		if err := d.importRetrievalConfig(ctx, kbID, kbItem.RetrievalConfig, kbItem.ModelConfig); err != nil {
			return fmt.Errorf("import retrieval config failed: %w", err)
		}
	}

	logx.I(ctx, "importKBConfigItem completed, kbID: %d", kbID)
	return nil
}

// convertModelInfoToPB 将模型信息转换为对应的pb结构体
// 支持 kb_package.ModelInfo 和 kb_package.FileParseModelInfo 两种输入类型
func (d *ImportKBPackageTaskHandler) convertModelInfoToPB(ctx context.Context, modelInfo interface{}, configType pb.KnowledgeBaseConfigType) (interface{}, error) {
	if modelInfo == nil {
		return nil, fmt.Errorf("modelInfo is nil")
	}

	// 注意：kb_package.ModelInfo.ModelParams 已经是 *common.ModelParams 类型，可以直接使用
	// 根据配置类型转换为对应的pb结构体
	switch configType {
	case pb.KnowledgeBaseConfigType_EMBEDDING_MODEL:
		// 转换为pb.EmbeddingModel（只有ModelName和AliasName）
		info, ok := modelInfo.(*kb_package.ModelInfo)
		if !ok {
			return nil, fmt.Errorf("invalid model info type for EMBEDDING_MODEL")
		}
		// 检查指针是否为 nil
		if info == nil {
			return nil, fmt.Errorf("EMBEDDING_MODEL info is nil")
		}
		return &pb.EmbeddingModel{
			ModelName: info.ModelName,
			AliasName: info.ModelAliasName,
		}, nil

	case pb.KnowledgeBaseConfigType_QA_EXTRACT_MODEL:
		// 转换为pb.QaExtractModel（包含ModelParams）
		info, ok := modelInfo.(*kb_package.ModelInfo)
		if !ok {
			return nil, fmt.Errorf("invalid model info type for QA_EXTRACT_MODEL")
		}
		// 检查指针是否为 nil
		if info == nil {
			return nil, fmt.Errorf("QA_EXTRACT_MODEL info is nil")
		}
		return &pb.QaExtractModel{
			ModelName:   info.ModelName,
			AliasName:   info.ModelAliasName,
			ModelParams: kb_package.ConvertFromLocalModelParams(info.ModelParams),
		}, nil

	case pb.KnowledgeBaseConfigType_KNOWLEDGE_SCHEMA_MODEL:
		// 转换为pb.KnowledgeSchemaModel（包含ModelParams）
		info, ok := modelInfo.(*kb_package.ModelInfo)
		if !ok {
			return nil, fmt.Errorf("invalid model info type for KNOWLEDGE_SCHEMA_MODEL")
		}
		// 检查指针是否为 nil
		if info == nil {
			return nil, fmt.Errorf("KNOWLEDGE_SCHEMA_MODEL info is nil")
		}
		return &pb.KnowledgeSchemaModel{
			ModelName:   info.ModelName,
			AliasName:   info.ModelAliasName,
			ModelParams: kb_package.ConvertFromLocalModelParams(info.ModelParams),
		}, nil

	case pb.KnowledgeBaseConfigType_FILE_PARSE_MODEL:
		// 转换为common.FileParseModel（使用不同的结构体）
		info, ok := modelInfo.(*kb_package.FileParseModelInfo)
		if !ok {
			return nil, fmt.Errorf("invalid model info type for FILE_PARSE_MODEL")
		}
		// 检查指针是否为 nil，防止 nil pointer dereference panic
		if info == nil {
			return nil, fmt.Errorf("FILE_PARSE_MODEL info is nil")
		}
		// 创建新的 bool 变量并返回指针，避免直接取结构体字段地址导致的指针失效问题
		formulaEnhancement := info.FormulaEnhancement
		largeLanguageModelEnhancement := info.LargeLanguageModelEnhancement
		outputHtmlTable := info.OutputHtmlTable
		return &common.FileParseModel{
			ModelName:                     info.ModelName,
			AliasName:                     info.ModelAliasName,
			FormulaEnhancement:            &formulaEnhancement,
			LargeLanguageModelEnhancement: &largeLanguageModelEnhancement,
			OutputHtmlTable:               &outputHtmlTable,
		}, nil

	default:
		return nil, fmt.Errorf("unsupported config type: %s", configType.String())
	}
}

// importModelConfig 导入模型配置（统一处理不同类型的模型）
// 流程：kb_package.ModelInfo/FileParseModelInfo(大驼峰) -> pb结构体 -> DB小驼峰JSON
// 支持 ModelInfo 和 FileParseModelInfo 两种输入类型
func (d *ImportKBPackageTaskHandler) importModelConfig(ctx context.Context, kbID uint64, configType pb.KnowledgeBaseConfigType, modelInfo interface{}, isShared bool) error {
	logx.I(ctx, "importModelConfig start, kbID: %d, configType: %s, isShared: %v", kbID, configType.String(), isShared)

	// 将输入转换为对应的pb结构体
	pbModel, err := d.convertModelInfoToPB(ctx, modelInfo, configType)
	if err != nil {
		logx.E(ctx, "importModelConfig convert to pb model failed, kbID: %d, configType: %s, err: %+v", kbID, configType.String(), err)
		return fmt.Errorf("convert to pb model failed: %w", err)
	}

	// 使用pb结构体序列化为JSON（pb的json tag是小驼峰，保证与DB格式一致）
	configJSON, err := jsonx.Marshal(pbModel)
	if err != nil {
		logx.E(ctx, "importModelConfig marshal pb model failed, kbID: %d, configType: %s, err: %+v", kbID, configType.String(), err)
		return fmt.Errorf("marshal pb model failed: %w", err)
	}

	logx.I(ctx, "importModelConfig converted to pb and marshaled, kbID: %d, configType: %s, json: %s", kbID, configType.String(), string(configJSON))

	// 构建KnowledgeConfig
	knowledgeConfig := &kbe.KnowledgeConfig{
		CorpBizID:      d.p.CorpBizID,
		KnowledgeBizID: kbID,
		AppBizID:       d.p.AppBizID,
		Type:           uint32(configType),
		IsDeleted:      false,
		UpdateTime:     time.Now(),
	}

	// 根据是否为共享知识库，填充不同的配置字段
	if isShared {
		// 共享知识库只填 Config
		knowledgeConfig.Config = string(configJSON)
		knowledgeConfig.AppBizID = 0
	} else {
		// 默认知识库只填 PreviewConfig
		knowledgeConfig.PreviewConfig = string(configJSON)
	}

	// 调用logic层保存配置
	if err := d.kbLogic.SetKnowledgeBaseConfig(ctx, knowledgeConfig.CorpBizID, []*kbe.KnowledgeConfig{knowledgeConfig}); err != nil {
		logx.E(ctx, "importModelConfig set knowledge config failed, kbID: %d, configType: %s, err: %+v", kbID, configType.String(), err)
		return fmt.Errorf("set knowledge config failed: %w", err)
	}

	logx.I(ctx, "importModelConfig completed, kbID: %d, configType: %s, isShared: %v", kbID, configType.String(), isShared)
	return nil
}

// importFileParseModelConfig 导入文件解析模型配置
// 流程：kb_package.FileParseModelInfo(大驼峰) -> common.FileParseModel -> DB小驼峰JSON
// importRetrievalConfig 导入检索配置
func (d *ImportKBPackageTaskHandler) importRetrievalConfig(ctx context.Context, kbID uint64, retrievalConfig *kb_package.RetrievalConfigData, modelConfig *kb_package.ModelConfig) error {
	logx.I(ctx, "importRetrievalConfig start, kbID: %d", kbID)

	// 构建pb.RetrievalConfig结构体
	pbConfig := &pb.RetrievalConfig{}

	// 转换retrievals - 使用pb.RetrievalInfo
	if len(retrievalConfig.Filters) > 0 {
		pbConfig.Retrievals = make([]*pb.RetrievalInfo, 0, len(retrievalConfig.Filters))
		for _, filter := range retrievalConfig.Filters {
			pbConfig.Retrievals = append(pbConfig.Retrievals, &pb.RetrievalInfo{
				RetrievalType: common.KnowledgeType(common.KnowledgeType_value[filter.RetrievalType]),
				IndexId:       uint32(filter.IndexId),
				Confidence:    float32(filter.Confidence),
				TopN:          uint32(filter.TopN),
				IsEnable:      filter.IsEnable,
			})
		}
	}

	// 转换search_strategy - 使用pb.SearchStrategy
	if retrievalConfig.SearchStrategy != nil {
		pbConfig.SearchStrategy = &pb.SearchStrategy{
			RerankModelSwitch: retrievalConfig.SearchStrategy.RerankModelSwitch,
			RerankModel:       retrievalConfig.SearchStrategy.RerankModel,
			TableEnhancement:  retrievalConfig.SearchStrategy.EnableTableEnhancement,
			StrategyType:      pb.SearchStrategyTypeEnum(pb.SearchStrategyTypeEnum_value[retrievalConfig.SearchStrategy.StrategyType]),
		}

		// 如果有NaturalLanguageToSqlModel，转换为pb结构体
		if modelConfig != nil && modelConfig.NaturalLanguageToSqlModel != nil {
			pbConfig.SearchStrategy.NatureLanguageToSqlModelConfig = &pb.NL2SQLModelConfig{
				Model: &common.AppModelDetailInfo{
					ModelName:   modelConfig.NaturalLanguageToSqlModel.ModelName,
					AliasName:   modelConfig.NaturalLanguageToSqlModel.ModelAliasName,
					ModelParams: kb_package.ConvertFromLocalModelParams(modelConfig.NaturalLanguageToSqlModel.ModelParams),
				},
			}
		}
	}

	// 转换retrieval_range - 使用pb.RetrievalRange
	if retrievalConfig.RetrievalRange != nil && len(retrievalConfig.RetrievalRange.ApiVarAttrInfos) > 0 {
		pbConfig.RetrievalRange = &pb.RetrievalRange{
			Condition: retrievalConfig.RetrievalRange.Condition,
		}
		apiVarAttrInfos := make([]*pb.RetrievalRange_ApiVarAttrInfo, 0, len(retrievalConfig.RetrievalRange.ApiVarAttrInfos))
		for _, info := range retrievalConfig.RetrievalRange.ApiVarAttrInfos {
			apiVarAttrInfos = append(apiVarAttrInfos, &pb.RetrievalRange_ApiVarAttrInfo{
				ApiVarId:  d.idMap.GetMappedID(kb_package.ModuleAppVariable, info.ApiVarId).BizID,
				AttrBizId: cast.ToUint64(d.idMap.GetMappedID(kb_package.ModuleKbLabel, cast.ToString(info.KnowledgeItemLabelId)).BizID),
			})
		}
		pbConfig.RetrievalRange.ApiVarAttrInfos = apiVarAttrInfos
	}

	// 使用jsonx序列化（pb结构体的json tag是小驼峰）
	configJSON, err := jsonx.Marshal(pbConfig)
	if err != nil {
		logx.E(ctx, "importRetrievalConfig marshal retrieval config failed, kbID: %d, err: %+v", kbID, err)
		return fmt.Errorf("marshal retrieval config failed: %w", err)
	}

	logx.I(ctx, "importRetrievalConfig config: %s", string(configJSON))

	// 构建KnowledgeConfig
	knowledgeConfig := &kbe.KnowledgeConfig{
		CorpBizID:      d.p.CorpBizID,
		KnowledgeBizID: kbID,
		AppBizID:       d.p.AppBizID,
		Type:           uint32(pb.KnowledgeBaseConfigType_RETRIEVAL_SETTING),
		IsDeleted:      false,
		UpdateTime:     time.Now(),
	}
	knowledgeConfig.PreviewConfig = string(configJSON)

	// 调用logic层保存配置
	if err := d.kbLogic.SetKnowledgeBaseConfig(ctx, knowledgeConfig.CorpBizID, []*kbe.KnowledgeConfig{knowledgeConfig}); err != nil {
		logx.E(ctx, "importRetrievalConfig set knowledge config failed, kbID: %d, err: %+v", kbID, err)
		return fmt.Errorf("set knowledge config failed: %w", err)
	}

	logx.I(ctx, "importRetrievalConfig completed, kbID: %d", kbID)
	return nil
}

// abc
// abc的副本01，abc的副本02，abc的副本03

// findAvailableKbName 查找可用的知识库名称
// 如果原名称可用则返回原名称，否则返回"原名称+的副本01/02/..."格式的名称
// 优化：批量查询所有候选名称，避免多次数据库查询
func (d *ImportKBPackageTaskHandler) findAvailableKbName(ctx context.Context, baseName string) (string, error) {
	const maxKbNameLength = 30
	const copySuffix = "的副本"
	const maxCopyNumber = 30 // 最多尝试30个副本

	// 1. 生成所有候选名称列表（包括原名称和所有副本名称）
	candidateNames := make([]string, 0, maxCopyNumber+1)
	candidateNames = append(candidateNames, baseName) // 先添加原名称

	// 检查名称长度，如果"原名称+的副本01"超过限制，则需要截断原名称
	testName := fmt.Sprintf("%s%s%02d", baseName, copySuffix, 1)
	if len([]rune(testName)) > maxKbNameLength {
		// 计算后缀长度（"的副本01"占5个字符）
		suffixLen := len([]rune(copySuffix)) + 2 // "的副本" + 2位数字
		maxBaseLen := maxKbNameLength - suffixLen
		if maxBaseLen > 0 {
			baseName = string([]rune(baseName)[:maxBaseLen])
		}
	}

	// 生成所有副本候选名称
	for i := 1; i <= maxCopyNumber; i++ {
		candidateName := fmt.Sprintf("%s%s%02d", baseName, copySuffix, i)
		candidateNames = append(candidateNames, candidateName)
	}

	// 2. 批量查询所有候选名称是否存在
	knowledgeList, err := d.kbDao.RetrieveSharedKnowledgeByName(ctx, d.p.CorpBizID, candidateNames, d.p.SpaceID)
	if err != nil {
		return "", errs.ErrSharedKnowledgeNameQueryFailed
	}

	// 3. 构建已存在的名称集合
	existingNames := make(map[string]bool, len(knowledgeList))
	for _, kb := range knowledgeList {
		existingNames[kb.Name] = true
	}

	// 4. 从候选名称列表中找到第一个不存在的名称
	for _, candidateName := range candidateNames {
		if !existingNames[candidateName] {
			logx.I(ctx, "findAvailableKbName found available name: %s", candidateName)
			return candidateName, nil
		}
	}

	// 5. 如果所有候选名称都已存在，拼接随机数生成唯一名称
	// 格式：原名称_随机6位数字
	randomSuffix := fmt.Sprintf("_%06d", idgen.GetId()%1000000)
	finalName := baseName + randomSuffix

	// 如果拼接后超过长度限制，截断baseName
	if len([]rune(finalName)) > maxKbNameLength {
		maxBaseLen := maxKbNameLength - len([]rune(randomSuffix))
		if maxBaseLen > 0 {
			finalName = string([]rune(baseName)[:maxBaseLen]) + randomSuffix
		}
	}

	logx.W(ctx, "findAvailableKbName all candidate names are taken, using random suffix: %s", finalName)
	return finalName, nil
}

// createSharedKBIfNeeded 创建共享知识库（如果需要）
// 返回知识库的业务ID
func (d *ImportKBPackageTaskHandler) createSharedKBIfNeeded(ctx context.Context, kbID string) (uint64, error) {
	logx.I(ctx, "createSharedKBIfNeeded start, kbID: %s", kbID)

	// 1. 从 appMetadata 获取知识库信息
	oldKbID := ""
	var kbInfo kb_package.AppKnowledgeBaseItem
	if d.appMetadata != nil && d.appMetadata.KbInfoMap != nil {
		// 查找旧的知识库ID
		for oldID, info := range d.appMetadata.KbInfoMap {
			mappedID := d.idMap.GetMappedID(kb_package.ModuleKb, oldID)
			if mappedID.BizID == kbID {
				oldKbID = oldID
				kbInfo = info
				break
			}
		}
	}

	if oldKbID == "" {
		logx.W(ctx, "createSharedKBIfNeeded kb info not found in metadata, kbID: %s", kbID)
		return 0, fmt.Errorf("kb info not found in metadata, kbID: %s", kbID)
	}

	logx.I(ctx, "createSharedKBIfNeeded found kb info, oldKbID: %s, newKbID: %s, name: %s",
		oldKbID, kbID, kbInfo.Name)

	// 2. 解析知识库ID
	kbBizID, err := strconv.ParseUint(kbID, 10, 64)
	if err != nil {
		logx.E(ctx, "createSharedKBIfNeeded parse kbID failed, kbID: %s, err: %+v", kbID, err)
		return 0, fmt.Errorf("parse kbID failed: %w", err)
	}

	// 3. 判断新老ID是否一致，决定是否需要创建共享知识库
	var knowledgeBizID uint64
	if oldKbID == kbID {
		// 新老ID一致，不需要创建共享知识库，直接使用现有的知识库ID
		knowledgeBizID = kbBizID
		logx.I(ctx, "createSharedKBIfNeeded kb id unchanged, skip create, kbID: %s", kbID)
	} else {
		// 新老ID不一致，需要创建共享知识库
		// 使用 findAvailableKbName 查找可用的知识库名称
		kbName, err := d.findAvailableKbName(ctx, kbInfo.Name)
		if err != nil {
			logx.E(ctx, "createSharedKBIfNeeded findAvailableKbName failed, baseName: %s, err: %+v", kbInfo.Name, err)
			return 0, fmt.Errorf("find available kb name failed: %w", err)
		}

		logx.I(ctx, "createSharedKBIfNeeded final kb name, original: %s, final: %s, length: %d",
			kbInfo.Name, kbName, len([]rune(kbName)))

		createParams := &kbe.CreateSharedKnowledgeRequest{
			Uin:                     d.p.Uin,
			CorpBizID:               d.p.CorpBizID,
			StaffPrimaryID:          d.p.StaffPrimaryID,
			StaffBizID:              d.p.StaffBizID,
			SpaceID:                 d.p.SpaceID,
			Name:                    kbName,
			Description:             "",      // 从metadata中没有description字段，使用空字符串
			EmbeddingModel:          "",      // 使用默认模型
			SharedKnowledgeAppBizID: kbBizID, // 使用已有的知识库应用ID
		}

		result, err := d.kbLogic.CreateSharedKnowledge(ctx, createParams)
		if err != nil {
			logx.E(ctx, "createSharedKBIfNeeded CreateSharedKnowledge failed, kbID: %s, err: %+v", kbID, err)
			return 0, fmt.Errorf("create shared knowledge failed: %w", err)
		}

		knowledgeBizID = result.KnowledgeBizID
		logx.I(ctx, "createSharedKBIfNeeded CreateSharedKnowledge success, kbID: %s, knowledgeBizID: %d",
			kbID, knowledgeBizID)
	}

	logx.I(ctx, "createSharedKBIfNeeded completed, kbID: %s, knowledgeBizID: %d", kbID, knowledgeBizID)
	return knowledgeBizID, nil
}

// createSharedKBReferences 统一创建应用与所有共享知识库的引用关系
func (d *ImportKBPackageTaskHandler) createSharedKBReferences(ctx context.Context, knowledgeBizIDs []uint64) error {
	logx.I(ctx, "createSharedKBReferences start, count: %d, kbIDs: %v", len(knowledgeBizIDs), knowledgeBizIDs)

	if len(knowledgeBizIDs) == 0 {
		logx.I(ctx, "createSharedKBReferences no shared kb to refer, skip")
		return nil
	}

	// 1. 获取应用信息
	appInfo, err := d.rpc.AppAdmin.GetAppBaseInfo(ctx, d.p.AppBizID)
	if err != nil {
		logx.E(ctx, "createSharedKBReferences GetAppBaseInfo failed, appBizID: %d, err: %+v", d.p.AppBizID, err)
		return fmt.Errorf("get app base info failed: %w", err)
	}

	// 2. 创建应用与所有共享知识库的引用关系
	referParams := &kbe.ReferShareKnowledgeRequest{
		AppBizID:        d.p.AppBizID,
		AppPrimaryID:    d.p.AppPrimaryID,
		KnowledgeBizIDs: knowledgeBizIDs,
		CorpPrimaryID:   d.p.CorpPrimaryID,
		CorpBizID:       d.p.CorpBizID,
		SpaceID:         appInfo.SpaceId,
		AppName:         appInfo.Name,
	}

	if err := d.kbLogic.ReferShareKnowledge(ctx, referParams); err != nil {
		logx.E(ctx, "createSharedKBReferences ReferShareKnowledge failed, kbIDs: %v, err: %+v", knowledgeBizIDs, err)
		return fmt.Errorf("refer share knowledge failed: %w", err)
	}

	logx.I(ctx, "createSharedKBReferences completed, created references for %d shared kbs", len(knowledgeBizIDs))
	return nil
}

// supplementIDsToMap 将metadata中的IDs补充到idMap中
// 只添加不存在的ID，已存在的不更新
func (d *ImportKBPackageTaskHandler) supplementIDsToMap(ctx context.Context, ids *kb_package.KbMetadataIds) {
	logx.I(ctx, "supplementIDsToMap start")

	// 定义ID类型与模块类型的映射关系
	idTypeMapping := []struct {
		moduleType kb_package.ModuleType
		items      []string
		typeName   string
	}{
		{kb_package.ModuleKbLabel, ids.KbLabel, "KbLabels"},
		{kb_package.ModuleKbLabelValue, ids.KbLabelValue, "KbLabelValues"},
		{kb_package.ModuleKbQaCategory, ids.KbQaCategory, "KbQaCategories"},
		{kb_package.ModuleKbDocCategory, ids.KbDocCategory, "KbDocCategories"},
		{kb_package.ModuleKbDoc, ids.KbDoc, "KbDocs"},
		{kb_package.ModuleKbSegment, ids.KbSegment, "KbSegments"},
		{kb_package.ModuleKbDocClusterSchema, ids.KbSchemaCluster, "KbSchemaClusters"},
		{kb_package.ModuleKbQa, ids.KbQa, "KbQas"},
	}

	// 遍历每种ID类型
	for _, mapping := range idTypeMapping {
		addedCount := 0
		for _, item := range mapping.items {
			if item == "" {
				continue
			}

			// 检查ID是否已存在
			if !d.idMap.IsMappedIDExist(mapping.moduleType, item) {
				// 不存在则添加，使用原ID作为映射（表示不需要转换）
				d.idMap.SetMappedID(mapping.moduleType, item, kb_package.MappedID{
					BizID: strconv.FormatUint(idgen.GetId(), 10), // 生成新的bizID
				})
				addedCount++
			}
		}

		if addedCount > 0 {
			logx.I(ctx, "supplementIDsToMap added %d IDs for module %s", addedCount, mapping.typeName)
		}
	}

	logx.I(ctx, "supplementIDsToMap completed")
}

// callbackTaskStatus 回调任务状态
func (d *ImportKBPackageTaskHandler) callbackTaskStatus(ctx context.Context, success bool) error {
	logx.I(ctx, "callbackTaskStatus start, taskID: %d, subTaskID: %d, success: %v",
		d.p.TaskID, d.p.SubTaskID, success)

	message := ""
	if !success {
		message = i18n.Translate(ctx, i18nkey.KeyImportKbPackageFailed)
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

// downloadObjectToFile 从S3流式下载对象到本地文件
// 使用 GetObjectToFile 分块下载，避免将整个文件加载到内存中，适用于大文件下载
func (d *ImportKBPackageTaskHandler) downloadObjectToFile(ctx context.Context, cosURL, destFilePath string) error {
	start := time.Now()
	logx.I(ctx, "downloadObjectToFile start, cosURL: %s, destFilePath: %s", cosURL, destFilePath)

	// 1. 确保目标目录存在
	destDir := filepath.Dir(destFilePath)
	if err := os.MkdirAll(destDir, 0755); err != nil {
		logx.E(ctx, "downloadObjectToFile mkdir failed, dir: %s, err: %+v", destDir, err)
		return fmt.Errorf("mkdir failed: %w", err)
	}

	// 2. 使用 GetObjectToFile 分块下载到文件，避免占用大量内存
	if err := d.s3.GetObjectToFile(ctx, cosURL, destFilePath); err != nil {
		logx.E(ctx, "downloadObjectToFile GetObjectToFile failed, cosURL: %s, destFilePath: %s, err: %+v", cosURL, destFilePath, err)
		return fmt.Errorf("get object to file failed: %w", err)
	}

	// 3. 获取文件大小用于日志
	fileInfo, err := os.Stat(destFilePath)
	if err != nil {
		logx.W(ctx, "downloadObjectToFile stat file failed, path: %s, err: %+v", destFilePath, err)
	} else {
		logx.I(ctx, "downloadObjectToFile completed, size: %d bytes, duration: %v", fileInfo.Size(), time.Since(start))
	}

	return nil
}

// verifyKbPackageIntegrity 校验知识库包完整性（CRC64校验）
func (d *ImportKBPackageTaskHandler) verifyKbPackageIntegrity(ctx context.Context, knowledgeBasesExtractPath string, kbInfo kb_package.KbExportInfo) error {
	logx.I(ctx, "verifyKbPackageIntegrity start, kbID: %s, expectedHash: %s", kbInfo.KnowledgeBaseId, kbInfo.Hash)

	// 1. 构建知识库zip文件路径
	kbZipPath := filepath.Join(knowledgeBasesExtractPath, fmt.Sprintf("kb_%s.zip", kbInfo.KnowledgeBaseId))

	// 2. 检查zip文件是否存在
	if _, err := os.Stat(kbZipPath); err != nil {
		logx.E(ctx, "verifyKbPackageIntegrity kb zip not found, kbID: %s, path: %s, err: %+v", kbInfo.KnowledgeBaseId, kbZipPath, err)
		return fmt.Errorf("kb zip not found, kbID: %s, path: %s, err: %w", kbInfo.KnowledgeBaseId, kbZipPath, err)
	}

	// 3. 计算zip文件的CRC64值
	actualCRC64, err := d.kbPKGLogic.CalculateFileCRC64(ctx, kbZipPath, config.App().KbPackageConfig.SecretKey)
	if err != nil {
		logx.E(ctx, "verifyKbPackageIntegrity calculate crc64 failed, kbID: %s, path: %s, err: %+v", kbInfo.KnowledgeBaseId, kbZipPath, err)
		return fmt.Errorf("calculate crc64 failed, kbID: %s, err: %w", kbInfo.KnowledgeBaseId, err)
	}

	// 4. 比对CRC64值
	if actualCRC64 != kbInfo.Hash {
		logx.E(ctx, "verifyKbPackageIntegrity crc64 mismatch, kbID: %s, expected: %s, actual: %s",
			kbInfo.KnowledgeBaseId, kbInfo.Hash, actualCRC64)
		return fmt.Errorf("kb package integrity check failed, kbID: %s, expected hash: %s, actual hash: %s",
			kbInfo.KnowledgeBaseId, kbInfo.Hash, actualCRC64)
	}

	logx.I(ctx, "verifyKbPackageIntegrity success, kbID: %s, hash: %s", kbInfo.KnowledgeBaseId, actualCRC64)
	return nil
}
