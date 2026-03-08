package service

import (
	"context"
	"fmt"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/knowledge_schema"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
	jsoniter "github.com/json-iterator/go"
	"path"
	"strings"

	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
)

// GenerateKnowledgeSchema 创建生成知识库schema任务
func (s *Service) GenerateKnowledgeSchema(ctx context.Context, req *pb.GenerateKnowledgeSchemaReq) (*pb.GenerateKnowledgeSchemaRsp, error) {
	log.InfoContextf(ctx, "GenerateKnowledgeSchema Req:%+v", req)
	rsp := new(pb.GenerateKnowledgeSchemaRsp)
	if req.GetAppBizId() == 0 {
		return rsp, errs.ErrParams
	}

	app, err := s.getAppByAppBizID(ctx, req.GetAppBizId())
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}

	corpID := pkg.CorpID(ctx)
	corpBizID := pkg.CorpBizID(ctx)

	err = knowledge_schema.GenerateKnowledgeSchema(ctx, s.dao, corpID, corpBizID, req.GetAppBizId(), s.dao.GenerateSeqID(), app.ID)
	if err != nil {
		log.ErrorContextf(ctx, "GenerateKnowledgeSchema knowledge_schema.GenerateKnowledgeSchema fail, err: %+v", err)
		return rsp, err
	}
	return rsp, nil
}

func (s *Service) GetKnowledgeSchemaTask(ctx context.Context, req *pb.GetKnowledgeSchemaTaskReq) (*pb.GetKnowledgeSchemaTaskRsp, error) {
	log.InfoContextf(ctx, "GetKnowledgeSchemaTask Req:%+v", req)
	rsp := new(pb.GetKnowledgeSchemaTaskRsp)
	if req.GetAppBizId() == 0 {
		return rsp, errs.ErrParams
	}

	resp, err := knowledge_schema.GetKnowledgeSchemaTask(ctx, req.GetAppBizId())
	if err != nil {
		log.ErrorContextf(ctx, "GetKnowledgeSchemaTask knowledge_schema.GetKnowledgeSchemaTask fail, err: %+v", err)
		return rsp, err
	}

	return resp, nil
}

func (s *Service) GetKnowledgeSchema(ctx context.Context, req *pb.GetKnowledgeSchemaReq) (*pb.GetKnowledgeSchemaRsp, error) {
	return GetKnowledgeSchema(ctx, req, s.dao)
}

func GetKnowledgeSchema(ctx context.Context, req *pb.GetKnowledgeSchemaReq, d dao.Dao) (*pb.GetKnowledgeSchemaRsp, error) {
	log.InfoContextf(ctx, "GetKnowledgeSchema Req:%+v", req)
	rsp := new(pb.GetKnowledgeSchemaRsp)
	if req.GetAppBizId() == 0 {
		return rsp, errs.ErrParams
	}
	if req.GetEnvType() == "" {
		return rsp, errs.ErrParams
	}
	if req.GetEnvType() != model.EnvTypeSandbox && req.GetEnvType() != model.EnvTypeProduct {
		return rsp, errs.ErrParams
	}
	app, err := client.GetAppInfo(ctx, req.GetAppBizId(), model.AppTestScenes)
	if err != nil {
		log.ErrorContextf(ctx, "GetKnowledgeSchema logicApp.GetAppByAppBizID fail, err=%+v", err)
		return rsp, errs.ErrAppNotFound
	}
	corpBizID := pkg.CorpBizID(ctx)
	if corpBizID == 0 {
		corpBizID = app.GetCorpBizId()
	}
	knowledgeSchemaList, err := knowledge_schema.GetKnowledgeSchema(ctx, d, corpBizID, req.GetAppBizId(), req.GetEnvType())
	if err != nil {
		log.ErrorContextf(ctx, "GetKnowledgeSchema knowledge_schema.GetKnowledgeSchema fail, err=%+v", err)
		return rsp, err
	}
	// 需要将结构化文件的schema信息展开
	commonFileSchemas := make([]*pb.GetKnowledgeSchemaRsp_SchemaItem, 0)
	structFileSchemas := make([]*pb.GetKnowledgeSchemaRsp_SchemaItem, 0)
	for _, schema := range knowledgeSchemaList {
		fileSuffix := path.Ext(schema.Name)
		if len(fileSuffix) < 2 {
			commonFileSchemas = append(commonFileSchemas, schema)
			continue
		}
		_, ok := model.StructFileTypeMap[fileSuffix[1:]]
		if ok && strings.HasPrefix(schema.Summary, "[{") && strings.HasSuffix(schema.Summary, "}]") {
			// 结构化文件的schema信息展开
			oneStructFileSchemas, err := getStructFileSchema(ctx, schema)
			if err != nil {
				log.ErrorContextf(ctx, "GetKnowledgeSchema getStructFileSchema fail, err=%+v", err)
				return rsp, err
			}
			if len(oneStructFileSchemas) != 0 {
				structFileSchemas = append(structFileSchemas, oneStructFileSchemas...)
			}
		} else {
			commonFileSchemas = append(commonFileSchemas, schema)
		}
	}

	rsp.Schemas = append(commonFileSchemas, structFileSchemas...)
	schemaNeedUpdate, err := knowledge_schema.GetKnowledgeSchemaNeedUpdate(ctx, d, corpBizID, app.GetId(), req.GetAppBizId())
	if err != nil {
		log.ErrorContextf(ctx, "GetKnowledgeSchema knowledge_schema.GetKnowledgeSchemaNeedUpdate fail, err=%+v", err)
		return rsp, err
	}
	rsp.SchemaNeedUpdate = schemaNeedUpdate
	return rsp, nil
}

// getStructFileSchema 将结构化文件的schema信息展开
func getStructFileSchema(ctx context.Context, schema *pb.GetKnowledgeSchemaRsp_SchemaItem) ([]*pb.GetKnowledgeSchemaRsp_SchemaItem, error) {
	structFileSchemas := make([]*pb.GetKnowledgeSchemaRsp_SchemaItem, 0)
	text2sqlMeta := make([]model.Text2sqlMetaMappingPreview, 0)
	err := jsoniter.Unmarshal([]byte(schema.Summary), &text2sqlMeta)
	if err != nil {
		log.ErrorContextf(ctx, "GetKnowledgeSchema json.Unmarshal err: %v", err)
		return nil, err
	}
	for _, text2sqlMetaItem := range text2sqlMeta {
		summary := ""
		text2sqlSummary := "{"
		tableInfo := &model.MappingData{}
		err = jsoniter.Unmarshal([]byte(text2sqlMetaItem.Mapping), &tableInfo)
		if err != nil {
			log.WarnContextf(ctx, "task(KnowledgeGenerateSchema) Process json.Unmarshal err: %v", err)
			continue
		}
		for _, filed := range tableInfo.Fields {
			text2sqlSummary += fmt.Sprintf("'%s': '%s, %s', ", filed.FormattedText, filed.RawText,
				model.TableDataCellDataType2String[filed.DataType])
		}
		if text2sqlSummary != "{" {
			text2sqlSummary = text2sqlSummary[:len(text2sqlSummary)-2] + "}"
			summary += text2sqlSummary + ";"
		}
		schemaItem := &pb.GetKnowledgeSchemaRsp_SchemaItem{
			BusinessId: schema.BusinessId,
			Name:       schema.Name + "." + tableInfo.TableName.RawText,
			Summary:    summary,
		}
		structFileSchemas = append(structFileSchemas, schemaItem)
	}
	return structFileSchemas, nil
}
