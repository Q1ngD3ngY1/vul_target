package dao

import (
	"context"
	"fmt"
	knowClient "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/baicaoyuan/moss/types/slicex"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
)

const (
	releaseDocFields = `
		id,version_id,doc_id,business_id,robot_id,corp_id,staff_id,file_name,file_type,file_size,bucket,cos_url,
		cos_hash,message,status,is_refer,is_deleted,source,web_url,batch_id,audit_flag,is_creating_qa,is_creating_index,
		action,attr_range,create_time,update_time,expire_time
	`
	createReleaseDoc = `
		INSERT INTO
			t_release_doc(%s)
		VALUES
			(null,:version_id,:doc_id,:business_id,:robot_id,:corp_id,:staff_id,:file_name,:file_type,:file_size,
			:bucket,:cos_url,:cos_hash,:message,:status,:is_refer,:is_deleted,:source,:web_url,:batch_id,:audit_flag,
			:is_creating_qa,:is_creating_index,:action,:attr_range,:create_time,:update_time,:expire_time)
	`
	getReleaseDocCountByVersion = `
		SELECT
			count(*)
		FROM
		    t_release_doc
		WHERE
			 robot_id = ? AND version_id = ? %s
	`
	getReleaseDocByVersion = `
		SELECT
			%s
		FROM
		    t_release_doc
		WHERE
		    robot_id = ? AND version_id = ? %s
		LIMIT ?,?
	`
	updateReleaseDocSuccess = `
		UPDATE
			t_release_doc
		SET
		    status = :status,
		    update_time = :update_time
		WHERE
		    id = :id
	`
	// 脚本sql 执行后删除
	getReleaseDocs = `
		SELECT
		   version_id,doc_id,action
		FROM
		    t_release_segment
		WHERE
		    version_id <= ?
		GROUP BY
		    version_id,doc_id,action
	`
)

// GetModifyDocCount 获取版本改动文档数量
func (d *dao) GetModifyDocCount(ctx context.Context, robotID, versionID uint64, fileName string, actions []uint32,
	statuses []uint32) (
	uint64, error) {
	args := make([]any, 0, 3+len(actions)+len(statuses))
	args = append(args, robotID, versionID)
	condition := ""
	if fileName != "" {
		condition = fmt.Sprintf("%s%s", condition, " AND file_name LIKE ? ")
		args = append(args, fmt.Sprintf("%%%s%%", special.Replace(fileName)))
	}
	if len(actions) > 0 {
		condition = fmt.Sprintf("%s AND action IN (%s)", condition, placeholder(len(actions)))
		for _, action := range actions {
			args = append(args, action)
		}
	}
	if len(statuses) > 0 {
		condition = fmt.Sprintf("%s AND status IN (%s)", condition, placeholder(len(statuses)))
		for _, status := range statuses {
			args = append(args, status)
		}
	}
	var total uint64
	querySQL := fmt.Sprintf(getReleaseDocCountByVersion, condition)
	if err := d.db.Get(ctx, &total, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取版本改动文档数量失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return 0, err
	}
	return total, nil
}

// GetModifyDocList 获取版本改动文档范围
func (d *dao) GetModifyDocList(ctx context.Context, robotID, versionID uint64, fileName string, actions []uint32,
	page, pageSize uint32) ([]*model.ReleaseDoc, error) {
	args := make([]any, 0, 5+len(actions))
	args = append(args, robotID, versionID)
	condition := ""
	if fileName != "" {
		condition = fmt.Sprintf("%s%s", condition, " AND file_name LIKE ?")
		fileNameArg := fmt.Sprintf("%%%s%%", special.Replace(fileName))
		args = append(args, fileNameArg)
	}
	if len(actions) > 0 {
		condition = fmt.Sprintf("%s AND action IN (%s)", condition, placeholder(len(actions)))
		for _, action := range actions {
			args = append(args, action)
		}
	}
	offset := (page - 1) * pageSize
	args = append(args, offset, pageSize)
	querySQL := fmt.Sprintf(getReleaseDocByVersion, releaseDocFields, condition)
	modifyDocs := make([]*model.ReleaseDoc, 0)
	if err := d.db.QueryToStructs(ctx, &modifyDocs, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取版本改动文档范围失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	return modifyDocs, nil
}

// ReleaseDocRebuild 发布文档重建 执行后删除
func (d *dao) ReleaseDocRebuild(ctx context.Context, vID uint64) error {
	list := make([]*model.RebuildVersionDoc, 0)
	querySQL := getReleaseDocs
	if err := d.db.QueryToStructs(ctx, &list, querySQL, vID); err != nil {
		log.ErrorContextf(ctx, "发布文档重建查询失败 sql:%s versionID:%d err:%+v", querySQL, vID, err)
		return err
	}
	docIDs := make([]uint64, 0)
	versionList := make(map[uint64]map[uint64]*model.RebuildDoc, 0)
	for _, item := range list {
		docIDs = append(docIDs, item.DocID)
		tmpDocMap, ok := versionList[item.VersionID]
		if !ok {
			tmpDocMap = make(map[uint64]*model.RebuildDoc, 0)
		}
		tmpDoc, ok := tmpDocMap[item.DocID]
		if !ok {
			tmpDoc = &model.RebuildDoc{DocID: item.DocID, Action: item.Action}
		}
		if tmpDoc.Action != item.Action {
			tmpDoc.Action = model.DocNextActionUpdate
		}
		tmpDocMap[item.DocID] = tmpDoc
		versionList[item.VersionID] = tmpDocMap
	}
	// 废弃接口，兼容处理
	docs, err := d.GetDocByIDs(ctx, slicex.Unique(docIDs), knowClient.NotVIP)
	if err != nil {
		return err
	}
	releaseDocs := make([]*model.ReleaseDoc, 0)
	for versionID, docMap := range versionList {
		for _, rdoc := range docMap {
			doc, ok := docs[rdoc.DocID]
			if !ok {
				continue
			}
			releaseDocs = append(releaseDocs, &model.ReleaseDoc{
				VersionID:       versionID,
				DocID:           rdoc.DocID,
				BusinessID:      doc.BusinessID,
				RobotID:         doc.RobotID,
				CorpID:          doc.CorpID,
				StaffID:         doc.StaffID,
				FileName:        doc.FileName,
				FileType:        doc.FileType,
				FileSize:        doc.FileSize,
				Bucket:          doc.Bucket,
				CosURL:          doc.CosURL,
				CosHash:         doc.CosHash,
				Message:         "rebuild release doc",
				Status:          doc.Status,
				IsDeleted:       doc.IsDeleted,
				IsRefer:         doc.IsRefer,
				Source:          doc.Source,
				WebURL:          doc.WebURL,
				BatchID:         doc.BatchID,
				AuditFlag:       doc.AuditFlag,
				IsCreatingQA:    doc.IsCreatingQaV1(),
				IsCreatingIndex: doc.IsCreatingIndexV1(),
				Action:          rdoc.Action,
				CreateTime:      time.Now(),
				UpdateTime:      time.Now(),
				ExpireTime:      doc.ExpireEnd,
			})
		}
	}
	log.DebugContextf(ctx, "releaseDocs:%+v", releaseDocs)
	querySQL = fmt.Sprintf(createReleaseDoc, releaseDocFields)
	if _, err = d.db.NamedExec(ctx, querySQL, releaseDocs); err != nil {
		log.ErrorContextf(ctx, "发布文档重建 插入失败 sql:%s releaseDocs:%+v err:%+v", querySQL, releaseDocs, err)
		return err
	}
	return nil
}
