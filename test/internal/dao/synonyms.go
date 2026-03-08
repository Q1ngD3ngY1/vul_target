package dao

import (
	"context"
	"errors"
	"fmt"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	trpcerr "git.code.oa.com/trpc-go/trpc-go/errs"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
	entityextractor "git.woa.com/dialogue-platform/proto/pb-stub/entity-extractor"
	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

const (
	synonymsFields = `
		synonyms_id,robot_id,corp_id,category_id,parent_id,word,word_md5,release_status,next_action,
		is_deleted,create_time,update_time
	`

	synonymsFieldsLen = 12

	// addSynonyms 添加同义词
	addSynonyms = `
		INSERT INTO
			t_synonyms (%s)
		VALUES
			%s
	`

	// updateSynonyms 更新同义词
	updateSynonyms = `
        UPDATE
			t_synonyms
        SET
			word = :word,
            word_md5 = :word_md5,
            release_status = :release_status,
            category_id = :category_id,
            next_action = :next_action,
            update_time = :update_time
        WHERE
            corp_id = :corp_id AND robot_id = :robot_id AND synonyms_id = :synonyms_id
     `

	// 删除标准词下的同义词
	deleteSynonyms = `
        UPDATE
            t_synonyms
        SET
            is_deleted = synonyms_id,
            update_time = ?,
            release_status = ?,
			next_action = ?
        WHERE
            corp_id = ? AND robot_id = ? AND synonyms_id in (%s)
    `

	// getSynonymsDetailsByParentID 根据 parent_id 获取同义词详情列表
	getSynonymsDetailsByParentID = `
        SELECT
            %s
        FROM
            t_synonyms
        WHERE
            corp_id = ? AND robot_id = ? AND parent_id = ? AND is_deleted = ?
	`

	// getSynonymsByWordMD5 根据 md5 获取同义词详情
	getSynonymsByWordMD5 = `
        SELECT
            %s
        FROM
		    t_synonyms
        WHERE
            corp_id = ? AND robot_id = ? AND word_md5 = ? AND is_deleted = ?
`

	// getSynonymsCount 获取同义词的个数，同时统计标准词和同义词的个数
	getSynonymsCount = `
        SELECT
			COUNT(DISTINCT CASE WHEN parent_id = 0 THEN synonyms_id ELSE parent_id END) AS total
        FROM
	        t_synonyms
	    WHERE
            corp_id = ? AND robot_id = ? %s
    `

	// getSynonymsStandardList 获取标准词列表
	getSynonymsStandardList = `
		SELECT
			%s
		FROM
			t_synonyms
		WHERE synonyms_id IN (
			SELECT DISTINCT
				CASE WHEN parent_id = 0 THEN synonyms_id ELSE parent_id END as synonyms_id
			FROM
				t_synonyms
			WHERE
				corp_id = ? AND robot_id = ? %s
		) AND corp_id = ? AND robot_id = ?
		ORDER BY
			update_time DESC, id DESC
		LIMIT ?, ?
		`

	// getSynonymsStandardListByIDs 根据标准词ID获取标准词列表
	getSynonymsStandardListByIDs = `
        SELECT
            %s
        FROM
	        t_synonyms
		WHERE
            corp_id = ? AND robot_id = ? AND synonyms_id in (%s)
		ORDER BY
            update_time DESC, id DESC
        LIMIT ?, ?
	`

	// getSynonymsListByStandardIDs 根据标准词ID获取同义词列表
	getSynonymsListByStandardIDs = `
        SELECT
            %s
        FROM
	        t_synonyms
		WHERE
            corp_id = ? AND robot_id = ? AND is_deleted = 0 AND parent_id in (%s)
		ORDER BY
            update_time DESC, id DESC
        LIMIT ?, ?
	`

	// getSynonymsByID 获取同义词详情
	getSynonymsDetailByID = `
        SELECT
            %s
        FROM
			t_synonyms
        WHERE
            corp_id = ? AND robot_id = ? AND synonyms_id = ?
	`

	// deleteSynonymsByID 根据主键删除标准词
	deleteSynonymsById = `
        UPDATE
		    t_synonyms
		SET
            is_deleted = :synonyms_id,
            update_time = :update_time,
            release_status = :release_status,
            next_action = :next_action
        WHERE
            corp_id = :corp_id AND robot_id = :robot_id AND synonyms_id = :synonyms_id
`

	// deleteSynonymsByParentSynonyms 根据父同义词ID删除同义词
	deleteSynonymsByParentSynonyms = `
        UPDATE
            t_synonyms
        SET
            is_deleted = synonyms_id,
            update_time = ?,
            release_status = ?,
            next_action = ?
        WHERE
            corp_id = ? AND robot_id = ? AND parent_id = ?
    `
)

const (
	synonymsTableName = "t_synonyms"

	synonymsTblColId            = "id"
	synonymsTblColBusinessId    = "synonyms_id"
	synonymsTblColRobotId       = "robot_id"
	synonymsTblColCorpId        = "corp_id"
	synonymsTblColCateId        = "category_id"
	synonymsTblColParentId      = "parent_id"
	synonymsTblColWord          = "word"
	synonymsTblColWordMd5       = "word_md5"
	synonymsTblColReleaseStatus = "release_status"
	synonymsTblColNextAction    = "next_action"
	synonymsTblColIsDeleted     = "is_deleted"
	synonymsTblColCreateTime    = "create_time"
	synonymsTblColUpdateTime    = "update_time"
)

// CreateSynonyms 创建单个标准词及其同义词
func (d *dao) CreateSynonyms(ctx context.Context, req *model.SynonymsCreateReq) (
	rsp *model.SynonymsCreateRsp, err error) {
	err = d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		rsp, err = d.createSynonymsWithSqlx(ctx, req, tx)
		return err
	})
	if err != nil {
		log.ErrorContextf(ctx, "CreateSynonyms 失败, error: %+v", err)
		return nil, err
	}
	return rsp, nil
}

// createSynonymsWithSqlx 创建单个标准词及其同义词
func (d *dao) createSynonymsWithSqlx(ctx context.Context, req *model.SynonymsCreateReq, tx *sqlx.Tx) (
	rsp *model.SynonymsCreateRsp, err error) {

	synonymsID := d.GenerateSeqID()
	cTime := time.Now()
	synonyms := make([]*model.Synonyms, 0, len(req.Synonyms)+1)
	// 标准词
	synonyms = append(synonyms, &model.Synonyms{
		SynonymsID:    synonymsID,
		RobotID:       req.RobotID,
		CorpID:        req.CorpID,
		CategoryID:    req.CateID,
		ParentID:      0, // 标准词 parent_id 为 0
		Word:          req.StandardWord,
		WordMD5:       util.Md5Hex(req.StandardWord),
		ReleaseStatus: model.ReleaseStatusInit,
		NextAction:    model.NextActionAdd,
		IsDeleted:     model.SynonymIsNotDeleted,
		CreateTime:    cTime,
		UpdateTime:    cTime,
	})
	// 标准词下的同义词
	for i := range req.Synonyms {
		sBizID := d.GenerateSeqID()
		synonyms = append(synonyms, &model.Synonyms{
			SynonymsID:    sBizID, // 当前同义词的 biz id
			RobotID:       req.RobotID,
			CorpID:        req.CorpID,
			CategoryID:    req.CateID,
			ParentID:      synonymsID, // 标准词的 biz id
			Word:          req.Synonyms[i],
			WordMD5:       util.Md5Hex(req.Synonyms[i]),
			ReleaseStatus: model.ReleaseStatusInit,
			NextAction:    model.NextActionAdd,
			IsDeleted:     model.SynonymIsNotDeleted,
			CreateTime:    cTime,
			UpdateTime:    cTime,
		})
	}

	var conflictType uint32
	var conflictContent string
	if len(req.Synonyms) == 0 {
		log.ErrorContextf(ctx, "CreateSynonyms get empty synonyms")
		return nil, errs.ErrParams
	}
	// 建立同义词 word_md5 的 map
	synonymsMap := make(map[string]*model.Synonyms, len(req.Synonyms))
	for i, s := range synonyms {
		synonymsMap[s.WordMD5] = synonyms[i]
	}
	querySql, args := d.getAddSqlAndArgs(synonyms)
	// 去重逻辑通过duplicated key 实现在 dao 层
	if _, err = tx.ExecContext(ctx, querySql, args...); err != nil && !isDupEntryError(err) {
		err.Error()
		log.ErrorContextf(ctx, "添加同义词失败，sql: %s, err: %+v", querySql, err)
		return nil, errs.ErrSystem
	}
	if isDupEntryError(err) {
		corpID := synonyms[0].CorpID
		robotID := synonyms[0].RobotID
		conflictType, conflictContent, err = d.getConflictSynonyms(ctx, corpID, robotID, synonymsMap, tx, err)
	}
	return &model.SynonymsCreateRsp{
		ConflictType:    conflictType,
		ConflictContent: conflictContent,
		SynonymsID:      synonymsID,
	}, nil
}

func (d *dao) getAddSqlAndArgs(synonyms []*model.Synonyms) (string, []any) {
	sql := addSynonyms
	placeholders := "(" + placeholder(synonymsFieldsLen) + ")"
	values := make([]string, len(synonyms))
	for i := range synonyms {
		values[i] = placeholders
	}
	querySql := fmt.Sprintf(sql, synonymsFields, strings.Join(values, ","))
	// prepare the arguments
	args := make([]any, 0, len(synonyms)*synonymsFieldsLen)
	for _, s := range synonyms {
		args = append(args,
			s.SynonymsID,
			s.RobotID,
			s.CorpID,
			s.CategoryID,
			s.ParentID,
			s.Word,
			s.WordMD5,
			s.ReleaseStatus,
			s.NextAction,
			s.IsDeleted,
			s.CreateTime,
			s.UpdateTime,
		)
	}
	return querySql, args
}

// getConflictSynonyms 获取有冲突的同义词
func (d *dao) getConflictSynonyms(ctx context.Context, corpID uint64, robotID uint64,
	synonymsMap map[string]*model.Synonyms, tx *sqlx.Tx, err error) (uint32, string, error) {
	if tx == nil || err == nil {
		log.ErrorContextf(ctx, "getConflictSynonyms get invalid tx or err")
		return 0, "", errs.ErrParams
	}
	var conflictType uint32
	var conflictContent string
	dupKey := extractDuplicateWordMD5(err.Error())
	if dupKey != "" {
		existSynonym, err := d.getExistSynonyms(ctx, corpID, robotID, dupKey)
		if err != nil {
			log.ErrorContextf(ctx, "getConflictSynonyms getExistSynonym err: %+v", err)
			return conflictType, conflictContent, errs.ErrSynonymsInvalidDupError
		}
		if existSynonym == nil {
			log.ErrorContextf(ctx, "getConflictSynonyms get nil synonym, word_md5: %s", dupKey)
			return conflictType, conflictContent, errs.ErrSynonymsInvalidDupError
		}
		// 判断当前 word_md5	是标准词还是同义词
		isStandard := false
		if synonymsMap == nil {
			isStandard = true
		} else {
			if s, exist := synonymsMap[dupKey]; exist {
				if s.ParentID == 0 {
					isStandard = true
				}
			}
		}
		if isStandard {
			// 当前的标准词与已有的标准词或者同义词冲突，只需要一个类型，只需要提示前端当前的标准词冲突了
			conflictType = model.SynonymsConflictTypeStandard
		} else {
			// 当前的同义词语已有的标准词或者同义词冲突，需要两个类型，方便展示
			if existSynonym.ParentID == 0 {
				conflictType = model.SynonymsConflictTypeSynonymsAndStandard
			} else {
				conflictType = model.SynonymsConflictTypeSynonymsAndSynonyms
			}
		}
		conflictContent = existSynonym.Word
	} else {
		log.ErrorContextf(ctx, "getConflictSynonyms get invalid err: %+v", err)
		return conflictType, conflictContent, errs.ErrSynonymsInvalidDupError
	}

	return conflictType, conflictContent, nil
}

func extractDuplicateWordMD5(errorMessage string) string {
	// key 重复时，错误信息如下:
	// "Duplicate entry 'robot_id-word_md5-is_deleted' for key 't_synonyms.uk_word_md5'"
	re := regexp.MustCompile(`Duplicate entry '[^']*-(?P<word_md5>[^']*)-[^']*' for key 't_synonyms.uk_word_md5'`)
	matches := re.FindStringSubmatch(errorMessage)
	for i, name := range re.SubexpNames() {
		if name == "word_md5" && i < len(matches) {
			return matches[i]
		}
	}
	return ""
}

// getExistSynonyms 根据dup_key(word_md5)获取同义词详情
func (d *dao) getExistSynonyms(ctx context.Context, corpID uint64, robotID uint64, dupKey string) (*model.Synonyms,
	error) {
	sqlQuery := fmt.Sprintf(getSynonymsByWordMD5, synonymsFields)
	args := []any{corpID, robotID, dupKey, model.SynonymIsNotDeleted}
	synonyms := &model.Synonyms{}
	if err := d.db.QueryToStruct(ctx, synonyms, sqlQuery, args...); err != nil {
		log.ErrorContextf(ctx, "getExistSynonym get synonyms failed, sql: %s, args: %+v, err: %v",
			sqlQuery, args, err)
		return nil, err
	}

	return synonyms, nil
}

// GetSynonymsListCount 获取同义词列表总数
func (d *dao) GetSynonymsListCount(ctx context.Context, req *model.SynonymsListReq) (uint32, error) {
	condition, args := d.getConditionAndArgsFromListReq(ctx, req)
	querySql := fmt.Sprintf(getSynonymsCount, condition)
	stat := &model.SynonymsStat{}
	if err := d.db.QueryToStruct(ctx, stat, querySql, args...); err != nil {
		log.ErrorContextf(ctx, "GetSynonymsListCount getSynonymsTotal failed, sql: %s, args: %+v, err: %v",
			querySql, args, err)
		return 0, err
	}

	return uint32(stat.Total), nil
}

func (d *dao) getConditionAndArgsFromListReq(ctx context.Context, req *model.SynonymsListReq) (string, []any) {
	condition := ""
	var args []any
	args = append(args, req.CorpID, req.RobotID)
	if req.IsDeleted != 0 {
		condition = fmt.Sprintf("%s%s", condition, " AND is_deleted != 0")
		// is_deleted可能是biz_id，这里用 != 0
	} else {
		condition = fmt.Sprintf("%s%s", condition, " AND is_deleted = 0")
	}
	if req.Query != "" {
		condition = fmt.Sprintf("%s%s", condition, " AND word LIKE ?")
		args = append(args, fmt.Sprintf("%%%s%%", special.Replace(req.Query)))
	}
	if len(req.CateIDs) > 0 {
		condition = fmt.Sprintf("%s AND category_id IN (%s)", condition, placeholder(len(req.CateIDs)))
		for _, cID := range req.CateIDs {
			args = append(args, cID)
		}
	}
	if len(req.ReleaseStatus) > 0 {
		condition = fmt.Sprintf("%s AND release_status IN (%s)", condition, placeholder(len(req.ReleaseStatus)))
		for _, rs := range req.ReleaseStatus {
			args = append(args, rs)
		}
	}
	// 如下两个条件暂时用不到，先在接口上支持了
	if !req.UpdateTime.IsZero() && !req.UpdateTimeEqual {
		condition = fmt.Sprintf("%s%s", condition, " AND update_time > ?")
		args = append(args, req.UpdateTime)
	}
	if req.UpdateTimeEqual && !req.UpdateTime.IsZero() && req.ID != 0 {
		condition = fmt.Sprintf("%s%s", condition, " AND update_time = ? AND id >= ?")
		args = append(args, req.UpdateTime, req.ID)
	}
	log.DebugContextf(ctx, "getConditionAndArgsFromListReq, condition: %s, args: %+v", condition, args)

	return condition, args
}

// GetSynonymsList 获取同义词列表
// 1. 先处理标准词的分页等逻辑，获取到分页里的标准词
// 2. 再获取当前分页的所有的标准词（可能会数据量比较大, 需要考虑分页获取）
func (d *dao) GetSynonymsList(ctx context.Context, req *model.SynonymsListReq) (*model.SynonymsListRsp, error) {
	var args []any
	pageSize := uint32(15)
	page := uint32(1)
	if req.PageSize != 0 {
		pageSize = req.PageSize
	}
	if req.Page != 0 {
		page = req.Page
	}
	offset := (page - 1) * pageSize

	synonyms := make([]*model.Synonyms, 0)
	if len(req.SynonymsIDs) > 0 {
		// 如果有 synonymsIDS 的话，走单独的拉取逻辑
		// 前端没有此类场景, 导出会有
		args = append(args, req.CorpID, req.RobotID)
		querySQL := fmt.Sprintf(getSynonymsStandardListByIDs, synonymsFields, placeholder(len(req.SynonymsIDs)))
		for _, sID := range req.SynonymsIDs {
			args = append(args, sID)
		}
		args = append(args, offset, pageSize)
		if err := d.db.QueryToStructs(ctx, &synonyms, querySQL, args...); err != nil {
			log.ErrorContextf(ctx, "GetSynonymsList getSynonymsStandardListByIDs failed, sql: %s, args: %+v, "+
				"err: %v", querySQL, args, err)
			return nil, err
		}
	} else {
		// 1. 先处理标准词
		var condition string
		condition, args = d.getConditionAndArgsFromListReq(ctx, req)
		args = append(args, req.CorpID, req.RobotID)
		args = append(args, offset, pageSize)
		querySQL := fmt.Sprintf(getSynonymsStandardList, synonymsFields, condition)
		if err := d.db.QueryToStructs(ctx, &synonyms, querySQL, args...); err != nil {
			log.ErrorContextf(ctx, "GetSynonymsList getSynonymsStandardList failed, sql: %s, args: %+v, err: %v",
				querySQL, args, err)
			return nil, err
		}
	}

	if len(synonyms) == 0 {
		return &model.SynonymsListRsp{}, nil
	}

	// 2. 获取所有的标准词 id
	args = []any{}
	args = append(args, req.CorpID, req.RobotID)
	querySQL := fmt.Sprintf(getSynonymsListByStandardIDs, synonymsFields, placeholder(len(synonyms)))
	for _, s := range synonyms {
		args = append(args, s.SynonymsID)
	}

	// 3. 分页获取所有同义词
	limitFrom := 0
	limitOffset := 800
	args = append(args, limitFrom, limitOffset)
	for {
		args[len(args)-2] = limitFrom
		log.DebugContextf(ctx, "分页获取同义词，from_limit:%+v", args[len(args)-2:])
		synonymsList := make([]*model.Synonyms, 0)
		if err := d.db.QueryToStructs(ctx, &synonymsList, querySQL, args...); err != nil {
			log.ErrorContextf(ctx, "GetSynonymsList getSynonymsListByStandardIDs failed, sql: %s, args: %+v, "+
				"err: %v", querySQL, args, err)
			return nil, err
		}
		synonyms = append(synonyms, synonymsList...)
		if len(synonymsList) < limitOffset {
			break
		}
		limitFrom += limitOffset
		if limitFrom > math.MaxInt32 {
			break
		}
	}

	return d.getSynonymsListBySynonyms(ctx, synonyms)
}

// getSynonymsListBySynonyms 根据synonyms列表获取同义词列表
func (d *dao) getSynonymsListBySynonyms(ctx context.Context, list []*model.Synonyms) (*model.SynonymsListRsp, error) {
	// 从 list 中的同义词附加到对应的标准词下
	synonymsMap := make(map[uint64][]string)
	for i := range list {
		if list[i].ParentID == 0 {
			continue
		}
		synonymsMap[list[i].ParentID] = append(synonymsMap[list[i].ParentID], list[i].Word)
	}
	synonymsList := make([]*model.SynonymsItem, 0, len(list))
	for i := range list {
		if list[i].ParentID != 0 {
			// 同义词不单独返回，而是附加到标准词下
			continue
		}
		var synonymsWordList []string
		synonymID := list[i].SynonymsID
		if _, ok := synonymsMap[synonymID]; ok {
			synonymsWordList = synonymsMap[synonymID]
		}
		synonymsList = append(synonymsList, &model.SynonymsItem{
			SynonymsID:   list[i].SynonymsID,
			CateID:       list[i].CategoryID,
			StandardWord: list[i].Word,
			Status:       list[i].ReleaseStatus,
			StatusDesc:   i18n.Translate(ctx, list[i].StatusDesc()),
			Synonyms:     synonymsWordList,
			UpdateTime:   list[i].UpdateTime,
			CreateTime:   list[i].CreateTime,
		})
	}
	return &model.SynonymsListRsp{
		Synonyms: synonymsList,
	}, nil
}

// GetSynonymDetailsByBizID 通过bizID获取同义词详情
func (d *dao) GetSynonymDetailsByBizID(ctx context.Context, corpId, appID, synonymsID uint64) (*model.Synonyms, error) {
	if corpId == 0 || appID == 0 || synonymsID == 0 {
		log.ErrorContextf(ctx, "GetSynonymDetailsByBizID corpId: %d, appID: %d, synonymsID: %d", corpId, appID,
			synonymsID)
		return nil, errs.ErrParams
	}
	querySQL := fmt.Sprintf(getSynonymsDetailByID, synonymsFields)
	args := []any{corpId, appID, synonymsID}
	synonyms := &model.Synonyms{}
	if err := d.db.QueryToStruct(ctx, synonyms, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "GetSynonymDetailsByBizID getSynonymsDetailByID failed, sql: %s, "+
			"args: %+v, err: %v", querySQL, args, err)
		return nil, err
	}

	return synonyms, nil
}

// UpdateSynonyms 更新同义词
// 1. 获取当前所有的标准词，并进行 diff，找出新增和删除的同义词
// 2. 如果标准词本身发生变化，则 update 标准词，并发布标准词，标准词只是发生分类变化，则不需要发布
// 3. 更新删除的同义词
// 4. 更新新增的同义词
func (d *dao) UpdateSynonyms(ctx context.Context, oldSynonyms *model.Synonyms,
	synonymsModifyReq *model.SynonymsModifyReq) (conflictType uint32, conflictContent string, err error) {
	if oldSynonyms == nil || synonymsModifyReq == nil {
		log.ErrorContextf(ctx, "UpdateSynonyms oldSynonym or synonymsModifyReq is nil")
		return 0, "", errs.ErrParams
	}
	log.DebugContextf(ctx, "UpdateSynonyms oldSynonyms: %+v, synonymsModifyReq: %+v",
		oldSynonyms, synonymsModifyReq)
	synonyms, isStandardNeedUpdate, isStandardNeedPublish := d.getUpdateSynonyms(ctx, oldSynonyms, synonymsModifyReq)

	err = d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		// 1. 获取标准词下的所有同义词
		allSynonyms := make(map[string]*model.Synonyms)
		allSynonyms, err = d.getSynonymsDetailsByStandard(ctx, synonyms)
		if err != nil {
			log.ErrorContextf(ctx, "getSynonymsDetailsByStandard failed, err: %v", err)
			return err
		}

		// 2. 获取新增和删除的同义词
		needAddSynonyms, needDeleteSynonyms := d.getAddAndDeleteSynonyms(ctx, synonyms, synonymsModifyReq, allSynonyms)
		log.DebugContextf(ctx, "getAddAndDeleteSynonyms, addSynonyms len: %d, deleteSynonyms len: %d",
			len(needAddSynonyms), len(needDeleteSynonyms))

		// 3. 判断是否需要更新标准词状态
		if len(needAddSynonyms) != 0 || len(needDeleteSynonyms) != 0 {
			isStandardNeedUpdate = true
			isStandardNeedPublish = true
		}

		// 4. 更新标准词，需要考虑冲突问题
		if isStandardNeedUpdate {
			// 如果同义词发生变化，需要更新标准词以及状态
			if conflictType, conflictContent, err = d.updateStandardSynonyms(ctx, tx, synonyms,
				isStandardNeedPublish); err != nil {
				return err
			}
		}

		// 5. 判断同义词个数
		if err = d.checkSynonymsCount(ctx, allSynonyms, needAddSynonyms, needDeleteSynonyms); err != nil {
			return err
		}

		// 6. 新增同义词，需要考虑冲突问题
		if len(needAddSynonyms) > 0 {
			if conflictType, conflictContent, err = d.addSynonyms(ctx, tx, needAddSynonyms, synonyms); err != nil {
				return err
			}
		}

		// 7. 删除同义词
		if len(needDeleteSynonyms) > 0 {
			err = d.deleteNeedSynonyms(ctx, synonyms, needDeleteSynonyms, tx)
			if err != nil {
				return err
			}
		}

		return nil
	})

	if err != nil && !isDupEntryBusinessError(err) {
		log.ErrorContextf(ctx, "UpdateSynonyms failed, err: %v", err)
		return conflictType, conflictContent, err
	}

	return conflictType, conflictContent, nil
}

func (d *dao) updateStandardSynonyms(ctx context.Context, tx *sqlx.Tx, synonyms *model.Synonyms,
	isStandardNeedPublish bool) (conflictType uint32, conflictContent string, err error) {
	sql := updateSynonyms
	if isStandardNeedPublish {
		synonyms.ReleaseStatus = model.SynonymsReleaseStatusWaiting
		if synonyms.NextAction != model.NextActionAdd {
			synonyms.NextAction = model.NextActionUpdate
		}
	}
	if _, err = tx.NamedExecContext(ctx, sql, synonyms); err != nil && !isDupEntryError(err) {
		log.ErrorContextf(ctx, "UpdateSynonyms update synonyms failed, sql: %s, err: %v", sql, err)
		return 0, "", err
	}
	if isDupEntryError(err) {
		var getErr error // dup_entry_err 不能被覆盖
		conflictType, conflictContent, getErr = d.getConflictSynonyms(ctx, synonyms.CorpID, synonyms.RobotID,
			nil, tx, err)
		if getErr != nil {
			log.ErrorContextf(ctx, "getConflictSynonyms failed, err: %v", getErr)
			return 0, "", getErr
		}
	}

	return conflictType, conflictContent, err
}

func (d *dao) checkSynonymsCount(ctx context.Context, allSynonyms map[string]*model.Synonyms,
	needAddSynonyms []*model.Synonyms, needDeleteSynonyms []*model.Synonyms) error {
	if len(allSynonyms)+len(needAddSynonyms)-len(needDeleteSynonyms) >
		int(config.App().Synonyms.MaxSynonymsCountPerWord) {
		log.WarnContextf(ctx, "UpdateSynonyms synonyms count is too many, current synonyms count: %d, "+
			"needAddSynonyms len: %d, needDeleteSynonyms len: %d",
			len(allSynonyms), len(needAddSynonyms), len(needDeleteSynonyms))
		return errs.ErrSynonymsTooMany
	}
	return nil
}

func (d *dao) addSynonyms(ctx context.Context, tx *sqlx.Tx, needAddSynonyms []*model.Synonyms,
	synonyms *model.Synonyms) (conflictType uint32, conflictContent string, err error) {
	if len(needAddSynonyms) > 0 {
		synonymsMap := make(map[string]*model.Synonyms, len(needAddSynonyms))
		for i, s := range needAddSynonyms {
			synonymsMap[s.WordMD5] = needAddSynonyms[i]
		}
		sqlQuery, args := d.getAddSqlAndArgs(needAddSynonyms)
		if _, err = tx.ExecContext(ctx, sqlQuery, args...); err != nil && !isDupEntryError(err) {
			log.ErrorContextf(ctx, "UpdateSynonyms add synonyms failed, sql: %s, err: %v", sqlQuery, err)
			return 0, "", err
		}
		if isDupEntryError(err) {
			var getErr error
			conflictType, conflictContent, getErr = d.getConflictSynonyms(ctx, synonyms.CorpID, synonyms.RobotID,
				synonymsMap, tx, err)
			if getErr != nil {
				log.ErrorContextf(ctx, "getConflictSynonyms failed, err: %v", getErr)
				return 0, "", getErr
			}
		}
	}

	return conflictType, conflictContent, err
}

func (d *dao) getUpdateSynonyms(ctx context.Context, oldSynonym *model.Synonyms,
	synonymsModifyReq *model.SynonymsModifyReq) (synonyms *model.Synonyms, isStandardNeedUpdate bool,
	isStandardNeedPublish bool) {
	if oldSynonym == nil || synonymsModifyReq == nil {
		log.ErrorContextf(ctx, "getUpdateSynonyms oldSynonyms or synonymsModifyReq is nil")
		return nil, false, false
	}
	isStandardNeedUpdate = false
	isStandardNeedPublish = false
	synonym := *oldSynonym
	if oldSynonym.Word != synonymsModifyReq.StandardWord {
		synonym.Word = synonymsModifyReq.StandardWord
		synonym.WordMD5 = util.Md5Hex(synonymsModifyReq.StandardWord)
		isStandardNeedUpdate = true
		isStandardNeedPublish = true
	}
	uTime := time.Now()
	synonym.UpdateTime = uTime
	if oldSynonym.CategoryID != synonymsModifyReq.CateID {
		synonym.CategoryID = synonymsModifyReq.CateID
		isStandardNeedUpdate = true
	}

	return &synonym, isStandardNeedUpdate, isStandardNeedPublish
}

func (d *dao) deleteNeedSynonyms(ctx context.Context, synonyms *model.Synonyms, needDeleteSynonyms []*model.Synonyms,
	tx *sqlx.Tx) error {
	if len(needDeleteSynonyms) == 0 {
		return nil
	}
	sql := deleteSynonyms
	// 针对删掉的同义词，需要分两批删除，一批是新增的同义词删除，一批是已经发布的同义词
	var nonPublishedSynonymsIDs []uint64
	var publishedSynonymsIDs []uint64
	for _, s := range needDeleteSynonyms {
		if s.NextAction == model.NextActionAdd { // 删除新增未发布的同义词
			nonPublishedSynonymsIDs = append(nonPublishedSynonymsIDs, s.SynonymsID)
		} else { // 删除发布的同义词
			publishedSynonymsIDs = append(publishedSynonymsIDs, s.SynonymsID)
		}
	}
	if len(publishedSynonymsIDs) > 0 {
		sqlQuery := fmt.Sprintf(sql, placeholder(len(publishedSynonymsIDs)))
		args := []any{time.Now(), model.ReleaseStatusInit, model.NextActionDelete, synonyms.CorpID, synonyms.RobotID}
		for _, s := range publishedSynonymsIDs {
			args = append(args, s)
		}
		if _, err := tx.ExecContext(ctx, sqlQuery, args...); err != nil {
			log.ErrorContextf(ctx, "deletePublishedSynonyms failed, sql: %s, err: %v", sql, err)
			return err
		}
	}
	if len(nonPublishedSynonymsIDs) > 0 {
		// 如果删除未发布的同义词, next action 保持不变
		sqlQuery := fmt.Sprintf(sql, placeholder(len(nonPublishedSynonymsIDs)))
		args := []any{time.Now(), model.ReleaseStatusInit, model.NextActionAdd, synonyms.CorpID, synonyms.RobotID}
		for _, s := range nonPublishedSynonymsIDs {
			args = append(args, s)
		}
		if _, err := tx.ExecContext(ctx, sqlQuery, args...); err != nil {
			log.ErrorContextf(ctx, "deleteNonPublishedSynonyms failed, sql: %s, err: %v", sql, err)
			return err
		}
	}

	return nil
}

// getSynonymsDetailsByStandard 根据标准词获取所有同义词
func (d *dao) getSynonymsDetailsByStandard(ctx context.Context, synonyms *model.Synonyms) (
	map[string]*model.Synonyms, error) {
	sqlQuery := fmt.Sprintf(getSynonymsDetailsByParentID, synonymsFields)
	args := []any{synonyms.CorpID, synonyms.RobotID, synonyms.SynonymsID, model.SynonymIsNotDeleted}
	synonymsList := make([]*model.Synonyms, 0)
	if err := d.db.QueryToStructs(ctx, &synonymsList, sqlQuery, args...); err != nil {
		log.ErrorContextf(ctx, "getSynonymsDetailsByStandard failed, sql: %s, args: %+v, err: %v",
			sqlQuery, args, err)
		return nil, err
	}
	log.DebugContextf(ctx, "getSynonymsDetailsByStandard, synonyms len: %d", len(synonymsList))
	res := make(map[string]*model.Synonyms)
	for i, s := range synonymsList {
		res[s.Word] = synonymsList[i]
	}

	return res, nil
}

// getAddAndDeleteSynonyms 获取新增和删除的同义词
func (d *dao) getAddAndDeleteSynonyms(ctx context.Context, synonyms *model.Synonyms,
	synonymsModifyReq *model.SynonymsModifyReq, allSynonyms map[string]*model.Synonyms) (
	addSynonyms []*model.Synonyms, deleteSynonyms []*model.Synonyms) {

	if synonyms == nil || synonymsModifyReq == nil || allSynonyms == nil {
		log.ErrorContextf(ctx, "getAddAndDeleteSynonyms synonym or synonymsModifyReq or allSynonyms is nil")
		return nil, nil
	}
	// 获取需要新增的同义词
	cTime := time.Now()
	needAddSynonyms := make([]*model.Synonyms, 0)
	needDeleteSynonyms := make([]*model.Synonyms, 0)
	for _, s := range synonymsModifyReq.Synonyms {
		if _, ok := allSynonyms[s]; !ok {
			synId := d.GenerateSeqID()
			needAddSynonyms = append(needAddSynonyms, &model.Synonyms{
				SynonymsID:    synId,
				CorpID:        synonyms.CorpID,
				RobotID:       synonyms.RobotID,
				CategoryID:    synonyms.CategoryID,
				ParentID:      synonyms.SynonymsID,
				Word:          s,
				WordMD5:       util.Md5Hex(s),
				ReleaseStatus: model.ReleaseStatusInit,
				NextAction:    model.NextActionAdd,
				IsDeleted:     model.SynonymIsNotDeleted,
				CreateTime:    cTime,
				UpdateTime:    cTime,
			})
		}
	}
	// 构建当前请求的同义词 map
	synonymsWordMap := make(map[string]struct{})
	for _, s := range synonymsModifyReq.Synonyms {
		synonymsWordMap[s] = struct{}{}
	}

	// 计算需要删除的同义词
	for _, s := range allSynonyms {
		if _, ok := synonymsWordMap[s.Word]; !ok {
			needDeleteSynonyms = append(needDeleteSynonyms, s)
		}
	}

	return needAddSynonyms, needDeleteSynonyms
}

// GetSynonymsDetailsByBizIDs 批量获取同义词详情
func (d *dao) GetSynonymsDetailsByBizIDs(ctx context.Context, corpID, appID uint64,
	synonymsIDs []uint64) (map[uint64]*model.Synonyms, error) {
	if corpID == 0 || appID == 0 || len(synonymsIDs) == 0 {
		return nil, errs.ErrParams
	}
	args := []any{corpID, appID}
	querySQL := fmt.Sprintf(getSynonymsStandardListByIDs, synonymsFields, placeholder(len(synonymsIDs)))
	for _, synonymID := range synonymsIDs {
		args = append(args, synonymID)
	}
	// 分页获取所有同义词
	synonyms, err := d.getSynonymsBySQLAndArgs(ctx, querySQL, args)
	if err != nil {
		log.ErrorContextf(ctx, "GetSynonymsDetailsByBizIDs getSynonymsBySQLAndArgs failed, sql: %s, args: %+v, "+
			"err: %v", querySQL, args, err)
		return nil, err
	}
	synonymsMap := make(map[uint64]*model.Synonyms)
	for _, s := range synonyms {
		synonymsMap[s.SynonymsID] = s
	}
	return synonymsMap, nil
}

// getSynonymsBySQLAndArgs 根据SQL获取同义词
func (d *dao) getSynonymsBySQLAndArgs(ctx context.Context, querySQL string, args []any) ([]*model.Synonyms, error) {
	synonyms := make([]*model.Synonyms, 0)
	limitFrom := 0
	limitOffset := 800
	args = append(args, limitFrom, limitOffset)
	for {
		args[len(args)-2] = limitFrom
		log.DebugContextf(ctx, "分页获取同义词，from_limit:%+v", args[len(args)-2:])
		synonymsList := make([]*model.Synonyms, 0)
		if err := d.db.QueryToStructs(ctx, &synonymsList, querySQL, args...); err != nil {
			log.ErrorContextf(ctx, "GetSynonymsList getSynonymsListByStandardIDs failed, sql: %s, args: %+v, "+
				"err: %v", querySQL, args, err)
			return nil, err
		}
		synonyms = append(synonyms, synonymsList...)
		if len(synonymsList) < limitOffset {
			break
		}
		limitFrom += limitOffset
		if limitFrom > math.MaxInt32 {
			break
		}
	}

	return synonyms, nil
}

// DeleteSynonyms 删除同义词
func (d *dao) DeleteSynonyms(ctx context.Context, corpID, robotID, staffID uint64, synonyms []*model.Synonyms) error {
	if err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		return d.deleteSynonyms(ctx, tx, synonyms)
	}); err != nil {
		log.ErrorContextf(ctx, "DeleteSynonyms failed, err: %+v", err)
	}
	return nil
}

func (d *dao) deleteSynonyms(ctx context.Context, tx *sqlx.Tx, synonyms []*model.Synonyms) error {
	// 1. 删除标准词
	length := len(synonyms)
	pageSize := 100
	pages := int(math.Ceil(float64(length) / float64(pageSize)))
	now := time.Now()
	for i := 0; i < pages; i++ {
		start := i * pageSize
		end := (i + 1) * pageSize
		if end > length {
			end = length
		}
		synonymsSlice := synonyms[start:end]
		if len(synonymsSlice) == 0 {
			continue
		}
		for _, s := range synonymsSlice {
			s.IsDeleted = model.IsDeleted
			s.UpdateTime = now
			// 如果当前不是新增状态，则设置为删除状态,如果是新增的，则状态保持不变
			if !s.IsNextActionAdd() {
				s.NextAction = model.NextActionDelete
				s.ReleaseStatus = model.ReleaseStatusInit
			}
			querySQL := deleteSynonymsById
			if _, err := tx.NamedExecContext(ctx, querySQL, s); err != nil {
				log.ErrorContextf(ctx, "删除标准词失败, sql:%s, args:%+v, err: %+v", querySQL, s, err)
				return err
			}
			if err := d.deleteSynonymsByParentSynonyms(ctx, tx, s); err != nil {
				log.ErrorContextf(ctx, "删除同义词失败, err: %+v", err)
				return err
			}
		}
	}

	return nil
}

// deleteSynonymsByParentSynonyms 删除同义词
func (d *dao) deleteSynonymsByParentSynonyms(ctx context.Context, tx *sqlx.Tx, synonyms *model.Synonyms) error {
	querySQL := deleteSynonymsByParentSynonyms
	args := []any{time.Now(), synonyms.ReleaseStatus, synonyms.NextAction,
		synonyms.CorpID, synonyms.RobotID, synonyms.SynonymsID}
	if _, err := tx.ExecContext(ctx, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "删除同义词失败, sql:%s, args:%+v, err: %+v", querySQL, args, err)
		return err
	}

	return nil
}

// GetSynonymsNER 获取同义词NER
func (d *dao) GetSynonymsNER(ctx context.Context, req *model.SynonymsNERReq) (*model.SynonymsNERRsp, error) {
	var clientProxy entityextractor.EntityExtractorObjClientProxy
	if req.Scenes == model.RunEnvSandbox {
		clientProxy = d.nerSandBoxCli
	} else {
		clientProxy = d.nerProdCli
	}
	entityInfo := &entityextractor.EntityInfo{
		BotId: strconv.FormatUint(req.RobotID, 10),
	}
	nerReq := &entityextractor.EntityExtractorReq{
		RequestId: getRequestID(ctx),
		Query:     req.Query,
		EntityIds: []*entityextractor.EntityInfo{entityInfo},
		Source:    entityextractor.EntityExtractorReq_SYNONYMS,
	}
	nerRsp, err := clientProxy.Extractor(ctx, nerReq)
	if err != nil {
		log.ErrorContextf(ctx, "GetSynonymsNER失败，err: %+v", err)
		return nil, err
	}
	log.DebugContextf(ctx, "GetSynonymsNER req: %+v，rsp: %+v", nerReq, nerRsp)
	nerInfos := make([]*model.NerInfo, 0)
	for _, charOccur := range nerRsp.CharOccurs {
		for _, keywordCand := range charOccur.KeywordCand {
			if keywordCand.Info != nil && len(keywordCand.Info.Record) == 1 {
				record := keywordCand.Info.Record[0] // 同义词只会匹配到一个，如果匹配到多个，说明有问题
				nerInfo := &model.NerInfo{
					Offset:       charOccur.GetOffset(),
					NumTokens:    keywordCand.GetNumTokens(),
					OriginalText: keywordCand.GetOriginalText(),
					RefValue:     record.GetRefValue(),
				}
				nerInfos = append(nerInfos, nerInfo)
			}
		}
	}
	// 依次匹配同义词，并替换成这个词对应的同义词
	replacedQuery := replaceSynonyms(req.Query, nerInfos)
	return &model.SynonymsNERRsp{
		ReplacedQuery: replacedQuery,
		NERInfos:      nerInfos,
	}, nil
}

func replaceSynonyms(originalQuery string, nerInfos []*model.NerInfo) string {
	if len(nerInfos) == 0 {
		return originalQuery
	}
	// Sort nerInfos by offset
	sort.Slice(nerInfos, func(i, j int) bool {
		return nerInfos[i].Offset < nerInfos[j].Offset
	})

	var result strings.Builder
	result.WriteString(originalQuery) // 先将 originalQuery 写入 result

	offsetAdjustment := 0 // 用于调整后续替换的偏移量

	for _, nerInfo := range nerInfos {
		resultString := result.String()
		startIndex := getRuneIndex(resultString, int(nerInfo.Offset)+offsetAdjustment)
		endIndex := getRuneIndex(resultString, int(nerInfo.Offset)+offsetAdjustment+utf8.RuneCountInString(nerInfo.OriginalText))

		// Check for out of bounds
		if startIndex < 0 || endIndex > result.Len() || startIndex >= endIndex {
			continue
		}

		// 替换对应位置的文本
		result.Reset()
		result.WriteString(resultString[:startIndex])
		result.WriteString(nerInfo.RefValue)
		result.WriteString(resultString[endIndex:])

		// 调整偏移量：新的替换文本可能与原文本长度不同，因此需要调整后续替换的起始位置
		offsetAdjustment += utf8.RuneCountInString(nerInfo.RefValue) - utf8.RuneCountInString(nerInfo.OriginalText)
	}

	return result.String()
}

// getRuneIndex returns the byte index in the string for the given rune index
func getRuneIndex(s string, runeIndex int) int {
	runeCount := 0
	for i := range s {
		if runeCount == runeIndex {
			return i
		}
		runeCount++
	}
	if runeCount == runeIndex {
		return len(s)
	}
	return -1
}

// GetSynonymsListReq 获取同义词列表请求参数
func (d *dao) GetSynonymsListReq(ctx context.Context, req *pb.ListSynonymsReq, robotID, corpID uint64) (
	*model.SynonymsListReq, error) {
	if req == nil {
		log.ErrorContextf(ctx, "GetSynonymsListReq get nil req")
		return nil, errs.ErrParams
	}
	var cateIDs []uint64
	cateBizId, err := util.CheckReqParamsIsUint64(ctx, req.GetCateBizId())
	if err != nil {
		return nil, err
	}
	if cateBizId != model.AllCateID {
		cateID, err := d.CheckCateBiz(ctx, model.SynonymsCate, corpID, cateBizId, robotID)
		if err != nil {
			return nil, err
		}
		childrenIDs, err := d.GetCateChildrenIDs(ctx, model.SynonymsCate, corpID, cateID, robotID)
		if err != nil {
			return nil, err
		}
		cateIDs = append(childrenIDs, cateID)
	}
	synonymsListReq := &model.SynonymsListReq{
		RobotID:       robotID,
		CorpID:        corpID,
		IsDeleted:     model.SynonymIsNotDeleted,
		CateIDs:       cateIDs,
		Query:         req.GetQuery(),
		ReleaseStatus: req.GetReleaseStatus(),
		Page:          req.GetPageNumber(),
		PageSize:      req.GetPageSize(),
	}
	return synonymsListReq, nil
}

func isDupEntryError(err error) bool {
	var e *mysql.MySQLError
	ok := errors.As(err, &e)
	if !ok {
		return false
	}
	return e.Number == 1062
}

// 在事务外判断是否是 dup entry error
func isDupEntryBusinessError(err error) bool {
	if errType(err) == trpcerr.ErrorTypeBusiness && trpcerr.Code(err) == 1062 {
		return true
	}
	return false
}

// errType 获取错误类型
func errType(err error) int {
	var e *trpcerr.Error
	if errors.As(err, &e) {
		return e.Type
	}
	return 0
}
