package release

import (
	"context"

	"git.woa.com/adp/common/x/logx"
	"gorm.io/gen"
)

// GetDevReleaseRelationInfoList 根据条件批量查询开发域和发布域的关联关系
// corpID: 企业ID
// robotID: 应用ID
// relationType: 类型(2:文档 3:QA 4:数据表 5:数据库)
// devBusinessIDs: 开发域业务ID列表
// 返回: map[dev_business_id]release_business_id
func (d *daoImpl) GetDevReleaseRelationInfoList(ctx context.Context, corpID, robotID uint64, relationType uint32, devBusinessIDs []uint64) (map[uint64]uint64, error) {
	tbl := d.mysql.TDevReleaseRelationInfo
	db := tbl.WithContext(ctx).Debug()

	conds := []gen.Condition{
		tbl.CorpID.Eq(corpID),
		tbl.RobotID.Eq(robotID),
		tbl.Type.Eq(relationType),
	}

	if len(devBusinessIDs) > 0 {
		conds = append(conds, tbl.DevBusinessID.In(devBusinessIDs...))
	}

	res, err := db.Where(conds...).Find()
	if err != nil {
		logx.E(ctx, "GetDevReleaseRelationInfoList error, corpID:%d, robotID:%d, type:%d, devBusinessIDs:%v, err:%v",
			corpID, robotID, relationType, devBusinessIDs, err)
		return nil, err
	}

	// 构建dev_business_id到release_business_id的映射
	resultMap := make(map[uint64]uint64, len(res))
	for _, item := range res {
		if item != nil {
			resultMap[item.DevBusinessID] = item.ReleaseBusinessID
		}
	}

	logx.I(ctx, "GetDevReleaseRelationInfoList success, corpID:%d, robotID:%d, type:%d, count:%d",
		corpID, robotID, relationType, len(resultMap))
	return resultMap, nil
}
