package permissions

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand/v2"
	"testing"
	"time"

	"git.code.oa.com/trpc-go/trpc-database/redis"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
	redisV8 "github.com/go-redis/redis/v8"
	"github.com/spf13/cast"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// InitDB 初始化GORM数据库连接
// dsn: 数据库连接字符串，格式如："user:pass@tcp(127.0.0.1:3306)/dbname?charset=utf8mb4&parseTime=True&loc=Local"
// maxIdleConns: 最大空闲连接数
// maxOpenConns: 最大打开连接数
// connMaxLifetime: 连接最大存活时间(秒)
func InitDB(maxIdleConns, maxOpenConns int, connMaxLifetime time.Duration) (*gorm.DB, error) {
	// dsn := "lokli:l123456@tcp(localhost:3306)/db_base?charset=utf8mb4&parseTime=True&loc=Local"
	dsn := "root:helloworld@tcp(127.0.0.1:3306)/test?charset=utf8mb4&parseTime=True&loc=Local"

	// 配置GORM日志级别
	newLogger := logger.Default.LogMode(logger.Info)

	// 打开数据库连接
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger: newLogger,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect database: %v", err)
	}

	// 获取底层sql.DB对象以设置连接池
	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("failed to get sql.DB: %v", err)
	}

	// 设置连接池参数
	sqlDB.SetMaxIdleConns(maxIdleConns)
	sqlDB.SetMaxOpenConns(maxOpenConns)
	sqlDB.SetConnMaxLifetime(connMaxLifetime * time.Second)

	if err := db.AutoMigrate(
		model.KnowledgeRole{},
		model.KnowledgeRoleKnow{},
		model.KnowledgeRoleDoc{},
		model.KnowledgeRoleQA{},
		model.KnowledgeRoleAttributeLabel{},
		model.KnowledgeRoleCate{},
		model.KnowledgeRoleDatabase{},
	); err != nil {
		return nil, err
	}

	dao.SetChunkNumber(3)

	// 简单测试连接是否正常
	var version string
	err = db.Raw("SELECT VERSION()").Scan(&version).Error
	if err != nil {
		return nil, err
	}

	return db, nil
}

type mLogicRole struct {
	db       *gorm.DB
	redisCli *redisV8.Client
}

// GlobalRedisCli implements LogicRoler.
func (m *mLogicRole) GlobalRedisCli(ctx context.Context) (redisV8.UniversalClient, error) {
	return m.redisCli, nil
}

// GetAppByAppBizID implements LogicRoler.
func (m *mLogicRole) GetAppByAppBizID(ctx context.Context, bID uint64) (*model.AppDB, error) {
	return &model.AppDB{
		ID:     bID,
		CorpID: bID,
	}, nil
}

// Lock implements LogicRoler.
func (m *mLogicRole) Lock(ctx context.Context, key string, duration time.Duration) error {
	return nil
}

// RedisCli implements LogicRoler.
func (m *mLogicRole) RedisCli() redis.Client {
	panic("unimplemented")
}

// UnLock implements LogicRoler.
func (m *mLogicRole) UnLock(ctx context.Context, key string) error {
	return nil
}

// BatchUpdateVector implements LogicRoler.
func (m *mLogicRole) BatchUpdateVector(ctx context.Context, qaIds []uint64, docTaskParams []*model.DocModifyParams) error {
	return nil
}

// RetrieveBaseSharedKnowledge implements LogicRoler.
func (m *mLogicRole) RetrieveBaseSharedKnowledge(ctx context.Context, corpBizID uint64, knowledgeBizIDList []uint64) ([]*model.SharedKnowledgeInfo, error) {
	res := make([]*model.SharedKnowledgeInfo, 0, len(knowledgeBizIDList))
	for _, v := range knowledgeBizIDList {
		res = append(res, &model.SharedKnowledgeInfo{
			BusinessID: v,
			Name:       "test",
		})
	}
	return res, nil
}

// GetQAsByBizIDs implements LogicRoler.
func (m *mLogicRole) GetQAsByBizIDs(ctx context.Context, corpID uint64, robotID uint64, qaBizIDs []uint64, offset uint64, limit uint64) ([]*model.DocQA, error) {
	res := make([]*model.DocQA, 0, len(qaBizIDs))
	for _, v := range qaBizIDs {
		res = append(res, &model.DocQA{
			ID: uint64(v),
		})
	}
	return res, nil
}

// GetCateListByBusinessIDs implements LogicRoler.
func (m *mLogicRole) GetCateListByBusinessIDs(ctx context.Context, t model.CateObjectType, corpID uint64, robotID uint64, cateBizIDs []uint64) (map[uint64]*model.CateInfo, error) {
	res := make(map[uint64]*model.CateInfo, len(cateBizIDs))
	for _, v := range cateBizIDs {
		res[v] = &model.CateInfo{
			ID:   v,
			Name: fmt.Sprintf("cate%d", v),
		}
	}
	return res, nil
}

// GetDocByBizIDs implements LogicRoler.
func (m *mLogicRole) GetDocByBizIDs(ctx context.Context, bizIDs []uint64, appID uint64) (map[uint64]*model.Doc, error) {
	res := make(map[uint64]*model.Doc, len(bizIDs))
	for _, v := range bizIDs {
		res[v] = &model.Doc{
			ID:         v,
			FileName:   fmt.Sprintf("doc%d", v),
			BusinessID: v + 1000,
		}
	}
	return res, nil
}

// GetQAList implements LogicRoler.
func (m *mLogicRole) GetQAList(ctx context.Context, req *model.QAListReq) ([]*model.DocQA, error) {
	res := make([]*model.DocQA, 0, len(req.QABizIDs))
	for _, v := range req.QABizIDs {
		res = append(res, &model.DocQA{
			ID: uint64(v),
		})
	}
	return res, nil
}

// GetAttributeByBizIDs implements LogicRoler.
func (m *mLogicRole) GetAttributeByBizIDs(ctx context.Context, robotID uint64, ids []uint64) (map[uint64]*model.Attribute, error) {
	res := make(map[uint64]*model.Attribute, len(ids))
	for _, v := range ids {
		res[v] = &model.Attribute{
			ID:      v,
			Name:    fmt.Sprintf("attr%d", v),
			AttrKey: fmt.Sprintf("attrKey%d", v),
		}
	}
	return res, nil
}

// GetAttributeLabelByBizIDs implements LogicRoler.
func (m *mLogicRole) GetAttributeLabelByBizIDs(ctx context.Context, ids []uint64, appID uint64) (map[uint64]*model.AttributeLabel, error) {
	res := make(map[uint64]*model.AttributeLabel, len(ids))
	for _, v := range ids {
		res[v] = &model.AttributeLabel{
			ID:   v,
			Name: fmt.Sprintf("label%d", v),
		}
	}
	return res, nil
}

// GetCateByIDs implements LogicRoler.
func (m *mLogicRole) GetCateByIDs(ctx context.Context, t model.CateObjectType, ids []uint64) (map[uint64]*model.CateInfo, error) {
	res := make(map[uint64]*model.CateInfo, len(ids))
	for _, v := range ids {
		res[v] = &model.CateInfo{
			ID:   v,
			Name: fmt.Sprintf("cate%d", v),
		}
	}
	return res, nil
}

var _ LogicRoler = (*mLogicRole)(nil)

func (m *mLogicRole) GetTdsqlGormDB() *gorm.DB {
	return m.db
}

func (m *mLogicRole) GenerateSeqID() uint64 {
	return rand.Uint64()
}

func TestLogicRole_CreateRole(t *testing.T) {
	// 测试数据库连接初始化
	client := NewLogicRole(NewMLogicRole())
	_, err := client.CheckRoleExist(GetContext(), 1, 0, "test")
	t.Logf("CheckRoleExist: %v", err)

	// 随时生成一个pb.CreateRoleReq对象
	req := &pb.CreateRoleReq{
		AppBizId:    "1",
		Name:        "test",
		SearchType:  1,
		Description: "test",
		KnowChoose: []*pb.KnowChoose{
			{
				KnowledgeBizId:    "1",
				KnowledgeName:     "test",
				Type:              1,
				SearchType:        1,
				DocBizIds:         []string{"1"},
				DocCateBizIds:     []string{"2", "3", "4", "20"},
				QuesAnsBizIds:     []string{"5", "6", "7", "28"},
				QuesAnsCateBizIds: []string{"8", "9", "10", "30"},
				Labels: []*pb.ChooseLabel{
					{
						AttrBizId: "1",
						AttrName:  "test",
						Labels: []*pb.ChooseLabel_Label{
							{
								LabelBizId: "1",
								LabelName:  "test",
							},
							{
								LabelBizId: "1",
								LabelName:  "test2",
							},
							{
								LabelBizId: "1",
								LabelName:  "test3",
							},
						},
					},
				},
				Condition: 1,
			},
		},
	}
	b, _ := json.Marshal(req)
	t.Log(string(b))
	client.CreateRole(context.Background(), req, 900330426645947807, model.KnowledgeRoleTypeCustom)
}

func TestLogicRole_DetailRole(t *testing.T) {
	client := NewLogicRole(NewMLogicRole())

	res, err := client.DetailRole(GetContext(), 1, []uint64{900330426645947807, 8207668132097539661, 337845818})
	if err != nil {
		t.Fatal(err)
		return
	}
	b, _ := json.Marshal(res)
	t.Log(string(b))
}

func TestLogicRole_ListKnowledgeRoles(t *testing.T) {
	client := NewLogicRole(NewMLogicRole())
	total, res, err := client.ListKnowledgeRoles(context.Background(),
		&dao.KnowledgeRoleReq{
			KnowledgeBase: dao.KnowledgeBase{
				CorpBizID: 1,
				AppBizID:  1,
				Limit:     1,
				Offset:    0,
			},
			BizIDs: []uint64{},
		},
	)
	if err != nil {
		t.Fatal(err)
		return
	}
	b, _ := json.Marshal(res)
	t.Log(total, string(b))
}

func TestLogicRole_DeleteKnowledge(t *testing.T) {
	client := NewLogicRole(NewMLogicRole())
	err := client.DeleteKnowledgeRole(GetContext(),
		1,
		[]uint64{900330426645947807},
	)
	if err != nil {
		t.Fatal(err)
		return
	}
}

func NewMLogicRole() LogicRoler {
	db, err := InitDB(10, 100, 3600)
	if err != nil {
		log.Fatalln(err)
	}
	redisCli := redisV8.NewClient(&redisV8.Options{
		Addr: "127.0.0.1:6379",
	})
	if redisCli.Ping(context.Background()).Err() != nil {
		fmt.Println("redis connect error")
		log.Fatal(err)
	}
	return &mLogicRole{
		db:       db,
		redisCli: redisCli,
	}
}

func TestLogicRole_ModifyRole(t *testing.T) {

	client := NewLogicRole(NewMLogicRole())
	req := &pb.ModifyReq{
		RoleBizId:   "337845818",
		AppBizId:    "1",
		Name:        "test11",
		SearchType:  2,
		Type:        1,
		Description: "description",
		KnowChoose: []*pb.KnowChoose{
			{
				KnowledgeBizId:    "337845818",
				KnowledgeName:     "test",
				Type:              1,
				SearchType:        2,
				DocBizIds:         []string{"810"},
				DocCateBizIds:     []string{"20", "33210", "40", "50"},
				QuesAnsBizIds:     []string{"5", "23"},
				QuesAnsCateBizIds: []string{"3431", "35", "3516", "37"},
				DbBizIds:          []string{"1", "5431111", "24", "3216", "9"},
				Labels:            []*pb.ChooseLabel{},
				Condition:         0,
			},
			{
				KnowledgeBizId: "3378458189",
				KnowledgeName:  "test",
				Type:           2,
				SearchType:     3,
				Labels: []*pb.ChooseLabel{
					{
						AttrBizId: "1",
						AttrName:  "test",
						Labels: []*pb.ChooseLabel_Label{
							{
								LabelBizId: "4234543",
								LabelName:  "test3",
							},
							{
								LabelBizId: "102914424",
								LabelName:  "tefdsfdst4",
							},
						},
					},
					{
						AttrBizId: "4",
						AttrName:  "attrTest2",
						Labels: []*pb.ChooseLabel_Label{
							{
								LabelBizId: "2205435111",
								LabelName:  "1test2",
							},
							{
								LabelBizId: "20925433214",
								LabelName:  "1test3",
							},
						},
					},
				},

				Condition: 1,
			},
			{
				KnowledgeBizId: "3333333",
				KnowledgeName:  "test",
				Type:           2,
				SearchType:     3,
				Labels: []*pb.ChooseLabel{
					{
						AttrBizId: "888",
						AttrName:  "test",
						Labels: []*pb.ChooseLabel_Label{
							{
								LabelBizId: "32",
								LabelName:  "test2",
							},
						},
					},
					{
						AttrBizId: "999",
						AttrName:  "attrTe",
						Labels: []*pb.ChooseLabel_Label{
							{
								LabelBizId: "23321788576434356",
								LabelName:  "1test2",
							},
						},
					},
				},

				Condition: 1,
			},
		},
	}
	_, syncInfos, err := client.ModifyRole(GetContext(), req, false)
	if err != nil {
		log.Println()
		t.Fatal(err)
		return
	}
	t.Log(syncInfos)
}

func TestLogicRole_CheckRole(t *testing.T) {
	client := NewLogicRole(NewMLogicRole())
	req := &pb.ModifyReq{
		RoleBizId:   "900330426645947807",
		AppBizId:    "1",
		Name:        "test11",
		SearchType:  1,
		Description: "description",
		KnowChoose: []*pb.KnowChoose{
			{
				KnowledgeBizId:    "1",
				KnowledgeName:     "test",
				Type:              1,
				SearchType:        1,
				DocBizIds:         []string{"11", "2", "3311", "44"},
				DocCateBizIds:     []string{"20", "70", "40453", "50"},
				QuesAnsBizIds:     []string{"543243", "6", "7321", "28"},
				QuesAnsCateBizIds: []string{"347675", "34444", "36", "37"},
				Labels: []*pb.ChooseLabel{
					{
						AttrBizId: "1",
						AttrName:  "test",
						Labels: []*pb.ChooseLabel_Label{
							{
								LabelBizId: "1543",
								LabelName:  "test",
							},
							{
								LabelBizId: "221321",
								LabelName:  "test2",
							},
							{
								LabelBizId: "3432432543",
								LabelName:  "test3",
							},
						},
					},
				},
				Condition: 1,
			},
		},
	}
	for _, v := range req.GetKnowChoose() {
		err := client.CheckKnowChoose(GetContext(), 1, v)
		if err != nil {
			t.Fatal(err)
			return
		}
	}
	t.Log("")
}

func TestLogicRole_RemoveKnowledgeAssociation(t *testing.T) {

	client := NewLogicRole(NewMLogicRole())
	err := client.RemoveKnowledgeAssociation(GetContext(),
		1, []uint64{2},
	)
	if err != nil {
		t.Fatal(err)
		return
	}
}

func TestLogicRole_checkPresetRole(t *testing.T) {
	client := NewLogicRole(NewMLogicRole())
	_, _, err := client.CheckPresetRole(GetContext(),
		1,
	)
	if err != nil {
		t.Fatal(err)
		return
	}
}

func GetContext() context.Context {
	ctx := pkg.WithCorpBizID(context.Background(), 1)
	ctx = pkg.WithAppID(ctx, 1)
	return ctx
}

func TestRoleFormat(t *testing.T) {
	client := NewLogicRole(NewMLogicRole())

	res, _, err := client.FormatFilter(GetContext(), &FormatFilterReq{
		AppID:                1,
		CorpBizID:            1,
		AppBizID:             1,
		RoleBizID:            337845818,
		CateKey:              "cate_key",
		RoleKey:              "role_key",
		FullLabelValue:       "default",
		GeneralVectorAttrKey: "general_vector_attr_key",
	})
	if err != nil {
		t.Fatal(err)
		return
	}
	for k, v := range res {
		// t.Log(k, v)
		b, _ := json.Marshal(v)
		fmt.Println(k, string(b))
	}
}

func RandModifyReq() *pb.ModifyReq {
	res := &pb.ModifyReq{}
	res.AppBizId = "1"
	res.RoleBizId = "9527" // cast.ToString(rand.UintN(1000))
	res.Type = uint32(rand.UintN(3) + 1)
	res.SearchType = uint32(rand.UintN(3) + 1)
	res.Description = "description"
	res.Name = "role_name"
	res.KnowChoose = make([]*pb.KnowChoose, 0, 4)
	for i := 0; i < int(rand.UintN(5)+1); i++ {
		choose := &pb.KnowChoose{
			KnowledgeBizId: cast.ToString(rand.UintN(10000)),
			Type:           uint32(rand.UintN(10) + 1),
			SearchType:     uint32(rand.UintN(10) + 1),
		}
		choose.DocBizIds = make([]string, 0, 4)
		choose.DocCateBizIds = make([]string, 0, 4)
		choose.QuesAnsBizIds = make([]string, 0, 4)
		choose.QuesAnsCateBizIds = make([]string, 0, 4)
		choose.DbBizIds = make([]string, 0, 4)
		for i := 0; i < int(rand.UintN(5)+4); i++ {
			choose.DocBizIds = append(choose.DocBizIds, cast.ToString(rand.UintN(100000)))
			choose.DocCateBizIds = append(choose.DocCateBizIds, cast.ToString(rand.UintN(10000)))
			choose.QuesAnsBizIds = append(choose.QuesAnsBizIds, cast.ToString(rand.UintN(100000)))
			choose.QuesAnsCateBizIds = append(choose.QuesAnsCateBizIds, cast.ToString(rand.UintN(10000)))
			choose.DbBizIds = append(choose.DbBizIds, cast.ToString(rand.UintN(100000)))
		}
		choose.Labels = make([]*pb.ChooseLabel, 0, 4)
		for i := 0; i < int(rand.UintN(5)+1); i++ {
			label := &pb.ChooseLabel{
				AttrBizId: cast.ToString(rand.UintN(100000)),
			}
			label.Labels = make([]*pb.ChooseLabel_Label, 0, 4)
			for i := 0; i < int(rand.UintN(5)+1); i++ {
				label.Labels = append(label.Labels, &pb.ChooseLabel_Label{
					LabelBizId: cast.ToString(rand.UintN(100000)),
				})
			}
			choose.Labels = append(choose.Labels, label)
		}
		res.KnowChoose = append(res.KnowChoose, choose)

	}
	return res
}

func isEqual(source *pb.ModifyReq, target *pb.RoleInfo) bool {
	source.Name = ""
	target.Name = ""
	source.Description = ""
	target.Description = ""
	for _, know := range source.KnowChoose {
		know.KnowledgeName = ""
		for _, attr := range know.GetLabels() {
			attr.AttrName = ""
			for _, val := range attr.Labels {
				val.LabelName = ""
			}
		}
	}

	for _, know := range target.KnowChoose {
		know.KnowledgeName = ""
		for _, attr := range know.GetLabels() {
			attr.AttrName = ""
			for _, val := range attr.Labels {
				val.LabelName = ""
			}
		}
	}
	if source.AppBizId != cast.ToString(target.AppBizId) {
		return false
	}
	if source.RoleBizId != cast.ToString(target.RoleBizId) {
		return false
	}
	if source.Type != uint32(target.Type) {
		return false
	}
	if source.SearchType != uint32(target.SearchType) {
		return false
	}

	b, _ := json.Marshal(source.KnowChoose)
	b2, _ := json.Marshal(target.KnowChoose)
	fmt.Println(string(b))
	fmt.Println("----------------------")
	fmt.Println(string(b2))
	return len(b) == len(b2)
}

func TestBatchModifyRole(t *testing.T) {
	for i := 0; i < 5; i++ {
		req := RandModifyReq()
		client := NewLogicRole(NewMLogicRole())
		_, syncInfo, _ := client.ModifyRole(GetContext(), req, true)
		roles, err := client.DetailRole(GetContext(), cast.ToUint64(req.AppBizId), []uint64{cast.ToUint64(req.RoleBizId)})
		if err != nil || len(roles) == 0 {
			t.Fatal(req.AppBizId, req.RoleBizId, err)
			return
		}

		log.Println(syncInfo)
		role := roles[0].RoleInfo
		role.UpdateTime = 0
		role.CreateTime = 0
		equal := isEqual(req, role)
		if !equal {
			t.Fatal(req, role)
			return
		}
		t.Log(req.RoleBizId, equal)
	}
}
