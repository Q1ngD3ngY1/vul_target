package dao

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"git.code.oa.com/trpc-go/trpc-database/mysql"
	"git.code.oa.com/trpc-go/trpc-go/client"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	retrieval "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_retrieval_server"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
	"gopkg.in/yaml.v3"
)

func Test_dao_GetAttributes(t *testing.T) {
	var app config.Application
	b, err := os.ReadFile("../../config/application.yaml")
	require.NoError(t, err)
	err = yaml.Unmarshal(b, &app)
	require.NoError(t, err)
	config.SetApp(app)
	_ = client.RegisterClientConfig("unit_test", &client.BackendConfig{
		Callee: "unit_test",
		Target: os.Getenv("trpc.qbot.admin.unit_test.target"),
	})
	d := &dao{db: mysql.NewClientProxy("unit_test")}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = d.db.Exec(ctx, `TRUNCATE t_attribute`)
	require.NoError(t, err)
	_, err = d.db.Exec(ctx, `INSERT INTO t_attribute (id, business_id, robot_id, attr_key, name, is_updating,
is_deleted, deleted_time, create_time, update_time) VALUES
(1, 1, 1, 'city', '城市', 0, 0, 0, '2024-02-23 10:47:04', '2024-02-23 17:06:38'),
(2, 2, 2, 'city', '城市', 0, 0, 0, '2024-02-23 10:47:04', '2024-02-23 17:06:38'),
(3, 3, 1, 'brand', '品牌', 0, 0, 0, '2024-03-07 15:09:21', '2024-03-07 15:09:21'),
(4, 4, 2, 'brand', '品牌', 0, 0, 0, '2024-03-07 15:09:21', '2024-03-07 15:09:21'),
(5, 5, 1, 'color', '颜色', 0, 0, 0, '2024-03-07 15:09:21', '2024-03-07 15:09:21'),
(6, 6, 2, 'color', '颜色', 0, 0, 0, '2024-03-07 15:09:21', '2024-03-07 15:09:21'),
(7, 7, 1, 'deleted', '已删除', 0, 1, 0, '2024-03-07 15:09:21', '2024-03-07 15:09:21'),
(8, 8, 2, 'deleted', '已删除', 0, 1, 0, '2024-03-07 15:09:21', '2024-03-07 15:09:21')`)
	require.NoError(t, err)
	_, err = d.db.Exec(ctx, `TRUNCATE t_attribute_label`)
	require.NoError(t, err)
	_, err = d.db.Exec(ctx, `INSERT INTO t_attribute_label (id, business_id, attr_id, name, similar_label,
	is_deleted, create_time, update_time) VALUES
(1, 1, 1, 'shanghai', '', 0, '2024-02-23 10:47:04', '2024-02-23 10:47:04'),
(2, 2, 2, 'changsha', '', 0, '2024-02-23 15:48:56', '2024-02-23 15:48:56'),
(3, 3, 1, 'chongqing', '', 0, '2024-02-23 15:48:56', '2024-02-23 15:48:56'),
(4, 4, 5, 'green', '', 0, '2024-02-23 17:06:38', '2024-02-23 17:06:38'),
(5, 5, 5, 'red', '', 0, '2024-02-23 17:06:38', '2024-02-23 17:06:38'),
(6, 6, 6, 'red', '', 0, '2024-02-23 17:06:38', '2024-02-23 17:06:38'),
(7, 7, 6, 'green', '', 0, '2024-03-07 15:09:21', '2024-03-07 15:09:21'),
(8, 8, 3, 'coke', '', 0, '2024-03-07 20:03:40', '2024-03-07 20:03:40'),
(9, 9, 3, 'pepsi', '', 0, '2024-03-07 20:03:41', '2024-03-07 20:03:41'),
(10, 10, 4, 'coke', '', 0, '2024-03-07 20:03:41', '2024-03-07 20:03:41'),
(11, 11, 4, 'pepsi', '', 0, '2024-03-07 20:03:41', '2024-03-07 20:03:41')`)
	require.NoError(t, err)
	_, err = d.db.Exec(ctx, `TRUNCATE t_doc_attribute_label`)
	require.NoError(t, err)
	_, err = d.db.Exec(ctx, `INSERT INTO t_doc_attribute_label (id, robot_id, doc_id, source, attr_id, label_id,
	is_deleted, create_time, update_time) VALUES
(1, 1, 1, 1, 1, 1, 0, '2024-02-23 16:23:31', '2024-02-23 16:23:31'),
(2, 1, 1, 1, 3, 8, 0, '2024-02-23 16:23:54', '2024-02-23 16:23:54'),
(3, 1, 1, 1, 3, 9, 1, '2024-02-23 16:24:10', '2024-02-23 16:24:10'),
(4, 1, 1, 1, 5, 5, 0, '2024-02-23 17:07:21', '2024-02-23 17:07:21'),
(5, 1, 2, 1, 1, 1, 0, '2024-02-23 16:23:31', '2024-02-23 16:23:31'),
(6, 1, 2, 1, 3, 8, 0, '2024-02-23 16:23:54', '2024-02-23 16:23:54'),
(7, 1, 2, 1, 3, 9, 0, '2024-02-23 16:24:10', '2024-02-23 16:24:10'),
(8, 1, 2, 1, 5, 5, 0, '2024-02-23 17:07:21', '2024-02-23 17:07:21'),
(9, 1, 2, 1, 5, 4, 0, '2024-02-23 17:07:21', '2024-02-23 17:07:21')`)
	require.NoError(t, err)
	_, err = d.db.Exec(ctx, `TRUNCATE t_qa_attribute_label`)
	require.NoError(t, err)
	_, err = d.db.Exec(ctx, `INSERT INTO t_qa_attribute_label (id, robot_id, qa_id, source, attr_id, label_id,
	is_deleted, create_time, update_time) VALUES
(1, 1, 2, 1, 1, 1, 0, '2024-02-23 16:23:31', '2024-02-23 16:23:31'),
(2, 1, 2, 1, 3, 8, 0, '2024-02-23 16:23:54', '2024-02-23 16:23:54'),
(3, 1, 2, 1, 3, 9, 1, '2024-02-23 16:24:10', '2024-02-23 16:24:10'),
(4, 1, 2, 1, 5, 5, 0, '2024-02-23 17:07:21', '2024-02-23 17:07:21'),
(5, 1, 1, 1, 1, 1, 0, '2024-02-23 16:23:31', '2024-02-23 16:23:31'),
(6, 1, 1, 1, 3, 8, 0, '2024-02-23 16:23:54', '2024-02-23 16:23:54'),
(7, 1, 1, 1, 3, 9, 0, '2024-02-23 16:24:10', '2024-02-23 16:24:10'),
(8, 1, 1, 1, 5, 0, 0, '2024-02-23 17:07:21', '2024-02-23 17:07:21')`)
	require.NoError(t, err)
	_, err = d.db.Exec(ctx, `TRUNCATE t_doc`)
	require.NoError(t, err)
	_, err = d.db.Exec(ctx, `INSERT INTO t_doc (id, business_id, robot_id, corp_id, staff_id, create_user_id,
file_name, file_type, file_size, bucket, cos_url, cos_hash, message, status, is_refer, refer_url_type,
is_deleted, source, web_url, batch_id, audit_flag, is_creating_qa, is_created_qa, is_creating_index,
next_action, attr_range, create_time, update_time, char_size, expire_start, expire_end) VALUES
(1, 1, 1, 1, 1, 1, 'name1', 'docx', 1, 'bucket', 'cos', 'hash', '', 10, 0, 0, 0, 0, '', 1, 0, 0, 0, 0, 0, 1,
'2024-03-08 10:29:07', '2024-03-08 10:29:07', 0, '1970-01-01 08:00:00', '1970-01-01 08:00:00'),
(2, 2, 1, 1, 1, 1, 'name2', 'docx', 1, 'bucket', 'cos', 'hash', '', 10, 0, 0, 0, 0, '', 1, 0, 0, 0, 0, 0, 2,
'2024-03-08 10:29:08', '2024-03-08 10:29:08', 0, '1970-01-01 08:00:00', '1970-01-01 08:00:00')`)
	require.NoError(t, err)
	attributes, err := d.GetAttributes(ctx, 1, []model.LabelAble{
		&model.Doc{ID: 1, RobotID: 1, AttrRange: model.AttrRangeCondition},
		&model.Doc{ID: 1, RobotID: 1, AttrRange: model.AttrRangeAll},
		&model.DocQA{ID: 1, RobotID: 1, OriginDocID: 1, AttrRange: model.AttrRangeDefault, Source: model.SourceFromDoc},
		&model.DocQA{ID: 1, RobotID: 1, OriginDocID: 1, AttrRange: model.AttrRangeAll, Source: model.SourceFromBatch},
		&model.DocQA{ID: 1, RobotID: 1, OriginDocID: 1, AttrRange: model.AttrRangeCondition, Source: model.SourceFromBatch},
		&model.DocQA{ID: 2, RobotID: 1, OriginDocID: 2, AttrRange: model.AttrRangeDefault, Source: model.SourceFromDoc},
		&model.DocQA{ID: 2, RobotID: 1, OriginDocID: 2, AttrRange: model.AttrRangeAll, Source: model.SourceFromBatch},
		&model.DocQA{ID: 2, RobotID: 1, OriginDocID: 2, AttrRange: model.AttrRangeCondition, Source: model.SourceFromBatch},
		&model.Doc{ID: 2, RobotID: 1, AttrRange: model.AttrRangeCondition},
		&model.Doc{ID: 2, RobotID: 1, AttrRange: model.AttrRangeAll},
	})
	var got [][]*retrieval.VectorLabel
	for _, v := range attributes {
		labels := v.ToVectorLabels()
		slices.SortFunc(labels, func(a, b *retrieval.VectorLabel) int {
			if strings.Compare(a.Name, b.Name) == 0 {
				return strings.Compare(a.Value, b.Value)
			}
			return strings.Compare(a.Name, b.Name)
		})
		got = append(got, labels)
	}
	want := [][]*retrieval.VectorLabel{
		{
			{Name: "brand", Value: "coke"},
			{Name: "city", Value: "shanghai"},
			{Name: "color", Value: "red"},
		},
		{
			{Name: config.App().AttributeLabel.GeneralVectorAttrKey, Value: config.App().AttributeLabel.FullLabelValue},
		},
		{
			{Name: config.App().AttributeLabel.GeneralVectorAttrKey, Value: config.App().AttributeLabel.FullLabelValue},
		},
		{
			{Name: config.App().AttributeLabel.GeneralVectorAttrKey, Value: config.App().AttributeLabel.FullLabelValue},
		},
		{
			{Name: "brand", Value: "coke"},
			{Name: "brand", Value: "pepsi"},
			{Name: "city", Value: "shanghai"},
			{Name: "color", Value: config.App().AttributeLabel.FullLabelValue},
		},
		{
			{Name: "brand", Value: "coke"},
			{Name: "brand", Value: "pepsi"},
			{Name: "city", Value: "shanghai"},
			{Name: "color", Value: "green"},
			{Name: "color", Value: "red"},
		},
		{
			{Name: config.App().AttributeLabel.GeneralVectorAttrKey, Value: config.App().AttributeLabel.FullLabelValue},
		},
		{
			{Name: "brand", Value: "coke"},
			{Name: "city", Value: "shanghai"},
			{Name: "color", Value: "red"},
		},
		{
			{Name: "brand", Value: "coke"},
			{Name: "brand", Value: "pepsi"},
			{Name: "city", Value: "shanghai"},
			{Name: "color", Value: "green"},
			{Name: "color", Value: "red"},
		},
		{
			{Name: config.App().AttributeLabel.GeneralVectorAttrKey, Value: config.App().AttributeLabel.FullLabelValue},
		},
	}
	require.EqualValues(t, len(want), len(got))
	for i, x := range want {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			for j, v := range x {
				require.EqualValues(t, v, got[i][j])
			}
		})
	}
}
