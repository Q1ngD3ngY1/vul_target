package dao

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"git.code.oa.com/trpc-go/trpc-database/mysql"
	"git.code.oa.com/trpc-go/trpc-go/client"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	// shutdown()
	os.Exit(code)
}

func setup() {
	var app config.Application
	b, err := os.ReadFile("../../config/application.yaml")
	if err != nil {
		// panic(err)
	}
	err = yaml.Unmarshal(b, &app)
	if err != nil {
		// panic(err)
	}
	config.SetApp(app)

	_ = client.RegisterClientConfig("unit_test", &client.BackendConfig{
		Callee: "unit_test",
		Target: os.Getenv("UNIT_TEST_QBOT_DB_TARGET"),
	})
}

func Test_dao_GetWaitEmbeddingUpgradeApp(t *testing.T) {
	d := &dao{db: mysql.NewClientProxy("unit_test")}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := d.db.Exec(ctx, `TRUNCATE t_robot`)
	require.NoError(t, err)
	fromVer := uint64(1)
	toVer := uint64(2)
	needUpgrade, err := jsoniter.MarshalToString(config.RobotEmbedding{Version: fromVer, UpgradeVersion: fromVer})
	require.NoError(t, err)
	notNeedUpgrade, err := jsoniter.MarshalToString(config.RobotEmbedding{Version: toVer, UpgradeVersion: toVer})
	require.NoError(t, err)
	upgrading, err := jsoniter.MarshalToString(config.RobotEmbedding{Version: fromVer, UpgradeVersion: toVer})
	require.NoError(t, err)
	now := time.Now()
	_, err = d.db.NamedExec(ctx, fmt.Sprintf(createApp, appFields), []model.AppDB{
		{AppKey: "a", BusinessID: 0, Embedding: needUpgrade, IsDeleted: model.AppIsNotDeleted,
			ExpireTime: now, CreateTime: now, UpdateTime: now},
		{AppKey: "b", BusinessID: 1, Embedding: needUpgrade, IsDeleted: model.AppIsNotDeleted,
			ExpireTime: now, CreateTime: now, UpdateTime: now},
		{AppKey: "c", BusinessID: 2, Embedding: notNeedUpgrade, IsDeleted: model.AppIsNotDeleted,
			ExpireTime: now, CreateTime: now, UpdateTime: now,
		},
		{AppKey: "d", BusinessID: 3, Embedding: upgrading, IsDeleted: model.AppIsNotDeleted,
			ExpireTime: now, CreateTime: now, UpdateTime: now,
		},
	})
	require.NoError(t, err)
	apps, err := d.GetWaitEmbeddingUpgradeApp(ctx, []uint64{}, fromVer, toVer)
	require.NoError(t, err)
	require.Equal(t, 1, len(apps))
	require.Equal(t, "b", apps[0].AppKey)
}

func Test_dao_StartEmbeddingUpgradeApp(t *testing.T) {
	d := &dao{db: mysql.NewClientProxy("unit_test")}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := d.db.Exec(ctx, `TRUNCATE t_robot`)
	require.NoError(t, err)
	fromVer := uint64(1)
	toVer := uint64(2)
	needUpgrade, err := jsoniter.MarshalToString(config.RobotEmbedding{Version: fromVer, UpgradeVersion: fromVer})
	require.NoError(t, err)
	notNeedUpgrade, err := jsoniter.MarshalToString(config.RobotEmbedding{Version: toVer, UpgradeVersion: toVer})
	require.NoError(t, err)
	now := time.Now()
	_, err = d.db.NamedExec(ctx, fmt.Sprintf(createApp, appFields), []model.AppDB{
		{AppKey: "a", BusinessID: 0, Embedding: needUpgrade, IsDeleted: model.AppIsNotDeleted,
			ExpireTime: now, CreateTime: now, UpdateTime: now},
	})
	_, err = d.db.NamedExec(ctx, fmt.Sprintf(createApp, appFields), []model.AppDB{
		{AppKey: "b", BusinessID: 1, Embedding: notNeedUpgrade, IsDeleted: model.AppIsNotDeleted,
			ExpireTime: now, CreateTime: now, UpdateTime: now},
	})
	require.NoError(t, err)
	t.Run("upgrade", func(t *testing.T) {
		err = d.StartEmbeddingUpgradeApp(ctx, 1, fromVer, toVer)
		require.NoError(t, err)
		app, err := d.GetAppByID(ctx, 1)
		require.NoError(t, err)
		emb, _, err := app.GetEmbeddingConf()
		require.NoError(t, err)
		require.Equal(t, fromVer, emb.Version)
		require.Equal(t, toVer, emb.UpgradeVersion)
	})
	t.Run("can not upgrade", func(t *testing.T) {
		err = d.StartEmbeddingUpgradeApp(ctx, 2, fromVer, toVer)
		require.ErrorContains(t, err, "embedding 版本不匹配")
	})
}

func Test_dao_FinishEmbeddingUpgradeApp(t *testing.T) {
	d := &dao{db: mysql.NewClientProxy("unit_test")}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := d.db.Exec(ctx, `TRUNCATE t_robot`)
	require.NoError(t, err)
	fromVer := uint64(1)
	toVer := uint64(2)
	upgrading, err := jsoniter.MarshalToString(config.RobotEmbedding{Version: fromVer, UpgradeVersion: toVer})
	require.NoError(t, err)
	upgraded, err := jsoniter.MarshalToString(config.RobotEmbedding{Version: toVer, UpgradeVersion: toVer})
	require.NoError(t, err)
	now := time.Now()
	_, err = d.db.NamedExec(ctx, fmt.Sprintf(createApp, appFields), []model.AppDB{
		{AppKey: "a", BusinessID: 0, Embedding: upgrading, IsDeleted: model.AppIsNotDeleted,
			ExpireTime: now, CreateTime: now, UpdateTime: now},
	})
	_, err = d.db.NamedExec(ctx, fmt.Sprintf(createApp, appFields), []model.AppDB{
		{AppKey: "b", BusinessID: 1, Embedding: upgraded, IsDeleted: model.AppIsNotDeleted,
			ExpireTime: now, CreateTime: now, UpdateTime: now},
	})
	require.NoError(t, err)
	t.Run("finish", func(t *testing.T) {
		err = d.StartEmbeddingUpgradeApp(ctx, 1, fromVer, toVer)
		require.NoError(t, err)
		app, err := d.GetAppByID(ctx, 1)
		require.NoError(t, err)
		emb, _, err := app.GetEmbeddingConf()
		require.NoError(t, err)
		require.Equal(t, fromVer, emb.Version)
		require.Equal(t, toVer, emb.UpgradeVersion)
	})
	t.Run("can not finish", func(t *testing.T) {
		err = d.StartEmbeddingUpgradeApp(ctx, 2, fromVer, toVer)
		require.ErrorContains(t, err, "embedding 版本不匹配")
	})
}
