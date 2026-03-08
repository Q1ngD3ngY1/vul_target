package config

import (
	"fmt"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/baicaoyuan/moss/configx"
)

const (
	dbSourceDecodeKey = "db_source_decode_key"
)

var (
	dbSourceDecodePub = ""
)

func initDbSourceConfig() {
	configx.MustWatch(dbSourceDecodeKey, dbSourceDecodePub)
	dbSourceConfig := configx.MustGetWatched(dbSourceDecodeKey).(string)
	log.Info("\n\n--------------------------------------------------------------------------------\n" +
		fmt.Sprintf("DbSourceConfig: %+v\n", dbSourceConfig) +
		"================================================================================")
}

func GetDbSourceConfig() string {
	return configx.MustGetWatched(dbSourceDecodeKey).(string)
}
