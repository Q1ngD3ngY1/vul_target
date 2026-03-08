// Package api 业务逻辑层
package api

import (
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	Permis "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/permissions"
)

// Service is service logic object
type Service struct {
	dao         dao.Dao
	permisLogic Permis.PermisLogic
}

// New creates service instance
func New() *Service {
	d := dao.New()
	logic := Permis.NewPermisLogic(d)
	srv := Service{
		dao:         d,
		permisLogic: logic,
	}
	return &srv
}
