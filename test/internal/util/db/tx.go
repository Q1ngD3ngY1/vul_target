// Package db TODO
// @Author: halelv
// @Date: 2023/12/21 19:58
package db

import (
	"context"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"gorm.io/gorm"
)

// BeginDBTx 开启DB事物
func BeginDBTx(ctx context.Context, db *gorm.DB) (tx *gorm.DB) {
	log.InfoContextf(ctx, "BeginDBTx...")
	return db.Begin().Debug()
}

// CommitOrRollbackTx 提交或回滚DB事物
func CommitOrRollbackTx(ctx context.Context, tx *gorm.DB, err error) (txErr error) {
	log.InfoContextf(ctx, "CommitOrRollbackTx err:%v", err)
	if err != nil {
		log.InfoContextf(ctx, "RollbackTx...")
		txErr = tx.Rollback().Error
	} else {
		log.InfoContextf(ctx, "CommitTx...")
		txErr = tx.Commit().Error
	}
	if txErr != nil {
		log.ErrorContextf(ctx, "CommitOrRollbackTx txErr:%v", txErr)
		return txErr
	}
	return nil
}
