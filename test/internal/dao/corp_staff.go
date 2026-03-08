package dao

import (
	"context"
	"fmt"

	"git.woa.com/adp/common/x/logx"
	"github.com/redis/go-redis/v9"

	"git.woa.com/adp/common/x/encodingx/jsonx"
	"git.woa.com/adp/common/x/errx"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
)

func sessionKey(token string) string {
	return fmt.Sprintf("qbot:admin:session:%s", token)
}

// GetStaffSession 获取员工session
func (d *dao) GetStaffSession(ctx context.Context, token string) (*entity.Session, error) {
	key := sessionKey(token)
	// replyVec, err := redis.String(d.redis.Do(ctx, "GET", key))
	replyVec, err := d.adminRdb.Get(ctx, key).Result()
	if err != nil {
		if errx.Is(err, redis.Nil) {
			logx.W(ctx, "获取员工session redis 为空 key:%s replyVec:%s", key, replyVec)
			return nil, nil
		}
		logx.E(ctx, "获取员工session失败 key:%s replyVec:%s err:%+v", key, replyVec, err)
		return nil, err
	}

	session := &entity.Session{}
	if err = jsonx.UnmarshalFromString(replyVec, session); err != nil {
		logx.E(ctx, "员工session解析失败 key:%s replyVec:%s err:%+v", key, replyVec, err)
		return nil, err
	}
	if session.IsEmpty() {
		return nil, errs.ErrSessionNotFound
	}
	session.SID = entity.CloudSID
	return session, nil
}
