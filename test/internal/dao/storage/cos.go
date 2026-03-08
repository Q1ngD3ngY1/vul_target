package storage

import (
	"context"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"

	"git.code.oa.com/trpc-go/trpc-database/cos"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"github.com/spf13/cast"
	sts "github.com/tencentyun/qcloud-cos-sts-sdk/go"
)

type cosCli struct {
	cfg         config.Cos
	cosCli      cos.Client
	innerCosCli cos.Client
}

type cosClient struct {
	cosMap map[string]*cosCli
}

const defaultCosKey = "offline"

// newCOS creates cos instance
func newCOS() *cosClient {
	cosMap := make(map[string]*cosCli)
	for key, value := range config.App().Storage.CosMap {
		log.Infof("key:%s", key)
		cfg := value
		cosMap[key] = &cosCli{
			cfg: cfg,
			cosCli: cos.New("trpc.http.cos.Access", cos.Conf{
				Bucket:    strings.TrimRight(cfg.Bucket, "-"+cfg.AppID),
				AppID:     cfg.AppID,
				SecretID:  cfg.SecretID,
				SecretKey: cfg.SecretKey,
				Region:    cfg.Region,
				Domain:    cfg.Domain,
			}),
			innerCosCli: cos.New("trpc.http.cos.Access", cos.Conf{
				Bucket:    strings.TrimRight(cfg.Bucket, "-"+cfg.AppID),
				AppID:     cfg.AppID,
				SecretID:  cfg.SecretID,
				SecretKey: cfg.SecretKey,
				Region:    cfg.Region,
				Domain:    cfg.Domain,
				Prefix:    "cos-internal",
			}),
		}
	}
	if _, ok := cosMap[defaultCosKey]; !ok {
		err := fmt.Errorf("empty default config")
		log.Fatalf("create cos client failed, err:%+v", err)
		panic(err)
	}
	log.Infof("newCOS|len(cosMap):%d", len(cosMap))
	return &cosClient{
		cosMap: cosMap,
	}
}

// GetDomain 获取domain
func (c *cosClient) GetDomain(ctx context.Context) string {
	return c.cosMap[defaultCosKey].cfg.Domain
}

// GetType 获取对象存储类型
func (c *cosClient) GetType(ctx context.Context) string {
	return model.StorageTypeCOS
}

// GetBucket 获取存储桶
func (c *cosClient) GetBucket(ctx context.Context) string {
	return c.cosMap[defaultCosKey].cfg.Bucket
}

// GetRegion 获取存储桶地域
func (c *cosClient) GetRegion(ctx context.Context) string {
	return c.cosMap[defaultCosKey].cfg.Region
}

// GetCredential 获取临时密钥
func (c *cosClient) GetCredential(ctx context.Context, pathList []string, storageAction string) (
	*model.CredentialResult, error) {
	resource := make([]string, 0)
	for _, path := range pathList {
		// e.g. qcs::cos:{region}:uid/{appid}:{bucket}/{path}
		res := "qcs::cos:" + c.cosMap[defaultCosKey].cfg.Region +
			":uid/" + c.cosMap[defaultCosKey].cfg.AppID + ":" + c.cosMap[defaultCosKey].cfg.Bucket + path
		if len(filepath.Ext(path)) == 0 {
			// e.g. qcs::cos:{region}:uid/{appid}:{bucket}/{path}/*
			res += "*"
		}
		resource = append(resource, res)
	}
	opt := &sts.CredentialOptions{
		DurationSeconds: int64(c.cosMap[defaultCosKey].cfg.CredentialTime.Seconds()),
		Region:          c.cosMap[defaultCosKey].cfg.Region,
		Policy: &sts.CredentialPolicy{Statement: []sts.CredentialPolicyStatement{{
			Action:   model.GetStorageCosAction(storageAction),
			Effect:   "allow",
			Resource: resource,
		}}},
	}

	r, err := sts.NewClient(c.cosMap[defaultCosKey].cfg.SecretID,
		c.cosMap[defaultCosKey].cfg.SecretKey, nil).GetCredential(opt)
	if err != nil {
		log.ErrorContextf(ctx, "Get cos credential error: %+v, opt: %+v", err, opt)
		return nil, err
	}

	return &model.CredentialResult{
		Credentials: &model.Credentials{
			TmpSecretID:  r.Credentials.TmpSecretID,
			TmpSecretKey: r.Credentials.TmpSecretKey,
			SessionToken: r.Credentials.SessionToken,
		},
		ExpiredTime: int64(r.ExpiredTime),
		StartTime:   int64(r.StartTime),
	}, nil
}

// GetPreSignedURL 获取 COS 预签名 URL
func (c *cosClient) GetPreSignedURL(ctx context.Context, key string) (string, error) {
	url, err := c.cosMap[defaultCosKey].cosCli.GetPreSignedURL(
		// NOCA:CLOUDRISK(key由业务代码生成)
		ctx, key, http.MethodGet, c.cosMap[defaultCosKey].cfg.ExpireTime)
	if err != nil {
		log.ErrorContextf(ctx, "获取 COS 预签名 URL 失败 key: %s err: %+v", key, err)
		return "", err
	}
	return url, nil
}

// GetObject 获取 COS 文件
func (c *cosClient) GetObject(ctx context.Context, key string) ([]byte, error) {
	object, err := c.cosMap[defaultCosKey].innerCosCli.GetObject(ctx, key)
	if err != nil {
		log.ErrorContextf(ctx, "获取 COS 文件失败 key: %s err: %+v", key, err)
		return nil, err
	}
	return object, nil
}

// PutObject 上传 COS 文件
func (c *cosClient) PutObject(ctx context.Context, bs []byte, key string) error {
	if _, err := c.cosMap[defaultCosKey].innerCosCli.PutObject(ctx, bs, key); err != nil {
		log.ErrorContextf(ctx, "上传 COS 文件失败 key: %s, len: %d, err: %+v", key, len(bs), err)
		return err
	}
	return nil
}

// DelObject 删除 COS 文件
func (c *cosClient) DelObject(ctx context.Context, key string) error {
	if err := c.cosMap[defaultCosKey].innerCosCli.DelObject(ctx, key); err != nil {
		log.ErrorContextf(ctx, "删除 COS 文件失败 key: %s, err: %+v", key, err)
		return err
	}
	return nil
}

// StatObject 获取object的元数据信息
func (c *cosClient) StatObject(ctx context.Context, key string) (*model.ObjectInfo, error) {
	h, err := c.cosMap[defaultCosKey].cosCli.HeadObject(ctx, key)
	if err != nil {
		log.WarnContextf(ctx, "获取 COS 文件元数据失败 key: %s, err: %+v", key, err)
		return nil, err
	}
	return &model.ObjectInfo{
		Hash: h.Get("x-cos-hash-crc64ecma"),
		ETag: h.Get("ETag"),
		Size: cast.ToInt64(h.Get("Content-Length")),
	}, nil
}

// GetDomainWithTypeKey 获取domain
func (c *cosClient) GetDomainWithTypeKey(ctx context.Context, typeKey string) (string, error) {
	if err := c.checkTypeKeyExist(ctx, typeKey); err != nil {
		return "", err
	}
	return c.cosMap[typeKey].cfg.Domain, nil
}

// GetTypeWithTypeKey 获取对象存储类型
func (c *cosClient) GetTypeWithTypeKey(ctx context.Context, typeKey string) (string, error) {
	if err := c.checkTypeKeyExist(ctx, typeKey); err != nil {
		return "", err
	}
	return model.StorageTypeCOS, nil
}

// GetBucketWithTypeKey 获取存储桶
func (c *cosClient) GetBucketWithTypeKey(ctx context.Context, typeKey string) (string, error) {
	if err := c.checkTypeKeyExist(ctx, typeKey); err != nil {
		return "", err
	}
	return c.cosMap[typeKey].cfg.Bucket, nil
}

// GetRegionWithTypeKey 获取存储桶地域
func (c *cosClient) GetRegionWithTypeKey(ctx context.Context, typeKey string) (string, error) {
	if err := c.checkTypeKeyExist(ctx, typeKey); err != nil {
		return "", err
	}
	return c.cosMap[typeKey].cfg.Region, nil
}

// GetCredentialWithTypeKey 获取临时密钥
func (c *cosClient) GetCredentialWithTypeKey(ctx context.Context, typeKey string, pathList []string,
	storageAction string) (*model.CredentialResult, error) {
	if err := c.checkTypeKeyExist(ctx, typeKey); err != nil {
		return nil, err
	}
	resource := make([]string, 0)
	for _, path := range pathList {
		// e.g. qcs::cos:{region}:uid/{appid}:{bucket}/{path}
		res := "qcs::cos:" + c.cosMap[typeKey].cfg.Region +
			":uid/" + c.cosMap[typeKey].cfg.AppID + ":" + c.cosMap[typeKey].cfg.Bucket + path
		if len(filepath.Ext(path)) == 0 {
			// e.g. qcs::cos:{region}:uid/{appid}:{bucket}/{path}/*
			res += "*"
		}
		resource = append(resource, res)
	}
	opt := &sts.CredentialOptions{
		DurationSeconds: int64(c.cosMap[typeKey].cfg.CredentialTime.Seconds()),
		Region:          c.cosMap[typeKey].cfg.Region,
		Policy: &sts.CredentialPolicy{Statement: []sts.CredentialPolicyStatement{{
			Action:   model.GetStorageCosAction(storageAction),
			Effect:   "allow",
			Resource: resource,
		}}},
	}

	r, err := sts.NewClient(c.cosMap[typeKey].cfg.SecretID,
		c.cosMap[typeKey].cfg.SecretKey, nil).GetCredential(opt)
	if err != nil {
		log.ErrorContextf(ctx, "Get cos credential error: %+v, opt: %+v", err, opt)
		return nil, err
	}

	return &model.CredentialResult{
		Credentials: &model.Credentials{
			TmpSecretID:  r.Credentials.TmpSecretID,
			TmpSecretKey: r.Credentials.TmpSecretKey,
			SessionToken: r.Credentials.SessionToken,
		},
		ExpiredTime: int64(r.ExpiredTime),
		StartTime:   int64(r.StartTime),
	}, nil
}

// GetPreSignedURLWithTypeKey 获取 COS 预签名 URL
func (c *cosClient) GetPreSignedURLWithTypeKey(ctx context.Context, typeKey string, key string) (string, error) {
	if err := c.checkTypeKeyExist(ctx, typeKey); err != nil {
		return "", err
	}
	url, err := c.cosMap[typeKey].cosCli.GetPreSignedURL(
		ctx, key, http.MethodGet, c.cosMap[typeKey].cfg.ExpireTime)
	if err != nil {
		log.ErrorContextf(ctx, "获取 COS 预签名 URL 失败 key: %s err: %+v", key, err)
		return "", err
	}
	return url, nil
}

// GetObjectWithTypeKey 获取 COS 文件
func (c *cosClient) GetObjectWithTypeKey(ctx context.Context, typeKey string, key string) ([]byte, error) {
	if err := c.checkTypeKeyExist(ctx, typeKey); err != nil {
		return nil, err
	}
	object, err := c.cosMap[typeKey].innerCosCli.GetObject(ctx, key)
	if err != nil {
		log.ErrorContextf(ctx, "获取 COS 文件失败 key: %s err: %+v", key, err)
		return nil, err
	}
	return object, nil
}

// PutObjectWithTypeKey 上传 COS 文件
func (c *cosClient) PutObjectWithTypeKey(ctx context.Context, typeKey string, bs []byte, key string) error {
	if err := c.checkTypeKeyExist(ctx, typeKey); err != nil {
		return err
	}
	if _, err := c.cosMap[typeKey].innerCosCli.PutObject(ctx, bs, key); err != nil {
		log.ErrorContextf(ctx, "上传 COS 文件失败 key: %s, len: %d, err: %+v", key, len(bs), err)
		return err
	}
	return nil
}

// DelObjectWithTypeKey 删除 COS 文件
func (c *cosClient) DelObjectWithTypeKey(ctx context.Context, typeKey string, key string) error {
	if err := c.checkTypeKeyExist(ctx, typeKey); err != nil {
		return err
	}
	if err := c.cosMap[typeKey].innerCosCli.DelObject(ctx, key); err != nil {
		log.ErrorContextf(ctx, "删除 COS 文件失败 key: %s, err: %+v", key, err)
		return err
	}
	return nil
}

// StatObjectWithTypeKey 获取object的元数据信息
func (c *cosClient) StatObjectWithTypeKey(ctx context.Context, typeKey string, key string) (*model.ObjectInfo, error) {
	if err := c.checkTypeKeyExist(ctx, typeKey); err != nil {
		return nil, err
	}
	h, err := c.cosMap[typeKey].cosCli.HeadObject(ctx, key)
	if err != nil {
		log.WarnContextf(ctx, "获取 COS 文件元数据失败 key: %s, err: %+v", key, err)
		return nil, err
	}
	return &model.ObjectInfo{
		Hash: h.Get("x-cos-hash-crc64ecma"),
		ETag: h.Get("ETag"),
		Size: cast.ToInt64(h.Get("Content-Length")),
	}, nil
}

// checkTypeKeyExist 校验指定类似是否存在
func (c *cosClient) checkTypeKeyExist(ctx context.Context, typeKey string) error {
	if _, ok := c.cosMap[typeKey]; !ok {
		err := fmt.Errorf("typeKey:%s not exist", typeKey)
		log.ErrorContextf(ctx, "checkTypeKeyExist|err:%+v", err)
		return err
	}
	return nil
}

// GetTypeKeyWithBucket 通过COS桶名称获取typeKey
func (c *cosClient) GetTypeKeyWithBucket(_ context.Context, bucket string) string {
	for typeKey, cli := range c.cosMap {
		if cli.cfg.Bucket == bucket {
			return typeKey
		}
	}
	return defaultCosKey
}
