// Package storage 存储interface
package storage

import (
	"context"

	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
)

// Storage 存储interface
type Storage interface {
	// 默认业务类型操作

	// GetDomain 获取domain
	GetDomain(ctx context.Context) string
	// GetType 获取对象存储类型
	GetType(ctx context.Context) string
	// GetBucket 获取存储桶
	GetBucket(ctx context.Context) string
	// GetRegion 获取存储桶地域
	GetRegion(ctx context.Context) string
	// GetCredential 获取临时密钥
	GetCredential(ctx context.Context, path []string, storageAction string) (*model.CredentialResult, error)
	// GetPreSignedURL 获取 COS 预签名 URL
	GetPreSignedURL(ctx context.Context, key string) (string, error)
	// GetObject 获取 COS 文件
	GetObject(ctx context.Context, key string) ([]byte, error)
	// PutObject 上传 COS 文件
	PutObject(ctx context.Context, bs []byte, key string) error
	// DelObject 删除 COS 文件
	DelObject(ctx context.Context, key string) error
	// StatObject 获取object的元数据信息
	StatObject(ctx context.Context, key string) (*model.ObjectInfo, error)

	// 指定业务类型操作

	// GetDomainWithTypeKey 获取domain
	GetDomainWithTypeKey(ctx context.Context, typeKey string) (string, error)
	// GetTypeWithTypeKey 获取对象存储类型
	GetTypeWithTypeKey(ctx context.Context, typeKey string) (string, error)
	// GetBucketWithTypeKey 获取存储桶
	GetBucketWithTypeKey(ctx context.Context, typeKey string) (string, error)
	// GetRegionWithTypeKey 获取存储桶地域
	GetRegionWithTypeKey(ctx context.Context, typeKey string) (string, error)
	// GetCredentialWithTypeKey 获取临时密钥
	GetCredentialWithTypeKey(ctx context.Context, typeKey string, path []string, storageAction string) (
		*model.CredentialResult, error)
	// GetPreSignedURLWithTypeKey 获取 COS 预签名 URL
	GetPreSignedURLWithTypeKey(ctx context.Context, typeKey string, key string) (string, error)
	// GetObjectWithTypeKey 获取 COS 文件
	GetObjectWithTypeKey(ctx context.Context, typeKey string, key string) ([]byte, error)
	// PutObjectWithTypeKey 上传 COS 文件
	PutObjectWithTypeKey(ctx context.Context, typeKey string, bs []byte, key string) error
	// DelObjectWithTypeKey 删除 COS 文件
	DelObjectWithTypeKey(ctx context.Context, typeKey string, key string) error
	// StatObjectWithTypeKey 获取object的元数据信息
	StatObjectWithTypeKey(ctx context.Context, typeKey string, key string) (*model.ObjectInfo, error)
	// GetTypeKeyWithBucket 通过COS桶名称获取typeKey
	GetTypeKeyWithBucket(ctx context.Context, bucket string) string
}

// New creates Storage instance
func New() Storage {
	cfg := config.App().Storage
	if cfg.Type == model.StorageTypeMinIO {
		return newMinIO()
	}
	return newCOS()
}
