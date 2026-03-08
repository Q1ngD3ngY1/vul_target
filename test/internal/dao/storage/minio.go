package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"path/filepath"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	jsoniter "github.com/json-iterator/go"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type minioCli struct {
	cfg      config.MinIO
	minioCli *minio.Client
}

type minioClient struct {
	minioMap map[string]*minioCli
}

const defaultMinioKey = "offline"

// newMinIO create minio instance
func newMinIO() *minioClient {
	minioMap := make(map[string]*minioCli)
	for key, value := range config.App().Storage.MinIOMap {
		log.Infof("key:%s", key)
		cfg := value
		cli, err := minio.New(cfg.EndPoint, &minio.Options{
			Creds:  credentials.NewStaticV4(cfg.SecretID, cfg.SecretKey, ""),
			Secure: cfg.UseHTTPS,
			Region: cfg.Region,
		})
		if err != nil {
			log.Fatalf("create minio client cfg:%+v err:%+v", cfg, err)
		}
		minioMap[key] = &minioCli{
			cfg:      cfg,
			minioCli: cli,
		}
	}
	if _, ok := minioMap[defaultMinioKey]; !ok {
		err := fmt.Errorf("empty default config")
		log.Fatalf("create minio client failed, err:%+v", err)
		panic(err)
	}
	log.Infof("newMinIO|len(minioMap):%d", len(minioMap))
	return &minioClient{
		minioMap: minioMap,
	}
}

// GetDomain 获取domain
func (m *minioClient) GetDomain(ctx context.Context) string {
	return m.minioMap[defaultMinioKey].cfg.STSEndpoint
}

// GetType 获取对象存储类型
func (m *minioClient) GetType(ctx context.Context) string {
	return model.StorageTypeMinIO
}

// GetBucket 获取存储桶
func (m *minioClient) GetBucket(ctx context.Context) string {
	return m.minioMap[defaultMinioKey].cfg.Bucket
}

// GetRegion 获取存储桶地域
func (m *minioClient) GetRegion(ctx context.Context) string {
	return m.minioMap[defaultMinioKey].cfg.Region
}

// GetCredential 获取临时密钥
func (m *minioClient) GetCredential(ctx context.Context, pathList []string, storageAction string) (
	*model.CredentialResult, error) {
	duration := time.Minute
	expireTime := time.Now().Add(duration)
	resource := make([]string, 0)
	for _, path := range pathList {
		res := "arn:aws:s3:::" + m.minioMap[defaultMinioKey].cfg.Bucket + path
		if len(filepath.Ext(path)) == 0 {
			res += "*"
		}
		resource = append(resource, res)
	}
	credentialPolicy := model.CredentialPolicy{
		Version: model.MinIOVersionID,
		Statement: []model.CredentialPolicyStatement{
			{
				Action:   model.GetStorageMinIOAction(storageAction),
				Effect:   "Allow",
				Resource: resource,
			},
		},
	}
	policy, err := jsoniter.MarshalToString(credentialPolicy)
	if err != nil {
		log.ErrorContextf(ctx, "marshal minio policy err:%+v", err)
		return nil, err
	}
	res, err := credentials.NewSTSAssumeRole(m.minioMap[defaultMinioKey].cfg.STSEndpoint,
		credentials.STSAssumeRoleOptions{
			AccessKey:       m.minioMap[defaultMinioKey].cfg.SecretID,
			SecretKey:       m.minioMap[defaultMinioKey].cfg.SecretKey,
			Policy:          policy,
			Location:        m.minioMap[defaultMinioKey].cfg.Region,
			DurationSeconds: int(duration.Seconds()),
		})
	if err != nil {
		log.ErrorContextf(ctx, "get minio credential err:%+v", err)
		return nil, err
	}
	cre, err := res.Get()
	if err != nil {
		log.ErrorContextf(ctx, "Error retrieving STS credentials err:%+v", err)
		return nil, err
	}
	return &model.CredentialResult{
		Credentials: &model.Credentials{
			TmpSecretID:  cre.AccessKeyID,
			TmpSecretKey: cre.SecretAccessKey,
			SessionToken: cre.SessionToken,
		},
		ExpiredTime: expireTime.Unix(),
		StartTime:   time.Now().Add(time.Second * 10).Unix(),
	}, nil
}

// GetPreSignedURL 获取 COS 预签名 URL
func (m *minioClient) GetPreSignedURL(ctx context.Context, key string) (string, error) {
	reqParams := make(url.Values)
	preSignedURL, err := m.minioMap[defaultMinioKey].minioCli.PresignedGetObject(
		ctx, m.minioMap[defaultMinioKey].cfg.Bucket, key, m.minioMap[defaultMinioKey].cfg.ExpireTime, reqParams)
	if err != nil {
		log.ErrorContextf(ctx, "获取 MinIO 预签名 URL 失败 key: %s err: %+v", key, err)
		return "", err
	}
	return preSignedURL.String(), nil
}

// GetObject 获取 COS 文件
func (m *minioClient) GetObject(ctx context.Context, key string) ([]byte, error) {
	object, err := m.minioMap[defaultMinioKey].minioCli.GetObject(
		ctx, m.minioMap[defaultMinioKey].cfg.Bucket, key, minio.GetObjectOptions{})
	if err != nil {
		log.ErrorContextf(ctx, "获取 MinIO 文件失败 key: %s err: %+v", key, err)
		return nil, err
	}
	defer func() { _ = object.Close() }()
	stat, err := object.Stat()
	if err != nil {
		log.ErrorContextf(ctx, "获取 MinIO 文件统计信息失败 key: %s err: %+v", key, err)
		return nil, err
	}
	content := make([]byte, stat.Size)
	if _, err = object.Read(content); err != io.EOF && err != nil {
		log.ErrorContextf(ctx, "读取 MinIO 文件内容失败 key: %s err: %+v", key, err)
		return nil, err
	}
	return content, nil
}

// PutObject 上传 COS 文件
func (m *minioClient) PutObject(ctx context.Context, bs []byte, key string) error {
	reader := bytes.NewReader(bs)
	putOptions := minio.PutObjectOptions{}
	if _, err := m.minioMap[defaultMinioKey].minioCli.PutObject(
		ctx, m.minioMap[defaultMinioKey].cfg.Bucket, key, reader, reader.Size(), putOptions); err != nil {
		log.ErrorContextf(ctx, "上传 MinIO 文件失败 key: %s, len: %d, err: %+v", key, len(bs), err)
		return err
	}
	return nil
}

// DelObject 删除 COS 文件
func (m *minioClient) DelObject(ctx context.Context, key string) error {
	removeOptions := minio.RemoveObjectOptions{}
	if err := m.minioMap[defaultMinioKey].minioCli.RemoveObject(
		ctx, m.minioMap[defaultMinioKey].cfg.Bucket, key, removeOptions); err != nil {
		log.ErrorContextf(ctx, "删除 MinIO 文件失败 key: %s, err: %+v", key, err)
		return err
	}
	return nil
}

// StatObject 获取object的元数据信息
func (m *minioClient) StatObject(ctx context.Context, key string) (*model.ObjectInfo, error) {
	getObjectOptions := minio.GetObjectOptions{}
	objectInfo, err := m.minioMap[defaultMinioKey].minioCli.StatObject(
		ctx, m.minioMap[defaultMinioKey].cfg.Bucket, key, getObjectOptions)
	if err != nil {
		log.WarnContextf(ctx, "获取 MinIO 文件元数据失败 key: %s err: %+v", key, err)
		return nil, err
	}
	return &model.ObjectInfo{
		ETag: objectInfo.ETag,
		Size: objectInfo.Size,
	}, nil
}

// GetDomainWithTypeKey 获取domain
func (m *minioClient) GetDomainWithTypeKey(ctx context.Context, typeKey string) (string, error) {
	if err := m.checkTypeKeyExist(ctx, typeKey); err != nil {
		return "", err
	}
	return m.minioMap[typeKey].cfg.STSEndpoint, nil
}

// GetTypeWithTypeKey 获取对象存储类型
func (m *minioClient) GetTypeWithTypeKey(ctx context.Context, typeKey string) (string, error) {
	if err := m.checkTypeKeyExist(ctx, typeKey); err != nil {
		return "", err
	}
	return model.StorageTypeMinIO, nil
}

// GetBucketWithTypeKey 获取存储桶
func (m *minioClient) GetBucketWithTypeKey(ctx context.Context, typeKey string) (string, error) {
	if err := m.checkTypeKeyExist(ctx, typeKey); err != nil {
		return "", err
	}
	return m.minioMap[typeKey].cfg.Bucket, nil
}

// GetRegionWithTypeKey 获取存储桶地域
func (m *minioClient) GetRegionWithTypeKey(ctx context.Context, typeKey string) (string, error) {
	if err := m.checkTypeKeyExist(ctx, typeKey); err != nil {
		return "", err
	}
	return m.minioMap[typeKey].cfg.Region, nil
}

// GetCredentialWithTypeKey 获取临时密钥
func (m *minioClient) GetCredentialWithTypeKey(ctx context.Context, typeKey string, pathList []string,
	storageAction string) (*model.CredentialResult, error) {
	if err := m.checkTypeKeyExist(ctx, typeKey); err != nil {
		return nil, err
	}
	duration := time.Minute
	expireTime := time.Now().Add(duration)
	resource := make([]string, 0)
	for _, path := range pathList {
		res := "arn:aws:s3:::" + m.minioMap[typeKey].cfg.Bucket + path
		if len(filepath.Ext(path)) == 0 {
			res += "*"
		}
		resource = append(resource, res)
	}
	credentialPolicy := model.CredentialPolicy{
		Version: model.MinIOVersionID,
		Statement: []model.CredentialPolicyStatement{
			{
				Action:   model.GetStorageMinIOAction(storageAction),
				Effect:   "Allow",
				Resource: resource,
			},
		},
	}
	policy, err := jsoniter.MarshalToString(credentialPolicy)
	if err != nil {
		log.ErrorContextf(ctx, "marshal minio policy err:%+v", err)
		return nil, err
	}
	res, err := credentials.NewSTSAssumeRole(m.minioMap[typeKey].cfg.STSEndpoint,
		credentials.STSAssumeRoleOptions{
			AccessKey:       m.minioMap[typeKey].cfg.SecretID,
			SecretKey:       m.minioMap[typeKey].cfg.SecretKey,
			Policy:          policy,
			Location:        m.minioMap[typeKey].cfg.Region,
			DurationSeconds: int(duration.Seconds()),
		})
	if err != nil {
		log.ErrorContextf(ctx, "get minio credential err:%+v", err)
		return nil, err
	}
	cre, err := res.Get()
	if err != nil {
		log.ErrorContextf(ctx, "Error retrieving STS credentials err:%+v", err)
		return nil, err
	}
	return &model.CredentialResult{
		Credentials: &model.Credentials{
			TmpSecretID:  cre.AccessKeyID,
			TmpSecretKey: cre.SecretAccessKey,
			SessionToken: cre.SessionToken,
		},
		ExpiredTime: expireTime.Unix(),
		StartTime:   time.Now().Add(time.Second * 10).Unix(),
	}, nil
}

// GetPreSignedURLWithTypeKey 获取 COS 预签名 URL
func (m *minioClient) GetPreSignedURLWithTypeKey(ctx context.Context, typeKey string, key string) (string, error) {
	if err := m.checkTypeKeyExist(ctx, typeKey); err != nil {
		return "", err
	}
	reqParams := make(url.Values)
	preSignedURL, err := m.minioMap[typeKey].minioCli.PresignedGetObject(
		ctx, m.minioMap[typeKey].cfg.Bucket, key, m.minioMap[typeKey].cfg.ExpireTime, reqParams)
	if err != nil {
		log.ErrorContextf(ctx, "获取 MinIO 预签名 URL 失败 key: %s err: %+v", key, err)
		return "", err
	}
	return preSignedURL.String(), nil
}

// GetObjectWithTypeKey 获取 COS 文件
func (m *minioClient) GetObjectWithTypeKey(ctx context.Context, typeKey string, key string) ([]byte, error) {
	if err := m.checkTypeKeyExist(ctx, typeKey); err != nil {
		return nil, err
	}
	object, err := m.minioMap[typeKey].minioCli.GetObject(
		ctx, m.minioMap[typeKey].cfg.Bucket, key, minio.GetObjectOptions{})
	if err != nil {
		log.ErrorContextf(ctx, "获取 MinIO 文件失败 key: %s err: %+v", key, err)
		return nil, err
	}
	defer func() { _ = object.Close() }()
	stat, err := object.Stat()
	if err != nil {
		log.ErrorContextf(ctx, "获取 MinIO 文件统计信息失败 key: %s err: %+v", key, err)
		return nil, err
	}
	content := make([]byte, stat.Size)
	if _, err = object.Read(content); err != io.EOF && err != nil {
		log.ErrorContextf(ctx, "读取 MinIO 文件内容失败 key: %s err: %+v", key, err)
		return nil, err
	}
	return content, nil
}

// PutObjectWithTypeKey 上传 COS 文件
func (m *minioClient) PutObjectWithTypeKey(ctx context.Context, typeKey string, bs []byte, key string) error {
	if err := m.checkTypeKeyExist(ctx, typeKey); err != nil {
		return err
	}
	reader := bytes.NewReader(bs)
	putOptions := minio.PutObjectOptions{}
	if _, err := m.minioMap[typeKey].minioCli.PutObject(
		ctx, m.minioMap[typeKey].cfg.Bucket, key, reader, reader.Size(), putOptions); err != nil {
		log.ErrorContextf(ctx, "上传 MinIO 文件失败 key: %s, len: %d, err: %+v", key, len(bs), err)
		return err
	}
	return nil
}

// DelObjectWithTypeKey 删除 COS 文件
func (m *minioClient) DelObjectWithTypeKey(ctx context.Context, typeKey string, key string) error {
	if err := m.checkTypeKeyExist(ctx, typeKey); err != nil {
		return err
	}
	removeOptions := minio.RemoveObjectOptions{}
	if err := m.minioMap[typeKey].minioCli.RemoveObject(
		ctx, m.minioMap[typeKey].cfg.Bucket, key, removeOptions); err != nil {
		log.ErrorContextf(ctx, "删除 MinIO 文件失败 key: %s, err: %+v", key, err)
		return err
	}
	return nil
}

// StatObjectWithTypeKey 获取object的元数据信息
func (m *minioClient) StatObjectWithTypeKey(ctx context.Context, typeKey string, key string) (
	*model.ObjectInfo, error) {
	if err := m.checkTypeKeyExist(ctx, typeKey); err != nil {
		return nil, err
	}
	getObjectOptions := minio.GetObjectOptions{}
	objectInfo, err := m.minioMap[typeKey].minioCli.StatObject(
		ctx, m.minioMap[typeKey].cfg.Bucket, key, getObjectOptions)
	if err != nil {
		log.WarnContextf(ctx, "获取 MinIO 文件元数据失败 key: %s err: %+v", key, err)
		return nil, err
	}
	return &model.ObjectInfo{
		ETag: objectInfo.ETag,
		Size: objectInfo.Size,
	}, nil
}

// checkTypeKeyExist 校验指定类似是否存在
func (m *minioClient) checkTypeKeyExist(ctx context.Context, typeKey string) error {
	if _, ok := m.minioMap[typeKey]; !ok {
		err := fmt.Errorf("typeKey:%s not exist", typeKey)
		log.ErrorContextf(ctx, "checkTypeKeyExist|err:%+v", err)
		return err
	}
	return nil
}

// GetTypeKeyWithBucket 通过COS桶名称获取typeKey
func (m *minioClient) GetTypeKeyWithBucket(_ context.Context, bucket string) string {
	for typeKey, cli := range m.minioMap {
		if cli.cfg.Bucket == bucket {
			return typeKey
		}
	}
	return defaultMinioKey
}
