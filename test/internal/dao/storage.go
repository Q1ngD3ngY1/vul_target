package dao

import (
	"context"
	"fmt"
	utilConfig "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/util/config"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	sts "github.com/tencentyun/qcloud-cos-sts-sdk/go"
)

const (
	// DefaultStorageTypeKey 存储默认的类型
	DefaultStorageTypeKey = "offline"
)

// GetCorpCOSPath 获取企业COS路径
func (d *dao) GetCorpCOSPath(_ context.Context, corpID uint64) string {
	return fmt.Sprintf("/corp/%d/doc/", corpID)
}

// GetCorpImagePath 获取企业COS图片路径
func (d *dao) GetCorpImagePath(_ context.Context, corpID uint64) string {
	return fmt.Sprintf("/public/%d/image/", corpID)
}

// GetCorpRobotCOSPath 获取企业机器人COS路径
func (d *dao) GetCorpRobotCOSPath(_ context.Context, corpBizID, botBizID uint64, fileName string) string {
	return fmt.Sprintf("/corp/%d/%d/doc/%s", corpBizID, botBizID, fileName)
}

// GetCorpAppImagePath 获取企业应用图片路径
func (d *dao) GetCorpAppImagePath(_ context.Context, corpBizID, botBizID uint64, fileName string) string {
	return fmt.Sprintf("/public/%d/%d/image/%s", corpBizID, botBizID, fileName)
}

// GetCorpCOSFilePath 获取企业COS文件路径
func (d *dao) GetCorpCOSFilePath(ctx context.Context, corpID uint64, filename string) string {
	return path.Join(d.GetCorpCOSPath(ctx, corpID), filename)
}

// CheckURLPrefix 校验文件URL前缀
func (d *dao) CheckURLPrefix(ctx context.Context, corpID, corpBizID, botBizID uint64, url string) error {
	log.InfoContextf(ctx, "CheckURLPrefix|corpID:%d, corpBizID:%d, botBizID:%d, url:%s",
		corpID, corpBizID, botBizID, url)
	if strings.HasPrefix(url, d.GetCorpRobotCOSPath(ctx, corpBizID, botBizID, "")) {
		return nil
	}
	if newAppID, ok := utilConfig.GetMainConfig().SearchKnowledgeAppIdReplaceMap[botBizID]; ok {
		// iSearch项目的bot_biz_id替换，临时实现共享知识库功能
		if strings.HasPrefix(url, d.GetCorpRobotCOSPath(ctx, corpBizID, newAppID, "")) {
			return nil
		}
	}
	// 兼容老数据
	if strings.HasPrefix(url, d.GetCorpCOSPath(ctx, corpID)) {
		return nil
	}
	return fmt.Errorf("url:%s illegal", url)
}

// CheckURLFile 校验文件URL有效
func (d *dao) CheckURLFile(ctx context.Context, corpID, corpBizID, botBizID uint64, url, eTag string) error {
	log.InfoContextf(ctx, "CheckURLFile|corpID:%d, corpBizID:%d, botBizID:%d, url:%s, eTag:%s",
		corpID, corpBizID, botBizID, url, eTag)
	err := d.CheckURLPrefix(ctx, corpID, corpBizID, botBizID, url)
	if err != nil {
		return err
	}
	if len(eTag) == 0 {
		err = fmt.Errorf("url:%s, eTag:%s is empty", url, eTag)
		return err
	}
	// 这里objectInfo.ETag的结果会带有转义字符 类似 "\"5784a190d6af4214020f54edc87429ab\""
	// 需要对转义字符特殊处理
	objectInfo, err := d.StatObject(ctx, url)
	if err != nil {
		return err
	}
	e1, err := strconv.Unquote(eTag)
	if err == nil {
		eTag = e1
	}
	e2, err := strconv.Unquote(objectInfo.ETag)
	if err == nil {
		objectInfo.ETag = e2
	}
	if eTag != objectInfo.ETag {
		err = fmt.Errorf("url:%s, objectInfo.ETag:%s, eTag:%s illegal", url, objectInfo.ETag, eTag)
		return err
	}
	// 文件大小校验
	if err = checkFileSize(url, objectInfo); err != nil {
		return err
	}
	return nil
}

func (d *dao) CheckURLFileByHash(ctx context.Context, corpID, corpBizID, botBizID uint64, url, hash string) error {
	log.InfoContextf(ctx, "CheckURLFileByHash|corpID:%d, corpBizID:%d, botBizID:%d, url:%s, hash:%s",
		corpID, corpBizID, botBizID, url, hash)
	err := d.CheckURLPrefix(ctx, corpID, corpBizID, botBizID, url)
	if err != nil {
		return err
	}
	if len(hash) == 0 {
		err = fmt.Errorf("url:%s, hash:%s is empty", url, hash)
		return err
	}
	// 需要对转义字符特殊处理
	objectInfo, err := d.StatObject(ctx, url)
	if err != nil {
		return err
	}
	e1, err := strconv.Unquote(hash)
	if err == nil {
		hash = e1
	}
	e2, err := strconv.Unquote(objectInfo.Hash)
	if err == nil {
		objectInfo.Hash = e2
	}
	if hash != objectInfo.Hash {
		err = fmt.Errorf("url:%s, objectInfo.Hash:%s, hash:%s illegal", url, objectInfo.Hash, hash)
		return err
	}
	// 文件大小校验
	if err = checkFileSize(url, objectInfo); err != nil {
		return err
	}
	return nil
}

// checkFileSize 校验文件大小
func checkFileSize(url string, objectInfo *model.ObjectInfo) (err error) {
	ext := util.GetFileExt(url)
	if len(ext) == 0 {
		err = fmt.Errorf("url:%s ext illegal", url)
		return err
	}
	size, ok := config.App().RobotDefault.FileTypeSize[ext]
	if !ok || objectInfo.Size > int64(size) {
		err = fmt.Errorf("url:%s size illegal", url)
		return err
	}
	return nil
}

// GetDomain 获取对象存储domain
func (d *dao) GetDomain(ctx context.Context) string {
	return d.storageCli.GetDomain(ctx)
}

// GetStorageType 获取对象存储类型
func (d *dao) GetStorageType(ctx context.Context) string {
	return d.storageCli.GetType(ctx)
}

// GetBucket 获取存储桶
func (d *dao) GetBucket(ctx context.Context) string {
	return d.storageCli.GetBucket(ctx)
}

// GetRegion 获取存储桶地域
func (d *dao) GetRegion(ctx context.Context) string {
	return d.storageCli.GetRegion(ctx)
}

// GetCredential 获取临时密钥
func (d *dao) GetCredential(ctx context.Context, pathList []string, storageAction string) (
	*sts.CredentialResult, error) {
	res, err := d.storageCli.GetCredential(ctx, pathList, storageAction)
	if err != nil {
		log.ErrorContextf(ctx, "Get storage credential error: %+v", err)
		return nil, err
	}
	return &sts.CredentialResult{
		Credentials: &sts.Credentials{
			TmpSecretID:  res.Credentials.TmpSecretID,
			TmpSecretKey: res.Credentials.TmpSecretKey,
			SessionToken: res.Credentials.SessionToken,
		},
		ExpiredTime: int(res.ExpiredTime),
		Expiration:  "",
		StartTime:   int(res.StartTime),
		RequestId:   "",
		Error:       nil,
	}, nil
}

// GetPresignedURL 获取 COS 预签名 URL
func (d *dao) GetPresignedURL(ctx context.Context, key string) (string, error) {
	url, err := d.storageCli.GetPreSignedURL(ctx, key)
	if err != nil {
		return "", err
	}
	return url, nil
}

// GetObject 获取 COS 文件
func (d *dao) GetObject(ctx context.Context, key string) ([]byte, error) {
	object, err := d.storageCli.GetObject(ctx, key)
	if err != nil {
		return nil, err
	}
	return object, nil
}

// PutObject 上传 COS 文件
func (d *dao) PutObject(ctx context.Context, bs []byte, key string) error {
	if err := d.storageCli.PutObject(ctx, bs, key); err != nil {
		return err
	}
	return nil
}

// DelObject 删除 COS 文件
func (d *dao) DelObject(ctx context.Context, key string) error {
	if err := d.storageCli.DelObject(ctx, key); err != nil {
		return err
	}
	return nil
}

// StatObject 获取object的元数据信息
func (d *dao) StatObject(ctx context.Context, key string) (*model.ObjectInfo, error) {
	// 公有云cos会出现单点过载被摘除的情况，导致客户端超时，需要重试
	var objectInfo *model.ObjectInfo
	var err error
	timeoutSeconds := []int{1, 5}
	for _, timeout := range timeoutSeconds {
		func() {
			newCtx, cancel := context.WithTimeout(ctx, time.Duration(timeout)*time.Second)
			defer cancel()
			objectInfo, err = d.storageCli.StatObject(newCtx, key)
			if err != nil {
				log.WarnContextf(ctx, "StatObject error:%+v", err)
			}
		}()
	}
	if err != nil {
		log.ErrorContextf(ctx, "StatObject error:%+v", err)
		return objectInfo, err
	}
	log.InfoContextf(ctx, "StatObject objectInfo:%+v", objectInfo)
	return objectInfo, nil
}

// GetObjectETag 获取存储对象的ETag
func (d *dao) GetObjectETag(ctx context.Context, urlValue string) string {
	u, err := url.Parse(urlValue)
	if err != nil {
		return ""
	}
	if len(u.Path) == 0 {
		return ""
	}
	// 公有云cos会出现单点过载被摘除的情况，导致客户端超时，需要重试
	var objectInfo *model.ObjectInfo
	timeoutSeconds := []int{1, 5}
	for _, timeout := range timeoutSeconds {
		func() {
			newCtx, cancel := context.WithTimeout(ctx, time.Duration(timeout)*time.Second)
			defer cancel()
			objectInfo, err = d.storageCli.StatObject(newCtx, u.Path)
			if err != nil {
				log.WarnContextf(ctx, "StatObject error:%+v", err)
			}
		}()
	}
	if err != nil || objectInfo == nil {
		log.ErrorContextf(ctx, "StatObject final error:%+v", err)
		return ""
	}
	log.InfoContextf(ctx, "StatObject objectInfo:%+v", objectInfo)
	return objectInfo.ETag
}

// GetCredentialWithTypeKey 获取cos临时密钥
func (d *dao) GetCredentialWithTypeKey(ctx context.Context, typeKey string, pathList []string, storageAction string) (
	*sts.CredentialResult, error) {
	res, err := d.storageCli.GetCredentialWithTypeKey(ctx, typeKey, pathList, storageAction)
	if err != nil {
		log.ErrorContextf(ctx, "Get storage credential error: %+v", err)
		return nil, err
	}
	return &sts.CredentialResult{
		Credentials: &sts.Credentials{
			TmpSecretID:  res.Credentials.TmpSecretID,
			TmpSecretKey: res.Credentials.TmpSecretKey,
			SessionToken: res.Credentials.SessionToken,
		},
		ExpiredTime: int(res.ExpiredTime),
		Expiration:  "",
		StartTime:   int(res.StartTime),
		RequestId:   "",
		Error:       nil,
	}, nil
}

// GetPresignedURLWithTypeKey 获取Cos预签名URL
func (d *dao) GetPresignedURLWithTypeKey(ctx context.Context, typeKey string, key string) (string, error) {
	url, err := d.storageCli.GetPreSignedURLWithTypeKey(ctx, typeKey, key)
	if err != nil {
		return "", err
	}
	return url, nil
}

// GetObjectWithTypeKey 获取 COS 文件
func (d *dao) GetObjectWithTypeKey(ctx context.Context, typeKey string, key string) ([]byte, error) {
	// 公有云cos会出现单点过载被摘除的情况，导致客户端超时，需要重试
	var object []byte
	var err error
	timeoutSeconds := []int{2, 10}
	for _, timeout := range timeoutSeconds {
		func() {
			newCtx, cancel := context.WithTimeout(ctx, time.Duration(timeout)*time.Second)
			defer cancel()
			object, err = d.storageCli.GetObjectWithTypeKey(newCtx, typeKey, key)
			if err != nil {
				log.WarnContextf(ctx, "GetObjectWithTypeKey error:%+v", err)
			}
		}()
	}
	if err != nil {
		log.ErrorContextf(ctx, "GetObjectWithTypeKey error:%+v", err)
		return object, err
	}
	return object, nil
}

// PutObjectWithTypeKey 上传 COS 文件
func (d *dao) PutObjectWithTypeKey(ctx context.Context, typeKey string, bs []byte, key string) error {
	if err := d.storageCli.PutObjectWithTypeKey(ctx, typeKey, bs, key); err != nil {
		return err
	}
	return nil
}

// DelObjectWithTypeKey 删除 COS 文件
func (d *dao) DelObjectWithTypeKey(ctx context.Context, typeKey string, key string) error {
	if err := d.storageCli.DelObjectWithTypeKey(ctx, typeKey, key); err != nil {
		return err
	}
	return nil
}

// StatObjectWithTypeKey 获取object的元数据信息
func (d *dao) StatObjectWithTypeKey(ctx context.Context, typeKey string, key string) (*model.ObjectInfo, error) {
	return d.storageCli.StatObjectWithTypeKey(ctx, typeKey, key)
}

// GetObjectETagWithTypeKey 获取存储对象的ETag
func (d *dao) GetObjectETagWithTypeKey(ctx context.Context, typeKey string, urlValue string) (string, error) {
	u, err := url.Parse(urlValue)
	if err != nil {
		return "", err
	}
	if len(u.Path) == 0 {
		return "", nil
	}
	objectInfo, err := d.storageCli.StatObjectWithTypeKey(ctx, typeKey, u.Path)
	if err != nil {
		return "", err
	}
	return objectInfo.ETag, nil
}

// GetDomainWithTypeKey 获取对象存储domain
func (d *dao) GetDomainWithTypeKey(ctx context.Context, typeKey string) (string, error) {
	return d.storageCli.GetDomainWithTypeKey(ctx, typeKey)
}

// GetStorageTypeWithTypeKey 获取对象存储类型
func (d *dao) GetStorageTypeWithTypeKey(ctx context.Context, typeKey string) (string, error) {
	return d.storageCli.GetTypeWithTypeKey(ctx, typeKey)
}

// GetBucketWithTypeKey 获取存储桶
func (d *dao) GetBucketWithTypeKey(ctx context.Context, typeKey string) (string, error) {
	return d.storageCli.GetBucketWithTypeKey(ctx, typeKey)
}

// GetRegionWithTypeKey 获取存储桶地域
func (d *dao) GetRegionWithTypeKey(ctx context.Context, typeKey string) (string, error) {
	return d.storageCli.GetRegionWithTypeKey(ctx, typeKey)
}

// GetTypeKeyWithBucket 通过COS桶名称获取typeKey
func (d *dao) GetTypeKeyWithBucket(ctx context.Context, bucket string) string {
	return d.storageCli.GetTypeKeyWithBucket(ctx, bucket)
}
