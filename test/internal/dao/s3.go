package dao

import (
	"context"
	"fmt"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"git.woa.com/adp/common/x/clientx/s3x"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/util"
)

const (
	// DefaultStorageTypeKey 存储默认的类型
	DefaultStorageTypeKey = "offline"
)

// S3 对象存储接口
type S3 interface {
	s3x.Storage

	GetCorpCOSPath(ctx context.Context, corpID uint64) string
	GetCorpImagePath(ctx context.Context, corpID uint64) string
	GetCorpRobotCOSPath(ctx context.Context, corpBizID, botBizID uint64, fileName string) string
	GetCorpAppImagePath(ctx context.Context, corpBizID, botBizID uint64, fileName string) string
	GetCorpCOSFilePath(ctx context.Context, corpID uint64, filename string) string
	CheckURLPrefix(ctx context.Context, corpID, corpBizID, botBizID uint64, url string) error
	CheckURLFile(ctx context.Context, corpID, corpBizID, botBizID uint64, url, eTag string) error
	CheckURLFileByHash(ctx context.Context, corpID, corpBizID, botBizID uint64, url, hash string) error
	GetObjectETag(ctx context.Context, urlValue string) string
	GetBucketURL(ctx context.Context) string
}

type S3Impl struct {
	s3x.Storage
}

func NewS3(storageCli s3x.Storage) S3 {
	s3 := &S3Impl{
		Storage: storageCli,
	}
	return s3
}

// GetBucketURL 获取存储桶的完整 URL
// 返回格式: https://{bucket}.cos.{region}.{domain}
func (d *S3Impl) GetBucketURL(ctx context.Context) string {
	bucket := d.GetBucket(ctx)
	region := d.GetRegion(ctx)
	domain := d.GetDomain(ctx)
	return fmt.Sprintf("https://%s.cos.%s.%s", bucket, region, domain)
}

// GetCorpCOSPath 获取企业COS路径
func (d *S3Impl) GetCorpCOSPath(_ context.Context, corpID uint64) string {
	return fmt.Sprintf("/corp/%d/doc/", corpID)
}

// GetCorpImagePath 获取企业COS图片路径
func (d *S3Impl) GetCorpImagePath(_ context.Context, corpID uint64) string {
	return fmt.Sprintf("/public/%d/image/", corpID)
}

// GetCorpRobotCOSPath 获取企业机器人COS路径
func (d *S3Impl) GetCorpRobotCOSPath(_ context.Context, corpBizID, botBizID uint64, fileName string) string {
	return fmt.Sprintf("/corp/%d/%d/doc/%s", corpBizID, botBizID, fileName)
}

// GetCorpAppImagePath 获取企业应用图片路径
func (d *S3Impl) GetCorpAppImagePath(_ context.Context, corpBizID, botBizID uint64, fileName string) string {
	return fmt.Sprintf("/public/%d/%d/image/%s", corpBizID, botBizID, fileName)
}

// GetCorpCOSFilePath 获取企业COS文件路径
func (d *S3Impl) GetCorpCOSFilePath(ctx context.Context, corpID uint64, filename string) string {
	return path.Join(d.GetCorpCOSPath(ctx, corpID), filename)
}

// CheckURLPrefix 校验文件URL前缀
func (d *S3Impl) CheckURLPrefix(ctx context.Context, corpID, corpBizID, botBizID uint64, url string) error {
	logx.I(ctx, "CheckURLPrefix|corpID:%d, corpBizID:%d, botBizID:%d, url:%s",
		corpID, corpBizID, botBizID, url)
	if strings.HasPrefix(url, d.GetCorpRobotCOSPath(ctx, corpBizID, botBizID, "")) {
		return nil
	}
	if newAppID, ok := config.GetMainConfig().SearchKnowledgeAppIdReplaceMap[botBizID]; ok {
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
func (d *S3Impl) CheckURLFile(ctx context.Context, corpID, corpBizID, botBizID uint64, url, eTag string) error {
	logx.I(ctx, "CheckURLFile|corpID:%d, corpBizID:%d, botBizID:%d, url:%s, eTag:%s", corpID, corpBizID, botBizID, url, eTag)
	err := d.CheckURLPrefix(ctx, corpID, corpBizID, botBizID, url)
	if err != nil {
		return err
	}
	if len(eTag) == 0 {
		err = fmt.Errorf("url:%s, eTag:%s is empty", url, eTag)
		return err
	}
	logx.I(ctx, "CheckURLFile|url:%s, eTag:%s", url, eTag)
	// 这里objectInfo.ETag的结果会带有转义字符 类似 "\"5784a190d6af4214020f54edc87429ab\""
	// 需要对转义字符特殊处理
	logx.I(ctx, "CheckURLFile|StatObject...")
	objectInfo, err := d.StatObject(ctx, url)
	logx.I(ctx, "CheckURLFile|StatObject...done: %+v", objectInfo)
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

func (d *S3Impl) CheckURLFileByHash(ctx context.Context, corpID, corpBizID, botBizID uint64, url, hash string) error {
	logx.I(ctx, "CheckURLFileByHash|corpID:%d, corpBizID:%d, botBizID:%d, url:%s, hash:%s",
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
	objectInfo, err := d.Storage.StatObject(ctx, url)
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
func checkFileSize(url string, objectInfo *s3x.ObjectInfo) (err error) {
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

// GetCredential 获取临时密钥
func (d *S3Impl) GetCredential(ctx context.Context, pathList []string, storageAction string) (
	*s3x.CredentialResult, error) {
	res, err := d.Storage.GetCredential(ctx, pathList, storageAction)
	if err != nil {
		logx.E(ctx, "Get storage credential error: %+v", err)
		return nil, err
	}
	return &s3x.CredentialResult{
		Credentials: &s3x.Credentials{
			TmpSecretID:  res.Credentials.TmpSecretID,
			TmpSecretKey: res.Credentials.TmpSecretKey,
			SessionToken: res.Credentials.SessionToken,
		},
		ExpiredTime: res.ExpiredTime,
		StartTime:   res.StartTime,
	}, nil
}

// StatObject 获取object的元数据信息
func (d *S3Impl) StatObject(ctx context.Context, key string) (*s3x.ObjectInfo, error) {
	// 公有云cos会出现单点过载被摘除的情况，导致客户端超时，需要重试
	var objectInfo *s3x.ObjectInfo
	var err error
	timeoutSeconds := []int{1, 5}
	for _, timeout := range timeoutSeconds {
		func() {
			newCtx, cancel := context.WithTimeout(ctx, time.Duration(timeout)*time.Second)
			defer cancel()
			objectInfo, err = d.Storage.StatObject(newCtx, key)
			if err != nil {
				logx.I(ctx, "StatObject error:%+v", err)
			}
		}()
	}
	if err != nil {
		return objectInfo, fmt.Errorf("StatObject error:%w", err)
	}
	logx.I(ctx, "StatObject objectInfo:%+v", objectInfo)
	return objectInfo, nil
}

// GetObjectETag 获取存储对象的ETag
func (d *S3Impl) GetObjectETag(ctx context.Context, urlValue string) string {
	u, err := url.Parse(urlValue)
	if err != nil {
		return ""
	}
	if len(u.Path) == 0 {
		return ""
	}
	// 公有云cos会出现单点过载被摘除的情况，导致客户端超时，需要重试
	var objectInfo *s3x.ObjectInfo
	timeoutSeconds := []int{1, 5}
	for _, timeout := range timeoutSeconds {
		func() {
			newCtx, cancel := context.WithTimeout(ctx, time.Duration(timeout)*time.Second)
			defer cancel()
			objectInfo, err = d.StatObject(newCtx, u.Path)
			if err != nil {
				logx.W(ctx, "StatObject error:%+v", err)
			}
		}()
	}
	if err != nil || objectInfo == nil {
		logx.W(ctx, "StatObject final error:%+v", err)
		return ""
	}
	logx.I(ctx, "StatObject objectInfo:%+v", objectInfo)
	return objectInfo.ETag
}

// GetCredentialWithTypeKey 获取cos临时密钥
func (d *S3Impl) GetCredentialWithTypeKey(ctx context.Context, req *s3x.GetCredentialReq) (*s3x.CredentialResult, error) {
	res, err := d.Storage.GetCredentialWithTypeKey(ctx, req)
	if err != nil {
		logx.E(ctx, "Get storage credential error: %+v", err)
		return nil, err
	}
	return &s3x.CredentialResult{
		Credentials: &s3x.Credentials{
			TmpSecretID:  res.Credentials.TmpSecretID,
			TmpSecretKey: res.Credentials.TmpSecretKey,
			SessionToken: res.Credentials.SessionToken,
		},
		ExpiredTime: res.ExpiredTime,
		StartTime:   res.StartTime,
	}, nil
}

// GetObjectWithTypeKey 获取 COS 文件
func (d *S3Impl) GetObjectWithTypeKey(ctx context.Context, typeKey string, key string) ([]byte, error) {
	// 公有云cos会出现单点过载被摘除的情况，导致客户端超时，需要重试
	var object []byte
	var err error
	timeoutSeconds := []int{2, 10}
	for _, timeout := range timeoutSeconds {
		func() {
			newCtx, cancel := context.WithTimeout(ctx, time.Duration(timeout)*time.Second)
			defer cancel()
			object, err = d.Storage.GetObjectWithTypeKey(newCtx, typeKey, key)
			if err != nil {
				logx.W(ctx, "GetObjectWithTypeKey error:%+v", err)
			}
		}()
	}
	if err != nil {
		logx.E(ctx, "GetObjectWithTypeKey error:%+v", err)
		return object, err
	}
	return object, nil
}

// GetObjectETagWithTypeKey 获取存储对象的ETag
func (d *S3Impl) GetObjectETagWithTypeKey(ctx context.Context, typeKey string, urlValue string) (string, error) {
	u, err := url.Parse(urlValue)
	if err != nil {
		return "", err
	}
	if len(u.Path) == 0 {
		return "", nil
	}
	objectInfo, err := d.StatObjectWithTypeKey(ctx, typeKey, u.Path)
	if err != nil {
		return "", err
	}
	return objectInfo.ETag, nil
}
