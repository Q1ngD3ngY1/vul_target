package storage

import (
	"bufio"
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"log"
	"os"
	"testing"

	"git.woa.com/baicaoyuan/moss/configx"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"github.com/agiledragon/gomonkey/v2"
	. "github.com/glycerine/goconvey/convey"
)

// NOCA:golint/fnsize(此处不适合拆分方法)
func Test_MinIO(t *testing.T) {
	Convey("test minio", t, func() {
		gomonkey.NewPatches()
		patches := gomonkey.NewPatches()
		patches.ApplyFunc(configx.MustGetWatched, func(key string, opts ...configx.WatchOption) any {
			return config.Application{
				Storage: config.Storage{
					Type: model.StorageTypeMinIO,
					MinIOMap: map[string]config.MinIO{
						"default": {
							SecretID:    "stan",
							SecretKey:   "admin12345678",
							Region:      "us-west-rack-2",
							Bucket:      "qbot",
							STSEndpoint: "http://127.0.0.1:9000",
							EndPoint:    "127.0.0.1:9000",
							UseHTTPS:    false,
						},
					},
				},
			}
		})
		defer patches.Reset()
		ctx := context.Background()
		corpID := 666
		path := fmt.Sprintf("/corp/%d/doc/", corpID)
		cli := New()
		Convey("test GetType", func() {
			So(cli.GetType(ctx), ShouldEqual, model.StorageTypeMinIO)
		})
		Convey("test GetBucket", func() {
			So(cli.GetBucket(ctx), ShouldEqual, "qbot")
		})
		Convey("test GetRegion", func() {
			So(cli.GetRegion(ctx), ShouldEqual, "us-west-rack-2")
		})
		res, err := cli.GetCredential(ctx, []string{path}, model.ActionUpAndDownload)
		Convey("test GetCredential", func() {
			So(err, ShouldBeNil)
			t.Logf("Credentials:%+v ExpiredTime:%d StartTime:%d", res.Credentials, res.ExpiredTime, res.StartTime)
		})
		fileName := "test.md"
		minioPath := fmt.Sprintf("%s%s", path, fileName)
		Convey("test PutObject", func() {
			file, err := os.OpenFile(fileName, os.O_RDONLY, 0666)
			if err != nil {
				log.Fatalln("文件打开失败 ", err)
			}
			defer file.Close()
			stat, err := file.Stat()
			if err != nil {
				t.Fatalf("stat %s failed: %v", fileName, err)
			}
			bs := make([]byte, stat.Size())
			if _, err = bufio.NewReader(file).Read(bs); err != nil && err != io.EOF {
				t.Fatalf("reader %s failed: %v", fileName, err)
			}
			if err = cli.PutObject(ctx, bs, minioPath); err != nil {
				t.Fatalf("PutObject %s failed: %v", fileName, err)
			}
			t.Logf("PutObject %s success minioPath:%s", fileName, minioPath)
			So(err, ShouldBeNil)
		})
		Convey("test GetObject", func() {
			bs, err := cli.GetObject(ctx, minioPath)
			So(err, ShouldBeNil)
			minioMD5 := fmt.Sprintf("%x", md5.Sum(bs))
			file, err := os.OpenFile(fileName, os.O_RDONLY, 0666)
			if err != nil {
				log.Fatalln("文件打开失败 ", err)
			}
			defer file.Close()
			stat, err := file.Stat()
			if err != nil {
				t.Fatalf("stat %s failed: %v", fileName, err)
			}
			bs = make([]byte, stat.Size())
			if _, err = bufio.NewReader(file).Read(bs); err != nil && err != io.EOF {
				t.Fatalf("reader %s failed: %v", fileName, err)
			}
			fileMD5 := fmt.Sprintf("%x", md5.Sum(bs))
			So(fileMD5, ShouldEqual, minioMD5)
		})
		Convey("test GetPreSignedURL", func() {
			url, err := cli.GetPreSignedURL(ctx, minioPath)
			So(err, ShouldBeNil)
			t.Logf("GetPreSignedURL %s success", url)
		})
	})
}
