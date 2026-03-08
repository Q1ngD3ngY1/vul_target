package service

import (
	"context"
	"io"
	"net/url"
	"reflect"
	"testing"

	"git.code.oa.com/trpc-go/trpc-go/client"
	thttp "git.code.oa.com/trpc-go/trpc-go/http"
)

func Test_getTitle(t *testing.T) {
	rowURL := "http://www.bankcomm.com/BankCommSite/shtml/jyjr/cn/7158/7162/2657431.shtml?channelId=7158"
	u, err := url.Parse(rowURL)
	if err != nil {
		panic(err)
	}
	ctx := context.Background()
	rspHead := &thttp.ClientRspHeader{ManualReadBody: true}
	// NOCA:tosa/linelength(这里不适合换行)
	userAgent := "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36"
	header := map[string][]string{"User-Agent": {userAgent}}
	reqHeader := &thttp.ClientReqHeader{Method: "GET", Schema: u.Scheme, Header: header}
	cli := thttp.NewClientProxy("http.qbot.fetchURL", client.WithTarget("dns://"+u.Host))
	err = cli.Get(ctx, u.Path, nil, client.WithRspHead(rspHead), client.WithReqHead(reqHeader))
	if err != nil {
		t.Errorf("Test_getTitle get url error: %v", err)
		return
	}
	by, err := io.ReadAll(rspHead.Response.Body)
	if err != nil {
		t.Errorf("Test_getTitle get url error: %v", err)
		return
	}
	wantTitle := "交通银行举办上海市明珠债创新实验室成立仪式暨明珠债服务宣介活动"
	t.Run("测试抓取网页标题是否一致", func(t *testing.T) {
		gotTitle, err := getTitle(context.Background(), string(by))
		if err != nil {
			t.Errorf("getTitle() error = %v", err)
			return
		}
		if !reflect.DeepEqual(gotTitle, wantTitle) {
			t.Errorf("textTxt() got = %v, want %v", gotTitle, wantTitle)
		}
	})
}
