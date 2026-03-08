package dao

import (
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/knowledge"
	"google.golang.org/protobuf/proto"
)

func Test_URL(t *testing.T) {
	txts := []string{
		`![](https://cos.ap-guangzhou.myqcloud.com/image_files/412f7b75644ba717838f72e0f3e63233-image.png?size=max|163.2*109.4|+Inf)`,
		`![](http://cos.ap-guangzhou.myqcloud.com/image_files/412f7b75644ba717838f72e0f3e63233-image.png?size=max|163.2*109.4|+Inf)`,
		`![](http://cos.ap-guangzhou.myqcloud.com/image_files/412f7b75644ba717838f72e0f3e63233-image.png)`,
		`![](https://qidian-qbot-test-1251316161.cos.ap-guangzhou.myqcloud.com/image_files/0d5b360e799390b04a44e83b877ec64e-image1.png?size=min\|8.7*8.7\|0.01)`,
		`![wqrew.sdfweeqwe123easdsa](https://cos.ap-guangzhou.myqcloud.com/image_files/412f7b75644ba717838f72e0f3e63233-image.png?size=max|163.2*109.4|+Inf)`,
		`![哈哈哈.13ksdfder](http://cos.ap-guangzhou.myqcloud.com/image_files/412f7b75644ba717838f72e0f3e63233-image.png?size=max|163.2*109.4|+Inf)`,
		`![32333](http://cos.ap-guangzhou.myqcloud.com/image_files/412f7b75644ba717838f72e0f3e63233-image.png)`,
		`![xx...000)()](https://qidian-qbot-test-1251316161.cos.ap-guangzhou.myqcloud.com/image_files/0d5b360e799390b04a44e83b877ec64e-image1.png?size=min\|8.7*8.7\|0.01)`,
		`![Figure 3: Visualizing unnormalized relative perplexity gains with r=0.1.](https://fileparser-1251316161.cos.ap-guangzhou.myqcloud.com/image_files/a6b6785371a33249bbbf2c6485952643-image.png?size=max|456.3*137.7|0.77)`,
		`![](https://fileparser-1251316161.cos.ap-guangzhou.myqcloud.com/image_files/a6b6785371a33249bbbf2c6485952643-image.png?size=max|456.3*137.7|0.77)`,
	}

	for _, txt := range txts {
		fmt.Println(txt)
		imageURL := ""
		match := imgReg.FindStringSubmatch(txt)
		if len(match) > 1 {
			imageURL = match[1]
		}
		imageParseURL, err := url.Parse(imageURL)
		if err != nil || imageParseURL.Path == "" {
			continue
		}
		path := imageParseURL.Path
		host := imageParseURL.Host
		t.Errorf("Test_ReplaceURL 1 URL:%s path:%s host:%s", imageURL, path, host)
	}
}

func Test_ReplaceURL(t *testing.T) {
	body, err := os.ReadFile("/Users/donghao/Downloads/de53f217e02542d5986e695f56a2d0df_split.pb")
	if err != nil {
		t.Errorf("Test_ReplaceURL 1 body:%+v err:%+v", body, err)
		return
	}
	unSerialPb := &pb.RichContents{}
	if err := proto.Unmarshal(body, unSerialPb); err != nil {
		t.Errorf("Test_ReplaceURL 2 body:%+v err:%+v", body, err)
	}
	imageURLs := make(map[string]string)
	for _, imageURL := range unSerialPb.GetImages() {

		URL, err := url.Parse(imageURL[4 : len(imageURL)-1])
		if err != nil || URL.Path == "" {
			continue
		}
		if _, ok := imageURLs[URL.Scheme+"://"+URL.Host+URL.Path]; ok {
			continue
		}

		imageURLs[URL.Scheme+"://"+URL.Host+URL.Path] = "https://qbot.qidian.qq.com/s/risnWkVu"

		t.Errorf("Test_ReplaceURL 3 host+path:%s", URL.Host+URL.Path)
	}
	t.Errorf("Test_ReplaceURL 4 imageURLs:%+v", imageURLs)
}

func Test_DocSegmentExtends(t *testing.T) {
	body, err := os.ReadFile("/Users/donghao/Downloads/04791bf7fbf943729738599616967160_split.pb")
	if err != nil {
		t.Errorf("Test_cosFile 1 body:%v+ err:%v+", body, err)
		return
	}
	var pageContents []*pb.PageContent
	unSerialPb := &pb.RichContents{}
	if err := proto.Unmarshal(body, unSerialPb); err != nil {
		t.Errorf("Test_cosFile 2 body:%v+ err:%v+", body, err)
	}
	t.Errorf("Test_cosFile 2.1 len(RichContents): %d", len(unSerialPb.GetRichContents()))
	for _, richContent := range unSerialPb.GetRichContents() {
		pageContents = append(pageContents, richContent.GetPageContents()...)
	}
	t.Errorf("Test_cosFile 2.2 len(TableSplitResults): %d", len(unSerialPb.GetTableSplitResults()))
	for _, richContent := range unSerialPb.GetTableSplitResults() {
		pageContents = append(pageContents, richContent.GetTablePageContents())
	}
	t.Errorf("Test_cosFile 3 unSerialPb:%v+ err:%v+", unSerialPb, err)
	t.Errorf("Test_cosFile 4 pageContents:%v+ err:%v+", pageContents, err)
	DocSegmentExtends := make([]*model.DocSegmentExtend, 0, len(pageContents))
	for _, pageContent := range pageContents {
		DocSegmentExtend := model.DocSegmentExtend{
			DocSegment: model.DocSegment{
				Outputs:         "",
				PageContent:     util.String(pageContent),
				OrgData:         pageContent.GetPageContentOrgString(),
				SplitModel:      "",
				Status:          model.SegmentStatusInit,
				IsDeleted:       model.SegmentIsNotDeleted,
				NextAction:      model.SegNextActionAdd,
				RichTextIndex:   int(pageContent.GetRichContentId()),
				UpdateTime:      time.Now(),
				StartChunkIndex: int(pageContent.GetOrgStart()),
				EndChunkIndex:   int(pageContent.GetOrgEnd()),
				LinkerKeep:      pageContent.GetLinkerKeep(),
				CreateTime:      time.Now(),
			},
		}
		t.Errorf("Test_cosFile 5 DocSegmentExtends:%v+ err:%v+", DocSegmentExtend, err)
	}
	t.Errorf("Test_cosFile 6 DocSegmentExtends:%v+ err:%v+", DocSegmentExtends, err)
	t.Errorf("Test_cosFile 7 len(Images): %d", len(unSerialPb.GetImages()))
	for _, image := range unSerialPb.GetImages() {
		t.Errorf("Test_cosFile 7.1 image:%v+ err:%v+", image, err)
		imageURL := ""
		match := imgReg.FindStringSubmatch(image)
		if len(match) > 1 {
			imageURL = match[1]
		}
		URL, err := url.Parse(imageURL)
		if err != nil || URL.Path == "" {
			continue
		}
		t.Errorf("Test_cosFile 7.2 URL:%v+ err:%v+", URL.Path, err)
	}
}

func Test_docQa(t *testing.T) {
	body, err := os.ReadFile("/tmp/db3ca54524c74c559fdc68a221fa105d_split.pb")
	if err != nil {
		t.Errorf("Test_cosFile 1 body:%v+ err:%v+", body, err)
		return
	}
	var pageContents []*pb.PageContent
	unSerialPb := &pb.RichContents{}
	if err := proto.Unmarshal(body, unSerialPb); err != nil {
		t.Errorf("Test_cosFile 2 body:%v+ err:%v+", body, err)
	}
	for _, richContent := range unSerialPb.GetRichContents() {
		pageContents = append(pageContents, richContent.GetPageContents()...)
	}
	t.Errorf("Test_cosFile 3 unSerialPb:%v+ err:%v+", unSerialPb, err)
	t.Errorf("Test_cosFile 4 pageContents:%v+ err:%v+", pageContents, err)
	DocSegmentExtends := make([]*model.DocSegmentExtend, 0, len(pageContents))
	for _, pageContent := range pageContents {
		DocSegmentExtend := model.DocSegmentExtend{
			DocSegment: model.DocSegment{
				Outputs:         "",
				PageContent:     util.String(pageContent),
				OrgData:         pageContent.GetPageContentOrgString(),
				SplitModel:      "",
				Status:          model.SegmentStatusInit,
				IsDeleted:       model.SegmentIsNotDeleted,
				NextAction:      model.SegNextActionAdd,
				RichTextIndex:   int(pageContent.GetRichContentId()),
				UpdateTime:      time.Now(),
				StartChunkIndex: int(pageContent.GetOrgStart()),
				EndChunkIndex:   int(pageContent.GetOrgEnd()),
				LinkerKeep:      pageContent.GetLinkerKeep(),
				CreateTime:      time.Now(),
			},
		}
		t.Errorf("Test_cosFile 5 DocSegmentExtends:%v+ err:%v+", DocSegmentExtend, err)
	}
	t.Errorf("Test_cosFile 6 DocSegmentExtends:%v+ err:%v+", DocSegmentExtends, err)
}
