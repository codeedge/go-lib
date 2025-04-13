package crawler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"

	"github.com/gogf/gf/v2/frame/g"
)

type WXArticle struct {
	Cost   bool `json:"cost"`
	Ok     bool `json:"ok"`
	Result struct {
		AdAbtestPadding   int           `json:"ad_abtest_padding"`
		AdvertisementInfo []interface{} `json:"advertisement_info"`
		AdvertisementNum  int           `json:"advertisement_num"`
		Ainfos            []interface{} `json:"ainfos"`
		Alias             string        `json:"alias"`
		AnchorTree        []interface{} `json:"anchor_tree"`
		AppmsgExtGet      struct {
			FuncFlag int `json:"func_flag"`
		} `json:"appmsg_ext_get"`
		AppmsgFeFilter string `json:"appmsg_fe_filter"`
		AppmsgLikeType int    `json:"appmsg_like_type"`
		Author         string `json:"author"`
		BanScene       int    `json:"ban_scene"`
		BaseResp       struct {
			BaseResp struct {
				CookieCount int    `json:"cookie_count"`
				Errmsg      string `json:"errmsg"`
				Ret         int    `json:"ret"`
				Sessionid   string `json:"sessionid"`
			} `json:"base_resp"`
			CookieCount int    `json:"cookie_count"`
			Errmsg      string `json:"errmsg"`
			Ret         int    `json:"ret"`
			Sessionid   string `json:"sessionid"`
			Wxtoken     int    `json:"wxtoken"`
		} `json:"base_resp"`
		BizCard struct {
			List []struct {
				Alias        string `json:"alias"`
				Fakeid       string `json:"fakeid"`
				IsBizBan     int    `json:"is_biz_ban"`
				Nickname     string `json:"nickname"`
				OrignalNum   int    `json:"orignal_num"`
				RoundHeadImg string `json:"round_head_img"`
				Signature    string `json:"signature"`
				Username     string `json:"username"`
			} `json:"list"`
			Total int `json:"total"`
		} `json:"biz_card"`
		Bizuin               string `json:"bizuin"`
		CanReward            int    `json:"can_reward"`
		CanSeeComplaint      int    `json:"can_see_complaint"`
		CanShare             int    `json:"can_share"`
		CanUsePage           int    `json:"can_use_page"`
		CanUseWecoin         int    `json:"can_use_wecoin"`
		CdnUrl               string `json:"cdn_url"`
		CdnUrl169            string `json:"cdn_url_16_9"`
		CdnUrl11             string `json:"cdn_url_1_1"`
		CdnUrl2351           string `json:"cdn_url_235_1"`
		CommentId            string `json:"comment_id"`
		ContentExtractedInfo struct {
			Images []string `json:"images"`
			Text   string   `json:"text"`
		} `json:"content_extracted_info"`
		ContentNoencode string `json:"content_noencode"`
		CopyrightInfo   struct {
			CopyrightStat      int `json:"copyright_stat"`
			IsCartoonCopyright int `json:"is_cartoon_copyright"`
		} `json:"copyright_info"`
		CreateTime    string `json:"create_time"`
		CspNonceStr   int    `json:"csp_nonce_str"`
		DelReasonId   int    `json:"del_reason_id"`
		Desc          string `json:"desc"`
		FasttmplInfos []struct {
			Fullversion  string `json:"fullversion"`
			Lang         string `json:"lang"`
			Type         int    `json:"type"`
			Version      int    `json:"version"`
			Versiongroup string `json:"versiongroup"`
		} `json:"fasttmpl_infos"`
		FasttmplVersion        int           `json:"fasttmpl_version"`
		FilterTime             int           `json:"filter_time"`
		HasRedPacketCover      int           `json:"has_red_packet_cover"`
		HdHeadImg              string        `json:"hd_head_img"`
		Hotspotinfolist        []interface{} `json:"hotspotinfolist"`
		Idx                    int           `json:"idx"`
		ImgFormat              string        `json:"img_format"`
		InMm                   int           `json:"in_mm"`
		IsAreaShield           int           `json:"is_area_shield"`
		IsAsync                int           `json:"is_async"`
		IsLimitUser            int           `json:"is_limit_user"`
		IsLogin                int           `json:"is_login"`
		IsOnlyRead             int           `json:"is_only_read"`
		IsPaySubscribe         int           `json:"is_pay_subscribe"`
		IsTopStories           int           `json:"is_top_stories"`
		IsWxgStuffUin          int           `json:"is_wxg_stuff_uin"`
		Isnew                  int           `json:"isnew"`
		Isprofileblock         int           `json:"isprofileblock"`
		ItemShowType           int           `json:"item_show_type"`
		Lang                   string        `json:"lang"`
		Link                   string        `json:"link"`
		LiveInfo               []interface{} `json:"live_info"`
		Locationlist           []interface{} `json:"locationlist"`
		MaliciousContentType   int           `json:"malicious_content_type"`
		MaliciousTitleReasonId int           `json:"malicious_title_reason_id"`
		Mid                    int64         `json:"mid"`
		MoonInline             int           `json:"moon_inline"`
		MoreReadType           int           `json:"more_read_type"`
		MsgDailyIdx            int           `json:"msg_daily_idx"`
		NeedReportCost         int           `json:"need_report_cost"`
		NickName               string        `json:"nick_name"`
		OptimizingFlag         int           `json:"optimizing_flag"`
		OriCreateTime          int           `json:"ori_create_time"`
		OriHeadImgUrl          string        `json:"ori_head_img_url"`
		OriSendTime            int           `json:"ori_send_time"`
		PaySubscribeInfo       struct {
			Desc           string `json:"desc"`
			Fee            int    `json:"fee"`
			GiftsCount     int    `json:"gifts_count"`
			PreviewPercent int    `json:"preview_percent"`
			WecoinAmount   int    `json:"wecoin_amount"`
		} `json:"pay_subscribe_info"`
		PicturePageInfoList []interface{} `json:"picture_page_info_list"`
		RealItemShowType    int           `json:"real_item_show_type"`
		RelatedArticleInfo  struct {
			HasRelatedArticleInfo int `json:"has_related_article_info"`
		} `json:"related_article_info"`
		RelatedTag       []interface{} `json:"related_tag"`
		ReqId            string        `json:"req_id"`
		RoundHeadImg     string        `json:"round_head_img"`
		ShieldAreaids    []interface{} `json:"shield_areaids"`
		ShowComment      int           `json:"show_comment"`
		ShowCoverPic     int           `json:"show_cover_pic"`
		ShowMsgVoice     int           `json:"show_msg_voice"`
		ShowTopBar       int           `json:"show_top_bar"`
		Signature        string        `json:"signature"`
		Sn               string        `json:"sn"`
		SourceUrl        string        `json:"source_url"`
		Srcid            string        `json:"srcid"`
		SvrTime          int           `json:"svr_time"`
		Title            string        `json:"title"`
		TotalItemNum     int           `json:"total_item_num"`
		Type             int           `json:"type"`
		UrlItemShowType  int           `json:"url_item_show_type"`
		UseOuterLink     int           `json:"use_outer_link"`
		UseTxVideoPlayer int           `json:"use_tx_video_player"`
		UserInfo         struct {
			Ckeys         []interface{} `json:"ckeys"`
			Clientversion string        `json:"clientversion"`
			IsPaid        int           `json:"is_paid"`
		} `json:"user_info"`
		UserName              string        `json:"user_name"`
		UserUin               int           `json:"user_uin"`
		VideoIds              []interface{} `json:"video_ids"`
		VideoInArticle        []interface{} `json:"video_in_article"`
		VideoPageInfos        []interface{} `json:"video_page_infos"`
		VoiceInAppmsg         []interface{} `json:"voice_in_appmsg"`
		VoiceInAppmsgListJson string        `json:"voice_in_appmsg_list_json"`
		WecoinTips            int           `json:"wecoin_tips"`
	} `json:"result"`
	RetCode int `json:"retCode"`
}

// WechatCrawling 微信公众号爬虫数据获取
func WechatCrawling(uri string) (wxArticle *WXArticle, err error) {
	key := "wechat.wechatCrawling"
	res, err := g.Client().Get(context.Background(), fmt.Sprintf("http://whosecard.com:8081/api/wx/article?url=%s&key=%s", url.QueryEscape(uri), key))
	if err != nil {
		fmt.Println(err)
		return nil, errors.New("爬取失败，请重试")
	}
	dataStr := res.ReadAll()
	wxArticle = &WXArticle{}
	err = json.Unmarshal(dataStr, &wxArticle)
	if err != nil {
		fmt.Println(err)
		return nil, errors.New("爬取失败，请重试")
	}
	if wxArticle.RetCode != 0 {
		return nil, errors.New("爬取失败，请重试")
	}
	return wxArticle, nil
}
