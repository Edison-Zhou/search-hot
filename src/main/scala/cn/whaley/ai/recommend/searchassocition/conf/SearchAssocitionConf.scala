package cn.whaley.ai.recommend.searchassocition.conf

import cn.whaley.ai.recommend.conf.CommonConf

/**
 * Created by xutong on 2017/2/15.
 * 搜索联想词公用配置
 */
trait SearchAssocitionConf extends CommonConf{

  // 每个contentType提取的视频个数
  val videoTopNum: Int = 20

  // 日志提取的天数
  val numDaysOfPlay: Int = 3

  // kafka topic
  val heliosSearchTopVideoTopic = "helios-rec4search-topvideo"
  val heliosSearchTopVideoSidTopic = "helios-rec4search-topvideo-sid"
  val heliosSearchTopVideoOta24Topic = "helios-rec4search-topvideo-ota24-sid"
  val heliosSearchTopContentTypeTopic = "helios-rec4search-userTopContentType"
  val heliosSearchTopContentTypeOta24Topic = "helios-rec4search-userTopContentType"

  // helios的hdfs日志字段
  val heliosFieldArray = Array("userId", "contentType", "videoSid", "datetime", "cast (accountId as long) as accountId")
  val heliosResultArray = Array("userId", "transformSid(videoSid) as sid", "getHour(datetime) as time", "accountId")
  val heliosFilterCondition = "((userId is not null and length(userId)=32) or (accountId is not null)) and " +
    "videoSid is not null and length(videoSid)>0 and length(datetime) > 14 " +
    "and contentType in " + videoType

}
