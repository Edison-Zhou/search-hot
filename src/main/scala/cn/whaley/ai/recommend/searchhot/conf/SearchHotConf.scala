package cn.whaley.ai.recommend.searchhot.conf

import cn.whaley.ai.recommend.conf.CommonConf

/**
 * Created by xutong on 2017/2/7.
 * 搜索热词的公用配置
 */
trait SearchHotConf extends CommonConf{

  // 每个contentType提取的视频个数
  val videoTopNum: Int = 20

  // 日志提取的天数
  val numOfDays: Int = 3

  // kafka topic
  val heliosSearchHotTopic: String = "Helios_SearchHot"
  //对应3.1.4以上的3.x的电视猫数据 （数据处理包含地域屏蔽）
  val moretvSearchHotTopicNew: String = "moretv-rec4search-topvideo"
  //moretv-rec4search-topvideo-old ：对应3.1.4以下的3.x的电视猫数据 （不包含地域屏蔽）
  val moretvSearchHotTopicNew2: String = "moretv-rec4search-topvideo-old"
  //对应2.x电视猫的数据
  val moretvSearchHotTopicOld: String = "MoreTV_SearchHot"
  //对应优视猫的数据
  val useeSearchHotTopic: String = "usee-rec4search-topvideo"
  //对应喵学堂的数据
  val kidseduSearchHotTopic: String = "kidsedu-rec4search-topvideo"

  // mysql name
  val heliosMysqlName: String = "helios_cms_mysql"
  val moretvMysqlName: String = "moretv_cms_mysql"

  // helios的hdfs日志字段
  val heliosFieldArray = Array("searchText", "contentType", "videoSid")
  val heliosResultArray = Array("searchText", "videoSid", "1 as num")

  // moretv的hdfs日志字段
  val moretvFieldArray = Array("path", "contentType", "videoSid")
  val moretvResultArray = Array("matchPathByMoretv(path) as searchText", "contentType", "videoSid", "1 as num")

  // medusa的hdfs日志字段
  val medusaFieldArray = Array("searchText", "contentType", "videoSid")
  val medusaResultArray = Array("searchText", "contentType", "videoSid", "1 as num")


}
