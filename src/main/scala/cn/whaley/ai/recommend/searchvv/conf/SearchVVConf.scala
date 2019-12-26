package cn.whaley.ai.recommend.searchvv.conf

import cn.whaley.ai.recommend.conf.CommonConf

/**
 * Created by xutong on 2017/2/13.
 * 搜索热度公用配置
 */
trait SearchVVConf extends CommonConf{

  // kafka topic
  val moretvSearchVVTopic: String = "Search_vv"

  //设置从hdfs读取的日志天数
  val numDaysOfPlay: Int = 7

  // moretv的hdfs日志字段和日志过滤条件
  val moretvFieldArray = Array("videoSid", "contentType", "duration")
  val moretvResultArray = Array("videoSid", "contentType")
  val moretvPlayFilterCondition = "duration<7200 and duration >300 and contentType in " + longVideoType

  // medusa的hdfs日志字段和日志过滤条件
  val medusaFieldArray = Array("videoSid", "contentType", "duration", "event")
  val medusaResultArray = Array("videoSid", "contentType")
  val medusaPlayFilterCondition = "duration is not null and duration<7200 and duration >300 and contentType in " + longVideoType +
    " and event in ('userexit','selfend') " +
    " and videoSid is not null and length(videoSid)>0"

}
