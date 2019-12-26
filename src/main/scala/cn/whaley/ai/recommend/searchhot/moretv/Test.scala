package cn.whaley.ai.recommend.searchhot.moretv

import cn.whaley.ai.recommend.searchhot.moretv.SearchHotNew._
import cn.whaley.ai.recommend.utils.DateFormatUtils
import cn.whaley.sdk.dataOps.HDFSOps

/**
  *
  * @author wang.baozhi 
  * @since 2018/7/9 上午11:59 
  */
object Test {
  def main(args: Array[String]) {
    val startDate = DateFormatUtils.enDateAdd(HDFSOps.today, -numOfDays)
    println(startDate)

    val shortVideo="SELECT searchtext as searchText, contenttype as contentType, " +
        "videosid as videoSid, 1 as num " +
        "from ods_view.log_medusa_main3x_play " +
        s"WHERE contenttype='mv' " +
        "AND searchtext != '' " +
        "AND searchtext IS NOT NULL " +
        s"AND key_day > ${startDate}"

    println(shortVideo)


   val longVideo= "SELECT searchtext as searchText, contenttype as contentType, " +
      "videosid as videoSid, 1 as num " +
      "from ods_view.log_medusa_main3x_detail " +
      s"WHERE contenttype ='tv' " +
      "AND searchtext != '' " +
      "AND searchtext IS NOT NULL " +
      s"AND key_day > ${startDate}"

    println(longVideo)

  }

}
