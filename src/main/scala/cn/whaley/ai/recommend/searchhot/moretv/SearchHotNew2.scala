package cn.whaley.ai.recommend.searchhot.moretv

import cn.whaley.ai.recommend.searchhot.conf.SearchHotConf
import cn.whaley.ai.recommend.searchhot.utils.DataProcess
import cn.whaley.ai.recommend.utils.{DataRetrieval, DateFormatUtils}
import cn.whaley.sdk.dataOps.HDFSOps
import cn.whaley.sdk.utils.{SendMail, TransformUDF}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by xutong on 2017/2/7.
 * moretv搜索热词（线上主程）旧数据, 3.1.4以下版本,不含地域屏蔽
 */

object SearchHotNew2 extends SearchHotConf with DataRetrieval{

  def main(args: Array[String]) {

    try{
      // 初始化spark
      val sparkConf: SparkConf = new SparkConf()
      implicit val sparkSession = SparkSession.builder()
        .config(sparkConf)
        .enableHiveSupport()
        .getOrCreate()
      val sc = sparkSession.sparkContext
      sc.setLocalProperty("spark.scheduler.pool", "production")
      TransformUDF.registerUDFSS
      val startDate = DateFormatUtils.enDateAdd(HDFSOps.today, -numOfDays)

      // 从ods_view读取用户长视频的浏览日志
     /* val medusaUserLongDetailInfo = sparkSession.
        sql("SELECT searchtext as searchText, contenttype as contentType, " +
          "videosid as videoSid, 1 as num " +
          "from ods_view.log_medusa_main3x_detail " +
          s"WHERE contenttype in ${longVideoType} " +
          "AND searchtext != '' " +
          "AND searchtext IS NOT NULL " +
          s"AND key_day > ${startDate}")*/
      val medusaUserShortPlayInfo = sparkSession.
        sql("SELECT searchtext as searchText, contenttype as contentType, " +
          "videosid as videoSid, 1 as num " +
          "from ods_view.log_medusa_main3x_play " +
          s"WHERE (contenttype in ${shortVideoType} or contenttype in ${longVideoType} ) " +
          "AND searchtext != '' " +
          "AND searchtext IS NOT NULL " +
          s"AND key_day > ${startDate}")

      // 合并两份日志信息得到RDD["searchText", "contentType", "videoSid", "num"]
      val resultData =medusaUserShortPlayInfo.
        filter("searchText is not null and searchText != ''").
        groupBy("searchText", "contentType", "videoSid").agg(Map("num" -> "sum")).
        withColumnRenamed("sum(num)", "num").
        selectExpr("searchText", "contentType", "videoSid", "num").
        persist(StorageLevel.MEMORY_AND_DISK)

      // 处理数据
      DataProcess.sendData2Kafka(resultData, moretvSearchHotTopicNew2, moretvMysqlName)
      //resultData.show(10)
      //println(resultData.count())
      resultData.unpersist()
      sc.stop()
    }catch {
      case e: Exception =>
        val info = prefix + "[moretv][searchHotNew2]" + "[" + HDFSOps.today + "]"
        val emailName = Array(email)
        SendMail.post(e, info, emailName)
        System.exit(-1)
    }

  }
}
