package cn.whaley.ai.recommend.searchhot.helios

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
 * helios搜索热词（线上主程）
 */

object SearchHot extends SearchHotConf with DataRetrieval{

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
      val userLongDetailInfo = sparkSession.
        sql("SELECT searchtext as searchText, " +
          "videosid as videoSid, 1 as num " +
          "from ods_view.log_whaleytv_main_detail " +
          s"WHERE contenttype in ${videoType} " +
          "AND searchtext != '' " +
          "AND searchtext IS NOT NULL " +
          s"AND key_day > ${startDate}")

      // 从ods_view读取用户短视频的播放日志
      val userShortPlayInfo = sparkSession.
        sql("SELECT searchtext as searchText, " +
          "videosid as videoSid, 1 as num " +
          "from ods_view.log_whaleytv_main_play " +
          s"WHERE contenttype in ${videoType} " +
          "AND searchtext != '' " +
          "AND searchtext IS NOT NULL " +
          s"AND key_day > ${startDate}")
        //得到搜索词、sid和播放计数

      // 从数据库读取sid对应的contentType
      import sparkSession.implicits._
      val sql = "select sid, content_type from mtv_basecontent where id >= ? AND id <= ? " +
        " and content_type in " + videoType
      val metaData = getMetaDataFromMysql(sc, "helios_cms_mysql", "mtv_basecontent", sql,
        r => (r.getString(1), r.getString(2))).
        toDF("sidMysql", "contentType")

      // 合并两份日志信息得到RDD["searchText", "contentType", "videoSid", "num"]
      val unionData = userLongDetailInfo.union(userShortPlayInfo)

      val resultData = unionData.join(metaData, unionData("videoSid") === metaData("sidMysql")).
        selectExpr("searchText", "contentType", "videoSid", "num").
        groupBy("searchText", "contentType", "videoSid").agg(Map("num" -> "sum")).
        withColumnRenamed("sum(num)", "num").
        selectExpr("searchText", "contentType", "videoSid", "num").
        persist(StorageLevel.MEMORY_AND_DISK)

      // 处理数据
      DataProcess.sendData2Kafka(resultData, heliosSearchHotTopic, heliosMysqlName)
      //sendData2Kafka是模块的核心处理算子, 通过groupBy方法获得每一影片对应的最高搜索词, 和总计搜索次数, 以json格式写入kafka
      //resultData.show(10)
      //println(resultData.count())
      resultData.unpersist()
      sc.stop()
    }catch {
      case e: Exception =>
        val info = prefix + "[helios][searchHot]" + "[" + HDFSOps.today + "]"
        val emailName = Array(email)
        SendMail.post(e, info, emailName)
        System.exit(-1)
    }

  }}
