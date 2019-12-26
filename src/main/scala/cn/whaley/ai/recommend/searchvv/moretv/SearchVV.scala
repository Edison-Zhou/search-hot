package cn.whaley.ai.recommend.searchvv.moretv

import cn.whaley.ai.recommend.searchvv.conf.SearchVVConf
import cn.whaley.ai.recommend.utils.{DataRetrieval, DateFormatUtils}
import cn.whaley.sdk.dataOps.{HDFSOps, KafkaOps}
import cn.whaley.sdk.utils.{SendMail, TransformUDF}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.json.JSONObject

/**
 * Created by xutong on 2017/2/7.
 * moretv搜索热度（线上主程）
 */

object SearchVV extends SearchVVConf with DataRetrieval{

  /**
   * 计算log2(某节目的vv) / log2(同类型top50节目vv的平均值) x 100
   * @param value ：某节目的vv
   * @param averageValue ：同类型top50节目vv的平均值
   * @return ：log2(某节目的vv) / log2(同类型top50节目vv的平均值) x 100
   */
  def getSingleScore(value: Double,averageValue:Double):Double={
    (Math.log(value)/Math.log(averageValue))*100.0
  }

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
      val startDate = DateFormatUtils.enDateAdd(HDFSOps.today, -numDaysOfPlay)

      // 合并两份日志信息得到RDD["contentType", "videoSid"]
      val unionData = sparkSession.
        sql("SELECT contenttype as contentType, " +
          "videosid as videoSid " +
          "from ods_view.log_medusa_main3x_play " +
          s"WHERE contenttype in ${longVideoType} " +
          "AND duration is not null " +
          "and duration<7200 " +
          "and duration >300 " +
          "and event in ('userexit','selfend') " +
          "and videoSid is not null and length(videoSid)>0 " +
          s"AND key_day > ${startDate}").
        selectExpr("videoSid", "contentType").
        rdd.
        map(r => (r.getString(0), r.getString(1))).
        persist(StorageLevel.MEMORY_AND_DISK)

      //计算视频的播放次数，并在同类型视频之间排序,取前50个视频
      val playVV = unionData.map(x => ((x._2,x._1),1)).reduceByKey(_+_).
        map(x=>(x._1._1,(x._1._2,x._2))).groupByKey().map(x => (x._1, x._2.toArray.sortBy(-_._2).take(50)))

      //计算各类型vv排序前50名的视频的vv的平均值(contentType,topMeanVV)
      val topVV = playVV.map(x => (x._1,x._2.map(_._2)))
      val kk = topVV.map(x => (x._1,x._2.sum*1.0/x._2.length))

      //计算视频的播放热度
      val moretv_vv = unionData.map(x => ((x._2,x._1), 1)).reduceByKey(_ + _).map(x => (x._1._1, (x._1._2, x._2))).
        join(kk).map(_._2).
        map(x => (x._1._1, getSingleScore(x._1._2.toDouble, x._2))).
        map(x => (x._1,if (x._2 < 100) x._2 else 100.0)).collect().
        map(x => {
        val json = new JSONObject()
        json.put("sid", x._1)
        json.put("score", x._2)
        json.put("type", "vv")
        json.toString
      })
      KafkaOps.writeLocal2Kafka(moretv_vv, moretvSearchVVTopic)
      unionData.unpersist()
      sc.stop()

    }catch {
      case e: Exception =>
        val info = prefix + "[moretv][searchvv]" + "[" + HDFSOps.today + "]"
        val emailName = Array(email)
        SendMail.post(e, info, emailName)
        System.exit(-1)
    }

  }
}
