package cn.whaley.ai.recommend.searchassocition.helios

import cn.whaley.ai.recommend.searchassocition.conf.SearchAssocitionConf
import cn.whaley.ai.recommend.utils.{DataRetrieval, DateFormatUtils}
import cn.whaley.sdk.dataOps.{HDFSOps, KafkaOps}
import cn.whaley.sdk.utils.{CodeIDOperator, SendMail, TransformUDF}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.json.{JSONArray, JSONObject}

/**
 * Created by xutong on 2017/2/7.
 * helios搜索联想词（线上主程）
 */

object SearchAssocitionOta24 extends SearchAssocitionConf with DataRetrieval{

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

      // 获取日志的启示天数
      val startDate = DateFormatUtils.enDateAdd(HDFSOps.today, -numDaysOfPlay)

      // 从ods_view层获取日志信息("userId", "sid", "time", "accountId")
      val userPlayInfoInit = sparkSession.
        sql("SELECT videosid as sid, accountid as accountId, key_hour as time, userid as userId " +
          "from ods_view.log_whaleytv_main_play " +
          s"where key_day > ${startDate} " +
          "and ((userid is not null and length(userid)=32) or (accountid is not null)) " +
          "and videosid is not null " +
          "and length(videosid)>0 " +
          s"and contenttype in ${videoType}").
        persist(StorageLevel.MEMORY_AND_DISK)


      // 从数据库读取sid对应的contentType
      import sparkSession.implicits._
      val sql = "select sid, content_type from mtv_basecontent where id >= ? AND id <= ? " +
        "and type=1 and status=1 and video_type in (0,1) and content_type in " + videoType
      val metaData = getMetaDataFromMysql(sc, "helios_cms_mysql", "mtv_basecontent", sql,
        r => (r.getString(1), r.getString(2))).toDF("sidMysql", "contentType")

      val userPlayInfo = userPlayInfoInit.join(metaData, userPlayInfoInit("sid") === metaData("sidMysql")).
        persist(StorageLevel.MEMORY_AND_DISK)

      // 计算每个contentType播放次数最多视频
      val topVideoJsonData = new org.json.JSONObject()
      val topVideoData = userPlayInfo.selectExpr("contentType", "sid", "1 as num").
        groupBy("contentType", "sid").agg(Map("num" -> "sum")).withColumnRenamed("sum(num)", "num").
        selectExpr("contentType", "sid", "num").rdd.
        map(r => (r.getString(0), (r.getString(1), r.getLong(2)))).
        groupByKey().map(r => {
        val contentType = r._1
        val sidArray = r._2.toArray.sortBy(-_._2).take(videoTopNum)
        (contentType, sidArray)
      }).collect().foreach(info => {
        val idJsonArray = new org.json.JSONArray()
        for(e <- info._2) idJsonArray.put(e._1)
        topVideoJsonData.put(info._1, idJsonArray)
      })

      for(i <- 0 to 9)
        KafkaOps.writrString2Kafka(topVideoJsonData.toString(), heliosSearchTopVideoOta24Topic)

      /** 上线的时候放开此注释
      // 计算每个用户播放次数最多的contentType
      val userPrefer = userPlayInfo.filter("accountId is not null").selectExpr("userId", "contentType", "accountId").rdd.
        map(r => ((r.getString(0), r.getLong(2)), r.getString(1))).groupByKey().
        map(x => (x._1,x._2.groupBy(y => y).map(y => (y._1,y._2.size/(x._2.size*1.0))).toArray.sortBy(-_._2))).
        map(x => (x._1,("allTime",x._2)))

      // 计算每个用户在11点到14点播放次数最多的contentType
      val userNoonPref = userPlayInfo.filter("accountId is not null").filter("time in ('11', '12', '13', '14')").
        selectExpr("userId", "contentType", "accountId").rdd.
        map(r => ((r.getString(0), r.getLong(2)), r.getString(1))).groupByKey().
        map(x => (x._1,x._2.groupBy(y => y).map(y => (y._1,y._2.size/(x._2.size*1.0))).toArray.sortBy(-_._2))).
        map(x => (x._1,("noon",x._2)))

      // 计算每个用户在16点到18点播放次数最多的contentType
      val userAfterPref = userPlayInfo.filter("accountId is not null").filter("time in ('16', '17', '18')").
        selectExpr("userId", "contentType", "accountId").rdd.
        map(r => ((r.getString(0), r.getLong(2)), r.getString(1))).groupByKey().
        map(x => (x._1,x._2.groupBy(y => y).map(y => (y._1,y._2.size/(x._2.size*1.0))).toArray.sortBy(-_._2))).
        map(x => (x._1,("afternoon",x._2)))

      // 计算每个用户在18点到21点播放次数最多的contentType
      val userEvenPref = userPlayInfo.filter("accountId is not null").filter("time in ('18', '19', '20', '21')").
        selectExpr("userId", "contentType", "accountId").rdd.
        map(r => ((r.getString(0), r.getLong(2)), r.getString(1))).groupByKey().
        map(x => (x._1,x._2.groupBy(y => y).map(y => (y._1,y._2.size/(x._2.size*1.0))).toArray.sortBy(-_._2))).
        map(x => (x._1,("night",x._2)))

      val unionData = userPrefer.union(userNoonPref).union(userAfterPref).union(userEvenPref).groupByKey().
        map(r => {
        val userId = r._1._1
        val accountId = r._1._2
        val json = new JSONObject()
        r._2.map(info => {
          val time = info._1
          val typeScoreArray = info._2
          val jsonArray = new JSONArray()
          for (r <- typeScoreArray){
            val idJsonArray = new org.json.JSONObject()
            idJsonArray.put("contentType",r._1)
            idJsonArray.put("score",r._2)
            jsonArray.put(idJsonArray)
          }
          json.put("accountId",accountId)
          json.put("userId", userId)
          json.put(time,jsonArray)
        })
        json.toString()
      })
      KafkaOps.writeRDD2Kafka(unionData, heliosSearchTopContentTypeOta24Topic)*/
      userPlayInfo.unpersist()
      sc.stop()
    }catch {
      case e: Exception =>
        val info = prefix + "[helios][searchAssocition]" + "[" + HDFSOps.today + "]"
        val emailName = Array(email)
        e.printStackTrace()
        SendMail.post(e, info, emailName)
        System.exit(-1)
    }

  }
}
