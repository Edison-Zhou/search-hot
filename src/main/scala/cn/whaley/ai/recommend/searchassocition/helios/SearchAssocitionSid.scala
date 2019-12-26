package cn.whaley.ai.recommend.searchassocition.helios

import cn.whaley.ai.recommend.searchassocition.conf.SearchAssocitionConf
import cn.whaley.ai.recommend.utils.{DataRetrieval, DateFormatUtils}
import cn.whaley.sdk.dataOps.{HDFSOps, KafkaOps}
import cn.whaley.sdk.utils.{SendMail, TransformUDF}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.json.{JSONArray, JSONObject}

/**
 * Created by xutong on 2017/2/7.
 * helios搜索联想词（线上主程）
  *
  * helios ota24之前的版本，目前只推送sid，而不是int类型的sid，目前已经发现CodeIDOperator.codeToId有问题，部分不能够从字符串sid计算出主键
 */

object SearchAssocitionSid extends SearchAssocitionConf with DataRetrieval{

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
      /**
        *  "helios_cms_mysql":{"driver":"com.mysql.jdbc.Driver",
          "url":"jdbc:mysql://bigdata-appsvr-130-1:3306/mtv_cms?useUnicode=true&characterEncoding=utf-8&autoReconnect=true",
        "user":"bislave",
        "password":"slave4bi@whaley"}
        对于ota24上线的时候，mtv_cms.mtv_basecontent表,会增加appVersionFlag字段,
        在此类中增加app_version_flag=0，来保证只推低版本的数据；对于ota24版本，则不需要做任何限制
        * */
      import sparkSession.implicits._
     /* val sql = "select sid, content_type from mtv_basecontent where id >= ? AND id <= ? " +
        "and type=1 and status=1 and video_type in (0,1) and content_type in " + videoType*/
      val sql = "select sid, content_type from mtv_basecontent where id >= ? AND id <= ? " +
        "and type=1 and status=1 and video_type in (0,1) and app_version_flag=0 and content_type in " + videoType

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
        KafkaOps.writrString2Kafka(topVideoJsonData.toString(), heliosSearchTopVideoSidTopic)
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
