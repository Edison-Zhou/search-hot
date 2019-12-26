package cn.whaley.ai.recommend.voiceoptimize.helios

import java.sql.ResultSet

import cn.whaley.ai.recommend.utils.DateFormatUtils
import cn.whaley.ai.recommend.voiceoptimize.conf.VoiceOptimizeConf
import cn.whaley.sdk.dataOps.{HDFSOps, KafkaOps, MySqlOps}
import cn.whaley.sdk.utils.{CodeIDOperator, TransformUDF}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.json.JSONObject

/**
 * Created by xutong on 2017/7/26.
 * 用于提供语音优化的节目评分数据
 */
object VoiceOptimize extends VoiceOptimizeConf{

  def main(args: Array[String]) {
    // 初始化spark
    val sparkConf: SparkConf = new SparkConf()
    implicit val sparkSession = SparkSession.builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()
    val sc = sparkSession.sparkContext
    sc.setLocalProperty("spark.scheduler.pool", "production")
    TransformUDF.registerUDFSS
    // 时间
    val enFormat = DateFormatUtils.enDateAdd(HDFSOps.today, -1)

    // 从数据库里获取sid和title
    val mysqlOps: MySqlOps = new MySqlOps(heliosMetaData)
    val sql = "SELECT sid, title, contentType FROM " +
      s"`mtv_program` WHERE ID >= ? AND ID <= ? and contentType in ${longVideoType}"

    // (id, (sid, title, contentType))
    val metaData = mysqlOps.getJdbcRDD(sc, sql, metaDataTableName,
      (r: ResultSet) => (CodeIDOperator.codeToId(r.getString(1)).toInt,
        (r.getString(1), r.getString(2), r.getString(3))))

    // 读取评分矩阵
    val data = sparkSession.read.parquet(HELIOS_SCOREMATRIX_PATH + enFormat + "/*").
      filter(s"content_type in ${longVideoType}").
      selectExpr("sid_or_subject_code", "score").groupBy("sid_or_subject_code").
      agg(Map("score" -> "sum")).
      withColumnRenamed("sum(score)", "score").
      selectExpr("sid_or_subject_code", "score").rdd.map(r => {
        val id = r.getString(0).toInt
        val score = r.getDouble(1)
      (id, score)
    }).join(metaData).map(r => {
      val sid = r._2._2._1
      val title = r._2._2._2
      val contentType = r._2._2._3
      val score = r._2._1
      val json = new JSONObject()
      json.put("sid", sid)
      json.put("title", title)
      json.put("contentType", contentType)
      json.put("score", score)
      json.toString()

    })

    //data.take(10).foreach(println)
    //println(data.count())
    KafkaOps.writeRDD2Kafka(data, HELIOS_VOICE_OPTIMIZE_TOPIC)
  }
}
