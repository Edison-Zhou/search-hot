package cn.whaley.ai.recommend.searchhot.usee

import cn.whaley.ai.recommend.searchhot.conf.SearchHotConf
import cn.whaley.ai.recommend.searchhot.utils.DataProcess
import cn.whaley.ai.recommend.utils.{DataRetrieval, DateFormatUtils}
import cn.whaley.sdk.dataOps.HDFSOps
import cn.whaley.sdk.utils.{SendMail, TransformUDF}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import cn.moretv.doraemon.common.enum.FileFormatEnum
import cn.moretv.doraemon.data.reader.DataReader
import cn.moretv.doraemon.common.path.HdfsPath

/**
  * Created in Usee Project on 2019/3/7
  *  项目一期依赖编辑给出的热门影片数据, 后续依赖用户播放日志
  */

object SearchHotNew extends SearchHotConf with DataRetrieval{

  def main(args: Array[String]) {

    try{
      // 初始化spark
      val sparkConf: SparkConf = new SparkConf()
      implicit val sparkSession = SparkSession.builder()
        .config(sparkConf)
        .enableHiveSupport()
        .getOrCreate()
      import sparkSession.implicits._
      val sc = sparkSession.sparkContext
      sc.setLocalProperty("spark.scheduler.pool", "production")
      TransformUDF.registerUDFSS

      val dataReader = new DataReader()
      val resultData = dataReader.read(new HdfsPath("/ai/search_hot_pro.txt", FileFormatEnum.TEXT)).rdd
        .map(line => line.getString(0).split("\t")).map(e => (e(0), e(1), e(2), e(3)))
        .toDF("sid", "contentType", "title", "searchText")



      // 处理数据
      //resultDF["searchText", "contentType", "videoSid", "num"]
      DataProcess.generalData2Kafka(resultData, useeSearchHotTopic)

      resultData.unpersist()
      sc.stop()
    }catch {
      case e: Exception =>
        val info = prefix + "[moretv][searchHotNew]" + "[" + HDFSOps.today + "]"
        val emailName = Array(email)
        SendMail.post(e, info, emailName)
        System.exit(-1)
    }
  }
}
