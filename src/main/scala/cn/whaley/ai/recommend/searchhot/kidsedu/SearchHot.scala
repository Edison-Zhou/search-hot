package cn.whaley.ai.recommend.searchhot.kidsedu

import cn.moretv.doraemon.common.enum.FileFormatEnum
import cn.whaley.ai.recommend.searchhot.conf.SearchHotConf
import cn.whaley.ai.recommend.searchhot.utils.DataProcess
import cn.whaley.ai.recommend.utils.DataRetrieval
import cn.whaley.sdk.utils.{SendMail, TransformUDF}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import cn.moretv.doraemon.data.reader.DataReader
import cn.moretv.doraemon.common.path.{HdfsPath, HivePath, MysqlPath}
import cn.moretv.doraemon.common.util.DateUtils


/*
 * Created by Edison_Zhou on 2019/4/22.
 * 喵学堂的大家在搜功能，第一期适逢数据改造，且暂无搜索日志，故直接读取数据库数据并写死
 * 二期考虑读取播放事实表，后续日志打点有“searchText”字段再恢复传统逻辑
 * 三期亦即目前运行在现网的版本，采用与优视猫相同的方式，读取编辑提供的固定数据（包括searchText）
*/


object SearchHot extends SearchHotConf with DataRetrieval{
//两trait文件分别记录热词搜索的配置信息; 获取用户日志信息和从MySQL中获取视频可用信息的API

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

//      val startDate = DateUtils.farthestDayWithOutDelimiter(2)

      /*
      // 从mtv_program_album中读取用户长视频的浏览日志
      val dataReader = new DataReader()
      val programInfo = dataReader.read(new MysqlPath("10.19.98.46", 3306, "aries_cms",
        "mtv_program_album", "readonly", "readonly", "id", Array("sid", "title", "content_type"),
        "sid is not null and title is not null and status=1 limit 30"))

      val programSearchData = programInfo.rdd.map(r => {
        val sid = r.getString(0)
        val title = r.getString(1)
        val contentType = r.getString(2)
        val fullSpell = ObtainPinYinUtils.polyPinYinQuanPin(title).toArray().head
        val fullSpell4Search = fullSpell.asInstanceOf[String]
        val fullInitialSearchWord = fullSpell4Search.split(" ").map(e => e.substring(0,1)).reduce((x, y)=> x+y)
        val searchText = if (fullInitialSearchWord.length >= 4) fullInitialSearchWord.substring(0,4) else fullInitialSearchWord
        (sid, contentType, title, searchText)
      }).toDF("sid", "contentType", "title", "searchText")

      val subjectInfo = dataReader.read(new MysqlPath("10.19.98.46", 3306, "aries_cms",
        "child_edu_course_subject", "readonly", "readonly", "id", Array("course_subject_sid", "name"),
        "course_subject_sid is not null and name is not null and status=1"))

      val subjectSearchData = subjectInfo.rdd.map(r => {
        val sid = r.getString(0)
        val title = r.getString(1)
        val fullSpell = ObtainPinYinUtils.polyPinYinQuanPin(title).toArray().head
        val fullSpell4Search = fullSpell.asInstanceOf[String]
        val fullInitialSearchWord = fullSpell4Search.split(" ").map(e => e.substring(0,1)).reduce((x, y)=> x+y)
        val searchText = if (fullInitialSearchWord.length >= 4) fullInitialSearchWord.substring(0,4) else fullInitialSearchWord
        (sid, "course_subject", title, searchText)
      }).toDF("sid", "contentType", "title", "searchText")

      val initialSearchData = programSearchData.union(subjectSearchData).persist(StorageLevel.MEMORY_AND_DISK)
      */

      /*
      val kidseduProgramPlayInfo = dataReader.read(HivePath("select distinct user_id, parent_sid as videoSid,"
        + " parent_title as title, content_type as contentType, 1 as num from dw_facts.fact_kids_play " +
        s"where parent_sid is not null and parent_sid != '' and parent_title is not null and day_p >= ${startDate}"))

      val titleData = kidseduProgramPlayInfo.
        groupBy("title", "contentType", "videoSid").agg(Map("num" -> "sum")).
        withColumnRenamed("sum(num)", "num").
        selectExpr("title", "contentType", "videoSid", "num").
        persist(StorageLevel.MEMORY_AND_DISK)

      val effectiveSid = dataReader.read(new MysqlPath("10.19.33.171", 3306, "tvservice",
        "mtv_program", "readonly", "readonly", "id", Array("sid"),
        "status = 1 and parentId = 0"))

      val unionData = titleData.join(effectiveSid, titleData("videoSid")===effectiveSid("sid")).
        selectExpr("title", "contentType", "videoSid", "num")

      val initialSearchData = unionData.rdd.map(r => {
        val title = r.getString(0)
        val contentType = r.getString(1)
        val sid = r.getString(2)
        val playNum = r.getLong(3)
        val fullSpell = ObtainPinYinUtils.polyPinYinQuanPin(title).toArray().head
        val fullSpell4Search = fullSpell.asInstanceOf[String]
        val fullInitialSearchWord = fullSpell4Search.split(" ").map(e => e.substring(0,1)).reduce((x, y)=> x+y)
        val searchText = if (fullInitialSearchWord.length >= 4) fullInitialSearchWord.substring(0,4) else fullInitialSearchWord
        (playNum, (sid, contentType, title, searchText))
      }).sortByKey(false).map(e => e._2).toDF("sid", "contentType", "title", "searchText")
*/
      val dataReader = new DataReader()
      val resultData = dataReader.read(new HdfsPath("/ai/kidsedu_search_hot.txt", FileFormatEnum.TEXT)).rdd
        .map(line => line.getString(0).split("\t")).map(e => (e(0), e(1), e(2), e(3)))
        .toDF("sid", "contentType", "title", "searchText")
      getDataFrameInfo(resultData, "initialSearchData")


      def getDataFrameInfo(df: DataFrame,name:String): Unit = {
        println(s"$name.count():"+df.count())
        println(s"$name.printSchema:")
        df.printSchema()
        df.show(10, false)
      }

      //数据写入kafka
      DataProcess.generalData2Kafka(resultData, kidseduSearchHotTopic)
      //resultData.show(10)
      //println(resultData.count())
      resultData.unpersist()
      sc.stop()
    }

  }
}
