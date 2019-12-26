package cn.whaley.ai.recommend.searchhot.utils

import java.util.Calendar
import java.util.Properties

import cn.whaley.ai.recommend.searchhot.conf.SearchHotConf
import cn.whaley.ai.recommend.utils.{DataRetrieval, DateFormatUtils}
import cn.whaley.sdk.dataOps.{KafkaOps, MySqlOps}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.kafka.clients.producer. {KafkaProducer, ProducerRecord}
import org.json.JSONObject
import cn.moretv.doraemon.common.util.ArrayUtils

/**
 * Created by xutong on 2017/2/7.
 * 处理搜索热词的数据
 */
object DataProcess extends SearchHotConf with DataRetrieval{

  /**
   * 处理大家在搜数据，并插入kafka
   * @param data 大家在搜数据DF["searchText", "contentType", "videoSid", "num"]
   * @param topic kafka topic名
   * @param mysqlName mysql映射名
   */
  def sendData2Kafka(data: DataFrame, topic: String, mysqlName: String)
                    (implicit sparkSession: SparkSession) = {

    import sparkSession.implicits._
    val sc = sparkSession.sparkContext
    // 创建数据库连接
    val mysqlOps = new MySqlOps(mysqlName)


    // 数据库查询语句, 连接的库为moretv_cms_mysql, 即bigdata-appsvr-130-2 (IP 10.255.130.2)
    val sql = topic match {
      case "moretv-rec4search-topvideo" => "SELECT sid, display_name, status FROM mtv_basecontent WHERE ID >= ? AND ID <= ? and display_name is not null and risk_flag = 0"
        //仅最新版本包含地域屏蔽, 其余版本不包含
      case _ => "SELECT sid, display_name, status FROM mtv_basecontent WHERE ID >= ? AND ID <= ? and display_name is not null "
    }

    // 数据库数据RDD[videoSid, title, status]
    val dataFromMysql = mysqlOps.getJdbcRDD(sc, sql, "mtv_basecontent",
      r => (r.getString(1), r.getString(2).replaceAll("\"", ""), r.getInt(3)), 5).
      toDF("sid", "title", "status")

    // 得到时间日期, yyyy-MM-dd的形式
    val cal = Calendar.getInstance()
    val date = DateFormatUtils.readFormat.format(cal.getTime)
    val day = DateFormatUtils.toDateCN(date)    //中文格式的日期 "yy-MM-dd"

    // 合并日志数据和数据库数据RDD[videosid, contentType, searchText, num, title, status, date]
    val unionData = data.join(dataFromMysql, data("videoSid") === dataFromMysql("sid")).
      selectExpr("videoSid", "contentType", "searchText", "num", "title", "status").rdd.
      map(r => {
      val videoSid = r.getString(0)
      val contentType = r.getString(1)
      val searchText = r.getString(2)
      val num = r.getLong(3)
      val title = r.getString(4)
      val status =  r.getInt(5)
      ((videoSid, contentType, title, status), (searchText, num))
    }).groupByKey().
      map(r => {
      val videoSid = r._1._1
      val contentType = r._1._2
      val title = r._1._3
      val status = r._1._4
      val searchValueArray = r._2.toList.sortBy(-_._2)
      val searchText = searchValueArray(0)._1     //得到的是搜索频次最高的搜索词
      val num = searchValueArray.map(_._2).sum    //得到的是视频被不同的搜索词搜索的总次数
      (videoSid, contentType, searchText, num, title, status, day)
    })

    topic match {
      case "Helios_SearchHot" =>
        videoTypeList.foreach(videoType => {

          //计算每个视频的播放次数，从大到小排序，取前num个[(sid, title, status, date), num]
          val videoTop = unionData.filter(_._2 == videoType).map(e => ((e._1, e._5, e._6, e._7,e._3), e._4)).
            //得到((sid,title,status,day,searchtext), num)的pairRdd
            sortBy(-_._2).take(videoTopNum).
            map(r => {
            val title = r._1._2
            val sid = r._1._1
            val searchCode = r._1._5
            val contentType = videoType
            val status = r._1._3
            val amount = r._2
            val arrange_date = r._1._4
            title + "||" + searchCode + "||" + sid + "||" + contentType  + "||" + status + "||" + amount + "||" + arrange_date
          } ).toSeq
          if(videoTop.size != 0)
            KafkaOps.writeLocal2Kafka(videoTop, topic)

        })
      case "MoreTV_SearchHot" =>
        videoTypeList.foreach(videoType => {

          //计算每个视频的播放次数，从大到小排序，取前num个[(sid, title, status, date), num]
          val videoTop = unionData.filter(_._2 == videoType).map(e => ((e._1, e._5, e._6, e._7, e._3), e._4)).
            sortBy(-_._2).take(videoTopNum).
            map(r => {
            val title = r._1._2
            val sid = r._1._1
            val searchCode = r._1._5
            val contentType = videoType
            val status = r._1._3
            val amount = r._2
            val arrange_date = r._1._4
            title + "||" + searchCode + "||" + sid + "||" + contentType  + "||" + status + "||" + amount + "||" + arrange_date
          } ).toSeq
          KafkaOps.writeLocal2Kafka(videoTop, topic)
        })
      case _ =>
      {
        val topVideoJsonData = new org.json.JSONObject()
        //unionData: RDD[videoSid, contentType, searchText, num, title, status, day]
        unionData.map(r => {
          val sid = r._1
          val contentType = r._2
          val searchCode = r._3
          val title = r._5
          val num = r._4
          (contentType, ((sid, searchCode, title), num))
        }).groupByKey().map(r => {
          val contentType = r._1
          val sidArray = r._2.toArray.sortBy(-_._2).take(videoTopNum)
          (contentType, sidArray)
        }).collect().foreach(info => {
          val idJsonArray = new org.json.JSONArray()
          for(e <- info._2) {
            val videoInfo = new JSONObject()
            videoInfo.put("sid", e._1._1)
            videoInfo.put("searchCode", e._1._2)
            videoInfo.put("title", e._1._3)
            videoInfo.put("contentType", info._1)
            idJsonArray.put(videoInfo)
          }
          topVideoJsonData.put(info._1, idJsonArray)
        })
        //分视频类别的json信息, 包含(contentType, [(sid, searchCode, title, contentType), ...])

        val totalInfo = unionData.map(r => {
          val sid = r._1
          val searchCode = r._3
          val title = r._5
          val num = r._4
          val contentType = r._2
          (((sid, searchCode, title, contentType), num))
        }).collect().sortBy(-_._2).take(videoTopNum)

        val idJsonArray = new org.json.JSONArray()
        for(e <- totalInfo) {
          val videoInfo = new JSONObject()
          videoInfo.put("sid", e._1._1)
          videoInfo.put("searchCode", e._1._2)
          videoInfo.put("title", e._1._3)
          videoInfo.put("contentType", e._1._4)
          idJsonArray.put(videoInfo)
        }
        topVideoJsonData.put("total", idJsonArray)
        //全量视频的json信息, 格式为("total", [(sid, searchCode, title, contentType), ...])

        println(topVideoJsonData.toString)
        for(i <- 1 to 10)
          KafkaOps.writrString2Kafka(topVideoJsonData.toString, topic)

      }

    }

   mysqlOps.destory()
  }

  def generalData2Kafka(data:DataFrame, topic:String)(implicit sparkSession: SparkSession) = {
    import sparkSession.implicits._
    val sc = sparkSession.sparkContext
    val topVideoJsonData = new org.json.JSONObject()
    //unionData: RDD[videoSid, contentType, searchText, num, title, status, day]
    data.rdd.map(r => {
      val sid = r.getString(0)
      val contentType = r.getString(1)
      val title = r.getString(2)
      val searchCode = r.getString(3)
      (contentType, (sid, searchCode, title))
    }).groupByKey().map(r => {
      val contentType = r._1
//      val sidArray = r._2.toArray.take(videoTopNum)
      val sidArray = ArrayUtils.randomTake(r._2.toArray, videoTopNum)
      (contentType, sidArray)
    }).collect().foreach(info => {
      val idJsonArray = new org.json.JSONArray()
      for (e <- info._2) {
        val videoInfo = new JSONObject()
        videoInfo.put("sid", e._1)
        videoInfo.put("searchCode", e._2)
        videoInfo.put("title", e._3)
        videoInfo.put("contentType", info._1)
        idJsonArray.put(videoInfo)
      }
      topVideoJsonData.put(info._1, idJsonArray)
    })

    val totalInfob4truncate = data.map(r => {
      val sid = r.getString(0)
      val searchCode = r.getString(3)
      val title = r.getString(2)
      val contentType = r.getString(1)
      (sid, searchCode, title, contentType)
    }).collect()

    val totalInfo = ArrayUtils.randomTake(totalInfob4truncate, videoTopNum)

    val idJsonArray = new org.json.JSONArray()
    for(e <- totalInfo) {
      val videoInfo = new JSONObject()
      videoInfo.put("sid", e._1)
      videoInfo.put("searchCode", e._2)
      videoInfo.put("title", e._3)
      videoInfo.put("contentType", e._4)
      idJsonArray.put(videoInfo)
    }
    topVideoJsonData.put("total", idJsonArray)
    //全量视频的json信息, 格式为("total", [(sid, searchCode, title, contentType), ...])

    println(topVideoJsonData.toString)
    for(i <- 1 to 10)
      writrString2Kafka(topVideoJsonData.toString, topic)
  }

  /**
   * moretv搜索路径匹配，若匹配成功，则返回搜索词
   * @param path：日志中的路径
   * @return：搜索词
   */
  def matchPathByMoretv(path: String): String = {
    val reg = "home-(movie-|tv-|zongyi-|comic-|jilu-|hot-|xiqu-|mv-|kids-|game-)(search-)?([A-Z]+)".r
    if(path == null){
      null
    }else{
      val format = reg findFirstMatchIn path
      val result = format match {
        case Some(x) =>{
          val searchCode = x.group(3)
          searchCode
        }
        case None => null
      }
      result
    }
  }

  /**
   * medusa搜索路径匹配，若匹配成功，则返回搜索词
   * @param path：日志中的路径
   * @return：搜索词
   */
  def matchPathByMedusa(path: String): String = {
    val reg = "search*([A-Z]+)".r
    if(path == null){
      null
    }else{
      val format = reg.findFirstMatchIn(path)
      val result = format match {
        case Some(x) =>{
          val searchCode = x.group(1)
          searchCode
        }
        case None => null
      }
      result
    }
  }
  def writrString2Kafka(data: String,topic: String): Unit ={
    val props = new Properties()

    props.put("bootstrap.servers", "bigdata-appsvr-130-1:9095,bigdata-appsvr-130-2:9095," +
      "bigdata-appsvr-130-3:9095,bigdata-appsvr-130-4:9095,bigdata-appsvr-130-5:9095,bigdata-appsvr-130-6:9095," +
      "bigdata-appsvr-130-7:9095,bigdata-appsvr-130-8:9095,bigdata-appsvr-130-9:9095")
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("partitioner.class", "kafka.producer.DefaultPartitioner")
    props.put("request.required.acks", "1")
    props.put("producer.type", "async")
    props.put("batch.num.messages", "100")
    props.put("compression.codec", "snappy")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String,String](props)
    val msg = new ProducerRecord[String,String](topic,Math.random()+"",data)

    producer.send(msg)
    producer.close()
  }

}
