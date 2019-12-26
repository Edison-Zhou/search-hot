package cn.whaley.ai.recommend.utils

import java.sql.ResultSet
import cn.whaley.sdk.dataOps.{HDFSOps, MySqlOps}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import scala.reflect.ClassTag

/**
 * Created by xutong on 2016/12/22.
 * 数据获取部分接口程序抽象
 */
trait DataRetrieval {

  /**
   * 获取用户的日志信息
   * @param service 业务线
   * @param logType 日志类型
   * @param numOfDays 当天起往前的天数
   * @param condition 日志过滤条件
   * @param fieldArray 日志选取字段
   * @param resultArray 输出字段
   * @param ss SparkSession
   * @return
   */
  def getUserInfoFromHDFS[T: ClassTag](service: String, logType: String, numOfDays: Int, condition: String,
                           fieldArray: Array[String], resultArray: Array[String])
                         (implicit ss: SparkSession) = {

    HDFSOps.getDataByTodayNumSS(service, logType, numOfDays).
      selectExpr(fieldArray:_*).
      filter(condition).
      selectExpr(resultArray:_*)
  }

  /**
   * 从mysql中获取视频的可用信息
   * @param dbName 数据库的映射名
   * @param tableName 表名
   * @param sql 查询语句
   * @param func 输出函数
   * @param sc SparkContext
   * @tparam T 类型
   * @return
   */
  def getMetaDataFromMysql[T: ClassTag](sc: SparkContext, dbName: String, tableName: String,
                                        sql: String, func: (ResultSet => T), numPartition: Int = 5)
                                        = {
    val mysqlOps = new MySqlOps(dbName)
    val result = mysqlOps.getJdbcRDD(sc, sql, tableName, func, numPartition)
    mysqlOps.destory()
    result
  }

}
