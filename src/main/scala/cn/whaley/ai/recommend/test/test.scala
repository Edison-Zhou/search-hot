package cn.whaley.ai.recommend.test

import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Administrator on 2017/8/16.
 */
object test {

   def main (args: Array[String]) {
     val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("a")
     val sc = new SparkContext(sparkConf)
     implicit val sparkSession = SparkSession.builder().getOrCreate()


     val df = sparkSession.createDataFrame(Seq(
       (0, "a"),
       (1, "b"),
       (2, "c"),
       (3, "a"),
       (4, "a"),
       (5, "c")
     )).toDF("id", "category")

     val indexer = new StringIndexer()
       .setInputCol("category")
       .setOutputCol("categoryIndex")
       .fit(df)
     val indexed = indexer.transform(df)

     val encoder = new OneHotEncoder()
       .setInputCol("categoryIndex")
       .setOutputCol("categoryVec")

     val encoded = encoder.transform(indexed)
     encoded.show()
  }
}
