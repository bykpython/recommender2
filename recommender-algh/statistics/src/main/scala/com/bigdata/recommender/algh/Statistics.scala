package com.bigdata.recommender.algh

import java.text.SimpleDateFormat
import java.util.Date

import com.bigdata.recommender.algh.common.{ConfigUtils, DataModel}
import com.bigdata.recommender.algh.common.DataModel.MongoConfig
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object Statistics {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Statistics")

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")

    // 连接mongodb
    val uri: String = ConfigUtils.getValueByKey("mongodb.uri")
    val db: String = ConfigUtils.getValueByKey("mongodb.db")

    // 读取数据
    val productDF: DataFrame = spark
      .read
      .option("uri", uri)
      .option("collection", DataModel.MONGODB_PRODUCT_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[DataModel.Product]
      .toDF()

    val ratingDF: DataFrame = spark
      .read
      .option("uri", uri)
      .option("collection", DataModel.MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[DataModel.Rating]
      .toDF()

    // 为评分记录数据创建一个临时视图--评分记录表ratings
    ratingDF.createOrReplaceTempView("rating")

    // 从ratings查询数据形成结构--->(productId, count)：商品被评分的次数
    // 按照商品id进行分组
    val rateSql = "select productId, count(productId) from rating group by productId"
    val ratesOfProduct: DataFrame = spark.sql(rateSql)

    // 将ratesOfProduc(productId, ratecount)t存入mongodb中
    ratesOfProduct
      .write
      .option("uri", uri)
      .option("collection", DataModel.RATE_MORE_PRODUCTS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    // 自定义一个udf函数，用于处理时间
    val dataFormat = new SimpleDateFormat("yyyyMM")
    spark.udf.register("changeDate", (x:Int)=>dataFormat.format(new Date(x*1000L)).toInt)

    val recentSql = "select productId, score, changeDate(timestamp) as yearmonth from rating"
    val ratingOfYearMonth: DataFrame = spark.sql(recentSql)

    // (productId, score, yyyyMM)
    ratingOfYearMonth.createOrReplaceTempView("ratingOfMonth")

    // 将ratingOfMonth视图数据按照月份，商品进行分组
    val ratesOfYearMonth = "select yearmonth, productId, count(productId) from ratingOfMonth group by yearmonth, productId"
    val rateMoreRecentlyProducts: DataFrame = spark.sql(ratesOfYearMonth)

    // 将rateMoreRecentlyProducts存入mongodb中
    rateMoreRecentlyProducts
      .write
      .option("uri", uri)
      .option("collection", DataModel.RATE_MORE_RECENTLY_PRODUCTS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    // 从评分记录表中，按照商品id分组，获取每一个商品的平均的评分
    val avgScoreOfProduct = "select productId, avg(score) as avg from rating group by productId order by avg desc"
    val avgScoreOfProductDF: DataFrame = spark.sql(avgScoreOfProduct)

    // 将avgScoreOfProductDF存入mongodb中
    avgScoreOfProductDF
      .write
      .option("uri", uri)
      .option("collection", DataModel.AVERAGE_PRODUCTS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    // 关闭spark
    spark.stop()
  }

}
