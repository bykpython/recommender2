package com.bigdata.recommender.algh.dataloader

import com.bigdata.recommender.algh.common.{ConfigUtils, DataModel}
import com.bigdata.recommender.algh.common.DataModel.{MongoConfig, Product}
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object DataLoader {

  // 定义一个含有隐式参数的方法
  def storeDataInMongoDB(productDF: DataFrame, ratingDF: DataFrame)(implicit config:MongoConfig) = {

    // 和mongodb建立连接
    val mongoClient = MongoClient(MongoClientURI(config.uri))

    // 如果mongodb中有对应的数据集，则删除
    mongoClient(config.db)(DataModel.MONGODB_PRODUCT_COLLECTION).dropCollection()
    mongoClient(config.db)(DataModel.MONGODB_RATING_COLLECTION).dropCollection()

    // 将product数据和rating数据存入到mongodb中
    productDF
      .write
      .option("uri", config.uri)
      .option("collection", DataModel.MONGODB_PRODUCT_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    ratingDF
      .write
      .option("uri", config.uri)
      .option("collection", DataModel.MONGODB_RATING_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    // 对数据集product中的productId建立索引
    mongoClient(config.db)(DataModel.MONGODB_PRODUCT_COLLECTION).createIndex(MongoDBObject("productId"->1))

    // 对数据集rating中的userId、productId建立索引
    mongoClient(config.db)(DataModel.MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("userId"->1))
    mongoClient(config.db)(DataModel.MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("productId"->1))

    // 关闭mongodb的连接
    mongoClient.close()

  }

  def main(args: Array[String]): Unit = {

    // 创建spark配置对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DataLoader")

    // 创建sparksession对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    // 导入隐式转换
    import spark.implicits._

    // 从指定文件读取product数据
    val productRDD: RDD[String] = spark.sparkContext.textFile(DataModel.PRODUCTS_DATA_PATH)

    // 将数据进行切分，获取所需字段，映射成product类型
    val productDF: DataFrame = productRDD.map(product => {
      val attr: Array[String] = product.split("\\^")

      Product(attr(0).toInt, attr(1).trim, attr(5).trim, attr(4).trim, attr(6).trim)
    }).toDF()


    // 从指定文件读取rating数据
    val ratingRDD: RDD[String] = spark.sparkContext.textFile(DataModel.RATING_DATA_PATH)

    val ratingDF: DataFrame = ratingRDD.map(rate => {
      val attr: Array[String] = rate.split(",")

      DataModel.Rating(attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
    }).toDF()

    // 设置隐式变量
    implicit val mongoConfig = MongoConfig(ConfigUtils.getValueByKey("mongodb.uri"), ConfigUtils.getValueByKey("mongodb.db"))

    // 存入mongodb中
    storeDataInMongoDB(productDF, ratingDF)

    spark.stop()
  }

}
