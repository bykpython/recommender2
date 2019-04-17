package com.bigdata.recommender.algh.itemcfrecommender

import com.bigdata.recommender.algh.common.{ConfigUtils, DataModel}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * 基于协同过滤CF的推荐：
  * 细分：基于物品的协同过滤推荐：
  *       分析：1.购买过product1的用户数usercount1
  *            2.购买过product2的用户数usercount2
  *            3.既购买过product1也购买过product2的用户数usercount3
  *
  *            4.使用同现相似度计算物品之间的相似度
  */
object ItemCFRec {

  /**
    * 同现相似度计算公式
    *
    * @param numOfRatersForAAndB
    * @param numOfRatersForA
    * @param numOfRatersForB
    * @return
    */
  def coocurence(numOfRatersForAAndB: Long, numOfRatersForA: Long, numOfRatersForB: Long): Double = {
    numOfRatersForAAndB / math.sqrt(numOfRatersForA * numOfRatersForB)
  }

  def main(args: Array[String]): Unit = {

    // 创建一个spark配置
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ItemCFRec")

    // 创建一个sparksession对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    // 导入隐式转换
    import spark.implicits._

    // 调高日志等级
    spark.sparkContext.setLogLevel("ERROR")

    val uri: String = ConfigUtils.getValueByKey("mongodb.uri")
    val db: String = ConfigUtils.getValueByKey("mongodb.db")

    // 从mongodb的rate(userId, productId, score, timestamp)获取数据,然后转换成(userId, productId, score)
    val rateDF: DataFrame = spark
      .read
      .option("uri", uri)
      .option("collection", DataModel.MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[DataModel.Rating]
      .map(rate => {
        (rate.userId, rate.productId, rate.score)
      })
      .cache()
      .toDF("userId", "productId", "score")

    // 每一商品有多少用户进行了评分(productId, userCount|nor)
    val userCountOfProductRate: Dataset[Row] = rateDF.groupBy("productId").count().alias("nor")

    // 在userCountOfProductRate记录基础上加上product的打分者的数量(userId, productId, score, userCount)
    // uid1, pid1, 1, pid1-usercount
    // uid2, pid1, 2, pid1-usercount
    // uid3, pid1, 1, pid1-usercount
    // uid1, pid2, 1, pid2-usercount
    // uid2, pid2, 2, pid2-usercount
    // uid3, pid2, 1, pid2-usercount
    val rateWithSize: DataFrame = rateDF.join(userCountOfProductRate, "productId")

    // 执行内联操作，自己内联自己rateWithSize(userId, productId1, score1, userCount1, productId2, score2, userCount2)
    // uid1, pid1, 1, pid1-usercount pid1, 1, pid1-usercount
    // uid1, pid1, 1, pid1-usercount pid2, 1, pid2-usercount

    // uid2, pid1, 2, pid1-usercount pid1, 2, pid1-usercount
    // uid2, pid1, 2, pid1-usercount pid2, 2, pid2-usercount

    // uid3, pid1, 1, pid1-usercount pid1, 1, pid1-usercount
    // uid3, pid1, 1, pid1-usercount pid2, 1, pid2-usercount
    val joinedDF: DataFrame = rateWithSize.join(rateWithSize, "userId")
      .toDF("userId", "product1", "rating1", "nor1", "product2", "rating2", "nor2")


    // uid1, pid1, 1, pid1-usercount pid2, 1, pid2-usercount
    // uid2, pid1, 2, pid1-usercount pid2, 2, pid2-usercount
    // uid3, pid1, 1, pid1-usercount pid2, 1, pid2-usercount
    val filterJoinedDF: Dataset[Row] = joinedDF.filter(rate => rate.getAs[Int]("product1") != rate.getAs[Int]("product2"))


    // uid1, pid1, 1, pid2, 1
    // uid2, pid1, 2, pid2, 2
    // uid3, pid1, 1, pid2, 1
    filterJoinedDF.selectExpr("userId", "product1", "nor1", "product2", "nor2").createOrReplaceTempView("filterjoined")

    // 选出既给product1评分过，又给product2评分过的用户，nor1/2都是通过product分组聚合得到的，同一个product的nor是相等的
    val doubleUsersql = "select product1, product2, count(userId), first(nor1), first(nor2) from filterjoined group by product1, product2"

    // uid3, pid1, pid2, userCount, userCountPid1, userCountPid2
    val doubleUserDF: DataFrame = spark.sql(doubleUsersql).toDF("product1","product2", "dbUserCount","nor1", "nor2")

    // 利用同现相似计算物品之间的相似度
    val sim: Dataset[(Int, Int, Double)] = doubleUserDF.map(rate => {
      val size: Long = rate.getAs[Long](2)
      val userSumOfProduct1: Long = rate.getAs[Long](3)
      val userSumOfProduct2: Long = rate.getAs[Long](4)

      val cooc: Double = coocurence(size, userSumOfProduct1, userSumOfProduct2)

      (rate.getInt(0), rate.getInt(1), cooc)
    })

    // 将物品之间的相似度进行结构转换
    val mapRDD: RDD[(Int, (Int, Double))] = sim.map(data => {
      val tuple: (Int, (Int, Double)) = (data._1, (data._2, data._3))
      tuple
    }).rdd

    val groupByRDD: RDD[(Int, Iterable[(Int, Double)])] = mapRDD.groupByKey()

    val simDF: DataFrame = groupByRDD.mapValues(iter => {
      val tuples: List[(Int, Double)] = iter.toList.sortWith {
        case (left, right) => {
          left._2 > right._2
        }
      }.take(5)

      val recommendations: List[DataModel.Recommendation] = tuples.map(tup => DataModel.Recommendation(tup._1, tup._2))
      recommendations
    }).toDF("product1", "items")

    // 将物品相似度存入mongodb中
    simDF
      .write
      .option("uri", uri)
      .option("collection", DataModel.PRODUCT_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    // 关闭spark
    spark.stop()

  }

}
