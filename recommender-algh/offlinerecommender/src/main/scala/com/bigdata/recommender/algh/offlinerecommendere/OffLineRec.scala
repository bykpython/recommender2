package com.bigdata.recommender.algh.offlinerecommendere

import com.bigdata.recommender.algh.common.{ConfigUtils, DataModel}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.jblas.DoubleMatrix

/**
  * 离线推荐
  * 基于协同过滤的推荐：
  * 需求一：基于模型的协同过滤:
  *        基于用户
  *        使用已有真是部分(productId, userId, score)数据训练出，特征模型model，然后将所有的(prodctId, userId)数据预测出prescore值，形成预测结果(productId, userId, prescore)
  *        特征模型中包括两个特征向量：用户特征向量，商品特征向量
  *
  * 需求二：使用需求一中的商品特征向量，进行余弦相似度计算，分析出物品之间的相似程度
  */
object OffLineRec {

  /**
    * 计算余弦相似度
    * @param product1 特征向量1
    * @param product2 特征向量2
    * @return
    */
  def consinSim(product1: DoubleMatrix, product2: DoubleMatrix) : Double ={
    product1.dot(product2) / ( product1.norm2()  * product2.norm2() )
  }

  def main(args: Array[String]): Unit = {

    //创建一个SparkConf配置
    val sparkConf = new SparkConf().setAppName("OfflineRecommender").setMaster("local[*]")

    //基于SparkConf创建一个SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // 导入隐式转换
    import spark.implicits._

    // 获取mongodb配置
    val uri: String = ConfigUtils.getValueByKey("mongodb.uri")
    val db: String = ConfigUtils.getValueByKey("mongodb.db")

    /*****从mongodb中读取rating数据，然后进行结构转换*******/
    // (userId, productId, score, timestamp) ----> (userId, productId, score)
    val rateRDD: RDD[(Int, Int, Double)] = spark
      .read
      .option("uri", uri)
      .option("collection", DataModel.MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[DataModel.Rating]
      .rdd
      .map(rate => {
        (rate.userId, rate.productId, rate.score)
      }).cache()

    // 通过rateDF得到用户数据集
    val userRDD: RDD[Int] = rateRDD.map(_._1).distinct()



    /******从mongodb中获取product数据*******/
    val productRDD: RDD[Int] = spark
      .read
      .option("uri", uri)
      .option("collection", DataModel.MONGODB_PRODUCT_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[DataModel.Product]
      .rdd
      .map(_.productId)


    /*****基于模型的协同过滤****创建训练数据集*************/
    val trainRDD: RDD[Rating] = rateRDD.map(rate=>Rating(rate._1, rate._2, rate._3))

    // r: M x N
    // u: M x K
    // i: K x N

    //rank参数是K参数，iterations是迭代次数，lambda是正则化系数
    val (rank ,iterations, lambda) = (50, 5, 0.01)

    // 通过已知数据trainRDD和参数(rank, iterations, lambda)训练出model
    // model中rank是特征向量的特征维度，userFeatures是用户特征向量，productFeatures是商品特征向量
    val model: MatrixFactorizationModel = ALS.train(trainRDD, rank, iterations, lambda)

    /**********计算用户推荐矩阵***********/
    // 需要构造一个usersProducts RDD[(Int, Int)]
    // (userId).cartesian(productId)===>(userId, productId)
    val userProductRDD: RDD[(Int, Int)] = userRDD.cartesian(productRDD)

    // 预测，得出预测模型
    val predictRates: RDD[Rating] = model.predict(userProductRDD)

    val predictRDD: RDD[(Int, (Int, Double))] = predictRates.filter(_.rating > 0).map(rate => {
      (rate.user, (rate.product, rate.rating))
    })

    val userRecRDD: RDD[(Int, List[(Int, Double)])] = predictRDD.groupByKey().mapValues(iter => {
      iter.toList.sortWith {
        case (left, right) => {
          left._2 > right._2
        }
      }
    })

    // 得出用户推荐列表
    val userRecDF: DataFrame = userRecRDD.map {
      case (user, arrRec) => {
        val recommendations: List[DataModel.Recommendation] = arrRec.map(tup => DataModel.Recommendation(tup._1, tup._2))
        DataModel.UserRecs(user, recommendations)
      }
    }.toDF()

    userRecDF
      .write
      .option("uri", uri)
      .option("collection", DataModel.USER_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()


    /*****使用物品的特征向量计算相似度来进行推荐********/

    val productFeatures: RDD[(Int, DoubleMatrix)] = model.productFeatures.map {
      case (productId, features) => {
        (productId, new DoubleMatrix(features))
      }
    }

    // productFeatures和自己笛卡儿积
    val cartsProductFeaturesRDD: RDD[((Int, DoubleMatrix), (Int, DoubleMatrix))] = productFeatures.cartesian(productFeatures)

    // 将相同向量过滤掉
    val filterProductFeaturesRDD: RDD[((Int, DoubleMatrix), (Int, DoubleMatrix))] = cartsProductFeaturesRDD.filter {
      case (product1, product2) => {
        product1._1 != product2._1
      }
    }

    // 利用两个物品的特征向量计算相似度
    val simScoreOfProduct: RDD[(Int, (Int, Double))] = filterProductFeaturesRDD.map {
      case (product1, product2) => {
        val simScore: Double = this.consinSim(product1._2, product2._2)
        (product1._1, (product2._1, simScore))
      }
    }

    // 过滤出相似度较大的物品，然后分组聚合，提取出每个商品的相似商品列表
    val simListOfProduct: RDD[(Int, List[(Int, Double)])] = simScoreOfProduct.filter {
      case (product1, (product2, simScore)) => {
        simScore > 0.6
      }
    }.groupByKey().mapValues(iter => {
      iter.toList.sortWith {
        case (left, right) => {
          left._2 > right._2
        }
      }.take(10)
    })

    // 转换结构
    val productRecs: DataFrame = simListOfProduct.map {
      case (productId, simList) => {
        val recommendations: List[DataModel.Recommendation] = simList.map(sim => DataModel.Recommendation(sim._1, sim._2))

        DataModel.ProductRecs(productId, recommendations)
      }
    }.toDF()

    // 将每个商品和其相似商品列表存入mongodb中
    productRecs
        .write
        .option("uri", uri)
        .option("collection", DataModel.OFFLINE_PRODUCT_RECS)
        .mode("overwrite")
        .format("com.mongodb.spark.sql")
        .save()

    spark.stop()
  }

}
