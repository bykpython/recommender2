package com.bigdata.recommender.algh.contentrecommender

import com.bigdata.recommender.algh.common.{ConfigUtils, DataModel}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{HashingTF, IDF, IDFModel, Tokenizer}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.jblas.DoubleMatrix

/**
  * 基于内容CB的推荐
  */
object ContentBasedRec {

  def consinSim(product1: DoubleMatrix, product2: DoubleMatrix) : Double ={
    product1.dot(product2) / ( product1.norm2() * product2.norm2() )
  }

  def main(args: Array[String]): Unit = {

    // 创建一个SparkConf配置
    val sparkConf = new SparkConf().setAppName("ContentBasedRecommender").setMaster("local[*]")

    // 基于SparkConf创建一个SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    // 导入隐式转化
    import spark.implicits._

    // 设置日志等级
    spark.sparkContext.setLogLevel("ERROR")

    // 读取mongodb配置
    val uri: String = ConfigUtils.getValueByKey("mongodb.uri")
    val db: String = ConfigUtils.getValueByKey("mongodb.db")

    // 从mongodb中读取product信息
    val productRDD: RDD[(Int, String, String)] = spark
      .read
      .option("uri", uri)
      .option("collection", DataModel.MONGODB_PRODUCT_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[DataModel.Product]
      .rdd
      .map(x => {
        val newTags: String = x.tags.map(t => if (t == "|") ' ' else t)
        (x.productId, x.name, newTags)
      })

    /*val productSeq: Array[(Int, String, String)] = productRDD.collect()

    val tagsData: DataFrame = spark.createDataFrame(productSeq).toDF("productId", "name", "tags")
    */

    //(productId, name, tags)
    val tagsData: DataFrame = productRDD.toDF("productId", "name", "tags")

    // 实例化一个分词器，默认按空格分开
    val tokenizer: Tokenizer = new Tokenizer().setInputCol("tags").setOutputCol("words")

    // 把tagsData中的tags进行分词
    // 用分词器做转换，生成列words，返回一个dataframe，增加列words
    // (productId, name, words)
    val wordsData: DataFrame = tokenizer.transform(tagsData)

    wordsData.show(5)

    // HashingTF是一个工具，可以把一个词语序列，转换成词频（初始特征）
    val hashingTF: HashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(189)

    // 用HashingTF做处理，返回dataframe
    // (productId, name, rawFeatures)
    val featurizedData: DataFrame = hashingTF.transform(wordsData)

    // IDF也是一个工具，用于计算文档的IDF
    val idf: IDF = new IDF().setInputCol("rawFeatures").setOutputCol("features")

    // 将词频数据传入，得到idf模型（统计文档）
    // (productId, name, features)
    val idfModel: IDFModel = idf.fit(featurizedData)

    // 模型对原始数据做处理，计算出idf后，用tf-idf得到新的特征矩阵
    val rescaledData: DataFrame = idfModel.transform(featurizedData)

    rescaledData.show(5)

    // 商品的特征向量
    val prodctFeatures: Dataset[(Int, Array[Double])] = rescaledData.map(row => {

      (row.getAs[Int]("productId"), row.getAs[SparseVector]("features").toArray)
    })

    val productMatrix: RDD[(Int, DoubleMatrix)] = prodctFeatures.rdd.map(x=>(x._1, new DoubleMatrix(x._2)))

    // 使用商品的特征向量进行笛卡儿积，然后过滤，计算相似度，分组，选前几个
    val cartsProductRDD: RDD[((Int, DoubleMatrix), (Int, DoubleMatrix))] = productMatrix.cartesian(productMatrix)

    // 过滤掉相同商品的笛卡儿积
    val filterCartsProductRDD: RDD[((Int, DoubleMatrix), (Int, DoubleMatrix))] = cartsProductRDD.filter {
      case ((product1, matrix1), (product2, matrix2)) => {
        product1 != product2
      }
    }

    // 使用物品的特征向量计算物品之间的相似度
    val productRecs: RDD[DataModel.ProductRecs] = filterCartsProductRDD.map {
      case ((product1, matrix1), (product2, matrix2)) => {
        val simScore: Double = this.consinSim(matrix1, matrix2)

        (product1, (product2, simScore))
      }
    }.groupByKey()
      .map {
        case (product1, simList) => {
          val recommendations: List[DataModel.Recommendation] = simList.toList.sortWith {
            case (left, right) => {
              left._2 > right._2
            }
          }.take(5).map(tup => DataModel.Recommendation(tup._1, tup._2))

          DataModel.ProductRecs(product1, recommendations)
        }
      }

    val productRecsDF: DataFrame = productRecs.toDF()

    productRecsDF.show(5)

    productRecsDF
      .write
      .option("uri", uri)
      .option("collection", DataModel.CONTENT_PRODUCT_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    // 关闭spark
    spark.close()

  }
}
