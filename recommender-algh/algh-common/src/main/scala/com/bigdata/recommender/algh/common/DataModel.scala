package com.bigdata.recommender.algh.common

object DataModel {

  /**
    * 商品信息
    * @param productId  商品id
    * @param name       商品名称
    * @param categories  商品类别
    * @param imageUrl  商品图片url
    * @param tags       商品标签
    */
  case class Product(productId: Int, name: String, categories: String, imageUrl: String, tags: String)

  /**
    * mongo数据库配置
    * @param uri      数据库连接
    * @param db       数据库名称
    */
  case class MongoConfig(uri: String, db: String)

  /**
    * 评分记录
    * @param userId     用户id
    * @param productId  品id
    * @param score      商品评分
    * @param timestamp  时间戳
    */
  case class Rating(userId: Int, productId: Int, score: Double, timestamp: Int)

  /**
    * 用户信息
    * @param userId
    */
  case class User(userId: Int)

  /**
    * 推荐信息
    * @param rid  推荐的商品id
    * @param r    推荐指数
    */
  case class Recommendation(rid: Int, r: Double)

  /**
    * 相似商品信息
    * @param productId  商品id
    * @param recs       与当前商品id相似的推荐商品列表
    */
  case class ProductRecs(productId: Int, recs: Seq[Recommendation])


  case class UserRecs(userId: Int, recs: Seq[Recommendation])


  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_PRODUCT_COLLECTION = "Products"

  /*******统计模块相关变量*************/
  val RATE_MORE_PRODUCTS = "RateMoreProducts"
  val RATE_MORE_RECENTLY_PRODUCTS = "RateMoreRecentlyProducts"
  val AVERAGE_PRODUCTS = "AverageProducts"
  val TOP_PRODUCTS = "GenresTopProducts"


  /**********基于内容推荐模块相关变量*****************/
  val PRODUCT_RECS = "ItemCFProductRecs"


  /***********数据加载模块相关变量******************/
  val PRODUCTS_DATA_PATH = "D:\\tmpfile\\recommender2\\recommender-algh\\dataloader\\src\\main\\resources\\100products.csv"
  val RATING_DATA_PATH = "D:\\tmpfile\\recommender2\\recommender-algh\\dataloader\\src\\main\\resources\\9000_users_100_products_ratings.csv"

  /***********离线推荐模块相关变量***************/
  val USER_MAX_RECOMMENDATION = 20
  val USER_RECS = "UserRecs"
  val OFFLINE_PRODUCT_RECS = "OffLineProductRecs"


  /***********基于内容推荐相关模块*************/
  val CONTENT_PRODUCT_RECS = "ContentBasedProductRecs"

}
